package tenant

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/netip"
	"net/url"
	"strings"
	"testing"
	"time"
)

type roundTripFunc func(*http.Request) (*http.Response, error)

func (fn roundTripFunc) RoundTrip(request *http.Request) (*http.Response, error) { return fn(request) }

type resolverFunc func(context.Context, string, string) ([]netip.Addr, error)

func (fn resolverFunc) LookupNetIP(ctx context.Context, network, host string) ([]netip.Addr, error) {
	return fn(ctx, network, host)
}

func httpResponse(status int) *http.Response {
	return &http.Response{StatusCode: status, Header: make(http.Header), Body: io.NopCloser(strings.NewReader("ignored secret-like body"))}
}

func TestStripeExporterMatchesV1Schema(t *testing.T) {
	now := time.Unix(1_800_000_000, 0).UTC()
	var captured *http.Request
	var body string
	exporter, err := NewStripeExporter(StripeOptions{
		APIKey:     func(context.Context) (string, error) { return "sk_test_top_secret", nil },
		CustomerID: func(context.Context, string) (string, error) { return "cus_123", nil },
		EventNames: map[MeterKind]string{MeterBytesOut: "remount_bytes_out"},
		Now:        func() time.Time { return now },
		RoundTripper: roundTripFunc(func(request *http.Request) (*http.Response, error) {
			captured = request.Clone(request.Context())
			raw, _ := io.ReadAll(request.Body)
			body = string(raw)
			return httpResponse(http.StatusOK), nil
		}),
	})
	if err != nil {
		t.Fatal(err)
	}
	event := MeterEvent{ID: "meter_01", Tenant: "tenant-a", Kind: MeterBytesOut, Value: 42, At: now.Add(-time.Minute)}
	if err := exporter.Export(context.Background(), []MeterEvent{event}); err != nil {
		t.Fatal(err)
	}
	if captured.URL.String() != stripeMeterEventsURL || captured.Method != http.MethodPost {
		t.Fatalf("request = %s %s", captured.Method, captured.URL)
	}
	if captured.Header.Get("Authorization") != "Bearer sk_test_top_secret" || captured.Header.Get("Idempotency-Key") != event.ID ||
		captured.Header.Get("Content-Type") != "application/x-www-form-urlencoded" {
		t.Fatalf("headers = %+v", captured.Header)
	}
	values, err := url.ParseQuery(body)
	if err != nil {
		t.Fatal(err)
	}
	want := map[string]string{
		"event_name": "remount_bytes_out", "identifier": "meter_01", "timestamp": "1799999940",
		"payload[stripe_customer_id]": "cus_123", "payload[value]": "42",
	}
	for key, value := range want {
		if values.Get(key) != value {
			t.Fatalf("form[%s] = %q, body=%q", key, values.Get(key), body)
		}
	}
}

func TestStripeRejectsWindowAndNeverLeaksSecrets(t *testing.T) {
	now := time.Unix(1_800_000_000, 0).UTC()
	secret := "sk_should_never_appear"
	exporter, err := NewStripeExporter(StripeOptions{
		APIKey: func(context.Context) (string, error) { return secret, nil }, CustomerID: func(context.Context, string) (string, error) { return "cus_ok", nil },
		EventNames: map[MeterKind]string{MeterBytesIn: "bytes_in"}, Now: func() time.Time { return now },
		RoundTripper: roundTripFunc(func(*http.Request) (*http.Response, error) { return httpResponse(http.StatusBadRequest), nil }),
	})
	if err != nil {
		t.Fatal(err)
	}
	event := MeterEvent{ID: "meter_old", Tenant: "tenant-a", Kind: MeterBytesIn, Value: 1, At: now.Add(-36 * 24 * time.Hour)}
	if err := exporter.Export(context.Background(), []MeterEvent{event}); !IsCode(err, CodeBadRequest) {
		t.Fatalf("old timestamp = %v", err)
	}
	event.At = now
	err = exporter.Export(context.Background(), []MeterEvent{event})
	if err == nil || strings.Contains(err.Error(), secret) || strings.Contains(err.Error(), "ignored secret") {
		t.Fatalf("unsafe Stripe error = %v", err)
	}
	var exportErr *Error
	if !errors.As(err, &exportErr) || exportErr.HTTPStatus != http.StatusBadRequest || exportErr.Retryable {
		t.Fatalf("Stripe failure classification = %#v", err)
	}
}

func TestHTTPExporterAllowlistSSRFAndEnvelope(t *testing.T) {
	for _, destination := range []string{
		"http://billing.example/v1", "https://127.0.0.1/v1", "https://metadata.google.internal/v1",
		"https://billing.example.evil.test/v1", "https://user:pass@billing.example/v1", "https://billing.example/v1?q=secret",
	} {
		if _, err := NewHTTPExporter(HTTPExporterOptions{Destination: destination, AllowedHosts: []string{"billing.example"}}); !IsCode(err, CodeBadRequest) {
			t.Fatalf("unsafe destination %q accepted: %v", destination, err)
		}
	}
	var captured []byte
	exporter, err := NewHTTPExporter(HTTPExporterOptions{Destination: "https://billing.example/v1/meters", AllowedHosts: []string{"billing.example"},
		Secret: func(context.Context) (string, error) { return "bearer-secret", nil },
		RoundTripper: roundTripFunc(func(request *http.Request) (*http.Response, error) {
			if request.Header.Get("Authorization") != "Bearer bearer-secret" {
				t.Fatalf("authorization = %q", request.Header.Get("Authorization"))
			}
			captured, _ = io.ReadAll(request.Body)
			return httpResponse(http.StatusNoContent), nil
		})})
	if err != nil {
		t.Fatal(err)
	}
	event := MeterEvent{Seq: 7, ID: "meter_7", Tenant: "tenant-a", Kind: MeterStorageBytes, Value: 99, At: time.Unix(1_800_000_000, 0)}
	if err := exporter.Export(context.Background(), []MeterEvent{event}); err != nil {
		t.Fatal(err)
	}
	var envelope struct {
		Version int          `json:"version"`
		Events  []MeterEvent `json:"events"`
	}
	if err := json.Unmarshal(captured, &envelope); err != nil || envelope.Version != 1 || len(envelope.Events) != 1 || envelope.Events[0].ID != event.ID {
		t.Fatalf("envelope = %+v, %v", envelope, err)
	}
	transport := safeTransport(resolverFunc(func(context.Context, string, string) ([]netip.Addr, error) {
		return []netip.Addr{netip.MustParseAddr("169.254.169.254")}, nil
	}), time.Second)
	if _, err := transport.DialContext(context.Background(), "tcp", "billing.example:443"); err == nil || !strings.Contains(err.Error(), "non-public") {
		t.Fatalf("private DNS target accepted: %v", err)
	}
	redirectCalls := 0
	redirecting, err := NewHTTPExporter(HTTPExporterOptions{Destination: "https://billing.example/v1", AllowedHosts: []string{"billing.example"},
		RoundTripper: roundTripFunc(func(request *http.Request) (*http.Response, error) {
			redirectCalls++
			response := httpResponse(http.StatusFound)
			response.Request = request
			response.Header.Set("Location", "https://evil.example/steal")
			return response, nil
		})})
	if err != nil {
		t.Fatal(err)
	}
	if err := redirecting.Export(context.Background(), []MeterEvent{event}); !IsCode(err, CodeUnavailable) || redirectCalls != 1 {
		t.Fatalf("redirect was followed: calls=%d error=%v", redirectCalls, err)
	}
}

func TestJSONLExporter(t *testing.T) {
	var output bytes.Buffer
	exporter := &JSONLExporter{Writer: &output}
	events := []MeterEvent{{Seq: 1, ID: "a", Tenant: "tenant-a", Kind: MeterBytesIn, Value: 1, At: time.Unix(1, 0)},
		{Seq: 2, ID: "b", Tenant: "tenant-a", Kind: MeterBytesOut, Value: 2, At: time.Unix(2, 0)}}
	if err := exporter.Export(context.Background(), events); err != nil {
		t.Fatal(err)
	}
	if lines := strings.Count(output.String(), "\n"); lines != 2 || strings.Contains(output.String(), "secret") {
		t.Fatalf("jsonl = %q", output.String())
	}
}
