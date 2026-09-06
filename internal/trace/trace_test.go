package trace

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"remount.dev/remount/internal/metrics"
	"remount.dev/remount/internal/redact"
)

// canary is a synthetic, never-real credential shaped like the OpenAI-style
// keys internal/redact matches. The exporter test plants it and proves the
// scan finds it before export, so a passing "no secret in the payload"
// assertion cannot be a scan that silently matches nothing.
const canary = "sk-remount0000trace0000canary0000000000"

type collector struct {
	mu       sync.Mutex
	bodies   [][]byte
	status   int
	received chan struct{}
}

func newCollector(status int) *collector {
	return &collector{status: status, received: make(chan struct{}, 64)}
}

func (c *collector) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	body, _ := io.ReadAll(r.Body)
	c.mu.Lock()
	c.bodies = append(c.bodies, body)
	path, contentType := r.URL.Path, r.Header.Get("Content-Type")
	c.mu.Unlock()
	if r.Method != http.MethodPost || path != defaultPath || contentType != "application/json" {
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	w.WriteHeader(c.status)
	select {
	case c.received <- struct{}{}:
	default:
	}
}

func (c *collector) wait(t *testing.T) []byte {
	t.Helper()
	select {
	case <-c.received:
	case <-time.After(10 * time.Second):
		t.Fatal("collector received no batch")
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.bodies[len(c.bodies)-1]
}

func TestExporterPostsOTLPJSONShapeWithoutSecrets(t *testing.T) {
	// The instrument first: if the scan cannot see a planted secret, a clean
	// payload proves nothing.
	if redact.String(canary) == canary {
		t.Fatalf("redaction scan does not recognise its own canary")
	}

	c := newCollector(http.StatusOK)
	srv := httptest.NewServer(c)
	defer srv.Close()

	exporter, err := NewOTLPExporter(ExporterOptions{
		Endpoint: srv.URL, Service: "remount-test",
		MaxBatch: 1, FlushInterval: 20 * time.Millisecond, Client: srv.Client(),
	})
	if err != nil {
		t.Fatal(err)
	}
	defer exporter.Close(context.Background())
	if want := srv.URL + defaultPath; exporter.Endpoint() != want {
		t.Fatalf("endpoint = %q, want %q", exporter.Endpoint(), want)
	}

	tracer := NewTracer(exporter)
	before := metrics.TraceSpansExported.Value()

	_, parent := tracer.Start(context.Background(), "ws.create")
	parent.Set("remount.ws", "ws_trace")
	// A caller that hands a credential-shaped string to an attribute must not
	// be able to export it.
	parent.Set("remount.principal", "Authorization: Bearer "+canary)
	child, childSpan := tracer.StartRemote(context.Background(), "fs.write", parent.TraceID(), parent.SpanID())
	_ = child
	childSpan.End(errors.New("upstream said " + canary))
	parent.End(nil)

	// Both spans must arrive; MaxBatch is 1 so each is its own POST.
	first := c.wait(t)
	second := c.wait(t)
	for _, body := range [][]byte{first, second} {
		if strings.Contains(string(body), canary) {
			t.Fatalf("exported payload carries the planted credential: %s", body)
		}
	}

	var doc struct {
		ResourceSpans []struct {
			Resource struct {
				Attributes []struct {
					Key   string `json:"key"`
					Value struct {
						StringValue string `json:"stringValue"`
					} `json:"value"`
				} `json:"attributes"`
			} `json:"resource"`
			ScopeSpans []struct {
				Scope struct {
					Name string `json:"name"`
				} `json:"scope"`
				Spans []struct {
					TraceID           string `json:"traceId"`
					SpanID            string `json:"spanId"`
					ParentSpanID      string `json:"parentSpanId"`
					Name              string `json:"name"`
					Kind              int    `json:"kind"`
					StartTimeUnixNano string `json:"startTimeUnixNano"`
					EndTimeUnixNano   string `json:"endTimeUnixNano"`
					Attributes        []struct {
						Key   string `json:"key"`
						Value struct {
							StringValue string `json:"stringValue"`
						} `json:"value"`
					} `json:"attributes"`
					Status struct {
						Code    int    `json:"code"`
						Message string `json:"message"`
					} `json:"status"`
				} `json:"spans"`
			} `json:"scopeSpans"`
		} `json:"resourceSpans"`
	}
	byName := map[string]int{}
	for _, body := range [][]byte{first, second} {
		// Decode into a fresh slice each time: json.Unmarshal reuses existing
		// elements, so a retained value would carry an absent parentSpanId
		// over from the previous span and hide the bug this asserts on.
		doc.ResourceSpans = nil
		if err := json.Unmarshal(body, &doc); err != nil {
			t.Fatalf("exported payload is not OTLP JSON: %v (%s)", err, body)
		}
		if len(doc.ResourceSpans) != 1 || len(doc.ResourceSpans[0].ScopeSpans) != 1 {
			t.Fatalf("want one resourceSpans/scopeSpans pair, got %s", body)
		}
		resource := doc.ResourceSpans[0].Resource
		if len(resource.Attributes) != 1 || resource.Attributes[0].Key != "service.name" ||
			resource.Attributes[0].Value.StringValue != "remount-test" {
			t.Fatalf("resource attributes = %+v", resource.Attributes)
		}
		scope := doc.ResourceSpans[0].ScopeSpans[0]
		if scope.Scope.Name != scopeName {
			t.Fatalf("scope name = %q, want %q", scope.Scope.Name, scopeName)
		}
		for _, span := range scope.Spans {
			byName[span.Name]++
			if len(span.TraceID) != 32 || len(span.SpanID) != 16 {
				t.Fatalf("ids are not hex-encoded OTLP/HTTP ids: %q %q", span.TraceID, span.SpanID)
			}
			if span.Kind != KindServer {
				t.Fatalf("span kind = %d, want %d", span.Kind, KindServer)
			}
			start, err := strconv.ParseInt(span.StartTimeUnixNano, 10, 64)
			if err != nil {
				t.Fatalf("startTimeUnixNano is not a decimal string: %q", span.StartTimeUnixNano)
			}
			end, err := strconv.ParseInt(span.EndTimeUnixNano, 10, 64)
			if err != nil || end < start {
				t.Fatalf("endTimeUnixNano %q is not a decimal string at or after %d", span.EndTimeUnixNano, start)
			}
			switch span.Name {
			case "ws.create":
				if span.ParentSpanID != "" {
					t.Fatalf("root span has parent %q", span.ParentSpanID)
				}
				if span.Status.Code != StatusOK {
					t.Fatalf("successful span status = %d", span.Status.Code)
				}
				attrs := map[string]string{}
				for _, a := range span.Attributes {
					attrs[a.Key] = a.Value.StringValue
				}
				if attrs["remount.ws"] != "ws_trace" {
					t.Fatalf("workspace attribute = %q", attrs["remount.ws"])
				}
				if !strings.Contains(attrs["remount.principal"], redact.Mark) {
					t.Fatalf("credential-shaped attribute was not redacted: %q", attrs["remount.principal"])
				}
			case "fs.write":
				if span.ParentSpanID == "" {
					t.Fatal("remote child span lost its parent")
				}
				if span.Status.Code != StatusError {
					t.Fatalf("failed span status = %d, want %d", span.Status.Code, StatusError)
				}
				if !strings.Contains(span.Status.Message, redact.Mark) {
					t.Fatalf("error status message was not redacted: %q", span.Status.Message)
				}
			}
		}
	}
	if byName["ws.create"] != 1 || byName["fs.write"] != 1 {
		t.Fatalf("exported spans = %v", byName)
	}
	// The collector signals when it has the batch; the exporter counts it after
	// reading the response, so wait for the counter rather than racing it.
	deadline := time.Now().Add(5 * time.Second)
	for metrics.TraceSpansExported.Value() < before+2 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if metrics.TraceSpansExported.Value() < before+2 {
		t.Fatalf("export counter did not advance: %d -> %d", before, metrics.TraceSpansExported.Value())
	}
}

func TestRemoteSpanContinuesTheCallersTrace(t *testing.T) {
	tracer := NewTracer(recorderExporter{})
	ctx, parent := tracer.Start(context.Background(), "ws.create")
	traceID, spanID := Context(ctx)
	if traceID != parent.TraceID() || spanID != parent.SpanID() {
		t.Fatalf("Context = (%q,%q), span = (%q,%q)", traceID, spanID, parent.TraceID(), parent.SpanID())
	}
	_, child := tracer.StartRemote(context.Background(), "fs.write", traceID, spanID)
	if child.TraceID() != traceID {
		t.Fatalf("child trace id = %q, want %q", child.TraceID(), traceID)
	}
	if child.SpanID() == spanID {
		t.Fatal("child reused its parent's span id")
	}

	// A peer that sent nothing, or garbage, starts its own trace rather than
	// producing a span that points at an id nobody has.
	for _, bad := range []string{"", "not-hex", strings.Repeat("0", 32)} {
		_, orphan := tracer.StartRemote(context.Background(), "fs.write", bad, spanID)
		if orphan.TraceID() == "" || orphan.TraceID() == bad {
			t.Fatalf("StartRemote(%q) produced trace id %q", bad, orphan.TraceID())
		}
	}
}

func TestDisabledTracerRecordsAndExportsNothing(t *testing.T) {
	var disabled *Tracer
	if disabled.Enabled() {
		t.Fatal("a nil tracer reports enabled")
	}
	if NewTracer(nil) != nil {
		t.Fatal("a nil exporter produced an enabled tracer")
	}
	ctx, span := disabled.Start(context.Background(), "ws.create")
	if span != nil {
		t.Fatal("a disabled tracer produced a span")
	}
	// Every method must be safe on the nil span a disabled tracer returns.
	span.Set("remount.ws", "ws_1")
	span.SetStatus(StatusError, "boom")
	span.End(errors.New("boom"))
	if id := span.TraceID(); id != "" {
		t.Fatalf("nil span trace id = %q", id)
	}
	if traceID, spanID := Context(ctx); traceID != "" || spanID != "" {
		t.Fatalf("disabled context carries (%q,%q)", traceID, spanID)
	}
}

func TestExportFailuresAreCounted(t *testing.T) {
	c := newCollector(http.StatusInternalServerError)
	srv := httptest.NewServer(c)
	defer srv.Close()
	exporter, err := NewOTLPExporter(ExporterOptions{
		Endpoint: srv.URL, MaxBatch: 1, FlushInterval: 20 * time.Millisecond, Client: srv.Client(),
	})
	if err != nil {
		t.Fatal(err)
	}
	defer exporter.Close(context.Background())
	before := metrics.TraceExportFailures.Value()
	NewTracer(exporter).Start(context.Background(), "ws.create")
	_, span := NewTracer(exporter).Start(context.Background(), "ws.get")
	span.End(nil)
	c.wait(t)
	deadline := time.Now().Add(5 * time.Second)
	for metrics.TraceExportFailures.Value() == before && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if metrics.TraceExportFailures.Value() == before {
		t.Fatal("a rejected batch did not count an export failure")
	}
}

func TestEndpointMustBeAConfiguredHTTPURL(t *testing.T) {
	for _, bad := range []string{"", "   ", "example.com:4318", "ftp://example.com", "http://"} {
		if _, err := NewOTLPExporter(ExporterOptions{Endpoint: bad}); err == nil {
			t.Fatalf("NewOTLPExporter(%q) was accepted", bad)
		}
	}
	e, err := NewOTLPExporter(ExporterOptions{Endpoint: "http://collector.invalid:4318/custom/traces"})
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close(context.Background())
	if e.Endpoint() != "http://collector.invalid:4318/custom/traces" {
		t.Fatalf("an explicit path was rewritten: %q", e.Endpoint())
	}
}

type recorderExporter struct{}

func (recorderExporter) Enqueue(Record) {}
