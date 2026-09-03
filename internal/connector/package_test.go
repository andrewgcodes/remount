package connector

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"remount.dev/remount/internal/metrics"
	"remount.dev/remount/internal/proto"
)

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(req *http.Request) (*http.Response, error) { return f(req) }

func packageRule(max int64) proto.EgressRule {
	return proto.EgressRule{
		ID: "packages", Connector: proto.EgressConnectorPackage,
		Protocol: proto.EgressProtocolHTTPS, Hosts: []string{"registry.example"},
		Methods:     []string{http.MethodGet, http.MethodHead},
		SharedState: proto.SharedStateImmutableRead, MaxResponseBytes: max,
	}
}

func packageRequest(t *testing.T, workspace, expected string, rule proto.EgressRule) ConnectorRequest {
	t.Helper()
	target, err := url.Parse("https://registry.example/v2/package.whl")
	if err != nil {
		t.Fatal(err)
	}
	return ConnectorRequest{
		Workspace: workspace, Tenant: "tenant-private", Principal: "subject",
		Generation: 7, Rule: rule, Method: http.MethodGet, URL: target,
		Header: make(http.Header), ExpectedDigest: expected,
	}
}

func readConnectorResponse(t *testing.T, response ConnectorResponse) string {
	t.Helper()
	defer response.Body.Close()
	body, err := io.ReadAll(response.Body)
	if err != nil {
		t.Fatal(err)
	}
	return string(body)
}

func TestPackageCacheReferencesAreWorkspaceScoped(t *testing.T) {
	beforeUpstream := metrics.PackageUpstreamRequests.Value()
	beforeHits := metrics.PackageCacheHits.Value()
	beforeBytes := metrics.PackageResponseBytes.Value()
	root := t.TempDir()
	store, err := NewStore(root, StoreOptions{MaxBytes: 4096, MaxBytesPerScope: 2048, MaxObjectBytes: 1024})
	if err != nil {
		t.Fatal(err)
	}
	payload := []byte("immutable-package-object")
	digestBytes := sha256.Sum256(payload)
	digest := fmt.Sprintf("sha256:%x", digestBytes)
	var mu sync.Mutex
	upstreamCalls := 0
	transport := roundTripFunc(func(req *http.Request) (*http.Response, error) {
		mu.Lock()
		upstreamCalls++
		mu.Unlock()
		if req.Header.Get(ExpectedDigestHeader) != "" {
			t.Error("connector-private digest header reached registry")
		}
		return &http.Response{
			StatusCode: http.StatusOK, Header: http.Header{"Content-Type": {"application/octet-stream"}},
			Body: io.NopCloser(strings.NewReader(string(payload))), ContentLength: int64(len(payload)),
		}, nil
	})
	packages := NewPackage(PackageOptions{Transport: transport, Store: store})
	rule := packageRule(1024)

	firstA, err := packages.Execute(context.Background(), packageRequest(t, "ws_secret_alpha", digest, rule))
	if err != nil || readConnectorResponse(t, firstA) != string(payload) || firstA.Provenance.Cached {
		t.Fatalf("first A response=%+v err=%v", firstA, err)
	}
	secondA, err := packages.Execute(context.Background(), packageRequest(t, "ws_secret_alpha", digest, rule))
	if err != nil || readConnectorResponse(t, secondA) != string(payload) || !secondA.Provenance.Cached {
		t.Fatalf("second A response=%+v err=%v", secondA, err)
	}
	mu.Lock()
	if upstreamCalls != 1 {
		t.Fatalf("workspace A did not reuse its own cache reference: calls=%d", upstreamCalls)
	}
	mu.Unlock()

	// Knowing a digest is not enough to observe a sibling's cache occupancy.
	// B must independently fetch and validate the same object once.
	firstB, err := packages.Execute(context.Background(), packageRequest(t, "ws_secret_bravo", digest, rule))
	if err != nil || readConnectorResponse(t, firstB) != string(payload) || firstB.Provenance.Cached {
		t.Fatalf("first B response=%+v err=%v", firstB, err)
	}
	secondB, err := packages.Execute(context.Background(), packageRequest(t, "ws_secret_bravo", digest, rule))
	if err != nil || readConnectorResponse(t, secondB) != string(payload) || !secondB.Provenance.Cached {
		t.Fatalf("second B response=%+v err=%v", secondB, err)
	}
	mu.Lock()
	if upstreamCalls != 2 {
		t.Fatalf("workspace B observed/reused A's reference: calls=%d", upstreamCalls)
	}
	mu.Unlock()
	if metrics.PackageUpstreamRequests.Value()-beforeUpstream != 2 ||
		metrics.PackageCacheHits.Value()-beforeHits != 2 ||
		metrics.PackageResponseBytes.Value()-beforeBytes != uint64(2*len(payload)) {
		t.Fatalf("connector metrics did not reflect upstream/cache/bytes")
	}

	for _, response := range []ConnectorResponse{firstA, secondA, firstB, secondB} {
		for name, values := range response.Header {
			joined := name + ":" + strings.Join(values, ",")
			if strings.Contains(strings.ToLower(joined), "cache") || strings.Contains(joined, root) {
				t.Fatalf("workspace-visible cache detail: %q", joined)
			}
		}
	}
	forbidden := []string{"ws_secret_alpha", "ws_secret_bravo", "tenant-private"}
	if err := filepath.WalkDir(root, func(path string, entry os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		for _, secret := range forbidden {
			if strings.Contains(path, secret) {
				return fmt.Errorf("raw scope identity in cache path %q", path)
			}
		}
		if entry.IsDir() {
			return nil
		}
		data, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		for _, secret := range forbidden {
			if strings.Contains(string(data), secret) {
				return fmt.Errorf("raw scope identity in cache metadata %q", path)
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

func TestPackageConnectorDeniesMutationsWithoutCallingRegistry(t *testing.T) {
	store, err := NewStore(t.TempDir(), StoreOptions{MaxBytes: 1024, MaxBytesPerScope: 512, MaxObjectBytes: 256})
	if err != nil {
		t.Fatal(err)
	}
	calls := 0
	packages := NewPackage(PackageOptions{Store: store, Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
		calls++
		return nil, fmt.Errorf("must not execute")
	})})
	for _, method := range []string{http.MethodPost, http.MethodPut, http.MethodDelete, "PROPFIND", "MKCOL", "LOCK"} {
		req := packageRequest(t, "ws_a", "", packageRule(128))
		req.Method = method
		decision, err := packages.Authorize(context.Background(), req)
		if err != nil || decision.Allowed || decision.Code != "method_denied" {
			t.Fatalf("method=%s decision=%+v err=%v", method, decision, err)
		}
		_, err = packages.Execute(context.Background(), req)
		var connectorErr *Error
		if !errors.As(err, &connectorErr) || connectorErr.Code != "method_denied" {
			t.Fatalf("method=%s err=%v", method, err)
		}
	}
	if calls != 0 {
		t.Fatalf("denied methods reached registry %d times", calls)
	}
}

func TestPackageConnectorEnforcesDigestAndObjectBudget(t *testing.T) {
	store, err := NewStore(t.TempDir(), StoreOptions{MaxBytes: 1024, MaxBytesPerScope: 512, MaxObjectBytes: 128})
	if err != nil {
		t.Fatal(err)
	}
	packages := NewPackage(PackageOptions{Store: store, Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
		return &http.Response{StatusCode: http.StatusOK, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(strings.Repeat("x", 65))), ContentLength: -1}, nil
	})})
	req := packageRequest(t, "ws_a", "sha256:"+strings.Repeat("0", 64), packageRule(64))
	_, err = packages.Execute(context.Background(), req)
	var connectorErr *Error
	if !errors.As(err, &connectorErr) || connectorErr.Code != "response_too_large" {
		t.Fatalf("oversized response err=%v", err)
	}

	packages = NewPackage(PackageOptions{Store: store, Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
		return &http.Response{StatusCode: http.StatusOK, Header: make(http.Header), Body: io.NopCloser(strings.NewReader("wrong")), ContentLength: 5}, nil
	})})
	req = packageRequest(t, "ws_a", "sha256:"+strings.Repeat("0", 64), packageRule(64))
	_, err = packages.Execute(context.Background(), req)
	if !errors.As(err, &connectorErr) || connectorErr.Code != "digest_mismatch" {
		t.Fatalf("digest mismatch err=%v", err)
	}

	packages = NewPackage(PackageOptions{Store: store, Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
		header := make(http.Header)
		header.Set("ETag", strings.Repeat("x", maxPackageHeaderBytes+1))
		return &http.Response{
			StatusCode: http.StatusOK, Header: header,
			Body: io.NopCloser(strings.NewReader("ok")), ContentLength: 2,
		}, nil
	})})
	req = packageRequest(t, "ws_a", "", packageRule(64))
	_, err = packages.Execute(context.Background(), req)
	if !errors.As(err, &connectorErr) || connectorErr.Code != "response_headers_too_large" {
		t.Fatalf("oversized response headers err=%v", err)
	}
}

func TestPackageWorkspaceQuotaCannotConsumeSiblingReservation(t *testing.T) {
	store, err := NewStore(t.TempDir(), StoreOptions{MaxBytes: 128, MaxBytesPerScope: 64, MaxObjectBytes: 32})
	if err != nil {
		t.Fatal(err)
	}
	packages := NewPackage(PackageOptions{Store: store, Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
		body := fmt.Sprintf("%-20s", req.URL.Path)
		return &http.Response{
			StatusCode: http.StatusOK, Header: make(http.Header),
			Body: io.NopCloser(strings.NewReader(body)), ContentLength: int64(len(body)),
		}, nil
	})})
	rule := packageRule(32)
	fetch := func(workspace, path string) error {
		req := packageRequest(t, workspace, "", rule)
		req.URL.Path = path
		response, err := packages.Execute(context.Background(), req)
		if err == nil {
			_ = readConnectorResponse(t, response)
		}
		return err
	}
	if err := fetch("ws_greedy", "/one"); err != nil {
		t.Fatal(err)
	}
	if err := fetch("ws_greedy", "/two"); err != nil {
		t.Fatal(err)
	}
	err = fetch("ws_greedy", "/three")
	var connectorErr *Error
	if !errors.As(err, &connectorErr) || connectorErr.Code != "resource_exhausted" {
		t.Fatalf("greedy workspace exceeded quota: %v", err)
	}
	if err := fetch("ws_sibling", "/sibling"); err != nil {
		t.Fatalf("one workspace consumed sibling reservation: %v", err)
	}
}

func TestUnconsumedErrorBodyRetainsWorkspaceReservation(t *testing.T) {
	store, err := NewStore(t.TempDir(), StoreOptions{MaxBytes: 64, MaxBytesPerScope: 32, MaxObjectBytes: 32})
	if err != nil {
		t.Fatal(err)
	}
	packages := NewPackage(PackageOptions{Store: store, Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
		return &http.Response{
			StatusCode: http.StatusNotFound, Header: make(http.Header),
			Body: io.NopCloser(strings.NewReader(strings.Repeat("x", 20))), ContentLength: 20,
		}, nil
	})})
	req := packageRequest(t, "ws_slow", "", packageRule(32))
	first, err := packages.Execute(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	_, err = packages.Execute(context.Background(), req)
	var connectorErr *Error
	if !errors.As(err, &connectorErr) || connectorErr.Code != "resource_exhausted" {
		t.Fatalf("unconsumed body released its reservation: %v", err)
	}
	if err := first.Body.Close(); err != nil {
		t.Fatal(err)
	}
	second, err := packages.Execute(context.Background(), req)
	if err != nil {
		t.Fatalf("closed body did not release reservation: %v", err)
	}
	_ = second.Body.Close()
}

func TestPackageObjectCountQuotaCoversTinyObjects(t *testing.T) {
	store, err := NewStore(t.TempDir(), StoreOptions{
		MaxBytes: 1024, MaxBytesPerScope: 512, MaxObjectBytes: 128,
		MaxObjects: 10, MaxObjectsPerScope: 2,
	})
	if err != nil {
		t.Fatal(err)
	}
	packages := NewPackage(PackageOptions{Store: store, Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
		body := strings.TrimPrefix(req.URL.Path, "/")
		return &http.Response{
			StatusCode: http.StatusOK, Header: make(http.Header),
			Body: io.NopCloser(strings.NewReader(body)), ContentLength: int64(len(body)),
		}, nil
	})})
	rule := packageRule(16)
	for _, path := range []string{"/a", "/b"} {
		req := packageRequest(t, "ws_tiny", "", rule)
		req.URL.Path = path
		response, err := packages.Execute(context.Background(), req)
		if err != nil {
			t.Fatal(err)
		}
		_ = readConnectorResponse(t, response)
	}
	req := packageRequest(t, "ws_tiny", "", rule)
	req.URL.Path = "/c"
	_, err = packages.Execute(context.Background(), req)
	var connectorErr *Error
	if !errors.As(err, &connectorErr) || connectorErr.Code != "resource_exhausted" {
		t.Fatalf("tiny objects bypassed count quota: %v", err)
	}
}
