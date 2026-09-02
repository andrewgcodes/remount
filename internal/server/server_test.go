package server

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"remount.dev/remount/internal/artifact"
)

func testDigest(body string) string {
	sum := sha256.Sum256([]byte(body))
	return artifact.ID(sum[:])
}

func newTestServer(t *testing.T, opts Options) *Server {
	t.Helper()
	s, err := New(opts)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := s.Close(); err != nil {
			t.Errorf("Close: %v", err)
		}
	})
	return s
}

func TestArtifactPutMismatchCannotDeleteExistingBlob(t *testing.T) {
	s := newTestServer(t, Options{MaxArtifactBytes: 1024})
	body := "pre-existing valid body"
	bodyID, _, err := s.Store.Put(strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	wanted := testDigest("wanted")
	req := httptest.NewRequest(http.MethodPut, "/v1/artifacts/"+wanted, strings.NewReader(body))
	rec := httptest.NewRecorder()
	s.Handler().ServeHTTP(rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, body = %s", rec.Code, rec.Body.String())
	}
	if !s.Store.Has(bodyID) {
		t.Fatal("digest mismatch deleted the body's pre-existing valid artifact")
	}
	if s.Store.Has(wanted) {
		t.Fatal("digest mismatch published the requested artifact")
	}
}

func TestArtifactPutEnforcesDeclaredAndStreamingLimits(t *testing.T) {
	for _, tc := range []struct {
		name          string
		contentLength int64
	}{
		{name: "declared", contentLength: 5},
		{name: "streaming", contentLength: -1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := newTestServer(t, Options{MaxArtifactBytes: 4})
			body := "12345"
			req := httptest.NewRequest(http.MethodPut, "/v1/artifacts/"+testDigest(body), strings.NewReader(body))
			req.ContentLength = tc.contentLength
			rec := httptest.NewRecorder()
			s.Handler().ServeHTTP(rec, req)
			if rec.Code != http.StatusRequestEntityTooLarge {
				t.Fatalf("status = %d, body = %s", rec.Code, rec.Body.String())
			}
			ids, err := s.Store.List()
			if err != nil || len(ids) != 0 {
				t.Fatalf("oversized upload was published: %v, %v", ids, err)
			}
		})
	}
}

func TestServeWaitReadyHealthAndIdempotentClose(t *testing.T) {
	s := newTestServer(t, Options{})
	ctx, cancel := context.WithCancel(context.Background())
	serveErr := make(chan error, 1)
	go func() { serveErr <- s.Serve(ctx, "127.0.0.1:0") }()
	waitCtx, waitCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer waitCancel()
	addr, err := s.WaitReady(waitCtx)
	if err != nil || addr == "" || addr != s.Addr() {
		t.Fatalf("WaitReady = (%q, %v), Addr = %q", addr, err, s.Addr())
	}
	resp, err := http.Get("http://" + addr + "/readyz")
	if err != nil {
		t.Fatal(err)
	}
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK || !bytes.Contains(body, []byte(`"ok":true`)) {
		t.Fatalf("ready response = %d %s", resp.StatusCode, body)
	}
	cancel()
	select {
	case err := <-serveErr:
		if err != nil {
			t.Fatalf("Serve after cancellation: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Serve did not stop after context cancellation")
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatalf("second Close: %v", err)
	}
}

func TestCloseBeforeServeWakesWaitReadyAndRemovesTemporaryStore(t *testing.T) {
	s, err := New(Options{})
	if err != nil {
		t.Fatal(err)
	}
	tempDir := s.tempArtDir
	if tempDir == "" {
		t.Fatal("test server did not allocate temporary artifact directory")
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if _, err := s.WaitReady(ctx); err == nil {
		t.Fatal("WaitReady succeeded after Close-before-Serve")
	}
	if _, err := os.Stat(tempDir); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("temporary artifact directory remains: %v", err)
	}
	if err := s.Serve(context.Background(), "127.0.0.1:0"); err == nil {
		t.Fatal("Serve succeeded after Close")
	}
}

func TestConcurrentHealthAddrAndClose(t *testing.T) {
	s, err := New(Options{})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	serveErr := make(chan error, 1)
	go func() { serveErr <- s.Serve(ctx, "127.0.0.1:0") }()
	waitCtx, waitCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer waitCancel()
	if _, err := s.WaitReady(waitCtx); err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				_ = s.Addr()
				rec := httptest.NewRecorder()
				s.Handler().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/healthz", nil))
			}
		}()
	}
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_ = s.Close()
		}()
	}
	wg.Wait()
	cancel()
	select {
	case err := <-serveErr:
		if err != nil {
			t.Fatalf("Serve: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Serve did not stop")
	}
}
