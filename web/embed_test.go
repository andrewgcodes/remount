package webui

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestEmbeddedConsolePolicyAndFallback(t *testing.T) {
	h, err := NewHandler()
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		path, cache, contentType string
		status                   int
	}{
		{"/console/", "no-cache", "text/html", http.StatusOK},
		{"/console/config.json", "no-store", "application/json", http.StatusOK},
		{"/console/workspaces/ws_one", "no-cache", "text/html", http.StatusOK},
		{"/console/assets/missing.js", "", "text/plain", http.StatusNotFound},
		{"/console/config.json.missing", "", "text/plain", http.StatusNotFound},
	} {
		t.Run(tc.path, func(t *testing.T) {
			rec := httptest.NewRecorder()
			h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, tc.path, nil))
			if rec.Code != tc.status {
				t.Fatalf("status = %d, want %d: %s", rec.Code, tc.status, rec.Body.String())
			}
			if got := rec.Header().Get("Cache-Control"); got != tc.cache {
				t.Fatalf("Cache-Control = %q, want %q", got, tc.cache)
			}
			if got := rec.Header().Get("Content-Type"); !strings.HasPrefix(got, tc.contentType) {
				t.Fatalf("Content-Type = %q, want prefix %q", got, tc.contentType)
			}
			if rec.Header().Get("Content-Security-Policy") == "" || rec.Header().Get("X-Content-Type-Options") != "nosniff" {
				t.Fatal("security headers are missing")
			}
		})
	}
}

func TestEmbeddedConsoleRejectsMutation(t *testing.T) {
	h, err := NewHandler()
	if err != nil {
		t.Fatal(err)
	}
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/console/", nil))
	if rec.Code != http.StatusMethodNotAllowed || rec.Header().Get("Allow") != "GET, HEAD" {
		t.Fatalf("response = %d Allow=%q", rec.Code, rec.Header().Get("Allow"))
	}
}
