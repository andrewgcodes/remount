package providerutil

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
)

func TestHTTPRetriesBoundedAndSanitizesProviderBody(t *testing.T) {
	const secret = "enroll-secret-canary"
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if calls.Add(1) < 3 {
			http.Error(w, secret, http.StatusServiceUnavailable)
			return
		}
		_, _ = w.Write([]byte(`{"ok":true}`))
	}))
	defer server.Close()
	client, err := NewHTTP(HTTPOptions{Provider: "fake", Endpoint: server.URL, Retries: 3})
	if err != nil {
		t.Fatal(err)
	}
	var response struct {
		OK bool `json:"ok"`
	}
	if err := client.JSON(context.Background(), http.MethodGet, "/resource", nil, nil, &response, http.StatusOK); err != nil {
		t.Fatal(err)
	}
	if !response.OK || calls.Load() != 3 {
		t.Fatalf("response=%+v calls=%d", response, calls.Load())
	}

	server.Config.Handler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, secret, http.StatusBadRequest)
	})
	err = client.JSON(context.Background(), http.MethodGet, "/resource", nil, nil, nil, http.StatusOK)
	if err == nil || strings.Contains(err.Error(), secret) {
		t.Fatalf("error leaked provider body: %v", err)
	}
}

func TestHTTPRefusesRedirectAndBoundsResponse(t *testing.T) {
	redirect := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, "https://example.invalid/credential-sink", http.StatusFound)
	}))
	defer redirect.Close()
	client, err := NewHTTP(HTTPOptions{Provider: "fake", Endpoint: redirect.URL, Retries: 1, BodyLimit: 1024})
	if err != nil {
		t.Fatal(err)
	}
	err = client.JSON(context.Background(), http.MethodGet, "/", nil, nil, nil, http.StatusOK)
	var status *StatusError
	if !errors.As(err, &status) || status.Status != http.StatusFound {
		t.Fatalf("redirect must be returned, not followed: %v", err)
	}

	large := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte("123456789"))
	}))
	defer large.Close()
	client, err = NewHTTP(HTTPOptions{Provider: "fake", Endpoint: large.URL, Retries: 1, BodyLimit: 8})
	if err != nil {
		t.Fatal(err)
	}
	if err := client.JSON(context.Background(), http.MethodGet, "/", nil, nil, nil, http.StatusOK); err == nil || !strings.Contains(err.Error(), "exceeds") {
		t.Fatalf("expected bounded response error, got %v", err)
	}
}
