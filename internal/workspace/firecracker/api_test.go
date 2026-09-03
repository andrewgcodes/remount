package firecracker

import (
	"context"
	"encoding/json"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestUnixAPIUsesFirecrackerSchemasAndBoundsErrors(t *testing.T) {
	dir, err := os.MkdirTemp("/tmp", "remount-fc-api-")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)
	socket := filepath.Join(dir, "api.sock")
	listener, err := net.Listen("unix", socket)
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()

	var mu sync.Mutex
	var gotMethod, gotPath string
	var gotBody map[string]any
	server := &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		gotMethod, gotPath = r.Method, r.URL.Path
		_ = json.NewDecoder(r.Body).Decode(&gotBody)
		mu.Unlock()
		switch r.URL.Path {
		case "/version":
			_, _ = w.Write([]byte(`{"firecracker_version":"1.17.0"}`))
		case "/denied":
			w.WriteHeader(http.StatusBadRequest)
			_, _ = w.Write([]byte(`{"fault_message":"bad snapshot"}`))
		case "/oversized":
			w.WriteHeader(http.StatusInternalServerError)
			_, _ = w.Write([]byte(strings.Repeat("x", maxAPIErrorBytes+1)))
		default:
			w.WriteHeader(http.StatusNoContent)
		}
	})}
	done := make(chan error, 1)
	go func() { done <- server.Serve(listener) }()
	defer func() {
		_ = server.Close()
		<-done
	}()

	api := newUnixAPI(socket, time.Second)
	if err := api.Put(context.Background(), "/actions", map[string]string{"action_type": "InstanceStart"}); err != nil {
		t.Fatal(err)
	}
	mu.Lock()
	if gotMethod != http.MethodPut || gotPath != "/actions" || gotBody["action_type"] != "InstanceStart" {
		t.Fatalf("request=%s %s %#v", gotMethod, gotPath, gotBody)
	}
	mu.Unlock()
	var version struct {
		Version string `json:"firecracker_version"`
	}
	if err := api.Get(context.Background(), "/version", &version); err != nil || version.Version != "1.17.0" {
		t.Fatalf("version=%q err=%v", version.Version, err)
	}
	if err := api.Put(context.Background(), "/denied", nil); err == nil || !strings.Contains(err.Error(), "bad snapshot") {
		t.Fatalf("structured API error=%v", err)
	}
	if err := api.Put(context.Background(), "/oversized", nil); err == nil || !strings.Contains(err.Error(), "exceeded") {
		t.Fatalf("oversized API error=%v", err)
	}
}

func TestUnixAPIRejectsRelativePathWithoutDial(t *testing.T) {
	err := newUnixAPI("/does/not/exist", time.Second).Get(context.Background(), "version", nil)
	if err == nil || !strings.Contains(err.Error(), "not absolute") {
		t.Fatalf("err=%v", err)
	}
}

func TestUnixAPITimeoutIsObservable(t *testing.T) {
	dir, err := os.MkdirTemp("/tmp", "remount-fc-timeout-")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)
	socket := filepath.Join(dir, "api.sock")
	listener, err := net.Listen("unix", socket)
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	server := &http.Server{Handler: http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		time.Sleep(250 * time.Millisecond)
	})}
	go server.Serve(listener)
	defer server.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if err := newUnixAPI(socket, time.Second).Get(ctx, "/version", nil); err == nil || !strings.Contains(err.Error(), "context deadline exceeded") {
		t.Fatalf("timeout err=%v", err)
	}
}
