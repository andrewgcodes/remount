package e2b

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"remount.dev/remount/internal/provision"
)

func TestDriverCreateListDestroyContract(t *testing.T) {
	const enrollment = "one-time-secret-canary"
	var mu sync.Mutex
	var current *sandbox
	posts := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("X-API-Key") != "api-key" {
			t.Errorf("missing X-API-Key")
		}
		mu.Lock()
		defer mu.Unlock()
		switch r.Method {
		case http.MethodGet:
			if r.URL.Path != "/v2/sandboxes" {
				t.Errorf("GET path=%s", r.URL.Path)
			}
			if current == nil {
				_, _ = w.Write([]byte("[]"))
				return
			}
			_ = json.NewEncoder(w).Encode([]sandbox{*current})
		case http.MethodPost:
			posts++
			var create newSandbox
			if err := json.NewDecoder(r.Body).Decode(&create); err != nil {
				t.Error(err)
			}
			if create.TemplateID != "template" || !create.Secure || create.EnvVars["REMOUNT_ENROLL_TOKEN"] != enrollment {
				t.Errorf("create=%+v", create)
			}
			if create.Network == nil || create.Network.AllowPublicTraffic == nil || *create.Network.AllowPublicTraffic || len(create.Network.AllowOut) != 1 {
				t.Errorf("missing defense-in-depth network config: %+v", create.Network)
			}
			encodedMetadata, _ := json.Marshal(create.Metadata)
			if strings.Contains(string(encodedMetadata), enrollment) {
				t.Error("enrollment token entered metadata")
			}
			current = &sandbox{SandboxID: "sbx-id", StartedAt: time.Now(), State: "running", Metadata: create.Metadata}
			w.WriteHeader(http.StatusCreated)
			_ = json.NewEncoder(w).Encode(current)
		case http.MethodDelete:
			if r.URL.Path != "/sandboxes/sbx-id" {
				t.Errorf("DELETE path=%s", r.URL.Path)
			}
			current = nil
			w.WriteHeader(http.StatusNoContent)
		}
	}))
	defer server.Close()
	allowPublic := false
	driver, err := New(Config{Endpoint: server.URL, APIKey: "api-key", Template: "template", Network: &NetworkConfig{AllowPublicTraffic: &allowPublic, AllowOut: []string{"control.example"}}})
	if err != nil {
		t.Fatal(err)
	}
	request := testRequest(enrollment)
	machine, err := driver.Create(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	if machine.ID != "sbx-id" || machine.Tenant != "tenant-a" || machine.Provider != "e2b" {
		t.Fatalf("machine=%+v", machine)
	}
	if _, err := driver.Create(context.Background(), request); err != nil {
		t.Fatal(err)
	}
	if posts != 1 {
		t.Fatalf("idempotent create posted %d times", posts)
	}
	listed, err := driver.List(context.Background(), provision.ListOptions{Tenant: "tenant-a", Pool: "pool-a"})
	if err != nil || len(listed) != 1 {
		t.Fatalf("list=%+v err=%v", listed, err)
	}
	if err := driver.Destroy(context.Background(), machine.ID); err != nil {
		t.Fatal(err)
	}
}

func TestDriverUnavailablePlacementAndSanitizedProviderError(t *testing.T) {
	driver, err := New(Config{Endpoint: "https://example.invalid", APIKey: "api-key", Template: "template"})
	if err != nil {
		t.Fatal(err)
	}
	request := testRequest("secret")
	request.Region = "us-east-1"
	if _, err := driver.Create(context.Background(), request); !errors.Is(err, provision.ErrUnavailable) {
		t.Fatalf("region error=%v", err)
	}

	const echoed = "echoed-secret"
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { http.Error(w, echoed, http.StatusUnauthorized) }))
	defer server.Close()
	driver, err = New(Config{Endpoint: server.URL, APIKey: echoed, Template: "template", ProviderRetries: 1})
	if err != nil {
		t.Fatal(err)
	}
	_, err = driver.List(context.Background(), provision.ListOptions{})
	if err == nil || strings.Contains(err.Error(), echoed) {
		t.Fatalf("provider body leaked: %v", err)
	}
}

func TestDriverRejectsEnrollmentTokenInMetadata(t *testing.T) {
	const secret = "secret-canary"
	driver, err := New(Config{Endpoint: "https://example.invalid", APIKey: "api-key", Template: "template"})
	if err != nil {
		t.Fatal(err)
	}
	request := testRequest(secret)
	request.Labels["description"] = "copied-" + secret
	_, err = driver.Create(context.Background(), request)
	if err == nil || strings.Contains(err.Error(), secret) {
		t.Fatalf("secret boundary error=%v", err)
	}
}

func TestListFollowsV2Pagination(t *testing.T) {
	page := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		page++
		if page == 1 {
			w.Header().Set("X-Next-Token", "next-page")
			_, _ = w.Write([]byte(`[{"sandboxID":"first","metadata":{"remount_managed":"true","remount_name":"first","remount_tenant":"tenant-a","remount_pool":"pool-a"}}]`))
			return
		}
		if r.URL.Query().Get("nextToken") != "next-page" {
			t.Errorf("nextToken=%q", r.URL.Query().Get("nextToken"))
		}
		_, _ = w.Write([]byte(`[{"sandboxID":"second","metadata":{"remount_managed":"true","remount_name":"second","remount_tenant":"tenant-a","remount_pool":"pool-a"}}]`))
	}))
	defer server.Close()
	driver, err := New(Config{Endpoint: server.URL, APIKey: "api-key", Template: "template"})
	if err != nil {
		t.Fatal(err)
	}
	machines, err := driver.List(context.Background(), provision.ListOptions{Tenant: "tenant-a", Pool: "pool-a"})
	if err != nil || len(machines) != 2 || page != 2 {
		t.Fatalf("machines=%+v pages=%d err=%v", machines, page, err)
	}
}

func TestConcurrentCreateIsIdempotent(t *testing.T) {
	var mu sync.Mutex
	var current *sandbox
	posts := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		defer mu.Unlock()
		if r.Method == http.MethodGet {
			if current == nil {
				_, _ = w.Write([]byte("[]"))
			} else {
				_ = json.NewEncoder(w).Encode([]sandbox{*current})
			}
			return
		}
		posts++
		var create newSandbox
		if err := json.NewDecoder(r.Body).Decode(&create); err != nil {
			t.Error(err)
		}
		current = &sandbox{SandboxID: "only-one", Metadata: create.Metadata}
		w.WriteHeader(http.StatusCreated)
		_ = json.NewEncoder(w).Encode(current)
	}))
	defer server.Close()
	driver, err := New(Config{Endpoint: server.URL, APIKey: "api-key", Template: "template"})
	if err != nil {
		t.Fatal(err)
	}
	request := testRequest("secret")
	var wait sync.WaitGroup
	errorsSeen := make(chan error, 8)
	for range 8 {
		wait.Add(1)
		go func() { defer wait.Done(); _, err := driver.Create(context.Background(), request); errorsSeen <- err }()
	}
	wait.Wait()
	close(errorsSeen)
	for err := range errorsSeen {
		if err != nil {
			t.Fatal(err)
		}
	}
	if posts != 1 {
		t.Fatalf("concurrent create posted %d times", posts)
	}
}

func testRequest(token string) provision.Request {
	return provision.Request{Name: "node-a", Tenant: "tenant-a", Labels: map[string]string{provision.PoolLabel: "pool-a"}, Bootstrap: provision.Bootstrap{
		ServerURL: "https://control.example", EnrollmentToken: token, BinaryURL: "https://control.example/remount", Backend: "process", DataDir: "/var/lib/remount",
	}}
}
