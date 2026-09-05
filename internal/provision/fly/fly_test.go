package fly

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

type fakeSecrets struct {
	mu      sync.Mutex
	staged  map[string]string
	removed []string
}

func (s *fakeSecrets) Stage(_ context.Context, name, value string) (uint64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.staged == nil {
		s.staged = make(map[string]string)
	}
	s.staged[name] = value
	return 42, nil
}

func (s *fakeSecrets) Remove(_ context.Context, name string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.removed = append(s.removed, name)
	return nil
}

func TestDriverCreateListDestroyContract(t *testing.T) {
	const token = "one-time-secret-canary"
	var mu sync.Mutex
	var machine *flyMachine
	posts := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer api-token" {
			t.Errorf("missing bearer auth")
		}
		mu.Lock()
		defer mu.Unlock()
		switch {
		case r.Method == http.MethodGet && strings.HasSuffix(r.URL.Path, "/wait"):
			if r.URL.Query().Get("state") != "started" {
				t.Errorf("wait query=%s", r.URL.RawQuery)
			}
			_, _ = w.Write([]byte(`{"ok":true}`))
		case r.Method == http.MethodGet:
			if machine == nil {
				_, _ = w.Write([]byte("[]"))
				return
			}
			_ = json.NewEncoder(w).Encode([]flyMachine{*machine})
		case r.Method == http.MethodPost:
			posts++
			var body json.RawMessage
			if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
				t.Error(err)
			}
			if strings.Contains(string(body), token) {
				t.Errorf("token entered Machine config: %s", body)
			}
			var create flyCreate
			if err := json.Unmarshal(body, &create); err != nil {
				t.Error(err)
			}
			if len(create.Config.Processes) != 1 || len(create.Config.Processes[0].Secrets) != 1 {
				t.Errorf("missing process secret ref: %+v", create)
			}
			if create.MinSecretsVersion != 42 {
				t.Errorf("min secrets version=%d", create.MinSecretsVersion)
			}
			machine = &flyMachine{ID: "fly-id", Name: create.Name, State: "created", Region: create.Region, CreatedAt: time.Now()}
			machine.Config.Metadata = create.Config.Metadata
			w.WriteHeader(http.StatusCreated)
			_ = json.NewEncoder(w).Encode(machine)
		case r.Method == http.MethodDelete:
			if r.URL.Query().Get("force") != "true" {
				t.Errorf("destroy not forced")
			}
			machine = nil
			w.WriteHeader(http.StatusNoContent)
		default:
			http.Error(w, "unexpected", http.StatusBadRequest)
		}
	}))
	defer server.Close()
	secrets := &fakeSecrets{}
	driver, err := New(Config{Endpoint: server.URL, Token: "api-token", App: "app", Image: "image", Secrets: secrets, WaitTimeout: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	request := testRequest(token)
	got, err := driver.Create(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	if got.ID != "fly-id" || got.Tenant != "tenant-a" || got.Provider != "fly" || got.State != "started" {
		t.Fatalf("machine=%+v", got)
	}
	if _, err := driver.Create(context.Background(), request); err != nil {
		t.Fatal(err)
	}
	if posts != 1 {
		t.Fatalf("idempotent create posted %d times", posts)
	}
	if len(secrets.staged) != 1 || len(secrets.removed) != 1 {
		t.Fatalf("secret lifecycle staged=%v removed=%v", secrets.staged, secrets.removed)
	}
	for _, value := range secrets.staged {
		if value != token {
			t.Fatalf("staged=%q", value)
		}
	}
	listed, err := driver.List(context.Background(), provision.ListOptions{Tenant: "tenant-a", Pool: "pool-a"})
	if err != nil || len(listed) != 1 {
		t.Fatalf("list=%+v err=%v", listed, err)
	}
	if err := driver.Destroy(context.Background(), got.ID); err != nil {
		t.Fatal(err)
	}
}

func TestFlySizeAndAppSecretVersion(t *testing.T) {
	guest, err := parseSize("performance-2x")
	if err != nil || guest.CPUKind != "performance" || guest.CPUs != 2 || guest.MemoryMB != 4096 {
		t.Fatalf("guest=%+v err=%v", guest, err)
	}
	if _, err := parseSize("performance-cpu-2x"); err == nil {
		t.Fatal("accepted nonexistent preset spelling")
	}
	var updates []secretUpdate
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer api-token" || r.URL.Path != "/v1/apps/app/secrets" {
			t.Errorf("secret request path=%s auth=%q", r.URL.Path, r.Header.Get("Authorization"))
		}
		var update secretUpdate
		if err := json.NewDecoder(r.Body).Decode(&update); err != nil {
			t.Fatal(err)
		}
		updates = append(updates, update)
		_, _ = w.Write([]byte(`{"Version":42}`))
	}))
	defer server.Close()
	driver, err := New(Config{Endpoint: server.URL, Token: "api-token", App: "app", Image: "image", WaitTimeout: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	store := driver.secrets
	version, err := store.Stage(context.Background(), "NAME", "secret")
	if err != nil || version != 42 {
		t.Fatalf("stage version=%d err=%v", version, err)
	}
	if err := store.Remove(context.Background(), "NAME"); err != nil {
		t.Fatal(err)
	}
	if len(updates) != 2 || updates[0].Values["NAME"] == nil || *updates[0].Values["NAME"] != "secret" {
		t.Fatalf("stage updates=%+v", updates)
	}
	if value, ok := updates[1].Values["NAME"]; !ok || value != nil {
		t.Fatalf("remove updates=%+v", updates)
	}
}

func TestFlyWaitTimeoutMatchesProviderLimit(t *testing.T) {
	driver, err := New(Config{
		Endpoint: "https://example.invalid", Token: "api-token", App: "app",
		Image: "image", Secrets: &fakeSecrets{},
	})
	if err != nil {
		t.Fatal(err)
	}
	if driver.wait != time.Minute {
		t.Fatalf("default wait=%s", driver.wait)
	}
	if driver.httpTimeout <= driver.wait {
		t.Fatalf("HTTP timeout=%s does not cover provider wait=%s", driver.httpTimeout, driver.wait)
	}
	if _, err := New(Config{
		Endpoint: "https://example.invalid", Token: "api-token", App: "app",
		Image: "image", Secrets: &fakeSecrets{}, WaitTimeout: time.Minute + time.Second,
	}); err == nil {
		t.Fatal("accepted wait timeout above Fly's API limit")
	}
	if _, err := New(Config{
		Endpoint: "https://example.invalid", Token: "api-token", App: "app",
		Image: "image", Secrets: &fakeSecrets{}, WaitTimeout: time.Second,
		HTTPClient: &http.Client{Timeout: 500 * time.Millisecond},
	}); err == nil {
		t.Fatal("accepted HTTP timeout shorter than the provider wait")
	}
}

type leakingSecrets struct{ secret string }

func (s leakingSecrets) Stage(context.Context, string, string) (uint64, error) {
	return 0, errors.New(s.secret)
}
func (s leakingSecrets) Remove(context.Context, string) error { return nil }

func TestSecretStoreErrorCannotLeakEnrollment(t *testing.T) {
	const token = "secret-canary"
	driver, err := New(Config{Endpoint: "https://example.invalid", Token: "api-token", App: "app", Image: "image", Secrets: leakingSecrets{secret: token}, WaitTimeout: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	_, err = driver.Create(context.Background(), testRequest(token))
	if err == nil || strings.Contains(err.Error(), token) {
		t.Fatalf("secret store error leaked: %v", err)
	}
}

func testRequest(token string) provision.Request {
	return provision.Request{
		Name: "node-a", Tenant: "tenant-a", Region: "ord", Size: "shared-cpu-1x", Labels: map[string]string{provision.PoolLabel: "pool-a", "role": "worker"},
		Bootstrap: provision.Bootstrap{ServerURL: "https://control.example", EnrollmentToken: token, BinaryURL: "https://control.example/remount", Backend: "process", DataDir: "/var/lib/remount"},
	}
}
