package server

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"remount.dev/remount/internal/control"
	"remount.dev/remount/internal/proto"
)

func newAPIServer(t *testing.T, adjust func(*Options)) (*Server, *httptest.Server) {
	t.Helper()
	opts := Options{Token: "tok", ArtifactGCInterval: -1, EventGCInterval: -1, RecordGCInterval: -1}
	if adjust != nil {
		adjust(&opts)
	}
	s, err := New(opts)
	if err != nil {
		t.Fatal(err)
	}
	hs := httptest.NewServer(s.Handler())
	t.Cleanup(func() {
		hs.Close()
		_ = s.Close()
	})
	return s, hs
}

func apiCall(t *testing.T, hs *httptest.Server, method, path, token string, body string, hdr map[string]string) (*http.Response, []byte) {
	t.Helper()
	var rd io.Reader
	if body != "" {
		rd = strings.NewReader(body)
	}
	req, err := http.NewRequest(method, hs.URL+path, rd)
	if err != nil {
		t.Fatal(err)
	}
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	if body != "" {
		req.Header.Set("Content-Type", "application/json")
	}
	for k, v := range hdr {
		req.Header.Set(k, v)
	}
	hc := &http.Client{CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	resp, err := hc.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	out, _ := io.ReadAll(resp.Body)
	return resp, out
}

// A credential resolves to the same subject over HTTP as over the wire, so
// tenancy is enforced identically: a tenant never sees another tenant's
// agents, and an unknown credential is refused before any lookup.
func TestAPITenancyMatchesControlPlaneIdentity(t *testing.T) {
	_, hs := newAPIServer(t, func(o *Options) {
		o.Authenticator = control.StaticAuthenticator{
			"team-tok":  {ID: "alice", Tenant: "team"},
			"rival-tok": {ID: "bob", Tenant: "rivals"},
		}
	})
	create := `{"name":"mine","workspace":{"name":"ws"},"spec":{"recipe":"custom","task":"t","acp_command":["/bin/true"]}}`
	resp, body := apiCall(t, hs, "POST", "/v1/agents", "team-tok", create, map[string]string{"Idempotency-Key": "k1"})
	if resp.StatusCode != 201 {
		t.Fatalf("create = %d %s", resp.StatusCode, body)
	}
	var a proto.Agent
	if err := json.Unmarshal(body, &a); err != nil || a.Tenant != "team" || a.Owner != "alice" {
		t.Fatalf("agent = %+v err=%v", a, err)
	}
	if resp, body := apiCall(t, hs, "GET", "/v1/agents/"+a.ID, "rival-tok", "", nil); resp.StatusCode != 403 {
		t.Fatalf("cross-tenant read = %d %s", resp.StatusCode, body)
	}
	if resp, body := apiCall(t, hs, "GET", "/v1/agents", "rival-tok", "", nil); resp.StatusCode != 200 || !strings.Contains(string(body), `"agents":[]`) {
		t.Fatalf("cross-tenant list = %d %s", resp.StatusCode, body)
	}
	if resp, body := apiCall(t, hs, "POST", "/v1/agents/"+a.ID+"/cancel", "rival-tok", "", nil); resp.StatusCode != 403 {
		t.Fatalf("cross-tenant cancel = %d %s", resp.StatusCode, body)
	}
	for _, tok := range []string{"", "stranger", "tok"} {
		if resp, _ := apiCall(t, hs, "GET", "/v1/agents", tok, "", nil); resp.StatusCode != 401 {
			t.Fatalf("token %q = %d", tok, resp.StatusCode)
		}
	}
	// Error bodies are structured with the wire code, never free text.
	resp, body = apiCall(t, hs, "GET", "/v1/agents/ag_missing", "team-tok", "", nil)
	var eb apiErrorBody
	if resp.StatusCode != 404 || json.Unmarshal(body, &eb) != nil || eb.Error.Code != proto.CodeNotFound {
		t.Fatalf("not found = %d %s", resp.StatusCode, body)
	}
	if resp, body := apiCall(t, hs, "POST", "/v1/agents", "team-tok", `{"name":`, nil); resp.StatusCode != 400 {
		t.Fatalf("truncated json = %d %s", resp.StatusCode, body)
	}
}

// The pool holds one SDK client per credential, refuses new credentials
// past its cap while every client is busy, evicts idle ones to make room,
// and never keys by the raw token.
func TestAPIClientPoolBoundsCredentials(t *testing.T) {
	s, hs := newAPIServer(t, func(o *Options) {
		o.Authenticator = control.StaticAuthenticator{
			"a-tok": {ID: "a", Tenant: "t"},
			"b-tok": {ID: "b", Tenant: "t"},
			"c-tok": {ID: "c", Tenant: "t"},
		}
		o.MaxAPIClients = 2
		o.APIClientIdle = time.Hour
	})
	for _, tok := range []string{"a-tok", "b-tok"} {
		if resp, body := apiCall(t, hs, "GET", "/v1/agents", tok, "", nil); resp.StatusCode != 200 {
			t.Fatalf("%s = %d %s", tok, resp.StatusCode, body)
		}
	}
	s.clients.mu.Lock()
	for key := range s.clients.entries {
		if key == "a-tok" || key == "b-tok" || len(key) != 64 {
			s.clients.mu.Unlock()
			t.Fatalf("pool keyed by %q", key)
		}
	}
	n := len(s.clients.entries)
	s.clients.mu.Unlock()
	if n != 2 {
		t.Fatalf("pool size = %d", n)
	}
	// Both idle: a third credential evicts the least recently used.
	if resp, body := apiCall(t, hs, "GET", "/v1/agents", "c-tok", "", nil); resp.StatusCode != 200 {
		t.Fatalf("c-tok = %d %s", resp.StatusCode, body)
	}
	// Hold two references so nothing is idle; the fourth credential is
	// refused with a bounded, observable error rather than growing the pool.
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	_, relA, err := s.clients.acquire(ctx, "a-tok")
	if err != nil {
		t.Fatal(err)
	}
	_, relC, err := s.clients.acquire(ctx, "c-tok")
	if err != nil {
		t.Fatal(err)
	}
	if resp, body := apiCall(t, hs, "GET", "/v1/agents", "b-tok", "", nil); resp.StatusCode != 429 {
		t.Fatalf("over cap = %d %s", resp.StatusCode, body)
	}
	relA()
	relC()
	// The sweep closes clients idle past the window and the next request
	// simply reconnects.
	if n := s.clients.sweep(time.Now().Add(2 * time.Hour)); n != 2 {
		t.Fatalf("swept %d, want 2", n)
	}
	if resp, body := apiCall(t, hs, "GET", "/v1/agents", "a-tok", "", nil); resp.StatusCode != 200 {
		t.Fatalf("after sweep = %d %s", resp.StatusCode, body)
	}
	// A bad credential is not cached.
	if resp, _ := apiCall(t, hs, "GET", "/v1/agents", "zzz", "", nil); resp.StatusCode != 401 {
		t.Fatalf("bad credential = %d", resp.StatusCode)
	}
	s.clients.mu.Lock()
	_, cached := s.clients.entries[credentialKey("zzz")]
	s.clients.mu.Unlock()
	if cached {
		t.Fatal("failed credential was cached")
	}
}

func TestAPICORSAndStableLinks(t *testing.T) {
	_, hs := newAPIServer(t, func(o *Options) {
		o.CORSOrigins = []string{"*"}
		o.AgentURLBase = "https://ui.example/agents/{id}?tab=transcript"
		o.PublicURL = "https://api.example"
	})
	// Wildcard: any origin, but without credentials, so a browser cannot
	// ride a cookie across sites.
	resp, _ := apiCall(t, hs, "OPTIONS", "/v1/agents", "", "", map[string]string{"Origin": "https://anywhere.example", "Access-Control-Request-Method": "GET"})
	if resp.StatusCode != 204 || resp.Header.Get("Access-Control-Allow-Origin") != "*" || resp.Header.Get("Access-Control-Allow-Credentials") != "" {
		t.Fatalf("wildcard preflight = %d %v", resp.StatusCode, resp.Header)
	}
	resp, body := apiCall(t, hs, "POST", "/v1/agents", "tok", `{"workspace":{"name":"ws"},"spec":{"recipe":"custom","task":"t","acp_command":["/bin/true"]}}`, map[string]string{"Origin": "https://anywhere.example"})
	if resp.StatusCode != 201 || resp.Header.Get("Access-Control-Allow-Origin") != "*" {
		t.Fatalf("create = %d %v %s", resp.StatusCode, resp.Header, body)
	}
	var a proto.Agent
	if err := json.Unmarshal(body, &a); err != nil {
		t.Fatal(err)
	}
	if a.URL != "https://api.example/a/"+a.ID {
		t.Fatalf("url = %q", a.URL)
	}
	resp, _ = apiCall(t, hs, "GET", "/a/"+a.ID, "tok", "", nil)
	if resp.StatusCode != 302 || resp.Header.Get("Location") != "https://ui.example/agents/"+a.ID+"?tab=transcript" {
		t.Fatalf("/a/ = %d %q", resp.StatusCode, resp.Header.Get("Location"))
	}
	// The redirect needs no credential (the UI authenticates) but the id is
	// confined to one token so it cannot rewrite the target.
	if resp, _ := apiCall(t, hs, "GET", "/a/ag_nope", "", "", nil); resp.StatusCode != 302 {
		t.Fatalf("/a/ unauthenticated = %d", resp.StatusCode)
	}
	if resp, _ := apiCall(t, hs, "GET", "/a/x%3F%40evil.example%2F", "tok", "", nil); resp.StatusCode != 404 {
		t.Fatalf("/a/ hostile id = %d", resp.StatusCode)
	}
	// The wake reason is validated at the edge.
	if resp, body := apiCall(t, hs, "POST", "/v1/agents/"+a.ID+"/wake", "tok", `{"by":"timer"}`, nil); resp.StatusCode != 400 {
		t.Fatalf("wake by=timer = %d %s", resp.StatusCode, body)
	}
	// An unknown action is 404, not a silent no-op.
	if resp, _ := apiCall(t, hs, "POST", "/v1/agents/"+a.ID+"/explode", "tok", "", nil); resp.StatusCode != 404 {
		t.Fatalf("unknown action = %d", resp.StatusCode)
	}
}
