package computer_test

import (
	"bytes"
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"remount.dev/remount/internal/computer"
	"remount.dev/remount/internal/computer/fakecdp"
)

// canary is a synthetic capability. It is never a real credential, and it is
// deliberately long enough that the leak scan below cannot pass by accident on
// a short or common string.
const canary = "cap_synthetic_browser_capability_0123456789"

func proxyClient(t *testing.T, s *fakecdp.Server) *computer.Client {
	t.Helper()
	return connect(t, s, func(o *computer.Options) {
		o.ProxyAuth = computer.ProxyCredentials{Username: canary}
	})
}

func callsOf(s *fakecdp.Server, method string) []fakecdp.Call {
	var out []fakecdp.Call
	for _, c := range s.Calls() {
		if c.Method == method {
			out = append(out, c)
		}
	}
	return out
}

// waitCalls polls for a fire-and-forget CDP call. Fetch responses are issued
// from the read loop without waiting for a reply, so reading once would be a
// race rather than an assertion.
func waitCalls(t *testing.T, s *fakecdp.Server, method string, want int) []fakecdp.Call {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	var out []fakecdp.Call
	for time.Now().Before(deadline) {
		out = callsOf(s, method)
		if len(out) >= want {
			return out
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("%s was called %d times, want %d; got %v", method, len(out), want, s.Methods())
	return nil
}

func authResponse(t *testing.T, call fakecdp.Call) map[string]string {
	t.Helper()
	var out map[string]string
	if err := json.Unmarshal(call.Param("authChallengeResponse"), &out); err != nil {
		t.Fatalf("decode authChallengeResponse from %s: %v", call.Params, err)
	}
	return out
}

func TestConnectEnablesFetchAuthHandling(t *testing.T) {
	s := fakecdp.New()
	defer s.Close()
	proxyClient(t, s)

	enable, ok := s.Called("Fetch.enable")
	if !ok {
		t.Fatalf("Fetch.enable was not called; got %v", s.Methods())
	}
	if enable.SessionID != s.PageSession() {
		t.Fatalf("Fetch.enable ran on session %q, want the page session %q", enable.SessionID, s.PageSession())
	}
	if got := string(enable.Param("handleAuthRequests")); got != "true" {
		t.Fatalf("Fetch.enable handleAuthRequests = %s, want true", got)
	}
	auto, ok := s.Called("Target.setAutoAttach")
	if !ok {
		t.Fatalf("Target.setAutoAttach was not called; got %v", s.Methods())
	}
	if got := string(auto.Param("flatten")); got != "true" {
		t.Fatalf("Target.setAutoAttach flatten = %s, want true", got)
	}
}

func TestNoProxyCredentialLeavesFetchDisabled(t *testing.T) {
	s := fakecdp.New()
	defer s.Close()
	connect(t, s, nil)

	if _, ok := s.Called("Fetch.enable"); ok {
		t.Fatalf("Fetch.enable ran without a proxy credential; got %v", s.Methods())
	}
}

func TestProxyChallengeGetsTheCredentialExactlyOnce(t *testing.T) {
	s := fakecdp.New()
	defer s.Close()
	proxyClient(t, s)

	challenge := map[string]any{
		"requestId":     "REQ-1",
		"authChallenge": map[string]any{"source": "Proxy", "origin": "http://broker:9000", "scheme": "basic"},
	}
	s.Emit(s.PageSession(), "Fetch.authRequired", challenge)
	first := authResponse(t, waitCalls(t, s, "Fetch.continueWithAuth", 1)[0])
	if first["response"] != "ProvideCredentials" {
		t.Fatalf("proxy challenge answered with %q, want ProvideCredentials", first["response"])
	}
	if first["username"] != canary {
		t.Fatalf("proxy challenge username = %q", first["username"])
	}

	// A broker that refuses the capability challenges the same request again.
	// Answering twice would loop against a proxy that will never take it, so
	// the second answer cancels.
	s.Emit(s.PageSession(), "Fetch.authRequired", challenge)
	repeat := waitCalls(t, s, "Fetch.continueWithAuth", 2)
	if got := authResponse(t, repeat[1])["response"]; got != "CancelAuth" {
		t.Fatalf("repeated proxy challenge answered with %q, want CancelAuth", got)
	}
	provided := 0
	for _, c := range repeat {
		if authResponse(t, c)["response"] == "ProvideCredentials" {
			provided++
		}
	}
	if provided != 1 {
		t.Fatalf("the credential was provided %d times for one request, want exactly 1", provided)
	}
}

func TestOriginChallengeIsCancelled(t *testing.T) {
	s := fakecdp.New()
	defer s.Close()
	proxyClient(t, s)

	s.Emit(s.PageSession(), "Fetch.authRequired", map[string]any{
		"requestId":     "REQ-SITE",
		"authChallenge": map[string]any{"source": "Server", "origin": "https://site.example", "scheme": "basic"},
	})
	answer := authResponse(t, waitCalls(t, s, "Fetch.continueWithAuth", 1)[0])
	if answer["response"] != "CancelAuth" {
		t.Fatalf("origin challenge answered with %q, want CancelAuth", answer["response"])
	}
	if answer["username"] != "" || answer["password"] != "" {
		t.Fatalf("origin challenge carried credentials: %v", answer)
	}
}

func TestPausedRequestIsContinuedUnchanged(t *testing.T) {
	s := fakecdp.New()
	defer s.Close()
	proxyClient(t, s)

	s.Emit(s.PageSession(), "Fetch.requestPaused", map[string]any{
		"requestId": "REQ-2",
		"request":   map[string]any{"url": "https://example.com/", "method": "GET"},
	})
	call := waitCalls(t, s, "Fetch.continueRequest", 1)[0]
	if got := string(call.Param("requestId")); got != `"REQ-2"` {
		t.Fatalf("continueRequest requestId = %s", got)
	}
	// Continuing "unchanged" means naming nothing else: a url, method, header
	// or body override would rewrite a request nobody asked to change.
	var params map[string]json.RawMessage
	if err := json.Unmarshal(call.Params, &params); err != nil {
		t.Fatalf("decode continueRequest params: %v", err)
	}
	if len(params) != 1 {
		t.Fatalf("continueRequest carried %v, want only requestId", params)
	}
}

func TestAttachedTargetGetsTheSameInterception(t *testing.T) {
	s := fakecdp.New()
	defer s.Close()
	proxyClient(t, s)
	waitCalls(t, s, "Fetch.enable", 1)

	const child = "SESSION-IFRAME"
	s.Emit(s.PageSession(), "Target.attachedToTarget", map[string]any{
		"sessionId":  child,
		"targetInfo": map[string]any{"targetId": "TARGET-IFRAME", "type": "iframe", "url": "https://example.com/f"},
	})
	deadline := time.Now().Add(10 * time.Second)
	armed := map[string]bool{}
	for time.Now().Before(deadline) && len(armed) < 3 {
		for _, c := range s.Calls() {
			if c.SessionID == child {
				armed[c.Method] = true
			}
		}
		if len(armed) < 3 {
			time.Sleep(10 * time.Millisecond)
		}
	}
	for _, want := range []string{"Target.setAutoAttach", "Fetch.enable", "Runtime.runIfWaitingForDebugger"} {
		if !armed[want] {
			t.Fatalf("attached target never received %s; it got %v", want, armed)
		}
	}
}

// TestCredentialNeverLeavesContinueWithAuth is the leak scan. The same run
// proves the instrument: the canary IS present in continueWithAuth, so a scan
// that found it nowhere would be a broken scan rather than a clean result.
func TestCredentialNeverLeavesContinueWithAuth(t *testing.T) {
	s := fakecdp.New()
	defer s.Close()
	c := proxyClient(t, s)

	s.Emit(s.PageSession(), "Fetch.authRequired", map[string]any{
		"requestId":     "REQ-3",
		"authChallenge": map[string]any{"source": "Proxy"},
	})
	waitCalls(t, s, "Fetch.continueWithAuth", 1)
	s.Emit(s.PageSession(), "Fetch.requestPaused", map[string]any{
		"requestId": "REQ-3", "request": map[string]any{"url": "https://example.com/"},
	})
	waitCalls(t, s, "Fetch.continueRequest", 1)

	ctx := context.Background()
	if _, err := c.Navigate(ctx, "https://example.com/"); err != nil {
		t.Fatalf("navigate: %v", err)
	}
	if _, err := c.Screenshot(ctx); err != nil {
		t.Fatalf("screenshot: %v", err)
	}
	value, err := c.Eval(ctx, "document.title")
	if err != nil {
		t.Fatalf("eval: %v", err)
	}
	if bytes.Contains(value, []byte(canary)) {
		t.Fatalf("computer.eval returned the credential: %s", value)
	}
	if strings.Contains(c.Version(), canary) || strings.Contains(c.Reason(), canary) {
		t.Fatal("the client's own reported state carries the credential")
	}
	for _, d := range c.Downloads() {
		if strings.Contains(d.URL+d.Filename+d.GUID+d.State, canary) {
			t.Fatalf("a download record carries the credential: %+v", d)
		}
	}

	found := 0
	for _, call := range s.Calls() {
		if !bytes.Contains(call.Params, []byte(canary)) {
			continue
		}
		found++
		if call.Method != "Fetch.continueWithAuth" {
			t.Fatalf("%s carried the credential: %s", call.Method, call.Params)
		}
	}
	if found != 1 {
		t.Fatalf("the credential appeared in %d CDP calls, want exactly 1 (the scan must be able to find it)", found)
	}
}
