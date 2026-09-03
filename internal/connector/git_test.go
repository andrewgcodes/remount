package connector

import (
	"context"
	"encoding/base64"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"

	"remount.dev/remount/internal/connector/gittest"
	"remount.dev/remount/internal/proto"
)

func gitRule(push bool, repos ...string) proto.EgressRule {
	return proto.EgressRule{
		ID: "code", Connector: proto.EgressConnectorGit, Protocol: proto.EgressProtocolHTTPS,
		Hosts: []string{"git.example"}, Repos: repos, Push: push, Methods: []string{"GET", "POST"},
	}
}

func gitRequest(t *testing.T, method, rawURL string, rule proto.EgressRule) ConnectorRequest {
	t.Helper()
	target, err := url.Parse(rawURL)
	if err != nil {
		t.Fatal(err)
	}
	return ConnectorRequest{
		Workspace: "ws_1", Tenant: "t", Principal: "p", Generation: 3, Rule: rule,
		Method: method, URL: target, Header: http.Header{},
	}
}

// The connector is a positive grammar: only the three smart-HTTP endpoints
// for a covered repository pass, and push needs its own permission.
func TestGitAuthorizeGrammar(t *testing.T) {
	g := NewGit(GitOptions{})
	rule := gitRule(false, "acme/*", "other/tool")
	cases := []struct {
		name   string
		method string
		url    string
		rule   proto.EgressRule
		code   string
		op     string
	}{
		{"info refs fetch", "GET", "https://git.example/acme/app.git/info/refs?service=git-upload-pack", rule, "allowed", GitOpFetch},
		{"info refs no .git", "GET", "https://git.example/acme/app/info/refs?service=git-upload-pack", rule, "allowed", GitOpFetch},
		{"upload pack", "POST", "https://git.example/acme/app.git/git-upload-pack", rule, "allowed", GitOpFetch},
		{"exact repo", "POST", "https://git.example/other/tool/git-upload-pack", rule, "allowed", GitOpFetch},
		{"dumb http", "GET", "https://git.example/acme/app.git/info/refs", rule, "target_denied", ""},
		{"extra query", "GET", "https://git.example/acme/app.git/info/refs?service=git-upload-pack&x=1", rule, "target_denied", ""},
		{"api", "GET", "https://git.example/api/v3/user", rule, "target_denied", ""},
		{"raw file", "GET", "https://git.example/acme/app/raw/main/secret.txt", rule, "target_denied", ""},
		{"objects", "GET", "https://git.example/acme/app.git/objects/info/packs", rule, "target_denied", ""},
		{"lfs", "POST", "https://git.example/acme/app.git/info/lfs/objects/batch", rule, "target_denied", ""},
		{"traversal", "GET", "https://git.example/acme/../admin/info/refs?service=git-upload-pack", rule, "target_denied", ""},
		{"upload pack via GET", "GET", "https://git.example/acme/app.git/git-upload-pack", rule, "target_denied", ""},
		{"info refs via POST", "POST", "https://git.example/acme/app.git/info/refs?service=git-upload-pack", rule, "target_denied", ""},
		{"other repo", "GET", "https://git.example/other/secret.git/info/refs?service=git-upload-pack", rule, "repo_denied", ""},
		{"other host", "GET", "https://evil.example/acme/app.git/info/refs?service=git-upload-pack", rule, "rule_mismatch", ""},
		{"userinfo", "GET", "https://tok@git.example/acme/app.git/info/refs?service=git-upload-pack", rule, "target_denied", ""},
		{"plain http", "GET", "http://git.example/acme/app.git/info/refs?service=git-upload-pack", rule, "target_denied", ""},
		{"delete", "DELETE", "https://git.example/acme/app.git/info/refs?service=git-upload-pack", rule, "method_denied", ""},
		{"push refused", "POST", "https://git.example/acme/app.git/git-receive-pack", rule, "push_denied", ""},
		{"push advertisement refused", "GET", "https://git.example/acme/app.git/info/refs?service=git-receive-pack", rule, "push_denied", ""},
		{"push allowed", "POST", "https://git.example/acme/app.git/git-receive-pack", gitRule(true, "acme/app"), "allowed", GitOpPush},
		{"wrong connector", "GET", "https://git.example/acme/app.git/info/refs?service=git-upload-pack", packageRule(0), "wrong_connector", ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			d, err := g.Authorize(context.Background(), gitRequest(t, tc.method, tc.url, tc.rule))
			if err != nil {
				t.Fatal(err)
			}
			if d.Code != tc.code {
				t.Fatalf("code=%s reason=%q want %s", d.Code, d.Reason, tc.code)
			}
			if d.Allowed != (tc.code == "allowed") || d.Operation != tc.op {
				t.Fatalf("allowed=%v op=%q want op %q", d.Allowed, d.Operation, tc.op)
			}
			if d.Allowed && d.Resource == "" {
				t.Fatal("allowed decision names no repository")
			}
		})
	}
	anonymous := gitRequest(t, "GET", "https://git.example/acme/app.git/info/refs?service=git-upload-pack", rule)
	anonymous.Generation = 0
	if d, _ := g.Authorize(context.Background(), anonymous); d.Allowed || d.Code != "identity_required" {
		t.Fatalf("anonymous request authorized: %+v", d)
	}
}

// Against a real git http-backend the connector completes an advertisement
// and forwards only allow-listed headers in both directions.
func TestGitExecuteAgainstSmartHTTP(t *testing.T) {
	svc := gittest.New(t, "ghs_real_token")
	svc.CreateRepo(t, "acme/app", map[string]string{"README": "hi\n"})
	seenHeaders := make(chan http.Header, 1)
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		seenHeaders <- r.Header.Clone()
		w.Header().Set("Set-Cookie", "session=leaky")
		w.Header().Set("X-Request-Id", "abc")
		svc.Handler().ServeHTTP(w, r)
	}))
	defer up.Close()
	// The connector insists on https:// targets; rewrite the dial so the
	// plaintext test server answers for git.example.
	transport := roundTripFunc(func(r *http.Request) (*http.Response, error) {
		r.URL.Scheme, r.URL.Host = "http", strings.TrimPrefix(up.URL, "http://")
		return http.DefaultTransport.RoundTrip(r)
	})
	g := NewGit(GitOptions{Transport: transport})
	req := gitRequest(t, "GET", "https://git.example/acme/app.git/info/refs?service=git-upload-pack", gitRule(false, "acme/app"))
	req.Header.Set("Authorization", "Basic "+basic("x-access-token:ghs_real_token"))
	req.Header.Set("Cookie", "never=forwarded")
	req.Header.Set("X-Forwarded-For", "10.0.0.1")
	req.Header.Set("Git-Protocol", "version=2")
	res, err := g.Execute(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	defer res.Body.Close()
	body, _ := io.ReadAll(res.Body)
	if res.StatusCode != http.StatusOK || !strings.Contains(string(body), "version 2") && !strings.Contains(string(body), "# service=git-upload-pack") {
		t.Fatalf("status=%d body=%q", res.StatusCode, body)
	}
	if res.Provenance.Operation != GitOpFetch || res.Provenance.Source != "acme/app" {
		t.Fatalf("provenance %+v", res.Provenance)
	}
	if res.Header.Get("Set-Cookie") != "" || res.Header.Get("X-Request-Id") != "" {
		t.Fatalf("hosting-service headers leaked to the workspace: %v", res.Header)
	}
	hdr := <-seenHeaders
	if hdr.Get("Cookie") != "" || hdr.Get("X-Forwarded-For") != "" {
		t.Fatalf("workspace headers forwarded upstream: %v", hdr)
	}
	if hdr.Get("Git-Protocol") != "version=2" || hdr.Get("Authorization") == "" {
		t.Fatalf("needed headers dropped: %v", hdr)
	}
	// Without the token the hosting service says 401 and the connector says
	// so verbatim: the WWW-Authenticate challenge is on the allow list.
	req.Header.Del("Authorization")
	res, err = g.Execute(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	res.Body.Close()
	<-seenHeaders
	if res.StatusCode != http.StatusUnauthorized || res.Header.Get("WWW-Authenticate") == "" {
		t.Fatalf("status=%d header=%v", res.StatusCode, res.Header)
	}
}

// A redirect is refused rather than followed: following one would send the
// substituted credential wherever the hosting service pointed.
func TestGitExecuteRefusesRedirectsAndEnforcesBudgets(t *testing.T) {
	redirecting := roundTripFunc(func(r *http.Request) (*http.Response, error) {
		return &http.Response{StatusCode: http.StatusFound, Header: http.Header{"Location": {"https://evil.example/"}}, Body: http.NoBody}, nil
	})
	g := NewGit(GitOptions{Transport: redirecting})
	req := gitRequest(t, "GET", "https://git.example/acme/app.git/info/refs?service=git-upload-pack", gitRule(false, "acme/app"))
	if _, err := g.Execute(context.Background(), req); err == nil || !strings.Contains(err.Error(), "redirect") {
		t.Fatalf("redirect followed or misreported: %v", err)
	}

	var upstreamBody int
	counting := roundTripFunc(func(r *http.Request) (*http.Response, error) {
		if r.Body != nil {
			b, err := io.ReadAll(r.Body)
			upstreamBody = len(b)
			if err != nil {
				return nil, err
			}
		}
		return &http.Response{StatusCode: http.StatusOK, Header: http.Header{}, Body: io.NopCloser(strings.NewReader(strings.Repeat("x", 100))), ContentLength: 100}, nil
	})
	g = NewGit(GitOptions{Transport: counting})
	rule := gitRule(false, "acme/app")
	rule.MaxRequestBytes, rule.MaxResponseBytes = 10, 50
	post := gitRequest(t, "POST", "https://git.example/acme/app.git/git-upload-pack", rule)
	post.Body, post.ContentLength = strings.NewReader(strings.Repeat("y", 11)), 11
	if _, err := g.Execute(context.Background(), post); err == nil || !strings.Contains(err.Error(), "request body exceeds") {
		t.Fatalf("declared oversize body accepted: %v", err)
	}
	post.Body, post.ContentLength = strings.NewReader(strings.Repeat("y", 11)), 0 // chunked, lies by omission
	if _, err := g.Execute(context.Background(), post); err == nil || !strings.Contains(err.Error(), "request body exceeds") {
		t.Fatalf("streamed oversize body accepted: %v (upstream saw %d bytes)", err, upstreamBody)
	}
	if upstreamBody > 10 {
		t.Fatalf("upstream received %d bytes past the 10 byte budget", upstreamBody)
	}
	get := gitRequest(t, "GET", "https://git.example/acme/app.git/info/refs?service=git-upload-pack", rule)
	if _, err := g.Execute(context.Background(), get); err == nil || !strings.Contains(err.Error(), "response body exceeds") {
		t.Fatalf("oversize declared response accepted: %v", err)
	}
	streaming := roundTripFunc(func(r *http.Request) (*http.Response, error) {
		return &http.Response{StatusCode: http.StatusOK, Header: http.Header{}, Body: io.NopCloser(strings.NewReader(strings.Repeat("x", 100))), ContentLength: -1}, nil
	})
	res, err := NewGit(GitOptions{Transport: streaming}).Execute(context.Background(), get)
	if err != nil {
		t.Fatal(err)
	}
	defer res.Body.Close()
	n, err := io.Copy(io.Discard, res.Body)
	if err == nil || n > 50 {
		t.Fatalf("streamed %d bytes err=%v; want the 50 byte budget enforced", n, err)
	}
}

func basic(userinfo string) string {
	return base64.StdEncoding.EncodeToString([]byte(userinfo))
}
