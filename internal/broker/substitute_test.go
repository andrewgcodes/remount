package broker

import (
	"crypto/x509"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"testing"
	"time"

	"remount.dev/remount/internal/proto"
	"remount.dev/remount/internal/redact"
)

// echoUpstream records what actually reached the far side. It never echoes a
// request back, so a test that claims the broker substituted is reading the
// upstream's own record rather than a response the broker could have written.
type echoUpstream struct {
	srv  *httptest.Server
	host string
	mu   sync.Mutex
	got  []echoRequest
}

type echoRequest struct {
	Method string
	Path   string
	Query  string
	Header http.Header
	Body   string
}

func newEchoUpstream(t *testing.T) *echoUpstream {
	t.Helper()
	u := &echoUpstream{}
	u.srv = httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		u.mu.Lock()
		u.got = append(u.got, echoRequest{
			Method: r.Method, Path: r.URL.Path, Query: r.URL.RawQuery,
			Header: r.Header.Clone(), Body: string(body),
		})
		u.mu.Unlock()
		if location := r.URL.Query().Get("redirect_to"); location != "" {
			w.Header().Set("Location", location)
			w.WriteHeader(http.StatusFound)
			return
		}
		w.WriteHeader(http.StatusOK)
		_, _ = io.WriteString(w, "ok")
	}))
	t.Cleanup(u.srv.Close)
	parsed, _ := url.Parse(u.srv.URL)
	u.host = parsed.Host
	return u
}

func (u *echoUpstream) pool() *x509.CertPool {
	p := x509.NewCertPool()
	p.AddCert(u.srv.Certificate())
	return p
}

func (u *echoUpstream) requests() []echoRequest {
	u.mu.Lock()
	defer u.mu.Unlock()
	return append([]echoRequest(nil), u.got...)
}

func startWith(t *testing.T, adjust func(*Options)) *Broker {
	t.Helper()
	opts := Options{
		WS: "ws_sub", Principal: "a_test",
		AllowPrivate: []string{"127.0.0.1", "localhost"},
	}
	adjust(&opts)
	b := New(opts)
	if _, err := b.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { b.Close() })
	return b
}

// send issues one request through the broker and returns the response plus
// its body. It never follows redirects: a redirect is a broker decision the
// caller wants to see.
func send(t *testing.T, method, rawURL, contentType, body string, header map[string]string) (*http.Response, string) {
	t.Helper()
	var reader io.Reader
	if body != "" {
		reader = strings.NewReader(body)
	}
	req, err := http.NewRequest(method, rawURL, reader)
	if err != nil {
		t.Fatal(err)
	}
	if contentType != "" {
		req.Header.Set("Content-Type", contentType)
	}
	for k, v := range header {
		req.Header.Set(k, v)
	}
	client := &http.Client{
		Timeout:       15 * time.Second,
		CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
	}
	resp, err := client.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	return resp, string(raw)
}

// denialOf decodes the typed refusal a broker wrote, failing the test when
// the response is not one.
func denialOf(t *testing.T, resp *http.Response, body string) denialError {
	t.Helper()
	var decoded denialBody
	if err := json.Unmarshal([]byte(body), &decoded); err != nil {
		t.Fatalf("refusal body is not the typed shape: %v (%q)", err, body)
	}
	if header := resp.Header.Get(ReasonHeader); header != decoded.Error.Reason {
		t.Fatalf("%s=%q disagrees with body reason %q", ReasonHeader, header, decoded.Error.Reason)
	}
	return decoded.Error
}

func TestQuerySubstitutionRespectsDestinationScope(t *testing.T) {
	up := newEchoUpstream(t)
	rec := &recorder{}
	lease := proto.BindingLease{
		ID: "b_q", Secret: "sk-live-query-substitution-secret", Destinations: []string{up.host},
		Substitution: &proto.BindingSubstitution{Location: proto.SubstitutionQuery, Name: "api_key"},
	}
	foreign := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		t.Error("a blocked leak reached the foreign server")
	}))
	defer foreign.Close()
	foreignHost := strings.TrimPrefix(foreign.URL, "http://")
	b := startWith(t, func(o *Options) {
		o.Leases = []proto.BindingLease{lease}
		o.Allow = []string{foreignHost}
		o.Audit = rec.add
		o.RootCAs = up.pool()
	})

	resp, body := send(t, http.MethodGet, DestURL(b.BaseURL(), up.host)+"/v1/models?api_key=ref:b_q&page=2", "", "", nil)
	if resp.StatusCode != http.StatusOK || body != "ok" {
		t.Fatalf("bound request = %d %q", resp.StatusCode, body)
	}
	got := up.requests()
	if len(got) != 1 {
		t.Fatalf("upstream saw %d requests", len(got))
	}
	values, err := url.ParseQuery(got[0].Query)
	if err != nil {
		t.Fatal(err)
	}
	if values.Get("api_key") != lease.Secret {
		t.Fatalf("query parameter was not substituted: %q", got[0].Query)
	}
	if values.Get("page") != "2" || strings.Contains(got[0].Query, "ref:b_q") {
		t.Fatalf("query was not preserved around the substitution: %q", got[0].Query)
	}
	if used, ok := rec.lastDecision(DecisionSubstituted); !ok || used.Binding != "b_q" || used.Status != http.StatusOK {
		t.Fatalf("cred.used audit = %+v ok=%v", used, ok)
	}

	// The same placeholder in the same parameter, aimed at a host the binding
	// does not cover, is the exfiltration case and never reaches the wire.
	resp, body = send(t, http.MethodGet, b.BaseURL()+"/http/"+foreignHost+"/exfil?api_key=ref:b_q", "", "", nil)
	if resp.StatusCode != http.StatusForbidden {
		t.Fatalf("foreign query = %d %q", resp.StatusCode, body)
	}
	refusal := denialOf(t, resp, body)
	if refusal.Reason != proto.ReasonEgressDenied || refusal.Binding != "b_q" {
		t.Fatalf("refusal = %+v", refusal)
	}
	if blocked, ok := rec.lastDecision(DecisionLeakBlocked); !ok || blocked.Binding != "b_q" {
		t.Fatalf("leak_blocked audit = %+v ok=%v", blocked, ok)
	}
	if len(up.requests()) != 1 {
		t.Fatal("the blocked request reached an upstream")
	}
}

func TestFormBodySubstitutionRespectsDestinationScope(t *testing.T) {
	up := newEchoUpstream(t)
	rec := &recorder{}
	lease := proto.BindingLease{
		ID: "b_f", Secret: "sk-live-form-substitution-secret", Destinations: []string{up.host},
		Substitution: &proto.BindingSubstitution{Location: proto.SubstitutionBodyForm, Name: "client_secret"},
	}
	foreign := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		t.Error("a blocked leak reached the foreign server")
	}))
	defer foreign.Close()
	foreignHost := strings.TrimPrefix(foreign.URL, "http://")
	b := startWith(t, func(o *Options) {
		o.Leases = []proto.BindingLease{lease}
		o.Allow = []string{foreignHost}
		o.Audit = rec.add
		o.RootCAs = up.pool()
	})

	form := url.Values{"grant_type": {"client_credentials"}, "client_secret": {"ref:b_f"}}.Encode()
	resp, body := send(t, http.MethodPost, DestURL(b.BaseURL(), up.host)+"/oauth/token",
		"application/x-www-form-urlencoded", form, nil)
	if resp.StatusCode != http.StatusOK || body != "ok" {
		t.Fatalf("bound request = %d %q", resp.StatusCode, body)
	}
	got := up.requests()
	if len(got) != 1 {
		t.Fatalf("upstream saw %d requests", len(got))
	}
	values, err := url.ParseQuery(got[0].Body)
	if err != nil {
		t.Fatal(err)
	}
	if values.Get("client_secret") != lease.Secret || values.Get("grant_type") != "client_credentials" {
		t.Fatalf("form body = %q", got[0].Body)
	}
	if strings.Contains(got[0].Body, "ref:b_f") {
		t.Fatalf("the placeholder survived into the upstream body: %q", got[0].Body)
	}
	if got[0].Header.Get("Content-Length") == "" && got[0].Body != "" {
		t.Fatalf("substituted body was sent without a length: %+v", got[0].Header)
	}

	resp, body = send(t, http.MethodPost, b.BaseURL()+"/http/"+foreignHost+"/oauth/token",
		"application/x-www-form-urlencoded", form, nil)
	if resp.StatusCode != http.StatusForbidden {
		t.Fatalf("foreign form = %d %q", resp.StatusCode, body)
	}
	if refusal := denialOf(t, resp, body); refusal.Binding != "b_f" || refusal.Reason != proto.ReasonEgressDenied {
		t.Fatalf("refusal = %+v", refusal)
	}
	if blocked, ok := rec.lastDecision(DecisionLeakBlocked); !ok || blocked.Binding != "b_f" {
		t.Fatalf("leak_blocked audit = %+v ok=%v", blocked, ok)
	}
	if len(up.requests()) != 1 {
		t.Fatal("the blocked request reached an upstream")
	}
}

func TestJSONBodySubstitutionRespectsDestinationScope(t *testing.T) {
	up := newEchoUpstream(t)
	rec := &recorder{}
	lease := proto.BindingLease{
		ID: "b_j", Secret: "sk-live-json-substitution-secret", Destinations: []string{up.host},
		Substitution: &proto.BindingSubstitution{Location: proto.SubstitutionBodyJSON, JSONPointer: "/auth/token"},
	}
	foreign := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		t.Error("a blocked leak reached the foreign server")
	}))
	defer foreign.Close()
	foreignHost := strings.TrimPrefix(foreign.URL, "http://")
	b := startWith(t, func(o *Options) {
		o.Leases = []proto.BindingLease{lease}
		o.Allow = []string{foreignHost}
		o.Audit = rec.add
		o.RootCAs = up.pool()
	})

	document := `{"auth":{"token":"ref:b_j"},"max_tokens":1024,"prompt":"a < b & c"}`
	resp, body := send(t, http.MethodPost, DestURL(b.BaseURL(), up.host)+"/v1/messages", "application/json", document, nil)
	if resp.StatusCode != http.StatusOK || body != "ok" {
		t.Fatalf("bound request = %d %q", resp.StatusCode, body)
	}
	got := up.requests()
	if len(got) != 1 {
		t.Fatalf("upstream saw %d requests", len(got))
	}
	var sent struct {
		Auth      struct{ Token string } `json:"auth"`
		MaxTokens json.Number            `json:"max_tokens"`
		Prompt    string                 `json:"prompt"`
	}
	if err := json.Unmarshal([]byte(got[0].Body), &sent); err != nil {
		t.Fatalf("upstream body is not JSON: %v (%q)", err, got[0].Body)
	}
	if sent.Auth.Token != lease.Secret {
		t.Fatalf("pointer was not substituted: %q", got[0].Body)
	}
	// The rest of the document survives: an exact integer and a string the
	// encoder must not HTML-escape.
	if sent.MaxTokens.String() != "1024" || sent.Prompt != "a < b & c" {
		t.Fatalf("substitution rewrote unrelated fields: %q", got[0].Body)
	}
	if strings.Contains(got[0].Body, "ref:b_j") {
		t.Fatalf("the placeholder survived into the upstream body: %q", got[0].Body)
	}

	resp, body = send(t, http.MethodPost, b.BaseURL()+"/http/"+foreignHost+"/v1/messages", "application/json", document, nil)
	if resp.StatusCode != http.StatusForbidden {
		t.Fatalf("foreign json = %d %q", resp.StatusCode, body)
	}
	if refusal := denialOf(t, resp, body); refusal.Binding != "b_j" || refusal.Reason != proto.ReasonEgressDenied {
		t.Fatalf("refusal = %+v", refusal)
	}
	if blocked, ok := rec.lastDecision(DecisionLeakBlocked); !ok || blocked.Binding != "b_j" {
		t.Fatalf("leak_blocked audit = %+v ok=%v", blocked, ok)
	}
	if len(up.requests()) != 1 {
		t.Fatal("the blocked request reached an upstream")
	}
}

// A placeholder in a request body is scanned for scope exactly like one in a
// header: it is the same exfiltration attempt with a different envelope.
func TestBodyPlaceholderToForeignHostIsBlocked(t *testing.T) {
	up := newEchoUpstream(t)
	rec := &recorder{}
	// The binding covers only api.github.com; the foreign host is on the node
	// allow list, so the destination itself is permitted and only the
	// credential's scope refuses the request.
	lease := proto.BindingLease{
		ID: "b_scoped", Secret: "sk-live-scoped-binding-secret-value", Destinations: []string{"api.github.com"},
		Substitution: &proto.BindingSubstitution{Location: proto.SubstitutionBodyJSON, JSONPointer: "/token"},
	}
	b := startWith(t, func(o *Options) {
		o.Leases = []proto.BindingLease{lease}
		o.Allow = []string{up.host}
		o.Audit = rec.add
		o.RootCAs = up.pool()
	})

	resp, body := send(t, http.MethodPost, DestURL(b.BaseURL(), up.host)+"/exfil", "application/json",
		`{"token":"ref:b_scoped"}`, nil)
	if resp.StatusCode != http.StatusForbidden {
		t.Fatalf("body leak = %d %q", resp.StatusCode, body)
	}
	refusal := denialOf(t, resp, body)
	if refusal.Binding != "b_scoped" || refusal.Reason != proto.ReasonEgressDenied || refusal.Code != proto.CodeDenied {
		t.Fatalf("refusal = %+v", refusal)
	}
	if strings.Contains(body, lease.Secret) {
		t.Fatal("the refusal body carried the secret")
	}
	blocked, ok := rec.lastDecision(DecisionLeakBlocked)
	if !ok || blocked.Binding != "b_scoped" || blocked.Host != up.host {
		t.Fatalf("leak_blocked audit = %+v ok=%v", blocked, ok)
	}
	if len(up.requests()) != 0 {
		t.Fatal("the blocked request reached the upstream")
	}
	// Without the credential the allow-listed host still works, so the
	// refusal above was the binding's scope and not the destination's.
	resp, _ = send(t, http.MethodPost, DestURL(b.BaseURL(), up.host)+"/ok", "application/json", `{"token":"none"}`, nil)
	if resp.StatusCode != http.StatusOK || len(up.requests()) != 1 {
		t.Fatalf("uncredentialed request = %d, upstream saw %d", resp.StatusCode, len(up.requests()))
	}
}

// Every ambiguity is a refusal. Guessing which occurrence of a placeholder
// the workspace meant is how a credential lands somewhere nobody authorized,
// so the broker never guesses.
func TestBodySubstitutionFailsClosedOnAmbiguity(t *testing.T) {
	up := newEchoUpstream(t)
	jsonLease := proto.BindingLease{
		ID: "b_json", Secret: "sk-live-ambiguity-json-secret-value", Destinations: []string{up.host},
		Substitution: &proto.BindingSubstitution{Location: proto.SubstitutionBodyJSON, JSONPointer: "/auth/token"},
	}
	formLease := proto.BindingLease{
		ID: "b_form", Secret: "sk-live-ambiguity-form-secret-value", Destinations: []string{up.host},
		Substitution: &proto.BindingSubstitution{Location: proto.SubstitutionBodyForm, Name: "client_secret"},
	}
	queryLease := proto.BindingLease{
		ID: "b_query", Secret: "sk-live-ambiguity-query-secret-val", Destinations: []string{up.host},
		Substitution: &proto.BindingSubstitution{Location: proto.SubstitutionQuery, Name: "api_key"},
	}

	cases := []struct {
		name        string
		path        string
		contentType string
		body        string
		header      map[string]string
		status      int
		reason      string
		want        string
	}{
		{
			name: "pointer names a non-string", path: "/v1", contentType: "application/json",
			body:   `{"auth":{"token":42},"note":"ref:b_json"}`,
			status: http.StatusBadRequest, reason: proto.ReasonEgressDenied, want: "does not name a string",
		},
		{
			name: "pointer names no member", path: "/v1", contentType: "application/json",
			body:   `{"note":"ref:b_json"}`,
			status: http.StatusBadRequest, reason: proto.ReasonEgressDenied, want: "names no member",
		},
		{
			name: "content type is not JSON", path: "/v1", contentType: "text/plain",
			body:   `{"auth":{"token":"ref:b_json"}}`,
			status: http.StatusBadRequest, reason: proto.ReasonEgressDenied, want: "needs application/json",
		},
		{
			name: "malformed JSON", path: "/v1", contentType: "application/json",
			body:   `{"auth":{"token":"ref:b_json"`,
			status: http.StatusBadRequest, reason: proto.ReasonEgressDenied, want: "JSON body is malformed",
		},
		{
			name: "form field occurs twice", path: "/v1", contentType: "application/x-www-form-urlencoded",
			body:   "client_secret=ref%3Ab_form&client_secret=ref%3Ab_form",
			status: http.StatusBadRequest, reason: proto.ReasonEgressDenied, want: "occurs 2 times",
		},
		{
			name: "form location with a JSON body", path: "/v1", contentType: "application/json",
			body:   `{"client_secret":"ref:b_form"}`,
			status: http.StatusBadRequest, reason: proto.ReasonEgressDenied, want: "application/x-www-form-urlencoded",
		},
		{
			name: "placeholder also in a header", path: "/v1", contentType: "application/json",
			body: `{"auth":{"token":"ref:b_json"}}`, header: map[string]string{"Authorization": "Bearer ref:b_json"},
			status: http.StatusBadRequest, reason: proto.ReasonEgressDenied, want: "only in its declared",
		},
		{
			name: "query parameter occurs twice", path: "/v1?api_key=ref:b_query&api_key=ref:b_query",
			contentType: "application/json", body: `{}`,
			status: http.StatusBadRequest, reason: proto.ReasonEgressDenied, want: "occurs 2 times",
		},
		{
			name: "query placeholder outside its parameter", path: "/v1?other=ref:b_query",
			contentType: "application/json", body: `{}`,
			status: http.StatusBadRequest, reason: proto.ReasonEgressDenied, want: "occurs 0 times",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			rec := &recorder{}
			b := startWith(t, func(o *Options) {
				o.Leases = []proto.BindingLease{jsonLease, formLease, queryLease}
				o.Audit = rec.add
				o.RootCAs = up.pool()
			})
			before := len(up.requests())
			resp, body := send(t, http.MethodPost, DestURL(b.BaseURL(), up.host)+tc.path, tc.contentType, tc.body, tc.header)
			if resp.StatusCode != tc.status {
				t.Fatalf("status = %d %q, want %d", resp.StatusCode, body, tc.status)
			}
			refusal := denialOf(t, resp, body)
			if refusal.Reason != tc.reason {
				t.Fatalf("reason = %q, want %q", refusal.Reason, tc.reason)
			}
			if !strings.Contains(refusal.Message, tc.want) {
				t.Fatalf("message = %q, want it to name %q", refusal.Message, tc.want)
			}
			for _, lease := range []proto.BindingLease{jsonLease, formLease, queryLease} {
				if strings.Contains(body, lease.Secret) {
					t.Fatalf("the refusal carried %s's secret", lease.ID)
				}
			}
			if rec.count(DecisionSubstituted) != 0 {
				t.Fatal("a refused request still recorded a credential use")
			}
			if len(up.requests()) != before {
				t.Fatal("a refused request reached the upstream")
			}
		})
	}

	// The bound is real: a body larger than the substitution ceiling is
	// refused as a quota, not truncated and forwarded.
	t.Run("oversize body", func(t *testing.T) {
		rec := &recorder{}
		b := startWith(t, func(o *Options) {
			o.Leases = []proto.BindingLease{jsonLease}
			o.Audit = rec.add
			o.RootCAs = up.pool()
			o.MaxSubstitutionBodyBytes = 64
		})
		before := len(up.requests())
		document := `{"auth":{"token":"ref:b_json"},"pad":"` + strings.Repeat("x", 512) + `"}`
		resp, body := send(t, http.MethodPost, DestURL(b.BaseURL(), up.host)+"/v1", "application/json", document, nil)
		if resp.StatusCode != http.StatusRequestEntityTooLarge {
			t.Fatalf("status = %d %q", resp.StatusCode, body)
		}
		if refusal := denialOf(t, resp, body); refusal.Reason != proto.ReasonQuotaExceeded || refusal.Code != proto.CodeResourceExhausted {
			t.Fatalf("refusal = %+v", refusal)
		}
		if len(up.requests()) != before {
			t.Fatal("an oversize request reached the upstream")
		}
	})
}

// Every refusal a workspace can provoke carries the same machine-readable
// classification, and none of them carries a credential-shaped string.
func TestBrokerDenialsAreTypedAndCarryNoCredential(t *testing.T) {
	up := newEchoUpstream(t)
	rec := &recorder{}
	// A synthetic canary, never a real credential: it is shaped like an API
	// key so the redaction scan has something to find.
	const canary = "sk-remount-canary-do-not-use-0000"
	lease := proto.BindingLease{ID: "b_typed", Secret: canary, Destinations: []string{"api.github.com"}}
	expired := proto.BindingLease{
		ID: "b_stale", Secret: canary, Destinations: []string{up.host},
		ExpiresAt: time.Now().Add(-time.Minute).UnixMilli(),
	}
	b := startWith(t, func(o *Options) {
		o.Leases = []proto.BindingLease{lease, expired}
		o.Allow = []string{up.host}
		o.Audit = rec.add
		o.RootCAs = up.pool()
		o.Network = proto.NetworkPolicy{Default: proto.NetworkDefaultDeny, Rules: []proto.EgressRule{
			{ID: "read", Protocol: proto.EgressProtocolHTTPS, Hosts: []string{up.host},
				Methods: []string{http.MethodGet}, PathPrefixes: []string{"/ok"}, MaxRequests: 1},
		}}
	})

	cases := []struct {
		name   string
		method string
		url    string
		header map[string]string
		status int
		code   string
		reason string
	}{
		{
			name: "unmatched rule", method: http.MethodGet, url: DestURL(b.BaseURL(), up.host) + "/denied",
			status: http.StatusForbidden, code: proto.CodeDenied, reason: proto.ReasonEgressDenied,
		},
		{
			name: "placeholder aimed at a foreign host", method: http.MethodGet,
			url: DestURL(b.BaseURL(), up.host) + "/ok", header: map[string]string{"Authorization": "Bearer ref:b_typed"},
			status: http.StatusForbidden, code: proto.CodeDenied, reason: proto.ReasonEgressDenied,
		},
		{
			name: "expired lease", method: http.MethodGet,
			url: DestURL(b.BaseURL(), up.host) + "/ok", header: map[string]string{"Authorization": "Bearer ref:b_stale"},
			status: http.StatusForbidden, code: proto.CodeUnauthorized, reason: proto.ReasonGrantExpired,
		},
		{
			name: "unknown surface", method: http.MethodGet, url: b.BaseURL() + "/nonsense",
			status: http.StatusNotFound, code: proto.CodeNotFound, reason: proto.ReasonEgressDenied,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			resp, body := send(t, tc.method, tc.url, "", "", tc.header)
			if resp.StatusCode != tc.status {
				t.Fatalf("status = %d %q, want %d", resp.StatusCode, body, tc.status)
			}
			refusal := denialOf(t, resp, body)
			if refusal.Code != tc.code || refusal.Reason != tc.reason {
				t.Fatalf("refusal = %+v, want code %q reason %q", refusal, tc.code, tc.reason)
			}
			if strings.Contains(body, canary) {
				t.Fatalf("refusal body carried the canary: %q", body)
			}
			for name, values := range resp.Header {
				for _, value := range values {
					if strings.Contains(value, canary) {
						t.Fatalf("response header %s carried the canary", name)
					}
				}
			}
		})
	}

	// The rule's own request budget is a quota, not a policy denial.
	if resp, body := send(t, http.MethodGet, DestURL(b.BaseURL(), up.host)+"/ok", "", "", nil); resp.StatusCode != http.StatusOK {
		t.Fatalf("first allowed request = %d %q", resp.StatusCode, body)
	}
	resp, body := send(t, http.MethodGet, DestURL(b.BaseURL(), up.host)+"/ok", "", "", nil)
	if resp.StatusCode != http.StatusTooManyRequests {
		t.Fatalf("exhausted rule = %d %q", resp.StatusCode, body)
	}
	if refusal := denialOf(t, resp, body); refusal.Code != proto.CodeResourceExhausted || refusal.Reason != proto.ReasonQuotaExceeded {
		t.Fatalf("refusal = %+v", refusal)
	}
	if rec.count(DecisionLimitExceeded) == 0 {
		t.Fatal("the exhausted rule was not audited")
	}
}

// scanForCanary is the instrument the redaction test relies on. A scan that
// finds nothing and a scan that cannot work look identical, so the test below
// proves this one fires before it trusts a clean result.
func scanForCanary(canary string, subjects ...string) []string {
	var hits []string
	for _, subject := range subjects {
		if strings.Contains(subject, canary) {
			hits = append(hits, subject)
		}
	}
	return hits
}

func TestErrorMessagesRedactCredentialShapedStrings(t *testing.T) {
	const canary = "sk-remount-canary-do-not-use-0000"

	// 1. Prove the instrument. A scan that cannot find a planted canary
	// cannot report its absence either.
	if hits := scanForCanary(canary, "prefix "+canary+" suffix"); len(hits) != 1 {
		t.Fatal("the canary scan cannot find a planted canary; every clean result below would be meaningless")
	}
	if scrubbed := redact.String("value=" + canary); strings.Contains(scrubbed, canary) {
		t.Fatalf("the redactor does not recognize the canary shape: %q", scrubbed)
	}
	if redact.String("value="+canary) == "value="+canary {
		t.Fatal("redact.String returned its input unchanged for a planted canary")
	}

	// 2. Drive a real path whose error text quotes an upstream value. The
	// upstream is asked to redirect to a location that cannot be parsed and
	// that embeds the canary, so the parse error carries it into both the
	// audit reason and the text the workspace receives.
	up := newEchoUpstream(t)
	rec := &recorder{}
	b := startWith(t, func(o *Options) {
		o.Leases = []proto.BindingLease{{ID: "b_canary", Secret: canary, Destinations: []string{up.host}}}
		o.Allow = []string{up.host}
		o.Audit = rec.add
		o.RootCAs = up.pool()
	})
	poisoned := url.QueryEscape("://" + canary + "/next")
	resp, body := send(t, http.MethodGet, DestURL(b.BaseURL(), up.host)+"/redirect?redirect_to="+poisoned, "", "",
		map[string]string{"Authorization": "Bearer ref:b_canary"})
	if resp.StatusCode != http.StatusBadGateway {
		t.Fatalf("poisoned redirect = %d %q", resp.StatusCode, body)
	}
	// The unredacted text names the canary; without scrubbing this body and
	// this audit would both carry it.
	if !strings.Contains(body, redact.Mark) {
		t.Fatalf("the refusal was never scrubbed: %q", body)
	}

	// 3. Nothing that leaves the broker carries the canary: not the response
	// body, not a response header, not any audit field.
	subjects := []string{body}
	for name, values := range resp.Header {
		for _, value := range values {
			subjects = append(subjects, name+": "+value)
		}
	}
	rec.mu.Lock()
	audits := append([]Audit(nil), rec.ev...)
	rec.mu.Unlock()
	if len(audits) == 0 {
		t.Fatal("no audit was recorded, so the audit scan proves nothing")
	}
	for _, audit := range audits {
		subjects = append(subjects, audit.Reason, audit.Error, audit.Host, audit.Path, audit.Binding, audit.Rule)
	}
	if hits := scanForCanary(canary, subjects...); len(hits) != 0 {
		t.Fatalf("credential-shaped canary survived into %d caller-facing strings: %q", len(hits), hits)
	}
}
