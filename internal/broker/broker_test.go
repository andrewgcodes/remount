package broker

import (
	"crypto/tls"
	"crypto/x509"
	"encoding/base64"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"testing"
	"time"

	"remount.dev/remount/internal/proto"
)

type upstream struct {
	srv   *httptest.Server
	host  string
	mu    sync.Mutex
	seen  []http.Header
	paths []string
}

func newUpstream(t *testing.T) *upstream {
	u := &upstream{}
	u.srv = httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		u.mu.Lock()
		u.seen = append(u.seen, r.Header.Clone())
		u.paths = append(u.paths, r.URL.RequestURI())
		u.mu.Unlock()
		if r.URL.Path == "/stream" {
			w.Header().Set("Content-Type", "text/event-stream")
			f := w.(http.Flusher)
			for i := 0; i < 3; i++ {
				io.WriteString(w, "data: x\n\n")
				f.Flush()
				time.Sleep(5 * time.Millisecond)
			}
			return
		}
		w.Header().Set("X-Upstream", "yes")
		w.WriteHeader(200)
		io.WriteString(w, "auth="+r.Header.Get("Authorization")+";key="+r.Header.Get("X-Api-Key"))
	}))
	t.Cleanup(u.srv.Close)
	pu, _ := url.Parse(u.srv.URL)
	u.host = pu.Host
	return u
}

func (u *upstream) pool() *x509.CertPool {
	p := x509.NewCertPool()
	p.AddCert(u.srv.Certificate())
	return p
}

type recorder struct {
	mu sync.Mutex
	ev []Audit
}

func (r *recorder) add(a Audit) { r.mu.Lock(); r.ev = append(r.ev, a); r.mu.Unlock() }
func (r *recorder) last() Audit {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.ev[len(r.ev)-1]
}
func (r *recorder) count(dec string) int {
	r.mu.Lock()
	defer r.mu.Unlock()
	n := 0
	for _, e := range r.ev {
		if e.Decision == dec {
			n++
		}
	}
	return n
}

func start(t *testing.T, up *upstream, leases []proto.BindingLease, allow []string, rec *recorder) *Broker {
	t.Helper()
	b := New(Options{
		WS: "ws_t", Principal: "a_test", Leases: leases, Allow: allow,
		AllowPrivate: []string{"127.0.0.1", "localhost"}, Audit: rec.add, RootCAs: up.pool(),
	})
	if _, err := b.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { b.Close() })
	return b
}

func get(t *testing.T, rawURL string, hdr map[string]string) (*http.Response, string) {
	t.Helper()
	req, _ := http.NewRequest("GET", rawURL, nil)
	for k, v := range hdr {
		req.Header.Set(k, v)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	return resp, string(body)
}

func TestSubstitutesBearerForBoundHost(t *testing.T) {
	up := newUpstream(t)
	rec := &recorder{}
	lease := proto.BindingLease{ID: "b_gh", Secret: "ghp_REAL", Destinations: []string{up.host}, ExpiresAt: time.Now().Add(time.Hour).UnixMilli()}
	b := start(t, up, []proto.BindingLease{lease}, nil, rec)
	resp, body := get(t, DestURL(b.BaseURL(), up.host)+"/repos/x?y=1", map[string]string{"Authorization": "Bearer ref:b_gh"})
	if resp.StatusCode != 200 || body != "auth=Bearer ghp_REAL;key=" || resp.Header.Get("X-Upstream") != "yes" {
		t.Fatalf("%d %q", resp.StatusCode, body)
	}
	if up.paths[0] != "/repos/x?y=1" {
		t.Fatal(up.paths)
	}
	a := rec.last()
	if a.Decision != DecisionSubstituted || a.Binding != "b_gh" || a.Status != 200 || a.WS != "ws_t" {
		t.Fatalf("%+v", a)
	}
	// Placeholder inside a Basic credential (git over HTTP).
	basic := "Basic " + base64.StdEncoding.EncodeToString([]byte("x-access-token:ref:b_gh"))
	_, body = get(t, DestURL(b.BaseURL(), up.host)+"/git", map[string]string{"Authorization": basic})
	dec, _ := base64.StdEncoding.DecodeString(strings.TrimPrefix(strings.TrimPrefix(body, "auth=Basic "), " "))
	if !strings.HasPrefix(string(dec), "x-access-token:ghp_REAL") {
		t.Fatalf("basic not rewritten: %q", body)
	}
	// Shape-preserving placeholder.
	lease2 := proto.BindingLease{ID: "b_ant", Secret: "sk-ant-api03-REALREAL", Destinations: []string{up.host}, Placeholder: "sk-ant-api03-ref-b_ant-0000"}
	b.SetLeases([]proto.BindingLease{lease, lease2})
	_, body = get(t, DestURL(b.BaseURL(), up.host)+"/v1/messages", map[string]string{"X-Api-Key": Placeholder(lease2)})
	if body != "auth=;key=sk-ant-api03-REALREAL" {
		t.Fatalf("%q", body)
	}
}

func TestBlocksPlaceholderToForeignHost(t *testing.T) {
	up := newUpstream(t)
	rec := &recorder{}
	lease := proto.BindingLease{ID: "b_gh", Secret: "ghp_REAL", Destinations: []string{"api.github.com"}}
	b := start(t, up, []proto.BindingLease{lease}, []string{up.host}, rec)
	// Host is allowed (allow list) but the credential is bound elsewhere: leak attempt.
	resp, body := get(t, DestURL(b.BaseURL(), up.host)+"/exfil", map[string]string{"Authorization": "Bearer ref:b_gh"})
	if resp.StatusCode != 403 || !strings.Contains(body, "not bound") {
		t.Fatalf("%d %q", resp.StatusCode, body)
	}
	if rec.last().Decision != DecisionLeakBlocked || len(up.seen) != 0 {
		t.Fatalf("%+v seen=%d", rec.last(), len(up.seen))
	}
	// Without the credential the allow-listed host works, and the audit says allowed.
	resp, body = get(t, DestURL(b.BaseURL(), up.host)+"/ok", nil)
	if resp.StatusCode != 200 || body != "auth=;key=" || rec.last().Decision != DecisionAllowed {
		t.Fatalf("%d %q %+v", resp.StatusCode, body, rec.last())
	}
}

func TestDeniesUnlistedHostAndExpiredLease(t *testing.T) {
	up := newUpstream(t)
	rec := &recorder{}
	expired := proto.BindingLease{ID: "b_old", Secret: "s", Destinations: []string{up.host}, ExpiresAt: time.Now().Add(-time.Minute).UnixMilli()}
	b := start(t, up, []proto.BindingLease{expired}, nil, rec)
	resp, _ := get(t, DestURL(b.BaseURL(), up.host)+"/x", nil)
	if resp.StatusCode != 403 || rec.last().Decision != DecisionDenied {
		t.Fatalf("%d %+v", resp.StatusCode, rec.last())
	}
	resp, body := get(t, DestURL(b.BaseURL(), up.host)+"/x", map[string]string{"Authorization": "Bearer ref:b_old"})
	if resp.StatusCode != 403 || !strings.Contains(body, "expired") || rec.last().Decision != DecisionExpired {
		t.Fatalf("%d %q %+v", resp.StatusCode, body, rec.last())
	}
	if len(up.seen) != 0 {
		t.Fatal("upstream reached")
	}
	resp, _ = get(t, b.BaseURL()+"/nonsense", nil)
	if resp.StatusCode != 404 {
		t.Fatal(resp.StatusCode)
	}
	resp, _ = get(t, b.BaseURL()+"/healthz", nil)
	if resp.StatusCode != 200 {
		t.Fatal(resp.StatusCode)
	}
}

func TestRefusesPrivateAddressesUnlessAllowed(t *testing.T) {
	up := newUpstream(t)
	rec := &recorder{}
	b := New(Options{WS: "ws", Allow: []string{"*"}, Audit: rec.add, RootCAs: up.pool()})
	b.Start()
	defer b.Close()
	resp, body := get(t, DestURL(b.BaseURL(), up.host)+"/x", nil)
	if resp.StatusCode != 502 || !strings.Contains(body, "non-public") {
		t.Fatalf("%d %q", resp.StatusCode, body)
	}
	// Cloud metadata endpoint is never reachable.
	resp, body = get(t, b.BaseURL()+"/http/169.254.169.254/latest/meta-data", nil)
	if resp.StatusCode != 502 || !strings.Contains(body, "non-public") {
		t.Fatalf("%d %q", resp.StatusCode, body)
	}
}

func TestConnectTunnelAllowlisted(t *testing.T) {
	up := newUpstream(t)
	rec := &recorder{}
	b := start(t, up, nil, []string{up.host}, rec)
	proxyURL, _ := url.Parse(b.BaseURL())
	client := &http.Client{Transport: &http.Transport{Proxy: http.ProxyURL(proxyURL), TLSClientConfig: &tls.Config{RootCAs: up.pool()}}}
	resp, err := client.Get(up.srv.URL + "/via-connect")
	if err != nil {
		t.Fatal(err)
	}
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != 200 || string(body) != "auth=;key=" {
		t.Fatalf("%d %q", resp.StatusCode, body)
	}
	if rec.count(DecisionAllowed) < 1 {
		t.Fatalf("no allowed audit: %+v", rec.ev)
	}
	// Not allow-listed: CONNECT refused.
	other := newUpstream(t)
	_, err = client.Get(other.srv.URL + "/nope")
	if err == nil || !strings.Contains(err.Error(), "Forbidden") {
		t.Fatalf("expected 403 on CONNECT, got %v", err)
	}
	// Plain-HTTP forward proxy form (absolute URI) is also brokered.
	plain := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		io.WriteString(w, "plain:"+r.Header.Get("Authorization"))
	}))
	defer plain.Close()
	ph, _ := url.Parse(plain.URL)
	b.SetLeases([]proto.BindingLease{{ID: "b_p", Secret: "S3", Destinations: []string{ph.Host}}})
	req, _ := http.NewRequest("GET", plain.URL+"/p", nil)
	req.Header.Set("Authorization", "Token ref:b_p")
	resp, err = client.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	body, _ = io.ReadAll(resp.Body)
	if string(body) != "plain:Token S3" {
		t.Fatalf("%q", body)
	}
}

func TestStreamingIsFlushed(t *testing.T) {
	up := newUpstream(t)
	rec := &recorder{}
	b := start(t, up, nil, []string{up.host}, rec)
	resp, err := http.Get(DestURL(b.BaseURL(), up.host) + "/stream")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	buf := make([]byte, 64)
	start := time.Now()
	n, err := resp.Body.Read(buf)
	if err != nil || n == 0 {
		t.Fatal(err)
	}
	if time.Since(start) > 500*time.Millisecond {
		t.Fatal("first event not flushed promptly")
	}
	rest, _ := io.ReadAll(resp.Body)
	if strings.Count(string(buf[:n])+string(rest), "data: x") != 3 {
		t.Fatal("stream truncated")
	}
}

func TestHostMatchAndEnvResolution(t *testing.T) {
	cases := []struct {
		host string
		pats []string
		want bool
	}{
		{"api.github.com", []string{"api.github.com"}, true},
		{"api.github.com:443", []string{"api.github.com"}, true},
		{"api.github.com", []string{"*.github.com"}, true},
		{"github.com", []string{"*.github.com"}, false},
		{"evil-github.com", []string{"*.github.com"}, false},
		{"anything", []string{"*"}, true},
		{"API.GITHUB.COM", []string{"api.github.com"}, true},
		{"127.0.0.1:8443", []string{"127.0.0.1:9443"}, false},
		{"127.0.0.1:8443", []string{"127.0.0.1"}, true},
		{"127.0.0.1", []string{"127.0.0.1:9443"}, true},
	}
	for _, c := range cases {
		if hostMatches(c.host, c.pats) != c.want {
			t.Errorf("%s %v", c.host, c.pats)
		}
	}
	leases := []proto.BindingLease{{ID: "b_a", Placeholder: "sk-ph"}, {ID: "b_b"}}
	env := ResolveEnv(map[string]string{"A": "ref:b_a", "B": "ref:b_b", "U": "${REMOUNT_BROKER}/d/x", "C": "ref:b_unknown"}, "http://127.0.0.1:9", leases)
	if env["A"] != "sk-ph" || env["B"] != "ref:b_b" || env["U"] != "http://127.0.0.1:9/d/x" || env["C"] != "ref:b_unknown" {
		t.Fatalf("%v", env)
	}
	b := New(Options{})
	b.Start()
	defer b.Close()
	if b.PortOf() == 0 {
		t.Fatal("port")
	}
	if !strings.Contains(strings.Join(b.EnvFor(), " "), "HTTPS_PROXY="+b.BaseURL()) {
		t.Fatal(b.EnvFor())
	}
	if _, err := net.Dial("tcp", strings.TrimPrefix(b.BaseURL(), "http://")); err != nil {
		t.Fatal(err)
	}
}
