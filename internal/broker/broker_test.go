package broker

import (
	"bufio"
	"context"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"encoding/base64"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/netip"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"remount.dev/remount/internal/connector"
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
		if r.URL.Path == "/teapot" {
			w.WriteHeader(http.StatusTeapot)
			return
		}
		if r.URL.Path == "/redirect-ftp" {
			w.Header().Set("Location", "ftp://files.example/x")
			w.WriteHeader(http.StatusFound)
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
func (r *recorder) lastDecision(dec string) (Audit, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for i := len(r.ev) - 1; i >= 0; i-- {
		if r.ev[i].Decision == dec {
			return r.ev[i], true
		}
	}
	return Audit{}, false
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

func TestManagedPackageConnectorIsReadOnlyScopedAndAudited(t *testing.T) {
	payload := []byte("signed immutable wheel bytes")
	digestBytes := sha256.Sum256(payload)
	digest := fmt.Sprintf("sha256:%x", digestBytes)
	var upstreamCalls atomic.Int64
	var leakedDigestHeader atomic.Bool
	upstreamServer := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		upstreamCalls.Add(1)
		if r.Header.Get(connector.ExpectedDigestHeader) != "" {
			leakedDigestHeader.Store(true)
		}
		if r.URL.Path == "/redirect" {
			w.Header().Set("Location", "/artifact.whl")
			w.WriteHeader(http.StatusFound)
			return
		}
		w.Header().Set("Content-Type", "application/octet-stream")
		w.Header().Set("Set-Cookie", "registry-secret=must-not-cross")
		_, _ = w.Write(payload)
	}))
	defer upstreamServer.Close()
	upstreamURL, _ := url.Parse(upstreamServer.URL)
	pool := x509.NewCertPool()
	pool.AddCert(upstreamServer.Certificate())
	store, err := connector.NewStore(t.TempDir(), connector.StoreOptions{
		MaxBytes: 1 << 20, MaxBytesPerScope: 512 << 10, MaxObjectBytes: 256 << 10,
	})
	if err != nil {
		t.Fatal(err)
	}
	rule := proto.EgressRule{
		ID: "approved-packages", Connector: proto.EgressConnectorPackage,
		Protocol: proto.EgressProtocolHTTPS, Hosts: []string{upstreamURL.Host},
		Methods: []string{http.MethodGet, http.MethodHead}, PathPrefixes: []string{"/"},
		MaxRequests: 20, MaxResponseBytes: 256 << 10, SharedState: proto.SharedStateImmutableRead,
	}
	startPackage := func(workspace string, rec *recorder) *Broker {
		b := New(Options{
			WS: workspace, Generation: 9, Tenant: "tenant-a", Principal: "subject-a",
			Network:      proto.NetworkPolicy{Default: proto.NetworkDefaultDeny, Rules: []proto.EgressRule{rule}},
			AllowPrivate: []string{"127.0.0.1"}, RootCAs: pool, Audit: rec.add, ConnectorStore: store,
		})
		if _, err := b.Start(); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = b.Close() })
		return b
	}
	recorderA := &recorder{}
	brokerA := startPackage("ws_alpha", recorderA)
	packageURL := brokerA.PackageURL() + "/" + upstreamURL.Host + "/artifact.whl"

	for attempt := 0; attempt < 2; attempt++ {
		response, body := get(t, packageURL, map[string]string{connector.ExpectedDigestHeader: digest})
		if response.StatusCode != http.StatusOK || body != string(payload) {
			t.Fatalf("attempt=%d status=%d body=%q", attempt, response.StatusCode, body)
		}
		if response.Header.Get(connector.ContentDigestHeader) != digest || response.Header.Get("Set-Cookie") != "" {
			t.Fatalf("unsafe or missing connector headers: %v", response.Header)
		}
		for name, values := range response.Header {
			if strings.Contains(strings.ToLower(name+strings.Join(values, ",")), "cache") {
				t.Fatalf("cache state exposed to workspace: %s=%v", name, values)
			}
		}
	}
	if got := upstreamCalls.Load(); got != 1 {
		t.Fatalf("workspace did not reuse its own immutable reference: calls=%d", got)
	}
	if leakedDigestHeader.Load() {
		t.Fatal("connector-private expected digest leaked to registry")
	}
	allowed, ok := recorderA.lastDecision(DecisionAllowed)
	if !ok || allowed.WS != "ws_alpha" || allowed.Generation != 9 || allowed.Rule != rule.ID ||
		allowed.Connector != proto.EgressConnectorPackage || allowed.Digest != digest {
		t.Fatalf("incomplete provenance audit: %+v", allowed)
	}

	// The package capability does not grant the same host to the generic proxy.
	response, _ := get(t, DestURL(brokerA.BaseURL(), upstreamURL.Host)+"/artifact.whl", nil)
	if response.StatusCode != http.StatusForbidden || upstreamCalls.Load() != 1 {
		t.Fatalf("connector rule escaped to generic proxy: status=%d calls=%d", response.StatusCode, upstreamCalls.Load())
	}

	for _, method := range []string{http.MethodPost, http.MethodPut, http.MethodDelete, "PROPFIND", "MKCOL", "LOCK"} {
		req, err := http.NewRequest(method, packageURL, nil)
		if err != nil {
			t.Fatal(err)
		}
		response, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		_, _ = io.Copy(io.Discard, response.Body)
		_ = response.Body.Close()
		if response.StatusCode != http.StatusForbidden {
			t.Fatalf("method %s status=%d", method, response.StatusCode)
		}
		denied := recorderA.last()
		if denied.Decision != DecisionDenied || denied.Connector != proto.EgressConnectorPackage {
			t.Fatalf("method %s audit=%+v", method, denied)
		}
	}
	if upstreamCalls.Load() != 1 {
		t.Fatalf("denied package methods reached registry: calls=%d", upstreamCalls.Load())
	}

	// B cannot turn A's blob occupancy into a cache hit. It establishes its own
	// opaque reference with one independent registry request.
	recorderB := &recorder{}
	brokerB := startPackage("ws_bravo", recorderB)
	response, body := get(t, brokerB.PackageURL()+"/"+upstreamURL.Host+"/artifact.whl", map[string]string{connector.ExpectedDigestHeader: digest})
	if response.StatusCode != http.StatusOK || body != string(payload) || upstreamCalls.Load() != 2 {
		t.Fatalf("workspace B observed A cache: status=%d body=%q calls=%d", response.StatusCode, body, upstreamCalls.Load())
	}

	noFollow := &http.Client{CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	response, err = noFollow.Get(brokerA.PackageURL() + "/" + upstreamURL.Host + "/redirect")
	if err != nil {
		t.Fatal(err)
	}
	_ = response.Body.Close()
	location := response.Header.Get("Location")
	if response.StatusCode != http.StatusFound || !strings.HasPrefix(location, brokerA.PackageURL()+"/"+upstreamURL.Host+"/") {
		t.Fatalf("redirect escaped connector: status=%d location=%q", response.StatusCode, location)
	}
}

func TestPackageRuleFailsClosedWithoutConnectorStore(t *testing.T) {
	b := New(Options{WS: "ws", Generation: 1, Tenant: "tenant", Network: proto.NetworkPolicy{
		Rules: []proto.EgressRule{{
			ID: "packages", Connector: proto.EgressConnectorPackage, Protocol: proto.EgressProtocolHTTPS,
			Hosts: []string{"registry.example"}, SharedState: proto.SharedStateImmutableRead,
		}},
	}})
	if _, err := b.Start(); err == nil || !strings.Contains(err.Error(), "unavailable") {
		t.Fatalf("package broker started without connector store: %v", err)
	}
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
	// cred.used is the record of what the credential bought, so it carries
	// the upstream status and is emitted once the response headers exist.
	a, ok := rec.lastDecision(DecisionSubstituted)
	if !ok || a.Binding != "b_gh" || a.Status != 200 || a.Error != "" || a.WS != "ws_t" || rec.last().Decision != DecisionAllowed {
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

// A workspace on a non-loopback path to its broker (Docker's
// host.docker.internal) must exempt that host from the forward proxy, or a
// proxy-honoring client sends its capability URL through the proxy and the
// broker sees a placeholder addressed to itself.
func TestEnvExemptsAdvertisedBrokerHostFromProxy(t *testing.T) {
	up := newUpstream(t)
	rec := &recorder{}
	lease := proto.BindingLease{ID: "b_gh", Secret: "ghp_REAL", Destinations: []string{up.host}}
	b := New(Options{
		WS: "ws_t", Principal: "a_test", Leases: []proto.BindingLease{lease},
		AllowPrivate: []string{"127.0.0.1", "localhost"}, Audit: rec.add, RootCAs: up.pool(),
		Listen: "127.0.0.1:0", AdvertiseHost: "host.docker.internal",
	})
	if _, err := b.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { b.Close() })
	env := map[string]string{}
	for _, kv := range b.EnvFor() {
		k, v, _ := strings.Cut(kv, "=")
		env[k] = v
	}
	for _, k := range []string{"NO_PROXY", "no_proxy"} {
		got := strings.Split(env[k], ",")
		want := map[string]bool{"127.0.0.1": false, "localhost": false, "host.docker.internal": false}
		for _, h := range got {
			if _, ok := want[h]; ok {
				want[h] = true
			}
		}
		for h, seen := range want {
			if !seen {
				t.Fatalf("%s=%q lacks %s", k, env[k], h)
			}
		}
	}
	if !strings.Contains(env["REMOUNT_BROKER"], "host.docker.internal") || !strings.Contains(env["HTTPS_PROXY"], "host.docker.internal") {
		t.Fatalf("%+v", env)
	}
	if b.AdvertisedHost() != "host.docker.internal" {
		t.Fatal(b.AdvertisedHost())
	}
	// Defence in depth: a client that ignores NO_PROXY and forwards the
	// capability URL through the proxy is served directly, not recorded as a
	// leak. Dial the listener but address the request to the advertised host.
	_, port, _ := net.SplitHostPort(b.ln.Addr().String())
	target := "http://" + net.JoinHostPort("host.docker.internal", port) + "/c/" + b.token + "/d/" + up.host + "/repos"
	req, _ := http.NewRequest("GET", target, nil)
	req.Header.Set("Authorization", "Bearer ref:b_gh")
	proxyURL, _ := url.Parse(strings.Replace(b.ProxyURL(), "host.docker.internal", "127.0.0.1", 1))
	client := &http.Client{Transport: &http.Transport{Proxy: http.ProxyURL(proxyURL)}}
	resp, err := client.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != 200 || string(body) != "auth=Bearer ghp_REAL;key=" {
		t.Fatalf("%d %q", resp.StatusCode, body)
	}
	if rec.count(DecisionLeakBlocked) != 0 || rec.count(DecisionSubstituted) != 1 {
		t.Fatalf("leak_blocked=%d substituted=%d", rec.count(DecisionLeakBlocked), rec.count(DecisionSubstituted))
	}
	// Loopback brokers do not repeat the loopback entries.
	plain := start(t, up, nil, nil, &recorder{})
	if plain.NoProxy() != "127.0.0.1,localhost" {
		t.Fatal(plain.NoProxy())
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
	proxyURL, _ := url.Parse(b.ProxyURL())
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
	// Plain-HTTP forward proxy form is brokered, but a real credential is
	// never substituted onto its cleartext upstream leg.
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
	if resp.StatusCode != http.StatusForbidden || !strings.Contains(string(body), "never sent over plaintext") {
		t.Fatalf("%d %q", resp.StatusCode, body)
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
		{"127.0.0.1", []string{"127.0.0.1:9443"}, false},
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
	if !strings.Contains(strings.Join(b.EnvFor(), " "), "HTTPS_PROXY="+b.ProxyURL()) {
		t.Fatal(b.EnvFor())
	}
	// Bun reads "http://tok@host" as host "tok@host"; the userinfo must carry
	// an explicit empty password so Bun-compiled harnesses reach the broker.
	pu, err := url.Parse(b.ProxyURL())
	if err != nil {
		t.Fatal(err)
	}
	if _, set := pu.User.Password(); !set || pu.User.Username() != b.token || !strings.Contains(b.ProxyURL(), b.token+":@") {
		t.Fatalf("proxy url %q must be http://TOKEN:@host:port", b.ProxyURL())
	}
	if !b.validProxyAuthorization("Basic " + base64.StdEncoding.EncodeToString([]byte(b.token+":"))) {
		t.Fatal("token with empty password must authenticate")
	}
	if _, err := net.Dial("tcp", b.ln.Addr().String()); err != nil {
		t.Fatal(err)
	}
}

func TestPlaceholderMatchingIsExactAndNonCascading(t *testing.T) {
	up := newUpstream(t)
	rec := &recorder{}
	short := proto.BindingLease{ID: "b_a", Secret: "SHORT", Destinations: []string{up.host}}
	long := proto.BindingLease{ID: "b_ab", Secret: "ref:b_a", Destinations: []string{up.host}}
	b := start(t, up, []proto.BindingLease{short, long}, nil, rec)
	_, body := get(t, DestURL(b.BaseURL(), up.host)+"/exact", map[string]string{
		"X-Api-Key": "ref:b_ab, ref:b_a",
	})
	if body != "auth=;key=ref:b_a, SHORT" {
		t.Fatalf("prefix or cascading substitution: %q", body)
	}
	if rec.count(DecisionSubstituted) != 2 {
		t.Fatalf("expected one attempt audit per binding: %+v", rec.ev)
	}
}

func TestCredentialAttemptAuditedWhenUpstreamFails(t *testing.T) {
	rec := &recorder{}
	lease := proto.BindingLease{
		ID: "b_fail", Secret: "REAL", Destinations: []string{"does-not-exist.invalid:443"},
		ExpiresAt: time.Now().Add(time.Hour).UnixMilli(),
	}
	b := New(Options{WS: "ws", Leases: []proto.BindingLease{lease}, Audit: rec.add})
	if _, err := b.Start(); err != nil {
		t.Fatal(err)
	}
	defer b.Close()
	resp, _ := get(t, DestURL(b.BaseURL(), "does-not-exist.invalid:443")+"/x", map[string]string{"Authorization": "Bearer ref:b_fail"})
	if resp.StatusCode != http.StatusBadGateway {
		t.Fatal(resp.StatusCode)
	}
	if rec.count(DecisionSubstituted) != 1 || rec.count(DecisionDenied) == 0 {
		t.Fatalf("missing attempt/failure audit: %+v", rec.ev)
	}
	// The credential left the broker but bought nothing: status 0 and the
	// failure class, never the raw error text, which can embed the host.
	a, _ := rec.lastDecision(DecisionSubstituted)
	if a.Status != 0 || a.Error != ErrorClassDNS {
		t.Fatalf("%+v", a)
	}
}

// Every credential use is audited exactly once with the upstream outcome,
// including non-2xx statuses and responses the broker itself rejects.
func TestCredentialUseRecordsUpstreamOutcomeOnce(t *testing.T) {
	up := newUpstream(t)
	rec := &recorder{}
	lease := proto.BindingLease{ID: "b_gh", Secret: "REAL", Destinations: []string{up.host}, ExpiresAt: time.Now().Add(time.Hour).UnixMilli()}
	b := start(t, up, []proto.BindingLease{lease}, nil, rec)
	hdr := map[string]string{"Authorization": "Bearer ref:b_gh"}

	resp, _ := get(t, DestURL(b.BaseURL(), up.host)+"/teapot", hdr)
	if resp.StatusCode != http.StatusTeapot {
		t.Fatal(resp.StatusCode)
	}
	a, _ := rec.lastDecision(DecisionSubstituted)
	if rec.count(DecisionSubstituted) != 1 || a.Status != http.StatusTeapot || a.Error != "" {
		t.Fatalf("count=%d %+v", rec.count(DecisionSubstituted), a)
	}

	// A redirect the broker refuses to relay still consumed the credential;
	// the audit shows the upstream status and why the client saw 502.
	resp, _ = get(t, DestURL(b.BaseURL(), up.host)+"/redirect-ftp", hdr)
	if resp.StatusCode != http.StatusBadGateway {
		t.Fatal(resp.StatusCode)
	}
	a, _ = rec.lastDecision(DecisionSubstituted)
	if rec.count(DecisionSubstituted) != 2 || a.Status != http.StatusFound || a.Error != ErrorClassRedirect {
		t.Fatalf("count=%d %+v", rec.count(DecisionSubstituted), a)
	}

	// Connection refused: the dial never produced headers.
	dead := httptest.NewTLSServer(http.NotFoundHandler())
	deadHost := strings.TrimPrefix(dead.URL, "https://")
	dead.Close()
	b.SetLeases([]proto.BindingLease{lease, {ID: "b_dead", Secret: "REAL2", Destinations: []string{deadHost}, ExpiresAt: time.Now().Add(time.Hour).UnixMilli()}})
	resp, _ = get(t, DestURL(b.BaseURL(), deadHost)+"/x", map[string]string{"Authorization": "Bearer ref:b_dead"})
	if resp.StatusCode != http.StatusBadGateway {
		t.Fatal(resp.StatusCode)
	}
	a, _ = rec.lastDecision(DecisionSubstituted)
	if rec.count(DecisionSubstituted) != 3 || a.Binding != "b_dead" || a.Status != 0 || a.Error != ErrorClassRefused {
		t.Fatalf("count=%d %+v", rec.count(DecisionSubstituted), a)
	}
	for _, e := range rec.ev {
		if strings.Contains(e.Reason, "REAL") || strings.Contains(e.Error, "REAL") {
			t.Fatalf("secret in audit: %+v", e)
		}
	}
}

func TestBrokerRequiresWorkspaceCapability(t *testing.T) {
	up := newUpstream(t)
	rec := &recorder{}
	b := start(t, up, nil, []string{up.host}, rec)
	raw := "http://" + b.ln.Addr().String() + "/d/" + up.host + "/x"
	resp, _ := get(t, raw, nil)
	if resp.StatusCode != http.StatusForbidden || rec.last().Decision != DecisionUnauthenticated {
		t.Fatalf("status=%d audit=%+v", resp.StatusCode, rec.last())
	}
}

func TestBindingDoesNotAuthorizeConnect(t *testing.T) {
	up := newUpstream(t)
	for _, expiresAt := range []int64{time.Now().Add(time.Hour).UnixMilli(), time.Now().Add(-time.Second).UnixMilli()} {
		rec := &recorder{}
		lease := proto.BindingLease{ID: "b_bound", Destinations: []string{up.host}, ExpiresAt: expiresAt}
		b := start(t, up, []proto.BindingLease{lease}, nil, rec)
		proxyURL, _ := url.Parse(b.ProxyURL())
		client := &http.Client{Transport: &http.Transport{Proxy: http.ProxyURL(proxyURL), TLSClientConfig: &tls.Config{RootCAs: up.pool()}}}
		_, err := client.Get(up.srv.URL + "/binding-only")
		if err == nil || !strings.Contains(err.Error(), "Forbidden") {
			t.Fatalf("binding expiry=%d implicitly authorized CONNECT: %v", expiresAt, err)
		}
		if rec.last().Decision != DecisionDenied {
			t.Fatalf("expiry=%d audit=%+v", expiresAt, rec.last())
		}
	}
}

func TestCarrierGradeNATIsNonPublic(t *testing.T) {
	for _, raw := range []string{"100.64.0.1", "100.100.100.200", "198.18.0.1", "2001:db8::1"} {
		ip, err := netip.ParseAddr(raw)
		if err != nil {
			t.Fatal(err)
		}
		if !isPrivate(ip) {
			t.Errorf("%s was treated as public", raw)
		}
	}
}

func TestBodyBudgetArithmeticDoesNotOverflow(t *testing.T) {
	maxInt64 := int64(^uint64(0) >> 1)
	reader := &budgetReadCloser{ReadCloser: io.NopCloser(strings.NewReader("ok")), remaining: maxInt64}
	got, err := io.ReadAll(reader)
	if err != nil || string(got) != "ok" {
		t.Fatalf("response reader=%q err=%v", got, err)
	}
	body, n, err := bufferRequestBody(io.NopCloser(strings.NewReader("ok")), maxInt64)
	if err != nil || n != 2 {
		t.Fatalf("request buffer bytes=%d err=%v", n, err)
	}
	defer body.Close()
}

func TestTypedEgressRuleEnforcesMethodPathPortAndRequestCount(t *testing.T) {
	up := newUpstream(t)
	rec := &recorder{}
	parsed, err := url.Parse(up.srv.URL)
	if err != nil {
		t.Fatal(err)
	}
	_, rawPort, err := net.SplitHostPort(parsed.Host)
	if err != nil {
		t.Fatal(err)
	}
	port, err := net.LookupPort("tcp", rawPort)
	if err != nil {
		t.Fatal(err)
	}
	b := New(Options{
		WS: "ws_policy", Generation: 9, Principal: "agent", Allow: []string{"*"},
		AllowPrivate: []string{"127.0.0.1"}, RootCAs: up.pool(), Audit: rec.add,
		Network: proto.NetworkPolicy{Rules: []proto.EgressRule{{
			ID: "registry-read", Protocol: proto.EgressProtocolHTTPS,
			Hosts: []string{"127.0.0.1"}, Ports: []uint16{uint16(port)},
			Methods: []string{"GET"}, PathPrefixes: []string{"/v2/"}, MaxRequests: 2,
			SharedState: proto.SharedStateImmutableRead,
		}}},
	})
	if _, err := b.Start(); err != nil {
		t.Fatal(err)
	}
	defer b.Close()

	resp, _ := get(t, DestURL(b.BaseURL(), up.host)+"/v2/one", nil)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("allowed status=%d", resp.StatusCode)
	}
	req, _ := http.NewRequest(http.MethodPost, DestURL(b.BaseURL(), up.host)+"/v2/write", strings.NewReader("x"))
	resp, err = http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusForbidden {
		t.Fatalf("method status=%d", resp.StatusCode)
	}
	resp, _ = get(t, DestURL(b.BaseURL(), up.host)+"/admin", nil)
	if resp.StatusCode != http.StatusForbidden {
		t.Fatalf("path status=%d", resp.StatusCode)
	}
	resp, _ = get(t, DestURL(b.BaseURL(), up.host)+"/v2/two", nil)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("second allowed status=%d", resp.StatusCode)
	}
	resp, _ = get(t, DestURL(b.BaseURL(), up.host)+"/v2/three", nil)
	if resp.StatusCode != http.StatusTooManyRequests {
		t.Fatalf("limit status=%d", resp.StatusCode)
	}
	audit := rec.last()
	if audit.Decision != DecisionLimitExceeded || audit.Rule != "registry-read" ||
		audit.Generation != 9 || audit.Protocol != proto.EgressProtocolHTTPS ||
		audit.SharedState != proto.SharedStateImmutableRead {
		t.Fatalf("audit=%+v", audit)
	}
	up.mu.Lock()
	seen := len(up.seen)
	up.mu.Unlock()
	if seen != 2 {
		t.Fatalf("upstream requests=%d, want 2", seen)
	}
}

func TestBrokerAdmissionIsBoundedPerWorkspace(t *testing.T) {
	entered := make(chan struct{})
	release := make(chan struct{})
	defer func() {
		select {
		case <-release:
		default:
			close(release)
		}
	}()
	upstream := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		select {
		case <-entered:
		default:
			close(entered)
		}
		<-release
		_, _ = io.WriteString(w, "done")
	}))
	defer upstream.Close()
	parsed, _ := url.Parse(upstream.URL)
	roots := x509.NewCertPool()
	roots.AddCert(upstream.Certificate())
	rec := &recorder{}
	b := New(Options{
		WS: "ws_bounded", MaxConnections: 4, MaxConcurrentRequests: 1,
		AllowPrivate: []string{"127.0.0.1"}, RootCAs: roots, Audit: rec.add,
		Network: proto.NetworkPolicy{Rules: []proto.EgressRule{{
			ID: "bounded", Protocol: proto.EgressProtocolHTTPS, Hosts: []string{parsed.Host},
			Methods: []string{"GET"},
		}}},
	})
	if _, err := b.Start(); err != nil {
		t.Fatal(err)
	}
	defer b.Close()
	firstDone := make(chan error, 1)
	go func() {
		resp, err := http.Get(DestURL(b.BaseURL(), parsed.Host) + "/first")
		if err == nil {
			_, err = io.Copy(io.Discard, resp.Body)
			resp.Body.Close()
		}
		firstDone <- err
	}()
	select {
	case <-entered:
	case <-time.After(2 * time.Second):
		t.Fatal("first request did not reach upstream")
	}
	resp, err := http.Get(DestURL(b.BaseURL(), parsed.Host) + "/second")
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusTooManyRequests || rec.last().Decision != DecisionLimitExceeded {
		t.Fatalf("overload status=%d audit=%+v", resp.StatusCode, rec.last())
	}
	close(release)
	if err := <-firstDone; err != nil {
		t.Fatal(err)
	}
}

func TestTypedEgressRuleEnforcesRequestAndResponseBytes(t *testing.T) {
	var received atomic.Int64
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		received.Add(int64(len(body)))
		if r.URL.Path == "/large" {
			w.Header().Set("Content-Length", "8")
			_, _ = io.WriteString(w, "12345678")
			return
		}
		if r.URL.Path == "/chunked-large" {
			_, _ = io.WriteString(w, "123")
			w.(http.Flusher).Flush()
			_, _ = io.WriteString(w, "45678")
			return
		}
		_, _ = io.WriteString(w, "ok")
	}))
	defer srv.Close()
	parsed, _ := url.Parse(srv.URL)
	pool := x509.NewCertPool()
	pool.AddCert(srv.Certificate())
	rec := &recorder{}
	b := New(Options{
		WS: "ws_budget", Generation: 3, Principal: "agent", AllowPrivate: []string{"127.0.0.1"},
		RootCAs: pool, Audit: rec.add,
		Network: proto.NetworkPolicy{Rules: []proto.EgressRule{{
			ID: "bounded-api", Protocol: proto.EgressProtocolHTTPS, Hosts: []string{parsed.Host},
			Methods: []string{"GET", "POST"}, MaxRequestBytes: 4, MaxResponseBytes: 4,
			SharedState: proto.SharedStateScopedWrite,
		}}},
	})
	if _, err := b.Start(); err != nil {
		t.Fatal(err)
	}
	defer b.Close()

	resp, err := http.Post(DestURL(b.BaseURL(), parsed.Host)+"/upload", "text/plain", strings.NewReader("12345"))
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusRequestEntityTooLarge || received.Load() != 0 {
		t.Fatalf("oversized request status=%d received=%d", resp.StatusCode, received.Load())
	}
	unknownLength := io.NopCloser(strings.NewReader("12345"))
	req, err := http.NewRequest(http.MethodPost, DestURL(b.BaseURL(), parsed.Host)+"/upload", unknownLength)
	if err != nil {
		t.Fatal(err)
	}
	req.ContentLength = -1
	resp, err = http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusRequestEntityTooLarge || received.Load() != 0 {
		t.Fatalf("chunked oversized request status=%d received=%d", resp.StatusCode, received.Load())
	}
	resp, err = http.Post(DestURL(b.BaseURL(), parsed.Host)+"/upload", "text/plain", strings.NewReader("1234"))
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK || received.Load() != 4 {
		t.Fatalf("bounded request status=%d received=%d", resp.StatusCode, received.Load())
	}
	resp, err = http.Get(DestURL(b.BaseURL(), parsed.Host) + "/large")
	if err != nil {
		t.Fatal(err)
	}
	_, _ = io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != http.StatusBadGateway || rec.last().Decision != DecisionLimitExceeded {
		t.Fatalf("oversized response status=%d audit=%+v", resp.StatusCode, rec.last())
	}
	resp, err = http.Get(DestURL(b.BaseURL(), parsed.Host) + "/chunked-large")
	if err != nil {
		t.Fatal(err)
	}
	body, readErr := io.ReadAll(resp.Body)
	resp.Body.Close()
	if len(body) > 4 || readErr == nil || rec.last().Decision != DecisionLimitExceeded || rec.last().ResponseBytes != 5 {
		t.Fatalf("chunked response status=%d body=%q read_err=%v audit=%+v", resp.StatusCode, body, readErr, rec.last())
	}
}

func TestTypedEgressRuleRedactsResponseBeforeWorkspace(t *testing.T) {
	const secret = "sk-live-super-secret"
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = io.WriteString(w, "first="+secret+" second="+secret)
	}))
	defer srv.Close()
	parsed, _ := url.Parse(srv.URL)
	roots := x509.NewCertPool()
	roots.AddCert(srv.Certificate())
	rec := &recorder{}
	b := New(Options{
		WS: "ws_redact", Principal: "alice", AllowPrivate: []string{"127.0.0.1"}, RootCAs: roots, Audit: rec.add,
		Network: proto.NetworkPolicy{Rules: []proto.EgressRule{{
			ID: "redacted-api", Protocol: proto.EgressProtocolHTTPS, Hosts: []string{parsed.Host},
			Methods: []string{http.MethodGet}, Redact: []string{`sk-[a-z-]+`},
		}}},
	})
	if _, err := b.Start(); err != nil {
		t.Fatal(err)
	}
	defer b.Close()
	resp, body := get(t, DestURL(b.BaseURL(), parsed.Host)+"/", nil)
	if resp.StatusCode != http.StatusOK || strings.Contains(body, secret) || strings.Count(body, "[redacted]") != 2 {
		t.Fatalf("status=%d body=%q", resp.StatusCode, body)
	}
	audit := rec.last()
	if audit.Decision != DecisionRedacted || audit.Redactions != 2 || audit.ResponseBytes != int64(len("first="+secret+" second="+secret)) {
		t.Fatalf("redaction audit = %+v", audit)
	}
}

func TestApproveModeNeverReachesUpstreamBeforeDurableDecision(t *testing.T) {
	var reached atomic.Int64
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		reached.Add(1)
		_, _ = io.WriteString(w, "ok")
	}))
	defer srv.Close()
	parsed, _ := url.Parse(srv.URL)
	roots := x509.NewCertPool()
	roots.AddCert(srv.Certificate())
	var mu sync.Mutex
	allowed := false
	var fingerprints []string
	b := New(Options{
		WS: "ws_approve", Generation: 7, Principal: "agent:alice", AllowPrivate: []string{"127.0.0.1"}, RootCAs: roots,
		ApprovalWait: time.Millisecond, Network: proto.NetworkPolicy{Rules: []proto.EgressRule{{
			ID: "approval", Mode: proto.EgressModeApprove, Protocol: proto.EgressProtocolHTTPS, Hosts: []string{parsed.Host}, Methods: []string{"POST"},
		}}},
		Approval: func(_ context.Context, req proto.EgressApprovalReq) (*proto.EgressApprovalRes, error) {
			mu.Lock()
			fingerprints = append(fingerprints, req.Fingerprint)
			permit := allowed
			mu.Unlock()
			if permit {
				return &proto.EgressApprovalRes{ID: "ap_test", Status: proto.ApprovalDecided, Allowed: true}, nil
			}
			return &proto.EgressApprovalRes{ID: "ap_test", Status: proto.ApprovalPending}, nil
		},
	})
	if _, err := b.Start(); err != nil {
		t.Fatal(err)
	}
	defer b.Close()
	destination := DestURL(b.BaseURL(), parsed.Host) + "/repos?a=1"
	resp, err := http.Post(destination, "application/json", strings.NewReader(`{"x":1}`))
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusForbidden || resp.Header.Get("X-Remount-Approval") != "ap_test" || resp.Header.Get("Retry-After") == "" || reached.Load() != 0 {
		t.Fatalf("pending status=%d headers=%v reached=%d", resp.StatusCode, resp.Header, reached.Load())
	}
	mu.Lock()
	allowed = true
	mu.Unlock()
	resp, err = http.Post(destination, "application/json", strings.NewReader(`{"x":1}`))
	if err != nil {
		t.Fatal(err)
	}
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK || string(body) != "ok" || reached.Load() != 1 {
		t.Fatalf("approved status=%d body=%q reached=%d", resp.StatusCode, body, reached.Load())
	}
	mu.Lock()
	defer mu.Unlock()
	if len(fingerprints) != 2 || fingerprints[0] != fingerprints[1] {
		t.Fatalf("retry fingerprints = %v", fingerprints)
	}
}

func TestTypedEgressRuleRejectsInvalidResponseRedaction(t *testing.T) {
	b := New(Options{
		Network: proto.NetworkPolicy{Rules: []proto.EgressRule{{
			ID: "bad", Protocol: proto.EgressProtocolHTTPS, Hosts: []string{"example.com"}, Redact: []string{"["},
		}}},
	})
	if _, err := b.Start(); err == nil {
		_ = b.Close()
		t.Fatal("invalid response redaction was accepted")
	}
}

func TestTypedPolicyRejectsPathAndAuthorityAmbiguity(t *testing.T) {
	up := newUpstream(t)
	b := New(Options{
		WS: "ws_paths", AllowPrivate: []string{"127.0.0.1"}, RootCAs: up.pool(),
		Network: proto.NetworkPolicy{Rules: []proto.EgressRule{{
			ID: "safe", Protocol: proto.EgressProtocolHTTPS, Hosts: []string{up.host},
			Methods: []string{"GET"}, PathPrefixes: []string{"/safe"},
		}}},
	})
	if _, err := b.Start(); err != nil {
		t.Fatal(err)
	}
	defer b.Close()
	for rawURL, want := range map[string]int{
		DestURL(b.BaseURL(), up.host) + "/safe/ok":                http.StatusOK,
		DestURL(b.BaseURL(), up.host) + "/safe-but-not-a-segment": http.StatusForbidden,
		DestURL(b.BaseURL(), up.host) + "/safe%252f..%252fadmin":  http.StatusBadRequest,
		b.BaseURL() + "/d/user@" + up.host + "/safe":              http.StatusBadRequest,
		b.BaseURL() + "/d/" + up.host + ":not-a-port/safe":        http.StatusBadRequest,
	} {
		resp, err := http.Get(rawURL)
		if err != nil {
			t.Fatalf("GET %s: %v", rawURL, err)
		}
		resp.Body.Close()
		if resp.StatusCode != want {
			t.Errorf("GET %s status=%d want=%d", rawURL, resp.StatusCode, want)
		}
	}
	up.mu.Lock()
	paths := append([]string(nil), up.paths...)
	up.mu.Unlock()
	if len(paths) != 1 || paths[0] != "/safe/ok" {
		t.Fatalf("ambiguous request reached upstream: %v", paths)
	}
}

func TestSuspendingBrokerClosesEstablishedConnectTunnels(t *testing.T) {
	up := newUpstream(t)
	b := start(t, up, nil, []string{up.host}, &recorder{})
	conn, err := net.Dial("tcp", b.ln.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	auth := base64.StdEncoding.EncodeToString([]byte(b.token + ":"))
	if _, err := fmt.Fprintf(conn, "CONNECT %s HTTP/1.1\r\nHost: %s\r\nProxy-Authorization: Basic %s\r\n\r\n", up.host, up.host, auth); err != nil {
		t.Fatal(err)
	}
	reader := bufio.NewReader(conn)
	status, err := reader.ReadString('\n')
	if err != nil || !strings.Contains(status, " 200 ") {
		t.Fatalf("CONNECT status=%q err=%v", status, err)
	}
	for {
		line, readErr := reader.ReadString('\n')
		if readErr != nil {
			t.Fatal(readErr)
		}
		if line == "\r\n" {
			break
		}
	}
	b.Suspend()
	if err := conn.SetReadDeadline(time.Now().Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	if _, err := reader.ReadByte(); err == nil {
		t.Fatal("CONNECT tunnel remained readable after broker suspension")
	}
	b.mu.RLock()
	remaining := len(b.tunnels)
	b.mu.RUnlock()
	if remaining != 0 {
		t.Fatalf("tracked tunnels after suspension=%d", remaining)
	}
}

func TestTypedConnectRequiresExplicitConnectCapability(t *testing.T) {
	up := newUpstream(t)
	startBroker := func(policy proto.NetworkPolicy) *Broker {
		b := New(Options{
			WS: "ws_connect", Generation: 1, Network: policy, Allow: []string{"*"},
			AllowPrivate: []string{"127.0.0.1"}, RootCAs: up.pool(),
		})
		if _, err := b.Start(); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = b.Close() })
		return b
	}
	allowed := startBroker(proto.NetworkPolicy{Rules: []proto.EgressRule{{
		ID: "tls-tunnel", Protocol: proto.EgressProtocolConnect, Hosts: []string{up.host},
		Methods: []string{"CONNECT"}, SharedState: proto.SharedStateNone,
	}}})
	proxyURL, _ := url.Parse(allowed.ProxyURL())
	client := &http.Client{Transport: &http.Transport{Proxy: http.ProxyURL(proxyURL), TLSClientConfig: &tls.Config{RootCAs: up.pool()}}}
	resp, err := client.Get(up.srv.URL + "/explicit")
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()

	denied := startBroker(proto.NetworkPolicy{Rules: []proto.EgressRule{{
		ID: "reverse-only", Protocol: proto.EgressProtocolHTTPS, Hosts: []string{up.host},
	}}})
	proxyURL, _ = url.Parse(denied.ProxyURL())
	client = &http.Client{Transport: &http.Transport{Proxy: http.ProxyURL(proxyURL), TLSClientConfig: &tls.Config{RootCAs: up.pool()}}}
	if _, err := client.Get(up.srv.URL + "/implicit"); err == nil || !strings.Contains(err.Error(), "Forbidden") {
		t.Fatalf("HTTPS rule implicitly authorized CONNECT: %v", err)
	}
}

func TestBrokerRejectsUnenforceablePolicyBeforeListening(t *testing.T) {
	b := New(Options{Network: proto.NetworkPolicy{Rules: []proto.EgressRule{{
		ID: "bad-tunnel", Protocol: proto.EgressProtocolConnect, Hosts: []string{"example.com"},
		MaxResponseBytes: 10,
	}}}})
	if _, err := b.Start(); err == nil {
		t.Fatal("broker accepted byte-limited opaque CONNECT rule")
	}
	if b.ln != nil {
		t.Fatal("broker listened before validating policy")
	}
}

func TestReverseProxyRedirectsAreRewrittenAndReauthorized(t *testing.T) {
	var escapedHits atomic.Int32
	escaped := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		escapedHits.Add(1)
	}))
	defer escaped.Close()
	upstreamTLS := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, escaped.URL+"/exfil", http.StatusFound)
	}))
	defer upstreamTLS.Close()
	upstreamURL, _ := url.Parse(upstreamTLS.URL)
	pool := x509.NewCertPool()
	pool.AddCert(upstreamTLS.Certificate())
	rec := &recorder{}
	b := New(Options{
		WS: "ws_redirect", Generation: 5, Allow: []string{"*"}, AllowPrivate: []string{"127.0.0.1"},
		RootCAs: pool, Audit: rec.add,
		Network: proto.NetworkPolicy{Rules: []proto.EgressRule{{
			ID: "one-hop", Protocol: proto.EgressProtocolHTTPS, Hosts: []string{upstreamURL.Host},
			Methods: []string{"GET"}, PathPrefixes: []string{"/start"},
		}}},
	})
	if _, err := b.Start(); err != nil {
		t.Fatal(err)
	}
	defer b.Close()
	resp, err := http.Get(DestURL(b.BaseURL(), upstreamURL.Host) + "/start")
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusForbidden || escapedHits.Load() != 0 {
		t.Fatalf("redirect status=%d escaped_hits=%d", resp.StatusCode, escapedHits.Load())
	}
	if audit := rec.last(); audit.Decision != DecisionDenied || audit.Generation != 5 ||
		!strings.Contains(audit.Reason, "no typed egress rule") {
		t.Fatalf("redirect audit=%+v", audit)
	}
}
