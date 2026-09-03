// Package broker is the node's egress credential broker: the only place a
// real secret meets a workspace's network traffic.
//
// A workspace holds placeholders ("ref:b_github"). It sends requests to the
// broker's loopback listener, either as a reverse-proxy path
// (http://127.0.0.1:PORT/d/api.github.com/repos/...) or as an HTTP proxy
// (HTTP_PROXY / HTTPS_PROXY). The broker checks the destination against the
// workspace's bindings and allow list, swaps placeholders for the real
// secret only when the destination matches that binding, re-originates the
// connection over TLS, and emits an audit event for every decision.
//
// A placeholder aimed at the wrong host is not forwarded: it is a leak
// attempt and the request is blocked (docs/adr/0007-secret-blind-workspaces.md).
//
// CONNECT tunnels are allowed to permitted hosts but cannot be rewritten
// without terminating TLS; v0 does not install a CA in the workspace, so
// credentials flow only through the reverse-proxy path.
package broker

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/subtle"
	"crypto/tls"
	"crypto/x509"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httputil"
	"net/netip"
	"net/url"
	"os"
	pathpkg "path"
	"sort"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"remount.dev/remount/internal/connector"
	"remount.dev/remount/internal/metrics"
	"remount.dev/remount/internal/proto"
)

// Decision names for audit events.
const (
	DecisionAllowed         = "allowed"
	DecisionSubstituted     = "substituted"
	DecisionDenied          = "denied"          // destination not permitted
	DecisionLeakBlocked     = "leak_blocked"    // placeholder aimed at a foreign host
	DecisionExpired         = "expired"         // lease TTL passed; fail closed
	DecisionUnauthenticated = "unauthenticated" // caller lacks this workspace broker's capability
	DecisionLimitExceeded   = "limit_exceeded"  // a typed rule exhausted its request or byte budget
)

// Upstream failure classes recorded on a credential-use audit whose request
// produced no response headers. Classes, never raw error text: the text can
// embed the destination or a redacted secret's shape.
const (
	ErrorClassDNS       = "dns"
	ErrorClassRefused   = "connection_refused"
	ErrorClassReset     = "connection_reset"
	ErrorClassTimeout   = "timeout"
	ErrorClassTLS       = "tls"
	ErrorClassCanceled  = "canceled"
	ErrorClassEOF       = "eof"
	ErrorClassRedirect  = "redirect_rejected"
	ErrorClassLimit     = "response_limit"
	ErrorClassPrivate   = "non_public_address"
	ErrorClassConnector = "connector"
	ErrorClassOther     = "upstream"
)

// Audit is one broker decision.
type Audit struct {
	At            time.Time
	WS            string
	Generation    uint64
	Principal     string
	Decision      string
	Binding       string // binding id when a placeholder was involved
	Rule          string // typed egress rule id when policy selected one
	Protocol      string
	SharedState   string
	Connector     string
	Op            string // connector operation when the connector names one (git: fetch|push)
	Repo          string // owner/name for git connector decisions
	Digest        string
	Cached        bool
	Host          string
	Method        string
	Path          string
	Reason        string
	Status        int    // upstream status when known; 0 when no response headers arrived
	Error         string // ErrorClass* when a credential was released but the upstream failed
	RequestBytes  int64
	ResponseBytes int64
}

// Options configure a per-workspace broker.
type Options struct {
	WS         string
	Generation uint64
	Principal  string
	Tenant     string
	Leases     []proto.BindingLease
	Network    proto.NetworkPolicy
	// Allow lists hosts reachable without any credential. Patterns: exact
	// host, "*.suffix" (subdomains only), optional ":port".
	Allow []string
	// AllowPrivate lists host patterns that may resolve to loopback, private
	// or link-local addresses (e.g. a local Ollama). Everything else that
	// resolves there is refused, including cloud metadata endpoints.
	AllowPrivate []string
	Audit        func(Audit)
	// RootCAs overrides upstream TLS trust (tests).
	RootCAs *x509.CertPool
	// Listen address; default 127.0.0.1:0.
	Listen string
	// AdvertiseHost overrides the host placed in workspace URLs while keeping
	// the selected listener port (for example host.docker.internal).
	AdvertiseHost string
	// MaxConnections bounds accepted client sockets for this workspace. Zero
	// selects 128. MaxConcurrentRequests similarly defaults to 64 and includes
	// the full lifetime of CONNECT tunnels and streaming responses.
	MaxConnections        int
	MaxConcurrentRequests int
	// ConnectorStore is the node-owned immutable package cache. Package rules
	// fail closed when it is unavailable.
	ConnectorStore *connector.Store
	// Repo is the workspace's declared repository. Without typed policy it is
	// the only thing the /git/ surface serves: fetch and push of exactly that
	// repository, so `--repo` works under the local profile without a rule
	// while still refusing every other path on the hosting service.
	Repo proto.RepoSpec
}

// Broker serves one workspace.
type Broker struct {
	opts         Options
	mu           sync.RWMutex
	leases       []proto.BindingLease
	srv          *http.Server
	ln           net.Listener
	base         string
	proxyBase    string
	advertised   string // host:port workspaces dial
	token        string
	client       *http.Transport
	suspended    bool
	ruleRequests map[string]int64
	tunnels      map[*brokerTunnel]struct{}
	requestSlots chan struct{}
	connectors   map[string]connector.Connector
}

type brokerTunnel struct {
	downstream net.Conn
	upstream   net.Conn
}

type limitedListener struct {
	net.Listener
	slots chan struct{}
	done  chan struct{}
	once  sync.Once
}

type limitedConn struct {
	net.Conn
	release func()
	once    sync.Once
}

func (l *limitedListener) Accept() (net.Conn, error) {
	select {
	case l.slots <- struct{}{}:
	case <-l.done:
		return nil, net.ErrClosed
	}
	conn, err := l.Listener.Accept()
	if err != nil {
		<-l.slots
		return nil, err
	}
	return &limitedConn{Conn: conn, release: func() { <-l.slots }}, nil
}

func (l *limitedListener) Close() error {
	l.once.Do(func() { close(l.done) })
	return l.Listener.Close()
}

func (c *limitedConn) Close() error {
	err := c.Conn.Close()
	c.once.Do(c.release)
	return err
}

func cloneLeases(leases []proto.BindingLease) []proto.BindingLease {
	out := append([]proto.BindingLease(nil), leases...)
	for i := range out {
		out[i].Destinations = append([]string(nil), out[i].Destinations...)
	}
	return out
}

// New builds a broker; call Start to listen.
func New(opts Options) *Broker {
	if opts.MaxConnections <= 0 {
		opts.MaxConnections = 128
	}
	if opts.MaxConcurrentRequests <= 0 {
		opts.MaxConcurrentRequests = 64
	}
	opts.Leases = cloneLeases(opts.Leases)
	opts.Allow = append([]string(nil), opts.Allow...)
	opts.AllowPrivate = append([]string(nil), opts.AllowPrivate...)
	opts.Network.Rules = append([]proto.EgressRule(nil), opts.Network.Rules...)
	for i := range opts.Network.Rules {
		rule := &opts.Network.Rules[i]
		rule.Hosts = append([]string(nil), rule.Hosts...)
		rule.Repos = append([]string(nil), rule.Repos...)
		rule.Ports = append([]uint16(nil), rule.Ports...)
		rule.Methods = append([]string(nil), rule.Methods...)
		rule.PathPrefixes = append([]string(nil), rule.PathPrefixes...)
	}
	b := &Broker{
		opts: opts, leases: cloneLeases(opts.Leases), ruleRequests: map[string]int64{},
		tunnels: map[*brokerTunnel]struct{}{}, requestSlots: make(chan struct{}, opts.MaxConcurrentRequests),
		connectors: map[string]connector.Connector{},
	}
	dialer := &net.Dialer{Timeout: 15 * time.Second, Control: nil}
	b.client = &http.Transport{
		Proxy: nil, // never chain through an ambient proxy
		DialContext: func(ctx context.Context, network, addr string) (net.Conn, error) {
			return b.dial(ctx, dialer, network, addr)
		},
		TLSClientConfig:        &tls.Config{RootCAs: opts.RootCAs, MinVersion: tls.VersionTLS12},
		ForceAttemptHTTP2:      true,
		MaxIdleConns:           64,
		MaxIdleConnsPerHost:    16,
		MaxConnsPerHost:        opts.MaxConcurrentRequests,
		IdleConnTimeout:        60 * time.Second,
		TLSHandshakeTimeout:    15 * time.Second,
		ResponseHeaderTimeout:  5 * time.Minute, // LLM streaming responses can take a while to start
		MaxResponseHeaderBytes: 1 << 20,
		DisableCompression:     true, // pass bodies through untouched
	}
	if opts.ConnectorStore != nil {
		b.connectors[proto.EgressConnectorPackage] = connector.NewPackage(connector.PackageOptions{
			Transport: b.client, Store: opts.ConnectorStore,
		})
	}
	b.connectors[proto.EgressConnectorGit] = connector.NewGit(connector.GitOptions{Transport: b.client})
	return b
}

// Start listens and returns the base URL (http://127.0.0.1:PORT).
func (b *Broker) Start() (string, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.ln != nil {
		return "", errors.New("broker: already started")
	}
	security, err := proto.NormalizeSecurity(proto.SecuritySpec{
		Profile: proto.SecurityLocal, Network: b.opts.Network,
	})
	if err != nil {
		return "", fmt.Errorf("broker: network policy: %w", err)
	}
	b.opts.Network = security.Network
	for _, rule := range b.opts.Network.Rules {
		if rule.Connector != "" && b.connectors[rule.Connector] == nil {
			return "", fmt.Errorf("broker: connector %q required by rule %q is unavailable", rule.Connector, rule.ID)
		}
	}
	addr := b.opts.Listen
	if addr == "" {
		addr = "127.0.0.1:0"
	}
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return "", err
	}
	token := make([]byte, 32)
	if _, err := rand.Read(token); err != nil {
		_ = ln.Close()
		return "", err
	}
	limited := &limitedListener{
		Listener: ln, slots: make(chan struct{}, b.opts.MaxConnections), done: make(chan struct{}),
	}
	b.ln = limited
	b.token = base64.RawURLEncoding.EncodeToString(token)
	advertised := ln.Addr().String()
	if b.opts.AdvertiseHost != "" {
		_, port, splitErr := net.SplitHostPort(advertised)
		if splitErr != nil {
			_ = ln.Close()
			return "", splitErr
		}
		advertised = net.JoinHostPort(b.opts.AdvertiseHost, port)
	}
	b.advertised = advertised
	raw := "http://" + advertised
	b.base = raw + "/c/" + b.token
	// Explicit empty password: Bun (so every Bun-compiled harness such as
	// OpenCode) parses "http://tok@host:port" as the host "tok@host" and
	// dials that; "http://tok:@host:port" is read correctly by Bun, Node,
	// curl, Python and Go.
	proxyURL := &url.URL{Scheme: "http", Host: advertised, User: url.UserPassword(b.token, "")}
	b.proxyBase = proxyURL.String()
	b.srv = &http.Server{
		Handler: b, ReadHeaderTimeout: 30 * time.Second, IdleTimeout: 2 * time.Minute,
		MaxHeaderBytes: 1 << 20,
	}
	go func() { _ = b.srv.Serve(limited) }()
	return b.base, nil
}

// BaseURL returns the listener URL after Start.
func (b *Broker) BaseURL() string { return b.base }

// ProxyURL returns an authenticated URL suitable for HTTP_PROXY and
// HTTPS_PROXY. BaseURL is the capability-bearing reverse-proxy endpoint.
func (b *Broker) ProxyURL() string { return b.proxyBase }

// PackageURL is the capability-bearing base for managed package retrieval.
func (b *Broker) PackageURL() string { return strings.TrimSuffix(b.base, "/") + "/package" }

// GitURL returns the git connector root: <GitURL>/<host>/<owner>/<name>.git
// is what a workspace's insteadOf rewrites https://<host>/ to.
func (b *Broker) GitURL() string { return strings.TrimSuffix(b.base, "/") + "/git" }

// Close stops the listener.
func (b *Broker) Close() error {
	b.mu.Lock()
	srv := b.srv
	tunnels := b.takeTunnelsLocked()
	b.suspended = true
	b.mu.Unlock()
	closeBrokerTunnels(tunnels)
	if srv == nil {
		return nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	err := srv.Shutdown(ctx)
	b.client.CloseIdleConnections()
	return err
}

// SetLeases replaces the binding leases (renewal).
func (b *Broker) SetLeases(leases []proto.BindingLease) {
	b.mu.Lock()
	b.leases = cloneLeases(leases)
	b.mu.Unlock()
}

// Suspend fails every request closed while a workspace is quiesced for a
// release. Resume is used only after control durably aborts that release.
func (b *Broker) Suspend() {
	b.mu.Lock()
	b.suspended = true
	tunnels := b.takeTunnelsLocked()
	b.mu.Unlock()
	closeBrokerTunnels(tunnels)
}

func (b *Broker) Resume() {
	b.mu.Lock()
	b.suspended = false
	b.mu.Unlock()
}

func (b *Broker) takeTunnelsLocked() []*brokerTunnel {
	tunnels := make([]*brokerTunnel, 0, len(b.tunnels))
	for tunnel := range b.tunnels {
		tunnels = append(tunnels, tunnel)
		delete(b.tunnels, tunnel)
	}
	return tunnels
}

func closeBrokerTunnels(tunnels []*brokerTunnel) {
	for _, tunnel := range tunnels {
		_ = tunnel.downstream.Close()
		_ = tunnel.upstream.Close()
	}
}

// Placeholder is the string a workspace holds for a binding.
func Placeholder(l proto.BindingLease) string {
	if l.Placeholder != "" {
		return l.Placeholder
	}
	return "ref:" + l.ID
}

// EnvFor returns the environment variables a workspace should receive so
// harnesses reach their providers through the broker.
func (b *Broker) EnvFor() []string {
	noProxy := b.NoProxy()
	return []string{
		"REMOUNT_BROKER=" + b.base,
		"REMOUNT_PACKAGE_CONNECTOR=" + b.PackageURL(),
		"REMOUNT_GIT_CONNECTOR=" + b.GitURL(),
		"HTTP_PROXY=" + b.proxyBase,
		"HTTPS_PROXY=" + b.proxyBase,
		"http_proxy=" + b.proxyBase,
		"https_proxy=" + b.proxyBase,
		"NO_PROXY=" + noProxy,
		"no_proxy=" + noProxy,
	}
}

// NoProxy returns the NO_PROXY value a workspace needs so that a
// proxy-honoring client (curl, Node, Python, Go) reaches the broker's
// capability URLs directly. Without the advertised broker host in this list
// the client would forward-proxy a placeholder-bearing request to the broker
// itself, which the broker correctly records as a leak.
func (b *Broker) NoProxy() string {
	hosts := []string{"127.0.0.1", "localhost"}
	if host := b.AdvertisedHost(); host != "" && host != "127.0.0.1" && host != "localhost" {
		hosts = append(hosts, host)
	}
	return strings.Join(hosts, ",")
}

// AdvertisedHost is the host (without port) workspaces use to reach the
// broker. It is empty before Start.
func (b *Broker) AdvertisedHost() string {
	if b.advertised == "" {
		return ""
	}
	host, _, err := net.SplitHostPort(b.advertised)
	if err != nil {
		return b.advertised
	}
	return host
}

// ---------------------------------------------------------------------------
// request handling
// ---------------------------------------------------------------------------

func (b *Broker) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	select {
	case b.requestSlots <- struct{}{}:
		defer func() { <-b.requestSlots }()
	default:
		audit := b.auditFor(r.Method, "", r.Host, r.URL.Path)
		audit.Decision, audit.Reason = DecisionLimitExceeded, "concurrent request limit exhausted"
		b.emit(audit)
		http.Error(w, "remount broker: concurrent request limit exhausted", http.StatusTooManyRequests)
		return
	}
	b.mu.RLock()
	suspended := b.suspended
	b.mu.RUnlock()
	if suspended {
		audit := b.auditFor(r.Method, "", r.Host, r.URL.Path)
		audit.Decision, audit.Reason = DecisionDenied, "workspace is quiesced"
		b.emit(audit)
		http.Error(w, "remount broker: workspace is quiesced", http.StatusServiceUnavailable)
		return
	}
	proxyRequest := r.Method == http.MethodConnect || r.URL.IsAbs()
	if proxyRequest {
		if !b.validProxyAuthorization(r.Header.Get("Proxy-Authorization")) {
			b.rejectUnauthenticated(w, r, true)
			return
		}
	} else if !b.consumeCapabilityPath(r) {
		b.rejectUnauthenticated(w, r, false)
		return
	}
	if b.typedPolicyEnabled() && ambiguousPolicyPath(r.URL.EscapedPath()) {
		audit := b.auditFor(r.Method, "", r.Host, r.URL.Path)
		audit.Decision, audit.Reason = DecisionDenied, "ambiguous encoded path is not permitted by typed policy"
		b.emit(audit)
		http.Error(w, "remount broker: ambiguous encoded path", http.StatusBadRequest)
		return
	}
	switch {
	case r.Method == http.MethodConnect:
		b.handleConnect(w, r)
	case r.URL.IsAbs():
		// Forward-proxy form: GET http://host/path
		if r.URL.User != nil {
			http.Error(w, "remount broker: destination userinfo is not permitted", http.StatusBadRequest)
			return
		}
		if b.selfAddressed(r.URL) {
			// A client that ignored NO_PROXY forwarded a capability URL through
			// the proxy. Serve it as the direct request it was meant to be
			// rather than re-originating a placeholder to ourselves.
			b.serveSelfAddressed(w, r)
			return
		}
		b.proxy(w, r, r.URL.Scheme, r.URL.Host, r.URL.Path, r.URL.RawQuery)
	case strings.HasPrefix(r.URL.Path, "/d/"):
		host, rest := splitDest(strings.TrimPrefix(r.URL.Path, "/d/"))
		b.proxy(w, r, "https", host, rest, r.URL.RawQuery)
	case strings.HasPrefix(r.URL.Path, "/http/"):
		host, rest := splitDest(strings.TrimPrefix(r.URL.Path, "/http/"))
		b.proxy(w, r, "http", host, rest, r.URL.RawQuery)
	case strings.HasPrefix(r.URL.Path, "/package/"):
		host, rest := splitDest(strings.TrimPrefix(r.URL.Path, "/package/"))
		b.packageProxy(w, r, host, rest, r.URL.RawQuery)
	case strings.HasPrefix(r.URL.Path, "/git/"):
		host, rest := splitDest(strings.TrimPrefix(r.URL.Path, "/git/"))
		b.gitProxy(w, r, host, rest, r.URL.RawQuery)
	case r.URL.Path == "/healthz":
		w.WriteHeader(http.StatusOK)
		_, _ = io.WriteString(w, "ok\n")
	default:
		http.Error(w, "remount broker: use /d/<host>/<path>, /http/<host>/<path>, /package/<host>/<path>, /git/<host>/<owner>/<repo>.git/<endpoint>, or HTTP proxy mode", http.StatusNotFound)
	}
}

// selfAddressed reports whether an absolute proxy target names this broker's
// own advertised listener.
func (b *Broker) selfAddressed(u *url.URL) bool {
	if b.advertised == "" || u.Scheme != "http" {
		return false
	}
	host := u.Host
	if _, _, err := net.SplitHostPort(host); err != nil {
		host = net.JoinHostPort(host, "80")
	}
	return strings.EqualFold(host, b.advertised)
}

func (b *Broker) serveSelfAddressed(w http.ResponseWriter, r *http.Request) {
	r.URL.Scheme, r.URL.Host, r.URL.User = "", "", nil
	r.Header.Del("Proxy-Authorization")
	if !b.consumeCapabilityPath(r) {
		b.rejectUnauthenticated(w, r, false)
		return
	}
	switch {
	case strings.HasPrefix(r.URL.Path, "/d/"):
		host, rest := splitDest(strings.TrimPrefix(r.URL.Path, "/d/"))
		b.proxy(w, r, "https", host, rest, r.URL.RawQuery)
	case strings.HasPrefix(r.URL.Path, "/http/"):
		host, rest := splitDest(strings.TrimPrefix(r.URL.Path, "/http/"))
		b.proxy(w, r, "http", host, rest, r.URL.RawQuery)
	case strings.HasPrefix(r.URL.Path, "/package/"):
		host, rest := splitDest(strings.TrimPrefix(r.URL.Path, "/package/"))
		b.packageProxy(w, r, host, rest, r.URL.RawQuery)
	case strings.HasPrefix(r.URL.Path, "/git/"):
		host, rest := splitDest(strings.TrimPrefix(r.URL.Path, "/git/"))
		b.gitProxy(w, r, host, rest, r.URL.RawQuery)
	case r.URL.Path == "/healthz":
		w.WriteHeader(http.StatusOK)
		_, _ = io.WriteString(w, "ok\n")
	default:
		http.Error(w, "remount broker: use /d/<host>/<path>, /http/<host>/<path>, /package/<host>/<path>, /git/<host>/<owner>/<repo>.git/<endpoint>, or HTTP proxy mode", http.StatusNotFound)
	}
}

func (b *Broker) typedPolicyEnabled() bool {
	return b.opts.Network.Default != "" || len(b.opts.Network.Rules) != 0
}

func ambiguousPolicyPath(escapedPath string) bool {
	escapedPath = strings.ToLower(escapedPath)
	return strings.Contains(escapedPath, "\\") || strings.Contains(escapedPath, "%2f") ||
		strings.Contains(escapedPath, "%5c") || strings.Contains(escapedPath, "%2e") ||
		strings.Contains(escapedPath, "%25")
}

func (b *Broker) consumeCapabilityPath(r *http.Request) bool {
	prefix := "/c/" + b.token
	if !strings.HasPrefix(r.URL.Path, prefix) {
		return false
	}
	rest := strings.TrimPrefix(r.URL.Path, prefix)
	if rest != "" && !strings.HasPrefix(rest, "/") {
		return false
	}
	if rest == "" {
		rest = "/"
	}
	r.URL.Path = rest
	r.URL.RawPath = ""
	return true
}

func (b *Broker) validProxyAuthorization(value string) bool {
	if len(value) < 6 || !strings.EqualFold(value[:6], "basic ") {
		return false
	}
	raw, err := base64.StdEncoding.DecodeString(strings.TrimSpace(value[6:]))
	if err != nil {
		return false
	}
	got := string(raw)
	got = strings.TrimSuffix(got, ":")
	return subtle.ConstantTimeCompare([]byte(got), []byte(b.token)) == 1
}

func (b *Broker) rejectUnauthenticated(w http.ResponseWriter, r *http.Request, proxyRequest bool) {
	audit := b.auditFor(r.Method, "", r.Host, r.URL.Path)
	audit.Decision = DecisionUnauthenticated
	audit.Reason = "workspace broker capability missing or invalid"
	b.emit(audit)
	if proxyRequest {
		w.Header().Set("Proxy-Authenticate", `Basic realm="remount"`)
		http.Error(w, "remount broker: proxy authentication required", http.StatusProxyAuthRequired)
		return
	}
	http.Error(w, "remount broker: invalid workspace capability", http.StatusForbidden)
}

func splitDest(p string) (host, rest string) {
	i := strings.IndexByte(p, '/')
	if i < 0 {
		return p, "/"
	}
	return p[:i], p[i:]
}

func (b *Broker) auditFor(method, protocol, host, requestPath string) Audit {
	return Audit{
		At: time.Now(), WS: b.opts.WS, Generation: b.opts.Generation, Principal: b.opts.Principal,
		Host: host, Method: strings.ToUpper(method), Protocol: protocol, Path: requestPath,
	}
}

type policyAuthorization struct {
	enabled bool
	allowed bool
	rule    proto.EgressRule
	reason  string
}

var errResponseLimit = errors.New("response body exceeds rule limit")

type budgetReadCloser struct {
	io.ReadCloser
	remaining  int64
	exceeded   bool
	onExceeded func()
	once       sync.Once
}

func (r *budgetReadCloser) notifyExceeded() {
	if r.onExceeded != nil {
		r.once.Do(r.onExceeded)
	}
}

func (r *budgetReadCloser) Read(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	if r.exceeded {
		return 0, errResponseLimit
	}
	if r.remaining == 0 {
		var probe [1]byte
		n, err := r.ReadCloser.Read(probe[:])
		if n == 0 {
			return 0, err
		}
		r.exceeded = true
		r.notifyExceeded()
		return 0, errResponseLimit
	}
	limit := len(p)
	if int64(limit) > r.remaining {
		// The branch proves remaining < len(p), so +1 and int conversion
		// cannot overflow even when a policy uses math.MaxInt64.
		limit = int(r.remaining) + 1
	}
	n, err := r.ReadCloser.Read(p[:limit])
	if int64(n) <= r.remaining {
		r.remaining -= int64(n)
		return n, err
	}
	allowed := int(r.remaining)
	r.remaining = 0
	r.exceeded = true
	r.notifyExceeded()
	if allowed == 0 {
		return 0, errResponseLimit
	}
	return allowed, nil
}

func bufferRequestBody(body io.ReadCloser, limit int64) (io.ReadCloser, int64, error) {
	if body == nil || body == http.NoBody {
		return http.NoBody, 0, nil
	}
	defer body.Close()
	var buffered bytes.Buffer
	probeLimit := limit
	if limit < int64(^uint64(0)>>1) {
		probeLimit++
	}
	n, err := io.Copy(&buffered, io.LimitReader(body, probeLimit))
	if err != nil {
		return nil, n, err
	}
	if n > limit {
		return nil, n, errRequestLimit
	}
	return io.NopCloser(bytes.NewReader(buffered.Bytes())), n, nil
}

var errRequestLimit = errors.New("request body exceeds rule limit")

// authorizePolicy evaluates rules in declaration order and consumes a rule's
// request budget atomically. An explicit typed policy replaces the legacy
// node allow-list decision; it never falls through to that broader mechanism.
func (b *Broker) authorizePolicy(connectorName, protocol, host, method, requestPath string) policyAuthorization {
	policy := b.opts.Network
	if connectorName == "" && policy.Default == "" && len(policy.Rules) == 0 {
		return policyAuthorization{}
	}
	authorization := policyAuthorization{enabled: true}
	method = strings.ToUpper(method)
	requestPath = pathpkg.Clean("/" + strings.TrimPrefix(requestPath, "/"))
	port, portOK := destinationPort(host, protocol)
	for _, rule := range policy.Rules {
		if rule.Connector != connectorName || rule.Protocol != protocol || !hostMatches(host, rule.Hosts) || !portOK ||
			!containsPort(rule.Ports, port) || !containsStringFold(rule.Methods, method) ||
			!pathPrefixMatches(requestPath, rule.PathPrefixes) {
			continue
		}
		authorization.rule = rule
		if rule.SharedState == proto.SharedStateImmutableRead && method != http.MethodGet && method != http.MethodHead {
			authorization.reason = "immutable-read capability forbids mutating method"
			return authorization
		}
		b.mu.Lock()
		used := b.ruleRequests[rule.ID]
		if rule.MaxRequests > 0 && used >= rule.MaxRequests {
			b.mu.Unlock()
			authorization.reason = "request limit exhausted"
			return authorization
		}
		b.ruleRequests[rule.ID] = used + 1
		b.mu.Unlock()
		authorization.allowed = true
		return authorization
	}
	if connectorName != "" {
		authorization.reason = "no typed connector rule matched"
		return authorization
	}
	if policy.Default == proto.NetworkDefaultAllow {
		authorization.allowed = true
		return authorization
	}
	authorization.reason = "no typed egress rule matched"
	return authorization
}

func destinationPort(host, protocol string) (uint16, bool) {
	if _, rawPort, err := net.SplitHostPort(host); err == nil {
		port, err := strconv.ParseUint(rawPort, 10, 16)
		return uint16(port), err == nil && port != 0
	}
	switch protocol {
	case proto.EgressProtocolHTTP:
		return 80, true
	case proto.EgressProtocolHTTPS, proto.EgressProtocolConnect:
		return 443, true
	default:
		return 0, false
	}
}

// normalizeAuthority returns a lower-case, explicit host:port authority. It
// rejects userinfo, Unicode and malformed/ambiguous port spellings before the
// same value is used for both policy matching and the outbound dial.
func normalizeAuthority(raw, protocol string) (string, error) {
	if raw == "" || raw != strings.TrimSpace(raw) || strings.ContainsAny(raw, "/?#@\\\x00\t\r\n ") {
		return "", errors.New("invalid destination authority")
	}
	for _, r := range raw {
		if r > 0x7f {
			return "", errors.New("destination authority must be ASCII")
		}
	}
	host, rawPort := raw, ""
	if splitHost, splitPort, err := net.SplitHostPort(raw); err == nil {
		host, rawPort = splitHost, splitPort
	} else {
		if strings.HasPrefix(raw, "[") {
			if !strings.HasSuffix(raw, "]") {
				return "", errors.New("invalid bracketed destination authority")
			}
			host = strings.TrimSuffix(strings.TrimPrefix(raw, "["), "]")
		} else if strings.Count(raw, ":") == 1 {
			return "", errors.New("destination port must be numeric")
		}
	}
	if strings.HasSuffix(raw, ":") && rawPort == "" {
		return "", errors.New("destination port must not be empty")
	}
	host = strings.ToLower(strings.TrimSuffix(host, "."))
	if !validDestinationHost(host) {
		return "", errors.New("invalid destination host")
	}
	if rawPort == "" {
		port, ok := destinationPort(host, protocol)
		if !ok {
			return "", errors.New("destination has no valid port")
		}
		rawPort = strconv.Itoa(int(port))
	} else {
		port, err := strconv.ParseUint(rawPort, 10, 16)
		if err != nil || port == 0 {
			return "", errors.New("destination port must be between 1 and 65535")
		}
		rawPort = strconv.FormatUint(port, 10)
	}
	return net.JoinHostPort(host, rawPort), nil
}

func validDestinationHost(host string) bool {
	if host == "" || strings.Contains(host, "%") {
		return false
	}
	if ip := net.ParseIP(host); ip != nil {
		return true
	}
	if len(host) > 253 || strings.HasPrefix(host, ".") || strings.HasSuffix(host, ".") {
		return false
	}
	for _, label := range strings.Split(host, ".") {
		if label == "" || len(label) > 63 || label[0] == '-' || label[len(label)-1] == '-' {
			return false
		}
		for _, r := range label {
			if (r < 'a' || r > 'z') && (r < '0' || r > '9') && r != '-' && r != '_' {
				return false
			}
		}
	}
	return true
}

func containsPort(ports []uint16, port uint16) bool {
	if len(ports) == 0 {
		return true
	}
	for _, candidate := range ports {
		if candidate == port {
			return true
		}
	}
	return false
}

func containsStringFold(values []string, value string) bool {
	if len(values) == 0 {
		return true
	}
	for _, candidate := range values {
		if strings.EqualFold(candidate, value) {
			return true
		}
	}
	return false
}

func pathPrefixMatches(requestPath string, prefixes []string) bool {
	if len(prefixes) == 0 {
		return true
	}
	for _, prefix := range prefixes {
		if prefix == "/" || requestPath == prefix ||
			(strings.HasSuffix(prefix, "/") && strings.HasPrefix(requestPath, prefix)) ||
			strings.HasPrefix(requestPath, prefix+"/") {
			return true
		}
	}
	return false
}

type credentialRejection struct {
	decision string
	binding  string
	reason   string
	public   string
	status   int
}

// rewriteCredentials replaces exact placeholder tokens after binding the
// request to a destination. The caller must emit the returned rejection and
// must record every returned binding before attempting outbound I/O.
func (b *Broker) rewriteCredentials(header http.Header, scheme, host string) (map[string]bool, *credentialRejection) {
	b.mu.RLock()
	leases := append([]proto.BindingLease(nil), b.leases...)
	b.mu.RUnlock()
	sort.SliceStable(leases, func(i, j int) bool {
		return len(Placeholder(leases[i])) > len(Placeholder(leases[j]))
	})
	used := map[string]bool{}
	for name, vals := range header {
		for i, value := range vals {
			plain, basic := credentialText(value)
			var replacements []credentialReplacement
			for _, lease := range leases {
				placeholder := Placeholder(lease)
				if !containsToken(plain, placeholder) {
					continue
				}
				if !hostMatches(host, lease.Destinations) {
					return nil, &credentialRejection{
						decision: DecisionLeakBlocked, binding: lease.ID,
						reason: "placeholder for " + lease.ID + " sent to " + host,
						public: "credential " + lease.ID + " is not bound to " + host,
						status: http.StatusForbidden,
					}
				}
				if lease.ExpiresAt != 0 && time.Now().UnixMilli() > lease.ExpiresAt {
					return nil, &credentialRejection{
						decision: DecisionExpired, binding: lease.ID, reason: "lease expired",
						public: "lease for " + lease.ID + " expired; fail closed",
						status: http.StatusForbidden,
					}
				}
				if scheme != proto.EgressProtocolHTTPS {
					return nil, &credentialRejection{
						decision: DecisionDenied, binding: lease.ID,
						reason: "credential substitution requires a TLS upstream",
						public: "credentials are never sent over plaintext HTTP",
						status: http.StatusForbidden,
					}
				}
				replacements = append(replacements, credentialReplacement{placeholder: placeholder, secret: lease.Secret})
				used[lease.ID] = true
			}
			if len(replacements) > 0 {
				rewritten := substituteAll(plain, replacements)
				if basic {
					rewritten = "Basic " + base64.StdEncoding.EncodeToString([]byte(rewritten))
				}
				vals[i] = rewritten
			}
		}
		header[name] = vals
	}
	return used, nil
}

// packageProxy invokes the managed package connector. Connector-scoped rules
// cannot be exercised through proxy(), and generic rules cannot reach here.
func (b *Broker) packageProxy(w http.ResponseWriter, r *http.Request, host, requestPath, query string) {
	audit := b.auditFor(r.Method, proto.EgressProtocolHTTPS, host, requestPath)
	audit.Connector = proto.EgressConnectorPackage
	if r.ContentLength > 0 || len(r.TransferEncoding) != 0 {
		audit.Decision, audit.Reason = DecisionDenied, "package connector requests cannot carry a body"
		b.emit(audit)
		http.Error(w, "remount broker: "+audit.Reason, http.StatusBadRequest)
		return
	}
	authority, err := normalizeAuthority(host, proto.EgressProtocolHTTPS)
	if err != nil {
		audit.Decision, audit.Reason = DecisionDenied, err.Error()
		b.emit(audit)
		http.Error(w, "remount broker: "+err.Error(), http.StatusBadRequest)
		return
	}
	audit.Host = authority
	policy := b.authorizePolicy(proto.EgressConnectorPackage, proto.EgressProtocolHTTPS, authority, r.Method, requestPath)
	if policy.rule.ID != "" {
		audit.Rule, audit.SharedState = policy.rule.ID, policy.rule.SharedState
	}
	if !policy.allowed {
		audit.Decision, audit.Reason = DecisionDenied, policy.reason
		status := http.StatusForbidden
		if policy.reason == "request limit exhausted" {
			audit.Decision, status = DecisionLimitExceeded, http.StatusTooManyRequests
		}
		b.emit(audit)
		http.Error(w, "remount broker: "+policy.reason, status)
		return
	}
	managed := b.connectors[proto.EgressConnectorPackage]
	if managed == nil {
		audit.Decision, audit.Reason = DecisionDenied, "package connector is unavailable"
		b.emit(audit)
		http.Error(w, "remount broker: "+audit.Reason, http.StatusServiceUnavailable)
		return
	}
	used, rejected := b.rewriteCredentials(r.Header, proto.EgressProtocolHTTPS, authority)
	if rejected != nil {
		audit.Decision, audit.Binding, audit.Reason = rejected.decision, rejected.binding, rejected.reason
		b.emit(audit)
		http.Error(w, "remount broker: "+rejected.public, rejected.status)
		return
	}
	credUse := b.credentialUse(audit, used, "credential released to managed package connector")
	target := &url.URL{Scheme: proto.EgressProtocolHTTPS, Host: authority, Path: requestPath, RawQuery: query}
	response, err := managed.Execute(r.Context(), connector.ConnectorRequest{
		Workspace: b.opts.WS, Tenant: b.opts.Tenant, Principal: b.opts.Principal,
		Generation: b.opts.Generation, Rule: policy.rule, Method: strings.ToUpper(r.Method),
		URL: target, Header: r.Header, ExpectedDigest: r.Header.Get(connector.ExpectedDigestHeader),
	})
	if err != nil {
		status := http.StatusBadGateway
		audit.Decision, audit.Reason = DecisionDenied, "package connector request failed"
		var connectorErr *connector.Error
		if errors.As(err, &connectorErr) {
			if connectorErr.HTTPStatus != 0 {
				status = connectorErr.HTTPStatus
			}
			audit.Reason = connectorErr.Error()
			if connectorErr.Code == "resource_exhausted" || connectorErr.Code == "response_too_large" {
				audit.Decision = DecisionLimitExceeded
			}
		}
		credUse(0, ErrorClassConnector)
		b.emit(audit)
		http.Error(w, "remount broker: "+audit.Reason, status)
		return
	}
	defer response.Body.Close()
	audit.Status = response.StatusCode
	credUse(response.StatusCode, "")
	audit.ResponseBytes = response.ContentLength
	if response.Provenance.SHA256 != "" {
		audit.Digest = "sha256:" + response.Provenance.SHA256
	}
	audit.Cached = response.Provenance.Cached
	if err := b.rewritePackageRedirect(response.Header, target); err != nil {
		audit.Decision, audit.Reason = DecisionDenied, err.Error()
		b.emit(audit)
		http.Error(w, "remount broker: "+audit.Reason, http.StatusBadGateway)
		return
	}
	for name, values := range response.Header {
		for _, value := range values {
			w.Header().Add(name, value)
		}
	}
	w.WriteHeader(response.StatusCode)
	audit.Decision, audit.Reason = DecisionAllowed, "managed package retrieval"
	b.emit(audit)
	if r.Method != http.MethodHead {
		_, _ = io.Copy(w, response.Body)
	}
}

// hasTypedGitRule reports whether the workspace's policy speaks to git at
// all. When it does, it is authoritative for the /git/ surface; when it does
// not, the declared repository alone decides, whatever else the policy says
// about model hosts or package registries.
func (b *Broker) hasTypedGitRule() bool {
	for _, rule := range b.opts.Network.Rules {
		if rule.Connector == proto.EgressConnectorGit {
			return true
		}
	}
	return false
}

// implicitGitRule is the rule the /git/ surface uses when the workspace has
// no typed git rule: exactly the declared repository, fetch and push.
func (b *Broker) implicitGitRule(authority string) (proto.EgressRule, bool) {
	if b.opts.Repo.URL == "" {
		return proto.EgressRule{}, false
	}
	_, host, ownerRepo, err := proto.ParseRepoURL(b.opts.Repo.URL)
	if err != nil || !hostMatches(authority, []string{host}) {
		return proto.EgressRule{}, false
	}
	return proto.EgressRule{
		ID: "repo", Connector: proto.EgressConnectorGit, Protocol: proto.EgressProtocolHTTPS,
		Hosts: []string{host}, Repos: []string{ownerRepo}, Push: true,
		Methods: []string{"GET", "POST"}, SharedState: proto.SharedStateNone,
	}, true
}

// gitProxy serves the managed git connector: /git/<host>/<owner>/<name>.git/
// <smart-http endpoint>. The connector decides what a git request is; this
// method decides which rule (if any) covers the host and releases credentials.
func (b *Broker) gitProxy(w http.ResponseWriter, r *http.Request, host, requestPath, query string) {
	audit := b.auditFor(r.Method, proto.EgressProtocolHTTPS, host, requestPath)
	audit.Connector = proto.EgressConnectorGit
	fail := func(decision, reason string, status int) {
		audit.Decision, audit.Reason = decision, reason
		b.emit(audit)
		http.Error(w, "remount broker: "+reason, status)
	}
	if ambiguousPolicyPath(r.URL.EscapedPath()) {
		fail(DecisionDenied, "ambiguous encoded path is not a git endpoint", http.StatusBadRequest)
		return
	}
	authority, err := normalizeAuthority(host, proto.EgressProtocolHTTPS)
	if err != nil {
		fail(DecisionDenied, err.Error(), http.StatusBadRequest)
		return
	}
	audit.Host = authority
	var rule proto.EgressRule
	if b.hasTypedGitRule() {
		policy := b.authorizePolicy(proto.EgressConnectorGit, proto.EgressProtocolHTTPS, authority, r.Method, requestPath)
		if policy.rule.ID != "" {
			audit.Rule, audit.SharedState = policy.rule.ID, policy.rule.SharedState
		}
		if !policy.allowed {
			status := http.StatusForbidden
			decision := DecisionDenied
			if policy.reason == "request limit exhausted" {
				decision, status = DecisionLimitExceeded, http.StatusTooManyRequests
			}
			fail(decision, policy.reason, status)
			return
		}
		rule = policy.rule
	} else {
		implicit, ok := b.implicitGitRule(authority)
		if !ok {
			fail(DecisionDenied, "no repository is declared for this host; create the workspace with --repo or add a git connector rule", http.StatusForbidden)
			return
		}
		rule = implicit
		audit.Rule, audit.SharedState = rule.ID, rule.SharedState
	}
	managed := b.connectors[proto.EgressConnectorGit]
	if managed == nil {
		fail(DecisionDenied, "git connector is unavailable", http.StatusServiceUnavailable)
		return
	}
	target := &url.URL{Scheme: proto.EgressProtocolHTTPS, Host: authority, Path: requestPath, RawQuery: query}
	request := connector.ConnectorRequest{
		Workspace: b.opts.WS, Tenant: b.opts.Tenant, Principal: b.opts.Principal,
		Generation: b.opts.Generation, Rule: rule, Method: strings.ToUpper(r.Method),
		URL: target, Header: r.Header, Body: r.Body, ContentLength: r.ContentLength,
	}
	// Authorize before releasing a credential: a request for anything but
	// the covered repository never sees the token, even in an audit.
	decision, err := managed.Authorize(r.Context(), request)
	if err != nil {
		fail(DecisionDenied, "git connector authorization failed", http.StatusInternalServerError)
		return
	}
	audit.Op, audit.Repo = decision.Operation, decision.Resource
	if !decision.Allowed {
		fail(DecisionDenied, decision.Reason, http.StatusForbidden)
		return
	}
	used, rejected := b.rewriteCredentials(r.Header, proto.EgressProtocolHTTPS, authority)
	if rejected != nil {
		audit.Binding = rejected.binding
		fail(rejected.decision, rejected.reason, rejected.status)
		return
	}
	credUse := b.credentialUse(audit, used, "credential released to managed git connector")
	response, err := managed.Execute(r.Context(), request)
	if err != nil {
		status := http.StatusBadGateway
		audit.Decision, audit.Reason = DecisionDenied, "git connector request failed"
		var connectorErr *connector.Error
		if errors.As(err, &connectorErr) {
			if connectorErr.HTTPStatus != 0 {
				status = connectorErr.HTTPStatus
			}
			audit.Reason = connectorErr.Error()
			if connectorErr.Code == "resource_exhausted" || connectorErr.Code == "response_too_large" {
				audit.Decision = DecisionLimitExceeded
			}
		}
		credUse(0, ErrorClassConnector)
		b.emit(audit)
		http.Error(w, "remount broker: "+audit.Reason, status)
		return
	}
	defer response.Body.Close()
	audit.Status = response.StatusCode
	audit.RequestBytes = r.ContentLength
	credUse(response.StatusCode, "")
	for name, values := range response.Header {
		for _, value := range values {
			w.Header().Add(name, value)
		}
	}
	w.WriteHeader(response.StatusCode)
	audit.Decision, audit.Reason = DecisionAllowed, "managed git "+decision.Operation
	b.emit(audit)
	n, copyErr := io.Copy(&flushWriter{w: w}, response.Body)
	if copyErr != nil {
		a := audit
		a.Decision, a.Reason, a.ResponseBytes = DecisionLimitExceeded, copyErr.Error(), n
		if !strings.Contains(copyErr.Error(), "exceeds rule limit") {
			a.Decision, a.Reason = DecisionDenied, "upstream stream failed: "+copyErr.Error()
		}
		b.emit(a)
	}
}

// flushWriter flushes after every write so a pack negotiation round trip is
// not held back by buffering.
type flushWriter struct{ w http.ResponseWriter }

func (f *flushWriter) Write(p []byte) (int, error) {
	n, err := f.w.Write(p)
	if flusher, ok := f.w.(http.Flusher); ok && err == nil {
		flusher.Flush()
	}
	return n, err
}

func (b *Broker) rewritePackageRedirect(header http.Header, source *url.URL) error {
	raw := header.Get("Location")
	if raw == "" {
		return nil
	}
	reference, err := url.Parse(raw)
	if err != nil {
		return fmt.Errorf("invalid package registry redirect: %w", err)
	}
	destination := source.ResolveReference(reference)
	if destination.Scheme != proto.EgressProtocolHTTPS || destination.Host == "" || destination.User != nil {
		return errors.New("package registry redirect must target an HTTPS registry")
	}
	authority, err := normalizeAuthority(destination.Host, proto.EgressProtocolHTTPS)
	if err != nil {
		return fmt.Errorf("invalid package registry redirect: %w", err)
	}
	rewritten := strings.TrimSuffix(b.base, "/") + "/package/" + authority + destination.EscapedPath()
	if destination.RawQuery != "" {
		rewritten += "?" + destination.RawQuery
	}
	header.Set("Location", rewritten)
	return nil
}

// proxy rewrites and forwards one request.
func (b *Broker) proxy(w http.ResponseWriter, r *http.Request, scheme, host, path, query string) {
	scheme = strings.ToLower(scheme)
	audit := b.auditFor(r.Method, scheme, host, path)
	if scheme != proto.EgressProtocolHTTP && scheme != proto.EgressProtocolHTTPS {
		audit.Decision, audit.Reason = DecisionDenied, "invalid destination scheme"
		b.emit(audit)
		http.Error(w, "remount broker: invalid destination", http.StatusBadRequest)
		return
	}
	authority, err := normalizeAuthority(host, scheme)
	if err != nil {
		audit.Decision, audit.Reason = DecisionDenied, err.Error()
		b.emit(audit)
		http.Error(w, "remount broker: "+err.Error(), http.StatusBadRequest)
		return
	}
	host = authority
	matchHost := authority
	audit.Host = authority
	// 1. Substitute placeholders; block any placeholder aimed elsewhere.
	used, rejected := b.rewriteCredentials(r.Header, scheme, matchHost)
	if rejected != nil {
		audit.Decision, audit.Binding, audit.Reason = rejected.decision, rejected.binding, rejected.reason
		b.emit(audit)
		http.Error(w, "remount broker: "+rejected.public, rejected.status)
		return
	}
	// 2. Destination policy. Typed workspace rules replace the legacy
	// binding/node allow-list decision and are evaluated on every request.
	policy := b.authorizePolicy("", scheme, matchHost, r.Method, path)
	if policy.rule.ID != "" {
		audit.Rule = policy.rule.ID
		audit.SharedState = policy.rule.SharedState
	}
	if policy.enabled && !policy.allowed {
		audit.Decision, audit.Reason = DecisionDenied, policy.reason
		status := http.StatusForbidden
		if policy.reason == "request limit exhausted" {
			audit.Decision = DecisionLimitExceeded
			status = http.StatusTooManyRequests
		}
		b.emit(audit)
		http.Error(w, "remount broker: "+policy.reason, status)
		return
	}
	allowed := policy.allowed
	if !policy.enabled {
		allowed = len(used) > 0 || hostMatches(matchHost, b.opts.Allow)
	}
	if !allowed {
		audit.Decision, audit.Reason = DecisionDenied, "destination not in bindings or allow list"
		b.emit(audit)
		http.Error(w, "remount broker: egress to "+host+" is not permitted for this workspace", http.StatusForbidden)
		return
	}
	if policy.rule.MaxRequestBytes > 0 {
		if r.ContentLength > policy.rule.MaxRequestBytes {
			audit.RequestBytes = r.ContentLength
			audit.Decision, audit.Reason = DecisionLimitExceeded, "request body exceeds rule limit"
			b.emit(audit)
			http.Error(w, "remount broker: request body exceeds rule limit", http.StatusRequestEntityTooLarge)
			return
		}
		body, bodyBytes, bodyErr := bufferRequestBody(r.Body, policy.rule.MaxRequestBytes)
		audit.RequestBytes = bodyBytes
		if bodyErr != nil {
			if errors.Is(bodyErr, errRequestLimit) {
				audit.Decision, audit.Reason = DecisionLimitExceeded, errRequestLimit.Error()
				b.emit(audit)
				http.Error(w, "remount broker: "+errRequestLimit.Error(), http.StatusRequestEntityTooLarge)
				return
			}
			audit.Decision, audit.Reason = DecisionDenied, "read request body: "+bodyErr.Error()
			b.emit(audit)
			http.Error(w, "remount broker: invalid request body", http.StatusBadRequest)
			return
		}
		r.Body = body
		r.ContentLength = bodyBytes
		r.GetBody = nil
	}
	credUse := b.credentialUse(audit, used, "credential released to outbound transport")
	// 3. Forward.
	defer credUse(0, ErrorClassOther) // a handler path that produced neither headers nor an error
	target := &url.URL{Scheme: scheme, Host: authority}
	rp := &httputil.ReverseProxy{
		Rewrite: func(pr *httputil.ProxyRequest) {
			pr.Out.URL.Scheme = target.Scheme
			pr.Out.URL.Host = target.Host
			pr.Out.URL.Path = path
			pr.Out.URL.RawPath = ""
			pr.Out.URL.RawQuery = query
			pr.Out.Host = host
			pr.Out.Header.Del("Proxy-Authorization")
			pr.Out.Header.Del("Proxy-Connection")
			// Do not advertise the workspace to the upstream.
			pr.Out.Header.Del("X-Forwarded-For")
			pr.Out.Header.Del("X-Forwarded-Host")
			pr.Out.Header.Del("X-Forwarded-Proto")
		},
		Transport:     b.client,
		FlushInterval: -1, // stream SSE / chunked LLM responses immediately
		ErrorHandler: func(w http.ResponseWriter, r *http.Request, err error) {
			emit := true
			if errors.Is(err, errResponseLimit) {
				audit.Decision, audit.Reason, audit.Status = DecisionLimitExceeded, errResponseLimit.Error(), http.StatusBadGateway
				emit = false // ModifyResponse recorded the rejected response.
			} else if errors.Is(err, errRedirectRejected) {
				emit = false // ModifyResponse recorded the rejected redirect.
			} else {
				audit.Decision, audit.Reason, audit.Status = DecisionDenied, "upstream: "+err.Error(), http.StatusBadGateway
				credUse(0, classifyUpstreamError(err))
			}
			if emit {
				b.emit(audit)
			}
			http.Error(w, "remount broker: "+audit.Reason, http.StatusBadGateway)
		},
		ModifyResponse: func(resp *http.Response) error {
			audit.Status = resp.StatusCode
			if err := b.rewriteRedirect(resp); err != nil {
				credUse(resp.StatusCode, ErrorClassRedirect)
				audit.Decision, audit.Reason = DecisionDenied, err.Error()
				b.emit(audit)
				return fmt.Errorf("%w: %v", errRedirectRejected, err)
			}
			if policy.rule.MaxResponseBytes > 0 {
				audit.ResponseBytes = resp.ContentLength
				if resp.ContentLength > policy.rule.MaxResponseBytes {
					resp.Body.Close()
					credUse(resp.StatusCode, ErrorClassLimit)
					audit.Decision, audit.Reason = DecisionLimitExceeded, "response body exceeds rule limit"
					b.emit(audit)
					return errResponseLimit
				}
				resp.Body = &budgetReadCloser{
					ReadCloser: resp.Body, remaining: policy.rule.MaxResponseBytes,
					onExceeded: func() {
						a := audit
						a.Decision, a.Reason = DecisionLimitExceeded, "response body exceeds rule limit"
						a.ResponseBytes = policy.rule.MaxResponseBytes + 1
						b.emit(a)
					},
				}
			}
			credUse(resp.StatusCode, "")
			a := audit
			a.Decision = DecisionAllowed
			b.emit(a)
			return nil
		},
	}
	rp.ServeHTTP(w, r)
}

var errRedirectRejected = errors.New("upstream redirect rejected")

// credentialUse returns the one-shot recorder for the `substituted` audits of
// a request. cred.used is the record of what a released credential bought,
// so it is emitted once the outcome is known: the upstream status when
// response headers arrived, or status 0 and a failure class when they did
// not. Exactly one audit per released binding, whichever path fires first.
func (b *Broker) credentialUse(audit Audit, used map[string]bool, reason string) func(status int, class string) {
	if len(used) == 0 {
		return func(int, string) {}
	}
	var once sync.Once
	return func(status int, class string) {
		once.Do(func() {
			for id := range used {
				a := audit
				a.Decision, a.Binding, a.Reason = DecisionSubstituted, id, reason
				a.Status, a.Error = status, class
				b.emit(a)
			}
		})
	}
}

// classifyUpstreamError maps a transport failure to an ErrorClass.
func classifyUpstreamError(err error) string {
	var dnsErr *net.DNSError
	switch {
	case err == nil:
		return ""
	case errors.Is(err, context.Canceled):
		return ErrorClassCanceled
	case errors.Is(err, context.DeadlineExceeded), errors.Is(err, os.ErrDeadlineExceeded):
		return ErrorClassTimeout
	case errors.As(err, &dnsErr):
		return ErrorClassDNS
	case errors.Is(err, syscall.ECONNREFUSED):
		return ErrorClassRefused
	case errors.Is(err, syscall.ECONNRESET), errors.Is(err, syscall.EPIPE):
		return ErrorClassReset
	case errors.Is(err, io.EOF), errors.Is(err, io.ErrUnexpectedEOF):
		return ErrorClassEOF
	}
	var tlsRecord tls.RecordHeaderError
	var tlsAlert tls.AlertError
	var certErr *tls.CertificateVerificationError
	if errors.As(err, &tlsRecord) || errors.As(err, &tlsAlert) || errors.As(err, &certErr) {
		return ErrorClassTLS
	}
	var netErr net.Error
	if errors.As(err, &netErr) && netErr.Timeout() {
		return ErrorClassTimeout
	}
	if strings.Contains(err.Error(), "non-public address") {
		return ErrorClassPrivate
	}
	return ErrorClassOther
}

// rewriteRedirect keeps every hop on the capability-bearing broker URL. An
// absolute or scheme-relative Location would otherwise let a reverse-proxy
// client follow the next hop directly and skip destination policy.
func (b *Broker) rewriteRedirect(resp *http.Response) error {
	if resp.StatusCode < 300 || resp.StatusCode >= 400 || resp.Header.Get("Location") == "" {
		return nil
	}
	destination, err := resp.Location()
	if err != nil {
		return fmt.Errorf("invalid upstream redirect: %w", err)
	}
	if destination.Scheme != "http" && destination.Scheme != "https" {
		return fmt.Errorf("upstream redirect uses unsupported scheme %q", destination.Scheme)
	}
	if destination.Host == "" || destination.User != nil {
		return errors.New("upstream redirect has an invalid authority")
	}
	prefix := "/d/"
	if destination.Scheme == "http" {
		prefix = "/http/"
	}
	raw := strings.TrimSuffix(b.base, "/") + prefix + destination.Host + destination.EscapedPath()
	if destination.RawQuery != "" {
		raw += "?" + destination.RawQuery
	}
	if destination.Fragment != "" {
		raw += "#" + destination.Fragment
	}
	resp.Header.Set("Location", raw)
	return nil
}

// handleConnect tunnels TCP to a permitted host without inspection.
func (b *Broker) handleConnect(w http.ResponseWriter, r *http.Request) {
	audit := b.auditFor(r.Method, proto.EgressProtocolConnect, r.Host, "")
	host, err := normalizeAuthority(r.Host, proto.EgressProtocolConnect)
	if err != nil {
		audit.Decision, audit.Reason = DecisionDenied, err.Error()
		b.emit(audit)
		http.Error(w, "remount broker: "+err.Error(), http.StatusBadRequest)
		return
	}
	audit.Host = host
	policy := b.authorizePolicy("", proto.EgressProtocolConnect, host, r.Method, "")
	if policy.rule.ID != "" {
		audit.Rule = policy.rule.ID
		audit.SharedState = policy.rule.SharedState
	}
	if policy.enabled && !policy.allowed {
		audit.Decision, audit.Reason = DecisionDenied, policy.reason
		status := http.StatusForbidden
		if policy.reason == "request limit exhausted" {
			audit.Decision = DecisionLimitExceeded
			status = http.StatusTooManyRequests
		}
		b.emit(audit)
		http.Error(w, "remount broker: "+policy.reason, status)
		return
	}
	allowed := policy.allowed
	if !policy.enabled {
		// A credential binding authorizes substitution, not an opaque TCP
		// tunnel. Legacy/local mode still requires the node's explicit allow
		// list; typed policies require a CONNECT rule.
		allowed = hostMatches(host, b.opts.Allow)
	}
	if !allowed {
		audit.Decision, audit.Reason = DecisionDenied, "CONNECT requires an explicit allow or typed CONNECT rule"
		b.emit(audit)
		http.Error(w, "remount broker: CONNECT to "+host+" is not permitted", http.StatusForbidden)
		return
	}
	dialer := &net.Dialer{Timeout: 15 * time.Second}
	up, err := b.dial(r.Context(), dialer, "tcp", host)
	if err != nil {
		audit.Decision, audit.Reason = DecisionDenied, "dial: "+err.Error()
		b.emit(audit)
		http.Error(w, "remount broker: "+err.Error(), http.StatusBadGateway)
		return
	}
	hj, ok := w.(http.Hijacker)
	if !ok {
		up.Close()
		http.Error(w, "hijack unsupported", http.StatusInternalServerError)
		return
	}
	down, buf, err := hj.Hijack()
	if err != nil {
		up.Close()
		return
	}
	if _, err := buf.WriteString("HTTP/1.1 200 Connection Established\r\n\r\n"); err != nil {
		_ = down.Close()
		_ = up.Close()
		return
	}
	if err := buf.Flush(); err != nil {
		_ = down.Close()
		_ = up.Close()
		return
	}
	tunnel := &brokerTunnel{downstream: down, upstream: up}
	b.mu.Lock()
	if b.suspended {
		b.mu.Unlock()
		_ = down.Close()
		_ = up.Close()
		return
	}
	b.tunnels[tunnel] = struct{}{}
	b.mu.Unlock()
	audit.Decision = DecisionAllowed
	b.emit(audit)
	defer func() {
		b.mu.Lock()
		delete(b.tunnels, tunnel)
		b.mu.Unlock()
		_ = up.Close()
		_ = down.Close()
	}()
	go func() {
		if buf.Reader.Buffered() > 0 {
			_, _ = io.CopyN(up, buf, int64(buf.Reader.Buffered()))
		}
		_, _ = io.Copy(up, down)
		_ = up.Close()
		_ = down.Close()
	}()
	_, _ = io.Copy(down, up)
}

// dial resolves and connects, refusing private/loopback/link-local targets
// unless the host is explicitly allowed to be private.
func (b *Broker) dial(ctx context.Context, d *net.Dialer, network, addr string) (net.Conn, error) {
	host, port, err := net.SplitHostPort(addr)
	if err != nil {
		return nil, err
	}
	private := hostMatches(strings.ToLower(host), b.opts.AllowPrivate)
	ips, err := net.DefaultResolver.LookupNetIP(ctx, "ip", host)
	if err != nil {
		return nil, err
	}
	var lastErr error
	for _, ip := range ips {
		ip = ip.Unmap()
		if !private && isPrivate(ip) {
			lastErr = fmt.Errorf("refusing to connect %s to non-public address %s", host, ip)
			continue
		}
		// Pin: dial the literal address we validated, not the name.
		c, err := d.DialContext(ctx, network, net.JoinHostPort(ip.String(), port))
		if err == nil {
			return c, nil
		}
		lastErr = err
	}
	if lastErr == nil {
		lastErr = errors.New("no addresses")
	}
	return nil, lastErr
}

func isPrivate(ip netip.Addr) bool {
	if !ip.IsValid() || !ip.IsGlobalUnicast() || ip.IsLoopback() || ip.IsPrivate() ||
		ip.IsLinkLocalUnicast() || ip.IsLinkLocalMulticast() || ip.IsMulticast() ||
		ip.IsUnspecified() || ip.IsInterfaceLocalMulticast() {
		return true
	}
	for _, prefix := range nonPublicPrefixes {
		if prefix.Contains(ip) {
			return true
		}
	}
	return false
}

var nonPublicPrefixes = []netip.Prefix{
	netip.MustParsePrefix("0.0.0.0/8"),
	netip.MustParsePrefix("100.64.0.0/10"), // carrier-grade NAT; includes Alibaba metadata
	netip.MustParsePrefix("192.0.0.0/24"),
	netip.MustParsePrefix("192.0.2.0/24"),
	netip.MustParsePrefix("198.18.0.0/15"),
	netip.MustParsePrefix("198.51.100.0/24"),
	netip.MustParsePrefix("203.0.113.0/24"),
	netip.MustParsePrefix("240.0.0.0/4"),
	netip.MustParsePrefix("2001:db8::/32"),
}

func (b *Broker) emit(a Audit) {
	switch a.Decision {
	case DecisionSubstituted:
		metrics.CredUsed.Inc()
	case DecisionAllowed:
		metrics.EgressAllow.Inc()
	case DecisionLeakBlocked, DecisionUnauthenticated:
		metrics.LeakBlocked.Inc()
		metrics.EgressDeny.Inc()
	case DecisionExpired:
		metrics.LeaseExpired.Inc()
		metrics.EgressDeny.Inc()
	case DecisionDenied, DecisionLimitExceeded:
		metrics.EgressDeny.Inc()
	}
	if b.opts.Audit != nil {
		b.opts.Audit(a)
	}
}

// hostMatches reports whether host (optionally host:port, case-insensitive)
// matches any pattern. Patterns: "api.github.com", "*.github.com" (subdomains
// only), "*" (anything), each optionally with ":port". A pattern with a port
// matches only that port; a pattern without one matches any port.
func hostMatches(host string, patterns []string) bool {
	return proto.HostMatchesAny(host, patterns)
}

type credentialReplacement struct {
	placeholder string
	secret      string
}

func credentialText(value string) (string, bool) {
	if decoded, ok := decodeBasic(value); ok {
		return decoded, true
	}
	return value, false
}

// containsToken matches a placeholder as a complete credential token. This
// prevents ref:b_a from matching ref:b_ab while still allowing common forms
// such as "Bearer <placeholder>" and Basic's "username:<placeholder>".
func containsToken(value, placeholder string) bool {
	for offset := 0; offset <= len(value)-len(placeholder); {
		i := strings.Index(value[offset:], placeholder)
		if i < 0 {
			return false
		}
		i += offset
		leftOK := i == 0 || !credentialTokenByte(value[i-1]) || value[i-1] == ':'
		end := i + len(placeholder)
		rightOK := end == len(value) || !credentialTokenByte(value[end])
		if leftOK && rightOK {
			return true
		}
		offset = i + 1
	}
	return false
}

func credentialTokenByte(b byte) bool {
	return b >= 'a' && b <= 'z' || b >= 'A' && b <= 'Z' || b >= '0' && b <= '9' || b == '_' || b == '-' || b == '.'
}

// substituteAll performs a single non-cascading pass over the workspace's
// original header. A real secret that happens to contain another placeholder
// is therefore never rewritten a second time.
func substituteAll(value string, replacements []credentialReplacement) string {
	var out strings.Builder
	for offset := 0; offset < len(value); {
		matched := false
		for _, replacement := range replacements {
			ph := replacement.placeholder
			if !strings.HasPrefix(value[offset:], ph) {
				continue
			}
			leftOK := offset == 0 || !credentialTokenByte(value[offset-1]) || value[offset-1] == ':'
			end := offset + len(ph)
			rightOK := end == len(value) || !credentialTokenByte(value[end])
			if !leftOK || !rightOK {
				continue
			}
			out.WriteString(replacement.secret)
			offset = end
			matched = true
			break
		}
		if !matched {
			out.WriteByte(value[offset])
			offset++
		}
	}
	return out.String()
}

func decodeBasic(value string) (string, bool) {
	if len(value) < 6 || !strings.EqualFold(value[:6], "basic ") {
		return "", false
	}
	raw, err := base64.StdEncoding.DecodeString(strings.TrimSpace(value[6:]))
	if err != nil {
		return "", false
	}
	return string(raw), true
}

// ---------------------------------------------------------------------------
// helpers for callers
// ---------------------------------------------------------------------------

// ResolveEnv rewrites env values for a workspace: "ref:<binding>" becomes the
// binding's placeholder, and "${REMOUNT_BROKER}" the broker base URL. Values
// that reference an unknown binding are left as-is (the broker will block
// them if they ever reach a request, which is the safe failure).
func ResolveEnv(env map[string]string, base string, leases []proto.BindingLease) map[string]string {
	ph := map[string]string{}
	for _, l := range leases {
		ph["ref:"+l.ID] = Placeholder(l)
	}
	out := make(map[string]string, len(env))
	for k, v := range env {
		if p, ok := ph[v]; ok {
			v = p
		}
		v = strings.ReplaceAll(v, "${REMOUNT_BROKER}", base)
		v = strings.ReplaceAll(v, "${REMOUNT_PACKAGE_CONNECTOR}", strings.TrimSuffix(base, "/")+"/package")
		out[k] = v
	}
	return out
}

// DestURL builds the reverse-proxy URL for a host: base + "/d/" + host.
func DestURL(base, host string) string {
	return strings.TrimSuffix(base, "/") + "/d/" + host
}

// PortOf extracts the listener port (for docs/tests).
func (b *Broker) PortOf() int {
	if b.ln == nil {
		return 0
	}
	_, p, _ := net.SplitHostPort(b.ln.Addr().String())
	n, _ := strconv.Atoi(p)
	return n
}
