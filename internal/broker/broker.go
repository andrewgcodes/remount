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
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

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
)

// Audit is one broker decision.
type Audit struct {
	At        time.Time
	WS        string
	Principal string
	Decision  string
	Binding   string // binding id when a placeholder was involved
	Host      string
	Method    string
	Path      string
	Reason    string
	Status    int // upstream status when known
}

// Options configure a per-workspace broker.
type Options struct {
	WS        string
	Principal string
	Leases    []proto.BindingLease
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
}

// Broker serves one workspace.
type Broker struct {
	opts      Options
	mu        sync.RWMutex
	leases    []proto.BindingLease
	srv       *http.Server
	ln        net.Listener
	base      string
	proxyBase string
	token     string
	client    *http.Transport
	suspended bool
}

// New builds a broker; call Start to listen.
func New(opts Options) *Broker {
	b := &Broker{opts: opts, leases: opts.Leases}
	dialer := &net.Dialer{Timeout: 15 * time.Second, Control: nil}
	b.client = &http.Transport{
		Proxy: nil, // never chain through an ambient proxy
		DialContext: func(ctx context.Context, network, addr string) (net.Conn, error) {
			return b.dial(ctx, dialer, network, addr)
		},
		TLSClientConfig:       &tls.Config{RootCAs: opts.RootCAs, MinVersion: tls.VersionTLS12},
		ForceAttemptHTTP2:     true,
		MaxIdleConns:          64,
		IdleConnTimeout:       60 * time.Second,
		TLSHandshakeTimeout:   15 * time.Second,
		ResponseHeaderTimeout: 5 * time.Minute, // LLM streaming responses can take a while to start
		DisableCompression:    true,            // pass bodies through untouched
	}
	return b
}

// Start listens and returns the base URL (http://127.0.0.1:PORT).
func (b *Broker) Start() (string, error) {
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
	b.ln = ln
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
	raw := "http://" + advertised
	b.base = raw + "/c/" + b.token
	proxyURL := &url.URL{Scheme: "http", Host: advertised, User: url.User(b.token)}
	b.proxyBase = proxyURL.String()
	b.srv = &http.Server{Handler: b, ReadHeaderTimeout: 30 * time.Second}
	go func() { _ = b.srv.Serve(ln) }()
	return b.base, nil
}

// BaseURL returns the listener URL after Start.
func (b *Broker) BaseURL() string { return b.base }

// ProxyURL returns an authenticated URL suitable for HTTP_PROXY and
// HTTPS_PROXY. BaseURL is the capability-bearing reverse-proxy endpoint.
func (b *Broker) ProxyURL() string { return b.proxyBase }

// Close stops the listener.
func (b *Broker) Close() error {
	if b.srv == nil {
		return nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	err := b.srv.Shutdown(ctx)
	b.client.CloseIdleConnections()
	return err
}

// SetLeases replaces the binding leases (renewal).
func (b *Broker) SetLeases(leases []proto.BindingLease) {
	b.mu.Lock()
	b.leases = leases
	b.mu.Unlock()
}

// Suspend fails every request closed while a workspace is quiesced for a
// release. Resume is used only after control durably aborts that release.
func (b *Broker) Suspend() {
	b.mu.Lock()
	b.suspended = true
	b.mu.Unlock()
}

func (b *Broker) Resume() {
	b.mu.Lock()
	b.suspended = false
	b.mu.Unlock()
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
	return []string{
		"REMOUNT_BROKER=" + b.base,
		"HTTP_PROXY=" + b.proxyBase,
		"HTTPS_PROXY=" + b.proxyBase,
		"http_proxy=" + b.proxyBase,
		"https_proxy=" + b.proxyBase,
		"NO_PROXY=127.0.0.1,localhost",
		"no_proxy=127.0.0.1,localhost",
	}
}

// ---------------------------------------------------------------------------
// request handling
// ---------------------------------------------------------------------------

func (b *Broker) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	b.mu.RLock()
	suspended := b.suspended
	b.mu.RUnlock()
	if suspended {
		b.emit(Audit{At: time.Now(), WS: b.opts.WS, Principal: b.opts.Principal, Decision: DecisionDenied, Method: r.Method, Path: r.URL.Path, Reason: "workspace is quiesced"})
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
	switch {
	case r.Method == http.MethodConnect:
		b.handleConnect(w, r)
	case r.URL.IsAbs():
		// Forward-proxy form: GET http://host/path
		b.proxy(w, r, r.URL.Scheme, r.URL.Host, r.URL.Path, r.URL.RawQuery)
	case strings.HasPrefix(r.URL.Path, "/d/"):
		host, rest := splitDest(strings.TrimPrefix(r.URL.Path, "/d/"))
		b.proxy(w, r, "https", host, rest, r.URL.RawQuery)
	case strings.HasPrefix(r.URL.Path, "/http/"):
		host, rest := splitDest(strings.TrimPrefix(r.URL.Path, "/http/"))
		b.proxy(w, r, "http", host, rest, r.URL.RawQuery)
	case r.URL.Path == "/healthz":
		w.WriteHeader(http.StatusOK)
		_, _ = io.WriteString(w, "ok\n")
	default:
		http.Error(w, "remount broker: use /d/<host>/<path>, /http/<host>/<path>, or HTTP proxy mode", http.StatusNotFound)
	}
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
	audit := Audit{
		At: time.Now(), WS: b.opts.WS, Principal: b.opts.Principal,
		Decision: DecisionUnauthenticated, Host: r.Host, Method: r.Method, Path: r.URL.Path,
		Reason: "workspace broker capability missing or invalid",
	}
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

// proxy rewrites and forwards one request.
func (b *Broker) proxy(w http.ResponseWriter, r *http.Request, scheme, host, path, query string) {
	scheme = strings.ToLower(scheme)
	host = strings.ToLower(host)
	if host == "" || (scheme != "http" && scheme != "https") {
		http.Error(w, "remount broker: invalid destination", http.StatusBadRequest)
		return
	}
	audit := Audit{At: time.Now(), WS: b.opts.WS, Principal: b.opts.Principal, Host: host, Method: r.Method, Path: path}
	b.mu.RLock()
	leases := append([]proto.BindingLease(nil), b.leases...)
	b.mu.RUnlock()
	sort.SliceStable(leases, func(i, j int) bool {
		return len(Placeholder(leases[i])) > len(Placeholder(leases[j]))
	})

	// 1. Substitute placeholders; block any placeholder aimed elsewhere.
	used := map[string]bool{}
	for name, vals := range r.Header {
		for i, v := range vals {
			plain, basic := credentialText(v)
			var replacements []credentialReplacement
			for _, l := range leases {
				ph := Placeholder(l)
				if !containsToken(plain, ph) {
					continue
				}
				if !hostMatches(host, l.Destinations) {
					audit.Decision, audit.Binding, audit.Reason = DecisionLeakBlocked, l.ID, "placeholder for "+l.ID+" sent to "+host
					b.emit(audit)
					http.Error(w, "remount broker: credential "+l.ID+" is not bound to "+host, http.StatusForbidden)
					return
				}
				if l.ExpiresAt != 0 && time.Now().UnixMilli() > l.ExpiresAt {
					audit.Decision, audit.Binding, audit.Reason = DecisionExpired, l.ID, "lease expired"
					b.emit(audit)
					http.Error(w, "remount broker: lease for "+l.ID+" expired; fail closed", http.StatusForbidden)
					return
				}
				if scheme != "https" {
					audit.Decision, audit.Binding, audit.Reason = DecisionDenied, l.ID, "credential substitution requires a TLS upstream"
					b.emit(audit)
					http.Error(w, "remount broker: credentials are never sent over plaintext HTTP", http.StatusForbidden)
					return
				}
				replacements = append(replacements, credentialReplacement{placeholder: ph, secret: l.Secret})
				used[l.ID] = true
			}
			if len(replacements) > 0 {
				rewritten := substituteAll(plain, replacements)
				if basic {
					rewritten = "Basic " + base64.StdEncoding.EncodeToString([]byte(rewritten))
				}
				vals[i] = rewritten
			}
		}
		r.Header[name] = vals
	}
	// 2. Destination policy: a used binding permits its host; otherwise the allow list must.
	allowed := len(used) > 0 || hostMatches(host, b.opts.Allow)
	if !allowed {
		audit.Decision, audit.Reason = DecisionDenied, "destination not in bindings or allow list"
		b.emit(audit)
		http.Error(w, "remount broker: egress to "+host+" is not permitted for this workspace", http.StatusForbidden)
		return
	}
	// Record credential release before attempting outbound I/O. An upstream
	// reset or a cancelled workspace request cannot erase this forensic fact.
	for id := range used {
		a := audit
		a.Decision, a.Binding, a.Reason = DecisionSubstituted, id, "credential released to outbound transport"
		b.emit(a)
	}
	// 3. Forward.
	target := &url.URL{Scheme: scheme, Host: host}
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
			audit.Decision, audit.Reason, audit.Status = DecisionDenied, "upstream: "+err.Error(), http.StatusBadGateway
			b.emit(audit)
			http.Error(w, "remount broker: upstream error: "+err.Error(), http.StatusBadGateway)
		},
		ModifyResponse: func(resp *http.Response) error {
			audit.Status = resp.StatusCode
			if len(used) == 0 {
				a := audit
				a.Decision = DecisionAllowed
				b.emit(a)
			}
			return nil
		},
	}
	rp.ServeHTTP(w, r)
}

// handleConnect tunnels TCP to a permitted host without inspection.
func (b *Broker) handleConnect(w http.ResponseWriter, r *http.Request) {
	host := strings.ToLower(r.Host)
	h, _, err := net.SplitHostPort(host)
	if err != nil {
		h = host
		host = net.JoinHostPort(host, "443")
	}
	audit := Audit{At: time.Now(), WS: b.opts.WS, Principal: b.opts.Principal, Host: h, Method: r.Method, Path: ""}
	b.mu.RLock()
	leases := append([]proto.BindingLease(nil), b.leases...)
	b.mu.RUnlock()
	allowed := hostMatches(host, b.opts.Allow)
	expiredBinding := ""
	for _, l := range leases {
		if !hostMatches(host, l.Destinations) {
			continue
		}
		if l.ExpiresAt != 0 && time.Now().UnixMilli() > l.ExpiresAt {
			expiredBinding = l.ID
			continue
		}
		allowed = true
	}
	if !allowed {
		if expiredBinding != "" {
			audit.Decision, audit.Binding, audit.Reason = DecisionExpired, expiredBinding, "CONNECT binding lease expired"
			b.emit(audit)
			http.Error(w, "remount broker: binding lease expired", http.StatusForbidden)
			return
		}
		audit.Decision, audit.Reason = DecisionDenied, "CONNECT destination not permitted"
		b.emit(audit)
		http.Error(w, "remount broker: CONNECT to "+h+" is not permitted", http.StatusForbidden)
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
	w.WriteHeader(http.StatusOK)
	down, buf, err := hj.Hijack()
	if err != nil {
		up.Close()
		return
	}
	audit.Decision = DecisionAllowed
	b.emit(audit)
	go func() {
		defer up.Close()
		defer down.Close()
		if buf.Reader.Buffered() > 0 {
			_, _ = io.CopyN(up, buf, int64(buf.Reader.Buffered()))
		}
		_, _ = io.Copy(up, down)
	}()
	_, _ = io.Copy(down, up)
	up.Close()
	down.Close()
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
	case DecisionDenied:
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
	host = strings.ToLower(host)
	hport := ""
	if h, p, err := net.SplitHostPort(host); err == nil {
		host, hport = h, p
	}
	for _, p := range patterns {
		p = strings.ToLower(p)
		pport := ""
		if ph, pp, err := net.SplitHostPort(p); err == nil {
			p, pport = ph, pp
		}
		if pport != "" && hport != "" && pport != hport {
			continue
		}
		if p == "*" {
			return true
		}
		if strings.HasPrefix(p, "*.") {
			if strings.HasSuffix(host, p[1:]) && host != p[2:] {
				return true
			}
			continue
		}
		if host == p {
			return true
		}
	}
	return false
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
