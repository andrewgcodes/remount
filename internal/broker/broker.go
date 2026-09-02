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
	"strconv"
	"strings"
	"sync"
	"time"

	"remount.dev/remount/internal/metrics"
	"remount.dev/remount/internal/proto"
)

// Decision names for audit events.
const (
	DecisionAllowed     = "allowed"
	DecisionSubstituted = "substituted"
	DecisionDenied      = "denied"       // destination not permitted
	DecisionLeakBlocked = "leak_blocked" // placeholder aimed at a foreign host
	DecisionExpired     = "expired"      // lease TTL passed; fail closed
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
}

// Broker serves one workspace.
type Broker struct {
	opts   Options
	mu     sync.RWMutex
	leases []proto.BindingLease
	srv    *http.Server
	ln     net.Listener
	base   string
	client *http.Transport
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
	b.ln = ln
	b.base = "http://" + ln.Addr().String()
	b.srv = &http.Server{Handler: b, ReadHeaderTimeout: 30 * time.Second}
	go func() { _ = b.srv.Serve(ln) }()
	return b.base, nil
}

// BaseURL returns the listener URL after Start.
func (b *Broker) BaseURL() string { return b.base }

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
		"HTTP_PROXY=" + b.base,
		"HTTPS_PROXY=" + b.base,
		"http_proxy=" + b.base,
		"https_proxy=" + b.base,
		"NO_PROXY=127.0.0.1,localhost",
		"no_proxy=127.0.0.1,localhost",
	}
}

// ---------------------------------------------------------------------------
// request handling
// ---------------------------------------------------------------------------

func (b *Broker) ServeHTTP(w http.ResponseWriter, r *http.Request) {
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

func splitDest(p string) (host, rest string) {
	i := strings.IndexByte(p, '/')
	if i < 0 {
		return p, "/"
	}
	return p[:i], p[i:]
}

// proxy rewrites and forwards one request.
func (b *Broker) proxy(w http.ResponseWriter, r *http.Request, scheme, host, path, query string) {
	host = strings.ToLower(host)
	if host == "" {
		http.Error(w, "remount broker: missing destination host", http.StatusBadRequest)
		return
	}
	audit := Audit{At: time.Now(), WS: b.opts.WS, Principal: b.opts.Principal, Host: host, Method: r.Method, Path: path}
	b.mu.RLock()
	leases := b.leases
	b.mu.RUnlock()

	// 1. Substitute placeholders; block any placeholder aimed elsewhere.
	used := map[string]bool{}
	for name, vals := range r.Header {
		for i, v := range vals {
			for _, l := range leases {
				ph := Placeholder(l)
				if !strings.Contains(v, ph) && !basicContains(v, ph) {
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
				vals[i] = substitute(v, ph, l.Secret)
				used[l.ID] = true
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
			if len(used) > 0 {
				for id := range used {
					a := audit
					a.Decision, a.Binding = DecisionSubstituted, id
					b.emit(a)
				}
			} else {
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
	leases := b.leases
	b.mu.RUnlock()
	allowed := hostMatches(host, b.opts.Allow)
	for _, l := range leases {
		if hostMatches(host, l.Destinations) {
			allowed = true
		}
	}
	if !allowed {
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
	return ip.IsLoopback() || ip.IsPrivate() || ip.IsLinkLocalUnicast() || ip.IsLinkLocalMulticast() ||
		ip.IsMulticast() || ip.IsUnspecified() || ip.IsInterfaceLocalMulticast()
}

func (b *Broker) emit(a Audit) {
	switch a.Decision {
	case DecisionSubstituted:
		metrics.CredUsed.Inc()
	case DecisionAllowed:
		metrics.EgressAllow.Inc()
	case DecisionLeakBlocked:
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

// substitute replaces the placeholder in a header value, decoding and
// re-encoding HTTP Basic credentials when needed.
func substitute(value, placeholder, secret string) string {
	if strings.Contains(value, placeholder) {
		return strings.ReplaceAll(value, placeholder, secret)
	}
	if dec, ok := decodeBasic(value); ok && strings.Contains(dec, placeholder) {
		return "Basic " + base64.StdEncoding.EncodeToString([]byte(strings.ReplaceAll(dec, placeholder, secret)))
	}
	return value
}

func basicContains(value, placeholder string) bool {
	dec, ok := decodeBasic(value)
	return ok && strings.Contains(dec, placeholder)
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
