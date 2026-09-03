package server

import (
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httputil"
	"net/url"
	"os"
	"strconv"
	"sync"
	"time"

	"remount.dev/remount/internal/client"
	"remount.dev/remount/internal/metrics"
	"remount.dev/remount/internal/proto"
)

// The preview proxy: /v1/agents/{id}/ports/{port}/... forwards to a TCP
// port inside the agent's workspace through a port session, the same
// forward `remount port` uses. It is an authenticated reverse proxy for
// HTTP and WebSocket traffic; the workspace program sees a plain client on
// 127.0.0.1 and X-Forwarded-* headers naming the outside. A sleeping agent
// is woken (agent.woken{by: preview}) because a preview without a running
// workspace has nothing to show.

const (
	// proxyDialTimeout bounds opening the port session; the workspace may
	// have to be woken and claimed first, which materialized already waited
	// for, so this is only the node round trip.
	proxyDialTimeout = 30 * time.Second
	// proxyHeaderTimeout bounds how long the workspace program may take to
	// start answering.
	proxyHeaderTimeout = 60 * time.Second
)

func (s *Server) handlePortRoot(w http.ResponseWriter, r *http.Request) {
	u := *r.URL
	u.Path += "/"
	http.Redirect(w, r, u.String(), http.StatusTemporaryRedirect)
}

func (s *Server) handlePort(w http.ResponseWriter, r *http.Request) {
	cl, release := s.apiClient(w, r, true)
	if cl == nil {
		return
	}
	defer release()
	port, err := strconv.Atoi(r.PathValue("port"))
	if err != nil || port < 1 || port > 65535 {
		badRequest(w, "port must be 1-65535")
		return
	}
	id := r.PathValue("id")
	a, err := s.materialized(r.Context(), cl, id, true, proto.AgentWokenByPreview)
	if err != nil {
		writeError(w, err)
		return
	}
	metrics.HTTPProxyRequests.Inc()
	prefix := "/v1/agents/" + id + "/ports/" + strconv.Itoa(port)
	rest := "/" + r.PathValue("rest")
	target := &url.URL{Scheme: "http", Host: net.JoinHostPort("127.0.0.1", strconv.Itoa(port))}
	rp := &httputil.ReverseProxy{
		Rewrite: func(pr *httputil.ProxyRequest) {
			pr.SetURL(target)
			pr.Out.URL.Path = rest
			pr.Out.URL.RawPath = ""
			pr.Out.Host = target.Host
			pr.SetXForwarded()
			pr.Out.Header.Set("X-Forwarded-Prefix", prefix)
			// The credential authorized the proxy hop; the program inside the
			// workspace must not see it.
			pr.Out.Header.Del("Authorization")
			pr.Out.Header.Del("Cookie")
			for _, c := range pr.In.Cookies() {
				if c.Name != sessionCookie {
					pr.Out.AddCookie(c)
				}
			}
		},
		Transport: &http.Transport{
			DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
				dctx, cancel := context.WithTimeout(ctx, proxyDialTimeout)
				defer cancel()
				sess, err := cl.OpenPort(dctx, a.WS, port)
				if err != nil {
					return nil, err
				}
				return newSessionConn(sess, s.lifetime), nil
			},
			DisableKeepAlives:     true,
			ResponseHeaderTimeout: proxyHeaderTimeout,
			// The workspace side is a byte pipe, not a TLS dialer.
			ForceAttemptHTTP2: false,
		},
		FlushInterval: -1,
		ErrorHandler: func(w http.ResponseWriter, r *http.Request, err error) {
			var pe *proto.Error
			if errors.As(err, &pe) {
				writeError(w, err)
				return
			}
			if errors.Is(err, context.Canceled) {
				return
			}
			s.opts.Logger.Debug("preview proxy", "agent", id, "port", port, "err", err)
			writeError(w, proto.Err(proto.CodeUnreachable, "nothing answered on port %d in the workspace", port))
		},
	}
	rp.ServeHTTP(w, r)
}

// sessionConn adapts a port session to net.Conn so http.Transport can drive
// it: writes become session input, stdout chunks become reads, and the
// session's exit is EOF.
type sessionConn struct {
	sess *client.Session
	ctx  context.Context

	mu       sync.Mutex
	buf      []byte
	readErr  error
	deadline time.Time
	closed   chan struct{}
	once     sync.Once
}

func newSessionConn(sess *client.Session, lifetime context.Context) *sessionConn {
	return &sessionConn{sess: sess, ctx: lifetime, closed: make(chan struct{})}
}

func (c *sessionConn) Read(p []byte) (int, error) {
	c.mu.Lock()
	if len(c.buf) > 0 {
		n := copy(p, c.buf)
		c.buf = c.buf[n:]
		c.mu.Unlock()
		return n, nil
	}
	if c.readErr != nil {
		err := c.readErr
		c.mu.Unlock()
		return 0, err
	}
	var deadline <-chan time.Time
	if !c.deadline.IsZero() {
		d := time.Until(c.deadline)
		if d <= 0 {
			c.mu.Unlock()
			return 0, os.ErrDeadlineExceeded
		}
		t := time.NewTimer(d)
		defer t.Stop()
		deadline = t.C
	}
	c.mu.Unlock()
	for {
		select {
		case <-c.closed:
			return 0, net.ErrClosed
		case <-deadline:
			return 0, os.ErrDeadlineExceeded
		case ch, ok := <-c.sess.Chunks():
			if !ok {
				err := c.sess.Err()
				if err == nil || c.sess.Exit() != nil {
					err = io.EOF
				}
				c.mu.Lock()
				c.readErr = err
				c.mu.Unlock()
				return 0, err
			}
			switch ch.Stream {
			case proto.StreamStdout:
				if len(ch.Data) == 0 {
					continue
				}
				n := copy(p, ch.Data)
				if n < len(ch.Data) {
					c.mu.Lock()
					c.buf = append(c.buf, ch.Data[n:]...)
					c.mu.Unlock()
				}
				return n, nil
			case proto.StreamExit:
				c.mu.Lock()
				c.readErr = io.EOF
				c.mu.Unlock()
				return 0, io.EOF
			case proto.StreamGap:
				// Bytes of a TCP stream cannot be skipped; the connection is
				// broken rather than silently corrupted.
				err := proto.Err(proto.CodeEvicted, "port session lost output")
				c.mu.Lock()
				c.readErr = err
				c.mu.Unlock()
				return 0, err
			}
		}
	}
}

func (c *sessionConn) Write(p []byte) (int, error) {
	select {
	case <-c.closed:
		return 0, net.ErrClosed
	default:
	}
	ctx, cancel := context.WithTimeout(c.ctx, 30*time.Second)
	defer cancel()
	if err := c.sess.Input(ctx, append([]byte(nil), p...), false); err != nil {
		return 0, err
	}
	return len(p), nil
}

func (c *sessionConn) Close() error {
	c.once.Do(func() {
		close(c.closed)
		ctx, cancel := context.WithTimeout(c.ctx, 10*time.Second)
		defer cancel()
		_ = c.sess.Close(ctx, true)
	})
	return nil
}

type sessionAddr struct{ s string }

func (a sessionAddr) Network() string { return "remount" }
func (a sessionAddr) String() string  { return a.s }

func (c *sessionConn) LocalAddr() net.Addr  { return sessionAddr{"proxy"} }
func (c *sessionConn) RemoteAddr() net.Addr { return sessionAddr{c.sess.ID} }

func (c *sessionConn) SetDeadline(t time.Time) error {
	return c.SetReadDeadline(t)
}

func (c *sessionConn) SetReadDeadline(t time.Time) error {
	c.mu.Lock()
	c.deadline = t
	c.mu.Unlock()
	return nil
}

// SetWriteDeadline is accepted and ignored: writes are bounded requests
// with their own timeout.
func (c *sessionConn) SetWriteDeadline(time.Time) error { return nil }

var _ net.Conn = (*sessionConn)(nil)
