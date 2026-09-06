package session

import (
	"fmt"
	"io"
	"net"
	"time"

	"remount.dev/remount/internal/proto"
)

// DialPort opens the byte-accurate TCP path a port session would use, without
// creating a logged session.
//
// The node needs this for conversations it holds itself — a DevTools
// connection to a browser in the workspace, for instance — where the bytes are
// a protocol the node speaks rather than output a client replays. Everything
// that makes a port session reach the right place already lives in the Spec
// the backend's Prepare filled in: a resolved Host for the backends that share
// a network path, or a Runner for the ones that relay (firecracker's vsock
// guest bridge). Reusing that resolved Spec is what keeps a node-side dial and
// a client's port.open on exactly one path per backend.
//
// The caller must have run Backend.Prepare on spec first.
func DialPort(spec Spec) (net.Conn, error) {
	if spec.Kind != proto.SessionPort {
		return nil, proto.Err(proto.CodeBadRequest, "DialPort needs a %s spec, got %q", proto.SessionPort, spec.Kind)
	}
	if spec.Port < 1 || spec.Port > 65535 {
		return nil, proto.Err(proto.CodeBadRequest, "port must be between 1 and 65535")
	}
	if spec.Runner != nil {
		running, err := spec.Runner.Start(spec)
		if err != nil {
			return nil, err
		}
		if running == nil {
			return nil, proto.Err(proto.CodeInternal, "session: backend runner returned nil")
		}
		return &runnerConn{running: running, port: spec.Port}, nil
	}
	host := spec.Host
	if host == "" {
		host = "127.0.0.1"
	}
	d := net.Dialer{Timeout: 10 * time.Second}
	return d.Dial("tcp", net.JoinHostPort(host, fmt.Sprint(spec.Port)))
}

// runnerConn presents a backend-relayed byte stream as a net.Conn so ordinary
// net/http machinery can run over it. Deadlines are not enforceable across the
// relay, so they are accepted and ignored; callers bound the conversation with
// a context instead.
type runnerConn struct {
	running Running
	port    int
	closed  bool
}

func (c *runnerConn) Read(p []byte) (int, error) {
	r := c.running.Stdout()
	if r == nil {
		return 0, io.EOF
	}
	return r.Read(p)
}

func (c *runnerConn) Write(p []byte) (int, error) {
	w := c.running.Stdin()
	if w == nil {
		return 0, io.ErrClosedPipe
	}
	return w.Write(p)
}

// Close joins the backend producer. Cancellation is not completion: the relay
// is not released until Wait has returned.
func (c *runnerConn) Close() error {
	if c.closed {
		return nil
	}
	c.closed = true
	if w := c.running.Stdin(); w != nil {
		_ = w.Close()
	}
	_ = c.running.Signal("KILL")
	c.running.Wait()
	return nil
}

func (c *runnerConn) LocalAddr() net.Addr  { return portAddr{port: 0} }
func (c *runnerConn) RemoteAddr() net.Addr { return portAddr{port: c.port} }

func (c *runnerConn) SetDeadline(time.Time) error      { return nil }
func (c *runnerConn) SetReadDeadline(time.Time) error  { return nil }
func (c *runnerConn) SetWriteDeadline(time.Time) error { return nil }

type portAddr struct{ port int }

func (a portAddr) Network() string { return "remount-port" }
func (a portAddr) String() string  { return fmt.Sprintf("workspace:%d", a.port) }
