// Package transport carries proto.Frames over ordered, reliable byte
// transports. Two implementations exist: WebSocket (production) and an
// in-memory pipe with fault injection (simulation and tests).
//
// The transport is deliberately dumb: it moves whole frames, in order, or
// fails. Reconnection, replay and idempotency live above it (see peer.go and
// the session package), which is what lets the failure model have one
// recovery path.
package transport

import (
	"context"
	"errors"
	"sync"

	"remount.dev/remount/internal/proto"
)

// ErrClosed is returned once a Conn is closed, by either side or by a fault.
var ErrClosed = errors.New("transport: connection closed")

// ErrNotSent reports that a Conn refused to transmit one frame while the
// connection itself stays usable: the frame never reached the wire, so
// nothing downstream can have acted on it, and the peer is not torn down.
// A Conn that layers policy over the wire (see internal/e2ee) wraps it.
var ErrNotSent = errors.New("transport: frame not transmitted")

// MaxFrameBytes bounds a single frame. Chunks are split well below this.
const MaxFrameBytes = 4 << 20

// Conn is one bidirectional frame connection.
type Conn interface {
	Send(ctx context.Context, f *proto.Frame) error
	Recv(ctx context.Context) (*proto.Frame, error)
	Close() error
}

// Dialer opens a new Conn; used for (re)connecting.
type Dialer interface {
	Dial(ctx context.Context) (Conn, error)
}

// DialFunc adapts a function to Dialer.
type DialFunc func(ctx context.Context) (Conn, error)

func (d DialFunc) Dial(ctx context.Context) (Conn, error) { return d(ctx) }

// ---------------------------------------------------------------------------
// In-memory pipe with fault injection
// ---------------------------------------------------------------------------

// pipeEnd is one side of a Pipe.
type pipeShared struct {
	closed chan struct{}
	once   sync.Once
}

type pipeEnd struct {
	in     chan *proto.Frame
	out    chan *proto.Frame
	closed chan struct{}
	shared *pipeShared
	peer   *pipeEnd
	// hook lets a test drop or delay frames. Called with the frame being
	// sent from this end; return false to drop it.
	hook func(f *proto.Frame) bool
	mu   sync.Mutex
}

// Pipe returns two connected in-memory Conns. Frames sent on one are received
// on the other, in order. Buffer bounds how far a fast sender may run ahead.
func Pipe(buffer int) (a, b Conn) {
	ab := make(chan *proto.Frame, buffer)
	ba := make(chan *proto.Frame, buffer)
	sh := &pipeShared{closed: make(chan struct{})}
	ea := &pipeEnd{in: ba, out: ab, closed: sh.closed, shared: sh}
	eb := &pipeEnd{in: ab, out: ba, closed: sh.closed, shared: sh}
	ea.peer, eb.peer = eb, ea
	return ea, eb
}

// SetHook installs a per-send hook on a pipe end. Non-pipe Conns ignore it.
func SetHook(c Conn, hook func(f *proto.Frame) bool) {
	if p, ok := c.(*pipeEnd); ok {
		p.mu.Lock()
		p.hook = hook
		p.mu.Unlock()
	}
}

func (p *pipeEnd) Send(ctx context.Context, f *proto.Frame) error {
	// Encode/decode through the real codec so tests exercise it and so the
	// receiver gets its own copy (no shared mutable state across the "wire").
	b, err := proto.EncodeFrame(f)
	if err != nil {
		return err
	}
	if len(b) > MaxFrameBytes {
		return errors.New("transport: frame too large")
	}
	g, err := proto.DecodeFrame(b)
	if err != nil {
		return err
	}
	p.mu.Lock()
	hook := p.hook
	p.mu.Unlock()
	if hook != nil && !hook(g) {
		return nil // dropped by fault injection
	}
	select {
	case <-p.closed:
		return ErrClosed
	case <-ctx.Done():
		return ctx.Err()
	case p.out <- g:
		return nil
	}
}

func (p *pipeEnd) Recv(ctx context.Context) (*proto.Frame, error) {
	select {
	case f := <-p.in:
		return f, nil
	default:
	}
	select {
	case f := <-p.in:
		return f, nil
	case <-p.closed:
		// Drain anything already queued before reporting closure, so a
		// close racing a send is not lossy on the receiver side.
		select {
		case f := <-p.in:
			return f, nil
		default:
			return nil, ErrClosed
		}
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (p *pipeEnd) Close() error {
	p.shared.once.Do(func() { close(p.shared.closed) })
	return nil
}
