package transport

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"time"

	"remount.dev/remount/internal/proto"
)

// Handler receives inbound frames that are not responses to our requests:
// requests addressed to us, events, and chunks. It must not block for long;
// long work should be spawned. Returning a non-nil frame sends it.
type Handler interface {
	HandleFrame(ctx context.Context, p *Peer, f *proto.Frame)
}

// HandlerFunc adapts a function to Handler.
type HandlerFunc func(ctx context.Context, p *Peer, f *proto.Frame)

func (h HandlerFunc) HandleFrame(ctx context.Context, p *Peer, f *proto.Frame) { h(ctx, p, f) }

// Peer multiplexes request/response correlation, events and chunks over one
// Conn. It owns the read loop. When the Conn fails every pending request is
// failed with ErrClosed; the owner decides whether to redial.
type Peer struct {
	conn    Conn
	handler Handler
	nextID  atomic.Uint64

	mu      sync.Mutex
	pending map[uint64]pendingRequest
	closed  bool
	done    chan struct{}
	err     error

	// Name is the peer id this side speaks as (filled by hello).
	Name string
	// Remote is what the other side called itself (for logs).
	Remote string

	writeTimeout time.Duration
}

type pendingRequest struct {
	from string
	op   string
	ch   chan *proto.Frame
}

const maxPendingRequests = 4096

// NewPeer wraps conn and starts its read loop with the given handler.
func NewPeer(conn Conn, handler Handler) *Peer {
	p := &Peer{
		conn:         conn,
		handler:      handler,
		pending:      map[uint64]pendingRequest{},
		done:         make(chan struct{}),
		writeTimeout: 30 * time.Second,
	}
	go p.readLoop()
	return p
}

// Done is closed when the read loop has exited.
func (p *Peer) Done() <-chan struct{} { return p.done }

// Err returns the terminal error after Done is closed.
func (p *Peer) Err() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.err
}

func (p *Peer) readLoop() {
	ctx := context.Background()
	var err error
	for {
		var f *proto.Frame
		f, err = p.conn.Recv(ctx)
		if err != nil {
			break
		}
		switch f.T {
		case proto.KindRes, proto.KindPong:
			p.mu.Lock()
			pending, ok := p.pending[f.ID]
			if ok && pending.from != "" && f.From != "" && f.From != pending.from {
				ok = false
			}
			if ok && pending.op != "" && f.Op != pending.op {
				ok = false
			}
			if ok {
				delete(p.pending, f.ID)
			}
			p.mu.Unlock()
			if ok {
				pending.ch <- f
				continue
			}
			// Not one of ours: a relay sees transit responses addressed to
			// other peers, and an endpoint may see a late reply to a
			// timed-out request. The handler decides (route or ignore).
			if p.handler != nil {
				p.handler.HandleFrame(ctx, p, f)
			}
		case proto.KindPing:
			_ = p.Send(ctx, &proto.Frame{V: proto.Version, T: proto.KindPong, ID: f.ID, To: f.From})
		default:
			if p.handler != nil {
				p.handler.HandleFrame(ctx, p, f)
			}
		}
	}
	p.fail(err)
}

func (p *Peer) fail(err error) {
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return
	}
	p.closed = true
	p.err = err
	pend := p.pending
	p.pending = map[uint64]pendingRequest{}
	p.mu.Unlock()
	_ = p.conn.Close()
	for _, pending := range pend {
		pending.ch <- nil // nil = connection failed
	}
	close(p.done)
}

// Close tears down the connection.
func (p *Peer) Close() error {
	p.fail(ErrClosed)
	return nil
}

// Send writes one frame. It is safe for concurrent use.
func (p *Peer) Send(ctx context.Context, f *proto.Frame) error {
	p.mu.Lock()
	closed := p.closed
	p.mu.Unlock()
	if closed {
		return ErrClosed
	}
	if p.writeTimeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, p.writeTimeout)
		defer cancel()
	}
	if err := p.conn.Send(ctx, f); err != nil {
		if !errors.Is(err, context.Canceled) {
			p.fail(err)
		}
		return err
	}
	return nil
}

// Request sends a req frame and waits for the matching res. A nil result
// with ErrClosed means the connection died; the caller may retry with the
// same idempotency key on a new connection.
func (p *Peer) Request(ctx context.Context, to, op string, body any) (*proto.Frame, error) {
	id := p.nextID.Add(1)
	f := proto.NewReq(id, to, op, body)
	return p.roundTrip(ctx, id, to, op, f)
}

// RequestFrame sends an arbitrary frame that expects a res with the same ID
// (used for hello, whose kind is not "req"). ID is assigned if zero.
func (p *Peer) RequestFrame(ctx context.Context, f *proto.Frame) (*proto.Frame, error) {
	if f.ID == 0 {
		f.ID = p.nextID.Add(1)
	}
	return p.roundTrip(ctx, f.ID, f.To, f.Op, f)
}

// Hello performs the hello exchange: it must be the first frame on a
// connection to a relay.
func Hello(ctx context.Context, p *Peer, h proto.Hello) (*proto.HelloOK, error) {
	res, err := p.RequestFrame(ctx, &proto.Frame{V: proto.Version, T: proto.KindHello, Body: proto.MustMarshal(h)})
	if err != nil {
		return nil, err
	}
	var ok proto.HelloOK
	if err := res.Decode(&ok); err != nil {
		return nil, err
	}
	p.Name = ok.Peer
	return &ok, nil
}

// Ping measures liveness/RTT.
func (p *Peer) Ping(ctx context.Context) (time.Duration, error) {
	id := p.nextID.Add(1)
	start := time.Now()
	_, err := p.roundTrip(ctx, id, "", "", &proto.Frame{V: proto.Version, T: proto.KindPing, ID: id})
	return time.Since(start), err
}

func (p *Peer) roundTrip(ctx context.Context, id uint64, from, op string, f *proto.Frame) (*proto.Frame, error) {
	ch := make(chan *proto.Frame, 1)
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return nil, ErrClosed
	}
	if len(p.pending) >= maxPendingRequests {
		p.mu.Unlock()
		return nil, proto.Err(proto.CodeDenied, "too many pending requests")
	}
	p.pending[id] = pendingRequest{from: from, op: op, ch: ch}
	p.mu.Unlock()
	if err := p.Send(ctx, f); err != nil {
		p.mu.Lock()
		delete(p.pending, id)
		p.mu.Unlock()
		return nil, err
	}
	select {
	case res := <-ch:
		if res == nil {
			return nil, ErrClosed
		}
		if res.Err != nil {
			return res, res.Err
		}
		return res, nil
	case <-ctx.Done():
		p.mu.Lock()
		delete(p.pending, id)
		p.mu.Unlock()
		return nil, ctx.Err()
	}
}

// Call is Request plus decoding the response body into out (may be nil).
func (p *Peer) Call(ctx context.Context, to, op string, body, out any) error {
	res, err := p.Request(ctx, to, op, body)
	if err != nil {
		return err
	}
	if out == nil {
		return nil
	}
	return res.Decode(out)
}

// Respond sends a response to req.
func (p *Peer) Respond(ctx context.Context, req *proto.Frame, body any) error {
	return p.Send(ctx, proto.NewRes(req, body))
}

// RespondErr sends an error response to req.
func (p *Peer) RespondErr(ctx context.Context, req *proto.Frame, e *proto.Error) error {
	return p.Send(ctx, proto.NewErrRes(req, e))
}
