// Package relay forwards frames between peers by destination id. It never
// interprets frame bodies. The one peer it treats specially is "control",
// which is an in-process Controller rather than a connection
// (docs/adr/0008-dumb-relay.md).
//
// The relay is also where peers are authenticated: the first frame on a
// connection must be a hello, which the Controller validates.
package relay

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"time"

	"remount.dev/remount/internal/metrics"
	"remount.dev/remount/internal/proto"
	"remount.dev/remount/internal/transport"
)

// Controller is the control plane as seen by the relay.
type Controller interface {
	// Authenticate validates a hello and returns the peer id to use.
	Authenticate(ctx context.Context, h *proto.Hello) (string, *proto.HelloOK, error)
	// HandleFrame receives every frame addressed to "control". f.From is
	// authoritative. Responses are sent through Sender.
	HandleFrame(ctx context.Context, f *proto.Frame)
	// PeerConnected / PeerGone track liveness.
	PeerConnected(ctx context.Context, id string, h *proto.Hello)
	PeerGone(ctx context.Context, id string)
}

// Sender is what the relay offers the controller.
type Sender interface {
	// Send routes f by f.To (From is forced to "control").
	Send(ctx context.Context, f *proto.Frame) error
	// Request sends a req from "control" and waits for the res.
	Request(ctx context.Context, to, op string, body, out any) error
	// Online reports whether a peer is connected.
	Online(id string) bool
	// Peers lists connected peer ids.
	Peers() []string
}

// Relay routes frames.
type Relay struct {
	ctrl Controller

	mu      sync.RWMutex
	peers   map[string]*transport.Peer
	hellos  map[string]*proto.Hello
	active  map[*serveCall]struct{}
	serveWG sync.WaitGroup
	closed  bool
	// recent records, per peer, which peers it has exchanged frames with so
	// peer.gone can be delivered to the right places without content
	// inspection.
	recent map[string]map[string]struct{}

	pendMu  sync.Mutex
	pending map[uint64]relayPending
	nextID  atomic.Uint64
}

// serveCall keeps even pre-authentication connections visible to Close. A
// peer is not entered in peers until its hello succeeds, but shutdown must
// still wake and join a connection blocked waiting for that first frame.
type serveCall struct {
	conn transport.Conn
}

type relayPending struct {
	from string
	op   string
	ch   chan *proto.Frame
}

const maxPendingRequests = 4096

// New creates a relay for the controller.
func New(ctrl Controller) *Relay {
	return &Relay{
		ctrl: ctrl, peers: map[string]*transport.Peer{}, hellos: map[string]*proto.Hello{},
		active: map[*serveCall]struct{}{}, recent: map[string]map[string]struct{}{},
		pending: map[uint64]relayPending{},
	}
}

// Serve handles one accepted connection until it closes. It blocks.
func (r *Relay) Serve(ctx context.Context, conn transport.Conn) error {
	call := &serveCall{conn: conn}
	r.mu.Lock()
	if r.closed {
		r.mu.Unlock()
		_ = conn.Close()
		return transport.ErrClosed
	}
	r.active[call] = struct{}{}
	r.serveWG.Add(1)
	r.mu.Unlock()
	defer func() {
		r.mu.Lock()
		delete(r.active, call)
		r.mu.Unlock()
		r.serveWG.Done()
	}()

	hctx, cancel := context.WithTimeout(ctx, 15*time.Second)
	first, err := conn.Recv(hctx)
	cancel()
	if err != nil {
		conn.Close()
		return err
	}
	if first.T != proto.KindHello {
		_ = conn.Send(ctx, &proto.Frame{V: proto.Version, T: proto.KindRes, ID: first.ID, Err: proto.Err(proto.CodeBadRequest, "first frame must be hello")})
		conn.Close()
		return errors.New("relay: first frame not hello")
	}
	var h proto.Hello
	if err := first.Decode(&h); err != nil {
		conn.Close()
		return err
	}
	id, ok, err := r.ctrl.Authenticate(ctx, &h)
	if err != nil {
		e := proto.Err(proto.CodeUnauthorized, "%v", err)
		var pe *proto.Error
		if errors.As(err, &pe) {
			e = pe
		}
		_ = conn.Send(ctx, &proto.Frame{V: proto.Version, T: proto.KindRes, ID: first.ID, Err: e})
		conn.Close()
		return err
	}
	peer := transport.NewPeer(conn, transport.HandlerFunc(func(ctx context.Context, p *transport.Peer, f *proto.Frame) {
		f.From = id // never trust the sender's claim
		r.route(ctx, f)
	}))
	peer.Name = id
	r.mu.Lock()
	if r.closed {
		r.mu.Unlock()
		_ = peer.Close()
		return transport.ErrClosed
	}
	if old, exists := r.peers[id]; exists {
		// A reconnecting peer replaces its previous connection.
		old.Close()
	}
	r.peers[id] = peer
	r.hellos[id] = &h
	r.mu.Unlock()
	if err := peer.Send(ctx, &proto.Frame{V: proto.Version, T: proto.KindRes, ID: first.ID, Body: proto.MustMarshal(ok)}); err != nil {
		r.remove(ctx, id, peer)
		return err
	}
	metrics.PeersConnected.Set(int64(len(r.Peers())))
	r.ctrl.PeerConnected(ctx, id, &h)
	<-peer.Done()
	r.remove(ctx, id, peer)
	return peer.Err()
}

func (r *Relay) remove(ctx context.Context, id string, peer *transport.Peer) {
	r.mu.Lock()
	if r.peers[id] != peer {
		r.mu.Unlock()
		return // already replaced by a newer connection
	}
	delete(r.peers, id)
	delete(r.hellos, id)
	correspondents := r.recent[id]
	delete(r.recent, id)
	for other := range correspondents {
		delete(r.recent[other], id)
	}
	r.mu.Unlock()
	metrics.PeersConnected.Set(int64(len(r.Peers())))
	r.ctrl.PeerGone(ctx, id)
	for other := range correspondents {
		_ = r.Send(ctx, proto.NewEvent(other, proto.EvPeerGone, map[string]string{"peer": id}))
	}
}

// route delivers a frame from an authenticated peer.
func (r *Relay) route(ctx context.Context, f *proto.Frame) {
	if f.To == "" || f.To == proto.PeerControl {
		if f.T == proto.KindRes {
			r.pendMu.Lock()
			pending, ok := r.pending[f.ID]
			if ok && (f.From != pending.from || f.Op != pending.op) {
				ok = false
			}
			if ok {
				delete(r.pending, f.ID)
			}
			r.pendMu.Unlock()
			if ok {
				pending.ch <- f
				return
			}
		}
		r.ctrl.HandleFrame(ctx, f)
		return
	}
	r.mu.Lock()
	dst := r.peers[f.To]
	if dst != nil {
		if r.recent[f.From] == nil {
			r.recent[f.From] = map[string]struct{}{}
		}
		if r.recent[f.To] == nil {
			r.recent[f.To] = map[string]struct{}{}
		}
		r.recent[f.From][f.To] = struct{}{}
		r.recent[f.To][f.From] = struct{}{}
	}
	r.mu.Unlock()
	if dst == nil {
		metrics.FramesDropped.Inc()
		if f.T == proto.KindReq {
			_ = r.Send(ctx, &proto.Frame{V: proto.Version, T: proto.KindRes, ID: f.ID, To: f.From, Op: f.Op, Err: proto.Err(proto.CodeUnreachable, "peer %s not connected", f.To)})
		}
		return
	}
	metrics.FramesRouted.Inc()
	metrics.BytesRouted.Add(uint64(len(f.Body)))
	if err := dst.Send(ctx, f); err != nil && f.T == proto.KindReq {
		_ = r.Send(ctx, &proto.Frame{V: proto.Version, T: proto.KindRes, ID: f.ID, To: f.From, Op: f.Op, Err: proto.Err(proto.CodeUnreachable, "peer %s: %v", f.To, err)})
	}
}

// Send routes a frame originating from the control plane.
func (r *Relay) Send(ctx context.Context, f *proto.Frame) error {
	f.From = proto.PeerControl
	if f.V == 0 {
		f.V = proto.Version
	}
	r.mu.RLock()
	closed := r.closed
	dst := r.peers[f.To]
	r.mu.RUnlock()
	if closed {
		return transport.ErrClosed
	}
	if dst == nil {
		return proto.Err(proto.CodeUnreachable, "peer %s not connected", f.To)
	}
	return dst.Send(ctx, f)
}

// Request sends a control-originated request and waits for the response.
func (r *Relay) Request(ctx context.Context, to, op string, body, out any) error {
	id := r.nextID.Add(1)
	ch := make(chan *proto.Frame, 1)
	r.pendMu.Lock()
	if len(r.pending) >= maxPendingRequests {
		r.pendMu.Unlock()
		return proto.Err(proto.CodeDenied, "too many pending relay requests")
	}
	r.pending[id] = relayPending{from: to, op: op, ch: ch}
	r.pendMu.Unlock()
	f := proto.NewReq(id, to, op, body)
	if err := r.Send(ctx, f); err != nil {
		r.pendMu.Lock()
		delete(r.pending, id)
		r.pendMu.Unlock()
		return err
	}
	select {
	case res := <-ch:
		if res == nil {
			return transport.ErrClosed
		}
		if res.Err != nil {
			return res.Err
		}
		if out != nil {
			return res.Decode(out)
		}
		return nil
	case <-ctx.Done():
		r.pendMu.Lock()
		delete(r.pending, id)
		r.pendMu.Unlock()
		return ctx.Err()
	}
}

// Online reports whether id is connected.
func (r *Relay) Online(id string) bool {
	r.mu.RLock()
	defer r.mu.RUnlock()
	_, ok := r.peers[id]
	return ok
}

// Peers lists connected ids.
func (r *Relay) Peers() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]string, 0, len(r.peers))
	for id := range r.peers {
		out = append(out, id)
	}
	return out
}

// Hello returns the hello a connected peer presented.
func (r *Relay) Hello(id string) *proto.Hello {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return cloneHello(r.hellos[id])
}

func cloneHello(in *proto.Hello) *proto.Hello {
	if in == nil {
		return nil
	}
	out := *in
	out.Caps = append([]string(nil), in.Caps...)
	out.PubKey = append([]byte(nil), in.PubKey...)
	out.Nonce = append([]byte(nil), in.Nonce...)
	out.Proof = append([]byte(nil), in.Proof...)
	if in.Labels != nil {
		out.Labels = make(map[string]string, len(in.Labels))
		for key, value := range in.Labels {
			out.Labels[key] = value
		}
	}
	if in.Node != nil {
		node := *in.Node
		node.Backends = append([]string(nil), in.Node.Backends...)
		node.BackendDescriptors = append([]proto.BackendDescriptor(nil), in.Node.BackendDescriptors...)
		node.Caps = append([]string(nil), in.Node.Caps...)
		node.Connectors = append([]string(nil), in.Node.Connectors...)
		out.Node = &node
	}
	return &out
}

// Close disconnects everyone and waits until every Serve call, including a
// connection still waiting for its hello, has returned. Once Close starts no
// new connection or control-plane send is admitted.
func (r *Relay) Close() {
	r.mu.Lock()
	r.closed = true
	peers := r.peers
	r.peers = map[string]*transport.Peer{}
	r.hellos = map[string]*proto.Hello{}
	r.recent = map[string]map[string]struct{}{}
	active := make([]transport.Conn, 0, len(r.active))
	for call := range r.active {
		active = append(active, call.conn)
	}
	r.mu.Unlock()
	for _, conn := range active {
		_ = conn.Close()
	}
	for _, p := range peers {
		_ = p.Close()
	}
	r.pendMu.Lock()
	pending := r.pending
	r.pending = map[uint64]relayPending{}
	r.pendMu.Unlock()
	for _, request := range pending {
		request.ch <- nil
	}
	r.serveWG.Wait()
}
