package e2ee

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"remount.dev/remount/internal/proto"
	"remount.dev/remount/internal/transport"
)

// The tests in this file are the code-layer half of Plan B B25 and B27. The
// adversary they model is the relay itself: it sees every frame, may keep,
// drop, reorder, duplicate, rewrite or redirect any of them, and it is not
// trusted with the contents.

// ---------------------------------------------------------------------------
// harness
// ---------------------------------------------------------------------------

// fakeRelay routes frames between two guarded peers exactly as the real relay
// does — by To, stamping From — and records everything it carried, which is
// the capture a leak scan runs against. hook is the adversary.
type fakeRelay struct {
	t          *testing.T
	controlKey ed25519.PublicKey

	mu      sync.Mutex
	ends    map[string]transport.Conn
	capture []*proto.Frame
	hook    func(f *proto.Frame) []*proto.Frame
}

func (r *fakeRelay) serve(ctx context.Context, id string, conn transport.Conn) {
	for {
		f, err := conn.Recv(ctx)
		if err != nil {
			return
		}
		if f.T == proto.KindHello {
			_ = conn.Send(ctx, &proto.Frame{V: proto.Version, T: proto.KindRes, ID: f.ID,
				Body: proto.MustMarshal(proto.HelloOK{Peer: id, Caps: []string{proto.CapabilityV1}, PubKey: r.controlKey})})
			continue
		}
		f.From = id // the relay stamps From; a sender's claim is overwritten
		r.mu.Lock()
		r.capture = append(r.capture, f)
		hook := r.hook
		r.mu.Unlock()
		out := []*proto.Frame{f}
		if hook != nil {
			out = hook(f)
		}
		for _, g := range out {
			r.deliver(ctx, g)
		}
	}
}

func (r *fakeRelay) deliver(ctx context.Context, f *proto.Frame) {
	r.mu.Lock()
	dst := r.ends[f.To]
	r.mu.Unlock()
	if dst == nil {
		return
	}
	_ = dst.Send(ctx, f)
}

func (r *fakeRelay) setHook(hook func(f *proto.Frame) []*proto.Frame) {
	r.mu.Lock()
	r.hook = hook
	r.mu.Unlock()
}

// bytesCarried is the whole capture re-encoded: what an operator dumping the
// relay's traffic would hold.
func (r *fakeRelay) bytesCarried() []byte {
	r.mu.Lock()
	defer r.mu.Unlock()
	var out bytes.Buffer
	for _, f := range r.capture {
		b, err := proto.EncodeFrame(f)
		if err != nil {
			r.t.Fatal(err)
		}
		out.Write(b)
	}
	return out.Bytes()
}

func (r *fakeRelay) sealedFrames() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	n := 0
	for _, f := range r.capture {
		if f.Op == proto.OpE2EESealed {
			n++
		}
	}
	return n
}

func (r *fakeRelay) framesWithOp(op string) int {
	r.mu.Lock()
	defer r.mu.Unlock()
	n := 0
	for _, f := range r.capture {
		if f.Op == op {
			n++
		}
	}
	return n
}

// end is one guarded peer with a read loop, standing in for transport.Peer.
type end struct {
	id string
	g  *Guard
	in chan *proto.Frame
}

func (e *end) next(t *testing.T, d time.Duration) *proto.Frame {
	t.Helper()
	select {
	case f := <-e.in:
		return f
	case <-time.After(d):
		t.Fatalf("%s: no frame within %s", e.id, d)
		return nil
	}
}

func (e *end) none(t *testing.T, d time.Duration) {
	t.Helper()
	select {
	case f := <-e.in:
		t.Fatalf("%s: unexpected frame %s/%s", e.id, f.T, f.Op)
	case <-time.After(d):
	}
}

// kit is two guarded peers and the relay between them.
type kit struct {
	relay      *fakeRelay
	a, b       *end
	controlKey ed25519.PrivateKey
}

func newKit(t *testing.T, aPolicy, bPolicy Policy, adjust ...func(id string, c *Config)) *kit {
	t.Helper()
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	relay := &fakeRelay{t: t, controlKey: pub, ends: map[string]transport.Conn{}}
	k := &kit{relay: relay, controlKey: priv}
	k.a = k.join(t, ctx, "c_a", aPolicy, adjust...)
	k.b = k.join(t, ctx, "n_b", bPolicy, adjust...)
	return k
}

func (k *kit) join(t *testing.T, ctx context.Context, id string, policy Policy, adjust ...func(id string, c *Config)) *end {
	t.Helper()
	peerSide, relaySide := transport.Pipe(64)
	k.relay.mu.Lock()
	k.relay.ends[id] = relaySide
	k.relay.mu.Unlock()
	go k.relay.serve(ctx, id, relaySide)

	cfg := Config{
		Policy:           policy,
		ControlKey:       k.controlKey.Public().(ed25519.PublicKey),
		HandshakeTimeout: 3 * time.Second,
		Bind:             k.binder(t, "t_one"),
	}
	for _, a := range adjust {
		a(id, &cfg)
	}
	e := &end{id: id, g: Wrap(peerSide, cfg), in: make(chan *proto.Frame, 256)}
	go func() {
		for {
			f, err := e.g.Recv(ctx)
			if err != nil {
				close(e.in)
				return
			}
			e.in <- f
		}
	}()
	if err := e.g.Send(ctx, &proto.Frame{V: proto.Version, T: proto.KindHello, ID: 1,
		Body: proto.MustMarshal(proto.Hello{Peer: id, Role: proto.RoleClient, Caps: proto.PeerCapabilities()})}); err != nil {
		t.Fatal(err)
	}
	if f := e.next(t, 5*time.Second); f.T != proto.KindRes {
		t.Fatalf("hello answered with %s", f.T)
	}
	t.Cleanup(func() { _ = e.g.Close() })
	return e
}

// binder mints the control-plane-signed binding a peer authenticates its key
// agreements with. In a deployment this is a control-plane round trip.
func (k *kit) binder(t *testing.T, tenant string) func(context.Context, string) (*Identity, error) {
	return func(_ context.Context, self string) (*Identity, error) {
		id, err := NewIdentity(self, tenant, time.Now(), time.Hour)
		if err != nil {
			t.Fatal(err)
		}
		id.Binding = SignBinding(k.controlKey, id.Binding)
		return id, nil
	}
}

func req(id uint64, to, op string, body any) *proto.Frame {
	return proto.NewReq(id, to, op, body)
}

// ---------------------------------------------------------------------------
// tests
// ---------------------------------------------------------------------------

// A sealed frame leaves the relay nothing but the routing envelope: no op, no
// workspace, no session id, no body. The far peer still receives the frame it
// was sent, field for field.
func TestSealedFrameExposesOnlyTheRoutingEnvelope(t *testing.T) {
	k := newKit(t, PolicyRequired, PolicyRequired)
	ctx := context.Background()
	const canary = "sk-e2ee-unit-canary-0123456789"
	sent := req(7, k.b.id, "fs.write", map[string]string{"path": "secrets.txt", "data": canary})
	sent.WS, sent.S, sent.Seq = "w_1", "s_1", 42
	if err := k.a.g.Send(ctx, sent); err != nil {
		t.Fatal(err)
	}
	got := k.b.next(t, 5*time.Second)
	if got.Op != "fs.write" || got.WS != "w_1" || got.S != "s_1" || got.Seq != 42 || got.ID != 7 {
		t.Fatalf("delivered frame lost fields: %+v", got)
	}
	if !bytes.Equal(got.Body, sent.Body) {
		t.Fatal("delivered body differs")
	}
	if got.From != k.a.id {
		t.Fatalf("From = %q", got.From)
	}

	carried := k.relay.bytesCarried()
	// Positive control first: the scan must be able to find the canary, or
	// its absence proves nothing. The same bytes with the same scan, sealed
	// and unsealed.
	plain, err := proto.EncodeFrame(sent)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Contains(plain, []byte(canary)) {
		t.Fatal("the scan cannot find the canary in an unsealed frame; it proves nothing about a sealed one")
	}
	for _, secret := range []string{canary, "fs.write", "secrets.txt", "w_1", "s_1"} {
		if bytes.Contains(carried, []byte(secret)) {
			t.Fatalf("relay capture contains %q", secret)
		}
	}
	if k.relay.sealedFrames() != 1 {
		t.Fatalf("sealed frames carried = %d", k.relay.sealedFrames())
	}
	// The guard also records what the control plane echoed at hello, which is
	// where the capability will be advertised once both peers offer it.
	if got := k.a.g.Negotiated(); !proto.HasCapability(got, proto.CapabilityV1) {
		t.Fatalf("negotiated capabilities were not captured: %v", got)
	}
	if k.a.g.Self() != k.a.id {
		t.Fatalf("self = %q", k.a.g.Self())
	}
}

// B25, mutation. One flipped ciphertext byte is a forgery, and a forgery is
// never delivered to an operation handler.
func TestMutatedRecordIsRejected(t *testing.T) {
	k := newKit(t, PolicyRequired, PolicyRequired)
	ctx := context.Background()
	if err := k.a.g.Send(ctx, req(1, k.b.id, "fs.read", map[string]string{"path": "a"})); err != nil {
		t.Fatal(err)
	}
	k.b.next(t, 5*time.Second)

	k.relay.setHook(func(f *proto.Frame) []*proto.Frame {
		if f.Op == proto.OpE2EESealed && len(f.Body) > 0 {
			body := append([]byte(nil), f.Body...)
			body[len(body)-1] ^= 0x01
			copyFrame := *f
			copyFrame.Body = body
			return []*proto.Frame{&copyFrame}
		}
		return []*proto.Frame{f}
	})
	if err := k.a.g.Send(ctx, req(2, k.b.id, "fs.read", map[string]string{"path": "b"})); err != nil {
		t.Fatal(err)
	}
	k.b.none(t, 500*time.Millisecond)
}

// B25, replay. The same record delivered twice is accepted once.
func TestReplayedRecordIsRejected(t *testing.T) {
	k := newKit(t, PolicyRequired, PolicyRequired)
	ctx := context.Background()
	if err := k.a.g.Send(ctx, req(1, k.b.id, "fs.read", map[string]string{"path": "a"})); err != nil {
		t.Fatal(err)
	}
	k.b.next(t, 5*time.Second)

	k.relay.setHook(func(f *proto.Frame) []*proto.Frame {
		if f.Op == proto.OpE2EESealed {
			return []*proto.Frame{f, f} // deliver it twice
		}
		return []*proto.Frame{f}
	})
	if err := k.a.g.Send(ctx, req(2, k.b.id, "ws.exec", map[string]string{"cmd": "rm -rf /"})); err != nil {
		t.Fatal(err)
	}
	first := k.b.next(t, 5*time.Second)
	if first.Op != "ws.exec" {
		t.Fatalf("op = %q", first.Op)
	}
	k.b.none(t, 500*time.Millisecond)
}

// B25, cross-destination substitution. A relay that redirects a frame to
// another peer produces a record that peer cannot open: the destination is in
// the associated data, and the key belongs to the pair that agreed it.
func TestRedirectedRecordIsRejected(t *testing.T) {
	k := newKit(t, PolicyRequired, PolicyRequired)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	third := k.join(t, ctx, "n_c", PolicyRequired)

	// n_c and c_a agree their own session first, so the refusal below is not
	// simply "these two have never spoken".
	if err := k.a.g.Send(ctx, req(1, third.id, "fs.read", map[string]string{"path": "a"})); err != nil {
		t.Fatal(err)
	}
	third.next(t, 5*time.Second)
	if err := k.a.g.Send(ctx, req(2, k.b.id, "fs.read", map[string]string{"path": "a"})); err != nil {
		t.Fatal(err)
	}
	k.b.next(t, 5*time.Second)

	k.relay.setHook(func(f *proto.Frame) []*proto.Frame {
		if f.Op == proto.OpE2EESealed && f.To == k.b.id {
			copyFrame := *f
			copyFrame.To = third.id
			return []*proto.Frame{&copyFrame}
		}
		return []*proto.Frame{f}
	})
	if err := k.a.g.Send(ctx, req(3, k.b.id, "fs.write", map[string]string{"path": "x"})); err != nil {
		t.Fatal(err)
	}
	third.none(t, 500*time.Millisecond)
	k.b.none(t, 200*time.Millisecond)
}

// A record re-labelled with another correlation id fails: the request id is
// in the associated data, so a response cannot be pinned to a different call.
func TestRewrittenCorrelationIDIsRejected(t *testing.T) {
	k := newKit(t, PolicyRequired, PolicyRequired)
	ctx := context.Background()
	if err := k.a.g.Send(ctx, req(1, k.b.id, "fs.read", map[string]string{"path": "a"})); err != nil {
		t.Fatal(err)
	}
	k.b.next(t, 5*time.Second)
	k.relay.setHook(func(f *proto.Frame) []*proto.Frame {
		if f.Op == proto.OpE2EESealed {
			copyFrame := *f
			copyFrame.ID = f.ID + 1000
			return []*proto.Frame{&copyFrame}
		}
		return []*proto.Frame{f}
	})
	if err := k.a.g.Send(ctx, req(2, k.b.id, "fs.read", map[string]string{"path": "b"})); err != nil {
		t.Fatal(err)
	}
	k.b.none(t, 500*time.Millisecond)
}

// A record reflected back at its sender fails: each direction has its own key
// and its own direction byte in the associated data.
func TestReflectedRecordIsRejected(t *testing.T) {
	k := newKit(t, PolicyRequired, PolicyRequired)
	ctx := context.Background()
	if err := k.a.g.Send(ctx, req(1, k.b.id, "fs.read", map[string]string{"path": "a"})); err != nil {
		t.Fatal(err)
	}
	k.b.next(t, 5*time.Second)
	k.relay.setHook(func(f *proto.Frame) []*proto.Frame {
		if f.Op == proto.OpE2EESealed && f.From == k.a.id {
			copyFrame := *f
			copyFrame.To, copyFrame.From = k.a.id, k.b.id
			return []*proto.Frame{&copyFrame}
		}
		return []*proto.Frame{f}
	})
	if err := k.a.g.Send(ctx, req(2, k.b.id, "fs.read", map[string]string{"path": "b"})); err != nil {
		t.Fatal(err)
	}
	k.a.none(t, 500*time.Millisecond)
}

// Reordering is not an attack. The relay may deliver out of order and the
// replay window still accepts every record exactly once.
func TestReorderedRecordsAreStillDelivered(t *testing.T) {
	k := newKit(t, PolicyRequired, PolicyRequired)
	ctx := context.Background()
	if err := k.a.g.Send(ctx, req(1, k.b.id, "warm", nil)); err != nil {
		t.Fatal(err)
	}
	k.b.next(t, 5*time.Second)

	var held []*proto.Frame
	k.relay.setHook(func(f *proto.Frame) []*proto.Frame {
		if f.Op != proto.OpE2EESealed {
			return []*proto.Frame{f}
		}
		held = append(held, f)
		if len(held) < 4 {
			return nil
		}
		out := []*proto.Frame{held[3], held[1], held[2], held[0]}
		held = nil
		return out
	})
	for i := uint64(2); i <= 5; i++ {
		if err := k.a.g.Send(ctx, req(i, k.b.id, "fs.read", map[string]uint64{"n": i})); err != nil {
			t.Fatal(err)
		}
	}
	seen := map[uint64]bool{}
	for i := 0; i < 4; i++ {
		f := k.b.next(t, 5*time.Second)
		if seen[f.ID] {
			t.Fatalf("frame %d delivered twice", f.ID)
		}
		seen[f.ID] = true
	}
	if len(seen) != 4 {
		t.Fatalf("delivered %d of 4", len(seen))
	}
}

// B27. When a policy requires end-to-end encryption and the peer cannot
// negotiate it, the send fails, and it fails before anything reaches the
// wire: the destination never sees the operation, so it cannot have applied
// it. This is the ordering claim, not merely that an error was returned.
func TestRequiredPolicyRefusesBeforeAnythingIsSent(t *testing.T) {
	k := newKit(t, PolicyRequired, PolicyDisabled, func(id string, c *Config) {
		if id == "c_a" {
			c.HandshakeTimeout = 300 * time.Millisecond
		}
	})
	ctx := context.Background()
	// n_b has no guard behaviour at all: it never answers a key exchange,
	// which is exactly how a peer built before this capability behaves.
	err := k.a.g.Send(ctx, req(1, k.b.id, "fs.write", map[string]string{"path": "x", "data": "mutate me"}))
	if err == nil {
		t.Fatal("required policy sent to a peer that cannot negotiate")
	}
	var pe *proto.Error
	if !errors.As(err, &pe) || pe.Code != proto.CodeUnsupported {
		t.Fatalf("error = %v, want %s", err, proto.CodeUnsupported)
	}
	// Nothing but the unanswered key exchange was carried, and the peer above
	// the far guard was handed nothing at all.
	if n := k.relay.sealedFrames(); n != 0 {
		t.Fatalf("%d sealed frames reached the relay", n)
	}
	if n := k.relay.framesWithOp("fs.write"); n != 0 {
		t.Fatalf("%d plaintext operation frames reached the relay", n)
	}
	k.b.none(t, 200*time.Millisecond)
	if carried := k.relay.bytesCarried(); bytes.Contains(carried, []byte("mutate me")) {
		t.Fatal("the refused operation's payload reached the relay")
	}
	// The refusal is remembered briefly, so the second attempt is immediate
	// and still a refusal.
	if err := k.a.g.Send(ctx, req(2, k.b.id, "fs.write", nil)); err == nil {
		t.Fatal("second attempt succeeded")
	}
}

// Backward compatibility: a preferred policy talking to a peer that does not
// implement the capability keeps working, in plaintext, exactly as today.
func TestPreferredPolicyFallsBackToPlaintext(t *testing.T) {
	var fallbacks int
	var mu sync.Mutex
	k := newKit(t, PolicyPreferred, PolicyDisabled, func(id string, c *Config) {
		if id == "c_a" {
			c.HandshakeTimeout = 300 * time.Millisecond
			c.Observe = func(e Event) {
				mu.Lock()
				defer mu.Unlock()
				if e.Kind == EventFallback {
					fallbacks++
				}
			}
		}
	})
	ctx := context.Background()
	if err := k.a.g.Send(ctx, req(1, k.b.id, "fs.read", map[string]string{"path": "a"})); err != nil {
		t.Fatal(err)
	}
	got := k.b.next(t, 5*time.Second)
	if got.Op != "fs.read" {
		t.Fatalf("op = %q", got.Op)
	}
	mu.Lock()
	defer mu.Unlock()
	if fallbacks == 0 {
		t.Fatal("fallback was not reported")
	}
}

// A required policy also refuses inbound plaintext. Accepting a payload
// because it happens to parse would make the requirement advisory.
func TestRequiredPolicyRefusesInboundPlaintext(t *testing.T) {
	k := newKit(t, PolicyRequired, PolicyRequired)
	ctx := context.Background()
	if err := k.a.g.Send(ctx, req(1, k.b.id, "fs.read", nil)); err != nil {
		t.Fatal(err)
	}
	k.b.next(t, 5*time.Second)
	// The relay strips the sealing and forwards the shell of the frame.
	k.relay.setHook(func(f *proto.Frame) []*proto.Frame {
		if f.Op == proto.OpE2EESealed {
			return []*proto.Frame{{V: proto.Version, T: f.T, ID: f.ID, To: f.To, From: f.From, Op: "fs.write", Body: []byte{}}}
		}
		return []*proto.Frame{f}
	})
	if err := k.a.g.Send(ctx, req(2, k.b.id, "fs.read", nil)); err != nil {
		t.Fatal(err)
	}
	k.b.none(t, 500*time.Millisecond)
}

// A peer the control plane says is gone loses its keys immediately, so a
// connection that outlives many peers holds no key material for any of them.
func TestPeerGoneDiscardsKeys(t *testing.T) {
	k := newKit(t, PolicyRequired, PolicyRequired)
	ctx := context.Background()
	if err := k.a.g.Send(ctx, req(1, k.b.id, "fs.read", nil)); err != nil {
		t.Fatal(err)
	}
	k.b.next(t, 5*time.Second)
	if k.a.g.Session(k.b.id) == nil {
		t.Fatal("no session after a key agreement")
	}
	k.relay.deliver(ctx, proto.NewEvent(k.a.id, proto.EvPeerGone, map[string]string{"peer": k.b.id}))
	if f := k.a.next(t, 5*time.Second); f.Op != proto.EvPeerGone {
		t.Fatalf("op = %q", f.Op)
	}
	if s := k.a.g.Session(k.b.id); s != nil {
		t.Fatalf("keys for a gone peer survived: %s", s.ID())
	}
}

// Held sessions are bounded. A connection that talks to more peers than the
// bound keeps the bound, not the peers.
func TestSessionsAreBounded(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	k := newKit(t, PolicyRequired, PolicyRequired, func(id string, c *Config) {
		if id == "c_a" {
			c.MaxSessions = 2
		}
	})
	peers := []*end{k.b}
	for _, id := range []string{"n_c", "n_d"} {
		peers = append(peers, k.join(t, ctx, id, PolicyRequired))
	}
	for i, p := range peers {
		if err := k.a.g.Send(ctx, req(uint64(i+1), p.id, "fs.read", nil)); err != nil {
			t.Fatal(err)
		}
		p.next(t, 5*time.Second)
	}
	k.a.g.mu.Lock()
	held := len(k.a.g.sessions)
	k.a.g.mu.Unlock()
	if held > 2 {
		t.Fatalf("held %d sessions with a bound of 2", held)
	}
}

// A relay that keeps forging records loses the connection rather than being
// allowed to grind against the AEAD forever.
func TestRepeatedForgeriesCloseTheConnection(t *testing.T) {
	k := newKit(t, PolicyRequired, PolicyRequired, func(id string, c *Config) {
		if id == "n_b" {
			c.MaxAuthFailures = 4
		}
	})
	ctx := context.Background()
	if err := k.a.g.Send(ctx, req(1, k.b.id, "fs.read", nil)); err != nil {
		t.Fatal(err)
	}
	k.b.next(t, 5*time.Second)
	k.relay.setHook(func(f *proto.Frame) []*proto.Frame {
		if f.Op == proto.OpE2EESealed {
			body := append([]byte(nil), f.Body...)
			body[len(body)-1] ^= 0x80
			copyFrame := *f
			copyFrame.Body = body
			return []*proto.Frame{&copyFrame, &copyFrame, &copyFrame, &copyFrame, &copyFrame}
		}
		return []*proto.Frame{f}
	})
	if err := k.a.g.Send(ctx, req(2, k.b.id, "fs.read", nil)); err != nil {
		t.Fatal(err)
	}
	select {
	case _, open := <-k.b.in:
		if open {
			t.Fatal("a forged record was delivered")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the connection survived a run of forgeries")
	}
}

// A relay-generated unreachable response for a sealed request still reaches
// the caller as a failure of the operation it asked for, with the relay's own
// words discarded.
func TestRelayUnreachableIsReportedWithoutTrustingItsWords(t *testing.T) {
	k := newKit(t, PolicyRequired, PolicyRequired)
	ctx := context.Background()
	if err := k.a.g.Send(ctx, req(1, k.b.id, "fs.read", nil)); err != nil {
		t.Fatal(err)
	}
	k.b.next(t, 5*time.Second)
	k.relay.setHook(func(f *proto.Frame) []*proto.Frame {
		if f.Op == proto.OpE2EESealed && f.T == proto.KindReq {
			return []*proto.Frame{{V: proto.Version, T: proto.KindRes, ID: f.ID, To: f.From, From: proto.PeerControl,
				Op: proto.OpE2EESealed, Err: proto.Err(proto.CodeUnreachable, "attacker chosen text")}}
		}
		return []*proto.Frame{f}
	})
	if err := k.a.g.Send(ctx, req(9, k.b.id, "ws.exec", nil)); err != nil {
		t.Fatal(err)
	}
	f := k.a.next(t, 5*time.Second)
	if f.Op != "ws.exec" || f.Err == nil || f.Err.Code != proto.CodeUnreachable {
		t.Fatalf("frame = %+v", f)
	}
	if strings.Contains(f.Err.Msg, "attacker chosen") {
		t.Fatalf("relay-authored text reached the caller: %q", f.Err.Msg)
	}
}
