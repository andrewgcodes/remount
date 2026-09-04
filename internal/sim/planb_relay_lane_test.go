package sim

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"remount.dev/remount/internal/client"
	"remount.dev/remount/internal/e2ee"
	"remount.dev/remount/internal/node"
	"remount.dev/remount/internal/proto"
	"remount.dev/remount/internal/transport"
)

// The Plan B B6 lane puts an adversary in the relay's position on every peer
// connection: it records everything it carries and can drop, reorder,
// duplicate, rewrite or redirect any frame. The relay behind it is the real
// one, so a redirected frame really is routed to the peer the adversary chose.
//
// Nothing here changes the control plane, the node or the client. Both peers
// dial through a guarded dialer, which is the whole opt-in.

// ---------------------------------------------------------------------------
// the adversary
// ---------------------------------------------------------------------------

// tap is the adversary on one peer's connection. up is the direction from the
// peer toward the relay, down is the direction from the relay toward the peer;
// each hook may return zero frames (a drop), one (a rewrite) or several (a
// duplicate or a reorder).
type tap struct {
	mu       sync.Mutex
	capture  [][]byte
	peerOnly [][]byte // frames routed between two peers, never touching control
	sealed   int
	ops      map[string]int
	up, down func(f *proto.Frame) []*proto.Frame
}

func newTap() *tap { return &tap{ops: map[string]int{}} }

// record files a frame under the whole capture and, when it is peer-to-peer
// traffic, under the capture the relay is not supposed to be able to read.
// Control-plane traffic is deliberately readable: the control plane is a
// party to it, and this lane does not pretend otherwise.
func (t *tap) record(f *proto.Frame, peerToPeer bool) {
	b, err := proto.EncodeFrame(f)
	if err != nil {
		return
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	t.capture = append(t.capture, b)
	if peerToPeer {
		t.peerOnly = append(t.peerOnly, b)
		t.ops[f.Op]++
		if f.Op == proto.OpE2EESealed {
			t.sealed++
		}
	}
}

func (t *tap) apply(hook func(f *proto.Frame) []*proto.Frame, f *proto.Frame) []*proto.Frame {
	if hook == nil {
		return []*proto.Frame{f}
	}
	return hook(f)
}

func (t *tap) setUp(hook func(f *proto.Frame) []*proto.Frame) {
	t.mu.Lock()
	t.up = hook
	t.mu.Unlock()
}

func (t *tap) hooks() (up, down func(f *proto.Frame) []*proto.Frame) {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.up, t.down
}

func (t *tap) counts() (sealed int, ops map[string]int) {
	t.mu.Lock()
	defer t.mu.Unlock()
	out := make(map[string]int, len(t.ops))
	for k, v := range t.ops {
		out[k] = v
	}
	return t.sealed, out
}

// bytesCarried is the relay capture: every frame the adversary handled, in
// the wire encoding, which is what an operator dumping relay traffic holds.
func (t *tap) bytesCarried() []byte { return joined(t, false) }

// peerBytesCarried is the part of that capture that travels between two
// peers, which is the part end-to-end encryption is responsible for.
func (t *tap) peerBytesCarried() []byte { return joined(t, true) }

func joined(t *tap, peerOnly bool) []byte {
	t.mu.Lock()
	defer t.mu.Unlock()
	frames := t.capture
	if peerOnly {
		frames = t.peerOnly
	}
	var out bytes.Buffer
	for _, b := range frames {
		out.Write(b)
	}
	return out.Bytes()
}

// tappedConn is the server side of one peer's pipe, seen through the tap.
type tappedConn struct {
	inner transport.Conn
	tap   *tap
	queue []*proto.Frame
}

func (c *tappedConn) Recv(ctx context.Context) (*proto.Frame, error) {
	for {
		if len(c.queue) > 0 {
			f := c.queue[0]
			c.queue = c.queue[1:]
			return f, nil
		}
		f, err := c.inner.Recv(ctx)
		if err != nil {
			return nil, err
		}
		// Toward the relay the sender has not been stamped yet, so the
		// destination decides whether this is peer-to-peer traffic.
		c.tap.record(f, peerAddressed(f.To))
		up, _ := c.tap.hooks()
		c.queue = append(c.queue, c.tap.apply(up, f)...)
	}
}

func (c *tappedConn) Send(ctx context.Context, f *proto.Frame) error {
	c.tap.record(f, peerAddressed(f.From))
	_, down := c.tap.hooks()
	for _, g := range c.tap.apply(down, f) {
		if err := c.inner.Send(ctx, g); err != nil {
			return err
		}
	}
	return nil
}

func (c *tappedConn) Close() error { return c.inner.Close() }

// ---------------------------------------------------------------------------
// the lane
// ---------------------------------------------------------------------------

// lane is a world whose peers dial through the adversary and, optionally,
// through the confidentiality guard.
type lane struct {
	t          *testing.T
	w          *world
	controlKey ed25519.PrivateKey

	mu     sync.Mutex
	taps   map[string]*tap
	events map[string][]e2ee.Event
}

func newLane(t *testing.T) *lane {
	t.Helper()
	_, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	return &lane{t: t, w: newWorld(t), controlKey: priv, taps: map[string]*tap{}, events: map[string][]e2ee.Event{}}
}

func (l *lane) tap(who string) *tap {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.taps[who] == nil {
		l.taps[who] = newTap()
	}
	return l.taps[who]
}

// peerAddressed mirrors the guard's own rule for which frames are between two
// peers rather than with the control plane.
func peerAddressed(id string) bool { return id != "" && id != proto.PeerControl }

// captured is every byte the adversary carried on every connection.
func (l *lane) captured() []byte {
	l.mu.Lock()
	taps := make([]*tap, 0, len(l.taps))
	for _, t := range l.taps {
		taps = append(taps, t)
	}
	l.mu.Unlock()
	var out bytes.Buffer
	for _, t := range taps {
		out.Write(t.bytesCarried())
	}
	return out.Bytes()
}

// capturedPeerTraffic is the part of the capture that travelled between two
// peers.
func (l *lane) capturedPeerTraffic() []byte {
	l.mu.Lock()
	taps := make([]*tap, 0, len(l.taps))
	for _, t := range l.taps {
		taps = append(taps, t)
	}
	l.mu.Unlock()
	var out bytes.Buffer
	for _, t := range taps {
		out.Write(t.peerBytesCarried())
	}
	return out.Bytes()
}

func (l *lane) observed(who string) []e2ee.Event {
	l.mu.Lock()
	defer l.mu.Unlock()
	return append([]e2ee.Event(nil), l.events[who]...)
}

// dialer is the world's in-memory dialer with the adversary spliced in on the
// server side, so w.cut still severs the peer's connections.
func (l *lane) dialer(who string) transport.Dialer {
	t := l.tap(who)
	return transport.DialFunc(func(ctx context.Context) (transport.Conn, error) {
		peerSide, relaySide := transport.Pipe(256)
		go l.w.srv.AcceptConn(l.w.ctx, &tappedConn{inner: relaySide, tap: t})
		f := &fault{Conn: peerSide, who: who}
		l.w.mu.Lock()
		l.w.conns = append(l.w.conns, f)
		l.w.mu.Unlock()
		return f, nil
	})
}

// guarded wraps the lane's dialer in the confidentiality guard. The identity
// is minted here because a client's peer id is assigned at hello; in a
// deployment this callback is a control-plane round trip that returns a
// binding signed with the same key nodes already verify grants with.
func (l *lane) guarded(who string, policy e2ee.Policy) transport.Dialer {
	cfg := e2ee.Config{
		Policy:           policy,
		ControlKey:       l.controlKey.Public().(ed25519.PublicKey),
		HandshakeTimeout: 3 * time.Second,
		Bind: func(_ context.Context, self string) (*e2ee.Identity, error) {
			id, err := e2ee.NewIdentity(self, "t_sim", time.Now(), time.Hour)
			if err != nil {
				return nil, err
			}
			id.Binding = e2ee.SignBinding(l.controlKey, id.Binding)
			return id, nil
		},
		Observe: func(e e2ee.Event) {
			l.mu.Lock()
			l.events[who] = append(l.events[who], e)
			l.mu.Unlock()
		},
	}
	return e2ee.Dialer(l.dialer(who), cfg)
}

func (l *lane) node(name string, policy e2ee.Policy) *node.Node {
	l.t.Helper()
	return l.w.nodeWith(name, func(o *node.Options) { o.Dialer = l.guarded(name, policy) })
}

func (l *lane) client(name string, policy e2ee.Policy) *client.Client {
	l.t.Helper()
	c := client.New(client.Options{
		Dialer: l.guarded(name, policy), Token: "tok", Principal: "a_" + name,
		ArtifactURL: l.w.http.URL + "/v1/artifacts",
	})
	l.t.Cleanup(func() { c.Close() })
	return c
}

// sessionsEstablished is every distinct key agreement a peer completed.
func (l *lane) sessionsEstablished(who string) []string {
	var out []string
	seen := map[string]bool{}
	for _, e := range l.observed(who) {
		if e.Kind != e2ee.EventEstablished && e.Kind != e2ee.EventReplaced {
			continue
		}
		if !seen[e.Session] {
			seen[e.Session] = true
			out = append(out, e.Session)
		}
	}
	return out
}

func countDrops(events []e2ee.Event, substring string) int {
	n := 0
	for _, e := range events {
		if e.Kind == e2ee.EventDropped && bytes.Contains([]byte(e.Reason), []byte(substring)) {
			n++
		}
	}
	return n
}

// planBRelayCanary is a synthetic credential-shaped string. It is never a
// real key; it exists so a scan that finds nothing has first been shown to
// find something.
const planBRelayCanary = "sk-remount-planb-b6-canary-0123456789abcdef"

// planBRelayWorkload writes the canary through a node operation and reads it
// back through session output, so the canary travels in a request body, a
// response body and a chunk stream. None of it goes to the control plane.
func planBRelayWorkload(t *testing.T, ctx context.Context, c *client.Client, wsID string) {
	t.Helper()
	if err := c.WriteFile(ctx, wsID, "creds.txt", []byte(planBRelayCanary+"\n"), 0o600, client.WithIdempotencyKey("b6-write")); err != nil {
		t.Fatal(err)
	}
	back, err := c.ReadFile(ctx, wsID, "creds.txt")
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Contains(back, []byte(planBRelayCanary)) {
		t.Fatal("the workload did not move the canary; the scan below would prove nothing")
	}
	out, _, exit, err := c.Run(ctx, wsID, "cat", "creds.txt")
	if err != nil {
		t.Fatal(err)
	}
	if exit == nil || exit.Code != 0 || !bytes.Contains(out, []byte(planBRelayCanary)) {
		t.Fatalf("session output did not carry the canary: exit=%+v out=%q", exit, out)
	}
}

// ---------------------------------------------------------------------------
// B24
// ---------------------------------------------------------------------------

// TestPlanBRelayCaptureHasNoPlaintextPayload is Plan B B24. The same workload
// runs twice through the same scan. With confidentiality off the scan finds
// the canary, which is what makes its silence with confidentiality on mean
// anything at all.
func TestPlanBRelayCaptureHasNoPlaintextPayload(t *testing.T) {
	ctx := ctxT(t, 90*time.Second)

	// Positive control: the identical lane with the guard disabled.
	control := newLane(t)
	control.node("n1", e2ee.PolicyDisabled)
	cc := control.client("c1", e2ee.PolicyDisabled)
	controlWS := mustWS(t, cc, proto.WorkspaceSpec{Name: "b6-control"})
	planBRelayWorkload(t, ctx, cc, controlWS.ID)
	plain := control.captured()
	hits := bytes.Count(plain, []byte(planBRelayCanary))
	if hits == 0 {
		t.Fatal("the capture scan cannot find its own canary with encryption off; it proves nothing with encryption on")
	}
	t.Logf("positive control: %d plaintext canary occurrences in %d captured bytes", hits, len(plain))

	// The same lane, sealed.
	sealed := newLane(t)
	sealed.node("n1", e2ee.PolicyRequired)
	sc := sealed.client("c1", e2ee.PolicyRequired)
	sealedWS := mustWS(t, sc, proto.WorkspaceSpec{Name: "b6-sealed"})
	planBRelayWorkload(t, ctx, sc, sealedWS.ID)
	capture := sealed.captured()
	if len(capture) < len(plain)/2 {
		t.Fatalf("the sealed capture is implausibly small (%d bytes against %d); the scan is not seeing the traffic", len(capture), len(plain))
	}
	if n := bytes.Count(capture, []byte(planBRelayCanary)); n != 0 {
		t.Fatalf("relay capture contains the canary %d times", n)
	}
	// The operation names and the file path are payload too: the relay routes
	// by peer id and needs none of them.
	peerTraffic := sealed.capturedPeerTraffic()
	if len(peerTraffic) == 0 {
		t.Fatal("no peer-to-peer traffic was captured; the scan is looking at nothing")
	}
	for _, hidden := range []string{proto.OpFSWrite, proto.OpFSRead, "creds.txt"} {
		if bytes.Contains(peerTraffic, []byte(hidden)) {
			t.Fatalf("peer-to-peer relay capture contains %q", hidden)
		}
	}
	clientSealed, clientOps := sealed.tap("c1").counts()
	nodeSealed, _ := sealed.tap("n1").counts()
	if clientSealed == 0 || nodeSealed == 0 {
		t.Fatalf("nothing was sealed: client=%d node=%d", clientSealed, nodeSealed)
	}
	if clientOps[proto.OpFSWrite] != 0 {
		t.Fatalf("a plaintext %s frame reached the relay", proto.OpFSWrite)
	}
	t.Logf("sealed lane: %d sealed frames on the client connection, %d on the node connection", clientSealed, nodeSealed)
}

// ---------------------------------------------------------------------------
// B25
// ---------------------------------------------------------------------------

// TestPlanBRelayRejectsMutationReplayAndSubstitution is Plan B B25. A relay
// that rewrites, duplicates or redirects a sealed frame achieves nothing an
// honest relay could not achieve by dropping it.
func TestPlanBRelayRejectsMutationReplayAndSubstitution(t *testing.T) {
	ctx := ctxT(t, 120*time.Second)
	l := newLane(t)
	first := l.node("n1", e2ee.PolicyRequired)
	second := l.node("n2", e2ee.PolicyRequired)
	c := l.client("c1", e2ee.PolicyRequired)
	ws := mustWS(t, c, proto.WorkspaceSpec{Name: "b6-tamper", Placement: proto.Placement{Node: first.ID()}})

	// Warm the key agreement with an honest call.
	if err := c.WriteFile(ctx, ws.ID, "warm.txt", []byte("warm"), 0o600); err != nil {
		t.Fatal(err)
	}

	// Mutation. One flipped ciphertext byte on the next request from the
	// client. The node must not act on it.
	var mutated bool
	var mu sync.Mutex
	l.tap("c1").setUp(func(f *proto.Frame) []*proto.Frame {
		mu.Lock()
		defer mu.Unlock()
		if mutated || f.Op != proto.OpE2EESealed || f.T != proto.KindReq || len(f.Body) == 0 {
			return []*proto.Frame{f}
		}
		var sealed proto.E2EESealed
		if err := f.Decode(&sealed); err != nil || len(sealed.CT) == 0 {
			return []*proto.Frame{f}
		}
		mutated = true
		// Flip a ciphertext bit, leaving the framing intact, so the refusal
		// is the authenticator's and not the decoder's.
		sealed.CT[len(sealed.CT)-1] ^= 0x01
		copyFrame := *f
		copyFrame.Body = proto.MustMarshal(sealed)
		return []*proto.Frame{&copyFrame}
	})
	short, cancel := context.WithTimeout(ctx, 3*time.Second)
	err := c.WriteFile(short, ws.ID, "mutated.txt", []byte("must not land"), 0o600)
	cancel()
	if err == nil {
		t.Fatal("a mutated request was applied")
	}
	l.tap("c1").setUp(nil)
	if _, err := c.Stat(ctx, ws.ID, "mutated.txt"); err == nil {
		t.Fatal("the mutated request changed the workspace")
	}
	if n := countDrops(l.observed("n1"), "message authentication failed"); n == 0 {
		t.Fatalf("the node did not report a refused record: %+v", l.observed("n1"))
	}

	// Replay. Every sealed request from the client is delivered twice; the
	// node opens each record once.
	l.tap("c1").setUp(func(f *proto.Frame) []*proto.Frame {
		if f.Op == proto.OpE2EESealed && f.T == proto.KindReq {
			return []*proto.Frame{f, f}
		}
		return []*proto.Frame{f}
	})
	if err := c.WriteFile(ctx, ws.ID, "replayed.txt", []byte("once"), 0o600); err != nil {
		t.Fatal(err)
	}
	l.tap("c1").setUp(nil)
	if n := countDrops(l.observed("n1"), "replayed"); n == 0 {
		t.Fatalf("the node accepted a replayed record: %+v", l.observed("n1"))
	}
	body, err := c.ReadFile(ctx, ws.ID, "replayed.txt")
	if err != nil || string(body) != "once" {
		t.Fatalf("replay disturbed the result: %q %v", body, err)
	}

	// Cross-destination substitution. The client's sealed frames for n1 are
	// re-addressed to n2, which really does receive them through the real
	// relay and must refuse every one.
	before := len(l.observed("n2"))
	l.tap("c1").setUp(func(f *proto.Frame) []*proto.Frame {
		if f.Op == proto.OpE2EESealed && f.T == proto.KindReq {
			copyFrame := *f
			copyFrame.To = second.ID()
			return []*proto.Frame{&copyFrame}
		}
		return []*proto.Frame{f}
	})
	short, cancel = context.WithTimeout(ctx, 3*time.Second)
	err = c.WriteFile(short, ws.ID, "redirected.txt", []byte("wrong node"), 0o600)
	cancel()
	if err == nil {
		t.Fatal("a redirected request was applied")
	}
	l.tap("c1").setUp(nil)
	if n := countDrops(l.observed("n2")[before:], ""); n == 0 {
		t.Fatalf("the substituted destination did not refuse the record: %+v", l.observed("n2")[before:])
	}
	if _, err := c.Stat(ctx, ws.ID, "redirected.txt"); err == nil {
		t.Fatal("the redirected request changed the workspace")
	}
}

// ---------------------------------------------------------------------------
// B26
// ---------------------------------------------------------------------------

// TestPlanBRelayReconnectRekeysWithoutLosingCursorOrIdempotency is Plan B
// B26. Cutting a client mid-stream forces a new connection and therefore a
// new key agreement. The session cursor and the idempotency contract are the
// features reconnect exists for; adding confidentiality must not cost them.
func TestPlanBRelayReconnectRekeysWithoutLosingCursorOrIdempotency(t *testing.T) {
	ctx := ctxT(t, 120*time.Second)
	l := newLane(t)
	l.node("n1", e2ee.PolicyRequired)
	c := l.client("c1", e2ee.PolicyRequired)
	ws := mustWS(t, c, proto.WorkspaceSpec{Name: "b6-reconnect"})

	// An idempotent mutation before the cuts. Its replay after them must be a
	// no-op, and its key must still refuse different arguments.
	if err := c.WriteFile(ctx, ws.ID, "idem.txt", []byte("first"), 0o600, client.WithIdempotencyKey("b6-idem")); err != nil {
		t.Fatal(err)
	}

	session, err := c.Exec(ctx, proto.SOpenReq{WS: ws.ID, Program: []string{"sh", "-c",
		"i=0; while [ $i -lt 1500 ]; do echo line-$i; i=$((i+1)); if [ $((i % 150)) -eq 0 ]; then sleep 0.02; fi; done"}})
	if err != nil {
		t.Fatal(err)
	}
	var got bytes.Buffer
	var seqs []uint64
	cuts := 0
	for chunk := range session.Chunks() {
		switch chunk.Stream {
		case proto.StreamStdout:
			got.Write(chunk.Data)
			seqs = append(seqs, chunk.Seq)
		case proto.StreamGap:
			t.Fatal("a gap appeared; the cursor was not honoured across the rekey")
		}
		if got.Len() > 1500*(cuts+1) && cuts < 3 {
			cuts++
			l.w.cut("c1")
		}
	}
	if cuts == 0 {
		t.Fatal("the test never cut the connection")
	}
	var want bytes.Buffer
	for i := 0; i < 1500; i++ {
		fmt.Fprintf(&want, "line-%d\n", i)
	}
	if got.String() != want.String() {
		t.Fatalf("output differs after %d cuts: %d bytes against %d", cuts, got.Len(), want.Len())
	}
	for i := 1; i < len(seqs); i++ {
		if seqs[i] <= seqs[i-1] {
			t.Fatalf("cursor went backwards at %d: %d after %d", i, seqs[i], seqs[i-1])
		}
	}
	if session.Exit() == nil || session.Exit().Code != 0 {
		t.Fatalf("exit = %+v", session.Exit())
	}

	// Every reconnect produced a distinct key agreement, and none was reused.
	established := l.sessionsEstablished("c1")
	if len(established) < cuts+1 {
		t.Fatalf("%d key agreements for %d connections", len(established), cuts+1)
	}
	seen := map[string]bool{}
	for _, id := range established {
		if seen[id] {
			t.Fatalf("session id %s was reused across a reconnect", id)
		}
		seen[id] = true
	}

	// The idempotency contract survived the rekey in both directions: a
	// replay is a no-op and a changed argument under the same key is refused.
	if err := c.WriteFile(ctx, ws.ID, "idem.txt", []byte("first"), 0o600, client.WithIdempotencyKey("b6-idem")); err != nil {
		t.Fatalf("replay after rekey: %v", err)
	}
	if err := c.WriteFile(ctx, ws.ID, "idem.txt", []byte("second"), 0o600, client.WithIdempotencyKey("b6-idem")); err == nil {
		t.Fatal("idempotency key reuse with different arguments succeeded after a rekey")
	}
	body, err := c.ReadFile(ctx, ws.ID, "idem.txt")
	if err != nil || string(body) != "first" {
		t.Fatalf("idempotent write was applied twice: %q %v", body, err)
	}
}

// ---------------------------------------------------------------------------
// B27
// ---------------------------------------------------------------------------

// TestPlanBRelayRequiredEncryptionFailsBeforeMutation is Plan B B27. The
// claim is an ordering claim: when the policy requires end-to-end encryption
// and the peer cannot negotiate it, nothing changed, because nothing was
// sent.
func TestPlanBRelayRequiredEncryptionFailsBeforeMutation(t *testing.T) {
	ctx := ctxT(t, 90*time.Second)
	l := newLane(t)
	// The node is a peer built before the capability: it answers no key
	// exchange at all.
	l.node("n1", e2ee.PolicyDisabled)
	strict := l.client("c1", e2ee.PolicyRequired)
	relaxed := l.client("c2", e2ee.PolicyDisabled)
	ws := mustWS(t, relaxed, proto.WorkspaceSpec{Name: "b6-required"})

	err := strict.WriteFile(ctx, ws.ID, "must-not-exist.txt", []byte(planBRelayCanary), 0o600, client.WithIdempotencyKey("b6-required"))
	if err == nil {
		t.Fatal("a required-encryption client mutated a workspace through a peer that cannot negotiate")
	}
	var pe *proto.Error
	if !errors.As(err, &pe) || pe.Code != proto.CodeUnsupported {
		t.Fatalf("error = %v, want %s", err, proto.CodeUnsupported)
	}

	// Nothing was sent, so nothing was applied. Both halves are asserted: the
	// wire never carried the operation, and the workspace never grew the file.
	sealed, ops := l.tap("c1").counts()
	if sealed != 0 || ops[proto.OpFSWrite] != 0 {
		t.Fatalf("the refused operation reached the relay: %d sealed, %d %s frames", sealed, ops[proto.OpFSWrite], proto.OpFSWrite)
	}
	if bytes.Contains(l.captured(), []byte(planBRelayCanary)) {
		t.Fatal("the refused operation's payload reached the relay")
	}
	if _, err := relaxed.Stat(ctx, ws.ID, "must-not-exist.txt"); err == nil {
		t.Fatal("the refused operation changed the workspace")
	}
	// The workspace is otherwise healthy: a peer that does not require
	// encryption keeps working exactly as before.
	if err := relaxed.WriteFile(ctx, ws.ID, "fine.txt", []byte("fine"), 0o600); err != nil {
		t.Fatalf("an unencrypted peer stopped working: %v", err)
	}
}
