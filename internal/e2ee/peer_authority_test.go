package e2ee

import (
	"context"
	"testing"
	"time"

	"remount.dev/remount/internal/proto"
)

// peer.gone is a statement about the fleet, and only the control plane makes
// it. A peer that forges one in the clear must not be able to destroy another
// peer's keys, and under a required policy its frame must be refused like any
// other plaintext peer payload rather than handed to the layer above.
//
// The layer above acts on this event: a node's handler for peer.gone cancels
// that client's subscriptions and drops its grants. A peer that could forge it
// would reach that handler without ever holding a key.
func TestForgedPeerGoneFromAPeerIsRefused(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	k := newKit(t, PolicyRequired, PolicyRequired)
	third := k.join(t, ctx, "n_c", PolicyRequired)

	// c_a agrees a session with n_c, so the assertion below is about the
	// forgery and not about a key that never existed.
	if err := k.a.g.Send(ctx, req(1, third.id, "fs.read", nil)); err != nil {
		t.Fatal(err)
	}
	third.next(t, 5*time.Second)
	if k.a.g.Session(third.id) == nil {
		t.Fatal("no session with n_c after a key agreement")
	}

	// n_b claims n_c is gone. The relay stamps From, so this is exactly what a
	// hostile peer can put on the wire; it needs no key and no relay control.
	k.relay.deliver(ctx, &proto.Frame{
		V: proto.Version, T: proto.KindEvent, To: k.a.id, From: k.b.id,
		Op: proto.EvPeerGone, Body: proto.MustMarshal(map[string]string{"peer": third.id}),
	})

	k.a.none(t, 500*time.Millisecond)
	if k.a.g.Session(third.id) == nil {
		t.Fatal("a peer forged peer.gone and destroyed another peer's keys")
	}

	// The control plane's own peer.gone still works: it arrives with no peer
	// in From, which is the only source that speaks for the fleet.
	k.relay.deliver(ctx, proto.NewEvent(k.a.id, proto.EvPeerGone, map[string]string{"peer": third.id}))
	if f := k.a.next(t, 5*time.Second); f.Op != proto.EvPeerGone {
		t.Fatalf("op = %q", f.Op)
	}
	if s := k.a.g.Session(third.id); s != nil {
		t.Fatalf("keys for a gone peer survived: %s", s.ID())
	}
}

// The same forgery under a preferred policy reaches the peer above, because
// preferred accepts plaintext by definition, but it still may not reach into
// this layer's key state: a peer is not the authority on another peer.
func TestForgedPeerGoneNeverDiscardsKeysUnderAPreferredPolicy(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	k := newKit(t, PolicyPreferred, PolicyPreferred)
	third := k.join(t, ctx, "n_c", PolicyPreferred)

	if err := k.a.g.Send(ctx, req(1, third.id, "fs.read", nil)); err != nil {
		t.Fatal(err)
	}
	third.next(t, 5*time.Second)
	if k.a.g.Session(third.id) == nil {
		t.Fatal("no session with n_c after a key agreement")
	}
	k.relay.deliver(ctx, &proto.Frame{
		V: proto.Version, T: proto.KindEvent, To: k.a.id, From: k.b.id,
		Op: proto.EvPeerGone, Body: proto.MustMarshal(map[string]string{"peer": third.id}),
	})
	if f := k.a.next(t, 5*time.Second); f.Op != proto.EvPeerGone {
		t.Fatalf("op = %q", f.Op)
	}
	if k.a.g.Session(third.id) == nil {
		t.Fatal("a peer forged peer.gone and destroyed another peer's keys")
	}
}
