package e2ee

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"remount.dev/remount/internal/proto"
)

// A binding is a dated credential; a node's uplink is not. A connection that
// is still healthy after its binding expires must be able to negotiate again,
// because the control plane would sign a fresh binding on request.
//
// Caching the first binding for the life of the connection makes a required
// policy refuse every peer-addressed frame forever, and makes a preferred
// policy fall back to plaintext forever — a silent loss of the property the
// package exists to provide. Neither is a failure of the far peer; both are
// this side holding an expired credential it never renewed.
func TestAnExpiredBindingIsRebound(t *testing.T) {
	var offset atomic.Int64
	clock := func() time.Time { return time.Now().Add(time.Duration(offset.Load())) }
	var binds atomic.Int64

	var k *kit
	k = newKit(t, PolicyRequired, PolicyRequired, func(id string, c *Config) {
		c.Now = clock
		c.HandshakeTimeout = 2 * time.Second
		// The control plane signs against the same wall clock both peers read,
		// and issues an hour-long binding.
		c.Bind = func(_ context.Context, self string) (*Identity, error) {
			binds.Add(1)
			identity, err := NewIdentity(self, "t_one", clock(), time.Hour)
			if err != nil {
				return nil, err
			}
			identity.Binding = SignBinding(k.controlKey, identity.Binding)
			return identity, nil
		}
	})
	ctx := context.Background()

	if err := k.a.g.Send(ctx, req(1, k.b.id, "fs.read", nil)); err != nil {
		t.Fatal(err)
	}
	k.b.next(t, 5*time.Second)
	first := binds.Load()
	if first == 0 {
		t.Fatal("no binding was minted")
	}

	// The peers part company, which is the ordinary reason to renegotiate.
	k.relay.deliver(ctx, proto.NewEvent(k.a.id, proto.EvPeerGone, map[string]string{"peer": k.b.id}))
	k.a.next(t, 5*time.Second)
	k.relay.deliver(ctx, proto.NewEvent(k.b.id, proto.EvPeerGone, map[string]string{"peer": k.a.id}))
	k.b.next(t, 5*time.Second)

	// Two hours later, on a connection that never dropped, the bindings both
	// peers hold have expired.
	offset.Store(int64(2 * time.Hour))
	if err := k.a.g.Send(ctx, req(2, k.b.id, "fs.read", nil)); err != nil {
		t.Fatalf("a connection that outlived its binding could not encrypt again: %v", err)
	}
	got := k.b.next(t, 5*time.Second)
	if got.Op != "fs.read" || got.ID != 2 {
		t.Fatalf("delivered frame = %+v", got)
	}
	if binds.Load() <= first {
		t.Fatal("the expired binding was reused rather than renewed")
	}
}

// Renewal is not per-frame: a binding that is still good is reused, so the
// control plane is not asked to sign one for every key agreement.
func TestAUsableBindingIsNotRebound(t *testing.T) {
	var binds atomic.Int64
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var k *kit
	count := func(id string, c *Config) {
		c.Bind = func(_ context.Context, self string) (*Identity, error) {
			if self == "c_a" {
				binds.Add(1)
			}
			identity, err := NewIdentity(self, "t_one", time.Now(), time.Hour)
			if err != nil {
				return nil, err
			}
			identity.Binding = SignBinding(k.controlKey, identity.Binding)
			return identity, nil
		}
	}
	k = newKit(t, PolicyRequired, PolicyRequired, count)
	third := k.join(t, ctx, "n_c", PolicyRequired, count)
	for i, peer := range []*end{k.b, third} {
		if err := k.a.g.Send(ctx, req(uint64(i+1), peer.id, "fs.read", nil)); err != nil {
			t.Fatal(err)
		}
		peer.next(t, 5*time.Second)
	}
	// c_a agreed two sessions against one binding that was good for both.
	if binds.Load() != 1 {
		t.Fatalf("c_a minted %d bindings for two key agreements, want 1", binds.Load())
	}
}
