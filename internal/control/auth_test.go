package control

import (
	"context"
	"crypto/ed25519"
	"errors"
	"testing"

	"remount.dev/remount/internal/proto"
)

type fakeNodeAuthenticator struct {
	wantNode  string
	wantToken string
	identity  NodeIdentity
	err       error
	called    int
}

func (a *fakeNodeAuthenticator) AuthenticateNode(_ context.Context, node, token string, key []byte) (NodeIdentity, error) {
	a.called++
	if node != a.wantNode || token != a.wantToken || len(key) != ed25519.PublicKeySize {
		return NodeIdentity{}, errors.New("unexpected enrollment input")
	}
	return a.identity, a.err
}

func TestDynamicNodeEnrollmentOverridesHelloAuthorityAndEmitsOnce(t *testing.T) {
	auth := &fakeNodeAuthenticator{wantNode: "n_dynamic", wantToken: "enroll_secret", identity: NodeIdentity{
		Tenant: "tenant-a", Pool: "fly-iad", Labels: map[string]string{"region": "iad"}, Fresh: true,
	}}
	f := newControlFixture(t, "", func(opts *Options) {
		opts.NodeAuthenticator = auth
		opts.Token = "legacy-token-that-must-not-apply"
	})
	h, key := signedNodeHello(t, "n_dynamic", "enroll_secret", processNodeInfo(1024))
	h.Labels = map[string]string{"tenant": "attacker", "pool": "attacker", "region": "evil"}
	h.Proof = ed25519.Sign(key, proto.HelloProofBytes(h))
	id, _, err := f.c.Authenticate(context.Background(), &h)
	if err != nil || id != "n_dynamic" || auth.called != 1 {
		t.Fatalf("authenticate = %q, calls=%d, err=%v", id, auth.called, err)
	}
	if h.Labels["tenant"] != "tenant-a" || h.Labels["pool"] != "fly-iad" || h.Labels["region"] != "iad" {
		t.Fatalf("authoritative labels = %#v", h.Labels)
	}
	f.c.PeerConnected(context.Background(), id, &h)
	events, err := f.log.Read(context.Background(), 0, "", 10)
	if err != nil {
		t.Fatal(err)
	}
	var enrolled int
	for _, event := range events {
		if event.Type == proto.EvNodeEnrolled {
			enrolled++
		}
	}
	if enrolled != 0 {
		t.Fatalf("control duplicated authenticator-owned enrollment event: %+v", events)
	}
}

func TestDynamicNodeEnrollmentRejectsUnenforcedBackend(t *testing.T) {
	auth := &fakeNodeAuthenticator{wantNode: "n_weak", wantToken: "enroll_secret", identity: NodeIdentity{Tenant: "tenant-a"}}
	f := newControlFixture(t, "", func(opts *Options) {
		opts.NodeAuthenticator = auth
		opts.SecurityProfileFloor = proto.SecurityIsolated
	})
	h, _ := signedNodeHello(t, "n_weak", "enroll_secret", processNodeInfo(1024))
	if _, _, err := f.c.Authenticate(context.Background(), &h); err == nil {
		t.Fatal("dynamic enrollment accepted a backend below the deployment floor")
	}
}
