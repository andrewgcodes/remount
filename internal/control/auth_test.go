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
		Tenant: "tenant-a", Pool: "fly-iad", Labels: map[string]string{"region": "iad"}, Fresh: true, Token: "node-access",
	}}
	f := newControlFixture(t, "", func(opts *Options) {
		opts.NodeAuthenticator = auth
		opts.Token = "legacy-token-that-must-not-apply"
	})
	h, key := signedNodeHello(t, "n_dynamic", "enroll_secret", processNodeInfo(1024))
	h.Labels = map[string]string{"tenant": "attacker", "pool": "attacker", "region": "evil"}
	h.Proof = ed25519.Sign(key, proto.HelloProofBytes(h))
	id, ok, err := f.c.Authenticate(context.Background(), &h)
	if err != nil || id != "n_dynamic" || auth.called != 1 {
		t.Fatalf("authenticate = %q, calls=%d, err=%v", id, auth.called, err)
	}
	if h.Labels["tenant"] != "tenant-a" || h.Labels["pool"] != "fly-iad" || h.Labels["region"] != "iad" {
		t.Fatalf("authoritative labels = %#v", h.Labels)
	}
	if ok.NodeToken != "node-access" {
		t.Fatalf("node token = %q", ok.NodeToken)
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
	// The floor is decided entirely from the hello's own descriptors, so it
	// must be decided BEFORE the one-time credential is spent. Otherwise an
	// operator who starts a node with the wrong backend burns a credential
	// per attempt and has to mint a new one to try again — which is exactly
	// what a live production-single-tenant run on a host with only the
	// process backend did.
	if auth.called != 0 {
		t.Fatalf("a hello refused for its backend consumed the one-time enrollment (%d calls)", auth.called)
	}
}

// A hello with no NodeInfo at all is the same class of refusal: it can be
// decided without the credential, so it must not spend it.
func TestDynamicNodeEnrollmentWithoutDescriptorsKeepsTheCredential(t *testing.T) {
	auth := &fakeNodeAuthenticator{wantNode: "n_bare", wantToken: "enroll_secret", identity: NodeIdentity{Tenant: "tenant-a"}}
	f := newControlFixture(t, "", func(opts *Options) {
		opts.NodeAuthenticator = auth
		opts.SecurityProfileFloor = proto.SecurityIsolated
	})
	h, key := signedNodeHello(t, "n_bare", "enroll_secret", processNodeInfo(1024))
	h.Node = nil
	h.Proof = ed25519.Sign(key, proto.HelloProofBytes(h))
	if _, _, err := f.c.Authenticate(context.Background(), &h); err == nil {
		t.Fatal("a hello with no backend descriptors was accepted")
	}
	if auth.called != 0 {
		t.Fatalf("a hello with no backend descriptors consumed the enrollment (%d calls)", auth.called)
	}
}

// tokenSubjects authenticates each token as a distinct subject, so a test can
// present two different authenticated principals to one control plane.
type tokenSubjects map[string]Subject

func (a tokenSubjects) Authenticate(_ context.Context, cred Credential) (Subject, error) {
	subject, ok := a[cred.Token]
	if !ok {
		return Subject{}, errors.New("unknown token")
	}
	return subject, nil
}

// A client chooses its own peer id, and the relay replaces a peer that
// reconnects under an id it already holds. So an id claimed by a second,
// differently-authenticated subject would evict the holder and take delivery
// of replies addressed to it — across tenants. The claim must be refused
// while the id is bound.
func TestAClaimedClientPeerIDCannotBeReboundToAnotherSubject(t *testing.T) {
	f := newControlFixture(t, "", func(opts *Options) {
		opts.Authenticator = tokenSubjects{
			"tok-alice":   {ID: "alice", Tenant: "tenant-a", Roles: []string{"agent"}},
			"tok-mallory": {ID: "mallory", Tenant: "tenant-b", Roles: []string{"agent"}},
			"tok-alice-2": {ID: "alice", Tenant: "tenant-a", Roles: []string{"agent"}},
		}
		opts.Token = ""
	})
	ctx := context.Background()
	hello := func(token string) *proto.Hello {
		return &proto.Hello{Role: proto.RoleClient, Peer: "c_alice", Token: token, Caps: []string{proto.CapabilityV1}}
	}

	id, _, err := f.c.Authenticate(ctx, hello("tok-alice"))
	if err != nil || id != "c_alice" {
		t.Fatalf("alice claim = %q, err=%v", id, err)
	}

	// A different subject claiming the same live id must be refused.
	if _, _, err := f.c.Authenticate(ctx, hello("tok-mallory")); err == nil {
		t.Fatal("a second subject rebound another subject's peer id")
	}
	if subject, err := f.c.subjectOf("c_alice"); err != nil || subject.ID != "alice" || subject.Tenant != "tenant-a" {
		t.Fatalf("peer id now resolves to %+v (err=%v), want alice/tenant-a", subject, err)
	}

	// The holder reconnecting under its own id must still work.
	if _, _, err := f.c.Authenticate(ctx, hello("tok-alice-2")); err != nil {
		t.Fatalf("alice could not reconnect under her own peer id: %v", err)
	}

	// Once the holder is gone the id is free again.
	f.c.PeerGone(ctx, "c_alice")
	if _, _, err := f.c.Authenticate(ctx, hello("tok-mallory")); err != nil {
		t.Fatalf("a released peer id was not reusable: %v", err)
	}
}
