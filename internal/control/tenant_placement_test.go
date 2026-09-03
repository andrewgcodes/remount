package control

import (
	"context"
	"testing"

	"remount.dev/remount/internal/metrics"
	"remount.dev/remount/internal/proto"
)

type tenantNodeAuthenticator map[string]string

func (a tenantNodeAuthenticator) AuthenticateNode(_ context.Context, _ string, token string, _ []byte) (NodeIdentity, error) {
	tenantID := a[token]
	if tenantID == "" {
		return NodeIdentity{}, proto.Err(proto.CodeUnauthorized, "bad enrollment")
	}
	return NodeIdentity{Tenant: tenantID, Labels: map[string]string{"region": "test"}}, nil
}

func connectTenantNode(t *testing.T, control *Control, id, token string) {
	t.Helper()
	hello, _ := signedNodeHello(t, id, token, processNodeInfo(4096))
	peer, _, err := control.Authenticate(context.Background(), &hello)
	if err != nil {
		t.Fatal(err)
	}
	if hello.Labels["tenant"] == "" {
		t.Fatal("authenticated node tenant was not stamped")
	}
	control.PeerConnected(context.Background(), peer, &hello)
}

func TestEnrolledNodeTenantFencesPlacementAndClaim(t *testing.T) {
	f := newControlFixture(t, "", func(options *Options) {
		options.NodeAuthenticator = tenantNodeAuthenticator{"enroll-a": "tenant-a", "enroll-b": "tenant-b"}
	})
	connectTenantNode(t, f.c, "n_a", "enroll-a")
	connectTenantNode(t, f.c, "n_b", "enroll-b")
	workspaceA := createWorkspace(t, f.c, Subject{ID: "alice", Tenant: "tenant-a", Roles: []string{"admin"}}, proto.WorkspaceSpec{})
	workspaceB := createWorkspace(t, f.c, Subject{ID: "bob", Tenant: "tenant-b", Roles: []string{"admin"}}, proto.WorkspaceSpec{})

	f.c.mu.Lock()
	if f.c.eligibleLocked(workspaceA, f.c.nodes["n_b"]) || f.c.eligibleLocked(workspaceB, f.c.nodes["n_a"]) {
		f.c.mu.Unlock()
		t.Fatal("cross-tenant node was eligible")
	}
	if !f.c.eligibleLocked(workspaceA, f.c.nodes["n_a"]) || !f.c.eligibleLocked(workspaceB, f.c.nodes["n_b"]) {
		f.c.mu.Unlock()
		t.Fatal("same-tenant node was ineligible")
	}
	f.c.mu.Unlock()
	sender := &fakeSender{online: map[string]bool{"n_a": true, "n_b": true}}
	f.c.Attach(sender)
	f.c.offerPending(context.Background())
	sender.mu.Lock()
	gotOffers := make(map[string]string, len(sender.sent))
	for _, frame := range sender.sent {
		if frame.Op != proto.EvWSOffer {
			continue
		}
		var offered proto.WSGetReq
		if err := frame.Decode(&offered); err != nil {
			sender.mu.Unlock()
			t.Fatal(err)
		}
		gotOffers[frame.To] = offered.ID
	}
	sender.mu.Unlock()
	if gotOffers["n_a"] != workspaceA.ID || gotOffers["n_b"] != workspaceB.ID || len(gotOffers) != 2 {
		t.Fatalf("tenant-fenced offers=%v", gotOffers)
	}

	before := metrics.TenantPlacementDenied.Value()
	if _, err := f.c.wsClaim(context.Background(), "n_b", workspaceA.ID); codeOf(err) != proto.CodeDenied {
		t.Fatalf("cross-tenant claim=%v", err)
	}
	if metrics.TenantPlacementDenied.Value() != before+1 {
		t.Fatal("cross-tenant claim denial was not observable")
	}
	if claim, err := f.c.wsClaim(context.Background(), "n_a", workspaceA.ID); err != nil || claim.Workspace.Node != "n_a" {
		t.Fatalf("same-tenant claim=%+v err=%v", claim, err)
	}
}

func TestEnrolledNodeMissingTenantFailsClosed(t *testing.T) {
	f := newControlFixture(t, "", func(options *Options) {
		options.NodeAuthenticator = tenantNodeAuthenticator{"enroll-a": "tenant-a"}
	})
	// Model corrupted/recovered node state without the authoritative tenant.
	f.c.mu.Lock()
	f.c.nodes["n_unknown"] = &nodeState{Status: proto.NodeStatus{ID: "n_unknown", Online: true, Info: processNodeInfo(4096), Labels: map[string]string{}}}
	f.c.mu.Unlock()
	workspace := createWorkspace(t, f.c, Subject{ID: "alice", Tenant: "tenant-a", Roles: []string{"admin"}}, proto.WorkspaceSpec{})
	if _, err := f.c.wsClaim(context.Background(), "n_unknown", workspace.ID); codeOf(err) != proto.CodeDenied {
		t.Fatalf("tenant-less enrolled node claim=%v", err)
	}
}
