package control

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"remount.dev/remount/internal/proto"
)

// TestACLChangeBumpsRevisionAndRenewCarriesRevocations checks the control side
// of authz-push: every ACL change advances AuthzRevision, removed principals
// are recorded, the renew answer names exactly the principals revoked since
// the revision the node reports, and a replayed request is a no-op.
func TestACLChangeBumpsRevisionAndRenewCarriesRevocations(t *testing.T) {
	f := newControlFixture(t, "", nil)
	connectNode(t, f.c, "n_one", processNodeInfo(4096))
	owner := Subject{ID: "owner", Tenant: "tenant-a"}
	created := createWorkspace(t, f.c, owner, proto.WorkspaceSpec{ACL: proto.WorkspaceACL{Readers: []string{"r1"}, Writers: []string{"w1", "w2"}}})
	claim, err := f.c.wsClaim(context.Background(), "n_one", created.ID)
	if err != nil {
		t.Fatal(err)
	}
	if err := f.c.wsReady(context.Background(), "n_one", &proto.WSReadyReq{ID: created.ID, Gen: claim.Workspace.Generation}); err != nil {
		t.Fatal(err)
	}
	base := claim.Workspace.AuthzRevision

	req := &proto.WSACLReq{ID: created.ID, ACL: proto.WorkspaceACL{Writers: []string{"w2", "w3"}}, IdempotencyKey: "acl-1"}
	first, err := f.c.wsACL(context.Background(), owner.ID, req)
	if err != nil {
		t.Fatal(err)
	}
	if first.AuthzRevision != base+1 {
		t.Fatalf("revision %d, want %d", first.AuthzRevision, base+1)
	}
	if got := fmt.Sprint(first.Revocations); got != fmt.Sprintf("[{%d r1} {%d w1}]", base+1, base+1) {
		t.Fatalf("revocations %s", got)
	}
	replay, err := f.c.wsACL(context.Background(), owner.ID, req)
	if err != nil || replay.AuthzRevision != first.AuthzRevision {
		t.Fatalf("replay = %+v %v", replay, err)
	}
	// Widening also bumps the revision (old grants must re-verify) but names
	// nobody as revoked.
	second, err := f.c.wsACL(context.Background(), owner.ID, &proto.WSACLReq{ID: created.ID, ACL: proto.WorkspaceACL{Writers: []string{"w2", "w3", "w4"}}, IdempotencyKey: "acl-2"})
	if err != nil {
		t.Fatal(err)
	}
	if second.AuthzRevision != base+2 || len(second.Revocations) != 2 {
		t.Fatalf("widen = %+v", second)
	}

	// A node that renews with the revision it enforces learns what changed
	// since; one that is already current learns nothing.
	renew := func(known uint64) proto.WSRenewResult {
		res, err := f.c.wsRenew(context.Background(), "n_one", &proto.WSRenewReq{
			IDs: []string{created.ID}, Gen: map[string]uint64{created.ID: claim.Workspace.Generation}, Authz: map[string]uint64{created.ID: known},
		})
		if err != nil || len(res.Results) != 1 || !res.Results[0].Accepted {
			t.Fatalf("renew = %+v %v", res, err)
		}
		return res.Results[0]
	}
	stale := renew(base)
	if stale.AuthzRevision != base+2 || fmt.Sprint(stale.Revoked) != "[r1 w1]" || stale.AuthzReset {
		t.Fatalf("stale renew = %+v", stale)
	}
	current := renew(base + 2)
	if current.AuthzRevision != base+2 || current.Revoked != nil || current.AuthzReset {
		t.Fatalf("current renew = %+v", current)
	}

	// Grants stop being minted for the revoked principal at once and the ACL
	// is owner-only for non-admins.
	if _, err := f.c.authorizeWorkspace(context.Background(), "c_w1", created.ID, ActionRead); err == nil {
		t.Fatal("unbound peer should not pass; fixture has no subject")
	}
	f.c.mu.Lock()
	f.c.subjects["c_w1"] = Subject{ID: "w1", Tenant: "tenant-a"}
	f.c.subjects["c_w2"] = Subject{ID: "w2", Tenant: "tenant-a"}
	f.c.mu.Unlock()
	if _, err := f.c.authorizeWorkspace(context.Background(), "c_w1", created.ID, ActionRead); !errors.Is(err, &proto.Error{Code: proto.CodeDenied}) {
		t.Fatalf("revoked principal authorize = %v", err)
	}
	if _, err := f.c.authorizeWorkspace(context.Background(), "c_w2", created.ID, ActionWrite); err != nil {
		t.Fatalf("retained principal authorize = %v", err)
	}
	if _, err := f.c.authorizeWorkspace(context.Background(), "c_w2", created.ID, ActionACL); !errors.Is(err, &proto.Error{Code: proto.CodeDenied}) {
		t.Fatalf("writer changing acl = %v", err)
	}
}

// TestRevocationHistoryIsBoundedAndResetsBehindTheFloor checks that retained
// revocations never grow past MaxRetainedRevocations and that a node whose
// known revision predates the retained window is told to reset rather than
// handed an incomplete list.
func TestRevocationHistoryIsBoundedAndResetsBehindTheFloor(t *testing.T) {
	f := newControlFixture(t, "", nil)
	owner := Subject{ID: "owner", Tenant: "tenant-a"}
	created := createWorkspace(t, f.c, owner, proto.WorkspaceSpec{})
	var last *proto.Workspace
	for i := 0; i < proto.MaxRetainedRevocations+5; i++ {
		// Each step admits one principal and evicts the previous one.
		acl := proto.WorkspaceACL{Readers: []string{fmt.Sprintf("p%d", i)}}
		ws, err := f.c.wsACL(context.Background(), owner.ID, &proto.WSACLReq{ID: created.ID, ACL: acl, IdempotencyKey: fmt.Sprintf("k%d", i)})
		if err != nil {
			t.Fatal(err)
		}
		last = ws
	}
	if len(last.Revocations) != proto.MaxRetainedRevocations {
		t.Fatalf("retained %d revocations", len(last.Revocations))
	}
	// Steps 1..N+4 each revoked one principal (step 0 revoked nobody), so the
	// oldest retained entry is revision base+6 and the floor is the pruned
	// revision just before it.
	if last.RevocationFloor != last.Revocations[0].Revision-1 {
		t.Fatalf("floor %d, oldest retained %d", last.RevocationFloor, last.Revocations[0].Revision)
	}
	if revoked, reset := revokedSince(last, last.RevocationFloor-1); revoked != nil || !reset {
		t.Fatalf("behind floor = %v %v", revoked, reset)
	}
	if revoked, reset := revokedSince(last, last.RevocationFloor); len(revoked) != proto.MaxRetainedRevocations || reset {
		t.Fatalf("at floor = %d %v", len(revoked), reset)
	}
	if revoked, reset := revokedSince(last, last.AuthzRevision-1); fmt.Sprint(revoked) != fmt.Sprintf("[p%d]", proto.MaxRetainedRevocations+3) || reset {
		t.Fatalf("latest = %v %v", revoked, reset)
	}
}
