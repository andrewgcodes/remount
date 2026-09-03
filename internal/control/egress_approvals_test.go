package control

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"remount.dev/remount/internal/proto"
)

func approvalHash(ch byte) string {
	b := make([]byte, 64)
	for i := range b {
		b[i] = ch
	}
	return string(b)
}

func claimedApprovalWorkspace(t *testing.T, f *controlFixture) *proto.Workspace {
	t.Helper()
	connectNode(t, f.c, "n_approve", processNodeInfo(4096))
	ws := createWorkspace(t, f.c, localSubject(), proto.WorkspaceSpec{Security: proto.SecuritySpec{
		Profile: proto.SecurityLocal,
		Network: proto.NetworkPolicy{Rules: []proto.EgressRule{{
			ID: "github", Mode: proto.EgressModeApprove, Protocol: proto.EgressProtocolHTTPS, Hosts: []string{"api.github.com"},
		}}},
	}})
	claim, err := f.c.wsClaim(context.Background(), "n_approve", ws.ID)
	if err != nil {
		t.Fatal(err)
	}
	if err := f.c.wsReady(context.Background(), "n_approve", &proto.WSReadyReq{ID: ws.ID, Gen: claim.Workspace.Generation}); err != nil {
		t.Fatal(err)
	}
	return f.c.snapshotWS(ws.ID)
}

func TestEgressApprovalIsDurableIdempotentAndGenerationFenced(t *testing.T) {
	f := newControlFixture(t, "", nil)
	ws := claimedApprovalWorkspace(t, f)
	req := &proto.EgressApprovalReq{
		WS: ws.ID, Gen: ws.Generation, Principal: "alice", Rule: "github", Host: "api.github.com", Method: "POST",
		PathHash: approvalHash('a'), BodyHash: approvalHash('b'), Fingerprint: approvalHash('c'),
	}
	first, err := f.c.egressApproval(context.Background(), "n_approve", req)
	if err != nil || first.Status != proto.ApprovalPending || first.ID == "" {
		t.Fatalf("first approval = %+v, %v", first, err)
	}
	replay, err := f.c.egressApproval(context.Background(), "n_approve", req)
	if err != nil || replay.ID != first.ID {
		t.Fatalf("replay approval = %+v, %v; want %s", replay, err, first.ID)
	}
	decided, err := f.c.approvalDecide(context.Background(), localSubject(), &proto.ApprovalDecideReq{
		ID: first.ID, Remember: proto.ApprovalRememberHost, IdempotencyKey: "approve-once",
	})
	if err != nil || decided.Decision == nil || decided.Decision.Denied {
		t.Fatalf("decide = %+v, %v", decided, err)
	}
	req.Fingerprint = approvalHash('d')
	req.PathHash = approvalHash('e')
	remembered, err := f.c.egressApproval(context.Background(), "n_approve", req)
	if err != nil || !remembered.Allowed || remembered.ID != first.ID {
		t.Fatalf("remembered approval = %+v, %v", remembered, err)
	}
	stale := *req
	stale.Gen++
	if _, err := f.c.egressApproval(context.Background(), "n_approve", &stale); codeOf(err) != proto.CodeDenied {
		t.Fatalf("stale generation = %v", err)
	}
	if got := len(eventsOfType(t, f.log, proto.EvEgressPending)); got != 1 {
		t.Fatalf("egress.pending events = %d", got)
	}
	if got := len(eventsOfType(t, f.log, proto.EvPolicyUpdated)); got != 1 {
		t.Fatalf("policy.updated events = %d", got)
	}
}

func TestEgressApprovalTimeoutIsAnObservableDenial(t *testing.T) {
	var nowMillis atomic.Int64
	nowMillis.Store(time.Now().UnixMilli())
	f := newControlFixture(t, "", func(opts *Options) {
		opts.Now = func() time.Time { return time.UnixMilli(nowMillis.Load()) }
		opts.ApprovalTimeout = time.Second
	})
	ws := claimedApprovalWorkspace(t, f)
	req := &proto.EgressApprovalReq{
		WS: ws.ID, Gen: ws.Generation, Principal: "alice", Rule: "github", Host: "api.github.com", Method: "GET",
		PathHash: approvalHash('a'), BodyHash: approvalHash('b'), Fingerprint: approvalHash('c'),
	}
	pending, err := f.c.egressApproval(context.Background(), "n_approve", req)
	if err != nil {
		t.Fatal(err)
	}
	nowMillis.Add((2 * time.Second).Milliseconds())
	f.c.expireStaleApprovals(context.Background())
	got, err := f.c.approvalGet(context.Background(), localSubject(), pending.ID)
	if err != nil || got.Status != proto.ApprovalExpired || got.Decision == nil || !got.Decision.Denied {
		t.Fatalf("expired approval = %+v, %v", got, err)
	}
	denied := eventsOfType(t, f.log, proto.EvEgressDenied)
	if len(denied) != 1 {
		t.Fatalf("egress.denied events = %+v", denied)
	}
}
