package control

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"remount.dev/remount/internal/eventlog"
	"remount.dev/remount/internal/proto"
	"remount.dev/remount/internal/transport"
)

type fakeSender struct {
	mu      sync.Mutex
	online  map[string]bool
	request func(context.Context, string, string, any, any) error
	sent    []*proto.Frame
}

func (f *fakeSender) Send(_ context.Context, frame *proto.Frame) error {
	f.mu.Lock()
	f.sent = append(f.sent, frame)
	f.mu.Unlock()
	return nil
}

func (f *fakeSender) Request(ctx context.Context, to, op string, body, out any) error {
	if f.request != nil {
		return f.request(ctx, to, op, body, out)
	}
	return proto.Err(proto.CodeUnreachable, "no fake response")
}

func (f *fakeSender) Online(id string) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.online[id]
}

func (f *fakeSender) Peers() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	var peers []string
	for id, online := range f.online {
		if online {
			peers = append(peers, id)
		}
	}
	return peers
}

type controlFixture struct {
	c   *Control
	sq  *eventlog.SQLite
	log *eventlog.Log
}

func newControlFixture(t *testing.T, path string, configure func(*Options)) *controlFixture {
	t.Helper()
	if path == "" {
		path = filepath.Join(t.TempDir(), "control.db")
	}
	sq, err := eventlog.OpenSQLite(path)
	if err != nil {
		t.Fatal(err)
	}
	log := eventlog.New(sq)
	opts := Options{DB: sq.DB(), Log: log, Token: "node-token", LeaseSec: 10}
	if configure != nil {
		configure(&opts)
	}
	c, err := New(opts)
	if err != nil {
		_ = log.Close()
		t.Fatal(err)
	}
	f := &controlFixture{c: c, sq: sq, log: log}
	t.Cleanup(func() {
		c.Stop()
		_ = log.Close()
	})
	return f
}

func signedNodeHello(t *testing.T, id, token string, info proto.NodeInfo) (proto.Hello, ed25519.PrivateKey) {
	t.Helper()
	_, key, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	h := proto.Hello{
		Peer: id, Role: proto.RoleNode, Token: token, PubKey: key.Public().(ed25519.PublicKey),
		Node: &info, IssuedAt: time.Now().UnixMilli(), Nonce: make([]byte, 32),
	}
	if _, err := rand.Read(h.Nonce); err != nil {
		t.Fatal(err)
	}
	h.Proof = ed25519.Sign(key, proto.HelloProofBytes(h))
	return h, key
}

func connectNode(t *testing.T, c *Control, id string, info proto.NodeInfo) {
	t.Helper()
	h, _ := signedNodeHello(t, id, "node-token", info)
	got, _, err := c.Authenticate(context.Background(), &h)
	if err != nil || got != id {
		t.Fatalf("Authenticate node = (%q, %v)", got, err)
	}
	c.PeerConnected(context.Background(), id, &h)
}

func localSubject() Subject {
	return Subject{ID: "alice", Tenant: "tenant-a", Roles: []string{"admin"}}
}

func createWorkspace(t *testing.T, c *Control, subject Subject, spec proto.WorkspaceSpec) *proto.Workspace {
	t.Helper()
	ws, err := c.wsCreate(context.Background(), subject, &proto.WSCreateReq{Spec: spec})
	if err != nil {
		t.Fatal(err)
	}
	return ws
}

func processNodeInfo(mem int) proto.NodeInfo {
	return proto.NodeInfo{
		Backends: []string{"process"}, OS: "linux", Arch: "amd64", CPU: 8, MemMiB: mem,
		BackendDescriptors: []proto.BackendDescriptor{{
			Name: "process", Security: proto.BackendSecurityCaps{
				Isolation: "none", EgressMode: "cooperative_proxy", BrokerIdentity: "token", FilesystemBoundary: "root_handle",
			}, Runtime: proto.RuntimeCaps{Snapshots: "fs"},
		}},
	}
}

func TestNodeProofOfPossessionAndReplayProtection(t *testing.T) {
	f := newControlFixture(t, "", nil)
	missing, _ := signedNodeHello(t, "n_missing", "node-token", processNodeInfo(1024))
	missing.Proof = nil
	if _, _, err := f.c.Authenticate(context.Background(), &missing); err == nil {
		t.Fatal("node without proof authenticated")
	}
	h, _ := signedNodeHello(t, "n_valid", "node-token", processNodeInfo(1024))
	if _, _, err := f.c.Authenticate(context.Background(), &h); err != nil {
		t.Fatal(err)
	}
	if _, _, err := f.c.Authenticate(context.Background(), &h); err == nil {
		t.Fatal("replayed node proof authenticated")
	}
	tampered, key := signedNodeHello(t, "n_tampered", "node-token", processNodeInfo(1024))
	tampered.Labels = map[string]string{"privileged": "true"}
	// Signature covered the prior hello; changing labels must invalidate it.
	_ = key
	if _, _, err := f.c.Authenticate(context.Background(), &tampered); err == nil {
		t.Fatal("tampered node claims authenticated")
	}
}

func TestReadyRenewAndPersistenceAreStrict(t *testing.T) {
	f := newControlFixture(t, "", nil)
	sender := &fakeSender{online: map[string]bool{"n_one": true}}
	f.c.Attach(sender)
	connectNode(t, f.c, "n_one", processNodeInfo(4096))
	ws := createWorkspace(t, f.c, localSubject(), proto.WorkspaceSpec{})
	claim, err := f.c.wsClaim(context.Background(), "n_one", ws.ID)
	if err != nil || claim.Workspace.State != proto.WSClaiming {
		t.Fatalf("claim = %#v, %v", claim, err)
	}
	if err := f.c.wsReady(context.Background(), "n_one", &proto.WSReadyReq{ID: ws.ID, Gen: claim.Workspace.Generation + 1}); err == nil {
		t.Fatal("stale ready accepted")
	}
	if err := f.c.wsReady(context.Background(), "n_one", &proto.WSReadyReq{ID: ws.ID, Gen: claim.Workspace.Generation}); err != nil {
		t.Fatal(err)
	}
	renew, err := f.c.wsRenew(context.Background(), "n_one", &proto.WSRenewReq{
		IDs: []string{ws.ID, "ws_missing"}, Gen: map[string]uint64{ws.ID: claim.Workspace.Generation - 1},
	})
	if err != nil || len(renew.Results) != 2 || renew.Results[0].Accepted || renew.Results[1].Action != "destroy" {
		t.Fatalf("stale renew = %#v, %v", renew, err)
	}
	renew, err = f.c.wsRenew(context.Background(), "n_one", &proto.WSRenewReq{
		IDs: []string{ws.ID}, Gen: map[string]uint64{ws.ID: claim.Workspace.Generation},
	})
	if err != nil || !renew.Results[0].Accepted || renew.Results[0].Action != "continue" {
		t.Fatalf("valid renew = %#v, %v", renew, err)
	}

	// A failed durable write may not be published in memory.
	before := f.c.snapshotWS(ws.ID)
	if err := f.log.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := f.c.wsRenew(context.Background(), "n_one", &proto.WSRenewReq{
		IDs: []string{ws.ID}, Gen: map[string]uint64{ws.ID: claim.Workspace.Generation},
	}); err == nil {
		t.Fatal("renew succeeded after database close")
	}
	after := f.c.snapshotWS(ws.ID)
	if after.LeaseUntil != before.LeaseUntil || after.State != before.State {
		t.Fatalf("memory mutated after persistence failure: before=%#v after=%#v", before, after)
	}
}

func TestRestartPreservesHolderAndReleasedRecovery(t *testing.T) {
	path := filepath.Join(t.TempDir(), "restart.db")
	f1 := newControlFixture(t, path, nil)
	f1.c.Attach(&fakeSender{online: map[string]bool{"n_one": true}})
	connectNode(t, f1.c, "n_one", processNodeInfo(4096))
	ws := createWorkspace(t, f1.c, localSubject(), proto.WorkspaceSpec{})
	claim, err := f1.c.wsClaim(context.Background(), "n_one", ws.ID)
	if err != nil {
		t.Fatal(err)
	}
	if err := f1.c.wsReady(context.Background(), "n_one", &proto.WSReadyReq{ID: ws.ID, Gen: claim.Workspace.Generation}); err != nil {
		t.Fatal(err)
	}
	if err := f1.log.Close(); err != nil {
		t.Fatal(err)
	}

	f2 := newControlFixture(t, path, nil)
	recovered := f2.c.snapshotWS(ws.ID)
	if recovered.State != proto.WSClaiming || recovered.Node != "n_one" || recovered.Generation != claim.Workspace.Generation {
		t.Fatalf("restart lost holder: %#v", recovered)
	}

	f2.c.mu.Lock()
	next := *f2.c.workspaces[ws.ID]
	next.State = proto.WSReleased
	next.LastSnapshot = "art_sha256:" + string(make([]byte, 64))
	// Use a syntactically valid value; recovery does not dereference it.
	next.LastSnapshot = "art_sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	if err := f2.c.persistWS(&next); err != nil {
		f2.c.mu.Unlock()
		t.Fatal(err)
	}
	*f2.c.workspaces[ws.ID] = next
	f2.c.mu.Unlock()
	if err := f2.log.Close(); err != nil {
		t.Fatal(err)
	}

	f3 := newControlFixture(t, path, nil)
	recovered = f3.c.snapshotWS(ws.ID)
	if recovered.State != proto.WSPending || recovered.Node != "" || recovered.Spec.RestoreFrom != recovered.LastSnapshot {
		t.Fatalf("released recovery = %#v", recovered)
	}
}

func TestServerAuthoritativeIdentityAndTenantACL(t *testing.T) {
	auth := StaticAuthenticator{
		"alice-token":  {ID: "alice", Tenant: "tenant-a"},
		"bob-token":    {ID: "bob", Tenant: "tenant-b"},
		"reader-token": {ID: "reader", Tenant: "tenant-a"},
	}
	f := newControlFixture(t, "", func(opts *Options) { opts.Authenticator = auth })
	connectClient := func(token string) string {
		h := proto.Hello{Role: proto.RoleClient, Token: token, Principal: "spoofed-admin"}
		id, _, err := f.c.Authenticate(context.Background(), &h)
		if err != nil {
			t.Fatal(err)
		}
		f.c.PeerConnected(context.Background(), id, &h)
		return id
	}
	alice, bob, reader := connectClient("alice-token"), connectClient("bob-token"), connectClient("reader-token")
	body, err := f.c.dispatch(context.Background(), &proto.Frame{
		T: proto.KindReq, From: alice, Op: proto.OpWSCreate,
		Body: proto.MustMarshal(proto.WSCreateReq{Spec: proto.WorkspaceSpec{
			Principal: "bob", ACL: proto.WorkspaceACL{Readers: []string{"reader"}},
		}}),
	})
	if err != nil {
		t.Fatal(err)
	}
	ws := body.(*proto.Workspace)
	if ws.Owner != "alice" || ws.Tenant != "tenant-a" || ws.Spec.Principal != "alice" {
		t.Fatalf("client selected authority: %#v", ws)
	}
	list, err := f.c.dispatch(context.Background(), &proto.Frame{T: proto.KindReq, From: bob, Op: proto.OpWSList})
	if err != nil || len(list.(*proto.WSListRes).Workspaces) != 0 {
		t.Fatalf("cross-tenant list leaked workspace: %#v, %v", list, err)
	}
	if _, err := f.c.dispatch(context.Background(), &proto.Frame{
		T: proto.KindReq, From: bob, Op: proto.OpWSGet, Body: proto.MustMarshal(proto.WSGetReq{ID: ws.ID}),
	}); err == nil {
		t.Fatal("cross-tenant get succeeded")
	}
	if _, err := f.c.dispatch(context.Background(), &proto.Frame{
		T: proto.KindReq, From: reader, Op: proto.OpWSGet, Body: proto.MustMarshal(proto.WSGetReq{ID: ws.ID}),
	}); err != nil {
		t.Fatalf("ACL reader could not read: %v", err)
	}
	if _, err := f.c.dispatch(context.Background(), &proto.Frame{
		T: proto.KindReq, From: reader, Op: proto.OpWSDestroy, Body: proto.MustMarshal(proto.WSGetReq{ID: ws.ID}),
	}); err == nil {
		t.Fatal("read-only subject destroyed workspace")
	}
}

func TestBackendSecurityIsPerBackendAndUnknownMemoryFailsClosed(t *testing.T) {
	f := newControlFixture(t, "", nil)
	info := processNodeInfo(0)
	info.Backends = append(info.Backends, "micro")
	info.BackendDescriptors = append(info.BackendDescriptors, proto.BackendDescriptor{
		Name: "micro", Security: proto.BackendSecurityCaps{
			Isolation: "microvm", MultiTenant: true, SiblingIsolation: true,
			EgressMode: "enforced_gateway", BrokerIdentity: "workload_identity",
			FilesystemBoundary: "block_device", NetworkNamespace: true, DeviceIsolation: true,
		}, Runtime: proto.RuntimeCaps{Snapshots: "fs"},
	})
	connectNode(t, f.c, "n_secure", info)
	memory := createWorkspace(t, f.c, localSubject(), proto.WorkspaceSpec{Requires: proto.Requires{MemMiB: 1}})
	if _, err := f.c.wsClaim(context.Background(), "n_secure", memory.ID); err == nil {
		t.Fatal("node with unknown memory satisfied a memory requirement")
	}
	multi := createWorkspace(t, f.c, localSubject(), proto.WorkspaceSpec{Security: proto.SecuritySpec{Profile: proto.SecurityMultiTenant}})
	claim, err := f.c.wsClaim(context.Background(), "n_secure", multi.ID)
	if err != nil {
		t.Fatal(err)
	}
	if claim.Workspace.Spec.Requires.Backend != "micro" {
		t.Fatalf("multi-tenant policy selected %q", claim.Workspace.Spec.Requires.Backend)
	}
	forcedWeak := createWorkspace(t, f.c, localSubject(), proto.WorkspaceSpec{
		Requires: proto.Requires{Backend: "process"}, Security: proto.SecuritySpec{Profile: proto.SecurityMultiTenant},
	})
	if _, err := f.c.wsClaim(context.Background(), "n_secure", forcedWeak.ID); err == nil {
		t.Fatal("weak backend inherited strong sibling backend capabilities")
	}
}

func TestReleaseFailureRetainsAuthoritativeSource(t *testing.T) {
	f := newControlFixture(t, "", nil)
	sender := &fakeSender{online: map[string]bool{"n_one": true}}
	var aborted bool
	sender.request = func(_ context.Context, _ string, op string, _, _ any) error {
		switch op {
		case proto.OpWSRelease:
			return proto.Err(proto.CodeInternal, "checkpoint failed")
		case proto.OpWSReleaseAbort:
			aborted = true
			return nil
		}
		return nil
	}
	f.c.Attach(sender)
	connectNode(t, f.c, "n_one", processNodeInfo(4096))
	ws := createWorkspace(t, f.c, localSubject(), proto.WorkspaceSpec{})
	claim, err := f.c.wsClaim(context.Background(), "n_one", ws.ID)
	if err != nil {
		t.Fatal(err)
	}
	if err := f.c.wsReady(context.Background(), "n_one", &proto.WSReadyReq{ID: ws.ID, Gen: claim.Workspace.Generation}); err != nil {
		t.Fatal(err)
	}
	if _, err := f.c.wsMove(context.Background(), "alice", &proto.WSMoveReq{ID: ws.ID}); err == nil {
		t.Fatal("move succeeded after lost release response")
	}
	after := f.c.snapshotWS(ws.ID)
	if after.State != proto.WSClaimed || after.Node != "n_one" || after.Generation != claim.Workspace.Generation {
		t.Fatalf("release failure discarded source authority: %#v", after)
	}
	if !aborted {
		t.Fatal("release failure restored claimed state without an acknowledged node abort")
	}
}

func TestReleaseRetriesLostResponse(t *testing.T) {
	f := newControlFixture(t, "", nil)
	sender := &fakeSender{online: map[string]bool{"n_one": true}}
	var prepares int
	snapshot := "art_sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	sender.request = func(_ context.Context, _ string, op string, body, out any) error {
		switch op {
		case proto.OpWSRelease:
			prepares++
			if prepares == 1 {
				return transport.ErrClosed
			}
			req := body.(proto.WSReleaseReq)
			*out.(*proto.WSReleasedReq) = proto.WSReleasedReq{ID: req.WS, Gen: req.Gen, Snapshot: snapshot, Reason: req.Reason}
		}
		return nil
	}
	f.c.Attach(sender)
	connectNode(t, f.c, "n_one", processNodeInfo(4096))
	ws := createWorkspace(t, f.c, localSubject(), proto.WorkspaceSpec{})
	claim, err := f.c.wsClaim(context.Background(), "n_one", ws.ID)
	if err != nil {
		t.Fatal(err)
	}
	if err := f.c.wsReady(context.Background(), "n_one", &proto.WSReadyReq{ID: ws.ID, Gen: claim.Workspace.Generation}); err != nil {
		t.Fatal(err)
	}
	moved, err := f.c.wsMove(context.Background(), "alice", &proto.WSMoveReq{ID: ws.ID, IdempotencyKey: "move-once"})
	if err != nil {
		t.Fatal(err)
	}
	if prepares != 2 || moved.State != proto.WSPending || moved.Spec.RestoreFrom != snapshot {
		t.Fatalf("lost response did not converge: prepares=%d workspace=%#v", prepares, moved)
	}
}

func TestReleaseWithoutAbortAcknowledgementFailsClosed(t *testing.T) {
	f := newControlFixture(t, "", nil)
	sender := &fakeSender{online: map[string]bool{"n_one": true}}
	sender.request = func(_ context.Context, _ string, op string, _, _ any) error {
		if op == proto.OpWSRelease {
			return proto.Err(proto.CodeInternal, "checkpoint outcome ambiguous")
		}
		if op == proto.OpWSReleaseAbort {
			return proto.Err(proto.CodeConflict, "cannot prove abort")
		}
		return nil
	}
	f.c.Attach(sender)
	connectNode(t, f.c, "n_one", processNodeInfo(4096))
	ws := createWorkspace(t, f.c, localSubject(), proto.WorkspaceSpec{})
	claim, err := f.c.wsClaim(context.Background(), "n_one", ws.ID)
	if err != nil {
		t.Fatal(err)
	}
	if err := f.c.wsReady(context.Background(), "n_one", &proto.WSReadyReq{ID: ws.ID, Gen: claim.Workspace.Generation}); err != nil {
		t.Fatal(err)
	}
	if _, err := f.c.wsMove(context.Background(), "alice", &proto.WSMoveReq{ID: ws.ID}); err == nil {
		t.Fatal("move succeeded without a proven release or abort")
	}
	after := f.c.snapshotWS(ws.ID)
	if after.State != proto.WSFailed || after.Node != "n_one" || after.Generation != claim.Workspace.Generation {
		t.Fatalf("ambiguous source was made schedulable: %#v", after)
	}
}

func TestNodeEventsDetectGapsDeduplicateAndUseAssignmentHistory(t *testing.T) {
	f := newControlFixture(t, "", nil)
	f.c.Attach(&fakeSender{online: map[string]bool{"n_one": true}})
	connectNode(t, f.c, "n_one", processNodeInfo(4096))
	ws := createWorkspace(t, f.c, localSubject(), proto.WorkspaceSpec{})
	claim, err := f.c.wsClaim(context.Background(), "n_one", ws.ID)
	if err != nil {
		t.Fatal(err)
	}
	gen := claim.Workspace.Generation

	event := proto.Event{
		Seq: 3, At: 1234, Stream: ws.ID, Workspace: ws.ID, Generation: gen,
		Type: proto.EvFSWrite, Payload: proto.MustMarshal(map[string]any{"path": "a"}),
	}
	if err := f.c.eventsPost(context.Background(), "n_one", &proto.EventPost{Events: []proto.Event{event}}); err != nil {
		t.Fatal(err)
	}
	// An exact retry is a no-op, but changing a payload under an accepted
	// producer sequence is an integrity violation.
	if err := f.c.eventsPost(context.Background(), "n_one", &proto.EventPost{Events: []proto.Event{event}}); err != nil {
		t.Fatalf("exact retry: %v", err)
	}
	changed := event
	changed.Payload = proto.MustMarshal(map[string]any{"path": "different"})
	if err := f.c.eventsPost(context.Background(), "n_one", &proto.EventPost{Events: []proto.Event{changed}}); err == nil {
		t.Fatal("changed duplicate producer event was accepted")
	}

	all, err := f.log.Read(context.Background(), 1, "", 100)
	if err != nil {
		t.Fatal(err)
	}
	var gaps, writes int
	for _, e := range all {
		switch e.Type {
		case proto.EvEventGap:
			gaps++
			if e.ProducerSeq != 2 || e.Node != "n_one" {
				t.Fatalf("gap metadata = %#v", e)
			}
		case proto.EvFSWrite:
			if e.Origin == "node" {
				writes++
				if e.Tenant != ws.Tenant || e.Workspace != ws.ID || e.Generation != gen || e.Actor != "n_one" {
					t.Fatalf("node event metadata = %#v", e)
				}
			}
		}
	}
	if gaps != 1 || writes != 1 {
		t.Fatalf("gaps=%d writes=%d, want 1 each", gaps, writes)
	}

	// Late delivery after reassignment is accepted only because the exact
	// node/workspace/generation tuple was durably recorded at claim time.
	f.c.mu.Lock()
	next := *f.c.workspaces[ws.ID]
	next.State, next.Node, next.Generation = proto.WSPending, "", gen+1
	if err := f.c.persistWS(&next); err != nil {
		f.c.mu.Unlock()
		t.Fatal(err)
	}
	*f.c.workspaces[ws.ID] = next
	f.c.mu.Unlock()
	late := event
	late.Seq = 4
	late.Type = proto.EvWSFenced
	late.Payload = proto.MustMarshal(map[string]any{"reason": "lease expired"})
	if err := f.c.eventsPost(context.Background(), "n_one", &proto.EventPost{Events: []proto.Event{late}}); err != nil {
		t.Fatalf("late event from recorded assignment: %v", err)
	}
	forged := late
	forged.Seq = 5
	forged.Generation = gen + 1
	if err := f.c.eventsPost(context.Background(), "n_one", &proto.EventPost{Events: []proto.Event{forged}}); err == nil {
		t.Fatal("event for an unassigned generation was accepted")
	}
}

func TestHandleFrameRejectsWhenRequestCapacityIsExhausted(t *testing.T) {
	f := newControlFixture(t, "", func(opts *Options) { opts.MaxConcurrentRequests = 1 })
	sender := &fakeSender{online: map[string]bool{}}
	f.c.Attach(sender)
	// Occupy the only execution slot without creating a goroutine. The frame
	// must be answered by the separately bounded overload path.
	f.c.requestSlots <- struct{}{}
	f.c.HandleFrame(context.Background(), &proto.Frame{
		V: proto.Version, T: proto.KindReq, ID: 41, From: "c_test", Op: proto.OpTimerList,
	})
	deadline := time.Now().Add(time.Second)
	for {
		sender.mu.Lock()
		var got *proto.Frame
		if len(sender.sent) > 0 {
			got = sender.sent[0]
		}
		sender.mu.Unlock()
		if got != nil {
			if got.ID != 41 || got.Err == nil || got.Err.Code != proto.CodeResourceExhausted {
				t.Fatalf("overload response = %#v", got)
			}
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("no overload response")
		}
		time.Sleep(time.Millisecond)
	}
	<-f.c.requestSlots
}
