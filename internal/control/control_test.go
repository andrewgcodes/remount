package control

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"errors"
	"fmt"
	"path/filepath"
	"reflect"
	"sort"
	"sync"
	"testing"
	"time"

	"remount.dev/remount/internal/artifact"
	"remount.dev/remount/internal/eventlog"
	"remount.dev/remount/internal/metrics"
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
	return signedNodeHelloWithKey(t, id, token, info, key), key
}

// signedNodeHelloWithKey signs with a caller-held key so a node can come back
// after a control-plane restart as itself: the pinned key is durable.
func signedNodeHelloWithKey(t *testing.T, id, token string, info proto.NodeInfo, key ed25519.PrivateKey) proto.Hello {
	t.Helper()
	h := proto.Hello{
		Peer: id, Role: proto.RoleNode, Token: token, Caps: proto.PeerCapabilities(), PubKey: key.Public().(ed25519.PublicKey),
		Node: &info, IssuedAt: time.Now().UnixMilli(), Nonce: make([]byte, 32),
	}
	if _, err := rand.Read(h.Nonce); err != nil {
		t.Fatal(err)
	}
	h.Proof = ed25519.Sign(key, proto.HelloProofBytes(h))
	return h
}

func connectNode(t *testing.T, c *Control, id string, info proto.NodeInfo) {
	t.Helper()
	connectNodeWithKey(t, c, id, info, nil)
}

// connectNodeWithKey connects id signing with key, or a fresh key when nil,
// and returns the key used.
func connectNodeWithKey(t *testing.T, c *Control, id string, info proto.NodeInfo, key ed25519.PrivateKey) ed25519.PrivateKey {
	t.Helper()
	var h proto.Hello
	if key == nil {
		h, key = signedNodeHello(t, id, "node-token", info)
	} else {
		h = signedNodeHelloWithKey(t, id, "node-token", info, key)
	}
	got, _, err := c.Authenticate(context.Background(), &h)
	if err != nil || got != id {
		t.Fatalf("Authenticate node = (%q, %v)", got, err)
	}
	c.PeerConnected(context.Background(), id, &h)
	return key
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

func TestSnapshotCommitLosesToLifecycleTransition(t *testing.T) {
	f := newControlFixture(t, "", nil)
	connectNode(t, f.c, "n_one", processNodeInfo(4096))
	created := createWorkspace(t, f.c, localSubject(), proto.WorkspaceSpec{})
	claim, err := f.c.wsClaim(context.Background(), "n_one", created.ID)
	if err != nil {
		t.Fatal(err)
	}
	if err := f.c.wsReady(context.Background(), "n_one", &proto.WSReadyReq{
		ID: created.ID, Gen: claim.Workspace.Generation,
	}); err != nil {
		t.Fatal(err)
	}

	f.c.mu.Lock()
	f.c.workspaces[created.ID].State = proto.WSQuiescing
	f.c.mu.Unlock()
	err = f.c.wsSnapshotCommit(context.Background(), "n_one", &proto.WSSnapshotCommitReq{
		ID: created.ID, Gen: claim.Workspace.Generation,
		Snapshot: "art_sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
	})
	if !errors.Is(err, &proto.Error{Code: proto.CodeConflict}) {
		t.Fatalf("snapshot commit during lifecycle transition = %v, want conflict", err)
	}
	if got := f.c.snapshotWS(created.ID).LastSnapshot; got != "" {
		t.Fatalf("rejected snapshot became authoritative: %q", got)
	}
}

func TestArtifactReferencesAreStableAndRestoreMustExist(t *testing.T) {
	store, err := artifact.NewStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	restore := putTestSnapshot(t, store, "restore")
	checkpoint := putTestSnapshot(t, store, "checkpoint")
	f := newControlFixture(t, "", func(opts *Options) { opts.Artifacts = store })
	ws := createWorkspace(t, f.c, localSubject(), proto.WorkspaceSpec{RestoreFrom: restore})
	f.c.mu.Lock()
	current := *f.c.workspaces[ws.ID]
	current.LastSnapshot = checkpoint
	if err := f.c.persistWS(&current); err != nil {
		f.c.mu.Unlock()
		t.Fatal(err)
	}
	f.c.workspaces[ws.ID] = &current
	f.c.mu.Unlock()
	var got []string
	if err := f.c.WithArtifactReferences(func(references []string) error {
		got = append([]string(nil), references...)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	want := []string{checkpoint, restore}
	sort.Strings(want)
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("artifact references = %v, want %v", got, want)
	}
	if _, err := f.c.wsCreate(context.Background(), localSubject(), &proto.WSCreateReq{
		Spec: proto.WorkspaceSpec{RestoreFrom: artifact.ID(make([]byte, 32))},
	}); !errors.Is(err, &proto.Error{Code: proto.CodeNotFound}) {
		t.Fatalf("missing restore error = %v", err)
	}
}

func TestWorkspaceQuotasAreAtomicPerTenantAndSubject(t *testing.T) {
	f := newControlFixture(t, "", func(opts *Options) {
		opts.MaxWorkspacesPerTenant = 2
		opts.MaxWorkspacesPerSubject = 1
	})
	alice := Subject{ID: "alice", Tenant: "tenant", Roles: []string{"admin"}}
	bob := Subject{ID: "bob", Tenant: "tenant", Roles: []string{"admin"}}
	other := Subject{ID: "alice", Tenant: "other", Roles: []string{"admin"}}
	createWorkspace(t, f.c, alice, proto.WorkspaceSpec{Name: "alice-1"})
	if _, err := f.c.wsCreate(context.Background(), alice, &proto.WSCreateReq{Spec: proto.WorkspaceSpec{Name: "alice-2"}}); !errors.Is(err, &proto.Error{Code: proto.CodeResourceExhausted}) {
		t.Fatalf("subject quota error = %v", err)
	}
	createWorkspace(t, f.c, bob, proto.WorkspaceSpec{Name: "bob-1"})
	if _, err := f.c.wsCreate(context.Background(), Subject{ID: "carol", Tenant: "tenant", Roles: []string{"admin"}}, &proto.WSCreateReq{Spec: proto.WorkspaceSpec{Name: "carol-1"}}); !errors.Is(err, &proto.Error{Code: proto.CodeResourceExhausted}) {
		t.Fatalf("tenant quota error = %v", err)
	}
	if _, err := f.c.wsCreate(context.Background(), other, &proto.WSCreateReq{Spec: proto.WorkspaceSpec{Name: "other-1"}}); err != nil {
		t.Fatalf("sibling tenant was blocked: %v", err)
	}
}

func TestDurableRecordQuotasRetentionAndLockCleanup(t *testing.T) {
	now := time.Unix(2_000_000_000, 0)
	f := newControlFixture(t, "", func(opts *Options) {
		opts.Now = func() time.Time { return now }
		opts.MaxMutationRecords = 1
		opts.MaxTimers = 3
		opts.MaxTimersPerWorkspace = 1
	})
	subject := localSubject()
	if _, err := f.c.wsCreate(context.Background(), subject, &proto.WSCreateReq{
		Spec: proto.WorkspaceSpec{Name: "first"}, IdempotencyKey: "first",
	}); err != nil {
		t.Fatal(err)
	}
	f.c.mu.Lock()
	locks := len(f.c.mutationLocks)
	f.c.mu.Unlock()
	if locks != 0 {
		t.Fatalf("completed mutation retained %d keyed locks", locks)
	}
	if _, err := f.c.wsCreate(context.Background(), subject, &proto.WSCreateReq{
		Spec: proto.WorkspaceSpec{Name: "second"}, IdempotencyKey: "second",
	}); !errors.Is(err, &proto.Error{Code: proto.CodeResourceExhausted}) {
		t.Fatalf("mutation quota error = %v", err)
	}

	now = now.Add(2 * time.Hour)
	result, err := f.c.PruneRecords(context.Background(), now.Add(-time.Hour), 10)
	if err != nil || result.Mutations != 1 {
		t.Fatalf("PruneRecords mutation result = (%+v, %v)", result, err)
	}
	if _, err := f.c.wsCreate(context.Background(), subject, &proto.WSCreateReq{
		Spec: proto.WorkspaceSpec{Name: "second"}, IdempotencyKey: "second",
	}); err != nil {
		t.Fatalf("mutation after retention prune = %v", err)
	}

	fired := &proto.Timer{ID: "t_fired", WS: "ws", Fired: true, CreatedAt: now.Add(-3 * time.Hour).UnixMilli(), FiredAt: now.Add(-2 * time.Hour).UnixMilli()}
	pending := &proto.Timer{ID: "t_pending", WS: "ws", CreatedAt: now.Add(-3 * time.Hour).UnixMilli()}
	f.c.mu.Lock()
	for _, timer := range []*proto.Timer{fired, pending} {
		if _, err := f.c.db.Exec(`INSERT INTO timers(id, data) VALUES(?,?)`, timer.ID, proto.MustMarshal(timer)); err != nil {
			f.c.mu.Unlock()
			t.Fatal(err)
		}
		f.c.timers[timer.ID] = timer
	}
	f.c.mu.Unlock()
	if release, err := f.c.reserveTimer("ws"); release != nil || !errors.Is(err, &proto.Error{Code: proto.CodeResourceExhausted}) {
		if release != nil {
			release()
		}
		t.Fatalf("workspace timer quota = (%v, %v)", release != nil, err)
	}
	result, err = f.c.PruneRecords(context.Background(), now.Add(-time.Hour), 10)
	if err != nil || result.Timers != 1 {
		t.Fatalf("PruneRecords timer result = (%+v, %v)", result, err)
	}
	f.c.mu.Lock()
	_, firedPresent := f.c.timers[fired.ID]
	_, pendingPresent := f.c.timers[pending.ID]
	f.c.mu.Unlock()
	if firedPresent || !pendingPresent {
		t.Fatalf("timer retention fired=%t pending=%t", firedPresent, pendingPresent)
	}
}

func TestKeyedLifecycleProducerAndFleetLocksAreReleased(t *testing.T) {
	f := newControlFixture(t, "", nil)
	for name, acquire := range map[string]func(string) func(){
		"lifecycle": f.c.lockLifecycle,
		"producer":  f.c.lockProducer,
		"fleet":     f.c.lockFleet,
	} {
		t.Run(name, func(t *testing.T) {
			release := acquire("untrusted-cardinality-key")
			release()
			f.c.mu.Lock()
			counts := map[string]int{
				"lifecycle": len(f.c.lifecycle), "producer": len(f.c.producerLocks), "fleet": len(f.c.fleetLocks),
			}
			f.c.mu.Unlock()
			if counts[name] != 0 {
				t.Fatalf("retained %d keyed locks", counts[name])
			}
		})
	}
}

func TestRecordPruneCollectsTerminalMetadataButPreservesLiveAuthority(t *testing.T) {
	now := time.Unix(2_100_000_000, 0)
	f := newControlFixture(t, "", func(opts *Options) { opts.Now = func() time.Time { return now } })
	deleted := &proto.Workspace{ID: "ws_deleted", State: proto.WSDestroyed, Generation: 1, UpdatedAt: now.UnixMilli()}
	protected := &proto.Workspace{ID: "ws_protected", State: proto.WSDestroyed, Generation: 2, UpdatedAt: now.UnixMilli()}
	active := &proto.Workspace{ID: "ws_active", State: proto.WSClaimed, Generation: 7, Node: "n_owner", UpdatedAt: now.UnixMilli()}
	completed := &proto.FleetOperation{
		ID: "fleet_completed", State: proto.FleetStateCompleted,
		Results: []proto.FleetOperationResult{{Workspace: deleted.ID, Generation: 1}},
	}
	pending := &proto.FleetOperation{
		ID: "fleet_pending", State: proto.FleetStateRunning,
		Results: []proto.FleetOperationResult{{Workspace: protected.ID, State: proto.FleetTargetPending}},
	}
	f.c.mu.Lock()
	for _, workspace := range []*proto.Workspace{deleted, protected, active} {
		if err := f.c.persistWS(workspace); err != nil {
			f.c.mu.Unlock()
			t.Fatal(err)
		}
		f.c.workspaces[workspace.ID] = workspace
	}
	for _, operation := range []*proto.FleetOperation{completed, pending} {
		if err := f.c.persistFleetOperation(operation); err != nil {
			f.c.mu.Unlock()
			t.Fatal(err)
		}
		f.c.fleetOps[operation.ID] = operation
	}
	for _, assignment := range []struct {
		workspace  string
		generation uint64
		node       string
	}{
		{active.ID, 6, "n_old"}, {active.ID, 7, active.Node},
	} {
		if _, err := f.c.db.Exec(`INSERT INTO assignments(workspace,generation,node,tenant,created_at) VALUES(?,?,?,?,?)`,
			assignment.workspace, assignment.generation, assignment.node, "local", now.UnixMilli()); err != nil {
			f.c.mu.Unlock()
			t.Fatal(err)
		}
	}
	if _, err := f.c.db.Exec(`INSERT INTO idem(key,ws) VALUES('legacy','ws_deleted')`); err != nil {
		f.c.mu.Unlock()
		t.Fatal(err)
	}
	f.c.mu.Unlock()

	now = now.Add(2 * time.Hour)
	result, err := f.c.PruneRecords(context.Background(), now.Add(-time.Hour), 100)
	if err != nil {
		t.Fatal(err)
	}
	if result.Workspaces != 1 || result.FleetOperations != 1 || result.Assignments != 1 || result.LegacyIdem != 1 {
		t.Fatalf("prune result = %+v", result)
	}
	f.c.mu.Lock()
	_, deletedPresent := f.c.workspaces[deleted.ID]
	_, protectedPresent := f.c.workspaces[protected.ID]
	_, completedPresent := f.c.fleetOps[completed.ID]
	_, pendingPresent := f.c.fleetOps[pending.ID]
	f.c.mu.Unlock()
	if deletedPresent || !protectedPresent || completedPresent || !pendingPresent {
		t.Fatalf("retention maps: deleted=%t protected=%t completed=%t pending=%t",
			deletedPresent, protectedPresent, completedPresent, pendingPresent)
	}
	var assignments int
	if err := f.c.db.QueryRow(`SELECT COUNT(*) FROM assignments WHERE workspace=? AND generation=? AND node=?`,
		active.ID, active.Generation, active.Node).Scan(&assignments); err != nil || assignments != 1 {
		t.Fatalf("current assignment count = %d, err=%v", assignments, err)
	}
}

func TestRecordPruneDoesNotStarveBehindProtectedAssignments(t *testing.T) {
	now := time.Unix(2_200_000_000, 0)
	f := newControlFixture(t, "", func(opts *Options) { opts.Now = func() time.Time { return now } })
	f.c.mu.Lock()
	for i := range 5 {
		id := fmt.Sprintf("ws_live_%d", i)
		node := fmt.Sprintf("n_live_%d", i)
		workspace := &proto.Workspace{
			ID: id, State: proto.WSClaimed, Generation: 1, Node: node,
			Tenant: "local", UpdatedAt: now.UnixMilli(),
		}
		if err := f.c.persistWS(workspace); err != nil {
			f.c.mu.Unlock()
			t.Fatal(err)
		}
		f.c.workspaces[id] = workspace
		if _, err := f.c.db.Exec(`INSERT INTO assignments(workspace,generation,node,tenant,created_at) VALUES(?,?,?,?,?)`,
			id, 1, node, "local", now.UnixMilli()); err != nil {
			f.c.mu.Unlock()
			t.Fatal(err)
		}
	}
	if _, err := f.c.db.Exec(`INSERT INTO assignments(workspace,generation,node,tenant,created_at) VALUES(?,?,?,?,?)`,
		"ws_stale", 1, "n_stale", "local", now.UnixMilli()); err != nil {
		f.c.mu.Unlock()
		t.Fatal(err)
	}
	f.c.mu.Unlock()

	now = now.Add(2 * time.Hour)
	result, err := f.c.PruneRecords(context.Background(), now.Add(-time.Hour), 1)
	if err != nil || result.Assignments != 1 {
		t.Fatalf("PruneRecords assignment result = (%+v, %v)", result, err)
	}
	var stale int
	if err := f.c.db.QueryRow(`SELECT COUNT(*) FROM assignments WHERE workspace='ws_stale'`).Scan(&stale); err != nil || stale != 0 {
		t.Fatalf("stale assignment count = %d, err=%v", stale, err)
	}
	var live int
	if err := f.c.db.QueryRow(`SELECT COUNT(*) FROM assignments WHERE workspace LIKE 'ws_live_%'`).Scan(&live); err != nil || live != 5 {
		t.Fatalf("live assignment count = %d, err=%v", live, err)
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

func TestRenewKeepsPreparedLifecycleSourceAvailable(t *testing.T) {
	for _, state := range []string{proto.WSQuiescing, proto.WSCheckpointing, proto.WSDestroying} {
		t.Run(state, func(t *testing.T) {
			f := newControlFixture(t, "", nil)
			sender := &fakeSender{online: map[string]bool{"n_one": true}}
			f.c.Attach(sender)
			connectNode(t, f.c, "n_one", processNodeInfo(4096))
			created := createWorkspace(t, f.c, localSubject(), proto.WorkspaceSpec{})
			claim, err := f.c.wsClaim(context.Background(), "n_one", created.ID)
			if err != nil {
				t.Fatal(err)
			}
			if err := f.c.wsReady(context.Background(), "n_one", &proto.WSReadyReq{ID: created.ID, Gen: claim.Workspace.Generation}); err != nil {
				t.Fatal(err)
			}

			f.c.mu.Lock()
			transitioning := *f.c.workspaces[created.ID]
			transitioning.State = state
			if err := f.c.persistWS(&transitioning); err != nil {
				f.c.mu.Unlock()
				t.Fatal(err)
			}
			*f.c.workspaces[created.ID] = transitioning
			f.c.mu.Unlock()

			result, err := f.c.wsRenew(context.Background(), "n_one", &proto.WSRenewReq{
				IDs: []string{created.ID}, Gen: map[string]uint64{created.ID: claim.Workspace.Generation},
			})
			if err != nil || len(result.Results) != 1 || !result.Results[0].Accepted || result.Results[0].Action != "continue" {
				t.Fatalf("renew in %s = %#v, %v", state, result, err)
			}
		})
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
	if recovered.State != proto.WSReleased || recovered.Node != "n_one" || recovered.LastSnapshot == "" {
		t.Fatalf("released recovery = %#v", recovered)
	}
}

func TestRestartReconcilesDurableReleaseAbortBeforeClaimed(t *testing.T) {
	path := filepath.Join(t.TempDir(), "release-abort-restart.db")
	f1 := newControlFixture(t, path, nil)
	f1.c.Attach(&fakeSender{online: map[string]bool{"n_one": true}})
	key := connectNodeWithKey(t, f1.c, "n_one", processNodeInfo(4096), nil)
	ws := createWorkspace(t, f1.c, localSubject(), proto.WorkspaceSpec{})
	claim, err := f1.c.wsClaim(context.Background(), "n_one", ws.ID)
	if err != nil {
		t.Fatal(err)
	}
	if err := f1.c.wsReady(context.Background(), "n_one", &proto.WSReadyReq{ID: ws.ID, Gen: claim.Workspace.Generation}); err != nil {
		t.Fatal(err)
	}
	f1.c.mu.Lock()
	current := f1.c.workspaces[ws.ID]
	next, err := transitionWorkspace(current, lifecycleTransition{
		operation: transitionReleaseBegin, actor: actorControl, to: proto.WSQuiescing,
		expectGeneration: true, generation: current.Generation, expectNode: true, node: current.Node,
	})
	if err != nil {
		f1.c.mu.Unlock()
		t.Fatal(err)
	}
	next.ReleaseOperation = "rel_restart_proof"
	if err := f1.c.persistWS(&next, f1.c.transitionEvent(current.State, &next, lifecycleTransition{
		operation: transitionReleaseBegin, actor: actorControl, to: proto.WSQuiescing,
		expectGeneration: true, generation: current.Generation, expectNode: true, node: current.Node,
	})); err != nil {
		f1.c.mu.Unlock()
		t.Fatal(err)
	}
	*current = next
	f1.c.mu.Unlock()
	if err := f1.log.Close(); err != nil {
		t.Fatal(err)
	}

	f2 := newControlFixture(t, path, nil)
	recovered := f2.c.snapshotWS(ws.ID)
	if recovered.State != proto.WSQuiescing || recovered.ReleaseOperation != next.ReleaseOperation {
		t.Fatalf("restart discarded release abort authority: %+v", recovered)
	}
	var aborts, commits int
	f2.c.Attach(&fakeSender{online: map[string]bool{"n_one": true}, request: func(_ context.Context, _ string, op string, body, _ any) error {
		req := body.(proto.WSReleaseCommitReq)
		if req.OperationID != next.ReleaseOperation {
			t.Fatalf("recovery operation=%q want %q", req.OperationID, next.ReleaseOperation)
		}
		switch op {
		case proto.OpWSReleaseAbort:
			aborts++
		case proto.OpWSReleaseAbortCommit:
			commits++
			state := f2.c.snapshotWS(ws.ID)
			if state.State != proto.WSClaiming {
				t.Fatalf("abort commit sent before durable claiming: %+v", state)
			}
		default:
			t.Fatalf("unexpected recovery op %q", op)
		}
		return nil
	}})
	connectNodeWithKey(t, f2.c, "n_one", processNodeInfo(4096), key)
	recovered = f2.c.snapshotWS(ws.ID)
	if recovered.State != proto.WSClaimed || recovered.ReleaseOperation != "" || aborts < 2 || commits != 1 {
		t.Fatalf("release abort recovery did not converge: workspace=%+v aborts=%d commits=%d", recovered, aborts, commits)
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
		h := proto.Hello{Role: proto.RoleClient, Token: token, Caps: []string{proto.CapabilityV1}, Principal: "spoofed-admin"}
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

func TestPackageWorkspaceRequiresAdvertisedConnector(t *testing.T) {
	f := newControlFixture(t, "", nil)
	legacy := processNodeInfo(1024)
	connectNode(t, f.c, "n_legacy", legacy)
	workspace := createWorkspace(t, f.c, localSubject(), proto.WorkspaceSpec{
		Security: proto.SecuritySpec{Network: proto.NetworkPolicy{Rules: []proto.EgressRule{{
			ID: "packages", Connector: proto.EgressConnectorPackage,
			Protocol: proto.EgressProtocolHTTPS, Hosts: []string{"registry.example"},
		}}}},
	})
	if _, err := f.c.wsClaim(context.Background(), "n_legacy", workspace.ID); err == nil {
		t.Fatal("legacy node reinterpreted a connector rule as generic HTTPS authority")
	}
	capable := processNodeInfo(1024)
	capable.Connectors = []string{proto.EgressConnectorPackage}
	connectNode(t, f.c, "n_connector", capable)
	claim, err := f.c.wsClaim(context.Background(), "n_connector", workspace.ID)
	if err != nil {
		t.Fatal(err)
	}
	if claim.Workspace.ID != workspace.ID {
		t.Fatalf("claimed workspace=%+v", claim.Workspace)
	}
}

func TestGlobalWriteEgressRuleRequiresAdministrator(t *testing.T) {
	f := newControlFixture(t, "", nil)
	user := Subject{ID: "user", Tenant: "tenant-a"}
	globalWrite := proto.WorkspaceSpec{Security: proto.SecuritySpec{Network: proto.NetworkPolicy{Rules: []proto.EgressRule{{
		ID: "global-package-write", Protocol: proto.EgressProtocolHTTPS, Hosts: []string{"packages.example"},
		Methods: []string{"POST"}, SharedState: proto.SharedStateGlobalWrite,
	}}}}}
	if _, err := f.c.wsCreate(context.Background(), user, &proto.WSCreateReq{Spec: globalWrite}); err == nil {
		t.Fatal("non-administrator created a global-write egress capability")
	}
	scopedWrite := globalWrite
	scopedWrite.Security.Network.Rules = append([]proto.EgressRule(nil), globalWrite.Security.Network.Rules...)
	scopedWrite.Security.Network.Rules[0].ID = "scoped-package-write"
	scopedWrite.Security.Network.Rules[0].SharedState = proto.SharedStateScopedWrite
	if _, err := f.c.wsCreate(context.Background(), user, &proto.WSCreateReq{Spec: scopedWrite}); err != nil {
		t.Fatalf("owner-scoped write capability: %v", err)
	}
}

func TestReleaseFailureRetainsAuthoritativeSource(t *testing.T) {
	f := newControlFixture(t, "", nil)
	sender := &fakeSender{online: map[string]bool{"n_one": true}}
	var aborted, published bool
	var operationID string
	sender.request = func(_ context.Context, _ string, op string, body, _ any) error {
		switch op {
		case proto.OpWSRelease:
			return proto.Err(proto.CodeInternal, "checkpoint failed")
		case proto.OpWSReleaseAbort:
			req := body.(proto.WSReleaseCommitReq)
			if req.OperationID == "" {
				t.Fatal("release abort omitted operation epoch")
			}
			operationID = req.OperationID
			aborted = true
			return nil
		case proto.OpWSReleaseAbortCommit:
			req := body.(proto.WSReleaseCommitReq)
			current := f.c.snapshotWS(req.ID)
			if req.OperationID != operationID || current == nil || current.State != proto.WSClaiming || current.ReleaseOperation != operationID {
				t.Fatalf("abort publication crossed control commit: request=%+v workspace=%+v", req, current)
			}
			published = true
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
	if !aborted || !published || after.ReleaseOperation != "" {
		t.Fatal("release failure restored claimed state without an acknowledged node abort")
	}
}

func TestRecoveredPublishedReleaseAdvancesNextEpoch(t *testing.T) {
	f := newControlFixture(t, "", nil)
	const (
		nodeID         = "n_one"
		publishedEpoch = 9
	)
	var releaseRequest proto.WSReleaseReq
	sender := &fakeSender{
		online: map[string]bool{nodeID: true},
		request: func(_ context.Context, _ string, op string, body, out any) error {
			switch op {
			case proto.OpWSRelease:
				releaseRequest = body.(proto.WSReleaseReq)
				*out.(*proto.WSReleasedReq) = proto.WSReleasedReq{
					ID: releaseRequest.WS, Gen: releaseRequest.Gen,
					OperationID: releaseRequest.OperationID, Reason: releaseRequest.Reason,
				}
			case proto.OpWSReleaseCommit:
			default:
				t.Fatalf("unexpected operation %q", op)
			}
			return nil
		},
	}
	f.c.Attach(sender)
	connectNode(t, f.c, nodeID, processNodeInfo(4096))
	created := createWorkspace(t, f.c, localSubject(), proto.WorkspaceSpec{})
	claim, err := f.c.wsClaim(context.Background(), nodeID, created.ID)
	if err != nil {
		t.Fatal(err)
	}
	if err := f.c.wsReady(context.Background(), nodeID, &proto.WSReadyReq{ID: created.ID, Gen: claim.Workspace.Generation}); err != nil {
		t.Fatal(err)
	}

	observed := *f.c.snapshotWS(created.ID)
	published := proto.ControllerReleaseState{
		Request: proto.WSReleaseReq{
			WS: observed.ID, Gen: observed.Generation, ReleaseEpoch: publishedEpoch,
			OperationID: "rel_published", Tenant: observed.Tenant, Spec: observed.Spec,
		},
		OperationID: "rel_published",
		State:       "published",
	}
	if err := mergePublishedReleaseEpoch(nodeID, &observed, []proto.ControllerReleaseState{published}); err != nil {
		t.Fatal(err)
	}
	if err := f.c.reconcileObservedWorkspace(context.Background(), nodeID, observed, nil, 0, &RecoveryState{PreviousEpoch: 1}); err != nil {
		t.Fatal(err)
	}
	recovered := f.c.snapshotWS(created.ID)
	if recovered.ReleaseEpoch != publishedEpoch || recovered.ReleaseOperation != "" {
		t.Fatalf("recovered release authority = %+v", recovered)
	}
	if _, err := f.c.release(context.Background(), created.ID, false, "test"); err != nil {
		t.Fatal(err)
	}
	if releaseRequest.ReleaseEpoch != publishedEpoch+1 {
		t.Fatalf("next release epoch = %d, want %d", releaseRequest.ReleaseEpoch, publishedEpoch+1)
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
			*out.(*proto.WSReleasedReq) = proto.WSReleasedReq{ID: req.WS, Gen: req.Gen, OperationID: req.OperationID, Snapshot: snapshot, Reason: req.Reason}
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
	dbPath := filepath.Join(t.TempDir(), "control.db")
	f := newControlFixture(t, dbPath, nil)
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
		Principal: "forged-principal",
		Type:      proto.EvFSWrite, Payload: proto.MustMarshal(map[string]any{"path": "a"}),
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
				if e.Tenant != ws.Tenant || e.Workspace != ws.ID || e.Generation != gen ||
					e.Actor != "n_one" || e.Principal != ws.Owner {
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

	// Retention keeps the producer watermark even after it removes the event
	// body. A restart must not manufacture a gap or wedge the node's durable
	// outbox when the old acknowledged sequence is retried.
	if _, err := f.log.Prune(context.Background(), time.Now().Add(24*time.Hour).UnixMilli(), 10_000); err != nil {
		t.Fatal(err)
	}
	f.c.Stop()
	if err := f.log.Close(); err != nil {
		t.Fatal(err)
	}
	f2 := newControlFixture(t, dbPath, nil)
	if got := f2.c.producerSeq["n_one"]; got != 4 {
		t.Fatalf("recovered producer watermark = %d, want 4", got)
	}
	if err := f2.c.eventsPost(context.Background(), "n_one", &proto.EventPost{Events: []proto.Event{late}}); err != nil {
		t.Fatalf("retry after retention = %v", err)
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

func TestFleetQuarantineSelectsFencesAndIsIdempotent(t *testing.T) {
	f := newControlFixture(t, "", nil)
	sender := &fakeSender{online: map[string]bool{"n_one": true}}
	sender.request = func(_ context.Context, to, op string, body, out any) error {
		if to != "n_one" || op != proto.OpWSQuarantine {
			return fmt.Errorf("unexpected request %s %s", to, op)
		}
		req := body.(proto.WSQuarantineReq)
		*out.(*proto.WSQuarantineRes) = proto.WSQuarantineRes{
			Fenced: true, Generation: req.Gen, Action: req.Action, Backend: "process",
		}
		if req.OperationID == "" || req.WS == "" || req.Gen == 0 {
			return errors.New("incomplete quarantine request")
		}
		return nil
	}
	f.c.Attach(sender)
	connectNode(t, f.c, "n_one", processNodeInfo(4096))
	target := createWorkspace(t, f.c, localSubject(), proto.WorkspaceSpec{
		Run: "incident-run", Model: "target-model", Labels: map[string]string{"risk": "high"},
	})
	other := createWorkspace(t, f.c, localSubject(), proto.WorkspaceSpec{Run: "other-run"})
	claim, err := f.c.wsClaim(context.Background(), "n_one", target.ID)
	if err != nil {
		t.Fatal(err)
	}
	if err := f.c.wsReady(context.Background(), "n_one", &proto.WSReadyReq{ID: target.ID, Gen: claim.Workspace.Generation}); err != nil {
		t.Fatal(err)
	}

	req := &proto.FleetQuarantineReq{
		Selector: proto.WorkspaceSelector{Run: "incident-run", Model: "target-model", Labels: map[string]string{"risk": "high"}},
		Action:   proto.FleetActionFreeze, IdempotencyKey: "contain-once",
	}
	op, err := f.c.fleetQuarantine(context.Background(), localSubject(), req)
	if err != nil || len(op.Results) != 1 || op.Results[0].Workspace != target.ID {
		t.Fatalf("operation=%#v err=%v", op, err)
	}
	f.c.runFleetOperation(context.Background(), op.ID)
	done, err := f.c.fleetGet(context.Background(), localSubject(), op.ID)
	if err != nil || done.State != proto.FleetStateCompleted || !done.Results[0].Acknowledged {
		t.Fatalf("completed operation=%#v err=%v", done, err)
	}
	fenced := f.c.snapshotWS(target.ID)
	if fenced.State != proto.WSFailed || fenced.Generation != claim.Workspace.Generation+1 ||
		fenced.QuarantineOperation != op.ID || fenced.AuthzRevision <= target.AuthzRevision {
		t.Fatalf("fenced workspace=%#v", fenced)
	}
	if got := f.c.snapshotWS(other.ID); got.State != proto.WSPending {
		t.Fatalf("selector fenced unrelated workspace: %#v", got)
	}
	replay, err := f.c.fleetQuarantine(context.Background(), localSubject(), req)
	if err != nil || replay.ID != op.ID {
		t.Fatalf("idempotent replay=%#v err=%v", replay, err)
	}
	changed := *req
	changed.Action = proto.FleetActionStop
	if _, err := f.c.fleetQuarantine(context.Background(), localSubject(), &changed); err == nil {
		t.Fatal("changed request reused an idempotency key")
	}
}

func TestFleetQuarantinePersistsPendingTargetsAcrossRestart(t *testing.T) {
	path := filepath.Join(t.TempDir(), "fleet-restart.db")
	f1 := newControlFixture(t, path, nil)
	offline := &fakeSender{online: map[string]bool{"n_one": false}}
	f1.c.Attach(offline)
	connectNode(t, f1.c, "n_one", processNodeInfo(4096))
	ws := createWorkspace(t, f1.c, localSubject(), proto.WorkspaceSpec{})
	claim, err := f1.c.wsClaim(context.Background(), "n_one", ws.ID)
	if err != nil {
		t.Fatal(err)
	}
	if err := f1.c.wsReady(context.Background(), "n_one", &proto.WSReadyReq{ID: ws.ID, Gen: claim.Workspace.Generation}); err != nil {
		t.Fatal(err)
	}
	op, err := f1.c.fleetQuarantine(context.Background(), localSubject(), &proto.FleetQuarantineReq{
		Selector: proto.WorkspaceSelector{Node: "n_one"}, Action: proto.FleetActionStop,
		IdempotencyKey: "restartable-containment",
	})
	if err != nil {
		t.Fatal(err)
	}
	f1.c.runFleetOperation(context.Background(), op.ID)
	pending, err := f1.c.fleetGet(context.Background(), localSubject(), op.ID)
	if err != nil || pending.State != proto.FleetStateRunning || pending.Results[0].State != proto.FleetTargetPending {
		t.Fatalf("pending operation=%#v err=%v", pending, err)
	}
	if got := f1.c.snapshotWS(ws.ID); got.State != proto.WSFailed || got.Generation != claim.Workspace.Generation+1 {
		t.Fatalf("offline target was not authoritatively fenced: %#v", got)
	}
	f1.c.Stop()
	if err := f1.log.Close(); err != nil {
		t.Fatal(err)
	}

	f2 := newControlFixture(t, path, nil)
	online := &fakeSender{online: map[string]bool{"n_one": true}}
	online.request = func(_ context.Context, _ string, op string, body any, out any) error {
		if op != proto.OpWSQuarantine {
			return fmt.Errorf("unexpected op %s", op)
		}
		req := body.(proto.WSQuarantineReq)
		*out.(*proto.WSQuarantineRes) = proto.WSQuarantineRes{
			Fenced: true, Generation: req.Gen, Action: req.Action, Backend: "process",
		}
		return nil
	}
	f2.c.Attach(online)
	f2.c.runFleetOperation(context.Background(), op.ID)
	recovered, err := f2.c.fleetGet(context.Background(), localSubject(), op.ID)
	if err != nil || recovered.State != proto.FleetStateCompleted || !recovered.Results[0].Acknowledged {
		t.Fatalf("recovered operation=%#v err=%v", recovered, err)
	}
}

func TestFleetQuarantineCanEscalateCompletedFenceToDestroy(t *testing.T) {
	f := newControlFixture(t, "", nil)
	sender := &fakeSender{online: map[string]bool{"n_one": true}}
	var requests []proto.WSQuarantineReq
	var commits int
	sender.request = func(_ context.Context, _ string, op string, body, out any) error {
		switch op {
		case proto.OpWSQuarantine:
			req := body.(proto.WSQuarantineReq)
			requests = append(requests, req)
			res := proto.WSQuarantineRes{
				Fenced: true, Generation: req.Gen, Action: req.Action, Backend: "process",
			}
			if req.Action == proto.FleetActionDestroy {
				res.Snapshot = "art_sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
			}
			*out.(*proto.WSQuarantineRes) = res
		case proto.OpWSQuarantineCommit:
			commits++
		default:
			return fmt.Errorf("unexpected op %s", op)
		}
		return nil
	}
	f.c.Attach(sender)
	connectNode(t, f.c, "n_one", processNodeInfo(4096))
	ws := createWorkspace(t, f.c, localSubject(), proto.WorkspaceSpec{Run: "escalate"})
	claim, err := f.c.wsClaim(context.Background(), "n_one", ws.ID)
	if err != nil {
		t.Fatal(err)
	}
	if err := f.c.wsReady(context.Background(), "n_one", &proto.WSReadyReq{ID: ws.ID, Gen: claim.Workspace.Generation}); err != nil {
		t.Fatal(err)
	}

	freeze, err := f.c.fleetQuarantine(context.Background(), localSubject(), &proto.FleetQuarantineReq{
		Selector: proto.WorkspaceSelector{Run: "escalate"}, Action: proto.FleetActionFreeze,
		IdempotencyKey: "freeze-first",
	})
	if err != nil {
		t.Fatal(err)
	}
	f.c.runFleetOperation(context.Background(), freeze.ID)
	destroy, err := f.c.fleetQuarantine(context.Background(), localSubject(), &proto.FleetQuarantineReq{
		Selector: proto.WorkspaceSelector{Run: "escalate"}, Action: proto.FleetActionDestroy,
		IdempotencyKey: "destroy-second",
	})
	if err != nil {
		t.Fatal(err)
	}
	f.c.runFleetOperation(context.Background(), destroy.ID)
	done, err := f.c.fleetGet(context.Background(), localSubject(), destroy.ID)
	if err != nil || done.State != proto.FleetStateCompleted || len(done.Results) != 1 || !done.Results[0].Acknowledged {
		t.Fatalf("destroy escalation=%#v err=%v", done, err)
	}
	if len(requests) != 2 || requests[1].Gen != claim.Workspace.Generation ||
		requests[1].Action != proto.FleetActionDestroy || commits != 1 {
		t.Fatalf("requests=%#v commits=%d", requests, commits)
	}
	if got := f.c.snapshotWS(ws.ID); got.State != proto.WSDestroyed || got.QuarantineOperation != destroy.ID || got.LastSnapshot == "" {
		t.Fatalf("destroyed workspace=%#v", got)
	}
}

func TestFleetPartialOperationContinuesReconcilingAfterDeadline(t *testing.T) {
	now := time.Now()
	f := newControlFixture(t, "", func(opts *Options) { opts.Now = func() time.Time { return now } })
	sender := &fakeSender{online: map[string]bool{"n_one": false}}
	sender.request = func(_ context.Context, _ string, op string, body, out any) error {
		if op != proto.OpWSQuarantine {
			return fmt.Errorf("unexpected op %s", op)
		}
		req := body.(proto.WSQuarantineReq)
		*out.(*proto.WSQuarantineRes) = proto.WSQuarantineRes{
			Fenced: true, Generation: req.Gen, Action: req.Action, Backend: "process",
		}
		return nil
	}
	f.c.Attach(sender)
	connectNode(t, f.c, "n_one", processNodeInfo(4096))
	ws := createWorkspace(t, f.c, localSubject(), proto.WorkspaceSpec{Run: "late-node"})
	claim, err := f.c.wsClaim(context.Background(), "n_one", ws.ID)
	if err != nil {
		t.Fatal(err)
	}
	if err := f.c.wsReady(context.Background(), "n_one", &proto.WSReadyReq{ID: ws.ID, Gen: claim.Workspace.Generation}); err != nil {
		t.Fatal(err)
	}
	op, err := f.c.fleetQuarantine(context.Background(), localSubject(), &proto.FleetQuarantineReq{
		Selector: proto.WorkspaceSelector{Run: "late-node"}, Action: proto.FleetActionStop,
		DeadlineMillis: now.Add(time.Second).UnixMilli(), IdempotencyKey: "deadline-reconcile",
	})
	if err != nil {
		t.Fatal(err)
	}
	now = now.Add(2 * time.Second)
	f.c.runFleetOperation(context.Background(), op.ID)
	partial, err := f.c.fleetGet(context.Background(), localSubject(), op.ID)
	if err != nil || partial.State != proto.FleetStatePartial || partial.Results[0].State != proto.FleetTargetPending {
		t.Fatalf("deadline result=%#v err=%v", partial, err)
	}
	sender.mu.Lock()
	sender.online["n_one"] = true
	sender.mu.Unlock()
	f.c.runFleetOperation(context.Background(), op.ID)
	done, err := f.c.fleetGet(context.Background(), localSubject(), op.ID)
	if err != nil || done.State != proto.FleetStateCompleted || !done.Results[0].Acknowledged {
		t.Fatalf("post-deadline reconciliation=%#v err=%v", done, err)
	}
}

func TestFleetEmptySelectionIsDurablyAndObservablyComplete(t *testing.T) {
	f := newControlFixture(t, "", nil)
	req := &proto.FleetQuarantineReq{
		Selector: proto.WorkspaceSelector{Run: "does-not-exist"}, Action: proto.FleetActionFreeze,
		TimeoutMillis: time.Minute.Milliseconds(), IdempotencyKey: "empty-selection",
	}
	op, err := f.c.fleetQuarantine(context.Background(), localSubject(), req)
	if err != nil || op.State != proto.FleetStateCompleted || len(op.Results) != 0 {
		t.Fatalf("empty operation=%#v err=%v", op, err)
	}
	replayed, err := f.c.fleetQuarantine(context.Background(), localSubject(), req)
	if err != nil || replayed.ID != op.ID || replayed.Deadline != op.Deadline {
		t.Fatalf("timeout-based replay=%#v err=%v", replayed, err)
	}
	events, err := f.log.Read(context.Background(), 1, "", 100)
	if err != nil {
		t.Fatal(err)
	}
	requested, completed := 0, 0
	for _, event := range events {
		if event.OperationID != op.ID {
			continue
		}
		switch event.Type {
		case proto.EvFleetRequested:
			requested++
		case proto.EvFleetCompleted:
			completed++
		}
	}
	if requested != 1 || completed != 1 {
		t.Fatalf("fleet events requested=%d completed=%d", requested, completed)
	}
}

func TestMountPathNeedsANamespacedBackend(t *testing.T) {
	f := newControlFixture(t, "", nil)
	info := processNodeInfo(0)
	info.Backends = append(info.Backends, "docker")
	info.BackendDescriptors = append(info.BackendDescriptors, proto.BackendDescriptor{
		Name: "docker", Security: proto.BackendSecurityCaps{
			Isolation: "container", EgressMode: "cooperative_proxy", BrokerIdentity: "token",
			FilesystemBoundary: "bind_mount", NetworkNamespace: true, DeviceIsolation: true,
		}, Runtime: proto.RuntimeCaps{Snapshots: "fs", MountPath: true},
	})
	connectNode(t, f.c, "n_mixed", info)
	connectNode(t, f.c, "n_process", processNodeInfo(0))

	for _, bad := range []string{"relative/path", "/", "/etc/x", "/work/../x", "/proc/self", "/a//b"} {
		if _, err := f.c.wsCreate(context.Background(), localSubject(), &proto.WSCreateReq{Spec: proto.WorkspaceSpec{MountPath: bad}}); err == nil {
			t.Fatalf("mount_path %q accepted", bad)
		}
	}
	ws := createWorkspace(t, f.c, localSubject(), proto.WorkspaceSpec{MountPath: "/home/me/proj"})
	if _, err := f.c.wsClaim(context.Background(), "n_process", ws.ID); err == nil {
		t.Fatal("process-only node claimed a workspace with a mount path it cannot honor")
	}
	claim, err := f.c.wsClaim(context.Background(), "n_mixed", ws.ID)
	if err != nil {
		t.Fatal(err)
	}
	if claim.Workspace.Spec.Requires.Backend != "docker" || claim.Workspace.Spec.MountPath != "/home/me/proj" {
		t.Fatalf("claim = backend %q mount %q", claim.Workspace.Spec.Requires.Backend, claim.Workspace.Spec.MountPath)
	}
	forced := createWorkspace(t, f.c, localSubject(), proto.WorkspaceSpec{MountPath: "/home/me/proj", Requires: proto.Requires{Backend: "process"}})
	if _, err := f.c.wsClaim(context.Background(), "n_mixed", forced.ID); err == nil {
		t.Fatal("forced process backend accepted a mount path")
	}
	// The default is normalized away so an explicit /work behaves like "".
	plain := createWorkspace(t, f.c, localSubject(), proto.WorkspaceSpec{MountPath: proto.DefaultMountPath})
	if plain.Spec.MountPath != "" {
		t.Fatalf("default mount path stored as %q", plain.Spec.MountPath)
	}
	if _, err := f.c.wsClaim(context.Background(), "n_process", plain.ID); err != nil {
		t.Fatal(err)
	}
}

// TestQuotaRejectionEmitsAnEventAndAMetric pins the paired-signal rule for the
// in-process limits: a refusal that only advanced a counter left an operator
// with a number and no way to learn whose request was refused or which limit
// did it. The tenant-authority path emits the same type from inside its
// admission transaction.
func TestQuotaRejectionEmitsAnEventAndAMetric(t *testing.T) {
	f := newControlFixture(t, "", func(opts *Options) {
		opts.MaxWorkspacesPerTenant = 2
		opts.MaxWorkspacesPerSubject = 1
	})
	alice := Subject{ID: "alice", Tenant: "tenant", Roles: []string{"admin"}}
	createWorkspace(t, f.c, alice, proto.WorkspaceSpec{Name: "alice-1"})

	before := metrics.WorkspaceQuotaRejected.Value()
	if _, err := f.c.wsCreate(context.Background(), alice, &proto.WSCreateReq{Spec: proto.WorkspaceSpec{Name: "alice-2"}}); !errors.Is(err, &proto.Error{Code: proto.CodeResourceExhausted}) {
		t.Fatalf("subject quota error = %v", err)
	}
	createWorkspace(t, f.c, Subject{ID: "bob", Tenant: "tenant", Roles: []string{"admin"}}, proto.WorkspaceSpec{Name: "bob-1"})
	if _, err := f.c.wsCreate(context.Background(), Subject{ID: "carol", Tenant: "tenant", Roles: []string{"admin"}}, &proto.WSCreateReq{Spec: proto.WorkspaceSpec{Name: "carol-1"}}); !errors.Is(err, &proto.Error{Code: proto.CodeResourceExhausted}) {
		t.Fatalf("tenant quota error = %v", err)
	}
	if delta := metrics.WorkspaceQuotaRejected.Value() - before; delta != 2 {
		t.Fatalf("quota metric advanced by %d, want 2", delta)
	}

	events, err := f.log.Read(context.Background(), 0, "", 1000)
	if err != nil {
		t.Fatal(err)
	}
	scopes := map[string]map[string]any{}
	for _, event := range events {
		if event.Type != proto.EvQuotaExceeded {
			continue
		}
		if event.Tenant != "tenant" {
			t.Fatalf("quota event tenant = %q, want tenant", event.Tenant)
		}
		var payload map[string]any
		if err := proto.Unmarshal(event.Payload, &payload); err != nil {
			t.Fatalf("decode quota payload: %v", err)
		}
		scope, _ := payload["scope"].(string)
		scopes[scope] = payload
	}
	for _, scope := range []string{"subject", "tenant"} {
		payload := scopes[scope]
		if payload == nil {
			t.Fatalf("a %s quota refusal advanced the metric without emitting an event", scope)
		}
		if payload["resource"] != "workspaces" {
			t.Fatalf("%s quota event resource = %v", scope, payload["resource"])
		}
		if payload["limit"] == nil || payload["used"] == nil {
			t.Fatalf("%s quota event does not name the limit it hit: %v", scope, payload)
		}
	}
}
