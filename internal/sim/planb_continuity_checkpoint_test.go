package sim

// Plan B B15 and B16: the two sides of the durable checkpoint commit point.
//
// B15 loses the node after it has quiesced, archived and uploaded but before
// the control plane commits the digest. Nothing about that outcome is known,
// so the source must stay retained and fenced rather than be replaced from an
// older snapshot. B16 loses the node after a commit, and the replacement must
// take exactly one new generation and publish only what was committed — the
// stale tree the old node still holds must never become the answer.
//
// TestNodeDeathMovesWorkspaceFromSnapshot already covers the plain failover.
// What is new here is the ambiguous half of the protocol: the lost prepared
// response, and the return of the node whose copy diverged from the digest.

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"remount.dev/remount/internal/client"
	"remount.dev/remount/internal/node"
	"remount.dev/remount/internal/proto"
)

// planBLostPrepare drops the node's prepared release result on the wire. The
// failure is therefore a lost frame between two real peers, not a poke at
// Control's state: from the control plane's side it is indistinguishable from
// a node that died with the checkpoint already on disk.
type planBLostPrepare struct {
	armed    atomic.Bool
	dropped  atomic.Int64
	once     sync.Once
	prepared chan struct{}
}

func newPlanBLostPrepare() *planBLostPrepare {
	return &planBLostPrepare{prepared: make(chan struct{})}
}

// hook is installed on the node side of the pipe: it sees everything the node
// sends. A response carries the op it answers, so no correlation table is
// needed to recognize the prepared release.
func (l *planBLostPrepare) hook(f *proto.Frame) bool {
	if !l.armed.Load() || f.T != proto.KindRes || f.Op != proto.OpWSRelease {
		return true
	}
	l.dropped.Add(1)
	l.once.Do(func() { close(l.prepared) })
	return false
}

func TestPlanBContinuityNodeLostBeforeCheckpointCommitRetainsAndFencesSource(t *testing.T) {
	w := newWorldExpiring(t)
	lost := newPlanBLostPrepare()
	// Hooks are read when a peer dials, so they are installed before the
	// nodes exist. Both carry the same one: the holder is whichever node the
	// control plane picks, and only the holder is asked to release.
	w.mu.Lock()
	w.peerHooks["b15-n1"] = lost.hook
	w.peerHooks["b15-n2"] = lost.hook
	w.mu.Unlock()

	names := map[string]string{}
	for _, name := range []string{"b15-n1", "b15-n2"} {
		n := w.node(name, map[string]string{"zone": "b15"})
		names[n.ID()] = name
	}
	c := w.client("b15-c1")
	ctx := ctxT(t, 8*time.Minute)
	placement := proto.Placement{Allow: map[string]string{"zone": "b15"}}
	created, err := c.CreateWorkspace(ctx, proto.WorkspaceSpec{Name: "b15", Placement: placement})
	if err != nil {
		t.Fatal(err)
	}
	before, err := c.WaitClaimed(ctx, created.ID)
	if err != nil {
		t.Fatal(err)
	}
	if err := c.WriteFile(ctx, before.ID, "state.txt", []byte("v1"), 0); err != nil {
		t.Fatal(err)
	}
	info, err := c.WorkspaceInfo(ctx, before.ID)
	if err != nil {
		t.Fatal(err)
	}
	holder := names[before.Node]
	if holder == "" {
		t.Fatalf("unknown holder in %s", continuityRow(before))
	}

	// A move is the destructive lifecycle operation: prepare, checkpoint,
	// commit, destroy source. Arm the loss and start it.
	lost.armed.Store(true)
	moveErr := make(chan error, 1)
	go func() {
		_, err := c.MoveWorkspace(ctx, before.ID, nil, &placement, client.WithIdempotencyKey("b15-move"))
		moveErr <- err
	}()
	select {
	case <-lost.prepared:
	case <-time.After(2 * time.Minute):
		t.Fatal("the node never produced a prepared release; the loss was never injected")
	}
	// The node is gone with the checkpoint already archived and uploaded and
	// its source still on disk. The control plane has no result at all.
	w.stopNode(holder)

	select {
	case err := <-moveErr:
		if err == nil {
			t.Fatal("the move reported success although no checkpoint digest was ever committed")
		}
		t.Logf("move reported the unknown outcome: %v", err)
	case <-ctx.Done():
		t.Fatal("the move never returned an outcome")
	}
	if lost.dropped.Load() == 0 {
		t.Fatal("no prepared release was dropped; this run did not reach the commit boundary")
	}
	// The archive really was produced before the loss, so "before commit" is
	// a statement about the commit and not about the checkpoint.
	ids, err := w.srv.Store.List()
	if err != nil {
		t.Fatal(err)
	}
	if len(ids) == 0 {
		t.Fatal("the node uploaded no checkpoint, so the run stopped short of the commit boundary")
	}

	fenced := awaitWorkspace(t, ctx, c, before.ID, "fenced after the lost prepared release", 2*time.Minute,
		func(ws *proto.Workspace) bool { return ws.State == proto.WSFailed })
	if fenced.Node != before.Node || fenced.Generation != before.Generation {
		t.Fatalf("fenced workspace changed hands: %s want node=%s gen=%d", continuityRow(fenced), before.Node, before.Generation)
	}
	if fenced.LeaseUntil != 0 {
		t.Fatalf("a fenced workspace still holds a lease: %s", continuityRow(fenced))
	}
	// The uncommitted digest is not failover state. Adopting it would make an
	// archive whose commit never happened look like authority.
	if fenced.LastSnapshot != "" {
		t.Fatalf("an uncommitted checkpoint became the authoritative snapshot: %s", continuityRow(fenced))
	}

	// The other zone=b15 node is online and eligible the whole time. The
	// fence is only proven by it never taking the workspace.
	requireStableWorkspace(t, ctx, c, "a fenced source must not be reassigned", 8*time.Second, fenced)

	// Retention: the source tree is still on the lost node's disk, unchanged.
	if _, err := os.Stat(info.Root); err != nil {
		t.Fatalf("source was not retained at %s: %v", info.Root, err)
	}
	kept, err := os.ReadFile(filepath.Join(info.Root, "state.txt"))
	if err != nil || string(kept) != "v1" {
		t.Fatalf("retained source content = %q, %v; want v1", kept, err)
	}

	// Events name the transition that fenced it, and never claim a release.
	changes := continuityStateChanges(t, ctx, c, before.ID)
	var abort *continuityStateChange
	for i := range changes {
		if changes[i].From == proto.WSQuiescing && changes[i].To == proto.WSFailed {
			abort = &changes[i]
		}
		if changes[i].To == proto.WSReleased {
			t.Fatalf("the workspace was released without a committed checkpoint: %s", changes[i])
		}
	}
	if abort == nil {
		t.Fatalf("no quiescing->failed transition was recorded: %v", changes)
	}
	if abort.Operation != "release.abort" || abort.Node != before.Node || abort.Generation != before.Generation {
		t.Fatalf("fencing transition = %s, want release.abort on %s at gen %d", abort, before.Node, before.Generation)
	}
	counts := eventTypes(t, ctx, c, before.ID)
	if counts[proto.EvWSReleased] != 0 {
		t.Fatalf("ws.released was emitted %d times without a committed digest", counts[proto.EvWSReleased])
	}
	if counts[proto.EvWSClaimed] != 1 || counts[proto.EvWSRestored] != 0 {
		t.Fatalf("a fenced workspace was re-claimed or restored: %v", counts)
	}
}

func TestPlanBContinuityNodeLostAfterCommitYieldsOnlyTheNewGeneration(t *testing.T) {
	w := newWorldExpiring(t)
	dirs := map[string]string{"b16-n1": t.TempDir(), "b16-n2": t.TempDir()}
	configure := func(name string) func(*node.Options) {
		return func(o *node.Options) {
			o.DataDir = dirs[name]
			o.Labels = map[string]string{"zone": "b16"}
		}
	}
	names := map[string]string{}
	for name := range dirs {
		names[w.nodeWith(name, configure(name)).ID()] = name
	}
	c := w.client("b16-c1")
	ctx := ctxT(t, 6*time.Minute)
	placement := proto.Placement{Allow: map[string]string{"zone": "b16"}}
	created, err := c.CreateWorkspace(ctx, proto.WorkspaceSpec{Name: "b16", Placement: placement})
	if err != nil {
		t.Fatal(err)
	}
	before, err := c.WaitClaimed(ctx, created.ID)
	if err != nil {
		t.Fatal(err)
	}
	holder := names[before.Node]
	if holder == "" {
		t.Fatalf("unknown holder in %s", continuityRow(before))
	}

	// The commit point: an authoritative checkpoint whose digest the control
	// plane durably records as this workspace's failover state.
	if err := c.WriteFile(ctx, before.ID, "state.txt", []byte("v1-committed"), 0); err != nil {
		t.Fatal(err)
	}
	checkpoint, err := c.Checkpoint(ctx, before.ID, client.WithIdempotencyKey("b16-commit"))
	if err != nil {
		t.Fatal(err)
	}
	if !checkpoint.Authoritative || checkpoint.Artifact == "" {
		t.Fatalf("checkpoint = %+v", checkpoint)
	}
	committed, err := c.GetWorkspace(ctx, before.ID)
	if err != nil {
		t.Fatal(err)
	}
	if committed.LastSnapshot != checkpoint.Artifact {
		t.Fatalf("checkpoint digest %s is not the authoritative snapshot: %s", checkpoint.Artifact, continuityRow(committed))
	}
	// A write that lands after the commit exists only in the source tree. It
	// is the marker that proves which copy a later reader is looking at.
	if err := c.WriteFile(ctx, before.ID, "state.txt", []byte("v2-after-the-commit"), 0); err != nil {
		t.Fatal(err)
	}

	sourceInfo, err := c.WorkspaceInfo(ctx, before.ID)
	if err != nil {
		t.Fatal(err)
	}
	sourceRoot := sourceInfo.Root

	// A raw client peer keeps the grant it holds at this generation. The SDK
	// would refresh it, which is exactly what a fencing proof must not do.
	raw := continuityRawClient(t, ctx, w.dialer("b16-raw"), "tok", "c_b16raw")
	stale := continuityGrant(t, ctx, raw, before.ID)
	if stale.Claims.Gen != committed.Generation || stale.Claims.Node != committed.Node {
		t.Fatalf("grant %+v does not match %s", stale.Claims, continuityRow(committed))
	}

	// Lose the node after the commit.
	w.stopNode(holder)
	replaced := awaitWorkspace(t, ctx, c, before.ID, "claimed by a replacement node", 2*time.Minute,
		func(ws *proto.Workspace) bool { return ws.State == proto.WSClaimed && ws.Node != committed.Node })
	// A death failover advances the generation exactly twice and no further:
	// once when the lease expires, which is what revokes every grant made
	// under the lost assignment, and once when the replacement claims.
	if replaced.Generation != committed.Generation+2 {
		t.Fatalf("replacement took generation %d, want %d (one lease-expiry fence plus one claim from %d): %s",
			replaced.Generation, committed.Generation+2, committed.Generation, continuityRow(replaced))
	}
	planBRequireEventAtGeneration(t, ctx, c, before.ID, proto.EvWSLeaseExpired, committed.Generation+1)
	planBRequireEventAtGeneration(t, ctx, c, before.ID, proto.EvWSClaiming, replaced.Generation)
	planBRequireEventAtGeneration(t, ctx, c, before.ID, proto.EvWSClaimed, replaced.Generation)
	planBRequireEventAtGeneration(t, ctx, c, before.ID, proto.EvWSRestored, replaced.Generation)
	if replaced.LastSnapshot != checkpoint.Artifact {
		t.Fatalf("replacement restored from %q, want the committed digest %s", replaced.LastSnapshot, checkpoint.Artifact)
	}
	if body, err := c.ReadFile(ctx, before.ID, "state.txt"); err != nil || string(body) != "v1-committed" {
		t.Fatalf("replacement published %q (%v); only the committed checkpoint is authoritative", body, err)
	}
	counts := eventTypes(t, ctx, c, before.ID)
	if counts[proto.EvWSLeaseExpired] != 1 || counts[proto.EvWSRestored] != 1 || counts[proto.EvWSClaimed] != 2 {
		t.Fatalf("failover events = %v", counts)
	}

	// The replacement serves only its own generation: every grant minted
	// under the lost assignment is refused rather than quietly honored.
	// Control caps a grant's lifetime at the lease it was minted under, so
	// with this world's two-second lease the expiry fence answers before the
	// generation check is reached. B17 exercises the generation fence on its
	// own, with a lease long enough for the grant to outlive the move.
	_, err = continuityReadWithGrant(ctx, raw, replaced.Node, before.ID, "state.txt", stale)
	switch code := continuityCode(err); code {
	case proto.CodeUnauthorized, proto.CodeConflict:
	default:
		t.Fatalf("grant minted at gen %d on %s was not refused by generation %d on %s: %v (code %q)",
			stale.Claims.Gen, stale.Claims.Node, replaced.Generation, replaced.Node, err, code)
	}
	// Positive control: the same peer, the same call, a current grant.
	current := continuityGrant(t, ctx, raw, before.ID)
	if current.Claims.Gen != replaced.Generation {
		t.Fatalf("refreshed grant %+v does not match %s", current.Claims, continuityRow(replaced))
	}
	body, err := continuityReadWithGrant(ctx, raw, replaced.Node, before.ID, "state.txt", current)
	if err != nil || string(body) != "v1-committed" {
		t.Fatalf("current grant read = %q, %v", body, err)
	}

	// The replacement's copy moves on. Only it is authoritative.
	if err := c.WriteFile(ctx, before.ID, "state.txt", []byte("v3-on-the-replacement"), 0); err != nil {
		t.Fatal(err)
	}

	// The lost node returns with its whole tree, including the write that was
	// never committed. Its re-adoption must be refused: it is a node holding
	// a superseded generation, not the current holder.
	returned := w.nodeWith(holder, configure(holder))
	if returned.ID() != before.Node {
		t.Fatalf("restarted node identity changed: %s != %s", returned.ID(), before.Node)
	}
	requireStableWorkspace(t, ctx, c, "a superseded node must not reclaim its tree", 8*time.Second, replaced)
	if body, err := c.ReadFile(ctx, before.ID, "state.txt"); err != nil || string(body) != "v3-on-the-replacement" {
		t.Fatalf("after the stale node returned the workspace reads %q (%v); the diverged copy was published", body, err)
	}
	after := eventTypes(t, ctx, c, before.ID)
	if after[proto.EvWSClaiming] != counts[proto.EvWSClaiming] || after[proto.EvWSClaimed] != counts[proto.EvWSClaimed] {
		t.Fatalf("the returning node was granted a claim: before=%v after=%v", counts, after)
	}
	// The superseded tree still exists and still holds the uncommitted write,
	// which is why publishing it would have been a silent rollback.
	orphan, err := os.ReadFile(filepath.Join(sourceRoot, "state.txt"))
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("inspect the superseded tree at %s: %v", sourceRoot, err)
	}
	if err == nil && string(orphan) != "v2-after-the-commit" {
		t.Fatalf("superseded tree content = %q, want the uncommitted write it was lost with", orphan)
	}
}

// planBRequireEventAtGeneration asserts the workspace log carries typ stamped
// with exactly this generation. Naming the generation is the point: a
// recovery that reached the right state under the wrong authority is not a
// recovery.
func planBRequireEventAtGeneration(t *testing.T, ctx context.Context, c *client.Client, ws, typ string, generation uint64) {
	t.Helper()
	evs, err := c.ReadEvents(ctx, 0, ws)
	if err != nil {
		t.Fatal(err)
	}
	var seen []uint64
	for _, e := range evs {
		if e.Type != typ {
			continue
		}
		if e.Generation == generation {
			return
		}
		seen = append(seen, e.Generation)
	}
	t.Fatalf("no %s event at generation %d (saw generations %v)", typ, generation, seen)
}
