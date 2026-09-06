package control

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"remount.dev/remount/internal/proto"
)

// longClaimLease keeps the node's claim lease far outside the fake clock jumps
// these tests make, so what is under test is the lifecycle deadline rather
// than a claim lease expiring at the same instant.
func longClaimLease(o *Options) { o.LeaseSec = 1_000_000 }

// waitPaused polls until the workspace reaches paused. The expiry runs on its
// own goroutine, because the release it performs talks to a node.
func waitPaused(t *testing.T, c *Control, id string) *proto.Workspace {
	t.Helper()
	var last *proto.Workspace
	for deadline := time.Now().Add(10 * time.Second); time.Now().Before(deadline); {
		last = c.snapshotWS(id)
		if last != nil && last.State == proto.WSPaused {
			return last
		}
		c.Tick(context.Background())
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("workspace %s never paused: %+v", id, last)
	return nil
}

// A durable lifecycle deadline is only worth anything if it outlives the
// process that armed it. This restarts the control plane from the same SQLite
// file with the deadline still in the future, and asserts the restarted plane
// fires it: nothing about the schedule lived in memory.
func TestLifecycleDeadlineSurvivesControlRestart(t *testing.T) {
	path := filepath.Join(t.TempDir(), "control.db")
	af := newAgentFixture(t, path, longClaimLease)
	ctx := context.Background()
	ws := createWorkspace(t, af.c, localSubject(), proto.WorkspaceSpec{})
	af.claim(t, ws.ID)

	lease, err := af.c.wsLease(ctx, "alice", &proto.WSLeaseReq{ID: ws.ID, MaxAliveSec: 3600, Reason: "job"})
	if err != nil {
		t.Fatal(err)
	}
	// Mid-deadline: nothing has fired yet.
	af.c.Tick(ctx)
	if got := af.c.snapshotWS(ws.ID); got.State != proto.WSClaimed || !got.LifecycleDeadline.Pending() {
		t.Fatalf("before the deadline: %s %+v", got.State, got.LifecycleDeadline)
	}
	af.c.Stop()
	_ = af.log.Close()

	clock := &af.clock
	again := newAgentFixtureWithNode(t, path, af.nodeKey, func(o *Options) {
		o.Now = func() time.Time { return time.UnixMilli(clock.Load()) }
		longClaimLease(o)
	})
	// The node comes back and re-adopts the tree it still holds.
	again.claim(t, ws.ID)
	restored := again.c.snapshotWS(ws.ID)
	if restored.Lease == nil || restored.Lease.ID != lease.ID {
		t.Fatalf("restart lost the hold: %+v", restored.Lease)
	}
	if !restored.LifecycleDeadline.Pending() || restored.LifecycleDeadline.At != lease.MaxAliveUntil {
		t.Fatalf("restart lost the deadline: %+v", restored.LifecycleDeadline)
	}
	again.c.mu.Lock()
	timer := again.c.timers[lifecycleTimerID(ws.ID)]
	again.c.mu.Unlock()
	if timer == nil || timer.Fired || timer.Kind != proto.TimerKindLeaseExpiry {
		t.Fatalf("restart lost the durable timer row: %+v", timer)
	}

	clock.Add((2 * time.Hour).Milliseconds())
	paused := waitPaused(t, again.c, ws.ID)
	if paused.LifecycleDeadline != nil {
		t.Fatalf("expired deadline still pending: %+v", paused.LifecycleDeadline)
	}
	if paused.Lease == nil || paused.Lease.EndedReason != proto.LeaseEndDeadline {
		t.Fatalf("hold not ended by the deadline: %+v", paused.Lease)
	}
}

// The workspace row, not the timer, is the fence. Once the row stops naming a
// pending deadline, the durable timer must retire rather than act: a schedule
// that outlives the decision it came from is the exact shape of a split brain.
func TestLifecycleTimerRetiredWhenTheRowStopsNamingIt(t *testing.T) {
	af := newAgentFixture(t, "", longClaimLease)
	ctx := context.Background()
	ws := createWorkspace(t, af.c, localSubject(), proto.WorkspaceSpec{})
	af.claim(t, ws.ID)

	if _, err := af.c.wsLease(ctx, "alice", &proto.WSLeaseReq{ID: ws.ID, MaxAliveSec: 3600}); err != nil {
		t.Fatal(err)
	}
	// Simulate a decision recorded on the row without going through move, so
	// the fence itself is under test rather than the synchronous supersession
	// move performs.
	af.c.mu.Lock()
	af.c.workspaces[ws.ID].LifecycleDeadline = nil
	af.c.mu.Unlock()

	af.clock.Add((2 * time.Hour).Milliseconds())
	af.c.Tick(ctx)
	af.c.Tick(ctx)

	got := af.c.snapshotWS(ws.ID)
	if got.State != proto.WSClaimed {
		t.Fatalf("a timer the row no longer names released the workspace: %s", got.State)
	}
	af.c.mu.Lock()
	timer := *af.c.timers[lifecycleTimerID(ws.ID)]
	af.c.mu.Unlock()
	if !timer.Fired || !timer.Superseded {
		t.Fatalf("stale timer was not retired: %+v", timer)
	}
}

// A node loss re-places a workspace and advances its generation without any
// client asking for it. The hold must survive that: dropping the deadline
// there would leak the workspace this feature exists to reclaim, and it is
// precisely the case a client-side timer also gets wrong.
func TestLifecycleDeadlineSurvivesReplacement(t *testing.T) {
	af := newAgentFixture(t, "", longClaimLease)
	ctx := context.Background()
	ws := createWorkspace(t, af.c, localSubject(), proto.WorkspaceSpec{})
	generation := af.claim(t, ws.ID)
	if _, err := af.c.wsLease(ctx, "alice", &proto.WSLeaseReq{ID: ws.ID, MaxAliveSec: 3600}); err != nil {
		t.Fatal(err)
	}
	// The node is lost, the claim lease expires, and another claim takes the
	// workspace at a new generation.
	af.c.mu.Lock()
	live := af.c.workspaces[ws.ID]
	live.State, live.Node, live.LeaseUntil = proto.WSPending, "", 0
	live.Generation = generation + 1
	af.c.mu.Unlock()
	replaced := af.claim(t, ws.ID)
	if replaced <= generation {
		t.Fatalf("re-claim did not advance the generation: %d -> %d", generation, replaced)
	}

	af.clock.Add((2 * time.Hour).Milliseconds())
	paused := waitPaused(t, af.c, ws.ID)
	if paused.Lease == nil || paused.Lease.EndedReason != proto.LeaseEndDeadline {
		t.Fatalf("hold not ended by the deadline after re-placement: %+v", paused.Lease)
	}
	af.c.mu.Lock()
	timer := *af.c.timers[lifecycleTimerID(ws.ID)]
	af.c.mu.Unlock()
	if timer.Generation != replaced {
		t.Fatalf("fired timer recorded generation %d, want the placement it ended, %d", timer.Generation, replaced)
	}
}

// A hold whose deadline has fired cannot be renewed. The caller has to learn
// that its workspace is gone, not silently keep a handle on a lease that no
// longer holds anything.
func TestRenewAfterDeadlineReportsExpired(t *testing.T) {
	af := newAgentFixture(t, "", longClaimLease)
	ctx := context.Background()
	ws := createWorkspace(t, af.c, localSubject(), proto.WorkspaceSpec{})
	af.claim(t, ws.ID)
	lease, err := af.c.wsLease(ctx, "alice", &proto.WSLeaseReq{ID: ws.ID, MaxAliveSec: 60})
	if err != nil {
		t.Fatal(err)
	}
	af.clock.Add((2 * time.Minute).Milliseconds())
	waitPaused(t, af.c, ws.ID)

	_, err = af.c.wsLeaseRenew(ctx, "alice", &proto.WSLeaseRenewReq{ID: ws.ID, LeaseID: lease.ID, ExtendSec: 60})
	var perr *proto.Error
	if !errors.As(err, &perr) || perr.Code != proto.CodeConflict || perr.Reason != proto.ReasonLifecycleDeadlineExpired {
		t.Fatalf("renew after expiry: %v", err)
	}
}

// The activity signal is expected on every turn, so it must not consume a
// durable timer slot each time. One workspace keeps exactly one lifecycle
// timer row no matter how often its idle state flips.
func TestIdleMarkReusesOneTimerRow(t *testing.T) {
	af := newAgentFixture(t, "", func(o *Options) {
		o.MaxTimersPerWorkspace = 4
		longClaimLease(o)
	})
	ctx := context.Background()
	ws := createWorkspace(t, af.c, localSubject(), proto.WorkspaceSpec{})
	af.claim(t, ws.ID)
	if _, err := af.c.wsIdlePolicy(ctx, "alice", &proto.WSIdlePolicyReq{ID: ws.ID, SleepAfterSec: 600}); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 20; i++ {
		if _, err := af.c.wsIdleMark(ctx, "alice", &proto.WSIdleMarkReq{ID: ws.ID, Idle: true}); err != nil {
			t.Fatalf("mark idle %d: %v", i, err)
		}
		if _, err := af.c.wsIdleMark(ctx, "alice", &proto.WSIdleMarkReq{ID: ws.ID, Idle: false}); err != nil {
			t.Fatalf("mark active %d: %v", i, err)
		}
	}
	af.c.mu.Lock()
	rows := 0
	for _, timer := range af.c.timers {
		if timer.WS == ws.ID {
			rows++
		}
	}
	af.c.mu.Unlock()
	if rows != 1 {
		t.Fatalf("idle marking left %d timer rows, want 1", rows)
	}
}

// controlEventTypes counts a workspace's canonical events by type.
func controlEventTypes(t *testing.T, f *controlFixture, id string) map[string]int {
	t.Helper()
	f.c.Tick(context.Background()) // drain the outbox
	events, err := f.log.Read(context.Background(), 1, id, 10000)
	if err != nil {
		t.Fatal(err)
	}
	out := map[string]int{}
	for _, e := range events {
		out[e.Type]++
	}
	return out
}

// An expiry that cannot be carried out must eventually say so. Retrying
// forever and reporting nothing is the worst outcome available here: the
// workspace keeps running, the operator sees a healthy deadline, and the
// leak the hold existed to prevent happens anyway with a record that denies
// it.
func TestLifecycleExpiryFailureDegradesAfterItsRetryBound(t *testing.T) {
	af := newAgentFixture(t, "", longClaimLease)
	ctx := context.Background()
	ws := createWorkspace(t, af.c, localSubject(), proto.WorkspaceSpec{})
	af.claim(t, ws.ID)
	if _, err := af.c.wsLease(ctx, "alice", &proto.WSLeaseReq{ID: ws.ID, MaxAliveSec: 60}); err != nil {
		t.Fatal(err)
	}
	// Take the deadline the way expireLifecycle does, then fail every attempt
	// at carrying it out.
	af.clock.Add((2 * time.Minute).Milliseconds())
	af.c.mu.Lock()
	live := af.c.workspaces[ws.ID]
	fired := *live.LifecycleDeadline
	fired.Fired, fired.FiredAt = true, af.clock.Load()
	live.LifecycleDeadline = &fired
	af.c.mu.Unlock()

	cause := proto.Err(proto.CodeUnreachable, "node is gone")
	for i := 0; i < lifecycleExpiryAttempts; i++ {
		af.c.finishLifecycleExpiry(ws.ID, fired, cause)
		got := af.c.snapshotWS(ws.ID)
		if got.LifecycleDeadline == nil {
			t.Fatalf("attempt %d dropped the deadline", i)
		}
		if got.LifecycleDeadline.Attempts != i+1 {
			t.Fatalf("attempt %d recorded %d attempts", i, got.LifecycleDeadline.Attempts)
		}
		wantFailed := i == lifecycleExpiryAttempts-1
		if got.LifecycleDeadline.Failed != wantFailed {
			t.Fatalf("attempt %d failed=%v, want %v", i, got.LifecycleDeadline.Failed, wantFailed)
		}
	}
	degraded := af.c.snapshotWS(ws.ID)
	if degraded.LifecycleDeadline.Error == "" {
		t.Fatal("a degraded deadline recorded no error")
	}
	if types := controlEventTypes(t, af.controlFixture, ws.ID); types[proto.EvWSLifecycleExpiryFailed] != 1 {
		t.Fatalf("expiry_failed emitted %d times: %v", types[proto.EvWSLifecycleExpiryFailed], types)
	}

	// A degraded workspace is not permanently unschedulable: a fresh hold
	// arms a new deadline, and the failure stays in the log.
	again, err := af.c.wsLease(ctx, "alice", &proto.WSLeaseReq{ID: ws.ID, MaxAliveSec: 120})
	if err != nil {
		t.Fatalf("re-lease after degradation: %v", err)
	}
	rearmed := af.c.snapshotWS(ws.ID)
	if !rearmed.LifecycleDeadline.Pending() || rearmed.LifecycleDeadline.At != again.MaxAliveUntil {
		t.Fatalf("re-lease did not re-arm: %+v", rearmed.LifecycleDeadline)
	}
}

// A hold cannot be taken on a workspace whose expiry the control plane is
// already carrying out. Granting one would promise to keep awake a placement
// that is in the middle of being released.
func TestLeaseRefusedWhileAnExpiryIsExecuting(t *testing.T) {
	af := newAgentFixture(t, "", longClaimLease)
	ctx := context.Background()
	ws := createWorkspace(t, af.c, localSubject(), proto.WorkspaceSpec{})
	af.claim(t, ws.ID)
	if _, err := af.c.wsLease(ctx, "alice", &proto.WSLeaseReq{ID: ws.ID, MaxAliveSec: 60}); err != nil {
		t.Fatal(err)
	}
	af.c.mu.Lock()
	live := af.c.workspaces[ws.ID]
	fired := *live.LifecycleDeadline
	fired.Fired, fired.FiredAt = true, af.clock.Load()
	live.LifecycleDeadline = &fired
	af.c.mu.Unlock()

	_, err := af.c.wsLease(ctx, "alice", &proto.WSLeaseReq{ID: ws.ID, MaxAliveSec: 600})
	var perr *proto.Error
	if !errors.As(err, &perr) || perr.Code != proto.CodeConflict || perr.Reason != proto.ReasonLifecycleDeadlineExpired {
		t.Fatalf("lease during an executing expiry: %v", err)
	}
	if _, err := af.c.wsIdleMark(ctx, "alice", &proto.WSIdleMarkReq{ID: ws.ID, Idle: false}); err == nil {
		t.Fatal("mark active during an executing expiry was accepted")
	}
}
