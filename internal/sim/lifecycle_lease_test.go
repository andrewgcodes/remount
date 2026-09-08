package sim

import (
	"errors"
	"strings"
	"testing"
	"time"

	"remount.dev/remount/internal/client"
	"remount.dev/remount/internal/proto"
	"remount.dev/remount/internal/server"
)

// Durable workspace leases and idle policy (ADR 0090).
//
// The property under test in every case here is the same one: the deadline
// belongs to the control plane. A client that takes a hold and then dies must
// still see its workspace put to sleep, and a client that is alive and
// renewing must never see it slept out from under it.

// leaseDeadline waits for the workspace to reach state, reporting what it saw
// instead. The control plane ticks once a second and a release takes a
// checkpoint, so waits here are in seconds, not milliseconds.
func waitWorkspaceState(t *testing.T, c *client.Client, id, state string, within time.Duration) *proto.Workspace {
	t.Helper()
	ctx := ctxT(t, within+30*time.Second)
	var last *proto.Workspace
	for deadline := time.Now().Add(within); time.Now().Before(deadline); {
		ws, err := c.GetWorkspace(ctx, id)
		if err == nil {
			last = ws
			if ws.State == state {
				return ws
			}
		}
		time.Sleep(50 * time.Millisecond)
	}
	if last == nil {
		t.Fatalf("workspace %s never became readable while waiting for %s", id, state)
	}
	t.Fatalf("workspace %s is %s, want %s (lease=%+v deadline=%+v)", id, last.State, state, last.Lease, last.LifecycleDeadline)
	return nil
}

// leaseEventTypes counts a workspace's canonical events by type.
func leaseEventTypes(t *testing.T, c *client.Client, id string) map[string]int {
	t.Helper()
	ctx := ctxT(t, 60*time.Second)
	events, err := c.ReadEvents(ctx, 1, id)
	if err != nil {
		t.Fatal(err)
	}
	out := map[string]int{}
	for _, e := range events {
		out[e.Type]++
	}
	return out
}

// waitEvent waits for at least n events of typ on a workspace.
func waitLeaseEvent(t *testing.T, c *client.Client, id, typ string, n int, within time.Duration) map[string]int {
	t.Helper()
	var types map[string]int
	for deadline := time.Now().Add(within); time.Now().Before(deadline); {
		types = leaseEventTypes(t, c, id)
		if types[typ] >= n {
			return types
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("workspace %s never recorded %d %s events: %v", id, n, typ, types)
	return nil
}

// TestWorkspaceLeaseAutoSleepsAfterDeadline is the headline case: the client
// that took the hold is severed before the deadline and never comes back, and
// the workspace still sleeps.
func TestWorkspaceLeaseAutoSleepsAfterDeadline(t *testing.T) {
	w := newWorldExpiring(t)
	w.node("n1", nil)
	c := w.client("c1")
	ws := mustWS(t, c, proto.WorkspaceSpec{})
	ctx := ctxT(t, 60*time.Second)

	lease, err := c.LeaseWorkspace(ctx, proto.WSLeaseReq{ID: ws.ID, MaxAliveSec: 2, Reason: "background_job"})
	if err != nil {
		t.Fatal(err)
	}
	if lease.ID == "" || lease.OnExpiry != proto.LeaseExpirySleep {
		t.Fatalf("lease %+v", lease)
	}
	held, err := c.GetWorkspace(ctx, ws.ID)
	if err != nil {
		t.Fatal(err)
	}
	if held.State != proto.WSClaimed || !held.LifecycleDeadline.Pending() ||
		held.LifecycleDeadline.Source != proto.LifecycleSourceLease {
		t.Fatalf("held workspace %+v deadline %+v", held.State, held.LifecycleDeadline)
	}

	// The client dies. Nothing on this side can drive the deadline now.
	w.cut("c1")

	observer := w.client("c2")
	waitWorkspaceState(t, observer, ws.ID, proto.WSPaused, 45*time.Second)
	types := waitLeaseEvent(t, observer, ws.ID, proto.EvWSLifecycleExpired, 1, 20*time.Second)
	if types[proto.EvWSLeaseHoldExpired] == 0 {
		t.Fatalf("no ws.lease.expired event: %v", types)
	}
	if types[proto.EvWSLeaseGranted] == 0 {
		t.Fatalf("no ws.lease.granted event: %v", types)
	}
	after, err := observer.GetWorkspace(ctx, ws.ID)
	if err != nil {
		t.Fatal(err)
	}
	if after.LifecycleDeadline != nil {
		t.Fatalf("paused workspace still has a pending deadline: %+v", after.LifecycleDeadline)
	}
	if after.Lease == nil || after.Lease.EndedReason != proto.LeaseEndDeadline {
		t.Fatalf("lease not ended by the deadline: %+v", after.Lease)
	}
}

// TestWorkspaceLeaseRenewPreventsSleep is the other half of the contract: a
// live caller that keeps renewing keeps its workspace.
func TestWorkspaceLeaseRenewPreventsSleep(t *testing.T) {
	w := newWorld(t)
	w.node("n1", nil)
	c := w.client("c1")
	ws := mustWS(t, c, proto.WorkspaceSpec{})
	ctx := ctxT(t, 60*time.Second)

	lease, err := c.LeaseWorkspace(ctx, proto.WSLeaseReq{ID: ws.ID, MaxAliveSec: 3})
	if err != nil {
		t.Fatal(err)
	}
	// Renew across more than the original deadline.
	for i := 0; i < 5; i++ {
		time.Sleep(700 * time.Millisecond)
		renewed, err := c.RenewLease(ctx, ws.ID, lease.ID, 3)
		if err != nil {
			t.Fatalf("renew %d: %v", i, err)
		}
		if renewed.Renewals != uint64(i+1) {
			t.Fatalf("renew %d recorded %d renewals", i, renewed.Renewals)
		}
	}
	got, err := c.GetWorkspace(ctx, ws.ID)
	if err != nil {
		t.Fatal(err)
	}
	if got.State != proto.WSClaimed {
		t.Fatalf("renewed workspace is %s", got.State)
	}
	if types := leaseEventTypes(t, c, ws.ID); types[proto.EvWSLifecycleExpired] != 0 {
		t.Fatalf("a renewed hold expired anyway: %v", types)
	}
}

// TestWorkspaceLeaseCancelKeepsClaimed proves cancelling a hold is not the
// same as letting it expire: the workspace keeps running.
func TestWorkspaceLeaseCancelKeepsClaimed(t *testing.T) {
	w := newWorld(t)
	w.node("n1", nil)
	c := w.client("c1")
	ws := mustWS(t, c, proto.WorkspaceSpec{})
	ctx := ctxT(t, 60*time.Second)

	lease, err := c.LeaseWorkspace(ctx, proto.WSLeaseReq{ID: ws.ID, MaxAliveSec: 2})
	if err != nil {
		t.Fatal(err)
	}
	after, err := c.CancelLease(ctx, ws.ID, lease.ID)
	if err != nil {
		t.Fatal(err)
	}
	if after.LifecycleDeadline != nil {
		t.Fatalf("cancelled hold left a deadline: %+v", after.LifecycleDeadline)
	}
	if _, err := c.GetLease(ctx, ws.ID); err != nil {
		t.Fatalf("cancelled lease should still be readable: %v", err)
	}
	// Well past the original deadline.
	time.Sleep(4 * time.Second)
	got, err := c.GetWorkspace(ctx, ws.ID)
	if err != nil {
		t.Fatal(err)
	}
	if got.State != proto.WSClaimed {
		t.Fatalf("cancelled hold still slept the workspace: %s", got.State)
	}
	types := leaseEventTypes(t, c, ws.ID)
	if types[proto.EvWSLeaseCancelled] == 0 || types[proto.EvWSLifecycleExpired] != 0 {
		t.Fatalf("events: %v", types)
	}
	// A renew after cancel names why, rather than a bare not_found.
	_, err = c.RenewLease(ctx, ws.ID, lease.ID, 5)
	var perr *proto.Error
	if !errors.As(err, &perr) || perr.Code != proto.CodeConflict ||
		perr.Reason != proto.ReasonLifecycleDeadlineExpired {
		t.Fatalf("renew after cancel: %v", err)
	}
}

// TestIdlePolicySleepsAfterMarkIdle exercises the second half of the API: no
// explicit hold, just a policy and a caller saying the turn settled.
func TestIdlePolicySleepsAfterMarkIdle(t *testing.T) {
	w := newWorld(t)
	w.node("n1", nil)
	c := w.client("c1")
	ws := mustWS(t, c, proto.WorkspaceSpec{})
	ctx := ctxT(t, 60*time.Second)

	if _, err := c.SetIdlePolicy(ctx, ws.ID, 2, 0); err != nil {
		t.Fatal(err)
	}
	// A policy alone schedules nothing: idle is a statement, not an inference.
	got, err := c.GetWorkspace(ctx, ws.ID)
	if err != nil {
		t.Fatal(err)
	}
	if got.LifecycleDeadline != nil {
		t.Fatalf("policy armed a deadline before the workspace was idle: %+v", got.LifecycleDeadline)
	}
	marked, err := c.MarkIdle(ctx, ws.ID, "turn_settled")
	if err != nil {
		t.Fatal(err)
	}
	if !marked.LifecycleDeadline.Pending() || marked.LifecycleDeadline.Source != proto.LifecycleSourceIdle {
		t.Fatalf("mark idle deadline %+v", marked.LifecycleDeadline)
	}
	waitWorkspaceState(t, c, ws.ID, proto.WSPaused, 45*time.Second)
	types := waitLeaseEvent(t, c, ws.ID, proto.EvWSLifecycleExpired, 1, 20*time.Second)
	if types[proto.EvWSIdlePolicySet] == 0 || types[proto.EvWSIdleMarked] == 0 {
		t.Fatalf("events: %v", types)
	}
}

// TestMarkActiveResetsIdleDeadline proves the activity signal actually clears
// the pending deadline rather than merely restarting a clock nobody reads.
func TestMarkActiveResetsIdleDeadline(t *testing.T) {
	w := newWorld(t)
	w.node("n1", nil)
	c := w.client("c1")
	ws := mustWS(t, c, proto.WorkspaceSpec{})
	ctx := ctxT(t, 60*time.Second)

	if _, err := c.SetIdlePolicy(ctx, ws.ID, 3, 0); err != nil {
		t.Fatal(err)
	}
	if _, err := c.MarkIdle(ctx, ws.ID, "turn_settled"); err != nil {
		t.Fatal(err)
	}
	time.Sleep(time.Second)
	active, err := c.MarkActive(ctx, ws.ID, "new_turn")
	if err != nil {
		t.Fatal(err)
	}
	if active.LifecycleDeadline != nil {
		t.Fatalf("mark active left a deadline: %+v", active.LifecycleDeadline)
	}
	if active.IdleSince != 0 || active.LastActivityAt == 0 {
		t.Fatalf("idle clock not reset: idle_since=%d last_activity=%d", active.IdleSince, active.LastActivityAt)
	}
	// Well past the deadline the first mark would have set.
	time.Sleep(5 * time.Second)
	got, err := c.GetWorkspace(ctx, ws.ID)
	if err != nil {
		t.Fatal(err)
	}
	if got.State != proto.WSClaimed {
		t.Fatalf("active workspace slept anyway: %s", got.State)
	}
	if types := leaseEventTypes(t, c, ws.ID); types[proto.EvWSLifecycleExpired] != 0 {
		t.Fatalf("an active workspace expired: %v", types)
	}
}

// TestLongCommandAutoSleptReplaysExplicitExitReason answers the question a
// client actually asks after a lifecycle sleep: what happened to my command?
// The replayed log has to say the control plane ended it, not leave the reader
// to infer a policy decision from a process that merely stopped.
func TestLongCommandAutoSleptReplaysExplicitExitReason(t *testing.T) {
	w := newWorld(t)
	w.node("n1", nil)
	c := w.client("c1")
	ws := mustWS(t, c, proto.WorkspaceSpec{})
	ctx := ctxT(t, 120*time.Second)

	live, err := c.Exec(ctx, proto.SOpenReq{WS: ws.ID, Program: []string{"sh", "-c", "echo started; sleep 300"}})
	if err != nil {
		t.Fatal(err)
	}
	sid := live.ID
	// Wait for the command to have produced something, so the replay has a
	// prefix to be byte-identical about.
	select {
	case chunk, ok := <-live.Chunks():
		if !ok || !strings.Contains(string(chunk.Data), "started") {
			t.Fatalf("first chunk %q ok=%v", chunk.Data, ok)
		}
	case <-time.After(30 * time.Second):
		t.Fatal("session produced no output")
	}

	if _, err := c.LeaseWorkspace(ctx, proto.WSLeaseReq{ID: ws.ID, MaxAliveSec: 2, Reason: "long_command"}); err != nil {
		t.Fatal(err)
	}
	waitWorkspaceState(t, c, ws.ID, proto.WSPaused, 45*time.Second)

	// The durable event log names the reason without anyone reattaching.
	types := waitLeaseEvent(t, c, ws.ID, proto.EvSExited, 1, 30*time.Second)
	if types[proto.EvWSLifecycleExpired] == 0 {
		t.Fatalf("events: %v", types)
	}
	events, err := c.ReadEvents(ctx, 1, ws.ID)
	if err != nil {
		t.Fatal(err)
	}
	sawExitReason := false
	for _, e := range events {
		if e.Type != proto.EvSExited || e.Session != sid {
			continue
		}
		var payload struct {
			Reason string `cbor:"reason"`
		}
		if err := proto.Unmarshal(e.Payload, &payload); err != nil {
			t.Fatalf("decode s.exited payload: %v", err)
		}
		if payload.Reason != proto.ReasonLifecycleDeadlineExpired {
			t.Fatalf("s.exited reason %q, want %q", payload.Reason, proto.ReasonLifecycleDeadlineExpired)
		}
		sawExitReason = true
	}
	if !sawExitReason {
		t.Fatal("no s.exited event for the killed session")
	}

	// And so does a replay of the session log itself, once the workspace is
	// back: the exit chunk is the last one and it carries the reason.
	if _, err := c.WakeWorkspace(ctx, ws.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := c.WaitClaimed(ctx, ws.ID); err != nil {
		t.Fatal(err)
	}
	replay, err := c.Attach(ctx, ws.ID, sid, 0)
	if err != nil {
		t.Fatalf("attach for replay: %v", err)
	}
	var output strings.Builder
	for chunk := range replay.Chunks() {
		if chunk.Stream == proto.StreamStdout {
			output.Write(chunk.Data)
		}
	}
	exit := replay.Exit()
	if exit == nil {
		t.Fatalf("replay ended with no exit chunk: %v", replay.Err())
	}
	if exit.Reason != proto.ReasonLifecycleDeadlineExpired {
		t.Fatalf("replayed exit reason %q, want %q (exit %+v)", exit.Reason, proto.ReasonLifecycleDeadlineExpired, exit)
	}
	if !strings.Contains(output.String(), "started") {
		t.Fatalf("replay lost the output produced before the deadline: %q", output.String())
	}
}

// TestWorkspaceMoveSupersedesLeaseTimer proves a hold is bound to a placement.
// A move is a client decision to run somewhere else, so the old deadline must
// not act on the new generation, and renewing the old hold must say why.
func TestWorkspaceMoveSupersedesLeaseTimer(t *testing.T) {
	w := newWorld(t)
	w.node("n1", map[string]string{"zone": "a"})
	w.node("n2", map[string]string{"zone": "b"})
	c := w.client("c1")
	ws := mustWS(t, c, proto.WorkspaceSpec{Placement: proto.Placement{Allow: map[string]string{"zone": "a"}}})
	ctx := ctxT(t, 90*time.Second)

	lease, err := c.LeaseWorkspace(ctx, proto.WSLeaseReq{ID: ws.ID, MaxAliveSec: 3})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := c.MoveWorkspace(ctx, ws.ID, nil, &proto.Placement{Allow: map[string]string{"zone": "b"}}); err != nil {
		t.Fatal(err)
	}
	moved, err := c.WaitClaimed(ctx, ws.ID)
	if err != nil {
		t.Fatal(err)
	}
	if moved.LifecycleDeadline != nil {
		t.Fatalf("move kept a deadline from the old placement: %+v", moved.LifecycleDeadline)
	}
	_, err = c.RenewLease(ctx, ws.ID, lease.ID, 10)
	var perr *proto.Error
	if !errors.As(err, &perr) || perr.Code != proto.CodeConflict || perr.Reason != proto.ReasonGenerationMismatch {
		t.Fatalf("renew after move: %v", err)
	}
	// Well past the original deadline, on the new node.
	time.Sleep(5 * time.Second)
	got, err := c.GetWorkspace(ctx, ws.ID)
	if err != nil {
		t.Fatal(err)
	}
	if got.State != proto.WSClaimed {
		t.Fatalf("superseded timer slept the moved workspace: %s", got.State)
	}
	types := leaseEventTypes(t, c, ws.ID)
	if types[proto.EvWSLifecycleExpired] != 0 {
		t.Fatalf("a superseded deadline fired: %v", types)
	}
	if types[proto.EvWSLeaseHoldExpired] == 0 {
		t.Fatalf("the move recorded no supersession: %v", types)
	}
}

// TestDuplicateLeaseRequestIsOneTimer replays the same mutating request and
// asserts it neither grants a second hold nor arms a second timer.
func TestDuplicateLeaseRequestIsOneTimer(t *testing.T) {
	w := newWorldExpiring(t)
	w.node("n1", nil)
	c := w.client("c1")
	ws := mustWS(t, c, proto.WorkspaceSpec{})
	ctx := ctxT(t, 60*time.Second)

	req := proto.WSLeaseReq{ID: ws.ID, MaxAliveSec: 30, IdempotencyKey: "lease-once"}
	first, err := c.LeaseWorkspace(ctx, req)
	if err != nil {
		t.Fatal(err)
	}
	second, err := c.LeaseWorkspace(ctx, req)
	if err != nil {
		t.Fatal(err)
	}
	if first.ID != second.ID || first.MaxAliveUntil != second.MaxAliveUntil {
		t.Fatalf("replay granted a different hold: %+v vs %+v", first, second)
	}
	timers, err := c.ListTimers(ctx)
	if err != nil {
		t.Fatal(err)
	}
	lifecycle := 0
	for _, timer := range timers {
		if timer.WS == ws.ID && timer.Lifecycle() && !timer.Fired {
			lifecycle++
		}
	}
	if lifecycle != 1 {
		t.Fatalf("replay armed %d lifecycle timers: %+v", lifecycle, timers)
	}
	if types := leaseEventTypes(t, c, ws.ID); types[proto.EvWSLeaseGranted] != 1 {
		t.Fatalf("replay emitted %d grant events", types[proto.EvWSLeaseGranted])
	}
}

// TestLeaseExpiryDuringNodeCutNoSplitBrain cuts the node's uplink around the
// deadline. The workspace must reach exactly one outcome: slept, or explicitly
// degraded. What it must never do is expire twice, or expire and also carry on
// as though nothing happened.
func TestLeaseExpiryDuringNodeCutNoSplitBrain(t *testing.T) {
	w := newWorldExpiring(t)
	w.node("n1", nil)
	c := w.client("c1")
	ws := mustWS(t, c, proto.WorkspaceSpec{})
	ctx := ctxT(t, 120*time.Second)

	if _, err := c.LeaseWorkspace(ctx, proto.WSLeaseReq{ID: ws.ID, MaxAliveSec: 3}); err != nil {
		t.Fatal(err)
	}
	// Flap the node's uplink across the deadline.
	for i := 0; i < 6; i++ {
		w.cut("n1")
		time.Sleep(500 * time.Millisecond)
	}

	var types map[string]int
	settled := false
	for deadline := time.Now().Add(60 * time.Second); time.Now().Before(deadline); {
		types = leaseEventTypes(t, c, ws.ID)
		got, err := c.GetWorkspace(ctx, ws.ID)
		if err == nil && got.State == proto.WSPaused {
			settled = true
			break
		}
		if types[proto.EvWSLifecycleExpiryFailed] > 0 {
			settled = true
			break
		}
		time.Sleep(200 * time.Millisecond)
	}
	if !settled {
		t.Fatalf("lease expiry across a node cut never settled: %v", types)
	}
	if types[proto.EvWSLifecycleExpired] > 1 {
		t.Fatalf("the deadline fired %d times: %v", types[proto.EvWSLifecycleExpired], types)
	}
	if types[proto.EvWSLifecycleExpired] == 0 && types[proto.EvWSLifecycleExpiryFailed] == 0 {
		t.Fatalf("workspace settled without recording why: %v", types)
	}
}

// TestLeaseQuotaPerTenant proves holds are admission-controlled: a tenant
// cannot pin unbounded compute awake, and the refusal names its limit.
func TestLeaseQuotaPerTenant(t *testing.T) {
	w := newWorldWith(t, func(o *server.Options) { o.MaxHeldWorkspacesPerTenant = 1 })
	w.node("n1", nil)
	c := w.client("c1")
	ctx := ctxT(t, 90*time.Second)
	first := mustWS(t, c, proto.WorkspaceSpec{})
	second := mustWS(t, c, proto.WorkspaceSpec{})

	if _, err := c.LeaseWorkspace(ctx, proto.WSLeaseReq{ID: first.ID, MaxAliveSec: 60}); err != nil {
		t.Fatal(err)
	}
	_, err := c.LeaseWorkspace(ctx, proto.WSLeaseReq{ID: second.ID, MaxAliveSec: 60})
	var perr *proto.Error
	if !errors.As(err, &perr) || perr.Code != proto.CodeResourceExhausted || perr.Reason != proto.ReasonQuotaExceeded {
		t.Fatalf("second hold: %v", err)
	}
	// Releasing the first hold frees the slot.
	held, err := c.GetLease(ctx, first.ID)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := c.CancelLease(ctx, first.ID, held.Lease.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := c.LeaseWorkspace(ctx, proto.WSLeaseReq{ID: second.ID, MaxAliveSec: 60}); err != nil {
		t.Fatalf("hold after cancel: %v", err)
	}
}
