package control

import (
	"context"
	"time"

	"remount.dev/remount/internal/ids"
	"remount.dev/remount/internal/metrics"
	"remount.dev/remount/internal/proto"
)

// Durable workspace leases and idle policy (ADR 0090).
//
// A client that starts a long-running job cannot own the deadline for that
// job: its process gets deployed, scaled down, or simply dies, and an
// in-process timer dies with it. These operations move the deadline into the
// control plane, where it is a durable row that survives client death, node
// loss, and a control-plane restart.
//
// The model adds no top-level workspace state. "Idle but held" and "scheduled
// to sleep" are derived views over `claimed`, published on the workspace row
// as LifecycleDeadline so a client reads them from ws.get without knowing that
// timers exist.

const (
	// lifecycleExpiryTimeout bounds one attempt at executing a deadline. The
	// release underneath takes a checkpoint, so it is generous.
	lifecycleExpiryTimeout = 10 * time.Minute
	// lifecycleExpiryAttempts bounds how often the control plane retries a
	// failed expiry before declaring the workspace degraded. A deadline that
	// silently stops trying is worse than one that says it gave up.
	lifecycleExpiryAttempts = 5
	// lifecycleExpiryBackoff spaces those attempts.
	lifecycleExpiryBackoff = 5 * time.Second
	// defaultMaxLeaseSec bounds any single hold, renewals included.
	defaultMaxLeaseSec = 24 * 60 * 60
	// defaultMaxHeldWorkspacesPerTenant bounds how many workspaces one tenant
	// may pin awake at once.
	defaultMaxHeldWorkspacesPerTenant = 256
)

// lifecycleTimerID is the workspace's single durable lifecycle timer slot.
//
// One deterministic row per workspace, rewritten in place, is what makes a
// high-frequency activity signal affordable: ws.idle.mark is expected on every
// agent turn, and allocating a fresh timer id per call would exhaust
// MaxTimersPerWorkspace within a session and leave a spent row behind for
// retention to collect each time.
func lifecycleTimerID(ws string) string { return "t_lc_" + ws }

// lifecycleState is the per-workspace execution bookkeeping for a fired
// deadline. It is in-memory only: the durable authority is
// Workspace.LifecycleDeadline, which is why a restart re-derives the work to
// do instead of trusting anything here.
type lifecycleState struct {
	inFlight bool
	after    time.Time
}

// ---------------------------------------------------------------------------
// derivation
// ---------------------------------------------------------------------------

// idleDeadline returns the next action the idle policy asks for, or zero.
// A live hold's MinAliveUntil is a floor: the policy may not act before the
// caller's stated minimum, which is the whole point of asking for one.
func idleDeadline(ws *proto.Workspace) (at int64, action string) {
	if !ws.IdlePolicy.Set() || ws.IdleSince <= 0 {
		return 0, ""
	}
	floor := int64(0)
	if ws.Lease.Live() {
		floor = ws.Lease.MinAliveUntil
	}
	if ws.IdlePolicy.SleepAfterSec > 0 {
		at, action = ws.IdleSince+ws.IdlePolicy.SleepAfterSec*1000, proto.LeaseExpirySleep
	}
	if ws.IdlePolicy.DestroyAfterSec > 0 {
		destroyAt := ws.IdleSince + ws.IdlePolicy.DestroyAfterSec*1000
		if at == 0 || destroyAt < at {
			at, action = destroyAt, proto.LeaseExpiryDestroy
		}
	}
	// A sleep the policy has already performed must not be re-derived as the
	// pending deadline; only destroy still applies to a paused workspace.
	if ws.State == proto.WSPaused && action == proto.LeaseExpirySleep {
		if ws.IdlePolicy.DestroyAfterSec > 0 {
			at, action = ws.IdleSince+ws.IdlePolicy.DestroyAfterSec*1000, proto.LeaseExpiryDestroy
		} else {
			return 0, ""
		}
	}
	if at < floor {
		at = floor
	}
	return at, action
}

// deriveDeadline computes the workspace's single pending lifecycle deadline
// from its hold and its idle policy: the earlier of the two wins. It returns
// nil when nothing is scheduled.
func deriveDeadline(ws *proto.Workspace) *proto.LifecycleDeadline {
	if ws.State == proto.WSDestroyed {
		return nil
	}
	var out *proto.LifecycleDeadline
	if ws.Lease.Live() && ws.State != proto.WSPaused {
		out = &proto.LifecycleDeadline{
			At: ws.Lease.MaxAliveUntil, Action: ws.Lease.OnExpiry, Source: proto.LifecycleSourceLease,
		}
	}
	if at, action := idleDeadline(ws); at > 0 {
		if out == nil || at < out.At {
			out = &proto.LifecycleDeadline{At: at, Action: action, Source: proto.LifecycleSourceIdle}
		}
	}
	if out != nil {
		out.TimerID = lifecycleTimerID(ws.ID)
	}
	return out
}

// timerKind names the durable timer row a deadline arms.
func timerKind(d *proto.LifecycleDeadline) string {
	switch {
	case d == nil:
		return proto.TimerKindResume
	case d.Source == proto.LifecycleSourceLease:
		return proto.TimerKindLeaseExpiry
	case d.Action == proto.LeaseExpiryDestroy:
		return proto.TimerKindIdleDestroy
	default:
		return proto.TimerKindIdleSleep
	}
}

// lifecycleBusy reports whether an expiry the control plane already claimed is
// still being carried out. Taking a hold or changing a policy in that window
// would describe a workspace that is about to stop existing in this placement.
func lifecycleBusy(ws *proto.Workspace) bool {
	d := ws.LifecycleDeadline
	return d != nil && d.Fired && !d.Failed
}

// clearFailedDeadline drops a deadline whose execution gave up, so a fresh
// hold or policy can arm a new one. The failure stays in the event log, which
// is where a degradation belongs; keeping the spent row would instead make the
// workspace permanently unschedulable.
func clearFailedDeadline(next *proto.Workspace) {
	if next.LifecycleDeadline != nil && next.LifecycleDeadline.Failed {
		next.LifecycleDeadline = nil
	}
}

// reconcileLifecycleLocked recomputes next.LifecycleDeadline from next.Lease
// and next.IdlePolicy and returns the workspace's lifecycle timer row to
// commit alongside it. The caller holds c.mu and persists both in one
// transaction; a schedule that is not committed with the row it describes is
// a split brain waiting to happen.
//
// It returns nil when there is nothing to write: no deadline now and no timer
// row to retire.
func (c *Control) reconcileLifecycleLocked(next *proto.Workspace) *proto.Timer {
	now := c.now().UnixMilli()
	// An already-fired deadline is owned by the executor. Recomputing it here
	// would re-arm a timer for work that is in flight.
	if next.LifecycleDeadline != nil && next.LifecycleDeadline.Fired {
		return nil
	}
	deadline := deriveDeadline(next)
	id := lifecycleTimerID(next.ID)
	existing := c.timers[id]
	if deadline == nil {
		next.LifecycleDeadline = nil
		if existing == nil || existing.Fired {
			return nil
		}
		retired := *existing
		retired.Fired, retired.FiredAt, retired.Superseded = true, now, true
		retired.Reason = proto.LeaseEndSuperseded
		return &retired
	}
	next.LifecycleDeadline = deadline
	timer := proto.Timer{
		ID: id, WS: next.ID, At: deadline.At, Action: deadline.Action,
		Kind: timerKind(deadline), Generation: next.Generation, CreatedAt: now,
	}
	if existing != nil {
		timer.CreatedAt = existing.CreatedAt
	}
	return &timer
}

// retireLifecycleLocked invalidates a workspace's hold and pending deadline
// without deriving a replacement. move, destroy and cancel use it: each is an
// explicit decision that the hold no longer describes reality, so re-deriving
// one from the same inputs would resurrect it.
func (c *Control) retireLifecycleLocked(next *proto.Workspace, reason string) (*proto.Timer, *proto.WorkspaceLease) {
	now := c.now().UnixMilli()
	var ended *proto.WorkspaceLease
	if next.Lease.Live() {
		lease := *next.Lease
		lease.EndedAt, lease.EndedReason = now, reason
		next.Lease = &lease
		ended = &lease
	}
	next.LifecycleDeadline = nil
	existing := c.timers[lifecycleTimerID(next.ID)]
	if existing == nil || existing.Fired {
		return nil, ended
	}
	retired := *existing
	retired.Fired, retired.FiredAt, retired.Superseded = true, now, true
	retired.Reason = reason
	return &retired, ended
}

// publishLifecycleLocked applies a reconciled timer row to the in-memory map
// after its transaction committed.
func (c *Control) publishLifecycleLocked(timer *proto.Timer) {
	if timer == nil {
		return
	}
	cp := *timer
	c.timers[timer.ID] = &cp
}

// reserveLifecycleTimer takes a durable-timer slot for a workspace that does
// not have one yet. Rewriting the existing slot costs nothing and must never
// be refused, or an activity signal would start failing at the quota.
func (c *Control) reserveLifecycleTimer(workspace string) (func(), error) {
	c.mu.Lock()
	_, exists := c.timers[lifecycleTimerID(workspace)]
	c.mu.Unlock()
	if exists {
		return func() {}, nil
	}
	return c.reserveTimer(workspace)
}

// ---------------------------------------------------------------------------
// ws.lease
// ---------------------------------------------------------------------------

func (c *Control) wsLease(ctx context.Context, principal string, req *proto.WSLeaseReq) (*proto.WorkspaceLease, error) {
	unlock := c.lockLifecycle(req.ID)
	defer unlock()
	scope := principal + "|" + req.ID + "|workspace.lease"
	unlockMutation := c.lockMutation(scope, req.IdempotencyKey)
	defer unlockMutation()
	var prior proto.WorkspaceLease
	if hit, err := c.mutationLookup(scope, req.IdempotencyKey, proto.OpWSLease, req, &prior); err != nil {
		return nil, err
	} else if hit {
		return &prior, nil
	}
	onExpiry, err := normalizeExpiry(req.OnExpiry)
	if err != nil {
		return nil, err
	}
	if req.MaxAliveSec <= 0 {
		return nil, proto.Err(proto.CodeBadRequest, "lease needs a positive max_alive_sec")
	}
	if req.MinAliveSec < 0 {
		return nil, proto.Err(proto.CodeBadRequest, "min_alive_sec must not be negative")
	}
	if req.MinAliveSec > req.MaxAliveSec {
		return nil, proto.Err(proto.CodeBadRequest, "min_alive_sec %d exceeds max_alive_sec %d", req.MinAliveSec, req.MaxAliveSec)
	}
	if req.MaxAliveSec > c.opts.MaxLeaseSec {
		return nil, proto.Err(proto.CodeBadRequest,
			"max_alive_sec %d exceeds the control-plane maximum hold of %d seconds", req.MaxAliveSec, c.opts.MaxLeaseSec)
	}
	release, err := c.reserveLifecycleTimer(req.ID)
	if err != nil {
		return nil, err
	}
	defer release()

	now := c.now().UnixMilli()
	c.mu.Lock()
	ws := c.workspaces[req.ID]
	if ws == nil {
		c.mu.Unlock()
		return nil, proto.Err(proto.CodeNotFound, "workspace %s", req.ID)
	}
	if ws.State == proto.WSDestroyed {
		c.mu.Unlock()
		return nil, proto.Err(proto.CodeConflict, "workspace destroyed")
	}
	// A hold is a statement about a running workspace. Granting one against a
	// placement that has not happened yet would bind the deadline to a
	// generation the next claim is about to replace.
	if ws.State != proto.WSClaimed {
		state := ws.State
		c.mu.Unlock()
		return nil, proto.ErrReason(proto.CodeConflict, proto.ReasonWorkspaceNotReady,
			"workspace %s is %s; a lease holds a claimed workspace", req.ID, state)
	}
	if lifecycleBusy(ws) {
		c.mu.Unlock()
		return nil, proto.ErrReason(proto.CodeConflict, proto.ReasonLifecycleDeadlineExpired,
			"workspace %s is executing a lifecycle deadline", req.ID)
	}
	held, limit := c.heldWorkspacesLocked(ws.Tenant, ws.ID)
	if held >= limit {
		event := c.wsEvent(ws, proto.EvWSHoldMaxReached, principal, "", map[string]any{
			"scope": "tenant", "used": held, "limit": limit,
		})
		c.mu.Unlock()
		metrics.LeaseQuotaRejected.Inc()
		c.appendAudit(ctx, event)
		return nil, proto.ErrReason(proto.CodeResourceExhausted, proto.ReasonQuotaExceeded,
			"tenant held-workspace limit %d reached", limit)
	}
	next := *ws
	var events []*proto.Event
	// One hold per workspace: the new lease replaces the old pointer outright,
	// so the old one leaves only its event behind.
	if superseded := ws.Lease; superseded.Live() {
		events = append(events, c.wsEvent(&next, proto.EvWSLeaseHoldExpired, principal, "", map[string]any{
			"lease": superseded.ID, "reason": proto.LeaseEndSuperseded,
		}))
	}
	lease := proto.WorkspaceLease{
		ID: ids.New("wl"), WS: ws.ID, Generation: ws.Generation,
		MaxAliveUntil: now + req.MaxAliveSec*1000, OnExpiry: onExpiry,
		Reason: req.Reason, CreatedAt: now,
	}
	if req.MinAliveSec > 0 {
		lease.MinAliveUntil = now + req.MinAliveSec*1000
	}
	next.Lease = &lease
	// Asking for a hold is activity: it is a caller saying work is starting.
	next.IdleSince, next.LastActivityAt = 0, now
	clearFailedDeadline(&next)
	timer := c.reconcileLifecycleLocked(&next)
	result := lease
	events = append(events, c.wsEvent(&next, proto.EvWSLeaseGranted, principal, "", map[string]any{
		"lease": lease.ID, "min_alive_until": lease.MinAliveUntil, "max_alive_until": lease.MaxAliveUntil,
		"on_expiry": lease.OnExpiry, "reason": lease.Reason, "gen": lease.Generation,
	}))
	if err := c.persistWorkspaceTimerAndMutation(&next, timer, scope, req.IdempotencyKey, proto.OpWSLease, req, &result, events...); err != nil {
		c.mu.Unlock()
		return nil, err
	}
	*ws = next
	c.publishLifecycleLocked(timer)
	c.mu.Unlock()
	metrics.LeaseGranted.Inc()
	return &result, nil
}

// heldWorkspacesLocked counts a tenant's live holds, excluding the workspace
// being decided so a replacement hold is never refused by its predecessor.
func (c *Control) heldWorkspacesLocked(tenant, exclude string) (int, int) {
	count := 0
	for _, ws := range c.workspaces {
		if ws.ID == exclude || ws.Tenant != tenant || ws.State == proto.WSDestroyed {
			continue
		}
		if ws.Lease.Live() {
			count++
		}
	}
	return count, c.opts.MaxHeldWorkspacesPerTenant
}

func normalizeExpiry(value string) (string, error) {
	switch value {
	case "", proto.LeaseExpirySleep:
		return proto.LeaseExpirySleep, nil
	case proto.LeaseExpiryDestroy:
		return proto.LeaseExpiryDestroy, nil
	default:
		return "", proto.Err(proto.CodeBadRequest, "on_expiry must be %q or %q", proto.LeaseExpirySleep, proto.LeaseExpiryDestroy)
	}
}

// ---------------------------------------------------------------------------
// ws.lease.renew
// ---------------------------------------------------------------------------

func (c *Control) wsLeaseRenew(ctx context.Context, principal string, req *proto.WSLeaseRenewReq) (*proto.WorkspaceLease, error) {
	_ = ctx
	unlock := c.lockLifecycle(req.ID)
	defer unlock()
	scope := principal + "|" + req.ID + "|workspace.lease.renew"
	unlockMutation := c.lockMutation(scope, req.IdempotencyKey)
	defer unlockMutation()
	var prior proto.WorkspaceLease
	if hit, err := c.mutationLookup(scope, req.IdempotencyKey, proto.OpWSLeaseRenew, req, &prior); err != nil {
		return nil, err
	} else if hit {
		return &prior, nil
	}
	if req.ExtendSec <= 0 {
		return nil, proto.Err(proto.CodeBadRequest, "renew needs a positive extend_sec")
	}
	if req.ExtendSec > c.opts.MaxLeaseSec {
		return nil, proto.Err(proto.CodeBadRequest,
			"extend_sec %d exceeds the control-plane maximum hold of %d seconds", req.ExtendSec, c.opts.MaxLeaseSec)
	}
	if req.MinAliveSec < 0 || req.MinAliveSec > req.ExtendSec {
		return nil, proto.Err(proto.CodeBadRequest, "min_alive_sec must be between 0 and extend_sec")
	}
	release, err := c.reserveLifecycleTimer(req.ID)
	if err != nil {
		return nil, err
	}
	defer release()

	now := c.now().UnixMilli()
	c.mu.Lock()
	ws := c.workspaces[req.ID]
	if ws == nil {
		c.mu.Unlock()
		return nil, proto.Err(proto.CodeNotFound, "workspace %s", req.ID)
	}
	if ws.Lease == nil || ws.Lease.ID != req.LeaseID {
		c.mu.Unlock()
		return nil, proto.Err(proto.CodeNotFound, "lease %s", req.LeaseID)
	}
	if !ws.Lease.Live() {
		reason, endedReason := proto.ReasonLifecycleDeadlineExpired, ws.Lease.EndedReason
		if endedReason == proto.LeaseEndMoved || ws.Lease.Generation != ws.Generation {
			reason = proto.ReasonGenerationMismatch
		}
		c.mu.Unlock()
		return nil, proto.ErrReason(proto.CodeConflict, reason,
			"lease %s ended: %s", req.LeaseID, endedReason)
	}
	if ws.Lease.Generation != ws.Generation {
		leaseGeneration, current := ws.Lease.Generation, ws.Generation
		c.mu.Unlock()
		return nil, proto.ErrReason(proto.CodeConflict, proto.ReasonGenerationMismatch,
			"lease %s was granted at generation %d, workspace is at %d", req.LeaseID, leaseGeneration, current)
	}
	if ws.LifecycleDeadline != nil && ws.LifecycleDeadline.Fired {
		c.mu.Unlock()
		return nil, proto.ErrReason(proto.CodeConflict, proto.ReasonLifecycleDeadlineExpired,
			"lifecycle deadline for %s already fired", req.ID)
	}
	if ws.State == proto.WSDestroyed {
		c.mu.Unlock()
		return nil, proto.Err(proto.CodeConflict, "workspace destroyed")
	}
	lease := *ws.Lease
	lease.MaxAliveUntil = now + req.ExtendSec*1000
	lease.MinAliveUntil = 0
	if req.MinAliveSec > 0 {
		lease.MinAliveUntil = now + req.MinAliveSec*1000
	}
	lease.RenewedAt = now
	lease.Renewals++
	next := *ws
	next.Lease = &lease
	next.IdleSince, next.LastActivityAt = 0, now
	timer := c.reconcileLifecycleLocked(&next)
	result := lease
	event := c.wsEvent(&next, proto.EvWSLeaseRenewed, principal, "", map[string]any{
		"lease": lease.ID, "max_alive_until": lease.MaxAliveUntil,
		"min_alive_until": lease.MinAliveUntil, "renewals": lease.Renewals,
	})
	if err := c.persistWorkspaceTimerAndMutation(&next, timer, scope, req.IdempotencyKey, proto.OpWSLeaseRenew, req, &result, event); err != nil {
		c.mu.Unlock()
		return nil, err
	}
	*ws = next
	c.publishLifecycleLocked(timer)
	c.mu.Unlock()
	metrics.LeaseRenewed.Inc()
	return &result, nil
}

// ---------------------------------------------------------------------------
// ws.lease.cancel
// ---------------------------------------------------------------------------

func (c *Control) wsLeaseCancel(ctx context.Context, principal string, req *proto.WSLeaseCancelReq) (*proto.Workspace, error) {
	_ = ctx
	unlock := c.lockLifecycle(req.ID)
	defer unlock()
	scope := principal + "|" + req.ID + "|workspace.lease.cancel"
	unlockMutation := c.lockMutation(scope, req.IdempotencyKey)
	defer unlockMutation()
	var prior proto.Workspace
	if hit, err := c.mutationLookup(scope, req.IdempotencyKey, proto.OpWSLeaseCancel, req, &prior); err != nil {
		return nil, err
	} else if hit {
		return &prior, nil
	}
	c.mu.Lock()
	ws := c.workspaces[req.ID]
	if ws == nil {
		c.mu.Unlock()
		return nil, proto.Err(proto.CodeNotFound, "workspace %s", req.ID)
	}
	if ws.Lease == nil || ws.Lease.ID != req.LeaseID {
		c.mu.Unlock()
		return nil, proto.Err(proto.CodeNotFound, "lease %s", req.LeaseID)
	}
	if !ws.Lease.Live() {
		endedReason := ws.Lease.EndedReason
		c.mu.Unlock()
		return nil, proto.ErrReason(proto.CodeConflict, proto.ReasonLifecycleDeadlineExpired,
			"lease %s already ended: %s", req.LeaseID, endedReason)
	}
	if lifecycleBusy(ws) {
		c.mu.Unlock()
		return nil, proto.ErrReason(proto.CodeConflict, proto.ReasonLifecycleDeadlineExpired,
			"workspace %s is executing a lifecycle deadline", req.ID)
	}
	next := *ws
	ended := *ws.Lease
	ended.EndedAt, ended.EndedReason = c.now().UnixMilli(), proto.LeaseEndCancelled
	next.Lease = &ended
	// Cancelling the hold hands the workspace back to its idle policy, which
	// is exactly what it would have done had the hold never existed. Clearing
	// the deadline first makes the reconcile derive that from scratch rather
	// than preserve a schedule the hold owned.
	next.LifecycleDeadline = nil
	timer := c.reconcileLifecycleLocked(&next)
	if timer != nil && timer.Superseded {
		timer.Reason = proto.LeaseEndCancelled
	}
	events := []*proto.Event{c.wsEvent(&next, proto.EvWSLeaseCancelled, principal, "", map[string]any{"lease": ended.ID})}
	result := *snapshotWorkspace(&next)
	if err := c.persistWorkspaceTimerAndMutation(&next, timer, scope, req.IdempotencyKey, proto.OpWSLeaseCancel, req, &result, events...); err != nil {
		c.mu.Unlock()
		return nil, err
	}
	*ws = next
	c.publishLifecycleLocked(timer)
	c.mu.Unlock()
	return &result, nil
}

// ---------------------------------------------------------------------------
// ws.lease.get
// ---------------------------------------------------------------------------

func (c *Control) wsLeaseGet(id string) (*proto.WSLeaseRes, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	ws := c.workspaces[id]
	if ws == nil {
		return nil, proto.Err(proto.CodeNotFound, "workspace %s", id)
	}
	if ws.Lease == nil && ws.LifecycleDeadline == nil {
		return nil, proto.Err(proto.CodeNotFound, "workspace %s has no lease or lifecycle deadline", id)
	}
	return &proto.WSLeaseRes{Lease: ws.Lease.Clone(), Deadline: ws.LifecycleDeadline.Clone()}, nil
}

// ---------------------------------------------------------------------------
// ws.idle.policy and ws.idle.mark
// ---------------------------------------------------------------------------

func (c *Control) wsIdlePolicy(ctx context.Context, principal string, req *proto.WSIdlePolicyReq) (*proto.Workspace, error) {
	_ = ctx
	unlock := c.lockLifecycle(req.ID)
	defer unlock()
	scope := principal + "|" + req.ID + "|workspace.idle.policy"
	unlockMutation := c.lockMutation(scope, req.IdempotencyKey)
	defer unlockMutation()
	var prior proto.Workspace
	if hit, err := c.mutationLookup(scope, req.IdempotencyKey, proto.OpWSIdlePolicy, req, &prior); err != nil {
		return nil, err
	} else if hit {
		return &prior, nil
	}
	if req.SleepAfterSec < 0 || req.DestroyAfterSec < 0 {
		return nil, proto.Err(proto.CodeBadRequest, "idle policy durations must not be negative")
	}
	if req.SleepAfterSec > c.opts.MaxLeaseSec || req.DestroyAfterSec > c.opts.MaxLeaseSec {
		return nil, proto.Err(proto.CodeBadRequest,
			"idle policy durations must not exceed the control-plane maximum hold of %d seconds", c.opts.MaxLeaseSec)
	}
	release, err := c.reserveLifecycleTimer(req.ID)
	if err != nil {
		return nil, err
	}
	defer release()

	c.mu.Lock()
	ws := c.workspaces[req.ID]
	if ws == nil {
		c.mu.Unlock()
		return nil, proto.Err(proto.CodeNotFound, "workspace %s", req.ID)
	}
	if ws.State == proto.WSDestroyed {
		c.mu.Unlock()
		return nil, proto.Err(proto.CodeConflict, "workspace destroyed")
	}
	if lifecycleBusy(ws) {
		c.mu.Unlock()
		return nil, proto.ErrReason(proto.CodeConflict, proto.ReasonLifecycleDeadlineExpired,
			"workspace %s is executing a lifecycle deadline", req.ID)
	}
	next := *ws
	if req.SleepAfterSec == 0 && req.DestroyAfterSec == 0 {
		next.IdlePolicy = nil
	} else {
		next.IdlePolicy = &proto.IdlePolicy{SleepAfterSec: req.SleepAfterSec, DestroyAfterSec: req.DestroyAfterSec}
	}
	clearFailedDeadline(&next)
	timer := c.reconcileLifecycleLocked(&next)
	result := *snapshotWorkspace(&next)
	event := c.wsEvent(&next, proto.EvWSIdlePolicySet, principal, "", map[string]any{
		"sleep_after_sec": req.SleepAfterSec, "destroy_after_sec": req.DestroyAfterSec,
	})
	if err := c.persistWorkspaceTimerAndMutation(&next, timer, scope, req.IdempotencyKey, proto.OpWSIdlePolicy, req, &result, event); err != nil {
		c.mu.Unlock()
		return nil, err
	}
	*ws = next
	c.publishLifecycleLocked(timer)
	c.mu.Unlock()
	return &result, nil
}

func (c *Control) wsIdleMark(ctx context.Context, principal string, req *proto.WSIdleMarkReq) (*proto.Workspace, error) {
	_ = ctx
	unlock := c.lockLifecycle(req.ID)
	defer unlock()
	scope := principal + "|" + req.ID + "|workspace.idle.mark"
	unlockMutation := c.lockMutation(scope, req.IdempotencyKey)
	defer unlockMutation()
	var prior proto.Workspace
	if hit, err := c.mutationLookup(scope, req.IdempotencyKey, proto.OpWSIdleMark, req, &prior); err != nil {
		return nil, err
	} else if hit {
		return &prior, nil
	}
	release, err := c.reserveLifecycleTimer(req.ID)
	if err != nil {
		return nil, err
	}
	defer release()

	now := c.now().UnixMilli()
	c.mu.Lock()
	ws := c.workspaces[req.ID]
	if ws == nil {
		c.mu.Unlock()
		return nil, proto.Err(proto.CodeNotFound, "workspace %s", req.ID)
	}
	if ws.State == proto.WSDestroyed {
		c.mu.Unlock()
		return nil, proto.Err(proto.CodeConflict, "workspace destroyed")
	}
	if lifecycleBusy(ws) {
		c.mu.Unlock()
		return nil, proto.ErrReason(proto.CodeConflict, proto.ReasonLifecycleDeadlineExpired,
			"workspace %s is executing a lifecycle deadline", req.ID)
	}
	next := *ws
	if req.Idle {
		if next.IdleSince == 0 {
			next.IdleSince = now
		}
	} else {
		next.IdleSince, next.LastActivityAt = 0, now
	}
	clearFailedDeadline(&next)
	timer := c.reconcileLifecycleLocked(&next)
	result := *snapshotWorkspace(&next)
	event := c.wsEvent(&next, proto.EvWSIdleMarked, principal, "", map[string]any{
		"idle": req.Idle, "reason": req.Reason, "idle_since": next.IdleSince,
	})
	if err := c.persistWorkspaceTimerAndMutation(&next, timer, scope, req.IdempotencyKey, proto.OpWSIdleMark, req, &result, event); err != nil {
		c.mu.Unlock()
		return nil, err
	}
	*ws = next
	c.publishLifecycleLocked(timer)
	c.mu.Unlock()
	return &result, nil
}

// ---------------------------------------------------------------------------
// expiry
// ---------------------------------------------------------------------------

// expireLifecycle is the durable half of the whole feature: it runs from the
// control plane's own ticker, so a deadline fires whether or not the client
// that asked for it still exists.
//
// It does three things per tick, in order: retire timers whose workspace no
// longer names them, claim due timers into a durable "fired" mark, and execute
// the claimed ones. Claiming before executing is what makes the expiry exactly
// once across a crash: a restart sees the fired mark and re-runs execution
// instead of losing the deadline or firing it twice.
func (c *Control) expireLifecycle(ctx context.Context, now int64) {
	type claim struct {
		ws       string
		deadline proto.LifecycleDeadline
	}
	var retire []*proto.Timer
	var claims []claim
	var resume []claim

	c.mu.Lock()
	for _, t := range c.timers {
		if t.Fired || !t.Lifecycle() {
			continue
		}
		ws := c.workspaces[t.WS]
		if reason, stale := lifecycleTimerStale(ws, t); stale {
			retired := *t
			retired.Fired, retired.FiredAt, retired.Superseded = true, now, true
			retired.Reason = reason
			retire = append(retire, &retired)
			continue
		}
		if t.At > now {
			continue
		}
		if !lifecycleActionable(ws, t.Action) {
			// The workspace is held but transitional (a node reconnecting, a
			// checkpoint in flight). Wait: the claim lease bounds how long
			// that can last, and expiring against an unsettled placement is
			// how a workspace ends up released twice.
			continue
		}
		claims = append(claims, claim{ws: t.WS, deadline: *ws.LifecycleDeadline})
	}
	// A deadline that was already claimed but whose execution did not finish
	// is the crash-recovery case. Its retry schedule is in memory; its
	// authority is the durable fired mark on the row.
	for _, ws := range c.workspaces {
		d := ws.LifecycleDeadline
		if d == nil || !d.Fired || d.Failed {
			continue
		}
		if !lifecycleActionable(ws, d.Action) {
			continue
		}
		state := c.lifecycleWork[ws.ID]
		if state != nil && (state.inFlight || state.after.After(time.UnixMilli(now))) {
			continue
		}
		resume = append(resume, claim{ws: ws.ID, deadline: *d})
	}
	c.mu.Unlock()

	for _, t := range retire {
		c.retireLifecycleTimer(ctx, t)
	}
	for _, cl := range claims {
		c.claimLifecycleDeadline(ctx, cl.ws, cl.deadline)
	}
	for _, cl := range resume {
		c.startLifecycleExpiry(cl.ws, cl.deadline)
	}
}

// lifecycleTimerStale reports whether a pending lifecycle timer no longer
// describes its workspace, and why.
//
// The workspace row is the fence, not the generation. ws.move, ws.destroy and
// ws.lease.cancel each clear LifecycleDeadline in their own transaction, so a
// timer the row no longer names has already been overtaken by a recorded
// decision. Comparing generations here instead would be worse than useless: a
// node loss re-places a workspace and advances its generation without any
// client asking for it, and a strict compare would then silently drop the
// deadline, leaking exactly the workspace this feature exists to reclaim. The
// generation is still carried on the timer and stamped at the moment the
// deadline is claimed, so the durable record says which placement was ended.
func lifecycleTimerStale(ws *proto.Workspace, t *proto.Timer) (string, bool) {
	switch {
	case ws == nil:
		return proto.LeaseEndDestroyed, true
	case ws.State == proto.WSDestroyed:
		return proto.LeaseEndDestroyed, true
	case ws.LifecycleDeadline == nil:
		return proto.LeaseEndSuperseded, true
	case ws.LifecycleDeadline.TimerID != t.ID:
		return proto.LeaseEndSuperseded, true
	case ws.LifecycleDeadline.At != t.At:
		return proto.LeaseEndSuperseded, true
	default:
		return "", false
	}
}

// lifecycleActionable reports whether the workspace is in a state the named
// action can be executed against right now.
func lifecycleActionable(ws *proto.Workspace, action string) bool {
	if ws == nil {
		return false
	}
	if ws.State == proto.WSClaimed {
		return true
	}
	// Destroying an already-paused workspace needs no node, so an idle
	// destroy still applies after an idle sleep has run.
	return action == proto.LeaseExpiryDestroy && ws.State == proto.WSPaused
}

// retireLifecycleTimer commits a supersession: the timer row plus the event
// that says why it will never fire, in one transaction.
func (c *Control) retireLifecycleTimer(ctx context.Context, retired *proto.Timer) {
	_ = ctx
	c.mu.Lock()
	defer c.mu.Unlock()
	live := c.timers[retired.ID]
	if live == nil || live.Fired {
		return
	}
	ws := c.workspaces[retired.WS]
	if ws == nil {
		// No workspace row to commit the event against; drop the schedule.
		delete(c.timers, retired.ID)
		if _, err := c.db.Exec(`DELETE FROM timers WHERE id=?`, retired.ID); err != nil {
			c.logger.Error("delete orphan lifecycle timer", "timer", retired.ID, "err", err)
		}
		return
	}
	next := *ws
	leaseID := ""
	if ws.Lease != nil {
		leaseID = ws.Lease.ID
	}
	event := c.wsEvent(&next, proto.EvWSLeaseHoldExpired, "", "", map[string]any{
		"lease": leaseID, "reason": retired.Reason, "timer": retired.ID,
	})
	if err := c.persistWorkspaceTimerAndMutation(&next, retired, "", "", proto.OpWSLease, nil, nil, event); err != nil {
		c.logger.Error("retire lifecycle timer", "timer", retired.ID, "ws", retired.WS, "err", err)
		return
	}
	*ws = next
	c.publishLifecycleLocked(retired)
}

// claimLifecycleDeadline durably records that this deadline is now the control
// plane's to execute, then starts execution. Nothing else may fire it after
// the mark commits.
func (c *Control) claimLifecycleDeadline(ctx context.Context, id string, deadline proto.LifecycleDeadline) {
	_ = ctx
	now := c.now().UnixMilli()
	c.mu.Lock()
	ws := c.workspaces[id]
	if ws == nil || ws.LifecycleDeadline == nil || ws.LifecycleDeadline.Fired ||
		ws.LifecycleDeadline.At != deadline.At || !lifecycleActionable(ws, deadline.Action) {
		c.mu.Unlock()
		return
	}
	timer := c.timers[lifecycleTimerID(id)]
	if timer == nil || timer.Fired {
		c.mu.Unlock()
		return
	}
	fired := *timer
	fired.Fired, fired.FiredAt = true, now
	fired.Reason = proto.LeaseEndDeadline
	// Stamp the placement actually being ended, which is not necessarily the
	// one the hold was granted against: a node loss re-places a workspace
	// without a client decision, and the audit record should say so.
	fired.Generation = ws.Generation
	claimed := *ws.LifecycleDeadline
	claimed.Fired, claimed.FiredAt = true, now
	next := *ws
	next.LifecycleDeadline = &claimed
	events := []*proto.Event{c.wsEvent(&next, proto.EvWSLifecycleExpired, "", "", map[string]any{
		"action": claimed.Action, "source": claimed.Source, "at": claimed.At, "timer": fired.ID,
	})}
	if claimed.Source == proto.LifecycleSourceLease && next.Lease.Live() {
		ended := *next.Lease
		ended.EndedAt, ended.EndedReason = now, proto.LeaseEndDeadline
		next.Lease = &ended
		events = append(events, c.wsEvent(&next, proto.EvWSLeaseHoldExpired, "", "", map[string]any{
			"lease": ended.ID, "reason": proto.LeaseEndDeadline,
		}))
	}
	if err := c.persistWorkspaceTimerAndMutation(&next, &fired, "", "", proto.OpWSLease, nil, nil, events...); err != nil {
		c.logger.Error("claim lifecycle deadline", "ws", id, "err", err)
		c.mu.Unlock()
		return
	}
	*ws = next
	c.publishLifecycleLocked(&fired)
	c.mu.Unlock()
	if claimed.Source == proto.LifecycleSourceLease {
		metrics.LeaseHoldExpired.Inc()
	} else if claimed.Action == proto.LeaseExpirySleep {
		metrics.IdleSleeps.Inc()
	}
	c.startLifecycleExpiry(id, claimed)
}

// startLifecycleExpiry runs one execution attempt on its own goroutine. The
// request that armed the deadline is long gone, so the work uses the control
// plane's own lifetime context rather than any caller's.
func (c *Control) startLifecycleExpiry(id string, deadline proto.LifecycleDeadline) {
	c.mu.Lock()
	state := c.lifecycleWork[id]
	if state == nil {
		state = &lifecycleState{}
		c.lifecycleWork[id] = state
	}
	if state.inFlight {
		c.mu.Unlock()
		return
	}
	state.inFlight = true
	c.mu.Unlock()
	c.wg.Add(1)
	go func() {
		defer c.wg.Done()
		ctx, cancel := context.WithTimeout(c.requestCtx, lifecycleExpiryTimeout)
		defer cancel()
		err := c.executeLifecycleExpiry(ctx, id, deadline)
		c.finishLifecycleExpiry(id, deadline, err)
	}()
}

// executeLifecycleExpiry performs the action the deadline named. Sleep reuses
// the ordinary release-and-pause path, so the same checkpoint, fencing and
// source-cleanup guarantees apply; there is no separate expiry-only shortcut
// that could skip one of them.
func (c *Control) executeLifecycleExpiry(ctx context.Context, id string, deadline proto.LifecycleDeadline) error {
	switch deadline.Action {
	case proto.LeaseExpiryDestroy:
		return c.wsDestroy(ctx, "", id, "")
	default:
		return c.lifecycleSleep(ctx, id)
	}
}

// finishLifecycleExpiry clears the pending deadline on success, or schedules a
// retry and eventually degrades the workspace. An expiry that quietly stops
// trying is a leaked machine nobody is billed for noticing.
func (c *Control) finishLifecycleExpiry(id string, deadline proto.LifecycleDeadline, cause error) {
	now := c.now()
	c.mu.Lock()
	state := c.lifecycleWork[id]
	if state != nil {
		state.inFlight = false
	}
	ws := c.workspaces[id]
	if ws == nil {
		delete(c.lifecycleWork, id)
		c.mu.Unlock()
		return
	}
	next := *ws
	var events []*proto.Event
	switch {
	case cause == nil:
		// The action happened. Re-derive: an idle sleep may still owe a
		// destroy, and the hold is over either way.
		next.LifecycleDeadline = nil
		if next.Lease.Live() {
			ended := *next.Lease
			ended.EndedAt, ended.EndedReason = now.UnixMilli(), proto.LeaseEndDeadline
			next.Lease = &ended
		}
		delete(c.lifecycleWork, id)
	case ws.LifecycleDeadline == nil:
		// A destroy or a cancel committed while this attempt was running.
		// There is no longer a deadline to carry out or to fail.
		delete(c.lifecycleWork, id)
	default:
		attempted := *ws.LifecycleDeadline
		attempted.Attempts++
		attempted.Error = cause.Error()
		if attempted.Attempts >= lifecycleExpiryAttempts {
			attempted.Failed = true
			events = append(events, c.wsEvent(&next, proto.EvWSLifecycleExpiryFailed, "", "", map[string]any{
				"action": attempted.Action, "source": attempted.Source,
				"attempts": attempted.Attempts, "error": attempted.Error,
			}))
			delete(c.lifecycleWork, id)
		} else if state != nil {
			state.after = now.Add(lifecycleExpiryBackoff)
		}
		next.LifecycleDeadline = &attempted
	}
	timer := c.reconcileLifecycleLocked(&next)
	if err := c.persistWorkspaceTimerAndMutation(&next, timer, "", "", proto.OpWSLease, nil, nil, events...); err != nil {
		c.logger.Error("record lifecycle expiry outcome", "ws", id, "err", err)
		c.mu.Unlock()
		return
	}
	*ws = next
	c.publishLifecycleLocked(timer)
	failed := next.LifecycleDeadline != nil && next.LifecycleDeadline.Failed
	c.mu.Unlock()
	if cause != nil {
		c.logger.Warn("lifecycle expiry attempt failed", "ws", id, "action", deadline.Action, "err", cause)
	}
	if failed {
		metrics.LifecycleExpiryFailed.Inc()
	}
}

// lifecycleSleep releases the workspace with a checkpoint and publishes it as
// paused. It mirrors wsSleep without arming a wake timer: a hold that expired
// says only that nobody extended it, never when the work should resume.
func (c *Control) lifecycleSleep(ctx context.Context, id string) error {
	unlock := c.lockLifecycle(id)
	defer unlock()
	current, err := c.wsGet(id)
	if err != nil {
		return err
	}
	if current.State == proto.WSDestroyed {
		return proto.Err(proto.CodeConflict, "workspace destroyed")
	}
	if current.State == proto.WSPaused {
		return nil
	}
	snap, err := c.release(ctx, id, true, proto.ReasonLifecycleDeadlineExpired)
	if err != nil {
		return err
	}
	c.mu.Lock()
	ws := c.workspaces[id]
	if ws == nil || ws.State == proto.WSDestroyed {
		c.mu.Unlock()
		return proto.Err(proto.CodeConflict, "workspace destroyed during lifecycle expiry")
	}
	if ws.State == proto.WSPaused {
		c.mu.Unlock()
		return nil
	}
	if ws.Generation != current.Generation {
		c.mu.Unlock()
		return proto.Err(proto.CodeConflict, "workspace generation changed during lifecycle expiry")
	}
	next, transitionErr := transitionWorkspace(ws, lifecycleTransition{
		operation: transitionSleep, actor: actorControl, to: proto.WSPaused,
		expectGeneration: true, generation: current.Generation,
	})
	if transitionErr != nil {
		c.mu.Unlock()
		return transitionErr
	}
	next.Spec.RestoreFrom = snap
	next.Spec.RestoreFormat = ws.LastSnapshotFormat
	next.Spec.RestoreObjects = append([]string(nil), ws.LastSnapshotObjects...)
	next.Node = ""
	next.LeaseUntil = 0
	// The spent deadline, the ended hold and the paused state commit together.
	// Publishing paused first would leave a window in which a reader sees a
	// sleeping workspace that still claims to be scheduled, and a crash inside
	// that window would leave a deadline nothing re-derives. The reconcile
	// also arms whatever the idle policy still owes — an idle sleep may owe a
	// later destroy — so the follow-on schedule survives the same crash.
	next.LifecycleDeadline = nil
	if next.Lease.Live() {
		ended := *next.Lease
		ended.EndedAt, ended.EndedReason = c.now().UnixMilli(), proto.LeaseEndDeadline
		next.Lease = &ended
	}
	timer := c.reconcileLifecycleLocked(&next)
	event := c.wsEvent(&next, proto.EvWSPaused, "", "", map[string]any{
		"snapshot": snap, "reason": proto.ReasonLifecycleDeadlineExpired,
	})
	if err := c.persistWorkspaceTimerAndMutation(&next, timer, "", "", proto.OpWSLease, nil, nil, event); err != nil {
		c.mu.Unlock()
		return err
	}
	*ws = next
	c.publishLifecycleLocked(timer)
	c.mu.Unlock()
	return nil
}

// publishLifecycleGauges refreshes the held and paused gauges. Called from
// Tick with c.mu not held.
func (c *Control) publishLifecycleGauges() {
	var heldCount, pausedCount int64
	c.mu.Lock()
	for _, ws := range c.workspaces {
		if ws.State == proto.WSPaused {
			pausedCount++
		}
		if ws.State == proto.WSClaimed && ws.Lease.Live() {
			heldCount++
		}
	}
	c.mu.Unlock()
	metrics.WSHeld.Set(heldCount)
	metrics.WSPaused.Set(pausedCount)
}
