# ADR 0090: Durable workspace leases and idle policy

## Status

Accepted.

## Context

Remount already had `ws.sleep`, but its semantics are "sleep now, and here is
when to wake". Nothing expressed the opposite: "keep this workspace claimed
until a deadline because work is still running, then pause it if nobody
extends the deadline."

Every adopter builds that missing primitive the same wrong way — an
`asyncio.sleep` or `setTimeout` in the API worker that took the request. That
reduces leaks in a demo and guarantees nothing in production. A deploy, a
scale-down, an OOM kill, or a network partition takes the timer with it, and
the workspace runs until somebody notices the bill. The deadline has to live
where the workspace lives.

The built-in Agent resource already had an idle policy
(`AgentPolicy.SleepAfterSec` with `Agent.IdleSince`, reconciled from durable
fields in the control loop). Generic workspace clients had no equivalent.
`WorkspaceSpec.Idle{SnapshotEverySec, DestroyAfterSec}` existed in the wire
format and was read by no code at all.

## Decision

Six control-plane operations, and no new workspace state.

`ws.lease` takes a durable hold on a `claimed` workspace:
`min_alive_sec` is how long the idle policy is forbidden from acting,
`max_alive_sec` is the hard deadline, and `on_expiry` is `sleep` or `destroy`.
`ws.lease.renew` extends it, `ws.lease.cancel` removes it, `ws.lease.get`
reads it. `ws.idle.policy` installs the no-work rule and `ws.idle.mark` drives
its clock.

### No new states

"Idle but held" and "scheduled to sleep" are derived views over `claimed`, not
lifecycle states. Adding states would have meant new rows in the normative
transition table, new recovery cases in `load()`, and new ways for a
half-finished transition to strand a workspace — for a distinction that is
entirely expressible as data on the row.

`Workspace.lifecycle_deadline` publishes that view: what happens next, when,
and from which source. A client reads its position from `ws.get` and never has
to know that `timer.list` exists.

### The expiry executes through the ordinary paths

Sleep-on-expiry calls the same `release(..., reason)` and released→paused
transition that `ws.sleep` uses; destroy-on-expiry calls `ws.destroy`. There is
no expiry-only shortcut, so an automatic expiry cannot skip the checkpoint,
the release epoch, the source fencing, or the volume detach that an explicit
call performs. Whatever those paths guarantee, expiry guarantees.

### Exactly once across a crash: claim, then execute

`expireLifecycle` runs from the control plane's own ticker. A due deadline is
first *claimed* — the timer row is marked fired and `ws.lifecycle.expired`
commits in the same transaction as the workspace row — and only then executed.

That order is what makes the expiry exactly once. Executing first would fire
twice after a crash mid-release. Clearing the deadline first would lose it
entirely. With the durable fired mark, a restarted control plane finds a
deadline that is fired, not failed, and not yet carried out, and resumes
execution from the row rather than from anything in memory.

### The workspace row is the fence, not the generation

The obvious design binds the timer to the generation it was armed against and
refuses to fire against any other. We do not do that, and the reason is worth
recording because the obvious design leaks.

`ws.move`, `ws.destroy` and `ws.lease.cancel` each clear
`lifecycle_deadline` in their own transaction. A timer the row no longer names
has therefore already been overtaken by a recorded decision, and is retired
with `ws.lease.expired{reason}` instead of acting. That is the fence, and it is
synchronous with the decision that motivates it.

A generation compare would fence more than that. A lost node, an expired claim
lease, or an ordinary failover all advance the generation without any client
asking for anything. Under a strict compare the deadline would be silently
dropped at exactly that moment, the workspace would be re-claimed somewhere
else, and it would then run forever with nothing scheduled — the precise leak
this feature exists to close, reintroduced by the mechanism meant to prevent a
different one.

The generation is still carried. It is stamped on the timer when the deadline
is claimed, so the durable record names the placement that was actually ended,
and it fences `ws.lease.renew`: a hold granted at another generation is
refused with `generation_mismatch`, telling the caller to take a fresh hold
rather than assume it still holds the thing it asked for.

### One durable timer row per workspace

The lifecycle timer id is derived from the workspace id (`t_lc_<ws>`) and the
row is rewritten in place.

`ws.idle.mark` is expected on every agent turn. Allocating a fresh timer id per
call would exhaust `MaxTimersPerWorkspace` (default 128) within a single busy
session and leave a spent row behind for retention to collect each time. One
reconciled row per workspace makes the activity signal free, and it also gives
the firing decision a single object to be authoritative about.

The cost is that a workspace has one pending lifecycle deadline rather than one
per kind. `deriveDeadline` therefore computes the earlier of the hold's hard
deadline and the idle policy's next action, and re-derives after every
mutation. This is why `min_alive_until` is a floor rather than a separate
timer.

### Activity is explicit

`ws.idle.mark{idle:false}`, `ws.lease` and `ws.lease.renew` are the activity
signals. Session traffic is not, for two reasons. Treating a session open as
activity would make every `s.open` a durable control-plane write on a hot path.
And only the caller can distinguish a settled turn from a pause: a workspace
with an idle shell attached is not busy, and one waiting on a webhook with no
sessions at all may be.

### Sessions ended by an expiry say so

A session killed by a lifecycle expiry ends with
`ExitInfo.Reason = "lifecycle_deadline_expired"`, not the generic
`"workspace released"`. A client replaying the log after its `sleep 300` was
cut short can tell a policy decision from a crash without correlating
timestamps against another stream.

The node also gets a bounded grace: `Manager.TerminateWorkspaceGraceful` sends
TERM, waits `Options.LifecycleGrace` (default five seconds), then KILL, and
joins every session before the workspace can be reported quiesced. Cancellation
is not completion; a checkpoint taken while a producer is still writing is not
the tree anybody asked for. An ordinary release still kills immediately — a
lifecycle expiry is the only release that is both expected and scheduled, so
it is the only one worth waiting for. On Windows a job object has no distinct
polite signal, so the grace degrades to immediate termination there; the
recorded reason is the same.

### Failure is loud

If the release underneath an expiry fails, it is retried with backoff up to
five attempts. After that the deadline is marked failed on the durable row and
`ws.lifecycle.expiry_failed` is emitted with the attempt count and the error.
The workspace is then degraded and operator-actionable. A deadline that quietly
stops trying is worse than one that says it gave up, because the failure mode
it produces is indistinguishable from success.

### Quotas

`Options.MaxLeaseSec` (default 24 hours) bounds one hold, renewals included,
and every idle-policy duration. `Options.MaxHeldWorkspacesPerTenant` (default
256) bounds how many workspaces a tenant may pin awake at once; the refusal is
`resource_exhausted` with reason `quota_exceeded` and an accompanying
`ws.hold.max_reached` audit event, so a rejection is never only a counter.

`MaxLeaseSec` sits beside the existing `LeaseSec`, which is the node's claim
renewal. The names are close and the concepts are not related; both carry doc
comments saying so. Likewise `ws.lease_expired` (claim lease) and
`ws.lease.expired` (client hold) are different events.

## What raw workspaces get, versus the Agent resource

The Agent resource keeps its own policy. `sleepAgent` cancels the harness run
before sleeping so the transcript flushes, and an agent wakes on an inbox
message. Neither is expressible for a raw workspace, which has no harness and
no inbox.

What a raw workspace now gets is the durable half: a control-plane deadline, an
idle clock, generation-aware refusals, and an expiry that survives every
process involved in asking for it. An adopter building its own agent loop no
longer has to adopt Remount's Agent abstraction to get lifecycle safety.

## Non-guarantees

- **No memory checkpoint.** An expiry takes a filesystem snapshot and ends the
  processes. Firecracker full-VM checkpointing remains a separate path with its
  own compatibility rules (ADR 0063); a hold does not opt into it.
- **No wake.** A hold that expires says only that nobody extended it. It never
  implies when the work should resume, so no wake timer is armed. Use
  `ws.sleep` with `after_sec` or `on_event` for that.
- **No inference of activity.** A workspace with running sessions and no
  explicit signal will sleep at its deadline. That is the contract, not a bug:
  a deadline nobody has to renew is a deadline that never fires.
- **No cross-node hold.** The hold names a workspace, and a moved workspace
  needs a fresh one.
- **Older nodes degrade quietly.** A node that predates this change releases
  correctly but reports the ordinary release reason in the exit chunk and
  performs no graceful stop. The property lost is explanatory, not safety.

## Consequences

The control plane owns one more durable schedule and one more background
reconciliation. `Workspace` grows four additive fields, three of them pointers,
which are replaced wholesale and never mutated through, and are deep-copied at
every path that hands a workspace out from under `control.mu`.

Adopters that were relying on a client-side timer can delete it. Adopters that
were not can now bound their compute spend without writing one.
