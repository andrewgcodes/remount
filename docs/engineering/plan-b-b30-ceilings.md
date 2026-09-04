# Plan B B30: declared resource ceilings and their measured evidence

Scenario **B30** of
[the Plan B repository-executable plan](./plan-b-repository-executable-2026-09-03.md)
(§15.4 item 4, §15.5, evidence layer `code`/`sim`) asks for bounded scale
scenarios with deterministic workloads and then states the acceptance
property in one sentence:

> Enforce memory, goroutine, descriptor, artifact, log, event and retained
> state ceilings. Every dropped or rejected unit needs a metric and explicit
> result.

This document records what was measured, at what width, which ceilings held,
and which did not. It is a point-in-time measurement of a candidate, not a
standing production claim; the layer is `code`, and §6.2's outcomes apply —
there is no `skipped-success`.

**Current status: eight of the eleven tests pass and three fail.** The three
failures are the findings in "Ceilings that did not hold" below, and each one
requires a change outside `internal/sim` to go green. They are left failing
deliberately: a scale suite that is green while a ceiling is missing is worth
nothing, and a rejection nobody counted is indistinguishable from a silent
drop. B30 is therefore `failed`, not `passed`, until those three land.

## The tests

All of them live in `internal/sim` and share the measurement helpers in
`planb_scale_support_test.go`.

| File | Test | §15.4 item |
|---|---|---|
| `planb_scale_support_test.go` | (helpers only) | measurement discipline |
| `planb_scale_cursors_test.go` | `TestPlanBScaleReconnectingCursorsReleaseTheirState` | many simultaneous reconnecting cursors |
| `planb_scale_pool_test.go` | `TestPlanBScalePoolBurstAndDrainCyclesStayBounded` | provider pool bursts and drains |
| `planb_scale_pool_test.go` | `TestPlanBScalePoolWideBurstDrainsEveryIdleMachine` | provider pool bursts and drains |
| `planb_scale_spill_test.go` | `TestPlanBScaleSessionLogSpillCeilingIsCountedAndExplicit` | session-log spill and artifact tiering |
| `planb_scale_spill_test.go` | `TestPlanBScaleSnapshotDeduplicationAndArtifactCeiling` | snapshot chunk deduplication |
| `planb_scale_backpressure_test.go` | `TestPlanBScaleEventLogRetentionCeilingIsCountedAndExplicit` | event and export backpressure |
| `planb_scale_backpressure_test.go` | `TestPlanBScaleSlowEventSubscriberLossIsCountedAndExplicit` | event and export backpressure |
| `planb_scale_backpressure_test.go` | `TestPlanBScaleEventTailSubscriptionsAreBounded` | event and export backpressure |
| `planb_scale_quota_test.go` | `TestPlanBScaleWorkspaceQuotaRejectionIsCountedAndRecovers` | quota rejection and recovery |
| `planb_scale_quota_test.go` | `TestPlanBScaleSessionQuotaRejectionIsCountedAndRecovers` | quota rejection and recovery |

```sh
go test -count=1 -timeout 40m ./internal/sim/ -run PlanBScale
go test -race -count=1 -timeout 60m ./internal/sim/ -run PlanBScale
REMOUNT_SCALE_N=800 go test -count=1 ./internal/sim/ -run PlanBScaleReconnectingCursors
```

`REMOUNT_SCALE_N` overrides every workload width; `REMOUNT_SCALE_SEED`
replays the one seeded choice (which cursors get partitioned). `-short`
skips the heavy tests and narrows the rest.

## Method

Three rules make these measurements mean something.

**A leak is a slope, not a level.** Every test drives the same workload to
steady state at least twice and compares a later cycle against an earlier one.
Cycle one pays one-off costs — arenas, SQLite pages, grants, a pool warming up
— and charging those to a leak would be wrong. Only repeated growth counts.

**Teardown is asynchronous, so measurement polls to a floor.** A cut
connection wakes a reader that closes a peer that cancels a subscriber.
`planbScaleFloor` forces two GCs and re-reads until the goroutine count stops
falling. Reading `runtime.NumGoroutine()` once after a cycle measures the
scheduler, not the program.

**Counters are deltas.** `metrics.Default` is process-global and shared by
every test in the binary, so nothing here asserts an absolute counter value
and none of these tests may run in parallel with each other.

Descriptors are counted from `/proc/self/fd` or `/dev/fd`; on a platform with
neither, the count is `-1` and the descriptor assertions are skipped rather
than passed.

Nothing in the workloads is random except which three-quarters of the cursors
get partitioned in each cycle, which is seeded and logged.

## Ceilings that held

### Reconnecting cursors — `code`, passed

400 simultaneous cursors on one live session log, three cycles of
connect / attach / partition three quarters / reattach / detach.

| | baseline | cycle 1 | cycle 2 | cycle 3 |
|---|---|---|---|---|
| goroutines at peak | 29 | 2,429 | 2,429 | 2,429 |
| goroutines settled | 29 | 29 | 29 | 29 |
| heap in use settled | 4.7 MiB | 8.0 MiB | 8.6 MiB | 8.6 MiB |
| descriptors | 15 | 15 | 15 | 15 |
| relay `remount_peers_connected` | 2 | 2 | 2 | 2 |

At 800 cursors (`REMOUNT_SCALE_N=800`): 4,829 goroutines and 52.5 MiB heap at
peak, settling to 29 goroutines and 11.7 / 11.8 / 12.1 MiB across three
cycles. **Six goroutines and about 65 KiB per simultaneous cursor**, released
completely on teardown at both widths.

Why 400 and not the 10,000 the plan names: in this simulator a cursor is a
real client with its own pipe, transport peer, relay `Serve` goroutine,
control-plane registration and a node-side subscriber, so 10,000 is roughly
60,000 live goroutines and, extrapolating the measured per-cursor cost,
about 650 MiB — a machine-sized run rather than a laptop-sized one. The test
proves a *slope* that is independent of width, and 800 confirms the slope is
flat, so the extrapolation to 10,000 is arithmetic rather than hope. See the
open question below about what happens above roughly 1,500 in this harness.

### Provider pool burst and drain, single pool — `code`, passed

Four cycles of create workspace → claim on a provisioned machine → destroy →
idle scale-down to zero inventory.

- drain time: 878 ms, 873 ms, 871 ms, 823 ms
- goroutines settled: 14 every cycle; heap in use 4.4 / 4.4 / 4.5 / 4.5 MiB;
  descriptors 9 every cycle
- `remount_pool_scale_actions_total` +2 per cycle (one create, one destroy),
  `remount_pool_provision_failures_total` +0, `remount_pool_machines` back to 0

### Session-log spill and tiering — `code`, passed

Four sessions, each producing about 316 KB, under a 16 KiB memory and 128 KiB
spill ceiling with no blob tier configured.

- `remount_session_bytes_total` +1,249,064 (deterministic);
  `remount_session_chunks_evicted_total` +680 without the race detector and
  +465 with it, since chunk boundaries follow write timing
- largest spill file 82.8 KiB (96.0 KiB under `-race`) against a 128 KiB
  per-session ceiling; 282.5 KiB (352.1 KiB under `-race`) total across four
  retained logs against a 512 KiB worst case. The ceiling binds in both.
- descriptors 12 → 16: exactly one spill descriptor per retained session
- replaying an evicted range delivered a `gap` chunk and moved
  `remount_session_gaps_total`; the surviving suffix was never presented as
  complete output

### Snapshot chunk deduplication — `code`, passed

Four snapshots of 20 MiB of unchanged, highly repetitive content:
80.0 MiB logical considered, **1.3 MiB uploaded**,
`remount_snapshot_dedupe_ratio` 0.984.

### Event-log retention ceiling — `code`, passed

1,200 events posted under a 200-event ceiling: `remount_events_pruned_total`
moved by 838, 868 and 995 across three runs — the exact figure depends on when
the retention pass lands relative to the last post — and a read from sequence 1
returned `evicted` naming the oldest surviving sequence rather than a short
answer that reads as complete. Reading from the reported oldest sequence
succeeded. What the test asserts is the invariant, not the sample: something
was pruned, it was counted, and the reader was told.

### Quota rejection and recovery — `code`, passed

- 24 workspace creates above a 4-workspace tenant/subject limit: 24 refusals,
  all `resource_exhausted`, `remount_workspace_quota_rejections_total` +24 and
  24 `quota.exceeded` events naming the resource, scope, use and limit.
  Repeating the whole burst cost no additional goroutines and no heap growth.
  Destroying one workspace admitted exactly one more, and the ceiling closed
  again behind it.
- 24 session opens above a 3-session active limit:
  `remount_session_quota_rejections_total` +24, all `resource_exhausted`, the
  node still retained exactly the three admitted sessions, and an exit made
  the slot immediately reusable.

## Ceilings that did not hold

### 1. Event-tail subscriptions are unbounded retained state

`Control.tails` (`internal/control/control.go:329`, populated in
`eventsTail`, `control.go:5512`) has no admission limit of any kind — not per
requester, not per connection, not fleet-wide. One authenticated peer, on one
connection, opens as many follow subscriptions as it likes simply by naming a
new `subscription` id each time.

Measured with `TestPlanBScaleEventTailSubscriptionsAreBounded`, one raw client
peer, control-plane cost only:

| tails | refusals | goroutines above baseline | heap in use |
|---|---|---|---|
| 2,000 | **0** | +2,000 | 150.1 MiB |

That is **one control-plane goroutine and about 75 KiB per subscription**,
growing linearly with no ceiling, no rejection code and no metric. Ten
thousand subscriptions is 10,000 goroutines and roughly 750 MiB; the cost is
paid by the control plane, not the caller. Each subscription also takes a slot
in `Log.fanOutLocked`, which iterates every subscriber for every appended
event while holding the log mutex, so append cost grows linearly in the same
number.

This is the same shape as every bound listed in hardening lesson 6 ("bounded
means every stage and every index") and it is the one retained collection that
has none of admission, accounting, an exhaustion error code or a metric.

### 2. A slow event subscriber loses events with no metric and no distinguishable result

`eventSubscription.enqueue` (`internal/client/client.go:815`) drops into its
`default:` branch when the subscriber's 512-event inbound channel is full,
and responds by closing the subscription. The consumer of `TailEvents` sees
its channel close — exactly what it would see after cancelling the tail, or
after a clean shutdown.

Measured with `TestPlanBScaleSlowEventSubscriberLossIsCountedAndExplicit`: a
consumer that stops reading while 4,000 events are posted received **256 of
4,000**, the stream closed, and **no counter in `internal/metrics` moved**.

Both halves of §15.5 fail here. There is no metric, so 3,744 lost events are
invisible to an operator; and there is no explicit result, so the consumer
cannot tell loss from a clean end. The comment at the drop site says event
delivery is replayable, which is true of the log, but nothing tells the
consumer that it now needs to replay or from where.

Contrast the session path, which gets this right: `Session.enqueue`
(`client.go:1514`) fails the session with a typed
`resource_exhausted: session delivery queue is full` instead of closing
silently.

### 3. Idle provider inventory is not reliably destroyed once more than one pool exists

`TestPlanBScalePoolWideBurstDrainsEveryIdleMachine`: six pools, one machine
each, every workspace destroyed so every machine is idle and past its 50 ms
idle threshold, with the control plane reconciling every pool once a second.

| elapsed | machines still provisioned |
|---|---|
| 10 s | 4 of 6 |
| 20 s | 4 of 6 |
| 30 s | 4 of 6 |
| 40 s | 3 of 6 |
| 50 s | 2 of 6 |
| 60 s | 2 of 6 — budget exhausted |

An earlier eight-pool run left three machines running after **three and a half
minutes**, with the control plane reporting every one of them online,
assignment-free and correctly labelled. The single-pool version of the same
cycle drains in about 870 ms, every time.

The mechanism is visible in `Control.enrichPoolInventory`
(`internal/control/pool_reconcile.go:150`). `c.poolIdle` is one control-wide
map keyed by machine id (`control.go:342`), but the prune at the end of that
function deletes every entry whose machine is not in `visible`, and `visible`
holds only the machines of the pool currently being reconciled:

```go
for machine := range c.poolIdle {
    if _, ok := visible[machine]; !ok {
        delete(c.poolIdle, machine)
    }
}
```

So reconciling pool A erases the idle clock of every machine in pools B…Z. A
machine can only be destroyed if it survives from "marked idle" to "scale down"
without any other pool reconciling in between — and since `reconcilePoolsAsync`
starts every pool concurrently on the same tick, the wider the fleet the less
likely that is. Idle provider inventory is the one retained resource here that
costs money for as long as it survives, and above one pool nothing reliably
releases it.

## Open questions, not claims

- **Cursor width above roughly 1,500 in this harness.** At
  `REMOUNT_SCALE_N=1500` the mass partition is followed by the node logging
  `uplink lost; reconnecting err="transport: connection closed"`, and the run
  did not finish. `Relay.route` calls `dst.Send` synchronously from the source
  peer's read loop, and `Peer.Send` fails the connection after a 30 s write
  timeout, so one destination that is not draining applies backpressure to
  everything its source is sending. Whether that is the cause, and whether it
  reproduces over a real WebSocket rather than the simulator's 256-frame
  in-memory pipe, was not established. Treat 800 as the widest validated
  cursor count.
- **Uncounted drop paths not driven here.** A survey of the code found
  further drop sites with no counter that these tests do not reach: the
  second-tier control and node request overload paths
  (`control.go:1874`, `node.go` `scheduleRequest`, where the node closes the
  peer outright), the broker's concurrent-request and pending-approval slot
  exhaustion, `Relay.Request` refusing above `maxPendingRequests`,
  `eventlog.Memory.Append` trimming a node's in-memory log past 10,000 entries
  without touching `remount_events_pruned_total`, and
  `control.dropInboxLocked` clearing an agent inbox in bulk. Each is a
  candidate for the same §15.5 treatment; none is claimed as tested.
- **Export backpressure** is covered here only through the event tail. The
  `eventlog.RunExport` cursor path was not driven at scale.

## What a fix would need to prove

For finding 1: an admission limit with its own option, a
`resource_exhausted` refusal naming the limit, a rejection counter, and a
test that the ceiling closes and reopens — the same shape the workspace and
session quotas already pass here.

For finding 2: a counter for lost events and a result the consumer can act
on. The session path's typed failure is the precedent.

For finding 3: an idle clock scoped to the pool being reconciled, and a test
at a width greater than one pool. The single-pool test passes today and would
have kept passing through this defect, which is the point of running the wide
one.
