# Plan B B3: continuity and authority failure matrix

Scope: scenarios **B14–B18** of
[Plan B §11](./plan-b-repository-executable-2026-09-03.md#11-phase-b3-continuity-and-authority-failure-matrix).
Layer: `sim`. Backend: `process`. No environment variable, credential, daemon
or network access is required, and no Docker.

B19 (kill a live E2B worker) is the optional `live-service` row and is not
covered here.

## What this phase is actually about

Every one of these scenarios is a question about **truth after an interruption**,
not about whether the system keeps working. The exit criterion in §11 is the
whole point:

> Unknown outcomes remain unknown rather than being retried as success.

So each test below has two halves. The first injects a real failure through a
real peer. The second asserts what the control plane *still knows* — naming the
authoritative row, the generation, the checkpoint digest, the session cursor,
the event, and what was cleaned up (§11.3). A test that lets an ambiguous
result pass as recovery is worse than no test, so several of the assertions
below are deliberately about things that must **not** have happened.

## How the failures are injected

§11.1 is a hard constraint: express every failure through the public client or
protocol path, never a private state mutation. Reaching into `Control` to force
a state proves the state exists, not that the system reaches it. This lane uses
only:

| Mechanism | What it models |
|---|---|
| `world.cut(peer)` | the peer's connections are severed; it may redial |
| `world.stopNode(name)` | the node process dies and never returns |
| `world.peerHooks[name]` | one frame the node sends is dropped on the wire |
| a raw `transport.Peer` speaking `client` role | a client that does not refresh its grants |
| a raw `transport.Peer` speaking `node` role | a node sending claim/ready/renew for an assignment it does not hold |
| `server.New` over the same `DataDir` | an operator restart of the control plane |

The raw peers are not mocks. They complete the real hello (a node peer signs a
real `HelloProofBytes` challenge) and speak the real protocol; they exist
because the SDK is deliberately forgiving — `Client.nodeCall` refreshes a stale
grant and retries once, which is right for a user and would silently convert
every fencing refusal in this lane into a success.

## The scenarios

### B14 — cut the client mid-command; reattach is byte-identical or has an explicit gap

`internal/sim/planb_continuity_session_test.go`,
`TestPlanBContinuityClientCutReattachIsLosslessOrExplicitlyGapped`.

Two sub-tests, one per branch of the disjunction.

`seeded-cuts-stay-byte-identical` replays four deterministic cut schedules
(§11.4). Each seed picks one to three byte offsets in the first three quarters
of the stream and severs the client's connection there. The invariants are that
the reassembled output equals the produced output exactly, the sequence never
goes backwards, the final cursor follows the last delivered sequence, and no
gap was needed. Every failure message names the seed and the schedule, which is
the whole reproduction recipe: rerun with that seed. Each intended cut must
sever at least one live connection, so a schedule that quietly injected nothing
fails rather than passing.

`stale-cursor-is-an-explicit-gap` is the other branch. The node runs with no
artifact endpoint, which disables the tiered session log, so a full spill file
is dropped rather than sealed into a blob — the shape in which replay from an
old cursor is genuinely impossible. The client reads a prefix, records its
cursor, is cut, and stops following; the process then overruns the bounded log.
The test first proves the precondition (`SessionStatus.Oldest` really moved past
the cursor — a log that never rotated cannot demonstrate anything about
rotation) and then asserts the reattach is an explicit, bounded gap: the gap
chunk carries the requested cursor as its sequence, `Gap.From` is that cursor
and `Gap.To` is `Oldest-1`, the bytes that follow are a proper suffix of the
produced output and shorter than the whole of it, and `client.Copy` renders the
elision marker instead of concatenating around it.

### B15 — lose the node before the checkpoint commit; the source stays retained and fenced

`internal/sim/planb_continuity_checkpoint_test.go`,
`TestPlanBContinuityNodeLostBeforeCheckpointCommitRetainsAndFencesSource`.

A move is the destructive lifecycle operation: prepare, checkpoint, commit,
destroy source. A frame hook on the node's side of the pipe drops the node's
`ws.release` response — a response carries the op it answers, so recognizing it
needs no correlation table — and the node is then stopped. The control plane
has quiesced the workspace and the node has archived and uploaded the
checkpoint, but no result ever arrives, which is indistinguishable from a node
that died holding a finished checkpoint.

Asserted:

- the move returns an error; success is never reported for an uncommitted digest;
- the run really reached the commit boundary — a prepared response was dropped, and the artifact store holds an uploaded checkpoint;
- the row settles at `failed` on the same node at the same generation with `LeaseUntil == 0`;
- `LastSnapshot` is still empty: an uncommitted archive never becomes failover state;
- the second, equally eligible node never takes the workspace, checked by polling the row for several lease intervals while that node is online;
- the source tree still exists on disk and still holds its pre-move content;
- the transition log carries `quiescing -> failed` under operation `release.abort` at that node and generation, and no `ws.released`, `ws.claimed` or `ws.restored` was emitted.

### B16 — lose the node after the commit; the replacement claims only the new generation

Same file,
`TestPlanBContinuityNodeLostAfterCommitYieldsOnlyTheNewGeneration`.

An authoritative checkpoint commits `v1-committed`. A further write,
`v2-after-the-commit`, exists only in the source tree — it is the marker that
identifies which copy a later reader is looking at. The holder is then stopped.

Asserted:

- the replacement holds exactly `committed.Generation + 2`, and each bump is named in the event log: `ws.lease_expired` at `+1` (the fence that revokes every grant made under the lost assignment) and `ws.claiming`/`ws.claimed`/`ws.restored` at `+2`;
- it restored from the committed digest and publishes `v1-committed`, never the uncommitted write;
- a grant minted under the lost assignment is refused. Control caps a grant's lifetime at the lease it was minted under, so under the two-second sim lease the expiry fence answers before the generation check is reached; the generation fence itself is proven in B17 with a lease long enough for the grant to outlive the move. A freshly minted grant on the same raw peer works, which is the positive control that the refusal was about authority and not about the call;
- the lost node is restarted with the same data directory and the same identity. Its re-adoption is refused: the row does not move for several lease intervals, no new `ws.claiming`/`ws.claimed` is emitted, the workspace still reads what the replacement wrote, and the superseded tree still holds `v2-after-the-commit` — which is exactly why publishing it would have been a silent rollback.

### B17 — restart control; stale grants, nodes and ready messages are refused

`internal/sim/planb_continuity_control_test.go`,
`TestPlanBContinuityControlRestartRefusesStaleAuthority`.

This scenario needs a control plane that can be closed and reopened over its
own SQLite state, which the shared `world` cannot do, so the file carries a
small `planBControlWorld` of its own: the same topology (real nodes and clients
over in-memory pipes) with a swappable server generation. Its lease is 60
seconds, because a lease-expiry failover in the middle of a restart test would
prove something else.

Asserted:

- durable authority survives: after the restart the same node holds the same generation, and a grant issued before the restart still works. The honest boundary matters as much as the refusals — a test that only shows things being rejected cannot tell a fence from a system that stopped working;
- a node peer that does not hold the workspace is refused on `ws.ready` at the current generation, at a generation that never existed, and at generation zero, and on `ws.claim` for an already-claimed workspace;
- `ws.renew` from that node answers `accepted=false`, `action=fence`, and names the authoritative generation. Renewal is the standing authority question, and the answer to a non-holder is to fence, never to continue;
- none of it moved the row;
- the generation fence itself: a move pinned to the node that already holds the workspace advances the generation while the pre-move grant is still unexpired (both asserted before the grant is used, so neither a changed destination nor an expired grant can masquerade as a generation refusal), and the grant is then refused with `conflict`. A fresh grant on the same peer works.

### B18 — the queue resumes on another node without repeating a completed item

`internal/sim/planb_continuity_queue_test.go`,
`TestPlanBContinuityQueueResumesWithoutRepeatingACompletedItem`.

Item 0 runs to a known outcome and its result and the cursor commit together.
Item 1 is still running when its node dies, so its outcome is unknown.

Asserted:

- exactly once from both directions: retrying the same advance under the same idempotency key replays the recorded result without a second attempt, and advancing the same index under a new key is a conflict;
- after the node loss the cursor still points at item 1 and item 1 carries no outcome at all — no attempt, no finish time, no session, no exit — and the event log contains a `queue.advanced` for index 0 and nothing else. An interrupted item is due again, which is the only truthful answer;
- the workspace returns on the other node from the committed checkpoint, so the completed item's effect appears exactly once and the interrupted item's partial write, which was never committed, is absent;
- the cursor lives only in the control plane: `.remount/queue` does not exist in the tree;
- the resumed driver finishes items 1 and 2, the queue reaches `done`, item 0 still records one attempt, and the event log records each index exactly once in order.

## Relationship to what already existed

| Existing test | What it already proves | What this lane adds |
|---|---|---|
| `TestReconnectMidStreamIsLossless` | one fixed cut schedule is lossless | bounded seeded schedules, and the explicit-gap branch |
| `TestNodeDeathMovesWorkspaceFromSnapshot` | plain failover restores from the last snapshot | the ambiguous prepare, and the return of a superseded node |
| `TestNodeRestartAdoptsLocalWorkspaces` | the same node re-adopts at the same generation | a node whose generation was superseded is refused |
| `TestAuthoritativeCheckpointFencesManagedProcessWriters` | a checkpoint quiesces its writers | what happens when the checkpoint result is lost |
| `TestAgentSurvivesNodeLoss` | an Agent survives a node loss | a queue's cursor semantics across the same loss |
| `TestQueueRunsTasksAcrossSleepAndMove` | a queue continues on a second node after a clean stop | continuation after an item's outcome became unknown |
| `internal/control` fencing tests | the rules hold at the control-plane API | the same rules hold through the protocol, with real peers |

## Determinism and cost

B14's chaos loop uses four fixed seeds and prints the seed and the schedule on
failure, so a failure is replayed by rerunning that seed. Each seed draws one
offset per equal band of the stream and at most one cut fires per received
chunk: a seed may vary where a cut lands, but it can never ask for two cuts the
client has no chance to reconnect between, which would test the harness rather
than the reconnect. The cut itself waits, bounded, for a connection to exist
and fails if it never severs one, so a seed cannot pass by injecting nothing.
Nothing else in the lane is randomized. No test sleeps to establish ordering:
each waits on a real signal — a dropped frame, a session marker, an
authoritative row, an event.

Wall clock on a development machine, process backend:

| Scenario | Approx. |
|---|---|
| B14 | 25 s (four seeded reconnect schedules dominate) |
| B15 | 60 s (the control plane's own prepare-retry and abort-retry budgets) |
| B16 | 12 s |
| B17 | 4 s |
| B18 | 4 s |

B15's cost is inherent: the control plane retries a prepare it never heard the
answer to and then retries the abort, and refusing to shorten those budgets is
the property under test.

## Gates

```sh
gofmt -l internal/sim
go build ./...
go vet ./internal/sim/...
go test -count=1 -timeout 30m ./internal/sim/ -run 'PlanBContinuity'
go test -race -count=1 -timeout 40m ./internal/sim/ -run 'PlanBContinuity'
staticcheck ./internal/sim/...
```

The lane also passes with no environment variables set
(`env -i PATH=... HOME=... go test ...`).

## Open, and deliberately not fixed here

While building B18 the sim surfaced a node-shutdown hang that is a production
concern, not a test one, and was therefore reported rather than fixed.

Stopping a node immediately after a managed exec session produces its first
output leaves `Node.shutdown` parked in `session.Manager.Shutdown` at
`m.observers.Wait()` (`internal/session/session.go`), with the session's stdout
and stderr pumps still blocked reading a live child. The per-session
`s.Wait` in that loop is bounded to five seconds; `m.observers.Wait()` is not,
so a producer that does not stop pins node shutdown without any bound or
observable outcome. It reproduces with `sh -c "echo started; sleep 120"`, where
the shell replaces itself with the sleep, and does not reproduce when the shell
stays resident or when the stop is delayed by three seconds — so it is a race,
not a property of long-running work. It is the same shape as
[MISTAKES #41](../../MISTAKES.md): every wait on a process needs a bound and an
observable outcome when the bound is hit.

B18 works around it by keeping its interrupted task resident in the shell. The
scenario is unaffected — its subject is the queue's cursor, not node shutdown —
but the underlying bound belongs in the node.
