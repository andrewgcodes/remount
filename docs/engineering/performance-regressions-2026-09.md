# Performance regressions introduced by `a3b5235`, 2026-09

Two regressions were already known: workspace move (0.85 s → 75.79 s for 200 MB)
and session reattach (0.855 s → 6.754 s), both bisected to `a3b5235`
("feat(platform): converge hosted runtime handoff", 12,504 insertions, 66
files). This document records a wider sweep for the same commit and reports
three further regressions, the mechanism behind each, and the lanes that were
swept and came back clean.

Everything below is a measurement. Where a number is within noise it says so.

## Host and method

- Host: Darwin arm64, 18 cores, shared with other agents running heavy suites.
  Load average at the start of the sweep was 2.90 / 5.41 / 7.28.
- Builds: `git worktree add --detach <dir> <commit>` for `21ef995` (last good),
  `a3b5235` (first bad) and `acb3e42` (HEAD at the time of writing), then
  `go test -c -o <bin> ./internal/sim` in each worktree. The compiled binaries
  are run directly from each worktree's `internal/sim` directory so that the
  test source, fixtures and working directory match the library under test.
- Comparisons are only made between tests whose **source is identical at both
  commits**. `internal/sim/scale_handoff_test.go` and `bench/` are byte
  identical across `21ef995..a3b5235` (`git diff 21ef995 a3b5235 -- bench/
  internal/sim/scale_handoff_test.go` is empty), so those lanes measure the
  library and nothing else.
- Runs were interleaved (good, bad, head, repeat) rather than batched, so that
  drift in machine load does not land entirely on one commit.

## Ranked results

| # | Operation | `21ef995` | `a3b5235` | Ratio | HEAD | Above noise? |
|---|---|---:|---:|---:|---:|---|
| 1 | Session reattach after mid-stream cut (`TestReconnectMidStreamIsLossless`) | 0.52 s | 6.49 s | 12.5× | 7.05 s | yes, already known |
| 2 | Workspace claim, p99 at 2,000 workspaces | 0.79 s | 7.82 s | 9.9× | 6.63–9.43 s | yes, ranges disjoint |
| 3 | Exec round trip, p50 at 200-way concurrency | 0.645 s | 3.125 s | 4.8× | 3.12–3.40 s | yes, ranges disjoint |
| 4 | Workspace move, p50 at fleet scale | 1.88 s | 5.75 s | 3.1× | 5.58–6.11 s | yes, ranges disjoint |
| 5 | Reattach after control restart, p50, 200 peers | 2.24 s | 4.90 s | 2.2× | 5.10–5.42 s | yes, ranges disjoint |
| 6 | Volume fence write per claim/move (absolute, new code path) | did not exist | 7.9 ms @ 1 fence → 17.3 ms @ 10,000 | n/a | same | yes |
| 7 | Whole fleet-scale scenario, wall clock | 16.5 s | 35.1 s | 2.1× | 33.5–40.3 s | yes, ranges disjoint |
| 8 | Short single-node exec-shaped sim tests | 0.26–1.28 s | 0.35–1.69 s | 1.3–1.5× | same | marginal; consistent in sign, small in absolute terms |

Rows 2–5 and 7 come from one command, run four times per commit:

```
cd <worktree>/internal/sim
<simbin> -test.run '^TestHandoffScaleAndControlFailover$' -test.v -test.timeout=30m
```

The test itself prints the per-operation percentiles (`scale_handoff_test.go`
lines 94/119/141/178/225). Samples, in milliseconds unless stated:

| Measurement | `21ef995` samples | `a3b5235` samples |
|---|---|---|
| claim p50 | 390, 428, 449, 477 | 370, 378, 444, 491 |
| claim p99 | 726, 786, 793, 824 | 7064, 7747, 7887, 8330 |
| exec_round_trip p50 | 609, 620, 645, 670, 770 | 2982, 3058, 3125, 3256, 3372 |
| move p50 (s) | 1.75, 1.80, 1.88, 1.90, 2.29 | 5.31, 5.73, 5.75, 5.79, 6.15 |
| reattach p50 (s) | 1.86, 2.18, 2.24, 2.57, 2.97 | 4.62, 4.77, 4.90, 5.20, 5.45 |
| control restart | 314, 400, 526 | 237, 242, 476 |
| whole test (s) | 15.0, 16.5, 17.2, 22.6 | 35.0, 35.1, 36.3, 39.1 |

Note that **claim p50 and control restart did not move**. The claim regression
is entirely in the tail, which is the signature of a serialized resource rather
than added work on every request.

## Regression 2 and 4: every claim and every move rewrites the node's whole volume catalog

`internal/node/node.go:4269` `attachWorkspaceVolumesScoped` calls
`n.volumes.SetWorkspaceGeneration(...)` **before** it checks whether the
workspace has any volumes at all:

```go
n.volumeMu.Lock()
defer n.volumeMu.Unlock()
if err := n.volumes.SetWorkspaceGeneration(ctx, w.Tenant, w.ID, w.Generation); err != nil {
	return fmt.Errorf("set volume generation fence: %w", err)
}
if len(w.Spec.Volumes) == 0 {
	return nil
}
```

`SetWorkspaceGeneration` (`internal/volume/local.go:842`) sets
`needsCommit := generation != current`, so a first claim (0 → 1) and every move
(n → n+1) commit through `mutateLocked` (`internal/volume/local.go:1114`):

```go
func (b *LocalBackend) mutateLocked(change func()) error {
	before, err := cloneState(b.state)   // json.Marshal + json.Unmarshal of the whole catalog
	...
	if err := saveState(b.filename, b.state); err != nil {   // json.Marshal again, temp file, fsync, fsync parent
```

So one workspace claim costs two full JSON marshals plus one unmarshal of the
node's entire volume catalog, a temp-file write, an fsync of that file and an
fsync of the parent directory — all under the node-wide `volumeMu`. The fence
map is bounded only by `MaxWorkspaceFences`, which defaults to 65,536
(`internal/volume/local.go:86`), so claiming *n* workspaces on a node is O(n²)
work.

This is on the **claim** path for every workspace including workspaces with no
volumes, and the fleet-scale scenario uses no volumes at all. It is also on the
**move** path twice (detach on release-prepare, attach on the destination).

`internal/volume` is entirely new in `a3b5235`, and every node opens a
`LocalBackend` unconditionally (`internal/node/node.go:423`), even when the bind
mount probe fails and the node reports `read-only volumes unavailable`. On such
a host the catalog is fsynced on every claim to support a feature that host
cannot use.

Measurement (`bench/volume_fence_bench_test.go`, added by this sweep; it
re-fences one workspace at an advancing generation, which is exactly what a
claim or a move does, with the catalog held at a fixed size):

```
go test -run '^$' -bench 'BenchmarkVolumeFenceCatalog' -benchtime=200x -count=3 ./bench
```

| Catalog size | ns/op samples | Median |
|---:|---|---:|
| 1 | 7,945,516 / 7,723,271 / 7,985,718 | 7.95 ms |
| 100 | 8,125,185 / 8,234,909 / 8,389,736 | 8.23 ms |
| 1,000 | 9,609,043 / 14,319,171 / 9,974,840 | 9.97 ms |
| 10,000 | 17,269,711 / 24,641,624 | 17.3 ms |

The floor of ~8 ms per claim is the two fsyncs; the growth from 8 ms to 17 ms is
the catalog re-serialization. Neither cost existed at `21ef995`.

CPU profile corroboration, same scenario, same command plus
`-test.cpuprofile`:

```
go tool pprof -top -cum <simbin> <profile>
```

`a3b5235`, 24.71 s of samples over 39.18 s wall:

```
2.53s 10.24%  remount.dev/remount/internal/node.(*Node).attachWorkspaceVolumesScoped
2.53s 10.24%  remount.dev/remount/internal/volume.(*LocalBackend).SetWorkspaceGeneration
2.53s 10.24%  remount.dev/remount/internal/volume.(*LocalBackend).mutateLocked
2.53s 10.24%  remount.dev/remount/internal/volume.saveState
1.68s  6.80%  os.(*File).Sync
1.25s  5.06%  remount.dev/remount/internal/node.(*Node).persistReleasesLocked
```

`21ef995`, 18.45 s of samples over 17.29 s wall: no `internal/volume` frames at
all, and `os.(*File).Sync` is 0.31 s (1.68%).

`persistReleasesLocked` (`internal/node/release_journal.go`, also new) is the
second entry on that list at 1.25 s: it re-marshals the node's whole release map
and fsyncs the file plus its parent directory on **each** of the four or five
release state transitions in a single move.

## Regression 3: every exec now uploads its session log and takes two extra network round trips before `Wait` returns

`a3b5235` wires tiered session logs (`internal/node/session_logs.go`, new file).
`tieredSessionLogsEnabled()` is true whenever `n.opts.ArtifactURL != ""` and the
capability is negotiated, which is the normal production configuration. The
session `Log` therefore gets a `BlobStore`, and `closeWithPublish`
(`internal/session/log.go:325`) — which runs on every session close, holding
`l.mu` for its whole body — takes this branch:

```go
if l.opts.BlobStore != nil {
	err = l.evictLocked(true)
	if err == nil {
		err = l.sealSpillLocked()
	}
```

`sealSpillLocked` (`internal/session/log.go:502`) then does, in order and
synchronously under the lock:

1. `l.spill.Sync()` — an fsync.
2. `l.opts.BlobStore.Put(...)` — an authenticated HTTP PUT to the artifact
   server.
3. `l.opts.BlobStore.Head(id)` — a second HTTP round trip to verify the size.
4. `l.opts.CommitRecord(...)` — which reaches
   `node.commitSessionLogRecord` → `peer.Call(proto.PeerControl,
   proto.OpSessionLogCommit, ...)`, a full control-plane round trip.

At `21ef995` `BlobStore` was nil on the node side, so `closeWithPublish` did
none of this. The measured effect is 0.645 s → 3.125 s p50 for a
`printf`-sized exec at 200-way concurrency, and roughly 1.3–1.5× on the small
single-node sim tests, where there is no contention to amplify it.

Mutex profile, same scenario, `-test.mutexprofile -test.mutexprofilefraction=10`:

`a3b5235`, 2,214 s of total mutex delay:

```
1041.72s 47.04%  remount.dev/remount/internal/session.(*Session).finish
1041.72s 47.04%  remount.dev/remount/internal/session.(*Log).closeWithPublish
 224.64s 10.14%  remount.dev/remount/internal/server.(*Server).handleArtifact
 223.84s 10.11%  remount.dev/remount/internal/artifact.(*Store).PutExpected
```

`21ef995`, 1,294 s of total mutex delay: `closeWithPublish`, `Session.finish`,
`handleArtifact` and `PutExpected` do not appear anywhere in the profile
(`go tool pprof -top -cum -nodecount=200 | grep` returns nothing for any of
them). The top entries there are `control.wsCreate`, `control.wsClaim` and the
runtime fork lock, i.e. ordinary work.

## Regression 1 note: the reattach cost is a retry, not new work

The reattach regression is already owned elsewhere, but the sweep produced one
diagnostic worth recording. `TestReconnectMidStreamIsLossless` at `a3b5235` is
bimodal — 3.08 s, 6.41 s, 6.49 s, 6.49 s, 6.83 s, 7.09 s, 7.67 s, 10.59 s,
11.15 s across nine runs — with the modes separated by roughly the 2 s cap of
the pre-existing `Session.reattach` backoff. A read of the diff found **no new
sleep, timer, or lengthened constant** anywhere on the client-cut → redial →
`OpSAttach` path; `internal/client/client.go` is byte identical across the two
commits for all three of its backoff loops. The shape says something on the
reattach path now fails and is retried through the old backoff, rather than
that the path itself got slower. `authorizeClaims` in `internal/node/node.go`
gained exactly one new rejection branch at this commit:

```go
if proto.HasCapability(n.protocol, proto.CapabilityControllerEpoch) && g.Claims.ControllerEpoch != n.currentControllerEpoch() {
	return nil, proto.GrantClaims{}, proto.Err(proto.CodeUnauthorized, "grant controller epoch is stale")
}
```

That is the branch to instrument first. This is a lead, not a proven cause: no
run was instrumented to show that branch firing.

## Lanes swept that came back clean

Recording these so the next sweep does not repeat them.

**`bench/run.py`'s four Go microbenchmarks.** `bench/` is unchanged across the
two commits, so this is a pure library comparison.

```
go test -run '^$' -bench 'Benchmark(Chunked|Direct|Broker)' -benchtime=3x -count=5 ./bench
```

Medians of five processes:

| Benchmark | `21ef995` | `a3b5235` | HEAD |
|---|---:|---:|---:|
| ChunkedSnapshotFull8MiB | 841.7 ms | 917.9 ms | 845.7 ms |
| ChunkedSnapshotDelta4KiBOf8MiB | 28.4 ms | 27.5 ms | 28.4 ms |
| DirectTLSRoundTrip | 77.3 µs | 75.1 µs | 70.4 µs |
| BrokerAllowedRoundTrip | 143.4 µs | 144.7 µs | 149.5 µs |

Every sample range overlaps (full snapshot: 832–894 ms old, 790–1009 ms new).
Brokered-minus-direct overhead is 66 / 70 / 79 µs — flat. In particular the new
`CapabilityVerifier` in `internal/broker/broker.go` does **not** sit on the
workspace-bearer path this benchmark exercises. Upload bytes and dedupe ratio
are byte-identical across all three commits, so chunking behaviour did not
change either.

**Per-package `go test` wall time across `./internal/...`.** Running
`go test -count=1 ./internal/...` in each worktree suggested large regressions
in `eventlog` (6.2×), `connector` (5.2×), `artifact/chunked` (4.4×), `fsops`
(3.8×), `broker` (3.1×) and `localfs` (3.1×). **All of these are artefacts.**
Re-running those packages as standalone compiled binaries, one at a time,
twice per commit, and comparing only the tests present at both commits:

| Package | common-test total, `21ef995` | `a3b5235` | Ratio | Tests only at `a3b5235` |
|---|---:|---:|---:|---:|
| eventlog | 0.11 s | 0.11 s | 0.95 | 0 |
| connector | 0.32 s | 0.32 s | 1.00 | 0 |
| artifact/chunked | 1.26 s | 1.26 s | 1.00 | 0 |
| fsops | 0.06 s | 0.06 s | 1.00 | 0 |
| broker | 4.83 s | 4.83 s | 1.00 | 0 |
| localfs | 0.45 s | 0.45 s | 1.00 | 0 |
| artifact | 0.31 s | 0.31 s | 1.00 | 0 |
| identity | 0.01 s | 0.01 s | 1.00 | 11 (0.06 s) |
| server | 0.06 s | 0.07 s | 1.36 | 8 (0.20 s) |
| control | 5.59 s | 5.63 s | 1.01 | 31 (10.01 s) |

The same sweep also reported many packages getting 2–5× *faster*
(`provision/ix` 3.13 s → 0.52 s, `transport` 2.96 s → 0.79 s), which is the
tell: `go test ./internal/...` schedules packages in parallel across all cores,
so a package's reported wall time mostly reflects what else was running beside
it. **Do not use repo-wide `go test` wall time as a performance instrument.**

**Repo materialize (`TestRepoRefShapes`,
`TestRepoClonedAtMaterializeWithoutTokenInWorkspace`).** A first pass showed
these 2–3× slower at HEAD. Repeating showed the good commit spiking too
(`TestRepoRefShapes` samples 1.31, 1.36, 4.42 s at `21ef995`; 1.32, 1.34,
1.27 s at `a3b5235`). These tests fork `git` subprocesses and are simply noisy
on a loaded machine. Not a regression.

**Control-plane restart.** 314 / 400 / 526 ms old versus 237 / 242 / 476 ms
new. Unchanged.

**Node-loss recovery (`TestNodeDeathMovesWorkspaceFromSnapshot`), sleep/wake,
queue-across-move, pool scale-up/down.** 3.21 → 3.16 s, 2.34 → 2.31 s,
8.22 → 8.19 s, 3.03 → 3.05 s. All flat; these are dominated by fixed waits.

**The rest of the `internal/sim` suite.** All 61 tests that exist at both
commits were run at both. Outside `TestReconnectMidStreamIsLossless` and
`TestHandoffScaleAndControlFailover`, the largest genuine ratios are the
1.3–1.5× on short exec-shaped tests recorded as row 8 above
(`TestIdempotentExec` 0.42 → 0.65 s, `TestPTYAndStdin` 0.26 → 0.35 s,
`TestExecEndToEnd` 0.34 → 0.45 s, `TestConsoleE15SimulatedOperatorFlow`
1.28 → 1.69 s). These are the single-node shadow of regression 3 plus the
per-claim fence write of regression 2; each is worth ~0.1–0.4 s in absolute
terms and none is individually above the 3× bar, but the sign is consistent
across every exec-shaped test and the console test's ranges do not overlap.

## Correctness note, not performance

`TestLocalDirectoryRoundTrip` fails at `a3b5235` with `localfs_test.go:148:
gzip: invalid header`. That is the defect fixed later by `da21b9f`
("fix(snapshot): export chunked snapshots as tar streams"), not a timing
result. Its 0.40 s → 2.86 s is a failure path and was excluded from the ranking.

## What a regression gate would have to measure

None of the three new regressions is visible in `bench/run.py` as it stands.
The lanes that would have caught them:

- `TestHandoffScaleAndControlFailover`'s own `claim` p99, `exec_round_trip` p50
  and `move` p50, which the test already computes and logs but which nothing
  compares against a baseline.
- A per-claim cost benchmark like `bench/volume_fence_bench_test.go`, which
  makes the O(n) catalog rewrite visible without needing 2,000 workspaces.
- A single-session exec round trip with an artifact URL configured, which is
  what makes the session-log upload path live.

---

## Disposition, 2026-09-03 (added after the sweep)

| Row | Regression | Status |
|---|---|---|
| 1 | Session reattach mid-stream | **fixed** — per-record spill fsync removed; 6.75 s → 0.72 s |
| 2 | Workspace claim p99 at 2,000 workspaces | **fixed** — 7.82 s → 0.666 s, better than the 0.79 s baseline |
| 4 | Workspace move p50 at fleet scale | **improved** — 5.75 s → 4.43 s |
| 5 | Reattach after control restart | **improved** — 4.90 s → 4.17 s |
| 6 | Volume fence write per claim/move | **fixed** for volume-incapable nodes |
| 7 | Whole fleet-scale scenario | **improved** — 35.1 s → 22.4 s |
| 3 | Exec round trip p50 at 200-way concurrency | **diagnosed and partly reduced** — see "Regression 3, re-diagnosed" below. The first diagnosis in this document was wrong. |

Also fixed separately: the 200 MiB two-node move, 33–76 s → 0.88 s (228 MB/s
against a 235 MB/s baseline), by bounding chunk-transfer concurrency and
declining to chunk a tree that will not deduplicate.

### What the volume fix was

`Node.attachWorkspaceVolumesScoped` commits a workspace generation fence before
its `len(w.Spec.Volumes) == 0` early return, and each commit rewrites the whole
catalog — two JSON marshals, an unmarshal, a temp file and two fsyncs — under
the node-wide volume lock. Claiming *n* workspaces is O(n²).

The fix removes the cost only where it is unambiguously pointless: a node whose
bind-mount probe fails never advertises `read-only-volumes`, so the control
plane never places a volume-bearing workspace on it and no attachment can
succeed. Such a node now drops the backend entirely and takes the existing
`n.volumes == nil` path, which returns immediately for a workspace with no
volumes and refuses one that asks for them.

**Still open for a volume-capable node:** a workspace with no volumes continues
to pay a full catalog rewrite on every claim and move, and the catalog is
bounded only by `MaxWorkspaceFences` = 65536, so the O(n²) remains. Moving the
early return above the fence commit is *not* obviously safe: the fence also
clears `b.authorized`, which matters for a workspace that previously had
volumes and no longer does. The correct fix is either a cheap "is this
workspace known to the catalog" test before committing, or a catalog that does
not rewrite every entry per fence. `bench/volume_fence_bench_test.go` measures
the per-claim cost directly: 7.95 / 8.23 / 9.97 / 17.3 ms per operation at
catalog sizes 1 / 100 / 1000 / 10000.

### Row 3 remains open, with its cause identified

`Log.closeWithPublish` holds `l.mu` across `evictLocked(true)` and
`sealSpillLocked`, and that seal performs an fsync, a `BlobStore.Put`, a
`BlobStore.Head` and a `CommitRecord` — two HTTP requests and a control-plane
round trip — before the lock is released. Every reader of that session blocks
behind all of it, on every session close. The mutex profile at `a3b5235`
attributes 1041 s of 2214 s total delay to `Session.finish → closeWithPublish`;
none of those frames appear at `21ef995`.

This is the same shape as the `Node.Diag` race fixed earlier in this pass —
work that must not hold a lock, holding one — but the remedy is not a
snapshot-and-release. The seal is a durable commit: the segment must be
published and recorded before the record is authoritative, and readers must
never observe a partially sealed log. Restructuring it needs the authority,
resource, irreversible action, durable commit point and observable
postcondition restated, and a test that makes the forbidden interleaving
visible. It was deliberately not attempted unreviewed.

Skipping the seal for short sessions is **not** a valid shortcut: sealing is
what makes a completed session survive node loss, which is the whole promise of
`tiered-session-logs` and what E11 asserts.

---

## Regression 3, re-diagnosed (2026-09-03, later the same night)

An earlier revision of this document, and the commit message for `c89e413`,
said regression 3 was `Log.closeWithPublish` holding `l.mu` across an fsync,
a `BlobStore.Put`, a `BlobStore.Head` and a `CommitRecord`, "47% of all mutex
delay". **That attribution was wrong**, and MISTAKES.md #47 records how it was
arrived at. Re-profiling the same scenario and using `go tool pprof -peek` on
each symbol rather than reading the top of the profile:

| Path | Share of mutex delay |
|---|---:|
| `control.(*Control).dispatch` | 26.5% |
| `artifact.(*Store).publish` | 9.7% |
| `syscall.forkExec` (Go's process-wide `ForkLock`) | 9.4% |
| `control.(*Control).sessionLogCommit` | 1.4% |

### What the cost actually is

The mechanism was found by building the lane this document had already said
was missing — "a single-session exec round trip with an artifact URL
configured, which is what makes the session-log upload path live". That lane is
now `internal/sim.TestExecRoundTripCostOfTheDurableSessionTier`, and it holds
everything equal except whether the node advertises a durable tier:

```
exec round trip p50: artifact tier off 11ms, on 29ms, ratio 2.66x (n=24 each)
```

Instrumenting each phase of a seal of a session that printed ten bytes:

| Phase | Cost |
|---|---:|
| spill fsync before upload | 3.4 ms |
| `BlobStore.Put` (node-local store: 2 fsyncs, then HTTP upload to control: 2 more) | 14.5 ms |
| `BlobStore.Head` | 0.12 ms |
| `CommitRecord` | 0.6 ms |
| **total** | **~19 ms** |

The two-stores-two-fsyncs-each claim is measured, not inferred: tagging every
`artifact.Store.put` with its store directory shows exactly two puts per seal,
one under the node's own `.../<node>/artifacts` and one under the control
plane's `.../server/artifacts`, both `created=true` and both taking the full
`tmp.Sync()` plus `syncDir()` path.

So closing a trivial session costs five fsyncs and two round trips to durably
archive a 195-byte segment, all on the caller's critical path. That is what
spec §8.2 buys — an exited session replays byte-exactly after node loss — and
it is a real guarantee, not waste. But it is paid synchronously by every exec.

### What was fixed

The spill fsync before the upload was redundant and is removed. The bytes
handed to the blob store are read back through the same descriptor, so they are
the bytes that were written regardless of whether they reached the platter, and
§8.2 states plainly that the ring and spill "are node-local, so they do not
survive node loss". It made the non-durable tier durable immediately before
copying it into the tier that provides durability. A node lost mid-seal commits
no record, so the segment is legitimately absent rather than silently short.

Measured: seal 19 ms → 15.8 ms; exec ratio 2.81x → 2.47–2.66x across three runs.

This is the same defect as MISTAKES.md #45 one layer down: #45 removed an fsync
per *record* in `spillLocked`; this removes an fsync per *seal* in
`sealSpillLocked`. Both were syncing a tier the protocol declares non-durable.

### What is still open, and deliberately not changed

**The seal is on the exec's critical path.** Roughly 15 ms of the remaining
cost is the genuine price of §8.2 durability: two fsyncs in the node's artifact
store, two in the control plane's, an HTTP upload, and a control commit.
Removing it from the caller's critical path would change an observable
guarantee — that an exited session is immediately durably replayable — and so
is a design decision needing an ADR, not a performance patch.

A narrower version is available and also unstarted: the node's *local* artifact
store fsyncs twice (`tmp.Sync`, then `syncDir` after the rename) for a copy
that is only a cache, because the durable authority is the remote store plus
the committed record. A node that dies after the local put and before the
upload leaves a segment no record references — garbage, not data. `artifact.Store`
is also the authority in standalone mode, so this needs an explicit cache-put
path rather than a global change.

**It is worth the work, and here is the number rather than a guess.** Gating
both fsyncs behind an environment variable and re-running the lane:

| Configuration | exec ratio (tier on ÷ tier off) |
|---|---|
| today: both stores durable | 2.47-2.66x |
| node-local store as a cache, control plane still durable | **1.48-1.79x** |
| neither store fsyncs (not a legal configuration; upper bound only) | 1.11x |

So the node-local fsyncs are roughly half the durable tier's cost on an exec —
about +17 ms today against about +7 ms with the cache-put path — and the control
plane's fsyncs, which must stay, are most of the remainder. The third row is
included only to show that after both are gone almost nothing is left: the HTTP
upload, the HEAD and the control commit together are worth about 2 ms. The
experiment was reverted; nothing in the tree is gated on that variable.

**`artifact.(*Store).publish` holds the store-wide lock across filesystem
syscalls** (9.7% of mutex delay). The work inside the lock is cheap — `-peek`
shows `os.Chmod` at 4.6 ms and `os.Rename` at 1.6 ms across the entire run, and
`verifyBlobPath` never executes in this workload — so the delay is pure
queueing: every upload to the shared control-plane store serialises on one
mutex. The fix is to stop deriving "did I create this blob?" from a lock and
start deriving it from the filesystem: `os.Link(tmp, dst)` fails with `EEXIST`
if the blob is already there, which is an atomic create-if-absent, leaving the
mutex to guard only the accounting counters. That is deliberately **not** done
here: hardlink semantics differ on Windows, which
`docs/engineering/handoff-windows-host-2026-09-03.md` identifies as the least
verified platform in the repository, and this is a durable-artifact path. It
wants a reviewer and a Windows run, not a 3 a.m. commit.

**`syscall.forkExec` at 9.4% is a simulation artifact, not a product defect.**
Go serialises `fork`/`exec` process-wide behind `ForkLock`. The sim runs 200
nodes inside one test process, so all 200 execs queue there; in production each
node is its own process. Measured directly: 200 concurrent `sh -c printf`
spawns from one Go process take 436 ms wall (p50 247 ms), against 5 ms
unloaded. That is a floor the 200-way lane cannot go below and it should not be
attributed to Remount.

**`control.(*Control).dispatch` at 26.5% is the control plane's single state
machine lock, and is a design property rather than a defect.** Breaking it down
by the operation holding it:

| Operation | Share of dispatch delay |
|---|---:|
| `wsReady` | 34.4% |
| `wsCreate` | 31.5% |
| `wsClaim` | 17.2% |
| `sessionLogCommit` | 5.2% |
| `wsMove` | 3.8% |

These are workspace lifecycle transitions, and they serialise against shared
state — generations, leases, placement, authorization revisions — that the
whole system's correctness depends on being consistent. Removing the
serialisation would mean sharding the control lock per tenant or per workspace,
which is an architectural change with real fencing consequences, not a
performance patch. It is recorded here so the next person to profile this does
not spend the evening rediscovering it. The number to watch is whether any
*single* operation's share grows, which would indicate new work added under the
lock rather than the expected cost of the lock existing.

### Candidates found by shape, deliberately not acted on

A scan for the "lock held across I/O" shape across `internal/` produced 30 hits,
most of them false positives (`sync.Once.Do`, `http.StatusNotFound` matching a
crude pattern). Two are genuine instances of the shape:

| Site | Shape |
|---|---|
| `connector.(*Store).lookup` | holds the store-wide `s.mu` across `os.ReadFile`, `os.Open` and `Stat` |
| `artifact/encrypted.(*FileStore)` and `keys.go` | hold a lock across `os.Open` / `os.Remove` |

**Neither is being changed, and the reason is the point.** Lesson 14 of
`docs/engineering/hardening-lessons.md` exists because this exact reasoning —
"here is a lock held across I/O, that must be the slow thing" — produced a wrong
diagnosis earlier the same night, and nearly produced a risky restructure of a
durable-commit boundary to buy 1.4% of a profile. A shape is a hypothesis, not a
finding.

What would settle each: a benchmark driving concurrent `lookup` calls at a
realistic cache-hit rate, with `-mutexprofile`, and `go tool pprof -peek` on the
symbol by name. If the delay is material, the remedy is the same one available
to `artifact.publish` — the cached blobs are content-addressed and immutable, so
reading them needs no lock at all, and the mutex only has to guard the
accounting. Until someone runs that, this table is a list of places to look, not
a list of defects.
