# Benchmarks

These are point-in-time engineering measurements, not an SLA. The runner fails
if a correctness test fails and labels every lane it could not exercise.

## Candidate and host

- Commit: `3e8cc9707213581a478947dd02e488e025900072` + uncommitted changes
- Measured: 2026-09-04T05:00:12.184980+00:00
- Host: Darwin arm64 / arm / Python 3.14.5
- Command: `python3 bench/run.py --count 3 --iterations 2 --output bench/results/latest.json --markdown docs/benchmarks.md`
- Samples: 3 microbenchmark processes; 3 reconnect and 3 recovery runs

## A regression that lived here for weeks

Two operations were 8-90x slower between `a3b5235` and 2026-09-03, and this
file is why nobody knew: its numbers were recorded at `0bb2137`, before the
regression, and carried forward as fact.

| Operation | `21ef995` | regressed | fixed |
|---|---:|---:|---:|
| Two-node move, 200 MiB | 0.85 s | 33-76 s | 2.57 s |
| Client cut and lossless reattach | 0.855 s | 6.24 s | 0.72 s |

The cause was one fsync per unit inside two streaming paths; see `MISTAKES.md`
#42 and #45. The move benchmark that would have caught it could not run against
a standalone server at all, because presenting a bearer to the artifact
endpoint returned 401 while presenting none succeeded — so the one instrument
pointed at this was itself broken.

Every number below is re-earned on the candidate named above. Do not carry a
row forward from an earlier file: a committed performance number is a claim,
and claims decay while the code moves underneath them.

## Local results

| Measurement | p50 / median | p99 | Evidence |
|---|---:|---:|---|
| Full 8 MiB chunked snapshot | 996.20 ms | not sampled | 8.01 MiB uploaded |
| 4 KiB edit of an 8 MiB snapshot | 33.26 ms | not sampled | 82.6 KiB uploaded; 99.2× logical/upload ratio |
| Direct loopback TLS request | 74.6 µs | not sampled | Go benchmark, warm connection |
| Brokered allowed request | 143.2 µs | not sampled | 68.7 µs median incremental overhead |
| Client cut mid-stream and lossless reattach | 0.720 s | 0.800 s | `TestReconnectMidStreamIsLossless`; bytes are asserted identical |
| Graceful move, then node-loss recovery | 3.170 s | 3.230 s | `TestNodeDeathMovesWorkspaceFromSnapshot`; generation and bytes asserted |

The chunk fixture is deterministic pseudo-random data. Baseline creation for
the delta case is outside the timed interval. The HTTP comparison uses the
same local TLS origin and warm connections; it measures policy/broker overhead,
not internet latency.

## Two-node move matrix

The `move_matrix.py` runner uploaded deterministic incompressible fixtures,
moved each workspace back and forth between two independently enrolled nodes,
and ran SHA-256 inside the workspace after every move. A hash mismatch fails
the run. These are local `process` backend numbers, not network or isolation
backend claims.

- Commit: `0bb21377a1ae242ac482d7efe51a3ee95a8edc69` + uncommitted changes
- Measured: 2026-09-03T19:29:51Z
- Host/backend: Darwin arm64 / Python 3.14.5 / `process`

| Workspace fixture | p50 | p99 (nearest-rank) | Samples |
|---:|---:|---:|---:|
| 1 MiB | 0.188 s | 0.251 s | 3 |
| 100 MiB | 0.663 s | 0.706 s | 3 |
| 500 MiB | 2.733 s | 3.308 s | 3 |


## Fleet-scale control failover

The checked-in scenario used real in-process WebSocket peers and the process
backend. It created 200 nodes and clients, claimed 2,000 workspaces, exercised
one exec and move per node, restarted the control plane over the shared SQLite
store, reconnected 400 peers, and required byte-exact session completion with
no gaps or generation mismatch. These measurements are not Docker or gVisor
performance claims.

- Commit: `8d5304ec173b376f0abd4f37a3e5b3ceceb2ecc1` + uncommitted changes
- Measured: 2026-09-03T21:50:00-07:00
- Host/backend: Darwin arm64 / Go 1.27.1 / `process`
- Control restart: 0.234 s; reconnected peers: 400

| Operation | Samples | p50 | p99 |
|---|---:|---:|---:|
| claim | 2000 | 0.516 s | 6.938 s |
| exec round trip | 200 | 3.245 s | 3.287 s |
| move | 200 | 6.695 s | 8.196 s |
| reattach after control restart | 200 | 5.686 s | 5.705 s |

### Cost of the durable session-log tier, per exec

`internal/sim.TestExecRoundTripCostOfTheDurableSessionTier` measures one
trivial exec with the artifact tier live against the same exec without it,
back to back on the same host, and reports the ratio. It exists because the
2026-09 regression sweep found nothing in `bench/run.py` that would have caught
the exec regression at `a3b5235`, and named this as the lane that would have.

The ratio is what to watch; the absolute numbers are a property of the host.

| Configuration | p50 (n=24) |
|---|---:|
| artifact tier off | 11-12 ms |
| artifact tier on | 29 ms |
| **ratio** | **2.47-2.66x** (three runs) |

Sealing a ~195-byte segment writes it to the node's own artifact store (two
fsyncs), uploads it to the control plane's store (two more, plus HTTP), and
commits a record: about 15 ms. That is the price of spec §8.2 — an exited
session replays byte-exactly after node loss — paid synchronously on every
exec. The test's gate is set at 5x rather than at the observed value so that a
loaded host does not fail the suite; see
`docs/engineering/performance-regressions-2026-09.md` for what remains open.


## Unavailable evidence

- **Other move backends:** Docker and gVisor were not available in the local
  two-node run. The process results must not be relabelled as isolation-backend
  performance.

- **Six-hour / 2 GiB session:** the tiered-log correctness suite is separate;
  the short reconnect timing above must not be relabelled as E11.

`bench/results/latest.json` is the machine-readable evidence. A release should
regenerate both files from a clean tagged commit and attach the raw JSON. A
failed or skipped lane remains unavailable, never zero or healthy.
