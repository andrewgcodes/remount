# Benchmarks

These are point-in-time engineering measurements, not an SLA. The runner fails
if a correctness test fails and labels every lane it could not exercise.

## Candidate and host

- Commit: `acb3e427ef5b327d805f3434d1f82f69894bd6fd` + uncommitted changes
- Measured: 2026-09-04T04:06:40.946856+00:00
- Host: Darwin arm64 / arm / Python 3.14.5
- Command: `python3 bench/run.py --count 3 --iterations 2 --output bench/results/latest.json --markdown docs/benchmarks.md`
- Samples: 3 microbenchmark processes; 3 reconnect and 3 recovery runs

## Open regression, measured 2026-09-03

Two operations are slower on this candidate than on `21ef995`, bisected to
`a3b5235 "feat(platform): converge hosted runtime handoff"`:

| Operation | `21ef995` | this candidate | ratio |
|---|---:|---:|---:|
| Two-node move, 200 MiB | 0.85 s | 33–76 s | ~40–90x |
| Client cut and lossless reattach | 0.855 s | 6.24 s | ~7x |

The reattach row below therefore reports a regressed number, not a normal one.
Both are under investigation; see `MISTAKES.md` #42 for why neither was caught
for weeks — the checked-in evidence was recorded before the regression and the
move benchmark could not run against a standalone server at all, so the one
instrument pointed at this was itself broken.

Numbers here are re-earned on the candidate. Do not carry a row forward from an
earlier file: a committed performance number is a claim, and claims decay.

## Local results

| Measurement | p50 / median | p99 | Evidence |
|---|---:|---:|---|
| Full 8 MiB chunked snapshot | 611.73 ms | not sampled | 8.01 MiB uploaded |
| 4 KiB edit of an 8 MiB snapshot | 17.90 ms | not sampled | 82.5 KiB uploaded; 99.3× logical/upload ratio |
| Direct loopback TLS request | 79.1 µs | not sampled | Go benchmark, warm connection |
| Brokered allowed request | 161.8 µs | not sampled | 82.7 µs median incremental overhead |
| Client cut mid-stream and lossless reattach | 6.240 s | 6.570 s | `TestReconnectMidStreamIsLossless`; bytes are asserted identical |
| Graceful move, then node-loss recovery | 3.090 s | 3.090 s | `TestNodeDeathMovesWorkspaceFromSnapshot`; generation and bytes asserted |

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


## Unavailable evidence

- **Other move backends:** Docker and gVisor were not available in the local
  two-node run. The process results must not be relabelled as isolation-backend
  performance.

- **Six-hour / 2 GiB session:** the tiered-log correctness suite is separate;
  the short reconnect timing above must not be relabelled as E11.

`bench/results/latest.json` is the machine-readable evidence. A release should
regenerate both files from a clean tagged commit and attach the raw JSON. A
failed or skipped lane remains unavailable, never zero or healthy.
