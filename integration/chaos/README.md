# Scale and reconnect-storm evidence

`internal/sim/scale_handoff_test.go` is the reproducible Phase 6.4 evidence
scenario. It creates 200 real `node.Node` supervisors, 200 real SDK clients,
and 2,000 process-backed workspaces over encoded in-memory transports. One
workspace per node executes and moves. A live stdin session on every node then
crosses a control-plane process replacement and proves replay by receiving a
unique post-restart marker with no `gap` chunks.

Run the scenario and its required repetition/race lanes with:

```sh
go test ./internal/sim -run '^TestHandoffScaleAndControlFailover$' -count=1 -timeout=15m -v
go test ./internal/sim -run '^TestHandoffScaleAndControlFailover$' -count=2 -timeout=20m
go test -race ./internal/sim -run '^TestHandoffScaleAndControlFailover$' -count=1 -timeout=30m -v
```

The test logs nearest-rank p50/p99 samples for all 2,000 claims and for 200
exec round trips, moves, and reattachments. It also checks the complete
workspace/node inventory after recovery and waits for tracked node and relay
goroutines during teardown. The checked-in local run record is
[`results/process-local-2026-09-03.json`](results/process-local-2026-09-03.json);
it distinguishes runtime availability from benchmark evidence and records the
exact revision, commands, counts, host, measurements, and residual limits.

The failover model is deliberately narrow: a distinct server object reopens
the same durable SQLite database after the old object closes. It proves
process-restart recovery and the 400-peer reconnect storm. It does **not**
prove replicated control storage, warm-standby promotion, S3 replication RPO,
or behavior during simultaneous writers.

The ordinary simulation lane uses the unisolated process backend. Docker and
gVisor numbers are unavailable unless their host gates pass; absence must not
be reported as a successful benchmark. Check a host without starting a
container:

```sh
integration/chaos/backend-gates.sh
```

The script reports gVisor unavailable until an actual `runsc` workload probe
is requested. On a benchmark host, set an approved image and run:

```sh
REMOUNT_CHAOS_IMAGE=registry.example/remount/bench@sha256:... \
  integration/chaos/backend-gates.sh --probe
```

Docker documents `docker version --format` as the daemon-version check and
gVisor documents both registering `runsc` with Docker and verifying it with a
`docker run --runtime=runsc` workload:

- <https://docs.docker.com/reference/cli/docker/version/>
- <https://gvisor.dev/docs/user_guide/quick_start/docker/>

Passing these gates establishes runtime availability only. Publishing Docker
or gVisor p50/p99 still requires running the 200-node/2,000-workspace scenario
against a node registry configured for that backend; the current in-process
scenario provides process-backend evidence and prints the other lanes as
unavailable.
