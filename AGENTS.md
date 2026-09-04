# AGENTS.md

Read this before changing anything in the repo. It is operational: what the
code is, how to build and test it, what must stay true, and where each kind of
change goes.

## What this is

Remount is a protocol and a single Go binary that gives an AI agent a computer
it can run on from anywhere. The agent's computer is a **workspace**: a
filesystem plus processes that can be snapshotted, moved to another node, put
to sleep, and reattached mid-command without losing output. The workspace never
holds a credential; the node's broker substitutes real secrets at the network
edge and every decision lands in one event log.

## Build and test

```sh
make          # vet + test + build a static binary
make race     # the suite under the race detector
make cover    # coverage summary
make dist     # linux/darwin × amd64/arm64 static binaries in dist/
make demo     # remount standalone on 127.0.0.1:7443

make verify       # every gate CI runs, locally — run this before pushing
make verify-fast  # the same without the race lane and conformance
```

**Verify locally before you push.** This repository is private, so Actions
minutes are metered, and it is expensive per push: ten workflows fire on every
push to `main`, `ci.yml`'s matrix includes macOS at 10x billing and a Windows
job at 2x. A push that fails CI costs minutes and teaches nothing that
`make verify` would not have told you for free. It runs gofmt, `go vet` for
darwin, linux and windows, the lock-discipline lint, the suite, `make dist`,
`go mod verify`/`tidy -diff`, staticcheck, govulncheck, the seeded fuzz corpus,
the race lane and conformance — reporting every gate rather than stopping at
the first, so one run replaces a bisect through six pushes.

Cross-vetting all three `GOOS` values matters more than it looks: the Windows
lane has caught defects a darwin-only vet cannot see, including tests that
shell out to POSIX scripts and a comparison that only fails on CRLF.

**Tests must pass under `-race`.** Three real bugs in this codebase were only
visible there: a live pointer escaping the control-plane mutex, a lease
expiring during a slow restore, and a renew interval longer than the lease.
If `make race` is red, the change is not done.

Go 1.27, `CGO_ENABLED=0` everywhere. SQLite is `modernc.org/sqlite`, pure Go,
so the binary stays static.

## Before changing a moving checkout

Read `docs/engineering/README.md` and follow its current-status link before a
review or hardening change. Read the relevant ADRs, protocol sections,
`MISTAKES.md`, and
`docs/engineering/hardening-lessons.md`. Codex agents should use the
repo-scoped `remount-hardening-review` skill in
`.agents/skills/remount-hardening-review/`; Claude agents should also read
`.claude/skills/remount-dev/SKILL.md`.

This tree may have another agent writing to it:

1. Fetch and record `git status --short --branch`, `HEAD`, and
   `origin/main` before editing.
2. Treat every unfamiliar dirty path as user-owned. Use narrow patches and
   never reset or check out another contributor's work.
3. State the authority, resource, irreversible action, durable commit point and
   expected observable postcondition before changing a lifecycle boundary.
4. Add a regression test that makes the forbidden result observable.
5. Inspect the complete diff and fetch again before commit or push. If upstream
   moved, integrate it and rerun the affected proof.

## Layout

| Path | Owns |
|---|---|
| `cmd/remount` | the single binary: `server`, `up`, `standalone`, and the client CLI |
| `internal/proto` | the one frame type, every request and response body, op names, event names, error codes |
| `internal/transport` | `Conn` and `Peer`: WebSocket, in-memory pipe with fault injection, request correlation |
| `internal/session` | the sequenced output log with spill, cursors, exec/pty/port runners, the session manager |
| `internal/fsops` | jailed filesystem operations, server-side search, atomic multi-edit |
| `internal/workspace` | the `Backend` interface, `process` and `docker` backends |
| `internal/artifact` | content-addressed blob store, deterministic tar.gz snapshots and restore |
| `internal/eventlog` | the canonical log, memory and SQLite stores, subscriptions with backfill |
| `internal/broker` | the egress credential broker: substitution, leak blocking, allow lists, audit |
| `internal/relay` | frame routing by destination id; authenticates hellos; interprets nothing else |
| `internal/control` | claim queue, leases, generations, timers, bindings, grants, re-adoption |
| `internal/node` | the supervisor: uplink, claims, materialize, sessions, snapshots, `.remount/env` |
| `internal/client` | the Go SDK with reconnect, reattach, orphan buffering, idempotent calls |
| `internal/server` | HTTP surface: `/v1/link`, `/v1/artifacts/{id}`, `/v1/events`, `/healthz` |
| `internal/metrics` | dependency-free counters and gauges rendered as Prometheus text |
| `internal/sim` | the whole system in one process with fault injection; the failure model lives here |
| `internal/ids` | prefixed, time-sortable ids |
| `spec/PROTOCOL.md` | the normative wire protocol |
| `docs/` | design, tutorial, operations, harness integration, ADRs |
| `.agents/skills/` | repo-scoped Codex workflows; keep the detailed engineering truth in `docs/` |
| `.claude/skills/` | repo-scoped Claude development and operating workflows |
| `examples/` | a minimal real agent loop against the SDK |
| `deploy/` | the Modal deployment that was actually run |

## Invariants

Check your own change against every line here before calling it done.

| Invariant | Where it is enforced |
|---|---|
| The workspace is trusted with nothing. Secrets, policy and lifecycle live in the node or control plane. | `broker`, `node`, ADR 10 |
| A state change that emits no event is a bug, and a control-plane resource row and its events commit in one SQLite transaction. | `control.transact` + `eventlog.Transact` (ADR 0050), `node.emit`; tests assert on events |
| Seq 0 of every session is the `info` chunk and the `exit` chunk is last. | `session.Manager.Open`, `session.finish` |
| A replay gap is reported with a `gap` chunk, never silent, never fatal to the session. | `node.subscribe` |
| Input is deduplicated by `iseq`; a retried keystroke is never applied twice. | `session.Input` |
| A grant is bound to a workspace generation and refused after a move. | `node.authorize`, `control.VerifyGrant` |
| A placeholder sent to a host its binding does not cover is blocked and recorded as `leak_blocked`. | `broker.proxy` |
| Approve-mode egress releases no upstream byte before a durable, generation-bound request fingerprint is allowed; timeout remains a retryable durable approval. | `broker.awaitApproval`, `control.egressApproval` |
| A governed request reserves every matching hard budget before secret substitution or upstream I/O; settlement, node loss, and TTL expiry remain durably charged and observable. | `broker.reserveBudget`, `control.budgetReserve`, `control.expireBudgets` (ADR 0067) |
| `ws.ready` gates `claimed`; a client never talks to a node that is still restoring. | `control.wsClaim`, `control.wsReady`, `node.materialize` |
| A node renews its leases at no more than one third of the lease interval, including while materializing. | `node.renewLoop`, `node.renew` |
| Every mutating request carries an idempotency key and a replay is a no-op. | `client`, `control.wsCreate`, `session.Manager.Open` |
| `.remount/env` is rewritten on every materialize and never travels in a snapshot. | `node.writeWorkspaceEnv`, `node.snapshot` |
| Anything returned from under `control.mu` is a copy, never a live pointer. | `control.snapshotWS` and every `cp := *ws` |
| Every workspace state change passes through the central transition table and validates actor, predecessor and authority. | `control/state_machine.go` |
| A destructive lifecycle operation retains and fences the source until the checkpoint and next state are durably committed. | release/quarantine prepare and commit handlers in `control` and `node` |
| Authorization is revalidated after acquiring the workspace tree boundary. A queued stale operation never touches the handle. | `node.lockWorkspaceTree` |
| Cancellation is not completion. Snapshot producers and managed sessions are joined before locks, capacity or success are released. | `node.snapshotRaw`, `session.Manager.KillWorkspace` |
| Session output loss is an explicit `gap` and public helpers return `evicted`; incomplete output is never reported complete. | `session.Log`, `client.Copy`, `client.Run` |
| A session-log range moves from disk to blob only after its immutable artifact and contiguous reference commit; retention dereferences before releasing capacity, and replay is byte-identical or an explicit tier-named gap. | `session.Log` tier transitions (ADR 0073) |
| Every retained collection and staging path has admission, accounting, cleanup and an observable rejection or degradation signal. | resource options, GC loops, diagnostics and quota metrics |
| Shared volumes are immutable artifact versions: mounts are probed read-only, pinned by workspace generation, excluded from workspace snapshots, and materialized before `ws.ready`. | `control/volumes.go`, `node.attachWorkspaceVolumes`, `volume.LocalBackend` |
| `Session.Wait` is the active-capacity handoff: accounting is committed before exit becomes observable. | `session.finish`, `session.Manager.markInactive` |
| A check that cannot run is unavailable, never healthy. | `doctor`, `node.diag_unavailable`, `scripts/explain.py` |
| Notification delivery is at least once: the tenant cursor advances only after destination acceptance or a durable sanitized dead letter plus signal. | `notifier.Runner`, `server.configureNotifiers` |
| An export cursor advances only after its destination accepts the bounded batch; retries may duplicate but never skip events. | `eventlog.RunExport`, `control.ExportCursors` |
| Identity comes only from a live, unrevoked signed credential; one-time node enrollment is digest-only and atomically consumed once. Tenant isolation precedes role checks. | `identity.Manager`, the configured identity `Store` |
| Artifact ids are plaintext digests, but encrypted physical objects are tenant/key-version scoped. Publication is atomic, decrypt verifies authentication and plaintext digest, and retired keys remain until verified migration. | `artifact/encrypted` (ADR 0065) |
| Enforced-gateway networking is deny-first: the sandbox starts with its veth down, the broker-only rule commits before link activation, and any setup error or revoke synchronously deletes the veth before serviceability changes. | `netns`, `workspace/gvisor`, `node.materialize` (ADR 0061) |
| Vendor drivers provision one whole node for one tenant per machine; vendor network controls never upgrade backend capabilities. | `provision` drivers + `pool.Spec` tenant boundary (ADR 0062) |
| Pool scale-down destroys only provider inventory whose `remount.node` identity exactly matches an online, assignment-free control node; provider calls never hold the control mutex or lease loop. | `pool.Reconciler`, `control.reconcilePoolsAsync` (ADR 0064) |

## Where a change goes

| I want to | Touch |
|---|---|
| add a node operation a client can call | op constant and body types in `internal/proto/types.go`; dispatch case in `internal/node/node.go`; SDK method in `internal/client/client.go`; row in `spec/PROTOCOL.md` §7; a test in `internal/sim/sim_test.go` |
| add a control-plane operation | same, but dispatch in `internal/control/control.go` and §6 of the spec |
| add an event type | constant in `internal/proto/types.go`, emit at the state change, list it in `spec/PROTOCOL.md` §11 |
| add a workspace backend | implement `workspace.Backend` in `internal/workspace`, register it in `buildNode` in `cmd/remount/main.go`, advertise honest `Caps` |
| change the wire format | `internal/proto/proto.go`; additive fields only, or bump `proto.Version` |
| change what the broker allows | `internal/broker/broker.go`; add a case to `broker_test.go` first |
| change lease or claim semantics | `internal/control/control.go` and a sim test that cuts a node with `w.cut` |
| add a CLI subcommand | `cmd/remount/main.go`; parse flags with `parse(fs, args)`, never `fs.Parse` |
| add a metric | a named var in `internal/metrics/metrics.go`, then increment it at the site |
| record a design decision | a new file in `docs/adr/`, never an edit to an existing one |

## Testing philosophy

`internal/sim` is the important package. It builds a server, several nodes and
clients in one process, connected through `transport.Pipe` with a per-connection
hook that can drop or sever frames. Every row of the failure model in
`docs/design.md` has a test there.

```go
w := newWorld(t)                   // server + relay + artifact store
n := w.node("n1", labels)          // a node dialing in through a pipe
c := w.client("c1")                // a client
ws := mustWS(t, c, spec)           // create and wait for claimed
w.cut("n1")                        // sever every connection that peer holds
```

`w.cut` is the fault injector. Cut a client mid-stream and assert the output is
byte-identical. Cut a node permanently and assert another node claims from the
last snapshot. Cut a node briefly and assert the session kept running.

Unit packages test their own contract: the session log's eviction and spill, the
broker's decisions against a TLS test server, the jail against symlink escapes.
The sim tests check that the contracts compose.

Keep test contexts generous. The `go test -timeout` is the real bound, and the
race detector makes everything several times slower.

Match the proof to the boundary. Protocol changes also need negotiation,
compatibility and golden-fixture coverage. Public SDK changes need
`make public-api`, which compiles from outside the module. Capacity changes
need concurrent overcommit and idempotent-at-capacity tests. Deployment changes
need the exact candidate exercised against the named service, with readiness,
restart when relevant, observability capture and verified cleanup. The full
matrix is in `docs/engineering/hardening-lessons.md`.

## Deploying to a cloud sandbox

`deploy/modal_app.py` is the deployment that was actually run. These rules cost
real time to learn and apply to any similar platform.

- **Set the platform's environment in your shell, not per command.**
  `MODAL_ENVIRONMENT=dev` once. A flag remembered on `deploy` and forgotten on
  `run` sends the second command to a different environment, and the error
  names that environment rather than the missing flag.
- **Never let a container read a credential from a module-level env default.**
  That line runs inside the container, where your environment does not exist,
  so it takes the default. A default credential on a public tunnel is a real
  credential. Pass secrets as explicit run arguments and fail when absent.
- **Stop a pinned singleton before redeploying it.** The control plane must be
  exactly one container, so a rolling deploy has no free slot and waits
  forever. The platform reports that as a capacity problem.
- **Exclude reproducible directories from snapshots.** `--exclude node_modules`
  turns a 38 second move back into a one second move.
- **A harness discovers the broker at run time.** Source `.remount/env` in the
  launcher. The broker's address is different on every node and after every
  move.


## Lock discipline

The control plane guards its workspace map with one mutex. The failure mode
that actually happened is a read of guarded state on the line after the
unlock, usually while formatting an error:

```go
if ws.State != proto.WSPending {
    c.mu.Unlock()
    return nil, proto.Err(..., ws.State)   // race: read after unlock
}
```

Hoist the value first. `make lint` runs `scripts/lint-locks.sh`, which walks
forward from every unlock and flags a guarded read before the next return or
brace. Suppress a genuine false positive with a `lint:locks-ok` comment and a
reason on the line above.

Two further rules from the same family:

- Never return a pointer into control-plane state. The caller serializes it
  without the lock. Return a copy.
- Lifecycle work triggered by a request that returns immediately needs
  `context.WithoutCancel`, or it is cancelled the moment the handler returns.

## Observability

Three depths, all with `--json`:

| Command | Use |
|---|---|
| `remount status` | is anything wrong right now |
| `remount inspect WS` | one workspace, down to session log positions and bytes on disk |
| `remount doctor --deep` | re-hash every artifact and report damage or disagreement |
| `remount metrics` | raw counters |

`scripts/collect.sh` gathers all of it into one JSON document and
`scripts/explain.py` turns that into prose that leads with the verdict. Use
those two when debugging a deployment rather than issuing a dozen commands.

A check that cannot run must never render as a pass. `doctor` emits
`node.diag_unavailable` and names what went unchecked, because an unearned
"healthy" is the most dangerous output a diagnostic can produce.

## Gotchas that have already cost time

- **Go's `flag` stops at the first positional.** `remount ws move WS --node X`
  silently ignored `--node`. Every subcommand parses through `parse(fs, args)`,
  which permutes flags ahead of positionals. Do not call `fs.Parse` directly.
- **`grep ... | head` always exits 0.** A leak scan written that way reports
  "leak found" for no matches and "clean" for nothing. Capture grep's own count
  and prove the scan works by planting a canary the same scan must find.
- **Never return a live pointer from under `control.mu`.** The caller
  serializes it without the lock. The race detector found this twice.
- **Use `context.WithoutCancel` for lifecycle work started by a request that
  returns immediately.** The webhook handler passed `r.Context()` into the
  control plane and the event append was cancelled the instant it returned 202.
- **The node starts streaming before the open response arrives.** The client
  buffers chunks for session ids it has not registered yet. If you add a new
  session-opening call, register through `client.register`.
- **Two offers for one workspace reach the same node.** Reserve the id in
  `n.materializing` before calling `ws.claim`, or the second claim looks like a
  re-adoption and two materializations fight over one directory.
- **The broker's address changes on every materialize.** Read
  `.remount/env` at start-up. Do not bake `REMOUNT_BROKER` into a file that
  travels in a snapshot.
- **A hosted control plane must be exactly one process.** A serverless
  endpoint that scales out is N control planes with N databases.
- **Assert the postcondition after a scripted text edit.** `gofmt` realigns
  const blocks, so an exact-match patch written against the old alignment
  matches nothing and still reports success. Check that the change is present,
  not that the command exited zero.

## Style

- Comments say why, not what. The what is the code.
- Every exported identifier has a doc comment.
- Errors that cross the wire carry a stable `proto` code. Use `proto.Err(code,
  format, ...)`; callers match on `Code`, never on message text.
- One idea per function. `dispatch` is a switch that calls named methods.
- No new dependencies without a reason written in the pull request. The binary
  is static and 13 MB; keep it that way.
- Prefer a sim test to a mock. If the behavior involves two peers, it belongs in
  `internal/sim`.
