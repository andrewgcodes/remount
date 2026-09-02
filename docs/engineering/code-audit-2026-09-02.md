# Remount code audit — 2026-09-02

**Status:** open findings  
**Scope:** all Go packages, CLI, simulation tests, CI, Makefile and Modal deployment  
**Audit type:** source review plus local build, test, race and cross-compile evidence  
**Target audience:** senior engineer / technical lead  

## 1. Executive conclusion

The architecture is coherent, but the current tree is not release-ready. The
normal test suite is red, a targeted race run reproduces a production
control-plane race, and several high-confidence paths can cause split brain,
data loss, credential-policy bypass or host access.

The most urgent cluster is not cosmetic:

1. fix the relay and control-plane races;
2. make leases locally self-fencing;
3. permit only `claiming -> claimed`;
4. serialize lifecycle operations;
5. make checkpoint failure preserve the source;
6. repair control-plane restart reconciliation;
7. harden snapshot extraction and `port.open`;
8. make identity and authorization server-authoritative; and
9. restore the normal and race suites to green.

This is a point-in-time audit. The tree was being actively changed during
review, so each fix should re-check the current symbol rather than relying only
on line numbers.

## 2. Severity

- **P0:** blocks a release or can cause process crash, split brain, silent data
  loss, host compromise or collapse of a claimed security boundary.
- **P1:** high-impact correctness, reliability, security or operability issue
  that must be resolved before production.
- **P2:** real defect, incomplete contract or misleading operational surface
  that should be scheduled after the P0/P1 gate.

## 3. Commands run

### Failing

```text
go test -count=1 -timeout 600s ./...
```

Fails in `internal/sim.TestExecEndToEnd`: the test expects only `src` at the
workspace root, but materialization now also creates `.remount`.

```text
go test -race -count=1 -timeout 1200s ./...
```

Fails on the same stale assertion.

A second race run excluding that test found a real race:

```text
go test -race -count=1 -timeout 1200s \
  -run '^(TestPTYAndStdin|TestReconnectMidStreamIsLossless|TestNodeUplinkFlapKeepsSessionRunning|TestNodeDeathMovesWorkspaceFromSnapshot|TestSleepAndWake|TestWorkspaceEnvFileIsRefreshedAndNotSnapshotted|TestSecretBlindWorkspace|TestGrantsAreEnforced|TestPortForward|TestPlacementWaitsForEligibleNode|TestIdempotentExec|TestNodeRestartAdoptsLocalWorkspaces|TestDockerWorkspaceIfAvailable)$' \
  ./internal/sim
```

The detector reports `Control.wsReady` writing `ws.State` concurrently with
`Control.wsClaim` reading it after unlock.

```text
CGO_ENABLED=0 GOOS=windows GOARCH=amd64 go build ./...
```

Fails in `internal/session/session.go` on `Setpgid`, `syscall.Kill`,
`SIGUSR1` and `SIGUSR2`.

```text
make conformance
```

Returns success with `Nothing to be done for 'conformance'.`

### Passing

```text
go vet ./...
gofmt -l .
```

`go vet` succeeds and `gofmt -l` produces no output.

## 4. P0 findings

### RM-001 — CI and both required test gates are red

**Evidence:** `internal/sim/sim_test.go:198-200` asserts that root listing has
one entry named `src`. `internal/node/node.go:429-454` now creates
`.remount/env`, so the actual listing contains `.remount` and `src`.

**Impact:** `make`, `make test`, `make race` and both CI test jobs fail. More
importantly, the stale assertion masks later race output in a normal full-suite
run.

**Required fix:** make an explicit product decision:

- expose `.remount` and update the test; or
- treat `.remount` as node metadata and hide it consistently from agent-facing
  list/search operations.

The second choice must still permit harnesses to source `.remount/env`, so a
blanket denial is not automatically correct.

**Regression test:** update `TestExecEndToEnd`, then require both full commands
above to pass.

### RM-002 — relay mutates maps under `RLock` and can panic after disconnect

**Evidence:** `internal/relay/relay.go`, `Relay.route`, currently initializes
`r.recent[f.From]` and `r.recent[f.To]` while holding `r.mu.RLock`. It then
drops that lock and later writes both inner maps under `Lock`.

**Trigger:**

1. multiple peers route frames concurrently; or
2. `Relay.remove` deletes a `recent` entry between the read-lock section and
   write-lock section.

**Impact:** race detector failure, fatal `concurrent map writes`, or
`assignment to entry in nil map`. This crashes the single server process.

**Required fix:** perform destination lookup and correspondence-map
initialization/update in one correctly scoped write lock, or split
correspondence tracking behind its own mutex. Do not retain a logically atomic
operation across an unlock/relock window.

**Regression test:** add direct relay stress tests that route while peers are
replaced and removed under `go test -race`.

### RM-003 — `wsClaim` reads live state after unlocking

**Evidence:** `internal/control/control.go`, `Control.wsClaim`, unlocks in the
non-pending conflict branch and then formats the error using `ws.State`.
`Control.wsReady` writes the same field under the mutex.

**Observed failure:** `TestNodeDeathMovesWorkspaceFromSnapshot` reproduces this
under the race detector, with the read in `wsClaim` and write in `wsReady`.

**Impact:** undefined concurrent access and a permanently red race suite. The
specific value only affects an error message, but the pattern indicates unsafe
ownership of control-plane state.

**Required fix:** copy `state := ws.State` before unlock. Audit every control
path for reads from `ws`, `timer` or `nodeState` after releasing `c.mu`.

**Regression test:** retain the targeted race test and add a direct
`wsClaim`/`wsReady` concurrency test.

### RM-004 — lease expiry does not fence the old node

**Evidence:**

- `Control.Tick` changes an expired workspace to `pending`, clears `Node` and
  offers it again.
- `Control.wsRenew` silently ignores unknown, stale or no-longer-held
  workspaces, but returns a globally successful response.
- `Node.renew` receives no per-workspace accept/reject result.
- `Node.authorize` checks the generation stored in its local `ws`, not the
  control plane's current assignment.
- grants are valid for one hour, much longer than the default lease.

**Trigger:** N1 loses renewal authority without losing its process or local
client reachability. Control expires and reassigns the workspace to N2. N1
continues running and accepts a cached generation-G grant while N2 serves
generation G+1.

**Impact:** two serviceable copies, divergent files, continued egress and stale
client access. This violates the central generation/fencing invariant.

**Required fix:**

- return an affirmative result for every renewed workspace;
- maintain a monotonic local lease deadline;
- block new operations, revoke egress and stop/suspend processes when authority
  cannot be renewed;
- make stale/rejected renewal explicitly fence local state; and
- bound grant lifetime to useful assignment authority.

**Regression test:** keep N1 connected but force its renewal to be rejected,
advance fake time, let N2 claim, then prove all operation classes fail on N1.

### RM-005 — readiness accepts illegal states and failure leaves a ghost copy

There are two coupled defects.

**Control defect:** `Control.wsReady` validates node and generation, but then
promotes every state other than `claimed` to `claimed`. A late ready can
therefore perform `released -> claimed`.

**Node defect:** `Node.materialize` inserts the workspace into
`n.workspaces`, calls `ws.ready`, logs any error, and returns `nil`. A rejected
ready leaves a serviceable local handle and broker.

**Impact:** release/move can be undone by a late ready; a stale node can keep
serving after control rejects its claim; clients can observe a workspace as
claimed during a destructive transition.

**Required fix:** only permit:

```text
claiming(node, generation, claim-operation) -> claimed
```

On conflict/not-found from `ws.ready`, the node must fence and drop the local
copy. Other transient failures must leave it non-serviceable until
reconciliation succeeds.

**Regression tests:**

- ready against `released`, `paused`, `pending` and `destroyed` is rejected;
- a delayed materialization that loses authority destroys/fences its handle;
- duplicate ready for the already-claimed same operation remains idempotent.

### RM-006 — lifecycle operations are not serialized

**Evidence:** `Control.release` persists `released`, unlocks for an RPC of up to
ten minutes, and later continues through the original `*Workspace`. Move,
sleep, wake and destroy have no operation ID or per-workspace lock/state token.

**Concrete interleavings:**

- move enters `release`; destroy sets `destroyed`; move resumes and writes
  `pending`, resurrecting the destroyed workspace;
- two moves race; the second sees `released` as not held, immediately requeues
  from the old snapshot while the first node is still checkpointing;
- sleep and manual wake can interleave around `released`/`paused`;
- move and lease expiry can each offer a new generation.

**Impact:** resurrection, duplicate claims, stale restoration, split brain and
misleading operation success.

**Required fix:** create a durable per-workspace operation record with ID,
expected generation, legal predecessor state and terminal result. Every state
transition must compare the active operation. Conflicting operations return
`conflict`; retries with the same key return the first result.

**Regression test:** property-test reordered and concurrent move, sleep, wake,
destroy, ready, released and lease-expiry transitions.

### RM-007 — failed checkpoint still destroys the source

**Evidence:**

- `Node.release` removes the workspace from the serving map and kills sessions.
- If `n.snapshot(..., true)` fails, it only logs
  `snapshot on release failed`.
- It closes the broker and calls `handle.Destroy` anyway.
- The response contains no snapshot.
- `Control.release` treats an unreachable/rejected release as success and
  returns the previous `LastSnapshot`.

**Impact:** move or sleep can report success while losing all changes since the
older snapshot. A temporary upload, disk or network error becomes permanent
data loss.

**Required fix:** use a two-phase release:

1. quiesce;
2. create and verify checkpoint;
3. commit checkpoint at control;
4. advance the fence; and
5. authorize source destruction.

On failure, preserve the source in a non-serving quarantined state and return a
failed operation.

**Regression test:** inject failure at archive, local store, upload, digest,
control commit and destroy. The latest acknowledged state must always remain
recoverable.

### RM-008 — control-plane restart discards the node's newer local state

**Evidence:**

- `Control.load` converts every `claiming`/`claimed` workspace to `pending`,
  clears its node and points restore at `LastSnapshot`.
- After reconnect, `Node.helloAndServe` calls `resync` before `reclaimLocal`.
- `resync` sends `ws.ready`, which conflicts because control now has no node.
- On that conflict, `Node.resync` calls `dropWorkspace`, which kills sessions
  and destroys the local filesystem.
- `reclaimLocal` then has nothing left to adopt.

**Impact:** restarting only the control plane can destroy unsnapshotted changes
on a healthy node and recreate the workspace from an older artifact. This
contradicts the load comment claiming the node will reclaim and adopt its local
copy.

**Required fix:** implement an explicit restart-reconciliation handshake. The
node reports local workspace ID, generation and checkpoint lineage; control
decides adopt/fence without requiring the node to destroy first. Preserve the
local copy until authority and durability are resolved.

**Regression test:** write unsnapshotted data, restart only control with the
same database, reconnect the same node, and prove the data remains.

### RM-009 — explicit snapshot does not update control-plane durability state

**Evidence:** `Client.Snapshot` calls `OpWSSnapshot` directly on the node.
`Node.snapshot` uploads and emits `ws.snapshot`, but there is no control
operation that commits the artifact to `Workspace.LastSnapshot`.

**Impact:** `remount ws snapshot` prints a valid artifact, but lease failover,
move and control restart may still restore an older `LastSnapshot` or nothing.
The user's explicit checkpoint is not the checkpoint the lifecycle trusts.

**Required fix:** add an acknowledged control-plane snapshot-commit operation
bound to workspace and generation. Update `LastSnapshot` only after upload and
digest verification succeed.

**Regression test:** explicit snapshot, write newer uncheckpointed data, kill
the node, and prove recovery uses the explicit snapshot.

### RM-010 — persistence failures are logged but successful state is returned

**Evidence:** `Control.saveWS`, `saveTimer` and `saveNode` return no error. They
log SQLite failures while callers continue mutating memory, emitting events and
returning success. Several raw `db.Exec` calls also discard errors.

**Impact:** disk full, closed database or I/O error can produce an acknowledged
workspace/timer/assignment that disappears on restart. A timer described as
durable may never have reached disk.

**Required fix:** make persistence return errors and make each state transition
transactional. Do not mutate the authoritative in-memory state or return
success until the durable write commits. Couple state and audit publication
through a transaction/outbox.

**Regression test:** inject SQLite write failure for every mutation and assert
the operation fails without changing observable state.

### RM-011 — crafted snapshots can write or later access outside the root

**Evidence:** `artifact.Restore` validates `hdr.Name`, but
`tar.TypeSymlink` passes unvalidated `hdr.Linkname` to `os.Symlink`. Extraction
then uses path-based `MkdirAll` and `OpenFile`.

**Concrete exploit shapes:**

- `link -> /etc` followed by `link/target`;
- `link -> ../../../node-data` followed by a regular entry below `link`;
- symlink entry `x -> /host/file` restored and later opened by an exec/PTY
  session.

The `fsops.Resolve` static check does not protect shell processes, and a
symlink parent can also be followed during extraction.

**Impact:** host file read/write, sibling workspace access or node key/artifact
access when an untrusted artifact is restored.

**Required fix:** extract into a fresh staging root using dirfd-relative
operations with no-follow semantics. Reject absolute/escaping link targets,
hard links, devices and link-parent traversal. Atomically install the validated
tree.

**Regression test:** hostile archives covering absolute links, relative links,
symlink-parent writes, duplicate symlink/regular names and concurrent swaps.

### RM-012 — `port.open` is a broker-bypassing SSRF primitive

**Evidence:** `PortOpenReq` permits a caller-selected `Host`.
`Node.portOpen` passes it through the backend, and the process backend leaves it
unchanged. `Session.startPort` performs a direct `net.Dial` from the trusted
node host.

**Impact:** an authorized workspace client can connect to:

- node loopback services;
- cloud metadata endpoints;
- private VPC services;
- databases and admin ports; or
- any route available to the node but forbidden by the broker.

The public Go helper defaults to localhost, but a custom protocol client can set
`Host`, so the wire-level boundary is exploitable.

**Required fix:** make port destinations capability-scoped and pass them
through the same default-deny IP/DNS policy as egress. Distinguish “workspace
localhost” from “node localhost.”

**Regression test:** deny loopback, metadata, RFC1918, IPv6-local and DNS
rebind targets unless an explicit port capability permits them.

### RM-013 — client-selected identity bypasses binding policy and there are no workspace ACLs

**Evidence:**

- `Hello.Principal` is accepted and returned by `principalOf`.
- `wsCreate` only fills `WorkspaceSpec.Principal` when the client left it empty;
  a non-empty client value is trusted.
- binding ACL checks use `ws.Spec.Principal`.
- `wsGet`, `wsList`, grant, destroy, move, sleep, wake, events and timers have
  no owner/tenant ACL.

**Impact:** with a shared token, a client can claim another principal, obtain a
binding intended for that principal, enumerate workspaces, get a grant, execute
inside them, read files or destroy/move them.

**Required fix:** authentication must produce a server-authoritative subject
and tenant. Ignore client authority fields. Apply authorization to every
resource and operation. Keep display/audit labels separate from authorization
identity.

**Regression test:** two subjects sharing a control endpoint cannot see or
operate on each other's workspaces or bindings.

### RM-014 — peer IDs are replayable and node keys prove no possession

**Evidence:**

- a client may request any `c_...` ID;
- relay replacement closes the prior connection for the same ID;
- node authentication compares the supplied public-key bytes with stored bytes,
  but there is no signed challenge;
- one shared bearer token authenticates users, nodes, artifact traffic and
  webhooks.

**Impact:**

- a token holder can reconnect as a known client ID, disconnect the victim and
  receive frames/grants addressed to that ID;
- anyone who knows a registered node's public key can replay it without the
  private key;
- any token holder can enroll a fresh node and receive workloads/secrets allowed
  by placement.

**Required fix:** server-assign client IDs and issue signed reconnect tickets.
Require node proof-of-possession over a fresh server challenge. Separate
credential classes and authorize node enrollment.

**Regression test:** replayed public key without a signature and duplicate
client ID without a reconnect ticket are rejected without replacing the
legitimate peer.

### RM-015 — the broker can substitute a real secret into plaintext HTTP

**Evidence:** `Broker.ServeHTTP` supports `/http/<host>/...` and absolute HTTP
forward-proxy requests. `Broker.proxy` substitutes placeholders before creating
the target URL and does not require `scheme == "https"`.

**Trigger:** send the placeholder for a binding to
`http://<allowed-binding-host>/...`.

**Impact:** the broker places the real credential in a plaintext request. Even
if the destination later redirects to HTTPS, the initial header has already
crossed the network unencrypted.

**Required fix:** never substitute credentials over plaintext transport.
Bindings should declare allowed schemes and ports; credential-bearing
capabilities should default to verified TLS only. Local HTTP exceptions must be
explicit and separately risk-labelled.

**Regression test:** `/http/` and absolute `http://` requests containing a
placeholder are denied before dialing.

### RM-016 — documented production isolation is not enforced

**Evidence:**

- process sessions are ordinary processes under the node's OS user and can walk
  outside the workspace directory;
- Docker runs as root with a writable bind mount and default outbound network;
- both backends report `EgressEnforced: false`;
- `HTTP_PROXY`/`HTTPS_PROXY` are environment hints a process can ignore;
- backend security capabilities are not used by placement.

**Impact:** on the process backend, hostile code can read node data, sibling
workspaces and local services permitted to the same user. In Docker, direct
egress bypasses the broker. The README's “compromised box has nothing to leak”
claim is false for these backends.

**Required fix:** restrict process to explicit trusted-local mode. Add a
production backend with an independent filesystem/process/network boundary and
enforced egress. Make workspace security policy reject a backend that cannot
satisfy it.

**Regression test:** hostile-workspace conformance suite from inside each
production backend.

## 5. P1 findings

### RM-017 — sleep/wake timing is not atomic

**Evidence:** `wsSleep` persists and exposes the timer before asking the node to
release. `fireTimer` marks the timer fired before calling `wsWake`.

**Failure modes:**

- a short timer fires while the workspace is still held or `released`;
  `wsWake` no-ops because it is not yet `paused`; sleep later writes `paused`,
  leaving a permanently fired timer;
- control crashes after `Fired=true` but before the paused workspace becomes
  pending;
- negative `AfterSec` bypasses the all-zero validation, creates no deadline and
  pauses forever;
- an `AtMillis` in the past races release in the same way.

**Required fix:** commit paused state and an armed timer atomically, or model
`sleeping/checkpointing` as a durable operation. Validate positive duration and
future timestamp. Mark a timer complete only with the wake transition or a
retryable delivery state.

**Regression test:** slow snapshot with one-second timer, crash at each commit
boundary, negative values and past timestamps.

### RM-018 — `released` workspaces are not reconciled after restart

**Evidence:** `Control.release` persists `WSReleased` before the node RPC.
`Control.load` only resets states for which `held(state)` is true; `released`
is excluded.

**Impact:** a crash between persisting `released` and the enclosing move/sleep
transition leaves a workspace permanently unoffered and unreachable after
restart.

**Required fix:** persist the lifecycle operation and resume it, or map orphaned
`released` state to a safe, explicit recovery state.

**Regression test:** restart control at each instruction boundary of move and
sleep.

### RM-019 — session `StreamInfo` is not guaranteed to be sequence zero

**Evidence:** `Manager.Open` starts exec/PTY/port first. Their output pumps start
immediately. Only afterward does `Manager.Open` append `StreamInfo`, while the
comment declares it “always seq 0.”

**Impact:** a fast process can write stdout/stderr at sequence zero and move the
session header later. Replay and attach clients cannot rely on the documented
header invariant.

**Required fix:** reserve/append the header before output pumps become runnable,
or gate pumps until the PID-enriched header commits.

**Regression test:** deterministic barrier that lets a process write before
`Open` returns, repeated under `-race`.

### RM-020 — session spill and pump failures silently lose output

**Evidence:**

- `Log.evictLocked` removes a chunk from memory before spill success is known;
- spill truncate, seek and `WriteAt` failures are ignored;
- `pump` returns immediately on `Log.Append` failure;
- `Session.finish` ignores failure to append the exit record.

**Impact:** unmarked sequence holes, truncated output, or a client-side stream
that never receives its exit chunk. This contradicts the explicit “gap, never
silent” invariant.

**Required fix:** spill must return an error before eviction commits. Preserve
the memory chunk, emit a durable gap/error, or terminate the session visibly.
An exit record append failure must reach subscribers.

**Regression test:** inject disk-full, short write, truncate/seek failure,
closed log and quota failure.

### RM-021 — blocking stdin holds the session lifecycle mutex

**Evidence:** `Session.Input` holds `s.mu` while calling `s.stdin.Write`.
`Signal`, timeout kill and parts of finish also need `s.mu`.

**Trigger:** write more than the pipe/socket buffer to a process that does not
read stdin.

**Impact:** the input RPC blocks without respecting its request context; timeout
and release cannot acquire the mutex to kill the process; workspace teardown
can hang.

**Required fix:** separate a write mutex from lifecycle state. Do not hold the
lifecycle mutex across blocking I/O. Add context/deadline-aware input and close
the writer during cancellation.

**Regression test:** child never reads stdin, client sends a large frame, then
timeout/release must still terminate promptly.

### RM-022 — event streaming can stall or panic and supports only one tail

**Evidence:**

- `Client.handle` sends to the 256-entry event channel on the peer read-loop
  goroutine;
- the handler context is `context.Background`, so a full channel cannot cancel;
- cancellation reads the channel pointer under lock, later closes it outside
  that lock, while the handler may already hold the old pointer;
- each `TailEvents` overwrites the single `c.events` field;
- cancellation of one tail clears/stops whichever tail is current;
- setup failure leaves `c.events` installed;
- `events.stop` uses an unbounded background context before closing the channel.

**Impact:** one slow event consumer can freeze every RPC and session on that
client; cancellation can produce `send on closed channel`; two tails interfere;
the returned channel may never close.

**Required fix:** model each tail as an owned subscription keyed by ID. Give one
delivery goroutine ownership of closing its channel. Use bounded queues and an
explicit overflow/gap policy; never block the transport read loop.

**Regression test:** blocked consumer plus concurrent exec, cancellation during
send, two simultaneous filters, failed setup and disconnected stop.

### RM-023 — session output can block the whole client connection

**Evidence:** `Session.deliver` holds `s.mu` while sending into the 1024-entry
`s.out` channel. It is called synchronously from `Peer.readLoop`.

**Additional lifecycle gaps:**

- `Client.Close` closes the peer but leaves live session output channels open;
- `Session.Close` closes `out` but not `exited`, so `Wait` relies on context
  after detach;
- reconnect attach errors are discarded;
- orphan and out-of-order buffers silently stop accepting data at hard-coded
  limits.

**Impact:** a caller that pauses reading output can freeze unrelated RPCs and
events. Close/reconnect can leave consumers hanging or silently stop a stream.

**Required fix:** decouple transport decode from per-session delivery, enforce
byte-aware bounds, emit explicit gaps/errors, and define one owner for stream
completion. Surface failed reattach and retry it.

**Regression test:** stop reading a high-volume session while another RPC
completes; close client with live sessions; reject the first reattach then
recover; exceed orphan bounds and receive a gap.

### RM-024 — `WorkspaceInfo` and `ListSessions` omit the grant they just fetched

**Evidence:**

- `Client.nodeCall` obtains a grant and passes it to a body callback.
- `WorkspaceInfo` ignores the callback grant and sends `WSGetReq`, which has no
  grant field.
- `ListSessions` ignores it and sends `SListReq`, which has no grant field.
- the node calls `authorize(..., nil)` and can only succeed if an earlier,
  unrelated operation cached a grant for that connection.

**Impact:** both methods fail as the first node operation on a fresh client, but
may appear to work after another call. Behavior depends on hidden call order.

**Required fix:** add grant-bearing request bodies or a first-class
connection-level grant-install operation.

**Regression test:** call each method first on a newly connected client.

### RM-025 — most declared idempotency is not implemented

**Evidence:**

- move and sleep carry `IdempotencyKey`, but control never reads it;
- filesystem write and edit carry keys, but node never reads them;
- remove, rename, mkdir, destroy, wake and snapshot lack an effective replay
  record;
- create idempotency is global rather than subject-scoped and stores no request
  fingerprint;
- SDK helpers generate keys internally, preventing recovery after caller
  restart.

**Impact:** ambiguous network failure can duplicate side effects, create
multiple timers, repeat a move, or return an old result for a different request
that reused a key. The protocol comment “every mutating request” is false.

**Required fix:** persist idempotency by authenticated subject, operation and
key, with request fingerprint and terminal result. Let callers supply stable
keys.

**Regression test:** drop every mutation's response after commit, retry, and
prove exactly one side effect.

### RM-026 — Docker workspaces receive an unreachable broker URL

**Evidence:** every broker listens on host `127.0.0.1`. `Docker.Prepare` passes
that URL into `docker exec`, where `127.0.0.1` means the container itself.
The container is created on the default bridge network with no broker sidecar,
host alias or socket mount.

**Impact:** `REMOUNT_BROKER`, `HTTP_PROXY` and `HTTPS_PROXY` do not reach the
broker from a normal Docker workspace. Secret-blind model calls and package
proxying fail. If the application bypasses those variables, Docker has direct
unrestricted egress.

**Required fix:** give each container an authenticated broker endpoint
reachable only from its network namespace, such as a mounted Unix socket,
sidecar or dedicated bridge address. Independently block direct egress.

**Regression test:** perform the verified Codex/provider request from the Docker
backend and attempt direct bypass.

### RM-027 — broker caller identity and CONNECT lease semantics are incomplete

**Evidence:**

- broker HTTP has no caller authentication;
- loopback reachability is treated as identity;
- `handleConnect` permits a destination if any matching lease exists but does
  not check `ExpiresAt`;
- existing CONNECT tunnels are not tied to lease revocation.

**Impact:** another same-host process can exercise a workspace broker if it
discovers the port. An expired binding still authorizes a new encrypted tunnel,
unlike the reverse-proxy path.

**Required fix:** authenticate workspace ID and generation at the broker
boundary, isolate its endpoint, and apply lease validity to all connection
forms.

**Regression test:** cross-workspace broker call, expired CONNECT and revoked
lease.

### RM-028 — filesystem jail is path-check-then-use

**Evidence:** `fsops.Resolve` validates an existing path prefix with
`EvalSymlinks`, returns a string, and each operation later performs a separate
path-based open, rename, remove or mkdir. A workspace process can replace a
component during that window.

**Impact:** symlink-swap races can make node filesystem APIs read or write
outside the jail, especially with a bind-mounted Docker workspace concurrently
controlled by the agent.

**Other concrete filesystem issues:**

- `Edit` reads the entire file despite `Read` having a cap;
- `Search` ignores `Scanner.Err`, so a line over the scanner limit silently
  truncates the search;
- operation byte/work limits are incomplete.

**Required fix:** use dirfd-relative no-follow operations (`openat`/equivalent)
or a platform isolation boundary. Apply consistent edit/search limits and
return scanner errors.

**Regression test:** concurrent symlink swap for every mutation, multi-GB edit
rejection and overlong search line.

### RM-029 — artifacts have no enforced upload or expansion budget

**Evidence:**

- HTTP PUT passes `r.Body` directly to `Store.Put`;
- `Store.Put` uses unbounded `io.Copy`;
- restore has no maximum expanded bytes, file count, path depth or per-file
  size;
- node fetch caches the entire artifact before extraction.

**Impact:** an authenticated peer can fill control/node disk with one upload or
small compressed archive. Millions of entries can exhaust CPU/inodes.

**Required fix:** configurable compressed and expanded size limits, file-count
and path limits, content-length precheck, streaming counters, quotas and 413
responses. Store a verified snapshot manifest.

**Regression test:** one byte over every limit, gzip bomb and excessive entry
count.

### RM-030 — node subscriber cleanup identifies the wrong subscription/workspace

**Evidence:**

- subscriber keys are `client|sessionID`;
- release/drop searches for keys ending in `|workspaceID`, which normally
  matches nothing;
- replacement cancellation installs a new subscriber, but the old goroutine's
  defer tests whether its own context is done and can delete the new map entry
  instead of comparing subscriber identity.

**Impact:** untracked/leaked subscriber goroutines, lost replacement ownership
and cleanup that does not occur when a workspace moves or is destroyed.

**Required fix:** store a subscriber object containing workspace and session;
index by workspace; compare `current == mine` in defer.

**Regression test:** repeated reattach followed by release, with deterministic
subscriber/goroutine counts.

### RM-031 — node events can be lost, reordered and spoofed

**Evidence:**

- each `Node.emit` starts an independent goroutine for `events.post`;
- failures are discarded and events are not replayed after reconnect;
- local sequence is not carried as authoritative producer sequence;
- non-node clients may provide non-empty `Event.Principal` and `Event.Node`;
- event timestamps are accepted when nonzero.

**Impact:** the canonical log can show exit before open, omit security-relevant
events, or contain client-forged actor/time/node metadata. It cannot yet support
strong forensic claims.

**Required fix:** ordered node outbox with event ID and producer sequence,
retry/deduplication, and authenticated origin. Control assigns received time,
actor and node identity.

**Regression test:** disconnect between local append and control ack; reorder
delivery; submit forged metadata as a client.

### RM-032 — server startup, address publication and shutdown are racy/incomplete

**Evidence:**

- `Server.Serve` writes `s.ln` and `s.http` without synchronization;
- `Server.Addr` reads `s.ln` concurrently;
- standalone launches `Serve` in a goroutine, discards its error and polls
  `Addr`;
- `Server.Close` does not shut down the HTTP server/listener;
- relay close does not wait for `Relay.Serve` goroutines before the event log is
  closed;
- observed shutdown logs include `emit type=node.offline err="sql: database is closed"`;
- `Control.Stop` waits only for the tick loop, not request goroutines.

**Impact:** race detector failures, startup continuing after bind failure,
half-closed servers, lost shutdown events and database use after close.

**Required fix:** explicit readiness result channel, synchronized immutable
listener publication, idempotent shutdown ordering and wait groups for accepted
connections/control dispatch.

**Regression test:** bind failure, immediate close, repeated close, shutdown
during move/event append and restart integrity.

### RM-033 — request dispatch and queues are insufficiently bounded

**Evidence:**

- control spawns one goroutine for every request;
- node does the same;
- peer pending requests have no count/byte limit;
- client session pending reorder maps and server connections are unbounded;
- artifact, event and session paths have independent fixed limits but no
  principal/workspace quotas.

**Impact:** an authenticated or compromised peer can exhaust goroutines,
memory, file descriptors, disk or outbound connections and affect unrelated
workspaces.

**Required fix:** bounded worker/queue policy, per-principal and per-workspace
quotas, overload responses, byte-aware stream limits and metrics.

**Regression test:** sustained request/frame flood must produce bounded
resource use and explicit capacity errors.

### RM-034 — materialization can permanently miss required binding leases

**Evidence:** if a workspace declares bindings but `n.peer` becomes nil during
`Node.materialize`, the binding-lease block is skipped rather than failed. The
workspace is installed with an empty lease set. `renew` only refreshes
workspaces that already contain a lease near expiry, so zero leases never
trigger refresh.

**Impact:** after reconnect and successful resync, the workspace can be claimed
but every bound provider request fails indefinitely.

**Required fix:** bindings are a materialization prerequisite. Fail/fence while
control is unavailable, and refresh when the configured binding count differs
from active valid leases.

**Regression test:** cut the node after claim but before binding lease, then
reconnect.

### RM-035 — explicit snapshots are taken from a live mutable tree

**Evidence:** release kills sessions before snapshot, but direct
`OpWSSnapshot` calls `n.snapshot` without quiescing processes. `artifact.Snapshot`
walks and reads ordinary paths while the workspace can mutate them.

**Impact:** an uploaded artifact may combine file versions, omit renamed files
or fail midway. It is not a transactionally consistent checkpoint.

**Required fix:** define snapshot consistency. For durable checkpoints,
quiesce/freeze the backend or use a filesystem snapshot primitive. If live
snapshots remain, label them crash-inconsistent and do not use them for
authoritative failover.

**Regression test:** mutate/rename files continuously during snapshot and
verify the chosen consistency contract.

### RM-036 — client file helpers silently truncate or exceed frame limits

**Evidence:**

- `Client.ReadFile` makes one `FSRead` with default limit and returns only
  `res.Data`, discarding `Size` and `EOF`;
- the default read limit is 4 MiB;
- `WriteFile` and CLI `fs write` send the whole file in one frame, while the
  transport frame limit is 4 MiB;
- CLI reads all stdin into memory before the inevitable oversized-frame error.

**Impact:** “read whole file” silently returns a prefix, while similarly sized
writes fail. Large stdin can waste memory.

**Required fix:** paginate reads by offset, add chunked upload/write, and expose
explicit size limits where streaming is not supported.

**Regression test:** round-trip files below, at and above 4 MiB and 32 MiB.

### RM-037 — placement treats unknown memory as satisfying any requirement

**Evidence:** `eligibleLocked` rejects insufficient memory only when
`MemMiB > 0`. A node reporting zero/unknown is therefore eligible for any
requested memory. `mem_other.go` reports zero on unsupported systems.

**Impact:** a workspace requesting substantial memory can be placed on a node
whose capacity is unknown.

**Required fix:** fail closed for numeric requirements when capacity is unknown.
Separately advertise backend-specific isolation, egress and snapshot
capabilities rather than a flat node list.

**Regression test:** unknown capacity never satisfies a positive requirement.

### RM-038 — historical event reads silently stop at 1,000

**Evidence:** non-follow `eventsTail` calls `log.Read(..., 1000)`.
`Client.ReadEvents` and the CLI expose no pagination token or truncated flag.

**Impact:** operators can believe they read the complete audit history while
silently missing later events.

**Required fix:** cursor pagination or loop until end, plus explicit truncation.

**Regression test:** append more than 1,000 events and retrieve all of them.

### RM-039 — binding configuration accepts missing secrets

**Evidence:** `loadBindings` replaces `$NAME` with `os.Getenv(NAME)` but does
not reject an unset/empty result. Control later leases that empty secret.

**Impact:** the server starts “successfully”; broker substitution removes the
placeholder or sends an empty credential, surfacing as a confusing upstream
failure rather than configuration error.

**Required fix:** validate binding IDs, non-empty secrets, destinations,
placeholder uniqueness and TTL at startup.

**Regression test:** missing environment reference fails startup with the
binding ID and variable name.

## 6. P2 findings

### RM-040 — Windows support is implied but the package does not compile

**Evidence:** a Windows resize file exists, but shared `session.go` uses Unix
`syscall` APIs. The verified Windows build fails. The Makefile quietly excludes
Windows from `PLATFORMS`.

**Required fix:** either document Linux/macOS-only support and remove misleading
stubs, or move process control into build-tagged Unix/Windows files and add CI.

### RM-041 — several close paths do not wake their waiters

**Evidence:**

- `wsConn.Close` constructs a two-second context but never passes it to
  `websocket.Conn.Close`; `_ = ctx` makes the timeout ineffective;
- `Relay.Close` does not fail `Relay.Request` entries, so callers wait for their
  own deadlines;
- `eventlog.Subscription.Close` sets `closed` but does not wake a `Next` blocked
  on `s.ch` with a background context.

**Required fix:** use immediate/timeout-capable transport close and explicit
close signals for every waiter.

### RM-042 — wire version comments and behavior disagree

**Evidence:** `DecodeFrame` says it rejects some newer-version frames, but it
does not inspect `f.V` at all. Hello always returns `v1` without actual
capability intersection.

**Impact:** peers can silently interpret a newer protocol with incompatible
semantics.

**Required fix:** define supported version/capability rules and fail closed when
a required semantic capability is absent.

### RM-043 — `events.stop` is an undocumented string operation

**Evidence:** client and control use literal `"events.stop"`; there is no
protocol constant or normative operation entry.

**Impact:** alternate clients cannot implement event-tail cleanup reliably.

**Required fix:** add a constant, request type, protocol documentation and
interoperability test, preferably with a subscription ID.

### RM-044 — diagnostics and metrics are only partially connected

**Evidence:**

- several counters are incremented in relay, session, broker, artifact and
  eventlog;
- many declared counters (`WSCreated`, claims, lease expiry, move/destroy,
  snapshots, restores, gaps, timers) have no call sites;
- `ControlDiag` checks metrics that can therefore remain zero;
- artifact count/bytes and version fields are not populated;
- `NodeDiag` and `OpNodeDiag` are declared but have no node dispatch
  implementation;
- there is no Prometheus `/metrics` endpoint.

**Impact:** monitoring reports false zeroes and exposes an incomplete
diagnostic contract.

**Required fix:** either complete and test the surface or keep it explicitly
experimental until every value has an owner.

### RM-045 — durable logs and artifacts have no retention/GC policy

**Evidence:** SQLite events grow without bound; artifact blobs have no
reference-aware garbage collector; session timers retain records; temporary
and local stores have no capacity manager.

**Impact:** a healthy long-running system eventually exhausts disk.

**Required fix:** quotas, retention, compaction, reference-aware artifact GC and
alerting.

### RM-046 — checked-in Modal deployment is not reproducible or safe by default

**Evidence:**

- `deploy/modal_app.py` copies `remount-linux-amd64` from repository root;
- the Makefile builds it under `dist/`, and a fresh checkout contains neither;
- the image installs only `curl` and `procps`, while `MISTAKES.md` says the
  verified harness fix installed Node 22;
- the default token is the public literal `modal-demo-token`;
- opened log file descriptors and child processes have no explicit shutdown
  management;
- operations documentation uses a different volume name in its snippet.

**Required fix:** deterministic build/deploy target, required Modal secret,
runtime smoke checks, aligned docs and clean process supervision.

### RM-047 — `make conformance` is a false-success target

**Evidence:** it appears in `.PHONY` but has no recipe. Make exits zero.

**Impact:** automation can claim conformance without running anything.

**Required fix:** implement the hostile-workspace conformance suite or remove
the target until it exists.

### RM-048 — README and architecture claims exceed current guarantees

Examples:

- “compromised box has nothing to leak” conflicts with process-backend host
  access, open Docker egress and unauthenticated local brokers;
- “never losing its place” conflicts with silent spill failure and bounded
  orphan output;
- “log is the source of truth” conflicts with state being authoritative in
  mutable maps/tables and events being asynchronously lossy;
- “working and tested” conflicts with the red suite;
- “any machine” conflicts with the verified Windows build failure.

**Required fix:** label design goals, implemented behavior and tested guarantees
separately.

### RM-049 — critical packages have no direct tests

There are no package-local tests for control, node, relay, server, client or
metrics. Simulation coverage is valuable but does not isolate lock ownership,
shutdown, malformed input or every state transition.

**Required fix:** add direct unit/state-machine tests plus simulation,
property, fuzz, adversarial, restart and stress coverage.

Priority missing tests:

- invalid ready states;
- connected stale node after lease rejection;
- control restart with unsnapshotted local state;
- restart from `released`;
- checkpoint failure;
- client backpressure/close;
- peer identity replay;
- cross-principal ACL;
- broker plaintext and expired CONNECT;
- hostile artifact links/expansion; and
- relay route/remove stress.

### RM-050 — temporary artifact directories and health reporting are incomplete

**Evidence:** `server.New` creates `remount-artifacts-*` when no data directory
is supplied but does not retain/remove the temp root in `Close`. `/healthz`
always reports `ok: true` without checking SQLite, control loop, disk or
artifact store.

**Impact:** test/embedded use leaks directories, and orchestration can route
traffic to a process that cannot persist state.

**Required fix:** own and remove temporary resources; split liveness from
readiness and dependency/security health.

## 7. Findings investigated and not included as bugs

These suspected issues were checked and should not be filed in their previously
reported form:

- **Concurrent `Manager.Open` with one idempotency key:** registration occurs
  while holding `Manager.mu`; the second caller observes the first mapping.
- **`FS.Search` file-descriptor leak from `defer fh.Close()`:** the defer is
  inside the `WalkDir` callback invocation and runs when that callback returns,
  so descriptors do not accumulate across files.
- **Client `Session.Close` racing exit into a double channel close:** both
  `deliver/push` and the close of `out` are serialized by `Session.mu`. Other
  close semantics remain in RM-023.
- **Static path traversal via ordinary `..`:** `fsops.Resolve` blocks it. The
  remaining issue is check/use races.
- **Static broker DNS rebinding after validation:** the broker resolves and
  dials the validated IP literal. Special-address classification and network
  enforcement still deserve hardening.
- **Tar entry-name `..` traversal:** names are checked. Link targets and
  link-parent extraction remain exploitable.
- **Cross-workspace session-open idempotency collision through the node:** node
  prefixes the workspace ID before calling the manager.
- **Relay dropping unmatched transit responses:** current `Peer.readLoop`
  forwards unmatched responses to the relay handler.
- **Node request handling blocking the transport read loop:** node dispatches
  request work in a goroutine. That creates the unbounded-dispatch issue in
  RM-033 instead.

## 8. Required remediation order

### Batch 0 — restore trustworthy feedback

1. RM-001: fix the stale simulation expectation.
2. RM-003: fix the observed control race.
3. RM-002: fix relay locking and add direct stress tests.
4. Run full normal and race suites repeatedly.

### Batch 1 — ownership and data safety

5. RM-004: affirmative renewals and node self-fencing.
6. RM-005: strict ready transition and drop on rejected ready.
7. RM-006: durable lifecycle-operation serialization.
8. RM-007: checkpoint-before-destroy release.
9. RM-008/RM-018: restart reconciliation.
10. RM-009/RM-010: committed snapshots and propagated persistence failure.

### Batch 2 — close security escapes

11. RM-011: safe archive extraction.
12. RM-012: policy-enforced port forwarding.
13. RM-015: TLS-only secret substitution.
14. RM-013/RM-014: authoritative identity, proof-of-possession and ACLs.
15. RM-016/RM-026/RM-027/RM-028: production isolation, egress and broker
    boundary.

### Batch 3 — reliability and resource bounds

16. RM-017: transactional wake.
17. RM-019/RM-020/RM-021: session log and process lifecycle.
18. RM-022/RM-023/RM-030: stream ownership/backpressure/subscribers.
19. RM-025/RM-031/RM-032/RM-033: idempotency, ordered events, shutdown and
    quotas.
20. RM-029/RM-035/RM-036: artifact and file transfer consistency/limits.

### Batch 4 — operations and contract accuracy

21. Complete diagnostics/metrics, retention and conformance.
22. Repair Modal deployment and platform support.
23. Add direct, restart, adversarial, property and fuzz tests.
24. Narrow documentation to verified guarantees.

## 9. Exit criteria for this audit

Do not mark the audit resolved until:

- `go test ./...` and `go test -race ./...` pass repeatedly;
- no serviceable stale generation exists after lease loss;
- a required checkpoint failure cannot destroy the only good copy;
- control restart preserves/reconciles newer node-local state;
- snapshot extraction and port forwarding pass hostile-input tests;
- subjects, principals, peer identities and grants are authoritative;
- every mutating operation has durable idempotency;
- client streams cannot block the transport read loop;
- production backends enforce their advertised isolation and egress;
- persistence and event publication failures reach callers;
- resource and retention limits are enforced; and
- documentation matches the exact tested deployment profile.

