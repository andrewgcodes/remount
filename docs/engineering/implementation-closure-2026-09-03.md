# Remount hardening implementation closure — 2026-09-03

This document records the implementation and verification pass performed after
the 2026-09-02 deep dive, code audit, adversarial review, production-readiness
request, and OpenAI/Hugging Face incident-hardening SDMR. It is the current
status ledger for those point-in-time documents; it does not rewrite their
historical observations.

The reviewed candidate is `main` after `51ffa15` plus the changes delivered
with this report. The upstream base was fetched again immediately before the
final pass. The test evidence below is point-in-time evidence, not a substitute
for continuously enforced CI or an independent production assessment.

## Executive conclusion

All 50 numbered defects in the code audit and all 37 independently confirmed
defects in the adversarial review have an implemented correction, a fail-closed
product-claim narrowing, and regression evidence. The immediate blockers C1-C9
and the implementable architecture requirements R17-R29 in the incident SDMR
are likewise represented in code and tests.

The largest changes are structural rather than cosmetic:

- workspace lifecycle state now changes through one checked transition
  function, with authority, predecessor, node, and generation validation;
- destructive lifecycle operations use commit-gated checkpoint protocols and
  never discard the last known-good materialization after a failed durability
  step;
- a node self-fences when a lease renewal is rejected or cannot be renewed;
- authentication, ACL decisions, grants, node enrollment, and node-originated
  event attribution are server-authoritative;
- filesystem and archive operations use descriptor-rooted traversal and
  bounded, transactional restore;
- egress policy is typed and generation-scoped, secret substitution is HTTPS
  only, and production modes reject backends that cannot provide an enforced
  gateway and the requested isolation;
- session, request, event, artifact, connector, timer, mutation, and workspace
  resources have explicit bounds, retention, diagnostics, and metrics;
- the protocol has strict v1 negotiation and a golden fixture, while a public
  `api` and `client` surface is compile-tested from an external Go module; and
- CI, cross-platform release construction, checksums, SBOM/signing/provenance
  workflow, dependency updates, fuzzing, conformance, static analysis, and
  vulnerability scanning are defined and locally exercised where possible.

This is not a claim that Remount is now universally “10/10 production ready.”
The repository still deliberately has a single SQLite writer, ships no built-in
backend that qualifies for either production security profile, has filesystem
rather than process-memory checkpoints, and has not received the independent
security/architecture sign-off required by the request. Those limits are now
enforced or documented instead of being hidden by broader claims.

## Status vocabulary

- **Verified fixed** means the vulnerable or incorrect behavior was changed and
  focused regression evidence plus the broad test gates passed.
- **Verified fail-closed** means Remount cannot safely provide the requested
  property with a built-in backend, so configuration or placement is rejected
  before workload execution rather than silently weakening the property.
- **Externally gated** means code and workflow support exist, but the remaining
  evidence requires a release tag, target production backend, performance
  environment, failover infrastructure, or independent reviewer.
- **Residual limitation** means the behavior is intentionally narrower than a
  full production platform and is recorded in the risk register below.

## Level 1: what Remount is and why it exists

Remount is a single Go binary and an open protocol for giving an agent a
durable, reconnectable computer boundary. A workspace is not a chat session. It
is a named resource containing a filesystem, execution environment, placement
and security requirements, ownership and ACL data, lifecycle generation, and
an optional authoritative checkpoint. Sessions are reconnectable streams into
that workspace. Nodes materialize workspaces on concrete backends. The control
plane owns desired state and authority. The relay moves opaque protocol frames
between authenticated peers.

The project exists because an agent harness should not have to own machine
lifecycle, durable shell state, filesystem transfer, credentials, placement,
or recovery logic. A harness should be able to disconnect, restart, or be
replaced while the workspace remains available; a workspace should be able to
move between compatible nodes without changing the client contract; and a
reusable credential should remain outside the workspace even when a request is
authorized to use it.

There are four central product ideas:

1. **Portable workspaces.** The durable filesystem can be checkpointed,
   restored, released, moved, slept, and reclaimed on another compatible node.
2. **Harness-neutral sessions.** Output is sequenced and retained so a client
   can resume from a cursor after reconnecting.
3. **Secret-blind execution.** A workspace receives placeholders and a broker
   capability, not the reusable upstream credential itself.
4. **Capability honesty.** A workspace is placed only on a backend whose
   evidence-bearing descriptor satisfies its policy. The built-in process and
   Docker backends are useful local backends, but neither is advertised as a
   hardened multi-tenant execution boundary.

The high-level non-goals remain equally important. Remount does not implement
a hypervisor, container runtime, cloud IAM, distributed consensus system, or
full desktop/display stack. It composes such facilities through backend
contracts and refuses production profiles when the registered backend cannot
prove the needed capabilities.

## Level 2: component architecture and ownership

The runtime topology is intentionally asymmetric:

```text
CLI / public Go client ---- outbound WebSocket ----+
                                                  |
node ---- outbound WebSocket ---- relay ---- control plane ---- SQLite
  |                               |                 |
  |                               |                 +---- canonical events
  |                               +---- frame routing     timers / intents
  +---- backend handle                                  workspace authority
        |
        +---- jailed filesystem       HTTP artifact store ---- content blobs
        +---- process / PTY / port
        +---- checkpoint
        +---- optional enforced network controller
        +---- workspace-scoped broker / package connector
```

Only the server listens for Remount protocol traffic. Nodes and clients make
outbound links. The server HTTP listener also exposes artifact transfer,
webhook wake, metrics, liveness, and readiness endpoints. In standalone mode,
server and node live in one process but still use the same protocol path.

### Component responsibilities

| Component | Owns | Must not own |
|---|---|---|
| `internal/server` | HTTP lifecycle, dependency assembly, health/readiness, store and GC loops | workspace transition policy or backend execution |
| `internal/relay` | authenticated peer registry and bounded frame routing | client authority, workspace policy, or payload interpretation beyond routing metadata |
| `internal/control` | workspace authority, placement, grants, ACLs, timers, event ingestion, fleet operations, durable lifecycle intent/result | local process or filesystem mechanics |
| `internal/node` | materializations, lease self-fencing, session service, local mutation journal, artifact cache, brokers and connectors | choosing caller identity or inventing control-plane state |
| `internal/workspace` | responsibility-split backend and handle contracts; process and Docker implementations | claiming capabilities a backend does not enforce |
| `internal/fsops` | descriptor-rooted filesystem jail and bounded file operations | lifecycle or authorization policy |
| `internal/artifact` | digest-addressed storage, bounded snapshot/restore, inventory and reference-aware GC | deciding which snapshot is authoritative |
| `internal/session` | process/PTY/port lifecycle and sequenced memory/spill logs | transport reconnection or workspace grants |
| `internal/broker` | typed egress authorization, substitution, forwarding, leak prevention and audit | storing secrets in workspace state |
| `internal/connector` | immutable, scoped managed-package retrieval and bounded cache | general-purpose arbitrary egress |
| `internal/eventlog` | strictly ordered durable events, replay cursors, subscriptions and prefix retention | mutable workspace truth |
| `internal/transport` | framing, requests, cancellation, peer-bound responses and close wakeups | operation semantics |
| `internal/proto` | deterministic CBOR model, error taxonomy, security normalization and v1 negotiation | server or node policy state |
| `api`, `client` | supported external model and client methods | exposure of grants, frames, or `internal` types |

SQLite resource rows are authoritative mutable state. The event log is a
canonical audit and replay stream, not a reconstruction oracle for every
resource. Node-local state is authoritative for a retained materialization
until the commit-gated control protocol safely transfers or destroys it.

### Security authority

The authenticated server connection supplies subject and tenant. A client
cannot select its principal in a create request. The control plane owns ACL
evaluation and signs generation- and authorization-revision-scoped grants.
Nodes verify those grants before workspace operations. Node enrollment uses an
approved public key plus challenge proof-of-possession; link IDs and stale
proofs are not bearer identities. Node-emitted events carry monotonic producer
sequence and are rebound to assignment history by control before publication.

Backend capability descriptors separate isolation, sibling isolation,
filesystem boundary, network namespace, device isolation, broker identity,
egress mode, snapshots, and display. Security normalization produces explicit
local, isolated, or multi-tenant requirements. Placement and materialization
both validate them, preventing descriptor drift between scheduling and use.

## Level 3: important end-to-end flows and invariants

### Create and claim

1. The authenticated client submits a workspace specification.
2. Control normalizes the security policy, rejects unenforceable rules, derives
   authoritative owner/tenant/ACL fields, enforces atomic subject and tenant
   quotas, and durably creates a `pending` resource.
3. Placement filters online, non-quarantined nodes and their per-backend
   descriptors. Unknown memory does not satisfy a positive memory request.
4. A node asks to claim a specific generation and proves that it can satisfy
   the selected backend/security policy.
5. Control serializes the workspace operation, checks predecessor, actor,
   generation and node, persists `claiming`, and returns a scoped grant.
6. The node acquires binding leases before materializing, restores any
   authoritative checkpoint, applies an enforced network policy when required,
   and reports ready.
7. Only `claiming -> claimed` for the expected node/generation is accepted. A
   rejected ready causes local fencing and retention rather than a ghost copy.

### Exec and reconnect

1. The client asks control for current workspace information and a fresh grant.
2. It sends the open request to the authoritative node. The node verifies
   workspace, generation, subject, authorization revision, and requested scope.
3. A durable mutation intent is written before a non-idempotent effect. A
   completed result is persisted before success is acknowledged. Pending
   intents discovered after restart are not blindly replayed.
4. The session appends `StreamInfo` as sequence zero before starting the runner.
   Output is split into bounded chunks, retained in memory, and then spilled to
   a bounded file without creating silent holes.
5. Each client session has its own bounded delivery queue. The peer read loop
   never waits on application consumption while holding the session mutex.
6. After reconnect, the client obtains a current grant, reattaches at its last
   cursor, resumes input sequence, and surfaces replay gaps and attach errors.

### Lease loss and restart reconciliation

The node renews leases affirmatively. Rejection, expiry, generation mismatch,
assignment loss, or prolonged inability to renew cancels the workspace
execution context, revokes enforced network access, stops serving requests, and
retains the filesystem for reconciliation. Control advances authority before
another node can claim. Generation and authorization revision increments are
checked for overflow; exhaustion fails the workspace closed instead of wrapping
an authority token.

After control restart, claimed ownership is loaded from SQLite rather than
cleared. A reconnecting node reports retained materializations, and control
accepts only the copy consistent with durable assignment and generation.
Interrupted transient states converge through explicit recovery transitions.
Released workspaces return to a claimable state. Conflicting local copies are
quarantined/retained until control has enough proof to direct safe cleanup.

### Snapshot, checkpoint, move, and sleep

A regular snapshot is explicitly **live**: it can observe concurrent writes and
is useful for inspection or best-effort backup. An authoritative checkpoint is
**quiesced**: the node excludes direct filesystem mutations, asks the backend to
freeze managed execution, streams and verifies the archive, uploads it, and
commits the artifact reference to control. An authoritative checkpoint is
rejected when there is no control-plane artifact store.

Move and sleep use a two-phase release:

1. control durably enters a quiescing operation for the expected generation;
2. node fences new work and creates/verifies/uploads the checkpoint;
3. control durably commits the checkpoint reference and next state;
4. only after the commit acknowledgement may node destroy the source; and
5. a lost response is retried by operation ID, while an abort acknowledgement
   is required before the node resumes a failed release.

Timer firing and explicit wake serialize with release. Destroyed workspaces
cannot be resurrected by stale move, sleep, wake, idempotency, or node messages.

### Egress and credential mediation

The policy is a list of typed rules covering protocol, host, port, method, path
prefix, request count, and request/response byte budgets. Destination parsing
rejects ambiguity. DNS answers are checked against loopback, link-local,
private, carrier-grade NAT, and metadata-sensitive address ranges unless a
specific private destination is allowed. Redirects are rewritten and
reauthorized instead of escaping the original decision.

Reusable secrets are resolved on the trusted side. Placeholder matching is
exact and non-cascading. Substitution is allowed only for a bound HTTPS origin;
it is blocked for plaintext HTTP and foreign hosts. Attempts are audited even
when upstream fails. CONNECT needs its own explicit capability and an unexpired
generation-scoped lease. Suspending a broker closes established tunnels.

The built-in process and Docker backends expose only cooperative proxying. A
production mode or workspace requiring enforced egress therefore fails closed
unless a backend handle also implements the network-controller contract.

### Events, quarantine, and operations

Node events carry a durable producer high-water mark. Control rejects spoofed
attribution, detects gaps, deduplicates retries, and publishes according to
assignment history. Historical reads paginate; retention removes only a
contiguous prefix and reports the oldest available sequence so clients receive
a typed gap rather than silent truncation. Each live subscription owns its own
cursor and close signal.

Fleet quarantine persists selector, targets, requested actions, per-target
progress, and final/partial result. Freeze, egress revocation, checkpoint, stop,
and destroy are generation-aware and idempotently reconciled after restart.
Diagnostics and metrics expose incomplete convergence rather than treating a
request acknowledgement as completion.

## Level 4: bare-metal implementation mechanics

### Framing and transport

Frames are deterministic CBOR with a 4 MiB transport ceiling. Decode limits
bound nesting, array elements, map pairs, and malformed input work. Protocol v1
is exact: unsupported versions fail instead of being treated as future-compatible.
Hello negotiation intersects explicit capabilities, and a checked-in golden
fixture detects accidental wire drift. Requests are keyed by unpredictable IDs;
responses must match the pending peer and operation. Closing a peer or WebSocket
wakes pending request, read, and write waiters.

Relay maps use the correct exclusive lock for mutation, and removed/replaced
links cannot be re-indexed after unlock. Relay and per-peer queues are bounded;
slow consumers cannot synchronously block the global read/routing path.

### Lifecycle locking and persistence

Control has a map-level mutex plus keyed workspace-operation locks. Keyed lock
entries are reference-counted and removed, preventing an attacker from turning
arbitrary workspace IDs into a permanent map leak. All assignments to
`Workspace.State` route through `transitionWorkspace`, whose rule table encodes
actor and legal `(from,to)` pairs and whose request can require node and
generation equality. Exhaustive, authority, reordered-operation property, and
fuzz tests exercise the table.

SQLite uses one open connection to preserve strict event sequence and the
single-writer invariant. Workspace/resource changes are persisted before they
are published as success. Control mutation results, timer records, node
producer watermarks, fleet assignments, and tombstones have explicit
retention/capacity handling. Persistence errors propagate to callers.

### Filesystem and archive safety

On Unix, `fsops` walks from an opened root directory descriptor using
`openat`-style operations and no-follow semantics. Each component is validated;
NULs are typed bad requests, symlinks cannot redirect later operations outside
the root, recursive removal unlinks a symlink rather than walking its target,
and FIFOs/devices are rejected before blocking I/O. Shared abstractions isolate
platform-specific mechanics so Windows builds compile.

Snapshots copy from opened regular files rather than trusting a stale `Lstat`
size. Restore validates every path and link, rejects hard links and escaping
symlinks, limits compressed bytes, expanded bytes, entries and metadata, writes
into a staging directory, and renames only after the entire digest/format
operation succeeds. Artifact publication is digest-addressed and atomic;
staging reservations count against byte/object capacity and mismatch cannot
delete a valid existing blob. GC inventories crash leftovers, preserves
referenced or grace-period objects, and is resumable.

### Sessions and backpressure

Each log chunk has stream, sequence, timestamp, and bytes. Sequence zero is
reserved for session metadata. Large writes split at the configured maximum.
Memory eviction happens only after a complete spill record is durable; spill
records carry length and are range-validated when read. Reads honor batch limits
instead of rescanning an entire spill while holding the lock. A typed gap marks
data that genuinely fell outside retention.

Process groups are signalled only while the session is live, stdin does not hold
the lifecycle mutex during a blocking write, start failure still emits exit and
calls `OnExit`, and partial writes advance input sequence only after completion.
Node-wide, active, per-workspace, and per-principal session counts are reserved
atomically.

### Resource bounds

Defaults are explicit CLI flags rather than hidden unbounded behavior. Server
defaults include 128 concurrent control requests, 1,000 workspaces per tenant,
100 per subject, 100,000 mutation records, 100,000 timers, 128 timers per
workspace, 1,000,000 retained event rows, 30-day event/control-record retention,
8 GiB per artifact, 64 GiB/100,000 objects in the artifact store, and periodic
reference-aware GC. Node defaults include 128 concurrent requests, four
concurrent snapshots, a one-second per-workspace snapshot interval, 1,024
retained/256 active sessions, per-workspace/principal session bounds, 2 MiB
memory plus 128 MiB spill per session, 32 KiB chunks, 32 GiB/50,000 cached
artifacts, and byte/object bounds for the managed connector.

Admission fails with `resource_exhausted`; it does not wait indefinitely or
silently shed unrelated work. Metrics count each rejection and collector run,
while diagnostics expose current/limit pairs and retention watermarks.

## Code-audit disposition: RM-001 through RM-050

| Finding | Disposition and primary evidence |
|---|---|
| RM-001 | **Verified fixed.** Stale simulation expectations were corrected; clean lint, unit, race, conformance and external-SDK tests pass. |
| RM-002 | **Verified fixed.** Relay mutation uses exclusive synchronization; replacement/removal stress coverage is in `TestRouteRemoveAndReplacementAreSynchronized`. |
| RM-003 | **Verified fixed.** Claim responses copy authoritative state while locked; race tests cover concurrent lifecycle reads. |
| RM-004 | **Verified fixed.** Affirmative renewals and node self-fencing stop local service after authority loss; `TestRejectedRenewalFencesAndRetainsFilesystem` covers the boundary. |
| RM-005 | **Verified fixed.** Ready is a strict expected-node/generation transition; rejected ready fences and retains the local copy. |
| RM-006 | **Verified fixed.** Keyed lifecycle serialization plus the central transition table prevents overlapping stale operations; exhaustive/property/fuzz tests cover legal orderings. |
| RM-007 | **Verified fixed.** Release is checkpoint-before-destroy with durable commit/abort; `TestReleaseSnapshotFailureRestoresSourceWithoutDestroy` and control release tests cover failures. |
| RM-008 | **Verified fixed.** Durable holder/generation survive restart and matching node state is adopted; `TestRestartPreservesHolderAndReleasedRecovery` plus live local/E2B restart tests cover it. |
| RM-009 | **Verified fixed.** Authoritative checkpoint uploads and commits `LastSnapshot`; live snapshots are explicitly non-authoritative. |
| RM-010 | **Verified fixed.** State/result persistence errors propagate and success is not returned before commit; strict persistence tests use failing stores. |
| RM-011 | **Verified fixed.** Transactional bounded restore rejects symlink and path escapes; hostile archive tests and `FuzzRestore` cover it. |
| RM-012 | **Verified fixed.** Port sessions are backend-resolved and capability/policy checked; they no longer expose arbitrary node-loopback dialing. |
| RM-013 | **Verified fixed.** Subject/tenant come from the authenticated connection and resource ACLs are enforced; cross-tenant tests cover denial. |
| RM-014 | **Verified fixed.** Approved node keys prove possession of a fresh challenge and replay is rejected by `TestNodeProofOfPossessionAndReplayProtection`. |
| RM-015 | **Verified fixed.** Credential substitution is bound-host HTTPS only; plaintext/cross-host tests and a live OpenAI request/leak attempt verify the boundary. |
| RM-016 | **Verified fail-closed.** Production modes reject cooperative/open egress or insufficient isolation; built-in backends are documented local-only. |
| RM-017 | **Verified fixed.** Sleep/release/timer operations serialize and persist; timer firing cannot strand the workspace mid-release. |
| RM-018 | **Verified fixed.** Recovery converts durable `released` state back to claimable pending and reconciles retained node copies. |
| RM-019 | **Verified fixed.** `StreamInfo` is appended before runner start and is always sequence zero, including immediate output/start failure tests. |
| RM-020 | **Verified fixed.** Spill failure preserves the contiguous in-memory range and returns an error/gap instead of silently deleting it. |
| RM-021 | **Verified fixed.** Blocking stdin writes occur outside the lifecycle mutex and sequence advances only after complete writes. |
| RM-022 | **Verified fixed.** Tail subscriptions have independent ownership/cursors and close safely; slow consumers catch up without send-on-closed panic. |
| RM-023 | **Verified fixed.** Bounded per-session delivery queues decouple application reads from the peer connection and surface overflow as a gap/error. |
| RM-024 | **Verified fixed.** Workspace/session info paths attach the freshly issued internal grant used by subsequent node operations. |
| RM-025 | **Verified fixed.** Declared idempotent mutations use durable intent/result journals, bounded retention, exact replay and fail-closed uncertain recovery. |
| RM-026 | **Verified fixed.** Docker receives a host-reachable broker endpoint and the reverse broker form works from the workspace network. |
| RM-027 | **Verified fixed.** Broker capability is workspace/generation scoped; CONNECT checks explicit permission and expiry and tunnels close on suspension. |
| RM-028 | **Verified fixed.** Descriptor-rooted filesystem operations replace check-then-use path strings; symlink-race/remove regression coverage is present. |
| RM-029 | **Verified fixed.** Upload, store, archive entry, expanded-size, metadata, staging, connector byte and object budgets are enforced atomically. |
| RM-030 | **Verified fixed.** Node subscriber cleanup removes the exact subscription/workspace owner and does not kill unrelated tails. |
| RM-031 | **Verified fixed.** Node producer sequence is durable; control detects gaps, deduplicates and assigns actor/node/workspace from authority history. |
| RM-032 | **Verified fixed.** Listener-first readiness, `/readyz`, synchronized address/health state, idempotent close and bounded shutdown replace polling races. |
| RM-033 | **Verified fixed.** Relay, control, node, client, broker, session and snapshot admission/queues have explicit bounds and typed rejection. |
| RM-034 | **Verified fixed.** Required binding leases are acquired before materialization and failure cannot leave a claimed workspace missing credentials. |
| RM-035 | **Verified fixed.** API/protocol distinguish live snapshot from quiesced authoritative checkpoint; checkpoint coordinates filesystem mutation and backend execution. |
| RM-036 | **Verified fixed.** Public/internal copy helpers chunk transfers within frame limits and report stream gaps instead of silently truncating. |
| RM-037 | **Verified fixed.** Unknown memory does not satisfy a positive placement requirement; backend capability validation is repeated at materialization. |
| RM-038 | **Verified fixed.** Historical reads paginate to completion and retention below the watermark returns `evicted`, never a silent 1,000-row stop. |
| RM-039 | **Verified fixed.** Missing/empty binding secret resolution is rejected at startup rather than accepted as a usable binding. |
| RM-040 | **Verified fixed.** Unix mechanics are build-tagged/isolated and both Windows architectures cross-compile. |
| RM-041 | **Verified fixed.** Peer, WebSocket, event subscription, server-ready and session close paths wake blocked waiters; focused tests cover each. |
| RM-042 | **Verified fixed.** Decode enforces exact protocol v1 and handshake capability negotiation; the golden frame fixes compatibility expectations. |
| RM-043 | **Verified fixed.** Event-stop is a named protocol operation documented in `spec/PROTOCOL.md`. |
| RM-044 | **Verified fixed.** Diagnostics and metrics expose quotas, retention, gaps, security decisions, lifecycle and GC state; the live deep bundle was healthy. |
| RM-045 | **Verified fixed.** Events, sessions, control records and artifacts have bounded retention/capacity and observable, reference-aware collection. |
| RM-046 | **Verified fixed.** Modal names/config are parameterized, secrets are external, JSON decoding matches the CLI, readiness is deterministic, smoke resolves the deployed app, and live deploy/smoke/log/redeploy passed. |
| RM-047 | **Verified fixed.** `make conformance` executes the security/lifecycle packages and CI runs it plus bounded fuzzing. |
| RM-048 | **Verified fixed.** README, design, tutorial, operations, observability and integration docs now separate implemented guarantees from compatible-backend requirements. |
| RM-049 | **Verified fixed.** Direct tests cover relay/control/node/session/artifact/fsops/eventlog/server/transport/protocol plus eight fuzz targets. |
| RM-050 | **Verified fixed.** Temporary artifact state is owned and removed on close; deep health re-hashes both stores and reports damage/loss signals. |

## Adversarial-review disposition

The adversarial review predates the fixes and intentionally remains unchanged.
This table maps each surviving finding to its current control. “Fixed” below
also means the broad race and conformance suites passed after the focused test.

| # | Original failure mode | Current disposition |
|---:|---|---|
| 1 | Control restart clears the holder and blocks re-adoption | Fixed by durable holder/generation recovery and matching-copy adoption. |
| 2 | Node resync destroys the only live copy after conflict | Fixed by local fencing/retention and commit-gated cleanup. |
| 3 | Archive symlink lets later entries escape root | Fixed by staging restore, link validation and descriptor-rooted writes. |
| 4 | Secret substituted into plaintext HTTP | Fixed: substitution is HTTPS-only and origin-bound. |
| 5 | Session operations omit the refreshed grant | Fixed: current scoped grant is attached/retried internally. |
| 6 | Slow session consumer blocks the entire client | Fixed with bounded per-session delivery decoupled from the peer reader. |
| 7 | Reattach discards subscription failure | Fixed: attach failure reaches the session/client caller. |
| 8 | Recreated client session restarts input sequence | Fixed: attach resumes authoritative input sequence and partial writes do not advance it. |
| 9 | Lost release response discards a completed snapshot | Fixed by operation-ID result replay and commit-gated destruction. |
| 10 | Move/sleep resurrect a destroyed workspace | Fixed by centralized predecessor/generation checks. |
| 11 | Old node never learns it lost its lease | Fixed by affirmative renewals and local self-fencing. |
| 12 | Node ignores rejected ready and keeps serving | Fixed: rejection cancels service and retains/quarantines materialization. |
| 13 | Upload failure destroys the good local source | Fixed: release abort retains/restores service only after authoritative acknowledgement. |
| 14 | Relay writes a map under `RLock` and re-indexes stale data | Fixed with exclusive mutation and replacement stress coverage. |
| 15 | Spill failure silently deletes a sequence range | Fixed: the contiguous range stays in memory and the error is surfaced. |
| 16 | Spill replay ignores batch limit and scans under lock | Fixed with bounded indexed reads and focused batch test. |
| 17 | Forged response can satisfy another peer's request | Fixed by binding pending response to peer and operation. |
| 18 | Filesystem path validation races later syscall | Fixed by root-descriptor traversal and no-follow operations. |
| 19 | Recursive remove follows a final symlink | Fixed: final symlink is unlinked, never traversed. |
| 20 | Placeholder prefix collision causes false leak blocks | Fixed by exact token parsing. |
| 21 | Multi-substitution uses stale loop value | Fixed with non-cascading replacement and regression test. |
| 22 | Failed upstream suppresses credential-use audit | Fixed: attempted use is emitted before/independent of upstream success. |
| 23 | CONNECT ignores binding lease expiry | Fixed with generation/expiry validation and suspension teardown. |
| 24 | CGNAT/metadata address remains reachable | Fixed by complete non-public range classification and resolution checks. |
| 25 | Reconnect supervisor exits during empty-session window | Fixed: connection supervision follows client lifetime, not momentary session count. |
| 26 | Orphan chunk table fills permanently | Fixed with bounded ownership, expiry/cleanup and explicit gap behavior. |
| 27 | Event tail closes its own channel or replaces another tail | Fixed with per-subscription owner and cancellation. |
| 28 | Session ID is read/written under different locks | Fixed by consistent session synchronization; race suite is green. |
| 29 | Idempotency hit returns destroyed resource/panics | Fixed by scope- and request-fingerprint-bound durable results plus terminal-state validation. |
| 30 | Timer firing during release strands paused workspace | Fixed by keyed serialization and transactional timer state. |
| 31 | Signal after exit can hit a recycled process group | Fixed by live-state synchronization before signalling. |
| 32 | Runner output wins sequence zero before `StreamInfo` | Fixed by recording info before runner start. |
| 33 | Start failure never invokes exit callback | Fixed and directly tested. |
| 34 | Snapshot trusts stale `Lstat` size | Fixed by copying from the opened regular file and its actual stream. |
| 35 | Restore has no entry/expanded-byte limits | Fixed with archive-wide limits and boundary tests/fuzzing. |
| 36 | NUL path returns internal error | Fixed with typed `bad_request`. |
| 37 | FIFO file operation blocks a request forever | Fixed by rejecting special files before I/O. |

## Incident-hardening SDMR disposition

### Immediate blockers C1-C9

| Requirement | Status |
|---|---|
| C1 relay synchronization | **Verified fixed** by exclusive map mutation and stress/race tests. |
| C2 locked control reads | **Verified fixed** by copying claim/lifecycle state under lock. |
| C3 strict readiness | **Verified fixed** by the checked `claiming -> claimed` transition and rejected-ready cleanup. |
| C4 per-workspace claim serialization | **Verified fixed** by lifecycle keyed locks with cardinality cleanup. |
| C5 session header ordering | **Verified fixed** by pre-run `StreamInfo` sequence zero. |
| C6 spill failure propagation | **Verified fixed** with contiguous retention and typed gaps/errors. |
| C7 subscriber ownership | **Verified fixed** with independent subscription identity/cancellation. |
| C8 readiness without polling | **Verified fixed** by listener-first `WaitReady` and `/readyz`. |
| C9 no connection-wide blocking | **Verified fixed** by bounded asynchronous per-session delivery and request admission. |

### Architecture requirements R17-R29

| Requirement | Status |
|---|---|
| R17 backend-specific capabilities | **Implemented.** Descriptors and responsibility interfaces carry evidence per backend. |
| R18 explicit workspace security policy | **Implemented.** Typed profile/isolation/sibling/egress/secret/audit requirements normalize fail-closed. |
| R19 enforced egress | **Implemented as a mandatory backend contract.** Built-ins are rejected for production; a qualifying backend must supply `NetworkController`. |
| R20 capability-scoped network policy | **Implemented.** Rules scope protocol, host, port, method, path and budgets. |
| R21 per-workspace broker identity | **Implemented.** Capabilities bind workspace, generation, policy and lease. |
| R22 authoritative identity/authorization | **Implemented.** Authenticator/authorizer own subject, tenant, ACL and admin decisions. |
| R23 lease fencing/reconciliation | **Implemented and exercised** across unit, simulation, local and E2B restart paths. |
| R24 two-phase release/checkpoint | **Implemented** with begin, verified checkpoint, commit/abort and post-commit destruction. |
| R25 sibling non-interference | **Fail-closed.** Policies needing it schedule only to a descriptor advertising it; no built-in backend makes that claim. |
| R26 managed connectors | **Implemented** for immutable, scoped package retrieval with digest checks and byte/object quotas. |
| R27 authoritative event ingestion | **Implemented** with producer sequencing, attribution, gap detection and assignment history. |
| R28 fleet quarantine | **Implemented** as durable selector/target/action state with restart reconciliation and diagnostics. |
| R29 explicit production modes | **Implemented.** Standalone, production single-tenant and production multi-tenant floors are distinct and startup fails on weak backend sets. |

## Production-readiness request disposition

### Release gates

| Gate | Current status | Evidence / remaining condition |
|---|---|---|
| Gate 0: trustworthy baseline | **Verified locally.** | Format, vet, lock lint, unit, race, staticcheck, vulnerability, module, conformance, fuzz, external API and cross-build gates pass. CI pins actions/toolchain and least privilege. |
| Gate 1: lifecycle correctness | **Verified in the candidate.** | Central model, durable idempotency, self-fencing, restart reconciliation, failure injection and live restart tests pass. |
| Gate 2: trust model | **Verified fail-closed.** | Identity, ACL, enrollment, grants, artifacts, events and policy validation are tested. Actual production execution still requires a third-party backend satisfying the enforced contracts. |
| Gate 3: durability and scale | **Partially externally gated.** | Integrity, quotas, backpressure and crash-boundary tests exist; local/E2B/Modal persistence was rehearsed. Published target-scale latency/load measurements remain an operator/release task. |
| Gate 4: operability | **Partially externally gated.** | Metrics, structured logs, diagnostics, health/readiness, shutdown, fleet controls, backup/rollback/capacity runbooks exist. Distributed tracing and an HA controller deployment are not implemented. |
| Gate 5: public product | **Code-complete, review gated.** | Public API, strict v1, docs, release workflow and artifacts exist. A signed release run and independent review have not occurred in this pass. |

### Final acceptance checklist

- [x] Candidate builds with the declared Go toolchain.
- [x] Formatting, vet, lock analysis, static analysis and dependency checks pass.
- [x] Unit, simulation, integration, public-module and race suites pass.
- [x] Eight fuzz targets complete their configured local budget without crash.
- [x] Lifecycle model is normative, exhaustive/property tested and fuzzed.
- [x] Lease loss and rejected renewals self-fence.
- [x] Move/sleep failure injection prevents acknowledged destruction before commit.
- [x] Production profiles reject weak built-in backends.
- [x] Egress bypass, cross-tenant authorization and hostile-artifact tests pass.
- [x] Control/node restart and re-adoption pass locally and in E2B; Modal redeploy re-adopts persistent state.
- [x] Metrics, alerts guidance, structured logs, diagnostics and deep collection cover critical paths.
- [x] Backup/restore mechanics and durable fleet quarantine are implemented and rehearsed at single-controller scope.
- [x] README and operational docs match current guarantees.
- [x] Public API and protocol compatibility policy are versioned and externally compile-tested.
- [x] Release workflow constructs checksums, SBOM, signatures and provenance.
- [x] Known residual risks are published below.
- [ ] Sustained performance/chaos targets are measured on release hardware and published.
- [ ] A tagged release proves the hosted SBOM/signing/provenance workflow end to end.
- [ ] A production-qualified backend passes the suite; built-ins intentionally cannot.
- [ ] Active/passive or transactional shared-database controller failover is selected and exercised if HA is required.
- [ ] An independent reviewer closes architecture/security findings.

The unchecked items are not defects being silently accepted. They are external
or architectural gates that this repository cannot truthfully self-certify in
a local development pass.

## Verification evidence

### Repository gates

The final candidate was exercised with:

```text
make lint
make test
make race
make conformance
make fuzz FUZZTIME=5s
go mod verify
go mod tidy -diff
staticcheck ./...
govulncheck ./...
actionlint
make dist VERSION=verification-2026-09-02
```

All passed. `make test` also enters `integration/publicsdk`, a separate Go
module, so importing the public surface cannot accidentally rely on Go's
same-module exception for `internal` packages. The race run covered every root
package. Fuzzing exercised protocol decode, hostile archive restore, filesystem
path resolution, broker destination parsing, grant verification, reordered
workspace lifecycle operations, ID prefixes, and session cursor ranges.
`govulncheck` reported no known reachable vulnerabilities. Distribution builds
were produced for Linux, macOS and Windows on amd64 and arm64 and inspected as
static/correct-format binaries with checksums.

### Local process backend

A fresh standalone deployment on a non-default loopback port completed create,
claim, jailed file write/read, stdout/stderr exec, live snapshot, quiesced
authoritative checkpoint, move, timed sleep/wake, metrics, health/readiness,
inspect and deep doctor. It was stopped without losing its data directory,
restarted against the same control and node state, re-adopted the workspace at
the same generation, preserved the file, and remained healthy.

### Local Docker backend

A real `ubuntu:24.04` container workspace completed create/claim, host-side
jailed write and container-side read/exec in `/work`, Docker pause/unpause
authoritative checkpoint, control and node artifact re-hash, and destroy. The
container label inventory was empty afterward. While live,
`scripts/collect.sh --deep --ws ...` produced a valid JSON bundle and
`scripts/explain.py` reported one online node, one claimed workspace, one
snapshot, both artifact copies verified, and `VERDICT: healthy`.

### Real credential broker

A temporary binding referred to `$OPENAI_API_KEY`; no literal credential was
written to the binding file or logs. A workspace received a placeholder and
used the reverse broker URL to request OpenAI's `/v1/models`: the upstream
returned HTTP 200 with a non-empty response. Sending the same placeholder to an
unbound host returned 403. Metrics recorded one credential substitution, one
leak block, and the associated denials; doctor surfaced the attempted leak as a
warning. A direct HTTPS proxy CONNECT attempt was also denied because opaque
CONNECT payloads cannot safely support application-header substitution. The
temporary binding and deployment were removed.

### E2B

A Linux/amd64 candidate was uploaded to a fresh E2B sandbox and run with an
explicit process-replacing wrapper. It completed create, jailed file
write/read, Linux exec, authoritative checkpoint and healthy doctor. The
server was killed, its port was independently confirmed down, and the same
durable data directory was started again. The node re-adopted the claimed
generation, the payload remained present, adoption events appeared, and deep
doctor remained healthy. The sandbox was then killed.

### Modal

A uniquely named development app, volume and secret were created. The deployed
HTTPS endpoint passed readiness, create/claim, filesystem I/O, exec,
authoritative checkpoint, diagnostics and metrics. A clean redeploy passed the
repository's `modal-smoke` target against the named persistent deployment, and
both streamed app logs and persistent server/node logs showed enrollment and
workspace re-adoption. The test app was stopped and its test-only volume and
secret were deleted.

That exercise found and fixed four deployment-specific defects: import-time
binary validation running inside the container, assuming the node list was a
JSON object rather than an array, an unnecessary explicit `Volume.commit()` in
shutdown, and smoke accidentally targeting `modal run`'s ephemeral app instead
of the deployed function.

## Additional defects found during implementation review

The requested self-review found issues that were not explicit rows in the two
bug reports:

- node filesystem handlers shadowed and discarded operation errors in several
  branches; the error path now propagates;
- session stdin treated a short write as complete; sequence now advances only
  after all bytes are accepted;
- session termination failures were ignored before authoritative checkpoint,
  release, and quarantine, and a failed release did not restore its local lease
  deadline; quiescence failures now abort truthfully and restore self-fencing;
- a failed bounded snapshot could return and release the workspace tree lock
  before its archive-producing goroutine had exited; pipe shutdown now joins
  the producer before lifecycle work continues;
- an already-authorized filesystem operation could wait behind an explicit
  checkpoint and then mutate after archive completion but before control
  committed the digest; the checkpoint now holds the tree boundary through the
  generation-specific control commit;
- a filesystem/session operation authorized just before release, quarantine,
  fencing, or checkpoint could remain queued and touch a stale handle after the
  lifecycle boundary; tree-lock acquisition now revalidates serviceability,
  handle close drains in-flight work, release drains even without a snapshot,
  and client checkpoint commits lose once a lifecycle transition has begun;
- workspace generation could overflow protocol/persistence authority at the
  signed SQLite boundary; authority exhaustion now transitions to durable
  failure without wrapping;
- keyed lifecycle locks retained arbitrary keys forever; entries are now
  reference-counted and collected;
- assignment retention could repeatedly scan only protected oldest rows and
  starve deletable history behind them; SQL now excludes the live-authority set
  before applying the bounded deletion limit;
- an authoritative checkpoint could be requested without an artifact endpoint,
  yielding a result that could not serve as failover state; it now returns
  `unsupported` before snapshot work;
- the public `Copy` helper did not validate `StreamGap`; it now validates and
  renders a gap marker and returns `evicted`, as does aggregate `Run`, instead
  of presenting discontinuous output as complete;
- CI's seeded-fuzz command selected no fuzz functions; it now runs the fuzz
  seeds explicitly;
- defaulting a frame's version during encoding mutated the caller-owned frame,
  creating avoidable shared-state risk; encoding now defaults a private copy;
- the Unix descriptor implementation imported `golang.org/x/sys` directly
  while `go.mod` classified it as indirect; module metadata is corrected;
- Modal readiness, JSON shape, volume lifecycle, and deployed-vs-ephemeral
  function selection had the four live-deployment bugs described above; and
- one negative-limit validation message described only part of the rejected
  configuration; it now identifies the full quota class.

Each code issue has focused coverage or is exercised by a live deployment and
the broad final gates.

## Residual risk register

### Single controller and SQLite writer

Control state is intentionally single-writer. SQLite is backed up consistently
and restart recovery is tested, but there is no leader election or automatic
active/passive takeover. Running two independent controllers over copied or
shared SQLite files is unsupported and unsafe. Deployments requiring controller
HA must add a fenced active/passive design or transactional shared database.

### No built-in production-qualified backend

The process backend has no isolation. Docker provides a container boundary and
network namespace but its current Remount integration is cooperative-proxy,
does not claim sibling isolation, and supplies no enforced gateway controller.
This is now rejected by production profiles. A production operator must provide
and conformance-test a backend such as a hardened container/microVM integration
that implements the required network and isolation contracts.

### Filesystem-only portability

Built-in checkpoints archive the filesystem. They do not capture RAM, kernel
state, live TCP connections, or process continuation. Sessions may reconnect to
retained execution on the same node; after movement, processes must be started
again against restored files.

### Relay metadata visibility

The relay is payload-agnostic at the operation level, but it necessarily parses
frame envelopes for source, destination, kind, operation, IDs and routing. It
is not end-to-end encrypted from client to node independently of the server.

### Connector retention

The managed package connector is byte/object capacity bounded and scoped, but
does not presently have independent age-based cache eviction. Capacity protects
the node; operators may need explicit cache lifecycle policy for long-running,
high-churn fleets.

### Event/resource atomicity

Resource rows are authoritative and events are canonical audit history, but a
resource commit and event append are not a single database outbox transaction
in every path. Failure is diagnosed and important node ingestion is sequenced;
strict external audit completeness may require a transactional outbox.

### Public event streaming API

The public event tail follows Go channel conventions and surfaces setup/stream
errors through its current API shape. Consumers still need cancellation and
cursor persistence; a future major API may prefer an iterator with an explicit
terminal error method.

### Tracing and performance evidence

Metrics, structured logs and diagnostic bundles exist, but there is no OpenTelemetry
span export. Unit/race/fuzz/conformance and live cloud tests establish
correctness boundaries, not production-scale latency, throughput, memory or
reconnect-storm targets. Those measurements must be made on the intended
backend and hardware.

### Release and independent review

The workflow is defined and statically validated, but no signed public tag was
created during this pass. The same implementer performed the closing self-review;
that cannot substitute for the independent architecture/security review named
in the original acceptance criteria.

### Point-in-time cloud evidence

E2B and Modal tests prove the candidate worked in fresh development resources
at the recorded time. They do not guarantee future provider behavior. The
tests deliberately cleaned up their sandbox/app/volume/secret, and no provider
credential or reusable application secret is committed to the repository.

## Operating recommendation

The candidate is appropriate for local development, integration testing, and a
carefully operated single-controller early deployment using explicitly trusted
backends. Do not label the built-in process or Docker configuration as
multi-tenant production isolation. Before a broader production claim, run the
conformance and chaos/load program against the chosen hardened backend, exercise
the release workflow from a signed tag, select a controller failover design if
required, and obtain the independent review called for by the original request.
