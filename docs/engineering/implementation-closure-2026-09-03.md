# Remount hardening implementation closure — 2026-09-03

This document records the implementation and verification pass performed after
the 2026-09-02 deep dive, code audit, adversarial review, production-readiness
request, and OpenAI/Hugging Face incident-hardening SDMR. It is the current
status ledger for those point-in-time documents; it does not rewrite their
historical observations.

The reviewed candidate is `main` after `2b35975` plus the changes delivered
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

The operation ID is only exact-retry identity. Control durably increments a
separate per-workspace release epoch before each cycle, and the node requires a
greater epoch before replacing an abort-published same-generation tombstone.
Delayed requests from older cycles therefore fail before session stop,
filesystem fencing, or source cleanup.

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
| RM-007 | **Verified fixed.** Release is checkpoint-before-destroy with durable commit/abort and ordered release-cycle authority; node and simulation regressions cover source retention, exact retries, and delayed stale requests after abort publication. |
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
- active-session capacity was released by an asynchronous observer after
  `Session.Wait` had already returned, allowing a completed session to cause a
  transient quota rejection; exit publication now follows manager accounting;
- the simulator port-forward conformance test used a fixed loopback port and a
  separately started Python server, colliding with services on hosted runners;
  it now uses an in-process HTTP server on an OS-assigned port;
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

## 2026-09 build plan disposition

This section dispositions every numbered Phase 2–6 item from
`handoff-2026-09-03.md`, every acceptance scenario it defines, and the residual
gaps carried by `handoff-2026-09-03-codex-wrap.md`. It is the current status
entry for that build plan.

Every row is exactly one of:

- **implemented and verified** — the code exists and a named test or gate
  proves it on the host recorded below;
- **implemented, externally gated** — the code exists and its local proof
  passes, but the product claim needs a resource this host cannot provide;
- **deliberately fail-closed** — the behavior refuses or reports unavailable,
  and that refusal is the correct answer;
- **not performed, by decision** — an owner decided not to do it;
- **open** — with a named owner and the exact next proof.

A skipped secret-gated job is *unavailable*, never healthy. Absence of evidence
is never recorded as a pass.

### Host and candidate

- Host: Darwin 25.3.0 arm64, Go 1.27, `CGO_ENABLED=0`.
- Base commit: `58a9d15` (merge of `codex/handoff-2026-09-03`).
- Docker: **available**, server 29.4.1 — the Docker backend lane runs here.
- gVisor (`runsc`): **unavailable**, not registered with Docker. Docker Desktop
  on macOS runs its daemon inside a VM, so `runsc` cannot be registered.
- `/dev/kvm`: **absent**. macOS cannot provide it, so no Firecracker lane can
  execute on this host.

### Defects found and fixed during this pass

Five defects were found while trying to *run* the evidence rather than while
reading the code. Each is recorded because the acceptance claim it touches
would otherwise have been false.

1. **`ws.moved` asserted process continuity it could not know.**
   `internal/control/control.go` set `processes="preserved"` from
   `RestoreFormat == firecracker-full-v1` at the moment the move entered
   `pending` — before a destination was chosen, before its Firecracker and CPU
   compatibility were checked, and before any guest was restored. ADR 0063
   permits that claim only for a checkpoint that actually restored. The event
   is the audit record, so this made the log assert continuity for moves that
   in fact cold-started. Now reports `restore_pending`; the positive claim is
   reserved for a destination that restored. Regression:
   `TestMoveNeverClaimsPreservedProcessesBeforeRestore` and
   `TestMoveReportsRestartedForFilesystemOnlyCheckpoints`.

2. **The E1 Docker acceptance test could never pass.** It scanned
   `ws.info`'s `Root` as a host directory. `ws.info` reports `MountPathOf` —
   the root the *workspace* sees, which under the docker backend is the
   in-container mount `/work`. The scan died with `lstat /work: no such file or
   directory`. This is precisely the confusion `HostFileSystem`'s own doc
   comment warns against ("callers must never treat an in-guest tree as a local
   directory"). The test now asserts that `ws.info` reports the in-container
   mount path and reaches the host tree through the backend's layout. E1, E2
   and E3 pass live as a result.

3. **The MCP gateway dropped the artifact representation.** `internal/mcp`
   decoded `FSApplyTarReq` and called `ApplyTar`, discarding `Format`, so a
   chunked apply routed through MCP would be parsed as a tar. It failed closed
   on the gzip header rather than corrupting a tree, but the operation was
   unusable.

4. **`fs.apply_tar` was advertised over MCP but permanently unreachable.** The
   manifest builds a tool name by replacing dots with underscores
   (`fs.apply_tar` → `op_fs_apply_tar`) and the gateway reversed it by
   replacing underscores with dots, yielding `fs.apply.tar` — an operation that
   does not exist. `fs.apply_tar` is the only protocol op whose name contains
   an underscore, so it was the only tool silently broken. The reverse mapping
   is now an exact lookup over the manifest. Regression for 3 and 4:
   `TestMCPGatewayCarriesApplyArtifactFormat`, which also proves that applying
   a chunked manifest *as tar* fails closed, i.e. the node honors the declared
   format instead of guessing from the id.

5. **Access tokens could be issued above the lifetime the verifier accepts.**
   `IssueAccessToken` admitted any TTL up to 24 h, while `verify` rejects an
   access token whose lifetime exceeds the configured `accessTTL` (default
   1 h). A `--bootstrap-ttl 2h` therefore minted a bearer that was unusable the
   instant it was written to the bootstrap file, and failed later as a bare
   `unauthorized` with nothing in the log to explain it — the operator's first
   contact with the product silently handing them a dead credential. Issuance
   now refuses above the ceiling and names it, rather than shortening the
   request silently. Regression:
   `TestIssueAccessTokenRefusesATTLTheVerifierWouldReject`, which asserts the
   issuer and the verifier agree at and above the boundary.

### Repository gates

| Gate | Result |
|---|---|
| `gofmt -l .` | clean |
| `git diff --check` | clean |
| `go build ./...` | ok |
| `go vet ./...` | ok |
| `go test -count=1 ./...` | ok |
| `make race` | ok |
| `make lint` (vet, gofmt, lock discipline, `llms.txt` freshness, acpgen) | ok |
| `make public-api` | ok |
| `make conformance` | ok |
| `make fuzz FUZZTIME=10s`, all 8 targets | ok |
| `staticcheck ./...` (pinned v0.8.1) | zero findings, including U1000 |
| `go run ./cmd/protogen --check` | clean |
| web `npm test`, `npm run build`, `npm run check:dist` | ok |

### Acceptance scenarios

| Scenario | Status | Evidence |
|---|---|---|
| E1 real key through the broker | implemented and verified, **live** | `TestRunOpenCodeDockerIntegration` passes in 46.9 s against a real `node:22` container, the real `opencode` harness and a real OpenAI key. The key appears in 0 workspace files and 0 log lines. Required fixing defect 2 above. |
| E2 two turns, move across nodes, third turn recalls both | implemented and verified, **live** | `TestRunOpenCodeHandoffAcrossNodesDockerIntegration`, 152.1 s; key in 0 log lines |
| E3 two-task queue with sleep between | implemented and verified, **live** | `TestRunOpenCodeQueueDockerIntegration`, 71.7 s; key in 0 log lines |
| E4 gVisor enforced egress | implemented, externally gated | `runsc` is not registered on this host, so the seven-check denial suite cannot run. The backend advertises `enforced_gateway` only after that suite passes, so it is correctly unavailable rather than falsely green. |
| E5 two untrusting tenants on one gVisor node | implemented, externally gated | same gate as E4 |
| E6 approve-on-first-use | implemented and verified | committed `egress.pending` reaches the notifier while the upstream request stays parked, and a decision releases it (`0780b79`) |
| E7 budgets | implemented and verified | `internal/budget` suite |
| E8 principal revocation | implemented and verified | `TestE8PrincipalRevocationComposes`, `TestE8OperatorRevocationClosesAmbientSession`, and `TestE8PrincipalRevocationCoreSurvivesRestart` in `integration/identity` |
| E9 chunked snapshot dedupe | implemented and verified | `TestE9ChunkedSnapshotMoveDeduplicates500MBLogicalFixture`. Literal gap: the fixture is a 500 MiB *logical* tree and the under-3s move is measured on a local process backend, not across a network. |
| E10 control-plane failover | implemented and verified | `integration/failover` passes |
| E11 tiered session replay | implemented and verified | `TestE11TieredSessionReplayCrossesNodeAndNamesUnavailableBlob`, `TestE11TieredSessionRecordSurvivesNodeRestart`. Literal gap: the proof uses 8 MiB across more than 128 real 128 KiB segments, not a six-hour 2 GB PTY session. |
| E12 published SDK packages | implemented, externally gated | both packages install and build from the local tree and their generated types are drift-checked, but neither is published to PyPI or npm. A local install is not publication. |
| E13 two real MCP harnesses | deliberately fail-closed | no harness keys present; the lane skips explicitly rather than passing |
| E14 real GitHub App | implemented, externally gated | exercised against a local Git HTTP/token fake; no GitHub App is registered |
| E15 console operator flow | see Phase 5.2 | |
| E16 exports | implemented and verified | `internal/control/exports` suite and durable cursor |
| E17 pool scale up and down | implemented and verified against a fake provisioner | `TestE17PoolClaimScalesUpAndIdleScalesDown`. Real vendor scaling is externally gated — see `verification-2026-09.md`. |
| E18 signed release | **not performed, by decision** | The user decided on 2026-09-03 not to release. No tag, image, formula or `go install` path was created or verified. No implementation depends on it. |
| E19–E25 durable agent scenarios | implemented and verified | `internal/sim` agent suite |

### Phase 2

| Item | Status | Notes |
|---|---|---|
| 2.1 gVisor `enforced_gateway` backend | implemented, externally gated | code and unit tests exist; the denial-conformance suite requires `runsc`, which cannot be registered under Docker Desktop on macOS |
| 2.2 vendors as node provisioners | implemented, externally gated | fake-API unit tests pass for e2b, fly, modal, ix and ssh. Live lanes skip with an explicit list of missing identifiers. E2B and Modal credentials were probed and are **valid**; the E2B account has **zero templates** and the Modal CLI helper is absent, so the gate is real and not a guessable variable. |
| 2.3 Firecracker backend | implemented, externally gated — **deferred by decision** | format, compatibility, protocol and lifecycle logic are unit-tested; no `/dev/kvm` on this host. The user deferred this to a future Linux-VM session on 2026-09-03. `ws.moved.processes=preserved` is now fail-closed (defect 1). |
| 2.4 node pools | implemented and verified | `TestE17PoolClaimScalesUpAndIdleScalesDown`; `pool.scaled` and `pool.provision_failed` are durable |
| 2.5 secrets at rest | implemented and verified | encrypted tenant artifacts; snapshot fuzzing proves `.remount/env` and placeholder values never enter a snapshot |
| **Gate 2** | **not met** | E4 and E5 cannot run without gVisor. This is unavailable, not passing. |

### Phase 3

| Item | Status | Notes |
|---|---|---|
| 3.1 approve-on-first-use | implemented and verified | E6 |
| 3.2 budgets and rate limits | implemented and verified | E7; reserve-then-settle against the control-plane counter |
| 3.3 response redaction | implemented and verified | `internal/broker` suite; `egress.redacted` |
| 3.4 identity | implemented and verified | E8 in sim and integration |
| 3.5 external secret sources | implemented and verified against fakes | `env://` and `file://` are real; Vault, AWS SM and GCP SM are verified only against fakes and must stay labelled as such |
| 3.6 tenancy and metering | implemented and verified | per-tenant quotas, `quota.exceeded`, usage aggregation, tenant-scoped visibility |
| 3.7 webhooks in, notifications out | implemented and verified against fixtures | real Slack signature and delivery remain externally gated |
| **Gate 3** | met, with the noted fake-source limits | E6, E7, E8 pass; production modes start with zero shared tokens |

### Phase 4

| Item | Status | Notes |
|---|---|---|
| 4.1 chunked snapshots | implemented and verified | E9. `push --chunked` now exists, closing the wrap handoff's "push still emits the legacy tar representation" gap. |
| 4.2 tiered session log | implemented and verified | E11, plus retention pruning now emits `session.log.deleted` per row |
| 4.3 S3-compatible blob store | implemented and verified against MinIO | non-AWS conditional writes and multipart remain externally gated |
| 4.4 control-plane durability and failover | implemented and verified | E10; controller epochs fence a stale writer; a lost RPO window is recorded as `control.recovered` |
| 4.5 | n/a | reassigned to the already-delivered item 0.9 |
| 4.6 shared data volumes | implemented and verified | `internal/volume` and sim lifecycle suites |
| **Gate 4** | met | E9, E10, E11 pass; `doctor --deep` verifies chunked and encrypted artifacts |

### Phase 6.4 scale evidence

`TestHandoffScaleAndControlFailover` reproduced on this host in **37.9 s**: 200
real protocol nodes and clients, 2,000 workspaces claimed, one exec and one
move per node, the control plane restarted over the shared SQLite store, 400
peers reconnected, and byte-exact session completion with no gap and no
generation mismatch. Recorded p50/p99 for claim, exec round trip, move and
reattach are in `bench/results/scale-process-local.json`.

**Honest limit.** This is control/protocol scale evidence on the *process*
backend. Item 6.4 asks for the same numbers on the docker and gVisor backends.
The Docker backend is proven functional here (`TestDockerBackendReal`,
`TestDockerWorkspaceIfAvailable`, and the live E1/E2/E3 runs), but the 200-node
matrix has not been re-run under it, and gVisor cannot run on this host at all.
Do not quote these numbers as isolation-backend performance.

### Residual gaps carried forward

| Gap | Owner | Exact next proof |
|---|---|---|
| gVisor E4/E5 denial conformance | isolation | register `runsc` on a Linux host and run the seven-check suite at connect time |
| Firecracker KVM end to end | deferred to a Linux-VM session | boot through the real jailer and vsock bridge, checkpoint a running guest, move it, restore disk/state/memory, and prove the process continues exactly once; then and only then may a destination report `processes=preserved` |
| Real vendor pool runs (E2B, Modal, Fly, ix, SSH) | external verification | a built node template, a publicly reachable control-plane URL, an enrollment token and a binary URL; record in `verification-2026-09.md` |
| E12 publication | distribution | install from a published PyPI/npm artifact, not a local directory |
| E13 two real MCP harnesses | distribution | run with two harness keys present; absence stays an explicit skip |
| E14 real GitHub App | integrations | register an app and exercise clone plus push through `/d/github.com` |
| Docker/gVisor scale matrix | performance | re-run the 200-node scenario under the docker backend on a named host |
| E18 signed release | **withheld pending release authority** | not to be performed without an explicit instruction |
| Independent architecture and security review | external | the closing self-review does not substitute for it |

### Phase 5

| Item | Status | Notes |
|---|---|---|
| 5.1 exports | implemented and verified | E16; OTLP, S3 hourly JSONL, stdout JSONL and SIEM, with a durable `export.cursor` |
| 5.2 console | implemented and verified, **now browser-to-real-server** | see below |
| 5.3 SDKs | implemented and verified locally; publication externally gated | both packages install and build from the tree and `cmd/protogen --check` enforces type drift. E12 asks for `pip install remount` / `npm i @remount/sdk` from a real index; a local install is not publication. |
| 5.4 MCP | implemented and verified | `remount mcp serve` / `wrap`; two defects fixed this pass (see defects 3 and 4). E13's two real harnesses remain an explicit skip. |
| 5.5 orchestrator adapters and examples | implemented and verified | Temporal, LangGraph, OpenHands and GitHub Action examples with smoke tests |
| 5.6 docs, releases, benchmarks | partially met | `bench/` publishes `docs/benchmarks.md` with numbers and the measured commit. The signed release (E18) was **not performed, by decision**. |
| **Gate 5** | **not met** | E15 and E16 pass; E12 publication, E13 two real harnesses and E18 remain open. The under-five-minute clean-machine README flow in a disposable VM was not performed. |

#### 5.2 / E15 — the browser-to-server claim is now real

The wrap handoff recorded that "the browser DOM/Playwright suite still uses
mocked HTTP responses" and listed "the browser mock suite is not a
browser-to-real-server proof" as a standing honesty constraint. That constraint
is now discharged.

`web/tests/mock-server.mjs` and the `page.routeWebSocket` version of the
operator spec are **deleted**. Playwright drives the real `remount` binary
through a fixture in `web/tests/e15/`, in two projects:

- a `standalone` project with two real nodes, and
- an `rbac` project in `production-single-tenant` mode with real
  bootstrap-minted bearers.

Seven tests pass in 7.4 s. What is genuinely real: browser-driven workspace
create reaching `claimed`; a real PTY over
`ws://…/v1/console/workspaces/{id}/terminal` carrying real chunk frames;
server-side replay on re-dial with `since=`; file read/write with `If-Match`
verified out of band; a real `art_sha256:` snapshot; a real cross-node move
(the second node's log shows `gen=2 … restore=art_sha256:…`); RBAC for
operator, viewer and a credential the server never issued; a real
`leak_blocked`; and a real parked approve-mode egress released only after the
browser commits the decision, confirmed against the upstream's own hit log.

The leak scan has a positive control: a second synthetic string is planted in a
file the browser *is* meant to read, and response bodies, base64-decoded
WebSocket frames, DOM text and form-field values must each demonstrably contain
it before the secret's absence counts for anything. That control was verified
by mutation.

One console defect was fixed in passing: `ErrorNotice error={state.error ??
mutationError}` meant a denied *mutation* was invisible whenever the page's own
load had also failed — precisely the viewer's situation, so the RBAC denial the
test needed to observe was unobservable. The two notices now render separately.

Remaining honest gaps in this lane:

- successful credential substitution (`cred.used`) is **not** browser-driven.
  The broker refuses credentials over plaintext HTTP and its upstream TLS trust
  is in-process only, with no CLI flag to trust a locally generated CA. The
  browser suite therefore proves the *blocked* path plus a real binding lease
  and a placeholder-only workspace view; the substituted-and-allowed path stays
  covered by `TestConsoleE15SimulatedOperatorFlow`;
- the production-mode fixture runs no node, so RBAC covers the console surface
  rather than PTY and lifecycle under a role;
- Chromium only, plaintext `127.0.0.1` only, so `wss:` is unexercised.

### Phase 6

| Item | Status | Notes |
|---|---|---|
| 6.1 production-multi-tenant end to end | implemented and verified | cross-tenant negative tests for events, artifacts, attach, approvals and usage; per-tenant blob prefix and key |
| 6.2 operator RBAC and SSO | implemented and verified; real WorkOS externally gated | roles enforced across CLI, console and API; OIDC discovery, RFC 8628 device flow, RS256/JWKS verification, group-to-role mapping, and an audit event for every role change. Simulated OIDC is not WorkOS evidence. |
| 6.3 retention and residency | see the retention section | |
| 6.4 load and chaos evidence | implemented and verified on the process backend | see the scale section above; docker and gVisor matrices remain open |
| **Gate 6** | partially met | E17 and the cross-tenant negative suite pass; this residual-risk register is rewritten below |

#### 6.3 — retention, residency and compliance export

The wrap handoff recorded that `Retention.Events` and `Retention.Artifacts`
were stored and validated but never read, that no doctor finding existed for
retention or residency, and that `remount audit export` did not exist at all —
`internal/compliance` was a complete, tested library with no CLI, no protocol
operation and no server wiring.

The design problem was real and is worth recording, because the obvious
implementation would have been wrong. The canonical event log is one sequence,
and retention deletes only a contiguous oldest prefix. `internal/compliance`
depends on that: a hole punched into the middle of the sequence would make an
export that skipped it look *complete* rather than gapped. Naive per-tenant
deletion would therefore have silently converted a compliance gap into a
clean bundle — the exact failure the export engine exists to prevent.

The implemented answer is two mechanisms rather than one:

1. The contiguous prefix is deleted at the **oldest** cutoff any tenant still
   requires, never at a single tenant's own. A ninety-day tenant therefore
   holds rows a seven-day tenant would rather have gone. No tenant's rows are
   ever removed before that tenant's own policy allows, which is precisely the
   property that stops one tenant's policy from destroying another's evidence.
2. Between a tenant's own cutoff and that global floor, the tenant's event
   **content** is removed in place. The envelope — sequence, time, type,
   tenant — survives, so the sequence stays contiguous and a reader still
   learns that something happened, while the payload becomes a constant
   redaction marker. This is what makes `Retention.Events` delete on the
   tenant's own schedule instead of the slowest tenant's schedule.

Artifacts are the mirror image. `Retention.Artifacts` is a maximum age, so it
may only *accelerate* collection relative to the shared grace window and never
delay it, and it can never select a referenced object at any age: a live
workspace, base, fleet or session-log closure outranks a retention policy, and
an artifact still held by one is reported as a **violation** rather than
deleted. Both passes are batched and bounded so a large backlog cannot starve
other tenants or the prune that follows.

Residency was already enforced at the claim boundary before this pass and was
not redone. What was added is the diagnostic surface: retention and residency
violations are now doctor findings, and a residency check that cannot run
reports unavailable rather than healthy, following the existing
`artifact.verify_unavailable` pattern.

**Status: implemented and verified.** ADR 0080 records the design.

Wire path, verified by hand against a real `production-single-tenant` server on
this host rather than only in tests:

| Command | Result |
|---|---|
| `remount audit key` | returns the key id, `ed25519`, and the public half only |
| `audit export --tenant '*'` | refused — `*` is never a valid export subject |
| `audit export --range 1..40` over 5 events | refused as `evicted`, "range is no longer complete", never a shorter bundle |
| `audit export --tenant acme --range 1..3` | signed JSONL bundle with `payload_sha256` over exactly the event bytes |
| `audit verify` with the published key | verified, offline, naming tenant, range and key id |
| `audit verify` after editing `range_to` | refused, "manifest signature is invalid" |
| `audit verify --tenant other` | refused, "manifest fields are invalid" |

The refusal path emits `audit.export_denied`, so a denied export is itself in
the log.

Tests: `TestRetentionPlanFloorIsTheOldestTenantRequirement`,
`TestRetentionPlanArtifactCutoffOnlyAccelerates`,
`TestTenantRetentionNeverTouchesAnotherTenant`,
`TestTenantRetentionEmitsAnEventAndAMetric`,
`TestResidencyDriftEmitsAnEventAndAMetric`,
`TestResidencyCheckThatCannotRunIsUnavailable`,
`TestRetentionFindingsNameTheEnvelopeLimit`,
`TestSessionLogRetentionNeverExpiresAnotherTenant`,
`TestAuditSigningKeyIsDurableAndPrivate`,
`TestAuditExportCannotCrossTenants`,
`TestAuditExportIsDeterministicAndVerifies`,
`TestAuditExportRefusesAnIncompleteRange`,
`TestAuditExportRangeValidation`,
`TestRedactTenantRemovesOnlyThatTenantsContent`,
`TestRedactTenantPreservesSequenceContiguity`,
`TestTenantRetentionCollectsOnlyItsOwnNamespace`,
`TestTenantRetentionNeverCollectsAReferencedObject`, and the sim proofs
`TestE63AuditExportIsolatesTenantsAndIsDeterministic`,
`TestE63RetentionRemovesOneTenantsContentAndKeepsTheRangeHonest`,
`TestE63ArtifactRetentionPreservesALiveClosure`.

`doctor --deep` now labels canonical manifest validation and closure
completeness separately from a digest rehash
(`artifact.manifest_canonical`, `artifact.closure_complete`, and
`artifact.closure_unavailable` when the walk cannot run), closing the
last §2.3 residual.

**Residual limit, deliberately visible:** redaction preserves envelopes, so a
tenant whose retention has elapsed still leaves sequence, time, type and tenant
rows behind until the global floor advances. `doctor` reports that as
`tenant.retention_violation` naming the tenant holding the prefix. It is a
consequence of one contiguous sequence, not an oversight, and it is reported
rather than hidden.
