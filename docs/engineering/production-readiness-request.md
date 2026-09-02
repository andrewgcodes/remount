# Engineering Request: bring Remount to production-readiness

**Status:** proposed  
**Audience:** senior engineer / technical lead  
**Written:** 2026-09-02  
**Target:** evidence-backed 10/10, not “looks complete”  

## 1. Executive assessment

Remount has a strong architectural thesis and an unusually coherent resource
model for an early codebase. Its best decisions are:

- a workspace is a logical resource rather than a machine;
- a session is a durable log rather than a live socket;
- the relay forwards frames without owning session semantics;
- artifacts are content-addressed files;
- nodes dial outward and may run different backends;
- credential use is intended to be mediated outside the workspace;
- placement, lifecycle, transport and harness integration are separated; and
- simulation and fault injection are part of the design rather than an
  afterthought.

The implementation is not yet production-safe. Current documents sometimes
describe design intentions as guarantees, while the process backend has no
isolation, Docker egress is not enforced, identity is too flat, some lifecycle
paths fail open, and the race-enabled suite has found production races.

Point-in-time score:

| Dimension | Current | Why it is not 10 |
| --- | ---: | --- |
| Architecture and design judgment | 8.5 | Strong model; implementation contradicts some invariants |
| Interface design and Go idiom | 7.5 | Good package boundaries; lifecycle and backend contracts are incomplete |
| Comments and readability | 8.0 | New docs are substantial; some claims and comments overstate guarantees |
| Test strategy | 7.0 | Good package/simulation tests; critical direct, adversarial and upgrade tests are missing |
| Security | 5.0 | Good intent; isolation, egress, auth and broker guarantees are not enforced |
| Concurrency and reliability | 5.5 | Useful patterns, but race detector and fail-open lifecycle paths remain |
| Operations and release engineering | 6.0 | Useful operations guide; HA, migrations, observability and CI gates are immature |

A 10/10 does not mean mathematically perfect or vulnerability-free. It means
the project makes narrow promises, enforces them, tests the failure modes, and
ships enough operational evidence for another team to trust it.

## 2. Release gates

Work through the gates in order. Do not start polishing later gates while an
earlier safety gate is red.

### Gate 0: establish a trustworthy baseline

Required outcome:

- repository builds from a clean checkout;
- `go test ./...` and `go test -race ./...` are deterministic and green;
- Linux, macOS and supported cross-compilation targets are explicit;
- CI runs formatting, vetting, tests, race tests, static analysis and dependency
  scanning;
- every current race or failing test has an owner and regression test;
- `.env`, keys, bindings, state, artifacts and build outputs are ignored; and
- the README accurately states what is implemented and what is experimental.

No production-readiness claim may be made while Gate 0 is red.

### Gate 1: make the lifecycle correct

Required outcome:

- claims, readiness, renewals, fencing, release, move, sleep and wake form an
  explicit state machine;
- every transition has a durable operation ID and expected generation;
- retrying an operation is idempotent;
- stale messages cannot resurrect or mutate a newer generation;
- node disconnect, control restart and message duplication converge; and
- a failed durability step cannot destroy the only good workspace copy.

### Gate 2: enforce the trust model

Required outcome:

- authenticated, server-authoritative subjects and ACLs;
- node proof-of-possession and approved capabilities;
- backend-specific security requirements;
- per-workspace authenticated credential mediation;
- default-deny, non-bypassable production egress;
- artifact restore hardening; and
- authoritative event attribution.

The full specification is in
[`openai-huggingface-hardening-sdmr.md`](./openai-huggingface-hardening-sdmr.md).

### Gate 3: prove durability and scale

Required outcome:

- persistent state has a documented schema and migration policy;
- snapshots, event logs and session spill have integrity checks and quotas;
- crash recovery is tested at each durable write boundary;
- slow clients and resource exhaustion cannot stop unrelated traffic;
- load targets are measured rather than inferred; and
- backup restore is rehearsed.

### Gate 4: make it operable

Required outcome:

- metrics, structured logs, traces and audit export;
- health, readiness and dependency checks;
- documented SLOs and alert thresholds;
- safe shutdown and bounded drain;
- admin incident controls;
- rolling upgrade and rollback procedure;
- capacity model and quotas; and
- runbooks validated in a staging exercise.

### Gate 5: stabilize the public product

Required outcome:

- versioned protocol compatibility policy;
- stable client API outside `internal/`;
- clear CLI behavior, exit codes and machine-readable output;
- complete quickstart and reference docs;
- security guarantees and non-guarantees stated precisely;
- release artifacts, provenance, changelog and support policy; and
- independent security and architecture review.

## 3. P0 engineering work

P0 items block production use or can cause data loss, split brain, security
boundary failure or process-wide crashes.

### P0.1 Eliminate all known data races

#### Relay correspondence map

`internal/relay/relay.go` writes `recent` map entries while holding `RLock` and
splits logically atomic work across read and write critical sections. Put the
lookup and update under one write lock or give correspondence state its own
lock. Test concurrent route, disconnect, duplicate identity and replacement.

#### Control claim state

`internal/control/control.go` reads workspace state after unlocking while
forming a conflict error. Copy all fields required by a response before unlock.
Audit every `Unlock` followed by access to aliased state.

#### Server readiness

`internal/server/server.go` publishes listener/server fields from `Serve` while
another goroutine polls `Addr`. Replace polling with one synchronization event
that returns address or startup error.

#### Client event shutdown

`internal/client/client.go` can close a public event channel while delivery is
still possible. Give the delivery goroutine sole ownership of closing the
channel. Cancellation must unregister first and await or fence senders.

#### Test-only races

Tests must not read a `bytes.Buffer`, fake connection or other mutable fixture
while a goroutine writes it. Fix tests rather than suppressing the race
detector.

Acceptance:

- 100 consecutive `go test -race ./...` runs pass in CI or an equivalent
  stress job;
- every fixed race has a focused regression test; and
- no synchronization failure is handled with sleeps.

### P0.2 Formalize the workspace state machine

Write one transition table in the protocol specification and implement it in
one control-plane transition function.

Suggested states:

```text
pending
claiming
claimed
quiescing
checkpointing
releasing
paused
destroying
destroyed
failed
```

Every transition checks:

- current state;
- expected generation;
- authoritative node;
- operation ID;
- actor permission; and
- legal predecessor.

`ws.ready` must not promote a released workspace. Duplicate claim messages must
return the same operation result without starting a second materialization.
Reconnect reconciliation must be explicit.

Acceptance:

- property tests generate reordered, duplicated and delayed lifecycle frames;
- stale generations never become serviceable;
- exactly one node is serviceable for each workspace; and
- every operation reaches a terminal or operator-actionable state.

### P0.3 Add lease self-fencing

Control-plane lease expiry currently permits the conceptual possibility that a
workspace is reassigned while the old node continues serving. The node must
maintain a local safety deadline and stop serving without waiting for a remote
fence command.

Requirements:

- renewal returns a result per workspace and generation;
- rejected or missing renewal fences only the affected workspace;
- client grants expire no later than useful assignment authority;
- network access is disabled before or with process suspension;
- reassignment advances the generation; and
- reconnect does not resume execution until reconciliation succeeds.

Acceptance:

- partition the old node, advance lease time and assign a new node;
- prove the old node rejects execution, attach, filesystem and egress;
- prove stale grants fail at both control and node; and
- run this under fake time, race detector and process-level integration tests.

### P0.4 Make checkpointed operations fail safely

`internal/node/node.go` currently logs snapshot-on-release failure and proceeds
to destroy. That converts a transient storage failure into data loss.

Implement a two-phase release:

1. quiesce and checkpoint;
2. upload, verify and commit the artifact reference;
3. advance ownership/fence state; and
4. authorize source destruction.

If a required snapshot fails, keep the source quarantined. Explicit snapshots
must update `LastSnapshot` through an acknowledged control-plane commit.

Acceptance:

- inject failures at archive, write, upload, digest verification, event append,
  control commit and destroy;
- the most recent acknowledged workspace state always survives;
- retries do not create ambiguous ownership; and
- operations expose a durable failure reason.

### P0.5 Fix session-log integrity

Requirements:

- reserve and commit `StreamInfo` as sequence zero before output pumps;
- never ignore log append errors;
- propagate spill file truncate/seek/write/sync failures;
- define and enforce maximum memory, spill bytes and subscriber lag;
- use a documented gap/error record if data cannot be retained;
- preserve stream and global ordering semantics; and
- clean up spill files according to an explicit retention policy.

Acceptance:

- commands that write immediately still have sequence-zero metadata;
- disk-full and permission failures are visible to clients;
- slow subscribers do not block the process or unrelated sessions;
- cursor behavior is tested across ring eviction and spill rotation; and
- crash recovery either replays valid data or declares an exact gap.

### P0.6 Enforce or narrow the security claim

Until the security SDMR is implemented:

- label `process` as trusted local only;
- do not describe Docker as a multi-tenant boundary;
- describe proxy settings as cooperative credential mediation, not enforced
  egress;
- disable unsupported production profiles at startup; and
- keep multi-tenant and incident-prevention claims out of the README.

This documentation correction is P0 because overclaiming a missing boundary can
cause unsafe deployment.

## 4. P1 architecture and interface work

P1 items are required for a mature, extensible system but can follow the
correctness gate.

### P1.1 Split backend contracts by responsibility

The current `Backend`/`Handle` shape is small, but a microVM or remote backend
needs more lifecycle semantics than create/adopt/destroy.

Prefer capability-specific interfaces:

```go
type Provisioner interface {
    Create(context.Context, WorkspacePlan) (Handle, error)
    Adopt(context.Context, AdoptionPlan) (Handle, error)
}

type Checkpointer interface {
    Quiesce(context.Context) error
    Snapshot(context.Context, SnapshotWriter) (SnapshotManifest, error)
    Restore(context.Context, SnapshotManifest) error
}

type Fencer interface {
    Fence(context.Context, FenceReason) error
    Fenced() bool
}

type NetworkController interface {
    ApplyPolicy(context.Context, NetworkPolicy) error
    Revoke(context.Context) error
}
```

Avoid one interface whose optional methods are discovered through type
assertions. Use explicit capability negotiation and conformance suites.

### P1.2 Create a stable public client package

Anything under `internal/` cannot be imported by an external Go module. Move
the intended SDK surface to a versioned public package such as:

```text
remount.dev/remount/client
remount.dev/remount/api
```

Keep wire structures separate from ergonomic client structures when evolution
needs differ.

Requirements:

- context on every blocking call;
- caller-supplied or operation-object idempotency key;
- typed errors with stable codes;
- explicit stream ownership and close semantics;
- retry policy documented and bounded;
- no background goroutine leak on cancellation; and
- compatibility tests against supported server versions.

### P1.3 Repair idempotency semantics

Generating a new idempotency key inside each API call does not help a caller
recover an ambiguous result after process or network failure.

Provide one of:

```go
CreateWorkspace(ctx, spec, WithIdempotencyKey(key))
```

or:

```go
op := client.NewCreateWorkspaceOperation(spec)
result, err := op.Run(ctx)
```

The key belongs to the logical operation and remains stable across retries.
The server stores a request fingerprint and rejects key reuse with different
arguments.

Apply the same rule to snapshot, move, sleep, wake, destroy and other mutating
operations.

### P1.4 Make protocol evolution real

The protocol contains a version field, but compatibility needs behavioral
rules:

- supported version range;
- capability names and semantics;
- required versus optional capability handling;
- unknown operation and unknown field behavior;
- maximum frame size and payload limits;
- request deadline/cancellation semantics;
- retryability per error code;
- deprecation window; and
- golden fixtures for every supported version.

Add strict decode limits before allocating attacker-controlled payloads.

### P1.5 Make persistence the actual source of truth

The event log says state elsewhere is a cache, but current control structures
are authoritative in memory with selective persistence. Choose one accurate
architecture:

1. event-sourced control state reconstructed from a canonical durable log; or
2. transactionally persisted control tables plus an append-only audit log.

Do not claim the first while implementing the second.

For the pragmatic second option:

- persist workspace, assignment, generation, operation and idempotency state in
  one transaction;
- append an outbox/audit record transactionally;
- deliver events from the outbox;
- recover timers and in-progress operations on startup; and
- define schema migrations and backup consistency.

### P1.6 Harden artifact format and restore

Requirements:

- validate symlink and hard-link targets;
- reject absolute paths, traversal and device nodes;
- prevent write-through of pre-existing symlink parents;
- extract into a fresh staging directory;
- enforce expanded byte, file count, path length and nesting limits;
- verify digest before extraction;
- define ownership, mode, xattr and timestamp behavior;
- fsync file and directory boundaries before commit;
- atomically rename the validated tree into place; and
- use a versioned manifest.

Deterministic `.tar.gz` is useful but reproducibility must be tested across
platforms and Go versions.

### P1.7 Decouple transport reading from handlers

`Peer.readLoop` should only decode, validate, correlate and enqueue. It should
not let one slow callback block all response, event and chunk handling.

Add:

- bounded request and stream queues;
- maximum pending requests;
- maximum frame and aggregate queued bytes;
- handler worker policy;
- overload errors;
- per-stream cancellation;
- idle and heartbeat timeouts; and
- a clear close reason propagated to waiters.

### P1.8 Remove Unix assumptions from shared files

Move process-group creation, signaling, terminal resize and platform-specific
behavior behind build-tagged files. Decide whether Windows is supported. If it
is not, fail with a documented platform constraint rather than maintaining
partial files that imply support.

### P1.9 Correct subscriber lifecycle

The node must track subscriptions by identity and by workspace/session.
Replacement cleanup compares the exact subscriber object. Releasing a
workspace cancels every related subscriber. Add leak tests using goroutine
counts or deterministic ownership instrumentation.

### P1.10 Introduce dependency injection at trust boundaries

Make time, identity verification, authorization, storage, backend registry,
network policy and event append explicit dependencies. This enables fake-time
state-machine tests without creating global hooks or sleeping.

Avoid introducing abstraction merely for style. Add an interface where:

- another implementation is required;
- fault injection is required; or
- the boundary has independent security semantics.

## 5. P1 security completion

Implement all requirements in the security SDMR. The minimum production set is:

- backend-specific security capabilities;
- explicit workspace security profiles;
- server-authoritative principal and tenant;
- per-resource ACL enforcement;
- node proof-of-possession;
- short, scoped grants;
- authenticated per-workspace broker;
- mandatory egress boundary;
- immutable or isolated shared caches;
- controlled `port.open`;
- authoritative event metadata;
- fleet quarantine; and
- independent review.

Security tests must run from inside the hostile workspace, not merely unit-test
policy helper functions.

## 6. P1 test strategy

### Direct package tests

Add direct tests for:

- `internal/control`: every state transition, ACL, idempotency and timer;
- `internal/node`: reconciliation, claim serialization, fencing, subscriber
  cleanup and broker lifecycle;
- `internal/relay`: routing, replacement, disconnect and backpressure;
- `internal/client`: retries, cancellation, channel closure and stale grant;
- `internal/server`: startup, shutdown, readiness, auth and webhook handling;
- `cmd/remount`: argument parsing, exit codes and machine output.

Simulation tests are not a substitute for package tests because a simulation
can miss unsafe local interleavings.

### State-machine and property tests

Generate event sequences with:

- duplicate, delayed, lost and reordered frames;
- disconnect during every transition;
- control restart;
- node restart with and without local workspace state;
- artifact failure;
- event append failure;
- timer overlap; and
- generation overflow boundaries.

Assert invariants, not one expected happy-path sequence.

### Fuzzing

Fuzz:

- CBOR frame decode and operation payload decode;
- snapshot extraction;
- path resolution and edits;
- broker URL/host/header parsing;
- grant parsing and signature verification;
- ID parsing;
- session cursor ranges; and
- lifecycle command sequences.

Seed fuzzers with real protocol and artifact fixtures.

### Integration tests

Run real:

- WebSocket client/server pairs;
- process and Docker backends;
- broker networking from inside Docker;
- SQLite restart and migration;
- filesystem permission and disk-full failures;
- node process termination and reconnect;
- TLS and authentication rotation; and
- move across two nodes.

### Adversarial security tests

Attempt:

- principal spoofing;
- stale generation use;
- cross-workspace attach and broker calls;
- proxy bypass over IPv4/IPv6/UDP/DNS;
- metadata and localhost access;
- cache-based cross-workspace signaling;
- symlink/hard-link archive escape;
- event-origin forgery;
- oversized frame and decompression exhaustion; and
- fleet quarantine evasion.

### Performance tests

Measure:

- relay throughput and tail latency;
- attach replay at multiple log sizes;
- event subscription fan-out;
- control operations at target node/workspace count;
- snapshot time and throughput;
- broker concurrency;
- memory per idle and active session; and
- reconnect storms.

Publish hardware, data size, concurrency and percentile—not one laptop number
without context.

## 7. P1 observability and operations

### Metrics

At minimum:

- connected nodes and clients;
- workspaces by state/backend/profile;
- claim, ready, move and release latency;
- lease renewal lag and fencing count;
- snapshot bytes, latency and failure stage;
- session in-memory and spill bytes;
- subscribers and dropped/lagged streams;
- relay queue bytes and rejected frames;
- broker decisions by rule/result;
- event append latency and sequence gaps;
- artifact disk usage;
- goroutines and process handles; and
- fleet operation convergence.

Use bounded-cardinality labels. Workspace IDs belong in logs/traces, not broad
metric dimensions.

### Structured logs and traces

Every operation should carry:

- operation ID;
- request ID;
- workspace and generation;
- node;
- authenticated actor/tenant;
- state transition;
- duration;
- result and stable error code.

Do not log credentials, substituted headers or unredacted environment values.

### Health model

Separate:

- liveness: process/event loop is alive;
- readiness: can safely accept new work;
- degraded: can serve existing sessions but a dependency is impaired; and
- security readiness: required isolation, auth and egress controls are active.

### Backup and recovery

Document and rehearse:

- SQLite-consistent backup;
- artifact-store backup and reconciliation;
- node identity recovery;
- restore into a fresh control-plane instance;
- lost-node handling;
- corrupt artifact handling; and
- recovery-point and recovery-time objectives.

### High availability

The current single-stateful-controller shape is acceptable for an early
deployment if documented honestly. For a 10/10 production target, choose:

- active/passive controller with one writer and durable failover; or
- transactional shared database with leader election and fencing.

Do not put multiple independent SQLite controllers behind a load balancer.

### Quotas and garbage collection

Define quotas for:

- workspaces and sessions per subject;
- concurrent processes;
- pending RPCs and stream bytes;
- session log memory/spill;
- artifact bytes and count;
- broker requests and connections;
- event retention; and
- snapshot frequency.

Garbage collection must be reference-aware, resumable and observable. A crash
during GC cannot remove a referenced artifact.

## 8. P1 documentation and readability

### README

Replace the one-line README with:

1. one-sentence purpose;
2. the problem and differentiators;
3. current maturity warning;
4. architecture diagram;
5. five-minute local quickstart;
6. verified example;
7. implemented versus planned matrix;
8. security model and non-guarantees;
9. links to tutorial, design, protocol, operations and ADRs; and
10. contribution/release status.

### Keep claims synchronized with code

For each strong statement—“log is truth,” “workspace is trusted with nothing,”
“egress denied,” “crash-only”—add a conformance test or narrow the wording.

Documentation should distinguish:

- invariant;
- implemented behavior;
- tested guarantee;
- operational recommendation; and
- roadmap.

### Package comments and exported identifiers

Add package documentation for every package and GoDoc for the future public
API. Comments should explain invariants and ownership, not restate syntax.

Particularly document:

- which goroutine owns a channel and closes it;
- what a mutex protects;
- whether methods are concurrency-safe;
- retry and idempotency behavior;
- durable write boundaries;
- security authority for each field; and
- context cancellation effects.

### ADR follow-through

The ADRs are useful. Add ADRs for unresolved decisions:

- authoritative state model;
- production isolation profiles;
- identity and multi-tenancy;
- lease fencing;
- two-phase release;
- protocol compatibility;
- control-plane availability; and
- public SDK boundary.

An ADR records a decision; it must not substitute for tests or implementation.

## 9. P2 engineering quality

### Error taxonomy

Create stable error classes for:

- invalid input;
- unauthenticated;
- unauthorized;
- conflict/stale generation;
- unavailable/retryable;
- capacity exhausted;
- deadline;
- data loss/integrity;
- unsupported capability; and
- internal error.

Preserve causes internally while redacting sensitive details on the wire.
Document which mutations are safe to retry.

### Resource ownership

For every goroutine, timer, channel, file, listener, process and temporary
directory, define:

- creator;
- owner;
- cancellation trigger;
- closer;
- whether close is idempotent; and
- shutdown deadline.

Add leak tests for repeated connect/disconnect, session attach/detach, failed
claims and broker start/stop.

### Configuration

Validate configuration once at startup. Separate:

- public bind address;
- advertised address;
- control/data TLS;
- auth provider;
- state/artifact paths;
- backend definitions;
- security mode;
- quotas;
- retention; and
- observability endpoints.

Support secret references rather than encouraging raw credentials in command
lines or environment dumps.

### Dependency and supply-chain policy

- pin toolchain and direct dependencies;
- run vulnerability scanning;
- generate an SBOM;
- sign release artifacts;
- publish checksums and provenance;
- configure automated dependency updates;
- document minimum supported Go version; and
- review risky parsing/network dependencies.

### Performance engineering

Profile before changing data structures. Establish target scale first:

- nodes;
- active and idle workspaces;
- sessions per workspace;
- session output rate;
- attached clients;
- event rate;
- artifact size; and
- control-plane recovery time.

Then build repeatable benchmarks and a capacity envelope.

## 10. Suggested ticket sequence

### Batch A: race and integrity

1. Relay correspondence locking.
2. Control state-after-unlock audit.
3. Server readiness synchronization.
4. Client stream/event ownership.
5. Session sequence-zero and spill errors.
6. Subscriber indexing and identity cleanup.
7. Race suite stability.

### Batch B: lifecycle

8. Transition function and operation IDs.
9. Claim/reconcile idempotency.
10. Affirmative renewal and self-fencing.
11. Two-phase release/checkpoint commit.
12. Restart reconciliation.
13. Fake-time/property tests.

### Batch C: trust boundary

14. Authenticator/authorizer and tenant model.
15. Node enrollment and proof-of-possession.
16. Scoped grants and revocation.
17. Backend security descriptors.
18. Security profiles and scheduler enforcement.
19. Artifact extraction hardening.

### Batch D: network and broker

20. Per-workspace broker identity.
21. Enforced network controller.
22. Typed connector policy.
23. Package connector with no shared writable namespace.
24. Bypass and sibling-channel conformance suite.

### Batch E: persistence and operations

25. Durable operation/state schema and migrations.
26. Transactional audit/outbox.
27. Authoritative event envelope.
28. Quotas and garbage collection.
29. Metrics/traces/runbooks.
30. Fleet quarantine.

### Batch F: product and release

31. Public Go SDK.
32. Protocol compatibility fixtures.
33. CLI contract and machine output.
34. README and support matrix.
35. Release pipeline, SBOM, signing and provenance.
36. Independent security review and release candidate exercise.

## 11. Evidence required for a 10/10 rating

### Architecture and design: 10/10

- state, ownership and trust models agree across code, protocol and docs;
- each capability has one authority and one lifecycle;
- alternative backends can conform without bypassing invariants; and
- major tradeoffs are documented in accepted ADRs.

### Interfaces and Go idiom: 10/10

- stable public API;
- small responsibility-based interfaces;
- explicit ownership/cancellation;
- typed errors and operation idempotency;
- platform-specific code isolated cleanly; and
- `go vet`, static analysis and API compatibility checks pass.

### Comments and readability: 10/10

- a new engineer can trace create, exec, reconnect and move end to end;
- concurrency and security invariants are documented next to enforcement;
- no aspirational statement is phrased as current behavior; and
- docs are tested or reviewed as part of feature changes.

### Test strategy: 10/10

- unit, simulation, integration, property, fuzz, adversarial, chaos, upgrade and
  performance coverage;
- deterministic fake time and fault injection;
- all critical invariants mapped to required tests; and
- failures retain artifacts/logs sufficient to reproduce.

### Security: 10/10

- production profiles pass hostile-workspace conformance;
- identity and authorization are resource-scoped;
- isolation and egress are enforced outside the workspace;
- credentials remain brokered and non-reusable;
- event attribution is authoritative;
- fleet containment is bounded; and
- an independent assessment has no unresolved critical/high findings.

### Concurrency and reliability: 10/10

- race suite and stress suite remain green;
- no unbounded queues or silent durability errors;
- partitions and restarts converge without split brain;
- all destructive actions are commit-gated; and
- leak and backpressure behavior are tested.

### Operations and release: 10/10

- SLOs, alerts, capacity limits and runbooks exist;
- backup/restore, incident containment and rollback are rehearsed;
- upgrades are compatible and reversible;
- releases are reproducible, signed and documented; and
- production support boundaries are clear.

## 12. Final acceptance checklist

The technical lead signs off only when all are true:

- [ ] Clean checkout builds with the pinned Go toolchain.
- [ ] Formatting, vet, static analysis and dependency checks pass.
- [ ] Unit and integration suites pass.
- [ ] Race and stress suites pass repeatedly.
- [ ] Fuzz targets meet the configured time budget with no crash.
- [ ] Lifecycle model is documented and property-tested.
- [ ] Lease partitions prove self-fencing.
- [ ] Move/sleep failure injection proves no acknowledged data loss.
- [ ] Production security profile rejects weak backends.
- [ ] Egress bypass and sibling-channel suites pass.
- [ ] Cross-tenant authorization suite passes.
- [ ] Artifact hostile-input suite passes.
- [ ] Control/node restart and rolling upgrade suites pass.
- [ ] Metrics, alerts and structured logs cover every critical operation.
- [ ] Backup restoration and fleet quarantine have been rehearsed.
- [ ] README and operations guide match the released behavior.
- [ ] Public API and protocol compatibility policy are versioned.
- [ ] Release artifacts include checksums, SBOM, signature and provenance.
- [ ] Independent architecture and security reviews are closed.
- [ ] Known residual risks are published.

When this checklist is complete, “10/10” means there is repeatable evidence
behind the score rather than confidence based on code shape alone.

