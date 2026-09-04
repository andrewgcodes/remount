# Remount codebase review and remediation handoff — 2026-09-04

**This is a review, not an implementation closure. No production code or executable
test files are changed by this handoff. Do not interpret merging it as fixing its
findings.**

## 1. Executive assessment

Remount addresses a real problem: giving an agent a computer whose lifetime,
placement, credentials, and observable execution are independent of the terminal
or harness that started it. Its strongest idea is the combination of a named
workspace, a replayable session log, generation-fenced ownership, and brokered
capabilities. That is substantially more useful than a remote `exec` wrapper.

The architecture is coherent, the dependency footprint is restrained, and the
repository invests unusually seriously in failure injection, explicit authority,
negative tests, and truthful operational evidence. The session-as-log model,
prepare/commit lifecycle, and distinction between resource truth and audit history
are sound foundations worth preserving.

**The implementation does not yet justify relying on all of its advertised safety
invariants.** Eleven focused invariant tests fail on the reviewed code. They expose
policy paths that skip governance, memory published before durable commit,
reservation accounting confused with upstream execution, a destructive decision
based on stale inventory, and reference/quota checks invalidated while locks are
released. These are substantive issues, not formatting preferences or objections
to SQLite.

The best current fit is a controlled internal deployment with an explicit threat
model, measured recovery requirements, and an operator who understands the chosen
backend. I would not approve an unattended hostile-workload service that depends
on the affected approval, hard-budget, scale-down, and transcript-durability
properties until those findings are closed. This is not a claim that every
workspace or every request is unsafe.

The product opportunity remains credible, but the competitive claim needs updating:
OpenSandbox and Daytona now document secret-blind outbound credential handling;
E2B documents host request transforms; Coder has an AI Gateway and Agent Firewall.
The defensible proposition is the **integrated, self-hostable, provider-neutral
workspace and recovery contract**, not simply “other sandboxes hold the keys.”

### Overall quality, without a misleading single score

| Dimension | Assessment | Why |
|---|---|---|
| Usefulness | Strong for portable, governed agent execution | Durable workspace identity, reconnect, checkpointing, and credential mediation solve distinct harness problems |
| Architecture | Strong core, increasingly broad surrounding platform | Explicit ownership boundaries; growing interaction surface across agents, pools, approvals, storage, identities, and SDKs |
| Implementation correctness | Mixed; important current defects | Eleven reproduced failed invariants, including irreversible-action and authorization boundaries |
| Security design | Thoughtful; enforcement is uneven across paths | Good defaults and honest backend descriptors do not compensate for connector governance bypasses |
| Testing | Strong methods, incomplete composition coverage | Ordinary and race short-mode suites pass while targeted boundary tests fail |
| Maintainability | Manageable Go code, but substantial concentration and duplication | Several very large coordinators; important invariants remain conventions callers must remember |
| Documentation | Rich reasoning and evidence, weak current-state navigation | Old “not built” and “fixed” statements coexist with newer implementations and findings |
| Operational maturity | Real tools and real historical deployments, not general qualification | Host-specific evidence, release/publication gates, and recovery objectives remain deployment-specific |

## 2. Provenance, scope, and evidence rules

### Candidate

- Initial checkout: clean `main`,
  `afea7428bc0cce668845e7a67472ceb7d3a3bf36`.
- Initial fetch: `origin/main` matched that commit.
- During review, upstream advanced to
  `1dca7730a25bb3c5e4ba9a78b8bf6b8f4122b747`, merging PR #14. The complete delta
  was reviewed: writable npm prefixes in three recipes and their regression test.
- The documentation branch was fast-forwarded to that upstream revision. None of
  the eleven finding paths changed. Targeted failures were rerun after integration.
- Host: macOS/Darwin 25.3.0, arm64, Go 1.27.1.
- This review made no live cloud deployments, provider deletions, paid model calls,
  identity changes, or credential reads. Provider destruction in RMR-001 is a fake
  driver recording a call, not a real machine deletion.

The three audit test sources already open in the editor at review start were used
as supplied evidence inputs, not presented as newly authored discoveries. Their
original basenames were `remount-audit-fd34775-{broker,connector,control}_test.go`.
They were read, executed through a Go overlay, checked against current production
paths, and reproduced under `-race`. Appendix A embeds them so the handoff does
not depend on this workstation's `/tmp` directory.

RMR-008 duplicates the already reported **RBH-001** in
[PR #7](https://github.com/andrewgcodes/remount/pull/7). It is a fresh reproduction
of an existing issue, not another independent defect to inflate the count.

### Scope

The review covered the resource model and normative protocol; control/node
lifecycle and persistence; broker, connectors, budgets and approvals; sessions,
transport and clients; filesystem, artifacts, volume and replication boundaries;
identity/isolation; SDK and console surfaces; CI/evidence tooling; and the
engineering ledgers. Source inspection, historical evidence, and fresh execution
are kept separate. This is a deep code review, not a proof of every source line,
an independent penetration test, or an audit of all external dependencies.

Use these evidence classes throughout:

- **Reproduced:** a local test exercised the forbidden outcome on the candidate.
- **Static confirmation:** current code demonstrates the described path, but this
  review did not execute its full end-to-end scenario.
- **Historical evidence:** a checked-in ledger records another exact candidate,
  host, and run. It is not promoted into a fresh pass.
- **Design tradeoff / unverified:** an explicit limitation or a question needing
  another proof, not a newly confirmed defect.

Severity is conditional on the named deployment and preconditions. “High” here
means a safety, authorization, durability, or destructive-action contract can
fail; it is not a claim of unauthenticated remote code execution.

## 3. What the system actually is

```text
CLI / Go / Python / TypeScript / MCP / browser / agent harness
                         |
              outbound WebSocket / HTTP
                         |
             server + relay + control authority
                  |                 |
            SQLite + outbox    artifact service
                  |
         claims / grants / generations / leases
                  |
             outbound node link
                  |
        supervisor + sessions + broker + artifact cache
                  |
       process | Docker | gVisor | Firecracker workspace
```

The relay and control handler are logically separate, but the default binary
colocates them. “The control handler does not interpret stdout” is not “session
traffic avoids the server” or “the relay cannot read stdout.” The server remains
a bandwidth, availability, and, without operational E2EE, confidentiality boundary.

### Ownership that should remain explicit

| Concern | Authority | Important distinction |
|---|---|---|
| Workspace assignment | Control's durable state and generation | A connected node is not necessarily entitled to serve |
| Local materialization | Node while its authority remains valid | A filesystem copy existing is not permission to use or delete it |
| Session output | Sequenced log and committed retained segments | Delivery, retention, and process exit are separate facts |
| Egress permission | Selected policy plus approval/budget authority | Matching a rule is not completing every governance gate |
| Reusable credentials | Trusted control/node/secret source | A placeholder removes secret possession, not the granted API power |
| Recovery references | Durable resource rows | A digest that verified a moment ago is not a committed GC root |
| Provider machines | Provider inventory plus live control authority | Tenant/node identity matching is necessary but not sufficient for safe destruction |

The root module has **71 Go packages** on the inspected host. The six direct Go
module dependencies are WebSocket, PTY, CBOR, `x/sys`, `x/term`, and pure-Go SQLite.
That is a notably restrained library footprint. It is not a correspondingly small
trusted computing base: custom lifecycle, authentication, storage, S3 signing,
protocol, policy, and recovery logic also need maintenance.

Source-size examples at the original candidate, including comments and blank lines:
`internal/control/control.go` 6,173 lines; `internal/node/node.go` 5,171;
`internal/broker/broker.go` 2,335; `internal/control/agents.go` 1,895. Size is not
itself a defect, but these are high-cost review and integration points. The
“small protocol / about thirty operations” language in `docs/design.md` no longer
conveys the breadth of the implementation.

### Current backend and continuity reality

| Boundary | Current implementation | Do not promise |
|---|---|---|
| Process | Host directory and subprocesses; cooperative broker use | Isolation from the host account or non-bypassable egress |
| Docker | Container execution and mounted workspace tree; cooperative proxy | That every direct network path is broker-enforced, or host Docker CLI termination proves in-container descendants stopped |
| gVisor | Built-in Linux backend and deny-first enforced-gateway integration | That it is merely roadmap code, or that its descriptor grants every stricter profile; its `Caps` does not set `MultiTenant` |
| Firecracker | Built-in probed jailer/network/volume/guest composition, `fs+mem` checkpoints | Arbitrary CPU/runtime/provider compatibility or survival of external TCP peers |
| Filesystem move | Portable files and identity between compatible backends | RAM, host-installed packages, external side effects, or architecture-specific binaries automatically moving |
| Client reconnect | Retained output replay with explicit gap semantics | Infinite retention or arbitrary workflow exactly-once semantics |
| Warm standby | Implemented conditional object-store lease, epoch, SQLite recovery shipping | Synchronous replication, zero RPO, or independently writable controllers |
| E2EE | Tested internal library, not wired into normal client/node dialing | Default relay-blind sessions |

Evidence: `internal/workspace/workspace.go:34-81`,
`internal/workspace/gvisor/gvisor.go:109-119`,
`internal/workspace/firecracker/firecracker.go:191-195`,
[ADR 0074](../adr/0074-warm-standby-control-plane.md), and
`internal/proto/proto.go:223-255`.

There is actual Firecracker KVM evidence in
[evidence/firecracker-kvm-2026-09-04.log](evidence/firecracker-kvm-2026-09-04.log)
for candidate `8d27d6d`, not this review's candidate. The current KVM workflow
runs the exact-host script; the old handoff's statement that its final step
intentionally fails is no longer current.

## 4. Design and engineering worth preserving

1. **A session is a log, not a socket.** Reusing cursor-based replay for live and
   reconnect paths is a good abstraction. Orphan buffering addresses the real
   ordering where chunks precede the open response. Bounded delivery queues avoid
   making every consumer's speed the transport reader's problem.
2. **Explicit authority transfer.** `claiming` versus `claimed`, `ws.ready`,
   generation-bound grants, post-tree-lock revalidation, and node self-fencing
   make subtle distributed-system states representable rather than timing guesses.
3. **Commit-gated destruction.** Prepare, quiesce, verify, commit, destroy is the
   right shape for moves and quarantine. The pool defect below shows why that
   principle must cover provider destruction too.
4. **Resource/event transactions.** `control.transact` and `eventlog.Transact`
   distinguish a failed SQL transaction from a committed outbox whose delivery is
   delayed. `DrainError` is deliberately treated as committed. This is good design;
   it does not automatically roll back mutable Go objects in callers.
5. **Capability-specific backend contracts.** Separating a guest filesystem from
   `HostFileSystem`, and advertising what a backend actually supports, prevents a
   container path or guest disk from accidentally becoming a host-path capability.
6. **A simulator with meaningful fault injection.** Real control/node/client code
   runs through cuttable transports. This is materially better than asserting
   mocks were called. The additional tests here use explicit barriers and failed
   commits, not sleeps intended to make a race likely.
7. **Integrity and scope in storage.** Content-derived artifact identity,
   deterministic snapshots, transactional restore, tenant/key-version encrypted
   storage, and reference-aware retention are sensible foundations. The gaps
   below concern the composition of those mechanisms, not a rejection of them.
8. **Evidence that can say unavailable.** The Plan B registry, positive-control
   leak scans, black-box conformance, native-host ledgers, and candid `MISTAKES.md`
   are valuable. The new local verification script should meet the same standard.
9. **Practical deployment shape.** Outbound node/client connections, a static
   binary, separate public Go module testing, and a functioning operator console
   lower real integration costs.

## 5. Reproduced finding register

| ID | Severity | Finding | Primary owner |
|---|---|---|---|
| RMR-001 | High | Pool scale-down can destroy a newly assigned node | control / pool |
| RMR-002 | High | Failed approval persistence remains visible as allowed | control / approvals |
| RMR-003 | High | Package and Git paths skip approve-mode authority | broker / connectors |
| RMR-004 | High | Package and Git paths skip authoritative budget admission | broker / budgets |
| RMR-005 | High | A workspace-controlled repeated key buys repeated upstream requests for one budget unit | broker / budgets |
| RMR-006 | High | Session-log commit can acknowledge a GC-deleted artifact | control / artifacts / sessions |
| RMR-007 | Medium | Concurrent session-log commits exceed the tenant record limit | control / capacity |
| RMR-008 | High | Failed agent report advances volatile watermark; retry acknowledges uncommitted progress | control / agents; existing RBH-001 |
| RMR-009 | Medium | Package cache reports the expected digest for same-size corrupted bytes | connector storage |
| RMR-010 | Medium | Cached package bypasses a tightened response-size rule | broker / connector storage |
| RMR-011 | Medium | Abandoned package staging is neither reconstructed nor charged after restart | connector storage |

All eleven failed once without `-race`, then on all three repetitions of the
focused race-enabled run. There were no race-detector diagnostics in that run:
these are logical ordering and authority defects, which a data-race detector need
not flag. The register counts top-level invariant tests; the connector tests each
contain both failing connector cases and a passing generic-proxy control.

### RMR-001 — Pool scale-down uses authority that can expire before destruction

**Reproduction:** `TestAuditPoolScaleDownFencesNewClaims`.

`control.reconcilePool` gets inventory, enriches it under `c.mu`, then calls the
provider reconciler without the control lock. `enrichPoolInventory` correctly
matches tenant, pool, and `remount.node`, and counts held workspaces. However,
`pool.scaleDown` later uses that copied count to select a victim and immediately
calls `driver.Destroy`. No claim fence bridges those two moments.

The test parks reconciliation after it captured an idle node, commits a new
`wsClaim` and `wsReady` on that node, then resumes. The fake provider records a
destroy while the workspace is held. The observed failure is:

```text
provider destroyed a node after a new workspace claim and ws.ready both committed
```

**Preconditions/impact:** configured pool reconciliation and idle scale-down, with
a new assignment during provider work. This can kill active execution and lose
work since the last authoritative checkpoint. An exact provider/node identity
match does not fix stale assignment authority.

**Boundary:** control owns placement; the resource is the provider machine and
its materializations; destruction is irreversible. A drain/retirement reservation
must durably fence new claims while provider work is in flight. Checking only
when `commitPoolResult` runs is too late: the provider has already destroyed it.
Holding the global mutex over network I/O is not the recommended fix either.

**Next proof:** deterministic concurrent claim-versus-retire test, abort/unfence
on provider failure, ambiguous provider result and controller restart, exact
node/tenant matching, and absence of provider I/O under the lease/control mutex.

**Source:** `internal/control/pool_reconcile.go:132-179`;
`internal/pool/reconciler.go:269-293`; [ADR 0064](../adr/0064-declarative-node-pools.md).

### RMR-002 — An uncommitted approval is consumed as permission

**Reproduction:** `TestAuditFailedApprovalDecisionIsNotVisible`.

`approvalDecide` mutates the live map entry to `decided`, installs its decision,
and marks it dirty **before** `persistEgressDecision`. A failing INSERT rolls
back SQLite but does not restore that Go object. `egressApproval` subsequently
uses `approvalCopy` and returns `Allowed=true` from the volatile decision.

The test injects a temporary SQLite trigger into its own fixture database,
attempts an allow decision, verifies that the write failed, removes the trigger,
and asks the broker-facing approval function again:

```text
durable status=pending, decision=<nil>; observed Status=decided, Allowed=true
```

**Preconditions/impact:** an authorized human submits allow and its durable write
fails. This is not approval without any human intent; it is release without the
required durable decision and event. The API reports failure while egress may
proceed, and restart forgets the permission that was exercised.

**Boundary:** the approval row and event are the authority; secret-bearing egress
is the irreversible effect. Memory must publish only the committed candidate, or
must be fully restored on every pre-commit failure. A later unrelated flush of
`dirtyApprovals` is not a valid substitute for the failed decision transaction.
Do not confuse this with `DrainError`, where the SQL commit actually succeeded.

**Next proof:** failed allow/deny/remember decisions, agent-associated approvals,
retry with the same key, restart after failure, and an upstream hit counter that
stays zero until the durable decision exists.

**Source:** `internal/control/approvals.go:672-677,706-738,758-782,447-454`.

### RMR-003 — Connector approve rules do not call the approval authority

**Reproduction:** `TestAuditConnectorApprovalBoundary`.

`authorizePolicy` returns `allowed=true, approval=true` for approve mode. That is
a matching result, not an approval decision. The generic proxy honors the second
flag with `awaitApproval`; `packageProxy` and `gitProxy` only check `allowed`,
then rewrite credentials and execute the connector.

Observed for both managed connectors:

```text
upstream_hits=1 approval_calls=0 status=200
```

The otherwise equivalent generic-proxy case reaches the approval callback,
releases no upstream request, and returns 403 while pending.

**Applicability:** these are valid normalized rules, not malformed fixtures.
`normalizeEgressRule` accepts approve mode with either connector; package rules
receive `immutable_read` when omitted. Real nodes wire the approval callback into
the same broker options. `Git.Authorize`/`Package.Authorize` do not supply the
missing human-approval step. The approve branch also skips local rule-count
consumption until `consumeApprovedRule`, which these paths never call.

**Impact:** an operator's approve requirement is silently treated as permission
for matching connector operations. The Git surface can also support writes when
`Push` is enabled, although this reproducer uses a read-only discovery request.

**Next proof/remediation boundary:** make governance an unavoidable pre-execution
stage shared by generic proxy, package, Git, and applicable CONNECT handling;
keep connector-specific validation and request fingerprinting intact. Assert
zero upstream bytes and zero secret substitution on pending, deny, timeout,
authority failure, and stale generation, including a Git write fixture.

**Source:** `internal/broker/broker.go:1098-1101,1345-1396,1475-1548,1725-1755`;
`internal/proto/proto.go:436-597`; `internal/node/node.go:3726-3745`.

### RMR-004 — Connectors bypass the central hard-budget ledger

**Reproduction:** `TestAuditConnectorBudgetBoundary`.

The same two connector paths never call `reserveBudget`/`settleBudget`. A broker
configured with a budget callback that refuses admission still fetches upstream
through package and Git connectors. Both report:

```text
upstream_hits=1 reservation_calls=0 status=200
```

The generic-proxy control invokes reservation and returns 429 without reaching
the upstream. This callback represents an exhausted authority; the test does not
use `MaxRequests=0`, which means no finite request limit rather than exhaustion.

**Impact:** central tenant/principal/workspace/binding request budgets do not
cover those requests. Node-local `EgressRule.MaxRequests` can still limit ordinary
allow rules, but that is a different, per-materialization counter, not the durable
cross-node ledger promised by ADR 0067. Unknown provider token metering is not an
excuse to skip request-count admission.

**Next proof:** a real control budget shared across connectors and generic HTTP,
concurrent last-unit admission, restart/move reconstruction, cache-hit policy,
failed authority, and conservative settlement after an interrupted stream.
Specify whether cache hits consume a request unit rather than choosing silently.

**Source:** `internal/broker/broker.go:1345-1588,1757-1769`;
`internal/node/node.go:3746-3770`; [ADR 0067](../adr/0067-authoritative-budgets-and-response-metering.md).

### RMR-005 — Reservation replay is mistaken for upstream execution deduplication

**Reproduction:** `TestAuditRepeatedHeaderDoesNotBypassHardBudget`.

The broker derives a stable reservation key from a workspace-supplied
`Idempotency-Key` and request scope. The budget manager correctly returns an
existing reservation for that key, including after settlement. The broker still
forwards each new HTTP request upstream. There is no corresponding broker
execution/result deduplication or enforced upstream idempotency contract.

Observed with a one-request tenant budget and three identical POSTs:

```text
upstream requests=3; budget Requests=1; ActiveReservations=0
```

**Important nuance:** ADR 0067 explicitly intends upstream idempotency keys to
avoid double charging. This is therefore also a design-contract defect: accepting
an arbitrary header is not evidence that this host, operation, body, retention
window, or upstream actually deduplicates the effect. The reproducer deliberately
uses a permitted HTTPS upstream that does not. A request-count ceiling cannot be
considered hard for hostile callers under that assumption.

**Next boundary:** distinguish a retry of a single broker-to-control reservation
RPC from another admitted outbound attempt. Charge each attempt, or implement a
bounded, argument-sensitive execution/result fence tied to an actual provider
idempotency contract. Simply removing the header from the digest while leaving a
stable endpoint-derived key would make unrelated calls collapse together and is
not a fix. Preserve idempotency across ambiguous control responses without
turning repeated upstream work into free work.

**Next proof:** repeated keys after settlement and expiry, concurrent duplicate
keys, changed equal-length request bodies, a non-idempotent upstream, lost control
responses, and an upstream with an explicitly verified idempotency contract.

**Source:** `internal/broker/broker.go:983-1010,1757-1783`;
`internal/budget/manager.go:215-234,361-369`;
`internal/control/budgets.go:372-392`; ADR 0067 lines 36-40.

### RMR-006 — Verified session artifact disappears before its reference commits

**Reproduction:** `TestAuditSessionLogCommitRacesArtifactGC`.

`sessionLogCommit` verifies and opens each newly referenced artifact outside
`c.mu`, closes the reader, and then reacquires the mutex. It rechecks workspace
and record authority, but neither pins the artifacts through commit nor verifies
their continued existence inside an equivalent GC exclusion boundary.

The test uses an old, unreferenced object, uploads identical bytes again, then
parks the commit after verification. A reference-aware collection runs while no
session row references the object. The commit resumes and succeeds after the
object was removed:

```text
completed session-log commit succeeded after reference-aware GC removed its verified artifact
```

**Why the grace period does not refute this:** deduplicating a new upload of an
old object does not refresh its modification time. The test explicitly repeats
the upload before racing GC. `WithArtifactReferences` protects already published
roots, not an unpinned verified candidate between verification and insertion.
The local-store path is reproduced; encrypted/remote variants need their own proof.

**Impact:** a complete retained-log record can name unavailable bytes. A node may
then reclaim its spill under the belief that the record committed safely. Later
replay must gap or fail instead of providing the promised durable bytes. The
fixture uses synthetic segment bytes to isolate reference lifetime; add a full
session-encoded simulation test for the composed output consequence.

**Next boundary/proof:** hold a publication pin through durable reference commit,
with cleanup on every error, or another protocol that excludes collection over
that interval. Test duplicate old uploads, multiple segments, cancellation,
failed SQL commit, concurrent GC, and actual replay after node loss. A larger
grace interval is not a proof of safety.

**Source:** `internal/control/session_logs.go:159-205`;
`internal/control/control.go:1135-1166`;
`internal/artifact/artifact.go:540-605,752-815`.

### RMR-007 — Session-log quota checks are not atomic with admission

**Reproduction:** `TestAuditConcurrentSessionLogQuota`.

The tenant's existing log-record count is captured under `c.mu`, then checked
before potentially slow artifact verification. On reacquisition, only the
workspace and that session's prior record are revalidated. Two different session
IDs can both observe the last slot as free and both commit.

```text
accepted 2 simultaneous new session logs with MaxSessionLogsPerTenant=1
```

**Impact:** the configured retained-record ceiling can be exceeded under ordinary
concurrency; retries and generous verification latency enlarge the window.
This is not a claim that the count is globally unbounded despite every other
request/session limit.

**Next proof:** reserve tenant record capacity before verification or atomically
recheck/admit at commit. Preserve updates and exact replays at capacity. Release
reservations on verification/cancellation/SQL failures, reconstruct durable usage
on restart, and emit an observable rejection.

**Source:** `internal/control/session_logs.go:106-121,152-158,184-205`.

### RMR-008 / RBH-001 — Failed report persistence poisons the retry watermark

**Reproduction:** `TestAuditAgentReportRetryAfterCommitFailure`.

`agentReport` obtains live pointers, advances `run.LastReport`, updates turns and
the inbox, and only later persists. Failed persistence leaves those memory changes
in place. The same report retried then satisfies `rep.Seq <= run.LastReport` and
returns success **without committing the missing progress**.

```text
memory turns=1 inbox=0 report=3; durable turns=0 inbox=1 report=2
```

This is not a double increment on retry: the retry is incorrectly suppressed.
After restart the old inbox/report state returns, and a completed turn can be
lost from the durable record or re-prompted. An event for that acknowledged
progress may also be absent.

**Next boundary/proof:** transactional publication of deep-copied candidate state,
including slices, run records, delivery maps and approval side effects; or complete
rollback before releasing authority. Exercise every report kind, validation
failure after the watermark update, failed commit, retry, and restart. Compare
memory, rows, and canonical events, not only the RPC's returned error.

**Source:** `internal/control/agents.go:1311-1352,1389-1411,1467-1480`;
[existing PR #7](https://github.com/andrewgcodes/remount/pull/7).

### RMR-009 — Cache integrity check validates size, not the digest it advertises

**Reproduction:** `TestAuditPackageCacheRejectsSameSizeCorruption`.

`Store.lookup` checks reference metadata, regular-file type, and size. It returns
the raw file without hashing it. `Package.Execute` labels the response with the
expected digest and cached provenance. Replacing the blob with different bytes
of the same size still returns successfully with the original claimed SHA-256.

**Threat qualification:** the blob is node-private and mode 0444. The test changes
permissions to simulate corruption; it does not establish that an isolated
workspace can write this path. The confirmed problem is silent corruption or a
trusted-cache writer violating integrity, not a remote cache-poisoning exploit.

**Next proof:** verify bytes against the claimed digest before reporting verified
provenance, or use an explicit verified immutable-object mechanism with a sound
invalidity/revalidation policy. Cover same-size mutation, missing/truncated blobs,
read failures, and oversized metadata. Preserve bounded streaming and admission.

**Source:** `internal/connector/store.go:192-219`;
`internal/connector/package.go:179-195`.

### RMR-010 — Cache reuse skips the current response-size ceiling

**Reproduction:** `TestAuditPackageCacheHonorsSmallerResponseRule`.

A ten-byte package is cached while `MaxResponseBytes=1024`. A broker sharing the
same tenant/workspace cache later selects a rule with a one-byte ceiling and the
caller supplies its expected digest. The cache-hit return occurs before the size
checks used for fresh fetches:

```text
cache returned 10 bytes with MaxResponseBytes=1: status=200
```

**Impact:** policy tightening or selecting a stricter rule does not constrain
cached delivery. A digest is evidence of content identity, not current permission
to deliver that quantity of content.

**Next proof:** apply current authorization and size rules equally to cached and
fresh responses, including GET/HEAD, changed rule/scope/generation, and rejection
observability. Cache reuse must not carry old admission forward implicitly.

**Source:** `internal/connector/package.go:179-195,229-237`.

### RMR-011 — Restart forgets package staging reservations but retains their bytes

**Reproduction:** `TestAuditPackageStagingCountedAfterRestart`.

A staged `body-*` file is left without a commit/abort, as after process loss.
`NewStore` creates empty in-memory reservation maps. New admission recounts
committed reference files and blobs, but not abandoned files in scope `tmp`
directories. There is no startup reconciliation of those staging reservations.

```text
after restart staging holds 16 bytes/2 objects with limit 8/1
```

**Impact:** repeated interrupted retrievals and restarts can leave physical bytes
outside the advertised node/workspace/object ceilings. Graceful abort cleanup
cannot run after a crash. No production directory was modified by this test.

**Next proof:** safe startup staging reconciliation before admission, with an
ownership/lifetime rule if concurrent store instances are possible. Either charge
retained staging or collect proven-orphan objects; never blindly delete arbitrary
paths. Assert physical bytes/objects, not just in-memory counters, across restart,
repeated failure, and successful subsequent retrieval.

**Source:** `internal/connector/store.go:74-113,222-271,419-469`.

## 6. Additional confirmed review concerns and inherited work

### RMR-012 — Local verification can label unavailable analyzers “all passed”

**Static confirmation:** `scripts/verify-local.sh:90-108,155-160` records missing
`staticcheck`/`govulncheck` as `skip` without incrementing failure or incomplete
status. With no other failures it exits zero and says “all gates passed — safe to
push.” Neither analyzer was installed on this review host. The skips are printed;
the problem is the misleading aggregate result, not completely invisible output.

CI installs and executes them. A future clean checkout can therefore get a local
success without earning those CI checks. The script also runs raw `go test ./...`
instead of `make test`, so it omits the separate `integration/publicsdk` module.
Separate SDK, browser, isolation, vendor and release workflows are not all covered
by the “every CI gate” wording either.

**Next proof:** a missing-tool fixture must produce incomplete/failure, not full
success; distinguish fast/selected/full gate sets; include the external Go module
where claiming parity. Do not quietly install tools or change project policy as
part of this documentation PR.

### Existing PR #7: keep identities and changed behavior straight

The remote report was read through `gh`; it is not yet in the main checkout.

| Existing finding | Current review disposition |
|---|---|
| RBH-001 | Reproduced here as RMR-008; do not duplicate ownership |
| RBH-002 | Producer-sequence recovery has newer fixes and restart tests; do not simply reopen the original reset-to-one mechanism |
| RBH-003 | Client peer ID is now checked against the existing subject and tenant in `control/auth.go:94-108` |
| RBH-004 | Synchronous relay-to-destination send remains in `relay.go:280-304`; source read-loop blocking can last through the transport's 30-second write timeout. Current static confirmation; original reproduction is historical |
| RBH-005 | `Peer.nextID` still starts at zero per instance; response matching has no connection-incarnation identity. Reused numbers alone are not a complete stale-response proof; preserve the delayed-response scenario from the original report |
| RBH-006 | `FS.Read` checks regular type before open, but its post-open check is still only `IsDir` (`fsops.go:138-154`). The merge handoff's partial-fix note does not establish this exact race is closed |
| RBH-007 | Overflow behavior changed: it now drops, marks an internal `Lagged` bit and increments a metric, rather than closing immediately. The public `TailEvents` still returns only a channel; reattach failure still closes it and the CLI returns success. Needs an updated proof, not repetition of the old overflow mechanism |
| RBH-008 | Historical Docker reproduction remains relevant. Docker `Prepare` wraps `docker exec`; host process-group signaling does not itself prove the in-container process tree terminated. Not rerun as an isolated hostile-descendant experiment here |

Sources: `internal/transport/peer.go:28-75,103-132,162-207`,
`internal/client/client.go:777-905`, `cmd/remount/cmd_events.go:95-102`,
`internal/workspace/workspace.go:732-757`, and
[merge/hardening handoff](handoff-2026-09-04-merge-and-hardening.md).

For event tails, global event sequence discontinuity is not by itself an
unambiguous loss signal after tenant/workspace filtering. The internal `Lagged`
method is not reachable through a channel-only public return value. An explicit
subscription error/gap contract would be preferable to asking consumers to infer
loss from numbers that can legitimately skip.

### Current-state documentation drift is a product problem

- README lines 297-300 still call Firecracker and the web UI “not built.” Both
  have implementations and newer evidence.
- `docs/design.md` still describes several implemented extensions as absent and
  carries old performance/binary-size observations. Do not quote those as fresh
  measurements.
- The closure's original “no built-in production-qualified backend” and
  filesystem-only statements precede the gVisor/Firecracker work. Its later
  sections supersede some, and September 4 ledgers supersede more.
- E2EE is the opposite case: a substantial tested library exists, but
  `implementedCapabilities` and normal dialing deliberately leave it unavailable.
- Audit export dispatch **is integrated** at `control.go:2772-2790`; the pending
  protocol-row document is not evidence that the CLI still returns unsupported.
- The browser proof now uses real servers. Its actual remaining gaps are
  Chromium/plaintext localhost, no node in the production RBAC fixture, and no
  browser-driven successful TLS credential substitution—not “only mocks.”

Keep immutable ADRs as historical decisions. Add a compact current capability /
backend / verification matrix with links to exact evidence, and label historical
paragraphs or navigation accordingly. Do not solve drift by overwriting old
measurements with an unearned new pass.

## 7. Architecture, API, performance, and product tradeoffs

### The main maintainability risk is duplicated boundary responsibility

The strongest common explanation of the new failures is not lack of comments or
tests. It is that important rules remain distributed obligations:

- each execution path must remember approval and budget admission;
- each persistence caller must remember not to publish mutable state early;
- each verifier must remember that GC and quota authority can change while unlocked;
- each asynchronous provider action must remember that an idle observation expires.

Prefer small shared boundary abstractions and staged candidate-state APIs over
copying another checklist into each handler. Avoid a wholesale rewrite or a move
to microservices merely because coordinators are large. First make the forbidden
outcomes unrepresentable or consistently fenced, then separate responsibilities
where it actually reduces coupling.

### Single binary is good deployment ergonomics, not free simplicity

SQLite and a single authoritative writer are reasonable for a small fleet. They
simplify transaction semantics and recovery. A Postgres migration or distributed
consensus would not fix the in-memory commit ordering or connector omissions.
Conversely, adding warm standby creates real operational responsibilities:
conditional-write semantics, clock assumptions, object-store availability,
nonzero RPO, and reconciliation. Measure those at the deployment's actual maximum
state size before publishing an RTO/RPO.

All session bytes still traverse the relay. This makes NAT traversal easy but
concentrates throughput and backpressure. Fix the bounded per-destination
isolation problem before assuming a larger controller instance resolves it.

### Portability needs a more precise contract than “never losing its place”

There are at least four different promises:

1. reconnect to a process that never stopped;
2. replay output after a process or node stopped;
3. restore filesystem state and restart a harness;
4. restore a compatible VM's memory and resume processes.

Remount implements mechanisms in all four areas, with backend-specific evidence.
They are not interchangeable, and none makes external API mutations, files
outside the snapshot boundary, live network peers, or a harness's private state
magically transactional. Recipes and stable mount paths reduce portability work;
they cannot eliminate differing images, OSes, architectures, or upstream APIs.

### Credential mediation reduces exposure, not malicious authorized behavior

The proxy model is especially useful for tools that support a configurable base
URL and header-based credentials. It is narrower than transparent interception:
CONNECT is not a general credential-rewriting MITM, and arbitrary tool auth flows
need explicit compatibility. A local process backend is not a hostile-code
boundary just because its supplied environment contains placeholders.

Policy should be evaluated in terms of authority, not key possession: an allowed
API can still leak workspace data, spend money, modify a repository, or return
sensitive information. Typed host/method/path rules, approvals and budgets help,
which is exactly why their bypasses are release-relevant. Also audit every
response/error/audit surface that could reflect a real credential; do not equate
header substitution alone with a complete non-disclosure proof.

### Protocol and SDK assessment

A small envelope, stable error codes, sequence/cursor semantics, and explicit
capability negotiation are good protocol decisions. The independent black-box
codec is useful: reusing the server's implementation everywhere would hide
shared mistakes. Replacing CBOR with protobuf is not a prerequisite for quality.

The cost is maintaining multiple client state machines, retry semantics, generated
types, and compatibility tests. The public Go wrapper is genuinely separate from
the internal implementation and is checked by an external module. Python and
TypeScript provide both wire and Agent HTTP clients; their READMEs explicitly say
local builds are not publication evidence. Prioritize a common behavioral
conformance matrix—timeouts, cancellation, reconnect, ordering, errors, terminal
status, and numeric bounds—rather than identical method spelling across languages.

The current conformance manifest has a required stdout-delivery row
(`CONF-SESS-009`) but no equally explicit required stderr byte-delivery row.
That is a coverage gap, not proof stderr is broken. Add the negative mutation that
drops stderr and require the independent suite to catch it.

### Performance assessment

This review does not assert new benchmark superiority. The historical performance
report includes later corrections to its own original diagnoses; read to the end.

- Synchronous durable session sealing is an accepted correctness tradeoff in
  [ADR 0082](../adr/0082-the-session-log-seal-stays-on-the-critical-path.md), not
  an unreviewed optimization opportunity. Publishing exit before durable output
  would weaken a deliberate contract.
- The old per-record/per-seal fsync and shared-store publication costs have fixes.
  Their original 4x/12x numbers are not measurements of this candidate.
- A volume-capable node still has the full-catalog generation-fence cost even for
  some no-volume workspaces. The volume-incapable-node path was fixed. A naive
  early return could leave old volume authorization unfenced.
- A lock held across I/O is a profiling lead, not a measured latency attribution.
- Hosted Windows/macOS/Linux exclusions are named in CI. Some are noisy timing
  instruments; others involve actual fencing, reattach, or transcript failures.
  Do not dismiss every excluded test as “only performance.”
- The process simulator's fork contention and local dedupe fixtures are not WAN,
  isolation-backend, sustained production, or cost comparisons.

Use realistic mixed workloads: short execs, large output, idle tails, checkpoint
plus GC, concurrent approvals, provider delays, and storage failure together.
Measure p99 and recovery behavior, not just an isolated happy-path median.

## 8. Comparison with adjacent systems

Primary documentation was consulted on 2026-09-04. DeepWiki was used to explore
`alibaba/OpenSandbox`, `e2b-dev/infra`, and `daytonaio/daytona`; current official
docs were used to check feature semantics. In particular, a DeepWiki inference
about Daytona memory persistence was not treated as evidence. None of these
products was load-tested or security-audited in this review.

| System | Relevant overlap | Important difference / lesson for Remount |
|---|---|---|
| E2B | Remote command/filesystem SDK, Firecracker sandbox persistence, memory and filesystem-only pause, network policy and request transforms | Managed execution is not just disposable shells. Remount must compete on control and portable contract, not claim competitors lack pause or credential mediation |
| Daytona | Persistent sandboxes, runner architecture, container/VM-specific lifecycle, snapshots, BYOC, secret placeholders and outbound HTTPS substitution | Particularly close overlap. Compare exact state preserved and governance behavior; BYOC and secret-blind execution are not unique to Remount |
| OpenSandbox | Public sandbox contracts, SDKs, Docker/Kubernetes runtime providers, exec daemon, egress sidecar and Credential Vault | Closest open protocol/runtime comparison. Its own pause and credential-sidecar lifetime limits show why lifecycle plus credential restoration is a real integration problem |
| Modal | Managed sandbox execution, storage, snapshots and broader cloud compute | A deployment target and possible alternative. Managed convenience trades away direct ownership of the service implementation; memory snapshots are explicitly early preview in the inspected API reference |
| Coder | Self-hosted workspace control plane, outbound agents, Terraform provisioning, AI Gateway and Agent Firewall | BYO infrastructure plus centralized AI governance already has an ecosystem. Remount is more directly organized around agent workspace/session continuity rather than general developer-environment provisioning |
| OpenHands | Local/remote workspace abstraction, command/filesystem operations, agent-server and agent framework | Often a consumer/adapter target rather than a replacement for infrastructure. Keep the runtime usable independently of Remount's own agent orchestration |
| Temporal | Durable workflow history, retries, timers and external activities | Complementary orchestration. Neither its replay nor Remount's request deduplication makes arbitrary external effects exactly once |
| Fly Machines | VM lifecycle API, suspend/resume and machine leases | Useful underlying compute, not the same abstraction as a portable governed workspace. Provider suspend semantics should not silently redefine Remount's cross-provider contract |

### Specific comparisons that change the product positioning

**Secrets:** [Daytona's secrets documentation](https://www.daytona.io/docs/en/secrets/)
describes opaque environment placeholders, HTTPS-header substitution, host
allowlists, and response scrubbing. It says an unmatched destination receives an
unchanged placeholder; Remount's broker instead explicitly blocks a misplaced
binding placeholder. That is a concrete semantic distinction—not a blanket claim
that Daytona exposes real keys.

[OpenSandbox Credential Vault](https://open-sandbox.ai/guides/credential-vault)
also keeps credentials outside the workload and requires its enforced `dns+nft`
mode for credential injection. It documents that a recreated sidecar loses its
in-memory vault and needs trusted reinjection. Remount's generation-scoped binding
lease/materialization model is valuable here if all its boundaries hold.

[E2B network controls](https://docs.e2b.dev/network/internet-access) include
per-host header transforms, currently labeled public beta, as well as allow/deny
rules. Its documentation explicitly limits domain filtering and warns about
shared infrastructure. Avoid claiming a provider firewall is equivalent to
Remount's typed application policy—or that the provider offers no mediation.

**Continuity:** [E2B persistence](https://docs.e2b.dev/sandbox/persistence) preserves
memory by default and offers filesystem-only pause. [Daytona persistence](https://www.daytona.io/docs/en/persistence/)
distinguishes container stop/start from VM pause/hot snapshots. [OpenSandbox's
Kubernetes pause guide](https://open-sandbox.ai/guides/pause-resume) describes
root-filesystem OCI snapshots and runtime recreation. Match the actual backend
semantics before comparing latency, cost, or “resume.”

**Governance and infrastructure:** [Coder architecture](https://coder.com/docs/admin/infrastructure/architecture.md)
describes Postgres, Terraform provisioners, workspace agents, and an AI Gateway;
[Agent Firewall](https://coder.com/docs/ai-coder/agent-firewall.md) documents
method/domain/path controls and its own backend limitations. [Daytona BYOC](https://www.daytona.io/docs/en/bring-your-own-compute/)
attaches owned runner infrastructure to its control plane. Remount's outbound
node binary and self-hosted authority are attractive, but must earn their simpler
operations claim under real recovery and upgrade workloads.

### Source list and scope

- E2B: [persistence](https://docs.e2b.dev/sandbox/persistence),
  [filesystem-only snapshots](https://docs.e2b.dev/sandbox/filesystem-only-snapshots),
  [network controls](https://docs.e2b.dev/network/internet-access).
- Daytona: [persistence](https://www.daytona.io/docs/en/persistence/),
  [secrets](https://www.daytona.io/docs/en/secrets/),
  [BYOC](https://www.daytona.io/docs/en/bring-your-own-compute/).
- OpenSandbox: [architecture](https://open-sandbox.ai/architecture/),
  [Credential Vault](https://open-sandbox.ai/guides/credential-vault),
  [pause/resume](https://open-sandbox.ai/guides/pause-resume).
- Modal: [SandboxSnapshot API](https://modal.com/docs/reference/modal.SandboxSnapshot),
  [Go Sandbox API](https://modal.com/docs/sdk/go/latest/Sandbox.md).
- Coder: [architecture](https://coder.com/docs/admin/infrastructure/architecture.md),
  [Agent Firewall](https://coder.com/docs/ai-coder/agent-firewall.md).
- OpenHands: [workspace architecture](https://docs.openhands.dev/sdk/arch/workspace).
- Temporal: [activity definition and idempotency](https://docs.temporal.io/activity-definition).
- Fly: [suspend/resume](https://fly.io/docs/reference/suspend-resume/).
- DeepWiki exploratory answers:
  [OpenSandbox](https://deepwiki.com/search/for-a-comparison-with-a-provid_5633b0cb-1844-4ff2-9e35-7a87042f7f8d),
  [E2B/Daytona](https://deepwiki.com/search/compare-the-actual-implementat_73575513-c0e2-4745-933f-640d51e298ad).

These are dated documentation observations, not claims of equal maturity,
license terms, independent security validation, or comparative performance.
Remount declares Apache-2.0; evaluate each alternative's actual component and
commercial-feature license before making a deployment decision.

## 9. Recommended handoff order — no fixes made here

1. **Contain the high-impact boundaries:** RMR-001, RMR-002, RMR-003, RMR-004,
   RMR-005 and RMR-006. Until fixed, do not depend on automatic idle destruction,
   connector approval/budget enforcement, or the affected transcript publication
   interval for unattended safety.
2. **Make mutable-state publication systematic:** RMR-008 and the broader family
   of agent/approval mutations. Reuse the rollback/candidate-state discipline
   already present in budget handlers where appropriate. Do not merely patch one
   trigger test while leaving adjacent report kinds vulnerable.
3. **Close capacity and cache contracts:** RMR-007, RMR-009, RMR-010, RMR-011.
   Require concurrent admission, physical accounting, restart, and corruption
   tests—not only happy-path limits.
4. **Resolve inherited transport/session work:** RBH-004/005/006/007/008 with
   current reproducers and precise status. In particular, revalidate Docker child
   termination and public event-tail terminal-error behavior.
5. **Make evidence discoverable and mechanically honest:** RMR-012, current-state
   docs, required stderr conformance, and named host/release gates.
6. **Consolidate before expanding:** prioritize a small number of supported
   deployment profiles and consumer workflows. Additional providers, displays,
   orchestration features or APIs increase the number of boundary combinations
   that must be proven.

Suggested acceptance standard for each remediation PR:

- encode the forbidden outcome before fixing it;
- name authority, resource, fence, irreversible action, and durable commit;
- make the injected fault deterministic;
- test failure, retry, cancellation and restart, not only success;
- assert upstream bytes, durable rows/events, physical files and resource cleanup;
- run focused repetitions under `-race`, relevant simulation and public API checks;
- identify target-host/live prerequisites without promoting skipped checks;
- link the disposition back to this ID and the original RBH ID where applicable.

The strategic next step is not “replace everything” or “add more features.” It is
to make the existing contract dependable enough that a harness author can stop
owning machine lifecycle and recovery. That would turn the project's best design
ideas into its strongest product advantage.

## 10. Verification record

### Fresh checks on `afea7428`

| Check | Observed result |
|---|---|
| Initial fetch, status, `HEAD` / `origin/main` | Clean and identical |
| `go vet ./...` | Passed |
| `CGO_ENABLED=0 GOOS=linux go vet ./...` | Passed |
| `CGO_ENABLED=0 GOOS=windows go vet ./...` | Passed |
| `go mod verify` | Passed: all modules verified |
| `go mod tidy -diff` | Passed without changes |
| `go test -short -p 1 -count=1 -timeout 900s ./...` with live-service gates disabled as below | Passed; sim package 304.349s |
| `go test -race -short -p 1 -count=1 -timeout 900s ./...` with the same gates | Passed; sim package 308.105s |
| External `integration/publicsdk` module, uncached | Passed |
| Supplied overlay, `-count=1 -run '^TestAudit' -v` on broker/connector/control | Eleven added invariant tests failed; six existing audit-export tests also selected and passed |
| Focused overlay, `-race -count=3` | All eleven added tests failed on every repetition; no data-race warning |
| `staticcheck`, `govulncheck` executable availability | Not installed; not counted as passed |

The broad commands explicitly unset `REMOUNT_INTEGRATION_OPENAI_KEY`,
`REMOUNT_S3_INTEGRATION_ENDPOINT`, `REMOUNT_S3_INTEGRATION_BUCKET`, and
`REMOUNT_CONFORMANCE_UPDATE_GOLDEN`, and set `REMOUNT_GVISOR_INTEGRATION=0` and
`REMOUNT_FIRECRACKER_INTEGRATION=0`. The ordinary short run also set
`CGO_ENABLED=0`; the race run used the host toolchain required by Go's race detector.
No `.env` was sourced. Local tests can use local Docker and deterministic model
fixtures; passing the package does not prove every optional lane ran.

### Integrated documentation candidate

The branch includes `1dca7730a25bb3c5e4ba9a78b8bf6b8f4122b747`. The eleven
finding paths are unchanged from `afea7428`.

- The focused overlay was rerun with `-race -count=1` after integration. All
  eleven diagnostic tests again failed with the same forbidden outcomes; no
  data-race warning was reported.
- The full `make verify` command was attempted with the same live-service gates
  disabled. Its background output became unavailable after the session resumed;
  the run was stopped to finish this report-only task. **No completed full-gate
  result or cleanup result is claimed for that interrupted run.** The completed
  short-mode runs above remain the broad execution evidence.
- `staticcheck` and `govulncheck` remain unavailable on the host, regardless of
  what the aggregate script would have reported.
- The PR changes documentation only. The deliberately failing diagnostic tests
  remain Markdown examples, not new failures added to the executable suite.

### Not established by this review

No fresh Linux gVisor/KVM deployment, native Windows execution, remote S3/MinIO
service, production browser-over-TLS run, published npm/PyPI installation,
signed release, provider pool lifecycle, multi-day soak, independent cryptographic
assessment, or comparative cloud benchmark was performed. Checked-in evidence for
some of those exists and was considered as historical evidence only.

## Appendix A. Self-contained local reproducers

These tests intentionally fail on the reviewed implementation. They are embedded
in Markdown rather than added as failing executable tests to the normal suite.
They exercise local TLS fixtures, temporary SQLite databases, temporary files and
a fake provider only. They do not access real credentials or cloud resources.

To reproduce from a fresh checkout of the reviewed revision:

1. Save the three `go` blocks below as `broker_test.go`, `connector_test.go` and
   `control_test.go` in a new scratch directory outside the checkout. Do not copy
   them over existing repository tests.
2. Create `overlay.json` in that scratch directory with absolute paths:

```json
{"Replace": {
  "/ABSOLUTE/REPO/internal/broker/audit_review_test.go": "/ABSOLUTE/SCRATCH/broker_test.go",
  "/ABSOLUTE/REPO/internal/connector/audit_review_test.go": "/ABSOLUTE/SCRATCH/connector_test.go",
  "/ABSOLUTE/REPO/internal/control/audit_review_test.go": "/ABSOLUTE/SCRATCH/control_test.go"
}}
```

3. From the repository root, run:

```sh
go test -overlay=/ABSOLUTE/SCRATCH/overlay.json -count=1 -run '^TestAudit' -v -timeout 180s ./internal/broker ./internal/connector ./internal/control
go test -race -overlay=/ABSOLUTE/SCRATCH/overlay.json -count=3 -run '^TestAudit(RepeatedHeader|Connector|Package|ConcurrentSessionLog|SessionLogCommitRaces|FailedApproval|AgentReportRetry|PoolScaleDown)' -timeout 180s ./internal/broker ./internal/connector ./internal/control
```

The helpers referenced by these blocks are existing same-package test fixtures
at the reviewed revision. The `TestAudit` prefix also selects existing audit
export tests; use the narrower second expression when counting only this appendix.
The expected current exit is nonzero. These are diagnostic reproducers, not all
ready-made post-fix regression tests: the quota fixture waits for both contenders
to reach verification, and the GC fixture requires the collector to remove the
candidate. Correct early admission or pinning can prevent those intermediate
steps. Adapt that coordination to the chosen fix while preserving the final
safety assertion; do not require a fixed implementation to recreate the unsafe
interleaving. Add composed protocol/restart tests as specified in each finding.

### A.1 `broker_test.go`

```go
package broker

import (
	"context"
	"io"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"remount.dev/remount/internal/budget"
	"remount.dev/remount/internal/connector"
	"remount.dev/remount/internal/proto"
)

func TestAuditRepeatedHeaderDoesNotBypassHardBudget(t *testing.T) {
	ctx := context.Background()
	up := newUpstream(t)
	manager, err := budget.NewManager(budget.Config{Budgets: []budget.Budget{{ID:"one-request",Tenant:"tenant-a",AttachTo:budget.AttachTenant,AttachID:"tenant-a",Window:budget.WindowHour,MaxRequests:1}}})
	if err != nil { t.Fatal(err) }
	b := New(Options{WS:"ws_audit",Tenant:"tenant-a",Generation:1,Principal:"alice",RootCAs:up.pool(),AllowPrivate:[]string{"127.0.0.1"},Allow:[]string{up.host},
		BudgetReserve:func(ctx context.Context, req proto.BudgetReserveReq)(*proto.BudgetReservation,error) { r,err:=manager.Reserve(ctx,budget.ReserveRequest{Key:req.Key,Node:"n_audit",Subject:budget.Subject{Tenant:"tenant-a",Workspace:req.WS,Generation:req.Gen,Principal:req.Principal,Bindings:req.Bindings},Provider:req.Provider,Model:req.Model,InputTokens:req.InputTokens,MaxOutputTokens:req.MaxOutputTokens,Metered:req.Metered}); if err!=nil { return nil,err }; return &proto.BudgetReservation{ID:r.ID,Tracked:r.Tracked},nil },
		BudgetSettle:func(ctx context.Context,req proto.BudgetSettleReq)(*proto.BudgetSettlement,error) { _,err:=manager.Settle(ctx,budget.SettleRequest{ReservationID:req.Reservation,Mode:budget.SettlementMode(req.Mode),InputTokens:req.InputTokens,OutputTokens:req.OutputTokens}); return &proto.BudgetSettlement{},err },
	})
	if _,err:=b.Start();err!=nil { t.Fatal(err) }; t.Cleanup(func(){b.Close()})
	for range 3 { res,err:=http.NewRequest("POST",DestURL(b.BaseURL(),up.host)+"/charged",strings.NewReader("repeat")); if err!=nil { t.Fatal(err) }; res.Header.Set("Idempotency-Key","workspace-controlled"); response,err:=http.DefaultClient.Do(res); if err!=nil {t.Fatal(err)}; io.Copy(io.Discard,response.Body);response.Body.Close() }
	usage,err:=manager.Usage(ctx,budget.UsageQuery{Tenant:"tenant-a"});if err!=nil{t.Fatal(err)}
	up.mu.Lock();hits:=len(up.seen);up.mu.Unlock()
	if hits>1 { t.Fatalf("hard request budget bypass: upstream requests=%d, budget usage=%+v",hits,usage) }
}

func TestAuditConnectorApprovalBoundary(t *testing.T) {
	for _, kind := range []string{"", proto.EgressConnectorPackage, proto.EgressConnectorGit} {
		t.Run("connector="+kind, func(t *testing.T) {
			up := newUpstream(t)
			store, err := connector.NewStore(t.TempDir(), connector.StoreOptions{})
			if err != nil { t.Fatal(err) }
			rule := proto.EgressRule{ID: "requires-human", Mode: proto.EgressModeApprove, Connector: kind, Protocol: proto.EgressProtocolHTTPS, Hosts: []string{up.host}, Methods: []string{"GET"}, MaxRequests: 1}
			if kind == proto.EgressConnectorGit { rule.Repos = []string{"acme/repo"} }
			var approvals atomic.Int64
			b := New(Options{WS: "ws_audit", Tenant: "t", Generation: 1, Principal: "alice", RootCAs: up.pool(), AllowPrivate: []string{"127.0.0.1"}, ConnectorStore: store, Network: proto.NetworkPolicy{Default: proto.NetworkDefaultDeny, Rules: []proto.EgressRule{rule}}, ApprovalWait: time.Millisecond, Approval: func(context.Context, proto.EgressApprovalReq) (*proto.EgressApprovalRes, error) { approvals.Add(1); return &proto.EgressApprovalRes{ID: "ap_audit", Status: proto.ApprovalPending}, nil }})
			if _, err := b.Start(); err != nil { t.Fatal(err) }
			t.Cleanup(func() { b.Close() })
			target := DestURL(b.BaseURL(), up.host)+"/artifact"
			if kind == proto.EgressConnectorPackage { target = b.BaseURL()+"/package/"+up.host+"/artifact" }
			if kind == proto.EgressConnectorGit { target = b.BaseURL()+"/git/"+up.host+"/acme/repo.git/info/refs?service=git-upload-pack" }
			res, err := http.Get(target)
			if err != nil { t.Fatal(err) }
			io.Copy(io.Discard, res.Body); res.Body.Close()
			up.mu.Lock(); hits := len(up.seen); up.mu.Unlock()
			if hits != 0 || approvals.Load() == 0 || res.StatusCode != http.StatusForbidden { t.Fatalf("request requiring human approval: upstream_hits=%d approval_calls=%d status=%d", hits, approvals.Load(), res.StatusCode) }
		})
	}
}

func TestAuditConnectorBudgetBoundary(t *testing.T) {
	for _, kind := range []string{"", proto.EgressConnectorPackage, proto.EgressConnectorGit} {
		t.Run("connector="+kind, func(t *testing.T) {
			up := newUpstream(t)
			store, err := connector.NewStore(t.TempDir(), connector.StoreOptions{})
			if err != nil { t.Fatal(err) }
			rule := proto.EgressRule{ID: "budgeted", Connector: kind, Protocol: proto.EgressProtocolHTTPS, Hosts: []string{up.host}, Methods: []string{"GET"}}
			if kind == proto.EgressConnectorGit { rule.Repos = []string{"acme/repo"} }
			var reserves atomic.Int64
			b := New(Options{WS: "ws_audit", Tenant: "t", Generation: 1, Principal: "alice", RootCAs: up.pool(), AllowPrivate: []string{"127.0.0.1"}, ConnectorStore: store, Network: proto.NetworkPolicy{Default: proto.NetworkDefaultDeny, Rules: []proto.EgressRule{rule}}, BudgetReserve: func(context.Context, proto.BudgetReserveReq) (*proto.BudgetReservation, error) { reserves.Add(1); return nil, budget.ErrBudgetExceeded }})
			if _, err := b.Start(); err != nil { t.Fatal(err) }
			t.Cleanup(func() { b.Close() })
			target := DestURL(b.BaseURL(), up.host)+"/artifact"
			if kind == proto.EgressConnectorPackage { target = b.BaseURL()+"/package/"+up.host+"/artifact" }
			if kind == proto.EgressConnectorGit { target = b.BaseURL()+"/git/"+up.host+"/acme/repo.git/info/refs?service=git-upload-pack" }
			res, err := http.Get(target)
			if err != nil { t.Fatal(err) }
			io.Copy(io.Discard, res.Body); res.Body.Close()
			up.mu.Lock(); hits := len(up.seen); up.mu.Unlock()
			if hits != 0 || reserves.Load() == 0 || res.StatusCode != http.StatusTooManyRequests { t.Fatalf("exhausted budget: upstream_hits=%d reservation_calls=%d status=%d", hits, reserves.Load(), res.StatusCode) }
		})
	}
}

func TestAuditPackageCacheHonorsSmallerResponseRule(t *testing.T) {
	up := newUpstream(t)
	store, err := connector.NewStore(t.TempDir(), connector.StoreOptions{})
	if err != nil { t.Fatal(err) }
	var digest string
	for _, max := range []int64{1024, 1} {
		b := New(Options{WS: "ws_audit", Tenant: "t", Generation: 1, Principal: "alice", RootCAs: up.pool(), AllowPrivate: []string{"127.0.0.1"}, ConnectorStore: store, Network: proto.NetworkPolicy{Default: proto.NetworkDefaultDeny, Rules: []proto.EgressRule{{ID: "packages", Connector: proto.EgressConnectorPackage, Protocol: proto.EgressProtocolHTTPS, Hosts: []string{up.host}, Methods: []string{"GET"}, MaxResponseBytes: max}}}})
		if _, err := b.Start(); err != nil { t.Fatal(err) }
		req, _ := http.NewRequest("GET", b.BaseURL()+"/package/"+up.host+"/artifact", nil)
		req.Header.Set(connector.ExpectedDigestHeader, digest)
		res, err := http.DefaultClient.Do(req)
		if err != nil { b.Close(); t.Fatal(err) }
		body, _ := io.ReadAll(res.Body); res.Body.Close(); b.Close()
		if max == 1024 { if res.StatusCode != 200 { t.Fatalf("seed: %d", res.StatusCode) }; digest = res.Header.Get(connector.ContentDigestHeader); if !strings.HasPrefix(digest, "sha256:") { t.Fatalf("digest: %q", digest) }; continue }
		if res.StatusCode == 200 && int64(len(body)) > max { t.Fatalf("cache returned %d bytes with MaxResponseBytes=%d: status=%d", len(body), max, res.StatusCode) }
	}
}
```

### A.2 `connector_test.go`

```go
package connector

import (
	"context"
	"crypto/sha256"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestAuditPackageCacheRejectsSameSizeCorruption(t *testing.T) {
	root := t.TempDir()
	store, err := NewStore(root, StoreOptions{MaxBytes: 4096, MaxBytesPerScope: 2048, MaxObjectBytes: 1024})
	if err != nil { t.Fatal(err) }
	payload := "known-package"
	digest := fmt.Sprintf("%x", sha256.Sum256([]byte(payload)))
	packages := NewPackage(PackageOptions{Store: store, Transport: roundTripFunc(func(*http.Request) (*http.Response, error) { return &http.Response{StatusCode: 200, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(payload)), ContentLength: int64(len(payload))}, nil })})
	response, err := packages.Execute(context.Background(), packageRequest(t, "ws_audit", digest, packageRule(1024)))
	if err != nil { t.Fatal(err) }; response.Body.Close()
	blob := store.blobPath(digest)
	if err := os.Chmod(blob, 0600); err != nil { t.Fatal(err) }
	if err := os.WriteFile(blob, []byte(strings.Repeat("x", len(payload))), 0600); err != nil { t.Fatal(err) }
	response, err = packages.Execute(context.Background(), packageRequest(t, "ws_audit", digest, packageRule(1024)))
	if err == nil { body, readErr := io.ReadAll(response.Body); response.Body.Close(); if readErr == nil && fmt.Sprintf("%x", sha256.Sum256(body)) != digest { t.Fatalf("cache accepted corrupted bytes with claimed SHA256=%s; cached=%v bytes=%q", response.Provenance.SHA256, response.Provenance.Cached, body) } }
}

func TestAuditPackageStagingCountedAfterRestart(t *testing.T) {
	root := t.TempDir()
	opts := StoreOptions{MaxBytes: 8, MaxBytesPerScope: 8, MaxObjectBytes: 8, MaxObjects: 1, MaxObjectsPerScope: 1}
	first, err := NewStore(root, opts)
	if err != nil { t.Fatal(err) }
	_, staged, err := first.begin("tenant", "ws_audit", 8)
	if err != nil { t.Fatal(err) }
	if _, err := staged.Write([]byte("12345678")); err != nil { t.Fatal(err) }; staged.Close()
	restarted, err := NewStore(root, opts)
	if err != nil { t.Fatal(err) }
	reservation, next, err := restarted.begin("tenant", "ws_audit", 8)
	if err != nil { return }
	defer reservation.abort()
	if _, err := next.Write([]byte("abcdefgh")); err != nil { t.Fatal(err) }; next.Close()
	var bytes int64
	var objects int
	if err := filepath.WalkDir(filepath.Join(root, "scopes"), func(path string, entry os.DirEntry, err error) error { if err != nil { return err }; if !entry.IsDir() && strings.HasPrefix(entry.Name(), "body-") { info, err := entry.Info(); if err != nil { return err }; bytes += info.Size(); objects++ }; return nil }); err != nil { t.Fatal(err) }
	if bytes > opts.MaxBytes || int64(objects) > opts.MaxObjects { t.Fatalf("after restart staging holds %d bytes/%d objects with limit %d/%d", bytes, objects, opts.MaxBytes, opts.MaxObjects) }
}
```

### A.3 `control_test.go`

```go
package control

import (
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"remount.dev/remount/internal/artifact"
	nodepool "remount.dev/remount/internal/pool"
	"remount.dev/remount/internal/proto"
	"remount.dev/remount/internal/provision"
)

type auditBarrierArtifactStore struct {
	*artifact.Store
	opened chan struct{}
	release chan struct{}
}

func (s *auditBarrierArtifactStore) Open(id string) (io.ReadCloser, int64, error) {
	reader, size, err := s.Store.Open(id)
	if err != nil { return nil, 0, err }
	s.opened <- struct{}{}
	<-s.release
	return reader, size, nil
}

func TestAuditConcurrentSessionLogQuota(t *testing.T) {
	store, err := artifact.NewStore(t.TempDir())
	if err != nil { t.Fatal(err) }
	id, size, err := store.Put(strings.NewReader("session bytes"))
	if err != nil { t.Fatal(err) }
	barrier := &auditBarrierArtifactStore{Store: store, opened: make(chan struct{}, 2), release: make(chan struct{})}
	f := newControlFixture(t, "", func(opts *Options) { opts.Artifacts = barrier; opts.MaxSessionLogsPerTenant = 1 })
	putHeldSessionWorkspace(f.c, "ws_audit", "tenant-a", "n_audit", 1)
	results := make(chan error, 2)
	for _, sid := range []string{"s_one", "s_two"} {
		go func(sid string) { _, err := f.c.sessionLogCommit(context.Background(), "n_audit", &proto.SessionLogCommitReq{Session: sid, Workspace: "ws_audit", Generation: 1, Principal: "alice", Kind: proto.SessionExec, MaxChunk: 32<<10, Segments: []proto.SessionLogSegment{{First: 0, Next: 1, Artifact: id, Bytes: size}}}); results <- err }(sid)
	}
	for range 2 { select { case <-barrier.opened: case <-time.After(10*time.Second): close(barrier.release); t.Fatal("both commits did not reach verification") } }
	close(barrier.release)
	var accepted int
	for range 2 { if err := <-results; err == nil { accepted++ } else if !errors.Is(err, &proto.Error{Code: proto.CodeResourceExhausted}) { t.Fatal(err) } }
	if accepted > 1 { t.Fatalf("accepted %d simultaneous new session logs with MaxSessionLogsPerTenant=1", accepted) }
}

type auditCloseArtifactStore struct {
	*artifact.Store
	closed chan struct{}
	release chan struct{}
}

type auditArtifactReader struct {
	io.ReadCloser
	closed chan struct{}
	release chan struct{}
}

func (r auditArtifactReader) Close() error { err := r.ReadCloser.Close(); r.closed <- struct{}{}; <-r.release; return err }
func (s *auditCloseArtifactStore) Open(id string) (io.ReadCloser, int64, error) { r, size, err := s.Store.Open(id); if err != nil { return nil, 0, err }; return auditArtifactReader{r, s.closed, s.release}, size, nil }

func TestAuditSessionLogCommitRacesArtifactGC(t *testing.T) {
	root := t.TempDir()
	store, err := artifact.NewStore(root)
	if err != nil { t.Fatal(err) }
	id, size, err := store.Put(strings.NewReader("old unreferenced segment"))
	if err != nil { t.Fatal(err) }
	digest, err := artifact.Digest(id)
	if err != nil { t.Fatal(err) }
	old := time.Now().Add(-48*time.Hour)
	if err := os.Chtimes(filepath.Join(root, digest[:2], digest), old, old); err != nil { t.Fatal(err) }
	if repeated, _, err := store.Put(strings.NewReader("old unreferenced segment")); err != nil || repeated != id { t.Fatalf("dedup upload: %s %v", repeated, err) }
	barrier := &auditCloseArtifactStore{Store: store, closed: make(chan struct{}, 1), release: make(chan struct{})}
	f := newControlFixture(t, "", func(opts *Options) { opts.Artifacts = barrier })
	putHeldSessionWorkspace(f.c, "ws_audit", "tenant-a", "n_audit", 1)
	results := make(chan error, 1)
	go func() { _, err := f.c.sessionLogCommit(context.Background(), "n_audit", &proto.SessionLogCommitReq{Session: "s_audit", Workspace: "ws_audit", Generation: 1, Principal: "alice", Kind: proto.SessionExec, MaxChunk: 32<<10, Complete: true, Info: proto.SessionInfo{ID: "s_audit", WS: "ws_audit", Kind: proto.SessionExec}, Segments: []proto.SessionLogSegment{{First: 0, Next: 1, Artifact: id, Bytes: size}}}); results <- err }()
	select { case <-barrier.closed: case <-time.After(10*time.Second): close(barrier.release); t.Fatal("commit did not verify artifact") }
	var collected artifact.GCResult
	err = f.c.WithArtifactReferences(func(refs []string) error { var err error; collected, err = store.Collect(refs, time.Now().Add(-24*time.Hour)); return err })
	close(barrier.release)
	commitErr := <-results
	if err != nil { t.Fatal(err) }
	if collected.Removed != 1 { t.Fatalf("GC did not remove segment: %+v", collected) }
	if commitErr == nil && !store.Has(id) { t.Fatal("completed session-log commit succeeded after reference-aware GC removed its verified artifact") }
}

type auditPoolDriver struct {
	machine provision.Machine
	onDestroy func(string)
}
func (d *auditPoolDriver) Name() string { return "audit" }
func (d *auditPoolDriver) Create(context.Context, provision.Request) (provision.Machine, error) { return provision.Machine{}, errors.New("unexpected create") }
func (d *auditPoolDriver) Destroy(_ context.Context, id string) error { d.onDestroy(id); return nil }
func (d *auditPoolDriver) List(context.Context, provision.ListOptions) ([]provision.Machine, error) { return []provision.Machine{d.machine}, nil }

type auditPoolTokens struct{}
func (auditPoolTokens) Issue(context.Context, string, string, time.Duration) (string, error) { return "unused-synthetic", nil }

type auditBlockingReconciler struct { *nodepool.Reconciler; entered chan []nodepool.Node; release chan struct{}; once sync.Once }
func (r *auditBlockingReconciler) Reconcile(ctx context.Context, spec nodepool.Spec, nodes []nodepool.Node, demand int) ([]nodepool.Action, error) { r.once.Do(func() { r.entered <- nodes; <-r.release }); return r.Reconciler.Reconcile(ctx, spec, nodes, demand) }

func TestAuditFailedApprovalDecisionIsNotVisible(t *testing.T) {
	ctx := context.Background()
	f := newControlFixture(t, "", nil)
	ws := claimedApprovalWorkspace(t, f)
	req := &proto.EgressApprovalReq{WS:ws.ID,Gen:ws.Generation,Principal:"alice",Rule:"github",Host:"api.github.com",Method:"POST",PathHash:approvalHash('a'),BodyHash:approvalHash('b'),Fingerprint:approvalHash('c')}
	pending,err:=f.c.egressApproval(ctx,"n_approve",req); if err!=nil {t.Fatal(err)}
	if _,err:=f.c.db.Exec(`CREATE TRIGGER audit_fail_approval BEFORE INSERT ON approvals BEGIN SELECT RAISE(FAIL, 'injected durable write failure'); END`);err!=nil{t.Fatal(err)}
	_,decisionErr:=f.c.approvalDecide(ctx,localSubject(),&proto.ApprovalDecideReq{ID:pending.ID,Option:"allow",IdempotencyKey:"audit-decide"})
	if _,err:=f.c.db.Exec(`DROP TRIGGER audit_fail_approval`);err!=nil{t.Fatal(err)}
	if decisionErr==nil{t.Fatal("injected approval write failure was not reached")}
	observed,err:=f.c.egressApproval(ctx,"n_approve",req);if err!=nil{t.Fatal(err)}
	var raw []byte
	if err:=f.c.db.QueryRow(`SELECT data FROM approvals WHERE id=?`,pending.ID).Scan(&raw);err!=nil{t.Fatal(err)}
	var durable proto.Approval;if err:=proto.Unmarshal(raw,&durable);err!=nil{t.Fatal(err)}
	if observed.Allowed {t.Fatalf("broker was allowed after decision commit failed; durable status=%s, decision=%v, observed=%+v",durable.Status,durable.Decision,observed)}
}

func TestAuditAgentReportRetryAfterCommitFailure(t *testing.T) {
	ctx := context.Background()
	af := newAgentFixture(t, "", nil)
	a, run := af.start(t, localSubject(), "task")
	message := af.agent(t, a.ID).Inbox[0].ID
	af.report(t, run, 2, proto.AgentReport{Kind: proto.AgentReportTurnStarted, Message: message})
	if _, err := af.c.db.Exec(`CREATE TRIGGER audit_fail_agent BEFORE INSERT ON agents BEGIN SELECT RAISE(FAIL, 'injected durable write failure'); END`); err != nil { t.Fatal(err) }
	rep := &proto.AgentReport{Agent: a.ID, Run: run.Run, WS: run.WS, Gen: run.Gen, Seq: 3, Kind: proto.AgentReportTurnFinished, Message: message}
	firstErr := af.c.agentReport(ctx, "n_one", rep)
	if _, err := af.c.db.Exec(`DROP TRIGGER audit_fail_agent`); err != nil { t.Fatal(err) }
	if firstErr == nil { t.Fatal("injected commit failure was not reached") }
	if err := af.c.agentReport(ctx, "n_one", rep); err != nil { t.Fatalf("retry: %v", err) }
	var raw []byte
	if err := af.c.db.QueryRow(`SELECT data FROM agents WHERE id=?`, a.ID).Scan(&raw); err != nil { t.Fatal(err) }
	var persisted proto.Agent
	if err := proto.Unmarshal(raw, &persisted); err != nil { t.Fatal(err) }
	live := af.agent(t, a.ID)
	if persisted.Turns != 1 || len(persisted.Inbox) != 0 { t.Fatalf("retry acknowledged uncommitted turn: memory turns=%d inbox=%d report=%d; durable turns=%d inbox=%d report=%d", live.Turns, len(live.Inbox), live.Runs[0].LastReport, persisted.Turns, len(persisted.Inbox), persisted.Runs[0].LastReport) }
}

func TestAuditPoolScaleDownFencesNewClaims(t *testing.T) {
	ctx := context.Background()
	driver := &auditPoolDriver{machine: provision.Machine{ID: "machine-audit", Provider: "audit", Tenant: "tenant-a", Labels: map[string]string{provision.PoolLabel:"audit", provision.NodeLabel:"n_audit"}}}
	r, err := nodepool.New([]provision.Driver{driver}, auditPoolTokens{}, nodepool.Options{})
	if err != nil { t.Fatal(err) }
	blocked := &auditBlockingReconciler{Reconciler:r, entered: make(chan []nodepool.Node,1), release: make(chan struct{})}
	f := newControlFixture(t,"",func(opts *Options) { opts.PoolReconciler=blocked; opts.PoolBootstrap=PoolBootstrap{ServerURL:"https://control.example",BinaryURL:"https://control.example/remount",DataDir:"/var/lib/remount"} })
	info := processNodeInfo(4096)
	connectNode(t,f.c,"n_audit",info)
	f.c.mu.Lock(); f.c.nodes["n_audit"].Status.Labels=map[string]string{provision.PoolLabel:"audit","tenant":"tenant-a"}; f.c.mu.Unlock()
	_,err=f.c.poolCreate(ctx,localSubject(),&proto.PoolCreateReq{Spec:proto.PoolSpec{Name:"audit",Vendor:"audit",Max:1,Backend:"process",IdleScaleDownMilli:1},IdempotencyKey:"audit-pool"})
	if err!=nil { t.Fatal(err) }
	f.c.mu.Lock(); f.c.poolIdle[poolMachine{pool:poolKey("tenant-a","audit"),machine:"machine-audit"}]=time.Now().Add(-time.Hour); f.c.mu.Unlock()
	activeAtDestroy := make(chan bool,1)
	driver.onDestroy=func(id string) { f.c.mu.Lock(); active:=false; for _,ws:=range f.c.workspaces { if ws.Node=="n_audit" && held(ws.State) { active=true } }; f.c.mu.Unlock(); activeAtDestroy<-active }
	f.c.reconcilePoolsAsync()
	select { case nodes:=<-blocked.entered: if len(nodes)!=1 || nodes[0].Workspaces!=0 { close(blocked.release); f.c.poolWG.Wait(); t.Fatalf("not idle: %+v",nodes) }; case <-time.After(10*time.Second): close(blocked.release); t.Fatal("no reconcile") }
	created:=createWorkspace(t,f.c,localSubject(),proto.WorkspaceSpec{})
	claim,claimErr:=f.c.wsClaim(ctx,"n_audit",created.ID)
	if claimErr==nil { claimErr=f.c.wsReady(ctx,"n_audit",&proto.WSReadyReq{ID:created.ID,Gen:claim.Workspace.Generation}) }
	close(blocked.release); f.c.poolWG.Wait()
	select { case active:=<-activeAtDestroy: if active && claimErr==nil { t.Fatal("provider destroyed a node after a new workspace claim and ws.ready both committed") }; default: }
}
```
