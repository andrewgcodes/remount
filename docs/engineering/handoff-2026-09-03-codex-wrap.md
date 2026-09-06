# Remount continuation handoff — Codex quota wrap, 2026-09-03

This is the continuation document for the implementation requested by
`handoff-2026-09-03.md`. It exists because the implementing Codex session had
to stop for usage quota after completing most of the code work. It is not a
claim that every external acceptance gate is green. It separates implemented
and locally proven behavior from work that is partial, externally gated, or
still needs final integration review.

Read this document together with, in order:

1. `AGENTS.md`;
2. `docs/engineering/README.md` and `handoff-2026-09-03.md`;
3. `docs/engineering/hardening-lessons.md` and `MISTAKES.md`;
4. the relevant ADRs and `spec/PROTOCOL.md`; and
5. `.agents/skills/remount-hardening-review/SKILL.md` when using Codex.

Do not resume in the original checkout at
`/Users/you/Documents/remount`: another agent was working there during
this implementation. The integration checkout used here was
`/private/tmp/remount-codex-handoff-2026-09-03` on branch
`codex/handoff-2026-09-03`. Fetch first and create a fresh worktree from the PR
branch unless that exact worktree still exists and is clean.

## 1. Repository and integration state at wrap time

- Integration branch: `codex/handoff-2026-09-03`.
- Last pre-wrap local commit before the final integration commit:
  `21ef995 test(sim): record fleet-scale failover evidence`.
- Main was independently repaired through `202042a` while feature agents were
  editing the integration worktree. The two main-only fixes are:
  - `197b3ef fix(ci): satisfy static analysis`;
  - `202042a fix(session): treat closed PTY as EOF`.
- The final integration commit and PR URL are recorded in the delivery message
  for this handoff. Verify them with `git log --oneline --decorate -20` rather
  than assuming a hash from this point-in-time document.
- Never force-push main. Preserve unfamiliar dirty paths. Before any new edit,
  record `git status --short --branch`, `HEAD`, and `origin/main` as required by
  `AGENTS.md`.

The branch was intentionally developed in a separate worktree because Claude
Code was writing to the original checkout concurrently. Three subagents shared
the integration worktree, so the final commit groups a large convergence diff.
Review by subsystem and behavior, not merely by commit size.

## 2. What was implemented in this continuation

The commits before the final convergence commit cover these major slices:

- vendor provisioner contracts and bounded E2B, Fly, Modal, ix and SSH drivers;
- durable tenant-scoped pools and end-to-end capacity reconciliation;
- broker response redaction, approval parking, budgets and usage metering;
- signed principals, atomic node enrollment and shared-token-free production;
- tenant-qualified encrypted artifacts and S3-compatible storage;
- gVisor enforced-gateway wiring and denial/conformance checks;
- canonical chunked snapshots and tiered immutable session-log segments;
- external secret sources and fenced binding leases;
- resumable SIEM/OTLP/S3 exports and durable export cursors;
- MCP gateway, notifier delivery, orchestrator examples and embedded console;
- generated Python and TypeScript SDK packages and drift checks;
- warm-standby control replication, controller epochs and reconciliation;
- shared-volume lifecycle and replay hardening;
- Firecracker guest/runtime integration, described separately below;
- production tenant onboarding, RBAC and OIDC; and
- 200-node/2,000-workspace control-failover scale evidence.

Use `git log --reverse --oneline 7e61cde..HEAD` for the exact commit ledger.
Important focused acceptance commits already present include:

- `8d5304e test(sim): compose approval notification flow`, which proves a
  committed `egress.pending` reaches the notifier while the upstream request
  remains parked and that a decision releases it;
- `21ef995 test(sim): record fleet-scale failover evidence`, which records the
  real-protocol 200-node/2,000-workspace scenario and checked-in p50/p99 data;
- `9155d21 fix(ci): include generated SDK packages`, which repaired the SDK
  distribution workflow by checking in both installable packages; and
- `4d0d7c5 fix(console): preserve terminal replay cursor`, which prevents the
  console from restarting replay at the wrong cursor.

### 2.1 Onboarding, RBAC and SSO

Implemented and focused-race-tested:

- production bootstrap with `--bootstrap-principal` and an exclusive mode-0600
  `--bootstrap-token-file`, without requiring a shared production token;
- `tenant create|ls`, tenant OIDC configuration, `invite`,
  `principal create|ls|revoke`, and access-token issuance with role and TTL;
- a durable principal directory, tenant-scoped roles, role-change audit events
  and authorization-revision fencing;
- OIDC discovery and RFC 8628 device flow;
- RS256/JWKS verification including issuer, audience, nonce and time checks;
- group-to-role mapping, tenant boundary checks, public config/exchange/refresh
  endpoints and single-use refresh rotation;
- RFC 8628 `slow_down` plus transport-timeout backoff;
- mode-0600 CLI credential storage and automatic refresh;
- the named `identity-admin` capability and MCP/protocol coverage.

Primary paths are `cmd/remount/cmd_identity.go`, `cmd/remount/cmd_login.go`,
`internal/identity/`, `internal/tenant/`, `internal/control/identity_runtime.go`,
`internal/control/tenant_runtime.go`, `internal/client/identity.go`,
`internal/server/bootstrap.go`, `internal/server/identity_http.go`, and
`internal/proto/identity_tenant.go`.

Residual identity work:

- run the same OIDC exchange against a real WorkOS tenant when credentials and
  an approved test tenant are available;
- decide whether token-issuance idempotency needs a metadata-only replay
  record. Persisting the returned bearer itself would violate the no-secret
  durability rule, so do not solve this by storing plaintext tokens.

### 2.2 E15 operator console

The real embedded sim/server test now covers a two-node fleet, workspace
navigation, live PTY, forced disconnect and cursor reattach, ten-minute
server-side replay, snapshot and move, broker credential substitution,
`leak_blocked`, parked egress approval and committed decision. The focused
normal and race tests passed:

```sh
go test ./internal/sim -run '^TestConsoleE15SimulatedOperatorFlow$' -count=1
go test -race ./internal/sim -run '^TestConsoleE15SimulatedOperatorFlow$' -count=1
```

The browser DOM/Playwright suite still uses mocked HTTP responses. The
authorization, PTY, event and broker boundary is real in sim, but a future
change should drive an actual embedded server from Playwright if Gate 5 is to
claim a complete browser-to-server proof.

### 2.3 E9 chunked snapshots and E11 tiered logs

The chunked representation uses a canonical, tenant-qualified manifest and
content chunks, carries representation metadata, preserves old gzip artifact
compatibility, and includes the transitive references in GC roots. Restore is
fed through the backend-created filesystem boundary; it must not atomically
replace a process backend root after the jailed filesystem handle has opened.

The E9 test uses a generated 500 MiB logical fixture and verifies the second
snapshot uploads less than 1 MiB, move timing, and byte identity. The E11 test
uses real exec transport, 8 MiB across more than 128 chunks, a move to a second
node and byte-exact replay from start/middle/end cursors. Forced cold cache plus
blob deletion must emit an explicit first `gap` with `tier=blob`; unavailable
archived output must never be reported complete.

Relevant paths include `internal/artifact/chunked/`,
`internal/control/chunked_artifacts.go`, `internal/node/chunked_artifacts.go`,
`internal/control/session_logs.go`, `internal/node/session_logs.go`,
`internal/proto/artifact_format.go`, `internal/proto/artifact_proof.go`,
`internal/proto/session_log.go`, `internal/sim/chunked_e9_test.go`, and
`internal/sim/tiered_e11_test.go`.

Before editing this area, preserve these contracts:

- artifact proofs are short-lived and revalidated against tenant, current
  holder and generation before and after transfer;
- representation metadata is explicit; do not guess solely from an ID;
- GC retains the transitive closure of manifests, chunks and log segments;
- snapshot/log producers are joined on cancellation and failure; and
- loss is a visible gap, never a successful complete replay.

Focused tests passed repeatedly in normal and race modes, including 10-count
control stress and full package tests for control, session, node, proto, client
and CLI. Remaining literal/coverage gaps are:

- the E11 proof uses 8 MiB across many real 128 KiB segments rather than
  spending six hours to generate 2 GiB;
- client `pull` understands chunked snapshots, while local `push` still emits
  the legacy tar representation;
- chunked `pull` needs a command-level integration regression in addition to
  its compile/package coverage;
- `doctor --deep` rehashes the blobs but does not separately label canonical
  manifest/closure validation;
- background session-log pruning should emit a deletion event for each pruned
  row; and
- `spec/PROTOCOL.md` still needs explicit rows for
  `session.log.commit|get|delete` and their events.

### 2.4 Warm standby and controller epochs

The warm-standby lane adds WAL shipping, bounded lag, promotion, controller
epoch fencing, node-driven reconciliation, server CLI/configuration and the E10
failover scenario. A stale controller must not mutate a resource after a newer
epoch is active. The in-flight move must either complete or roll back without a
duplicate generation, and any lost RPO window must be recorded as
`control.recovered` rather than silently omitted.

Primary paths include `internal/control/replicate/`,
`internal/control/controller_epoch.go`, `internal/node/controller_epoch.go`,
`internal/node/controller_state.go`, `internal/server/replication.go`,
`cmd/remount/replication_runtime.go`, `integration/failover/`, and ADR 0074.

### 2.5 Firecracker

The Firecracker lane adds a guest-backed workspace filesystem and session
runner seam, a versioned bounded virtio-vsock bridge, Linux guest server,
jailer/vsock integration, compatibility detection, bounded full bundle
manifests for disk/state/memory, hash verification, atomic retention, reflink
restore, and network-namespace/TAP plumbing with a deny-first policy.

This is host-gated. macOS cannot provide `/dev/kvm`, so local unit tests prove
format, compatibility, protocol and lifecycle logic but cannot prove a real VM
boot or snapshot restore. The optional self-hosted Linux KVM workflow/runbook
must be green before advertising this backend as production evidence.

The most important lifecycle contract is:

- authority: the current workspace generation holder plus the control-plane
  release/quarantine transaction;
- resource: guest VM execution state and the source workspace handle;
- irreversible action: destructive checkpoint and source teardown;
- durable commit: checkpoint metadata plus the control next-state/event
  transaction; and
- postcondition: a guest remains paused/fenced after successful prepare until
  durable commit; abort resumes exactly once; ordinary non-destructive snapshot
  resumes; stale-generation work touches nothing.

Do not claim `ws.moved.processes=preserved` unless that prepare/commit/abort
contract and a real KVM restore test are both satisfied.

At wrap time the implementation is compile-safe and its focused unit/race
matrix passed, including 10-count lifecycle/session/network/bundle tests and
Linux/Windows cross-compiles. The remaining concrete gaps are not cosmetic:

- `.github/workflows/kvm.yml` deliberately fails its final integrated-host
  adapter step because the real KVM E2E invocation is not wired yet;
- Firecracker-specific `doctor` reporting is unfinished; and
- `ws.moved.processes=preserved` is currently derived from the full-VM artifact
  format when the move enters pending. ADR 0063 requires delaying or
  supplementing that claim until destination compatibility and guest restore
  have succeeded.

### 2.6 Phase 6.4 scale evidence

The following passed locally in 37.44 seconds on Darwin arm64 with the process
backend:

```sh
go test ./internal/sim -run '^TestHandoffScaleAndControlFailover$' \
  -count=1 -v -timeout=15m
```

It created 200 real protocol nodes and clients, claimed 2,000 workspaces,
executed and moved one workspace per node, restarted the control plane over the
shared SQLite store, reconnected 400 peers, and required byte-exact session
completion without gaps or generation mismatch. Recorded results:

| Operation | Samples | p50 | p99 |
|---|---:|---:|---:|
| claim | 2,000 | 0.516 s | 6.938 s |
| exec round trip | 200 | 3.245 s | 3.287 s |
| move | 200 | 6.695 s | 8.196 s |
| reattach after control restart | 200 | 5.686 s | 5.705 s |

This is control/protocol scale evidence, not Docker/gVisor performance. Raw
evidence is in `bench/results/scale-process-local.json`; rendering and evidence
validation live in `bench/run.py` and `bench/test_tools.py`.

## 3. CI incidents fixed during integration

The first main push exposed three issues that local focused tests did not:

1. The Examples workflow installed generated SDK directories that were not yet
   tracked. Commit `9155d21` checked in both packages and their exact install,
   build, typecheck and smoke tests passed.
2. Staticcheck found five style/correctness findings, including a real nested
   webhook-array append defect. Commit `197b3ef` fixed them and added a
   regression.
3. Linux PTYs return `EIO` after slave closure. The code treated that as a log
   error, and the test raced resize against initial-size output. Commit
   `202042a` normalizes Unix `EIO` to EOF and makes the resize assertion
   deterministic.

The replacement main CI run `33809584139` passed static analysis, ordinary
tests, Ubuntu and macOS race suites, cross-platform builds, hostile conformance
and bounded fuzzing. The MinIO run `33809584226` saw one transient container
EOF and passed unchanged on retry. If it recurs, add container liveness/log
capture around the test rather than hiding it with unbounded retries.

## 4. Verification performed before wrap

Evidence already green at the time this document was written:

- compile-only matrix for `./internal/... ./integration/... ./cmd/...`;
- the focused normal/race suites reported above;
- E8 principal revocation and tenant-boundary normal/race stress;
- E9 generated 500 MiB fixture;
- E10 failover normal/race composition;
- E11 real transport/move/replay and blob-loss gap;
- onboarding identity/tenant/server/client/CLI/MCP focused race suite;
- `make public-api`, lock discipline, vet and CGO-disabled compile for the
  onboarding lane;
- Python SDK exact local install and Agent API/LangGraph/OpenHands examples;
- TypeScript SDK exact local install/build and example typecheck/smoke;
- a live OpenAI broker request using the ignored root `.env`, with a synthetic
  canary scan and no key output; and
- the full green main CI run identified above.

The final PR must still be judged by its own CI. The green main run predates the
last convergence diff and is evidence for the CI repairs, not blanket evidence
for every feature in this PR.

## 5. Required next actions, in order

### P0 — integrate and prove the PR candidate

1. Fetch origin and confirm the PR branch contains the two main-only repair
   commits or equivalent changes. Resolve by merge/rebase without force.
2. Run `git diff --check` and `gofmt -l` over changed Go files.
3. Run the exact pinned staticcheck used by CI. No U1000 finding is acceptable;
   audit lost call sites before deleting lifecycle helpers.
4. Regenerate protocol schema and both SDK type surfaces only after
   `internal/proto` stops moving:

   ```sh
   go run ./cmd/protogen
   go run ./cmd/protogen --check
   ```

5. Run SDK install/build/smoke, web build/Playwright, `make public-api`,
   `make lint`, `make race`, `make`, and `make conformance`.
6. Because protocol, artifact and broker changed, also run the bounded fuzz
   gate specified in the original handoff.
7. Inspect the complete diff, fetch again, integrate any upstream movement and
   repeat affected proofs before merging.

### P1 — complete Phase 6.3 retention, residency and compliance export

The original plan requires per-tenant retention for events, session logs and
artifacts; standardized `region`/`residency` placement; doctor findings for
violations; and `remount audit export --tenant --range` with a signed manifest.

Required proof:

- tenant A's retention cannot delete or retain tenant B's resources;
- active workspace/base/fleet/log roots preserve their entire artifact closure;
- retention/admission failure emits an event and metric;
- a residency check that cannot run is unavailable, never healthy;
- scheduling refuses a node outside the tenant residency policy;
- audit export cannot cross tenants even if IDs or cursor values are guessed;
- repeated export is deterministic and the signed manifest covers the exact
  JSONL content hash; and
- range parsing has explicit inclusive/exclusive semantics in the protocol and
  docs.

Likely areas: `internal/tenant`, `internal/eventlog`, `internal/artifact`,
`internal/session`, `internal/control/diag.go`, CLI command files,
`internal/compliance`, `spec/PROTOCOL.md`, `docs/operations.md` and a sim test.

### P1 — finish the Firecracker production proof

On a Linux x86_64 or arm64 host with KVM:

- build the guest agent and verify its manifest/hash;
- boot through the actual jailer and vsock bridge;
- prove file, exec, PTY and port operations;
- checkpoint a running guest, move it, restore disk/state/memory, and prove the
  process continues exactly once;
- exercise prepare-abort, prepare-commit, stale generation, incompatible CPU or
  Firecracker version, corrupt bundle and disk-full staging;
- prove TAP/netns deny-first behavior and cleanup after every failure;
- make doctor advertise only caps earned by that exact candidate; and
- archive the workflow/runbook output as point-in-time evidence.

Do not convert a skipped KVM job into a pass. Optional self-hosted means the PR
may merge without that host, but the product claim remains externally gated.

### P1 — close E15 browser-to-server and Gate 5 distribution evidence

- Run Playwright against the real embedded server rather than only mock routes.
- Exercise login/RBAC in the browser for operator, viewer and denied roles.
- Verify the browser never receives reusable upstream secrets.
- Run E12 against an actually published test package or documented local
  package artifact; local install alone does not prove PyPI/npm publication.
- Run E13 with two real MCP-capable harnesses when keys are present. Absence is
  an explicit skip, not a pass.
- Perform the under-five-minute clean-machine README flow in a disposable VM.

### P2 — external/vendor evidence

The ignored root `.env` contained names for OpenAI, E2B and Modal credentials,
but not all template, enrollment, binary, pool or hosted-server identifiers.
Do not invent them. Run only with explicit, disposable resources and verified
cleanup. Record commands, provider IDs, results and teardown in
`docs/engineering/verification-2026-09.md`.

Still externally gated:

- real E2B and Modal candidate runs;
- Fly/ix/SSH pool E17 evidence where credentials exist;
- real WorkOS OIDC/group/revocation;
- a real GitHub App for E14;
- non-AWS S3 conditional writes and multipart behavior;
- Temporal Cloud worker-restart evidence;
- real Slack signature/delivery behavior; and
- Docker/gVisor scale benchmarks on named hosts.

### P2 — signed release E18

E18 requires an actual signed public tag, release artifacts, Docker image,
Homebrew formula/tap, `go install`, SBOM and signature verification. This is an
irreversible public action and was not performed in this session. Obtain
explicit release authority, choose the version, ensure registry/tap signing
credentials are available, run from a clean commit, verify every installation
path reports the exact tag, and document rollback/yank behavior before tagging.

## 6. Known honesty constraints and residual risks

- Process-backend scale numbers are not isolation-backend performance.
- Simulated OIDC is not WorkOS evidence.
- Local MCP fakes are not two real harnesses.
- Local Git HTTP/token fakes are not a GitHub App.
- A generated/installable SDK directory is not a published PyPI/npm package.
- A release workflow definition is not an exercised signed release.
- Firecracker unit tests on macOS are not KVM execution evidence.
- Warm standby is bounded-RPO single-writer failover, not consensus.
- The browser mock suite is not a browser-to-real-server proof.
- A skipped secret-gated job is unavailable, never healthy.

There was one secret-handling incident during an earlier E1 test: an actual
OpenAI key was briefly written as a canary into a disposable workspace, then
deleted without being printed. It was not committed and did not appear in test
output. Rotate that key as a precaution. Future scans must plant a synthetic
canary with the same shape, never the real credential.

## 7. Safe continuation and ownership map

Parallelize only after the PR candidate is stable:

- integrator: protocol regeneration, branch/main reconciliation, full gates,
  closure ledger and release coordination;
- retention owner: `internal/tenant`, artifact/event/session retention,
  compliance export and doctor residency findings;
- Firecracker owner: `internal/workspace/firecracker`, its KVM integration lane
  and only the narrow lifecycle seam agreed with the integrator;
- console/distribution owner: real-server Playwright, SDK packaging and MCP
  harness evidence; and
- external verification owner: disposable vendor runs and teardown ledger,
  without changing core lifecycle code.

Before a lifecycle edit, restate authority, resource, irreversible action,
durable commit and observable postcondition. Add a test that makes the
forbidden result visible. Re-read shared files immediately before applying a
patch; never overwrite another agent's moving change.

## 8. Closure-document work still required

After the final candidate gates are green, append a section named
`2026-09 build plan disposition` to
`docs/engineering/implementation-closure-2026-09-03.md`. It must list every
Phase 2–6 numbered item, its ADR, implementation paths, focused test, broad
gate, external evidence and residual status. Update
`docs/engineering/README.md` so this continuation handoff is the current-status
entry while unfinished work remains.

Do not edit historical audit findings to make them appear closed. The closure
ledger is where status changes belong. Every claim must be one of implemented
and verified, implemented but externally gated, deliberately fail-closed, or
open with an owner and exact next proof.
