# Gap brief 2026-09-06 — implementation record

This dispositions every item of [the gap brief](./gap-brief-2026-09-06.md)
against code, tests and dated evidence on branch
`claude/gap-brief-2026-09-06` (cut from `origin/main` `b6c8b18`). It is a
ledger, not a certificate: a row says what exists and how it was proven, and
an `unavailable` result is never a pass. Update it when the boundary moves.

Owner constraint honored throughout: nothing here is a hosted product. Every
operator surface (doctor, conformance, metrics, traces, console) runs against
the adopter's own deployment, and no release was tagged or published.

## Cross-cutting decisions

| Decision | Where |
|---|---|
| Typed errors are an additive `reason` on the wire `Error`, never new codes; SDKs map `(code, reason)` to classes and callers never match on message text. | `internal/proto/proto.go` (`Reason*`, `ErrReason`, `ErrorReason`), [PROTOCOL §2](../../spec/PROTOCOL.md) |
| One builder per gap in its own git worktree; the integrator merged and resolved the additive conflicts in `internal/proto/types.go`, `internal/control/control.go`, `internal/node/node.go`, `internal/metrics/metrics.go` and `spec/PROTOCOL.md`. | commits on the branch |
| Live verification with the real keys in `.env` is required after the unit gate for every credential, egress, browser and lifecycle change; results go to [the verification ledger](./verification-2026-09.md). | owner instruction 2026-09-06 |

## Gap 1 — production isolation profile

Delivered (ADR [0089](../adr/0089-runtime-profiles-and-drift.md)):

- `internal/profile`: `dev`, `trusted-single-tenant`, `multi-tenant-isolated`,
  `microvm`, evaluated server-side from backend descriptors only; a node's
  self-reported runtime checks can only downgrade.
- Node `--profile` (env `REMOUNT_PROFILE`) startup gate that fails closed and
  refuses `process`/`docker` under the two production profiles; a reprobe loop
  (`--profile-health-interval`, default 30s) with `workspace.Reprober`
  implementations for gVisor and Firecracker; events
  `node.profile.{verified,unschedulable,restored}`; metrics
  `remount_nodes_online`, `remount_nodes_profile_unschedulable`,
  `remount_profile_drift_total`.
- Scheduling honors `requires.profile`; a workspace no node satisfies stays
  pending with `pending_reason: profile_unschedulable`; the node re-validates
  at materialize.
- Control op `node.profile.get`; `remount doctor --profile X [--node] [--json]`
  exits 0 pass, 1 any fail, 2 unavailable-without-fail.
- `Finding.Status` is `pass|fail|unavailable`; empty is unavailable.

Proof: `internal/profile` unit tests over synthetic descriptors for every
backend and profile; sim tests `TestProfileSchedulingRequiresEvidence`,
`TestProfileDriftMakesNodeUnschedulable`, `TestNodeProfileGetReportsEveryNode`;
control test that a self-reported pass cannot upgrade a failing descriptor;
`cmd/remount` gate tests; a live standalone smoke of every `doctor --profile`
exit path.

Conformance product (no new ADR; it implements ADR 0089's contract):
`cmd/conformance --profile` and `remount conformance --profile` add one
required row per profile obligation plus a scheduling row that proves
`requires.profile` is honored, `--markdown` writes the review document, exit
codes are 0/1/2, `make conformance-report` wraps a fresh standalone, and
evidence gates B33 (profile conformance) and E26 (exact-host drift, gVisor)
are registered. `docs/operations.md` and `docs/security-profiles.md` carry the
profile column.

Live on macOS (process backend): `--profile dev` conformant at exit 0
(70 checks, 62 passed, 8 unavailable); `--profile multi-tenant-isolated`
exit 1 with the seven profile obligations failing by name and the parked
workspaces reported `profile_unschedulable`; `microvm` host-compat exit 2.

Live on Linux (Colima Ubuntu aarch64 VM with `runsc release-20260831.0`,
ledger entry "live gVisor isolation, profile and drift lanes in a Colima VM,
2026-09-05"): the gVisor lanes E4, E5 and B28 passed; the E26 drift lane
passed in 5 s; a real node started under `--profile multi-tenant-isolated`
with the gVisor backend, `doctor --profile multi-tenant-isolated` exited 0
with nine checks passing, a workspace with `requires.profile` scheduled and
ran, `remount conformance --profile multi-tenant-isolated --backend gvisor`
judged 78 checks with 64 passed, 0 failed, 14 unavailable and every required
row green, and removing a host prerequisite out of band produced
`node.profile.unschedulable` within a second, `doctor` exit 1, a parked
workspace with `pending_reason profile_unschedulable` and a refused claim,
with `node.profile.restored` after the prerequisite came back. gVisor's real
descriptor satisfies every `multi-tenant-isolated` predicate exactly as ADR
0089 states.

The lane found and fixed two gVisor defects with unprivileged regression
tests: a workspace whose sandbox never started could never be materialized
again (Adopt/Create looped forever), and a symlinked rootfs path passed every
probe but failed every materialization because `runsc` cannot serve a
symlinked OCI root. Residual: a failed `runsc create` can leave an empty
cgroup directory; recorded, not fixed.

Firecracker B29 is `unavailable` on this host with the exact missing items
recorded (pool permissions, a guest image whose init does not export `PATH`,
1.3 GiB free in the pool); the 2026-09-04 x86_64 pass stands. `microvm` has
no live pass on this branch.

## Gap 2 — brokered identity, credentials, egress, audit

Delivered (ADRs [0091](../adr/0091-dynamic-binding-lifecycle-and-session-principals.md),
[0092](../adr/0092-broker-substitution-locations-and-redaction.md)):

- Durable, tenant-scoped, mutable binding store seeded from `--bindings` on
  first start; ops `binding.{create,list,get,rotate,revoke}` with events and
  write-only secrets; kinds `api_key|bearer|cookie|header`, `methods`,
  `path_prefixes`, `retention` metadata; revocation reaches a live workspace
  within one renew through `binding_revision` on `ws.renew`.
- `principal.session.create`: an ephemeral principal plus its workspace- and
  generation-bound capability in one call.
- Substitution locations `header|query|body_form|body_json` with bounded,
  fail-closed body handling; placeholders in a query or body aimed at an
  unbound host are `leak_blocked`.
- Every broker refusal carries `X-Remount-Reason` and a JSON error body with
  `code`, `reason`, `binding`, `host`; `internal/redact` scrubs
  credential-shaped strings from refusal text and audit fields; revoking
  egress cuts the gateway synchronously.
- `events.tail` filters `binding`, `host`, `types`; `remount events
  --binding --host --type`.
- Go SDK: `CreateBinding`, `ListBindings`, `GetBinding`, `RotateBinding`,
  `RevokeBinding`, `CreateSessionPrincipal`, `RevokePrincipal`,
  `CredentialEvents`.

Proof: broker tests for each substitution location, foreign-host body leak,
ambiguity refusals and redaction with a planted canary; control tests for
lifecycle, restart persistence, idempotent replay and secret-never-returned;
`TestConcurrentBudgetReservationsCannotOverrun`; sim tests
`TestBindingRevokeStopsSubstitutionWithinRenew`,
`TestBindingRotateInvalidatesOldLease`,
`TestRevokedPrincipalCannotOpenAttachReadOrLease`,
`TestSessionPrincipalIsWorkspaceAndGenerationBound`,
`TestFleetRevokeEgressKillsGatewayAccess`.

Live: a real-provider smoke at `d64e175` (before the dynamic store) proved
substitution at `api.openai.com` and `api.anthropic.com`, the OpenAI
placeholder blocked at the Anthropic host with a `leak_blocked` audit, unbound
hosts denied, and zero key-shaped strings in the workspace or logs. Note for
adopters: substitution happens on the broker's reverse-proxy path
(`/d/<host>/...`); CONNECT tunnels stay opaque and host-granular.

Known limits: `approval_required` is not surfaced as a control-plane refusal
because approvals are returned as values; the process backend can cut its
broker but cannot stop a workspace reaching the network around it; the
package and git connectors substitute in headers only.

Follow-up delivered (ADR [0096](../adr/0096-the-credential-surface-is-the-cli-the-sdks-and-runnable-examples.md)):
`remount binding create|ls|get|rotate|revoke|preset apply` (secrets read from
a named environment variable of the CLI process, never an argument),
`remount principal session`, Python and TypeScript credential helpers,
`docs/credentials.md`, `internal/testutil/fakeprovider`, and three keyless
examples (`brokered-model-call` header, `brokered-search-api` query,
`brokered-custom-http` JSON pointer) run in-process by `integration/examples`
as evidence B35. It fixed three 2A/2B defects with failing-first tests:
`methods`/`path_prefixes` on a binding were not enforced, a revoked binding's
placeholder was forwarded upstream as an inert string, and the new `revoked`
decision was logged as `egress.allowed`. Live with the real keys: presets for
OpenAI and Anthropic answered 200 at their bound hosts, the OpenAI placeholder
at the Anthropic host got a typed `egress_denied` refusal, rotation moved to
revision 2 with every probe still 200, revocation was refused as `revoked`
within 4 s, `remount events --binding` showed the lifecycle and denials, and
an exact-bytes scan found the keys only in the control plane's database.
Revoking one binding withdraws every binding the workspace declares, now as
a typed refusal rather than silently. `principal session` could not be
exercised live because no CLI mints a one-time node enrollment credential for
a production-mode server; that gap has its own follow-up.

## Gap 3 — computer/browser sessions

Delivered (ADR [0088](../adr/0088-computer-sessions-over-port-substrate.md)):

- Ops `computer.{create,screenshot,input,navigate,eval,downloads,close,get}`
  over a node-side Chrome DevTools Protocol client that rides the existing
  port-session substrate, so every backend that can open a port session can
  drive a browser; events `computer.{created,closed,download,degraded}`.
- Input is batched and deduplicated by `iseq`; coordinates are CSS pixels
  from the top-left of the declared viewport; screenshots are PNG with an
  8 MiB cap; downloads become tenant-scoped artifacts.
- Failures are typed by reason: `browser_crashed`, `display_unavailable`,
  `input_rejected`, `navigation_denied`, `profile_corrupt`,
  `download_blocked`, `backend_unsupported`.
- Go SDK `Computer` type; generated Python and TypeScript types.

Proof: `internal/computer` unit tests against a fake CDP server; node tests;
sim tests `TestComputerAttachFakeCDP`, `TestComputerInputDeduplicatesByISeq`,
`TestComputerCrashReportsClosed`, `TestComputerDownloadBecomesArtifact`,
`TestComputerClosesWithWorkspace`.

Known limits: the browser profile lives under the snapshot-excluded
`.remount` directory, so it is node-local and does not survive sleep or move
(documented, deliberate: cookies never travel in a snapshot); navigation
policy is per host because the broker does not terminate TLS; Firecracker
can drive a browser but reports downloads as `backend_unsupported`.

Follow-up delivered: `images/browser` (Chromium plus `socat`, health probe),
`remount computer create|get|screenshot|click|type|key|scroll|navigate|eval|downloads|close`,
Python and TypeScript `Computer` classes, `integration/browser` conformance
(evidence B34) and `scripts/browser-conformance.sh`. The live lane ran real
Chromium 152 inside the Colima VM on the docker backend and verified create,
`file://` navigation, click, screenshot change, typing into an input, a
contenteditable and an iframe, download to artifact with its event,
unbound-host navigation denied with an `egress.denied` audit, crash reported
as `browser_crashed`, and sleep/wake dropping the computer and profile while
the filesystem survived. It found and fixed three defects: Chromium ignores
`--remote-debugging-address` (a forwarder now bridges the port), Chromium's
`$HOME/.config` broke `ws.sleep` (the browser's home is now its profile
directory), and a fresh handle restarted `iseq` at 1 so every CLI action
after the first was dropped (`computer.get` now returns `last_iseq` and
handles resume from it; ADR 0094). Open: Chromium never sends
`Proxy-Authorization`, so brokered navigation to an allowed host is refused
as unauthenticated; the fix is the CDP `Fetch.authRequired` handler, in
progress.

## Gap 4 — durable lifecycle for running work

Delivered (ADR [0090](../adr/0090-durable-workspace-leases-and-idle-policy.md)):

- Ops `ws.lease`, `ws.lease.renew`, `ws.lease.cancel`, `ws.lease.get`,
  `ws.idle.policy`, `ws.idle.mark`; the workspace carries `lease`,
  `idle_policy`, `idle_since`, `last_activity_at` and the derived
  `lifecycle_deadline` a new client reads without touching timers.
- Deadlines are one durable timer row per workspace in the control plane,
  replayed after restart, fenced by the workspace row and by generation for
  renew, superseded synchronously by move, destroy and cancel; expiry runs the
  release path with reason `lifecycle_deadline_expired`, a graceful TERM then
  KILL on the node (`LifecycleGrace`, default 5s), and an explicit exit reason
  in the session replay; failure after the retry bound emits
  `ws.lifecycle.expiry_failed` and degrades rather than leaks.
- Quotas `MaxLeaseSec` (24h) and `MaxHeldWorkspacesPerTenant` (256); gauges
  `remount_workspaces_held`, `remount_workspaces_paused`; hold counters.
- Activity is explicit (`ws.idle.mark`, lease, renew); session traffic does
  not extend a deadline.

Proof: control tests including `TestLifecycleDeadlineSurvivesControlRestart`;
sim tests `TestWorkspaceLeaseAutoSleepsAfterDeadline`,
`TestWorkspaceLeaseRenewPreventsSleep`, `TestWorkspaceLeaseCancelKeepsClaimed`,
`TestIdlePolicySleepsAfterMarkIdle`, `TestMarkActiveResetsIdleDeadline`,
`TestLongCommandAutoSleptReplaysExplicitExitReason`,
`TestWorkspaceMoveSupersedesLeaseTimer`, `TestDuplicateLeaseRequestIsOneTimer`,
`TestLeaseExpiryDuringNodeCutNoSplitBrain`, `TestLeaseQuotaPerTenant`. Two
base defects were found and fixed on the way: the wake scan fired lifecycle
timers as resumes, and `paused` was published before the spent deadline was
cleared.

Follow-up delivered: `remount ws lease|idle-policy|mark-idle|mark-active`,
`ws get` prints the hold and deadline, Python and TypeScript methods,
`examples/long-running-autosleep` with an in-process test, a user-guide
section and the tutorial reference. The live lane on a real standalone
verified all four claims: a 30 s hold outlived a SIGKILLed client and paused
the workspace with `s.exited{reason: lifecycle_deadline_expired}`; a 90 s hold
fired after the control plane was stopped and restarted mid-deadline; an idle
policy slept the workspace after `mark-idle`; `mark-active` cleared a pending
deadline. The lane found a third defect: an attached client hung forever on
expiry because output subscriptions were cut before the exit chunk was
written (also affected `ws sleep` and `ws move`). Fixed by draining
subscribers after every session is joined; `TestLiveSessionReceivesLifecycleExitChunk`
failed first and now passes. Ledger entry in the verification file.

## Gap 5 — SDK, conformance, release, observability

Delivered so far:

- Runnable public examples `examples/minimal-shell` and
  `examples/artifact-transfer` with an in-process end-to-end test
  (`integration/examples`); `CHANGELOG.md`; `docs/compatibility-policy.md`;
  a schema-upgrade test with a pinned older fixture
  (`internal/control/migrate_upgrade_test.go`); `make protogen` and
  `make protogen-check`.
- Release mechanics (SBOM, checksums, reproducible build, install tests) were
  already scripted; they remain unexercised because tagging and publishing
  are deferred by owner decision.

## Follow-up builders (in progress at the time of writing)

| Builder | Scope | Status |
|---|---|---|
| Browser lane | `images/browser`, `remount computer` CLI, Python/TS `Computer`, real-Chromium conformance (B34) run live in the Colima VM, docs | merged (5a425b6); brokered browsing to an allowed host still needs CDP proxy auth (follow-up running) |
| Conformance product | `cmd/conformance --profile`, markdown report, `remount conformance`, evidence E26/B33, gVisor drift lane script, operator docs | merged (ced0f00) |
| Linux lanes | gVisor E4/E5/B28, E26 drift, a real `multi-tenant-isolated` gVisor node with `doctor` and `conformance --profile`, Firecracker B29 if its environment survived, all inside the Colima VM | merged (e55593d); gVisor verified, Firecracker unavailable |
| Credentials | `remount binding`/`principal session` CLI, Python/TS helpers, keyless examples against a fake provider (B35), docs, live `.env` run with rotate and revoke | merged (29bae6b); `principal session` live test unavailable until a node can enroll in production mode from the CLI (follow-up running) |
| Lifecycle | `remount ws lease`/idle CLI, Python/TS methods, auto-sleep example, docs, live timer run across a server restart | merged (39f951d) |
| Typed errors and observability | typed error classes in three languages, support matrix, dependency-free OTLP tracing, log redaction, ADR 0093 | merged (b94530d); spans verified live against a local collector |

## Deferred by owner decision

- Signed public release, tags, package and image publishing (2026-09-03).
- Any hosted Remount service or dashboard (2026-09-06).
- The Firecracker KVM exact-host lane and the gVisor drift lane belong to a
  Linux session; both are recorded as `unavailable` on this host.
