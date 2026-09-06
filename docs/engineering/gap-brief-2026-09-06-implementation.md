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

Not proven here: the gVisor and Firecracker `Reprobe` paths compile under
`GOOS=linux` but were not executed on this macOS host; the exact-host drift
lane (E26) and profile conformance (B33) are the conformance builder's, run on
Linux. See the follow-up section below.

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
| Browser lane | `images/browser`, `remount computer` CLI, Python/TS `Computer`, real-Chromium conformance (B34) run live in Docker, docs | in progress |
| Conformance product | `cmd/conformance --profile`, markdown report, `remount conformance`, evidence E26/B33, gVisor drift lane script, operator docs | in progress |
| Credentials | `remount binding`/`principal session` CLI, Python/TS helpers, keyless examples against a fake provider (B35), docs, live `.env` run with rotate and revoke | in progress |
| Lifecycle | `remount ws lease`/idle CLI, Python/TS methods, auto-sleep example, docs, live timer run across a server restart | in progress |
| Typed errors and observability | typed error classes in three languages, support matrix, dependency-free OTLP tracing, log redaction, ADR 0093 | in progress |

## Deferred by owner decision

- Signed public release, tags, package and image publishing (2026-09-03).
- Any hosted Remount service or dashboard (2026-09-06).
- The Firecracker KVM exact-host lane and the gVisor drift lane belong to a
  Linux session; both are recorded as `unavailable` on this host.
