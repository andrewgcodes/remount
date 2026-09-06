# Current implementation status

This is the maintained status index, checked against `main` on 2026-09-05
after the 2026-09-06 gap-brief work landed (the pre-brief baseline was
`9b5dff6`). It supersedes the
earlier reconciliation against PR #30 (`6b301c2`, merged as `b76d21d`) and the
documentation-only PR #31 merge `fd4f507`. It separates implemented code, dated
verification and remaining deployment requirements. It is not a
production-readiness certificate. Recheck source and evidence when the candidate
changes.

What landed on this branch, and exactly how each claim was proven, is in
[the gap-brief implementation record](gap-brief-2026-09-06-implementation.md).
That document is the evidence; the table below is the index.

For operating instructions start with [Using Remount](../using-remount.md).
For exact wire contracts use [the protocol](../../spec/PROTOCOL.md). Dated
audits, ADRs and handoffs record what was known on their own candidates; their
old “not built” or “unavailable” statements are not a current feature inventory.

## Implementation and evidence

| Surface | Current boundary | Where to verify |
|---|---|---|
| Workspace lifecycle and sessions | Generation/authorization fencing, durable release, bounded replay, stdin deduplication, snapshots and sleep/move are implemented. Gaps and failed completion must remain explicit. | `internal/control`, `internal/node`, `internal/session`, `internal/client`, `internal/sim`; [protocol](../../spec/PROTOCOL.md) |
| Process and Docker | Implemented; process has no host isolation and both use cooperative proxy egress. Neither satisfies `isolated` or `multi_tenant`. | [backend guide](../operations.md#choosing-a-backend), [capability matrix](../security-profiles.md) |
| gVisor | Linux `runsc` backend with node-owned deny-first networking; registration probes and E4 denial conformance are required. Satisfies `isolated`, not the microVM requirement of `multi_tenant`. | `internal/workspace/gvisor`, `internal/netns`, `scripts/gvisor-conformance.sh`; dated Linux evidence in [verification](verification-2026-09.md) |
| Firecracker | Linux KVM/jailer backend with enforced gateway and full disk/state/memory checkpoint support. Only a successfully constructed backend advertises microVM/multi-tenant capabilities. Restore requires compatible host/VMM/guest/CPU metadata. | [host guide](../../integration/firecracker/README.md), `internal/workspace/firecracker`, `.github/workflows/kvm.yml` |
| Controller failover | One active SQLite writer, with optional automatic warm-standby promotion using a conditional object-store lease, epochs, shipped recovery points and node reconciliation. Not multi-writer consensus or zero RPO. | [operations](../operations.md#controller-availability), `internal/control/replicate`, [E10](../../integration/failover/README.md) |
| Relay confidentiality | `internal/e2ee` and malicious-relay simulations exist. Stock client/node opt-in and the control-plane binding operation are **not** integrated. Normal CLI/SDK connections are not end-to-end encrypted through the relay. | [B6 implementation](plan-b-b6-relay-confidentiality.md), [pending integration](pending-protocol-rows-b6.md) |
| Durable Agents and composition | ACP/PTY adapters, durable transcripts/inboxes, approvals, schedules, fork and policy-constrained children are implemented. Harness resume capabilities vary. | [Agent API](../api.md), [harness guide](../harness-integration.md), `internal/node/agentrun.go` |
| Claude/Codex authentication | New launches and direct Agents require explicit `subscription` or `api-key` billing. API-key mode requires a brokered binding. Subscription mode is trusted-local, strips API-key overrides, verifies provider-native login, and uses exact-command-gated login/status/logout sessions that are active-client-only and non-durable. The selected mode commits before launching on new or reused workspaces; resume preserves it and handoff refuses nonportable subscription state. | [ADR 0098](../adr/0098-provider-subscription-auth-is-explicit-and-confidential.md), [harness guide](../harness-integration.md#explicit-billing-mode-and-confidential-provider-login), `internal/providerauth`, `internal/launch`, `internal/node`, `internal/session`, `internal/sim` |
| Operator console | Embedded at `/console/` with a real server adapter and browser-to-server Playwright tests. It is an operator surface, not an end-user chat/phone UI. | [console guide](../console.md), `web/tests/e15`, `internal/server/console*.go` |
| SDKs | Public Go API and local Python/TypeScript packages are implemented. Strict-profile protocol negotiation is tested across languages; this does not prove a hardened execution backend. | [external Go module](../../integration/publicsdk), [cross-language gate](../../integration/sdks/README.md) |
| Security and governance | Signed identity/enrollment, tenant boundaries, encrypted artifacts/key versions, egress approvals, hard budget reservation/settlement, retention and exports exist. Configuration and failed/unavailable diagnostics still determine the deployment claim. | `internal/identity`, `internal/artifact/encrypted`, `internal/control`, [operations](../operations.md) |
| Events | Control resource rows and their events commit through `control.transact` and `eventlog.Transact`. Node delivery is a separate sequenced/acknowledged boundary. Notification/export cursors advance after acceptance or the documented durable failure path. | [ADR 0050](../adr/0050-transactional-event-outbox.md), `internal/node`, `internal/eventlog/export.go`, `internal/notifier` |
| Provider pools | Whole nodes are tenant-scoped; provider isolation never upgrades backend caps. Drivers require provider-specific bootstrap and ownership metadata. The ix.dev driver still needs an external helper; a manually enrolled ix.dev node is not a native-driver proof. | [pool operations](../operations.md#provider-backed-node-pools), [ix.dev boundary](ix-dev-native-driver-2026-09-04.md), `internal/provision` |
| Runtime profiles | `dev`, `trusted-single-tenant`, `multi-tenant-isolated` and `microvm` are evaluated server-side from backend descriptors; a node's self-report can only downgrade. The `--profile` startup gate fails closed, `requires.profile` scheduling parks an unplaceable workspace as `profile_unschedulable`, drift makes a node unschedulable within one probe interval, and `doctor --profile` / `conformance --profile` are three-valued. gVisor passed `multi-tenant-isolated` live; `microvm` has **no** live pass on this branch. | [ADR 0089](../adr/0089-runtime-profiles-and-drift.md), `internal/profile`, [implementation record](gap-brief-2026-09-06-implementation.md#gap-1--production-isolation-profile), [profile guidance](../using-remount.md#choose-a-runtime-profile) |
| Dynamic bindings and session principals | Bindings are a durable, tenant-scoped, mutable store: create, list, get, rotate and revoke, with write-only secrets and revocation reaching a live workspace within one renew. `principal.session.create` issues an ephemeral workspace- and generation-bound principal. Substitution locations are `header`, `query`, `body_form` and `body_json`, fail-closed, with refusal text and audit fields redacted. Known limits are listed in the record. | [ADR 0091](../adr/0091-dynamic-binding-lifecycle-and-session-principals.md), [ADR 0092](../adr/0092-broker-substitution-locations-and-redaction.md), `internal/broker`, `internal/redact`, [implementation record](gap-brief-2026-09-06-implementation.md#gap-2--brokered-identity-credentials-egress-audit) |
| Computer (browser) sessions | `computer.{create,screenshot,input,navigate,eval,downloads,close,get}` over the port substrate, so any backend that can forward a workspace port can host one. Input is deduplicated by `iseq` and a resumed handle reads it back; downloads become tenant artifacts; failures are typed. Browser profiles are node-local by design and do not survive sleep or move; navigation policy is per host, never per URL. | [ADR 0088](../adr/0088-computer-sessions-over-port-substrate.md), [ADR 0094](../adr/0094-a-resumed-computer-handle-reads-the-input-sequence.md), `internal/computer`, [implementation record](gap-brief-2026-09-06-implementation.md#gap-3--computerbrowser-sessions), [user guide](../using-remount.md#computer-sessions-the-built-in-browser-api) |
| Workspace leases and idle policy | `ws.lease{,.renew,.cancel,.get}` and `ws.idle.{policy,mark}` hold a claimed workspace to a control-plane deadline that survives client death, node loss and a control-plane restart. Activity is explicit; session traffic does not extend a deadline. Expiry runs the release path with `exit.reason = lifecycle_deadline_expired` and degrades loudly rather than leaking. | [ADR 0090](../adr/0090-durable-workspace-leases-and-idle-policy.md), `internal/control`, [implementation record](gap-brief-2026-09-06-implementation.md#gap-4--durable-lifecycle-for-running-work), [user guide](../using-remount.md#hold-a-workspace-for-background-work) |
| Typed errors and tracing | Errors carry a stable `code` plus an optional stable `reason`; callers match with `api.Is` or the generated Python/TypeScript classes and never on message text, and an unknown reason degrades to the base class. Optional OTLP/HTTP span export goes to a collector the operator runs: no new dependency, no default endpoint, attributes scrubbed for credential shapes. | [ADR 0093](../adr/0093-typed-errors-support-matrix-and-dependency-free-tracing.md), `internal/proto`, `internal/trace`, [implementation record](gap-brief-2026-09-06-implementation.md#gap-5--sdk-conformance-release-observability), [observability](../observability.md#traces) |

## Verification is candidate-specific

- [The September verification ledger](verification-2026-09.md) contains native
  Windows, Linux gVisor/KVM, provider, browser, scale, handoff and regression
  runs. Read each entry's commit, host, prerequisites and cleanup; an old pass
  does not qualify a new candidate.
- The [PR #30 live regression pass](verification-2026-09.md#2026-09-05--pr-30-live-functional-regression-verification)
  exercised OpenCode with OpenAI on Docker, Claude on a disposable Modal VM,
  cross-language artifacts/sessions and reconnect/restart behavior. It did
  not rerun Windows, gVisor, KVM or every provider.
- [Benchmarks](../benchmarks.md) are generated from dated measurements. Do not
  promote their latencies, throughput or binary sizes into current guarantees.
- `make verify` is the portable CI-equivalent gate; SDK, web, host-isolation,
  provider and release workflows have additional gates. An unavailable tool,
  missing host or blocked CI job is not a pass.

## Remaining limits

- No Apple Virtualization backend, direct peer-to-peer transport, general RL
  scheduler, or end-user chat app. Computer sessions are a native **browser**,
  not a display session: there is no X11, window-manager or VNC resource in the
  protocol, and external X11/browser/VNC processes still run as ordinary
  workspace workloads when a whole desktop is required; see
  [virtual desktops](../harness-integration.md#browser-and-virtual-desktop-workloads).
- A browser profile is node-local by design and does not survive sleep, move or
  `remount pull`; navigation policy is per host, never per URL. The current
  state of brokered browsing to an allowed host is recorded in
  [the computer-session section](../using-remount.md#computer-sessions-the-built-in-browser-api)
  and in the [implementation record](gap-brief-2026-09-06-implementation.md#gap-3--computerbrowser-sessions).
- Brokered keys stay outside workspace state; harness-native logins do not.
  A compromised node can exfiltrate the upstream keys it sees, and broker TTL
  expiry is not provider-side key revocation.
- Filesystem moves do not transfer arbitrary processes or live connections.
  Firecracker's full-checkpoint path has its own compatibility constraints.
- The object store is part of controller-failover authority. Ordinary copied
  SQLite databases must never be run as independent active controllers.
- No automatic age-based package-cache eviction is claimed; capacity bounds and
  metrics are not a substitute for it. OpenTelemetry span export **is** now
  implemented, but only to a collector the operator runs: there is no default
  endpoint and no Remount-operated collector, so an unset `--otlp-endpoint`
  records nothing and sends nothing.
- The `microvm` runtime profile has no live pass on this branch. The
  Firecracker backend's dated 2026-09-04 x86_64 KVM evidence stands on its own
  terms; the profile lane is recorded as `unavailable`, which is not a pass.
- The standalone doctor reports unavailable checks such as
  `tenant.residency_unavailable` as incomplete: exit 2, `ok: false`, and
  `incomplete: true`. Exit 0 is reserved for a fully healthy result.
- Public release, package and Homebrew-tap readiness and independent security
  review remain separate from local builds and tests. The authenticated GitHub
  release inventory was empty on 2026-09-05 and tagging is deferred by owner
  decision. The vanity domain is the one exception: `remount.dev` and
  `get.remount.dev` were deployed and verified on 2026-09-05, so Go import
  metadata resolves today while `go install` and the installer URL still wait on
  the repository being public. See [release prerequisites](../releases.md) and
  [the vanity site](../../deploy/vanity/README.md).

## Maintaining this index

Update current user guides and this index when the implementation boundary
changes. Preserve original observations and candidate identifiers in dated
reviews; add new verification entries instead of retroactively rewriting a
failure or unavailable result as success. Record new architecture in a new
ADR. Regenerate `llms.txt` and `llms-full.txt` with `make docs` after updating
the guides included by their generator. Run `python3 scripts/test_gen_llms.py`
to check the default/overridden repository links, section targets, deterministic
generation and checked-in output freshness without network access.
