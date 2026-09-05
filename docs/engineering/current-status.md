# Current implementation status

This is the maintained status index, checked against PR #30 commit `47b379f`
(merged as `0cab8a1`, with an identical tree), then reconciled with the
documentation-only PR #31 merge `4e3066b` on 2026-09-05. It separates
implemented code, dated verification and remaining deployment requirements.
It is not a production-readiness certificate. Recheck source and evidence when
the candidate changes.

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
| Operator console | Embedded at `/console/` with a real server adapter and browser-to-server Playwright tests. It is an operator surface, not an end-user chat/phone UI. | [console guide](../console.md), `web/tests/e15`, `internal/server/console*.go` |
| SDKs | Public Go API and local Python/TypeScript packages are implemented. Strict-profile protocol negotiation is tested across languages; this does not prove a hardened execution backend. | [external Go module](../../integration/publicsdk), [cross-language gate](../../integration/sdks/README.md) |
| Security and governance | Signed identity/enrollment, tenant boundaries, encrypted artifacts/key versions, egress approvals, hard budget reservation/settlement, retention and exports exist. Configuration and failed/unavailable diagnostics still determine the deployment claim. | `internal/identity`, `internal/artifact/encrypted`, `internal/control`, [operations](../operations.md) |
| Events | Control resource rows and their events commit through `control.transact` and `eventlog.Transact`. Node delivery is a separate sequenced/acknowledged boundary. Notification/export cursors advance after acceptance or the documented durable failure path. | [ADR 0050](../adr/0050-transactional-event-outbox.md), `internal/node`, `internal/eventlog/export.go`, `internal/notifier` |
| Provider pools | Whole nodes are tenant-scoped; provider isolation never upgrades backend caps. Drivers require provider-specific bootstrap and ownership metadata. The ix.dev driver still needs an external helper; a manually enrolled ix.dev node is not a native-driver proof. | [pool operations](../operations.md#provider-backed-node-pools), [ix.dev boundary](ix-dev-native-driver-2026-09-04.md), `internal/provision` |

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

- No Apple Virtualization backend, native display/browser session implementation,
  direct peer-to-peer transport, general RL scheduler, or end-user chat app.
  External X11/browser/VNC processes can run as ordinary workspace workloads;
  see [virtual desktops](../harness-integration.md#browser-and-virtual-desktop-workloads).
- Brokered keys stay outside workspace state; harness-native logins do not.
  A compromised node can exfiltrate the upstream keys it sees, and broker TTL
  expiry is not provider-side key revocation.
- Filesystem moves do not transfer arbitrary processes or live connections.
  Firecracker's full-checkpoint path has its own compatibility constraints.
- The object store is part of controller-failover authority. Ordinary copied
  SQLite databases must never be run as independent active controllers.
- No automatic age-based package-cache eviction or OpenTelemetry span export
  is claimed. Capacity bounds and metrics are not substitutes for those.
- The standalone doctor can return `ok: true` with a warning such as
  `tenant.residency_unavailable`. Inspect findings and named checks; a zero
  exit alone is not evidence that every policy was checked.
- Public release/package/tap/vanity-domain readiness and independent security
  review remain separate from local builds and tests. The authenticated GitHub
  release inventory was empty on 2026-09-05; see [release prerequisites](../releases.md).

## Maintaining this index

Update current user guides and this index when the implementation boundary
changes. Preserve original observations and candidate identifiers in dated
reviews; add new verification entries instead of retroactively rewriting a
failure or unavailable result as success. Record new architecture in a new
ADR. Regenerate `llms.txt` and `llms-full.txt` with `make docs` after updating
the guides included by their generator. Run `python3 scripts/test_gen_llms.py`
to check the default/overridden repository links, section targets, deterministic
generation and checked-in output freshness without network access.
