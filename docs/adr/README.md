# Architecture decision records

One file per decision: what was decided, why, and what it cost. An ADR is a
record of a choice made at a point in time, not a status page — if you want to
know what is currently built and verified, read
[current implementation status](../engineering/current-status.md) and
[the design document](../design.md). An old ADR's "not built" or "pending" line
describes its own candidate, not today's tree.

New decisions go in a **new** file. An existing ADR is never edited to reflect a
later change and never renumbered; a decision that is superseded gets a
successor that says so. The sequence has gaps — no file in this repository
carries 0020-0038, 0042 or 0046.

| # | Decision |
|---|---|
| [0001](0001-why-remount-exists.md) | Why Remount exists |
| [0002](0002-one-frame-type.md) | One frame type |
| [0003](0003-session-is-a-log.md) | A session is a log, not a socket |
| [0004](0004-log-is-truth.md) | The event log is the truth; state is a cache |
| [0005](0005-no-plugin-abi.md) | Backends are compiled in, not plugins |
| [0006](0006-snapshots-are-files.md) | Snapshots are files, never memory |
| [0007](0007-secret-blind-workspaces.md) | The workspace never holds a credential |
| [0008](0008-dumb-relay.md) | The relay is dumb and the control plane carries no data |
| [0009](0009-claim-queue.md) | Placement is a claim queue, not a scheduler |
| [0010](0010-trust-boundaries.md) | Trust boundaries |
| [0011](0011-ready-handshake.md) | Claiming is not serving |
| [0012](0012-outbound-only.md) | Nothing listens |
| [0013](0013-durable-fleet-quarantine.md) | Fleet quarantine is durable and destruction needs two proofs |
| [0014](0014-typed-egress-capabilities.md) | Typed egress capabilities and honest enforcement |
| [0015](0015-managed-package-connectors.md) | Managed package retrieval and scope-private cache references |
| [0016](0016-transactional-lifecycle-state.md) | Transactional resource rows own lifecycle authority |
| [0017](0017-snapshot-consistency.md) | Live snapshots and authoritative checkpoints are different operations |
| [0018](0018-resource-bounds-and-retention.md) | Long-lived state is bounded and retention fails truthfully |
| [0019](0019-public-api-and-v1-negotiation.md) | Public clients use resource types and protocol v1 negotiates exactly |
| [0039](0039-harness-auth-model.md) | Recipes are data; brokered keys and workspace-resident logins are distinct |
| [0040](0040-named-capabilities.md) | Security semantics are named capabilities, not additive fields |
| [0041](0041-stable-paths-handoff-queue.md) | Stable logical paths, hand-off and the durable task queue |
| [0043](0043-agent-resource.md) | An Agent is a durable control-plane resource, not a process |
| [0044](0044-approvals.md) | Approvals are control-plane rows that never outlive their run |
| [0045](0045-acp-client-no-new-dependency.md) | ACP is spoken by a hand-written client with schema-generated types |
| [0047](0047-interrupt-detaches.md) | Ctrl-C detaches; the timeout is the node's |
| [0048](0048-cred-used-records-outcome.md) | `cred.used` records the upstream outcome |
| [0049](0049-structural-guards.md) | Structural guards ahead of the durable runtime |
| [0050](0050-transactional-event-outbox.md) | Every resource commit and its events are one transaction |
| [0051](0051-revocation-epochs.md) | Revocation is a pushed epoch, fail closed, bounded by the lease |
| [0052](0052-local-directory-sync.md) | Local directories travel as artifacts; overlays land by rename |
| [0053](0053-base-images-from-snapshots.md) | Bases are pinned snapshots resolved at create time |
| [0054](0054-git-connector-and-server-side-clone.md) | A typed git connector and a clone that happens before `ws.ready` |
| [0055](0055-autostart-local-standalone.md) | `remount run` starts a local standalone when nothing answers |
| [0056](0056-agent-http-api.md) | The agent HTTP API is a translation layer, not a second control plane |
| [0057](0057-children-schedules-triggers.md) | Children, schedules and triggers reuse the Agent inbox |
| [0058](0058-preview-origin-cookie-scope.md) | The browser cookie authenticates only what a browser navigates to |
| [0059](0059-per-peer-lifecycle-ordering.md) | A peer's connect and disconnect are ordered per peer id |
| [0060](0060-stable-link-json-takes-header-only.md) | The stable link's JSON form takes a header credential only |
| [0061](0061-gvisor-enforced-gateway.md) | gVisor workspaces use one deny-first network namespace |
| [0062](0062-vendors-provision-whole-nodes.md) | Vendors provision whole Remount nodes |
| [0063](0063-firecracker-checkpoints.md) | Firecracker checkpoints compose four probed host boundaries |
| [0064](0064-declarative-node-pools.md) | Declarative node pools reconcile whole machines |
| [0065](0065-encrypted-artifacts.md) | Artifact identity is plaintext-derived and storage is tenant-encrypted |
| [0066](0066-durable-egress-approvals.md) | Egress approvals are durable request fences |
| [0067](0067-authoritative-budgets-and-response-metering.md) | Broker budgets reserve centrally and settle observed usage |
| [0068](0068-principal-and-enrollment-identity.md) | Principal identity, session capabilities and node enrollment |
| [0069](0069-external-secret-sources.md) | Binding values resolve from external secret sources at lease time |
| [0070](0070-multi-tenant-control-plane.md) | Multi-tenant control plane, quotas, and usage metering |
| [0071](0071-provider-webhooks-and-durable-notifications.md) | Provider webhooks enter the event log; notifications leave through durable cursors |
| [0072](0072-chunked-snapshots.md) | Chunked snapshots are canonical manifests over plaintext FastCDC chunks |
| [0073](0073-tiered-session-log.md) | Tiered session logs commit references before reclaiming disk |
| [0074](0074-warm-standby-control-plane.md) | Warm-standby control-plane replication |
| [0075](0075-shared-volume-semantics.md) | Shared data volumes are immutable versions with read-only mounts |
| [0076](0076-resumable-event-exports.md) | Event exports are bounded, resumable, and at least once |
| [0077](0077-operator-console.md) | A bounded, embedded operator console |
| [0078](0078-generated-cross-language-sdks.md) | Cross-language SDKs derive types and preserve cursor semantics |
| [0079](0079-mcp-distribution-boundaries.md) | MCP distribution is a bounded client adapter |
| [0080](0080-per-tenant-retention-and-signed-audit-export.md) | Per-tenant retention removes content in place; audit export is tenant-scoped and signed |
| [0081](0081-relay-payload-confidentiality.md) | The relay routes payloads it cannot read |
| [0082](0082-the-session-log-seal-stays-on-the-critical-path.md) | The session-log seal stays on the caller's critical path |
| [0083](0083-every-admitted-upstream-attempt-is-charged.md) | Every admitted upstream attempt is charged, on every egress surface |
| [0084](0084-pool-scale-down-fences-the-node-before-the-provider-destroy.md) | Pool scale-down fences the node before the provider destroy |
| [0085](0085-fly-machine-launches-are-secret-version-fenced.md) | Fly Machine launches are secret-version fenced |
| [0086](0086-fly-processes-select-enrollment-secrets-explicitly.md) | Fly processes select enrollment secrets explicitly |
| [0087](0087-release-cycles-use-a-durable-ordered-epoch.md) | Release cycles use a durable ordered epoch |
| [0088](0088-computer-sessions-over-port-substrate.md) | Computer sessions ride the port substrate, not a workspace daemon |
| [0089](0089-runtime-profiles-and-drift.md) | Named runtime profiles, a fail-closed startup gate, and drift detection |
| [0090](0090-durable-workspace-leases-and-idle-policy.md) | Durable workspace leases and idle policy |
| [0091](0091-dynamic-binding-lifecycle-and-session-principals.md) | Dynamic binding lifecycle and session-scoped principals |
| [0092](0092-broker-substitution-locations-and-redaction.md) | Broker substitution locations, typed refusals, and redaction |
| [0093](0093-typed-errors-support-matrix-and-dependency-free-tracing.md) | Typed errors, a generated support matrix, and dependency-free tracing |
| [0094](0094-a-resumed-computer-handle-reads-the-input-sequence.md) | A resumed computer handle reads the node's input sequence |
| [0095](0095-browser-proxy-auth-through-cdp.md) | Browser proxy auth through CDP |
| [0096](0096-the-credential-surface-is-the-cli-the-sdks-and-runnable-examples.md) | The credential surface is the CLI, the SDKs and runnable examples |
| [0097](0097-node-enrollment-is-an-operator-command.md) | Node enrollment is an operator command, and the floor is checked before the credential is spent |
| [0098](0098-provider-subscription-auth-is-explicit-and-confidential.md) | Provider subscription auth is explicit and confidential |

## Where to start

- **The shape of the system:** 0001, 0002, 0003, 0004, 0008, 0009, 0012.
- **Why a workspace holds no key:** 0007, 0014, 0048, 0051, 0067, 0069, 0091, 0092.
- **What "isolated" is allowed to mean:** 0010, 0040, 0061, 0063, 0089.
- **Work that outlives its client:** 0016, 0017, 0043, 0047, 0050, 0073, 0090.
- **Running it for other people:** 0018, 0068, 0070, 0074, 0080, 0087.
