# ADR 0089: Named runtime profiles, a fail-closed startup gate, and drift detection

## Status

Accepted.

## Context

Remount already has a per-workspace security contract. `proto.SecuritySpec`
carries `profile: local|isolated|multi_tenant` plus explicit minimums, and
`proto.ValidateBackendSecurity` refuses to place a workspace on a backend whose
`proto.BackendDescriptor` does not prove every requested property. The server
has a separate `--mode standalone|production-single-tenant|production-multi-tenant`
which gates control-plane configuration (`internal/server/server.go`,
`validateSecurityMode`). Both work.

What is missing is a statement about the *machine*. Three gaps produced it:

1. **Nothing describes a node's posture as a whole.** A node registering
   `gvisor` and `process` together looks strong for one workspace and weak for
   another. There was no supported way to say "this machine only runs backends
   fit for mutually untrusted tenants", and no way to refuse to start one that
   is not.

2. **Backend capabilities are static after construction.** Every backend's
   `Caps()` is a constant expression evaluated once. `gvisor.New` verifies
   runsc, the rootfs, the runsc state root, orphaned namespaces and
   `network.Probe` before returning; `firecracker.New` runs four named probes
   and sets a `verified` flag that a zero descriptor depends on. All of that is
   proved exactly once, at start-up. If `CAP_NET_ADMIN` is dropped, `/dev/kvm`
   disappears, runsc is replaced, or the cgroup parent is removed an hour
   later, the node keeps advertising the same descriptor and the control plane
   keeps placing untrusted work on it. There was no reprobe loop of any kind
   (`eventLoop`, `artifactGCLoop`, `renewLoop`, `fenceLoop` are the whole set).

3. **There is no scheduling constraint expressed in these terms.**
   `Requires.Caps` matches `NodeInfo.Caps`, which the type's own comment calls
   "never security evidence", and `Placement.Allow` matches operator labels. An
   operator who wanted "only isolated machines" had to encode it as a label,
   which any node can claim.

## Decision

### The four profiles

`internal/profile` defines four names, weakest first:

| Profile | Claim |
|---|---|
| `dev` | none; process and Docker are fine |
| `trusted-single-tenant` | isolated from the host, secrets brokered |
| `multi-tenant-isolated` | mutually untrusted workloads |
| `microvm` | `multi-tenant-isolated` on verified microVMs only |

A node claims one with `remount up --profile` (env `REMOUNT_PROFILE`, default
`dev`). A workspace requires one with `Requires.Profile`.

### The predicates, and the exact fields they read

Every predicate reads `proto.BackendSecurityCaps`, which
`workspace.Registry.Descriptors` fills from the backend's own `Caps()`. The
values as of this ADR:

| Backend | `Isolation` | `MultiTenant` | `SiblingIsolation` | `EgressMode` | `BrokerIdentity` | `NetworkNamespace` | `DeviceIsolation` |
|---|---|---|---|---|---|---|---|
| `process` | `none` | false | false | `cooperative_proxy` | `token` | false | false |
| `docker` | `container` | false | false | `cooperative_proxy` | `token` | true | true |
| `gvisor` | `container` | false | true | `enforced_gateway` | `per_session_capability` | true | true |
| `firecracker` verified | `microvm` | true | true | `enforced_gateway` | `per_session_capability` | true | true |
| `firecracker` unverified | `none` | false | false | `open` | `none` | false | false |

(`EgressMode` is the descriptor value: `Registry.Descriptors` rewrites it to
`enforced_gateway` whenever `Caps.EgressEnforced` is set, and to `open` when
the backend named nothing.)

The predicates hold over **every** registered backend, not some backend. A
strong backend never launders a weak one sharing the same node — the same
reason `BackendDescriptor` exists per backend rather than as a node-wide union.

- `dev`: at least one backend is registered (`profile.backend.registered`).
- `trusted-single-tenant`: adds `Isolation != none`
  (`profile.backend.isolated`) and `BrokerIdentity != none`
  (`profile.backend.brokered_secrets`). Docker qualifies; process does not; an
  unverified Firecracker does not.
- `multi-tenant-isolated`: adds `profile.backend.untrusted_absent` (no backend
  *named* `process` or `docker` is registered at all),
  `SiblingIsolation`, `EgressMode == enforced_gateway`, `NetworkNamespace`,
  `IsolationRank >= container`, and `profile.runtime.healthy` (every host check
  the node reports passes). gVisor and verified Firecracker qualify.
- `microvm`: adds `Isolation == microvm`, `MultiTenant`, `DeviceIsolation` —
  which together are exactly a verified Firecracker — and
  `profile.host.microvm_compat`, a host compatibility result only the node can
  produce.

`untrusted_absent` is deliberately redundant with the capability predicates
today: Docker already fails sibling isolation and enforced egress. It is a
name check as well as a property check so that a future `Caps` change cannot
make a shared-kernel backend pass by accident.

### Node self-reports may only downgrade

`NodeInfo.RuntimeChecks` and `WSRenewReq.RuntimeChecks` carry
`proto.Finding`s the node observed about its own host. `profile.Evaluate`
takes them as an argument, but no descriptor predicate consults them. Their
only effect is `profile.runtime.healthy`, which fails or goes unavailable when
one of them does. A node reporting `pass` for every check name in the package
still fails `multi-tenant-isolated` if its descriptors say `process`
(`TestNodeSelfReportCannotUpgradeADescriptor`).

The rule is asymmetric on purpose. Downgrading is safe to believe from an
untrusted source: the worst a lying node achieves is refusing work. Upgrading
is not: it is precisely the claim the descriptor mechanism exists to make
unfalsifiable.

`profile.host.microvm_compat` is the one check with no server-side source.
Absence is `unavailable`, never `pass`, so the honest outcome of "no node ever
said anything about this host" is that the `microvm` profile is not satisfied.

### The startup gate is fail-closed and pre-registration

`cmd/remount.buildNode` refuses `process` and `docker` by name before
construction when the profile is `multi-tenant-isolated` or stronger. Leaving
them out of the registry is what makes the refusal unbypassable: a backend
that was never registered cannot be selected by a workspace, a label, or a
later drift.

After every backend is constructed, `gateRuntimeProfile` runs one drift probe
and `profile.Evaluate`. A report that is not `pass` prints as JSON on stderr
and the process exits non-zero (exit 1, through `main`'s error path). The node
never reaches `node.New`, so it never enrolls, never advertises a descriptor
and never receives an offer. This preserves the existing rule that a node fails
closed when a requested backend's constructor fails
(`cmd/remount/build_node.go`, asserted by `cmd/remount/main_test.go`).

### Drift detection

`workspace.Reprober` is an optional backend interface:
`Reprobe(ctx) []proto.Finding`. gVisor implements it as runsc `--version`, a
rootfs stat, and `netns.Manager.Probe`. Firecracker implements it as the
machine, network, volume and guest probes plus a compatibility fence, and
`JailerFactory.Reprobe` re-runs only the read-only half of `Probe`.

Reprobe is read-only by contract. It must never call `reapStateRoot`,
`netns.ReclaimOrphans`, `recoverOwnedJails`, `recoverOwnedCgroups` or take the
host ownership lock. Those steps are safe in a constructor precisely because
nothing this process owns can exist yet; running them against a serving node
would destroy live sandboxes and reclaim the namespaces they inhabit.
`JailerFactory.Probe` also memoizes success, so it cannot see drift at all;
that is why the read-only entry point is separate, and why a factory that does
not offer one is reported as a *failing* machine check rather than a pass it
did not earn.

`node.profileHealthLoop` probes immediately at start and then every
`Options.ProfileHealthInterval` (default 30s, `--profile-health-interval`).
Thirty seconds is short enough that a lost guarantee is visible within one
lease window on a production deployment and long enough that the probe costs
nothing measurable. The probe holds neither `n.mu` nor `n.profileMu` while it
runs host I/O.

### Drift reaches the control plane on renewal

Renewal is the existing periodic node→control channel, so drift rides on it
rather than opening a second one. `WSRenewReq` gains `profile`,
`runtime_checks` and `report_checks`; a node with no workspaces still sends a
renewal when its checks changed, so an idle node reports drift at the same
cadence. The node clears its pending flag only after control accepts the
frame, so a failed renewal leaves the checks pending instead of losing them.
A reconnecting node also carries them in its hello.

### The control plane decides, and the decision is an event

`eligibleBackendLocked` honors `Requires.Profile` by evaluating the node's
descriptors, ahead of and independent of every label. `profileHealthLocked`
recomputes a node's standing against the profile it *claims* and returns the
events its transitions require, which `saveNode` commits in the same
transaction as the node row: `node.profile.verified` the first time a claimed
non-dev profile is satisfied, `node.profile.unschedulable` on pass →
not-pass, `node.profile.restored` on the way back. `dev` emits none of the
three; it promises nothing, so there is nothing to lose.

The node emits `node.profile.unschedulable`/`node.profile.restored` for its own
local degraded state as well. Two different pieces of state change, so both
emit; `Event.origin` distinguishes them, and the node's copy is what survives a
transition that happened while its uplink was down.

Metrics: `remount_nodes_online`, `remount_nodes_profile_unschedulable` (gauges,
recomputed whenever membership or health changes) and
`remount_profile_drift_total` (counter, incremented on each pass → not-pass
transition).

An offline node satisfies nothing. Its evidence is its last hello and it cannot
be asked to re-prove anything, so `node.profile.get` reports it as
`unavailable` rather than repeating a stale `pass`.

### The node re-checks after the claim

`materializeWithReadyHook` calls `requireSchedulableProfile` next to the
existing `ValidateBackendSecurity` re-check. A node that drifted between the
control plane's placement decision and the materialization refuses with
`proto.ErrReason(proto.CodeDenied, proto.ReasonProfileUnschedulable, …)` rather
than serving the workspace with the boundary missing. `CodeDenied` is not in
the set `Client.nodeCall` retries (`CodeConflict`, `CodeUnreachable`), so the
refusal is not spun on.

### `remount doctor --profile` and its exit codes

`remount doctor --profile <name> [--node N] [--json]` calls `node.profile.get`
and prints per-node, per-check `pass`/`fail`/`unavailable`. The exit code is
three-valued:

| Exit | Meaning |
|---|---|
| 0 | every check on every node passed |
| 1 | at least one check failed |
| 2 | nothing failed but something could not be checked, including "no nodes are known" |

There is no path on which an `unavailable` result exits 0. That is the same
rule `node.diag_unavailable` already encodes: an unearned "healthy" is the most
dangerous output a diagnostic can produce.

### Relation to the existing contracts

Runtime profiles compose with the workspace `SecuritySpec` and the server
`--mode`; none of them replaces another.

- `SecuritySpec.Profile` is a **per-workspace** placement contract enforced by
  `ValidateBackendSecurity`. Nothing here relaxes it. Note that gVisor
  satisfies the `multi-tenant-isolated` *runtime* profile but not a workspace
  asking for `security.profile: multi_tenant`, because
  `ValidateBackendSecurity` additionally requires `MultiTenant`, which gVisor's
  `Caps` does not claim. The two names sound alike and mean different things:
  the runtime profile is about the machine's posture, the workspace profile is
  about one workload's contract.
- `--mode` is a **deployment** configuration gate on the control plane. It says
  nothing about any node.
- `Requires.Profile` is the **node-level** constraint that was missing.

`Workspace.PendingReason` surfaces `profile_unschedulable` on `ws.get` and
`ws.create`. It is derived in `snapshotWS` from the current fleet, never
persisted: it is a statement about the fleet rather than about the workspace,
it changes without the workspace changing, and deriving it is what keeps it
from being a state change with no event.

## Consequences

An operator can provision a machine, run one command, and get a per-check
answer about whether it is fit for untrusted work. A process or Docker node
cannot enter a multi-tenant pool by mislabelling itself, and a machine that
loses a prerequisite stops receiving work that depends on it within one probe
interval instead of silently serving it.

The cost is one more periodic loop per node and one more evaluation per
scheduling decision. Both are pure function calls over a handful of
descriptors.

## What is unavailable on this host

This change was written and tested on darwin/arm64. gVisor and Firecracker
cannot run here, so:

- `gvisor.Backend.Reprobe` and `firecracker.Backend.Reprobe`, and
  `JailerFactory.Reprobe`, are **compiled and cross-vetted for linux but not
  executed**. Their behavior against a real host is unproved.
- The exact-host drift lane — break nftables or unload KVM and watch the node
  become unschedulable — belongs on the Linux proof lanes and is not claimed
  here.

What is proved here is the model, the gate, the transport and the decision:
`internal/profile` against synthetic descriptors for all five backend states,
`internal/control` for the downgrade-only rule, `internal/sim` for scheduling,
scripted drift, recovery and `node.profile.get` end to end through a fake
backend that advertises a chosen descriptor and answers a scripted `Reprobe`,
and `cmd/remount` for the fail-closed startup gate.

## References

- Gap brief 2026-09-06, Gap 1 (`docs/engineering/gap-brief-2026-09-06.md`).
- ADR 0061 (deny-first enforced-gateway networking), ADR 0062 (vendor drivers
  never upgrade backend capabilities), ADR 0040 (security profiles require
  protocol capabilities).
- `spec/PROTOCOL.md` §3 (hello, `runtime_checks`), §5 (`requires.profile`,
  `pending_reason`), §6 (`node.profile.get`, `ws.renew`), §11 (events).
