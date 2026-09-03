# ADR 0062: vendors provision whole Remount nodes

## Status

Accepted.

## Context

`workspace.Backend` is a local execution contract. Its handle exposes a
host-side jailed filesystem, rewrites local session specifications, freezes
managed execution for checkpoints, and may synchronously revoke a workspace's
network capability. A remote vendor sandbox cannot implement that contract
without a second node-like protocol and a second source of lifecycle truth.

Provider credentials also have a different authority boundary from workspace
credentials. They create machines for the control plane; they must never be
sent to a node or workspace. A newly created node needs only one short-lived,
single-use enrollment capability and then proves possession of its persistent
node key on every connection.

## Decision

Infrastructure vendors implement `internal/provision.Driver`. A driver creates
and destroys a whole machine that runs `remount up`; it is not a workspace
backend. The backend selected inside that node advertises the capabilities it
actually enforces. Vendor firewall or sandbox settings are defense in depth and
do not upgrade those backend capabilities.

A vendor-pool machine belongs to exactly one tenant for its lifetime. The
`provision.Request` carries that tenant and a short-lived enrollment token only
during `Create`. Drivers place the token in the provider's protected process
environment, never argv, labels, returned `Machine` state, reusable images,
events, or logs. Returned provider inventory is non-secret and copied before it
crosses a goroutine or reconciliation boundary.

## Consequences

- One node per vendor machine and one tenant per machine is the tenant boundary.
- The same provisioner works with process, Docker, gVisor, or future
  Firecracker backends; only the backend's descriptor determines placement.
- Pool reconciliation can be provider-neutral and test against a fake driver.
- Provider APIs and eventual consistency remain external failure modes. Create,
  destroy, and list must be idempotent by provider identity and expose partial
  or unavailable state rather than reporting success from request acceptance.
- The control plane must mint, consume once, expire, and audit node enrollment
  tokens before pool-created nodes can be wired into production.

## Rejected alternatives

- A remote `workspace.Backend` or `remount shim`: it duplicates the node
  protocol and splits filesystem/process authority.
- Long-lived node tokens baked into images: compromise scales to every future
  node made from that image.
- Treating a vendor network allow-list as `enforced_gateway`: it cannot prove
  the per-workspace broker and revocation contract.
