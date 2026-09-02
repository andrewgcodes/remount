# 10. Trust boundaries

**Status:** accepted

## Context

An agent with root is the design, not an accident. So the question is not how to
constrain the agent, but what must remain true when the agent is fully hostile.

## Decision

Trust, in order:

| Component | Trusted with | Compromise means |
|---|---|---|
| control plane | policy, bindings, grant signing key | everything, as with every vendor; be honest about it |
| node supervisor + broker | real credentials, egress decisions, workspace lifecycle | that node's workspaces and its leased credentials until TTL |
| relay | routing metadata | metadata, and today frame contents (see ADR 8) |
| **workspace** | **nothing** | **nothing worth having** |

The workspace is on the far side of the boundary. It gets root, it gets a
filesystem, it gets a network path with no credentials in it, and it gets no
trust at all.

## Consequences

Every design question resolves against this table. Should the broker run inside
the workspace? No, it holds secrets. Should backends be plugins? No, they run in
the supervisor (ADR 5). Should the agent choose its own destinations? Only
within an allow list the agent cannot edit.

The filesystem layer is jailed at the supervisor, not by convention. Paths are
resolved with symlinks followed, and a path that escapes the workspace root is
denied even if a symlink inside the workspace points at it. Archive extraction
refuses entries containing a parent-directory component.

The broker refuses any destination that resolves to a loopback, private,
link-local or multicast address unless explicitly allowed, which closes cloud
metadata endpoints by default. It dials the validated IP literal rather than the
name so DNS cannot change under the check.

**Known gaps, stated plainly:** the `process` backend is `isolation: none` and
is only appropriate on a machine you own where the agent is already trusted with
your user account. There is no microVM backend yet. There is no in-workspace CA,
so `CONNECT` traffic is allow-listed but not inspected. A compromised control
plane is total, which is true of every system in this category and is not made
less true by declining to write it down.
