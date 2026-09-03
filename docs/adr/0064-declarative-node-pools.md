# ADR 0064: declarative node pools reconcile whole machines

## Status

Accepted.

## Context

The workspace claim queue places work only on nodes that already exist. Vendor
drivers from ADR 0062 can create whole nodes, but allowing claim handlers to
call providers directly would put slow, retrying, irreversible work under the
control-plane lifecycle path and make duplicate offers create duplicate
machines.

## Decision

A durable, tenant-scoped pool spec declares provider, minimum and maximum
capacity, labels, backend, region, size and idle scale-down. The control plane
reports unmet eligible demand to a separate `internal/pool.Reconciler`; normal
workspace offers and claims remain the only placement authority.

Provider actions serialize per `(tenant,pool)`. Before create, control durably
mints a ten-minute, single-use enrollment token. A failed create or destroy is
observable and enters exponential backoff. Scale-down considers only nodes with
zero assigned workspaces and an explicit idle timestamp; it never infers idle
from a missing observation. Deleting a pool removes reconciler bookkeeping only
after its in-flight action has joined.

The reconciler reserves a successful create until provider inventory exposes
its machine or a bounded visibility timeout expires, so concurrent callers
carrying the same stale inventory cannot over-scale. Control-plane integration
also persists desired capacity and the action result with its event; after a
controller restart, provider inventory is the source for reconstructing any
in-memory visibility reservation.

## Consequences

- Provider latency does not block control request or workspace lifecycle locks.
- A claim that finds no eligible node requests capacity; it does not acquire
  authority over a future node.
- Capacity, retries and partial provider outcomes are visible as pool events,
  metrics and diagnostics.
- Vendor pool nodes remain one-tenant machines even if the selected backend is
  only suitable for cooperative local execution.
