# ADR 0084: pool scale-down fences the node before the provider destroy

## Status

Accepted.

## Context

ADR 0064 lets scale-down destroy a provider machine only when its
`remount.node` label resolves to an online, assignment-free control node with
an explicit idle timestamp. That check ran once, under `control.mu`, while the
inventory snapshot was enriched. The snapshot was then handed to the pool
reconciler, the mutex was released so the provider call would not hold it, and
the reconciler chose its victim from the copy. A workspace could claim the
node and reach `ws.ready` in that window, and the provider destroy still ran
(review finding RMR-001). The idle observation was being used as destroy
authority, and an observation that predates the decision cannot be one.

Holding `control.mu` across the destroy would close the window and violate the
older rule that provider I/O never runs under the control mutex or lease loop.

## Decision

The enriched inventory carries, for every idle node, a `nodepool.Retirement`
owned by the control plane. Immediately before the destroy, the reconciler
calls `Retire`. Under `control.mu` and touching no provider, control re-checks
that the pool is the same pool, that the node still has this pool's exact
tenant and pool labels and is online, and that no held workspace is assigned
to it. Only then does it commit a `pool_retirements` row keyed by node, with
the tenant, pool and exact provider machine id, together with a
`pool.retiring` event in one transaction, and mirror it in memory. A node
with a fence is ineligible for every new claim: `eligibleBackendLocked`
refuses it before `WSClaiming`, so the decision is made where placement
authority already lives. A `Retire` that finds a held workspace, a changed
identity, or a replaced pool returns false and the reconciler skips the node;
a node without a `Retirement` is never destroyed.

The fence outlives the destroy call. A definite provider error (`Release`)
deletes the row with a `pool.retire_aborted` event, because the machine is
still there and should take work. An ambiguous result — the context expired
or was cancelled, or the error wraps a deadline or cancellation — keeps the
fence, because the machine may be half gone; the next reconcile re-fences the
same node idempotently and retries. A successful destroy keeps the fence too:
the node may still be connected, and only the next inventory that no longer
lists the machine releases the fence, with a `pool.retired` event in the same
transaction as the row deletion. On restart the rows are loaded before the
first reconcile, so a destroy that was in flight when the controller died
cannot be raced by a claim. A pool with an unresolved fence refuses removal
with `conflict`: its reconciler is the only authority that can observe the
machine leave inventory, and a zero machine count is not that observation.

## Consequences

- Authority: `control.mu` plus the durable `pool_retirements` row. Resource:
  one `(tenant, pool, node, provider machine)`. Irreversible action: the
  provider destroy. Durable commit: the fence row and `pool.retiring` before
  the destroy. Observable postcondition: no `pool.scaled` with reason `idle`
  for a node that has a held workspace, and a claim on a fenced node returns
  `denied`. Every fence ends in exactly one of `pool.retire_aborted` or
  `pool.retired`.
- A `Retire` takes `control.mu` briefly between two provider calls; that is
  the same lock discipline every workspace claim already follows, and the
  provider call itself still runs unlocked. The existing authority test that
  blocks inside the provider and creates workspaces meanwhile still passes.
- A fence that cannot be pruned (the delete fails) stays in force. Refusing
  claims on one node is the safe failure; the error is logged and the next
  reconcile retries.
- The pool reconciler's `Node` gained a field and `scaleDown` may now return
  no action when every candidate refuses to be fenced. That is the correct
  answer, not a failure, and does not enter backoff.
