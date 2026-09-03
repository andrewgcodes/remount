# 16. Transactional resource rows own lifecycle authority

**Status:** accepted

This ADR corrects the overly broad “event log is truth” wording in ADR 4. The
event log remains canonical for ordered audit and observation; it is not replayed
to reconstruct workspace authority.

## Context

Workspace ownership changes combine state, node, generation, lease, snapshot,
timer, assignment and idempotency data. Earlier code assigned lifecycle strings
at many call sites, sometimes after unlocking or before persistence, and startup
recovery discarded recorded holders. An event append cannot atomically rebuild
those relationships with the existing schema, and treating an incomplete audit
append as permission to mutate authority is unsafe.

## Decision

SQLite resource rows are the transactional recovery source for authority. Every
workspace lifecycle mutation passes through one operation-specific transition
table in `internal/control/state_machine.go`. A transition names its actor and,
where applicable, requires the exact predecessor generation and node. Any pair
not listed is rejected. Generation values never wrap past SQLite's signed
integer range; exhaustion moves an expiring holder to durable `failed` state.

Successful lifecycle changes emit either their semantic event or
`ws.state_changed`. Events carry authoritative tenant, workspace, node and
generation metadata derived by control or checked against durable assignment
history. Producer sequence high-water marks remain after event bodies are
pruned.

Startup preserves recorded holders through a recovery grace period so the same
node may re-adopt at the same generation. Interrupted quiesce/checkpoint/destroy
states become `failed` for reconciliation. Nodes independently stop serving at
their local lease deadline, and affirmative renew responses fence stale
generations.

## Consequences

Recovery no longer depends on reinterpreting an event stream, and an audit
consumer can still explain every successful transition in order. Exhaustive
state-pair tests, randomized reordered command tests, lifecycle fuzzing and
full-system fault simulation all exercise the same transition function.

The supported authority model still has one control writer. State-row commit
and audit append are separately durable operations, so a crash can omit the
audit record for an already committed resource transition; diagnostics and a
future transactional outbox can close that observability gap. Independent
SQLite controllers behind one endpoint remain unsupported.
