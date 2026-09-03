# 17. Live snapshots and authoritative checkpoints are different operations

**Status:** accepted

## Context

A filesystem archive taken while processes write can be useful, but it is not a
safe implicit failover point. Earlier explicit snapshots were live yet appeared
eligible to become authoritative, while release paths could destroy the only
good source after upload or persistence failure.

## Decision

Every snapshot result declares `consistency` and `authoritative`. Ordinary
`ws.snapshot` uses the backend's live capture, may be uploaded, reports
`consistency=live`, and never changes control's `last_snapshot`.

An authoritative checkpoint is explicitly requested. The node rejects it
without an upload target, blocks new managed work, drains Remount-managed
sessions for the process backend or pauses the Docker container, takes the tree
lock exclusively, calls the backend checkpoint contract, uploads the artifact,
and commits the digest at control for the exact node and generation. Only that
successful control commit makes the result authoritative.

Move, sleep, release and destructive containment use a prepare/commit/abort
sequence. Snapshot or upload failure restores service without deleting source
bytes. An ambiguous or failed abort becomes visible `failed` state; fleet
destroy additionally requires the node's durable phase-one proof before
physical deletion.

## Consequences

Callers can choose availability-oriented live export or recovery-oriented
quiescence without one masquerading as the other. Backends own the execution
freeze semantics and are tested through a split checkpointer contract.

The process backend can fence only Remount-managed sessions. A hostile host
process that escaped this unisolated backend is outside the checkpoint contract;
production isolation still requires a stronger external backend. Archives hold
filesystem state, not process memory.
