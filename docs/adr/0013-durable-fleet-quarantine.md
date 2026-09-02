# 13. Fleet quarantine is durable and destruction needs two proofs

**Status:** accepted

## Context

An incident responder must be able to contain more than one workspace without
depending on a healthy client connection, a healthy node uplink, or an
in-memory loop surviving a control-plane restart. A selector evaluated again on
every retry is also unsafe: it can omit an original target after metadata
changes or unexpectedly include workspaces created after the incident began.

Destruction has a stricter failure boundary. A node may hold the only current
filesystem. A control request, response, or process can fail after a checkpoint
side effect but before either side records the result. Treating a destroy RPC
as sufficient authority can therefore delete the only recoverable copy, while
blindly replaying an ambiguous RPC can repeat an effect against the wrong
generation.

## Decision

`fleet.quarantine` creates a durable `FleetOperation`. Control evaluates the
selector once, records the target workspace, node, backend, and physical
generation, and stores the operation atomically with its caller-scoped
idempotency result. The operation and its per-target results survive restart.
An empty selector is rejected unless `all` is explicit. A deadline turns an
unfinished result into the visible `partial` state; pending unreachable targets
continue reconciliation afterward.

Every quarantine action uses the containment sequence: remove the workspace
from service, revoke broker and grant capability, stop sessions, advance the
authoritative generation, retain source bytes, and report the target result.
Checkpoint and destroy additionally require a verified artifact. Failed
checkpointing leaves the source quarantined and reports failure.

Destroy is a two-proof protocol:

1. The node write-ahead journals the exact phase-one request before fencing and
   durably records its response, including operation, workspace generation,
   action, backend, and snapshot.
2. Control durably commits the snapshot reference and advanced workspace fence.
3. Control sends `ws.quarantine.commit`.
4. The node deletes source bytes only if the commit exactly matches its durable
   phase-one proof and no replacement is serviceable locally.

A lost phase-two acknowledgement is safe to retry because phase two has its own
durable mutation key. An uncompleted node intent after restart is ambiguous and
is refused rather than replayed. A later terminal fleet operation may escalate
an earlier quarantine while retaining the earlier physical generation needed
to address the source.

## Consequences

Incident response has a stable operation id, immutable initial target set,
bounded first result, per-target acknowledgement, authoritative audit metadata,
and restart recovery. An offline target is reported as pending rather than
silently counted as contained by the node; the advanced control-plane
generation still invalidates new grants while the node's lease deadline
self-fences an isolated old holder.

Source deletion is deliberately slower than logical destruction. Control can
mark the workspace destroyed after committing the fence and checkpoint while
the physical delete acknowledgement remains pending. That retained copy is not
serviceable and background reconciliation continues.

The operation table and node mutation journal consume durable storage and need
future retention/compaction policy. A persistently failed checkpoint requires a
new operation after the underlying storage problem is corrected. The current
actions all choose strong containment; they are not promises that
`revoke_egress` leaves execution running or that `freeze` preserves process
memory.
