# ADR 0050: Every resource commit and its events are one transaction

## Status

Accepted.

## Context

The control plane persisted a resource row and then appended the events that
described the change in two steps: commit the workspace, release the mutex,
call `emit`. A crash, a full disk or a failed append between the two left a
workspace with no `ws.created`, a destroy with no `ws.destroyed`, or a state
change whose `ws.state_changed` never happened. Nothing downstream could tell
the difference between "that event was never emitted" and "that event has not
arrived yet", so the audit log could not be trusted as a complete record.

Phases 1 through 3 add pools, approvals, principals, budgets, bases, queues and
volumes, each with several new state changes. Fixing the residual after they
exist means fixing it in dozens of places. Fixing it first means every one of
them inherits the guarantee.

## Decision

1. `eventlog.Log.Transact(ctx, db, fn)` runs `fn` inside one SQLite
   transaction. Resource rows go through the embedded `*sql.Tx`; events go
   through `Tx.Emit`, which writes them to an `event_outbox` table in the same
   transaction. Commit makes the rows and their events durable together;
   rollback discards both. After commit the outbox drains into the log in
   staging order. `Transact` holds the log's mutex for the whole call so a
   concurrent `Append` cannot fan out ahead of a staged event that will
   receive a lower sequence.
2. The store drains with `AppendTx` when it is the SQLite store sharing the
   database, so appending the event and deleting its outbox row is one
   transaction and delivery is exactly once. The memory store gets the same
   contract through the same `Transact`; its delivery is at-least-once within
   the process, which is the strongest thing a store that dies with the
   process can offer.
3. Every control-plane persistence helper (`persistWS`, `persistClaim`,
   `persistWSAndMutation`, the timer and fleet variants, `saveNode`) takes the
   events for the change as trailing arguments and commits them through
   `transact`. Events are built while `c.mu` is held, so attribution
   (`Tenant`, `Workspace`, `Generation`) is read from the exact copy that
   commits, not looked up again after the unlock. The post-unlock `emit`,
   `emitFleetEvent` and `emitWorkspaceTransition` helpers are gone; a new
   state change has no path that persists without staging its event.
4. A drain failure after commit is `*eventlog.DrainError`. The control plane
   logs it and reports success to the caller, because the resource and the
   event are both durable; `Tick` and the next `Transact` retry the drain, and
   `control.New` drains before serving so a process that died between commit
   and drain delivers on restart.
5. Lock order is `control.mu`, then `Log.mu`. Nothing acquires them the other
   way round, and `fn` never calls back into the log.

## Consequences

- A resource without its event and an event without its resource are both
  impossible by construction, and `TestOutboxCrashBetweenCommitAndAppend...`
  and `TestOutboxResourceWriteFailureEmitsNothing` inject the fault at the
  store layer to prove it across a tick and across a restart.
- `Transact` serialises event delivery behind the resource transaction, so a
  burst of lifecycle changes pays one extra outbox insert and delete per
  event. SQLite is single-writer already, so this is not a new bottleneck.
- Node-originated events still arrive through `Append`; they describe no
  control-plane resource and are ordered against control events by the log's
  single mutex as before.
- Later resources (pools, approvals, budgets, agents) add rows inside the
  same `transact` closure and events in the same trailing list. They do not
  get to choose a weaker guarantee.
