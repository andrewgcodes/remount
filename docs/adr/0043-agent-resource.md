# ADR 0043: An Agent is a durable control-plane resource, not a process

## Status

Accepted.

## Context

`remount run` (ADR 0039) and queues (ADR 0041) run a harness command in a
workspace and record the session. That is a job, not an agent: when the
process exits the conversation is gone, a second prompt is a second cold
start, and a laptop that closes mid-run loses the only place the intent
lived. A DIY-Devin needs the opposite: something addressable by id that you
can message tomorrow, that survives its node dying, that can be forked, put
to sleep and destroyed, and that a UI can show as one status word.

Three shapes were considered.

- **A long-lived harness process per agent, supervised by the node.** The
  node already supervises sessions, so this is the least code. But the node
  is the disposable part of the system: a lost node or a moved workspace
  would take the inbox, the status and the ACP session id with it, and two
  nodes racing to re-adopt would run one conversation twice.
- **A queue with an ACP session id attached.** Queues are FIFO task lists that
  advance on exit codes; an agent conversation has approvals, cancels, forks
  and a harness that can stay up between turns. Bending the queue would have
  broken its one-unfinished-per-workspace guarantee.
- **A control-plane resource whose only truth is durable.** The control
  plane holds the inbox, run history, ACP session id and policy in SQLite
  and reconciles that intent to node operations on every tick. The node runs
  a harness and reports what it observed.

## Decision

1. **`Agent` is a row.** `{id, tenant, owner, ws, owns_ws, spec, mode,
   acp_session_id, inbox[], runs[], policy, status, ...}` in the `agents`
   table, committed in the same transaction as its events and the mutation
   record (ADR 0050). In-memory state is a cache of that row, rebuilt at
   start.
2. **Status is derived, not commanded.** `deriveAgentStatus(agent,
   workspace)` computes the status word from the workspace state, the live
   run and the inbox: `creating` until a node first holds the tree,
   `running` while a prompt is queued or in flight, `waiting_approval`,
   `waiting_input` (harness up, turn done), `idle` (no harness, nothing
   queued), `sleeping`, and the terminals `failed`, `finished`, `destroyed`.
   Every change is validated by the central transition table and emits
   `agent.waiting`, `agent.slept`, `agent.failed` and friends; a terminal
   agent cannot be resurrected.
3. **The node is an observer.** `agent.run` starts a harness for a run;
   `agent.report{seq, kind}` tells the control plane what happened. Reports
   are fenced to the node holding the run and the workspace generation and
   deduplicated by sequence, so a redelivered or stale report changes
   nothing. A run whose node is gone past its lease, or whose workspace
   moved, is closed by reconciliation as `node gone` and retried; the second
   consecutive failure fails the agent.
4. **A prompt is durable until its turn ends.** An inbox message is deleted
   only on `turn_finished` for it, never on `turn_started`, so a harness that
   dies mid-turn is re-prompted with the same text on the retry. `agent.cancel`
   drops the inbox and cancels the turn but keeps the run, so the next
   message continues the same session.
5. **Fork is snapshot plus session id.** `agent.fork` asks the holding node
   for an uploaded snapshot on the control plane's own authority, fenced to
   the generation it believes the node holds, and creates a child whose
   workspace restores it and whose `acp_session_id` is the parent's. A child's
   policy may only narrow.
6. **The agent owns its workspace only if it made it.** `owns_ws` is
   recorded at create; `agent.destroy` on an adopted workspace leaves the
   workspace alone.
7. **Events carry hashes, not prompts.** `agent.created` and `agent.message`
   carry `task_hash`/`text_hash`; the prompt body lives in the inbox row and
   the transcript, never in the audit log.

## Consequences

- One agent per live workspace and at most 1024 live agents per tenant; the
  inbox holds 64 messages and refuses with `resource_exhausted`.
- `steer` degrades to a queued follow-up (`degraded: true`) because ACP has
  no mid-turn input. When a harness advertises otherwise this can change
  without a wire change.
- Reconciliation, not request handlers, launches runs. A request returns as
  soon as the row is committed; `context.WithoutCancel` carries the node call.
- The control plane can be restarted at any time: `TestAgentsAndApprovalsSurviveRestart`
  proves the status, session id and pending approvals come back.
