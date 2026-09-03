# ADR 0044: Approvals are control-plane rows that never outlive their run

## Status

Accepted.

## Context

An ACP harness asks its client for permission before a risky tool call
(`session/request_permission`) and can ask a question mid-turn
(elicitation). The broker (ADR 0014) can likewise hold an egress it is not
sure about. In a laptop client the human is at the keyboard; in Remount the
harness runs on a node, the human is behind an HTTP API or a CLI on another
machine, and the workspace may move or sleep between the question and the
answer. The question therefore has to be a durable thing with an id, an
owner, a policy and an audit trail.

The alternatives were an in-memory map on the node (lost with the node, and
invisible to a second control-plane client) and blocking the `agent.report`
frame until a human answered (a network hold on a human decision).

## Decision

1. **`Approval` is a row.** `{id, agent, ws, run, kind, title, tool_call,
   options[], detail, status, decision, delivered_at}` in the `approvals`
   table, committed with its events and, when an agent row changes in the
   same step, in the same transaction as that agent.
2. **Three kinds, one shape.** `tool_call` and `elicitation` are parked by the
   node from ACP; `egress` is parked by the broker. `detail` is the harness's
   raw request (64 KiB bound) so a UI can render what Remount does not model.
3. **The node parks, the control plane decides, the node answers.** The
   node keeps the ACP request open and reports `permission`/`elicitation`
   with the approval. `approval.decide` validates the option against what the
   harness offered (an empty option picks the first `allow_*`), commits the
   decision and its `approval.decided` event, and only then sends
   `agent.approval.decided` to the run's node. Delivery is retried every ten
   seconds until the node acknowledges (`delivered_at`) or the run ends.
4. **An approval never outlives its run.** ACP has no way to re-ask a
   pending permission, so when the run that parked it ends (exit, cancel,
   sleep, node loss) every pending approval of that run expires and the
   harness asks again on its next turn. An undelivered decision for an ended
   run is marked `delivered_at: -1` and stops retrying.
5. **Policy decides who is asked.** `policy.approve` is `never` (deny
   everything without asking), `on-request` (default: park and ask) or `auto`
   (allow without asking). `auto` is refused for a local process workspace
   because nothing but the human stands between the harness and the host.
6. **Bounded.** At most 64 pending approvals per agent; the 65th is refused
   with `resource_exhausted` and counted.
7. **Events carry the decision, not the question.** `approval.pending` and
   `approval.decided` carry ids, kind, title, option and who decided; the
   request `detail` and an elicitation's `content` never enter the event log.

## Consequences

- `waiting_approval` is a first-class agent status derived from pending
  approvals; a decision returns the agent to `running` and a policy sleep
  does not fire while one is pending.
- Authorization on an approval falls back to the agent's: whoever may execute
  on the agent may decide for it.
- `remount approve ID [--option X | --deny | --content JSON]` and the HTTP
  API are thin wrappers over `approval.decide`.
