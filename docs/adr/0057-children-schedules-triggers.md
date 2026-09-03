# ADR 0057: children, schedules and triggers reuse the Agent inbox

## Status

Accepted.

## Context

Three things people expect from a durable agent turned out to be the same
mechanism. A coordinating agent wants to hand work to a child and hear back
when it is done. An operator wants an agent that starts at nine, not now. A
repository wants an issue label or a comment to start or continue an agent
without a human at a terminal.

Each has a tempting bespoke design: a child-completion callback into the
harness, a scheduler process with its own table, a webhook router with its own
rules language. Each of those would be a second place where lifecycle state
lives, a second thing that has to survive a control-plane restart, and a
second surface that can leak a credential or a prompt into an event.

## Decision

All three are expressed as inbox messages and policy on the existing Agent
resource; the reconciler already knows how to deliver an inbox, wake a
sleeping workspace and retry a lost run.

**Children.** `agent.create{parent}` creates a child that inherits what it
does not name — providers, primary, sandbox, workspace bindings, security —
and is refused (`denied`) anything the parent lacks or any wider policy.
Trees are at most three deep. When a child becomes terminal, the reconciler
appends one `kind: child` message to the parent's inbox carrying a
`ChildSummary` JSON document, emits `agent.child.finished` on the parent's
stream, and sets `child.parent_notified`; the two rows and the event commit in
one transaction, so a restart neither loses nor repeats the notification. The
parent handles the summary as an ordinary follow-up turn. A parent that is
gone or terminal is marked notified without a message; a full parent inbox is
retried on the next reconcile rather than dropped.

**Schedules.** `policy.start_at` is an instant. The agent is created and its
workspace claimed at once (so the first turn is fast), the derived status is
`scheduled`, and the reconciler neither launches a run nor delivers the inbox
before the instant. Messages queue. Nothing about the schedule lives in a
timer process; the reconciler's next pass after the instant launches it,
which is the same path a wake or a retry takes. Starts more than 366 days
out are refused. The CLI accepts `--at HH:MM`, `--at 90m` or RFC 3339, and
`remount run --sleep-until HH:MM` on an ACP recipe sets the same policy. A
recurring schedule (`--cron`) is deliberately not a policy field: each firing
is a new Agent, so a recurrence is a separate resource that creates Agents,
and until one exists an external scheduler posting a signed webhook (below)
does the job with no new state in the control plane.

**Triggers.** `POST /v1/events` keeps its shape and gains an optional `agent`
mapping: `{wake, message}` sends a follow-up to an existing agent, `{create}`
creates one; `wake` may be `name:<template>` and resolves to exactly one live
agent of that name. Templates (`message`, `wake`, `task`, `name`,
`workspace.name`, `idempotency_key`) are Go
`text/template` over the decoded payload with `missingkey=error`, so a
webhook whose shape changed is refused rather than rendered into a half-empty
prompt. A request authenticates with a bearer the control plane knows or with
an HMAC-SHA256 of the raw body under `--webhook-secret`; GitHub's
`X-Hub-Signature-256` header is accepted directly. A signed request acts as
`--webhook-token`, an ordinary credential the operator provisions for the
webhook principal, and without one it may only append events. The handler is
a translation layer like the rest of the HTTP API (ADR 0056): it calls
`CreateAgent`/`MessageAgent` through the SDK and authorizes nothing itself.

## Consequences

- No new tables, timers or processes. Children, schedules and triggers are
  visible through `agent get`, `agent ls --parent`, `agent watch` and the
  event stream like everything else.
- A child cannot escalate: the credential set it may use is a subset of its
  parent's at creation and is checked again when the workspace binds.
- Idempotency is inherited: a redelivered webhook with the same rendered
  `idempotency_key` returns the same agent or message.
- The trade-off is delivery granularity. A schedule fires on the reconciler's
  next pass (seconds), not to the millisecond, and a child summary is a prompt
  the parent's harness must interpret rather than a structured tool result.
  Both are acceptable for the agents this serves.
