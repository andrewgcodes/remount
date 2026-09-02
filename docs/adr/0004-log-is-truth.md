# 4. The event log is the truth; state is a cache

**Status:** accepted

## Context

There are several things that want to know what happened: the workspace state
machine, the audit trail, a future UI, and a human debugging a failure at 2am.
Each could maintain its own view, updated by whoever remembers to update it.

## Decision

Every consequential action appends to one ordered event log. Workspace state,
the audit trail and any UI are consumers of that log, not peers of it.

Events carry `stream`, which is a workspace or node id, so a workspace's entire
history is one filter. They carry `principal`, so every action is attributable.
They carry `cause`, so an event can point at the event that produced it.

## Consequences

"Why is this workspace here?" is answerable by reading, not by inference. The
sequence `ws.created`, `ws.claiming`, `ws.claimed`, `ws.snapshot`,
`ws.released`, `ws.moved`, `ws.restored`, `ws.claimed` is the full story of a
migration, in order, with timestamps.

Audit is not a separate subsystem. `cred.used` and `egress.denied` are events in
the same log as everything else, which means the answer to "what did this agent
touch" is one query rather than a correlation exercise across three systems.

The cost is discipline: a state change that does not emit an event is a bug, and
we have no compiler check for it. The mitigation is that the tests assert on
events rather than on internal state, so an unemitted event fails a test.
