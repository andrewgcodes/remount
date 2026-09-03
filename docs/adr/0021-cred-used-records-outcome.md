# ADR 0021: `cred.used` records the upstream outcome

## Status

Accepted.

## Context

The broker emitted the `substituted` audit (rendered as `cred.used`) the
instant a placeholder was rewritten, before any byte left the node. The
recorded `status` was therefore always `0`, and a reader of the log could not
tell a request the upstream accepted from one that never connected. The
argument for emitting early was forensic: an upstream reset or a cancelled
workspace request must not erase the fact that a secret left the node.

Events are also attributed to a workspace and generation but not to the session
that ran the command. Finding "everything this one `exec` did" meant parsing
the `s` payload field of `s.opened` and `s.exited`.

## Decision

1. `cred.used` is emitted once per released binding when the outcome is known.
   `status` is the upstream response status when headers arrived. When they did
   not, `status` is `0` and `error` names a failure class (`dns`,
   `connection_refused`, `timeout`, `tls`, `redirect_rejected`, …), never the
   transport's error text. The forensic property survives: every path that
   releases a credential terminates in exactly one of the recorder's call
   sites (`ModifyResponse`, `ErrorHandler`, the connector result, or a deferred
   fallback), guarded by `sync.Once`.
2. `proto.Event` gains `session`. The node sets it on every event attributed to
   one session. The control plane clears a client-supplied value; only the node
   that runs a session may attribute to it. `remount events --session SID`
   filters on it client-side and `--json` renders it.

## Consequences

- Broker tests that asserted `status == 0` on a successful request now assert
  the upstream's real status. The old assertion encoded the gap this ADR
  closes, not a contract anyone relied on.
- Error text is deliberately lossy. Operators who need the raw transport error
  read the node's log; the audit stream is exported and must not carry
  destination or secret-shaped text.
- The SQLite event store gains an additive `session` column; older databases
  are migrated on open.
