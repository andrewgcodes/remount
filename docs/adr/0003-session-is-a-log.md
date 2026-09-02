# 3. A session is a log, not a socket

**Status:** accepted

## Context

An agent runs a twenty minute build. The network drops. What happens to the
output?

The industry default is: it is gone, and you repaint. tmux and dtach re-send a
screen. mosh synchronizes screen state and deliberately discards intermediate
frames, which is exactly right for a human terminal and exactly wrong here,
because for an agent the scrolled-past output **is the product**. You cannot
hand a model a build log with a hole in it and call it a transcript.

## Decision

A session's output is an append-only log of chunks with a per-session
monotonically increasing sequence number. A client is a cursor over that log.

Attaching from sequence N and tailing live are the same code path. The node
holds a bounded in-memory ring plus a spill file, and evicts on both a byte cap
and a chunk count cap, because a pty emitting one byte at a time exhausts a
chunk budget long before a byte budget.

When a client asks for a sequence that is genuinely gone, the node sends an
explicit `gap` chunk naming the lost range and then continues. It does not go
silent, and it does not kill the session.

## Consequences

Reconnect is not a feature, it is the absence of one. There is no reconnect code
path: there is a cursor, and reading from it after a new connection is the same
operation as reading from it before.

The node keeps no per-client state, so a client that vanishes costs nothing to
forget. A second viewer is another cursor.

Honesty about gaps is a deliberate cost. Eternal Terminal kills the session when
a client falls too far behind; Codex's exec-server silently truncates. Both
choices are worse for an agent: one loses the work, the other corrupts the
transcript without saying so.

Input needs the mirror property. Every input carries a client-side sequence, and
the node drops duplicates, because a keystroke retried after a dropped
connection must not be typed twice.
