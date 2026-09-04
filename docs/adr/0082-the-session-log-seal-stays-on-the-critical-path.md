# ADR 0082: The session-log seal stays on the caller's critical path

## Status

Accepted.

## Context

A node advertising `tiered-session-logs` seals a completed session's output into
an immutable artifact and registers it with the control plane, so that spec
§8.2's promise holds: "an exited session can still be replayed byte-exactly
after a move or a node restart."

Today that seal is synchronous. `Log.closeWithPublish` flushes the ring to the
spill, uploads the segment, and commits the record before the session's exit
becomes observable. So an exec pays for the archive of its own output before it
returns.

The 2026-09 performance sweep measured what that costs. Holding everything equal
except whether the node advertises a durable tier
(`internal/sim.TestExecRoundTripCostOfTheDurableSessionTier`):

| Configuration | trivial exec, p50 |
|---|---:|
| artifact tier off | 11-12 ms |
| artifact tier on, as of 2026-09-03 | 28-30 ms |
| artifact tier on, after ADR-adjacent fixes below | ~20 ms |

Three costs were removed without touching this boundary at all: a redundant
fsync of the spill before upload, the two fsyncs the node paid for a local copy
that is only a cache, and a store-wide lock held across filesystem calls in
`artifact.publish`. Together they took the durable tier's overhead on an exec
from about +17 ms to about +8 ms, and the 200-way exec round trip from 2.78 s to
1.41 s.

That reframes the question this ADR exists to answer. The proposal on the table
was to take the seal off the critical path — report the exit, seal in the
background — which would buy the remaining ~8 ms.

## Decision

**The seal stays synchronous.** An exited session is durably replayable at the
moment its exit is observable, and that ordering is a property callers may rely
on.

## Consequences

Deferring the seal would open a window in which a session has exited, a client
has been told so, and the output exists only in the node's memory and spill —
both of which §8.2 states plainly "are node-local, so they do not survive node
loss." A node lost inside that window would produce a session that the control
plane says completed and whose output no longer exists. Worse, the failure is
silent in the direction that matters: the client already has the exit status and
has no reason to ask again.

Every mitigation reintroduces the cost or the complexity somewhere else. Making
`s.wait` block until sealed moves the latency rather than removing it. Reporting
the exit and marking the record incomplete means every reader must handle a
fourth state, and "exited but not yet durable" is exactly the ambiguity the
evidence model exists to avoid. A background sealer needs its own retry, its own
bound, its own metric, and a policy for what happens when it fails after the
client is gone.

The price is now about 8 ms on an exec, against 11-12 ms for the exec itself,
and what remains is genuinely irreducible without weakening the guarantee: one
durable write in the control plane's artifact store, the HTTP upload that
carries the segment to it, and one control round trip to commit the record. A
guarantee that survives node loss cannot cost less than writing the bytes
somewhere that survives node loss.

Callers who do not want to pay it have a supported way not to: a node that does
not advertise `tiered-session-logs` keeps the ring and spill and skips the seal
entirely, at 11-12 ms, and is honest about what it can replay. That is a
deployment choice with a visible capability attached, which is the right shape
for this trade — not an invisible durability window inside a node that claims
the capability.

## Revisiting

This decision is about a measured 8 ms, so it should be revisited if that number
moves. Two things would move it: an artifact store whose durable write is much
slower than a local disk (an S3 endpoint across a WAN rather than MinIO beside
the control plane), or a workload of very short sessions where the seal
dominates by ratio rather than by absolute time.
`TestExecRoundTripCostOfTheDurableSessionTier` reports that ratio on every run,
so the evidence for revisiting will surface on its own rather than needing
someone to go looking.
