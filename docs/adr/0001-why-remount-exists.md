# 1. Why Remount exists

**Status:** accepted

## Context

The interface between an agent and tools was standardized by MCP. The interface
between an agent and its instructions was standardized by AGENTS.md. The
interface between an agent and **the computer it acts through** was not
standardized by anything, so every harness reimplements "run bash somewhere".

The reimplementations are not equivalent, and each one is missing something
different. Some run only locally. Some tie the agent's process to the socket of
whoever started it, so a dropped laptop lid kills a build. Most put the model
API key inside the same box the agent has root in, which means an agent that can
read its own environment can exfiltrate the key it runs on.

Two labs drew the same architectural line in 2026 and drew it privately.
Cognition's Devin Outposts keeps the agent loop in their cloud and ships tool
calls to a machine you own over an outbound connection. Anthropic's Managed
Agents does the same thing and names it "decoupling the brain from the hands".
Each vendor owns their version of the boundary. Nobody owns the neutral one.

## Decision

Build the neutral layer: a protocol for the agent-to-computer boundary, and one
binary that implements it. Three properties, and the combination is the point
because each one alone is a feature someone already ships:

1. **The computer moves.** A workspace is a value, not a machine. It can be
   snapshotted, paused, and resumed somewhere else.
2. **The workspace is secret-blind.** It holds references; the real credential
   is substituted at the network edge, per destination, with a TTL and an audit
   record.
3. **Nothing is lost.** Sessions are logs, not sockets. A client is a cursor.

## Consequences

We are not building an agent framework, a prompt format, a memory system, or a
chat UI. A harness that runs on a computer runs on Remount unchanged; that is
the test of whether the boundary is drawn in the right place.

We accept that the individual pillars have strong incumbents. Tailscale owns
identity and connectivity. Fly, Modal, E2B and Daytona own the sandbox. The
argument for a separate thing is the seam: none of them spans a laptop, an
on-prem box, and burstable cloud compute under one identity with crash
migration, for arbitrary agents rather than one vendor's.
