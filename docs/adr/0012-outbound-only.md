# 12. Nothing listens

**Status:** accepted

## Context

Remount must work on a laptop behind a home router, a container in someone's
VPC, and a Mac mini under a desk. None of them can accept an inbound connection
without a person configuring something.

## Decision

Nodes and clients both dial out over TLS to a single endpoint. Nothing in
Remount opens an inbound port on a participant's machine. The only listening
socket is the server, and the only listening socket on a node is the broker on
loopback, reachable exclusively by that node's own workspaces.

The reference transport is one WebSocket message per frame, because it traverses
corporate proxies and terminates at any reverse proxy an operator already runs.

## Consequences

Enrollment is a single command with no firewall step, which is the difference
between a tool people try and a tool people mean to try.

This is the same topology Devin Outposts uses, and Anthropic's MCP tunnels, and
Temporal's workers, arrived at independently by everyone who has had to run code
inside someone else's network.

The transport is deliberately replaceable. Frames need an ordered reliable byte
stream and nothing else, so the in-memory pipe used by the simulation tests and
a real WebSocket are the same interface. QUIC or a peer-to-peer path can be
swapped in without the session layer noticing.

The cost is a relay in the path for every byte of session output. Direct
peer-to-peer connections with the relay as fallback would be faster and is the
obvious next step; the protocol does not preclude it because peers are addressed
by id rather than by route.
