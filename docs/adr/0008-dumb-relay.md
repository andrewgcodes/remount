# 8. The relay is dumb and the control plane carries no data

**Status:** accepted

## Context

Clients and nodes both sit behind NAT and both must be reachable. Something has
to sit in the middle. The question is how much that something is allowed to
understand.

## Decision

The relay forwards frames by destination id and never interprets a body. The
control plane is a peer named `control` that happens to share its process. It
carries coordination: claims, leases, policy, timers, grants and the event log.
Session bytes go client to node through the relay and never enter the control
plane.

This is Tailscale's split, applied one layer up. Their coordination server
distributes public keys and policy and cannot read traffic, because the data
plane is peer to peer. Ours distributes claims and grants and does not carry
stdout.

## Consequences

The blast radius of the middle is bounded and stateable. A compromised relay
sees frame metadata and ciphertext-equivalent payloads it cannot act on; it does
not see the contents of a build.

Scaling the busy path does not mean scaling the smart part. The relay is a map
from peer id to connection.

Grants are what make this work without the node calling home per request. The
control plane signs a claim once, the node verifies it with a public key it got
at hello, and authorization needs no round trip.

**Not yet done:** end-to-end encryption between client and node through the
relay. The transport is TLS to the relay, so today the relay could read frames
if it wanted to. Noise IK inside the frame stream is the intended fix, and the
protocol has room for it because the relay already ignores bodies.
