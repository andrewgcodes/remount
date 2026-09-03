# ADR 0066: Egress approvals are durable request fences

## Status

Accepted.

## Context

Holding a broker connection open until a person answers is not a durable
approval system. The node, connection, or control plane can restart; an
unbounded set of parked bodies can exhaust memory; and approving only a host
can accidentally authorize a different mutation. The protected boundary is
the first byte released to an upstream, including an opaque CONNECT tunnel.

HTTP 403 is the interoperable response for an understood request the broker
refuses to fulfill. `Retry-After` is a non-negative delay or HTTP date under
[RFC 9110 §10.2.3](https://www.rfc-editor.org/rfc/rfc9110.html#section-10.2.3),
so the broker uses a one-second retry hint alongside its approval identifier.

## Decision

`EgressRule.Mode` is `allow`, `deny`, or `approve`; omitted mode normalizes to
`allow` for wire compatibility. An approve-mode request is identified by a
SHA-256 fingerprint over workspace, generation, principal, rule, normalized
authority, method, path/query hash, and body hash. Request bodies are buffered
under a 16 MiB hard ceiling (or the rule's smaller request limit) before the
fingerprint is sent. They are not released while the decision is pending.

The current workspace-generation holder calls `egress.approval`. Control
validates the assignment and that the named rule still requires approval,
then commits an `Approval{kind: egress}` row and `egress.pending` in one SQLite
transaction. Creation is idempotent on the fingerprint. The broker waits at
most 30 seconds and then returns 403 with `X-Remount-Approval` and
`Retry-After`; the approval remains open for ten minutes. A restart therefore
loses only the in-flight HTTP exchange, not the question or decision.

`approval.decide` stores allow or deny before the broker can observe it.
`remember: none` authorizes the exact fingerprint, `host` authorizes later
fingerprints for that principal and host, and `rule` authorizes later
fingerprints for that principal and rule until the decision TTL. Host memory
also appends an explicit allow rule to the workspace policy and commits
`policy.updated` with the approval and decision event. Expiry commits
`egress.denied{reason: approval_timeout}`. Pending requests are bounded both
at each broker and per tenant in control.

The control row is the authority; the exact workspace generation and request
fingerprint are the fence; outbound release is irreversible; the decision and
event transaction is the commit point. A broker or approval authority error
fails closed. Rule request limits are consumed only after approval, so a
pending attempt cannot spend capacity and an approved retry cannot bypass it.

## Consequences

Agents must retry after a pending response; `remount run --approve on-request`
documents this harness behavior. Approvals survive node and control restarts,
and modified requests require a new decision unless an explicit remember scope
covers them. Fingerprinting non-empty bodies costs bounded memory and refuses
larger approve-mode requests rather than producing an ambiguous authorization.

CONNECT is approved before dialing, but its encrypted application request is
not inspectable, so its fingerprint covers the authority and method only. An
operator who needs path- or body-level review must use the broker's reverse
proxy route instead of CONNECT.
