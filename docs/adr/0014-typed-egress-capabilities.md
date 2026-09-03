# 14. Typed egress capabilities and honest enforcement

**Status:** accepted

This ADR narrows the egress and compromise claims in ADRs 7 and 10; those
historical decisions remain immutable.

## Context

A hostname allow list is too broad for package registries and APIs with shared
mutable state. Proxy environment variables are also cooperative hints: a
hostile process can ignore them and open a direct socket. Finally, an opaque
CONNECT tunnel cannot enforce HTTP methods, paths, body sizes, credential
substitution, or read-only semantics.

The security contract therefore needs to distinguish what the broker can
mediate from what a backend can force. Treating those as the same thing would
recreate the fail-open claim this project is intended to prevent.

## Decision

Workspace security contains a first-match `NetworkPolicy` of typed
`EgressRule` capabilities. A rule identifies protocol, canonical ASCII hosts,
ports, methods, path-prefix segments, request count and body budgets, plus one
of four shared-state classes: `none`, `immutable_read`, `scoped_write`, or
`global_write`.

Rules imply default deny. Typed policy replaces the legacy node allow list
rather than widening into it. Control authorizes shared-state classes as
execute, read, write, and admin. Request-count budgets are atomic and scoped to
one workspace generation. Bounded request bodies are fully read before the
upstream dial; bounded streaming responses are cut off on the first excess
byte. Redirects remain on the capability-bearing broker URL and every hop is
reauthorized. Ambiguous authorities and path encodings are rejected.

Each broker has a random 256-bit capability unique to one materialization.
Audit decisions carry workspace, generation, rule, protocol, shared-state
class, outcome, and byte counts. Suspending or closing the broker also closes
established CONNECT tunnels. Per-workspace listener and request ceilings bound
idle sockets, active requests, streams, and tunnels.

CONNECT requires a rule whose protocol is `connect`, or the node allow list in
legacy local mode. A binding never grants CONNECT authority. Because the
tunnel is opaque, its rule cannot claim path, byte-limit, or shared-state
enforcement.

`isolated` and `multi_tenant` profiles require `enforced_gateway`. A backend
that advertises that capability must return a handle implementing
`NetworkController`. The node installs policy before `ws.ready` and revokes it
during self-fencing, fleet quarantine, failed materialization, and shutdown.
Failure to install prevents service. Failure to revoke prevents a positive
fleet fence acknowledgement and is emitted as an explicit autonomous-fence
error. The built-in `process` and `docker` backends remain explicitly
`cooperative_proxy` and are not eligible for these production profiles.

## Consequences

Package reads and API writes can now be separately authorized and audited.
Node-wide allow lists no longer silently broaden explicit workspace policy.
Redirect, encoded-path, request-overrun, response-overrun, cross-workspace
broker, and stale-tunnel paths all have direct regression tests, including a
hostile command executed inside a workspace.

Request buffering trades bounded memory for the guarantee that an oversized
write is rejected before any upstream side effect. Response limits preserve
streaming; if an upstream omits `Content-Length`, headers may already have been
sent when the stream is terminated, so the audit event is the authoritative
limit result.

This decision does not manufacture a production sandbox. A deployment cannot
claim enforced egress until its concrete backend passes in-workspace IPv4,
IPv6, UDP, DNS, metadata, raw-socket, sibling, and policy-loss bypass tests.
