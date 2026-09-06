# 0092. Broker substitution locations, typed refusals, and redaction

Status: accepted (2026-09-06)

## Context

Credential substitution was header-only. A binding's placeholder was replaced
wherever it appeared in a header value, and nowhere else. That covers bearer
tokens and Basic credentials, which is most of what an agent talks to, and it
covers nothing else: an OAuth token endpoint wants `client_secret` in a form
body, several search and geocoding APIs want `api_key` in the query string,
and a growing number of provider SDKs put the key in a JSON field. An adopter
whose provider works that way had two options, both wrong: put the real key in
the workspace, or write their own proxy.

Two smaller gaps travelled with it. A broker refusal was `http.Error` prose,
so a harness had to string-match to tell "you have no budget" from "this host
is not allowed" from "an approval is pending" — and the repository's own rule
is that callers match on codes, never message text. And the redactor that
scrubs credential-shaped strings out of agent transcripts lived inside
`internal/node`, unavailable to the broker, whose refusal bodies and audit
reasons can quote an upstream's own error text.

## Decision

### Substitution locations

`proto.BindingLease` gains an optional `Substitution{location, name,
json_pointer}`. `nil` means `header`, so every existing binding behaves
exactly as before. The four locations are `header`, `query`, `body_form` and
`body_json`; the control plane refuses a malformed declaration when the
binding is registered rather than one request at a time.

The scan and the substitution are deliberately different operations:

- **Scanning is location-independent.** Every placeholder is looked for in
  every part of the request the surface can read. A placeholder aimed at a
  host its binding does not cover is an exfiltration attempt whether it sits
  in a header, a query parameter or a JSON body, and all three are blocked and
  recorded as `leak_blocked`. Before this change a placeholder in a request
  body reached the wire unexamined; the sim proof for this is
  `TestBodyPlaceholderToForeignHostIsBlocked`, which returned `200 ok` against
  the previous behavior.
- **Substitution happens only at the declared location.** A binding that
  declares a query parameter and finds its placeholder in a header is refused,
  not helpfully substituted in both.

### Body substitution is bounded

Reading a request body means buffering it. The bound is the matching rule's
`max_request_bytes` when it declares one, and otherwise a new broker option
defaulting to 1 MiB. That number is chosen from what a credential-bearing body
actually is: a form or a small JSON document. It is never an upload, because
an upload endpoint does not take its credential in the payload.

Buffering is a property of the workspace's bindings, not of every request. A
broker whose leases declare no body location never buffers, so an agent that
streams a large upload through the reverse proxy is unaffected. This is the
reason the bound can be small: it applies only where the operator asked for
body substitution.

Body substitution is a reverse-proxy capability. The package and git
connectors substitute in headers alone — their grammars forbid the shapes body
substitution needs (the package connector refuses bodies outright; a git pack
body is binary) — and CONNECT stays host-granular and opaque, because
rewriting inside a tunnel would mean terminating TLS with a CA installed in
the workspace, which ADR 0007 rules out.

### Fail closed on every ambiguity

A body over the bound, a content type the declared location cannot parse, a
malformed form or JSON document, a named parameter or field occurring other
than exactly once, a pointer that names no member or names something that is
not a string, a placeholder outside its declared location: each is a refusal,
audited, before any upstream byte.

The alternative is to guess which occurrence the workspace meant. Guessing is
how a credential ends up in a location nobody authorized, and the failure is
silent — the request succeeds and the operator learns about it from the
provider's logs. A refusal is loud, cheap to diagnose from the typed reason,
and safe.

One consequence is recorded rather than hidden: a `body_json` substitution
re-serializes the document, so keys come back in sorted order. Numbers
round-trip exactly (the decoder keeps them as `json.Number`) and nothing is
HTML-escaped, but a caller that needs byte-identical bodies should declare a
header or query location.

### The refusal contract

Every refusal carries `X-Remount-Reason` and a JSON body
`{"error":{code, reason, binding, host, message}}`. `code` is a wire code and
`reason` is one of the `proto.Reason*` constants introduced on the base
branch, so a workspace matches the same pair the control plane returns for the
same situation over the wire. There are no new codes.

`message` is present and diagnostic. Removing it would have been cleaner on
paper and worse in practice: an operator reading a workspace's stderr needs to
know *which* ambiguity refused the request, and the machine contract is the
first two fields regardless. It is scrubbed before it is written.

Every refusal goes through one function, `Broker.deny`. That is the point of
the change as much as the shape is: forty-odd `http.Error` call sites across
four surfaces could not have stayed consistent, and the header, the body and
the scrubbing now cannot drift apart.

### Redaction scope

`internal/redact` holds the expressions and the literal-aware `Redactor`;
`internal/node/agentredact.go` is a thin wrapper so the transcript call sites
are unchanged. The broker applies it in two places: `deny`, for the text a
workspace receives, and `emit`, for an audit's `reason` and `error` before
they become durable events.

Redaction is defence in depth and nothing more. The broker does not put
secrets in messages: a rejection names a binding id and a host, and an upstream
failure is recorded as a class (`dns`, `tls`, `timeout`), never as transport
error text. What redaction guards is the day something else does — the
`TestErrorMessagesRedactCredentialShapedStrings` case is real: an upstream
that answers with an unparseable `Location` puts its own bytes into the error
text the broker quotes.

That test follows the repository's rule about scans. It proves its instrument
finds a planted canary before it trusts a clean result, and the canary is
synthetic and shaped like a key, never a real credential.

### Revocation cuts what is already running

`revoke_egress` closed the broker with `http.Server.Shutdown`, which is a
graceful shutdown: it closes the listener and then *waits* for active
connections to go idle. A response already streaming from an allowed
destination therefore kept streaming after the operator was told the workspace
was contained. Measured, it survived the full twenty seconds the new test
waits.

`Broker.Revoke` force-closes instead, and the fencing paths (`quarantine` and
`fenceWorkspace`) call it. `Close` keeps its graceful window, because an
ordinary teardown should let an in-flight response finish. Containment is not
a request to stop soon.

## What the process backend can and cannot do

`TestFleetRevokeEgressKillsGatewayAccess` runs on the `process` backend and
proves all three halves of the claim there: the in-flight transfer is cut, the
next connection is refused, and `ws.fenced` records the action. The process
backend can do this because the broker owns the listening socket, so closing
it is a real revocation of the gateway.

What it cannot do is stop a workspace from reaching the network *around* the
broker. Proxy mediation on `process` and `docker` is cooperative: a process
that dials a destination directly was never going through the broker and is
unaffected by revoking it. That is not a gap this ADR closes; it is why those
backends are rejected by production security profiles and why
`enforced_gateway` exists (ADR 0061). On an enforced-gateway backend the same
path additionally calls `RevokeNetwork`, which deletes the veth.

## Consequences

- A binding can now serve a provider that takes its credential in a query
  parameter, a form field or a JSON body, with the same destination scoping,
  TTL, HTTPS requirement and audit trail as a header binding.
- A placeholder in a request body is scanned for scope where it previously was
  not. This is strictly more blocking, and the blocked case was an
  exfiltration path.
- Workspaces with a body-substituting binding have their request bodies
  buffered under a bound. Everything else still streams.
- Refusals are machine-readable. Nothing in the repository matched on the old
  prose, and existing substring assertions still hold because `message` keeps
  the same text.
- The `events.tail` request gains `binding`, `host` and `types` filters,
  applied by the control plane so an audit question is one call rather than a
  history the caller sifts. They are additive; a peer that sets none sees the
  stream it always saw.
