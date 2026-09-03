# ADR 0056: the agent HTTP API is a translation layer, not a second control plane

## Status

Accepted.

## Context

An Agent is only useful if a person can watch it, answer it and look at what
it did. The CLI can do that over the frame protocol, but a browser cannot: it
cannot open a CBOR WebSocket to a relay, it cannot hold a capability grant
across a page load, and the surfaces people actually want — a transcript that
follows, a terminal, a file tree, a diff, the port the agent is serving — are
HTTP surfaces.

The tempting shape is a web service that talks to SQLite directly and does its
own authorization, sessions and rate limiting. That is a second control plane
with a second copy of every rule, and the copies diverge: the CLI would allow
what the API refuses, an event would be emitted on one path and not the other,
an idempotent retry would be idempotent in one place.

## Decision

The HTTP API is a translation layer over the Go SDK. Each handler does four
things: take the request's bearer credential and get an in-process
`client.Client` for it, decode the body into the protocol request, call the
one SDK method, and map the protocol error code to a status. It authorizes
nothing itself. `internal/server/api.go` has no access to the control plane's
tables.

Consequences that follow from that single decision:

- **One authorization model.** A capability that cannot read an agent cannot
  read it over HTTP either, because the control plane is the thing being
  asked. Tenancy, revocation epochs and grants need no HTTP-specific code.
- **One idempotency model.** `Idempotency-Key` becomes
  `client.WithIdempotencyKey`. A retry replays; a key reused with different
  arguments is `409`.
- **One event log.** Every mutation emits its event where it always did.
- **Bounded client pool.** Credentials are keyed by SHA-256 in a reference
  counted LRU (`--max-api-clients`, idle close after ten minutes) so N browser
  tabs are one uplink, and the credential itself is never a map key.

### Browser credentials

Three carriers, deliberately unequal:

1. `Authorization: Bearer` — everywhere.
2. WebSocket subprotocol `remount.bearer.<base64url(cred)>` — a browser
   cannot set a header on a `WebSocket`, and the subprotocol is the only
   field it controls. The server echoes it, as the handshake requires.
3. Cookie `remount_session` (`HttpOnly`, `SameSite=Strict`, `Secure` over
   TLS, 12 h), minted by `POST /v1/session` from a header credential — for
   the two things a browser does without JavaScript in the loop: loading a
   preview subresource and opening a WebSocket.

The cookie is accepted only on safe methods, WebSocket upgrades and the
preview proxy. A cookie-authenticated mutation is a CSRF target and the header
form is not, so the cookie simply cannot perform one. `SameSite=Strict` keeps
the cookie off cross-site requests already; we additionally refuse a
cookie-authenticated request whose `Origin` is neither our own host nor a
listed CORS origin, because "the browser should not have sent it" is not a
security boundary. Cross-origin access is opt-in per origin (`--cors`);
credentials are never allowed for `*`.

### Reads never wake

Sleep is how an agent stops costing anything. If opening a UI woke it, every
glance would resume a workspace, and a dashboard polling ten agents would keep
ten workspaces claimed. So the transcript is served from the durable mirror
(§6.2) and never touches a node, and the filesystem and terminal refuse a
sleeping agent with `conflict`. Waking is explicit and attributable: `POST
/wake`, `diff?wake=true` and the preview proxy, each emitting `agent.woken`
with `by: request|diff|preview`. The wake path is shared with the CLI
(`client.AgentMaterialized`) so both wait for `claimed` the same way and time
out the same way.

### Streaming is the mirror, three encodings

`GET .../transcript` is one implementation with three emitters: a JSON page, SSE
and a WebSocket. All three are cursor-resumable (`from`, or `Last-Event-ID`
for an `EventSource` reconnect), all three report an eviction as an explicit
`gap`, and all three end with `done` when the agent is terminal and the
cursor has reached the end. Because the source is the mirror, a stream
survives a node loss, a move and a sleep without the client noticing.

### The stable URL holds no capability

`Agent.url` is `https://host/a/{id}`. It is safe to paste into an issue: it
carries no token. With `--agent-ui` it redirects to the operator UI (which
authenticates), otherwise it answers `agent.get`. The id is validated as one
URL-safe token before any redirect target is built, so it cannot rewrite the
target beyond its own slot.

### The preview proxy is a normal reverse proxy over a port session

`/v1/agents/{id}/ports/{port}/...` dials the same port forward `remount port`
uses and proxies HTTP and WebSocket traffic. The workspace program sees
`127.0.0.1` and `X-Forwarded-Host`/`-Proto`/`-Prefix`. `Authorization` and
the session cookie are stripped on the way in: the workspace is trusted with
nothing, and that includes the credential of the person looking at it.

## Consequences

- A UI is a client, not a plugin. `examples/` and any external web client use
  exactly the routes documented in `docs/api.md`.
- The API cannot outgrow the protocol. A new capability needs an operation
  first; the route is then three lines.
- The cost is one in-process client per distinct credential and a JSON
  rendering of transcript records (the ACP frame is decoded so a UI reads
  JSON rather than base64 CBOR).
- `remount server --public-url` must be set for `Agent.url` to be minted at
  all; without it the field is empty and a client builds `/a/{id}` against
  whatever host it reached.
