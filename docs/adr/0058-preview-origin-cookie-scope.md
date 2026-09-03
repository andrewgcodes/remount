# ADR 0058: the browser cookie authenticates only what a browser navigates to

## Status

Accepted. Narrows the cookie rule in ADR 0056.

## Context

ADR 0056 accepted the `remount_session` cookie on every safe method, every
WebSocket upgrade and the preview proxy, reasoning that a cookie cannot
perform a mutation and `SameSite=Strict` plus an `Origin` check keeps it off
cross-site requests.

That reasoning has a hole the preview proxy opens itself. A preview is served
from the API's own origin (`/v1/agents/{id}/ports/{port}/...`), and its
content is written by the program running in the workspace, which the whole
design trusts with nothing. JavaScript on that page is same-origin with the
API: `SameSite` does not apply, the `Origin` header is the API's own host and
passes the check, and the browser attaches the cookie. With the ADR 0056 rule
that page could list every agent the operator can see, read any file in any
of their workspaces, open a terminal (a WebSocket upgrade, so a "safe"
request that spawns a program), and wake sleeping agents through
`diff?wake=true`. One compromised dependency in one workspace's dev server
becomes the operator.

## Decision

The cookie is a navigation credential. It is accepted on exactly two routes:
the preview proxy, so a browser can load a preview and its subresources, and
`GET /a/{id}`, so the stable link opens. Every other route takes
`Authorization: Bearer` or the `remount.bearer.<base64url>` WebSocket
subprotocol only, including the safe ones. The `Origin` check and the
mutation refusal stay.

The preview proxy also strips the `remount.bearer.*` subprotocol from a
forwarded WebSocket handshake, as it already stripped `Authorization` and the
session cookie: a browser opening a socket through the preview carries its
credential there, and the workspace must not see it.

Operators who want the two origins fully separate front
`/v1/agents/{id}/ports/` from a distinct hostname; the API does not need to
know.

## Consequences

- The reference web client already spoke bearer everywhere except the preview
  link, so it does not change. A client that relied on the cookie for
  `GET /v1/agents/...` must send the header.
- The remaining cookie exposure is the preview proxy itself: page A can load
  page B's preview for another agent the operator owns. That is read access
  to another workspace's web UI, not to its files or a shell, and a separate
  preview hostname removes it.
- `internal/sim` asserts, per route, that a cookie-only request to the
  agent, transcript, approvals, diff-with-wake, filesystem and terminal routes
  is `401 unauthorized`, that the stable link and the preview still accept it,
  and that a bearer subprotocol does not reach the upstream program.
