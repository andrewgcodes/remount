# ADR 0060: the stable link's JSON form takes a header credential only

## Status

Accepted. Narrows ADR 0058.

## Context

ADR 0058 left the `remount_session` cookie valid on two routes: the preview
proxy and `GET /a/{id}`. The second was meant for navigation, but the route
has two forms. With `AgentURLBase` configured it is an unauthenticated
redirect to the operator UI, which then authenticates on its own. Without it
the route returns the `proto.Agent` record itself: the task text, the recipe
YAML, the inbox messages, the parent, owner and tenant, and the workspace id.

That JSON form is exactly the kind of read ADR 0058 removed the cookie from
everywhere else. A page served by the preview proxy is same-origin with the
API and carries the cookie, so it could `fetch("/a/<id>")` for any agent id
it can guess or was given and read another agent's task and inbox.

## Decision

The JSON form of `GET /a/{id}` authenticates like `GET /v1/agents/{id}`:
`Authorization: Bearer` only. The redirect form is unchanged and still needs
no credential. The cookie is now accepted on the preview proxy alone.

## Consequences

- A browser that opens `/a/{id}` on a server without an operator UI sees
  `401 unauthorized` instead of raw JSON. That form was never a page; a client
  that wants the record sends the header, as the reference clients already
  do.
- `docs/api.md` names the preview proxy as the cookie's only route, and the
  `internal/sim` cookie-scope test lists `/a/{id}` among the routes that
  refuse a cookie-only request. The `Origin` check for cookie requests is
  asserted on the preview route.
