# ADR 0077: a bounded, embedded operator console

## Status

Accepted, 2026-09-03.

## Context

The event log and lifecycle APIs are complete enough to operate without a UI,
but they are difficult to inspect together during an incident. The product
needs a fleet and audit surface, not a second agent product. In particular it
must not turn transcript or chat rendering into a privileged code path next to
an interactive terminal.

The console handles hostile workspace names, filenames, terminal bytes and
event metadata. A browser terminal gives every script in its origin access to
keystrokes, so the dependency and scripting boundary must stay small. A
deployment must not inject credentials into a generated bundle or runtime
configuration file.

## Decision

`web/` is a Vite, Preact and strict TypeScript application with hand-written
CSS. `@xterm/xterm` and the first-party fit addon are its only runtime
dependencies besides Preact. Vite builds with `/console/` as its fixed base;
the Go binary embeds the committed `web/dist` files and serves them at that
path. `web/dist.sha256` covers every built file and CI rebuilds and compares
it. CI also sums deterministic gzip output for all built assets and rejects
more than 1,500,000 bytes.

The views are fleet, workspace lifecycle, terminal, files, canonical event
timeline, credential/leak decisions, pending approvals, usage and budgets.
There is no chat, transcript, prompt composer, harness UI or end-user agent
view. Reads require an authenticated tenant viewer; create, exec, file writes,
approvals, snapshot, move, quarantine and destroy require the operator role.
The server remains the enforcement point.

The static `config.json` contains only a same-origin API path and a bounded
refresh period. A cross-origin or credential-bearing API root is rejected.
The operator enters a bearer credential after each page load. It is retained
only in one `APIClient` instance, is never written to Web Storage, and is sent
as an `Authorization` header or the existing
`remount.bearer.<base64url>` WebSocket subprotocol. The preview-only
`remount_session` cookie is deliberately not accepted by console APIs: an
untrusted preview shares the control-plane origin today and must not inherit
operator authority.

The console does not use `innerHTML`, evaluate code, load remote fonts or
scripts, turn terminal output into links, or use the xterm attach addon. It
implements the Remount terminal framing directly. Static responses receive a
restrictive CSP, `X-Content-Type-Options: nosniff`, `Referrer-Policy:
no-referrer`, and frame denial. Browser writes carry idempotency keys supplied
by the browser and required by the server adapter, and file updates are
conditional on an ETag so a stale
editor cannot silently overwrite a newer generation.

The terminal attach API reuses the session log and wraps each output chunk with
its durable sequence for the browser. Session chunks do not have wall-clock
timestamps, so a timestamp replay request resolves conservatively to the
oldest retained cursor whose session interval overlaps that time. An
evicted interval is always a visible `gap` control message. Reconnect is an
attach to the same session and cursor, never a new process. The console uses a
bounded 100,000-line xterm scrollback; durable retention remains the server's
responsibility.

## Consequences

The binary gains an operator surface without a runtime Node process. Browser
assets remain reproducible and far below the declared cap. Operators must
enter a credential again after a reload until console-specific OIDC is added;
this inconvenience avoids broadening the preview cookie's authority.

The console uses a thin, role-enforcing HTTP adapter over existing control and
node operations. Its exact contract is in `docs/console.md`. CSP verification,
route authorization and the sim-backed version of E15 are server integration
proofs, in addition to the mock-backed browser flow in `web/tests/e2e`.

Read-only shared or stale state is labelled with its observation time. An API
or diagnostic that cannot be reached is rendered as an error, not an empty or
healthy fleet.

## References

- [Vite production builds and public base paths](https://vite.dev/guide/build)
- [Preact TypeScript guidance](https://preactjs.com/guide/v10/typescript/)
- [xterm.js security guidance](https://xtermjs.org/docs/guides/security/)
- [Playwright WebSocketRoute](https://playwright.dev/docs/api/class-websocketroute)
