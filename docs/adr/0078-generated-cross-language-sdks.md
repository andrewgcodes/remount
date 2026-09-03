# ADR 0078: Cross-language SDKs derive types and preserve cursor semantics

## Status

Accepted.

## Context

The Go client is executable protocol documentation, but it is not usable by
Python- and TypeScript-native agent harnesses. Copying its body types by hand
would create three competing protocol schemas. Treating a WebSocket like a
session socket would also regress Remount's defining property: session output
is a replayable log, and a disconnect cannot silently cut a hole in it.

## Decision

`cmd/protogen` parses the Go syntax trees in `internal/proto` without importing
or executing that package. It emits the committed Draft 2020-12 JSON Schema,
the operation table, Python `TypedDict` declarations, and TypeScript
interfaces. Generation sorts declarations and operations and `--check`
byte-compares each output, making `internal/proto` the sole authority. Unknown
object fields remain permitted because protocol v1 explicitly allows additive
fields. JSON byte strings use base64; the generated SDK type uses native byte
arrays for CBOR.

Python uses `asyncio`, HTTPX, `websockets`, and `cbor2`. TypeScript uses the
platform Fetch and WebSocket APIs (falling back to `ws` on Node) plus `cbor-x`,
and publishes conditional ESM and CommonJS entry points. These choices follow
the current upstream APIs: [websockets asyncio client](https://websockets.readthedocs.io/en/latest/reference/asyncio/client.html),
[HTTPX async streaming](https://www.python-httpx.org/async/), [RFC 8949 CBOR](https://www.rfc-editor.org/rfc/rfc8949.html),
[cbor-x encoding](https://github.com/kriszyp/cbor-x), [ws client](https://github.com/websockets/ws),
and [Node conditional exports](https://nodejs.org/api/packages.html). Python
project metadata uses the standardized [`pyproject.toml` project table](https://packaging.python.org/specifications/declaring-project-metadata/),
and the schema declares the official [JSON Schema Draft 2020-12](https://json-schema.org/draft/2020-12).

Both transports implement the same lifecycle:

1. A connection negotiates exact protocol v1 before application traffic.
2. Request IDs correlate a bounded set of pending calls. A connection loss
   fails all pending calls; calls reconnect and retry with the original body.
3. Every public mutation creates one idempotency key before the first attempt,
   or accepts a caller key for retries that survive a client process.
4. A session is registered by server session ID. Chunks that race ahead of the
   open response enter a bounded orphan buffer. Chunks are deduplicated and
   reordered by sequence in bounded memory.
5. Reconnect clears connection-scoped grants, fetches fresh grants, and issues
   `s.attach` from the next undelivered sequence. A `gap` remains explicit; it
   is never rendered as complete output.
6. Artifact uploads name the SHA-256 of the exact bytes. Downloads are bounded
   and the digest is checked before the aggregate is returned.

The JSON Agent client is a translation client for the existing HTTP surface,
not another authority. It covers creation, lookup, listing, messaging, fork,
lifecycle actions, cursor-based transcript streaming, approval listing and
decisions, bounded status/approval waits, diffs, and terminal attachment. Diff
reads do not set `wake=true` unless the caller explicitly requests it. Terminal
attachments require `open` first, retain a bounded delivery queue, expose their
session and next replay cursor, and preserve `gap`/`exit` as control events.
Browser TypeScript authenticates the terminal with the server's base64url
bearer subprotocol; Python uses the Authorization header. HTTP mutation retries
carry `Idempotency-Key`; transcript eviction raises a stable `evicted` error.

## Authority and failure model

Control remains assignment, agent, and approval authority; the current node
and generation remain workspace execution authority. SDK state is only
correlation state and replay cursors. The durable commit point for a mutation
is the server's response after its resource/event transaction, not the local
send. An ambiguous disconnect therefore permits only an idempotent retry.

The bounded resources are pending requests, orphan sessions/chunks and bytes,
session and terminal delivery/reorder queues and bytes, reconnect attempts,
frames, HTTP response bodies, and aggregate artifact downloads. Overflow closes or fails the affected transport with
`resource_exhausted` so replay can recover; bytes are never silently dropped.
Credentials appear only in transport authentication and are never included in
SDK error text.

## Alternatives rejected

- Reflection or a second hand-authored IDL would allow initialization side
  effects or schema drift.
- Generated transports would hide reconnect and session-log invariants in a
  generic RPC runtime that does not model them.
- Retrying mutations with a fresh key could duplicate effects after an
  ambiguous response.
- Treating an unavailable live E12 endpoint as a passing test would overstate
  distribution evidence.

## Consequences

The repository can prove deterministic drift, offline behavior, wheel/sdist
creation, ESM/CJS builds, and external package consumption. The live E12 job
runs both languages against one configured server and otherwise labels the
gate unavailable; each probe destroys the Agent it created. Publishing
`remount` to PyPI and `@remount/sdk` to npm is a separate release action and is
not implied by these files. Real-harness E19/E20 coverage and E21 outbound
`approval.pending` webhook delivery remain external/server gates; the SDK tests
prove their route, cursor and decision behavior with offline fixtures only.

The invariant added to `AGENTS.md` is: **generated SDK declarations have one Go
AST authority, and SDK reconnect resumes bounded cursors with the same logical
idempotency key; transport loss never becomes silent output loss or duplicate
mutation authority.**
