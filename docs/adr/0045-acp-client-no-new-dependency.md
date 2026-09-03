# ADR 0045: ACP is spoken by a hand-written client with schema-generated types

## Status

Accepted.

## Context

Phase 1A turns Remount into a runtime for ACP-speaking coding agents
(ADR 0042: Remount is the *client*, the harness in the workspace is the
*agent*). The protocol is JSON-RPC 2.0 over the agent's stdio, one message per
line, with a published JSON schema for every request, response, notification
and content type. The obvious shortcut, the existing Go ACP SDK, pulls in a web
framework and its dependency tree, which breaks the rule that the binary stays
static, small and free of dependencies nobody in this repo understands.

Two things about the schema shaped the design. It is large (170 definitions)
and it moves monthly, so hand-maintained structs would drift. And it is
deliberately extensible: every object carries `_meta`, unions are open, and a
newer agent may send fields the client has never heard of. A client that
rejects what it does not recognize is a client that breaks on every agent
release.

## Decision

`internal/acp` is written by hand where the protocol is small and generated
where it is large.

**Hand-written** (`client.go`): the JSON-RPC layer. `Client` owns one
agent's stdio pair: a read loop that classifies each line as request,
notification or response; a correlation map from integer ids to waiting
callers; a mutex-serialized writer; and one goroutine per inbound request so
an agent's `fs/read_text_file` is answered while its `session/prompt` is in
flight. Typed methods (`Initialize`, `Authenticate`, `NewSession`,
`LoadSession`, `ResumeSession`, `Prompt`, `Cancel`, `SetMode`,
`CloseSession`, `Reopen`) wrap `Call`/`Notify`. The node supplies a `Handler`
for the agent's calls back; `UnimplementedHandler` answers "method not found"
for anything the node does not serve, so a capability that was not advertised
cannot be exercised by an agent that ignores the advertisement.

**Generated** (`types_gen.go`, `methods_gen.go`, by `cmd/acpgen` from
`spec/acp/schema.json` and `spec/acp/meta.json`, pinned in `spec/acp/VERSION`):

- objects become structs with `json` tags; required properties come first in
  schema order, optional ones are `omitempty`, `_meta` is `json.RawMessage`;
- string enumerations become string types with constants, and open ones
  (`SessionConfigOptionCategory`) keep the constants while accepting any
  value;
- tagged unions (`SessionUpdate`, `ContentBlock`, `ToolCallContent`,
  `McpServer`, `AuthMethod`, `RequestPermissionOutcome`, ...) become
  `{Kind string; Raw json.RawMessage}` with `As<Variant>()` decoders and
  `New<Union><Variant>()` constructors. Marshalling writes `Raw` back
  byte-for-byte, so a variant this build does not know, or a known variant
  carrying fields this build does not know, passes through the transcript and
  back to the agent unchanged;
- anything the generator does not understand becomes `json.RawMessage` with a
  comment, never a build failure.

Go's decoder ignores unknown object fields, so structs also tolerate a newer
agent; the transcript stores the raw frame (`Options.Frames` sees every line in
both directions before it is acted on), which is where fidelity matters.

`make lint` runs `go run ./cmd/acpgen -check`, the same drift discipline as
`llms.txt`: the committed output must match what the generator produces from
the committed schema. Bumping the schema is `cp` + `make acpgen` + a commit that
shows exactly what changed on the wire.

**Capabilities.** `Initialize` defaults to the schema's protocol version and
refuses an agent that answers with any other (`ErrProtocolVersion`); ACP
versions are bumped only for breaking changes. The agent's advertised
capabilities are remembered; `Reopen` chooses `session/resume` when advertised,
falls back to `session/load` (which replays history as updates), and returns
`ErrCannotReopen` when the agent offers neither, so the node can decide to
start a fresh session and say so. `additionalDirectories` and, later, the
node's own `fs`/`terminal` advertisements follow the same rule: never send what
the other side did not say it accepts.

**Cancellation.** `Prompt` does not abandon a turn when its context ends. It
sends `session/cancel` and waits up to `CancelGrace` for the agent's
`cancelled` response, because a turn that is still running on the agent while
the client believes it stopped is exactly the confusion the protocol's stop
reason exists to prevent. Only when the agent fails to confirm does `Prompt`
return the context error. Inbound requests get the mirror image: `$/cancel_request`
from the agent cancels the handler's context.

**Failure.** A malformed line is answered with a JSON-RPC parse error (id
`null`) and the connection continues; agents are allowed to be buggy without
losing the session. A line over `MaxLine` (16 MiB default) ends the connection,
because an unbounded buffer is a memory bomb. When the agent's stdout closes,
every pending call fails with `ErrClosed`, `Updates` is closed after the last
delivered update, and `Done`/`Err` report why.

## Consequences

- One new package and one generator, zero new modules in `go.mod`.
- `internal/acp/acptest` is an in-process fake agent (a Go struct on a pipe,
  or a real process via `acptest.Main`) that scripts turns, requests
  permission and files from the client, honours or ignores cancel, and can
  emit garbage or die mid-turn. Every method and every failure above has a
  test under `-race`.
- The generated field names use Go initialisms (`SessionID`) while type
  names keep the schema's spelling (`SessionId`) so a reader can grep the
  schema for them.
- Unknown-field fidelity is exact for unions and for the transcript, and
  best-effort for plain structs (unknown fields are dropped if a struct is
  re-marshalled). Nothing in Remount re-marshals a struct it received from an
  agent to send it back; if that ever changes, the struct in question should
  become a union-style raw carrier.
