# ADR 0079: MCP distribution is a bounded client adapter

**Status:** Accepted

**Date:** 2026-09-03

## Context

Remount needs one MCP surface that works over stdio and stateless HTTP, exposes
every protocol operation, supplies useful agent/workspace composites, and can
be configured by the common agent harnesses. It must not become a second
control plane or move credentials into a workspace or harness transcript.

The current MCP specification is the 2026-07-28 stateless revision. It replaces
initialization with `server/discover`, adds per-request method/name headers for
HTTP, and formalizes result metadata and cache behavior. Existing clients still
widely implement the 2025-11-25 initialization and `tools/list`/`tools/call`
surface, so the server supports both discovery generations on the same tool
manifest.

Primary references:

- [MCP 2026-07-28 versioning](https://modelcontextprotocol.io/specification/2026-07-28/basic/versioning)
- [MCP 2026-07-28 server discovery](https://modelcontextprotocol.io/specification/2026-07-28/server/discover)
- [MCP 2026-07-28 tools](https://modelcontextprotocol.io/specification/2026-07-28/server/tools)
- [MCP 2026-07-28 stdio transport](https://modelcontextprotocol.io/specification/2026-07-28/basic/transports/stdio)
- [MCP 2026-07-28 Streamable HTTP transport](https://modelcontextprotocol.io/specification/2026-07-28/basic/transports/streamable-http)
- [MCP 2025-11-25 transports](https://modelcontextprotocol.io/specification/2025-11-25/basic/transports)
- [MCP 2025-11-25 tools](https://modelcontextprotocol.io/specification/2025-11-25/server/tools)
- [JSON-RPC 2.0](https://www.jsonrpc.org/specification)
- [Claude Code MCP configuration](https://code.claude.com/docs/en/mcp)
- [Claude Code headless mode](https://code.claude.com/docs/en/headless)
- [OpenAI Codex MCP configuration](https://developers.openai.com/codex/mcp)
- [OpenAI Codex non-interactive mode](https://developers.openai.com/codex/noninteractive)
- [Cursor MCP configuration](https://docs.cursor.com/context/model-context-protocol)
- [Gemini CLI MCP servers](https://geminicli.com/docs/tools/mcp-server/)
- [OpenCode MCP servers](https://opencode.ai/v2/docs/mcp-servers)
- [Goose CLI and extensions](https://block.github.io/goose/)

## Decision

`internal/mcp` is a stateless adapter around the reconnecting Go client.
Control and node code remain authoritative for all durable state. The adapter
never owns a workspace, agent, approval, lease, grant, event, or credential.

One deterministic manifest generates the advertised protocol tools. Every
protocol operation is discoverable as `op_<operation>`, while operations that
cannot honestly be performed by a client-role connection return the stable
`unavailable` execution result. Composite tools provide `agent_create`,
`message`, `get`, `transcript_tail`, `approve`, `run`, `handoff`, `resume`, and
`events_tail`. Mutating schemas expose an idempotency key; a JSON-RPC request id
is correlation and cancellation state, never an idempotency key.

Stdio is newline-delimited JSON-RPC. Input size, output size, concurrency, and
call duration are bounded. EOF and cancellation cancel outstanding operations
and join them before returning. Stdout carries protocol messages only. HTTP is
the modern stateless POST transport: it validates the protocol/method/tool
headers, rejects untrusted origins, uses explicit bearer authentication, and
may not bind a non-loopback address without a token.

Authentication comes only from explicit flags or environment variables. The
MCP package never reads `.remount/env`. Tool results recursively remove
credential-bearing fields, and workspace-info results expose only workspace id
and broker URL. `mcp wrap` uses argv execution rather than a shell, replaces
selected inherited secrets with `ref:<binding>`, routes HTTPS `*_BASE_URL`
origins through the explicit broker, and cancels and joins its child before
returning.

The durable commit point for a mutation is the existing control-plane
transaction that commits the resource row and canonical event together. MCP
reports success only after the Go client observes that response. A transport
failure before a response is ambiguous, so callers must retry with the same
idempotency key. Stable protocol codes are returned in MCP tool error results;
unknown tools and malformed JSON remain JSON-RPC protocol errors.

## Verification

The deterministic E13 lane drives initialize, discovery, child agent creation,
status, transcript, and canonical event reads through in-process JSON-RPC. It
requires no model key, bounds the interaction, checks the full generated
manifest, and plants credential-shaped canaries that must not appear.

Two optional headless lanes exercise the built candidate through the official
Codex CLI with OpenAI and Claude Code with OpenRouter. Each lane explicitly
reports `SKIPPED` when its required repository secret is absent; an absent key
is never recorded as a pass. Live lanes verify child completion, canonical
event readability, and transcript leak scans. They prove the MCP lifecycle
path, not arbitrary model quality or every individual protocol operation.

## Consequences

Adding a protocol operation requires changing the canonical protocol first;
the manifest-completeness test then fails until MCP exposes it. Some advertised
low-level operations intentionally remain unavailable because widening the
client role would weaken authority boundaries. Harness configuration snippets
contain environment-variable names but never secret values, so users retain
responsibility for supplying credentials to the outer MCP process.
