# Remount MCP server

Remount exposes its durable agents, workspaces, approvals, events, and protocol
operations to MCP hosts through one bounded server:

```sh
remount mcp serve
```

The default transport is stdio. It reads the normal explicit client settings
(`REMOUNT_SERVER`, `REMOUNT_TOKEN`, and `REMOUNT_PRINCIPAL`) and never reads
`.remount/env`. To run stateless HTTP locally:

```sh
remount mcp serve --http 127.0.0.1:8765 --mcp-token "$REMOUNT_MCP_TOKEN"
```

The endpoint is `http://127.0.0.1:8765/mcp`. Non-loopback listeners require
`--mcp-token`. HTTP callers also send the modern
MCP protocol, method, and tool-name headers.

Generate a host-native configuration without embedding credentials:

```sh
remount mcp config claude
remount mcp config codex
remount mcp config cursor
remount mcp config goose
remount mcp config gemini
remount mcp config opencode
```

Use `--command /absolute/path/to/remount` when the binary is not on the host's
PATH. The generated snippets pass only explicit environment names where their
host format supports that distinction.

To place an existing stdio MCP server behind the workspace broker, map each
credential environment variable to a binding and provide the broker explicitly:

```sh
remount mcp wrap --broker "$REMOUNT_BROKER" \
  --secret-env OPENAI_API_KEY=b_openai -- third-party-mcp --stdio
```

The child receives `ref:b_openai` instead of the inherited key. Existing HTTPS
variables ending in `_BASE_URL` are routed through the broker. Query strings,
fragments, embedded URL credentials, non-HTTPS upstreams, and shell command
strings are rejected or never interpreted.

MCP request ids are not mutation idempotency keys. Reuse the same
`idempotency_key` argument when retrying an ambiguous mutating call.
