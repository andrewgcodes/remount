# Remount Python SDK

`remount` provides the asyncio CBOR/WebSocket client and the JSON agent and
approval API client. Mutating helpers accept an optional stable
`idempotency_key`; when omitted, one is generated once per logical call and
reused across transport retries.

```python
from remount import AgentClient

async with AgentClient("https://remount.example", token="...") as agents:
    agent = await agents.create_agent({"spec": {"recipe": "opencode", "task": "fix tests"}})
    await agents.message_agent(agent["id"], "run the focused tests")
    async for record in agents.stream_transcript(agent["id"]):
        print(record)
```

The same client reads diffs without waking a sleeping workspace, polls for
agent states or pending approvals with explicit deadlines, and attaches the
terminal WebSocket. A terminal exposes `session` and `next_seq`; pass both to
`connect_terminal` after a disconnect to resume retained output.

Package publication and live-provider validation are release gates; this tree
only builds and tests the package locally.

The protocol `Client` negotiates the current capabilities, including controller
epochs, with strict-profile servers. It accepts session output only from the
grant-authorized node and retains explicit replay gaps. Node-owned revocation,
session capabilities and release ordering stay enforced by the server/node.
Artifact helpers transfer digest-verified `art_sha256` byte objects; they do
not reconstruct chunked snapshot manifests. These checks do not certify an
execution backend for production isolation.

See [the cross-language protocol gate](../../integration/sdks/README.md) for
local interoperability verification without cloud or model credentials.
