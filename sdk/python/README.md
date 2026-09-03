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
