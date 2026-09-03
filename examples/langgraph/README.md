# LangGraph orchestrating a Remount Agent

This graph treats the Remount Agent as durable external state. LangGraph owns
the orchestration branches; Remount owns the workspace, harness process,
transcript, inbox, and approval resource. The graph creates an Agent, waits for
an explicit state, optionally sends one follow-up, and chooses the first
server-offered approval option.

```sh
python -m pip install -e ../../sdk/python
python -m pip install -r requirements.txt
export REMOUNT_SERVER=https://remount.example REMOUNT_TOKEN=...
python graph.py --binding b_openai --operation-id ticket-123 \
  --follow-up 'Run the focused test.' \
  'Ask me which test to run before editing, then request permission.'
```

No credential is graph state. `Context` contains the live SDK client only for
the current graph invocation, while serializable state contains the Agent ID,
status, and caller-owned operation ID. Every mutating node derives a stable
idempotency key from that ID. In production, add a LangGraph checkpointer and
persist this state; recreating the runtime context reconnects to the same
Remount resource.

The smoke test executes every branch with a fake SDK client. It proves adapter
logic but does not claim a model, harness, or hosted LangGraph deployment ran.

Reference: [LangGraph StateGraph and runtime context](https://reference.langchain.com/python/langgraph/graph/state/StateGraph).
