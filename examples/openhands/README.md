# OpenHands on a durable Remount computer

This example uses the generated Remount Python SDK to launch the built-in
`openhands` recipe. OpenHands runs inside the Remount workspace; the calling
process only holds the Agent ID and can reconnect, send a follow-up, or read
the control-plane transcript after the original process exits.

```sh
python -m pip install -e ../../sdk/python
export REMOUNT_SERVER=https://remount.example REMOUNT_TOKEN=...
python run_openhands.py --binding b_openai \
  --operation-id ticket-123 \
  --follow-up 'Also run the focused tests.' \
  'Add a regression test for the reconnect bug.'
```

The binding ID refers to a server-side secret and policy. No model key is
passed as an Agent request field or written into the workspace. Approval mode
is `on-request`; if OpenHands reaches `waiting_approval`, decide it through the
Agent API or `remount approve`. Reuse `--operation-id` if the orchestrator
restarts after an ambiguous response; create and message then replay safely.

This is intentionally not a second OpenHands workspace adapter. Remount owns
the durable computer and already ships an OpenHands ACP recipe, so nesting an
OpenHands Docker workspace would weaken lifecycle and credential ownership.
The smoke test uses a fake generated-SDK client and does not claim a real model
call.

References: [OpenHands ACP Agent](https://docs.openhands.dev/sdk/guides/agent-acp),
[Remount harness integration](../../docs/harness-integration.md).
