# `@remount/sdk`

The package contains a CBOR/WebSocket protocol client and JSON Agent API
client. It builds both ESM and CommonJS entry points. All mutations generate
an idempotency key once and reuse it across reconnect retries; callers may
supply their own key for retries that outlive a process.

```ts
import { AgentClient } from "@remount/sdk";

const agents = new AgentClient("https://remount.example", process.env.REMOUNT_TOKEN!);
const agent = await agents.createAgent({ spec: { recipe: "opencode", task: "fix tests" } });
await agents.messageAgent(agent.id, "run the focused tests");
for await (const record of agents.streamTranscript(agent.id)) console.log(record);
```

`diff`, `waitForAgent`, `waitForApproval`, and the approval helpers cover the
durable Agent workflow. `connectTerminal` returns a terminal with `session`
and `nextSeq`; reuse them to reattach after a disconnect without silently
skipping retained output.

Package publication and live-provider validation remain release gates. The
repository workflow builds and tests the package without publishing it.
