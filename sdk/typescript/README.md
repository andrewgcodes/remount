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

The protocol `Client` negotiates the current capabilities with strict-profile
servers and fences frames using exact uint64 controller epochs. Session output
is accepted only from the grant-authorized node, including early buffered
chunks; replay gaps remain explicit. Revocation, session capabilities and
release ordering remain server/node responsibilities. Artifact helpers transfer
digest-verified `art_sha256` byte objects, not reconstructed chunked manifests.
Downloads verify nonempty response bytes before returning them.

See [the cross-language protocol gate](../../integration/sdks/README.md) for
local verification. Passing it is not production backend-isolation evidence.
