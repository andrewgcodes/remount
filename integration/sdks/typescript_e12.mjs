// E12 live smoke: run after npm build with REMOUNT_E12_URL and TOKEN.
import { AgentClient } from "../../sdk/typescript/dist/index.js";

const url = process.env.REMOUNT_E12_URL;
const token = process.env.REMOUNT_E12_TOKEN;
if (!url || !token) throw new Error("REMOUNT_E12_URL and REMOUNT_E12_TOKEN are required");
const client = new AgentClient(url, token);
const runID = process.env.REMOUNT_E12_RUN_ID ?? crypto.randomUUID();
const agent = await client.createAgent({
  name: "e12-typescript",
  spec: {
    recipe: process.env.REMOUNT_E12_RECIPE ?? "opencode",
    task: process.env.REMOUNT_E12_TASK ?? "Reply with the word ready, then wait for input.",
  },
  policy: {},
}, `e12-typescript-${runID}-create`);
try {
  await client.messageAgent(agent.id, "continue", { idempotencyKey: `e12-typescript-${runID}-message` });
  const iterator = client.streamTranscript(agent.id);
  const first = await Promise.race([
    iterator.next(),
    new Promise((_, reject) => setTimeout(() => reject(new Error("E12 transcript deadline exceeded")), 120_000)),
  ]);
  if (first.done) throw new Error("E12 transcript ended without a record");
  console.log(JSON.stringify(first.value));
  await iterator.return();
} finally {
  await client.agentAction(agent.id, "destroy", { idempotencyKey: `e12-typescript-${runID}-destroy` });
}
