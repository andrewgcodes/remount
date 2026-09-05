import assert from "node:assert/strict";
import { Client } from "../../sdk/typescript/dist/index.js";

const [url, profile] = process.argv.slice(2);
const client = new Client(url, "sdk-synthetic-token");
try {
  // Concurrent cold calls must all wait for hello/epoch negotiation.
  await Promise.all(Array.from({ length: 8 }, () => client.call("ws.list")));
  const workspace = await client.createWorkspace({ name: "typescript-protocol-test" });
  try {
    assert.equal(workspace.spec.security.profile, profile);
    assert.equal((await client.getWorkspace(workspace.id)).id, workspace.id);
  } finally {
    await client.destroyWorkspace(workspace.id);
  }
} finally {
  await client.close();
}
console.log(`TypeScript strict-profile protocol passed: ${profile}`);
