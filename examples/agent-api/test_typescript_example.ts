import assert from "node:assert/strict";
import test from "node:test";
import type { Agent, AgentCreateReq, AgentMessageRes, Approval } from "@remount/sdk";
import { createMessageAndApprove, type AgentAPI } from "./typescript_example.js";

function agent(status: string): Agent {
  return { id: "ag_1", tenant: "t_1", owner: "user:1", ws: "ws_1", spec: { recipe: "opencode", task: "task" }, mode: "acp", status, inbox: [], runs: [], turns: 0, policy: {}, created_at: 1, updated_at: 1 };
}

test("creates, waits for input, messages, waits for approval, and approves", async () => {
  const calls: string[] = [];
  let created: AgentCreateReq | undefined;
  const client: AgentAPI = {
    async createAgent(request) { calls.push("create"); created = request; return agent("creating"); },
    async waitForAgent(_id, statuses) { calls.push("wait"); assert.deepEqual([...statuses], ["waiting_input"]); return agent("waiting_input"); },
    async messageAgent(_id, text) { calls.push("message"); return { agent: agent("running"), message: { id: "m_1", kind: "follow_up", text, at: 1 } } as AgentMessageRes; },
    async waitForApproval() { calls.push("wait_approval"); return { id: "ap_1", tenant: "t_1", owner: "user:1", kind: "tool_call", status: "pending", options: [{ id: "allow_once" }], created_at: 1, updated_at: 1 } as Approval; },
    async approve(id, option) { calls.push("approve"); return { id, tenant: "t_1", owner: "user:1", kind: "tool_call", status: "approved", decision: { option }, created_at: 1, updated_at: 2 } as Approval; },
  };
  const result = await createMessageAndApprove(client, { recipe: "opencode", task: "ask", reply: "continue", workspaceName: "example", binding: "b_openai" });
  assert.deepEqual(calls, ["create", "wait", "message", "wait_approval", "approve"]);
  assert.deepEqual(created?.workspace?.bindings, ["b_openai"]);
  assert.equal(result.approval.status, "approved");
});
