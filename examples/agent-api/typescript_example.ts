#!/usr/bin/env node
import { pathToFileURL } from "node:url";
import { AgentClient } from "@remount/sdk";
import type { Agent, AgentCreateReq, AgentMessageRes, Approval } from "@remount/sdk";

export interface AgentAPI {
  createAgent(request: AgentCreateReq, idempotencyKey?: string): Promise<Agent>;
  waitForAgent(id: string, statuses: ReadonlySet<string>, options?: { timeoutMilliseconds?: number; pollMilliseconds?: number }): Promise<Agent>;
  messageAgent(id: string, text: string, options?: { kind?: string; idempotencyKey?: string }): Promise<AgentMessageRes>;
  waitForApproval(agentID: string, options?: { timeoutMilliseconds?: number; pollMilliseconds?: number }): Promise<Approval>;
  approve(id: string, option?: string, idempotencyKey?: string): Promise<Approval>;
}

export async function createMessageAndApprove(
  client: AgentAPI,
  options: {
    recipe: string;
    task: string;
    reply: string;
    workspaceName: string;
    binding?: string;
    approvalOption?: string;
    operationID?: string;
    timeoutMilliseconds?: number;
  },
): Promise<{ agent: Agent; approval: Approval }> {
  const workspace: AgentCreateReq["workspace"] = {
    name: options.workspaceName,
    requires: {},
    placement: {},
    idle: {},
  };
  if (options.binding) workspace!.bindings = [options.binding];
  const operationID = options.operationID ?? globalThis.crypto.randomUUID();
  let agent = await client.createAgent({
    workspace,
    spec: { recipe: options.recipe, task: options.task },
    policy: { approve: "on-request" },
  }, await stableKey(operationID, "create"));
  const wait = { timeoutMilliseconds: options.timeoutMilliseconds ?? 300_000 };
  agent = await client.waitForAgent(agent.id, new Set(["waiting_input"]), wait);
  const message = await client.messageAgent(agent.id, options.reply, { idempotencyKey: await stableKey(operationID, "message") });
  const approval = await client.waitForApproval(agent.id, wait);
  const offered = (approval.options ?? []).map((item) => item.id);
  const option = options.approvalOption ?? offered[0] ?? "allow_once";
  if (offered.length && !offered.includes(option)) throw new Error(`approval option ${JSON.stringify(option)} is not offered: ${offered.join(", ")}`);
  const decision = await client.approve(approval.id, option, await stableKey(operationID, "approve"));
  return { agent: message.agent, approval: decision };
}

async function main(): Promise<void> {
  const [task, reply] = process.argv.slice(2);
  if (!task || !reply) throw new Error("usage: typescript_example.ts TASK REPLY");
  const server = process.env.REMOUNT_SERVER ?? "http://127.0.0.1:7443";
  const client = new AgentClient(server, process.env.REMOUNT_TOKEN ?? "");
  const result = await createMessageAndApprove(client, {
    recipe: process.env.REMOUNT_RECIPE ?? "opencode",
    task,
    reply,
    workspaceName: process.env.REMOUNT_WORKSPACE_NAME ?? "agent-api-example",
    binding: process.env.REMOUNT_BINDING,
    operationID: process.env.REMOUNT_OPERATION_ID,
  });
  process.stdout.write(JSON.stringify(result, null, 2) + "\n");
}

if (process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href) {
  main().catch((error) => { console.error(error instanceof Error ? error.message : error); process.exitCode = 1; });
}

async function stableKey(operationID: string, stage: string): Promise<string> {
  const digest = await globalThis.crypto.subtle.digest("SHA-256", new TextEncoder().encode(`${operationID}\0${stage}`));
  return "idem_" + [...new Uint8Array(digest)].map((value) => value.toString(16).padStart(2, "0")).join("");
}
