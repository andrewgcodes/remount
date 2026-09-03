#!/usr/bin/env python3
"""Use a LangGraph state machine to orchestrate one durable Remount Agent."""

from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
import os
import secrets
from dataclasses import dataclass
from typing import Any, Literal, Protocol, TypedDict

from langgraph.graph import END, START, StateGraph
from langgraph.runtime import Runtime
from remount import AgentClient


class AgentAPI(Protocol):
    async def create_agent(self, request: dict[str, Any], *, idempotency_key: str | None = None) -> dict[str, Any]: ...
    async def wait_for_agent(self, agent_id: str, statuses: set[str], *, timeout: float = 120, poll_interval: float = 0.2) -> dict[str, Any]: ...
    async def message_agent(self, agent_id: str, text: str, *, kind: str = "", idempotency_key: str | None = None) -> dict[str, Any]: ...
    async def wait_for_approval(self, agent_id: str, *, timeout: float = 120, poll_interval: float = 0.2) -> dict[str, Any]: ...
    async def approve(self, approval_id: str, option: str = "allow_once", *, idempotency_key: str | None = None) -> dict[str, Any]: ...


class State(TypedDict, total=False):
    task: str
    follow_up: str
    agent_id: str
    status: str
    message_sent: bool
    approved: bool
    approval_id: str
    operation_id: str


@dataclass(frozen=True)
class Context:
    client: AgentAPI
    recipe: str = "opencode"
    workspace_name: str = "langgraph-agent"
    binding: str = ""
    timeout: float = 300


async def create_agent(state: State, runtime: Runtime[Context]) -> State:
    workspace: dict[str, Any] = {"name": runtime.context.workspace_name, "requires": {}, "placement": {}, "idle": {}}
    if runtime.context.binding:
        workspace["bindings"] = [runtime.context.binding]
    agent = await runtime.context.client.create_agent(
        {
            "workspace": workspace,
            "spec": {"recipe": runtime.context.recipe, "task": state["task"]},
            "policy": {"approve": "on-request"},
        },
        idempotency_key=_key(state["operation_id"], "create"),
    )
    return {"agent_id": agent["id"], "status": agent["status"]}


async def wait_for_agent(state: State, runtime: Runtime[Context]) -> State:
    agent = await runtime.context.client.wait_for_agent(
        state["agent_id"],
        {"waiting_input", "waiting_approval", "finished", "failed"},
        timeout=runtime.context.timeout,
    )
    return {"status": agent["status"]}


def route(state: State) -> Literal["message", "approve", "__end__"]:
    if state["status"] == "waiting_input" and state.get("follow_up") and not state.get("message_sent"):
        return "message"
    if state["status"] == "waiting_approval" and not state.get("approved"):
        return "approve"
    return END


async def message_agent(state: State, runtime: Runtime[Context]) -> State:
    result = await runtime.context.client.message_agent(
        state["agent_id"], state["follow_up"], idempotency_key=_key(state["operation_id"], "message")
    )
    return {"status": result["agent"]["status"], "message_sent": True}


async def approve_agent(state: State, runtime: Runtime[Context]) -> State:
    approval = await runtime.context.client.wait_for_approval(state["agent_id"], timeout=runtime.context.timeout)
    offered = [option["id"] for option in approval.get("options", [])]
    option = offered[0] if offered else "allow_once"
    await runtime.context.client.approve(
        approval["id"], option, idempotency_key=_key(state["operation_id"], "approve")
    )
    return {"approved": True, "approval_id": approval["id"]}


def build_graph():
    builder = StateGraph(State, context_schema=Context)
    builder.add_node("create", create_agent)
    builder.add_node("wait", wait_for_agent)
    builder.add_node("message", message_agent)
    builder.add_node("approve", approve_agent)
    builder.add_edge(START, "create")
    builder.add_edge("create", "wait")
    builder.add_conditional_edges("wait", route)
    builder.add_edge("message", "wait")
    builder.add_edge("approve", "wait")
    return builder.compile()


def _key(operation_id: str, stage: str) -> str:
    return "idem_" + hashlib.sha256(f"{operation_id}\0{stage}".encode()).hexdigest()


async def _main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("task")
    parser.add_argument("--follow-up", default="")
    parser.add_argument("--recipe", default="opencode")
    parser.add_argument("--name", default="langgraph-agent")
    parser.add_argument("--binding", default="")
    parser.add_argument("--server", default=os.environ.get("REMOUNT_SERVER", "http://127.0.0.1:7443"))
    parser.add_argument("--operation-id", default="", help="stable caller id used to deduplicate graph retries")
    args = parser.parse_args()
    async with AgentClient(args.server, os.environ.get("REMOUNT_TOKEN", "")) as client:
        result = await build_graph().ainvoke(
            {"task": args.task, "follow_up": args.follow_up, "operation_id": args.operation_id or secrets.token_hex(16)},
            context=Context(client=client, recipe=args.recipe, workspace_name=args.name, binding=args.binding),
        )
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    asyncio.run(_main())
