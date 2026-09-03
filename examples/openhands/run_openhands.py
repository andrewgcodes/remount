#!/usr/bin/env python3
"""Launch the built-in OpenHands recipe as a durable Remount Agent."""

from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
import os
import secrets
from typing import Any, Protocol

from remount import AgentClient


class AgentAPI(Protocol):
    async def create_agent(self, request: dict[str, Any], *, idempotency_key: str | None = None) -> dict[str, Any]: ...
    async def wait_for_agent(self, agent_id: str, statuses: set[str], *, timeout: float = 120, poll_interval: float = 0.2) -> dict[str, Any]: ...
    async def message_agent(self, agent_id: str, text: str, *, kind: str = "", idempotency_key: str | None = None) -> dict[str, Any]: ...
    async def transcript(self, agent_id: str, *, from_index: int = 0, limit: int = 0) -> dict[str, Any]: ...


def _key(operation_id: str, stage: str) -> str:
    return "idem_" + hashlib.sha256(f"{operation_id}\0{stage}".encode()).hexdigest()


async def run_openhands(
    client: AgentAPI,
    task: str,
    *,
    workspace_name: str = "openhands-agent",
    binding: str = "",
    follow_up: str = "",
    operation_id: str = "",
    timeout: float = 600,
) -> dict[str, Any]:
    workspace: dict[str, Any] = {
        "name": workspace_name,
        "requires": {},
        "placement": {},
        "idle": {},
    }
    if binding:
        workspace["bindings"] = [binding]
    operation_id = operation_id or secrets.token_hex(16)
    agent = await client.create_agent(
        {
            "workspace": workspace,
            "spec": {"recipe": "openhands", "task": task},
            "policy": {"approve": "on-request"},
        },
        idempotency_key=_key(operation_id, "create"),
    )
    stop = {"waiting_input", "waiting_approval", "finished", "failed"}
    agent = await client.wait_for_agent(agent["id"], stop, timeout=timeout)
    if agent["status"] == "waiting_input" and follow_up:
        await client.message_agent(agent["id"], follow_up, idempotency_key=_key(operation_id, "message"))
        agent = await client.wait_for_agent(agent["id"], stop, timeout=timeout)
    page = await client.transcript(agent["id"], from_index=0, limit=1000)
    return {"agent": agent, "transcript": page}


async def _main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("task")
    parser.add_argument("--follow-up", default="")
    parser.add_argument("--name", default="openhands-agent")
    parser.add_argument("--binding", default="")
    parser.add_argument("--server", default=os.environ.get("REMOUNT_SERVER", "http://127.0.0.1:7443"))
    parser.add_argument("--operation-id", default="", help="stable caller id used to deduplicate an orchestrator retry")
    args = parser.parse_args()
    async with AgentClient(args.server, os.environ.get("REMOUNT_TOKEN", "")) as client:
        result = await run_openhands(
            client,
            args.task,
            workspace_name=args.name,
            binding=args.binding,
            follow_up=args.follow_up,
            operation_id=args.operation_id,
        )
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    asyncio.run(_main())
