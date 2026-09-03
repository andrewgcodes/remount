#!/usr/bin/env python3
"""Create, message, wait, and approve a Remount Agent using the generated SDK."""

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
    async def wait_for_approval(self, agent_id: str, *, timeout: float = 120, poll_interval: float = 0.2) -> dict[str, Any]: ...
    async def approve(self, approval_id: str, option: str = "allow_once", *, idempotency_key: str | None = None) -> dict[str, Any]: ...


def _key(operation_id: str, stage: str) -> str:
    return "idem_" + hashlib.sha256(f"{operation_id}\0{stage}".encode()).hexdigest()


async def create_message_and_approve(
    client: AgentAPI,
    *,
    recipe: str,
    task: str,
    reply: str,
    workspace_name: str,
    binding: str = "",
    approval_option: str = "",
    operation_id: str = "",
    timeout: float = 300,
) -> dict[str, Any]:
    """Exercise the Agent API's durable input and approval path."""
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
            "spec": {"recipe": recipe, "task": task},
            "policy": {"approve": "on-request"},
        },
        idempotency_key=_key(operation_id, "create"),
    )
    agent = await client.wait_for_agent(agent["id"], {"waiting_input"}, timeout=timeout)
    message = await client.message_agent(agent["id"], reply, idempotency_key=_key(operation_id, "message"))
    approval = await client.wait_for_approval(agent["id"], timeout=timeout)

    offered = [option["id"] for option in approval.get("options", [])]
    option = approval_option or (offered[0] if offered else "allow_once")
    if offered and option not in offered:
        raise ValueError(f"approval option {option!r} is not offered: {offered}")
    decision = await client.approve(approval["id"], option, idempotency_key=_key(operation_id, "approve"))
    return {"agent": message["agent"], "approval": decision}


async def _main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--server", default=os.environ.get("REMOUNT_SERVER", "http://127.0.0.1:7443"))
    parser.add_argument("--recipe", default="opencode")
    parser.add_argument("--name", default="agent-api-example")
    parser.add_argument("--binding", default="")
    parser.add_argument("--option", default="", help="approval option id; defaults to the first offered option")
    parser.add_argument("--operation-id", default="", help="stable caller id used to deduplicate an orchestrator retry")
    parser.add_argument("--timeout", type=float, default=300)
    parser.add_argument("--task", required=True, help="task that first reaches waiting_input, then requests a protected tool")
    parser.add_argument("--reply", required=True)
    args = parser.parse_args()
    async with AgentClient(args.server, os.environ.get("REMOUNT_TOKEN", "")) as client:
        result = await create_message_and_approve(
            client,
            recipe=args.recipe,
            task=args.task,
            reply=args.reply,
            workspace_name=args.name,
            binding=args.binding,
            approval_option=args.option,
            operation_id=args.operation_id,
            timeout=args.timeout,
        )
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    asyncio.run(_main())
