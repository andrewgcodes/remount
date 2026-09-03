"""E12 live smoke: run with REMOUNT_E12_URL and REMOUNT_E12_TOKEN."""

import asyncio
import os
import secrets

from remount import AgentClient


async def main() -> None:
    url = os.environ["REMOUNT_E12_URL"]
    token = os.environ["REMOUNT_E12_TOKEN"]
    recipe = os.getenv("REMOUNT_E12_RECIPE", "opencode")
    task = os.getenv("REMOUNT_E12_TASK", "Reply with the word ready, then wait for input.")
    run_id = os.getenv("REMOUNT_E12_RUN_ID", secrets.token_hex(8))
    async with AgentClient(url, token) as client:
        agent = await client.create_agent(
            {"name": "e12-python", "spec": {"recipe": recipe, "task": task}, "policy": {}},
            idempotency_key=f"e12-python-{run_id}-create",
        )
        try:
            await client.message_agent(agent["id"], "continue", idempotency_key=f"e12-python-{run_id}-message")
            async with asyncio.timeout(120):
                record = await anext(client.stream_transcript(agent["id"]))
                print(record)
        finally:
            await client.agent_action(agent["id"], "destroy", idempotency_key=f"e12-python-{run_id}-destroy")


asyncio.run(main())
