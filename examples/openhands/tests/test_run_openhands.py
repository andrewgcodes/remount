from __future__ import annotations

import unittest
from typing import Any

from run_openhands import run_openhands


class FakeClient:
    def __init__(self) -> None:
        self.calls: list[str] = []
        self.statuses = iter(["waiting_input", "finished"])

    async def create_agent(self, request: dict[str, Any], *, idempotency_key: str | None = None) -> dict[str, Any]:
        self.calls.append("create:" + request["spec"]["recipe"])
        return {"id": "ag_1", "status": "creating"}

    async def wait_for_agent(self, agent_id: str, statuses: set[str], *, timeout: float = 120, poll_interval: float = 0.2) -> dict[str, Any]:
        self.calls.append("wait")
        return {"id": agent_id, "status": next(self.statuses)}

    async def message_agent(self, agent_id: str, text: str, *, kind: str = "", idempotency_key: str | None = None) -> dict[str, Any]:
        self.calls.append("message:" + text)
        return {"agent": {"id": agent_id, "status": "running"}}

    async def transcript(self, agent_id: str, *, from_index: int = 0, limit: int = 0) -> dict[str, Any]:
        self.calls.append("transcript")
        return {"records": [{"index": 0, "stream": "info"}], "next": 1, "done": True}


class OpenHandsExampleTest(unittest.IsolatedAsyncioTestCase):
    async def test_uses_openhands_recipe_and_durable_follow_up(self) -> None:
        client = FakeClient()
        result = await run_openhands(client, "inspect the repo", follow_up="continue")
        self.assertEqual(client.calls, ["create:openhands", "wait", "message:continue", "wait", "transcript"])
        self.assertEqual(result["agent"]["status"], "finished")
        self.assertTrue(result["transcript"]["done"])


if __name__ == "__main__":
    unittest.main()
