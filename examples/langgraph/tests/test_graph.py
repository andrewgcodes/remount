from __future__ import annotations

import unittest
from typing import Any

from graph import Context, build_graph


class FakeClient:
    def __init__(self) -> None:
        self.calls: list[str] = []
        self.statuses = iter(["waiting_input", "waiting_approval", "finished"])

    async def create_agent(self, request: dict[str, Any], *, idempotency_key: str | None = None) -> dict[str, Any]:
        self.calls.append("create")
        return {"id": "ag_1", "status": "creating"}

    async def wait_for_agent(self, agent_id: str, statuses: set[str], *, timeout: float = 120, poll_interval: float = 0.2) -> dict[str, Any]:
        self.calls.append("wait")
        return {"id": agent_id, "status": next(self.statuses)}

    async def message_agent(self, agent_id: str, text: str, *, kind: str = "", idempotency_key: str | None = None) -> dict[str, Any]:
        self.calls.append("message")
        return {"agent": {"id": agent_id, "status": "running"}, "message": {"text": text}}

    async def wait_for_approval(self, agent_id: str, *, timeout: float = 120, poll_interval: float = 0.2) -> dict[str, Any]:
        self.calls.append("wait_approval")
        return {"id": "ap_1", "options": [{"id": "allow_once"}]}

    async def approve(self, approval_id: str, option: str = "allow_once", *, idempotency_key: str | None = None) -> dict[str, Any]:
        self.calls.append("approve")
        return {"id": approval_id, "status": "approved"}


class LangGraphExampleTest(unittest.IsolatedAsyncioTestCase):
    async def test_graph_drives_message_and_approval_branches(self) -> None:
        client = FakeClient()
        result = await build_graph().ainvoke(
            {"task": "ask then edit", "follow_up": "edit the test", "operation_id": "graph-run-1"},
            context=Context(client=client, binding="b_openai"),
        )
        self.assertEqual(
            client.calls,
            ["create", "wait", "message", "wait", "wait_approval", "approve", "wait"],
        )
        self.assertEqual(result["status"], "finished")
        self.assertTrue(result["message_sent"])
        self.assertTrue(result["approved"])


if __name__ == "__main__":
    unittest.main()
