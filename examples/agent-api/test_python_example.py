from __future__ import annotations

import unittest
from typing import Any

from python_example import create_message_and_approve


class FakeClient:
    def __init__(self) -> None:
        self.calls: list[tuple[Any, ...]] = []

    async def create_agent(self, request: dict[str, Any], *, idempotency_key: str | None = None) -> dict[str, Any]:
        self.calls.append(("create", request))
        return {"id": "ag_1", "status": "creating"}

    async def wait_for_agent(self, agent_id: str, statuses: set[str], *, timeout: float = 120, poll_interval: float = 0.2) -> dict[str, Any]:
        self.calls.append(("wait", agent_id, statuses))
        return {"id": agent_id, "status": "waiting_input"}

    async def message_agent(self, agent_id: str, text: str, *, kind: str = "", idempotency_key: str | None = None) -> dict[str, Any]:
        self.calls.append(("message", agent_id, text))
        return {"agent": {"id": agent_id, "status": "running"}, "message": {"text": text}}

    async def wait_for_approval(self, agent_id: str, *, timeout: float = 120, poll_interval: float = 0.2) -> dict[str, Any]:
        self.calls.append(("wait_approval", agent_id))
        return {"id": "ap_1", "status": "pending", "options": [{"id": "allow_once"}, {"id": "deny"}]}

    async def approve(self, approval_id: str, option: str = "allow_once", *, idempotency_key: str | None = None) -> dict[str, Any]:
        self.calls.append(("approve", approval_id, option))
        return {"id": approval_id, "status": "approved", "decision": {"option": option}}


class AgentAPIExampleTest(unittest.IsolatedAsyncioTestCase):
    async def test_create_message_wait_and_approve(self) -> None:
        client = FakeClient()
        result = await create_message_and_approve(
            client,
            recipe="opencode",
            task="ask for input",
            reply="continue",
            workspace_name="example",
            binding="b_openai",
        )
        self.assertEqual([call[0] for call in client.calls], ["create", "wait", "message", "wait_approval", "approve"])
        create = client.calls[0][1]
        self.assertEqual(create["policy"], {"approve": "on-request"})
        self.assertEqual(create["workspace"]["bindings"], ["b_openai"])
        self.assertEqual(result["approval"]["status"], "approved")

    async def test_rejects_an_unoffered_option(self) -> None:
        client = FakeClient()
        with self.assertRaisesRegex(ValueError, "not offered"):
            await create_message_and_approve(
                client,
                recipe="opencode",
                task="ask",
                reply="continue",
                workspace_name="example",
                approval_option="always",
            )
        self.assertNotIn("approve", [call[0] for call in client.calls])


if __name__ == "__main__":
    unittest.main()
