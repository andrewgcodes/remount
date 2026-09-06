import asyncio
import json

import cbor2
import pytest

from remount import Client, Computer
from remount.computer import MODIFIER_SHIFT


class ComputerSocket:
    """A fake node that answers the computer operations and records requests."""

    def __init__(self, responses=None):
        self.incoming = asyncio.Queue()
        self.closed = False
        self.calls = []
        self.responses = responses or {}

    async def send(self, wire):
        frame = cbor2.loads(wire)
        if frame["t"] == "hello":
            body = {"peer": "client_1", "caps": ["v1"], "controller_epoch": 0}
        elif frame["op"] == "grant":
            body = {"node": "node_1", "claims": {}}
        else:
            request = cbor2.loads(frame["body"]) if frame.get("body") else {}
            self.calls.append((frame["op"], request))
            configured = self.responses.get(frame["op"], {})
            body = configured(request) if callable(configured) else configured
        await self.incoming.put(
            cbor2.dumps(
                {
                    "v": 1,
                    "t": "res",
                    "controller_epoch": 0,
                    "id": frame["id"],
                    "op": frame.get("op", ""),
                    "from": frame.get("to", ""),
                    "body": cbor2.dumps(body),
                }
            )
        )

    async def close(self):
        if not self.closed:
            self.closed = True
            await self.incoming.put(None)

    def __aiter__(self):
        return self

    async def __anext__(self):
        value = await self.incoming.get()
        if value is None:
            raise StopAsyncIteration
        return value


def client_for(socket) -> Client:
    async def connector(*_args, **_kwargs):
        return socket

    return Client("https://cp.example", "token", connector=connector)


@pytest.mark.asyncio
async def test_create_carries_the_launch_and_returns_the_handle():
    socket = ComputerSocket(
        {
            "computer.create": {
                "computer": "cmp_1",
                "s": "s_1",
                "cdp": "Chrome/152.0.0.0",
                "viewport": {"w": 1024, "h": 768},
            }
        }
    )
    client = client_for(socket)
    computer = await client.create_computer(
        "ws_1",
        viewport={"w": 1024, "h": 768},
        profile="research",
        launch={"port": 9333},
    )
    assert isinstance(computer, Computer)
    assert (computer.id, computer.session, computer.cdp_version) == ("cmp_1", "s_1", "Chrome/152.0.0.0")
    assert computer.viewport == {"w": 1024, "h": 768}
    op, request = socket.calls[0]
    assert op == "computer.create"
    assert request["ws"] == "ws_1"
    assert request["profile"] == "research"
    assert request["launch"] == {"port": 9333}
    assert request["idem"].startswith("idem_")
    await client.close()


@pytest.mark.asyncio
async def test_every_action_batch_takes_the_next_input_sequence():
    socket = ComputerSocket({"computer.input": {"applied": True, "last_iseq": 0}})
    client = client_for(socket)
    computer = client.computer("ws_1", "cmp_1")
    await computer.click(10, 20)
    await computer.type("hello")
    await computer.key("Enter", modifiers=MODIFIER_SHIFT)
    await computer.scroll(1, 2, 0, 120)
    await computer.drag(1, 2, 3, 4)
    sequences = [request["iseq"] for op, request in socket.calls if op == "computer.input"]
    assert sequences == [1, 2, 3, 4, 5]
    actions = [request["actions"][0] for op, request in socket.calls if op == "computer.input"]
    assert actions[0] == {"kind": "click", "x": 10, "y": 20, "mod": 0}
    assert actions[1] == {"kind": "type", "text": "hello"}
    assert actions[2] == {"kind": "key", "key": "Enter", "mod": MODIFIER_SHIFT}
    assert actions[3] == {"kind": "scroll", "x": 1, "y": 2, "dx": 0, "dy": 120}
    assert actions[4] == {"kind": "drag", "x": 1, "y": 2, "tox": 3, "toy": 4}
    for _, request in socket.calls:
        assert request["computer"] == "cmp_1"
        assert request["ws"] == "ws_1"
    await client.close()


@pytest.mark.asyncio
async def test_eval_decodes_the_page_value_and_never_executes_it():
    socket = ComputerSocket(
        {"computer.eval": {"value": json.dumps({"title": "Remount"}).encode()}}
    )
    client = client_for(socket)
    computer = client.computer("ws_1", "cmp_1")
    assert await computer.eval("document.title") == {"title": "Remount"}
    assert socket.calls[0][1]["expr"] == "document.title"
    await client.close()


@pytest.mark.asyncio
async def test_eval_of_an_undefined_expression_is_none():
    socket = ComputerSocket({"computer.eval": {}})
    client = client_for(socket)
    computer = client.computer("ws_1", "cmp_1")
    assert await computer.eval("window.missing") is None
    await client.close()


@pytest.mark.asyncio
async def test_a_blocked_download_is_listed_with_its_reason():
    socket = ComputerSocket(
        {
            "computer.downloads": {
                "downloads": [
                    {"filename": "report.csv", "state": "completed", "artifact": "art_sha256:" + "0" * 64},
                    {"filename": "big.bin", "state": "blocked", "reason": "download_blocked"},
                ]
            }
        }
    )
    client = client_for(socket)
    downloads = await client.computer("ws_1", "cmp_1").downloads()
    assert [d["state"] for d in downloads] == ["completed", "blocked"]
    assert downloads[1]["reason"] == "download_blocked"
    await client.close()


@pytest.mark.asyncio
async def test_get_refreshes_the_handle_and_reports_why_it_died():
    socket = ComputerSocket(
        {
            "computer.get": {
                "computer": "cmp_1",
                "state": "closed",
                "reason": "browser_crashed",
                "viewport": {"w": 800, "h": 600},
                "s": "s_9",
            }
        }
    )
    client = client_for(socket)
    computer = client.computer("ws_1", "cmp_1")
    state = await computer.get()
    assert (state["state"], state["reason"]) == ("closed", "browser_crashed")
    assert computer.viewport == {"w": 800, "h": 600}
    assert computer.session == "s_9"
    await client.close()


@pytest.mark.asyncio
async def test_a_handle_addressed_by_id_resumes_the_nodes_input_sequence():
    # This is the CLI's shape: every invocation builds a fresh handle. A
    # sequence restarted at one is one the node has already applied, so the
    # action would be dropped and nothing would happen.
    socket = ComputerSocket(
        {
            "computer.get": {"computer": "cmp_1", "state": "ready", "last_iseq": 7},
            "computer.input": {"applied": True, "last_iseq": 8},
        }
    )
    client = client_for(socket)
    computer = client.computer("ws_1", "cmp_1")
    await computer.click(1, 2)
    await computer.type("x")
    assert [op for op, _ in socket.calls] == ["computer.get", "computer.input", "computer.input"]
    assert [request["iseq"] for op, request in socket.calls if op == "computer.input"] == [8, 9]
    await client.close()


@pytest.mark.asyncio
async def test_a_created_handle_does_not_pay_for_a_sync():
    socket = ComputerSocket(
        {
            "computer.create": {"computer": "cmp_1"},
            "computer.input": {"applied": True, "last_iseq": 1},
        }
    )
    client = client_for(socket)
    computer = await client.create_computer("ws_1")
    await computer.click(1, 2)
    assert [op for op, _ in socket.calls] == ["computer.create", "computer.input"]
    await client.close()


@pytest.mark.asyncio
async def test_navigate_and_close_carry_an_idempotency_key():
    socket = ComputerSocket(
        {
            "computer.navigate": {"url": "https://example.test/", "status": "loaded", "title": "t"},
            "computer.close": {},
        }
    )
    client = client_for(socket)
    computer = client.computer("ws_1", "cmp_1")
    result = await computer.navigate("https://example.test/")
    assert result["status"] == "loaded"
    await computer.close(idempotency_key="close-once")
    keys = {op: request.get("idem") for op, request in socket.calls}
    assert keys["computer.navigate"].startswith("idem_")
    assert keys["computer.close"] == "close-once"
    await client.close()
