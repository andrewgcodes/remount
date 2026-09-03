import json

import httpx
import pytest

from remount import AgentClient, ProtocolError


@pytest.mark.asyncio
async def test_mutations_are_authenticated_and_idempotent():
    seen = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(request)
        return httpx.Response(201, json={"id": "ag_1"})

    transport = httpx.MockTransport(handler)
    async with httpx.AsyncClient(transport=transport) as http:
        client = AgentClient("https://cp.example", "secret", client=http)
        result = await client.create_agent({"spec": {"recipe": "custom"}}, idempotency_key="stable")
    assert result["id"] == "ag_1"
    assert seen[0].headers["authorization"] == "Bearer secret"
    assert seen[0].headers["idempotency-key"] == "stable"
    assert json.loads(seen[0].content)["spec"]["recipe"] == "custom"


@pytest.mark.asyncio
async def test_transcript_gap_is_not_reported_as_complete():
    def handler(_: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json={"records": [], "next": 4, "gap": {"from": 0, "to": 4}})

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as http:
        client = AgentClient("https://cp.example", "token", client=http)
        with pytest.raises(ProtocolError, match="evicted"):
            await anext(client.stream_transcript("ag_1"))


@pytest.mark.asyncio
async def test_http_error_redacts_credential_canary():
    canary = "sdk-secret-canary"

    def handler(_: httpx.Request) -> httpx.Response:
        return httpx.Response(403, json={"error": {"code": "denied", "message": "failed " + canary}})

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as http:
        client = AgentClient("https://cp.example", canary, client=http)
        with pytest.raises(ProtocolError) as caught:
            await client.get_agent("ag_1")
    assert canary not in str(caught.value)
    assert caught.value.code == "denied"


@pytest.mark.asyncio
async def test_http_response_is_bounded():
    def handler(_: httpx.Request) -> httpx.Response:
        return httpx.Response(200, content=b'12345')

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as http:
        client = AgentClient("https://cp.example", "token", client=http, max_response_bytes=4)
        with pytest.raises(ProtocolError, match="configured limit") as caught:
            await client.get_agent("ag_1")
    assert caught.value.code == "resource_exhausted"


@pytest.mark.asyncio
async def test_http_retry_reuses_idempotency_key():
    keys = []

    def handler(request: httpx.Request) -> httpx.Response:
        keys.append(request.headers["idempotency-key"])
        if len(keys) == 1:
            raise httpx.ConnectError("cut", request=request)
        return httpx.Response(201, json={"id": "ag_1"})

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as http:
        client = AgentClient("https://cp.example", "token", client=http)
        await client.create_agent({"spec": {"recipe": "custom"}})
    assert len(keys) == 2 and keys[0] == keys[1]


@pytest.mark.asyncio
async def test_diff_is_read_only_unless_wake_is_explicit():
    paths = []

    def handler(request: httpx.Request) -> httpx.Response:
        paths.append(str(request.url))
        return httpx.Response(200, json={"status": " M file", "diff": "patch", "truncated": False})

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as http:
        client = AgentClient("https://cp.example", "token", client=http)
        assert (await client.diff("ag_1"))["diff"] == "patch"
        await client.diff("ag_1", wake=True)
    assert paths[0].endswith("/v1/agents/ag_1/diff")
    assert paths[1].endswith("/v1/agents/ag_1/diff?wake=true")


@pytest.mark.asyncio
async def test_approval_convenience_uses_durable_decision_route():
    bodies = []

    def handler(request: httpx.Request) -> httpx.Response:
        bodies.append(json.loads(request.content))
        return httpx.Response(200, json={"id": "ap_1", "status": "decided"})

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as http:
        client = AgentClient("https://cp.example", "token", client=http)
        await client.approve("ap_1", "allow_once", idempotency_key="approve-once")
        await client.deny("ap_2", idempotency_key="deny-once")
    assert bodies == [{"option": "allow_once"}, {"denied": True}]


class FakeTerminal:
    def __init__(self):
        self.incoming = __import__("asyncio").Queue()
        self.sent = []
        self.closed = False

    async def recv(self):
        return await self.incoming.get()

    async def send(self, value):
        self.sent.append(value)

    async def close(self):
        self.closed = True


@pytest.mark.asyncio
async def test_terminal_attach_tracks_replay_cursor_and_controls():
    socket = FakeTerminal()
    seen = {}

    async def connector(url, **kwargs):
        seen.update(url=url, kwargs=kwargs)
        return socket

    client = AgentClient("https://cp.example", "secret", terminal_connector=connector)
    terminal = await client.connect_terminal("ag_1", session="s_1", from_seq=4)
    await socket.incoming.put('{"type":"open","session":"s_1","next":4}')
    await socket.incoming.put(b"output")
    await socket.incoming.put('{"type":"gap","from":5,"to":7}')
    await socket.incoming.put('{"type":"exit","code":0}')
    assert (await anext(terminal))["type"] == "open"
    assert (await anext(terminal)) == {"type": "data", "data": b"output"}
    assert (await anext(terminal))["type"] == "gap"
    assert (await anext(terminal))["type"] == "exit"
    assert terminal.next_seq == 9
    await terminal.resize(30, 100)
    await terminal.input(b"x")
    assert json.loads(socket.sent[0]) == {"type": "resize", "rows": 30, "cols": 100}
    assert socket.sent[1] == b"x"
    assert seen["url"] == "wss://cp.example/v1/agents/ag_1/terminal?session=s_1&from=4"
    assert seen["kwargs"]["additional_headers"]["Authorization"] == "Bearer secret"
    await terminal.close()
    assert socket.closed


@pytest.mark.asyncio
async def test_terminal_refuses_output_before_open():
    socket = FakeTerminal()
    await socket.incoming.put(b"out of order")
    from remount import Terminal

    terminal = Terminal(socket, timeout=1, max_message_bytes=1024)
    with pytest.raises(ProtocolError, match="preceded open"):
        await anext(terminal)
    await terminal.close()


@pytest.mark.asyncio
async def test_wait_for_approval_has_an_explicit_deadline():
    def handler(_: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json={"approvals": []})

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as http:
        client = AgentClient("https://cp.example", "token", client=http)
        with pytest.raises(ProtocolError) as caught:
            await client.wait_for_approval("ag_1", timeout=0.001, poll_interval=0.01)
    assert caught.value.code == "timeout"


@pytest.mark.asyncio
async def test_transcript_stream_resumes_same_cursor_after_disconnect():
    calls = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        if calls == 1:
            raise httpx.ConnectError("cut", request=request)
        return httpx.Response(200, json={"records": [{"index": 4}], "next": 5, "done": True})

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as http:
        client = AgentClient("https://cp.example", "token", client=http, retries=1)
        record = await anext(client.stream_transcript("ag_1", from_index=4, poll_interval=0))
    assert record["index"] == 4 and calls == 2
