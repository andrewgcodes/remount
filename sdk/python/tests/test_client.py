import hashlib
from pathlib import Path

import cbor2
import httpx
import pytest

from remount import Client, ProtocolError
from remount.client import Session


def test_decodes_go_protocol_golden_fixture():
    wire = bytes.fromhex((Path(__file__).parents[3] / "internal/proto/testdata/v1-request.hex").read_text().strip())
    frame = cbor2.loads(wire)
    assert (frame["v"], frame["t"], frame["id"], frame["op"]) == (1, "req", 7, "ws.get")
    assert cbor2.loads(frame["body"])["id"] == "ws_fixture"
    body = cbor2.dumps({"id": "ws_fixture"}, canonical=True)
    assert cbor2.dumps({"v": 1, "t": "req", "id": 7, "to": "control", "op": "ws.get", "body": body}, canonical=True) == wire


class FakeSocket:
    def __init__(self):
        self.incoming = __import__("asyncio").Queue()
        self.closed = False
        self.sent = []
        self.attached = __import__("asyncio").Event()

    async def send(self, wire):
        frame = cbor2.loads(wire)
        self.sent.append(frame)
        if frame["t"] == "hello":
            body = {"peer": "client_1", "caps": ["v1"]}
        elif frame["op"] == "grant":
            body = {"node": "node_1", "claims": {}}
        elif frame["op"] == "s.open":
            await self.incoming.put(cbor2.dumps({"v": 1, "t": "chunk", "s": "s_1", "seq": 0, "body": cbor2.dumps({"st": 4, "d": b"info"})}))
            body = {"s": "s_1", "next": 1}
        elif frame["op"] == "s.attach":
            self.attached.set()
            body = {"s": "s_1", "next": 4}
        else:
            body = {}
        await self.incoming.put(cbor2.dumps({"v": 1, "t": "res", "id": frame["id"], "op": frame.get("op", ""), "from": frame.get("to", ""), "body": cbor2.dumps(body)}))

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


@pytest.mark.asyncio
async def test_chunk_that_precedes_open_response_is_replayed():
    socket = FakeSocket()

    async def connector(*_args, **_kwargs):
        return socket

    client = Client("https://cp.example", "token", connector=connector)
    session = await client.exec("ws_1", ["true"])
    chunk = await anext(session.__aiter__())
    assert (chunk.seq, chunk.stream, chunk.data) == (0, 4, b"info")
    assert cbor2.loads(socket.sent[0]["body"])["caps"] == ["v1", "authz-push"]
    await client.close()


@pytest.mark.asyncio
async def test_request_wait_is_bounded():
    socket = FakeSocket()
    original_send = socket.send

    async def send(wire):
        frame = cbor2.loads(wire)
        if frame["t"] == "hello":
            await original_send(wire)

    socket.send = send

    async def connector(*_args, **_kwargs):
        return socket

    client = Client("https://cp.example", "token", connector=connector, retries=1, request_timeout=0.01)
    with pytest.raises(TimeoutError):
        await client.call("ws.list")
    await client.close()


@pytest.mark.asyncio
async def test_disconnect_supervisor_reattaches_from_next_sequence():
    first, second = FakeSocket(), FakeSocket()
    sockets = iter([first, second])

    async def connector(*_args, **_kwargs):
        return next(sockets)

    client = Client("https://cp.example", "token", connector=connector)
    await client.connect()
    session = Session(client, "ws_1", "exec", "s_1")
    session.next_seq = 4
    client._sessions[session.id] = session
    await first.close()
    await __import__("asyncio").wait_for(second.attached.wait(), timeout=1)
    attach = next(frame for frame in second.sent if frame.get("op") == "s.attach")
    assert cbor2.loads(attach["body"])["from"] == 4
    await client.close()


@pytest.mark.asyncio
async def test_session_reorders_and_deduplicates_chunks():
    client = Client("https://cp.example", "token")
    session = Session(client, "ws_1", "exec", "s_1")
    await session._enqueue({"seq": 1, "body": cbor2.dumps({"st": 1, "d": b"b"})})
    await session._enqueue({"seq": 0, "body": cbor2.dumps({"st": 1, "d": b"a"})})
    await session._enqueue({"seq": 1, "body": cbor2.dumps({"st": 1, "d": b"b"})})
    await session._enqueue({"seq": 2, "body": cbor2.dumps({"st": 3, "d": cbor2.dumps({"code": 0})})})
    chunks = [chunk async for chunk in session]
    assert [chunk.seq for chunk in chunks] == [0, 1, 2]
    assert session.exit == {"code": 0}


@pytest.mark.asyncio
async def test_gap_advances_cursor_and_releases_following_chunk():
    client = Client("https://cp.example", "token")
    session = Session(client, "ws_1", "exec", "s_1")
    await session._enqueue({"seq": 4, "body": cbor2.dumps({"st": 1, "d": b"kept"})})
    await session._enqueue({"seq": 0, "body": cbor2.dumps({"st": 5, "d": cbor2.dumps({"from": 0, "to": 3})})})
    first = await anext(session.__aiter__())
    second = await anext(session.__aiter__())
    assert (first.stream, second.seq, second.data) == (5, 4, b"kept")


@pytest.mark.asyncio
async def test_artifact_download_verifies_digest():
    payload = b"artifact"
    artifact_id = "art_sha256:" + hashlib.sha256(payload).hexdigest()

    def handler(_: httpx.Request) -> httpx.Response:
        return httpx.Response(200, content=payload + b"corrupt")

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as http:
        client = Client("https://cp.example", "token", http_client=http)
        with pytest.raises(ProtocolError, match="digest mismatch"):
            await client.download_artifact(artifact_id)


@pytest.mark.asyncio
async def test_artifact_error_preserves_code_and_redacts_token():
    canary = "artifact-secret-canary"
    artifact_id = "art_sha256:" + "0" * 64

    def handler(_: httpx.Request) -> httpx.Response:
        return httpx.Response(404, json={"error": {"code": "not_found", "message": "missing " + canary}})

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as http:
        client = Client("https://cp.example", canary, http_client=http)
        with pytest.raises(ProtocolError) as caught:
            await client.download_artifact(artifact_id)
    assert caught.value.code == "not_found"
    assert canary not in str(caught.value)


def test_generated_types_import():
    from remount.types import Agent, OPERATIONS, Workspace

    assert Agent and Workspace
    assert OPERATIONS["ws.create"]["constant"] == "OpWSCreate"


@pytest.mark.asyncio
async def test_failed_input_does_not_consume_sequence():
    class FakeClient:
        def __init__(self):
            self.calls = []

        async def _node_call(self, _workspace, _op, body):
            self.calls.append(body["iseq"])
            if len(self.calls) == 1:
                raise OSError("cut")

    client = FakeClient()
    session = Session(client, "ws_1", "exec", "s_1")
    with pytest.raises(OSError):
        await session.input(b"x")
    await session.input(b"x")
    assert client.calls == [1, 1]


@pytest.mark.asyncio
async def test_session_failure_rejects_pending_consumer():
    client = Client("https://cp.example", "token")
    session = Session(client, "ws_1", "exec", "s_1")
    pending = __import__("asyncio").create_task(anext(session.__aiter__()))
    await __import__("asyncio").sleep(0)
    await session._fail(ProtocolError("resource_exhausted", "queue full"))
    with pytest.raises(ProtocolError, match="queue full"):
        await pending


@pytest.mark.asyncio
async def test_non_byte_chunk_data_is_rejected_without_allocating_from_its_value():
    client = Client("https://cp.example", "token")
    session = Session(client, "ws_1", "exec", "s_1")
    with pytest.raises(ProtocolError, match="chunk data must be bytes"):
        await session._enqueue({"seq": 0, "body": cbor2.dumps({"st": 1, "d": 1 << 62})})
