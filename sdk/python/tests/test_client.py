import hashlib
from pathlib import Path

import cbor2
import httpx
import pytest

from remount import Client, ProtocolError
from remount.client import PEER_CAPABILITIES, Session


def test_decodes_go_protocol_golden_fixture():
    wire = bytes.fromhex((Path(__file__).parents[3] / "internal/proto/testdata/v1-request.hex").read_text().strip())
    frame = cbor2.loads(wire)
    assert (frame["v"], frame["t"], frame["id"], frame["op"]) == (1, "req", 7, "ws.get")
    assert cbor2.loads(frame["body"])["id"] == "ws_fixture"
    body = cbor2.dumps({"id": "ws_fixture"}, canonical=True)
    assert cbor2.dumps({"v": 1, "t": "req", "id": 7, "to": "control", "op": "ws.get", "body": body}, canonical=True) == wire


class FakeSocket:
    def __init__(self, responses=None):
        self.incoming = __import__("asyncio").Queue()
        self.closed = False
        self.sent = []
        self.attached = __import__("asyncio").Event()
        self.caps = ["v1"]
        self.epoch = 0
        self.early_frames = []
        self.responses = responses or {}

    async def send(self, wire):
        frame = cbor2.loads(wire)
        self.sent.append(frame)
        if frame["t"] == "hello":
            body = {"peer": "client_1", "caps": self.caps, "controller_epoch": self.epoch}
        elif frame["op"] == "grant":
            body = {"node": "node_1", "claims": {}}
        elif frame["op"] == "s.open":
            for early in self.early_frames:
                await self.incoming.put(cbor2.dumps(early))
            await self.incoming.put(cbor2.dumps({"v": 1, "t": "chunk", "controller_epoch": self.epoch, "from": "node_1", "ws": "ws_1", "s": "s_1", "seq": 0, "body": cbor2.dumps({"st": 4, "d": b"info"})}))
            body = {"s": "s_1", "next": 1}
        elif frame["op"] == "s.attach":
            self.attached.set()
            body = {"s": "s_1", "next": 4}
        else:
            configured = self.responses.get(frame["op"], {})
            body = configured(frame) if callable(configured) else configured
        await self.incoming.put(cbor2.dumps({"v": 1, "t": "res", "controller_epoch": self.epoch, "id": frame["id"], "op": frame.get("op", ""), "from": frame.get("to", ""), "body": cbor2.dumps(body)}))

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
    assert cbor2.loads(socket.sent[0]["body"])["caps"] == PEER_CAPABILITIES
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


@pytest.mark.asyncio
async def test_output_authority_covers_early_live_and_rebound_producers():
    socket = FakeSocket()

    def chunk(source, workspace="ws_1", seq=0):
        return {"v": 1, "t": "chunk", "from": source, "ws": workspace, "s": "s_1", "seq": seq,
                "body": cbor2.dumps({"st": 3, "d": cbor2.dumps({"code": 99})})}

    socket.early_frames = [chunk(""), chunk("client_evil"), chunk("node_evil"), chunk("node_1", "ws_other")]

    async def connector(*_args, **_kwargs):
        return socket

    client = Client("https://cp.example", "token", connector=connector)
    session = await client.exec("ws_1", ["cat"])
    assert (await anext(session.__aiter__())).stream == 4
    assert session.exit is None
    for source in ("", "client_evil", "node_evil"):
        await client._handle_chunk(chunk(source, seq=1))
    session._bind_node("node_new")
    await client._handle_chunk(chunk("node_1", seq=1))
    assert session.exit is None
    await client._handle_chunk(chunk("node_new", seq=1))
    assert session.exit == {"code": 99}
    await client.close()


@pytest.mark.asyncio
async def test_controller_epoch_fences_frames_and_preserves_uint64():
    socket = FakeSocket()
    socket.caps = ["v1", "controller-epoch"]
    socket.epoch = (1 << 53) + 1
    socket.early_frames = [{"v": 1, "t": "chunk", "controller_epoch": socket.epoch - 1,
                            "from": "node_1", "ws": "ws_1", "s": "s_1", "seq": 0,
                            "body": cbor2.dumps({"st": 3, "d": cbor2.dumps({"code": 99})})}]
    original_send = socket.send

    async def send(wire):
        request = cbor2.loads(wire)
        if request.get("op") == "ws.list":
            for source, epoch in (("control", socket.epoch - 1), ("", socket.epoch), ("client_evil", socket.epoch)):
                await socket.incoming.put(cbor2.dumps({"v": 1, "t": "res", "from": source,
                    "controller_epoch": epoch, "id": request["id"], "op": request["op"],
                    "body": cbor2.dumps({"forged": True})}))
        await original_send(wire)

    socket.send = send

    async def connector(*_args, **_kwargs):
        return socket

    client = Client("https://cp.example", "token", connector=connector)
    assert await client.call("ws.list") == {}
    session = await client.exec("ws_1", ["true"])
    assert (await anext(session.__aiter__())).stream == 4
    assert all(frame["controller_epoch"] == socket.epoch for frame in socket.sent[1:])
    await client.close()


@pytest.mark.asyncio
async def test_missing_negotiated_epoch_is_refused():
    socket = FakeSocket()
    socket.caps = ["v1", "controller-epoch"]

    async def connector(*_args, **_kwargs):
        return socket

    client = Client("https://cp.example", "token", connector=connector)
    with pytest.raises(ProtocolError) as caught:
        await client.connect()
    assert caught.value.code == "conflict"
    await client.close()


@pytest.mark.asyncio
async def test_open_retry_keeps_early_output_after_grant_cache_is_cleared():
    first, second = FakeSocket(), FakeSocket()
    sockets = iter([first, second])
    original_send = first.send

    async def send(wire):
        if cbor2.loads(wire).get("op") == "s.open":
            await first.close()
            return
        await original_send(wire)

    first.send = send

    async def connector(*_args, **_kwargs):
        return next(sockets)

    client = Client("https://cp.example", "token", connector=connector)
    try:
        session = await client.exec("ws_1", ["true"])
        assert client.generation == 2
        assert not client._grants
        chunk = await __import__("asyncio").wait_for(anext(session.__aiter__()), timeout=5)
        assert (chunk.seq, chunk.stream, chunk.data) == (0, 4, b"info")
    finally:
        await client.close()


@pytest.mark.asyncio
async def test_orphan_routing_metadata_is_charged_to_the_byte_limit(monkeypatch):
    monkeypatch.setattr("remount.client.MAX_ORPHAN_BYTES", 128)
    client = Client("https://cp.example", "token")
    socket = FakeSocket()
    client._socket = socket
    await client._handle_chunk({"s": "s_1", "from": "node_1", "ws": "x" * 128, "body": b""})
    assert socket.closed
    assert not client._orphans and client._orphan_bytes == 0
    await client.close()


@pytest.mark.asyncio
async def test_workspace_lifecycle_helpers_encode_control_operations():
    socket = FakeSocket(
        {
            "ws.get": {"id": "ws_1", "state": "claimed"},
            "ws.sleep": {"id": "timer_1"},
            "ws.wake": {"id": "ws_1", "state": "claiming"},
        }
    )

    async def connector(*_args, **_kwargs):
        return socket

    client = Client("https://cp.example", "token", connector=connector)
    workspace = await client.wait_workspace("ws_1", timeout=0)
    timer = await client.sleep_workspace("ws_1", after_sec=30, on_event="job.done", match={"id": "job_1"})
    waking = await client.wake_workspace("ws_1")

    assert workspace["state"] == "claimed"
    assert timer["id"] == "timer_1"
    assert waking["state"] == "claiming"
    bodies = {
        frame["op"]: cbor2.loads(frame["body"])
        for frame in socket.sent
        if frame.get("op") in {"ws.sleep", "ws.wake"}
    }
    assert bodies["ws.sleep"]["after_sec"] == 30
    assert bodies["ws.sleep"]["match"] == {"id": "job_1"}
    assert bodies["ws.wake"]["id"] == "ws_1"
    await client.close()


@pytest.mark.asyncio
async def test_file_helpers_chunk_reads_and_idempotent_writes():
    reads = iter(
        [
            {"d": b"abc", "size": 5, "eof": False},
            {"d": b"de", "size": 5, "eof": True},
        ]
    )
    socket = FakeSocket({"fs.read": lambda _frame: next(reads), "fs.write": {}})

    async def connector(*_args, **_kwargs):
        return socket

    client = Client("https://cp.example", "token", connector=connector)
    assert await client.read_file("ws_1", "workspace/report.txt", chunk_bytes=3) == b"abcde"
    await client.write_file(
        "ws_1",
        "workspace/empty.txt",
        b"",
        idempotency_key="idem_file",
    )

    node_frames = [
        frame
        for frame in socket.sent
        if frame.get("op") in {"fs.read", "fs.write"}
    ]
    read_bodies = [cbor2.loads(frame["body"]) for frame in node_frames if frame["op"] == "fs.read"]
    write_body = next(cbor2.loads(frame["body"]) for frame in node_frames if frame["op"] == "fs.write")
    assert [body["offset"] for body in read_bodies] == [0, 3]
    assert write_body["d"] == b""
    assert write_body["idem"] == "idem_file:0"
    assert write_body["append"] is False
    await client.close()


@pytest.mark.asyncio
async def test_snapshot_archive_and_apply_tar_helpers_encode_node_operations():
    socket = FakeSocket(
        {
            "fs.list": {"entries": [{"name": "file.txt", "size": 1, "mode": 0o644, "dir": False, "mtime": 0}]},
            "fs.apply_tar": {"files": 1, "dirs": 0, "bytes": 1},
            "ws.snapshot": {
                "artifact": "art_sha256:" + "0" * 64,
                "bytes": 1,
                "consistency": "quiesced",
                "authoritative": True,
            },
            "volume.archive": {
                "artifact": "art_sha256:" + "1" * 64,
                "bytes": 1,
                "consistency": "live",
                "authoritative": False,
            },
        }
    )

    async def connector(*_args, **_kwargs):
        return socket

    client = Client("https://cp.example", "token", connector=connector)
    entries = await client.list_files("ws_1", "workspace")
    applied = await client.apply_tar("ws_1", "art_sha256:" + "2" * 64)
    snapshot = await client.snapshot_workspace("ws_1", authoritative=True)
    archive = await client.archive_path("ws_1", "workspace")

    assert entries[0]["name"] == "file.txt"
    assert applied["files"] == 1
    assert snapshot["authoritative"] is True
    assert archive["authoritative"] is False
    bodies = {
        frame["op"]: cbor2.loads(frame["body"])
        for frame in socket.sent
        if frame.get("op") in {"fs.apply_tar", "ws.snapshot", "volume.archive"}
    }
    assert bodies["fs.apply_tar"]["format"] == "tar"
    assert bodies["ws.snapshot"]["upload"] is True
    assert bodies["ws.snapshot"]["authoritative"] is True
    assert bodies["volume.archive"]["path"] == "workspace"
    await client.close()


@pytest.mark.asyncio
async def test_authoritative_snapshot_requires_upload():
    client = Client("https://cp.example", "token")
    with pytest.raises(ValueError, match="must be uploaded"):
        await client.snapshot_workspace("ws_1", upload=False, authoritative=True)
