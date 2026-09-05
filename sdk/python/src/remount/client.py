"""Bounded asyncio implementation of the Remount v1 CBOR transport."""

from __future__ import annotations

import asyncio
import hashlib
import json
import secrets
import time
from dataclasses import dataclass
from typing import Any, AsyncIterator, Awaitable, Callable, cast
from urllib.parse import quote, urlsplit, urlunsplit

import cbor2
import httpx
from websockets.asyncio.client import connect as websocket_connect

from .types import (
    FSApplyTarRes,
    FSEntry,
    FSReadRes,
    SessionStatus,
    Timer,
    Workspace,
    WSSnapshotRes,
)

PROTOCOL_VERSION = 1
CONTROL = "control"
MAX_FRAME_BYTES = 16 << 20
MAX_FILE_CHUNK_BYTES = 8 << 20
MAX_PENDING = 4096
MAX_ORPHAN_SESSIONS = 256
MAX_ORPHAN_CHUNKS = 16_384
MAX_ORPHAN_BYTES = 16 << 20
MAX_REORDER_CHUNKS = 4096
MAX_REORDER_BYTES = 16 << 20
MAX_DELIVERY_BYTES = 16 << 20
STREAM_EXIT = 3
STREAM_GAP = 5
# Node-owned enforcement stays on the node; clients fence frames, verify
# artifact bytes and preserve explicit gaps in replayed session output.
PEER_CAPABILITIES = ["v1", "authz-push", "controller-epoch", "session-cap", "chunked-artifacts", "tiered-session-logs", "release-epoch", "identity-admin"]


class ProtocolError(Exception):
    """A stable Remount protocol error."""

    def __init__(self, code: str, message: str = "", oldest: int = 0):
        self.code = code
        self.message = message
        self.oldest = oldest
        super().__init__(f"{code}: {message}" if message else code)


class ConnectionClosed(Exception):
    pass


@dataclass(frozen=True)
class Chunk:
    seq: int
    stream: int
    data: bytes


def _idempotency_key() -> str:
    return "idem_" + secrets.token_hex(16)


def _wire_bytes(value: Any, field: str) -> bytes:
    if not isinstance(value, (bytes, bytearray, memoryview)):
        raise ProtocolError("bad_request", f"{field} must be bytes")
    return bytes(value)


def _wire_epoch(value: Any) -> int:
    if type(value) is not int or not 0 <= value <= (1 << 64) - 1:
        raise ProtocolError("bad_request", "invalid controller epoch")
    return value


def _orphan_frame_bytes(session_id: str, frame: dict[str, Any]) -> int:
    # Charge routing metadata at the worst-case Unicode width, not just body.
    return len(frame["body"]) + 4 * (len(session_id) + len(frame["from"]) + len(frame["ws"]))


def _ws_url(base_url: str) -> str:
    value = urlsplit(base_url)
    scheme = {"http": "ws", "https": "wss", "ws": "ws", "wss": "wss"}.get(value.scheme)
    if scheme is None:
        raise ValueError("Remount URL must use http, https, ws, or wss")
    path = value.path.rstrip("/")
    if not path.endswith("/v1/link"):
        path += "/v1/link"
    return urlunsplit((scheme, value.netloc, path, value.query, ""))


class Session:
    """A resumable cursor over one session's ordered chunk log."""

    def __init__(self, client: Client, workspace: str, kind: str, session_id: str = ""):
        self.client = client
        self.workspace = workspace
        self.kind = kind
        self.id = session_id
        self.next_seq = 0
        self.last_input_seq = 0
        self.exit: dict[str, Any] | None = None
        self.error: Exception | None = None
        self._queue: asyncio.Queue[Chunk | None] = asyncio.Queue(maxsize=1024)
        self._pending: dict[int, tuple[dict[str, Any], int]] = {}
        self._pending_bytes = 0
        self._queue_bytes = 0
        self._closed = False
        self._attach_lock = asyncio.Lock()
        self._input_lock = asyncio.Lock()
        self._node = ""

    def _bind_node(self, node: str) -> None:
        self._node = node

    async def _enqueue_from_node(self, frame: dict[str, Any]) -> None:
        if self._node and frame.get("from") == self._node and frame.get("ws") == self.workspace:
            await self._enqueue({"seq": frame.get("seq", 0), "body": frame.get("body", b"")})

    def __aiter__(self) -> AsyncIterator[Chunk]:
        return self._iterate()

    async def _iterate(self) -> AsyncIterator[Chunk]:
        while True:
            if self._closed and self._queue.empty():
                if self.error:
                    raise self.error
                return
            chunk = await self._queue.get()
            if chunk is None:
                if self.error:
                    raise self.error
                return
            self._queue_bytes -= len(chunk.data)
            yield chunk

    async def _enqueue(self, frame: dict[str, Any]) -> None:
        if self._closed:
            return
        seq = int(frame.get("seq", 0))
        raw_body = _wire_bytes(frame.get("body", b""), "chunk body")
        body = cbor2.loads(raw_body) if raw_body else {}
        stream = int(body.get("st", 0))
        data = _wire_bytes(body.get("d", b""), "chunk data")
        if stream == STREAM_GAP:
            gap = cbor2.loads(data)
            self.next_seq = max(self.next_seq, int(gap["to"]) + 1)
            for pending_seq in list(self._pending):
                if pending_seq < self.next_seq:
                    _, size = self._pending.pop(pending_seq)
                    self._pending_bytes -= size
            if not await self._put(Chunk(seq, stream, data)):
                return
            while self.next_seq in self._pending and not self._closed:
                pending, size = self._pending.pop(self.next_seq)
                self._pending_bytes -= size
                decoded = cbor2.loads(pending.get("body", b""))
                await self._deliver(self.next_seq, int(decoded.get("st", 0)), bytes(decoded.get("d", b"")))
            return
        if seq < self.next_seq:
            return
        if seq > self.next_seq:
            if seq not in self._pending:
                self._pending[seq] = (frame, len(raw_body))
                self._pending_bytes += len(raw_body)
            if len(self._pending) > MAX_REORDER_CHUNKS or self._pending_bytes > MAX_REORDER_BYTES:
                await self._fail(ProtocolError("resource_exhausted", "session reorder buffer is full"))
            return
        await self._deliver(seq, stream, data)
        while self.next_seq in self._pending and not self._closed:
            pending, size = self._pending.pop(self.next_seq)
            self._pending_bytes -= size
            decoded = cbor2.loads(pending.get("body", b""))
            await self._deliver(self.next_seq, int(decoded.get("st", 0)), bytes(decoded.get("d", b"")))

    async def _deliver(self, seq: int, stream: int, data: bytes) -> None:
        self.next_seq = seq + 1
        if not await self._put(Chunk(seq, stream, data)):
            return
        if stream == STREAM_EXIT:
            self.exit = cbor2.loads(data)
            self._closed = True
            self.client._sessions.pop(self.id, None)
            await self._put(None)

    async def _put(self, value: Chunk | None) -> bool:
        size = len(value.data) if value is not None else 0
        if value is not None and self._queue_bytes + size > MAX_DELIVERY_BYTES:
            await self._fail(ProtocolError("resource_exhausted", "session delivery queue bytes are full"))
            return False
        try:
            self._queue.put_nowait(value)
            self._queue_bytes += size
            return True
        except asyncio.QueueFull:
            await self._fail(ProtocolError("resource_exhausted", "session delivery queue is full"))
            return False

    async def _fail(self, error: Exception) -> None:
        if self._closed:
            return
        self.error = error
        self._closed = True
        self.client._sessions.pop(self.id, None)
        if self._queue.full():
            discarded = self._queue.get_nowait()
            if discarded is not None:
                self._queue_bytes -= len(discarded.data)
        self._queue.put_nowait(None)

    async def _reattach(self, generation: int) -> None:
        if self._closed or not self.id:
            return
        async with self._attach_lock:
            delay = 0.1
            for _ in range(8):
                if self._closed or self.client.generation != generation:
                    return
                try:
                    response = await self.client._node_call(
                        self.workspace, "s.attach", {"s": self.id, "from": self.next_seq}, self._bind_node
                    )
                    async with self._input_lock:
                        self.last_input_seq = max(self.last_input_seq, int(response.get("last_iseq", 0)))
                    return
                except ProtocolError as error:
                    if error.code == "not_found":
                        await self._fail(error)
                        return
                except (ConnectionClosed, OSError):
                    pass
                await asyncio.sleep(delay)
                delay = min(delay * 2, 2)
            await self._fail(ConnectionClosed("session reattach retries exhausted"))

    async def input(self, data: bytes = b"", eof: bool = False) -> None:
        async with self._input_lock:
            sequence = self.last_input_seq + 1
            await self.client._node_call(
                self.workspace,
                "s.input",
                {"s": self.id, "iseq": sequence, "d": data, "eof": eof},
            )
            self.last_input_seq = sequence


class Client:
    """A reconnecting Remount protocol client."""

    def __init__(
        self,
        base_url: str,
        token: str,
        *,
        principal: str = "",
        retries: int = 5,
        request_timeout: float = 30.0,
        max_frame_bytes: int = MAX_FRAME_BYTES,
        connector: Callable[..., Awaitable[Any]] | None = None,
        http_client: httpx.AsyncClient | None = None,
    ):
        if retries < 1 or retries > 10:
            raise ValueError("retries must be between 1 and 10")
        if request_timeout <= 0:
            raise ValueError("request_timeout must be positive")
        if max_frame_bytes < 1:
            raise ValueError("max_frame_bytes must be positive")
        self.base_url = base_url.rstrip("/")
        self.token = token
        self.principal = principal
        self.retries = retries
        self.request_timeout = request_timeout
        self.max_frame_bytes = max_frame_bytes
        self._connector = connector or websocket_connect
        self._http = http_client
        self._owns_http = http_client is None
        self._socket: Any = None
        self._reader: asyncio.Task[None] | None = None
        self._connect_lock = asyncio.Lock()
        self._send_lock = asyncio.Lock()
        self._pending: dict[int, tuple[asyncio.Future[dict[str, Any]], str, str, bool]] = {}
        self._controller_epoch = 0
        self._request_id = 0
        self._closed = False
        self._sessions: dict[str, Session] = {}
        self._orphans: dict[str, list[dict[str, Any]]] = {}
        self._orphan_count = 0
        self._orphan_bytes = 0
        self._grants: dict[str, dict[str, Any]] = {}
        self.generation = 0
        self.peer_id = ""

    async def __aenter__(self) -> Client:
        await self.connect()
        return self

    async def __aexit__(self, *_: object) -> None:
        await self.close()

    async def close(self) -> None:
        self._closed = True
        socket, self._socket = self._socket, None
        if socket is not None:
            await socket.close()
        if self._reader and self._reader is not asyncio.current_task():
            await asyncio.gather(self._reader, return_exceptions=True)
        for session in list(self._sessions.values()):
            await session._fail(ConnectionClosed("client closed"))
        if self._http is not None and self._owns_http:
            await self._http.aclose()

    async def connect(self) -> None:
        if self._closed:
            raise ConnectionClosed("client closed")
        if self._socket is not None and not self._connect_lock.locked():
            return
        async with self._connect_lock:
            if self._socket is not None:
                return
            socket = await asyncio.wait_for(
                self._connector(
                    _ws_url(self.base_url),
                    compression=None,
                    max_size=self.max_frame_bytes,
                    max_queue=16,
                ),
                timeout=self.request_timeout,
            )
            self._socket = socket
            self._controller_epoch = 0
            self._reader = asyncio.create_task(self._read_loop(socket))
            try:
                hello = await self._round_trip(
                    {"v": 1, "t": "hello", "body": cbor2.dumps({"peer": self.peer_id, "role": "client", "token": self.token, "principal": self.principal, "caps": PEER_CAPABILITIES})}
                )
                body = cbor2.loads(hello.get("body", b""))
                if "v1" not in body.get("caps", []):
                    raise ProtocolError("unsupported", "server did not negotiate v1")
                self.peer_id = body.get("peer", "")
                self.generation += 1
                generation = self.generation
                self._grants.clear()
                self._orphans.clear()
                self._orphan_count = 0
                self._orphan_bytes = 0
                for session in list(self._sessions.values()):
                    asyncio.create_task(session._reattach(generation))
            except BaseException:
                await socket.close()
                self._socket = None
                raise

    async def _read_loop(self, socket: Any) -> None:
        failure: Exception = ConnectionClosed("connection closed")
        try:
            async for message in socket:
                if self._socket is not socket:
                    return
                if isinstance(message, str):
                    raise ProtocolError("bad_request", "text WebSocket frame")
                if len(message) > self.max_frame_bytes:
                    raise ProtocolError("resource_exhausted", "wire frame exceeds configured limit")
                frame = cbor2.loads(message)
                kind = frame.get("t")
                if int(frame.get("v", 0)) != PROTOCOL_VERSION:
                    raise ProtocolError("unsupported", "wire version mismatch")
                pending = self._pending.get(int(frame.get("id", 0)))
                if not (pending and pending[3]) and _wire_epoch(frame.get("controller_epoch", 0)) < self._controller_epoch:
                    continue
                if kind in ("res", "pong") and int(frame.get("id", 0)) in self._pending:
                    future, expected_from, expected_op, hello = self._pending[int(frame["id"])]
                    if ((not expected_from or frame.get("from") == expected_from)
                            and (not expected_op or frame.get("op") == expected_op)):
                        if hello and not frame.get("err"):
                            body = cbor2.loads(frame.get("body", b""))
                            if "controller-epoch" in body.get("caps", []):
                                epoch = _wire_epoch(body.get("controller_epoch", 0))
                                if not epoch:
                                    raise ProtocolError("conflict", "server negotiated controller-epoch without an epoch")
                                self._controller_epoch = epoch
                        self._pending.pop(int(frame["id"]))
                        if not future.done():
                            future.set_result(frame)
                elif kind == "chunk":
                    await self._handle_chunk(frame)
                elif kind == "ping":
                    await self._send({"v": 1, "t": "pong", "id": frame.get("id", 0), "to": frame.get("from", "")})
        except asyncio.CancelledError:
            raise
        except Exception as error:
            failure = error if isinstance(error, ProtocolError) else ConnectionClosed("connection failed")
        finally:
            if self._socket is socket or self._socket is None:
                self._socket = None
                for future, _, _, _ in list(self._pending.values()):
                    if not future.done():
                        future.set_exception(failure)
                self._pending.clear()
                if not self._closed and self._sessions:
                    asyncio.create_task(self._supervise())

    async def _supervise(self) -> None:
        delay = 0.1
        while not self._closed and self._sessions and self._socket is None:
            try:
                await self.connect()
                return
            except Exception:
                await asyncio.sleep(delay)
                delay = min(delay * 2, 5)

    async def _send(self, frame: dict[str, Any]) -> None:
        socket = self._socket
        if socket is None:
            raise ConnectionClosed("not connected")
        if frame.get("t") != "hello" and self._controller_epoch:
            frame = {**frame, "controller_epoch": self._controller_epoch}
        encoded = cbor2.dumps(frame, canonical=True)
        if len(encoded) > self.max_frame_bytes:
            raise ProtocolError("resource_exhausted", "wire frame exceeds configured limit")
        async with self._send_lock:
            try:
                await socket.send(encoded)
            except Exception:
                if self._socket is socket:
                    self._socket = None
                await socket.close()
                raise ConnectionClosed("connection send failed") from None

    async def _round_trip(self, frame: dict[str, Any]) -> dict[str, Any]:
        if len(self._pending) >= MAX_PENDING:
            raise ProtocolError("resource_exhausted", "too many pending requests")
        self._request_id += 1
        request_id = self._request_id
        frame["id"] = request_id
        future = asyncio.get_running_loop().create_future()
        self._pending[request_id] = (future, str(frame.get("to", "")), str(frame.get("op", "")), frame.get("t") == "hello")
        try:
            await self._send(frame)
            response = await asyncio.wait_for(asyncio.shield(future), timeout=self.request_timeout)
        finally:
            self._pending.pop(request_id, None)
        error = response.get("err")
        if error:
            message = str(error.get("msg", "")).replace(self.token, "[redacted]") if self.token else str(error.get("msg", ""))
            raise ProtocolError(error.get("code", "internal"), message, int(error.get("oldest", 0)))
        return response

    async def call(self, op: str, body: dict[str, Any] | None = None, *, to: str = CONTROL) -> dict[str, Any]:
        failure: Exception = ConnectionClosed("call was not attempted")
        for attempt in range(self.retries):
            try:
                await self.connect()
                response = await self._round_trip({"v": 1, "t": "req", "to": to, "op": op, "body": cbor2.dumps(body or {}, canonical=True)})
                return cbor2.loads(response.get("body", b"")) if response.get("body") else {}
            except (ConnectionClosed, OSError, TimeoutError) as error:
                failure = error
                if attempt + 1 < self.retries:
                    await asyncio.sleep(0.2 * (attempt + 1))
        raise failure

    async def _node_call(self, workspace: str, op: str, body: dict[str, Any], bind_node: Callable[[str], None] | None = None) -> dict[str, Any]:
        for attempt in range(2):
            grant = self._grants.get(workspace)
            if grant is None:
                grant = await self.call("grant", {"ws": workspace})
                self._grants[workspace] = grant
            if bind_node:
                bind_node(str(grant["node"]))
            request = dict(body)
            request["grant"] = grant
            try:
                return await self.call(op, request, to=str(grant["node"]))
            except ProtocolError as error:
                if attempt == 0 and error.code in ("conflict", "unreachable", "unauthorized"):
                    self._grants.pop(workspace, None)
                    continue
                raise
        raise ConnectionClosed("node call retries exhausted")

    async def _handle_chunk(self, frame: dict[str, Any]) -> None:
        session_id = str(frame.get("s", ""))
        session = self._sessions.get(session_id)
        if session is not None:
            await session._enqueue_from_node(frame)
            return
        # Reconnect may clear the cache while an open still holds its grant.
        # Registration authenticates this bounded orphan against that open.
        raw_body = _wire_bytes(frame.get("body", b""), "chunk body")
        orphan = {"seq": int(frame.get("seq", 0)), "body": raw_body,
                  "from": frame.get("from", "") if isinstance(frame.get("from"), str) else "",
                  "ws": frame.get("ws", "") if isinstance(frame.get("ws"), str) else ""}
        size = _orphan_frame_bytes(session_id, orphan)
        bucket = self._orphans.setdefault(session_id, [])
        if (len(self._orphans) > MAX_ORPHAN_SESSIONS or len(bucket) >= MAX_REORDER_CHUNKS
                or self._orphan_count >= MAX_ORPHAN_CHUNKS or self._orphan_bytes + size > MAX_ORPHAN_BYTES):
            self._orphans.clear()
            self._orphan_count = 0
            self._orphan_bytes = 0
            socket, self._socket = self._socket, None
            if socket is not None:
                await socket.close()
            return
        bucket.append(orphan)
        self._orphan_count += 1
        self._orphan_bytes += size

    async def create_workspace(self, spec: dict[str, Any], idempotency_key: str | None = None) -> dict[str, Any]:
        return await self.call("ws.create", {"spec": spec, "idem": idempotency_key or _idempotency_key()})

    async def get_workspace(self, workspace: str) -> dict[str, Any]:
        return await self.call("ws.get", {"id": workspace})

    async def destroy_workspace(self, workspace: str, idempotency_key: str | None = None) -> None:
        await self.call("ws.destroy", {"id": workspace, "idem": idempotency_key or _idempotency_key()})

    async def wait_workspace(
        self,
        workspace: str,
        *,
        states: tuple[str, ...] = ("claimed",),
        timeout: float = 60,
        poll_interval: float = 0.1,
    ) -> Workspace:
        deadline = time.monotonic() + timeout
        while True:
            current = cast(Workspace, await self.get_workspace(workspace))
            if current["state"] in states:
                return current
            if time.monotonic() >= deadline:
                raise TimeoutError(f"workspace {workspace} did not reach {states}")
            await asyncio.sleep(poll_interval)

    async def sleep_workspace(
        self,
        workspace: str,
        *,
        after_sec: int = 0,
        at_millis: int = 0,
        on_event: str = "",
        match: dict[str, str] | None = None,
        idempotency_key: str | None = None,
    ) -> Timer:
        if after_sec == 0 and at_millis == 0 and not on_event:
            raise ValueError(
                "workspace sleep requires after_sec, at_millis, or on_event"
            )
        body: dict[str, object] = {
            "id": workspace,
            "idem": idempotency_key or _idempotency_key(),
        }
        if after_sec:
            body["after_sec"] = after_sec
        if at_millis:
            body["at"] = at_millis
        if on_event:
            body["on"] = on_event
        if match:
            body["match"] = match
        return cast(Timer, await self.call("ws.sleep", body))

    async def post_event(
        self,
        event_type: str,
        *,
        stream: str = "",
        payload: dict[str, Any] | None = None,
    ) -> None:
        event: dict[str, object] = {"seq": 0, "at": 0, "type": event_type}
        if stream:
            event["stream"] = stream
        if payload is not None:
            event["payload"] = cbor2.dumps(payload, canonical=True)
        await self.call("events.post", {"events": [event]})

    async def wake_workspace(
        self, workspace: str, idempotency_key: str | None = None
    ) -> Workspace:
        return cast(
            Workspace,
            await self.call(
                "ws.wake",
                {"id": workspace, "idem": idempotency_key or _idempotency_key()},
            ),
        )

    async def read_file(
        self,
        workspace: str,
        path: str,
        *,
        max_bytes: int = 512 << 20,
        chunk_bytes: int = MAX_FILE_CHUNK_BYTES,
    ) -> bytes:
        if max_bytes < 0 or chunk_bytes <= 0 or chunk_bytes > MAX_FILE_CHUNK_BYTES:
            raise ValueError("invalid file read limits")
        chunks: list[bytes] = []
        offset = 0
        while True:
            response = cast(
                FSReadRes,
                await self._node_call(
                    workspace,
                    "fs.read",
                    {"ws": workspace, "path": path, "offset": offset, "limit": chunk_bytes},
                ),
            )
            data = _wire_bytes(response["d"], "file data")
            if offset + len(data) > max_bytes or response["size"] > max_bytes:
                raise ProtocolError("resource_exhausted", "file exceeds configured download limit")
            chunks.append(data)
            offset += len(data)
            if response["eof"]:
                return b"".join(chunks)
            if not data:
                raise ProtocolError("internal", "file read made no progress")

    async def write_file(
        self,
        workspace: str,
        path: str,
        data: bytes,
        *,
        mode: int = 0,
        mkdirp: bool = True,
        idempotency_key: str | None = None,
    ) -> None:
        key = idempotency_key or _idempotency_key()
        chunks = range(0, max(len(data), 1), MAX_FILE_CHUNK_BYTES)
        for index, offset in enumerate(chunks):
            chunk = data[offset : offset + MAX_FILE_CHUNK_BYTES]
            body: dict[str, object] = {
                "ws": workspace,
                "path": path,
                "d": chunk,
                "append": index > 0,
                "mkdirp": mkdirp,
                "idem": f"{key}:{index}",
            }
            if mode:
                body["mode"] = mode
            await self._node_call(workspace, "fs.write", body)

    async def list_files(self, workspace: str, path: str) -> list[FSEntry]:
        response = await self._node_call(
            workspace, "fs.list", {"ws": workspace, "path": path}
        )
        return cast(list[FSEntry], response["entries"])

    async def list_sessions(self, workspace: str) -> list[SessionStatus]:
        response = await self._node_call(
            workspace, "s.list", {"ws": workspace}
        )
        return cast(list[SessionStatus], response["sessions"])

    async def close_session(
        self, workspace: str, session_id: str, *, kill: bool = False
    ) -> None:
        await self._node_call(
            workspace,
            "s.close",
            {"s": session_id, "kill": kill},
        )

    async def apply_tar(
        self,
        workspace: str,
        artifact: str,
        *,
        path: str = "",
        format: str = "tar",
        idempotency_key: str | None = None,
    ) -> FSApplyTarRes:
        body = {
            "ws": workspace,
            "artifact": artifact,
            "format": format,
            "idem": idempotency_key or _idempotency_key(),
        }
        if path:
            body["path"] = path
        return cast(
            FSApplyTarRes,
            await self._node_call(
                workspace,
                "fs.apply_tar",
                body,
            ),
        )

    async def snapshot_workspace(
        self,
        workspace: str,
        *,
        upload: bool = True,
        authoritative: bool = False,
        idempotency_key: str | None = None,
    ) -> WSSnapshotRes:
        if authoritative and not upload:
            raise ValueError("authoritative snapshots must be uploaded")
        return cast(
            WSSnapshotRes,
            await self._node_call(
                workspace,
                "ws.snapshot",
                {
                    "ws": workspace,
                    "upload": upload,
                    "authoritative": authoritative,
                    "idem": idempotency_key or _idempotency_key(),
                },
            ),
        )

    async def archive_path(
        self,
        workspace: str,
        path: str,
        *,
        idempotency_key: str | None = None,
    ) -> WSSnapshotRes:
        return cast(
            WSSnapshotRes,
            await self._node_call(
                workspace,
                "volume.archive",
                {
                    "ws": workspace,
                    "path": path,
                    "upload": True,
                    "idem": idempotency_key or _idempotency_key(),
                },
            ),
        )

    async def exec(
        self,
        workspace: str,
        program: list[str],
        *,
        kind: str = "exec",
        cwd: str = "",
        env: dict[str, str] | None = None,
        stdin: bool = False,
        idempotency_key: str | None = None,
    ) -> Session:
        key = idempotency_key or _idempotency_key()
        session = Session(self, workspace, kind)
        response = await self._node_call(workspace, "s.open", {"ws": workspace, "kind": kind, "program": program, "cwd": cwd, "env": env or {}, "stdin": stdin, "idem": key}, session._bind_node)
        session_id = str(response["s"])
        existing = self._sessions.get(session_id)
        if existing is not None:
            return existing
        session.id = session_id
        session.last_input_seq = int(response.get("last_iseq", 0))
        self._sessions[session_id] = session
        early = self._orphans.pop(session_id, [])
        self._orphan_count -= len(early)
        self._orphan_bytes -= sum(_orphan_frame_bytes(session_id, frame) for frame in early)
        for frame in early:
            await session._enqueue_from_node(frame)
        return session

    async def attach(self, workspace: str, session_id: str, from_seq: int = 0) -> Session:
        session = self._sessions.get(session_id) or Session(self, workspace, "", session_id)
        session.next_seq = from_seq
        self._sessions[session_id] = session
        await session._reattach(self.generation)
        return session

    async def upload_artifact(self, data: bytes) -> tuple[str, int]:
        digest = hashlib.sha256(data).hexdigest()
        artifact_id = "art_sha256:" + digest
        client = await self._http_client()
        headers = {"Content-Type": "application/gzip"}
        if self.token:
            headers["Authorization"] = f"Bearer {self.token}"
        async with client.stream(
            "PUT",
            f"{self.base_url}/v1/artifacts/{quote(artifact_id, safe=':')}",
            headers=headers,
            content=data,
        ) as response:
            await self._raise_http(response)
        return artifact_id, len(data)

    async def download_artifact(self, artifact_id: str, *, max_bytes: int = 8 << 30) -> bytes:
        artifact_digest = artifact_id.removeprefix("art_sha256:")
        if len(artifact_digest) != 64 or any(char not in "0123456789abcdef" for char in artifact_digest):
            raise ValueError("invalid artifact id")
        client = await self._http_client()
        headers = {"Authorization": f"Bearer {self.token}"} if self.token else {}
        async with client.stream(
            "GET",
            f"{self.base_url}/v1/artifacts/{quote(artifact_id, safe=':')}",
            headers=headers,
        ) as response:
            await self._raise_http(response)
            chunks: list[bytes] = []
            size = 0
            digest = hashlib.sha256()
            async for chunk in response.aiter_bytes():
                size += len(chunk)
                if size > max_bytes:
                    raise ProtocolError("resource_exhausted", "artifact exceeds configured download limit")
                digest.update(chunk)
                chunks.append(chunk)
        if digest.hexdigest() != artifact_digest:
            raise ProtocolError("conflict", "artifact digest mismatch")
        return b"".join(chunks)

    async def _http_client(self) -> httpx.AsyncClient:
        if self._http is None:
            self._http = httpx.AsyncClient(timeout=30, follow_redirects=False)
        return self._http

    async def _raise_http(self, response: httpx.Response) -> None:
        if response.is_success:
            return
        chunks: list[bytes] = []
        size = 0
        async for chunk in response.aiter_bytes():
            size += len(chunk)
            if size > 4096:
                chunks = []
                break
            chunks.append(chunk)
        detail: Any = {}
        try:
            body = json.loads(b"".join(chunks))
            detail = body.get("error", body) if isinstance(body, dict) else {}
        except (ValueError, UnicodeDecodeError):
            pass
        if not isinstance(detail, dict):
            detail = {}
        message = str(detail.get("message", f"HTTP {response.status_code}"))
        if self.token:
            message = message.replace(self.token, "[redacted]")
        raise ProtocolError(str(detail.get("code", "internal")), message)
