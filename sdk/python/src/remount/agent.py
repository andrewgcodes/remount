"""JSON HTTP clients for Remount Agents and durable approvals."""

from __future__ import annotations

import asyncio
import json as jsonlib
import secrets
from typing import Any, AsyncIterator, Awaitable, Callable, NotRequired, TypedDict
from urllib.parse import quote, urlencode, urlsplit, urlunsplit

import httpx
from websockets.asyncio.client import connect as websocket_connect

from .client import ConnectionClosed, ProtocolError
from .types import Agent, AgentCreateReq, AgentMessageRes, Approval

MAX_HTTP_RESPONSE_BYTES = 16 << 20


class ApprovalDecisionInput(TypedDict):
    """The HTTP decision body; the approval id is carried by the URL."""

    option: NotRequired[str]
    denied: NotRequired[bool]
    content: NotRequired[Any]
    remember: NotRequired[str]


def _key() -> str:
    return "idem_" + secrets.token_hex(16)


class Terminal:
    """One authenticated Agent terminal attachment with an explicit replay cursor."""

    def __init__(self, socket: Any, *, timeout: float, max_message_bytes: int):
        self._socket = socket
        self.timeout = timeout
        self.max_message_bytes = max_message_bytes
        self.session = ""
        self.next_seq = 0
        self._opened = False
        self._closed = False
        self._socket_closed = False

    async def __aenter__(self) -> Terminal:
        return self

    async def __aexit__(self, *_: object) -> None:
        await self.close()

    def __aiter__(self) -> AsyncIterator[dict[str, Any]]:
        return self

    async def __anext__(self) -> dict[str, Any]:
        if self._closed:
            raise StopAsyncIteration
        try:
            message = await asyncio.wait_for(self._socket.recv(), timeout=self.timeout)
        except StopAsyncIteration:
            self._closed = True
            raise
        except TimeoutError:
            await self.close()
            raise ProtocolError("timeout", "terminal receive deadline exceeded") from None
        except asyncio.CancelledError:
            raise
        except Exception:
            self._closed = True
            raise ConnectionClosed("terminal disconnected; reconnect from next_seq") from None
        if isinstance(message, str):
            if len(message.encode()) > self.max_message_bytes:
                await self.close()
                raise ProtocolError("resource_exhausted", "terminal message exceeds configured limit")
            try:
                event = jsonlib.loads(message)
            except ValueError:
                await self.close()
                raise ProtocolError("bad_request", "terminal sent invalid JSON") from None
            if not isinstance(event, dict) or not isinstance(event.get("type"), str):
                await self.close()
                raise ProtocolError("bad_request", "terminal sent invalid control event")
            if not self._opened and event["type"] != "open":
                await self.close()
                raise ProtocolError("bad_request", "terminal first event was not open")
            if event["type"] == "open":
                if self._opened:
                    await self.close()
                    raise ProtocolError("bad_request", "terminal sent a duplicate open event")
                self._opened = True
                self.session = str(event.get("session", ""))
                self.next_seq = int(event.get("next", 0))
            elif event["type"] == "gap":
                self.next_seq = max(self.next_seq, int(event.get("to", 0)) + 1)
            elif event["type"] == "exit":
                self.next_seq += 1
                self._closed = True
            return event
        if not isinstance(message, (bytes, bytearray, memoryview)):
            await self.close()
            raise ProtocolError("bad_request", "terminal sent an unsupported message")
        data = bytes(message)
        if len(data) > self.max_message_bytes:
            await self.close()
            raise ProtocolError("resource_exhausted", "terminal message exceeds configured limit")
        if not self._opened:
            await self.close()
            raise ProtocolError("bad_request", "terminal output preceded open")
        self.next_seq += 1
        return {"type": "data", "data": data}

    async def input(self, data: bytes) -> None:
        await self._socket.send(data)

    async def resize(self, rows: int, cols: int) -> None:
        await self._control({"type": "resize", "rows": rows, "cols": cols})

    async def signal(self, signal: str) -> None:
        await self._control({"type": "signal", "signal": signal})

    async def eof(self) -> None:
        await self._control({"type": "eof"})

    async def _control(self, event: dict[str, Any]) -> None:
        await self._socket.send(jsonlib.dumps(event, separators=(",", ":")))

    async def close(self) -> None:
        self._closed = True
        if not self._socket_closed:
            self._socket_closed = True
            await self._socket.close()


class AgentClient:
    """Async HTTP client for agent, transcript, and approval resources."""

    def __init__(self, base_url: str, token: str, *, client: httpx.AsyncClient | None = None, max_response_bytes: int = MAX_HTTP_RESPONSE_BYTES, retries: int = 3, terminal_connector: Callable[..., Awaitable[Any]] | None = None):
        if retries < 1 or retries > 10:
            raise ValueError("retries must be between 1 and 10")
        if max_response_bytes < 1:
            raise ValueError("max_response_bytes must be positive")
        self.base_url = base_url.rstrip("/")
        self.token = token
        self._client = client
        self._owns_client = client is None
        self.max_response_bytes = max_response_bytes
        self.retries = retries
        self._terminal_connector = terminal_connector or websocket_connect

    async def __aenter__(self) -> AgentClient:
        await self._http()
        return self

    async def __aexit__(self, *_: object) -> None:
        await self.close()

    async def close(self) -> None:
        """Close the internally owned HTTP connection pool."""
        if self._client is not None and self._owns_client:
            await self._client.aclose()
            self._client = None

    async def _http(self) -> httpx.AsyncClient:
        if self._client is None:
            self._client = httpx.AsyncClient(timeout=30, follow_redirects=False)
        return self._client

    async def _request(self, method: str, path: str, *, json: Any = None, idempotency_key: str | None = None) -> Any:
        headers = {"Authorization": f"Bearer {self.token}"}
        if method != "GET":
            headers["Idempotency-Key"] = idempotency_key or _key()
        request: dict[str, Any] = {"headers": headers}
        if json is not None:
            request["json"] = json
        for attempt in range(self.retries):
            try:
                async with (await self._http()).stream(method, self.base_url + path, **request) as response:
                    chunks: list[bytes] = []
                    size = 0
                    async for chunk in response.aiter_bytes():
                        size += len(chunk)
                        if size > self.max_response_bytes:
                            raise ProtocolError("resource_exhausted", "HTTP response exceeds configured limit")
                        chunks.append(chunk)
                    raw = b"".join(chunks)
                    status = response.status_code
                    success = response.is_success
                break
            except httpx.TransportError:
                if attempt + 1 == self.retries:
                    raise ProtocolError("unreachable", "HTTP request failed") from None
                await asyncio.sleep(0.1 * (attempt + 1))
        if not success:
            try:
                body = jsonlib.loads(raw)
            except (ValueError, UnicodeDecodeError):
                body = {}
            detail = body.get("error", body) if isinstance(body, dict) else {}
            if not isinstance(detail, dict):
                detail = {}
            message = str(detail.get("message", f"HTTP {status}"))
            if self.token:
                message = message.replace(self.token, "[redacted]")
            raise ProtocolError(str(detail.get("code", "internal")), message)
        if status == 204:
            return None
        try:
            return jsonlib.loads(raw)
        except (ValueError, UnicodeDecodeError) as error:
            raise ProtocolError("internal", "server returned invalid JSON") from error

    async def create_agent(self, request: AgentCreateReq, *, idempotency_key: str | None = None) -> Agent:
        return await self._request("POST", "/v1/agents", json=request, idempotency_key=idempotency_key)

    async def get_agent(self, agent_id: str) -> Agent:
        return await self._request("GET", "/v1/agents/" + quote(agent_id, safe=""))

    async def list_agents(self, *, status: str = "", workspace: str = "", parent: str = "") -> list[Agent]:
        params = httpx.QueryParams({k: v for k, v in {"status": status, "ws": workspace, "parent": parent}.items() if v})
        result = await self._request("GET", "/v1/agents" + ("?" + str(params) if params else ""))
        return result["agents"]

    async def message_agent(self, agent_id: str, text: str, *, kind: str = "", idempotency_key: str | None = None) -> AgentMessageRes:
        return await self._request("POST", f"/v1/agents/{quote(agent_id, safe='')}/messages", json={"text": text, "kind": kind}, idempotency_key=idempotency_key)

    async def fork_agent(self, agent_id: str, request: dict[str, Any] | None = None, *, idempotency_key: str | None = None) -> Agent:
        return await self._request("POST", f"/v1/agents/{quote(agent_id, safe='')}/fork", json=request or {}, idempotency_key=idempotency_key)

    async def agent_action(self, agent_id: str, action: str, *, by: str = "", idempotency_key: str | None = None) -> Agent | None:
        if action not in {"cancel", "sleep", "wake", "destroy"}:
            raise ValueError("unsupported agent action")
        return await self._request("POST", f"/v1/agents/{quote(agent_id, safe='')}/{action}", json={"by": by} if action == "wake" else None, idempotency_key=idempotency_key)

    async def transcript(self, agent_id: str, *, from_index: int = 0, limit: int = 0) -> dict[str, Any]:
        params = httpx.QueryParams({"from": from_index, "limit": limit})
        return await self._request("GET", f"/v1/agents/{quote(agent_id, safe='')}/transcript?{params}")

    async def stream_transcript(self, agent_id: str, *, from_index: int = 0, poll_interval: float = 0.2) -> AsyncIterator[dict[str, Any]]:
        cursor = from_index
        while True:
            try:
                page = await self.transcript(agent_id, from_index=cursor)
            except ProtocolError as error:
                if error.code != "unreachable":
                    raise
                await asyncio.sleep(poll_interval)
                continue
            if page.get("gap"):
                gap = page["gap"]
                raise ProtocolError("evicted", f"transcript records [{gap['from']}, {gap['to']}) were evicted", int(gap["to"]))
            for record in page.get("records", []):
                yield record
            next_cursor = int(page.get("next", cursor))
            if next_cursor < cursor:
                raise ProtocolError("internal", "transcript cursor moved backwards")
            cursor = next_cursor
            if page.get("done"):
                return
            await asyncio.sleep(poll_interval)

    async def list_approvals(self, agent_id: str, *, status: str = "") -> list[Approval]:
        suffix = "?" + str(httpx.QueryParams({"status": status})) if status else ""
        result = await self._request("GET", f"/v1/agents/{quote(agent_id, safe='')}/approvals{suffix}")
        return result["approvals"]

    async def get_approval(self, approval_id: str) -> Approval:
        return await self._request("GET", "/v1/approvals/" + quote(approval_id, safe=""))

    async def decide_approval(self, approval_id: str, decision: ApprovalDecisionInput, *, idempotency_key: str | None = None) -> Approval:
        return await self._request("POST", "/v1/approvals/" + quote(approval_id, safe=""), json=decision, idempotency_key=idempotency_key)

    async def approve(self, approval_id: str, option: str = "allow_once", *, idempotency_key: str | None = None) -> Approval:
        """Choose one offered approval option."""
        return await self.decide_approval(approval_id, {"option": option}, idempotency_key=idempotency_key)

    async def deny(self, approval_id: str, *, idempotency_key: str | None = None) -> Approval:
        """Deny a pending approval."""
        return await self.decide_approval(approval_id, {"denied": True}, idempotency_key=idempotency_key)

    async def wait_for_agent(self, agent_id: str, statuses: set[str], *, timeout: float = 120, poll_interval: float = 0.2) -> Agent:
        """Poll the read-only Agent resource until it reaches one of statuses."""
        try:
            async with asyncio.timeout(timeout):
                while True:
                    agent = await self.get_agent(agent_id)
                    if agent.get("status") in statuses:
                        return agent
                    await asyncio.sleep(poll_interval)
        except TimeoutError:
            raise ProtocolError("timeout", "agent status wait deadline exceeded") from None

    async def wait_for_approval(self, agent_id: str, *, timeout: float = 120, poll_interval: float = 0.2) -> Approval:
        """Poll without waking the Agent until its first pending approval exists."""
        try:
            async with asyncio.timeout(timeout):
                while True:
                    approvals = await self.list_approvals(agent_id)
                    if approvals:
                        return approvals[0]
                    await asyncio.sleep(poll_interval)
        except TimeoutError:
            raise ProtocolError("timeout", "approval wait deadline exceeded") from None

    async def diff(self, agent_id: str, *, wake: bool = False) -> dict[str, Any]:
        """Read the bounded unified diff, optionally waking a sleeping Agent."""
        suffix = "?wake=true" if wake else ""
        return await self._request("GET", f"/v1/agents/{quote(agent_id, safe='')}/diff{suffix}")

    async def connect_terminal(
        self,
        agent_id: str,
        *,
        session: str = "",
        from_seq: int = 0,
        program: list[str] | None = None,
        cwd: str = "",
        rows: int = 24,
        cols: int = 80,
        timeout: float = 30,
    ) -> Terminal:
        """Attach a terminal; reconnect with returned session and next_seq."""
        if timeout <= 0:
            raise ValueError("timeout must be positive")
        query: list[tuple[str, str]] = []
        if session:
            query.extend((("session", session), ("from", str(from_seq))))
        else:
            query.extend(("program", value) for value in (program or ["/bin/sh"]))
            query.extend((("cwd", cwd), ("rows", str(rows)), ("cols", str(cols))))
        base = urlsplit(self.base_url)
        if base.scheme not in {"http", "https", "ws", "wss"} or not base.netloc:
            raise ValueError("Remount URL must use http, https, ws, or wss")
        path = base.path.rstrip("/") + f"/v1/agents/{quote(agent_id, safe='')}/terminal"
        scheme = "wss" if base.scheme in {"https", "wss"} else "ws"
        url = urlunsplit((scheme, base.netloc, path, urlencode(query), ""))
        try:
            socket = await asyncio.wait_for(
                self._terminal_connector(
                    url,
                    additional_headers={"Authorization": f"Bearer {self.token}"},
                    compression=None,
                    max_size=self.max_response_bytes,
                    max_queue=16,
                ),
                timeout=timeout,
            )
        except TimeoutError:
            raise ProtocolError("timeout", "terminal connect deadline exceeded") from None
        except asyncio.CancelledError:
            raise
        except Exception:
            raise ConnectionClosed("terminal connection failed") from None
        return Terminal(socket, timeout=timeout, max_message_bytes=self.max_response_bytes)
