"""Browser and computer-use sessions over the Remount port substrate.

A computer is a Chrome DevTools Protocol conversation the node holds with a
browser inside the workspace; nothing here talks to the browser directly. See
ADR 0088 for why the node owns that conversation.

Coordinates are CSS pixels with the origin at the top-left of the viewport
declared at ``create``, which is the same rectangle a screenshot is clipped to.
"""

from __future__ import annotations

import asyncio
import json
import secrets
from typing import TYPE_CHECKING, Any, cast

from .types import (
    ComputerAction,
    ComputerDownload,
    ComputerGetRes,
    ComputerLaunch,
    ComputerNavigateRes,
    ComputerScreenshotRes,
    ComputerViewport,
)

if TYPE_CHECKING:  # pragma: no cover - import cycle guard, not behaviour
    from .client import Client

#: The DevTools port a computer uses when ``create`` names none.
DEFAULT_PORT = 9222
#: The viewport a computer uses when ``create`` declares none.
DEFAULT_VIEWPORT: ComputerViewport = {"w": 1280, "h": 720}
#: The profile directory name a computer uses when ``create`` names none.
DEFAULT_PROFILE = "default"

# Modifier bits for an action; the values are CDP's own.
MODIFIER_ALT = 1
MODIFIER_CTRL = 2
MODIFIER_META = 4
MODIFIER_SHIFT = 8

# Action kinds accepted by computer.input.
ACTION_CLICK = "click"
ACTION_TYPE = "type"
ACTION_KEY = "key"
ACTION_SCROLL = "scroll"
ACTION_DRAG = "drag"
ACTION_MOVE = "move"


def _key() -> str:
    return "idem_" + secrets.token_hex(16)


class Computer:
    """One browser conversation, addressed by workspace and computer id.

    The input sequence this handle keeps is what makes a retried action batch
    idempotent: the node drops any sequence at or below the last it applied, so
    a click replayed after a dropped connection is never a second click.
    """

    def __init__(
        self,
        client: "Client",
        workspace: str,
        computer_id: str,
        *,
        session: str = "",
        cdp_version: str = "",
        viewport: ComputerViewport | None = None,
    ):
        self.client = client
        self.workspace = workspace
        self.id = computer_id
        self.session = session
        self.cdp_version = cdp_version
        self.viewport: ComputerViewport = viewport or {}
        self._iseq = 0
        # A handle addressed by id has to learn the node's input sequence
        # before it acts. A sequence restarted at one is one the node has
        # already applied, and the action would be correctly dropped.
        self._synced = False
        self._input_lock = asyncio.Lock()

    @classmethod
    async def create(
        cls,
        client: "Client",
        workspace: str,
        *,
        launch: ComputerLaunch | None = None,
        viewport: ComputerViewport | None = None,
        profile: str = "",
        env: dict[str, str] | None = None,
        idempotency_key: str | None = None,
    ) -> "Computer":
        """Start, or attach to, a browser in the workspace."""
        body: dict[str, Any] = {"ws": workspace, "idem": idempotency_key or _key()}
        if launch is not None:
            body["launch"] = launch
        if viewport is not None:
            body["viewport"] = viewport
        if profile:
            body["profile"] = profile
        if env:
            body["env"] = env
        response = await client._node_call(workspace, "computer.create", body)
        computer = cls(
            client,
            workspace,
            str(response["computer"]),
            session=str(response.get("s", "")),
            cdp_version=str(response.get("cdp", "")),
            viewport=cast(ComputerViewport, response.get("viewport") or {}),
        )
        computer._synced = True
        return computer

    async def get(self) -> ComputerGetRes:
        """Report the computer's state, so a reconnecting caller can tell a
        live browser from one that crashed while it was away."""
        response = await self._call("computer.get", {})
        self.viewport = cast(ComputerViewport, response.get("viewport") or self.viewport)
        self.session = str(response.get("s", self.session))
        async with self._input_lock:
            self._iseq = max(self._iseq, int(response.get("last_iseq", 0) or 0))
            self._synced = True
        return cast(ComputerGetRes, response)

    async def screenshot(self) -> ComputerScreenshotRes:
        """Capture the viewport as a PNG."""
        return cast(ComputerScreenshotRes, await self._call("computer.screenshot", {}))

    async def input(self, *actions: ComputerAction) -> None:
        """Apply a batch of actions under one input sequence.

        The batch is validated whole by the node, so a malformed action cannot
        leave half a batch applied. A handle that did not create the computer
        reads the node's sequence first: without that, every such handle would
        start at one and every action after the first would be dropped.
        """
        if not self._synced:
            await self.get()
        async with self._input_lock:
            self._iseq += 1
            iseq = self._iseq
        await self._call("computer.input", {"iseq": iseq, "actions": list(actions)})

    async def click(self, x: int, y: int, *, modifiers: int = 0) -> None:
        """Press and release the left button at a viewport coordinate."""
        await self.input({"kind": ACTION_CLICK, "x": x, "y": y, "mod": modifiers})

    async def move(self, x: int, y: int) -> None:
        """Move the pointer without pressing a button."""
        await self.input({"kind": ACTION_MOVE, "x": x, "y": y})

    async def type(self, text: str) -> None:
        """Insert text into whatever has focus.

        This is ``Input.insertText``, the one path that behaves the same in an
        ordinary field and a contenteditable region and does not depend on a
        keyboard layout the workspace image may not carry.
        """
        await self.input({"kind": ACTION_TYPE, "text": text})

    async def key(self, key: str, *, modifiers: int = 0) -> None:
        """Press one named key, such as ``Enter`` or ``ArrowDown``."""
        await self.input({"kind": ACTION_KEY, "key": key, "mod": modifiers})

    async def scroll(self, x: int, y: int, dx: int, dy: int) -> None:
        """Dispatch a wheel event at a viewport coordinate."""
        await self.input({"kind": ACTION_SCROLL, "x": x, "y": y, "dx": dx, "dy": dy})

    async def drag(self, x: int, y: int, to_x: int, to_y: int) -> None:
        """Press at one coordinate, move, and release at another."""
        await self.input({"kind": ACTION_DRAG, "x": x, "y": y, "tox": to_x, "toy": to_y})

    async def navigate(
        self, url: str, *, idempotency_key: str | None = None
    ) -> ComputerNavigateRes:
        """Load a URL and wait for the page's load event or the node's timeout.

        The response says which of the two happened. A destination the egress
        policy refuses raises ``ProtocolError`` with code ``denied`` and reason
        ``navigation_denied``; that policy is per host, never per URL, because
        the broker's CONNECT tunnel is opaque by design.
        """
        body = {"url": url, "idem": idempotency_key or _key()}
        return cast(ComputerNavigateRes, await self._call("computer.navigate", body))

    async def eval(self, expression: str) -> Any:
        """Run an expression in the page and return its decoded JSON value.

        The value is page-controlled data: decode it, never execute it.
        """
        response = await self._call("computer.eval", {"expr": expression})
        raw = response.get("value")
        if not raw:
            return None
        return json.loads(bytes(raw))

    async def downloads(self) -> list[ComputerDownload]:
        """List what the browser fetched, and the artifact each became.

        A file that finished but could not be published is reported here with
        state ``blocked`` and a reason, never omitted.
        """
        response = await self._call("computer.downloads", {})
        return cast(list[ComputerDownload], response.get("downloads") or [])

    async def close(self, *, idempotency_key: str | None = None) -> None:
        """End the conversation and kill a browser the node spawned.

        Closing a computer that is already gone succeeds: that is the
        postcondition the caller asked for.
        """
        await self._call("computer.close", {"idem": idempotency_key or _key()})

    async def _call(self, op: str, body: dict[str, Any]) -> dict[str, Any]:
        request = dict(body)
        request["ws"] = self.workspace
        request["computer"] = self.id
        return await self.client._node_call(self.workspace, op, request)
