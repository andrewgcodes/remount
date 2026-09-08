"""Typed Remount protocol errors.

A wire error carries a stable ``code`` and, when the code alone is ambiguous, a
stable ``reason`` that refines it (``proto.Reason*`` in the Go tree). This
module gives every reason one exception class so a caller can write

    try:
        await client.run(workspace, ["curl", "https://api.example.com"])
    except remount.errors.EgressDenied as denied:
        ...

instead of comparing strings. Every class subclasses :class:`ProtocolError`, so
existing ``except ProtocolError`` handlers keep working and an unknown reason —
a newer server than this package — degrades to the base class rather than to an
unhandled case.

The reason vocabulary itself lives in the generated ``types`` module, so the
table below is checked against the protocol rather than maintained beside it.
"""

from __future__ import annotations

from typing import ClassVar

from .types import REASONS

__all__ = [
    "ApprovalRequired",
    "BackendUnsupported",
    "BindingMissing",
    "BrowserCrashed",
    "DisplayUnavailable",
    "DownloadBlocked",
    "EgressDenied",
    "GenerationMismatch",
    "GrantExpired",
    "InputRejected",
    "LifecycleDeadlineExpired",
    "NavigationDenied",
    "OutputEvicted",
    "PermissionDenied",
    "ProfileCorrupt",
    "ProfileUnschedulable",
    "ProtocolError",
    "QuotaExceeded",
    "Revoked",
    "WorkspaceMoved",
    "WorkspaceNotReady",
    "error_for",
    "raise_for",
]


class ProtocolError(Exception):
    """A stable Remount protocol error.

    ``code`` is the stable classification every peer sends. ``reason`` refines
    it and may be empty; match on ``code`` first and narrow on ``reason`` only
    when the distinction matters. ``oldest`` is set with the ``evicted`` code
    and names the oldest sequence still replayable.
    """

    #: The reason this subclass represents. Empty on the base class.
    REASON: ClassVar[str] = ""

    def __init__(self, code: str, message: str = "", oldest: int = 0, reason: str = ""):
        self.code = code
        self.message = message
        self.oldest = oldest
        self.reason = reason or type(self).REASON
        super().__init__(f"{code}: {message}" if message else code)


class PermissionDenied(ProtocolError):
    """The principal lacks a role or tenant scope."""

    REASON = "permission_denied"


class EgressDenied(ProtocolError):
    """The broker refused a destination or a placeholder."""

    REASON = "egress_denied"


class ApprovalRequired(ProtocolError):
    """Approve-mode egress is waiting for a durable approval decision."""

    REASON = "approval_required"


class BindingMissing(ProtocolError):
    """No credential binding covers the placeholder that was used."""

    REASON = "binding_missing"


class GrantExpired(ProtocolError):
    """The grant or capability TTL passed; fetch a fresh one."""

    REASON = "grant_expired"


class Revoked(ProtocolError):
    """The principal, binding or grant was revoked."""

    REASON = "revoked"


class QuotaExceeded(ProtocolError):
    """A tenant quota or hard budget is spent."""

    REASON = "quota_exceeded"


class WorkspaceNotReady(ProtocolError):
    """The workspace has not reached ``ws.ready`` yet."""

    REASON = "workspace_not_ready"


class NeedsContainment(ProtocolError):
    """A failed workspace is destroyed through fleet quarantine, not ``ws.destroy``."""

    REASON = "needs_containment"


class WorkspaceMoved(ProtocolError):
    """The workspace now lives on another node."""

    REASON = "workspace_moved"


class GenerationMismatch(ProtocolError):
    """The request names a stale workspace generation or controller epoch."""

    REASON = "generation_mismatch"


class BackendUnsupported(ProtocolError):
    """The workspace backend cannot perform this operation."""

    REASON = "backend_unsupported"


class OutputEvicted(ProtocolError):
    """The requested session range is no longer retained."""

    REASON = "output_evicted"


class LifecycleDeadlineExpired(ProtocolError):
    """A lease or idle deadline already fired."""

    REASON = "lifecycle_deadline_expired"


class BrowserCrashed(ProtocolError):
    """The computer session's browser exited."""

    REASON = "browser_crashed"


class DisplayUnavailable(ProtocolError):
    """No display or CDP endpoint could be reached."""

    REASON = "display_unavailable"


class InputRejected(ProtocolError):
    """A computer input action was malformed."""

    REASON = "input_rejected"


class NavigationDenied(ProtocolError):
    """Egress policy blocked a browser navigation."""

    REASON = "navigation_denied"


class ProfileCorrupt(ProtocolError):
    """A persisted browser profile could not be opened."""

    REASON = "profile_corrupt"


class DownloadBlocked(ProtocolError):
    """Policy refused a browser download."""

    REASON = "download_blocked"


class ProfileUnschedulable(ProtocolError):
    """No node currently satisfies the requested runtime profile."""

    REASON = "profile_unschedulable"


_BY_REASON: dict[str, type[ProtocolError]] = {
    cls.REASON: cls
    for cls in (
        ApprovalRequired,
        BackendUnsupported,
        BindingMissing,
        BrowserCrashed,
        DisplayUnavailable,
        DownloadBlocked,
        EgressDenied,
        GenerationMismatch,
        GrantExpired,
        InputRejected,
        LifecycleDeadlineExpired,
        NavigationDenied,
        NeedsContainment,
        OutputEvicted,
        PermissionDenied,
        ProfileCorrupt,
        ProfileUnschedulable,
        QuotaExceeded,
        Revoked,
        WorkspaceMoved,
        WorkspaceNotReady,
    )
}


def error_for(code: str, reason: str = "", message: str = "", oldest: int = 0) -> ProtocolError:
    """Build the typed error for a wire failure.

    An empty or unrecognised ``reason`` yields the base :class:`ProtocolError`
    carrying the reason verbatim, so a newer server never turns into a
    ``KeyError`` in an older client.
    """
    cls = _BY_REASON.get(reason, ProtocolError)
    return cls(code, message, oldest, reason)


def raise_for(code: str, reason: str = "", message: str = "", oldest: int = 0) -> None:
    """Raise the typed error for a wire failure. Never returns."""
    raise error_for(code, reason, message, oldest)


def classes() -> dict[str, type[ProtocolError]]:
    """Return a copy of the reason-to-class table, for tests and tooling."""
    return dict(_BY_REASON)


def unmapped_reasons() -> tuple[str, ...]:
    """Return protocol reasons this package has no class for.

    The generated ``REASONS`` tuple is the protocol's own vocabulary, so this
    is empty in a consistent build and is what the package test asserts on.
    """
    return tuple(reason for reason in REASONS if reason not in _BY_REASON)
