"""Brokered credentials: bindings, session principals, and the audit trail.

The workspace never holds a provider key. An operator defines a *binding* —
a credential plus the destinations, methods, paths and substitution location it
may be spent on — and the workspace receives only an opaque placeholder. The
node's broker substitutes the real value at the network edge and records the
decision.

These operations are mixed into :class:`remount.Client`, so they share its one
reconnecting connection and its idempotency conventions:

.. code-block:: python

    await client.create_binding({
        "id": "b_openai", "kind": "api_key", "secret": key,
        "destinations": ["api.openai.com"], "ttl_sec": 900,
    })
    await client.create_workspace({
        "bindings": ["b_openai"],
        "env": {"OPENAI_API_KEY": "ref:b_openai",
                "OPENAI_BASE_URL": "${REMOUNT_BROKER}/d/api.openai.com/v1"},
    })

``secret`` is write-only. It is accepted by ``create_binding`` and
``rotate_binding`` and never appears in a response, an event, or a diagnostic,
so nothing here ever returns a credential.
"""

from __future__ import annotations

import secrets
from dataclasses import dataclass, field
from typing import Any, Protocol, cast

import cbor2

from .types import BindingSpec, Principal, PrincipalSessionCreateRes

# The audit events the broker emits per request. A credential value is never
# part of any of them.
CREDENTIAL_EVENT_TYPES = ("cred.used", "egress.allowed", "egress.denied", "egress.redacted")

# The lifecycle events a binding emits. They stream under the binding id
# rather than a workspace, so they are read from the tenant-wide stream.
BINDING_EVENT_TYPES = ("binding.created", "binding.rotated", "binding.revoked")


def _idempotency_key() -> str:
    return "idem_" + secrets.token_hex(16)


@dataclass(frozen=True)
class CredentialFilter:
    """Selects credential-use and egress-decision events.

    Every field is optional. ``ws``, ``binding`` and ``host`` are applied by
    the control plane, so setting them is materially cheaper than reading the
    whole log; ``decision`` is matched here against the audit payload.
    """

    ws: str = ""
    binding: str = ""
    host: str = ""
    decision: str = ""
    since: int = 0


@dataclass(frozen=True)
class CredentialEvent:
    """One recorded broker decision.

    It answers the audit question in one shape: who used which binding, for
    which workspace, against which host, with what outcome, under which tenant,
    at what time. It never carries a credential value.
    """

    seq: int
    at: int
    type: str
    tenant: str = ""
    ws: str = ""
    generation: int = 0
    node: str = ""
    principal: str = ""
    binding: str = ""
    host: str = ""
    method: str = ""
    path: str = ""
    decision: str = ""
    reason: str = ""
    status: int = 0
    event: dict[str, Any] = field(default_factory=dict)


def decode_credential_event(event: dict[str, Any]) -> CredentialEvent | None:
    """Decode one log entry, or return None when it is not a broker decision."""
    if str(event.get("type", "")) not in CREDENTIAL_EVENT_TYPES:
        return None
    audit: dict[str, Any] = {}
    payload = event.get("payload")
    if payload:
        try:
            decoded = cbor2.loads(payload)
            if isinstance(decoded, dict):
                audit = decoded
        except Exception:
            # A payload this client cannot decode is still a real decision.
            # Report what the envelope says rather than dropping the record.
            audit = {}
    return CredentialEvent(
        seq=int(event.get("seq", 0)),
        at=int(event.get("at", 0)),
        type=str(event.get("type", "")),
        tenant=str(event.get("tenant", "")),
        ws=str(event.get("workspace") or event.get("stream") or ""),
        generation=int(event.get("generation") or audit.get("generation") or 0),
        node=str(event.get("node", "")),
        principal=str(event.get("principal", "")),
        binding=str(audit.get("binding", "")),
        host=str(audit.get("host", "")),
        method=str(audit.get("method", "")),
        path=str(audit.get("path", "")),
        decision=str(audit.get("decision", "")),
        reason=str(audit.get("reason", "")),
        status=int(audit.get("status", 0) or 0),
        event=event,
    )


class _Caller(Protocol):
    async def call(self, op: str, body: dict[str, Any] | None = None, *, to: str = ...) -> dict[str, Any]:
        ...


class CredentialOperations:
    """The brokered-credential surface, mixed into :class:`remount.Client`."""

    async def create_binding(
        self: _Caller, binding: dict[str, Any], idempotency_key: str | None = None
    ) -> BindingSpec:
        """Define a tenant-scoped brokered credential.

        ``binding["secret"]`` is write-only: it is accepted here and never
        returned by any later call. Name ``binding["source"]`` instead to keep
        the credential in an external manager; exactly one of the two is set.
        """
        response = await self.call(
            "binding.create",
            {"binding": binding, "idem": idempotency_key or _idempotency_key()},
        )
        return cast(BindingSpec, response)

    async def list_bindings(
        self: _Caller, tenant: str = "", include_revoked: bool = False
    ) -> list[BindingSpec]:
        """Return the bindings visible to the caller's tenant, without secrets.

        Revoked rows are retained so an audit can still resolve a binding id
        seen in an older event; they are returned only on request.
        """
        body: dict[str, Any] = {}
        if tenant:
            body["tenant"] = tenant
        if include_revoked:
            body["include_revoked"] = True
        response = await self.call("binding.list", body)
        return cast(list[BindingSpec], response.get("bindings", []))

    async def get_binding(self: _Caller, binding_id: str, tenant: str = "") -> BindingSpec:
        """Return one binding definition without its secret."""
        body: dict[str, Any] = {"id": binding_id}
        if tenant:
            body["tenant"] = tenant
        return cast(BindingSpec, await self.call("binding.get", body))

    async def rotate_binding(
        self: _Caller,
        binding_id: str,
        *,
        secret: str = "",
        source: str = "",
        tenant: str = "",
        idempotency_key: str | None = None,
    ) -> BindingSpec:
        """Replace the credential behind a binding and bump its revision.

        A node holding a lease minted from the previous revision re-leases
        within one renew interval, after which the old secret is no longer
        substituted. Exactly one of ``secret`` and ``source`` is set.
        """
        body: dict[str, Any] = {"id": binding_id, "idem": idempotency_key or _idempotency_key()}
        if tenant:
            body["tenant"] = tenant
        if secret:
            body["secret"] = secret
        if source:
            body["source"] = source
        return cast(BindingSpec, await self.call("binding.rotate", body))

    async def revoke_binding(
        self: _Caller,
        binding_id: str,
        *,
        reason: str = "",
        tenant: str = "",
        idempotency_key: str | None = None,
    ) -> BindingSpec:
        """Permanently stop substitution for a binding.

        This is not provider-side revocation. Remount stops substituting the
        credential within one renew interval; the credential itself stays valid
        at the provider until it is rotated or deleted there.
        """
        body: dict[str, Any] = {"id": binding_id, "idem": idempotency_key or _idempotency_key()}
        if tenant:
            body["tenant"] = tenant
        if reason:
            body["reason"] = reason
        return cast(BindingSpec, await self.call("binding.revoke", body))

    async def create_session_principal(
        self: _Caller,
        workspace: str,
        *,
        subject: str = "",
        roles: list[str] | None = None,
        tenant: str = "",
        ttl_sec: int = 0,
        idempotency_key: str | None = None,
    ) -> PrincipalSessionCreateRes:
        """Mint an ephemeral principal and its generation-bound capability.

        The returned ``token`` is delivered exactly once and is never written to
        durable control state. It stops verifying when the workspace moves, when
        the TTL passes, or when the principal is revoked.
        """
        body: dict[str, Any] = {"ws": workspace, "idem": idempotency_key or _idempotency_key()}
        if tenant:
            body["tenant"] = tenant
        if subject:
            body["subject"] = subject
        if roles:
            body["roles"] = roles
        if ttl_sec:
            body["ttl_sec"] = ttl_sec
        return cast(PrincipalSessionCreateRes, await self.call("principal.session.create", body))

    async def revoke_principal(
        self: _Caller, principal: str, *, tenant: str = "", idempotency_key: str | None = None
    ) -> int:
        """Invalidate every credential and live workspace authority of a principal."""
        body: dict[str, Any] = {"principal": principal, "idem": idempotency_key or _idempotency_key()}
        if tenant:
            body["tenant"] = tenant
        response = await self.call("principal.revoke", body)
        return int(response.get("revision", 0))

    async def credential_events(
        self: _Caller, filter: CredentialFilter | None = None
    ) -> list[CredentialEvent]:
        """Return the credential-use audit trail matching ``filter``.

        The binding, host and workspace filters are applied by the control
        plane; the decision filter is applied here because it is a field of the
        audit payload rather than of the log envelope.
        """
        selector = filter or CredentialFilter()
        body: dict[str, Any] = {"from": selector.since, "types": list(CREDENTIAL_EVENT_TYPES)}
        if selector.ws:
            body["ws"] = selector.ws
        if selector.binding:
            body["binding"] = selector.binding
        if selector.host:
            body["host"] = selector.host
        response = await self.call("events.tail", body)
        out: list[CredentialEvent] = []
        for raw in response.get("events", []) or []:
            decoded = decode_credential_event(raw)
            if decoded is None:
                continue
            if selector.decision and decoded.decision != selector.decision:
                continue
            if selector.host and decoded.host.lower() != selector.host.lower():
                continue
            out.append(decoded)
        return out


__all__ = [
    "BINDING_EVENT_TYPES",
    "CREDENTIAL_EVENT_TYPES",
    "CredentialEvent",
    "CredentialFilter",
    "CredentialOperations",
    "Principal",
    "decode_credential_event",
]
