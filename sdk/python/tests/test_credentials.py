"""The brokered-credential surface, against a fake transport.

These tests assert on the wire: the op name, the request body, and what the
caller gets back. That is what makes them a contract check rather than a
restatement of the implementation — the Go control plane is the other half.
"""

import asyncio

import cbor2
import pytest

from remount import Client, CredentialFilter, ProtocolError
from remount.credentials import CREDENTIAL_EVENT_TYPES, decode_credential_event


class FakeSocket:
    """The minimum transport a control-plane call needs: hello, then bodies."""

    def __init__(self, responses=None, error=None):
        self.incoming = asyncio.Queue()
        self.closed = False
        self.sent = []
        self.responses = responses or {}
        self.error = error

    async def send(self, wire):
        frame = cbor2.loads(wire)
        self.sent.append(frame)
        response = {"v": 1, "t": "res", "id": frame["id"], "op": frame.get("op", ""), "from": frame.get("to", "")}
        if frame["t"] == "hello":
            response["body"] = cbor2.dumps({"peer": "client_1", "caps": ["v1"], "controller_epoch": 0})
        elif self.error is not None:
            response["err"] = self.error
        else:
            configured = self.responses.get(frame["op"], {})
            body = configured(frame) if callable(configured) else configured
            response["body"] = cbor2.dumps(body)
        await self.incoming.put(cbor2.dumps(response))

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


def client_for(socket):
    async def connector(*_args, **_kwargs):
        return socket

    return Client("https://cp.example", "token", connector=connector)


def request_for(socket, op):
    for frame in socket.sent:
        if frame.get("op") == op:
            return cbor2.loads(frame["body"])
    raise AssertionError(f"no {op} request was sent: {[f.get('op') for f in socket.sent]}")


@pytest.mark.asyncio
async def test_create_binding_sends_the_spec_and_an_idempotency_key():
    socket = FakeSocket({"binding.create": lambda frame: {"id": "b_openai", "revision": 1}})
    client = client_for(socket)
    created = await client.create_binding({
        "id": "b_openai", "kind": "api_key", "secret": "sk-not-a-real-key",
        "destinations": ["api.openai.com"], "ttl_sec": 900,
    })
    await client.close()

    assert created["id"] == "b_openai"
    body = request_for(socket, "binding.create")
    assert body["binding"]["destinations"] == ["api.openai.com"]
    # Every mutation carries an idempotency key, so a retry is a no-op rather
    # than a second binding.
    assert body["idem"].startswith("idem_")


@pytest.mark.asyncio
async def test_a_caller_supplied_key_makes_a_retry_a_no_op():
    socket = FakeSocket({"binding.create": {"id": "b_x", "revision": 1}})
    client = client_for(socket)
    await client.create_binding({"id": "b_x", "secret": "s", "destinations": ["h"]}, "idem-stable")
    await client.create_binding({"id": "b_x", "secret": "s", "destinations": ["h"]}, "idem-stable")
    await client.close()

    keys = [cbor2.loads(f["body"])["idem"] for f in socket.sent if f.get("op") == "binding.create"]
    assert keys == ["idem-stable", "idem-stable"]


@pytest.mark.asyncio
async def test_list_and_get_bindings_never_ask_for_a_secret():
    socket = FakeSocket({
        "binding.list": {"bindings": [{"id": "b_a", "revision": 2}, {"id": "b_b", "revision": 1}]},
        "binding.get": {"id": "b_a", "revision": 2},
    })
    client = client_for(socket)
    listed = await client.list_bindings(include_revoked=True)
    one = await client.get_binding("b_a")
    await client.close()

    assert [b["id"] for b in listed] == ["b_a", "b_b"]
    assert one["revision"] == 2
    assert request_for(socket, "binding.list")["include_revoked"] is True
    assert request_for(socket, "binding.get") == {"id": "b_a"}


@pytest.mark.asyncio
async def test_rotate_and_revoke_carry_only_what_the_operator_named():
    socket = FakeSocket({
        "binding.rotate": {"id": "b_x", "revision": 2},
        "binding.revoke": {"id": "b_x", "revision": 3, "revoked_at": 1, "revoked_reason": "leaked"},
    })
    client = client_for(socket)
    rotated = await client.rotate_binding("b_x", secret="sk-second-not-a-real-key")
    revoked = await client.revoke_binding("b_x", reason="leaked")
    await client.close()

    assert rotated["revision"] == 2
    assert revoked["revoked_reason"] == "leaked"
    rotate = request_for(socket, "binding.rotate")
    assert rotate["id"] == "b_x" and "source" not in rotate and "tenant" not in rotate
    assert request_for(socket, "binding.revoke")["reason"] == "leaked"


@pytest.mark.asyncio
async def test_create_session_principal_returns_the_token_once():
    socket = FakeSocket({"principal.session.create": {
        "principal": {"id": "a_ephemeral", "roles": ["agent"]},
        "token": "cap_opaque", "expires_at": 1000, "ws": "ws_1", "gen": 4,
    }})
    client = client_for(socket)
    session = await client.create_session_principal("ws_1", roles=["agent"], ttl_sec=900)
    await client.close()

    assert session["token"] == "cap_opaque"
    assert session["gen"] == 4
    body = request_for(socket, "principal.session.create")
    assert body["ws"] == "ws_1" and body["roles"] == ["agent"] and body["ttl_sec"] == 900


@pytest.mark.asyncio
async def test_credential_events_filters_server_side_and_by_decision():
    def audit(decision, binding, host, status):
        return cbor2.dumps({"decision": decision, "binding": binding, "host": host,
                            "method": "GET", "path": "/v1/models", "status": status})

    socket = FakeSocket({"events.tail": {"events": [
        {"seq": 7, "at": 1, "type": "cred.used", "workspace": "ws_1",
         "payload": audit("substituted", "b_openai", "api.openai.com:443", 200)},
        {"seq": 8, "at": 2, "type": "egress.denied", "workspace": "ws_1",
         "payload": audit("leak_blocked", "b_openai", "api.anthropic.com:443", 0)},
        {"seq": 9, "at": 3, "type": "ws.claimed", "workspace": "ws_1"},
    ]}})
    client = client_for(socket)
    every = await client.credential_events(CredentialFilter(ws="ws_1", binding="b_openai", since=1))
    blocked = await client.credential_events(CredentialFilter(ws="ws_1", decision="leak_blocked"))
    await client.close()

    # The workspace lifecycle event is not a credential decision and is dropped.
    assert [e.type for e in every] == ["cred.used", "egress.denied"]
    assert every[0].host == "api.openai.com:443" and every[0].status == 200
    assert [e.decision for e in blocked] == ["leak_blocked"]
    body = request_for(socket, "events.tail")
    assert body["ws"] == "ws_1" and body["binding"] == "b_openai" and body["from"] == 1
    assert body["types"] == list(CREDENTIAL_EVENT_TYPES)


def test_decode_ignores_events_that_are_not_broker_decisions():
    assert decode_credential_event({"seq": 1, "at": 1, "type": "ws.created"}) is None
    # A payload this client cannot decode is still a real decision: the
    # envelope is reported rather than the record being dropped.
    decoded = decode_credential_event({"seq": 2, "at": 1, "type": "egress.denied", "payload": b"\xff\xff"})
    assert decoded is not None and decoded.seq == 2 and decoded.binding == ""


@pytest.mark.asyncio
async def test_protocol_error_exposes_code_and_reason():
    socket = FakeSocket(error={"code": "unauthorized", "msg": "binding was revoked", "reason": "revoked"})
    client = client_for(socket)
    with pytest.raises(ProtocolError) as raised:
        await client.get_binding("b_gone")
    await client.close()

    # Callers match on the code and reason pair, never on the prose.
    assert raised.value.code == "unauthorized"
    assert raised.value.reason == "revoked"
