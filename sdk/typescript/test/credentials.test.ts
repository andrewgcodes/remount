import { Decoder, Encoder } from "cbor-x";
import { expect, it } from "vitest";
import { Client, CREDENTIAL_EVENT_TYPES, decodeCredentialEvent, ProtocolError } from "../src/index.js";

const encoder = new Encoder({ useRecords: false, variableMapSize: true });
const decoder = new Decoder({ mapsAsObjects: true });


/**
 * The minimum transport a control-plane call needs: hello, then bodies. These
 * tests assert on the wire — the op name, the request body and what the caller
 * gets back — so they are a contract check against the Go control plane rather
 * than a restatement of the implementation.
 */
class FakeSocket {
  listeners = new Map<string, Array<(...args: any[]) => void>>();
  sent: any[] = [];
  constructor(readonly responses: Record<string, any> = {}, readonly error?: Record<string, any>) {}

  addEventListener(name: string, listener: (...args: any[]) => void) {
    const list = this.listeners.get(name) ?? [];
    list.push(listener);
    this.listeners.set(name, list);
    if (name === "open") queueMicrotask(() => listener({}));
  }

  emit(name: string, event: any) {
    for (const listener of this.listeners.get(name) ?? []) listener(event);
  }

  send(wire: Uint8Array) {
    const frame: any = decoder.decode(wire);
    this.sent.push(frame);
    const response: any = { v: 1, t: "res", id: frame.id, op: frame.op ?? "", from: frame.to ?? "" };
    if (frame.t === "hello") {
      response.body = encoder.encode({ peer: "client_1", caps: ["v1"], controller_epoch: 0 });
    } else if (this.error) {
      response.err = this.error;
    } else {
      const configured = this.responses[frame.op] ?? {};
      response.body = encoder.encode(typeof configured === "function" ? configured(frame) : configured);
    }
    queueMicrotask(() => this.emit("message", { data: encoder.encode(response) }));
  }

  close() { this.emit("close", {}); }
}

function clientFor(socket: FakeSocket): Client {
  return new Client("https://cp.example", "token", { websocketFactory: async () => socket as any });
}

function requestFor(socket: FakeSocket, op: string): any {
  const frame = socket.sent.find((sent) => sent.op === op);
  if (!frame) throw new Error(`no ${op} request was sent: ${socket.sent.map((s) => s.op).join(",")}`);
  return decoder.decode(frame.body);
}

it("creates a binding with the spec and an idempotency key", async () => {
  const socket = new FakeSocket({ "binding.create": { id: "b_openai", revision: 1 } });
  const client = clientFor(socket);
  const created = await client.createBinding({
    id: "b_openai", kind: "api_key", secret: "sk-not-a-real-key",
    destinations: ["api.openai.com"], ttl_sec: 900,
  });
  await client.close();

  expect(created.id).toBe("b_openai");
  const body = requestFor(socket, "binding.create");
  expect(body.binding.destinations).toEqual(["api.openai.com"]);
  // Every mutation carries a key, so a retry is a no-op, not a second binding.
  expect(String(body.idem)).toMatch(/^idem_/);
});

it("keeps a caller-supplied idempotency key across a retry", async () => {
  const socket = new FakeSocket({ "binding.create": { id: "b_x", revision: 1 } });
  const client = clientFor(socket);
  await client.createBinding({ id: "b_x", secret: "s", destinations: ["h"] }, "idem-stable");
  await client.createBinding({ id: "b_x", secret: "s", destinations: ["h"] }, "idem-stable");
  await client.close();

  const keys = socket.sent.filter((s) => s.op === "binding.create").map((s) => decoder.decode(s.body).idem);
  expect(keys).toEqual(["idem-stable", "idem-stable"]);
});

it("lists and gets bindings without asking for a secret", async () => {
  const socket = new FakeSocket({
    "binding.list": { bindings: [{ id: "b_a", revision: 2 }, { id: "b_b", revision: 1 }] },
    "binding.get": { id: "b_a", revision: 2 },
  });
  const client = clientFor(socket);
  const listed = await client.listBindings({ includeRevoked: true });
  const one = await client.getBinding("b_a");
  await client.close();

  expect(listed.map((b) => b.id)).toEqual(["b_a", "b_b"]);
  expect(one.revision).toBe(2);
  expect(requestFor(socket, "binding.list").include_revoked).toBe(true);
  expect(requestFor(socket, "binding.get")).toEqual({ id: "b_a" });
});

it("rotates and revokes carrying only what the operator named", async () => {
  const socket = new FakeSocket({
    "binding.rotate": { id: "b_x", revision: 2 },
    "binding.revoke": { id: "b_x", revision: 3, revoked_at: 1, revoked_reason: "leaked" },
  });
  const client = clientFor(socket);
  const rotated = await client.rotateBinding("b_x", { secret: "sk-second-not-a-real-key" });
  const revoked = await client.revokeBinding("b_x", { reason: "leaked" });
  await client.close();

  expect(rotated.revision).toBe(2);
  expect(revoked.revoked_reason).toBe("leaked");
  const rotate = requestFor(socket, "binding.rotate");
  expect(rotate.id).toBe("b_x");
  expect(rotate.source).toBeUndefined();
  expect(requestFor(socket, "binding.revoke").reason).toBe("leaked");
});

it("returns a session principal token once", async () => {
  const socket = new FakeSocket({ "principal.session.create": {
    principal: { id: "a_ephemeral", roles: ["agent"] },
    token: "cap_opaque", expires_at: 1000, ws: "ws_1", gen: 4,
  } });
  const client = clientFor(socket);
  const session = await client.createSessionPrincipal("ws_1", { roles: ["agent"], ttlSec: 900 });
  await client.close();

  expect(session.token).toBe("cap_opaque");
  expect(session.gen).toBe(4);
  const body = requestFor(socket, "principal.session.create");
  expect([body.ws, body.roles, body.ttl_sec]).toEqual(["ws_1", ["agent"], 900]);
});

it("filters credential events server-side and by decision", async () => {
  const audit = (decision: string, binding: string, host: string, status: number) =>
    encoder.encode({ decision, binding, host, method: "GET", path: "/v1/models", status });
  const socket = new FakeSocket({ "events.tail": { events: [
    { seq: 7, at: 1, type: "cred.used", workspace: "ws_1", payload: audit("substituted", "b_openai", "api.openai.com:443", 200) },
    { seq: 8, at: 2, type: "egress.denied", workspace: "ws_1", payload: audit("leak_blocked", "b_openai", "api.anthropic.com:443", 0) },
    { seq: 9, at: 3, type: "ws.claimed", workspace: "ws_1" },
  ] } });
  const client = clientFor(socket);
  const every = await client.credentialEvents({ ws: "ws_1", binding: "b_openai", since: 1 });
  const blocked = await client.credentialEvents({ ws: "ws_1", decision: "leak_blocked" });
  await client.close();

  // The workspace lifecycle event is not a credential decision and is dropped.
  expect(every.map((e) => e.type)).toEqual(["cred.used", "egress.denied"]);
  expect([every[0].host, every[0].status]).toEqual(["api.openai.com:443", 200]);
  expect(blocked.map((e) => e.decision)).toEqual(["leak_blocked"]);
  const body = requestFor(socket, "events.tail");
  expect([body.ws, body.binding, body.from]).toEqual(["ws_1", "b_openai", 1]);
  expect(body.types).toEqual(CREDENTIAL_EVENT_TYPES);
});

it("ignores events that are not broker decisions", () => {
  expect(decodeCredentialEvent({ seq: 1, at: 1, type: "ws.created" })).toBeUndefined();
  // A payload this client cannot decode is still a real decision: the envelope
  // is reported rather than the record being dropped.
  const decoded = decodeCredentialEvent({ seq: 2, at: 1, type: "egress.denied", payload: new Uint8Array([0xff, 0xff]) });
  expect(decoded?.seq).toBe(2);
  expect(decoded?.binding).toBe("");
});

it("exposes the code and reason of a refusal", async () => {
  const socket = new FakeSocket({}, { code: "unauthorized", msg: "binding was revoked", reason: "revoked" });
  const client = clientFor(socket);
  // Callers match on the code and reason pair, never on the prose.
  await expect(client.getBinding("b_gone")).rejects.toMatchObject({ code: "unauthorized", reason: "revoked" });
  await expect(client.getBinding("b_gone")).rejects.toBeInstanceOf(ProtocolError);
  await client.close();
});
