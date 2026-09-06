import { Decoder, Encoder } from "cbor-x";
import { readFileSync } from "node:fs";
import { createHash } from "node:crypto";
import { describe, expect, it } from "vitest";
import { AgentClient, Client, encodeProtocol, ProtocolError, Session } from "../src/index.js";
import { OPERATIONS } from "../src/types.js";

const encoder = new Encoder({ useRecords: false });
const decoder = new Decoder({ mapsAsObjects: true });

it("decodes the Go protocol golden fixture", () => {
  const hex = readFileSync(new URL("../../../internal/proto/testdata/v1-request.hex", import.meta.url), "utf8").trim();
  const frame: any = decoder.decode(Uint8Array.from(Buffer.from(hex, "hex")));
  expect([frame.v, frame.t, frame.id, frame.op]).toEqual([1, "req", 7, "ws.get"]);
  expect((decoder.decode(frame.body) as any).id).toBe("ws_fixture");
  const encoded = encodeProtocol({ v: 1, t: "req", id: 7, to: "control", op: "ws.get", body: encodeProtocol({ id: "ws_fixture" }) });
  expect(Buffer.from(encoded).toString("hex")).toBe(hex);
});

class FakeSocket {
  listeners = new Map<string, Array<(...args: any[]) => void>>();
  sent: any[] = [];
  attached?: () => void;
  caps = ["v1"];
  epoch = 0n;
  earlyFrames: any[] = [];
  addEventListener(name: string, listener: (...args: any[]) => void) {
    const list = this.listeners.get(name) ?? [];
    list.push(listener);
    this.listeners.set(name, list);
  }
  send(wire: Uint8Array) {
    const frame: any = decoder.decode(wire);
    this.sent.push(frame);
    let body: any = {};
    if (frame.t === "hello") body = { peer: "client_1", caps: this.caps, controller_epoch: this.epoch };
    else if (frame.op === "grant") body = { node: "node_1", claims: {} };
    else if (frame.op === "s.open") {
      for (const frame of this.earlyFrames) this.emit("message", { data: encoder.encode(frame) });
      this.emit("message", { data: encoder.encode({ v: 1, t: "chunk", controller_epoch: this.epoch, from: "node_1", ws: "ws_1", s: "s_1", seq: 0, body: encoder.encode({ st: 4, d: new Uint8Array([1]) }) }) });
      body = { s: "s_1", next: 1 };
    }
    else if (frame.op === "s.attach") { this.attached?.(); body = { s: "s_1", next: 4 }; }
    this.emit("message", { data: encoder.encode({ v: 1, t: "res", controller_epoch: this.epoch, id: frame.id, op: frame.op ?? "", from: frame.to ?? "", body: encoder.encode(body) }) });
  }
  close() { this.emit("close", {}); }
  emit(name: string, value: any) { queueMicrotask(() => this.listeners.get(name)?.forEach((listener) => listener(value))); }
}

describe("session cursor", () => {
  it("does not consume an input sequence on failure", async () => {
    const client = new Client("https://cp.example", "token");
    const calls: number[] = [];
    client.nodeCall = async (_workspace, _op, body) => {
      calls.push(body.iseq);
      if (calls.length === 1) throw new Error("cut");
      return {};
    };
    const session = new Session(client, "ws_1", "exec", "s_1");
    await expect(session.input(new Uint8Array([1]))).rejects.toThrow("cut");
    await session.input(new Uint8Array([1]));
    expect(calls).toEqual([1, 1]);
  });

  it("keeps a chunk that races ahead of the open response", async () => {
    const socket = new FakeSocket();
    const client = new Client("https://cp.example", "token", { websocketFactory: async () => socket });
    const session = await client.exec("ws_1", ["true"]);
    const first = await session[Symbol.asyncIterator]().next();
    expect(first.value).toEqual({ seq: 0, stream: 4, data: new Uint8Array([1]) });
    expect(decoder.decode(socket.sent[0].body).caps).toEqual(["v1", "authz-push", "controller-epoch", "session-cap", "chunked-artifacts", "tiered-session-logs", "release-epoch", "identity-admin"]);
    await client.close();
  });

  it("bounds a peer that never answers a request", async () => {
    const socket = new FakeSocket();
    const send = socket.send.bind(socket);
    socket.send = (wire) => {
      const frame: any = decoder.decode(wire);
      if (frame.t === "hello") send(wire);
    };
    const client = new Client("https://cp.example", "token", { websocketFactory: async () => socket, retries: 1, requestTimeoutMilliseconds: 10 });
    await expect(client.call("ws.list")).rejects.toThrow("deadline exceeded");
    await client.close();
  });

  it("supervises a disconnect and reattaches from the next sequence", async () => {
    const first = new FakeSocket(); const second = new FakeSocket();
    const sockets = [first, second];
    const client = new Client("https://cp.example", "token", { websocketFactory: async () => sockets.shift()! });
    await client.connect();
    const session = new Session(client, "ws_1", "exec", "s_1"); session.nextSeq = 4;
    (client as any).sessions.set(session.id, session);
    const attached = new Promise<void>((resolve) => { second.attached = resolve; });
    first.close();
    await attached;
    const request = second.sent.find((frame) => frame.op === "s.attach");
    expect(decoder.decode(request.body).from).toBe(4);
    await client.close();
  });

  it("reorders and deduplicates chunks", async () => {
    const client = new Client("https://cp.example", "token", { fetch: async () => new Response() });
    const session = new Session(client, "ws_1", "exec", "s_1");
    await session.accept({ seq: 1, body: encoder.encode({ st: 1, d: new Uint8Array([98]) }) });
    await session.accept({ seq: 0, body: encoder.encode({ st: 1, d: new Uint8Array([97]) }) });
    await session.accept({ seq: 1, body: encoder.encode({ st: 1, d: new Uint8Array([98]) }) });
    await session.accept({ seq: 2, body: encoder.encode({ st: 3, d: encoder.encode({ code: 0 }) }) });
    const seen = [];
    for await (const chunk of session) seen.push(chunk.seq);
    expect(seen).toEqual([0, 1, 2]);
    expect(session.exit).toEqual({ code: 0 });
  });

  it("advances across an explicit gap without hiding it", async () => {
    const client = new Client("https://cp.example", "token");
    const session = new Session(client, "ws_1", "exec", "s_1");
    await session.accept({ seq: 4, body: encoder.encode({ st: 1, d: new Uint8Array([4]) }) });
    await session.accept({ seq: 0, body: encoder.encode({ st: 5, d: encoder.encode({ from: 0, to: 3 }) }) });
    const iterator = session[Symbol.asyncIterator]();
    expect((await iterator.next()).value?.stream).toBe(5);
    expect((await iterator.next()).value).toEqual({ seq: 4, stream: 1, data: new Uint8Array([4]) });
  });

  it("rejects a pending consumer when the session fails", async () => {
    const client = new Client("https://cp.example", "token");
    const session = new Session(client, "ws_1", "exec", "s_1");
    const pending = session[Symbol.asyncIterator]().next();
    session.fail(new ProtocolError("resource_exhausted", "queue full"));
    await expect(pending).rejects.toEqual(expect.objectContaining<Partial<ProtocolError>>({ code: "resource_exhausted" }));
  });

  it("rejects non-byte chunk data without allocating from its value", async () => {
    const client = new Client("https://cp.example", "token");
    const session = new Session(client, "ws_1", "exec", "s_1");
    await expect(session.accept({ seq: 0, body: encoder.encode({ st: 1, d: Number.MAX_SAFE_INTEGER }) })).rejects.toEqual(expect.objectContaining<Partial<ProtocolError>>({ code: "bad_request" }));
  });
});

describe("protocol authority", () => {
  it("encodes small controller epochs as canonical unsigned integers", async () => {
    const socket = new FakeSocket(); socket.caps = ["v1", "controller-epoch"]; socket.epoch = 7n;
    const client = new Client("https://cp.example", "token", { websocketFactory: async () => socket });
    await client.call("ws.list");
    expect(socket.sent[1].controller_epoch).toBe(7);
    await client.close();
  });

  it("charges orphan routing metadata to the byte limit", async () => {
    const client = new Client("https://cp.example", "token");
    const socket = new FakeSocket();
    (client as any).socket = socket;
    await (client as any).handleChunk({ s: "s_1", from: "node_1", ws: "x".repeat(9 << 20), body: new Uint8Array() });
    expect((client as any).orphans.size).toBe(0);
    expect((client as any).orphanBytes).toBe(0);
    await client.close();
  });

  it("keeps early output when an open retry reconnects and clears grants", async () => {
    const first = new FakeSocket(), second = new FakeSocket();
    const sockets = [first, second];
    const original = first.send.bind(first);
    first.send = (wire) => {
      if (decoder.decode(wire).op === "s.open") { first.close(); return; }
      original(wire);
    };
    const client = new Client("https://cp.example", "token", { websocketFactory: async () => sockets.shift()! });
    try {
      const session = await client.exec("ws_1", ["true"]);
      expect(client.generation).toBe(2);
      expect((client as any).grants.size).toBe(0);
      expect((await session[Symbol.asyncIterator]().next()).value?.stream).toBe(4);
    } finally { await client.close(); }
  });

  it("rejects forged early/live chunks and updates the producer on reattach", async () => {
    const socket = new FakeSocket();
    const chunk = (from: string, ws = "ws_1", seq = 0) => ({ v: 1, t: "chunk", from, ws, s: "s_1", seq, body: encoder.encode({ st: 3, d: encoder.encode({ code: 99 }) }) });
    socket.earlyFrames = [chunk(""), chunk("client_evil"), chunk("node_evil"), chunk("node_1", "ws_other")];
    const client = new Client("https://cp.example", "token", { websocketFactory: async () => socket });
    const session = await client.exec("ws_1", ["cat"]);
    expect((await session[Symbol.asyncIterator]().next()).value?.stream).toBe(4);
    expect(session.exit).toBeUndefined();
    for (const from of ["", "client_evil", "node_evil"]) await (client as any).handleChunk(chunk(from, "ws_1", 1));
    session.bindNode("node_new");
    await (client as any).handleChunk(chunk("node_1", "ws_1", 1));
    expect(session.exit).toBeUndefined();
    await (client as any).handleChunk(chunk("node_new", "ws_1", 1));
    expect(session.exit?.code).toBe(99);
    await client.close();
  });

  it("fences stale responses/chunks and stamps exact uint64 epochs", async () => {
    const socket = new FakeSocket();
    socket.caps = ["v1", "controller-epoch"];
    socket.epoch = 9007199254740993n;
    socket.earlyFrames = [{ v: 1, t: "chunk", controller_epoch: socket.epoch - 1n, from: "node_1", ws: "ws_1", s: "s_1", seq: 0, body: encoder.encode({ st: 3, d: encoder.encode({ code: 99 }) }) }];
    const original = socket.send.bind(socket);
    socket.send = (wire) => {
      const request: any = decoder.decode(wire);
      if (request.op === "ws.list") {
        for (const [from, epoch] of [["control", socket.epoch - 1n], ["", socket.epoch], ["client_evil", socket.epoch]]) {
          socket.emit("message", { data: encoder.encode({ v: 1, t: "res", from, controller_epoch: epoch, op: request.op, id: request.id, body: encoder.encode({ forged: true }) }) });
        }
      }
      original(wire);
    };
    const client = new Client("https://cp.example", "token", { websocketFactory: async () => socket });
    expect(await client.call("ws.list")).toEqual({});
    const session = await client.exec("ws_1", ["true"]);
    expect((await session[Symbol.asyncIterator]().next()).value?.stream).toBe(4);
    for (const request of socket.sent.slice(1)) expect(request.controller_epoch).toBe(socket.epoch);
    await client.close();
  });

  it("refuses a negotiated epoch with no nonzero value", async () => {
    const socket = new FakeSocket(); socket.caps = ["v1", "controller-epoch"];
    const client = new Client("https://cp.example", "token", { websocketFactory: async () => socket });
    await expect(client.connect()).rejects.toMatchObject({ code: "conflict" });
    await client.close();
  });
});

describe("workspace lifecycle", () => {
  const lease = { id: "lease_1", ws: "ws_1", gen: 3, max_alive_until: 1800, on_expiry: "sleep", created_at: 0 };

  // Answers every control op with one fixed body so a request body can be read back off the wire.
  function replySocket(body: Record<string, any>): FakeSocket {
    const socket = new FakeSocket();
    const hello = socket.send.bind(socket);
    socket.send = (wire: Uint8Array) => {
      const frame: any = decoder.decode(wire);
      if (frame.t === "hello") { hello(wire); return; }
      socket.sent.push(frame);
      socket.emit("message", { data: encoder.encode({ v: 1, t: "res", controller_epoch: socket.epoch, id: frame.id, op: frame.op ?? "", from: frame.to ?? "", body: encoder.encode(body) }) });
    };
    return socket;
  }

  function sentBody(socket: FakeSocket, op: string, index = 0): any {
    return decoder.decode(socket.sent.filter((frame) => frame.op === op)[index].body);
  }

  it("takes a durable lease and states the expiry action on the wire", async () => {
    const socket = replySocket(lease);
    const client = new Client("https://cp.example", "token", { websocketFactory: async () => socket });
    expect(await client.leaseWorkspace("ws_1", { maxAliveSec: 1800, onExpiry: "destroy", reason: "batch" })).toEqual(lease);
    const request = sentBody(socket, "ws.lease");
    expect(request.idem).toMatch(/^idem_[0-9a-f]{32}$/);
    delete request.idem;
    expect(request).toEqual({ id: "ws_1", max_alive_sec: 1800, on_expiry: "destroy", reason: "batch" });
    await client.leaseWorkspace("ws_1", { maxAliveSec: 60, minAliveSec: 30, idempotencyKey: "stable" });
    expect(sentBody(socket, "ws.lease", 1)).toEqual({ id: "ws_1", max_alive_sec: 60, min_alive_sec: 30, on_expiry: "sleep", idem: "stable" });
    await client.close();
  });

  it("renews a held lease against its lease id", async () => {
    const socket = replySocket(lease);
    const client = new Client("https://cp.example", "token", { websocketFactory: async () => socket });
    expect(await client.renewLease("ws_1", "lease_1", { extendSec: 600 })).toEqual(lease);
    const request = sentBody(socket, "ws.lease.renew");
    expect(request.idem).toMatch(/^idem_[0-9a-f]{32}$/);
    delete request.idem;
    expect(request).toEqual({ id: "ws_1", lease: "lease_1", extend_sec: 600 });
    await client.close();
  });

  it("marks activity explicitly in both directions", async () => {
    const socket = replySocket({ id: "ws_1" });
    const client = new Client("https://cp.example", "token", { websocketFactory: async () => socket });
    await client.markIdle("ws_1", "turn ended");
    await client.markActive("ws_1");
    const idled = sentBody(socket, "ws.idle.mark");
    expect(idled.idle).toBe(true); expect(idled.reason).toBe("turn ended");
    const active = sentBody(socket, "ws.idle.mark", 1);
    // An absent flag would decode as the false default, so activity must be on the wire, not implied.
    expect("idle" in active).toBe(true);
    expect(active.idle).toBe(false); expect(active.reason).toBeUndefined();
    await client.close();
  });

  it("refuses lifecycle durations the control plane would reject", () => {
    const client = new Client("https://cp.example", "token");
    expect(() => client.leaseWorkspace("ws_1", { maxAliveSec: 0 })).toThrow("maxAliveSec must be a positive whole number of seconds");
    expect(() => client.leaseWorkspace("ws_1", { maxAliveSec: 60, minAliveSec: -1 })).toThrow("minAliveSec must be a non-negative whole number of seconds");
    expect(() => client.leaseWorkspace("ws_1", { maxAliveSec: 60, onExpiry: "terminate" as any })).toThrow('onExpiry must be "sleep" or "destroy"');
    expect(() => client.renewLease("ws_1", "lease_1", { extendSec: 0 })).toThrow("extendSec must be a positive whole number of seconds");
    expect(() => client.setIdlePolicy("ws_1", { destroyAfterSec: -30 })).toThrow("destroyAfterSec must be a non-negative whole number of seconds");
    // A fractional duration would encode as a CBOR float and the control
    // plane's int64 decode would reject it with a message about a field the
    // caller never typed, so it is refused here instead.
    expect(() => client.leaseWorkspace("ws_1", { maxAliveSec: 1.5 })).toThrow("maxAliveSec must be a positive whole number of seconds");
  });

  it("reads the lease and its durable deadline without an idempotency key", async () => {
    const socket = replySocket({ lease, deadline: { at: 1800, action: "sleep", source: "lease" } });
    const client = new Client("https://cp.example", "token", { websocketFactory: async () => socket });
    const held = await client.getLease("ws_1");
    expect(held.lease!.id).toBe("lease_1");
    expect(held.deadline!.action).toBe("sleep");
    expect(sentBody(socket, "ws.lease.get")).toEqual({ id: "ws_1" });
    await client.close();
  });
});

describe("artifact transfer", () => {
  const payload = new TextEncoder().encode("nonempty artifact");
  const id = `art_sha256:${createHash("sha256").update(payload).digest("hex")}`;
  it("downloads a successful nonempty upload without cancelling its body", async () => {
    let uploaded: Uint8Array | undefined;
    const client = new Client("https://cp.example", "token", { fetch: async (_url, options) => {
      if (options?.method === "PUT") { uploaded = options.body as Uint8Array; return new Response(null, { status: 201 }); }
      return new Response(uploaded);
    } });
    expect(await client.uploadArtifact(payload)).toEqual({ id, size: payload.length });
    expect(await client.downloadArtifact(id)).toEqual(payload);
  });
  it("still detects corruption and refuses oversized downloads", async () => {
    const client = new Client("https://cp.example", "token", { fetch: async () => new Response(payload) });
    await expect(client.downloadArtifact(id, 1)).rejects.toMatchObject({ code: "resource_exhausted" });
    await expect(client.downloadArtifact(`art_sha256:${"0".repeat(64)}`)).rejects.toMatchObject({ code: "conflict" });
  });
  it("preserves HTTP error codes while redacting credentials", async () => {
    const client = new Client("https://cp.example", "secret-canary", { fetch: async () => new Response(JSON.stringify({ error: { code: "not_found", message: "secret-canary missing" } }), { status: 404 }) });
    await expect(client.downloadArtifact(id)).rejects.toMatchObject({ code: "not_found", message: "not_found: [redacted] missing" });
  });
});

describe("agent HTTP client", () => {
  it("authenticates mutations and forwards a stable idempotency key", async () => {
    let request: Request | undefined;
    const fetcher: typeof fetch = async (input, init) => {
      request = new Request(input, init);
      return new Response(JSON.stringify({ id: "ag_1" }), { status: 201, headers: { "Content-Type": "application/json" } });
    };
    const client = new AgentClient("https://cp.example", "secret", { fetch: fetcher });
    await client.createAgent({ spec: { recipe: "custom" } }, "stable");
    expect(request!.headers.get("authorization")).toBe("Bearer secret");
    expect(request!.headers.get("idempotency-key")).toBe("stable");
  });

  it("reports transcript eviction instead of completing", async () => {
    const fetcher: typeof fetch = async () => new Response(JSON.stringify({ records: [], next: 4, gap: { from: 0, to: 4 } }), { status: 200 });
    const client = new AgentClient("https://cp.example", "token", { fetch: fetcher });
    const iterator = client.streamTranscript("ag_1");
    await expect(iterator.next()).rejects.toEqual(expect.objectContaining<Partial<ProtocolError>>({ code: "evicted" }));
  });

  it("redacts a credential canary from HTTP errors", async () => {
    const canary = "sdk-secret-canary";
    const fetcher: typeof fetch = async () => new Response(JSON.stringify({ error: { code: "denied", message: `failed ${canary}` } }), { status: 403 });
    const client = new AgentClient("https://cp.example", canary, { fetch: fetcher });
    try {
      await client.getAgent("ag_1");
      throw new Error("expected failure");
    } catch (error) {
      expect(String(error)).not.toContain(canary);
      expect(error).toEqual(expect.objectContaining<Partial<ProtocolError>>({ code: "denied" }));
    }
  });

  it("bounds HTTP response bodies", async () => {
    const fetcher: typeof fetch = async () => new Response("12345", { status: 200 });
    const client = new AgentClient("https://cp.example", "token", { fetch: fetcher, maxResponseBytes: 4 });
    await expect(client.getAgent("ag_1")).rejects.toEqual(expect.objectContaining<Partial<ProtocolError>>({ code: "resource_exhausted" }));
  });

  it("reuses the idempotency key after a network failure", async () => {
    const keys: Array<string | null> = [];
    const fetcher: typeof fetch = async (input, init) => {
      const request = new Request(input, init);
      keys.push(request.headers.get("idempotency-key"));
      if (keys.length === 1) throw new TypeError("cut");
      return new Response(JSON.stringify({ id: "ag_1" }), { status: 201 });
    };
    const client = new AgentClient("https://cp.example", "token", { fetch: fetcher });
    await client.createAgent({ spec: { recipe: "custom" } });
    expect(keys).toHaveLength(2);
    expect(keys[0]).toBe(keys[1]);
  });

  it("keeps diff reads non-waking unless requested", async () => {
    const urls: string[] = [];
    const fetcher: typeof fetch = async (input) => {
      urls.push(String(input));
      return new Response(JSON.stringify({ status: " M file", diff: "patch", truncated: false }), { status: 200 });
    };
    const client = new AgentClient("https://cp.example", "token", { fetch: fetcher });
    expect((await client.diff("ag_1")).diff).toBe("patch");
    await client.diff("ag_1", true);
    expect(urls[0]).toMatch(/\/v1\/agents\/ag_1\/diff$/);
    expect(urls[1]).toMatch(/\/v1\/agents\/ag_1\/diff\?wake=true$/);
  });

  it("provides idempotent approval decision helpers", async () => {
    const bodies: unknown[] = [];
    const fetcher: typeof fetch = async (_input, init) => {
      bodies.push(JSON.parse(String(init?.body)));
      return new Response(JSON.stringify({ id: "ap_1", status: "decided" }), { status: 200 });
    };
    const client = new AgentClient("https://cp.example", "token", { fetch: fetcher });
    await client.approve("ap_1", "allow_once", "approve-once");
    await client.deny("ap_2", "deny-once");
    expect(bodies).toEqual([{ option: "allow_once" }, { denied: true }]);
  });

  it("bounds a wait for an approval that never arrives", async () => {
    const fetcher: typeof fetch = async () => new Response(JSON.stringify({ approvals: [] }), { status: 200 });
    const client = new AgentClient("https://cp.example", "token", { fetch: fetcher });
    await expect(client.waitForApproval("ag_1", { timeoutMilliseconds: 1, pollMilliseconds: 5 })).rejects.toEqual(expect.objectContaining<Partial<ProtocolError>>({ code: "timeout" }));
  });

  it("resumes a transcript from the same cursor after a disconnect", async () => {
    let calls = 0;
    const fetcher: typeof fetch = async () => {
      calls++;
      if (calls === 1) throw new TypeError("cut");
      return new Response(JSON.stringify({ records: [{ index: 4 }], next: 5, done: true }), { status: 200 });
    };
    const client = new AgentClient("https://cp.example", "token", { fetch: fetcher, retries: 1 });
    const record = await client.streamTranscript("ag_1", { from: 4, pollMilliseconds: 0 }).next();
    expect(record.value.index).toBe(4); expect(calls).toBe(2);
  });
});

class FakeTerminalSocket {
  listeners = new Map<string, Array<(...args: any[]) => void>>();
  sent: Array<string | Uint8Array> = [];
  closed = false;
  addEventListener(name: string, listener: (...args: any[]) => void) {
    const listeners = this.listeners.get(name) ?? []; listeners.push(listener); this.listeners.set(name, listeners);
  }
  send(value: string | Uint8Array) { this.sent.push(value); }
  close() { this.closed = true; this.emit("close", {}); }
  emit(name: string, value: any) { for (const listener of this.listeners.get(name) ?? []) listener(value); }
}

it("attaches an authenticated terminal and tracks its replay cursor", async () => {
  const socket = new FakeTerminalSocket();
  let seenURL = ""; let seenProtocol = "";
  const client = new AgentClient("https://cp.example", "secret", {
    terminalWebSocketFactory: async (url, protocol) => { seenURL = url; seenProtocol = protocol; return socket; },
  });
  const terminal = await client.connectTerminal("ag_1", { session: "s_1", from: 4 });
  socket.emit("message", { data: '{"type":"open","session":"s_1","next":4}' });
  socket.emit("message", { data: new Uint8Array([1, 2]) });
  socket.emit("message", { data: '{"type":"gap","from":5,"to":7}' });
  terminal.resize(30, 100); terminal.input(new Uint8Array([3]));
  socket.emit("message", { data: '{"type":"exit","code":0}' });
  const events = [];
  for await (const event of terminal) events.push(event.type);
  expect(events).toEqual(["open", "data", "gap", "exit"]);
  expect(terminal.nextSeq).toBe(9);
  expect(JSON.parse(String(socket.sent[0]))).toEqual({ type: "resize", rows: 30, cols: 100 });
  expect(socket.sent[1]).toEqual(new Uint8Array([3]));
  expect(seenURL).toBe("wss://cp.example/v1/agents/ag_1/terminal?session=s_1&from=4");
  expect(Buffer.from(seenProtocol.slice("remount.bearer.".length), "base64url").toString()).toBe("secret");
  terminal.close(); expect(socket.closed).toBe(true);
});

it("refuses terminal output before the open event", async () => {
  const socket = new FakeTerminalSocket();
  const client = new AgentClient("https://cp.example", "token", { terminalWebSocketFactory: async () => socket });
  const terminal = await client.connectTerminal("ag_1");
  const pending = terminal[Symbol.asyncIterator]().next();
  socket.emit("message", { data: new Uint8Array([1]) });
  await expect(pending).rejects.toEqual(expect.objectContaining<Partial<ProtocolError>>({ code: "bad_request" }));
});

it("preserves artifact error codes without leaking the token", async () => {
  const canary = "artifact-secret-canary";
  const fetcher: typeof fetch = async () => new Response(JSON.stringify({ error: { code: "not_found", message: `missing ${canary}` } }), { status: 404 });
  const client = new Client("https://cp.example", canary, { fetch: fetcher });
  try {
    await client.downloadArtifact(`art_sha256:${"0".repeat(64)}`);
    throw new Error("expected failure");
  } catch (error) {
    expect(error).toEqual(expect.objectContaining<Partial<ProtocolError>>({ code: "not_found" }));
    expect(String(error)).not.toContain(canary);
  }
});

it("contains the generated op table", () => {
  expect(OPERATIONS["ws.create"].constant).toBe("OpWSCreate");
});
