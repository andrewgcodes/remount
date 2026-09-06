import { Decoder, Encoder } from "cbor-x";
import { describe, expect, it } from "vitest";
import { Client, Computer, MODIFIER_SHIFT } from "../src/index.js";

const encoder = new Encoder({ useRecords: false });
const decoder = new Decoder({ mapsAsObjects: true });

/** A fake node that answers the computer operations and records requests. */
class ComputerSocket {
  listeners = new Map<string, Array<(...args: any[]) => void>>();
  calls: Array<[string, any]> = [];
  constructor(readonly responses: Record<string, any> = {}) {}
  addEventListener(name: string, listener: (...args: any[]) => void) {
    const list = this.listeners.get(name) ?? [];
    list.push(listener);
    this.listeners.set(name, list);
  }
  send(wire: Uint8Array) {
    const frame: any = decoder.decode(wire);
    let body: any = {};
    if (frame.t === "hello") body = { peer: "client_1", caps: ["v1"], controller_epoch: 0n };
    else if (frame.op === "grant") body = { node: "node_1", claims: {} };
    else {
      const request = frame.body ? decoder.decode(frame.body) : {};
      this.calls.push([frame.op, request]);
      const configured = this.responses[frame.op] ?? {};
      body = typeof configured === "function" ? configured(request) : configured;
    }
    this.emit("message", {
      data: encoder.encode({
        v: 1, t: "res", controller_epoch: 0n, id: frame.id,
        op: frame.op ?? "", from: frame.to ?? "", body: encoder.encode(body),
      }),
    });
  }
  close() { this.emit("close", {}); }
  emit(name: string, value: any) { queueMicrotask(() => this.listeners.get(name)?.forEach((l) => l(value))); }
}

function clientFor(socket: ComputerSocket): Client {
  return new Client("https://cp.example", "token", { websocketFactory: async () => socket });
}

describe("computer sessions", () => {
  it("carries the launch and returns a usable handle", async () => {
    const socket = new ComputerSocket({
      "computer.create": { computer: "cmp_1", s: "s_1", cdp: "Chrome/152.0.0.0", viewport: { w: 1024, h: 768 } },
    });
    const client = clientFor(socket);
    const computer = await client.createComputer("ws_1", {
      viewport: { w: 1024, h: 768 }, profile: "research", launch: { port: 9333 },
    });
    expect(computer).toBeInstanceOf(Computer);
    expect([computer.id, computer.session, computer.cdpVersion]).toEqual(["cmp_1", "s_1", "Chrome/152.0.0.0"]);
    expect(computer.viewport).toEqual({ w: 1024, h: 768 });
    const [op, request] = socket.calls[0];
    expect(op).toBe("computer.create");
    expect(request.ws).toBe("ws_1");
    expect(request.profile).toBe("research");
    expect(request.launch).toEqual({ port: 9333 });
    expect(String(request.idem)).toMatch(/^idem_[0-9a-f]{32}$/);
    await client.close();
  });

  it("gives every action batch the next input sequence", async () => {
    const socket = new ComputerSocket({ "computer.input": { applied: true, last_iseq: 0 } });
    const client = clientFor(socket);
    const computer = client.computer("ws_1", "cmp_1");
    await computer.click(10, 20);
    await computer.type("hello");
    await computer.key("Enter", MODIFIER_SHIFT);
    await computer.scroll(1, 2, 0, 120);
    await computer.drag(1, 2, 3, 4);
    const inputs = socket.calls.filter(([op]) => op === "computer.input").map(([, request]) => request);
    expect(inputs.map((request) => Number(request.iseq))).toEqual([1, 2, 3, 4, 5]);
    expect(inputs.map((request) => request.actions[0])).toEqual([
      { kind: "click", x: 10, y: 20, mod: 0 },
      { kind: "type", text: "hello" },
      { kind: "key", key: "Enter", mod: MODIFIER_SHIFT },
      { kind: "scroll", x: 1, y: 2, dx: 0, dy: 120 },
      { kind: "drag", x: 1, y: 2, tox: 3, toy: 4 },
    ]);
    for (const request of inputs) {
      expect([request.ws, request.computer]).toEqual(["ws_1", "cmp_1"]);
    }
    await client.close();
  });

  it("decodes the page's value and never executes it", async () => {
    const socket = new ComputerSocket({
      "computer.eval": { value: new TextEncoder().encode(JSON.stringify({ title: "Remount" })) },
    });
    const client = clientFor(socket);
    expect(await client.computer("ws_1", "cmp_1").eval("document.title")).toEqual({ title: "Remount" });
    expect(socket.calls[0][1].expr).toBe("document.title");
    await client.close();
  });

  it("reports an undefined expression as null", async () => {
    const socket = new ComputerSocket({ "computer.eval": {} });
    const client = clientFor(socket);
    expect(await client.computer("ws_1", "cmp_1").eval("window.missing")).toBeNull();
    await client.close();
  });

  it("lists a blocked download with its reason rather than omitting it", async () => {
    const socket = new ComputerSocket({
      "computer.downloads": {
        downloads: [
          { filename: "report.csv", state: "completed", artifact: `art_sha256:${"0".repeat(64)}` },
          { filename: "big.bin", state: "blocked", reason: "download_blocked" },
        ],
      },
    });
    const client = clientFor(socket);
    const downloads = await client.computer("ws_1", "cmp_1").downloads();
    expect(downloads.map((d) => d.state)).toEqual(["completed", "blocked"]);
    expect(downloads[1].reason).toBe("download_blocked");
    await client.close();
  });

  it("refreshes the handle from get and reports why the browser died", async () => {
    const socket = new ComputerSocket({
      "computer.get": { computer: "cmp_1", state: "closed", reason: "browser_crashed", viewport: { w: 800, h: 600 }, s: "s_9" },
    });
    const client = clientFor(socket);
    const computer = client.computer("ws_1", "cmp_1");
    const state = await computer.get();
    expect([state.state, state.reason]).toEqual(["closed", "browser_crashed"]);
    expect(computer.viewport).toEqual({ w: 800, h: 600 });
    expect(computer.session).toBe("s_9");
    await client.close();
  });

  it("resumes the node's input sequence for a handle addressed by id", async () => {
    // This is the CLI's shape: every invocation builds a fresh handle. A
    // sequence restarted at one is one the node has already applied, so the
    // action would be dropped and nothing would happen.
    const socket = new ComputerSocket({
      "computer.get": { computer: "cmp_1", state: "ready", last_iseq: 7 },
      "computer.input": { applied: true, last_iseq: 8 },
    });
    const client = clientFor(socket);
    const computer = client.computer("ws_1", "cmp_1");
    await computer.click(1, 2);
    await computer.type("x");
    expect(socket.calls.map(([op]) => op)).toEqual(["computer.get", "computer.input", "computer.input"]);
    expect(socket.calls.filter(([op]) => op === "computer.input").map(([, r]) => Number(r.iseq))).toEqual([8, 9]);
    await client.close();
  });

  it("does not make a created handle pay for a sync", async () => {
    const socket = new ComputerSocket({
      "computer.create": { computer: "cmp_1" },
      "computer.input": { applied: true, last_iseq: 1 },
    });
    const client = clientFor(socket);
    const computer = await client.createComputer("ws_1");
    await computer.click(1, 2);
    expect(socket.calls.map(([op]) => op)).toEqual(["computer.create", "computer.input"]);
    await client.close();
  });

  it("carries an idempotency key on navigate and close", async () => {
    const socket = new ComputerSocket({
      "computer.navigate": { url: "https://example.test/", status: "loaded", title: "t" },
      "computer.close": {},
    });
    const client = clientFor(socket);
    const computer = client.computer("ws_1", "cmp_1");
    expect((await computer.navigate("https://example.test/")).status).toBe("loaded");
    await computer.close("close-once");
    const keys = Object.fromEntries(socket.calls.map(([op, request]) => [op, request.idem]));
    expect(String(keys["computer.navigate"])).toMatch(/^idem_[0-9a-f]{32}$/);
    expect(keys["computer.close"]).toBe("close-once");
    await client.close();
  });
});
