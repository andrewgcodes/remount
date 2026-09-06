import { Decoder, Encoder } from "cbor-x";
import type { Workspace, WorkspaceLease, WSLeaseRes } from "./types.js";

const encoder = new Encoder({ useRecords: false, variableMapSize: true });
const decoder = new Decoder({ mapsAsObjects: true });

const MAX_FRAME_BYTES = 16 << 20;
const MAX_PENDING = 4096;
const MAX_ORPHAN_SESSIONS = 256;
const MAX_ORPHAN_CHUNKS = 16_384;
const MAX_ORPHAN_BYTES = 16 << 20;
const MAX_REORDER_CHUNKS = 4096;
const MAX_REORDER_BYTES = 16 << 20;
const MAX_DELIVERY_BYTES = 16 << 20;
const MAX_DELIVERY_WAITERS = 1024;
const STREAM_EXIT = 3;
const STREAM_GAP = 5;
// Node-owned enforcement (revocation, session capabilities and release epochs)
// stays on the node. Clients fence frames, verify artifact bytes and preserve gaps.
const PEER_CAPABILITIES = ["v1", "authz-push", "controller-epoch", "session-cap", "chunked-artifacts", "tiered-session-logs", "release-epoch", "identity-admin"];

type Frame = Record<string, any>;

function orphanFrameBytes(sessionID: string, frame: Frame): number {
  // Routing metadata must not bypass the byte bound with an empty body.
  return bytes(frame.body).byteLength + 2 * (sessionID.length + frame.from.length + frame.ws.length);
}

function wireEpoch(value: unknown): bigint {
  if (value === undefined) return 0n;
  if ((typeof value !== "bigint" && (typeof value !== "number" || !Number.isSafeInteger(value))) ||
      BigInt(value) < 0n || BigInt(value) > 0xffffffffffffffffn) {
    throw new ProtocolError("bad_request", "invalid controller epoch");
  }
  return BigInt(value);
}

function compareBytes(left: Uint8Array, right: Uint8Array): number {
  const length = Math.min(left.byteLength, right.byteLength);
  for (let i = 0; i < length; i++) if (left[i] !== right[i]) return left[i] - right[i];
  return left.byteLength - right.byteLength;
}

function canonical(value: any): any {
  if (Array.isArray(value)) return value.map(canonical);
  if (value && typeof value === "object" && !(value instanceof Uint8Array) && !(value instanceof ArrayBuffer) && !ArrayBuffer.isView(value)) {
    const result: Record<string, any> = {};
    for (const key of Object.keys(value).sort((left, right) => compareBytes(encoder.encode(left), encoder.encode(right)))) result[key] = canonical(value[key]);
    return result;
  }
  return value;
}

/** Encode a value with Remount's RFC 8949 deterministic CBOR profile. */
export function encodeProtocol(value: unknown): Uint8Array {
  return encoder.encode(canonical(value));
}

/** Decode one CBOR protocol value. */
export function decodeProtocol(value: Uint8Array): unknown {
  return decoder.decode(value);
}

interface WebSocketLike {
  binaryType?: string;
  send(data: Uint8Array): void;
  close(): void;
  addEventListener?(name: string, listener: (event: any) => void): void;
  on?(name: string, listener: (...args: any[]) => void): void;
}

export type WebSocketFactory = (url: string) => Promise<WebSocketLike>;

export class ProtocolError extends Error {
  constructor(public readonly code: string, message = "", public readonly oldest = 0) {
    super(message ? `${code}: ${message}` : code);
    this.name = "ProtocolError";
  }
}

export class ConnectionClosed extends Error {}

export interface Chunk {
  seq: number;
  stream: number;
  data: Uint8Array;
}

function idem(): string {
  return `idem_${globalThis.crypto.randomUUID().replaceAll("-", "")}`;
}

// Lifecycle durations are wire seconds: reject locally what the control plane would reject.
function seconds(name: string, value: number, positive: boolean): void {
  if (!Number.isInteger(value) || value < 0 || (positive && value === 0)) throw new Error(`${name} must be a ${positive ? "positive" : "non-negative"} whole number of seconds`);
}

async function sha256(data: Uint8Array): Promise<string> {
  const digest = new Uint8Array(await globalThis.crypto.subtle.digest("SHA-256", Uint8Array.from(data).buffer));
  return Array.from(digest, (value) => value.toString(16).padStart(2, "0")).join("");
}

function websocketURL(baseURL: string): string {
  const url = new URL(baseURL);
  if (url.protocol === "http:") url.protocol = "ws:";
  else if (url.protocol === "https:") url.protocol = "wss:";
  else if (url.protocol !== "ws:" && url.protocol !== "wss:") throw new Error("Remount URL must use http, https, ws, or wss");
  if (!url.pathname.replace(/\/$/, "").endsWith("/v1/link")) url.pathname = `${url.pathname.replace(/\/$/, "")}/v1/link`;
  return url.toString();
}

async function defaultWebSocketFactory(url: string): Promise<WebSocketLike> {
  const Constructor: any = globalThis.WebSocket ?? (await import("ws")).default;
  return await new Promise((resolve, reject) => {
    const socket: WebSocketLike = new Constructor(url);
    socket.binaryType = "arraybuffer";
    const open = () => resolve(socket);
    const error = (event: any) => reject(event?.error ?? new ConnectionClosed("WebSocket handshake failed"));
    if (socket.addEventListener) {
      socket.addEventListener("open", open);
      socket.addEventListener("error", error);
    } else {
      socket.on!("open", open);
      socket.on!("error", error);
    }
  });
}

function listen(socket: WebSocketLike, name: string, listener: (...args: any[]) => void): void {
  if (socket.addEventListener) socket.addEventListener(name, listener);
  else socket.on!(name, listener);
}

function bytes(message: any): Uint8Array {
  const value = message?.data ?? message;
  if (value instanceof Uint8Array) return value;
  if (value instanceof ArrayBuffer) return new Uint8Array(value);
  if (ArrayBuffer.isView(value)) return new Uint8Array(value.buffer, value.byteOffset, value.byteLength);
  throw new ProtocolError("bad_request", "text WebSocket frame");
}

export class Session implements AsyncIterable<Chunk> {
  private node = "";

  /** Bind output to the producer authorized by the control-issued grant. */
  bindNode(node: string): void { this.node = node; }

  /** Authenticate both live and buffered frames before interpreting their body. */
  async acceptFromNode(frame: Frame): Promise<void> {
    if (this.node && frame.from === this.node && frame.ws === this.workspace) {
      await this.accept({ seq: frame.seq, body: frame.body });
    }
  }
  readonly chunks: Chunk[] = [];
  nextSeq = 0;
  lastInputSeq = 0;
  exit?: Record<string, any>;
  error?: Error;
  private waiters: Array<{ resolve: (value: IteratorResult<Chunk>) => void; reject: (error: Error) => void }> = [];
  private pending = new Map<number, { frame: Frame; bytes: number }>();
  private pendingBytes = 0;
  private chunkBytes = 0;
  private closed = false;
  private attaching?: Promise<void>;
  private inputTail: Promise<void> = Promise.resolve();

  constructor(readonly client: Client, readonly workspace: string, readonly kind: string, public id: string) {}

  [Symbol.asyncIterator](): AsyncIterator<Chunk> {
    return { next: () => this.next() };
  }

  private next(): Promise<IteratorResult<Chunk>> {
    if (this.chunks.length) {
      const chunk = this.chunks.shift()!;
      this.chunkBytes -= chunk.data.byteLength;
      return Promise.resolve({ value: chunk, done: false });
    }
    if (this.closed) {
      if (this.error) return Promise.reject(this.error);
      return Promise.resolve({ value: undefined, done: true });
    }
    if (this.waiters.length >= MAX_DELIVERY_WAITERS) {
      const error = new ProtocolError("resource_exhausted", "too many pending session consumers");
      this.fail(error);
      return Promise.reject(error);
    }
    return new Promise((resolve, reject) => this.waiters.push({ resolve, reject }));
  }

  async accept(frame: Frame): Promise<void> {
    if (this.closed) return;
    const seq = Number(frame.seq ?? 0);
    const rawBody = frame.body ? bytes(frame.body) : new Uint8Array();
    const body: any = rawBody.length ? decoder.decode(rawBody) : {};
    const stream = Number(body.st ?? 0);
    const data = body.d ? bytes(body.d) : new Uint8Array();
    if (stream === STREAM_GAP) {
      const gap: any = decoder.decode(data);
      this.nextSeq = Math.max(this.nextSeq, Number(gap.to) + 1);
      for (const [pendingSeq, saved] of this.pending) {
        if (pendingSeq < this.nextSeq) { this.pending.delete(pendingSeq); this.pendingBytes -= saved.bytes; }
      }
      if (!this.push({ seq, stream, data })) return;
      while (!this.closed && this.pending.has(this.nextSeq)) {
        const saved = this.pending.get(this.nextSeq)!;
        this.pending.delete(this.nextSeq); this.pendingBytes -= saved.bytes;
        const decoded: any = decoder.decode(bytes(saved.frame.body));
        this.deliver(this.nextSeq, Number(decoded.st ?? 0), decoded.d ? bytes(decoded.d) : new Uint8Array());
      }
      return;
    }
    if (seq < this.nextSeq) return;
    if (seq > this.nextSeq) {
      if (!this.pending.has(seq)) {
        this.pending.set(seq, { frame, bytes: rawBody.length });
        this.pendingBytes += rawBody.length;
      }
      if (this.pending.size > MAX_REORDER_CHUNKS || this.pendingBytes > MAX_REORDER_BYTES) this.fail(new ProtocolError("resource_exhausted", "session reorder buffer is full"));
      return;
    }
    this.deliver(seq, stream, data);
    while (!this.closed && this.pending.has(this.nextSeq)) {
      const saved = this.pending.get(this.nextSeq)!;
      this.pending.delete(this.nextSeq);
      this.pendingBytes -= saved.bytes;
      const decoded: any = decoder.decode(bytes(saved.frame.body));
      this.deliver(this.nextSeq, Number(decoded.st ?? 0), decoded.d ? bytes(decoded.d) : new Uint8Array());
    }
  }

  private deliver(seq: number, stream: number, data: Uint8Array): void {
    this.nextSeq = seq + 1;
    if (!this.push({ seq, stream, data })) return;
    if (stream === STREAM_EXIT) {
      this.exit = decoder.decode(data) as Record<string, any>;
      this.close();
      this.client.removeSession(this.id);
    }
  }

  private push(chunk: Chunk): boolean {
    const waiter = this.waiters.shift();
    if (waiter) waiter.resolve({ value: chunk, done: false });
    else {
      if (this.chunks.length >= 1024 || this.chunkBytes + chunk.data.byteLength > MAX_DELIVERY_BYTES) {
        this.fail(new ProtocolError("resource_exhausted", "session delivery queue is full"));
        return false;
      }
      this.chunks.push(chunk);
      this.chunkBytes += chunk.data.byteLength;
    }
    return true;
  }

  private close(): void {
    this.closed = true;
    for (const waiter of this.waiters.splice(0)) {
      if (this.error) waiter.reject(this.error);
      else waiter.resolve({ value: undefined, done: true });
    }
  }

  fail(error: Error): void {
    if (this.closed) return;
    this.error = error;
    this.close();
    this.client.removeSession(this.id);
  }

  reattach(generation: number): Promise<void> {
    const previous = this.attaching ?? Promise.resolve();
    const current = previous.catch(() => undefined).then(() => this.doReattach(generation));
    this.attaching = current;
    current.then(
      () => { if (this.attaching === current) this.attaching = undefined; },
      () => { if (this.attaching === current) this.attaching = undefined; },
    );
    return current;
  }

  private async doReattach(generation: number): Promise<void> {
    let delay = 100;
    for (let attempt = 0; attempt < 8 && !this.closed && this.client.generation === generation; attempt++) {
      try {
        const response = await this.client.nodeCall(this.workspace, "s.attach", { s: this.id, from: this.nextSeq }, (node) => this.bindNode(node));
        const serverSequence = Number(response.last_iseq ?? 0);
        const seed = this.inputTail.then(() => { this.lastInputSeq = Math.max(this.lastInputSeq, serverSequence); });
        this.inputTail = seed.catch(() => undefined);
        await seed;
        return;
      } catch (error) {
        if (error instanceof ProtocolError && error.code === "not_found") { this.fail(error); return; }
      }
      await new Promise((resolve) => setTimeout(resolve, delay));
      delay = Math.min(delay * 2, 2000);
    }
    if (!this.closed && this.client.generation === generation) this.fail(new ConnectionClosed("session reattach retries exhausted"));
  }

  async input(data = new Uint8Array(), eof = false): Promise<void> {
    const operation = this.inputTail.then(async () => {
      const sequence = this.lastInputSeq + 1;
      await this.client.nodeCall(this.workspace, "s.input", { s: this.id, iseq: sequence, d: data, eof });
      this.lastInputSeq = sequence;
    });
    this.inputTail = operation.catch(() => undefined);
    return operation;
  }
}

export interface ClientOptions {
  principal?: string;
  retries?: number;
  maxFrameBytes?: number;
  requestTimeoutMilliseconds?: number;
  websocketFactory?: WebSocketFactory;
  fetch?: typeof fetch;
}

export class Client {
  readonly baseURL: string;
  readonly token: string;
  readonly principal: string;
  readonly retries: number;
  readonly maxFrameBytes: number;
  readonly requestTimeoutMilliseconds: number;
  generation = 0;
  peerID = "";
  private factory: WebSocketFactory;
  private fetcher: typeof fetch;
  private socket?: WebSocketLike;
  private connecting?: Promise<void>;
  private requestID = 0;
  private controllerEpoch = 0n;
  private pending = new Map<number, { resolve: (frame: Frame) => void; reject: (error: Error) => void; from: string; op: string; hello: boolean }>();
  private sessions = new Map<string, Session>();
  private orphans = new Map<string, Frame[]>();
  private orphanCount = 0;
  private orphanBytes = 0;
  private grants = new Map<string, Record<string, any>>();
  private closed = false;

  constructor(baseURL: string, token: string, options: ClientOptions = {}) {
    this.baseURL = baseURL.replace(/\/$/, "");
    this.token = token;
    this.principal = options.principal ?? "";
    this.retries = options.retries ?? 5;
    this.maxFrameBytes = options.maxFrameBytes ?? MAX_FRAME_BYTES;
    this.requestTimeoutMilliseconds = options.requestTimeoutMilliseconds ?? 30_000;
    if (this.retries < 1 || this.retries > 10) throw new Error("retries must be between 1 and 10");
    if (this.requestTimeoutMilliseconds <= 0) throw new Error("requestTimeoutMilliseconds must be positive");
    if (this.maxFrameBytes < 1) throw new Error("maxFrameBytes must be positive");
    this.factory = options.websocketFactory ?? defaultWebSocketFactory;
    this.fetcher = options.fetch ?? globalThis.fetch.bind(globalThis);
  }

  async connect(): Promise<void> {
    if (this.closed) throw new ConnectionClosed("client closed");
    if (this.connecting) return this.connecting;
    if (this.socket) return;
    this.connecting = this.doConnect().finally(() => { this.connecting = undefined; });
    return this.connecting;
  }

  private async doConnect(): Promise<void> {
    const pendingSocket = this.factory(websocketURL(this.baseURL));
    let timedOut = false;
    let timerHandle: ReturnType<typeof setTimeout>;
    const timer = new Promise<never>((_, reject) => {
      timerHandle = setTimeout(() => {
        timedOut = true;
        reject(new ConnectionClosed("WebSocket connect deadline exceeded"));
      }, this.requestTimeoutMilliseconds);
    });
    pendingSocket.then((late) => { if (timedOut) late.close(); }, () => undefined);
    let socket: WebSocketLike;
    try { socket = await Promise.race([pendingSocket, timer]); }
    finally { clearTimeout(timerHandle!); }
    this.socket = socket;
    this.controllerEpoch = 0n;
    listen(socket, "message", (message) => {
      if (this.socket !== socket) return;
      void this.receive(message).catch((error) => {
        socket.close();
        this.disconnected(socket, error instanceof Error ? error : new ProtocolError("bad_request", "invalid wire frame"));
      });
    });
    listen(socket, "close", () => this.disconnected(socket, new ConnectionClosed("connection closed")));
    listen(socket, "error", (event) => this.disconnected(socket, event?.error ?? new ConnectionClosed("connection failed")));
    try {
      const response = await this.roundTrip({ v: 1, t: "hello", body: encodeProtocol({ peer: this.peerID, role: "client", token: this.token, principal: this.principal, caps: PEER_CAPABILITIES }) });
      const hello: any = response.body ? decoder.decode(bytes(response.body)) : {};
      if (!hello.caps?.includes("v1")) throw new ProtocolError("unsupported", "server did not negotiate v1");
      this.peerID = hello.peer ?? "";
      this.generation++;
      this.grants.clear();
      this.orphans.clear();
      this.orphanCount = 0;
      this.orphanBytes = 0;
      for (const session of this.sessions.values()) void session.reattach(this.generation);
    } catch (error) {
      socket.close();
      if (this.socket === socket) this.socket = undefined;
      throw error;
    }
  }

  async close(): Promise<void> {
    this.closed = true;
    this.socket?.close();
    this.socket = undefined;
    for (const session of this.sessions.values()) session.fail(new ConnectionClosed("client closed"));
    this.sessions.clear();
  }

  private disconnected(socket: WebSocketLike, error: Error): void {
    if (this.socket !== socket) return;
    this.socket = undefined;
    for (const pending of this.pending.values()) pending.reject(error);
    this.pending.clear();
    if (!this.closed && this.sessions.size) void this.supervise();
  }

  private async supervise(): Promise<void> {
    let delay = 100;
    while (!this.closed && this.sessions.size && !this.socket) {
      try { await this.connect(); return; } catch { /* retry */ }
      await new Promise((resolve) => setTimeout(resolve, delay));
      delay = Math.min(delay * 2, 5000);
    }
  }

  private async receive(message: any): Promise<void> {
    const data = bytes(message);
    if (data.byteLength > this.maxFrameBytes) { this.socket?.close(); return; }
    const frame: Frame = decoder.decode(data) as Frame;
    if (Number(frame.v) !== 1) { this.socket?.close(); return; }
    const pending = this.pending.get(Number(frame.id ?? 0));
    if (!pending?.hello && this.controllerEpoch && wireEpoch(frame.controller_epoch) < this.controllerEpoch) return;
    if ((frame.t === "res" || frame.t === "pong") && pending &&
        (!pending.from || frame.from === pending.from) &&
        (!pending.op || frame.op === pending.op)) {
      if (pending.hello && !frame.err) {
        const hello: any = frame.body ? decoder.decode(bytes(frame.body)) : {};
        if (hello.caps?.includes("controller-epoch")) {
          const epoch = wireEpoch(hello.controller_epoch);
          if (!epoch) throw new ProtocolError("conflict", "server negotiated controller-epoch without an epoch");
          this.controllerEpoch = epoch;
        }
      }
      this.pending.delete(Number(frame.id));
      pending.resolve(frame);
      return;
    }
    if (frame.t === "chunk") await this.handleChunk(frame);
    else if (frame.t === "ping") this.send({ v: 1, t: "pong", id: frame.id ?? 0, to: frame.from ?? "" });
  }

  private send(frame: Frame): void {
    if (!this.socket) throw new ConnectionClosed("not connected");
    const socket = this.socket;
    if (frame.t !== "hello" && this.controllerEpoch) {
      // cbor-x encodes bigint as uint64, but larger JS numbers as floats.
      // Select the shortest unsigned-integer encoding without losing bits.
      frame = { ...frame, controller_epoch: this.controllerEpoch <= 0xffffffffn ? Number(this.controllerEpoch) : this.controllerEpoch };
    }
    const encoded = encodeProtocol(frame);
    if (encoded.byteLength > this.maxFrameBytes) throw new ProtocolError("resource_exhausted", "wire frame exceeds configured limit");
    try { socket.send(encoded); } catch {
      socket.close();
      const failure = new ConnectionClosed("connection send failed");
      this.disconnected(socket, failure);
      throw failure;
    }
  }

  private roundTrip(frame: Frame): Promise<Frame> {
    if (this.pending.size >= MAX_PENDING) return Promise.reject(new ProtocolError("resource_exhausted", "too many pending requests"));
    const id = ++this.requestID;
    frame.id = id;
    return new Promise<Frame>((resolve, reject) => {
      const timer = this.requestTimeoutMilliseconds > 0 ? setTimeout(() => {
        this.pending.delete(id);
        reject(new ConnectionClosed("request deadline exceeded"));
      }, this.requestTimeoutMilliseconds) : undefined;
      const finish = (response: Frame) => { if (timer) clearTimeout(timer); resolve(response); };
      const fail = (error: Error) => { if (timer) clearTimeout(timer); reject(error); };
      this.pending.set(id, { resolve: finish, reject: fail, from: String(frame.to ?? ""), op: String(frame.op ?? ""), hello: frame.t === "hello" });
      try { this.send(frame); } catch (error) { this.pending.delete(id); reject(error); }
    }).then((response: Frame) => {
      if (response.err) {
        const message = String(response.err.msg ?? "").split(this.token).join("[redacted]");
        throw new ProtocolError(response.err.code ?? "internal", message, Number(response.err.oldest ?? 0));
      }
      return response;
    });
  }

  async call(op: string, body: Record<string, any> = {}, to = "control"): Promise<Record<string, any>> {
    let failure: Error = new ConnectionClosed("call was not attempted");
    for (let attempt = 0; attempt < this.retries; attempt++) {
      try {
        await this.connect();
        const response = await this.roundTrip({ v: 1, t: "req", to, op, body: encodeProtocol(body) });
        return response.body ? decoder.decode(bytes(response.body)) as Record<string, any> : {};
      } catch (error) {
        if (error instanceof ProtocolError) throw error;
        failure = error as Error;
        if (attempt + 1 < this.retries) await new Promise((resolve) => setTimeout(resolve, 200 * (attempt + 1)));
      }
    }
    throw failure;
  }

  async nodeCall(workspace: string, op: string, body: Record<string, any>, bindNode?: (node: string) => void): Promise<Record<string, any>> {
    for (let attempt = 0; attempt < 2; attempt++) {
      let grant = this.grants.get(workspace);
      if (!grant) { grant = await this.call("grant", { ws: workspace }); this.grants.set(workspace, grant); }
      bindNode?.(String(grant.node));
      try { return await this.call(op, { ...body, grant }, String(grant.node)); }
      catch (error) {
        if (attempt === 0 && error instanceof ProtocolError && ["conflict", "unreachable", "unauthorized"].includes(error.code)) { this.grants.delete(workspace); continue; }
        throw error;
      }
    }
    throw new ConnectionClosed("node call retries exhausted");
  }

  private async handleChunk(frame: Frame): Promise<void> {
    const sessionID = String(frame.s ?? "");
    const session = this.sessions.get(sessionID);
    if (session) { await session.acceptFromNode(frame); return; }
    // A reconnect can clear grants while an open still holds its grant.
    // Authenticate this bounded orphan against that open during registration.
    const body = frame.body ? bytes(frame.body) : new Uint8Array();
    const orphan = { seq: Number(frame.seq ?? 0), body, from: typeof frame.from === "string" ? frame.from : "", ws: typeof frame.ws === "string" ? frame.ws : "" };
    const size = orphanFrameBytes(sessionID, orphan);
    let bucket = this.orphans.get(sessionID);
    if (!bucket) { bucket = []; this.orphans.set(sessionID, bucket); }
    if (this.orphans.size > MAX_ORPHAN_SESSIONS || bucket.length >= MAX_REORDER_CHUNKS || this.orphanCount >= MAX_ORPHAN_CHUNKS || this.orphanBytes + size > MAX_ORPHAN_BYTES) {
      this.orphans.clear(); this.orphanCount = 0; this.orphanBytes = 0; this.socket?.close(); return;
    }
    bucket.push(orphan);
    this.orphanCount++; this.orphanBytes += size;
  }

  removeSession(id: string): void { this.sessions.delete(id); }

  createWorkspace(spec: Record<string, any>, idempotencyKey = idem()): Promise<Record<string, any>> {
    return this.call("ws.create", { spec, idem: idempotencyKey });
  }

  getWorkspace(workspace: string): Promise<Record<string, any>> { return this.call("ws.get", { id: workspace }); }

  async destroyWorkspace(workspace: string, idempotencyKey = idem()): Promise<void> {
    await this.call("ws.destroy", { id: workspace, idem: idempotencyKey });
  }

  /**
   * Hold a workspace alive for at most `maxAliveSec` seconds, then sleep it
   * (or destroy it when `onExpiry` is "destroy"); `minAliveSec` keeps it awake
   * that long regardless of idleness. The deadline is durable in the control
   * plane, so it still fires if this process dies, is redeployed or loses the
   * network — unlike a `setTimeout`, which dies with the process that set it.
   * Activity is explicit: session traffic does not extend the deadline, so
   * call `markActive` (or `renewLease`) on every turn the workspace is in use.
   */
  leaseWorkspace(workspace: string, options: { maxAliveSec: number; minAliveSec?: number; onExpiry?: "sleep" | "destroy"; reason?: string; idempotencyKey?: string }): Promise<WorkspaceLease> {
    seconds("maxAliveSec", options.maxAliveSec, true);
    if (options.minAliveSec !== undefined) seconds("minAliveSec", options.minAliveSec, false);
    if (options.onExpiry !== undefined && options.onExpiry !== "sleep" && options.onExpiry !== "destroy") throw new Error('onExpiry must be "sleep" or "destroy"');
    // The expiry action is sent explicitly so sleep-versus-destroy is never inferred from a default.
    const body: Record<string, any> = { id: workspace, max_alive_sec: options.maxAliveSec, on_expiry: options.onExpiry ?? "sleep", idem: options.idempotencyKey ?? idem() };
    if (options.minAliveSec !== undefined) body.min_alive_sec = options.minAliveSec;
    if (options.reason !== undefined) body.reason = options.reason;
    return this.call("ws.lease", body) as Promise<WorkspaceLease>;
  }

  /**
   * Push a held lease `extendSec` seconds further out. Throws a `ProtocolError`
   * with code "conflict" once the deadline already fired or the hold was
   * cancelled (reason "lifecycle_deadline_expired"), or once the workspace
   * moved (reason "generation_mismatch"); both mean take a fresh lease.
   */
  renewLease(workspace: string, lease: string, options: { extendSec: number; minAliveSec?: number; idempotencyKey?: string }): Promise<WorkspaceLease> {
    seconds("extendSec", options.extendSec, true);
    if (options.minAliveSec !== undefined) seconds("minAliveSec", options.minAliveSec, false);
    const body: Record<string, any> = { id: workspace, lease, extend_sec: options.extendSec, idem: options.idempotencyKey ?? idem() };
    if (options.minAliveSec !== undefined) body.min_alive_sec = options.minAliveSec;
    return this.call("ws.lease.renew", body) as Promise<WorkspaceLease>;
  }

  /** Release a hold early; the workspace returns to its idle policy without waiting for the deadline. */
  cancelLease(workspace: string, lease: string, idempotencyKey = idem()): Promise<Workspace> {
    return this.call("ws.lease.cancel", { id: workspace, lease, idem: idempotencyKey }) as Promise<Workspace>;
  }

  /** Read the hold and the durable deadline the control plane will act on; a read, so it carries no idempotency key. */
  getLease(workspace: string): Promise<WSLeaseRes> { return this.call("ws.lease.get", { id: workspace }) as Promise<WSLeaseRes>; }

  /** Sleep or destroy the workspace after that many seconds idle; activity is what `markActive` reports, not session traffic. */
  setIdlePolicy(workspace: string, options: { sleepAfterSec?: number; destroyAfterSec?: number; idempotencyKey?: string } = {}): Promise<Workspace> {
    if (options.sleepAfterSec !== undefined) seconds("sleepAfterSec", options.sleepAfterSec, false);
    if (options.destroyAfterSec !== undefined) seconds("destroyAfterSec", options.destroyAfterSec, false);
    const body: Record<string, any> = { id: workspace, idem: options.idempotencyKey ?? idem() };
    if (options.sleepAfterSec !== undefined) body.sleep_after_sec = options.sleepAfterSec;
    if (options.destroyAfterSec !== undefined) body.destroy_after_sec = options.destroyAfterSec;
    return this.call("ws.idle.policy", body) as Promise<Workspace>;
  }

  /** Report the workspace idle, starting the idle policy's clock. */
  markIdle(workspace: string, reason?: string, idempotencyKey = idem()): Promise<Workspace> { return this.mark(workspace, true, reason, idempotencyKey); }

  /** Report the workspace in use, resetting the idle clock; call it every turn, since session traffic alone does not. */
  markActive(workspace: string, reason?: string, idempotencyKey = idem()): Promise<Workspace> { return this.mark(workspace, false, reason, idempotencyKey); }

  private mark(workspace: string, idle: boolean, reason: string | undefined, idempotencyKey: string): Promise<Workspace> {
    // `idle` is always on the wire: an absent flag would read as the false default and silently mark active.
    const body: Record<string, any> = { id: workspace, idle, idem: idempotencyKey };
    if (reason !== undefined) body.reason = reason;
    return this.call("ws.idle.mark", body) as Promise<Workspace>;
  }

  async exec(workspace: string, program: string[], options: { kind?: string; cwd?: string; env?: Record<string, string>; stdin?: boolean; idempotencyKey?: string } = {}): Promise<Session> {
    const session = new Session(this, workspace, options.kind ?? "exec", "");
    const response = await this.nodeCall(workspace, "s.open", { ws: workspace, kind: options.kind ?? "exec", program, cwd: options.cwd ?? "", env: options.env ?? {}, stdin: options.stdin ?? false, idem: options.idempotencyKey ?? idem() }, (node) => session.bindNode(node));
    const id = String(response.s);
    const existing = this.sessions.get(id);
    if (existing) return existing;
    session.id = id;
    session.lastInputSeq = Number(response.last_iseq ?? 0);
    this.sessions.set(id, session);
    const early = this.orphans.get(id) ?? [];
    this.orphans.delete(id); this.orphanCount -= early.length;
    this.orphanBytes -= early.reduce((total, frame) => total + orphanFrameBytes(id, frame), 0);
    for (const frame of early) await session.acceptFromNode(frame);
    return session;
  }

  async attach(workspace: string, sessionID: string, from = 0): Promise<Session> {
    const session = this.sessions.get(sessionID) ?? new Session(this, workspace, "", sessionID);
    session.nextSeq = from; this.sessions.set(sessionID, session);
    await session.reattach(this.generation);
    return session;
  }

  async uploadArtifact(data: Uint8Array): Promise<{ id: string; size: number }> {
    const id = `art_sha256:${await sha256(data)}`;
    const signal = this.requestTimeoutMilliseconds > 0 ? AbortSignal.timeout(this.requestTimeoutMilliseconds) : undefined;
    const response = await this.fetcher(`${this.baseURL}/v1/artifacts/${encodeURIComponent(id)}`, { method: "PUT", headers: { Authorization: `Bearer ${this.token}`, "Content-Type": "application/gzip" }, body: data as BodyInit, redirect: "error", signal });
    await raiseHTTP(response, this.token);
    await response.body?.cancel();
    return { id, size: data.byteLength };
  }

  async downloadArtifact(id: string, maxBytes = 8 * 1024 * 1024 * 1024): Promise<Uint8Array> {
    if (!/^art_sha256:[0-9a-f]{64}$/.test(id)) throw new Error("invalid artifact id");
    const signal = this.requestTimeoutMilliseconds > 0 ? AbortSignal.timeout(this.requestTimeoutMilliseconds) : undefined;
    const response = await this.fetcher(`${this.baseURL}/v1/artifacts/${encodeURIComponent(id)}`, { headers: { Authorization: `Bearer ${this.token}` }, redirect: "error", signal });
    await raiseHTTP(response, this.token);
    if (!response.body) throw new ProtocolError("internal", "artifact response has no body");
    const reader = response.body.getReader(); const chunks: Uint8Array[] = []; let size = 0;
    for (;;) {
      const { value, done } = await reader.read(); if (done) break;
      size += value.byteLength; if (size > maxBytes) { await reader.cancel(); throw new ProtocolError("resource_exhausted", "artifact exceeds configured download limit"); }
      chunks.push(value);
    }
    const result = new Uint8Array(size); let offset = 0;
    for (const chunk of chunks) { result.set(chunk, offset); offset += chunk.byteLength; }
    if (await sha256(result) !== id.slice("art_sha256:".length)) throw new ProtocolError("conflict", "artifact digest mismatch");
    return result;
  }
}

export async function raiseHTTP(response: Response, secret = ""): Promise<void> {
  if (response.ok) return;
  let body: any = {};
  try {
    const reader = response.body?.getReader(); const chunks: Uint8Array[] = []; let size = 0;
    if (reader) for (;;) {
      const { value, done } = await reader.read(); if (done) break;
      size += value.byteLength; if (size > 4096) { chunks.length = 0; size = 0; await reader.cancel(); break; }
      chunks.push(value);
    }
    const raw = new Uint8Array(size); let offset = 0;
    for (const chunk of chunks) { raw.set(chunk, offset); offset += chunk.byteLength; }
    body = JSON.parse(new TextDecoder().decode(raw));
  } catch { /* bounded status only */ }
  const detail = body.error && typeof body.error === "object" ? body.error : body;
  const message = String(detail.message ?? `HTTP ${response.status}`).split(secret).join(secret ? "[redacted]" : "");
  throw new ProtocolError(String(detail.code ?? "internal"), message);
}
