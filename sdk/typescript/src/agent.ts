import { ConnectionClosed } from "./client.js";
import { ProtocolError } from "./errors.js";
import type { Agent, AgentCreateReq, AgentMessageRes, Approval, ApprovalDecideReq } from "./types.js";

function idem(): string { return `idem_${globalThis.crypto.randomUUID().replaceAll("-", "")}`; }

export interface TerminalWebSocket {
  binaryType?: string;
  send(data: string | Uint8Array): void;
  close(): void;
  addEventListener?(name: string, listener: (event: any) => void): void;
  on?(name: string, listener: (...args: any[]) => void): void;
}

export type TerminalWebSocketFactory = (url: string, bearerProtocol: string) => Promise<TerminalWebSocket>;

function bearerProtocol(token: string): string {
  let binary = "";
  for (const value of new TextEncoder().encode(token)) binary += String.fromCharCode(value);
  return `remount.bearer.${btoa(binary).replaceAll("+", "-").replaceAll("/", "_").replace(/=+$/, "")}`;
}

async function defaultTerminalFactory(url: string, protocol: string): Promise<TerminalWebSocket> {
  const Constructor: any = globalThis.WebSocket ?? (await import("ws")).default;
  return await new Promise((resolve, reject) => {
    const socket: TerminalWebSocket = new Constructor(url, protocol);
    socket.binaryType = "arraybuffer";
    const open = () => resolve(socket);
    const error = () => reject(new ConnectionClosed("terminal WebSocket handshake failed"));
    if (socket.addEventListener) {
      socket.addEventListener("open", open); socket.addEventListener("error", error);
    } else {
      socket.on!("open", open); socket.on!("error", error);
    }
  });
}

function terminalBytes(value: any): Uint8Array {
  if (value instanceof Uint8Array) return value;
  if (value instanceof ArrayBuffer) return new Uint8Array(value);
  if (ArrayBuffer.isView(value)) return new Uint8Array(value.buffer, value.byteOffset, value.byteLength);
  throw new ProtocolError("bad_request", "terminal sent an unsupported message");
}

export interface TerminalEvent { type: string; data?: Uint8Array; [key: string]: unknown }

/** The HTTP decision body; the approval id and idempotency key use headers/path. */
export type ApprovalDecisionInput = Omit<ApprovalDecideReq, "id" | "idem">;

/** One authenticated Agent terminal attachment with an explicit replay cursor. */
export class Terminal implements AsyncIterable<TerminalEvent> {
  session = "";
  nextSeq = 0;
  private opened = false;
  private ended = false;
  private error?: Error;
  private queue: TerminalEvent[] = [];
  private queueBytes = 0;
  private waiters: Array<{ resolve: (value: IteratorResult<TerminalEvent>) => void; reject: (error: Error) => void }> = [];

  constructor(private socket: TerminalWebSocket, private maxMessageBytes: number) {
    if (socket.addEventListener) {
      socket.addEventListener("message", (event) => this.receive(event.data));
      socket.addEventListener("close", () => this.disconnected());
      socket.addEventListener("error", () => this.fail(new ConnectionClosed("terminal connection failed")));
    } else {
      socket.on!("message", (data, isBinary) => this.receive(isBinary ? data : new TextDecoder().decode(terminalBytes(data))));
      socket.on!("close", () => this.disconnected());
      socket.on!("error", () => this.fail(new ConnectionClosed("terminal connection failed")));
    }
  }

  [Symbol.asyncIterator](): AsyncIterator<TerminalEvent> { return { next: () => this.next() }; }

  private next(): Promise<IteratorResult<TerminalEvent>> {
    if (this.queue.length) {
      const event = this.queue.shift()!;
      this.queueBytes -= event.data?.byteLength ?? 0;
      return Promise.resolve({ value: event, done: false });
    }
    if (this.ended) return this.error ? Promise.reject(this.error) : Promise.resolve({ value: undefined, done: true });
    if (this.waiters.length >= 1024) {
      const error = new ProtocolError("resource_exhausted", "too many pending terminal consumers");
      this.fail(error); return Promise.reject(error);
    }
    return new Promise((resolve, reject) => this.waiters.push({ resolve, reject }));
  }

  private receive(value: any): void {
    if (this.ended) return;
    try {
      if (typeof value === "string") {
        if (new TextEncoder().encode(value).byteLength > this.maxMessageBytes) throw new ProtocolError("resource_exhausted", "terminal message exceeds configured limit");
        const event = JSON.parse(value) as TerminalEvent;
        if (!event || typeof event.type !== "string") throw new ProtocolError("bad_request", "terminal sent invalid control event");
        if (!this.opened && event.type !== "open") throw new ProtocolError("bad_request", "terminal first event was not open");
        if (event.type === "open") {
          if (this.opened) throw new ProtocolError("bad_request", "terminal sent a duplicate open event");
          this.opened = true; this.session = String(event.session ?? ""); this.nextSeq = Number(event.next ?? 0);
        } else if (event.type === "gap") this.nextSeq = Math.max(this.nextSeq, Number(event.to ?? 0) + 1);
        else if (event.type === "exit") { this.nextSeq++; this.ended = true; }
        this.push(event);
        if (this.ended) this.finishWaiters();
        return;
      }
      const data = terminalBytes(value);
      if (data.byteLength > this.maxMessageBytes) throw new ProtocolError("resource_exhausted", "terminal message exceeds configured limit");
      if (!this.opened) throw new ProtocolError("bad_request", "terminal output preceded open");
      this.nextSeq++;
      this.push({ type: "data", data });
    } catch (error) {
      this.fail(error instanceof Error ? error : new ProtocolError("bad_request", "invalid terminal message"));
      this.socket.close();
    }
  }

  private push(event: TerminalEvent): void {
    const waiter = this.waiters.shift();
    if (waiter) { waiter.resolve({ value: event, done: false }); return; }
    const size = event.data?.byteLength ?? 0;
    if (this.queue.length >= 1024 || this.queueBytes + size > this.maxMessageBytes) {
      this.fail(new ProtocolError("resource_exhausted", "terminal delivery queue is full")); this.socket.close(); return;
    }
    this.queue.push(event); this.queueBytes += size;
  }

  private disconnected(): void {
    if (!this.ended) this.fail(new ConnectionClosed("terminal disconnected; reconnect from nextSeq"));
  }

  private fail(error: Error): void {
    if (this.ended) return;
    this.error = error; this.ended = true; this.finishWaiters();
  }

  private finishWaiters(): void {
    for (const waiter of this.waiters.splice(0)) {
      if (this.error) waiter.reject(this.error);
      else waiter.resolve({ value: undefined, done: true });
    }
  }

  input(data: Uint8Array): void { this.socket.send(data); }
  resize(rows: number, cols: number): void { this.control({ type: "resize", rows, cols }); }
  signal(signal: string): void { this.control({ type: "signal", signal }); }
  eof(): void { this.control({ type: "eof" }); }
  private control(event: Record<string, unknown>): void { this.socket.send(JSON.stringify(event)); }
  close(): void { this.ended = true; this.finishWaiters(); this.socket.close(); }
}

export interface AgentClientOptions { fetch?: typeof fetch; requestTimeoutMilliseconds?: number; maxResponseBytes?: number; retries?: number; terminalWebSocketFactory?: TerminalWebSocketFactory }

export class AgentClient {
  private fetcher: typeof fetch;
  private requestTimeoutMilliseconds: number;
  private maxResponseBytes: number;
  private retries: number;
  private terminalFactory: TerminalWebSocketFactory;
  readonly baseURL: string;
  constructor(baseURL: string, readonly token: string, options: AgentClientOptions = {}) {
    this.baseURL = baseURL.replace(/\/$/, "");
    this.fetcher = options.fetch ?? globalThis.fetch.bind(globalThis);
    this.requestTimeoutMilliseconds = options.requestTimeoutMilliseconds ?? 30_000;
    this.maxResponseBytes = options.maxResponseBytes ?? 16 << 20;
    this.retries = options.retries ?? 3;
    if (this.retries < 1 || this.retries > 10) throw new Error("retries must be between 1 and 10");
    if (this.maxResponseBytes < 1) throw new Error("maxResponseBytes must be positive");
    if (this.requestTimeoutMilliseconds <= 0) throw new Error("requestTimeoutMilliseconds must be positive");
    this.terminalFactory = options.terminalWebSocketFactory ?? defaultTerminalFactory;
  }

  private async request(method: string, path: string, body?: unknown, idempotencyKey?: string): Promise<any> {
    const headers: Record<string, string> = { Authorization: `Bearer ${this.token}` };
    if (method !== "GET") headers["Idempotency-Key"] = idempotencyKey ?? idem();
    if (body !== undefined) headers["Content-Type"] = "application/json";
    for (let attempt = 0; attempt < this.retries; attempt++) {
      try {
        const signal = this.requestTimeoutMilliseconds > 0 ? AbortSignal.timeout(this.requestTimeoutMilliseconds) : undefined;
        const response = await this.fetcher(this.baseURL + path, { method, headers, body: body === undefined ? undefined : JSON.stringify(body), redirect: "error", signal });
        if (response.status === 204 && response.ok) return undefined;
        const raw = await readBounded(response, this.maxResponseBytes);
        let payload: any = {};
        try { payload = JSON.parse(new TextDecoder().decode(raw)); } catch {
          if (response.ok) throw new ProtocolError("internal", "server returned invalid JSON");
        }
        if (!response.ok) {
          const detail = payload.error && typeof payload.error === "object" ? payload.error : payload;
          const message = String(detail.message ?? `HTTP ${response.status}`).split(this.token).join(this.token ? "[redacted]" : "");
          throw new ProtocolError(String(detail.code ?? "internal"), message);
        }
        return payload;
      } catch (error) {
        if (error instanceof ProtocolError) throw error;
        if (attempt + 1 === this.retries) throw new ProtocolError("unreachable", "HTTP request failed");
        await new Promise((resolve) => setTimeout(resolve, 100 * (attempt + 1)));
      }
    }
    throw new ProtocolError("unreachable", "HTTP request failed");
  }

  createAgent(request: AgentCreateReq, idempotencyKey?: string): Promise<Agent> { return this.request("POST", "/v1/agents", request, idempotencyKey); }
  getAgent(id: string): Promise<Agent> { return this.request("GET", `/v1/agents/${encodeURIComponent(id)}`); }
  async listAgents(filters: { status?: string; workspace?: string; parent?: string } = {}): Promise<Agent[]> {
    const query = new URLSearchParams();
    if (filters.status) query.set("status", filters.status);
    if (filters.workspace) query.set("ws", filters.workspace);
    if (filters.parent) query.set("parent", filters.parent);
    const result = await this.request("GET", `/v1/agents${query.size ? `?${query}` : ""}`);
    return result.agents;
  }
  messageAgent(id: string, text: string, options: { kind?: string; idempotencyKey?: string } = {}): Promise<AgentMessageRes> { return this.request("POST", `/v1/agents/${encodeURIComponent(id)}/messages`, { text, kind: options.kind ?? "" }, options.idempotencyKey); }
  forkAgent(id: string, request: Record<string, unknown> = {}, idempotencyKey?: string): Promise<Agent> { return this.request("POST", `/v1/agents/${encodeURIComponent(id)}/fork`, request, idempotencyKey); }
  agentAction(id: string, action: "cancel" | "sleep" | "wake" | "destroy", options: { by?: string; idempotencyKey?: string } = {}): Promise<Agent | undefined> { return this.request("POST", `/v1/agents/${encodeURIComponent(id)}/${action}`, action === "wake" ? { by: options.by ?? "" } : undefined, options.idempotencyKey); }

  transcript(id: string, from = 0, limit = 0): Promise<any> {
    const query = new URLSearchParams({ from: String(from), limit: String(limit) });
    return this.request("GET", `/v1/agents/${encodeURIComponent(id)}/transcript?${query}`);
  }

  async *streamTranscript(id: string, options: { from?: number; pollMilliseconds?: number } = {}): AsyncGenerator<any> {
    let cursor = options.from ?? 0;
    for (;;) {
      let page: any;
      try { page = await this.transcript(id, cursor); }
      catch (error) {
        if (!(error instanceof ProtocolError) || error.code !== "unreachable") throw error;
        await new Promise((resolve) => setTimeout(resolve, options.pollMilliseconds ?? 200));
        continue;
      }
      if (page.gap) throw new ProtocolError("evicted", `transcript records [${page.gap.from}, ${page.gap.to}) were evicted`, Number(page.gap.to));
      for (const record of page.records ?? []) yield record;
      const next = Number(page.next ?? cursor);
      if (next < cursor) throw new ProtocolError("internal", "transcript cursor moved backwards");
      cursor = next;
      if (page.done) return;
      await new Promise((resolve) => setTimeout(resolve, options.pollMilliseconds ?? 200));
    }
  }

  async listApprovals(agentID: string, status = ""): Promise<Approval[]> {
    const query = status ? `?${new URLSearchParams({ status })}` : "";
    const result = await this.request("GET", `/v1/agents/${encodeURIComponent(agentID)}/approvals${query}`);
    return result.approvals;
  }
  getApproval(id: string): Promise<Approval> { return this.request("GET", `/v1/approvals/${encodeURIComponent(id)}`); }
  decideApproval(id: string, decision: ApprovalDecisionInput, idempotencyKey?: string): Promise<Approval> { return this.request("POST", `/v1/approvals/${encodeURIComponent(id)}`, decision, idempotencyKey); }
  approve(id: string, option = "allow_once", idempotencyKey?: string): Promise<Approval> { return this.decideApproval(id, { option }, idempotencyKey); }
  deny(id: string, idempotencyKey?: string): Promise<Approval> { return this.decideApproval(id, { denied: true }, idempotencyKey); }

  async waitForAgent(id: string, statuses: ReadonlySet<string>, options: { timeoutMilliseconds?: number; pollMilliseconds?: number } = {}): Promise<Agent> {
    const deadline = Date.now() + (options.timeoutMilliseconds ?? 120_000);
    for (;;) {
      const agent = await this.getAgent(id);
      if (statuses.has(agent.status)) return agent;
      if (Date.now() >= deadline) throw new ProtocolError("timeout", "agent status wait deadline exceeded");
      await new Promise((resolve) => setTimeout(resolve, options.pollMilliseconds ?? 200));
    }
  }

  async waitForApproval(agentID: string, options: { timeoutMilliseconds?: number; pollMilliseconds?: number } = {}): Promise<Approval> {
    const deadline = Date.now() + (options.timeoutMilliseconds ?? 120_000);
    for (;;) {
      const approvals = await this.listApprovals(agentID);
      if (approvals.length) return approvals[0];
      if (Date.now() >= deadline) throw new ProtocolError("timeout", "approval wait deadline exceeded");
      await new Promise((resolve) => setTimeout(resolve, options.pollMilliseconds ?? 200));
    }
  }

  diff(id: string, wake = false): Promise<{ status: string; diff: string; truncated: boolean }> {
    return this.request("GET", `/v1/agents/${encodeURIComponent(id)}/diff${wake ? "?wake=true" : ""}`);
  }

  async connectTerminal(id: string, options: { session?: string; from?: number; program?: string[]; cwd?: string; rows?: number; cols?: number; timeoutMilliseconds?: number } = {}): Promise<Terminal> {
    const url = new URL(this.baseURL);
    if (!["http:", "https:", "ws:", "wss:"].includes(url.protocol)) throw new Error("Remount URL must use http, https, ws, or wss");
    url.protocol = url.protocol === "https:" || url.protocol === "wss:" ? "wss:" : "ws:";
    url.pathname = `${url.pathname.replace(/\/$/, "")}/v1/agents/${encodeURIComponent(id)}/terminal`;
    url.search = "";
    if (options.session) {
      url.searchParams.set("session", options.session); url.searchParams.set("from", String(options.from ?? 0));
    } else {
      for (const command of options.program ?? ["/bin/sh"]) url.searchParams.append("program", command);
      url.searchParams.set("cwd", options.cwd ?? "");
      url.searchParams.set("rows", String(options.rows ?? 24)); url.searchParams.set("cols", String(options.cols ?? 80));
    }
    const timeout = options.timeoutMilliseconds ?? this.requestTimeoutMilliseconds;
    if (timeout <= 0) throw new Error("timeoutMilliseconds must be positive");
    const pending = this.terminalFactory(url.toString(), bearerProtocol(this.token));
    let timedOut = false; let handle: ReturnType<typeof setTimeout>;
    const deadline = new Promise<never>((_, reject) => { handle = setTimeout(() => { timedOut = true; reject(new ProtocolError("timeout", "terminal connect deadline exceeded")); }, timeout); });
    pending.then((late) => { if (timedOut) late.close(); }, () => undefined);
    let socket: TerminalWebSocket;
    try { socket = await Promise.race([pending, deadline]); }
    catch (error) {
      if (error instanceof ProtocolError) throw error;
      throw new ConnectionClosed("terminal connection failed");
    } finally { clearTimeout(handle!); }
    return new Terminal(socket, this.maxResponseBytes);
  }
}

async function readBounded(response: Response, maxBytes: number): Promise<Uint8Array> {
  if (!response.body) return new Uint8Array();
  const reader = response.body.getReader(); const chunks: Uint8Array[] = []; let size = 0;
  for (;;) {
    const { value, done } = await reader.read(); if (done) break;
    size += value.byteLength;
    if (size > maxBytes) { await reader.cancel(); throw new ProtocolError("resource_exhausted", "HTTP response exceeds configured limit"); }
    chunks.push(value);
  }
  const result = new Uint8Array(size); let offset = 0;
  for (const chunk of chunks) { result.set(chunk, offset); offset += chunk.byteLength; }
  return result;
}
