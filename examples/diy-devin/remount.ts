// A typed Remount Agent API client for Node 20+, Deno, Bun or a browser.
// No dependencies: fetch, EventSource-style parsing over ReadableStream.
//
//   const rm = new Remount(process.env.REMOUNT_SERVER!, process.env.REMOUNT_TOKEN!);
//   const a = await rm.createAgent({ spec: { recipe: "codex", task: "add tests" }, workspace: { name: "t" } });
//   for await (const ev of rm.transcript(a.id)) { if (ev.type === "record") render(ev.record); }
//
// Everything here is one route from docs/api.md; the client adds only
// idempotency keys and cursor-resumable streaming.

/** Why an agent is being woken; the server accepts exactly these. */
export type WakeBy = "request" | "preview" | "diff";

export type AgentStatus =
  | "creating" | "scheduled" | "running" | "waiting_input" | "idle"
  | "waiting_approval" | "sleeping" | "failed" | "finished" | "destroyed";

export interface Agent {
  id: string; name?: string; ws: string; status: AgentStatus; status_reason?: string;
  turns: number; parent?: string; url?: string; inbox?: unknown[]; policy: AgentPolicy;
  transcript_next: number; pending_approvals?: number;
}
export interface AgentPolicy { approve?: "never" | "on-request" | "auto"; sleep_after_sec?: number; max_turns?: number; start_at?: number }
export interface AgentCreateReq {
  name?: string; ws?: string; parent?: string;
  workspace?: { name?: string; bindings?: string[]; repo?: { url: string; ref?: string }; base?: string; labels?: Record<string, string> };
  spec: { recipe: string; task: string; model?: string; sandbox?: string; acp_command?: string[] };
  policy?: AgentPolicy;
}
export interface TranscriptRecord {
  index: number; run: string; seq: number; stream: "acp_in" | "acp_out" | "stderr" | "exit" | "info" | "gap";
  at: number; frame?: { frame: Record<string, unknown>; redacted: boolean; truncated: boolean }; text?: string;
}
export interface Approval { id: string; kind: string; title?: string; status: string; options?: { id: string; name?: string; kind?: string }[] }
export type TranscriptEvent =
  | { type: "record"; record: TranscriptRecord }
  | { type: "gap"; gap: { from: number; to: number } }
  | { type: "done"; next: number };

export class RemountError extends Error {
  constructor(public status: number, public code: string, message: string) { super(`${status} ${code}: ${message}`); }
}

export class Remount {
  constructor(private base: string, private token: string) { this.base = base.replace(/\/$/, ""); }

  private headers(extra: Record<string, string> = {}): Record<string, string> {
    const h: Record<string, string> = { Accept: "application/json", ...extra };
    if (this.token) h.Authorization = `Bearer ${this.token}`;
    return h;
  }

  async call<T>(method: string, path: string, body?: unknown, idem?: string): Promise<T> {
    const headers = this.headers(body !== undefined ? { "Content-Type": "application/json" } : {});
    if (idem) headers["Idempotency-Key"] = idem;
    const res = await fetch(this.base + path, { method, headers, body: body === undefined ? undefined : JSON.stringify(body) });
    if (!res.ok) {
      let code = "internal", message = res.statusText;
      try { const e = (await res.json()).error; code = e.code; message = e.message; } catch { /* non-JSON error */ }
      throw new RemountError(res.status, code, message);
    }
    return res.status === 204 ? (undefined as T) : (await res.json()) as T;
  }

  createAgent(req: AgentCreateReq, idem = crypto.randomUUID()) { return this.call<Agent>("POST", "/v1/agents", req, idem); }
  getAgent(id: string) { return this.call<Agent>("GET", `/v1/agents/${id}`); }
  listAgents(q: { status?: string; ws?: string; parent?: string } = {}) {
    const qs = new URLSearchParams(Object.entries(q).filter(([, v]) => v) as [string, string][]).toString();
    return this.call<{ agents: Agent[] }>("GET", `/v1/agents${qs ? "?" + qs : ""}`);
  }
  message(id: string, text: string, kind: "follow_up" | "steer" = "follow_up", idem = crypto.randomUUID()) {
    return this.call<{ agent: Agent; degraded?: boolean; woken?: boolean }>("POST", `/v1/agents/${id}/messages`, { text, kind }, idem);
  }
  cancel(id: string) { return this.call<Agent>("POST", `/v1/agents/${id}/cancel`, undefined, crypto.randomUUID()); }
  sleep(id: string) { return this.call<Agent>("POST", `/v1/agents/${id}/sleep`, undefined, crypto.randomUUID()); }
  wake(id: string, by: WakeBy = "request") { return this.call<Agent>("POST", `/v1/agents/${id}/wake`, { by }, crypto.randomUUID()); }
  fork(id: string, req: { name?: string; task?: string; policy?: AgentPolicy } = {}) { return this.call<Agent>("POST", `/v1/agents/${id}/fork`, req, crypto.randomUUID()); }
  destroy(id: string) { return this.call<void>("POST", `/v1/agents/${id}/destroy`, undefined, crypto.randomUUID()); }
  approvals(id: string) { return this.call<{ approvals: Approval[] }>("GET", `/v1/agents/${id}/approvals`); }
  decide(approval: string, d: { option?: string; denied?: boolean; content?: unknown }) { return this.call<Approval>("POST", `/v1/approvals/${approval}`, d, crypto.randomUUID()); }
  diff(id: string, wake = false) { return this.call<{ status: string; diff: string; truncated: boolean }>("GET", `/v1/agents/${id}/diff${wake ? "?wake=true" : ""}`); }
  readFile(id: string, path: string) { return fetch(`${this.base}/v1/agents/${id}/fs/${path.replace(/^\//, "")}`, { headers: this.headers() }); }
  previewURL(id: string, port: number) { return `${this.base}/v1/agents/${id}/ports/${port}/`; }

  /** Follow the transcript over SSE from `from`, resuming across disconnects
   * with the last id, until the server says `done`. */
  async *transcript(id: string, from = 0, signal?: AbortSignal): AsyncGenerator<TranscriptEvent> {
    let cursor = from;
    while (!signal?.aborted) {
      let res: Response;
      try {
        res = await fetch(`${this.base}/v1/agents/${id}/transcript?from=${cursor}`, { headers: this.headers({ Accept: "text/event-stream" }), signal });
      } catch { await new Promise((r) => setTimeout(r, 1000)); continue; }
      if (!res.ok || !res.body) { await new Promise((r) => setTimeout(r, 1000)); continue; }
      const reader = res.body.pipeThrough(new TextDecoderStream()).getReader();
      let buf = "", event = "", data = "";
      try {
        for (;;) {
          const { value, done } = await reader.read();
          if (done) break;
          buf += value;
          let nl: number;
          while ((nl = buf.indexOf("\n")) >= 0) {
            const line = buf.slice(0, nl); buf = buf.slice(nl + 1);
            if (line.startsWith(":")) continue;
            if (line.startsWith("event:")) event = line.slice(6).trim();
            else if (line.startsWith("data:")) data += line.slice(5).trim();
            else if (line.startsWith("id:")) cursor = Number(line.slice(3));
            else if (line === "") {
              if (event === "record" && data) yield { type: "record", record: JSON.parse(data) };
              else if (event === "gap" && data) yield { type: "gap", gap: JSON.parse(data) };
              else if (event === "done") { yield { type: "done", next: cursor }; return; }
              event = ""; data = "";
            }
          }
        }
      } catch { /* dropped: resume from cursor */ }
      finally { reader.releaseLock(); }
    }
  }
}

/** Extract the text a person would read from one ACP record, or null. */
export function visibleText(rec: TranscriptRecord, thoughts = false): string | null {
  if (rec.stream === "stderr") return rec.text ?? null;
  if (rec.stream !== "acp_out" || !rec.frame) return null;
  const f = rec.frame.frame as { method?: string; params?: { update?: { sessionUpdate?: string; content?: { type: string; text?: string }; title?: string } } };
  if (f.method !== "session/update") return null;
  const u = f.params?.update;
  if (!u) return null;
  if (u.sessionUpdate === "agent_message_chunk" || (thoughts && u.sessionUpdate === "agent_thought_chunk")) return u.content?.type === "text" ? u.content.text ?? "" : null;
  if (u.sessionUpdate === "tool_call") return `\n[tool: ${u.title ?? ""}]\n`;
  return null;
}
