import type { Approval, EventRecord, FileEntry, FleetResponse, RuntimeConfig, Usage, WorkspaceDetail } from './types';

export class APIError extends Error {
  constructor(public readonly status: number, message: string, public readonly code?: string) {
    super(message);
  }
}

export class APIClient {
  private credential = '';

  constructor(private readonly config: RuntimeConfig) {}

  setCredential(credential: string) { this.credential = credential; }

  private async request<T>(path: string, init: RequestInit = {}): Promise<T> {
    const headers = new Headers(init.headers);
    headers.set('Accept', 'application/json');
    if (this.credential) headers.set('Authorization', `Bearer ${this.credential}`);
    if (init.body !== undefined) headers.set('Content-Type', 'application/json');
    if (init.method && !['GET', 'HEAD'].includes(init.method.toUpperCase()) && !headers.has('Idempotency-Key')) {
      headers.set('Idempotency-Key', crypto.randomUUID());
    }
    const response = await fetch(`${this.config.apiBase}${path}`, {
      ...init,
      headers,
      credentials: 'same-origin',
      cache: 'no-store',
    });
    if (!response.ok) {
      let body: { error?: string | { message?: string; code?: string }; message?: string; code?: string } = {};
      try { body = await response.json(); } catch { /* a proxy can return plain text */ }
      const nested = typeof body.error === 'object' ? body.error : undefined;
      const message = typeof body.error === 'string' ? body.error : nested?.message ?? body.message ?? `request failed (${response.status})`;
      throw new APIError(response.status, message, nested?.code ?? body.code);
    }
    if (response.status === 204) return undefined as T;
    return response.json() as Promise<T>;
  }

  fleet(signal?: AbortSignal) { return this.request<FleetResponse>('/console/fleet', { signal }); }
  workspace(id: string, signal?: AbortSignal) { return this.request<WorkspaceDetail>(`/console/workspaces/${encodeURIComponent(id)}`, { signal }); }
  createWorkspace(input: { name: string; image?: string }) {
    return this.request<WorkspaceDetail>('/console/workspaces', { method: 'POST', body: JSON.stringify(input) });
  }
  exec(id: string, command: string[]) {
    return this.request<{ session: string }>(`/console/workspaces/${encodeURIComponent(id)}/exec`, { method: 'POST', body: JSON.stringify({ command }) });
  }
  lifecycle(id: string, action: 'snapshot' | 'move' | 'destroy' | 'quarantine', input: Record<string, unknown> = {}) {
    return this.request<{ workspace?: WorkspaceDetail; snapshot?: string }>(`/console/workspaces/${encodeURIComponent(id)}/${action}`, { method: 'POST', body: JSON.stringify(input) });
  }
  files(id: string, path: string, signal?: AbortSignal) {
    return this.request<{ entries: FileEntry[] }>(`/console/workspaces/${encodeURIComponent(id)}/files?path=${encodeURIComponent(path)}`, { signal });
  }
  readFile(id: string, path: string, signal?: AbortSignal) {
    return this.request<{ path: string; content: string; etag: string }>(`/console/workspaces/${encodeURIComponent(id)}/file?path=${encodeURIComponent(path)}`, { signal });
  }
  writeFile(id: string, path: string, content: string, etag: string) {
    return this.request<{ path: string; etag: string }>(`/console/workspaces/${encodeURIComponent(id)}/file?path=${encodeURIComponent(path)}`, {
      method: 'PUT', headers: { 'If-Match': etag }, body: JSON.stringify({ content }),
    });
  }
  events(filters: URLSearchParams, signal?: AbortSignal) {
    return this.request<{ events: EventRecord[]; next: number }>(`/console/events?${filters}`, { signal });
  }
  approvals(signal?: AbortSignal) { return this.request<{ approvals: Approval[] }>('/console/approvals?status=pending', { signal }); }
  decideApproval(id: string, denied: boolean, option?: string) {
    return this.request<Approval>(`/approvals/${encodeURIComponent(id)}`, { method: 'POST', body: JSON.stringify({ denied, option }) });
  }
  usage(signal?: AbortSignal) { return this.request<{ usage: Usage[] }>('/usage?window=1d', { signal }); }

  terminalURL(workspace: string, session: string, replay: { since?: string; from?: number } = {}): string {
    const url = new URL(`${this.config.apiBase}/console/workspaces/${encodeURIComponent(workspace)}/terminal`, window.location.href);
    url.protocol = url.protocol === 'https:' ? 'wss:' : 'ws:';
    url.searchParams.set('session', session);
    if (replay.from !== undefined) url.searchParams.set('from', String(replay.from));
    else if (replay.since) url.searchParams.set('since', replay.since);
    return url.toString();
  }

  terminalProtocols(): string[] {
    if (!this.credential) return [];
    const bytes = new TextEncoder().encode(this.credential);
    let binary = '';
    for (const value of bytes) binary += String.fromCharCode(value);
    return [`remount.bearer.${btoa(binary).replaceAll('+', '-').replaceAll('/', '_').replace(/=+$/, '')}`];
  }
}
