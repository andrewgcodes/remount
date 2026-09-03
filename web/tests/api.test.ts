import { afterEach, describe, expect, it, vi } from 'vitest';
import { APIClient, APIError } from '../src/api';

afterEach(() => vi.unstubAllGlobals());

describe('API client', () => {
  it('uses an in-memory bearer and same-origin requests', async () => {
    const fetcher = vi.fn().mockResolvedValue(new Response(JSON.stringify({ nodes: [], pools: [], workspaces: [], observedAt: '' }), { status: 200, headers: { 'content-type': 'application/json' } }));
    vi.stubGlobal('fetch', fetcher);
    const api = new APIClient({ apiBase: '/v1', refreshMs: 5000 });
    api.setCredential('operator-token');
    await api.fleet();
    expect(fetcher).toHaveBeenCalledWith('/v1/console/fleet', expect.objectContaining({ credentials: 'same-origin', cache: 'no-store' }));
    const init = fetcher.mock.calls[0]?.[1] as RequestInit;
    expect(new Headers(init.headers).get('Authorization')).toBe('Bearer operator-token');
    expect(localStorage.length).toBe(0);
  });

  it('preserves stable protocol errors', async () => {
    vi.stubGlobal('fetch', vi.fn().mockResolvedValue(new Response(JSON.stringify({ error: { code: 'denied', message: 'operator required' } }), { status: 403 })));
    await expect(new APIClient({ apiBase: '/v1', refreshMs: 5000 }).fleet()).rejects.toEqual(expect.objectContaining<Partial<APIError>>({ status: 403, code: 'denied' }));
  });

  it('adds a fresh idempotency key to mutations', async () => {
    const fetcher = vi.fn().mockResolvedValue(new Response(JSON.stringify({ id: 'ws_one' }), { status: 201 }));
    vi.stubGlobal('fetch', fetcher);
    await new APIClient({ apiBase: '/v1', refreshMs: 5000 }).createWorkspace({ name: 'one' });
    const init = fetcher.mock.calls[0]?.[1] as RequestInit;
    expect(new Headers(init.headers).get('Idempotency-Key')).toMatch(/^[0-9a-f-]{36}$/);
  });

  it('maps HTTP to the documented WebSocket transport', () => {
    const api = new APIClient({ apiBase: '/v1', refreshMs: 5000 });
    api.setCredential('abc');
    expect(api.terminalURL('ws/a', 's:1', { since: '2026-09-03T00:00:00Z' })).toContain('ws://localhost:3000/v1/console/workspaces/ws%2Fa/terminal?session=s%3A1&since=');
    expect(api.terminalProtocols()).toEqual(['remount.bearer.YWJj']);
  });
});
