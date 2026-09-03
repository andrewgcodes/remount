import { render, screen, waitFor } from '@testing-library/preact';
import { describe, expect, it, vi } from 'vitest';
import { App } from '../src/App';
import { APIClient } from '../src/api';

function json(value: unknown, status = 200) { return Promise.resolve(new Response(JSON.stringify(value), { status, headers: { 'content-type': 'application/json' } })); }

describe('operator console', () => {
  it('renders fleet authority and navigates to workspace detail', async () => {
    window.location.hash = '#/fleet';
    vi.stubGlobal('fetch', vi.fn((url: string) => {
      if (url.endsWith('/console/fleet')) return json({ observedAt: '2026-09-03T00:00:00Z', nodes: [{ id: 'n_one', state: 'online', labels: { region: 'iad' }, assignments: 1, capacity: 4, lastSeen: '' }], pools: [], workspaces: [{ id: 'ws_one', name: 'demo', tenant: 'local', state: 'claimed', node: 'n_one', generation: 2, updatedAt: '2026-09-03T00:00:00Z' }] });
      return json({ id: 'ws_one', name: 'demo', tenant: 'local', state: 'claimed', node: 'n_one', generation: 2, updatedAt: '', spec: {}, sessions: [], snapshots: [], generations: [] });
    }));
    render(<App api={new APIClient({ apiBase: '/v1', refreshMs: 60_000 })} config={{ apiBase: '/v1', refreshMs: 60_000 }}/>);
    expect(await screen.findByRole('heading', { name: 'Fleet' })).toBeTruthy();
    expect(await screen.findByText('demo')).toBeTruthy();
    screen.getByText('demo').click();
    expect(await screen.findByRole('heading', { name: 'demo' })).toBeTruthy();
    vi.unstubAllGlobals();
  });

  it('exposes approval actions without rendering raw detail as HTML', async () => {
    window.location.hash = '#/security';
    const fetcher = vi.fn((url: string, init?: RequestInit) => {
      if (url.includes('/console/approvals')) return json({ approvals: [{ id: 'ap_one', workspace: 'ws_one', tenant: 'local', kind: 'egress', status: 'pending', prompt: '<img src=x onerror=alert(1)> github.com', options: ['allow_once'], createdAt: '2026-09-03T00:00:00Z' }] });
      if (url.includes('/console/events')) return json({ events: [], next: 1 });
      if (url.includes('/approvals/ap_one') && init?.method === 'POST') return json({ id: 'ap_one', status: 'approved' });
      return json({});
    });
    vi.stubGlobal('fetch', fetcher);
    render(<App api={new APIClient({ apiBase: '/v1', refreshMs: 60_000 })} config={{ apiBase: '/v1', refreshMs: 60_000 }}/>);
    expect(await screen.findByText('<img src=x onerror=alert(1)> github.com')).toBeTruthy();
    expect(document.querySelector('img')).toBeNull();
    screen.getByRole('button', { name: 'Approve' }).click();
    await waitFor(() => expect(fetcher).toHaveBeenCalledWith('/v1/approvals/ap_one', expect.objectContaining({ method: 'POST' })));
    vi.unstubAllGlobals();
  });
});
