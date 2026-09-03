import { createServer } from 'node:http';
import { promises as fs } from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
const now = '2026-09-03T12:00:00Z';
let workspace;
let file = { path: '/work/hello.txt', content: 'hello\n', etag: 'one' };
let approvals = [{ id: 'ap_1', workspace: 'ws_console', tenant: 'local', kind: 'egress', status: 'pending', prompt: 'Allow api.openai.com?', options: ['allow_once'], createdAt: now }];

function detail() {
  return { ...workspace, spec: { image: 'remount-workspace', security: 'isolated' }, sessions: workspace.sessions, snapshots: workspace.snapshots, generations: workspace.generations };
}

async function body(request) {
  const chunks = [];
  for await (const chunk of request) chunks.push(chunk);
  return chunks.length ? JSON.parse(Buffer.concat(chunks).toString()) : {};
}

function json(response, status, value) {
  const encoded = JSON.stringify(value);
  response.writeHead(status, { 'content-type': 'application/json', 'content-length': Buffer.byteLength(encoded), 'cache-control': 'no-store' });
  response.end(encoded);
}

const server = createServer(async (request, response) => {
  const url = new URL(request.url ?? '/', 'http://127.0.0.1:4173');
  if (url.pathname === '/healthz') return json(response, 200, { ok: true });
  if (url.pathname === '/console/config.json') return json(response, 200, { apiBase: '/v1', refreshMs: 60_000 });
  if (url.pathname === '/v1/console/fleet' && request.method === 'GET') return json(response, 200, { observedAt: now, nodes: [{ id: 'n_iad', state: 'online', labels: { region: 'iad' }, assignments: workspace ? 1 : 0, capacity: 4, lastSeen: now }], pools: [{ id: 'pool_iad', vendor: 'fly', desired: 1, actual: 1, min: 0, max: 5, state: 'ready' }], workspaces: workspace ? [workspace] : [] });
  if (url.pathname === '/v1/console/workspaces' && request.method === 'POST') {
    const input = await body(request);
    workspace = { id: 'ws_console', name: input.name, tenant: 'local', state: 'claimed', node: 'n_iad', generation: 1, updatedAt: now, sessions: [], snapshots: [], generations: [{ generation: 1, node: 'n_iad', state: 'claimed', at: now, eventSeq: 1 }] };
    return json(response, 201, detail());
  }
  if (url.pathname === '/v1/console/workspaces/ws_console' && request.method === 'GET') return json(response, 200, detail());
  if (url.pathname.endsWith('/exec') && request.method === 'POST') {
    const input = await body(request);
    workspace.sessions.unshift({ id: 's_console', kind: 'pty', command: input.command, startedAt: now, next: 4, earliest: 0 });
    return json(response, 201, { session: 's_console' });
  }
  if (url.pathname.endsWith('/files') && request.method === 'GET') return json(response, 200, { entries: [{ name: 'hello.txt', path: file.path, kind: 'file', size: file.content.length, modifiedAt: now }] });
  if (url.pathname.endsWith('/file') && request.method === 'GET') return json(response, 200, file);
  if (url.pathname.endsWith('/file') && request.method === 'PUT') { const input = await body(request); file = { ...file, content: input.content, etag: 'two' }; return json(response, 200, { path: file.path, etag: file.etag }); }
  if (url.pathname.endsWith('/snapshot') && request.method === 'POST') { workspace.snapshots.unshift({ id: 'snap_one', artifact: 'art_sha256:abc', generation: workspace.generation, createdAt: now, bytes: 128 }); return json(response, 200, { snapshot: 'snap_one', workspace: detail() }); }
  if (url.pathname.endsWith('/move') && request.method === 'POST') { const input = await body(request); workspace.node = input.node; workspace.generation += 1; workspace.updatedAt = now; workspace.generations.unshift({ generation: workspace.generation, node: workspace.node, state: 'claimed', at: now, eventSeq: 6 }); return json(response, 200, { workspace: detail() }); }
  if (url.pathname === '/v1/console/approvals' && request.method === 'GET') return json(response, 200, { approvals });
  if (url.pathname === '/v1/console/events' && request.method === 'GET') return json(response, 200, { next: 9, events: [{ seq: 8, at: now, type: 'leak_blocked', tenant: 'local', workspace: 'ws_console', credential: 'openai' }] });
  if (url.pathname === '/v1/approvals/ap_1' && request.method === 'POST') { approvals = []; return json(response, 200, { id: 'ap_1', status: 'approved' }); }
  if (url.pathname === '/v1/usage') return json(response, 200, { usage: [] });

  const relative = url.pathname.startsWith('/console/assets/') ? url.pathname.slice('/console/'.length) : url.pathname === '/console/' || url.pathname === '/console' ? 'index.html' : '';
  if (!relative || relative.includes('..')) { response.writeHead(404); return response.end('not found'); }
  try {
    const content = await fs.readFile(path.join(root, 'dist', relative));
    const contentType = relative.endsWith('.js') ? 'text/javascript' : relative.endsWith('.css') ? 'text/css' : 'text/html';
    response.writeHead(200, { 'content-type': contentType, 'content-security-policy': "default-src 'self'; script-src 'self'; style-src 'self' 'unsafe-inline'; connect-src 'self' ws:; img-src 'self' data:; object-src 'none'; base-uri 'none'; frame-ancestors 'none'" });
    response.end(content);
  } catch { response.writeHead(404); response.end('not found'); }
});

server.listen(4173, '127.0.0.1');
