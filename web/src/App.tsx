import { useCallback, useEffect, useMemo, useState } from 'preact/hooks';
import type { APIClient } from './api';
import { Badge, bytes, ConfirmButton, Empty, ErrorNotice, formatDate, Loading, PageHeader, StateBadge } from './components';
import { usePolling } from './hooks';
import { Terminal } from './Terminal';
import type { Approval, EventRecord, RuntimeConfig, WorkspaceDetail } from './types';

type Route = { view: 'fleet' | 'timeline' | 'security' | 'usage' } | { view: 'workspace'; id: string };

function routeFromHash(): Route {
  const value = window.location.hash.replace(/^#\/?/, '');
  if (value.startsWith('workspaces/')) {
    try { return { view: 'workspace', id: decodeURIComponent(value.slice('workspaces/'.length)) }; }
    catch { return { view: 'fleet' }; }
  }
  if (value === 'timeline' || value === 'security' || value === 'usage') return { view: value };
  return { view: 'fleet' };
}

function Nav({ route }: { route: Route }) {
  const selected = route.view;
  return <aside class="sidebar">
    <a class="brand" href="#/fleet" aria-label="Remount console home"><span class="brand-mark">R</span><span>Remount</span></a>
    <nav aria-label="Console">
      <a aria-current={selected === 'fleet' || selected === 'workspace' ? 'page' : undefined} href="#/fleet">Fleet</a>
      <a aria-current={selected === 'timeline' ? 'page' : undefined} href="#/timeline">Timeline</a>
      <a aria-current={selected === 'security' ? 'page' : undefined} href="#/security">Security</a>
      <a aria-current={selected === 'usage' ? 'page' : undefined} href="#/usage">Usage</a>
    </nav>
    <p class="sidebar-note">Operator surface<br/><span>mutations are role-gated</span></p>
  </aside>;
}

function Fleet({ api, refreshMs }: { api: APIClient; refreshMs: number }) {
  const load = useCallback((signal: AbortSignal) => api.fleet(signal), [api]);
  const state = usePolling(load, refreshMs);
  const [creating, setCreating] = useState(false);
  const [name, setName] = useState('');
  const [image, setImage] = useState('');
  const [mutationError, setMutationError] = useState<Error>();
  const create = async (event: Event) => {
    event.preventDefault(); setMutationError(undefined);
    try {
      const workspace = await api.createWorkspace({ name, image: image || undefined });
      window.location.hash = `#/workspaces/${encodeURIComponent(workspace.id)}`;
    } catch (error) { setMutationError(error as Error); }
  };
  return <>
    <PageHeader eyebrow="Control plane" title="Fleet" actions={<button class="button" type="button" onClick={() => setCreating(true)}>New workspace</button>} />
    <ErrorNotice error={state.error} />
    <ErrorNotice error={mutationError} title="Could not complete that action." />
    {creating && <div class="modal-backdrop" role="presentation"><form class="modal" aria-labelledby="new-workspace-title" onSubmit={create}>
      <h2 id="new-workspace-title">New workspace</h2>
      <label>Name<input autoFocus required value={name} onInput={(e) => setName(e.currentTarget.value)} /></label>
      <label>Image <span class="muted">optional</span><input value={image} onInput={(e) => setImage(e.currentTarget.value)} placeholder="ghcr.io/…" /></label>
      <div class="actions"><button class="button secondary" type="button" onClick={() => setCreating(false)}>Cancel</button><button class="button" type="submit">Create</button></div>
    </form></div>}
    {state.loading && !state.data ? <Loading /> : state.data && <>
      <div class="metrics-grid" aria-label="Fleet totals">
        <Metric label="Workspaces" value={state.data.workspaces.length} />
        <Metric label="Online nodes" value={state.data.nodes.filter((n) => n.state === 'online').length} />
        <Metric label="Pools" value={state.data.pools.length} />
        <Metric label="Attention" value={state.data.workspaces.filter((w) => w.state === 'failed' || w.state === 'quarantined').length} tone="bad" />
      </div>
      <section class="panel"><div class="panel-header"><h2>Workspaces</h2><span>Observed {formatDate(state.data.observedAt)}</span></div>
        {state.data.workspaces.length === 0 ? <Empty>No workspaces in this tenant.</Empty> : <div class="table-wrap"><table><thead><tr><th>Name</th><th>State</th><th>Node</th><th>Generation</th><th>Updated</th></tr></thead><tbody>
          {state.data.workspaces.map((ws) => <tr key={ws.id}><td><a class="row-link" href={`#/workspaces/${encodeURIComponent(ws.id)}`}>{ws.name || ws.id}<small>{ws.id}</small></a></td><td><StateBadge state={ws.state}/></td><td><code>{ws.node ?? '—'}</code></td><td>{ws.generation}</td><td>{formatDate(ws.updatedAt)}</td></tr>)}
        </tbody></table></div>}
      </section>
      <div class="two-column">
        <section class="panel"><div class="panel-header"><h2>Nodes</h2></div>{state.data.nodes.length === 0 ? <Empty>No enrolled nodes.</Empty> : <ul class="card-list">{state.data.nodes.map((node) => <li key={node.id}><div><strong>{node.id}</strong><p>{Object.entries(node.labels).map(([k,v]) => `${k}=${v}`).join(' · ') || 'No labels'}</p></div><div class="right"><StateBadge state={node.state}/><span>{node.assignments}/{node.capacity}</span></div></li>)}</ul>}</section>
        <section class="panel"><div class="panel-header"><h2>Pools</h2></div>{state.data.pools.length === 0 ? <Empty>No configured pools.</Empty> : <ul class="card-list">{state.data.pools.map((pool) => <li key={pool.id}><div><strong>{pool.id}</strong><p>{pool.vendor} · range {pool.min}–{pool.max}</p></div><div class="right"><StateBadge state={pool.state}/><span>{pool.actual}/{pool.desired}</span></div></li>)}</ul>}</section>
      </div>
    </>}
  </>;
}

function Metric({ label, value, tone }: { label: string; value: number; tone?: string }) {
  return <div class={`metric ${tone ?? ''}`}><span>{label}</span><strong>{value}</strong></div>;
}

function Workspace({ api, id, refreshMs }: { api: APIClient; id: string; refreshMs: number }) {
  const load = useCallback((signal: AbortSignal) => api.workspace(id, signal), [api, id]);
  const state = usePolling(load, refreshMs);
  const [tab, setTab] = useState<'overview' | 'terminal' | 'files'>('overview');
  const [session, setSession] = useState('');
  const [command, setCommand] = useState('');
  const [replayMinutes, setReplayMinutes] = useState(10);
  const [mutationError, setMutationError] = useState<Error>();
  const detail = state.data;
  useEffect(() => { if (detail && !session && detail.sessions[0]) setSession(detail.sessions[0].id); }, [detail, session]);
  const mutate = async (action: 'snapshot' | 'move' | 'destroy' | 'quarantine', input: Record<string, unknown> = {}) => {
    setMutationError(undefined);
    try { await api.lifecycle(id, action, input); if (action === 'destroy') window.location.hash = '#/fleet'; else state.reload(); }
    catch (error) { setMutationError(error as Error); }
  };
  const run = async (event: Event) => {
    event.preventDefault();
    const argv = command.trim().split(/\s+/).filter(Boolean);
    if (!argv.length) return;
    try { const result = await api.exec(id, argv); setSession(result.session); setTab('terminal'); setCommand(''); state.reload(); }
    catch (error) { setMutationError(error as Error); }
  };
  if (state.loading && !detail) return <Loading />;
  if (!detail) return <><PageHeader title="Workspace unavailable"/><ErrorNotice error={state.error}/></>;
  return <>
    <PageHeader eyebrow={<a href="#/fleet">Fleet</a>} title={detail.name || detail.id} actions={<><StateBadge state={detail.state}/><ConfirmButton label="Quarantine" confirm="Quarantine this workspace and terminate its serviceability?" onConfirm={() => mutate('quarantine')} danger/><ConfirmButton label="Destroy" confirm="Destroy this workspace after its durable lifecycle boundary? This cannot be undone." onConfirm={() => mutate('destroy')} danger/></>} />
    <ErrorNotice error={state.error}/>
    <ErrorNotice error={mutationError} title="Could not complete that action."/>
    <div class="tablist" role="tablist" aria-label="Workspace views">
      {(['overview','terminal','files'] as const).map((value) => <button key={value} role="tab" aria-selected={tab === value} onClick={() => setTab(value)}>{value}</button>)}
    </div>
    {tab === 'overview' && <WorkspaceOverview
      detail={detail}
      onSnapshot={() => mutate('snapshot')}
      onMove={(node) => mutate('move', { node })}
    />}
    {tab === 'terminal' && <section>
      <form class="command-bar" onSubmit={run}><label htmlFor="command">Run command</label><input id="command" value={command} onInput={(e) => setCommand(e.currentTarget.value)} placeholder="make test"/><button class="button" type="submit">Run</button></form>
      <div class="terminal-controls"><label>Session<select value={session} onChange={(e) => setSession(e.currentTarget.value)}>{detail.sessions.map((item) => <option value={item.id} key={item.id}>{item.id} · {item.kind}</option>)}</select></label><label>Replay<select value={replayMinutes} onChange={(e) => setReplayMinutes(Number(e.currentTarget.value))}><option value={0}>Live only</option><option value={10}>Last 10 minutes</option><option value={60}>Last hour</option></select></label></div>
      {session ? <Terminal api={api} workspace={id} session={session} replayMinutes={replayMinutes} initialNext={detail.sessions.find((item) => item.id === session)?.next ?? 0}/> : <Empty>Run or select a session to attach.</Empty>}
    </section>}
    {tab === 'files' && <Files api={api} workspace={id}/>}
  </>;
}

function WorkspaceOverview({ detail, onSnapshot, onMove }: { detail: WorkspaceDetail; onSnapshot: () => void | Promise<void>; onMove: (node: string) => void | Promise<void> }) {
  const [node, setNode] = useState('');
  return <div class="two-column">
    <section class="panel"><div class="panel-header"><h2>Authority</h2></div><dl class="details"><dt>ID</dt><dd><code>{detail.id}</code></dd><dt>Tenant</dt><dd>{detail.tenant}</dd><dt>Node</dt><dd><code>{detail.node ?? 'unassigned'}</code></dd><dt>Generation</dt><dd>{detail.generation}</dd><dt>Security</dt><dd>{detail.spec.security ?? 'default'}</dd></dl><div class="actions"><button class="button secondary" type="button" onClick={() => void onSnapshot()}>Snapshot</button><label class="inline-label">Move to<input aria-label="Move target node" value={node} onInput={(e) => setNode(e.currentTarget.value)} placeholder="node id"/></label><button class="button secondary" disabled={!node} type="button" onClick={() => void onMove(node)}>Move</button></div></section>
    <section class="panel"><div class="panel-header"><h2>Generation history</h2></div><ol class="timeline-list">{detail.generations.map((item) => <li key={`${item.generation}-${item.eventSeq}`}><span class="timeline-dot"/><div><strong>Generation {item.generation}</strong><p>{item.state} on {item.node ?? 'no node'}</p><small>{formatDate(item.at)} · event {item.eventSeq}</small></div></li>)}</ol></section>
    <section class="panel"><div class="panel-header"><h2>Sessions</h2></div>{detail.sessions.length === 0 ? <Empty>No sessions.</Empty> : <ul class="card-list">{detail.sessions.map((item) => <li key={item.id}><div><strong>{item.id}</strong><p>{item.command?.join(' ') ?? item.kind}</p></div><div class="right"><Badge>{item.kind}</Badge><span>{item.next - item.earliest} chunks</span></div></li>)}</ul>}</section>
    <section class="panel"><div class="panel-header"><h2>Snapshots</h2></div>{detail.snapshots.length === 0 ? <Empty>No snapshots.</Empty> : <ul class="card-list">{detail.snapshots.map((item) => <li key={item.id}><div><strong>{item.id}</strong><p>Generation {item.generation} · {formatDate(item.createdAt)}</p></div><span>{bytes(item.bytes)}</span></li>)}</ul>}</section>
  </div>;
}

function Files({ api, workspace }: { api: APIClient; workspace: string }) {
  const [path, setPath] = useState('/');
  const [entries, setEntries] = useState<Awaited<ReturnType<APIClient['files']>>['entries']>([]);
  const [selected, setSelected] = useState('');
  const [content, setContent] = useState('');
  const [etag, setEtag] = useState('');
  const [error, setError] = useState<Error>();
  const load = useCallback(async () => { try { setEntries((await api.files(workspace, path)).entries); setError(undefined); } catch (reason) { setError(reason as Error); } }, [api, workspace, path]);
  useEffect(() => { void load(); }, [load]);
  const open = async (file: string) => { try { const result = await api.readFile(workspace, file); setSelected(result.path); setContent(result.content); setEtag(result.etag); } catch (reason) { setError(reason as Error); } };
  const save = async () => { try { const result = await api.writeFile(workspace, selected, content, etag); setEtag(result.etag); } catch (reason) { setError(reason as Error); } };
  return <section class="panel files"><ErrorNotice error={error}/><div class="file-toolbar"><label>Path<input value={path} onInput={(e) => setPath(e.currentTarget.value)}/></label><button class="button secondary" type="button" onClick={() => void load()}>Refresh</button></div><div class="file-grid"><ul aria-label="Files">{entries.map((entry) => <li key={entry.path}><button type="button" onClick={() => entry.kind === 'directory' ? setPath(entry.path) : void open(entry.path)}><span>{entry.kind === 'directory' ? '▸' : '·'}</span>{entry.name}<small>{entry.kind === 'file' ? bytes(entry.size) : entry.kind}</small></button></li>)}</ul><div class="editor">{selected ? <><div class="editor-bar"><code>{selected}</code><button class="button" type="button" onClick={() => void save()}>Save</button></div><textarea aria-label="File contents" spellcheck={false} value={content} onInput={(e) => setContent(e.currentTarget.value)}/></> : <Empty>Select a file to inspect or edit.</Empty>}</div></div></section>;
}

function Timeline({ api, refreshMs }: { api: APIClient; refreshMs: number }) {
  const [principal, setPrincipal] = useState(''); const [session, setSession] = useState(''); const [credential, setCredential] = useState('');
  const query = useMemo(() => { const p = new URLSearchParams({ limit: '250' }); if (principal) p.set('principal', principal); if (session) p.set('session', session); if (credential) p.set('credential', credential); return p; }, [principal, session, credential]);
  const load = useCallback((signal: AbortSignal) => api.events(query, signal), [api, query]);
  const state = usePolling(load, refreshMs);
  return <><PageHeader eyebrow="Canonical audit log" title="Timeline"/><div class="filters"><label>Principal<input value={principal} onInput={(e) => setPrincipal(e.currentTarget.value)} /></label><label>Session<input value={session} onInput={(e) => setSession(e.currentTarget.value)} /></label><label>Credential<input value={credential} onInput={(e) => setCredential(e.currentTarget.value)} /></label></div><ErrorNotice error={state.error}/>{state.loading && !state.data ? <Loading/> : <EventList events={state.data?.events ?? []}/>}</>;
}

function EventList({ events }: { events: EventRecord[] }) {
  if (!events.length) return <Empty>No events match these filters.</Empty>;
  return <ol class="event-list">{events.map((event) => <li key={event.seq}><time dateTime={event.at}>{formatDate(event.at)}</time><div><strong>{event.type}</strong><p>{[event.workspace, event.session, event.principal, event.credential].filter(Boolean).join(' · ') || event.tenant}</p></div><code>#{event.seq}</code></li>)}</ol>;
}

function Security({ api, refreshMs }: { api: APIClient; refreshMs: number }) {
  const approvalLoad = useCallback((signal: AbortSignal) => api.approvals(signal), [api]);
  const approvals = usePolling(approvalLoad, refreshMs);
  const eventParams = useMemo(() => new URLSearchParams({ types: 'cred.used,leak_blocked,egress.pending,egress.denied', limit: '100' }), []);
  const eventLoad = useCallback((signal: AbortSignal) => api.events(eventParams, signal), [api, eventParams]);
  const events = usePolling(eventLoad, refreshMs);
  const decide = async (approval: Approval, denied: boolean) => { await api.decideApproval(approval.id, denied, denied ? undefined : approval.options?.[0]); approvals.reload(); };
  return <><PageHeader eyebrow="Credential and policy boundary" title="Security"/><ErrorNotice error={approvals.error ?? events.error}/><div class="two-column"><section class="panel"><div class="panel-header"><h2>Pending approvals</h2><Badge tone={approvals.data?.approvals.length ? 'warn' : 'good'}>{approvals.data?.approvals.length ?? 0}</Badge></div>{approvals.data?.approvals.length ? <ul class="approval-list">{approvals.data.approvals.map((item) => <li key={item.id}><div><Badge>{item.kind}</Badge><strong>{item.prompt}</strong><p>{item.workspace} · {formatDate(item.createdAt)}</p></div><div class="actions"><button class="button secondary" onClick={() => void decide(item, true)}>Deny</button><button class="button" onClick={() => void decide(item, false)}>Approve</button></div></li>)}</ul> : <Empty>No pending approvals.</Empty>}</section><section class="panel"><div class="panel-header"><h2>Credential and leak events</h2></div><EventList events={events.data?.events ?? []}/></section></div></>;
}

function UsageView({ api, refreshMs }: { api: APIClient; refreshMs: number }) {
  const load = useCallback((signal: AbortSignal) => api.usage(signal), [api]); const state = usePolling(load, refreshMs);
  return <><PageHeader eyebrow="Daily reserve and settle" title="Usage & budgets"/><ErrorNotice error={state.error}/>{state.loading && !state.data ? <Loading/> : <section class="panel"><div class="table-wrap"><table><thead><tr><th>Budget</th><th>Scope</th><th>Requests</th><th>Tokens</th><th>Cost</th></tr></thead><tbody>{state.data?.usage.map((row) => <tr key={`${row.budget_id}-${row.principal}-${row.binding}`}><td><strong>{row.budget_id}</strong></td><td>{row.principal ?? row.binding ?? row.tenant}</td><td><Progress value={row.requests} max={row.max_requests}/></td><td><Progress value={row.tokens} max={row.max_tokens}/></td><td><Progress value={row.cost_micros} max={row.max_cost_micros} money/></td></tr>)}</tbody></table></div></section>}</>;
}

function Progress({ value, max, money = false }: { value: number; max?: number; money?: boolean }) {
  const label = money ? `$${(value / 1_000_000).toFixed(2)}` : value.toLocaleString(); const percent = max ? Math.min(100, Math.round(value / max * 100)) : 0;
  return <div class="progress-cell"><span>{label}{max ? ` / ${money ? `$${(max / 1_000_000).toFixed(2)}` : max.toLocaleString()}` : ''}</span>{max && <progress max={100} value={percent} aria-label={`${percent}% used`}/>}</div>;
}

export function App({ api, config, onLogout }: { api: APIClient; config: RuntimeConfig; onLogout?: () => void }) {
  const [route, setRoute] = useState(routeFromHash);
  useEffect(() => { const change = () => setRoute(routeFromHash()); window.addEventListener('hashchange', change); return () => window.removeEventListener('hashchange', change); }, []);
  return <div class="shell"><Nav route={route}/><main id="main">{onLogout && <button class="logout" type="button" onClick={onLogout}>Forget credential</button>}{route.view === 'fleet' && <Fleet api={api} refreshMs={config.refreshMs}/>} {route.view === 'workspace' && <Workspace api={api} id={route.id} refreshMs={config.refreshMs}/>} {route.view === 'timeline' && <Timeline api={api} refreshMs={config.refreshMs}/>} {route.view === 'security' && <Security api={api} refreshMs={config.refreshMs}/>} {route.view === 'usage' && <UsageView api={api} refreshMs={config.refreshMs}/>}</main></div>;
}

export function Console({ api, config }: { api: APIClient; config: RuntimeConfig }) {
  const [authenticated, setAuthenticated] = useState(false);
  const [credential, setCredential] = useState('');
  if (!authenticated) return <main class="login" id="main"><form onSubmit={(event) => { event.preventDefault(); api.setCredential(credential); setCredential(''); setAuthenticated(true); }}><span class="brand-mark">R</span><p class="eyebrow">Remount operator console</p><h1>Connect to this control plane</h1><p>The credential stays in memory and is forgotten when this tab closes.</p><label>Operator credential<input type="password" autoComplete="off" required value={credential} onInput={(event) => setCredential(event.currentTarget.value)}/></label><button class="button" type="submit">Open console</button></form></main>;
  return <App api={api} config={config} onLogout={() => { api.setCredential(''); setAuthenticated(false); }}/>;
}
