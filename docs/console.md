# Operator console

Remount's console is a fleet and audit surface served at `/console/`. It is
not an agent chat UI: it intentionally has no prompt composer, transcript
renderer or harness-specific view. Operators use it to inspect authority and
capacity, attach to an explicitly selected session, read or edit workspace
files, follow canonical events, decide approvals, inspect budgets, and invoke
role-gated lifecycle actions.

## Build and test

```sh
cd web
npm ci --ignore-scripts
npm test
npm run build
npm run check:dist
npx playwright install chromium
npx playwright test
```

The production build is in `web/dist`. `dist.sha256` is the committed manifest
the embed package verifies. `scripts/check-dist.mjs` also rejects a sum larger
than 1,500,000 gzip bytes. The build uses `/console/` as its base and fetches
`/console/config.json` at startup. The only supported runtime keys are:

```json
{"apiBase":"/v1","refreshMs":5000}
```

`apiBase` must resolve to the page's origin and contain no username or
password. `refreshMs` is clamped to 1–60 seconds. Never put a token, provider
secret, hostname allow-list credential or tenant-specific value in this file:
it is a public static asset.

The login form holds the operator credential in memory only. Reloading or
closing the tab forgets it. Fetches use `Authorization: Bearer`; WebSockets use
the protocol's `remount.bearer.<base64url(credential)>` subprotocol. Do not
change the console to use the preview-only `remount_session` cookie. Preview
content is workspace-controlled and currently shares this origin.

## Server integration contract

The browser client is complete against the following adapter. These routes
must authenticate at the server, constrain reads to the subject's tenant, and
require `operator` for every mutation. A wildcard operator is the only subject
that may cross tenants. JSON errors use the protocol's stable error shape.
Mutation adapters must add or require an idempotency key before calling the
existing protocol operation.

All timestamps are RFC 3339 strings. Counts and cursors are JSON integers.
Every collection is bounded server-side; the maximum accepted `limit` below
is 1000.

### Fleet and workspace

| Method and route | Result or body |
|---|---|
| `GET /v1/console/fleet` | `{nodes: NodeSummary[], pools: PoolSummary[], workspaces: WorkspaceSummary[], observedAt}` from one bounded observation |
| `POST /v1/console/workspaces` | `{name, image?}` → `201 WorkspaceDetail` |
| `GET /v1/console/workspaces/{id}` | `WorkspaceDetail`, including bounded sessions, snapshots and generation history |
| `POST .../{id}/exec` | `{command: string[]}` → `201 {session}`; operator only |
| `POST .../{id}/snapshot` | `{}` → `{snapshot, workspace}` |
| `POST .../{id}/move` | `{node}` → `{workspace}` |
| `POST .../{id}/quarantine` | `{}` → `{workspace}` |
| `POST .../{id}/destroy` | `{}` → `204` or a final workspace envelope |

`WorkspaceSummary` is `{id,name,tenant,state,node?,generation,updatedAt}`.
`WorkspaceDetail` adds `spec`, `sessions`, `snapshots`, and `generations`.
Session rows are `{id,kind,command?,startedAt,endedAt?,next,earliest}`.
Snapshot rows are `{id,artifact,generation,createdAt,bytes}`. Generation rows
are `{generation,node?,state,at,eventSeq}`. State and event fields come from
durable control data; the adapter must not infer a successful move from a node
connection or request response.

### Files

| Method and route | Result or body |
|---|---|
| `GET .../{id}/files?path=/` | `{entries:[{name,path,kind,size,modifiedAt}]}`; jailed `fs.list` |
| `GET .../{id}/file?path=...` | `{path,content,etag}`; text files only, maximum 1 MiB for the console |
| `PUT .../{id}/file?path=...` | `If-Match: etag`, `{content}` → `{path,etag}`; jailed atomic write |

An ETag binds the file read to the workspace generation and content digest.
A stale generation or content digest is `409 conflict`; the adapter never
turns that into last-write-wins. Path validation and symlink containment stay
in `fsops`, not the UI.

### Timeline, approvals and usage

`GET /v1/console/events?after=&limit=&types=&principal=&session=&credential=`
returns `{events: Event[], next}`. `types` is a comma-separated allow-listed
set; other filters are exact matches. This is a bounded poll over the canonical
event log, not a second audit store. A request below retention returns the
existing explicit gap/eviction response. Credential values are identifiers,
never secret material.

`GET /v1/console/approvals?status=pending` returns `{approvals}` across Agents
and broker egress for the subject's tenant. Decisions use the existing
`POST /v1/approvals/{id}` route with `{denied,option?}`. `GET /v1/usage` is the
existing read-only meter route.

### Terminal

`GET /v1/console/workspaces/{id}/terminal` upgrades to a WebSocket. It accepts
`session=ID` and exactly one of `from=N` or `since=RFC3339`. Because session
chunks do not carry wall-clock timestamps, `since` resolves conservatively to
the oldest retained cursor whose session interval overlaps the requested time;
it may replay extra bytes but never silently omit bytes. If the requested time
predates retained data the first server message is a `gap`. The endpoint never
wakes or creates a process when `session` is present. The selected generation
and authorization are revalidated after taking the workspace boundary.

Input and lifecycle controls match the existing Agent terminal. Console output
adds the sequence required for byte-exact browser reconnect:

- binary browser → server: PTY input;
- text browser → server: `{"type":"resize","rows":R,"cols":C}`,
  `signal`, base64 `input`, or `eof` controls;
- text server → browser: `open` first; each output item is
  `{"type":"chunk","seq":N,"stream":"stdout|stderr","data":"base64"}`;
  optional explicit `gap` records and `exit` are last.

The UI requests `since` for the last 10 minutes, advances its cursor only after
accepting a sequenced chunk, and reconnects with `from`. The adapter may
internally translate this route to `client.Attach`; it must not strip sequence
information as the Agent terminal endpoint does. Slow readers are closed by
the existing write deadline rather than growing an unbounded buffer.

## Static response policy

Serve `index.html` with `Cache-Control: no-cache`; hashed assets may be
immutable. `config.json` is `no-store`. At minimum return:

```text
Content-Security-Policy: default-src 'self'; script-src 'self'; style-src 'self' 'unsafe-inline'; connect-src 'self' wss: ws:; img-src 'self' data:; object-src 'none'; base-uri 'none'; frame-ancestors 'none'
Referrer-Policy: no-referrer
X-Content-Type-Options: nosniff
```

The style exception is required by xterm element positioning; scripts remain
self-only with no inline code. Do not enable a service worker: a stale worker
would become a second authority over loaded console code. Unknown
`/console/*` paths may fall back to `index.html`, but `/console/assets/*` and
`config.json` must return 404 when absent rather than HTML with the wrong MIME
type.

## Verification

Unit tests cover fail-closed runtime configuration, bearer transport, stable
errors, hostile text rendering, and approval actions. The Playwright flow uses
a stateful mock adapter and proves create → exec/terminal → read/write →
snapshot → move → reattach plus `leak_blocked` visibility and approval. The
sim-backed Go E15 additionally drives two real nodes through create, exec,
forced disconnect and cursor reattach, file CAS, checkpoint, move and the
canonical timeline so authorization, generation fencing and replay are proved
rather than mocked.
