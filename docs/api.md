# The agent HTTP API

The control plane serves a JSON HTTP API for Agents next to the frame
protocol. It exists for user interfaces: a web client, an editor extension, a
chat integration, anything that wants to create an agent, follow its
conversation, look at its files, approve a tool call or open the preview it
is serving. The CLI's `remount agent ...` commands use the SDK over the frame
protocol; the HTTP API is a thin translation of the same SDK calls, so both
surfaces have the same authorization, the same idempotency and the same
events. Nothing is authorized in the HTTP layer; the control plane and the
node decide exactly as they would for the CLI.

The normative operation semantics are in `spec/PROTOCOL.md` §6.1–6.3. This
document is the HTTP shape.

## Base URL, authentication, errors

Every route is under the server's base URL (`--public-url` when set). A
request authenticates one of three ways:

| How | Where it is accepted |
|---|---|
| `Authorization: Bearer <credential>` | everywhere; the normal form |
| WebSocket subprotocol `remount.bearer.<base64url(credential)>` | WebSocket routes, for browsers that cannot set a header on a socket |
| cookie `remount_session` | the preview proxy (`/v1/agents/{id}/ports/...`) only |

The credential is the same token or capability the CLI uses (§4 of the
protocol). A cookie is minted by `POST /v1/session` with a header credential
and cleared by `DELETE /v1/session`; it is `HttpOnly`, `SameSite=Strict`,
`Secure` over TLS, and lives twelve hours. It exists so a browser can open a
preview link by navigation; it authenticates nothing else. In particular the
terminal, filesystem, diff, transcript and agent routes, and the JSON form of
the stable link `GET /a/{id}` (the redirect form needs no credential), take a
header or WebSocket subprotocol only, because a preview page is served from
this API's origin and is written by the untrusted program in the workspace: a
cookie honoured on those routes would let that page act as the operator on
every agent the operator can reach. A cookie-authenticated request whose
`Origin` is neither the API's own host nor a listed CORS origin is refused
with `403 denied`, before it reaches a workspace. Operators who want previews
isolated from the API entirely should front `/v1/agents/{id}/ports/` from a
separate hostname.

Errors are one shape with the protocol's stable code:

```json
{"error": {"code": "not_found", "message": "agent ag_01..."}}
```

| code | status |
|---|---|
| `bad_request` | 400 |
| `unauthorized` | 401 (with `WWW-Authenticate: Bearer`) |
| `denied` | 403 |
| `not_found` | 404 |
| `conflict` | 409 |
| `evicted` | 410 |
| `resource_exhausted` | 429 |
| `unsupported` | 501 |
| `unreachable`, `closed` | 503 |
| `timeout` | 504 |
| anything else | 500, message replaced by `internal error` |

JSON bodies are limited to 1 MiB and unknown fields are rejected. A mutation
takes an `Idempotency-Key` header; a retry with the same key returns the
original result and a reuse with different arguments is `409 conflict`,
exactly as for the SDK. Without the header every request is a new mutation.

`--cors ORIGIN[,ORIGIN...]` enables cross-origin browser access. A named
origin is echoed with `Access-Control-Allow-Credentials: true`; `*` allows
any origin without credentials. Preflights are answered with the methods and
headers the API uses (`Authorization`, `Content-Type`, `Idempotency-Key`,
`Last-Event-ID`) and `Location`/`Idempotency-Key` are exposed.

## Usage

`GET /v1/usage?tenant=&ws=&principal=&binding=&window=` translates to
`usage.get` and returns `UsageRes{usage}`. `window`, when present, is `1h`,
`1d`, or `30d`. Each row names its budget and reports requests, tokens,
estimated cost in micro-dollars, active reservations, unmetered requests and
incomplete requests. The authenticated subject may read only its tenant unless
it is a wildcard operator; an unavailable or unsupported meter is explicit and
is never represented as zero-cost healthy usage.

Budget definitions themselves are managed through the frame API and
`remount budget`; this HTTP route is read-only so dashboards cannot turn a
read credential into governance mutation authority.

## Agents

| Route | Body → result |
|---|---|
| `POST /v1/agents` | `AgentCreateReq` → `201` `Agent`, `Location: /v1/agents/{id}` |
| `GET /v1/agents?status=&ws=&parent=` | `AgentListRes{agents}`; only agents the caller may read |
| `GET /v1/agents/{id}` | `Agent` |
| `POST /v1/agents/{id}/messages` | `{text, kind?}` → `AgentMessageRes{agent, message, degraded?, woken?}` |
| `POST /v1/agents/{id}/cancel` | `Agent`; drops the inbox and cancels the turn, the run stays up |
| `POST /v1/agents/{id}/sleep` | `Agent`; checkpoints and pauses the workspace |
| `POST /v1/agents/{id}/wake` | `{by?}` (or `?by=`) → `Agent`; `agent.woken{by}` |
| `POST /v1/agents/{id}/fork` | `{name?, task?, policy?}` → `201` `Agent`, `Location` |
| `POST /v1/agents/{id}/destroy` | `204` |
| `GET /a/{id}` | the stable per-agent URL, see below |

`Agent`, `AgentCreateReq`, `AgentSpec` and `AgentPolicy` are the protocol
types serialized as JSON with their `json` tags; the request to create an
agent from a recipe is:

```json
{
  "name": "fix-ci",
  "workspace": {"name": "fix-ci", "repo": {"url": "https://github.com/o/r", "ref": "main"},
                "labels": {"remount.binding.b_openai": "..."}},
  "spec": {"recipe": "opencode", "recipe_yaml": "...", "task": "make CI green",
           "model": "openai/gpt-4o-mini", "sandbox": "workspace-write", "providers": ["openai"]},
  "policy": {"approve": "on-request", "max_turns": 50, "sleep_after_sec": 900}
}
```

`ws` adopts an existing workspace instead of `workspace`. The task text goes
into the inbox as the first message; events carry its hash, never the text.

`GET /a/{id}` is what `Agent.url` points at. It holds no capability. With
`--agent-ui URL` configured the server answers `302` to that UI (`{id}`
replaced, or the id appended), unauthenticated, because the UI authenticates.
Without one it authenticates and returns the `Agent` JSON. An id that is not
one URL-safe token is `404` before any redirect is built.

## Transcript

`GET /v1/agents/{id}/transcript?from=N&limit=M` reads the durable mirror
(§6.2) as a page:

```json
{"records": [{"index": 0, "run": "run_...", "seq": 3, "stream": "acp_out", "at": 1700000000000,
              "frame": {"at": ..., "frame": {"jsonrpc": "2.0", "method": "session/prompt", ...},
                        "redacted": false, "truncated": false}}],
 "next": 1, "gap": {"from": 0, "to": 12}, "done": false}
```

`stream` is `acp_in`, `acp_out`, `stdout`, `stderr`, `exit`, `info` or `gap`.
ACP streams carry the decoded frame (`frame`), stdout and stderr carry `text`, anything
else carries raw `data`. `gap` names an evicted range and the page starts at
`gap.to`; `done` is set once the agent is terminal and everything is read.
Reading the transcript never wakes a sleeping agent.

The same route streams two ways, both resumable from a cursor:

- **Server-Sent Events** with `Accept: text/event-stream`: events `record`
  (id = index + 1, data = the record), `gap` (id = gap.to) and `done`. A
  reconnect with `Last-Event-ID` resumes; the browser's `EventSource` does
  that by itself. A `: keepalive` comment is sent while idle.
- **WebSocket** upgrade on the same URL: JSON text messages
  `{"type":"record","record":...}`, `{"type":"gap","gap":...}` and
  `{"type":"done","next":N}`, with pings while idle.

Both end when `done` is emitted; a stream for an agent that is still running
stays open across sleeps and moves because it reads the mirror, not a node.

## Approvals

| Route | Result |
|---|---|
| `GET /v1/agents/{id}/approvals?status=` | `ApprovalListRes{approvals}`; pending unless `status` is given |
| `GET /v1/approvals/{id}` | `Approval`, including `detail` (the harness's raw request) |
| `POST /v1/approvals/{id}` | `{option?, denied?, content?}` → `Approval` as decided |

Decision validation is per kind (§6.1): a `tool_call` takes one of the
offered `options`, an `elicitation` takes `content` (a JSON object) or
`denied`, an `egress` takes `allow`/`deny`. A second decision is `409`.

## Diff

`GET /v1/agents/{id}/diff[?wake=true]` returns

```json
{"status": "<git status --porcelain=v1 --untracked-files=all>", "diff": "<git diff HEAD>", "truncated": false}
```

from the workspace, bounded to 4 MiB. Without `wake=true` a sleeping agent
is `409 conflict` rather than woken; with it the workspace is woken
(`agent.woken{by: diff}`) and materialized first. A workspace without a git
repository is `503 unreachable` with git's message.

## Terminal

`GET /v1/agents/{id}/terminal` upgrades to a WebSocket that is the xterm.js
endpoint. Binary messages are raw bytes in both directions. Text messages
are JSON control frames:

```
browser → server  {"type":"resize","rows":R,"cols":C}
                  {"type":"signal","signal":"SIGINT"}
                  {"type":"input","data":"<base64>"}
                  {"type":"eof"}
server → browser  {"type":"open","session":S,"next":N}          first
                  {"type":"gap","from":F,"to":T}                 output evicted
                  {"type":"exit","code":C,"signal":..,"error":..,"reason":..}  last
```

Without `?session=` a new pty runs `?program=` (repeatable; default
`/bin/sh`) in `?cwd=` with `?rows=`/`?cols=`. With `?session=ID&from=N` the
socket attaches to an existing session and replays retained output from `N`:
a reconnecting UI resumes, and a pty-mode agent's own session is watched
live. Closing the socket detaches; it never kills the session. A sleeping
agent is `409 conflict`, not woken: attaching a terminal is a read, so wake
it first through `/wake`. A browser that stops reading
is disconnected after 30 s rather than allowed to hold the session's output.

## Files

`/v1/agents/{id}/fs/{path}` is the workspace filesystem, jailed by the node
(§7). Paths are workspace-relative.

| Method | Effect |
|---|---|
| `GET` | a file's bytes (`application/octet-stream`, `nosniff`); a directory (trailing slash, `?list=1` or an actual directory) as `FSListRes{entries}`; `?stat=1` as one `FSEntry` |
| `HEAD` | as `GET` without the body |
| `PUT` | writes the body (≤ 64 MiB), `?mode=0644` octal, parents created |
| `DELETE` | removes; `?recursive=1` for a tree |

Reads and writes need a materialized workspace and do not wake a sleeping
agent (`409`); wake it first through `/wake` or a preview.

## Preview proxy

`/v1/agents/{id}/ports/{port}/{rest}` is an authenticated reverse proxy to
TCP `{port}` inside the workspace, the same forward `remount port` uses, for
HTTP and WebSocket traffic. The program in the workspace sees a plain client
on `127.0.0.1` with `X-Forwarded-Host`, `X-Forwarded-Proto` and
`X-Forwarded-Prefix` (`/v1/agents/{id}/ports/{port}`) naming the outside.
`Authorization`, the `remount_session` cookie and any `remount.bearer.*`
WebSocket subprotocol are stripped before forwarding; other cookies and
subprotocols pass. `/ports/{port}` without a trailing slash
redirects to it. A sleeping agent is woken first (`agent.woken{by:
preview}`). `remount agent open ID --ui` runs the recipe's `ui` command in
the workspace and prints this URL.

## Webhooks

`POST /v1/events` takes `{type, stream?, payload?, agent?}` and appends the
event. Authenticate with a bearer, or sign the raw body with the server's
`--webhook-secret` (`X-Remount-Signature: sha256=<hex>`; GitHub's
`X-Hub-Signature-256` is accepted as is). The optional `agent` block acts on
the event:

```json
{"type": "github.issues",
 "payload": {"action": "labeled", "issue": {"number": 42, "title": "…", "body": "…"}},
 "agent": {"idempotency_key": "gh-{{.issue.number}}",
           "create": {"name": "issue-{{.issue.number}}",
                      "workspace": {"name": "issue-{{.issue.number}}", "bindings": ["github"]},
                      "spec": {"recipe": "codex", "task": "Fix #{{.issue.number}}: {{.issue.title}}\n\n{{.issue.body}}"}}}}
```

`{"wake": "ag_…", "message": "{{.comment.body}}"}` sends a follow-up to an
existing agent instead; `"wake": "name:issue-{{.issue.number}}"` names the
one live agent with that name (`404` for none, `409` for several), so a
comment payload that only knows the issue still finds the agent the label
created. Templates are Go `text/template` over the payload; a missing key is
a `400`, never an empty prompt. `examples/diy-devin/github/` is a GitHub
Actions workflow that posts both. A signed request acts as
`--webhook-token`; without that flag signed requests may only append. The
response is `202 {"accepted": true, "agent": "ag_…", "ws": "ws_…", "status":
"…"}`. Redelivering the same webhook (same rendered `idempotency_key`)
returns the same agent.

## Scheduling and children

`policy.start_at` (Unix milliseconds) holds the first run until that time:
the agent is `scheduled`, its workspace is ready, and messages queue. The CLI
takes `--at 09:00`, `--at 90m` or an RFC 3339 instant; `remount run
--sleep-until 02:00` on an ACP recipe is the same policy.

`POST /v1/agents` with `parent` creates a child that inherits the parent's
providers, sandbox, bindings and security profile unless it names a subset;
anything wider is `403`. When the child ends, the parent receives a `kind:
child` inbox message carrying `{child, name, status, reason, turns, ws, url}`
and an `agent.child.finished` event. `GET /v1/agents?parent=ID` lists a
parent's children.

## Bounds

| What | Bound |
|---|---|
| JSON request body | 1 MiB |
| agent message | 256 KiB |
| filesystem write | 64 MiB |
| diff response | 4 MiB |
| transcript page | 1000 records |
| distinct credentials with a live in-process client | `--max-api-clients` (256); idle ones close after 10 min |
| wait for a woken workspace to be claimed | 90 s |

Every bound is a `4xx`/`5xx` with a stable code, never a truncated success.
