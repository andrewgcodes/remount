# Using Remount

This is the canonical entry point for people and coding agents that want to
**use** Remount rather than change its implementation. It describes the
supported workflows and points to the detailed source for each one.

The current binary remains the authority for command syntax:

```sh
remount help
remount COMMAND --help
```

If installed help and this guide disagree, use the installed help and report
the documentation drift. The wire-level authority is
[`spec/PROTOCOL.md`](../spec/PROTOCOL.md).

## Choose the workflow

| Goal | Start here |
|---|---|
| Let Codex, OpenCode, Claude Code, or another harness edit a local checkout | [`remount run`](#run-a-coding-agent-on-a-checkout) |
| Keep a structured agent alive across disconnects, sleep, or node loss | [Durable Agents](#operate-a-durable-agent) |
| Read or bring back files an agent changed | [Retrieve modified files](#retrieve-modified-files) |
| Use workspaces directly as remote filesystems and process hosts | [Workspace primitives](#use-workspace-primitives-directly) |
| Drive a browser — screenshot, click, type, navigate, download | [Computer sessions](#computer-sessions-the-built-in-browser-api) |
| Run Chromium on a virtual X11 desktop or through VNC | [Browser and desktop workloads](#run-browser-and-virtual-desktop-workloads) |
| Keep a workspace claimed while background work runs, without leaking compute | [Hold a workspace](#hold-a-workspace-for-background-work) |
| Prove what a node's isolation actually is before trusting it | [Choose a runtime profile](#choose-a-runtime-profile) |
| Handle failures programmatically, or send traces to your own collector | [Public API, errors and tracing](#public-api-errors-and-tracing) |
| Try the checked-in deployment on Modal | [Modal reference deployment](#deploy-the-modal-reference) |
| Operate a production control plane or node fleet | [`operations.md`](operations.md) |
| Integrate a custom harness or model provider | [`harness-integration.md`](harness-integration.md) |
| Call the Agent HTTP API | [`api.md`](api.md) and [`examples/agent-api`](../examples/agent-api/README.md) |
| Inspect the fleet in a browser | [Operator console](console.md) |
| Distinguish implemented features from dated verification | [Current implementation status](engineering/current-status.md) |

## Build and select a server

From an accessible repository checkout, with the Go version required by
`go.mod` (currently 1.27.1):

```sh
make build
./remount version
```

Public release, package and installer availability is a separate gate; see
[releases and installation](releases.md) before relying on published artifacts.

Client commands use `http://127.0.0.1:7443` by default. There are three common
ways to select a deployment:

1. **Automatic local standalone.** `remount run` starts a local standalone
   when nothing is listening on the default address and no explicit server or
   token was supplied.
2. **Manual local standalone.** Run `./remount standalone --data ./data` in a
   separate terminal, then export
   `REMOUNT_SERVER=http://127.0.0.1:7443`.
3. **Remote deployment.** Export its HTTPS URL and bearer:

   ```sh
   export REMOUNT_SERVER=https://remount.example
   export REMOUNT_TOKEN=...
   ```

Do not place a bearer on a command line on a shared host. Process arguments
are visible to other local processes. Production identity, TLS, tokens, OIDC,
backups, and node enrollment are covered in
[`operations.md`](operations.md).

## Run a coding agent on a checkout

The shortest local path is:

```sh
export OPENAI_API_KEY=...
./remount run codex --dir /absolute/path/to/repo -- \
  'Fix the failing test, run the focused test, and summarize the change.'
```

Replace `codex` with `opencode`, `claude`, or another built-in recipe listed by
`remount help`. With automatic local standalone, exported provider keys become
bindings by environment-variable reference; their values are not written into
the generated bindings file or workspace.

For an explicit server-side binding — a named credential the server holds and
the workspace only ever sees as a placeholder; see
[Broker provider credentials](#broker-provider-credentials) for how one is
defined:

```sh
./remount run opencode \
  --dir /absolute/path/to/repo \
  --binding b_openai \
  --approve on-request \
  -- 'Create GREETING.txt containing exactly hello.'
```

Important behavior:

- `--dir` uploads a Git-ignore-aware copy of the checkout. `.git` is included
  by default; use `--include-git=false` to omit it.
- ACP-capable recipes run as durable Agents unless `--pty` is supplied.
- `--sandbox` is `read-only`, `workspace-write` (default), or `full`. The
  control plane applies the same default to API requests that omit `sandbox`;
  child Agents inherit their parent's sandbox before that default is applied.
  Recipes apply their corresponding harness-native PTY flags and, when
  configured, ACP session mode before the first prompt.
- `--approve` is `never` (default) or `on-request`.
- With `--ws`, an explicit `--security` is a minimum-profile assertion on
  the existing workspace, not an upgrade to its isolation. A weaker workspace
  is rejected; the flag cannot make a cooperative backend enforce isolation.
- Ctrl-C detaches by default; it does not kill the agent. Use
  `--kill-on-interrupt` only when termination is intended.
- `--detach` prints the Agent/workspace identifiers and returns immediately.
- A successful launch is not proof that the requested work finished. Inspect
  the transcript, resulting files or diff, exit state, and durable events.

Recipe/provider mappings, workspace-resident login behavior, broker settings,
and custom recipe syntax are maintained in
[`harness-integration.md`](harness-integration.md).

## Operate a durable Agent

An Agent owns a workspace and a durable transcript. These are the normal
inspection and interaction commands:

```sh
./remount agent ls
./remount agent get AGENT_ID
./remount agent watch AGENT_ID
./remount agent message AGENT_ID -- 'Also add a regression test.'
./remount agent message AGENT_ID --steer -- 'Stop editing that file.'
./remount agent diff AGENT_ID
./remount agent diff AGENT_ID --stat
```

To run an Agent in a workspace you created separately, repeat the workspace's
binding with its provider preset:

```sh
WS=$(./remount ws create --binding b_team)
./remount agent create codex --ws "$WS" --binding b_team:openai -- \
  'Inspect the repository and fix the failing test.'
```

The Agent persists the non-secret `b_team:openai` declaration so the node can
construct the recipe's `OPENAI_API_KEY` and `OPENAI_BASE_URL` placeholder
environment. The control plane rejects a declaration whose binding ID is not
attached to that workspace. Custom binding IDs and parameterized presets, such
as `b_az:azure-openai?host=myres`, must be repeated exactly.

`message` queues a follow-up. `--steer` requests a mid-turn interruption when
the adapter supports it; otherwise the message remains a follow-up.

For an agent created with `--approve on-request`:

```sh
./remount agent approvals AGENT_ID
./remount agent approve APPROVAL_ID --option OPTION
```

Use only an option returned by the approval request. To deny it:

```sh
./remount agent approve APPROVAL_ID --deny
```

Lifecycle commands are explicit:

```sh
./remount agent sleep AGENT_ID
./remount agent wake AGENT_ID
./remount agent cancel AGENT_ID
./remount agent destroy AGENT_ID
```

Sleep preserves the workspace and transcript. Cancel stops the current agent
run. Destroy is terminal and is the normal cleanup for a disposable agent.

## Retrieve modified files

Agent changes live in the workspace, not automatically in the original host
checkout. Choose the retrieval method that matches the result you need.

### Inspect before copying

For a Git checkout:

```sh
./remount agent diff AGENT_ID
./remount agent diff AGENT_ID --stat
```

For individual files:

```sh
./remount fs ls WORKSPACE_ID
./remount fs read WORKSPACE_ID path/to/file
./remount fs stat WORKSPACE_ID path/to/file
```

### Pull the workspace tree into a local checkout

```sh
./remount pull WORKSPACE_ID --dir /absolute/path/to/repo
```

`pull` snapshots the workspace and writes files that differ locally. It does
not delete local files. It refuses when the destination has uncommitted
changes; inspect them rather than immediately bypassing that check. If
overwriting is intentionally safe:

```sh
./remount pull WORKSPACE_ID --dir /absolute/path/to/repo --force
```

The reverse direction overlays a local tree onto the workspace:

```sh
./remount push WORKSPACE_ID --dir /absolute/path/to/repo
```

`push` also does not delete files absent from the uploaded archive. The exact
ignore and conflict behavior is documented in
[`tutorial.md`](tutorial.md#working-from-a-local-checkout).

Public SDKs can apply an uploaded archive below the workspace root when a
component owns a dedicated subtree. The destination must already exist and is
resolved through the workspace jail:

```python
await client.mkdir(workspace_id, "state")
await client.apply_tar(workspace_id, artifact_id, path="state")
```

The Go equivalent is `Client.ApplyTarAt`. An empty destination preserves the
root-overlay behavior used by `remount push`; neither form deletes paths the
archive does not name.

This is Go and Python only today. The TypeScript SDK does not expose
`fs.apply_tar`, so a TypeScript caller uses `remount push` or the Agent HTTP
API for the same effect, or calls the operation directly through the client's
generic `nodeCall`.

### Preserve or move the whole filesystem

```sh
./remount ws snapshot WORKSPACE_ID
./remount ws snapshot WORKSPACE_ID --authoritative
./remount ws move WORKSPACE_ID --label zone=gpu
```

A normal snapshot is a live export and may observe concurrent writes. An
authoritative snapshot quiesces Remount-managed execution and commits the
failover checkpoint before returning. A move transfers filesystem state, not
arbitrary process memory or architecture-specific binaries. This describes
filesystem checkpoints; the Linux [Firecracker full-checkpoint path](../integration/firecracker/README.md)
has separate host and guest compatibility requirements.

## Use workspace primitives directly

Start a manual local deployment:

```sh
./remount standalone --data ./data
```

In another terminal:

```sh
export REMOUNT_SERVER=http://127.0.0.1:7443
WS=$(./remount ws create --name demo)
./remount exec "$WS" -- sh -c 'printf "hello\n" > result.txt'
./remount fs read "$WS" result.txt
./remount sh "$WS"
```

Filesystem mutations are jailed to the workspace root:

```sh
./remount fs write "$WS" notes.md < local-notes.md
./remount fs ls "$WS"
./remount fs search "$WS" 'pattern'
./remount fs edit "$WS" notes.md 'old text' 'new text'
./remount fs mkdir "$WS" archive
./remount fs mv "$WS" notes.md archive/notes.md
./remount fs rm "$WS" archive/notes.md
```

The Python SDK's `write_file` helper splits large values into wire-safe
chunks. If a later chunk fails, the destination contains the acknowledged
prefix; retry the same bytes with the same explicit `idempotency_key` to
continue without duplicating that prefix.

Long-running session output is a sequenced log. Ctrl-C detaches unless the
caller explicitly requests termination:

```sh
./remount exec "$WS" -- sh -c 'for i in $(seq 1 60); do echo "$i"; sleep 1; done'
./remount attach "$WS" SESSION_ID --from 0
```

Workspace lifecycle:

```sh
./remount ws get "$WS"
./remount ws snapshot "$WS" --authoritative   # at most one snapshot per second
./remount ws sleep "$WS" --after 1h
./remount ws wake "$WS"
./remount ws destroy "$WS"
```

Release, move, sleep, and destroy cycles are ordered by a durable
control-plane release epoch. Exact retries remain idempotent, while a delayed
request from an older aborted cycle cannot fence or stop work in the restored
workspace.

### Hold a workspace for background work

`ws sleep` means "sleep now". The opposite — "keep this workspace claimed
because work is still running, then sleep it if nobody extends the deadline" —
is `ws lease`, and the reason it is a Remount operation rather than a timer in
your own process is that your process is not durable. A deploy, a scale-down,
an OOM kill or a partition takes an `asyncio.sleep` or a `setTimeout` with it,
and the workspace then runs until somebody notices the bill. The deadline has
to live where the workspace lives.

```sh
LEASE=$(./remount ws lease "$WS" --max 20m --min 30s --reason background_job --json | jq -r .id)
./remount ws lease renew "$WS" "$LEASE" --extend 10m
./remount ws lease get "$WS"
./remount ws lease cancel "$WS" "$LEASE"
```

One naming note, because both words appear below: the CLI verb is `lease`, and
this guide calls the deadline it grants a **hold**. They are the same thing.
(`ws.lease_expired` is unrelated: that is the node's claim lease, not a client's
hold.)

`--max` is the hard deadline: when it passes with nobody renewing, Remount
performs `--on-expiry`, which is `sleep` (the default) or `destroy`. `--min` is
the earliest the idle policy below may act, so a hold and a policy do not fight
each other. A workspace has at most one hold; a second `ws lease` replaces it.
`--extend` is measured from now, not from the original grant.

The idle policy is the other half: a no-work cleanup rule rather than a job
deadline.

```sh
./remount ws idle-policy "$WS" --sleep-after 10m --destroy-after 2h
./remount ws mark-idle "$WS" --reason turn_settled
./remount ws mark-active "$WS" --reason new_turn
```

**Activity is explicit.** `ws mark-active`, `ws lease` and `ws lease renew`
reset the clock. Session traffic does not, in either direction: a workspace
with a running `sleep 300` and no renewal still sleeps at its deadline, and a
workspace with an idle shell attached and nothing to do is still idle. Only the
caller can tell a settled turn from a pause, so mark activity every turn rather
than expecting Remount to infer it.

Read the schedule from the workspace, not from a timer of your own.
`ws get` and `ws lease get` publish `lifecycle_deadline` — what happens next,
when, and whether it came from the hold or the idle policy — and the CLI prints
it in words:

```
lifecycle deadline: sleep at 2026-09-06T02:27:34Z (in 4m12s, source=lease)
```

When the deadline passes, Remount releases the workspace through the same path
`ws sleep` uses: the node sends `SIGTERM`, waits five seconds, then `SIGKILL`,
and joins every session before the checkpoint is taken. Sessions ended this way
carry `exit.reason = "lifecycle_deadline_expired"`, so a replayed log says a
policy decision ended the work rather than leaving you to infer it. The
workspace publishes `paused` and the event log carries `ws.lifecycle.expired`
with the action, the source and the timer. A hold is a filesystem checkpoint,
not a memory checkpoint: the files come back on `ws wake`, the processes do
not. Expiry never schedules a resume — use `ws sleep --after` or `--on` for
that.

Failure is loud. If the release underneath an expiry fails, Remount retries
with backoff and then emits `ws.lifecycle.expiry_failed` with the attempt count
and the error, leaving the workspace degraded and operator-actionable rather
than quietly still running.

Holds are bounded. `--max` (and every idle-policy duration) may not exceed the
deployment's maximum hold, 24 hours by default, and a tenant may pin only so
many workspaces awake at once, 256 by default; the refusal is
`resource_exhausted` with reason `quota_exceeded` alongside a
`ws.hold.max_reached` event. Renewing after the deadline fired or the hold was
cancelled is refused with `conflict` and reason `lifecycle_deadline_expired`;
renewing after a `ws move` is refused with reason `generation_mismatch`. Both
mean take a fresh hold rather than assume you still hold one.

The built-in Agent resource keeps its own idle policy, which additionally
cancels the harness run so the transcript flushes and wakes the agent on an
inbox message. Neither is expressible for a raw workspace, which has no harness
and no inbox. What a raw workspace gets is the durable half: a control-plane
deadline, an idle clock, generation-aware refusals, and an expiry that survives
every process involved in asking for it.

`examples/long-running-autosleep` runs the whole cycle against a local server,
and §8 of [`tutorial.md`](tutorial.md) is the full reference.

The complete walkthrough, including bases, repository seeding, event-triggered
wake, ACLs, and moves between two machines, is
[`tutorial.md`](tutorial.md).

## Run browser and virtual desktop workloads

A browser is a protocol resource — a computer session, driven by the node. A
whole desktop is not: X11, a window manager and VNC are ordinary workspace
processes you compose yourself.

### Computer sessions: the built-in browser API

A **computer** is a browser the node drives inside a workspace on your behalf.
The node holds the DevTools conversation over the same path `remount port`
uses, so every backend that can forward a workspace port can host one and
nothing extra runs inside the workspace. Screenshots, clicks, typing,
navigation, downloads and crash reporting are protocol operations, not a script
you assemble yourself.

Build the reference image once ([`images.md`](images.md)), then drive it:

```sh
docker build -t remount-browser:local images/browser
WS=$(./remount ws create --backend docker --image remount-browser:local --json | jq -r .id)

CMP=$(./remount computer create "$WS" --json | jq -r .computer)
./remount computer navigate "$WS" "$CMP" https://example.internal/
./remount computer screenshot "$WS" "$CMP" --out shot.png
./remount computer click "$WS" "$CMP" 640 360
./remount computer type "$WS" "$CMP" 'search text'
./remount computer key "$WS" "$CMP" Enter
./remount computer eval "$WS" "$CMP" 'document.title'
./remount computer downloads "$WS" "$CMP" --json
./remount computer close "$WS" "$CMP"
```

Every subcommand takes `--json`. The same operations are in the Go, Python and
TypeScript SDKs:

```python
computer = await client.create_computer(workspace, viewport={"w": 1280, "h": 720})
await computer.navigate("https://example.internal/")
await computer.click(640, 360)
await computer.type("search text")
shot = await computer.screenshot()          # shot["png"] is PNG bytes
title = await computer.eval("document.title")
for download in await computer.downloads():
    print(download["state"], download.get("artifact", ""))
await computer.close()
```

```ts
const computer = await client.createComputer(workspace, { viewport: { w: 1280, h: 720 } });
await computer.navigate("https://example.internal/");
await computer.click(640, 360);
await computer.type("search text");
const shot = await computer.screenshot();
const title = await computer.eval("document.title");
await computer.close();
```

What the API promises, and what it does not:

- **Coordinates are CSS pixels from the top-left of the viewport** declared at
  `create` (1280x720 by default), and a screenshot is a PNG clipped to that
  same rectangle. The picture and the click that follows it agree.
- **Typing uses text insertion, not synthesized keystrokes**, so it behaves the
  same in an ordinary field, a contenteditable region and an iframe, and does
  not depend on a keyboard layout the image may not carry. `key` presses one
  named key (`Enter`, `Tab`, `ArrowDown`, …); an unknown name is refused rather
  than guessed.
- **Every action batch carries an input sequence.** A retried click after a
  dropped connection is dropped by the node, never applied twice. A handle
  addressed by id — every CLI invocation is one — reads the node's sequence
  before its first action instead of restarting it, so nothing is silently
  swallowed ([ADR 0094](adr/0094-a-resumed-computer-handle-reads-the-input-sequence.md)).
  `computer get --json` reports it as `last_iseq`.
- **One computer per DevTools port.** A second `create` on the same workspace
  needs `--port`; without it the second browser cannot bind and is reported as
  `browser_crashed`.
- **Downloads become artifacts.** A finished file is published to the tenant's
  artifact store and announced as a `computer.download` event; the id is in
  `computer downloads`. A file that finished but could not be published is
  listed with state `blocked` and a reason rather than vanishing.
- **Failures are typed.** A dead browser is `closed`/`browser_crashed` on every
  later operation and in `computer get`, never a hang; a refused navigation is
  `denied`/`navigation_denied`; a backend with no workspace port path is
  `unsupported`/`backend_unsupported`.
- **Egress policy is per host, never per URL.** The broker tunnels CONNECT
  opaquely and terminates no TLS, so it can see the host a browser asked for
  and nothing else. Read no per-path guarantee into this.
- **A browser reaches the hosts the broker allows, and only those.** Chromium
  routes through the workspace's broker but discards the capability in the proxy
  URL and never volunteers `Proxy-Authorization`; it waits to be challenged, and
  a headless browser has nobody to ask. The node answers that challenge itself
  over CDP, with the same capability the browser's own `HTTPS_PROXY` carries, so
  an allowed host loads normally and an unbound one is refused by host policy as
  `denied`/`navigation_denied`
  ([ADR 0095](adr/0095-browser-proxy-auth-through-cdp.md)). Two consequences are
  worth knowing before you read an audit trail. A site that asks for a password
  itself gets `CancelAuth` — the workspace capability is never handed to an
  origin — so an HTTP-authenticated page fails rather than logging in. And the
  first `CONNECT` of a proxy connection is recorded as an `unauthenticated`
  `egress.denied`: that is the `407` challenge, and the retry that carries the
  credential is the `allowed` record beside it. Chromium's own component and
  metrics traffic is issued outside any page and cannot be authenticated at all,
  so it stays denied.
- **Profiles do not survive a move, a sleep or a `remount pull`.** A profile
  lives in `.remount/browser/<profile>`, which every snapshot excludes on
  purpose: a live browser profile contains singleton symlinks and lock files
  that artifact validation correctly refuses, and restoring one host's profile
  onto another is not meaningful. A computer after a wake is a fresh browser on
  the same filesystem. Durable state belongs in ordinary files, which snapshots
  do carry.
- **Backends.** Any backend that forwards a workspace port: `process`, `docker`
  and `gvisor`. `firecracker` can drive a browser but cannot yet publish what
  it downloaded, which is reported as a `blocked` download with
  `backend_unsupported`.

The conformance lane behind these claims is
[`scripts/browser-conformance.sh`](../scripts/browser-conformance.sh); its
recorded run is in
[verification-2026-09.md](engineering/verification-2026-09.md).

### A full desktop, assembled by hand

A computer session is a browser, not a desktop. When you need X11, a window
manager or a VNC client, Remount hosts those processes and you compose them.

Remount does not ship a browser, display server, window manager, VNC server, or
mobile client. It can host those processes when the selected node image already
contains them. A validated Linux composition is Xvfb, Openbox, Chromium, and
x11vnc, with xdotool available for direct XTEST input.

Install desktop packages in the node image rather than assuming a locked-down
workspace can run the operating-system package manager. Keep ephemeral browser
state out of snapshots:

```sh
WS=$(./remount ws create --name desktop \
  --exclude .chromium-profile \
  --exclude node_modules)
./remount exec "$WS" -- ./desktop-run.sh
```

`desktop-run.sh` should keep its supervisor in the foreground. Ctrl-C detaches
the initiating client while the Remount session and desktop processes continue.
A fresh client can replay the supervisor output:

```sh
./remount attach "$WS" SESSION_ID --from 0
```

If x11vnc listens only on workspace loopback, expose it to the current client
with a loopback-only Remount tunnel:

```sh
./remount port "$WS" 5900 --local 127.0.0.1:5900
```

That tunnel exists only while the client command runs. Phone access after a
laptop shuts down therefore requires a separate authenticated web or mobile
client that opens the Remount port/session; Remount is the durable workspace
and transport substrate, not that finished UI.

Do not expose `x11vnc -nopw` beyond workspace loopback. VNC without an
authenticated encrypted outer transport is not safe for production. A process
or Docker backend also does not become hardened multi-tenant isolation merely
because the desktop is remote.

Browser profile directories commonly contain host- or process-specific
singleton symlinks. Snapshot and `pull` correctly reject unsafe symlinks rather
than exporting them. Put the profile under an exclusion chosen when the
workspace is created, or stop the browser and remove disposable profile state
before snapshotting. Do not weaken symlink validation.

The exact process layout, persistence boundaries, broker rules, and verification
checklist are in
[`harness-integration.md`](harness-integration.md#browser-and-virtual-desktop-workloads).

## Broker provider credentials

Provider keys belong in the control plane, never in a workspace. Define a
**binding** — the credential plus the hosts, methods and paths it may be spent
on — and the workspace receives only an opaque placeholder. The node's broker
substitutes the real value at the network edge for an authorized destination,
records `cred.used`, and blocks that placeholder anywhere else.

Create one from the shell. The secret is read from a named environment
variable of the command, never from an argument:

```sh
export OPENAI_API_KEY=...
remount binding preset apply openai --secret-env OPENAI_API_KEY
```

`binding preset apply` uses the provider shapes `remount binding preset ls`
lists, so the hosts, the key variable and the broker base URL come from one
place. Anything else is a full `create`:

```sh
remount binding create b_search \
  --destination api.search.example \
  --secret-env SEARCH_API_KEY \
  --substitution query:key \
  --ttl 15m --method GET --path-prefix /v1/
```

Attach it to a workspace, which sees the placeholder and the broker's URL:

```sh
remount ws create --name agent \
  --binding b_openai \
  --env OPENAI_API_KEY=ref:b_openai \
  --env 'OPENAI_BASE_URL=${REMOUNT_BROKER}/d/api.openai.com/v1'
```

`remount run RECIPE --binding b_openai` does the same thing for a harness.
Inside, the variable holds the placeholder and the call still works.
Substitution happens on the broker's reverse-proxy path (`/d/<host>/…`); a
`CONNECT` tunnel is opaque and stays host-granular, so point the harness's base
URL at the broker rather than relying on the proxy variables.

The same workflow is a method in every SDK — `CreateBinding` in Go,
`create_binding` in Python, `createBinding` in TypeScript — with
`list`, `get`, `rotate`, `revoke`, `create_session_principal` and
`credential_events` alongside it.

Rotate and revoke without a restart. Both reach a running workspace within one
renew interval:

```sh
remount binding rotate b_openai --secret-env OPENAI_API_KEY   # new key, same id
remount binding revoke b_openai --reason "credential leaked"  # stop substituting
remount binding ls --include-revoked
```

**Broker revocation is not provider-side revocation.** `binding revoke` stops
Remount substituting the credential; the key stays valid at the provider until
you rotate or delete it there. Do both.

Hand one task a scoped, self-expiring authority instead of a shared token:

```sh
TOKEN=$(remount principal session --ws "$WS" --roles agent --ttl 15m)
```

The bearer is printed once. It stops verifying when the workspace moves, when
the TTL passes, or when the principal is revoked.

Every broker refusal carries an `X-Remount-Reason` header and a JSON body
`{"error":{"code","reason","binding","host","message"}}`. Match on `code` and
`reason` — `egress_denied`, `approval_required`, `quota_exceeded`,
`binding_missing`, `grant_expired`, `revoked` — never on the message. Audit in
one call:

```sh
remount events --binding b_openai --json
remount events --host api.openai.com --type egress --follow
```

Binding lifecycle events stream under the binding id rather than a workspace,
so read them without `--ws`.

[`credentials.md`](credentials.md) is the detailed guide: substitution
locations, method and path scope, cookie bindings, retention metadata, what
each kind of revocation stops, and three runnable examples
([brokered-model-call](../examples/brokered-model-call),
[brokered-search-api](../examples/brokered-search-api),
[brokered-custom-http](../examples/brokered-custom-http)) that CI executes
against a fake provider with no network and no key.

A bindings **file** (`--bindings ./bindings.json`) is still accepted, as a
bootstrap for a first start:

```json
[
  {
    "id": "b_openai",
    "secret": "$OPENAI_API_KEY",
    "destinations": ["api.openai.com"],
    "ttl_sec": 900
  }
]
```

Its entries are seeded into the durable store on the first start that does not
already have them. **After that the store wins**: editing the file changes
nothing, because a rotation or a revocation must not be undone by restarting
with the original file. Use `binding rotate` and `binding revoke`.

Hosts a workspace reaches *without* a credential still need `--allow`:

```sh
./remount standalone --data ./data \
  --allow registry.npmjs.org \
  --allow models.dev --allow models.opencode.ai --allow opencode.ai
```

Those are the OpenCode recipe's install and model-catalog destinations;
OpenCode's default model selection reaches `opencode.ai`, and naming an
explicit model does not remove the need for the other checked-in recipe hosts.
Automatic local standalone derives these allowances from the recipe, but an
explicitly started server must receive them through `--allow`. A host covered
by a binding needs no `--allow` entry.

The process backend provides no host isolation. The built-in Docker backend is
container isolation with cooperative proxy egress. Neither may be described
as production multi-tenant isolation; Remount rejects security profiles a
backend cannot enforce. See [`security-profiles.md`](security-profiles.md) and
[`operations.md`](operations.md#choosing-a-backend).

Linux gVisor and Firecracker backends implement enforced-gateway networking.
They require host prerequisites and exact-host conformance; merely selecting
the backend is not a production qualification. gVisor can satisfy `isolated`;
`multi_tenant` additionally requires the Firecracker microVM capability.

## Deploy the Modal reference

`deploy/modal_app.py` is a reference deployment of
one control plane and one process-backend node in one Modal container. It is
not the same thing as the declarative Modal node-pool provisioner. Read the
checked-in [`deployment source`](../deploy/modal_app.py) before changing its
resource or secret names.

Prerequisites:

- Modal CLI authenticated to the intended workspace.
- The Modal CLI environment includes `python-dotenv`, which
  `modal secret create --from-dotenv` requires.
- `MODAL_ENVIRONMENT` set explicitly.
- A named Modal secret containing `REMOUNT_TOKEN`.
- Optionally, named secrets containing `OPENAI_API_KEY` or
  `ANTHROPIC_API_KEY` for model calls through `b_openai` or `b_anthropic`.

Use unique names for disposable validation:

```sh
export MODAL_ENVIRONMENT=dev
export REMOUNT_MODAL_APP=remount-example
export REMOUNT_MODAL_VOLUME=remount-example-data
export REMOUNT_MODAL_SECRET=remount-example-control
export REMOUNT_MODAL_MODEL_SECRET=remount-example-openai
export REMOUNT_MODAL_ANTHROPIC_SECRET=remount-example-anthropic
```

Create only the provider secrets the deployment needs, without placing values
in process arguments. The files below must be mode 0600 and deleted
immediately after import:

```sh
umask 077
CONTROL_ENV=$(mktemp)
printf 'REMOUNT_TOKEN=%s\n' "$REMOUNT_TOKEN" > "$CONTROL_ENV"
modal secret create "$REMOUNT_MODAL_SECRET" --from-dotenv "$CONTROL_ENV"
rm -f "$CONTROL_ENV"

MODEL_ENV=$(mktemp)
printf 'OPENAI_API_KEY=%s\n' "$OPENAI_API_KEY" > "$MODEL_ENV"
modal secret create "$REMOUNT_MODAL_MODEL_SECRET" --from-dotenv "$MODEL_ENV"
rm -f "$MODEL_ENV"

ANTHROPIC_ENV=$(mktemp)
printf 'ANTHROPIC_API_KEY=%s\n' "$ANTHROPIC_API_KEY" > "$ANTHROPIC_ENV"
modal secret create "$REMOUNT_MODAL_ANTHROPIC_SECRET" --from-dotenv "$ANTHROPIC_ENV"
rm -f "$ANTHROPIC_ENV"
```

Deploy and run the smoke test:

```sh
make modal-deploy
make modal-smoke
```

`modal-smoke` resolves the named deployed function, checks `/healthz`, and
requires an enrolled online node. Set `REMOUNT_SERVER` to the returned URL,
keep `REMOUNT_TOKEN` in the client environment, and use the normal CLI:

```sh
./remount nodes
WS=$(./remount ws create --name modal-demo)
./remount exec "$WS" -- sh -c 'printf "remote\n" > result.txt'
./remount fs read "$WS" result.txt
./remount ws destroy "$WS"
```

Before redeploying this pinned singleton, stop the existing app. A rolling
replacement has no spare container slot:

```sh
modal app stop "$REMOUNT_MODAL_APP" -y
make modal-deploy
```

For disposable resources, destroy Agents and workspaces first, then remove
only resources created for that run:

```sh
modal app stop "$REMOUNT_MODAL_APP" -y
modal volume delete "$REMOUNT_MODAL_VOLUME" -y --allow-missing
modal secret delete "$REMOUNT_MODAL_SECRET" -y --allow-missing
modal secret delete "$REMOUNT_MODAL_MODEL_SECRET" -y --allow-missing
modal secret delete "$REMOUNT_MODAL_ANTHROPIC_SECRET" -y --allow-missing
```

Do not delete a shared model secret. Modal singleton, persistence, readiness,
and backup constraints are detailed in
[`operations.md`](operations.md#deploying-the-control-plane-on-serverless-platforms).

## Choose a runtime profile

A runtime profile is a machine's isolation claim, enforced as a fail-closed
startup gate on the node and as a scheduling constraint on the workspace, and
answered three-valued by `doctor --profile` and `conformance --profile`.

### Three unrelated things are called a profile

The word is overloaded across three surfaces that never interact. Read this once
and the rest of the section is unambiguous:

| Where you see it | What it names | Values |
|---|---|---|
| `remount up --profile P`, `ws create --requires-profile P`, `doctor --profile P`, `conformance --profile P` | the **runtime profile**: what a whole node currently proves about its isolation, and what a workspace demands of the node that runs it | `dev`, `trusted-single-tenant`, `multi-tenant-isolated`, `microvm` |
| `ws create --security P`, `run --security P`, the workspace's `security.profile` | the **workspace security profile**: the isolation one workspace asks its backend for, refused if the backend cannot enforce it | `local`, `isolated`, `multi_tenant` |
| `computer create --profile NAME` | the **browser profile**: a named on-disk Chromium profile directory inside the workspace | any name; `default` if unset |

The first two look alike and are not. A runtime profile is a property of a
machine and is evaluated over **every** backend that node registered; a workspace
security profile is a property of one workspace and is checked against the
backend that would run it. A node can satisfy `multi-tenant-isolated` while a
workspace on it asks only for `local`, and a workspace can ask for `isolated`
without ever naming a runtime profile. The third shares nothing but the word.

### The four runtime profiles

A runtime profile is what a **machine** claims, as distinct from
`security.profile`, which is what one **workspace** requires. There are four,
weakest first:

| `--profile` | Guarantees | Refuses |
|---|---|---|
| `dev` | nothing. It is the default and makes no security claim | nothing; a node with no backend at all still fails to start |
| `trusted-single-tenant` | every registered backend isolates the workspace from the host and authenticates to the broker | a node registering `process` |
| `multi-tenant-isolated` | mutually untrusted workloads: deny-first enforced egress, sibling isolation, a private network namespace, container-or-microVM isolation, and passing host checks | a node registering `process` or `docker` at all |
| `microvm` | `multi-tenant-isolated` restricted to verified microVM backends, plus a live host compatibility result | anything that is not a microVM |

Every predicate holds over **every** backend a node registered, so a node
serving `gvisor` and `process` together is a `dev` node. A strong backend
never launders a weak one sharing the machine.

Claim one when you start a node, and it is a startup gate rather than a label:

```sh
./remount up --profile multi-tenant-isolated   # or REMOUNT_PROFILE
```

Require one when you create a workspace, through `requires.profile` on the
workspace spec. The control plane schedules against the backend capabilities a
node actually advertised, never against operator labels:

```sh
./remount ws create --requires-profile multi-tenant-isolated
```

```go
ws, err := c.CreateWorkspace(ctx, proto.WorkspaceSpec{
    Requires: proto.Requires{Profile: "multi-tenant-isolated"},
})
```

`--requires-profile` is validated locally before the client dials, so a
misspelled profile is a named error rather than a workspace that parks forever.
Omitting the flag states no requirement at all, which is not the same as asking
for `dev`: no requirement matches every node, while `dev` is still a constraint
the control plane evaluates.

A workspace nobody can place stays `pending` with
`pending_reason: profile_unschedulable` instead of landing somewhere weaker.
A node that loses a prerequisite while serving becomes unschedulable within
one probe interval and emits `node.profile.unschedulable`; recovery emits
`node.profile.restored` and the parked workspaces are placed. See
[`operations.md`](operations.md#choosing-a-backend) for the drift behavior and
the per-backend table.

Two commands answer "is this fleet fit", and both are three-valued:

```sh
./remount doctor --profile multi-tenant-isolated --json   # per node, per check
./remount conformance --profile multi-tenant-isolated \
    --markdown conformance.md --report conformance.json
```

`doctor --profile` asks the control plane what it believes about each node.
`conformance --profile` additionally judges the protocol manifest black-box
and creates a workspace requiring the profile, so it proves scheduling
honours the constraint rather than merely reporting on it. It writes a review
document whose footer states how many checks were unavailable.

| Exit | Meaning |
|---|---|
| 0 | every check passed |
| 1 | at least one check failed |
| 2 | nothing failed, but something could not be checked |

There is no path on which an unavailable check exits 0. Treat exit 2 exactly
as you would treat a failure until you know which check could not run.

Be honest about where this can hold. `multi-tenant-isolated` and `microvm`
depend on Linux kernel mechanisms — `runsc`, network namespaces, nftables,
KVM — so only a Linux host can satisfy them. On macOS and Windows the
strongest honest answer is `dev`, and asking for more returns named failures
rather than a weaker pass.

## Provider-backed nodes are not credential checks

Fly, E2B, Modal, ix.dev, and SSH pool drivers create whole Remount nodes. A
valid provider credential alone does not prove that a driver can provision a
usable node. Each driver also needs its documented bootstrap image, template,
app, region, network, server URL, enrollment, and binary metadata.

Treat a lane whose prerequisites are absent as **unavailable**, not healthy.
The provider registry and pool lifecycle are covered in
[`operations.md`](operations.md#provider-backed-node-pools).

## Verify outcomes and clean up

Use the resource state and durable evidence, not a successful client call
alone:

```sh
./remount status
./remount inspect WORKSPACE_ID
./remount events --ws WORKSPACE_ID
./remount metrics
./remount doctor
```

For a coding-agent task, verify at least:

1. the Agent reached the expected terminal or input state;
2. requested files or `agent diff` contain the intended change;
3. relevant tests or commands completed with the expected exit code;
4. `cred.used`, approval, run, and lifecycle events match the operation;
5. disposable Agents, workspaces, provider resources, and temporary secret
   imports were removed.

`doctor` is three-valued: pass, fail, or unavailable. Unavailable checks are
not evidence of health. Read individual findings even when the command exits
zero or JSON says `ok: true`: warnings such as `tenant.residency_unavailable`
mean that named policy was not checked.

## Public API, errors and tracing

Remount's public surface is the Go `api` and `client` packages, the `remount`
Python package, the `@remount/sdk` npm package, the wire protocol, and the
documented CLI flags and `--json` shapes. What each version number promises,
and how a break is announced, is in
[`compatibility-policy.md`](compatibility-policy.md).

### Typed errors

Every failure carries a stable `code`. When one code covers outcomes you must
tell apart — a `denied` from egress policy is not a `denied` from a missing
role — it also carries a stable `reason`. Match on the code first and narrow on
the reason only when the distinction matters. Never match on the message: it is
for humans and may change in any release.

```go
if _, _, _, err := c.Run(ctx, ws, "curl", "https://api.example.com"); err != nil {
    if api.Is(err, api.CodeDenied, api.ReasonEgressDenied) {
        // bind a credential for that host, or add an egress rule
    }
}
```

```python
try:
    await client.exec(workspace, ["curl", "https://api.example.com"])
except remount.errors.EgressDenied as denied:
    print(denied.code, denied.reason)
```

```ts
try {
  await client.exec(workspace, ["curl", "https://api.example.com"]);
} catch (error) {
  if (error instanceof EgressDeniedError) { /* ... */ }
}
```

Python raises one exception class per reason, all subclassing `ProtocolError`;
TypeScript exports one error class per reason, all extending `ProtocolError`.
A reason a client has never heard of — a server newer than the package —
degrades to the base class rather than to an unhandled case, so an old client
never crashes on a new vocabulary. The vocabulary itself is generated from the
protocol into `remount.types.REASONS` and `Reason` in `@remount/sdk`:

| Reason | Refines | Means |
|---|---|---|
| `permission_denied` | `denied` | the principal lacks a role or tenant scope |
| `egress_denied` | `denied` | the broker refused a destination or placeholder |
| `approval_required` | `denied` | approve-mode egress awaits a durable approval |
| `binding_missing` | `not_found`, `denied` | no binding covers the placeholder |
| `grant_expired` | `unauthorized` | the grant or capability TTL passed |
| `revoked` | `unauthorized` | the principal, binding or grant was revoked |
| `quota_exceeded` | `resource_exhausted` | a tenant quota or hard budget is spent |
| `workspace_not_ready` | `conflict` | the workspace has not reached `ws.ready` |
| `workspace_moved` | `conflict` | the workspace now lives on another node |
| `generation_mismatch` | `conflict`, `unauthorized` | the request names a stale generation |
| `backend_unsupported` | `unsupported` | the backend cannot perform this |
| `output_evicted` | `evicted` | the requested session range is no longer retained |
| `lifecycle_deadline_expired` | `conflict` | a lease or idle deadline already fired |
| `browser_crashed` | `closed` | the computer session's browser exited |
| `display_unavailable` | `unsupported` | no display or CDP endpoint could be reached |
| `input_rejected` | `bad_request` | a computer input action was malformed |
| `navigation_denied` | `denied` | egress policy blocked a browser navigation |
| `profile_corrupt` | `conflict` | a persisted browser profile could not be opened |
| `download_blocked` | `denied` | policy refused a browser download |
| `profile_unschedulable` | `denied`, `unsupported` | no node satisfies the runtime profile |

### Support matrix

Which operating system can host a client and which can host a node, which
backends run where, and which features and runtime profiles each backend
supports, are generated from the backends' own advertised capabilities into
[`security-profiles.md`](security-profiles.md). A documented `pass` there means
the capabilities satisfy the profile; `unavailable` means a host check has to
run first, and is never a pass. `remount doctor --profile PROFILE` and
`remount conformance --profile PROFILE` settle it against a live deployment.

### Traces to your own collector

Spans go to a collector you run, or nowhere:

```sh
remount standalone --otlp-endpoint http://collector.internal:4318
```

`REMOUNT_OTLP_ENDPOINT` sets the same thing, and `server` and `up` take the
same flag. There is no Remount-operated collector and no default endpoint, so
with the flag unset nothing is recorded and nothing leaves the process. The
exporter posts OTLP/HTTP JSON, which the OpenTelemetry Collector's `otlp`
receiver accepts on port 4318. One span per request, named by its `op`, with a
node span appearing as a child of the control span that caused it; attributes
are identifiers only, and every one is scrubbed for credential shapes before it
is buffered. [`observability.md`](observability.md#traces) has the attribute and
counter tables.

## Documentation map and maintenance

This guide owns the supported **user journeys**. Everything else that exists is
listed below, so there is one place to look rather than a partial list per
document.

**Start here**

- [`README.md`](../README.md): what Remount is, the zero-credential quick start,
  and the honest status of every claim.
- This file: the maintained user and coding-agent entry point.
- [`tutorial.md`](tutorial.md): exact first-use and workspace walkthrough.

**Using a deployment**

- [`harness-integration.md`](harness-integration.md): recipes, ACP/PTY behavior,
  provider bindings, handoff, queues, custom harnesses, and virtual desktops.
- [`api.md`](api.md): the Agent HTTP API — agents, transcript streaming,
  approvals, diff, terminal, files, previews.
- [`console.md`](console.md): the embedded operator console at `/console/`.
- [`mcp.md`](mcp.md): exposing Remount and wrapped servers over MCP.
- [`images.md`](images.md): the default workspace image, the reference browser
  image, and what any substitute image must provide.

- [`credentials.md`](credentials.md): brokered credentials end to end -
  bindings, substitution locations, rotation, revocation, session principals,
  the refusal contract, and audit.

**Running it for real**

- [`operations.md`](operations.md): production deployment and fleet operation.
- [`security-profiles.md`](security-profiles.md): the generated support matrix —
  operating systems, per-backend features, and the runtime profile each backend
  can satisfy.
- [`observability.md`](observability.md): inspection at three depths, traces,
  metrics, and detecting damage.
- [`benchmarks.md`](benchmarks.md): dated measurements, never promoted into
  guarantees.

**Contracts and change management**

- [`spec/PROTOCOL.md`](../spec/PROTOCOL.md): normative protocol.
- [`compatibility-policy.md`](compatibility-policy.md): what is public, what a
  version number promises for each surface, and how a break is announced.
- [`releases.md`](releases.md): the release and installation workflow, and its
  current disposition.
- [`CHANGELOG.md`](../CHANGELOG.md): user-visible changes.
- [`design.md`](design.md): the architecture in full — data model, failure
  model, threat model, performance budget, and current limitations.
- [`adr/`](adr/README.md): one file per decision, indexed by number in
  [`adr/README.md`](adr/README.md). New decisions get a new ADR; an existing one
  is never rewritten.

**Evidence, not claims**

- [`engineering/current-status.md`](engineering/current-status.md): the
  maintained status index — what is implemented, what was verified when.
- [`engineering/`](engineering/): dated audits, implementation requests,
  dispositions, the monthly verification ledger, and
  [`hardening-lessons.md`](engineering/hardening-lessons.md), the review method
  distilled from them. A dated document describes its own candidate.
- [`MISTAKES.md`](../MISTAKES.md): every bug hit while building this, and what
  each one taught.

Command, recipe, provider, deployment, security, filesystem-transfer, or
lifecycle changes are incomplete until the corresponding user journey here
and its detailed document are updated. Run `make docs` after documentation
changes; `make lint` verifies that `llms.txt` and `llms-full.txt`, the
machine-readable documentation bundle used by coding agents, are current.
