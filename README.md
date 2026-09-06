# Remount

**Run [Claude Code](https://github.com/anthropics/claude-code),
[Codex](https://github.com/openai/codex), [OpenCode](https://github.com/sst/opencode),
[Pi](https://github.com/earendil-works/pi), [Gemini CLI](https://github.com/google-gemini/gemini-cli),
[Aider](https://github.com/Aider-AI/aider), [Goose](https://github.com/block/goose),
[Cline](https://github.com/cline/cline), [OpenHands](https://github.com/All-Hands-AI/OpenHands)
or your own agent loop as long-running background agents on machines you
control.**

Start a task in Claude Code on your laptop. Hand the live conversation to a
box in the cloud and close the lid. The agent keeps working. Tomorrow, from a
different machine, open the same conversation where it left off. Let it sleep
at 2 a.m. and wake when the pull request is merged. Give it API keys it never
sees. Fork it into three children for three subtasks. Move it to a GPU box
mid-task. Read every step it took in an event log.

```sh
# one command from a checkout to a running agent
remount run claude --dir . -- 'add integration tests for the payments module'

# hand a live Claude Code conversation from this laptop to the cloud, then leave
remount handoff --recipe claude --binding b_anthropic --task 'finish the refactor and open a PR'
remount resume $WS                       # tomorrow, from anywhere

# overnight: a queue of tasks, sleeping between them on a durable timer
remount run codex --queue tasks.txt --sleep-after 30m

# a durable agent that pauses when idle, asks before risky egress, forks helpers
remount agent create opencode --dir . --sleep-after 10m --approve on-request -- 'migrate to pnpm'
remount agent fork $AGENT -- 'update the docs to match'
remount ws sleep $WS --on github.pr.merged
```

This is not a sandbox behind someone else's API. E2B and Modal give you a
container in their cloud for the length of a request. Remount is one Go binary
and one open protocol that you run yourself, on a laptop, a VM, a GPU box,
Kubernetes, Modal or Fly, and it treats the agent's computer as something that
lasts: output is a replayable log, the workspace snapshots and moves between
machines, deadlines live in a control plane that outlives your process, the
workspace never holds a credential, and a node has to prove its isolation
before it gets untrusted work. Apache-2.0, no hosted service, nothing phones
home.

## What you can do with it

- **Hand off and resume.** `remount handoff` copies your checkout and the
  harness's saved conversation into a workspace and starts the harness there
  against a brokered key. `remount resume` reopens that exact conversation
  from any machine; Claude Code and Codex resume by transcript id, never by a
  "last conversation" guess.
- **Run agents overnight.** A task queue runs in one workspace, checkpoints
  between tasks, sleeps on a control-plane timer and continues on whichever
  node wakes it. A durable Agent sleeps when idle, wakes on a message or an
  event, forks children, and parks for approval before an egress you marked
  as sensitive. Transcripts stream; `agent diff` shows what changed.
- **Reattach and never lose output.** Close the laptop mid-command, come
  back, `attach`. The session lives on the node; what retention dropped is an
  explicit gap, never silence.
- **Move a workspace mid-task.** `ws move` snapshots the filesystem and a
  node in the zone you asked for restores it. Files and policy follow.
- **Hand out keys the agent never sees.** The workspace holds a placeholder;
  the broker substitutes only for allowed hosts, records every use, and a
  revocation reaches a live workspace within one renewal.
- **Drive a browser inside the workspace.** Navigate, screenshot, click,
  type, download files that become artifacts, under the same egress policy.
- **Run other people's agents.** A node declares a runtime profile and
  refuses to start unless its backends can prove it; a gVisor node has passed
  `multi-tenant-isolated` live, and `conformance --profile` gives a security
  reviewer a report where every check is pass, fail or unavailable.
- **Use it from Go, Python or TypeScript**, or the CLI, with the same typed
  errors in all three.

## How it differs from a sandbox API

- **A session is a log, not a socket.** Output is kept with sequence numbers;
  clients replay from where they were.
- **A workspace is a value.** Snapshot, move, sleep and wake, with every grant
  bound to a generation so a stale client cannot act on a moved workspace.
- **The workspace is trusted with nothing.** Secrets, policy and lifecycle
  live in the node and the control plane; the workspace sees placeholders.
- **Lifecycle belongs to the control plane.** Sleep, wake, leases and idle
  policy are durable timers, not a `setTimeout` in your worker.
- **Isolation is a checked promise.** A check that cannot run is reported
  unavailable, never healthy.
- **Everything is an event**, committed in the same transaction as the state
  it describes.
- **Nothing is hosted.** The protocol is public and the reference
  implementation is meant to be replaced if you can do better.

If you want to use it, start with [docs/using-remount.md](docs/using-remount.md)
and [docs/harness-integration.md](docs/harness-integration.md). If you want to
know how it works, read [docs/design.md](docs/design.md) and
[spec/PROTOCOL.md](spec/PROTOCOL.md).

## How it fits together

```
   your laptop                  control plane                 a GPU box
  ┌────────────┐   outbound    ┌──────────────┐   outbound   ┌────────────┐
  │  remount   │──────────────▶│  claim queue │◀─────────────│  remount   │
  │    up      │   wss:443     │  event log   │   wss:443    │    up      │
  └────────────┘               │  policy      │              └────────────┘
                               │  bindings    │
  ┌────────────┐               └──────────────┘
  │  remount   │──────────────────────▲
  │  exec/sh   │   frames relayed, never interpreted
  └────────────┘
```

Every machine that runs workspaces is a node. Nodes and clients both dial the
control plane, so nothing needs a public inbound port. The control plane owns
the queue of workspaces waiting for a node, the event log, policy and
credential bindings. Session traffic passes through it as opaque frames.

## Quick start

You need the Go version in `go.mod` (1.27.1 at the time of writing). Nothing
below needs an account, a token or an API key.

```sh
make build

# Control plane and one node, in one process.
./remount standalone --data ./data &
export REMOUNT_SERVER=http://127.0.0.1:7443

WS=$(./remount ws create --name hello)
./remount exec $WS -- sh -c 'echo hello from $(hostname)'
./remount sh $WS                       # interactive shell with a real pty
./remount fs write $WS notes.md < README.md
./remount fs search $WS 'claim queue'
```

Start something long, interrupt the client, and reattach:

```sh
./remount exec $WS -- sh -c 'for i in $(seq 1 100); do echo line $i; sleep 1; done'
# ^C, then:
./remount ws ls
./remount attach $WS <session-id> --from 0
```

The process kept running on the node the whole time.

### With an API key

`remount run` starts a coding harness in a fresh workspace. If nothing is
listening on the default address it starts `remount standalone` for you, turns
the provider keys in your environment into bindings, and hands the workspace a
placeholder for the one the recipe uses.

```sh
export OPENAI_API_KEY=sk-...
./remount run opencode --dir . --model openai/gpt-4o-mini -- 'add a README'
```

The key stays in the standalone process. `remount events --ws WS` shows each
`cred.used`. Set `REMOUNT_AUTOSTART=0` or `REMOUNT_SERVER` to stop the
automatic start.

## Move a workspace

```sh
# On the second machine (REMOUNT_TOKEN exported in the shell, not on argv):
./remount up --server https://your-server --label zone=gpu

# From anywhere:
./remount ws move $WS --label zone=gpu
```

The filesystem is snapshotted, the workspace is re-queued, a node in that zone
picks it up, and your files are there. Plain exec and pty processes are not
restarted; use the durable Agent lifecycle or `remount resume` to continue a
supported harness from its saved state. Files are portable between compatible
backends. Installed tools, running processes and architecture-specific
binaries are not.

A plain snapshot is a live capture, useful for inspection or export. Pass
`--authoritative` when the snapshot has to be the failover checkpoint: that
path quiesces execution, uploads the artifact and commits the reference
before returning.

```sh
./remount ws snapshot $WS                 # live capture
sleep 1                                   # at most one snapshot per second
./remount ws snapshot $WS --authoritative # quiesced and committed
```

## Sleep and wake

```sh
./remount ws sleep $WS --after 72h
./remount ws sleep $WS --on github.pr.merged     # wake on an event instead
```

A sleeping workspace has no node. Its artifacts still take storage, and a
provisioned node keeps costing money until you scale it down. Wake it with
`remount ws wake $WS`, or on an authenticated server with a webhook:

```sh
curl -X POST $REMOUNT_SERVER/v1/events -H "Authorization: Bearer $REMOUNT_TOKEN" \
     -d '{"type":"github.pr.merged","payload":{"pr":42}}'
```

## Keep a workspace awake for background work

`ws sleep` means sleep now. A lease is the opposite: keep the workspace
claimed while something is still running, and sleep it when the deadline
passes unless someone extends it. The deadline is stored in the control plane
because your own process is the thing most likely to die first.

```sh
LEASE=$(./remount ws lease $WS --max 20m --min 30s --reason background_job --json | jq -r .id)
./remount ws lease renew $WS $LEASE --extend 10m
./remount ws idle-policy $WS --sleep-after 10m --destroy-after 2h
./remount ws mark-active $WS --reason new_turn
```

Session traffic does not extend a deadline; you mark activity explicitly.
When the deadline passes the node sends `SIGTERM`, waits a short grace
period, then `SIGKILL`. The session log ends with
`exit.reason = "lifecycle_deadline_expired"` so a later reader can tell that
policy ended the work, and `ws get` shows the pending deadline. Details in
[the user guide](docs/using-remount.md#hold-a-workspace-for-background-work)
and [ADR 0090](docs/adr/0090-durable-workspace-leases-and-idle-policy.md).

## Credentials

Define a binding once, on the server:

```json
[{"id": "b_openai",
  "secret": "$OPENAI_API_KEY",
  "destinations": ["api.openai.com"],
  "placeholder": "sk-proj-PLACEHOLDER",
  "ttl_sec": 900}]
```

Give a workspace a reference to it:

```sh
WS=$(./remount ws create --binding b_openai \
      --env OPENAI_API_KEY=ref:b_openai \
      --env OPENAI_BASE_URL='${REMOUNT_BROKER}/d/api.openai.com/v1')
```

Inside the workspace the variable holds the placeholder. The request still
works because the broker substitutes the real key on the way out:

```sh
$ ./remount exec $WS -- sh -c 'echo $OPENAI_API_KEY'
sk-proj-PLACEHOLDER

$ ./remount exec $WS -- sh -c 'curl -s $OPENAI_BASE_URL/chat/completions \
      -H "Authorization: Bearer $OPENAI_API_KEY" -d @req.json'
{"choices":[{"message":{"content":"..."}}]}
```

Send the same placeholder to any other host and the broker refuses before
anything leaves the node:

```
$ ./remount exec $WS -- sh -c 'curl -s -o /dev/null -w "%{http_code}" \
      $REMOUNT_BROKER/d/api.anthropic.com/v1/messages -H "Authorization: Bearer $OPENAI_API_KEY"'
403

$ ./remount events --ws $WS | grep egress
egress.denied  {"decision":"leak_blocked","host":"api.anthropic.com",
                "reason":"placeholder for b_openai sent to api.anthropic.com"}
```

Bindings can be created, rotated and revoked at runtime with
`remount binding`, and a key can be substituted into a header, a query
parameter or a JSON body field. This removes static keys from the workspace;
it does not make an allowed API harmless, since a compromised workspace can
still use what it was granted. Harness logins (the kind a coding agent stores
in its own config) are a different thing and stay inside the workspace; see
[docs/credentials.md](docs/credentials.md) and
[the two kinds of auth](docs/harness-integration.md#two-kinds-of-auth-stated-plainly).

## Isolation

A runtime profile is a promise a node makes at startup, and the node refuses
to start if it cannot keep it. `dev` promises nothing. `trusted-single-tenant`
requires every backend to isolate the workspace from the host and to
authenticate to the broker. `multi-tenant-isolated` adds deny-first egress
enforcement, isolation between sibling workspaces and a private network
namespace, and refuses a node that registers `process` or `docker` at all.
`microvm` requires a verified microVM backend and a live host compatibility
check.

```sh
./remount up --profile multi-tenant-isolated
./remount ws create --requires-profile multi-tenant-isolated
./remount doctor --profile multi-tenant-isolated --json
./remount conformance --profile multi-tenant-isolated --markdown conformance.md
```

The control plane decides from the capabilities a backend advertised, not
from labels an operator typed, so a node serving `gvisor` and `process`
together is a `dev` node. A workspace nobody can place stays `pending` with
`pending_reason: profile_unschedulable` instead of landing on a weaker node.
A node that loses a prerequisite while serving stops receiving profile work
within one probe interval and gets it back when the prerequisite returns.
`doctor` and `conformance` exit 0 when everything passed, 1 when something
failed, and 2 when nothing failed but something could not be checked.

Only Linux can satisfy `multi-tenant-isolated` and `microvm`. A gVisor node
passed `multi-tenant-isolated` in a Linux VM on 2026-09-05: nine `doctor`
checks, 78 conformance checks with 64 passed, 0 failed and 14 unavailable,
and live drift and recovery. Firecracker has no equivalent run on this
branch yet. See [choosing a runtime profile](docs/using-remount.md#choose-a-runtime-profile),
[the support matrix](docs/security-profiles.md) and
[ADR 0089](docs/adr/0089-runtime-profiles-and-drift.md).

## Browser

A computer is a browser the node drives inside a workspace. The node talks
DevTools protocol over the same path `remount port` uses, so any backend that
can forward a workspace port can host one, and nothing extra runs inside the
workspace.

```sh
docker build -t remount-browser:local images/browser
WS=$(./remount ws create --backend docker --image remount-browser:local --json | jq -r .id)
CMP=$(./remount computer create $WS --json | jq -r .computer)

./remount computer navigate $WS $CMP https://example.com/
./remount computer screenshot $WS $CMP --out shot.png
./remount computer click $WS $CMP 640 360
./remount computer type $WS $CMP 'search text'
./remount computer downloads $WS $CMP --json
```

Coordinates are CSS pixels in the viewport you declared at `create`. Each
batch of input carries a sequence number, so a retried click is dropped
rather than applied twice. Downloads become artifacts. Failures are typed:
`closed` with `browser_crashed`, `denied` with `navigation_denied`. Browser
traffic goes through the broker like any other egress, and the node answers
the broker's proxy challenge on the browser's behalf. The same operations
exist in all three SDKs. The browser profile lives in a directory that is
excluded from snapshots, so it does not survive a sleep or a move; that is
deliberate, because cookies should not travel in a snapshot. See
[computer sessions](docs/using-remount.md#computer-sessions-the-built-in-browser-api),
[ADR 0088](docs/adr/0088-computer-sessions-over-port-substrate.md) and
[ADR 0095](docs/adr/0095-browser-proxy-auth-through-cdp.md).

## Events

```sh
./remount events --follow
```

```
   1 13:24:18.402 ws.created      ws_06g6…  you  {"name":"hello"}
   4 13:24:18.551 ws.claiming     ws_06g6…             {"gen":1}
   6 13:24:18.702 ws.claimed      ws_06g6…             {"gen":1}
   9 13:24:31.461 cred.used       ws_06g6…  you  {"binding":"b_openai","host":"api.openai.com","status":200}
  12 13:24:59.942 egress.denied   ws_06g6…  you  {"decision":"leak_blocked","host":"api.anthropic.com"}
```

Every state change writes an event in the same transaction as the state.
SQLite rows are the source of truth for lifecycle; the append-only log is the
audit record. Filter with `--ws`, `--binding`, `--host` and `--type`.

## Commands

| Command | What it does |
|---|---|
| `remount server` | control plane, relay and artifact store |
| `remount up [--profile P]` | run this machine as a node; outbound only |
| `remount standalone` | server and node in one process, for trying it out |
| `remount ws create\|ls\|get\|move\|sleep\|wake\|snapshot\|destroy` | manage workspaces |
| `remount ws create --requires-profile P` | only place this workspace on a node that satisfies `P` |
| `remount ws lease\|idle-policy\|mark-idle\|mark-active` | leases and the idle rule; the deadline lives in the control plane |
| `remount exec WS -- cmd` | run a command, streamed |
| `remount sh WS` | interactive pty |
| `remount attach WS SESSION --from N` | reattach to a running session |
| `remount fs read\|write\|ls\|stat\|rm\|mv\|mkdir\|search\|edit` | workspace filesystem |
| `remount port WS PORT` | forward a port out of the workspace |
| `remount computer create\|get\|screenshot\|click\|type\|key\|scroll\|navigate\|eval\|downloads\|close` | drive a browser inside a workspace |
| `remount binding create\|ls\|get\|rotate\|revoke\|preset apply` | manage credential bindings at runtime |
| `remount principal session --ws WS` | mint a short-lived principal bound to one workspace |
| `remount run RECIPE [--dir . \| --repo URL[@REF]] -- TASK` | seed a workspace, install a coding harness, run it |
| `remount run RECIPE --queue FILE [--sleep-after D]` | run a file of tasks in order in one workspace |
| `remount handoff` / `remount resume WS` | move this checkout and the harness conversation into a workspace, and pick it up from anywhere |
| `remount nodes` / `events` / `timers` | inspect the fleet |
| `remount fleet quarantine ...` | fence, checkpoint, stop or destroy an incident scope |
| `remount status` / `inspect` / `metrics` | health and capacity |
| `remount doctor [--profile P] [--deep]` | check for damage, loss or disagreement; per-node profile checks |
| `remount conformance [--profile P] [--markdown FILE]` | judge a deployment against the protocol manifest and a runtime profile |

Ctrl-C in `exec`, `sh` or `attach` detaches and prints the `attach` command
that picks the process back up. Pass `--kill-on-interrupt` to send SIGINT to
the remote process instead. `exec --timeout D` is enforced by the node; the
default is no timeout.

## Go SDK

Import the public packages, not `internal/`:

```go
import (
    "remount.dev/remount/api"
    "remount.dev/remount/client"
)

c, err := client.New(client.Options{
    Server: "https://remount.example",
    Token:  os.Getenv("REMOUNT_TOKEN"),
})
ws, err := c.CreateWorkspace(ctx, api.WorkspaceSpec{Name: "agent"})
ws, err = c.WaitClaimed(ctx, ws.ID)
stdout, stderr, exit, err := c.Run(ctx, ws.ID, "sh", "-c", "make test")
```

Mutations accept `client.WithIdempotencyKey`. Errors carry a stable `code`
and, where one code covers several outcomes, a stable `reason`. Match on
those, never on the message:

```go
if _, _, _, err := c.Run(ctx, ws.ID, "curl", "https://api.example.com"); err != nil {
    if api.Is(err, api.CodeDenied, api.ReasonEgressDenied) {
        // bind a credential for that host, or add an egress rule
    }
}
```

Python raises one exception class per reason and TypeScript exports one
error class per reason, all deriving from `ProtocolError`. A reason an older
client has not heard of falls back to the base class. CI compiles the Go SDK
from a separate module so a dependency on an `internal` package fails the
build. See [typed errors](docs/using-remount.md#typed-errors) and
[the compatibility policy](docs/compatibility-policy.md).

Install with `go install remount.dev/remount/cmd/remount@main`, or
`curl -fsSL https://get.remount.dev/install.sh | sh` once a release is tagged.

## Observability

```sh
remount status            # the fleet, problems first
remount inspect $WS       # one workspace, down to session log positions on disk
remount doctor --deep     # re-hash every snapshot and report anything damaged
```

All three take `--json`. `REMOUNT=./remount scripts/collect.sh` gathers
everything into one document and `scripts/explain.py` turns it into a short
verdict. Prometheus metrics are at `/metrics`. Traces go to a collector you
run: `--otlp-endpoint` (or `REMOUNT_OTLP_ENDPOINT`) on `server`, `up` and
`standalone` exports one span per request as OTLP/HTTP JSON. With the flag
unset nothing is recorded. See [docs/observability.md](docs/observability.md).

## Repository layout

```
api/        public resource types, constants and the error taxonomy
client/     the Go client, with reconnect and idempotent retries
sdk/        the Python and TypeScript clients
internal/   the implementation: control plane, node, session log, broker, backends
cmd/        the binaries: remount, and the standalone conformance judge
examples/   runnable programs written against the public packages only
web/        the embedded operator console
images/     the default workspace image and the browser image
deploy/     reference deployments and the static site behind remount.dev
spec/       the wire protocol and its JSON Schema
docs/       the prose, including one ADR per decision under docs/adr/
```

`internal/` is not a supported import path. `internal/sim` runs the whole
system in one process with fault injection; that is where the failure model
is tested.

## Documentation

- [docs/using-remount.md](docs/using-remount.md): using a deployment, from a first workspace to a moved one.
- [docs/design.md](docs/design.md): data model, failure model, threat model, performance budget.
- [spec/PROTOCOL.md](spec/PROTOCOL.md): the wire protocol.
- [docs/adr/](docs/adr/README.md): the decisions, indexed by number.
- [docs/tutorial.md](docs/tutorial.md): a guided walkthrough.
- [docs/harness-integration.md](docs/harness-integration.md): running Claude Code, Codex, OpenCode and your own loop on Remount.
- [docs/credentials.md](docs/credentials.md): bindings, substitution, rotation, revocation, session principals, audit.
- [docs/api.md](docs/api.md): the agent HTTP API.
- [docs/operations.md](docs/operations.md): deploying, hardening and running it for real.
- [docs/security-profiles.md](docs/security-profiles.md): the generated support matrix by operating system, backend and runtime profile.
- [docs/observability.md](docs/observability.md): status, inspect, doctor, metrics, traces.
- [docs/console.md](docs/console.md): the operator console at `/console/`.
- [docs/images.md](docs/images.md): the workspace and browser images.
- [docs/mcp.md](docs/mcp.md): Remount over MCP.
- [docs/compatibility-policy.md](docs/compatibility-policy.md): what is public and what a version number means.
- [docs/releases.md](docs/releases.md): the release workflow and where it stands.
- [docs/benchmarks.md](docs/benchmarks.md): dated measurements.
- [CHANGELOG.md](CHANGELOG.md): user-visible changes.
- [docs/engineering/](docs/engineering/): dated audits, verification evidence, and the notes we kept while building it, including [MISTAKES.md](MISTAKES.md).

## Status and limits

Everything above is implemented and tested, including under the race
detector and in the one-process simulator with fault injection. The newer
pieces (runtime profiles, leases, computer sessions, typed errors, tracing,
runtime binding lifecycle) were each also exercised live on real servers,
real providers and a real browser; the runs are recorded in
[docs/engineering/verification-2026-09.md](docs/engineering/verification-2026-09.md).

Limits worth knowing before you plan around them:

- `process` and `docker` use cooperative proxying, so the production runtime
  profiles refuse them. `gvisor` and `firecracker` enforce networking and
  probe the host at startup; whether they are available depends on the host.
- The `microvm` profile has no live pass on this branch. The Firecracker
  backend's 2026-09-04 x86_64 KVM run stands on its own.
- A browser profile does not survive a sleep or a move.
- The first CONNECT on each browser proxy connection is recorded as an
  `unauthenticated` denial next to the allowed retry, and Chromium's own
  component traffic stays denied.
- Moving a workspace does not move running processes.
- Relay payload encryption exists and is tested but is not wired into the
  stock CLI and SDK connection path.
- No release has been tagged yet. Install from source or with `go install`.

Not part of this project: Apple Virtualization, a full virtual desktop as a
protocol resource, peer-to-peer transport, an RL scheduler, an end-user chat
UI, or any hosted service. An X11 or VNC stack can still run as an ordinary
workspace process when you need a whole desktop; see
[virtual desktop workloads](docs/harness-integration.md#browser-and-virtual-desktop-workloads).

## License

Apache-2.0. Implement the protocol however you like; replace this
implementation if you can do better.
