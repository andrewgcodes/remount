# Remount

**Portable agent workspaces, brokered credentials, durable sessions.**

Remount is an open protocol and a single Go binary that turns a supported
machine — your laptop, a Mac mini, a bare-metal GPU box, a cloud VM — into a
place where an AI agent can run through one small exec / filesystem / session
interface.

**Using Remount:** start with [`docs/using-remount.md`](docs/using-remount.md).
It is the maintained user and coding-agent entry point for running harnesses,
retrieving modified files, operating durable Agents, and deploying the Modal
reference.

The agent's computer is a **workspace**, and Remount treats it as a movable value
rather than a machine. Its files can be snapshotted, put to sleep, and restored
on a compatible node. A client can detach mid-command and reattach to retained
execution without losing output; if retention has evicted bytes, the gap is
explicit. Moving a filesystem does not move a running process.

The workspace process receives credential references rather than reusable
secrets. The node's egress broker swaps a reference for the real value only on
an authorized TLS request, with a TTL and audit trail. This removes static keys
from the workspace, but it does not make an authorized API harmless: a
compromised workspace can still abuse capabilities it was granted. This applies
to brokered API keys, not harness-native subscription logins, whose credentials
remain workspace-resident; see [authentication modes](docs/harness-integration.md#two-kinds-of-auth-stated-plainly).

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

Nodes and clients require no public inbound listener; both dial the server.
Session frames transit the relay in the server process, while the control-plane
handler does not interpret their bodies.

---

## Quick start from source

Use the Go version declared in `go.mod` (currently 1.27.1). Repository access
is required; public package publication is a separate release gate.

One command runs a coding harness in a fresh workspace against a key it never
sees. With nothing listening on the default local address, `remount run` starts
`remount standalone` in the background for you, turns every provider key in
your environment into a brokered binding, and picks the one the recipe uses.

```sh
make build
export OPENAI_API_KEY=sk-...
./remount run opencode --dir . --model openai/gpt-4o-mini -- 'add a README'
```

```
started remount standalone in the background (pid 4242, data ~/.local/share/remount, ...)
using binding b_openai ($OPENAI_API_KEY) for opencode
...
```

The key stays in the standalone's process environment; the workspace gets a
placeholder and `remount events --ws WS` shows every `cred.used`. The default
ACP-capable recipe runs as a durable Agent; inspect its transcript and files
to verify completion. Set
`REMOUNT_AUTOSTART=0` or `REMOUNT_SERVER` to opt out of the background start.

The pieces underneath:

```sh
# Server + node in one process, no account, no token.
./remount standalone --data ./data &
export REMOUNT_SERVER=http://127.0.0.1:7443

WS=$(./remount ws create --name hello)
./remount exec $WS -- sh -c 'echo hello from $(hostname)'
./remount sh $WS                       # interactive shell, with a real pty
./remount fs write $WS notes.md < README.md
./remount fs search $WS 'movable value'
```

Kill the network mid-command and the process keeps running; reattach and the
output replays from where you left off:

```sh
./remount exec $WS -- sh -c 'for i in $(seq 1 100); do echo line $i; sleep 1; done'
# ^C, then:
./remount ws ls
./remount attach $WS <session-id> --from 0
```

## Move a running workspace to another machine

```sh
# On the second machine:
# REMOUNT_TOKEN is already exported in this shell; do not put it on argv.
./remount up --server https://your-server --label zone=gpu

# From anywhere:
./remount ws move $WS --label zone=gpu
```

The filesystem is snapshotted, the workspace is re-queued, a node in that zone
claims it, and your files are there. Plain exec/PTY processes are not restarted
from the event log. Use the durable Agent lifecycle or `remount resume` to
continue a supported harness from its saved state. Workspace identity, files
and policy travel with it.
Files are portable between compatible backends; installed tools, running
processes, permissions and architecture-specific binaries are not magically
portable.

An ordinary snapshot is a labeled live filesystem capture and is useful for
inspection or export. Use `--authoritative` when the result must become the
workspace's failover checkpoint; that path quiesces Remount-managed execution,
uploads the artifact, and commits the reference before returning success.

```sh
./remount ws snapshot $WS                 # consistency=live, not failover state
./remount ws snapshot $WS --authoritative # quiesced and durably committed
```

## Sleep and wake

```sh
./remount ws sleep $WS --after 72h
./remount ws sleep $WS --on github.pr.merged     # wake on an event instead
```

A sleeping workspace has no assigned node; its retained artifacts still use
storage. Provisioned nodes can keep incurring compute charges until separately
scaled down or terminated. On an authenticated server, wake it with a webhook
(or use `remount ws wake "$WS"`):

```sh
curl -X POST $REMOUNT_SERVER/v1/events -H "Authorization: Bearer $REMOUNT_TOKEN" \
     -d '{"type":"github.pr.merged","payload":{"pr":42}}'
```

## Brokered credentials without placing keys in the workspace

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

Inside the workspace, the variable holds the placeholder — and the call still
works, because the broker substitutes the real key on the way out:

```sh
$ ./remount exec $WS -- sh -c 'echo $OPENAI_API_KEY'
sk-proj-PLACEHOLDER

$ ./remount exec $WS -- sh -c 'curl -s $OPENAI_BASE_URL/chat/completions \
      -H "Authorization: Bearer $OPENAI_API_KEY" -d @req.json'
{"choices":[{"message":{"content":"..."}}]}
```

Send that same placeholder through the broker to any other destination and the
broker refuses it before making the outbound request:

```
$ ./remount exec $WS -- sh -c 'curl -s -o /dev/null -w "%{http_code}" \
      $REMOUNT_BROKER/d/api.anthropic.com/v1/messages -H "Authorization: Bearer $OPENAI_API_KEY"'
403

$ ./remount events --ws $WS | grep egress
egress.denied  {"decision":"leak_blocked","host":"api.anthropic.com",
                "reason":"placeholder for b_openai sent to api.anthropic.com"}
```

## Everything is an event

```sh
./remount events --follow
```

```
   1 13:24:18.402 ws.created      ws_06g6…  andrewgao  {"name":"hello"}
   4 13:24:18.551 ws.claiming     ws_06g6…             {"gen":1}
   6 13:24:18.702 ws.claimed      ws_06g6…             {"gen":1}
   9 13:24:31.461 cred.used       ws_06g6…  andrewgao  {"binding":"b_openai","host":"api.openai.com","status":200}
  12 13:24:59.942 egress.denied   ws_06g6…  andrewgao  {"decision":"leak_blocked","host":"api.anthropic.com"}
```

SQLite resource rows are the transactional source of lifecycle state; the
append-only event log is the durable audit and observation record.

---

## Commands

| Command | What it does |
|---|---|
| `remount server` | control plane + relay + artifact store |
| `remount up` | enroll this machine as a node (outbound only) |
| `remount standalone` | both, in one process, for trying it out |
| `remount ws create\|ls\|get\|move\|sleep\|wake\|snapshot\|destroy` | manage workspaces |
| `remount exec WS -- cmd` | run a command, streamed |
| `remount sh WS` | interactive pty |
| `remount attach WS SESSION --from N` | reattach to a running session |
| `remount fs read\|write\|ls\|stat\|rm\|mv\|mkdir\|search\|edit` | workspace filesystem |
| `remount port WS PORT` | forward a port out of the workspace |
| `remount run RECIPE [--dir . \| --repo URL[@REF]] [--binding b_openai] -- TASK` | seed a workspace, install a coding harness, run it against brokered keys |
| `remount ws create --repo URL[@REF] [--repo-depth N]` | the node clones through the broker before the workspace is ready; the token never enters the tree |
| `remount run RECIPE --queue FILE [--sleep-after D]` | run a file of tasks in order in one workspace; progress lives on the control plane |
| `remount handoff` / `remount resume WS` | move this checkout and the harness's conversation into a workspace; pick it back up from anywhere |
| `remount nodes` / `events` / `timers` | inspect the fleet |
| `remount fleet quarantine ...` | durably fence, checkpoint, stop or destroy an incident scope |
| `remount status` / `inspect` / `doctor` / `metrics` | inspect health and capacity |

Sessions belong to the node, not to the terminal that started them. Ctrl-C in
`exec`, `sh` or `attach` detaches and prints the `remount attach WS SID`
command that picks the process back up; pass `--kill-on-interrupt` to send
SIGINT to the remote process instead (a second Ctrl-C still detaches).
`exec --timeout D` is enforced server-side by the node and has no client-side
ceiling; `0` (the default) means no timeout. Long-running work is
`exec ... ` + Ctrl-C (or a closed laptop) + `attach --from N`.

## Go SDK

Applications import the supported public packages, not `internal/`:

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

Mutations accept `client.WithIdempotencyKey`; stable error codes live in
`api`. CI compiles the SDK from a separate Go module so accidental dependencies
on implementation-only packages fail the build.

## Seeing what is happening

```sh
remount status            # the fleet, with problems surfaced without asking
remount inspect $WS       # one workspace, down to session log positions on disk
remount doctor --deep     # re-hash every snapshot and report anything damaged
```

All three take `--json`. `scripts/collect.sh` gathers everything into one
document and `scripts/explain.py` turns it into prose that leads with the
verdict. See [docs/observability.md](docs/observability.md).

## Documentation

- [docs/design.md](docs/design.md) — the architecture, in full: data model, failure model, threat model, performance budget.
- [spec/PROTOCOL.md](spec/PROTOCOL.md) — the wire protocol, readable in ten minutes.
- [docs/adr/](docs/adr/) — the decisions, and what each one costs.
- [docs/tutorial.md](docs/tutorial.md) — a guided walkthrough from zero to a moved workspace.
- [docs/harness-integration.md](docs/harness-integration.md) — running Claude Code, Codex, OpenCode and your own loop on Remount.
- [docs/api.md](docs/api.md) — the agent HTTP API: agents, transcript streaming, approvals, diff, terminal, files, previews.
- [docs/operations.md](docs/operations.md) — deploying, hardening and running it for real.
- [docs/images.md](docs/images.md) — the default workspace image and what any substitute image must provide.
- [docs/observability.md](docs/observability.md) — inspecting a deployment at three depths, and detecting damage.
- [docs/engineering/hardening-lessons.md](docs/engineering/hardening-lessons.md) — the review method distilled from the audits, race failures and live cloud tests.
- [docs/engineering/](docs/engineering/) — dated audits, implementation requests, dispositions and verification evidence.
- [MISTAKES.md](MISTAKES.md) — every bug we hit building this, and what each one taught.

## Status

Working and tested: exec and pty sessions with replayable reconnect, workspace
filesystem with server-side search and atomic edits, snapshots and cross-node
moves, sleep and wake with durable timers, the credential broker, the claim
queue with leases, the event log, port forwarding, durable Agents with
fork/child composition, Python/TypeScript/Go SDKs, and an embedded
[operator console](docs/console.md). Typed broker policy can restrict host,
port, HTTP method, path and request/response budgets. The `process` and
`docker` backends provide cooperative proxying, so `isolated` and
`multi_tenant` security profiles reject them.

Linux `gvisor` and `firecracker` backends are implemented with enforced
networking and fail-closed runtime probes. Their availability and security
claims depend on the actual host; see [security profiles](docs/security-profiles.md)
and [backend setup](docs/operations.md#choosing-a-backend). Optional
[warm-standby control failover](docs/operations.md#controller-availability)
uses an object-store lease and controller epochs while retaining one active
SQLite writer.

Package retrieval can be granted separately with a
`connector:"package"` egress rule. The managed HTTPS endpoint is read-only,
reports SHA-256 provenance, stores immutable content by digest, and keeps cache
references and metadata private to each workspace. See the
[operations guide](docs/operations.md#network-policy) for the request format
and its deliberate limitations.

Long-lived state is bounded: workspace, session, request, snapshot, artifact,
connector, event, timer and idempotency limits have fail-closed defaults.
Reference-aware artifact collection and prefix-only event/control-record
retention are observable in diagnostics and metrics. See the
[operations guide](docs/operations.md#capacity-quotas-and-retention) before
changing those limits.

Not delivered as general-purpose features: Apple Virtualization, display and
browser sessions, direct peer-to-peer transport, or an end-user chat UI. Relay
payload encryption has a tested internal implementation but is not wired into
the stock CLI/SDK connection path. Public releases and production qualification
remain separate gates. See [current implementation status](docs/engineering/current-status.md)
and [design limits](docs/design.md#10-current-limitations).

External browser and VNC stacks can still run as ordinary workspace processes;
see [virtual desktop workloads](docs/harness-integration.md#browser-and-virtual-desktop-workloads).

## License

Apache-2.0. The protocol is meant to be implemented by anyone; the reference
implementation is meant to be replaced if you can do better.
