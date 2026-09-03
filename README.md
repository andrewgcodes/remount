# Remount

**Any agent, any machine, never holding the keys, never losing its place.**

Remount is an open protocol and a single Go binary that turns a supported
machine — your laptop, a Mac mini, a bare-metal GPU box, a cloud VM — into a
place where an AI agent can run through one small exec / filesystem / session
interface.

The agent's computer is a **workspace**, and Remount treats it as a movable value
rather than a machine. Its files can be snapshotted, paused for days at
storage-only cost, resumed on a different node, and reattached mid-command, while
the agent sees one unbroken stream with output replayed from where it left off.

The workspace process receives credential references rather than reusable
secrets. The node's egress broker swaps a reference for the real value only on
an authorized TLS request, with a TTL and audit trail. This removes static keys
from the workspace, but it does not make an authorized API harmless: a
compromised workspace can still abuse capabilities it was granted.

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

## Try it in 60 seconds

```sh
go build -o remount ./cmd/remount

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
./remount up --server https://your-server --token $REMOUNT_TOKEN --label zone=gpu

# From anywhere:
./remount ws move $WS --label zone=gpu
```

The filesystem is snapshotted, the workspace is re-queued, a node in that zone
claims it, and your files are there. Sessions are restarted by the harness from
the event log; the workspace identity, its files and its policy travel with it.
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

## Sleep for days at storage-only cost

```sh
./remount ws sleep $WS --after 72h
./remount ws sleep $WS --on github.pr.merged     # wake on an event instead
```

A sleeping workspace has no node and costs nothing but storage. Wake it with a
webhook:

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
| `remount nodes` / `events` / `timers` | inspect the fleet |
| `remount fleet quarantine ...` | durably fence, checkpoint, stop or destroy an incident scope |
| `remount status` / `inspect` / `doctor` / `metrics` | inspect health and capacity |

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
- [docs/operations.md](docs/operations.md) — deploying, hardening and running it for real.
- [docs/observability.md](docs/observability.md) — inspecting a deployment at three depths, and detecting damage.
- [docs/engineering/hardening-lessons.md](docs/engineering/hardening-lessons.md) — the review method distilled from the audits, race failures and live cloud tests.
- [docs/engineering/](docs/engineering/) — dated audits, implementation requests, dispositions and verification evidence.
- [MISTAKES.md](MISTAKES.md) — every bug we hit building this, and what each one taught.

## Status

Working and tested: exec and pty sessions with replayable reconnect, workspace
filesystem with server-side search and atomic edits, snapshots and cross-node
moves, sleep and wake with durable timers, the credential broker, the claim
queue with leases, the event log, port forwarding, and both `process` and
`docker` backends. Typed broker policy can restrict host, port, HTTP method,
path and request/response budgets. Both built-in backends provide cooperative
proxying rather than non-bypassable egress, so production security profiles
reject them.

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

Not built yet, and honestly named as such: a production backend with enforced
egress, automated multi-controller high availability, microVM backends
(Firecracker, Apple Virtualization), the display and browser session kinds,
and the web UI.
See [docs/design.md](docs/design.md#not-built-yet).

## License

Apache-2.0. The protocol is meant to be implemented by anyone; the reference
implementation is meant to be replaced if you can do better.
