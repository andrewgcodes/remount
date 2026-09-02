# Remount

**Any agent, any machine, never holding the keys, never losing its place.**

Remount is an open protocol and a single Go binary that turns any computer — your
laptop, a Mac mini, a bare-metal GPU box, a cloud VM — into a place where an AI
agent can run through one small exec / filesystem / session interface.

The agent's computer is a **workspace**, and Remount treats it as a movable value
rather than a machine. Its files can be snapshotted, paused for days at
storage-only cost, resumed on a different node, and reattached mid-command, while
the agent sees one unbroken stream with output replayed from where it left off.

The workspace never holds a real credential. It sees only references; the node's
egress broker swaps in the real key per destination, per agent, with a TTL and a
full audit trail. An agent can have root on your infrastructure and a compromised
box has nothing to leak.

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

Nothing listens. Nodes and clients both dial out. The control plane carries
coordination only — session bytes never pass through it.

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

## Secrets the agent can never leak

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

Send that same placeholder anywhere else and it is refused before a byte leaves
the machine:

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

The log is the source of truth. Workspace state, the UI, audit and replay are
all consumers of it.

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
- [MISTAKES.md](MISTAKES.md) — every bug we hit building this, and what each one taught.

## Status

Working and tested: exec and pty sessions with lossless reconnect, workspace
filesystem with server-side search and atomic edits, snapshots and cross-node
moves, sleep and wake with durable timers, the credential broker, the claim
queue with leases, the event log, port forwarding, and both `process` and
`docker` backends.

Not built yet, and honestly named as such: microVM backends (Firecracker,
Apple Virtualization), the display and browser session kinds, and the web UI.
See [docs/design.md](docs/design.md#not-built-yet).

## License

Apache-2.0. The protocol is meant to be implemented by anyone; the reference
implementation is meant to be replaced if you can do better.
