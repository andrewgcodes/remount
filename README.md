# Remount

**A durable computer for an agent: portable workspaces, brokered credentials,
and sessions that outlive the client, the connection and the machine.**

Remount is an open protocol and a single Go binary that turns a supported
machine — your laptop, a Mac mini, a bare-metal GPU box, a cloud VM — into a
place where an AI agent can run through one small exec / filesystem / session
interface.

Nothing here is hosted. You run the binary; there is no Remount service, no
account, and no telemetry endpoint. Apache-2.0.

**Using Remount:** start with [`docs/using-remount.md`](docs/using-remount.md).
It is the maintained user and coding-agent entry point for running harnesses,
retrieving modified files, operating durable Agents, and deploying the Modal
reference.

An agent's machine is a **workspace**, and Remount treats it as a movable value
rather than a machine. (A `computer`, the narrower resource added later, is a
browser the node drives *inside* a workspace.) A workspace's files can be
snapshotted, put to sleep, and restored on a compatible node. A client can
detach mid-command and reattach to retained execution without losing output; if
retention has evicted bytes, the gap is explicit. Moving a filesystem does not
move a running process.

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

## What this gives you that a container does not

Seven claims, each with the section or document that shows the mechanism and
states its limits.

1. **A durable computer, not a container.** A workspace is a filesystem plus
   processes that can be snapshotted, moved to another node, put to sleep, woken
   and reattached, with output replay and explicit gaps. An agent's work survives
   the client, the connection and the machine.
   → [move](#move-a-running-workspace-to-another-machine), [sleep and wake](#sleep-and-wake)
2. **Secret-blind execution.** The workspace never holds a provider key. The
   broker substitutes real credentials at the network edge, for allow-listed
   destinations only, with revocation reaching a live workspace within one renew,
   hard budgets, and an audit stream that answers who used which credential where.
   → [brokered credentials](#brokered-credentials-without-placing-keys-in-the-workspace)
3. **Isolation you can verify, not just claim.** Runtime profiles `dev`,
   `trusted-single-tenant`, `multi-tenant-isolated` and `microvm`; a node startup
   gate that fails closed; drift detection that parks workspaces rather than
   downgrading them; `remount conformance --profile` producing evidence you can
   attach to a review. A check that cannot run is reported `unavailable`, never
   healthy. gVisor passed `multi-tenant-isolated` live.
   → [isolation you can verify](#isolation-you-can-verify)
4. **Bounded background work.** Durable leases and idle policy live in the
   control plane, so a long command, a browser session or a download can outlive
   a turn without leaking compute forever. The deadline survives client death and
   control-plane restarts.
   → [hold a workspace](#hold-a-workspace-for-background-work)
5. **Browser and computer use built in.** `computer.*` operations — screenshot,
   input, navigation, downloads as artifacts — on any backend that can open a
   port, under the same egress policy as everything else.
   → [drive a browser](#drive-a-browser)
6. **One static binary, one protocol, three SDKs, your infrastructure.**
   `process`, `docker`, `gvisor` and `firecracker` backends; Go, Python and
   TypeScript clients; runs on a laptop, a VM, Kubernetes, Modal or Fly.
   Self-hosted only.
   → [project layout](#project-layout)
7. **An engineering culture you can inspect.** Failure-model simulation tests,
   race and fuzz lanes, protocol conformance, a dated verification ledger, and an
   ADR for every decision.
   → [docs/adr/](docs/adr/README.md), [MISTAKES.md](MISTAKES.md)

---

## Quick start, no credentials

Use the Go version declared in `go.mod` (currently 1.27.1). Nothing below needs
an account, a token or a provider key.

```sh
make build

# Server + node in one process.
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

## The fast path, once you have a provider key

One command runs a coding harness in a fresh workspace against a key it never
sees. With nothing listening on the default local address, `remount run` starts
`remount standalone` in the background for you, turns every provider key in
your environment into a brokered binding, and picks the one the recipe uses.

```sh
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

## Hold a workspace for background work

`ws sleep` means "sleep now". The opposite — keep this workspace claimed while
work is still running, then sleep it if nobody extends the deadline — is a
**lease**, and the deadline lives in the control plane rather than in your
process, because your process is not durable. A deploy, an OOM kill or a
partition takes a `setTimeout` with it; the workspace then runs until somebody
notices the bill.

```sh
./remount ws lease $WS --max 20m --min 30s --reason background_job
./remount ws lease renew $WS $LEASE --extend 10m
./remount ws idle-policy $WS --sleep-after 10m --destroy-after 2h
./remount ws mark-active $WS --reason new_turn
```

Activity is explicit: session traffic does not extend a deadline. When the
deadline passes the node sends `SIGTERM`, waits a bounded grace, then `SIGKILL`,
and the replayed session log carries
`exit.reason = "lifecycle_deadline_expired"` so a later reader knows a policy
decision ended the work. `ws get` publishes the pending `lifecycle_deadline`.
See [holding a workspace](docs/using-remount.md#hold-a-workspace-for-background-work)
and [ADR 0090](docs/adr/0090-durable-workspace-leases-and-idle-policy.md).

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

## Isolation you can verify

A **runtime profile** is what a machine claims, and claiming one is a startup
gate rather than a label. `dev` claims nothing; `trusted-single-tenant` requires
every registered backend to isolate the workspace from the host and authenticate
to the broker; `multi-tenant-isolated` additionally requires deny-first enforced
egress, sibling isolation and a private network namespace, and refuses a node
that registers `process` or `docker` at all; `microvm` restricts that to
verified microVM backends plus a live host compatibility result.

```sh
./remount up --profile multi-tenant-isolated             # a gate, not a label
./remount ws create --requires-profile multi-tenant-isolated
./remount doctor --profile multi-tenant-isolated --json  # per node, per check
./remount conformance --profile multi-tenant-isolated --markdown conformance.md
```

The control plane evaluates the backend descriptors a node actually advertised,
never operator labels, so a node serving `gvisor` and `process` together is a
`dev` node. A workspace nobody can place stays `pending` with
`pending_reason: profile_unschedulable` instead of landing somewhere weaker, and
a node that loses a prerequisite while serving becomes unschedulable within one
probe interval. All three commands are three-valued: exit `0` everything passed,
`1` something failed, `2` nothing failed but something could not be checked.
There is no path on which an unavailable check exits `0`.

`multi-tenant-isolated` and `microvm` depend on Linux kernel mechanisms, so only
a Linux host can satisfy them; on macOS and Windows the strongest honest answer
is `dev`. See [choose a runtime profile](docs/using-remount.md#choose-a-runtime-profile),
[the generated support matrix](docs/security-profiles.md) and
[ADR 0089](docs/adr/0089-runtime-profiles-and-drift.md).

## Drive a browser

A **computer** is a browser the node drives inside a workspace. The node holds
the DevTools conversation over the same path `remount port` uses, so every
backend that can forward a workspace port can host one and nothing extra runs
inside the workspace.

```sh
docker build -t remount-browser:local images/browser
WS=$(./remount ws create --backend docker --image remount-browser:local --json | jq -r .id)
CMP=$(./remount computer create $WS --json | jq -r .computer)

./remount computer navigate $WS $CMP https://example.internal/
./remount computer screenshot $WS $CMP --out shot.png
./remount computer click $WS $CMP 640 360
./remount computer type $WS $CMP 'search text'
./remount computer downloads $WS $CMP --json
```

Coordinates are CSS pixels in the viewport declared at `create`, so the picture
and the click that follows it agree; every action batch carries an input
sequence, so a retried click is dropped rather than applied twice; downloads
become tenant-scoped artifacts; and failures are typed
(`closed`/`browser_crashed`, `denied`/`navigation_denied`). The same operations
are in the Go, Python and TypeScript SDKs. Read
[the computer-session section](docs/using-remount.md#computer-sessions-the-built-in-browser-api)
for what the API does not promise — including the current state of brokered
browsing — and [ADR 0088](docs/adr/0088-computer-sessions-over-port-substrate.md).

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
| `remount up [--profile P]` | enroll this machine as a node (outbound only); `--profile` is a fail-closed startup gate |
| `remount standalone` | both, in one process, for trying it out |
| `remount ws create\|ls\|get\|move\|sleep\|wake\|snapshot\|destroy` | manage workspaces |
| `remount ws create --requires-profile P` | place this workspace only on a node that currently satisfies runtime profile `P` |
| `remount ws lease\|idle-policy\|mark-idle\|mark-active` | durable holds and the no-work cleanup rule; the deadline lives in the control plane |
| `remount exec WS -- cmd` | run a command, streamed |
| `remount sh WS` | interactive pty |
| `remount attach WS SESSION --from N` | reattach to a running session |
| `remount fs read\|write\|ls\|stat\|rm\|mv\|mkdir\|search\|edit` | workspace filesystem |
| `remount port WS PORT` | forward a port out of the workspace |
| `remount computer create\|get\|screenshot\|click\|type\|key\|scroll\|navigate\|eval\|downloads\|close` | drive a browser inside a workspace |
| `remount run RECIPE [--dir . \| --repo URL[@REF]] [--binding b_openai] -- TASK` | seed a workspace, install a coding harness, run it against brokered keys |
| `remount ws create --repo URL[@REF] [--repo-depth N]` | the node clones through the broker before the workspace is ready; the token never enters the tree |
| `remount run RECIPE --queue FILE [--sleep-after D]` | run a file of tasks in order in one workspace; progress lives on the control plane |
| `remount handoff` / `remount resume WS` | move this checkout and the harness's conversation into a workspace; pick it back up from anywhere |
| `remount nodes` / `events` / `timers` | inspect the fleet |
| `remount fleet quarantine ...` | durably fence, checkpoint, stop or destroy an incident scope |
| `remount status` / `inspect` / `metrics` | inspect health and capacity |
| `remount doctor [--profile P] [--node ID]` | every check for damage, loss or disagreement; with `--profile`, per-node profile evidence |
| `remount conformance [--profile P] [--markdown FILE]` | judge a deployment black-box against the protocol manifest and a runtime profile |

`doctor --profile` and `conformance --profile` exit `0` pass, `1` fail,
`2` nothing failed but something could not be checked.

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

Mutations accept `client.WithIdempotencyKey`. Errors carry a stable `code` and,
where one code covers outcomes you must tell apart, a stable `reason`; match
with `api.Is` and never on the message text, which is for humans:

```go
if _, _, _, err := c.Run(ctx, ws.ID, "curl", "https://api.example.com"); err != nil {
    if api.Is(err, api.CodeDenied, api.ReasonEgressDenied) {
        // bind a credential for that host, or add an egress rule
    }
}
```

`api` re-exports every `Reason*` constant. Python raises one exception class per
reason and TypeScript exports one error class per reason, all deriving from
`ProtocolError`; a reason an older client has never heard of degrades to the base
class rather than to an unhandled case. CI compiles the SDK from a separate Go
module so accidental dependencies on implementation-only packages fail the build.
See [typed errors](docs/using-remount.md#typed-errors) and
[the compatibility policy](docs/compatibility-policy.md).

## Seeing what is happening

```sh
remount status            # the fleet, with problems surfaced without asking
remount inspect $WS       # one workspace, down to session log positions on disk
remount doctor --deep     # re-hash every snapshot and report anything damaged
```

All three take `--json`. `scripts/collect.sh` gathers everything into one
document and `scripts/explain.py` turns it into prose that leads with the
verdict. See [docs/observability.md](docs/observability.md).

Spans go to a collector you run, or nowhere: `--otlp-endpoint` (or
`REMOUNT_OTLP_ENDPOINT`) on `server`, `up` and `standalone` exports one span per
request as OTLP/HTTP JSON. There is no default endpoint and no Remount-operated
collector, so with the flag unset nothing is recorded and nothing leaves the
process.

## Project layout

```
api/        stable public resource types, constants and the error taxonomy
client/     the supported reconnecting Go client
sdk/        the Python and TypeScript clients
internal/   the implementation: control plane, node, session log, broker, backends
cmd/        the binaries — remount, and the standalone conformance judge
examples/   runnable programs written against the public packages only
web/        the embedded operator console
images/     the default workspace image and the reference browser image
deploy/     reference deployments, and the static vanity import site
spec/       the wire protocol and its JSON Schema
docs/       the prose, including docs/adr/ — one file per decision
```

`internal/` is not a supported import path; the compatibility policy covers
`api`, `client`, the two SDK packages, the wire protocol, and the CLI's
documented flags and `--json` shapes. `internal/sim` runs the whole system in
one process with fault injection, which is where the failure model is tested.

## Documentation

- [docs/using-remount.md](docs/using-remount.md) — the maintained entry point for using a deployment.
- [docs/design.md](docs/design.md) — the architecture, in full: data model, failure model, threat model, performance budget.
- [spec/PROTOCOL.md](spec/PROTOCOL.md) — the wire protocol, readable in ten minutes.
- [docs/adr/](docs/adr/README.md) — the decisions, and what each one costs, indexed by number.
- [docs/tutorial.md](docs/tutorial.md) — a guided walkthrough from zero to a moved workspace.
- [docs/harness-integration.md](docs/harness-integration.md) — running Claude Code, Codex, OpenCode and your own loop on Remount.
- [docs/api.md](docs/api.md) — the agent HTTP API: agents, transcript streaming, approvals, diff, terminal, files, previews.
- [docs/operations.md](docs/operations.md) — deploying, hardening and running it for real.
- [docs/security-profiles.md](docs/security-profiles.md) — the generated support matrix: operating systems, per-backend features, and which runtime profile each backend can satisfy.
- [docs/observability.md](docs/observability.md) — inspecting a deployment at three depths, traces, and detecting damage.
- [docs/console.md](docs/console.md) — the embedded operator console at `/console/`.
- [docs/images.md](docs/images.md) — the default workspace image, the browser image, and what any substitute must provide.
- [docs/mcp.md](docs/mcp.md) — exposing Remount and wrapped servers over MCP.
- [docs/compatibility-policy.md](docs/compatibility-policy.md) — what is public, what a version number promises, and how a break is announced.
- [docs/releases.md](docs/releases.md) — the release and installation workflow, and its current disposition.
- [docs/benchmarks.md](docs/benchmarks.md) — dated measurements, not guarantees.
- [CHANGELOG.md](CHANGELOG.md) — user-visible changes, in Keep a Changelog form.
- [docs/engineering/hardening-lessons.md](docs/engineering/hardening-lessons.md) — the review method distilled from the audits, race failures and live cloud tests.
- [docs/engineering/](docs/engineering/) — dated audits, implementation requests, dispositions and verification evidence.
- [MISTAKES.md](MISTAKES.md) — every bug we hit building this, and what each one taught.

## Status

**Working and tested.** Exec and pty sessions with replayable reconnect;
workspace filesystem with server-side search and atomic edits; snapshots and
cross-node moves; sleep and wake with durable timers; the credential broker with
typed policy over host, port, HTTP method, path and request/response budgets;
the claim queue with leases; the event log; port forwarding; durable Agents with
fork/child composition; Python/TypeScript/Go SDKs; and an embedded
[operator console](docs/console.md).

Also working and tested, and newer:

- **Runtime profiles** — the startup gate, `requires.profile` scheduling, drift
  detection, `doctor --profile` and `conformance --profile`
  ([ADR 0089](docs/adr/0089-runtime-profiles-and-drift.md)). gVisor passed
  `multi-tenant-isolated` live in a Linux VM: nine `doctor` checks green, 78
  conformance checks judged with 64 passed, 0 failed, 14 unavailable and every
  required row green, and a removed host prerequisite parked workspaces within a
  second and restored them when it came back.
- **Durable workspace leases and idle policy** — deadlines that survive the
  client, node loss and a control-plane restart
  ([ADR 0090](docs/adr/0090-durable-workspace-leases-and-idle-policy.md)).
- **Computer sessions** — a real browser driven over the port substrate, verified
  against Chromium 152 on the docker backend
  ([ADR 0088](docs/adr/0088-computer-sessions-over-port-substrate.md),
  [0094](docs/adr/0094-a-resumed-computer-handle-reads-the-input-sequence.md)).
- **Typed errors** in all three languages, and optional OpenTelemetry tracing
  with no new dependency and no default endpoint
  ([ADR 0093](docs/adr/0093-typed-errors-support-matrix-and-dependency-free-tracing.md)).
- **Dynamic binding lifecycle and session principals** — create, rotate and
  revoke bindings at runtime, with revocation reaching a live workspace within
  one renew ([ADR 0091](docs/adr/0091-dynamic-binding-lifecycle-and-session-principals.md),
  [0092](docs/adr/0092-broker-substitution-locations-and-redaction.md)).

The `process` and `docker` backends provide cooperative proxying, so the
`isolated` and `multi_tenant` workspace security profiles reject them, and the
`multi-tenant-isolated` and `microvm` runtime profiles refuse a node that
registers them. Linux `gvisor` and `firecracker` are implemented with enforced
networking and fail-closed runtime probes; their availability depends on the
actual host. See [security profiles](docs/security-profiles.md) and
[backend setup](docs/operations.md#choosing-a-backend). Optional
[warm-standby control failover](docs/operations.md#controller-availability)
uses an object-store lease and controller epochs while retaining one active
SQLite writer.

Package retrieval can be granted separately with a `connector:"package"` egress
rule. The managed HTTPS endpoint is read-only, reports SHA-256 provenance,
stores immutable content by digest, and keeps cache references and metadata
private to each workspace. See the
[operations guide](docs/operations.md#network-policy) for the request format
and its deliberate limitations.

Long-lived state is bounded: workspace, session, request, snapshot, artifact,
connector, event, timer and idempotency limits have fail-closed defaults.
Reference-aware artifact collection and prefix-only event/control-record
retention are observable in diagnostics and metrics. See the
[operations guide](docs/operations.md#capacity-quotas-and-retention) before
changing those limits.

**Honest limits.**

- A browser profile lives under the snapshot-excluded `.remount` directory, so
  it is node-local and does not survive a sleep or a move on purpose; a computer
  after a wake is a fresh browser on the same filesystem.
- Brokered browsing to an allowed host has its own current status; read
  [the computer-session section](docs/using-remount.md#computer-sessions-the-built-in-browser-api)
  before planning on it.
- The `microvm` runtime profile has no live pass on this branch. The Firecracker
  backend's 2026-09-04 x86_64 KVM evidence stands, and the profile lane is
  recorded as `unavailable` rather than as a pass.
- Not delivered as general-purpose features: Apple Virtualization, a full
  virtual desktop as a protocol resource, direct peer-to-peer transport, a
  dedicated RL scheduler, or an end-user chat UI. Relay payload encryption has a
  tested internal implementation but is not wired into the stock CLI/SDK
  connection path.
- Public releases and production qualification remain separate gates; no release
  has been tagged. See [releases](docs/releases.md),
  [current implementation status](docs/engineering/current-status.md) and
  [design limits](docs/design.md#10-current-limitations).

External browser and VNC stacks can still run as ordinary workspace processes
when you need a whole desktop rather than a browser; see
[virtual desktop workloads](docs/harness-integration.md#browser-and-virtual-desktop-workloads).

## License

Apache-2.0. The protocol is meant to be implemented by anyone; the reference
implementation is meant to be replaced if you can do better.
