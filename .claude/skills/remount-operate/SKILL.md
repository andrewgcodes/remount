---
name: remount-operate
description: "Use when operating a live Remount deployment: creating or moving workspaces, running harnesses, configuring typed egress or brokered credentials, using the package connector, inspecting health, or containing an incident. Not for changing Remount code; use remount-dev."
---

# Operating Remount

Every command needs a server URL and a token. Set them once.

```sh
export REMOUNT_SERVER=https://your-server
export REMOUNT_TOKEN=...
```

For a local experiment, `remount standalone --data ./data` runs a server and a
node in one process with no token on `127.0.0.1:7443`.

Flags may come before or after positional arguments. `remount ws move WS --node
N` and `remount ws move --node N WS` are the same.

## The CLI

| Command | Does |
|---|---|
| `remount nodes` | list nodes with OS, CPU, memory, backends, labels |
| `remount ws create [--name N] [--node ID] [--label k=v] [--binding ID] [--env K=V] [--backend B] [--security PROFILE] [--egress-rule JSON] [--cpu N] [--mem MiB] [--exclude GLOB]` | create and wait until claimed |
| `remount ws ls` | every workspace with state, node, generation, last snapshot |
| `remount ws get WS` | full JSON, including requires and placement |
| `remount ws move WS [--node ID] [--label k=v] [--cpu N] [--mem MiB] [--backend B]` | snapshot, re-queue, wait for the new node |
| `remount ws sleep WS --after 72h` or `--on event.type` | snapshot, release, pause until a timer or event |
| `remount ws wake WS` | resume a paused workspace now |
| `remount ws snapshot WS [--authoritative]` | take a live snapshot, or quiesce and commit authoritative failover state |
| `remount ws destroy WS` | release and delete |
| `remount exec WS -- cmd args` | run a command; stdout, stderr and exit code stream back |
| `remount exec --timeout 10m --cwd DIR --env K=V --stdin WS -- cmd` | with options |
| `remount sh WS [cmd]` | an interactive pty |
| `remount attach WS SESSION --from N` | reattach to a session and replay from seq N |
| `remount fs read WS PATH` | print a file |
| `remount fs write WS PATH < file` | write a file, creating parents |
| `remount fs ls WS [PATH]`, `fs stat`, `fs mkdir`, `fs mv WS FROM TO`, `fs rm [-r] WS PATH` | the rest |
| `remount fs search WS PATTERN [PATH] [--glob '*.go'] [--max N]` | server-side regex search |
| `remount fs edit [--all] WS PATH OLD NEW` | atomic find and replace |
| `remount port WS PORT [--local ADDR]` | forward a port out of the workspace |
| `remount events [--follow] [--ws WS] [--from N] [--json]` | the canonical log |
| `remount timers` | durable timers |
| `remount fleet quarantine --action ACTION SELECTORS...` | durably freeze, revoke, checkpoint, stop or destroy an incident scope |
| `remount status`, `inspect WS`, `doctor --deep`, `metrics` | inspect fleet, workspace, integrity and capacity |

Add `--json` to `ws`, `nodes`, `fs ls`, and `events` for machine-readable output.

## Giving a workspace a secret it cannot leak

Define the binding on the server, in a JSON file passed as `--bindings`.
Secrets may be `$ENV` references so the file holds no secret itself.

```json
[{"id": "b_openai",
  "secret": "$OPENAI_API_KEY",
  "destinations": ["api.openai.com"],
  "placeholder": "sk-proj-REMOUNT-PLACEHOLDER-NOT-A-REAL-KEY",
  "ttl_sec": 900}]
```

Create the workspace with the binding and reference it in env.

```sh
WS=$(remount ws create --binding b_openai \
      --env OPENAI_API_KEY=ref:b_openai \
      --env OPENAI_BASE_URL='${REMOUNT_BROKER}/d/api.openai.com/v1')
```

Inside, the variable holds the placeholder and the call still works.

```sh
remount exec $WS -- sh -c 'echo $OPENAI_API_KEY'
# sk-proj-REMOUNT-PLACEHOLDER-NOT-A-REAL-KEY

remount exec $WS -- sh -c 'curl -s $OPENAI_BASE_URL/chat/completions \
  -H "Authorization: Bearer $OPENAI_API_KEY" -d @req.json'
```

The same placeholder sent to any other host returns 403 before a byte leaves
the machine and appears in the log as `egress.denied` with decision
`leak_blocked`.

Hosts a workspace may reach without a credential are set per node with
`remount up --allow api.github.com --allow registry.npmjs.org`. Everything
else is denied. `--allow-private HOST` permits a host that resolves to a
private address, for a local model server.

That is the legacy `local` security profile. An explicit typed network policy
replaces it with first-match capabilities for protocol, host, port, method,
path, request count and byte budgets. Production-oriented profiles require a
backend that can enforce the gateway. The built-in process and Docker backends
only provide cooperative proxying, so Remount rejects them for `isolated` and
`multi_tenant` profiles instead of presenting proxy environment variables as
non-bypassable isolation.

For an immutable package read, grant a `connector:"package"` HTTPS GET/HEAD
rule and use `${REMOUNT_PACKAGE_CONNECTOR}/<registry-host>/<path>`. This
managed path rechecks policy, can require
`X-Remount-Expected-Digest: sha256:<hex>`, returns the content digest, and
keeps cache paths and hit state private to the workspace. It cannot be spent
through the model reverse proxy or CONNECT.

## Pointing a harness at the broker

The broker's address is per node and per materialize. Read it from
`.remount/env` inside the workspace at start-up rather than copying it into a
config file, so the same launcher works after a move.

```sh
#!/bin/sh
. ./.remount/env
mkdir -p .codex
cat > .codex/config.toml <<CFG
model = "gpt-4o-mini"
model_provider = "remount"
approval_policy = "never"
sandbox_mode = "danger-full-access"

[model_providers.remount]
name = "OpenAI through the Remount broker"
base_url = "$REMOUNT_BROKER/d/api.openai.com/v1"
env_key = "OPENAI_API_KEY"
wire_api = "responses"
CFG
./node_modules/.bin/codex exec --skip-git-repo-check "$1"
```

Codex 0.152 rejects `wire_api = "chat"`; use `responses`. Install the harness
inside the workspace with `remount exec $WS -- npm install @openai/codex` after
allowing `registry.npmjs.org` on the node.

Model traffic goes through the `/d/<host>/` reverse-proxy path, where the
credential is substituted. In the legacy local profile, credential-free package
installs can use the automatically set `HTTPS_PROXY` as CONNECT only when the
node explicitly `--allow`s the registry. A binding never grants CONNECT.
Typed production policy should use the managed package connector or an explicit
`protocol:"connect"` rule whose deliberately weaker guarantees are acceptable.

## Running a harness that survives a move

Put the launcher in the workspace and let it rediscover the broker each time.
The same script then works on whichever machine holds the workspace.

```sh
#!/bin/sh
. ./.remount/env          # REMOUNT_BROKER for whichever node holds this workspace now
```

This was verified by running the Codex CLI on a Modal Linux container, moving
the workspace to an Apple Silicon Mac, and running the same launcher again. No
client-side reconfiguration was needed.

Create the workspace with `--exclude node_modules` when the harness installs
dependencies into it. A snapshot is the whole filesystem, so a 400 MB
dependency tree is uploaded and downloaded on every move. That turned a one
second move into 38 seconds. Reinstall on the far side instead.

## Moving a workspace

```sh
remount nodes
remount ws move $WS --node n_…            # this node, ignoring earlier labels
remount ws move $WS --label zone=gpu       # any node with that label
```

A move snapshots the filesystem, releases the workspace, re-queues it, and
waits for the new node to report ready. Files travel. Processes do not; restart
them. The generation increments, so an old grant is refused and clients refresh
automatically.

## Reading the event log

```sh
remount events --ws $WS
remount events --follow
```

A healthy migration reads `ws.snapshot`, `ws.released`, `ws.moved`,
`ws.claiming`, `ws.restored`, `ws.claimed`. A credential in use reads
`cred.used` with the host and status. Anything with `egress.denied` and
decision `leak_blocked` is an agent sending a secret reference somewhere it is
not bound, and deserves a look.

Events that matter operationally: `ws.lease_expired` means a node stopped
renewing, `node.offline` means a node lost its uplink, `egress.denied` with
reason `destination not in bindings or allow list` means the agent wanted a
host the node does not permit.

## A workspace stuck pending

Pending means no online node satisfies the workspace's constraints.

```sh
remount ws get $WS | grep -A8 '"requires"'
remount nodes
```

Eligibility is the intersection of `requires.backend`, `requires.cpu`,
`requires.mem_mib`, `requires.os`, `requires.arch`, `placement.node`, and
every label in `placement.allow`. Compare each against the node list. The usual
cause is a label left over from creation combined with a later `--node`, or a
backend no node advertises.

If the move command timed out, it now prints the requirements and placement it
was waiting on.

## Sleep and wake

```sh
remount ws sleep $WS --after 72h
remount ws sleep $WS --on github.pr.merged
curl -X POST $REMOUNT_SERVER/v1/events -H "Authorization: Bearer $REMOUNT_TOKEN" \
     -d '{"type":"github.pr.merged","payload":{"pr":42}}'
```

A sleeping workspace has no node and costs only storage. Waking restores from
the snapshot taken at sleep and waits for `claimed`.

## Incident containment

Use `fleet quarantine` when a tenant, principal, run, model, node, backend or
label set may be compromised. Always provide at least one selector or explicit
`--all`, and reuse a stable incident idempotency key.

```sh
remount fleet quarantine --run run_20260902 --action stop --idem incident-4821
remount fleet quarantine --node n_abc --action revoke_egress --idem node-4821
remount fleet quarantine --tenant acme --action destroy --deadline 10m \
  --idem incident-4821-destroy --json
remount fleet get fleet_abc --json
```

`destroy` checkpoints and durably commits the generation fence before the
node may delete source bytes. A partial operation remains durable and continues
reconciling unreachable targets; inspect each result instead of treating the
aggregate deadline as proof of containment.

## Diagnostics are three-valued

`remount doctor --deep` distinguishes healthy, unhealthy and unavailable. A
node it could not authenticate to or reach is not a pass. Use
`scripts/collect.sh --deep` and `scripts/explain.py`; an incomplete
collection has its own nonzero exit status. Preserve that distinction in
automation and incident notes.
