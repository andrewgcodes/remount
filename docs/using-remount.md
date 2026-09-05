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
| Try the checked-in deployment on Modal | [Modal reference deployment](#deploy-the-modal-reference) |
| Operate a production control plane or node fleet | [`operations.md`](operations.md) |
| Integrate a custom harness or model provider | [`harness-integration.md`](harness-integration.md) |
| Call the Agent HTTP API | [`api.md`](api.md) and [`examples/agent-api`](../examples/agent-api/README.md) |

## Build and select a server

From a repository checkout:

```sh
make build
./remount version
```

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

For an explicit server-side binding:

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
- `--sandbox` is `read-only`, `workspace-write` (default), or `full`.
- `--approve` is `never` (default) or `on-request`.
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
not delete local files. It refuses to overwrite conflicting uncommitted local
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

### Preserve or move the whole filesystem

```sh
./remount ws snapshot WORKSPACE_ID
./remount ws snapshot WORKSPACE_ID --authoritative
./remount ws move WORKSPACE_ID --label zone=gpu
```

A normal snapshot is a live export and may observe concurrent writes. An
authoritative snapshot quiesces Remount-managed execution and commits the
failover checkpoint before returning. A move transfers filesystem state, not
arbitrary process memory or architecture-specific binaries.

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
./remount fs mv "$WS" notes.md archive/notes.md
./remount fs rm "$WS" archive/notes.md
```

Long-running session output is a sequenced log. Ctrl-C detaches unless the
caller explicitly requests termination:

```sh
./remount exec "$WS" -- sh -c 'for i in $(seq 1 60); do echo "$i"; sleep 1; done'
./remount attach "$WS" SESSION_ID --from 0
```

Workspace lifecycle:

```sh
./remount ws get "$WS"
./remount ws snapshot "$WS" --authoritative
./remount ws sleep "$WS" --after 1h
./remount ws wake "$WS"
./remount ws destroy "$WS"
```

The complete walkthrough, including bases, repository seeding, event-triggered
wake, ACLs, and moves between two machines, is
[`tutorial.md`](tutorial.md).

## Broker provider credentials

Provider keys belong in the server or node environment, never in a workspace
or committed bindings file. A bindings file refers to an environment variable:

```json
[
  {
    "id": "b_openai",
    "secret": "$OPENAI_API_KEY",
    "destinations": ["api.openai.com"],
    "placeholder": "sk-proj-REMOUNT-PLACEHOLDER-NOT-A-REAL-KEY",
    "ttl_sec": 900
  }
]
```

Start standalone with the binding:

```sh
export OPENAI_API_KEY=...
./remount standalone \
  --data ./data \
  --bindings ./bindings.json \
  --allow api.openai.com \
  --allow registry.npmjs.org
```

A recipe binding such as `--binding b_openai` gives the workspace a
shape-preserving placeholder and a broker URL. The node substitutes the real
credential only for an authorized destination and records `cred.used`.
Sending that placeholder to another host is blocked and recorded.

The process backend provides no host isolation. The built-in Docker backend is
container isolation with cooperative proxy egress. Neither may be described
as production multi-tenant isolation; Remount rejects security profiles a
backend cannot enforce. See [`security-profiles.md`](security-profiles.md) and
[`operations.md`](operations.md#choosing-a-backend).

## Deploy the Modal reference

`deploy/modal_app.py` is a reference deployment of
one control plane and one process-backend node in one Modal container. It is
not the same thing as the declarative Modal node-pool provisioner. Read the
checked-in [`deployment source`](../deploy/modal_app.py) before changing its
resource or secret names.

Prerequisites:

- Modal CLI authenticated to the intended workspace.
- `MODAL_ENVIRONMENT` set explicitly.
- A named Modal secret containing `REMOUNT_TOKEN`.
- Optionally, a second named secret containing `OPENAI_API_KEY` for model
  calls through `b_openai`.

Use unique names for disposable validation:

```sh
export MODAL_ENVIRONMENT=dev
export REMOUNT_MODAL_APP=remount-example
export REMOUNT_MODAL_VOLUME=remount-example-data
export REMOUNT_MODAL_SECRET=remount-example-control
export REMOUNT_MODAL_MODEL_SECRET=remount-example-openai
```

Create secrets without placing values in process arguments. The files below
must be mode 0600 and deleted immediately after import:

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
```

Do not delete a shared model secret. Modal singleton, persistence, readiness,
and backup constraints are detailed in
[`operations.md`](operations.md#deploying-the-control-plane-on-serverless-platforms).

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
not evidence of health.

## Documentation map and maintenance

This guide owns the supported **user journeys** and links to their detailed
contracts:

- [`tutorial.md`](tutorial.md): exact first-use and workspace walkthrough.
- [`harness-integration.md`](harness-integration.md): recipes, ACP/PTY behavior,
  provider bindings, handoff, queues, and custom harnesses.
- [`operations.md`](operations.md): production deployment and fleet operation.
- [`api.md`](api.md): Agent HTTP API.
- [`spec/PROTOCOL.md`](../spec/PROTOCOL.md): normative protocol.

Command, recipe, provider, deployment, security, filesystem-transfer, or
lifecycle changes are incomplete until the corresponding user journey here
and its detailed document are updated. Run `make docs` after documentation
changes; `make lint` verifies that `llms.txt` and `llms-full.txt`, the
machine-readable documentation bundle used by coding agents, are current.
