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
| Run Chromium on a virtual X11 desktop or through VNC | [Browser and desktop workloads](#run-browser-and-virtual-desktop-workloads) |
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
./remount ws snapshot "$WS" --authoritative
./remount ws sleep "$WS" --after 1h
./remount ws wake "$WS"
./remount ws destroy "$WS"
```

Release, move, sleep, and destroy cycles are ordered by a durable
control-plane release epoch. Exact retries remain idempotent, while a delayed
request from an older aborted cycle cannot fence or stop work in the restored
workspace.

The complete walkthrough, including bases, repository seeding, event-triggered
wake, ACLs, and moves between two machines, is
[`tutorial.md`](tutorial.md).

## Run browser and virtual desktop workloads

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

## Documentation map and maintenance

This guide owns the supported **user journeys** and links to their detailed
contracts:

- [`tutorial.md`](tutorial.md): exact first-use and workspace walkthrough.
- [`harness-integration.md`](harness-integration.md): recipes, ACP/PTY behavior,
  provider bindings, handoff, queues, custom harnesses, and virtual desktops.
- [`credentials.md`](credentials.md): brokered credentials end to end -
  bindings, substitution locations, rotation, revocation, session principals,
  the refusal contract, and audit.
- [`operations.md`](operations.md): production deployment and fleet operation.
- [`api.md`](api.md): Agent HTTP API.
- [`spec/PROTOCOL.md`](../spec/PROTOCOL.md): normative protocol.

Command, recipe, provider, deployment, security, filesystem-transfer, or
lifecycle changes are incomplete until the corresponding user journey here
and its detailed document are updated. Run `make docs` after documentation
changes; `make lint` verifies that `llms.txt` and `llms-full.txt`, the
machine-readable documentation bundle used by coding agents, are current.
