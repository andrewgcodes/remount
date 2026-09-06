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

### Hold a workspace for background work

`ws sleep` means "sleep now". The opposite — "keep this workspace claimed
because work is still running, then sleep it if nobody extends the deadline" —
is `ws lease`, and the reason it is a Remount operation rather than a timer in
your own process is that your process is not durable. A deploy, a scale-down,
an OOM kill or a partition takes an `asyncio.sleep` or a `setTimeout` with it,
and the workspace then runs until somebody notices the bill. The deadline has
to live where the workspace lives.

```sh
./remount ws lease "$WS" --max 20m --min 30s --reason background_job
./remount ws lease renew "$WS" "$LEASE" --extend 10m
./remount ws lease get "$WS"
./remount ws lease cancel "$WS" "$LEASE"
```

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
  --allow registry.npmjs.org \
  --allow models.dev \
  --allow models.opencode.ai \
  --allow opencode.ai
```

A recipe binding such as `--binding b_openai` gives the workspace a
shape-preserving placeholder and a broker URL. The node substitutes the real
credential only for an authorized destination and records `cred.used`.
Sending that placeholder to another host is blocked and recorded.
The additional hosts above are the OpenCode recipe's install and model-catalog
destinations. OpenCode's default model selection reaches `opencode.ai`; an
explicit model does not remove the need to allow the other checked-in recipe
hosts. Automatic local standalone derives these allowances from the recipe,
but an explicitly started server must receive them through `--allow`.

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

```go
ws, err := c.CreateWorkspace(ctx, proto.WorkspaceSpec{
    Requires: proto.Requires{Profile: "multi-tenant-isolated"},
})
```

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

## Documentation map and maintenance

This guide owns the supported **user journeys** and links to their detailed
contracts:

- [`tutorial.md`](tutorial.md): exact first-use and workspace walkthrough.
- [`harness-integration.md`](harness-integration.md): recipes, ACP/PTY behavior,
  provider bindings, handoff, queues, custom harnesses, and virtual desktops.
- [`operations.md`](operations.md): production deployment and fleet operation.
- [`api.md`](api.md): Agent HTTP API.
- [`spec/PROTOCOL.md`](../spec/PROTOCOL.md): normative protocol.

Command, recipe, provider, deployment, security, filesystem-transfer, or
lifecycle changes are incomplete until the corresponding user journey here
and its detailed document are updated. Run `make docs` after documentation
changes; `make lint` verifies that `llms.txt` and `llms-full.txt`, the
machine-readable documentation bundle used by coding agents, are current.
