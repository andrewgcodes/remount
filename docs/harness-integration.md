# Running agent harnesses on Remount

A harness needs three things from a computer. It must run a command, it must
read and write files, and it must reach a model API. Remount supplies all three
through one interface and adds what no single machine gives you: sessions that
survive a dropped connection, workspaces that move between machines, and egress
that never exposes a real credential to the agent.

This document covers four things. It shows `remount run`, which launches a
harness from a recipe with one command. It shows the recipe we verified by
hand against OpenAI's Codex CLI. It gives the general pattern for pointing any
harness at the broker. And it walks through writing your own loop against the
Go SDK.

## `remount run`: one command from a checkout to a working agent

```sh
remount run opencode --dir . --binding b_openai --model openai/gpt-4o-mini \
  -- 'Create a file named GREETING.txt containing exactly the word hello.'
```

`run` creates a workspace from `--dir` (or `--base NAME`, or reuses `--ws WS`),
installs the harness if the image lacks it, writes a launcher under
`.remount/launch/`, and opens a session that runs it. The launcher sources
`.remount/env` when it starts, so the broker's address is discovered at run
time and is never baked into a file that moves with the workspace. Ctrl-C
detaches; `--detach` prints `WS SID` and the `attach` line and returns at
once. Every launch is bracketed by `run.started{recipe, task_hash, sandbox,
auth}` and `run.finished{exit}` in the event log; the task text is never
logged.

When reusing `--ws`, `--security` is a minimum profile assertion, not an
instruction to change the workspace. A weaker existing profile is rejected
with `denied` before installing the harness, writing the launcher, or opening
a session. A stronger existing profile remains unchanged. Omitting the flag
uses the local minimum; it never downgrades the existing workspace policy.

### Recipes

A recipe is a YAML file embedded in the binary (`internal/launch/recipes/`).
`--recipe-file PATH` loads your own with the same validator; adding a harness is
a YAML pull request, not Go.

| Recipe | Harness | Auth | Providers | `--sandbox` mapping | State that travels |
|---|---|---|---|---|---|
| `claude` | Claude Code | key or login | anthropic | PTY: `--permission-mode plan` / `acceptEdits` / `--dangerously-skip-permissions`; ACP: `plan` / `acceptEdits` / `bypassPermissions` session mode | `.claude/`, `.claude.json` |
| `codex` | Codex CLI | key or login | openai, azure-openai, openrouter | `--sandbox read-only` / `workspace-write` / `danger-full-access` | `.codex/` |
| `opencode` | OpenCode | key or login | anthropic, openai, google, openrouter, groq, deepseek, xai, mistral | `permission.edit/bash/webfetch` in a generated config | `.local/share/opencode/` |
| `openhands` | OpenHands CLI | key | anthropic, openai, google, openrouter, groq, together, fireworks, deepseek, xai, mistral | harness default | `.openhands/` |
| `goose` | Goose | key | openai, anthropic, google, openrouter, groq | harness default | `.local/share/goose/`, `.config/goose/` |
| `gemini` | Gemini CLI | key or login | google | harness default | `.gemini/` |
| `aider` | aider | key | openai, anthropic, google, openrouter, groq, deepseek, xai, mistral | harness default | `.aider.*` in the workspace |
| `cline` | Cline CLI | key or login | anthropic, openai, openrouter, google, xai, deepseek, mistral, groq | harness default | `.cline/` |
| `pi` | pi coding agent | key | anthropic, openai, openrouter, google, groq, xai, mistral | harness default | `.pi/` |
| `custom` | anything after `--` | key or login | every preset | env only | none |

OpenCode's checked-in fetch hosts are `registry.npmjs.org`, `models.dev`,
`models.opencode.ai`, and `opencode.ai`. The last host is required by default
model selection even when model requests use a separately bound provider.
Automatic local standalone derives these allowances from the recipe; when an
operator starts a server explicitly, each host must be supplied with
`--allow`.

#### Agents: structured or PTY transcript

An Agent (`remount agent create`, ADR 0043) runs the recipe's harness as an
[Agent Client Protocol](https://agentclientprotocol.com) server when the recipe
declares `acp.command`. That gives a **structured** transcript: message chunks,
tool calls, plans and permission requests as typed frames the API can filter
and a client can render. A recipe without `acp` still makes an Agent, but a
degraded one: the harness runs on a pseudo-terminal, the transcript is the
terminal log, and a follow-up message is typed at the prompt through the
recipe's `prompt_template`. The table says which you get; the API reports it as
`mode: acp | pty` so a client never has to guess.

For `agent create --ws`, the workspace binding ID and the recipe's provider
preset are separate facts. Repeat `--binding ID:PRESET` on the Agent command.
The client persists that non-secret declaration in `AgentSpec.binding_specs`;
the control plane verifies that each ID is attached to the target workspace,
and the node uses it to construct the recipe environment. Agents created by
older clients fall back to the launch metadata in `remount.bindings`.

| Recipe | Transcript | `acp.command` | `load_session` | Adapter | `ui` |
|---|---|---|---|---|---|
| `opencode` | structured | `opencode acp` | yes | native | `opencode web` on 4096 |
| `goose` | structured | `goose acp --with-builtin developer` | yes, with history | native | — |
| `openhands` | structured | `openhands acp` | — (`--resume ID` is the harness-native path) | native | — |
| `gemini` | structured | `gemini --acp` | — | native | — |
| `cline` | structured | `cline --acp` | — | native | — |
| `pi` | structured | `npx -y pi-acp` | — | `pi-acp` bridges to `pi --mode rpc`, which can steer mid-turn | — |
| `claude` | structured | `npx -y @agentclientprotocol/claude-agent-acp` | yes | official adapter; subscription login stays workspace-resident (ADR 0039) | — |
| `codex` | structured | `npx -y @agentclientprotocol/codex-acp` | — | official adapter | — |
| `aider` | **pty** | — | — | none; `prompt_template` types the message | — |
| `custom` | **pty** unless `--acp-cmd` | yours | — | — | — |

`load_session` is the recipe's claim. The node checks it against what the
agent advertises in `initialize` and records `agent.capabilities` either way;
when an agent cannot reopen a session, a woken Agent starts a fresh one and the
event says so. `session_id_from` (opencode: newest
`.local/share/opencode/storage/session/*/ses_*.json`, key `id`) lets a handoff
continue the laptop conversation on the node with `session/load`.

An ACP recipe may map each Remount sandbox level through
`acp.sandbox_modes`. The node applies that mode with `session/set_mode` after
new, load, or resume and before the first prompt. A configured mode that the
adapter rejects fails the run rather than silently weakening the requested
sandbox behavior.

Every backend sets `HOME` to the workspace root, so the state column is where
the harness's `~/.something` actually lands: inside the workspace, in every
snapshot, and back on your disk after `remount pull`. `--exclude` drops what
you would rather not carry.

Anything built on an SDK that honors `<PROVIDER>_API_KEY` and
`<PROVIDER>_BASE_URL` needs no recipe: `remount run custom --binding b_openai
-- python agent.py` gets the placeholder key and the broker URL in its
environment.

`--sandbox` names what the harness may write; it also shapes egress. Under
`--security local`, `read-only` and `workspace-write` (the default) keep the
node's own allow list and `full` opens it. Under `isolated` and `multi_tenant`
the policy is deny-by-default, admits the bound providers over HTTPS and the
recipe's declared `hosts` for `GET`/`HEAD` (so the harness can install itself
even when read-only), and `full` adds `CONNECT` to those hosts. Those two profiles need
a node whose backend enforces egress; the `process` and `docker` backends only
cooperate through `HTTPS_PROXY`, so they serve `local`.

### Hand-off, resume and queues

The recipe's `resume_command`, `state_dirs` and `path_keyed` fields drive the
unattended workflow (ADR 0041).

Configure `REMOUNT_SERVER` and `REMOUNT_TOKEN` for your remote deployment first,
and create the matching provider binding there. Without a remote server,
autostart may create a local standalone instead; `handoff` does not provision a
cloud machine. Save the conversation and stop local work before copying it.

```sh
cd ~/proj                       # a checkout with a Claude Code conversation open
remount handoff --recipe claude --binding b_anthropic \
  --task "finish the refactor and open a PR"
remount handoff --recipe codex --binding b_openai --task "continue the refactor"
```

These are alternative examples for the harness you used locally, not two steps
of one transfer. GitHub tools, authorization and network access must be
configured separately if the task needs to push or open a PR.

`handoff` detects a recipe from state under your home (`--recipe` when several
match, `--home` for an isolated state directory). Claude Code and Codex handoffs
require a brokered provider binding: they do not transfer Keychain credentials,
subscription logins, provider configuration or unrelated conversations. A local
autostarted server can select a matching binding from exported provider keys;
remote bindings must be named explicitly. The CLI does not automatically read
`.env` files.

For Claude/Codex, the newest valid saved transcript whose metadata matches the
canonical checkout is selected. Its UUID and transcript path are recorded in
workspace labels, and the harness resumes that exact UUID rather than using a
provider-dependent “last conversation” lookup. If no valid matching transcript
exists, handoff fails rather than silently starting a fresh conversation. Claude's
selected-session subagent transcripts and tool-result text files travel too.
State collection is bounded to 64 MiB per file, 256 MiB and 4096 files per
conversation, and 100,000 discovery entries.

Checkout copies of `.claude`, `.claude.json`, `.codex`, `.env` and `.env.*` are
excluded; only selected conversation files are added back. This is not a content
secret scanner: secrets previously pasted into conversation text, ordinary
source files or Git objects can still travel. Review what the agent has seen
before handing it to another machine. Other recipes retain their declared
`state_dirs` transfer behavior; the scoped Claude/Codex guarantees do not apply
to them.

The checkout is copied, not moved, and the local agent is not stopped by this
command. There is no live process-memory migration or continuous file sync.
Once the remote session is running, it no longer depends on the local client.
By default the CLI detaches and prints IDs; use `--attach` to follow output.
Opening a session is not proof that the harness finished the requested task.

For a `path_keyed` recipe, the tree appears at the same absolute path inside the
remote workspace. That needs a namespaced backend such as Docker. Claude/Codex
handoff checks for an online compatible backend before uploading; a process-only
deployment is rejected without creating a workspace. No host symlink is created.
The remote runtime must also support the harness's own sandbox: an ordinary
Docker profile can block Codex's nested namespace creation. Do not mistake a
successful API call for working file/command tools or disable the harness sandbox
to hide that incompatibility.

`remount resume WS` attaches to a live harness session; otherwise it wakes the
workspace and invokes the resume command with `--task` (default "Continue where
you left off."). The recipe, bindings, model and selected conversation come from
the workspace labels. For a scoped handoff, the recorded transcript is checked
before opening a new session. Legacy workspaces without a selected UUID keep
their recipe's latest-conversation behavior.

`remount run RECIPE --queue tasks.txt` runs one task per line in one
workspace, in order, and records progress as a control-plane `Queue`
resource rather than in the tree. Between tasks it checkpoints, or with
`--sleep-after 30m` / `--sleep-until 02:00` puts the workspace to sleep on a
durable timer and waits for it to come back — on whichever node claims it. A
non-zero exit stops the queue with the cursor on that task;
`remount run RECIPE --queue-continue QUEUE` retries it and carries on, from a
different machine if need be. Events record `queue.advanced{index, exit}`,
never the task text. `remount ws sleep WS --on EVENT` is the same mechanism
for waking on an external event (a merged PR, a Slack reply) and is the
intended hook for event-driven queues.

### Provider bindings are presets

A binding is a secret the node holds; a preset says how a harness consumes
it. `remount binding preset ls` lists them: `anthropic`, `openai`, `google`,
`openrouter`, `bedrock`, `vertex`, `azure-openai`, `mistral`, `groq`,
`together`, `fireworks`, `deepseek`, `xai`. `--binding b_openai` picks the
preset by id suffix; `--binding b_team:openai` names it; `--binding
b_bedrock:bedrock?host=us-east-1` fills a region. The workspace receives
`OPENAI_API_KEY=ref:b_openai` and
`OPENAI_BASE_URL=${REMOUNT_BROKER}/d/api.openai.com/v1`; the broker
substitutes the real key on the way out and records `cred.used`.

### Two kinds of auth, stated plainly

Remount brokers **API keys**. Claude Max, ChatGPT/Codex sign-in, Gemini's
Google sign-in and Copilot are **harness-native logins**: the harness runs its
own OAuth flow inside the workspace and keeps the token in its state directory.
That token travels with the workspace, is visible to anything running in it,
and is not secret-blind. When a recipe that supports a login is launched
without a provider binding, `run` marks the session `auth: workspace_resident`
and the node records `auth.workspace_resident{recipe}`. `--security isolated`
and `multi_tenant` refuse such a launch. Remount never proxies or rewrites a
subscription token; their terms restrict third-party use and the broker is not
the place to argue with that.

## Browser and virtual desktop workloads

For a browser, reach for the built-in computer session first: `remount
computer` and the SDK `Computer` handles drive screenshots, input, navigation
and downloads as protocol operations, against the reference image in
[`images/browser`](../images/browser/Dockerfile). The user-facing walkthrough,
including the coordinate system, the typed failures, the per-host egress
ceiling and the profile-persistence limit, is in
[using-remount.md](using-remount.md#run-browser-and-virtual-desktop-workloads);
the design is [ADR 0088](adr/0088-computer-sessions-over-port-substrate.md).
The recipe below is what you compose when you need a whole desktop - X11, a
window manager, a VNC client - rather than a browser.

Remount supervises filesystem and process sessions; it does not implement a
display server or VNC protocol. A node can nevertheless host a desktop stack,
and the session log remains available when the initiating client disconnects.
The following composition was exercised on a Modal-hosted Linux process node:

| Layer | Exercised implementation | Responsibility |
|---|---|---|
| display | `Xvfb :99 -screen 0 1280x720x24 -nolisten tcp -ac` | virtual X11 framebuffer |
| window manager | Openbox | window placement and focus |
| browser | Chromium with `DISPLAY=:99` | rendered application |
| direct input | xdotool/XTEST | X11 pointer and keyboard events |
| remote display | x11vnc on `127.0.0.1:5900` | RFB framebuffer and input |
| client transport | `remount port` | authenticated path to workspace loopback |

For a Debian-derived node image, the exercised package set was
`xvfb openbox chromium x11vnc xdotool scrot x11-utils fonts-liberation`.
Package names vary by distribution. Install them while building the node image:
an isolated workspace may intentionally lack package-manager egress, and a
provider credential binding does not authorize arbitrary package traffic.

Create the workspace with reproducible dependencies and the disposable browser
profile excluded from every snapshot:

```sh
WS=$(remount ws create --name desktop \
  --exclude node_modules \
  --exclude .chromium-profile)
```

An application-specific supervisor can use this process shape:

```sh
#!/bin/sh
set -eu

cleanup() {
  for pid in "${browser_pid:-}" "${vnc_pid:-}" "${app_pid:-}" \
    "${wm_pid:-}" "${xvfb_pid:-}"; do
    [ -z "$pid" ] || kill "$pid" 2>/dev/null || true
  done
}
trap cleanup EXIT INT TERM

Xvfb :99 -screen 0 1280x720x24 -nolisten tcp -ac &
xvfb_pid=$!
export DISPLAY=:99

i=0
until xdpyinfo -display :99 >/dev/null 2>&1; do
  i=$((i + 1))
  [ "$i" -lt 50 ] || exit 1
  sleep 0.1
done

openbox &
wm_pid=$!

# Start the application on workspace loopback before the browser.
./start-app.sh &
app_pid=$!
i=0
until curl -fsS http://127.0.0.1:4173/ >/dev/null; do
  i=$((i + 1))
  [ "$i" -lt 50 ] || exit 1
  sleep 0.1
done

x11vnc -display :99 -rfbport 5900 -localhost -nopw -forever -shared \
  -noxdamage -quiet &
vnc_pid=$!

chromium --no-sandbox --disable-dev-shm-usage --no-first-run \
  --user-data-dir="$PWD/.chromium-profile" \
  --window-size=1280,720 --start-maximized \
  --app=http://127.0.0.1:4173 &
browser_pid=$!

wait "$browser_pid"
```

The exact Chromium sandbox flags depend on the backend. `--no-sandbox` was
required by the exercised cooperative process container and weakens the browser
boundary; do not copy it to a backend where Chromium sandboxing works. Keep the
supervisor in the foreground so Remount owns a live sequenced session instead
of a shell that exits while untracked children remain.

Run the supervisor through Remount, then use a separate client for the VNC
tunnel:

```sh
remount exec "$WS" -- ./desktop-run.sh
remount port "$WS" 5900 --local 127.0.0.1:5900
```

`-localhost -nopw` is acceptable only for a disposable listener when workspace
loopback is not otherwise exposed. On an authenticated HTTPS deployment,
`remount port` carries it through Remount's authenticated TLS connection; it
inherits a plaintext connection when `REMOUNT_SERVER` is `http://`. Never
publish the listener directly. `remount port` is a live client process; it does
not provide an always-on web or phone UI.

Computer-use harnesses may capture screenshots from the VNC framebuffer and
inject RFB pointer/key events, or use xdotool for direct XTEST input. A
screenshot-driven model loop must return the new framebuffer after each action
group and correlate it with the model's original call identifier. Keep the loop
bounded, assert an application-side result rather than trusting the screenshot
alone, and reset server-side result state before every run so stale success
cannot produce a false positive.

Model credentials follow the normal binding rules. The controller can call a
provider through `${REMOUNT_BROKER}/d/HOST/...` with a placeholder credential;
the reusable provider key must not be written to the browser profile, workspace,
screenshot metadata, or test report.

The persistence boundary is deliberate:

- Killing or disconnecting the initiating CLI does not kill the remote exec
  session. A fresh client can `remount attach WS SESSION --from 0` and replay
  its sequenced output.
- Snapshot, move, sleep, and node failover preserve filesystem state, not X11,
  VNC, browser, or application process memory. Restart the supervisor after the
  workspace is materialized.
- An excluded browser profile is intentionally absent after materialization.
  Persistent application state belongs in ordinary portable files, not
  Chromium singleton locks or host-specific symlinks.
- `remount pull` snapshots the whole workspace. Chromium profiles can contain
  unsafe singleton symlinks, and artifact validation correctly rejects them.
  Exclude the profile at workspace creation or stop Chromium and remove
  disposable profile state before pulling; never relax symlink validation.

Verify a desktop run with evidence from both sides of the boundary:

1. a framebuffer screenshot showing the expected rendered result;
2. an application-side file or API result matching the screenshot;
3. session replay from a fresh client after disconnect;
4. direct `fs read` and `pull` artifacts compared byte-for-byte;
5. `inspect`, `events`, and `doctor --deep` results;
6. destruction of the workspace and disposable provider resources.

This composition proves that Remount can host and reconnect to a virtual
desktop. It does not make the process backend a production multi-tenant
boundary, turn plaintext VNC into a secure public service, or supply a mobile
client.

## Verified: OpenAI Codex CLI

We ran Codex CLI 0.152.1 inside a Remount workspace on macOS with the
`process` backend. The workspace held only a placeholder for the API key. Codex
made two live calls to the Responses API, executed a shell command, and wrote
the file it was asked to write.

### The server side

Define a binding once, in the bindings file the server loads. The secret can be
given as an environment reference so the file stays clean.

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

Start the server with the binding and with the package registry allowed, since
the install goes through the broker too. (`remount run` does all of this for
you when nothing is listening locally: it starts a standalone whose bindings
reference the provider keys in your environment by name; see the tutorial.)

```sh
export OPENAI_API_KEY=...
remount standalone --data ./data --bindings ./bindings.json \
  --allow api.openai.com --allow registry.npmjs.org
export REMOUNT_SERVER=http://127.0.0.1:7443
```

### The workspace

Create the workspace with the binding attached. The two environment variables
resolve at session start: `ref:b_openai` becomes the placeholder, and
`${REMOUNT_BROKER}` becomes the loopback address of this workspace's broker.

```sh
WS=$(remount ws create --name harness \
  --binding b_openai \
  --env OPENAI_API_KEY=ref:b_openai \
  --env OPENAI_BASE_URL='${REMOUNT_BROKER}/d/api.openai.com/v1')
```

### Install and configure Codex

Install it inside the workspace. `HOME` is the workspace root, so the config
lives with the workspace and moves with it.

```sh
remount exec $WS -- sh -c '
npm init -y >/dev/null
npm install @openai/codex --no-audit --no-fund
mkdir -p .codex
cat > .codex/config.toml <<CFG
model = "gpt-4o-mini"
model_provider = "remount"
approval_policy = "never"
sandbox_mode = "danger-full-access"

[model_providers.remount]
name = "OpenAI through the Remount broker"
base_url = "${REMOUNT_BROKER}/d/api.openai.com/v1"
env_key = "OPENAI_API_KEY"
wire_api = "responses"
CFG'
```

Two details matter. `base_url` must be the broker's reverse-proxy prefix for
the destination host, which is the path under `/d/`. And `wire_api` must be
`"responses"`; Codex 0.152 and later reject `wire_api = "chat"` at startup with
a pointer to their discussion thread.

### Run it

```sh
remount exec $WS -- sh -c './node_modules/.bin/codex exec --skip-git-repo-check \
  "Create a file named PROOF.md whose entire contents are the single line: codex ran inside a remount workspace. Then stop."'
```

This is the shape of the output we observed.

```
provider: remount
approval: never
sandbox: danger-full-access
session id: 01a063cd-9ad7-7230-a644-5fc654b1a6ef
--------
user
Create a file named PROOF.md whose entire contents are ...
exec
/bin/zsh -lc "echo 'codex ran inside a remount workspace.' > PROOF.md" in .../ws/ws_06g67k0y8jsrr48gp9xgevdn04
 succeeded in 0ms:
codex
I've created the file `PROOF.md` with the content:
codex ran inside a remount workspace.
tokens used
7,933
```

The file is there afterwards.

```sh
$ remount fs read $WS PROOF.md
codex ran inside a remount workspace.
```

### What the audit log recorded

Every model call was a substitution event on the canonical log. The workspace
sent the placeholder; the broker swapped in the real key for `api.openai.com`
only.

```
25 13:26:54.738 cred.used  ws_06g67k0y…  {"binding":"b_openai","decision":"substituted","host":"api.openai.com","method":"POST","path":"/v1/responses","status":200}
26 13:26:57.957 cred.used  ws_06g67k0y…  {"binding":"b_openai","decision":"substituted","host":"api.openai.com","method":"POST","path":"/v1/responses","status":200}
```

### What the workspace held

The workspace's `OPENAI_API_KEY` contained only the placeholder string. After
the run we scanned the entire workspace tree, 103 files including
`node_modules`, for the real key and found zero occurrences. To prove the scan
itself worked, we planted the real key in a canary file, scanned again, found
exactly one match, and deleted the canary. The only process holding the secret
was the control plane, which is where it belongs.

That paragraph records the historical test, not a safe procedure to repeat.
For new tests, validate the scanner with a synthetic non-credential canary in
a disposable fixture. Never plant a real provider key in a workspace or log.

## The general pattern for any harness

Every harness has a variable for where the model lives and a variable for the
key. Point the first at the broker and set the second to a binding reference.

The broker exposes each permitted destination as an HTTP path. A request to
`${REMOUNT_BROKER}/d/api.openai.com/v1/responses` is re-originated over TLS to
`https://api.openai.com/v1/responses` with the placeholder replaced. A request
to any host that is not bound and not allow-listed is refused with a 403 before
a byte leaves the machine.

| Harness | Base URL setting | Key setting | Auth header it sends | Status |
|---|---|---|---|---|
| Codex CLI | `base_url` in `.codex/config.toml` | `env_key` names an env var | `Authorization: Bearer` | Verified here, 0.152.1 |
| Claude Code | `ANTHROPIC_BASE_URL` | `ANTHROPIC_API_KEY` or `ANTHROPIC_AUTH_TOKEN` | `x-api-key` and, on newer builds, `Authorization: Bearer` | 2.1.260 exercised on a disposable Modal VM; [dated evidence](engineering/verification-2026-09.md#2026-09-05--pr-30-live-functional-regression-verification) |
| OpenCode | `provider.<id>.options.baseURL` in `opencode.json` | provider `apiKey` or env | SDK dependent, `x-api-key` for Anthropic | 1.18.29 exercised on Docker with OpenAI; [dated evidence](engineering/verification-2026-09.md#2026-09-05--pr-30-live-functional-regression-verification) |
| aider | `OPENAI_API_BASE` | `OPENAI_API_KEY` | `Authorization: Bearer` | Documented upstream, not tested here |

The broker matches on the placeholder string wherever it appears in a header,
including inside a Basic credential, so it does not matter which header a
harness chooses. What matters is that the destination host matches the binding.
A binding for `api.openai.com` will never substitute into a request bound for
`api.anthropic.com`, and that attempt is logged as `leak_blocked`.

A shape-preserving placeholder helps with harnesses that validate key format
before sending. The Codex recipe above uses `sk-proj-…`, which passes any
prefix check an OpenAI SDK performs.

## HTTP_PROXY mode

The broker is also an ordinary HTTP forward proxy. Every session in a workspace
starts with `HTTP_PROXY`, `HTTPS_PROXY` and their lowercase twins set to the
broker, and `NO_PROXY` set for loopback plus the broker's own advertised host
(`host.docker.internal` for Docker workspaces). Tools that honor those
variables, which includes npm, pip, curl and git, route through the broker
without any configuration, and requests to `${REMOUNT_BROKER}` itself go
direct rather than through the proxy. A client that ignores `NO_PROXY` and
forwards a capability URL through the proxy anyway is still served as a direct
request; the broker never re-originates a request to itself.

Two request forms behave differently.

Plain HTTP through the proxy, where the request line carries an absolute URL,
is handled exactly like the `/d/` path. Headers are inspected and placeholders
are substituted for a matching binding.

HTTPS through the proxy uses CONNECT. A binding never grants tunnel authority:
legacy local mode requires the node's `--allow`, while typed mode requires an
explicit `protocol:"connect"` rule. If permitted, the broker opens a TCP
tunnel and copies bytes in both directions. It cannot see or rewrite the
headers inside that tunnel. Substituting a credential there would require the
broker to terminate TLS with a certificate authority installed in the
workspace. Protocol v1 deliberately does not do that, because a per-workspace
CA would be one of the most valuable secrets in the system.

So the division of labor is explicit. Model keys go through the `/d/` reverse
proxy path where they can be substituted. The verified legacy-local Codex
install above used an allow-listed CONNECT tunnel for
`registry.npmjs.org` and the reverse proxy for `api.openai.com`.

For a hostile or production-oriented workspace, use a typed
`connector:"package"` rule for immutable registry GET/HEAD requests and call
`${REMOUNT_PACKAGE_CONNECTOR}/<registry-host>/<path>`. The managed connector
rechecks workspace and generation policy, supports an expected SHA-256 digest,
and does not expose cache paths or hit state. It is intentionally not a general
CONNECT proxy; a package manager needs an integration that can use its explicit
read endpoint. The built-in process and Docker backends remain
`cooperative_proxy`, so they cannot satisfy `isolated` or `multi_tenant`
profiles.

## Repositories: the git connector and the `/d/` stopgap

Two ways exist to reach a repository from a workspace.

The **stopgap** is the generic reverse proxy. With a binding whose hosts cover
`github.com` and a placeholder in the URL's userinfo, this routes every
`https://github.com/` URL through the broker and works because smart HTTP is
plain GET/POST:

```sh
. .remount/env
git config --global url."$REMOUNT_BROKER/d/github.com/".insteadOf https://github.com/
git config --global http."$REMOUNT_BROKER/d/github.com/".extraHeader \
  "Authorization: Basic $(printf 'x-access-token:%s' "$REMOUNT_REF_GH" | base64 -w0)"
```

It authorizes a *host*: anything the token can see on github.com — every
repository, the REST API, release assets, LFS — is reachable. Use it only for a
`local` profile you trust.

The **typed connector** (`connector: "git"`, ADR 0054) authorizes a
*repository*. Declare it on the workspace and the node does the rest:

```sh
remount ws create --repo github.com/acme/app@main --repo-depth 1 --binding b_gh
remount run codex --repo github.com/acme/app -- "fix the flaky test"
```

The node clones through its own broker **before** the workspace is `claimed`,
so the first `ls` shows the checkout and `repo.cloned{commit}` names the SHA
that was served. The tree's git configuration arrives as `GIT_CONFIG_*` in
`.remount/env` — insteadOf routing to `$REMOUNT_GIT_CONNECTOR/<host>/`,
credential helpers disabled, prompts off — so `git fetch` and `git push` inside
a session go through the same connector with the same placeholder. Only the
declared repository is reachable (or the `repos:` patterns of a typed rule,
with `push: true` required for `git-receive-pack`); `/api/`, raw files, LFS
and every sibling repository are denied before any credential is substituted.
A clone that fails never becomes an empty workspace: the tree is destroyed,
the claim released, and the retry backed off.

## Writing your own loop

The supported Go SDK is `remount.dev/remount/client`; its stable resource model
and error codes are in `remount.dev/remount/api`. Consumers never import a
Remount `internal/` package. `examples/agentloop` is a complete program and
`make public-api` compiles this surface from a separate module.

Give the client the HTTP(S) server URL. It constructs the WebSocket link,
connects lazily and reconnects by itself.

```go
import (
	"remount.dev/remount/api"
	"remount.dev/remount/client"
)

c, err := client.New(client.Options{
	Server: "https://remount.example",
	Token:  os.Getenv("REMOUNT_TOKEN"),
})
if err != nil {
	return err
}
defer c.Close()
```

Create a workspace and wait until a node has it ready. `WaitClaimed` returns
only once the node reports the filesystem is restored and serving.

```go
ws, err := c.CreateWorkspace(ctx, api.WorkspaceSpec{
	Name:     "my-agent",
	Bindings: []string{"b_openai"},
	Env: map[string]string{
		"OPENAI_API_KEY":  "ref:b_openai",
		"OPENAI_BASE_URL": "${REMOUNT_BROKER}/d/api.openai.com/v1",
	},
})
if err != nil {
	return err
}
ws, err = c.WaitClaimed(ctx, ws.ID)
if err != nil {
	return err
}
```

Run a command and copy its sequenced output with the gap-aware helper. Check
errors before using the returned exit information; a disconnected or evicted
stream is not successful completion.

```go
s, err := c.Exec(ctx, api.SessionOpenRequest{
	WS:             ws.ID,
	Kind:           api.SessionExec,
	Program:        []string{"sh", "-c", "make test 2>&1"},
	IdempotencyKey: "build-42",
})
if err != nil {
	return err
}
exit, err := client.Copy(ctx, s, os.Stdout, os.Stderr)
if err != nil {
	return err
}
fmt.Println("exit", exit.Code)
```

`client.Copy` and `c.Run` return `api.CodeEvicted` for incomplete output. Raw
`s.Chunks()` consumers must handle gap chunks and check `s.Err()` themselves.

For the common case there is a one-call form.

```go
stdout, stderr, exit, err := c.Run(ctx, ws.ID, "sh", "-c", "go test ./...")
```

Files work by path relative to the workspace root. Writes create parents and
are atomic. Search is a server-side regexp with an optional filename glob. Edit
applies every replacement or none.

```go
err = c.WriteFile(ctx, ws.ID, "src/main.go", src, 0o644)
data, err := c.ReadFile(ctx, ws.ID, "src/main.go")
res, err := c.Search(ctx, ws.ID, "/", `TODO\(`, "*.go", 200)
n, err := c.Edit(ctx, ws.ID, "src/main.go", []api.FileEdit{
	{Old: "return nil", New: "return errors.New(\"todo\")"},
})
```

Snapshot, move and sleep are one call each. A move with new requirements
re-queues the workspace with its last snapshot; the SDK drops its cached grant
so the next call reaches the new node.

```go
live, err := c.Snapshot(ctx, ws.ID, true) // labeled live; not failover state
checkpoint, err := c.Checkpoint(ctx, ws.ID) // quiesced and authoritative
ws, err = c.MoveWorkspace(ctx, ws.ID, &api.Requires{CPU: 32}, nil)
ws, err = c.WaitClaimed(ctx, ws.ID)

timer, err := c.SleepWorkspace(ctx, api.SleepRequest{ID: ws.ID, OnEvent: "github.pr.merged"})
```

## What a harness gets for free

Truthful bounded replay. Every output chunk carries a sequence number, the node
keeps bounded memory, disk spill and optional blob-backed ranges, and a client resumes from
the last sequence it delivered. If the client's connection drops mid-command,
the SDK redials, reattaches, and the caller's channel simply continues. If the
gap is genuinely unrecoverable, the stream carries an explicit gap marker
rather than silently splicing.

Idempotent exec. An `IdempotencyKey` on `api.SessionOpenRequest` makes a retried open return
the session that already exists instead of starting a second process. Input
carries a client sequence too, so a retried keystroke is dropped rather than
typed twice.

Sessions that outlive you. A process keeps running on the node after the
client dies. `remount attach WS SESSION --from N` picks it up later, from any
machine, with the output replayed.

An audit log you did not have to write. Every session open, exit, file write,
edit, removal, credential substitution and denied egress is an event on the
canonical log, attributed to the workspace's principal. `remount events --ws
$WS --follow` is the whole observability story for protocol v1.
