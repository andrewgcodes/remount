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

### Recipes

A recipe is a YAML file embedded in the binary (`internal/launch/recipes/`).
`--recipe-file PATH` loads your own with the same validator; adding a harness is
a YAML pull request, not Go.

| Recipe | Harness | Auth | Providers | `--sandbox` mapping | State that travels |
|---|---|---|---|---|---|
| `claude` | Claude Code | key or login | anthropic | `--permission-mode plan` / `acceptEdits` / `--dangerously-skip-permissions` | `.claude/`, `.claude.json` |
| `codex` | Codex CLI | key or login | openai, azure-openai, openrouter | `--sandbox read-only` / `workspace-write` / `danger-full-access` | `.codex/` |
| `opencode` | OpenCode | key or login | anthropic, openai, google, openrouter, groq, deepseek, xai, mistral | `permission.edit/bash/webfetch` in a generated config | `.local/share/opencode/` |
| `openhands` | OpenHands CLI | key | anthropic, openai, google, openrouter, groq, together, fireworks, deepseek, xai, mistral | harness default | `.openhands/` |
| `goose` | Goose | key | openai, anthropic, google, openrouter, groq | harness default | `.local/share/goose/`, `.config/goose/` |
| `gemini` | Gemini CLI | key or login | google | harness default | `.gemini/` |
| `aider` | aider | key | openai, anthropic, google, openrouter, groq, deepseek, xai, mistral | harness default | `.aider.*` in the workspace |
| `cline` | Cline CLI | key or login | anthropic, openai, openrouter, google, xai, deepseek, mistral, groq | harness default | `.cline/` |
| `custom` | anything after `--` | key or login | every preset | env only | none |

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

```sh
cd ~/proj                       # a checkout with a Claude Code conversation open
remount handoff --task "finish the refactor and open a PR"
```

`handoff` finds the harness whose state directory exists under your home
(`--recipe` when several do), packs the checkout and that state into one
artifact, and starts the `resume_command` in a new workspace. For a
`path_keyed` recipe — Claude Code, Codex, OpenCode, Gemini and Cline key their
per-project state on the absolute path of the working tree — the tree is
mounted at the same absolute path inside the workspace, which needs a node
with a mount namespace (`docker`). A `process`-only deployment leaves the
workspace `pending` and `handoff` says so; it never creates a symlink on the
node to fake the path. Goose, OpenHands and aider keep path-independent state
and run on either backend at `/work`.

`remount resume WS` picks the conversation back up: it attaches if a harness
session is still running, otherwise wakes the workspace if it sleeps and runs
the `resume_command` with `--task` (default "Continue where you left off.").
The recipe, bindings and model come from the workspace's labels, so the
command needs nothing but the id.

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
the install goes through the broker too.

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
| Claude Code | `ANTHROPIC_BASE_URL` | `ANTHROPIC_API_KEY` or `ANTHROPIC_AUTH_TOKEN` | `x-api-key` and, on newer builds, `Authorization: Bearer` | Documented upstream, not tested here |
| OpenCode | `provider.<id>.options.baseURL` in `opencode.json` | provider `apiKey` or env | SDK dependent, `x-api-key` for Anthropic | Verified via `remount run opencode` (docker lane, 1.18.x) |
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
ws, err = c.WaitClaimed(ctx, ws.ID)
```

Run a command and read its output as sequenced chunks. The channel closes after
the exit chunk, and `Exit` then holds the code.

```go
s, err := c.Exec(ctx, api.SessionOpenRequest{
	WS:             ws.ID,
	Kind:           api.SessionExec,
	Program:        []string{"sh", "-c", "make test 2>&1"},
	IdempotencyKey: "build-42",
})
for ch := range s.Chunks() {
	switch ch.Stream {
	case api.StreamStdout, api.StreamStderr:
		os.Stdout.Write(ch.Data)
	}
}
fmt.Println("exit", s.Exit().Code)
```

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
keeps a bounded ring plus a spill file per session, and a client resumes from
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
