# Running agent harnesses on Remount

A harness needs three things from a computer. It must run a command, it must
read and write files, and it must reach a model API. Remount supplies all three
through one interface and adds what no single machine gives you: sessions that
survive a dropped connection, workspaces that move between machines, and egress
that never exposes a real credential to the agent.

This document covers three things. It shows the recipe we verified against
OpenAI's Codex CLI. It gives the general pattern for pointing any harness at the
broker. And it walks through writing your own loop against the Go SDK.

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
| OpenCode | `provider.<id>.options.baseURL` in `opencode.json` | provider `apiKey` or env | SDK dependent, `x-api-key` for Anthropic | Documented upstream, not tested here |
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
broker, and `NO_PROXY` set for loopback. Tools that honor those variables, which
includes npm, pip, curl and git, route through the broker without any
configuration.

Two request forms behave differently.

Plain HTTP through the proxy, where the request line carries an absolute URL,
is handled exactly like the `/d/` path. Headers are inspected and placeholders
are substituted for a matching binding.

HTTPS through the proxy uses CONNECT. The broker checks the destination against
the bindings and the allow list, and if permitted it opens a TCP tunnel and
copies bytes in both directions. It cannot see or rewrite the headers inside
that tunnel. Substituting a credential there would require the broker to
terminate TLS with a certificate authority installed in the workspace. Version
zero deliberately does not do that, because a per-workspace CA is the single
most valuable secret in the system and we would rather ship without it than ship
it carelessly.

So the division of labor is simple. Model keys go through the `/d/` reverse
proxy path where they can be substituted. Package installs and other
credential-free HTTPS go through CONNECT to allow-listed hosts. The Codex
install above used the second path for `registry.npmjs.org` and the first for
`api.openai.com`.

## Writing your own loop

The Go SDK lives in `internal/client`. It is importable from any package in this
module, and `examples/agentloop` is a complete program built on it. The
signatures below are the real ones.

Connect with a dialer to the relay. The client connects lazily and reconnects
by itself.

```go
c := client.New(client.Options{
	Dialer: transport.DialFunc(func(ctx context.Context) (transport.Conn, error) {
		return transport.DialWS(ctx, "ws://127.0.0.1:7443/v1/link", nil)
	}),
	Token:     os.Getenv("REMOUNT_TOKEN"),
	Principal: "a_my_agent",
})
defer c.Close()
```

Create a workspace and wait until a node has it ready. `WaitClaimed` returns
only once the node reports the filesystem is restored and serving.

```go
ws, err := c.CreateWorkspace(ctx, proto.WorkspaceSpec{
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
s, err := c.Exec(ctx, proto.SOpenReq{
	WS:             ws.ID,
	Kind:           proto.SessionExec,
	Program:        []string{"sh", "-c", "make test 2>&1"},
	IdempotencyKey: "build-42",
})
for ch := range s.Chunks() {
	switch ch.Stream {
	case proto.StreamStdout, proto.StreamStderr:
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
n, err := c.Edit(ctx, ws.ID, "src/main.go", []proto.FSEdit{
	{Old: "return nil", New: "return errors.New(\"todo\")"},
})
```

Snapshot, move and sleep are one call each. A move with new requirements
re-queues the workspace with its last snapshot; the SDK drops its cached grant
so the next call reaches the new node.

```go
snap, err := c.Snapshot(ctx, ws.ID, true)
ws, err = c.MoveWorkspace(ctx, ws.ID, &proto.Requires{CPU: 32}, nil)
ws, err = c.WaitClaimed(ctx, ws.ID)

timer, err := c.SleepWorkspace(ctx, proto.WSSleepReq{ID: ws.ID, OnEvent: "github.pr.merged"})
```

## What a harness gets for free

Lossless reconnect. Every output chunk carries a sequence number, the node
keeps a bounded ring plus a spill file per session, and a client resumes from
the last sequence it delivered. If the client's connection drops mid-command,
the SDK redials, reattaches, and the caller's channel simply continues. If the
gap is genuinely unrecoverable, the stream carries an explicit gap marker
rather than silently splicing.

Idempotent exec. An `IdempotencyKey` on `SOpenReq` makes a retried open return
the session that already exists instead of starting a second process. Input
carries a client sequence too, so a retried keystroke is dropped rather than
typed twice.

Sessions that outlive you. A process keeps running on the node after the
client dies. `remount attach WS SESSION --from N` picks it up later, from any
machine, with the output replayed.

An audit log you did not have to write. Every session open, exit, file write,
edit, removal, credential substitution and denied egress is an event on the
canonical log, attributed to the workspace's principal. `remount events --ws
$WS --follow` is the whole observability story for version zero.
