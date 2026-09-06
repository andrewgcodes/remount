# Examples

| Example | What it shows | Needs a credential |
|---|---|---|
| [minimal-shell](minimal-shell/README.md) | connect, create a workspace, stream a command, write and read a file, destroy | no |
| [artifact-transfer](artifact-transfer/README.md) | upload a directory, snapshot, download and digest-check the artifact, restore into a second workspace, verify byte for byte | no |
| [agentloop](#agentloop) | a whole coding-agent loop whose model calls are brokered, so no key enters the workspace | a model key on the server |
| [diy-devin](diy-devin/README.md) | reference clients for the Agent HTTP API in Python, TypeScript, a web page and a GitHub Action | depends on the harness |

Every example expects a running server. The quickest one is standalone mode,
which needs no token.

The first two examples need nothing else — no key, no binding, no outbound
network:

```sh
go run ./cmd/remount standalone --data ./data
export REMOUNT_SERVER=http://127.0.0.1:7443
go run ./examples/minimal-shell
go run ./examples/artifact-transfer
```

Both are executed end to end against an in-process server by
`go test ./integration/examples/`, so an example that stops working fails CI
rather than a reader's laptop.

The examples that call a model need a binding, so that the key lives on the
server and never inside the workspace:

```sh
export OPENAI_API_KEY=...
go run ./cmd/remount standalone --data ./data --bindings ./examples/bindings.example.json \
  --allow api.openai.com
export REMOUNT_SERVER=http://127.0.0.1:7443
```

`examples/bindings.example.json` defines one binding, `b_openai`, whose secret
is read from `$OPENAI_API_KEY` on the server. Workspaces reference it as
`ref:b_openai` and never see the value.

## minimal-shell

The smallest complete program: one workspace, one streamed command, one file
written and read back, then destroyed — using only `remount.dev/remount/api`
and `remount.dev/remount/client`. Start here. See
[minimal-shell/](minimal-shell/README.md).

## artifact-transfer

A directory moved between two workspaces through the content-addressed
artifact store, verified byte for byte, with the downloaded object checked
against the digest that named it. See
[artifact-transfer/](artifact-transfer/README.md).

## diy-devin

Reference clients for the Agent HTTP API — a Python CLI, a TypeScript client,
a static web page and a GitHub Actions trigger — in
[diy-devin/](diy-devin/README.md).

## agentloop

A complete agent harness in one file. It creates a workspace, asks the model
for one shell command at a time, runs each in the workspace, and stops when the
model says it is done. Model calls are made from inside the workspace so the
broker substitutes the key.

```sh
go run ./examples/agentloop "list the files here, then create hello.txt containing hi"
```

Watch the audit trail while it runs.

```sh
go run ./cmd/remount events --follow
```

## Codex CLI

Not a program in this directory but a verified recipe. See
[docs/harness-integration.md](../docs/harness-integration.md) for the exact
config that runs OpenAI's Codex CLI inside a workspace with no key present.
