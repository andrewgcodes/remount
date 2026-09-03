# Examples

Every example expects a running server. The quickest one is standalone mode,
which needs no token.

```sh
export OPENAI_API_KEY=...
go run ./cmd/remount standalone --data ./data --bindings ./examples/bindings.example.json \
  --allow api.openai.com
export REMOUNT_SERVER=http://127.0.0.1:7443
```

`examples/bindings.example.json` defines one binding, `b_openai`, whose secret
is read from `$OPENAI_API_KEY` on the server. Workspaces reference it as
`ref:b_openai` and never see the value.

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
