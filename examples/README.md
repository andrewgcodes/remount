# Examples

| Example | What it shows | Needs a credential |
|---|---|---|
| [minimal-shell](minimal-shell/README.md) | connect, create a workspace, stream a command, write and read a file, destroy | no |
| [artifact-transfer](artifact-transfer/README.md) | upload a directory, snapshot, download and digest-check the artifact, restore into a second workspace, verify byte for byte | no |
| [brokered-model-call](brokered-model-call/README.md) | an OpenAI-compatible model API through the broker: header substitution, method and path scope, and the same placeholder refused at another host | a model key, held only by the control plane |
| [brokered-search-api](brokered-search-api/README.md) | a search API keyed in the query string (`substitution: query:key`), and what happens when the placeholder is put somewhere else | a search key, held only by the control plane |
| [brokered-custom-http](brokered-custom-http/README.md) | an internal HTTP service whose token lives at a JSON pointer in the request body | a service token, held only by the control plane |
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
rather than a reader's laptop. The three brokered examples are executed there
too, against a fake provider.

The examples that call a provider need a binding, so that the key lives in the
control plane and never inside the workspace. Create it with one command; the
secret is read from a named environment variable of that command, never from an
argument:

```sh
go run ./cmd/remount standalone --data ./data &
export REMOUNT_SERVER=http://127.0.0.1:7443
export OPENAI_API_KEY=...
go run ./cmd/remount binding preset apply openai --secret-env OPENAI_API_KEY
```

The workspace then holds `ref:b_openai` and never sees the value. The three
`brokered-*` examples each create their own binding and revoke it on the way
out, so they need only the key in their own environment.

`examples/bindings.example.json` is the older file form. It is still accepted
as a bootstrap for a first start, but the durable store wins afterwards; see
[docs/credentials.md](../docs/credentials.md).

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

## brokered-model-call, brokered-search-api, brokered-custom-http

The three shapes a provider takes a credential in, one example each: an
`Authorization` header, a `?key=` query parameter, and a JSON string at
`/auth/token` in the request body. Each one creates its binding through the
public API, gives the workspace only the placeholder, asserts the call works
against the bound host, and asserts the same placeholder aimed at another host
is refused with `403` and `X-Remount-Reason: egress_denied` before a byte
leaves the node. Each also counts occurrences of the real credential in the
workspace environment and fails on any.

```sh
export REMOUNT_SERVER=http://127.0.0.1:7443
export OPENAI_API_KEY=...
go run ./examples/brokered-model-call

export REMOUNT_SEARCH_HOST=api.search.example SEARCH_API_KEY=...
go run ./examples/brokered-search-api

export REMOUNT_SERVICE_HOST=service.internal.example SERVICE_TOKEN=...
go run ./examples/brokered-custom-http
```

`go test -run '^TestB35' ./integration/examples/` runs all three against a
loopback fake provider that answers 401 without the exact credential — no
network, no provider key. That is evidence gate B35, which is why these
examples cannot quietly stop working.

`examples/brokered/` holds the few helpers they share (the in-workspace probe,
the credential scan and the teardown). It imports nothing but `api`, `client`
and the standard library.

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
