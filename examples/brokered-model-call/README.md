# brokered-model-call

An OpenAI-compatible model API called from a workspace that never holds the
key.

The program creates a **binding** — the credential plus the host, the methods
and the path prefix it may be spent on — hands the workspace only the opaque
placeholder, and then proves both halves of the claim:

- the call works against the bound host (`GET /v1/models`, then
  `POST /v1/chat/completions`), because the node's broker substitutes the real
  credential at the network edge;
- the same placeholder aimed at any other host is refused with `403` and
  `X-Remount-Reason: egress_denied` before a byte reaches the network.

It then counts occurrences of the real credential in the workspace environment
and fails on any, and reads the audit trail back with one filtered call.

```sh
go run ./cmd/remount standalone --listen 127.0.0.1:7443 --data ./remount-data &
export REMOUNT_SERVER=http://127.0.0.1:7443
export OPENAI_API_KEY=...
go run ./examples/brokered-model-call
```

| Variable | Default | Meaning |
|---|---|---|
| `REMOUNT_SERVER` | `http://127.0.0.1:7443` | the control plane |
| `REMOUNT_TOKEN` | — | bearer, when the server requires one |
| `REMOUNT_MODEL_HOST` | `api.openai.com` | the provider host the binding covers |
| `REMOUNT_MODEL_KEY_ENV` | `OPENAI_API_KEY` | the variable **of this process** holding the credential |
| `REMOUNT_FOREIGN_HOST` | `api.anthropic.com` | the host used to prove the placeholder is useless off its binding |

The credential is read from a named environment variable, never from a
command-line argument, and never reaches the workspace.

`go test -run '^TestB35' ./integration/examples/` runs this against a fake
provider with no network and no key.

See [docs/credentials.md](../../docs/credentials.md).
