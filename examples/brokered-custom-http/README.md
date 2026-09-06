# brokered-custom-http

An internal HTTP service whose token lives inside a JSON request body, called
from a workspace that never holds the token.

The binding declares `substitution: body_json:/auth/token`, so the broker
buffers the request, replaces the JSON string at that RFC 6901 pointer, and
forwards the result. This is the shape a company's own service usually has, and
the one that used to force an adopter either to put the real token in the
workspace or to write their own proxy.

Two properties are worth knowing before choosing a body location:

- The broker buffers the request body — bounded, 1 MiB by default — for a
  workspace whose bindings declare one. A workspace with no body-substituting
  binding keeps streaming uploads straight through.
- A rewritten JSON document is re-serialized, so its keys come back in sorted
  order. Numbers round-trip exactly and nothing is HTML-escaped, but a caller
  that needs byte-identical requests should declare a header or query location.

A placeholder in a request body is scanned for scope exactly like one in a
header, so aiming it at another host is refused with `403` and
`X-Remount-Reason: egress_denied` before a byte reaches the network.

```sh
export REMOUNT_SERVER=http://127.0.0.1:7443
export REMOUNT_SERVICE_HOST=service.internal.example
export SERVICE_TOKEN=...
go run ./examples/brokered-custom-http
```

| Variable | Default | Meaning |
|---|---|---|
| `REMOUNT_SERVICE_HOST` | — (required) | the service host the binding covers |
| `REMOUNT_SERVICE_TOKEN_ENV` | `SERVICE_TOKEN` | the variable **of this process** holding the credential |
| `REMOUNT_SERVICE_PATH` | `/v1/records` | the path the record is posted to |
| `REMOUNT_SERVICE_POINTER` | `/auth/token` | the RFC 6901 pointer the token sits at |
| `REMOUNT_FOREIGN_HOST` | `api.openai.com` | the host used to prove the placeholder is useless off its binding |

`go test -run '^TestB35' ./integration/examples/` runs this against a fake
provider with no network and no key.

See [docs/credentials.md](../../docs/credentials.md).
