# brokered-search-api

A search API that takes its key in the query string, called from a workspace
that never holds the key.

This is the same shape as [brokered-model-call](../brokered-model-call), with
one difference that matters: the binding declares
`substitution: query:key`, so the broker replaces the placeholder in the named
query parameter and nowhere else. A provider that wants `?key=…` needed a
hand-written proxy before; here it is one field on the binding.

The example also shows the fail-closed half of that rule. Sending the same
placeholder in an `Authorization` header instead of the declared parameter is
refused with `400`, because guessing which occurrence the workspace meant is
how a credential reaches a location nobody authorized. And the placeholder
aimed at another host is refused with `403` and
`X-Remount-Reason: egress_denied` before a byte reaches the network.

```sh
export REMOUNT_SERVER=http://127.0.0.1:7443
export REMOUNT_SEARCH_HOST=api.search.example
export SEARCH_API_KEY=...
go run ./examples/brokered-search-api
```

| Variable | Default | Meaning |
|---|---|---|
| `REMOUNT_SEARCH_HOST` | — (required) | the provider host; there is no universal search API to default to |
| `REMOUNT_SEARCH_KEY_ENV` | `SEARCH_API_KEY` | the variable **of this process** holding the credential |
| `REMOUNT_SEARCH_PATH` | `/search` | the path the query is issued against |
| `REMOUNT_FOREIGN_HOST` | `api.openai.com` | the host used to prove the placeholder is useless off its binding |

`go test -run '^TestB35' ./integration/examples/` runs this against a fake
provider with no network and no key.

See [docs/credentials.md](../../docs/credentials.md).
