# Brokered credentials

A workspace is trusted with nothing. It holds an opaque **placeholder**; the
node's broker substitutes the real credential at the network edge, only for the
destinations the credential is bound to, and records every decision. This
document is the whole model: how to define a credential, how to hand it to a
workspace, what a refusal means, what revocation does and does not do, and how
to answer an audit question.

The short version of the user journey is in
[using-remount.md](using-remount.md#broker-provider-credentials). This is the
detail behind it. The normative rules are `spec/PROTOCOL.md` §9 and §9.1.

## The model in one paragraph

A **binding** is a credential plus its policy: which hosts it may be spent on,
which methods and paths, where in a request it is placed, how long a lease
lives, and what data-retention requirement the operator asserted. It lives in
the control plane, is scoped to a tenant, and is mutable — it can be rotated
and revoked without a restart. A workspace names binding ids in its spec and
receives placeholders in its environment. A node leases the real credential for
the workspaces it holds, and its broker is the only thing in the system that
ever puts the credential on a wire.

## Define a binding

```sh
export OPENAI_API_KEY=...            # in the shell that runs the command only
remount binding create b_openai \
  --kind api_key \
  --destination api.openai.com \
  --secret-env OPENAI_API_KEY \
  --ttl 15m \
  --method GET --method POST \
  --path-prefix /v1/
```

`--secret-env` names an environment variable **of the CLI process**. There is
no `--secret` flag: a credential on a command line is in the shell history and
in the process table every user on the machine can read.

`--source REF` is the alternative. It records a reference — `env://NAME`, a
secret-manager path — that the control plane resolves at lease time, so the
credential never enters the CLI process at all. Exactly one of `--secret-env`
and `--source` is set; "both" hides which one is authoritative.

For a provider Remount already knows, one command does the same thing:

```sh
remount binding preset apply openai --secret-env OPENAI_API_KEY
remount binding preset apply anthropic --secret-env ANTHROPIC_API_KEY
remount binding preset ls            # every preset, its hosts and its variables
```

A preset supplies the destination hosts, the environment variable a harness
reads the key from, and the broker path its base URL points at. `binding preset
apply` prints the `remount ws create` flags that attach it.

## Hand it to a workspace

```sh
remount ws create --name agent \
  --binding b_openai \
  --env OPENAI_API_KEY=ref:b_openai \
  --env 'OPENAI_BASE_URL=${REMOUNT_BROKER}/d/api.openai.com/v1'
```

`ref:b_openai` is resolved on the node at materialize time into the binding's
placeholder, so what the process inside actually reads is an opaque string, not
the literal `ref:` text. `${REMOUNT_BROKER}` is resolved at the same moment,
because the broker's address is different on every node and after every move —
which is why a harness must read `.remount/env` at start-up rather than baking
the address into anything that travels in a snapshot.

**Substitution happens on the broker's reverse-proxy path** (`/d/<host>/…` for
HTTPS upstreams, `/http/<host>/…` for plaintext ones, which never carry a
credential). A `CONNECT` tunnel is opaque and stays host-granular: rewriting
inside it would mean terminating TLS with a CA installed in the workspace,
which ADR 0007 rules out. Point the harness's base URL at the broker.

## Where the credential is placed

A binding declares one substitution location. Omitting it means `header`, which
is what every binding did before the field existed.

| `--substitution` | The broker replaces the placeholder in |
|---|---|
| `header` (default) | any header value, including inside a Basic credential |
| `query:NAME` | the named query parameter |
| `body_form:NAME` | the named `application/x-www-form-urlencoded` field |
| `body_json:/ptr` | the JSON string at that RFC 6901 pointer |

```sh
remount binding create b_search --destination api.search.example \
  --secret-env SEARCH_API_KEY --substitution query:key

remount binding create b_service --destination service.internal.example \
  --secret-env SERVICE_TOKEN --substitution body_json:/auth/token
```

Two rules make this safe, and they are deliberately different:

- **Scanning is location-independent.** Every placeholder is looked for in
  every part of the request the broker can read. A placeholder aimed at a host
  its binding does not cover is an exfiltration attempt whether it sits in a
  header, a query parameter or a JSON body, and all three are blocked and
  recorded as `leak_blocked`.
- **Substitution happens only at the declared location.** A binding that
  declares a query parameter and finds its placeholder in a header is refused,
  not helpfully substituted in both.

Every ambiguity is a refusal, audited, before any upstream byte: a body over
the bound, a content type the declared location cannot parse, a malformed form
or JSON document, a named parameter or field occurring other than exactly once,
a pointer that names no member or names something that is not a string.
Guessing which occurrence the workspace meant is how a credential ends up in a
location nobody authorized, and the failure would be silent.

Body locations make the broker buffer the request, bounded by the matching
egress rule's `max_request_bytes` or a 1 MiB default. Only a workspace whose
bindings declare a body location buffers at all, so a large upload through the
reverse proxy is unaffected. A `body_json` substitution re-serializes the
document, so its keys come back sorted; numbers round-trip exactly and nothing
is HTML-escaped. A caller that needs byte-identical requests declares a header
or query location.

## Narrow it further

`--method` and `--path-prefix` narrow a binding beyond its destination hosts,
and the broker enforces them in the same pass that checks the destination:

```sh
remount binding create b_completions --destination api.openai.com \
  --secret-env OPENAI_API_KEY --method POST --path-prefix /v1/chat/
```

That key can now only be spent on completions. `DELETE /v1/files` with the same
placeholder is refused before substitution. Empty means every method or every
path.

`--principal` and `--workspace` restrict who and what may use the binding.
`--retention-no-log` and `--retention-note` record a provider data-retention
requirement as metadata: Remount cannot enforce a provider's policy, but
recording the assertion is the difference between an audit that can answer
"was this binding declared zero-retention" and one that cannot.

## Kinds, and why a cookie is not an API key

`--kind` is one of `api_key`, `bearer`, `cookie`, `header`. `cookie` is a
separate kind on purpose. Browser login state has a different lifetime, a
different rotation story and a different blast radius from an API key, and one
destination scope covering both means revoking the key also logs the browser
out, while a cookie leaked to an API host is a session rather than a
rate-limited call. Never mix the two under one binding.

## Rotate

```sh
export OPENAI_API_KEY=<the new key>
remount binding rotate b_openai --secret-env OPENAI_API_KEY
```

Rotation bumps the binding's revision. A node holding a lease minted from the
previous revision re-leases within one renew interval — the renew loop runs at
no more than one third of the lease interval — after which the old credential
is no longer substituted. The running workspace is not restarted and its
placeholder does not change.

## Revoke, and what revocation means

```sh
remount binding revoke b_openai --reason "credential leaked"
```

Four separate things can be revoked, and it is worth being precise about which
one you need:

| Revoking | Stops |
|---|---|
| a binding (`binding revoke`) | further substitution, within one renew interval; the row is retained for audit |
| a principal (`principal revoke`) | new sessions, attaches, file access and broker use for that principal, immediately |
| network (`fleet quarantine`) | existing gateway access — the listener closes and in-flight responses are cut |
| a workspace generation (a move) | every grant and capability minted for the old generation |

**Broker TTL expiry is not provider-side key revocation.** `binding revoke`
stops Remount substituting the credential, durably and quickly. The credential
itself remains valid at the provider until you rotate or delete it there. If a
key leaked, revoke the binding *and* rotate it at the provider.

Revocation is permanent and a binding id is never reusable. The revoked row is
retained so an audit can still resolve an id seen in an older event; the secret
and source are dropped from it, so a revoked credential is not carried in later
snapshots of control state.

Revoking one binding stops substitution for **every** binding that workspace
declares, not only the revoked one. The control plane refuses the whole lease
set when one member is revoked — a partial set would leave the node unable to
tell "this binding was withdrawn" from "control chose not to send it this
time" — so the node holds no lease for any of them, and each placeholder is
refused as `revoked`. Give a workspace only the bindings it needs, and expect
a revocation to end that workspace's brokered egress.

A workspace that keeps using the placeholder afterwards is refused with `403`
and `X-Remount-Reason: revoked` before any upstream byte, and the refusal is
audited with the `revoked` decision. The node keeps the placeholders of the
bindings it can no longer lease for exactly this reason: forwarding an
orphaned placeholder to the provider would earn the provider's own 401, tell
the workspace nothing about why, and hand an internal identifier to a third
party. A custom placeholder for a binding this node never leased is the one
residue: it is refused as an ordinary unbound destination instead.

A file-configured binding (`--bindings`) is **seeded** into the durable store on
the first start that does not already have it. After that the store wins:
editing the file changes nothing, because a rotation or a revocation must not be
undone by restarting with the original file. The operation to use is
`binding rotate`.

## Session-scoped principals

```sh
TOKEN=$(remount principal session --ws "$WS" --roles agent --ttl 15m)
```

This mints an ephemeral principal and, in the same call, the workspace- and
generation-bound capability it acts with. The bearer is printed exactly once
and is never written to durable control state or to an event. It stops
verifying when the workspace moves to another node, when the TTL passes, or
when `remount principal revoke` ends the principal.

Use it to hand one task, one agent turn or one collaborator a scoped authority
that expires by itself, rather than sharing a long-lived token.

## What a refusal looks like

Every broker refusal carries an `X-Remount-Reason` header and a JSON body:

```json
{"error":{"code":"denied","reason":"egress_denied",
          "binding":"b_openai","host":"api.anthropic.com:443",
          "message":"remount broker: credential b_openai is not bound to api.anthropic.com:443"}}
```

`code` is a wire code and `reason` is its sub-classification. **Match on the
pair, never on the message**, which is diagnostic prose and may change. The
reasons a broker refusal uses are `egress_denied`, `approval_required`,
`quota_exceeded`, `binding_missing`, `grant_expired` and `revoked`.

A refusal carries no secret value, no header the workspace sent, and no byte of
the request or the upstream response, and its message is scrubbed of
credential-shaped strings before it is written. The same scrubbing is applied
to the reason and error fields of every audit record.

The SDKs surface the same pair. `ProtocolError` has `code` and `reason` in
Python and TypeScript, and Go's `proto.Error` has `Code` and `Reason`.

## Audit

One question — who used which binding, for which workspace, to which host, with
what status, under which tenant, at what time — is one call:

```sh
remount events --binding b_openai --json
remount events --host api.openai.com --type egress --follow
remount events --ws "$WS" --type cred.used
```

The `--binding`, `--host` and `--type` filters are applied by the control
plane. The decision appears in the payload as `substituted`, `leak_blocked`,
`denied`, `expired` or `limit_exceeded`.

Binding lifecycle events (`binding.created`, `binding.rotated`,
`binding.revoked`) stream under the **binding id**, not a workspace, so they
are read from the tenant-wide stream rather than `--ws`.

No credential value is ever part of any of these records.

## From code

Go:

```go
binding, err := c.CreateBinding(ctx, api.BindingSpec{
    ID: "b_openai", Kind: api.BindingKindAPIKey, Secret: os.Getenv("OPENAI_API_KEY"),
    Destinations: []string{"api.openai.com"}, TTLSec: 900,
})
records, err := c.CredentialEvents(ctx, client.CredentialFilter{WS: ws.ID, Binding: "b_openai"})
```

Python:

```python
await client.create_binding({
    "id": "b_openai", "kind": "api_key", "secret": os.environ["OPENAI_API_KEY"],
    "destinations": ["api.openai.com"], "ttl_sec": 900,
})
records = await client.credential_events(CredentialFilter(ws=ws_id, binding="b_openai"))
```

TypeScript:

```ts
await client.createBinding({
  id: "b_openai", kind: "api_key", secret: process.env.OPENAI_API_KEY!,
  destinations: ["api.openai.com"], ttl_sec: 900,
});
const records = await client.credentialEvents({ ws: wsID, binding: "b_openai" });
```

All three also have `listBindings`, `getBinding`, `rotateBinding`,
`revokeBinding`, `createSessionPrincipal` and `revokePrincipal`, spelled in each
language's convention.

## Runnable examples

Three examples cover the three substitution shapes. Each creates a binding,
gives the workspace only the placeholder, proves the call works at the bound
host, and proves the same placeholder is refused at any other host:

| Example | Location | Proves |
|---|---|---|
| [brokered-model-call](../examples/brokered-model-call) | header | an OpenAI-compatible model API, with method and path scope |
| [brokered-search-api](../examples/brokered-search-api) | `query:key` | a search API keyed in the query string, and that a misplaced placeholder is refused |
| [brokered-custom-http](../examples/brokered-custom-http) | `body_json:/auth/token` | an internal service whose token lives in the request body |

`go test -run '^TestB35' ./integration/examples/` runs all three against a fake
provider that answers 401 without the exact credential, with no network access
and no provider key. That is evidence gate **B35**.

## What this does not do

- It does not stop a workspace reaching the network *around* the broker on the
  `process` and `docker` backends, where proxy mediation is cooperative. That
  is why those backends are rejected by production security profiles and why
  enforced-gateway backends exist; see
  [security-profiles.md](security-profiles.md) and ADR 0061.
- It does not enforce a provider's data-retention policy. It records what the
  operator asserted.
- It does not make a leaked key safe at the provider. Revoke the binding and
  rotate the key.

## See also

- [using-remount.md](using-remount.md#broker-provider-credentials) — the user
  journey.
- [operations.md](operations.md) — running the control plane and nodes.
- [security-profiles.md](security-profiles.md) — what each backend can enforce.
- `spec/PROTOCOL.md` §9, §9.1 — the normative rules.
- ADR [0091](adr/0091-dynamic-binding-lifecycle-and-session-principals.md),
  [0092](adr/0092-broker-substitution-locations-and-redaction.md),
  [0096](adr/0096-the-credential-surface-is-the-cli-the-sdks-and-runnable-examples.md).
