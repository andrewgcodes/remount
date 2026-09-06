# 0096. The credential surface is the CLI, the SDKs, and runnable examples

Status: accepted (2026-09-06)

## Context

ADR 0091 made a binding a durable, mutable, tenant-scoped resource and ADR 0092
gave the broker substitution locations and a typed refusal contract. Both are
protocol and Go-SDK work. What an adopter actually meets first is none of that:
it is a command, a package method, and an example they can run.

At the end of 0091 and 0092 the only way to create a binding was to write Go
against `internal/client`, or to write a JSON file and restart the control
plane — and editing that file after the first start no longer changes anything,
because the durable store wins. The Python and TypeScript packages had no
credential surface at all. There were no examples, so the gap brief's
acceptance criterion ("the SDK has examples for model API, search API and
custom HTTP service through the broker") had nothing to point at, and the
criterion "no first-party example places provider credentials in the workspace
environment" had nothing to check.

Two smaller things fell out of trying to write those surfaces.

`BindingSpec.Substitution` is a `*proto.BindingSubstitution`, and `api` aliased
neither the struct nor the four location constants. A public consumer could
read a binding's declared location but could not construct one, so `query`,
`body_form` and `body_json` were reachable only from inside the module.

And `methods` / `path_prefixes` were carried to the node on the lease and then
ignored. `TestBindingMethodAndPathPrefixNarrowSubstitution` returned `200` with
the real key attached to a path the binding did not cover.

## Decision

### The secret is named, never passed

`remount binding create ID --secret-env NAME` reads the credential from the
named environment variable **of the CLI process**. There is no `--secret` flag
and there will not be one. A credential on a command line is in the shell
history and in the process table every user on the machine can read, and a
CLI that accepts it there teaches the habit even to operators who know better.
`--source REF` is the other half: a reference the control plane resolves, so
the credential never enters this process at all.

`binding preset apply <preset> --secret-env NAME` collapses the common case to
one command by reusing the provider shapes already in `internal/launch`, which
is where `remount run --binding` gets them. One source of provider truth, two
commands that read it.

`remount principal session --ws WS` prints the bearer once, to stdout, with the
human context on stderr — so `TOKEN=$(remount principal session --ws $WS)`
works and the token is not in the terminal scrollback of a run that piped it.

### The three languages expose the same workflow, not the same code

Go already had methods on `Client`. Python gains a `CredentialOperations` mixin
in `credentials.py` that `Client` inherits, and TypeScript gains functions in
`credentials.ts` that `Client` delegates to. All three end up flat —
`create_binding`, `createBinding`, `CreateBinding` — because a credential
workflow that reads differently in each language is a workflow an adopter has
to learn three times.

TypeScript uses explicit delegation rather than prototype augmentation because
the package is bundled with tree shaking, and a module whose only purpose is a
side effect is exactly what a bundler is entitled to drop.

`credential_events` / `credentialEvents` pass the binding, host and workspace
filters to the control plane (`events.tail` gained them in ADR 0092) and match
the decision locally, because the decision is a field of the audit payload
rather than of the log envelope. Neither package gains a typed exception
hierarchy; `ProtocolError` gains `reason` instead, so a caller matches the same
`(code, reason)` pair the broker puts in `X-Remount-Reason`.

### An example is evidence or it is decoration

`examples/brokered-model-call`, `examples/brokered-search-api` and
`examples/brokered-custom-http` each drive one substitution location, and each
asserts both halves: the call works at the bound host, and the same placeholder
aimed at any other host is refused with `403` and `egress_denied` before a byte
leaves the node.

They run in CI against `internal/testutil/fakeprovider`, a loopback TLS server
that answers `401` unless the exact expected credential arrived (evidence gate
**B35**). That is what makes them evidence: an example that "passed" because
the broker stopped substituting, or substituted the wrong value, fails the
gate. A second fake stands in for "some other host" and its request counter
must stay at zero.

Each example also counts occurrences of the credential *it holds* in the
workspace environment and fails on any. Counting the known value beats matching
a provider's key pattern: it cannot be fooled by a key shape the pattern does
not know, and there is no pattern to keep up to date. The static half of the
criterion is a scan of every example's `Env:` literal for a value computed at
run time, and it proves its instrument on a planted canary before it trusts a
clean result.

### Method and path scope are enforced where the credential is placed

`checkLeaseScope` refuses a request whose method or path the binding does not
cover, in the same pass that checks destination, expiry and TLS — before
substitution and before any upstream byte. An unknown method or path fails
closed for a scoped binding.

Carrying an operator's stated narrowing to the node and ignoring it is worse
than not offering it: the operator believes the key can only be spent on
completions, the broker spends it on anything, and the disagreement is silent
with the real credential attached. This is strictly more blocking, and the
newly blocked case is one an operator explicitly asked to block.

### A withdrawn placeholder is refused, not forwarded

ADR 0091 left one thing explicitly to a later decision: "Denying traffic whose
placeholder has lost its binding belongs with the substitution and denial work
in the broker (ADR 0092)." ADR 0092 did not take it, and the observed behaviour
was bad in both directions. Without the destination in `--allow`, a request
carrying the orphaned placeholder was refused as an ordinary unbound
destination — true, but it says nothing about the revocation. With the
destination allowed, the placeholder travelled to the provider as an ordinary
string, earned the provider's own 401, and handed an internal identifier to a
third party on the way.

So the node now keeps the placeholders behind. When `binding.lease` is refused,
it drops the leases as before and calls `Broker.SetWithdrawn`, and the broker
refuses any request still carrying one with `unauthorized` +
`revoked`, audited as the `revoked` decision, before any upstream byte.

The withdrawal set has two sources because neither is complete alone. The
leases being dropped carry the exact placeholder, including a shape-preserving
custom one, but exist only if this materialization ever held them; the
workspace's declared binding ids always exist and yield the default `ref:<id>`
form. A custom placeholder for a binding this node never leased is therefore
not recognized, and that request fails as an ordinary unbound destination
instead. That residue is recorded rather than hidden.

One consequence is worth stating rather than discovering: because control
refuses the whole lease set when one binding in it is revoked (ADR 0091), every
placeholder that workspace holds is refused, not just the revoked one. The live
run in `docs/engineering/verification-2026-09.md` shows exactly that. This is
not new behaviour — those placeholders already stopped being substituted — but
it is now visible as a typed refusal instead of a provider 401.

`TestBindingRevokeStopsSubstitutionWithinRenew` changes with this: it used to
assert the post-revocation request returned `200` with the inert placeholder
reaching the upstream, and now asserts `403` with `revoked` and nothing
reaching the upstream. The property it exists to prove — the credential stops
flowing within one renew — is unchanged and now strictly stronger.

## Consequences

- A credential can be defined, rotated and withdrawn from a shell, from Python,
  from TypeScript or from Go, with the same vocabulary in each.
- `--secret-env` means the value is in the environment of one command. An
  operator who wants it nowhere near the CLI uses `--source`.
- The examples are executed by CI on every change, with no network and no
  provider key, so "the SDK has examples for X through the broker" is a claim
  with a test behind it.
- `api` now exposes `BindingSubstitution` and the four `Substitution*`
  constants; a public consumer can declare a query, form or JSON-pointer
  binding.
- A binding that declares `methods` or `path_prefixes` now refuses out-of-scope
  requests. A deployment that declared them expecting them to be advisory will
  see refusals it did not see before; the fix is to widen or remove the
  declaration, which is the operator's own policy statement.
- A workspace that keeps using a revoked binding gets `403` and
  `X-Remount-Reason: revoked` instead of a provider 401, and the placeholder
  no longer reaches the provider. The broker audits the refusal as `revoked`.
- `examples/brokered` is a shared package the three examples import. It is the
  price of not triplicating sixty lines of probe and cleanup, and it imports
  nothing but `api`, `client` and the standard library, so copying an example
  out of the tree means copying one extra file.
