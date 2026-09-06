# Changelog

All notable user-visible changes to Remount are recorded here. The format
follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and Remount
versions its public surfaces under
[Semantic Versioning](https://semver.org/spec/v2.0.0.html) as described in
[docs/compatibility-policy.md](docs/compatibility-policy.md).

No release has been tagged or published. Tagging, signing and package
publication are deferred by owner decision; see
[docs/releases.md](docs/releases.md). Everything below is therefore in
`Unreleased`, and the first tagged release will move it into a dated section.

An entry belongs here when it changes something a user can observe: the wire
protocol, the Go `api`/`client` packages, the Python or TypeScript package, a
CLI flag or `--json` shape, or a documented default. Internal refactors,
tests and engineering notes do not.

## [Unreleased]

### Added

- Wire errors carry an optional `reason`: a stable sub-classification inside an
  existing `code` that a client may act on, alongside `proto.ErrReason` and the
  `proto.Reason*` constants. Older peers ignore the field and a code without a
  reason is still complete (`spec/PROTOCOL.md` §2). `spec/protocol.schema.json`
  and the generated Python and TypeScript `Error` declarations now expose it.
- `examples/minimal-shell`: connect, create a workspace, stream a command,
  write and read a file, destroy — the public `api`/`client` packages only, no
  credentials.
- `examples/artifact-transfer`: upload a directory into a workspace, snapshot
  it, download and digest-check the artifact, restore a second workspace from
  it, and verify every file byte for byte.
- `docs/compatibility-policy.md`: what is public, what semantic versioning
  means for each surface, the additive-fields rule for the wire format, how
  deprecations are announced, and the support window.
- `make protogen` and `make protogen-check` wrap `cmd/protogen`, which
  regenerates the JSON Schema, operation table and Python/TypeScript
  declarations from `internal/proto` and byte-compares them.
- `remount binding create|ls|get|rotate|revoke` and `remount binding preset
  apply PRESET`: the whole brokered-credential lifecycle from the shell, with
  `--json` everywhere. The secret is read from the environment variable named
  by `--secret-env`, never from a command-line argument; `--source` keeps it in
  an external manager instead. `--substitution header|query:NAME|body_form:NAME
  |body_json:/pointer`, `--method`, `--path-prefix`, `--retention-no-log`.
- `remount principal session --ws WS [--roles] [--ttl]`: an ephemeral principal
  and its workspace- and generation-bound capability in one call, with the
  bearer printed exactly once.
- Python and TypeScript credential helpers: `create_binding`, `list_bindings`,
  `get_binding`, `rotate_binding`, `revoke_binding`, `create_session_principal`,
  `revoke_principal` and `credential_events(filter)` (`createBinding`, … in
  TypeScript), in `remount.credentials` and `@remount/sdk`'s `credentials.ts`.
- `ProtocolError` exposes `reason` alongside `code` in the Python and
  TypeScript SDKs, so a caller matches the same pair the broker returns in
  `X-Remount-Reason`.
- `api.BindingSubstitution` and the `api.Substitution{Header,Query,BodyForm,
  BodyJSON}` constants, so a public consumer can declare a query, form-field or
  JSON-pointer binding rather than only read one.
- `examples/brokered-model-call`, `examples/brokered-search-api` and
  `examples/brokered-custom-http`: a model API, a search API keyed in the query
  string, and a service whose token lives at a JSON pointer in the body. Each
  creates its binding, hands the workspace only a placeholder, proves the call
  works at the bound host and is refused at any other, and counts zero copies
  of the credential in the workspace environment. `go test -run '^TestB35'
  ./integration/examples/` runs all three against a fake provider with no
  network and no key (evidence gate B35).
- `docs/credentials.md`: the brokered-credential guide — bindings, substitution
  locations, method and path scope, rotation, what each kind of revocation
  stops, session principals, the refusal contract and audit — linked from
  `docs/using-remount.md` and `docs/operations.md`.

### Changed

- A workspace still using a revoked binding's placeholder is refused with `403`
  and `X-Remount-Reason: revoked` before any upstream byte, and the refusal is
  audited with the `revoked` decision. The placeholder previously travelled to
  the provider as an ordinary string, which returned the provider's own 401 and
  handed an internal identifier to a third party.
- A binding's `methods` and `path_prefixes` are now enforced by the broker, in
  the same credential pass that checks destination, expiry and TLS: a request
  whose method or path the binding does not cover is refused before
  substitution and before any upstream byte. They were previously carried to
  the node and ignored, so a key scoped to `POST /v1/chat/` was spent on any
  path. A deployment that declared them expecting them to be advisory will see
  refusals; widen or remove the declaration.
- `docs/using-remount.md` documents dynamic bindings as the default path. The
  `--bindings` file remains accepted as a first-start bootstrap, and its
  entries are seeded into the durable store, after which the store wins:
  editing the file has no effect and `binding rotate` / `binding revoke` are
  the operations.

### In progress

These are the [2026-09-06 gap brief](docs/engineering/gap-brief-2026-09-06.md)
implementation order. Each line is a placeholder a later editor replaces with
the change it actually shipped, or deletes if it did not land.

- Runtime profiles: a truthful production-isolation gate on node startup,
  scheduling, `doctor` and `conformance --profile`.
- Durable workspace lifecycle deadlines: a public lifecycle API with
  control-plane durable timers and generation-bound cancel/extend, surviving
  client and node death.
- Browser and computer-use sessions: screenshot, click, type, key, scroll,
  navigation, downloads and reconnect, with broker-aware browser egress.
- SDK, conformance and observability polish: typed errors across languages,
  metrics and traces for control-plane operations, and attachable conformance
  evidence.

[Unreleased]: https://github.com/andrewgcodes/remount/commits/main
