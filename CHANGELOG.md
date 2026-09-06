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

### In progress

These are the [2026-09-06 gap brief](docs/engineering/gap-brief-2026-09-06.md)
implementation order. Each line is a placeholder a later editor replaces with
the change it actually shipped, or deletes if it did not land.

- Runtime profiles: a truthful production-isolation gate on node startup,
  scheduling, `doctor` and `conformance --profile`.
- Durable workspace lifecycle deadlines: a public lifecycle API with
  control-plane durable timers and generation-bound cancel/extend, surviving
  client and node death.
- Brokered credentials as the documented default: higher-level binding and
  principal APIs, provider-neutral schemas, revocation and audit.
- Browser and computer-use sessions: screenshot, click, type, key, scroll,
  navigation, downloads and reconnect, with broker-aware browser egress.
- SDK, conformance and observability polish: typed errors across languages,
  metrics and traces for control-plane operations, and attachable conformance
  evidence.

[Unreleased]: https://github.com/andrewgcodes/remount/commits/main
