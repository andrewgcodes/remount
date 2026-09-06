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
- Typed errors in all three languages. `api` re-exports every `Reason*`
  constant and adds `api.Is(err, code, reason)`. `remount.errors` (Python) and
  `@remount/sdk`'s `errors` module (TypeScript) define one exception class per
  reason — `EgressDenied`/`EgressDeniedError` and the rest — all deriving from
  `ProtocolError`, which now carries `reason`. An unrecognised reason degrades
  to `ProtocolError` rather than failing, so an older client keeps working
  against a newer server. `cmd/protogen` generates the reason vocabulary into
  `remount.types.REASONS` and `Reason` in `@remount/sdk`.
- The agent HTTP API's error body carries `reason` alongside `code` and
  `message` when the server set one. The field is omitted when empty.
- A generated support matrix in `docs/security-profiles.md`: which operating
  system can host a client and a node, which backends run there, and per
  backend the availability of sleep/wake, move, memory snapshot, computer
  sessions, brokered credentials, enforced egress and artifacts, plus the
  status of each runtime profile. Derived from the backends' own advertised
  capabilities; linked from `README.md` and `docs/compatibility-policy.md`.
- Optional OpenTelemetry tracing with no new dependency. `--otlp-endpoint` on
  `remount server`, `up` and `standalone`, or `REMOUNT_OTLP_ENDPOINT`, exports
  one span per request as OTLP/HTTP JSON to a collector the operator runs.
  There is no default endpoint: unset means nothing is recorded and nothing
  leaves the process. Node spans are children of the control span that caused
  them, carried by additive optional `trace` and `span` fields on the wire
  frame (`spec/PROTOCOL.md` §2). New counters
  `remount_trace_spans_exported_total` and
  `remount_trace_export_failures_total`; documented in
  `docs/observability.md`.

### Changed

- `internal/client` decides whether a stale-grant `unauthorized` answer is
  worth retrying from the error's stable `reason` (`revoked`,
  `generation_mismatch`) instead of its message text. A node too old to send a
  reason at that site now surfaces `unauthorized` to the caller rather than
  being retried.
- Structured logs are scrubbed for credential shapes: every message and string
  attribute passes through the same redaction the broker and agent transcript
  use before it reaches the log.

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
- SDK, conformance and observability polish: attachable conformance evidence.
  Typed errors, the support matrix, tracing and log redaction have landed and
  are listed above.

[Unreleased]: https://github.com/andrewgcodes/remount/commits/main
