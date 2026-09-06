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

- `remount conformance [--profile dev|trusted-single-tenant|multi-tenant-isolated|microvm]`
  judges a deployment black-box against the protocol manifest and, when a
  profile is named, against that profile: one required row per obligation read
  from `node.profile.get`, plus a row that creates a workspace with
  `requires.profile` and proves scheduling honours it. `--markdown FILE`
  writes a review document with one table per tier and a footer stating how
  many checks were unavailable; `--report`, `--junit`, `--evidence` and
  `--out DIR` write the other formats. `--launch self` boots a standalone from
  the running binary and judges that. Exit codes are `0` conformant, `1` a
  check failed, `2` nothing failed but something could not be observed — the
  same three-valued contract as `remount doctor --profile`.
- `cmd/conformance` gains the same `--profile` and `--markdown` flags, and the
  JSON report gains `profile` and `candidate` fields.
- `make conformance-report` builds this commit's binary, boots it as a
  standalone deployment and writes `conformance.json`, `conformance.md` and a
  Plan B §6 evidence record into `OUT` (default `dist/`, `PROFILE` default
  `dev`). It does not change `make conformance`, which still means the
  hostile-input suites under the race detector.
- `remount ws create --requires-profile dev|trusted-single-tenant|multi-tenant-isolated|microvm`
  sets `Requires.Profile`, the runtime profile a node must currently satisfy to
  run the workspace. The value is validated before the client dials, so a
  misspelled profile is a named local error rather than a workspace that parks
  as `profile_unschedulable`. Omitting the flag states no requirement, which is
  not the same as requiring `dev`.
- `docs/security-profiles.md` gains a "Runtime profiles satisfied" column per
  backend, derived from `internal/profile` rather than hand-maintained.
- `remount computer create|get|screenshot|click|type|key|scroll|navigate|eval|downloads|close`
  drives a browser inside a workspace, and `Computer` handles with the same
  operations are in the Python (`client.create_computer`) and TypeScript
  (`client.createComputer`) SDKs alongside the existing Go one. Coordinates are
  CSS pixels in the viewport declared at `create`, screenshots are PNGs clipped
  to it, downloads become artifacts, and failures are typed
  (`closed`/`browser_crashed`, `denied`/`navigation_denied`). Browser profiles
  are node-local and do not survive a move or a sleep; navigation policy is per
  host, never per URL.
- `images/browser`: a reference browser workspace image (Chromium, fonts,
  certificates, `socat`, and a `remount-browser-health` probe) documented in
  [docs/images.md](docs/images.md), plus
  `scripts/browser-conformance.sh`, the evidence gate B34 lane that drives a
  real Chromium through every computer operation.
- `ComputerGetRes` carries `last_iseq`, the highest input sequence the node has
  applied. A computer handle addressed by id adopts it before its first action
  instead of restarting at one, which the node would correctly drop as a
  duplicate; a handle returned by `create` is unaffected
  ([ADR 0094](docs/adr/0094-a-resumed-computer-handle-reads-the-input-sequence.md)).
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
- Durable workspace holds and idle policy (`spec/PROTOCOL.md` §5.5, ADR 0090).
  `ws.lease`, `ws.lease.renew`, `ws.lease.cancel` and `ws.lease.get` keep a
  claimed workspace awake until a control-plane deadline and then sleep or
  destroy it; `ws.idle.policy` and `ws.idle.mark` are the no-work cleanup rule
  and its clock. The deadline is durable, so it survives the client that asked
  for it, node loss, and a control-plane restart. `Workspace` gains `lease`,
  `idle_policy`, `idle_since`, `last_activity_at` and `lifecycle_deadline`, the
  derived view a client polls instead of reading timers. Activity is explicit:
  session traffic does not extend a deadline.
- CLI: `remount ws lease`, `ws lease renew|cancel|get`, `ws idle-policy`,
  `ws mark-idle` and `ws mark-active`, all with `--json` and Go-duration flags.
  `ws get` and `ws lease get` also print the pending lifecycle deadline in
  words when not in `--json` mode.
- SDKs: `LeaseWorkspace`, `RenewLease`, `CancelLease`, `GetLease`,
  `SetIdlePolicy`, `MarkIdle` and `MarkActive` on the Go client; the same seven
  as `lease_workspace`/`renew_lease`/`cancel_lease`/`get_lease`/
  `set_idle_policy`/`mark_idle`/`mark_active` in Python and
  `leaseWorkspace`/`renewLease`/`cancelLease`/`getLease`/`setIdlePolicy`/
  `markIdle`/`markActive` in TypeScript.
- Events `ws.lease.granted`, `ws.lease.renewed`, `ws.lease.cancelled`,
  `ws.lease.expired`, `ws.idle.policy_set`, `ws.idle.marked`,
  `ws.lifecycle.expired`, `ws.lifecycle.expiry_failed` and
  `ws.hold.max_reached`. Note that `ws.lease_expired` remains a different
  event: it is the node's claim lease, not a client's hold.
- `examples/long-running-autosleep`: take a hold, run a command that outlives
  it, renew once, watch the control plane sleep the workspace on its own, read
  the exit reason and the events, then wake it and confirm the filesystem
  survived.
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

- A session ended by a lifecycle deadline reports
  `exit.reason = "lifecycle_deadline_expired"` rather than the generic
  `"workspace released"`, and the node gives it `SIGTERM` plus a bounded grace
  (five seconds by default) before `SIGKILL`. On Windows the grace degrades to
  immediate termination; the recorded reason is the same.
- `internal/client` decides whether a stale-grant `unauthorized` answer is
  worth retrying from the error's stable `reason` (`revoked`,
  `generation_mismatch`) instead of its message text. A node too old to send a
  reason at that site now surfaces `unauthorized` to the caller rather than
  being retried.
- Structured logs are scrubbed for credential shapes: every message and string
  attribute passes through the same redaction the broker and agent transcript
  use before it reaches the log.

### Fixed

- A client streaming a session is no longer cut off before the exit chunk when
  its workspace is released. `releasePrepare` cancelled every output
  subscription before terminating the sessions, so the exit chunk the stop
  produced had no subscriber left to receive it and an attached reader hung
  forever on a stream that went quiet and never closed. Subscribers now deliver
  the exit chunk their joined sessions produced, bounded, before they are cut.

### In progress

These are the [2026-09-06 gap brief](docs/engineering/gap-brief-2026-09-06.md)
implementation order. Each line is a placeholder a later editor replaces with
the change it actually shipped, or deletes if it did not land.

- Brokered credentials as the documented default: higher-level binding and
  principal APIs, provider-neutral schemas, revocation and audit.
- Runtime profiles have landed and are listed above: `remount up --profile`
  (env `REMOUNT_PROFILE`) is a fail-closed startup gate that refuses `process`
  or `docker` under a production profile, `ws create --requires-profile` and
  `requires.profile` schedule against advertised backend capabilities and park
  an unplaceable workspace as `profile_unschedulable`, a reprobe loop emits
  `node.profile.{verified,unschedulable,restored}` on drift, and
  `remount doctor --profile` joins `remount conformance --profile` on the
  three-valued 0/1/2 contract.
- Browser and computer-use sessions have landed and are listed above:
  `remount computer` plus the Go, Python and TypeScript `Computer` handles cover
  create, screenshot, click, type, key, scroll, navigate, eval, downloads and
  close, with `iseq` deduplication across a reconnect. What broker-aware browser
  egress does and does not currently reach is documented in
  [docs/using-remount.md](docs/using-remount.md#computer-sessions-the-built-in-browser-api).
- SDK, conformance and observability polish: attachable conformance evidence.
  Typed errors, the support matrix, tracing and log redaction have landed and
  are listed above.

[Unreleased]: https://github.com/andrewgcodes/remount/commits/main
