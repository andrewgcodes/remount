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

### Changed

- A session ended by a lifecycle deadline reports
  `exit.reason = "lifecycle_deadline_expired"` rather than the generic
  `"workspace released"`, and the node gives it `SIGTERM` plus a bounded grace
  (five seconds by default) before `SIGKILL`. On Windows the grace degrades to
  immediate termination; the recorded reason is the same.

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

- Runtime profiles: a truthful production-isolation gate on node startup,
  scheduling, `doctor` and `conformance --profile`.
- Brokered credentials as the documented default: higher-level binding and
  principal APIs, provider-neutral schemas, revocation and audit.
- Browser and computer-use sessions: screenshot, click, type, key, scroll,
  navigation, downloads and reconnect, with broker-aware browser egress.
- SDK, conformance and observability polish: typed errors across languages,
  metrics and traces for control-plane operations, and attachable conformance
  evidence.

[Unreleased]: https://github.com/andrewgcodes/remount/commits/main
