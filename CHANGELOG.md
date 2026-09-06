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

- `remount node enroll --name NAME [--tenant T] [--labels k=v] [--ttl 10m]
  (--out FILE | --stdout)` mints the one-time credential a machine presents to
  join a production-mode control plane, over the new `node.enroll` control
  operation (`Client.EnrollNode`). `--out` is exclusive and mode 0600.
  `remount up` accepts it as `--enrollment-file FILE` /
  `REMOUNT_ENROLLMENT_FILE` and, for a provider-created machine, honours the
  `REMOUNT_ENROLL_TOKEN` the provisioner drivers already set; the file wins
  over an ambient `REMOUNT_TOKEN`. `remount node ls` is `remount nodes` under
  the other name. Before this there was no command between "a production
  control plane is running" and "a node is attached to it", so a
  production-mode deployment could not acquire its first node from the CLI.
  Issuing one emits `identity.node_enrollment_issued`, which names the
  operator and carries no bearer, and a second use of the same credential is
  refused `unauthorized` with reason `revoked`. See "Enroll a node" in
  `docs/operations.md`.
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

### Fixed

- A node hello refused for its backend no longer spends the one-time
  enrollment credential it presented. The control plane consumed the
  credential and only then checked the node's backend descriptors against the
  deployment security floor, so a node whose backend can never satisfy the
  floor burned a credential per connection attempt and the operator had to
  mint a new one to retry. The floor is decided entirely from the hello's own
  descriptors, so it is now decided first. A hello with no descriptors at all
  is refused the same way.
- `remount events --json` now includes `actor`, so an event whose only
  attribution is the operator who caused it — `identity.node_enrollment_issued`
  is the first — is no longer rendered with no actor at all.

- A client streaming a session is no longer cut off before the exit chunk when
  its workspace is released. `releasePrepare` cancelled every output
  subscription before terminating the sessions, so the exit chunk the stop
  produced had no subscriber left to receive it and an attached reader hung
  forever on a stream that went quiet and never closed. Subscribers now deliver
  the exit chunk their joined sessions produced, bounded, before they are cut.
- A computer session can now browse to a host the broker allows. Chromium
  discards proxy user-info and never volunteers `Proxy-Authorization`, so the
  broker refused every destination as `unauthenticated` and a browser was
  contained but useless. The node now answers the browser's proxy challenge
  over CDP with the capability the browser's own `HTTPS_PROXY` carries, cancels
  origin challenges so a site login prompt never receives it, and passes every
  other request through unchanged
  ([ADR 0095](docs/adr/0095-browser-proxy-auth-through-cdp.md)).
