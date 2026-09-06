# Phase B4: black-box standard conformance — 2026-09-03

Plan B §12 asks for the semantic core of `spec/PROTOCOL.md` in executable
form: a versioned manifest, a runner that can point at any implementation,
sanitized fixtures, and JUnit plus §6 evidence output. This document records
what landed, what it proves, and what it does not.

The whole design follows from one sentence in §12.3: **conformance assertions
may observe only public protocol, HTTP, CLI, SDK, event and diagnostic
surfaces.** A suite that can only test Remount because it reaches into
Remount is not a standard, it is a second copy of Remount's unit tests. So
`internal/conformance` imports no implementation package of this repository.
It re-declares the wire format from the spec, speaks CBOR over the WebSocket
at `/v1/link`, reads `/healthz`, `/v1/artifacts/{id}` and `POST /v1/events`,
and reads the canonical event log through `events.tail`. The only Remount
code it links is the module's third-party dependencies.

Two duplications are deliberate and are commented as such in the source:

- `internal/conformance/wire.go` re-declares `Frame`, `Hello`, `HelloOK` and
  `ChunkBody` rather than importing `internal/proto`. A decoder that shares a
  struct definition with the encoder it is judging cannot detect a
  disagreement between the implementation and the document.
- `internal/conformance/report.go` re-declares the Plan B §6 evidence record
  rather than importing `internal/evidence`. That package is an
  implementation package of this repository; importing it would make the
  suite unusable against any other implementation. The struct is small, its
  field names are fixed by §6, and `report_test.go` asserts the JSON keys
  against the documented schema so the two cannot drift apart silently.

## Layout

| Path | Owns |
|---|---|
| `internal/conformance/manifest.go` | the versioned requirement manifest and its invariants |
| `internal/conformance/wire.go` | an independent frame codec and relay client |
| `internal/conformance/types.go` | the request and response bodies, transcribed from the spec |
| `internal/conformance/target.go` | the three target modes of §12.2 |
| `internal/conformance/session.go` | the observation surfaces one run is given |
| `internal/conformance/checks*.go` | one function per requirement, grouped by category |
| `internal/conformance/runner.go` | manifest × target → results |
| `internal/conformance/report.go` | JUnit XML and the §6 evidence record |
| `internal/conformance/shim/` | a minimal implementation with switchable defects |
| `internal/conformance/testdata/failure-seeds.json` | the deterministic failure seeds |
| `internal/conformance/testdata/golden/` | v1 golden frames and the golden manifest |
| `cmd/conformance` | the runner as a command |

## The manifest

`conformance/1.0.0`, describing protocol `v1`. 67 requirements across the
nine semantic areas §12.1 names. Every row carries its own version, the
manifest version that introduced it, the normative section it comes from, and
a §12.5 tier.

| Category | required | capability-gated | extension | total |
|---|---:|---:|---:|---:|
| negotiation | 8 | 1 | 0 | 9 |
| workspace | 7 | 0 | 0 | 7 |
| authority | 6 | 1 | 0 | 7 |
| session | 7 | 1 | 0 | 8 |
| snapshot | 7 | 1 | 0 | 8 |
| binding | 0 | 5 | 0 | 5 |
| approval | 2 | 3 | 0 | 5 |
| agent | 7 | 1 | 0 | 8 |
| event | 8 | 1 | 1 | 10 |
| **total** | **52** | **14** | **1** | **67** |

`Manifest.Validate` refuses several shapes that would let a report be quietly
wrong: a duplicate id, a row with no version or no spec citation, a category
§12.1 does not name, a **required** row that names a gate (a gated obligation
is capability-gated by definition), a capability-gated row that names no gate
at all, a gate on an identifier §3.1 does not define, a requirement with no
registered check, and a check with no requirement.

### Gates: capabilities and prerequisites are different sentences

A capability-gated row may be gated on a negotiated capability (§3.1) or on a
**prerequisite** the *runner* must be able to arrange. Keeping the two apart
matters, because "this build does not implement approvals" and "this run had
no approve-mode rule to test approvals with" are different statements and a
report that conflates them is lying about one of them. Both produce
`unavailable` with a reason; neither produces a pass.

The declared prerequisites are `bindings`, `cli`, `session-eviction`,
`event-eviction`, `transcript-eviction`, `node-fault`, `egress-approval`,
`restart`, `quiesced-snapshot` and `notifier`.

## Targets (§12.2)

```sh
conformance --build .                       # build ./cmd/remount and judge the binary
conformance --binary ./remount              # judge a binary this run starts
conformance --endpoint http://host:7443     # judge something already running
conformance --endpoint URL --external       # judge another implementation
conformance --manifest                      # print the contract, judge nothing
```

A launched target is started as `standalone` with a generated bindings file,
which is what makes the whole binding category observable hermetically: the
run creates a workspace holding a synthetic binding, reads `.remount/env`
through `fs.read`, and drives the node's broker directly, because §9 makes
the broker's URL shape and its decisions part of the protocol.

A target that declares a protocol version this manifest does not describe is
not judged at all. The run aborts with a reason and the evidence record is
`unavailable`: a verdict about a contract the implementation never claimed
would be meaningless.

## Output

- **JUnit XML**, one suite per semantic category. A failure carries the
  failed postcondition; an unavailable row is `skipped` and carries the
  reason, which is as close as JUnit gets to Plan B's third outcome.
- **The §6 evidence record**, with `scenario`, `candidate`, `layer`,
  `status`, `command`, `started_at`, `duration_ms`, `environment`, `cleanup`,
  `artifacts`, `required` and `reason`.
- **The full JSON report**, every row with its status, reason and duration.
- **A summary that leads with the verdict**, then names what did not hold.

`Conformant()` is false when any required row failed **or** when any required
row could not be observed. §6.2 has no `skipped-success`, and an
implementation nobody could check is not one anybody should trust.

## Fixtures and seeds (§12.4)

- `testdata/golden/req-frame.cbor`, `hello-all-capabilities.cbor` and
  `chunk-info.cbor` pin the v1 encoding of a request frame, a hello offering
  every named capability, and a seq-0 info chunk. `TestGoldenV1FramesReEncodeByteForByte`
  fails if any of them drifts, which §13 says requires a version bump rather
  than a quiet edit. Regenerate deliberately with
  `REMOUNT_CONFORMANCE_UPDATE_GOLDEN=1`.
- `testdata/golden/manifest.json` pins the contract itself, so a requirement
  cannot change meaning without the diff showing it.
- `testdata/failure-seeds.json` is the deterministic failure seed table: each
  seed names one defect, the category §12.1 places it in, the requirements
  the suite **must** fail against it, and the requirements that must keep
  passing so a seed cannot be "caught" by a suite that fails everything.

Everything checked in is sanitized. The binding fixture's secret is a
synthetic canary that exists only inside a test process and is deliberately
not shaped like any provider credential; `TestSecretCanaryIsSynthetic`
enforces that, and `TestEvidenceRecordCarriesNoSecretValue` proves it never
reaches a record.

## The scenarios

### B20 — the reference binary passes the complete required manifest

**Proven**, `artifact` layer, at `7461799` on darwin/arm64, backend `process`.

```
$ conformance --build . --junit b20-junit.xml --evidence b20-evidence.json --scenario B20 --layer artifact
CONFORMANT: remount standalone (remount)
  manifest 1.1.0, protocol v1, built-binary, http://127.0.0.1:56476
  59 passed, 0 failed, 8 unavailable of 67 requirements in 11.674s
    required         53 passed, 0 failed, 0 unavailable
    capability-gated  7 passed, 0 failed, 7 unavailable
    extension         0 passed, 0 failed, 1 unavailable
  negotiated: v1, authz-push, controller-epoch, session-cap, chunked-artifacts,
              tiered-session-logs, identity-admin
  cleanup: verified
```

**The reference binary currently fails no requirement.** All 53 required rows
were observed and held. Seven capability-gated rows passed: the five binding
rows, the authoritative-snapshot row, and the controller-epoch fence.

The seven unavailable capability-gated rows and one extension row, each with
the reason the report prints:

| Row | Why it could not run |
|---|---|
| `CONF-AUTH-007` lease expiry returns a workspace to pending | the runner may not stop this target's nodes; a standalone's node and control plane are one process, so stopping the node stops the observer |
| `CONF-SESS-007` evicted replay emits a gap | the reference node's retention is 2 MiB of memory plus 128 MiB of spill per session with no knob to lower it, so a run cannot overrun it inside its budget |
| `CONF-APP-003/004/005` approval fingerprinting, second decision, durability | no approve-mode egress rule was configured for this target, and the `approvals` capability is reserved rather than implemented in this release |
| `CONF-AGT-005` transcript gap | the runner launches no harness, so there are no mirrored records to evict |
| `CONF-EVT-010` a request below the retention watermark is `evicted` | the runner cannot drive the log past the watermark |
| `CONF-EVT-009` export cursor advances only after acceptance | extension-specific: §11 makes outbound notification operator configuration, not a protocol request |

The `negotiated` line is itself a finding worth recording: this release offers
`v1`, `authz-push`, `controller-epoch`, `session-cap`, `chunked-artifacts`,
`tiered-session-logs` and `identity-admin`, and does **not** offer `approvals`
or `encrypted-artifacts`. §3.1 says those two are reserved, and the suite
reports rows gated on them as unavailable rather than passing them.

The already-running-endpoint mode was exercised against a separately started
`remount standalone`: 53 required passed, 0 failed, and the thirteen
capability-gated rows went unavailable because that endpoint was started
without a bindings file — which is the mode working correctly, not a defect.

### B21 — a deliberately broken implementation fails each semantic category

**Proven**, `code` layer, by `internal/conformance/shim` and
`TestB21BrokenImplementationFailsEachSemanticCategory`.

The shim is a second, minimal implementation of the protocol: an in-memory
control plane, node, broker, event log and artifact store behind the same
public surfaces. It is not a Remount implementation and must never be used as
one — it has no authentication, persistence, isolation or security properties
at all.

The control matters as much as the experiment. `TestB21BaselineShimPassesTheRequiredManifest`
asserts that the undefective shim passes **all 53 required requirements with
none unavailable** — the same 52 the reference binary passes. Without that,
a defective shim failing would prove nothing about the defect.

Then, one defect at a time, from the checked-in seeds:

| Defect | Violates | Rows the suite must fail |
|---|---|---|
| `negotiation` | echoes identifiers it does not implement; accepts a hello with no `v1` | `CONF-NEG-002`, `CONF-NEG-003` |
| `workspace` | ignores the idempotency key | `CONF-WS-002` |
| `authority` | accepts a grant bound to a stale generation | `CONF-AUTH-003/004/006` |
| `session` | applies an already-applied `iseq` | `CONF-SESS-004` |
| `snapshot` | fresh artifact id for an unchanged tree | `CONF-SNAP-001` |
| `binding` | forwards a misaimed placeholder | `CONF-BIND-002` |
| `approval` | reports settled approvals as pending | `CONF-APP-002` |
| `agent` | unbounded inbox | `CONF-AGT-003` |
| `event` | a sequence that repeats | `CONF-EVT-001`, `CONF-EVT-002` |
| `version` | accepts an unnegotiated frame version | `CONF-NEG-004` |
| `version-mutates` | mutates state, then refuses that version | `CONF-NEG-004` |

For every seed the test asserts three things: the named rows failed, they
failed **in the seed's category**, and the rows the seed names as untouched
still passed — so a defect cannot be "caught" by a suite that fails
everything. It also asserts that the failures land in exactly one category,
and that every one of the nine categories is exercised by some seed.

### B22 — unknown extensions stay ignorable, unknown required capabilities fail

**Proven**, `code` layer, by
`TestB22UnknownExtensionsStayIgnorableAndUnknownRequiredCapabilitiesFail` and
`TestB22CapabilityGatedRowsAreUnavailableNotPassed`, and by `CONF-NEG-002`
and `CONF-NEG-003` passing against the reference binary.

The three halves:

1. **Ignorable.** `CONF-NEG-002` offers `["session-cap",
   "x-conformance-unimplemented-extension", "v1", "authz-push"]` — out of
   canonical order, with an identifier nobody can implement in the middle.
   The handshake must succeed, must start with `v1`, must never echo the
   unknown identifier, must echo nothing that was not offered, and must
   return the result in §3.1's canonical order. A naive implementation that
   echoes its input fails here, and the shim's `negotiation` defect does
   exactly that and is caught.
2. **Required capabilities fail closed.** `CONF-NEG-003` offers `[]` and
   `["x-conformance-unimplemented-extension"]`. Both must be refused with the
   stable code `unsupported`: an empty capability list is not an implicit
   wildcard. The `negotiation` defect accepts them and is caught.
3. **A gate is never a pass.** A capability-gated row whose capability was
   not echoed, or whose prerequisite the target does not provide, is
   `unavailable` with a reason — never `passed`. `Manifest.Validate` also
   refuses a **required** row that names a gate, so the required tier can
   never be satisfied by a skip.

### B23 — a protocol-version mismatch fails before any lifecycle mutation

**Proven**, `code` layer, by
`TestB23ProtocolVersionMismatchFailsBeforeAnyLifecycleMutation` and
`TestB23TargetDeclaringAnotherProtocolVersionIsNotJudged`, and by
`CONF-NEG-004` passing against the reference binary.

"Before any lifecycle mutation" is an ordering claim, so `CONF-NEG-004`
proves the absence of a mutation rather than the presence of an error. It
records the head of the canonical event log and the workspace inventory,
opens a connection whose hello carries frame version 2, requires that
connection to be refused, and then requires the head and the inventory to be
**unchanged**. Against the reference binary the connection is closed with no
response frame at all — `proto.DecodeFrame` rejects the version before the
relay ever sees a hello — and nothing moved.

The load-bearing proof is the `version-mutates` shim. That implementation
*does* refuse the connection, so a suite that only checked for an error would
pass it. It creates a workspace and appends an event first, and the test
asserts the resulting failure rests on the ordering claim ("mutated state
before failing" / "changed the workspace inventory") and **not** on
acceptance. That is what distinguishes this requirement from a plain error
check.

The second test covers the other side of a version mismatch: a target that
declares protocol `2` is not measured against the v1 manifest at all. The run
aborts, produces no results, is not conformant, and emits an `unavailable`
evidence record naming the mismatch.


## Manifest 1.1.0: stdout delivery became a required row

`CONF-SESS-009` was added on 2026-09-03 and the manifest document version moved
to 1.1.0. Every pre-existing requirement keeps `Version: 1.0.0`, because none of
their assertions changed meaning; only `Since` and the document version record
that the set grew.

It was added because the session category could be satisfied by an
implementation that never delivered any output. Every other row there asserts
the *shape* of the log — seq 0 is the info chunk, sequences are dense, the exit
chunk is last, a replay is byte-identical to the live tail — and all of those
hold vacuously for an empty stream. The gap was found while judging an OCI image
built `FROM scratch`, where `/bin/echo` does not exist: the run still reported
**50 of 52 required rows passed**, failing only the two rows that happen to need
a working program. An implementation whose exec silently produced nothing would
have been called conformant in every category but two.

The other categories were audited for the same weakness and do not share it,
which is recorded here so the audit is not repeated. `CONF-SNAP-003` writes a
seed file, snapshots, restores into a second workspace and byte-compares what
that workspace serves, so an implementation that snapshots nothing fails it.
`CONF-EVT-005` fails explicitly when a workspace that was created, claimed and
written to produced no events, and `CONF-EVT-004` requires `ws.created`,
`ws.claiming` and `ws.claimed` in that order rather than merely requiring that
whatever arrived be well formed. Sessions were the outlier: they were the one
category whose every row could be satisfied by silence.

`CONF-SESS-009` asserts the bytes arrive, arrive verbatim, arrive tagged `st=1`,
and arrive alongside the exit status of the program that produced them — so
truncated, re-encoded, or wrongly-streamed output fails the same row as absent
output. The shim gained a matching defect (`stdout`) that emits a structurally
perfect but empty log, and `testdata/failure-seeds.json` pins that the new row
fails it while `CONF-SESS-001/002/003/005/008` keep passing. Without that seed
the row would be untested against the thing it exists to catch.

## What this does not prove

Passing means an implementation satisfies the named semantic contract at
manifest `1.0.0`. It does not mean the implementation is secure, nor
production-qualified on any backend. In particular:

- Nothing here exercises lease expiry under node loss, durable session-log
  tiering, transcript eviction, event retention eviction, approve-mode
  egress, or outbound notification. Those rows are honestly `unavailable`,
  and closing them needs a runner that can inject node faults and a target
  whose retention bounds are configurable.
- The binding category is exercised against the node's own loopback broker.
  That is a public surface (§9 defines its URL shape and decisions), but it
  means a target on another machine reports those rows unavailable unless it
  exposes the broker to the runner.
- The suite judges the frame protocol, the HTTP surface and the event log. It
  does not currently drive the CLI, though `PrereqCLI` and `Target.CLI` are
  in place for a requirement that needs it.

## Observations from building this

Two things the suite surfaced that are not manifest failures but are worth
recording.

- **`s.input` treats `iseq: 0` as unsequenced rather than as sequence zero.**
  §8 says a node drops any `iseq` at or below the last one it applied; the
  reference node exempts `0` from the check entirely, so input sent at `0`
  after `5` is applied. The spec does not say which reading is intended, and
  a conformance suite must not fail an implementation on an ambiguity in the
  document it is enforcing, so `CONF-SESS-004` probes the rule with nonzero
  sequences only (apply 5, retry 5, then 3) and the ambiguity is left to the
  spec rather than resolved by the test. If §8 is amended to say what `0`
  means, `CONF-SESS-004` should gain a version and cover it.
- **`agent.destroy` racing materialization can leave a workspace stuck.**
  Destroying an agent whose workspace is still `claiming` produced
  `not_found: workspace ... not here`, then a workspace in `failed` that
  refused `ws.destroy` with `conflict: workspace requires reconciliation
  before destroy`. `CONF-AGT-008` now waits for the workspace to settle
  before destroying, because whether a destroy that races materialization is
  safe is a different obligation from whether a destroyed agent stays
  destroyed, and conflating them would report one failure as the other. The
  race itself is outside this ticket's file scope and is left as a finding.

## Gates

```sh
gofmt -l internal/conformance cmd/conformance          # clean
go build ./...                                          # ok
go vet ./internal/conformance/... ./cmd/conformance/...  # ok
go test -count=1 ./internal/conformance/... ./cmd/conformance/...
go test -race -count=1 ./internal/conformance/... ./cmd/conformance/...
staticcheck ./internal/conformance/... ./cmd/conformance/...   # zero findings, including -checks=all
```
