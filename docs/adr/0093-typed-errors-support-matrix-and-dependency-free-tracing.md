# ADR 0093: Typed errors, a generated support matrix, and dependency-free tracing

## Status

Accepted.

## Context

Remount's core works. What kept it from being adopted as a default runtime SDK
was the product layer around it: an adopter could not tell which failures were
retryable without reading Go source, could not tell which backend ran on which
operating system without reading build tags, and could not put a request's
latency into the tracing system they already run.

Three specific gaps, each with the same shape — the information exists inside
the tree but is not published in a form a caller can act on.

**Errors were a code and a sentence.** ADR-era commit 0ffc725 added a stable
`Reason` beside `Error.Code`, and the control plane and node started setting it
at new failure sites. Nothing consumed it. `internal/client` still decided
whether an `unauthorized` answer was worth retrying by comparing
`err.Msg` against three exact English sentences — the precise thing
`AGENTS.md` forbids, sitting in the SDK that teaches the rule. Python and
TypeScript raised one untyped `ProtocolError` for every failure, so a caller
who wanted to handle "the broker refused this host" differently from "you lack
the role" had to compare strings too.

**The capability matrix answered a security question, not an adoption
question.** `docs/security-profiles.md` was generated from real `Caps` and said
which backend satisfied which placement profile. It said nothing about which
operating system can host a node, which backends run there, or which of
sleep/wake, move, memory snapshot, computer sessions, brokered credentials and
enforced egress a given backend actually has. Those facts were spread across
build tags, `runtime.GOOS` checks, a CI job summary and four documents.

**There was no way to trace a request.** Metrics answered "how many"; the event
log answered "what happened to this workspace". Neither answered "where did
this one request spend its time, and which node served it". The obvious fix —
the OpenTelemetry Go SDK — is a large dependency tree for a project whose
metrics layer is 345 lines of atomics precisely to avoid one.

## Decision

### One typed error per reason, in all three languages

`api` re-exports every `proto.Reason*` constant and adds
`api.Is(err, code, reason)` beside the existing `api.ErrorCode` and
`api.ErrorReason`. A test parses `internal/proto` and this package and fails
when the two vocabularies diverge, because a public SDK whose reasons lag the
wire forces a caller back into `internal/`.

`cmd/protogen` now emits the reason vocabulary into the generated
`sdk/python/src/remount/types.py` (`Reason` as a `Literal`, plus `REASONS`) and
`sdk/typescript/src/types.ts` (`Reason` as a union, plus `REASONS`). Only the
class tables are hand-written; the vocabulary stays generated, so a new reason
appears in both packages by regeneration and each package's test fails until a
class exists for it.

`sdk/python/src/remount/errors.py` and `sdk/typescript/src/errors.ts` hold
`ProtocolError` and one subclass per reason, plus a factory
(`raise_for` / `fromWire`). Both clients route their wire and HTTP error paths
through the factory. `ProtocolError` moved out of each client module into the
errors module so the factory and the class can live together without an import
cycle; the names are re-exported from the package root, so no caller's import
changes.

**An unknown reason degrades to the base class.** A client older than its
server will see reasons it has never heard of. Raising `KeyError` from a table
lookup, or refusing the response, would turn a forward-compatible protocol into
a hard version pin. The factory returns `ProtocolError` carrying the unknown
reason verbatim, and every language's test asserts it.

### `staleGrantAuthority` matches on reason

`internal/client` retried an `unauthorized` node answer only when its message
was one of three exact sentences. It now matches `Reason`:

- `revoked` — the grant's authorization revision is behind the node's.
  Revocation is what advances that revision, so this is the reason the node
  already set at the tenant/revision site; the second site, which ends a
  session opened in the authorization window, now sets it too.
- `generation_mismatch` — the grant's controller epoch is behind the node's.
  The controller epoch is the control plane's writer-lease generation, so a
  grant minted under an older epoch names a stale generation in exactly the
  sense the reason describes. The code at that site is unchanged.

Everything else under `unauthorized` — a grant for the wrong workspace, a bad
signature — is final and is returned rather than retried, which is what the
message match did and what the new predicate does. The behaviour change is at
the edge of a version skew: a node old enough to send no reason at all no
longer gets the bounded convergence retry, and its `unauthorized` surfaces to
the caller. That is the correct direction to fail. A reason is additive on the
wire and every node in this tree sets one at all three sites.

The HTTP error body gained an `omitempty` `reason` alongside `code` and
`message`, so the same typed errors work for SDK callers going through the
agent API rather than the wire protocol.

### The support matrix is generated, and a new backend cannot skip it

`scripts/security-profiles` gained two sections.

The operating-system table is a declared fact table, because no `Caps` value
knows about an operating system: what gates a backend at run time is
`exec.LookPath("runsc")`, `internal/netns`'s non-Linux stub, and Firecracker's
explicit `runtime.GOOS != "linux"` checks. Each entry names its gate in the
source. Generation fails when a registered backend has no recorded operating
systems, mirroring the existing check that a registered backend has a
descriptor — so a new backend cannot reach the documentation without someone
stating where it runs.

The feature and suitability columns are derived from the descriptors a live
node actually advertises: `Snapshots` gives sleep/wake, move and memory
snapshot; `MountPath` or a host-rooted filesystem boundary gives computer
sessions, which is exactly what `node.computerCreate` requires;
`BrokerIdentity` gives brokered credentials; `EgressMode` gives enforced
egress. The four profile columns are `internal/profile` evaluated against that
backend alone with no host evidence.

**A verified Firecracker row appears beside the unverified one.** The registry
can only construct the fail-closed zero value on a host without KVM, and a
matrix showing only that understates the backend as badly as showing only the
verified capabilities would overstate it. `firecracker.VerifiedCaps` is now
exported and is exactly what `Caps` returns when verified, so the two cannot
drift, and `workspace.DescriptorFor` normalizes it through the same function a
live registry uses. The row is labelled, and its `microvm` column reads
`unavailable`, not `pass`: a document cannot run a host check.

### Tracing with no new dependency

`internal/trace` is a span recorder and an OTLP/HTTP exporter, together about
the size of `internal/metrics` and for the same reason. It records trace id,
span id, parent, name, start, end, string attributes and status, and posts
batches as JSON.

The wire format is OTLP's `ExportTraceServiceRequest` under the protobuf JSON
mapping, from `opentelemetry/proto/collector/trace/v1/trace_service.proto`,
`opentelemetry/proto/trace/v1/trace.proto` (`ResourceSpans`, `ScopeSpans`,
`Span`, `Status`, `SpanKind`, `StatusCode`) and
`opentelemetry/proto/common/v1/common.proto` (`KeyValue`, `AnyValue`). Two
encoding rules are not derivable from the generic protobuf JSON mapping and are
honoured explicitly: OTLP/HTTP specifies that `trace_id` and `span_id`, though
`bytes` in the schema, are hex strings rather than base64 in JSON; and 64-bit
fields, including the `fixed64` timestamps, are JSON strings. The exporter test
asserts both against an `httptest` collector, so a hand-written encoder cannot
drift from the shape a real collector accepts.

**The endpoint is always operator-configured.** `--otlp-endpoint` on `server`,
`up` and `standalone`, or `REMOUNT_OTLP_ENDPOINT`. There is no default, because
Remount runs no hosted service; unset means no span is recorded and nothing
leaves the process. A disabled tracer costs one nil check per request and
returns a nil `*Span` whose methods are all no-ops.

`proto.Frame` gained additive optional `trace` and `span` fields. The relay
stamps them on control-originated requests from the calling context, so a node
span is a child of the control span that caused it. They carry no authority:
nothing is authorized, ordered, fenced or routed by them, and a malformed value
starts a fresh trace rather than refusing the frame.

An end-to-end-encrypted frame carries neither, and needed no change to make
that true: `e2ee.Session.Seal` lists the clear outer fields exactly, so the new
ones are dropped rather than exposed to the relay. A peer that paid for payload
confidentiality does not silently hand the relay a correlation id; its spans
are roots.

**Attributes are identifiers, and they are redacted anyway.** A span records
the op, the peer, the resolved principal, the workspace, the session, and the
stable code and reason of a failure. Never a token, never a body, never free
text. Every attribute value is nonetheless passed through `internal/redact` and
truncated on the way in, because a span leaves the deployment and a leak there
cannot be taken back. The test plants a synthetic canary, proves the scan finds
it, and then proves it is absent from the exported payload.

**Telemetry never applies back pressure.** A full export queue drops the span
and counts `remount_trace_export_failures_total`, which also counts spans lost
to a failed batch. `remount_trace_spans_exported_total` counts what a collector
accepted. Losing a span is always preferable to stalling the request that
produced it, and the loss is visible the way every other loss in this system
is: as a counter an operator can alert on.

### Logs pass through the same redaction

The `slog` handler in `cmd/remount` is wrapped so every message and string
attribute — including the string form of an error, a `Stringer`, a byte slice
and every attribute inside a group — goes through `internal/redact`. Numbers
and opaque structs are left alone: rewriting them would change the document
shape without protecting anything a credential travels in. This is defence in
depth, not the control that keeps secrets out of logs; the control is that no
component puts a secret in a log argument. The test plants a canary, asserts
the unwrapped handler emits it, and asserts the wrapped one does not.

## Consequences

A caller can now handle `EgressDenied` differently from `PermissionDenied` in
Go, Python and TypeScript without comparing message text, and the SDK that
teaches "never match on `Msg`" no longer does it itself.

An adopter can read one generated table to learn whether their operating system
can host a node, which backends run there, and what each backend can do — and
that table breaks the build rather than going stale when a backend's `Caps`
change.

An operator can point Remount at a collector they already run and see one span
per request, correlated across the control plane and the node, without Remount
gaining a dependency, a default endpoint, or any path by which telemetry
reaches anyone but them.

The costs are real and bounded. The OTLP encoder is hand-written, so a future
OTLP revision is a change here rather than a dependency bump; the exporter test
against a real HTTP server is what keeps that honest. The `trace`/`span` frame
fields are two more optional strings on every traced request. And the
message-text retry that survived a node with no reason is gone, which is the
change most likely to be noticed in a mixed-version fleet — noticed as a
returned `unauthorized`, never as a silent success.
