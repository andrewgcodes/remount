# ADR 0076: Event exports are bounded, resumable, and at least once

**Status:** accepted

## Context

The canonical event log is the audit product, but retaining it only in the
control-plane SQLite database does not make it usable by observability or SIEM
systems. Export must preserve the same retention-gap semantics as an event
reader, must not gather an unbounded history in memory, and must not report a
cursor past data that the destination never accepted. OTLP and SIEM endpoints
also carry destination credentials, so redirects, response bodies, and errors
must not turn into credential disclosure paths.

The source-side range contract was introduced in ADR 0049 as
`eventlog.Exporter`. This decision defines the destination and progress side of
that boundary.

## Decision

1. `RunExport` is the only event-to-destination pump. It asks an `Exporter` for
   a fixed retained range, verifies monotonic sequence continuity, forms
   bounded event/byte batches, and calls a `BatchSink`. A followed export
   repeats fixed snapshots; it does not keep an unbounded client-side queue.
2. Progress is the next sequence to deliver. A tenant-scoped `export.cursor`
   stores that sequence and a revision. The batch becomes visible at the
   destination first; only then may `CursorStore.CompareAndSwap` advance the
   revision and sequence. A crash between those actions can duplicate a batch
   but cannot skip one. Destinations are therefore idempotent where possible,
   and operators must treat delivery as at least once.
3. JSONL has one canonical encoding with a trailing newline included in its
   digest. CBOR payloads are decoded before JSON encoding. The same function is
   used for stdout, SIEM JSON, S3 objects, and the event download endpoint, so a
   content hash compares bytes rather than two interpretations of an event.
4. OTLP uses the standard-library HTTP client and OTLP/HTTP JSON. A base
   endpoint receives `POST /v1/traces`; protobuf 64-bit values are decimal
   strings and IDs are hexadecimal. Events in one Remount session share a
   deterministic trace ID. Session execution gets the applicable `gen_ai.*`
   identifiers; execution, filesystem, credential, egress, and other metadata
   remain in the `remount.*` namespace. Arbitrary event payload is deliberately
   not copied into span attributes because prompt, PII, or secret-like content
   is not safe telemetry metadata. This follows the
   [OTLP/HTTP JSON encoding and trace path](https://opentelemetry.io/docs/specs/otlp/)
   and the [GenAI attribute registry](https://opentelemetry.io/docs/specs/semconv/registry/attributes/gen-ai/).
5. S3 export consumes only `artifact.BlobStore`. Each bounded UTC-hour piece is
   immutable and content-addressed; an `ExportObjectRecorder` can atomically
   index `{hour, first, last, id, sha256, bytes}` without coupling eventlog to
   S3 credentials or provider types. A busy hour may have several pieces so an
   hour never becomes an unbounded in-memory batch.
6. SIEM export supports canonical JSONL and CEF lines over HTTPS. CEF omits the
   opaque payload and escapes all header/extension delimiters. HTTP exporters
   copy their client and headers, refuse redirects, reject credentials and
   query strings in endpoint URLs, cap request/response bodies, retry only
   transient transport/status failures with bounded jittered backoff, and never
   include headers, URLs, or response bodies in errors.
7. The in-memory cursor fake has a fixed name capacity. Production cursor rows
   are durable control-plane resources and their progress event commits in the
   same SQLite transaction. Cursor revisions never wrap. Object-store capacity
   and retention are owned by its configured `BlobStore`; remote SIEM/OTLP
   retention remains an operator policy outside Remount.

## Authority boundary

- **Authority:** the event log owns event sequence and retained-range truth;
  the control plane owns the tenant-scoped cursor row; the destination owns
  acknowledgement that a batch is externally visible.
- **Resource at risk:** audit events, cursor progress, destination credentials,
  and bounded process memory.
- **Fence:** event sequence continuity plus cursor name, next sequence, and
  revision compare-and-swap.
- **Irreversible action:** accepting an HTTP batch or publishing an immutable
  content-addressed object.
- **Durable commit point:** destination success precedes the transactional
  `export.cursor` advance and its event.
- **Observable postcondition:** the cursor's next sequence and revision, S3
  object metadata/digest, or a sanitized export error. A retained source gap is
  still `CodeEvicted`; it is never converted into apparent success.

## Consequences

Exports survive retries and process restarts without silent holes, at the cost
of possible duplicates after ambiguous acknowledgement or cursor-commit
failure. Receivers should deduplicate by event sequence (and S3 by digest).
The exporter adds no OpenTelemetry SDK or cloud SDK dependency and therefore
keeps the static binary contract, but it implements only the trace subset of
OTLP/HTTP JSON that Remount emits. Durable cursor CRUD and the canonical HTTP
download route are control/server integration points; neither destination
implementation is allowed to reach into control-plane state directly.
