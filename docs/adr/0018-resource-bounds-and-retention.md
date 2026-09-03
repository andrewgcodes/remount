# 18. Long-lived state is bounded and retention fails truthfully

**Status:** accepted

## Context

Per-operation limits do not keep a long-running service safe when workspaces,
sessions, event rows, artifacts, timers, replay records, keyed locks and staging
files accumulate forever. Silent truncation is worse: callers may believe they
received complete output or event history after retention removed it.

## Decision

Control enforces workspace quotas per tenant and subject, concurrent request
admission, timer quotas and durable mutation-record capacity. Nodes enforce
retained/active/per-workspace/per-principal session quotas, per-session memory,
spill, chunk and chunk-count limits, concurrent request/snapshot admission,
snapshot frequency, artifact capacity and connector byte/object capacity.
Reservation accounting includes in-flight uploads before bytes are accepted.
Keyed lifecycle, producer, fleet and mutation locks are reference-counted and
removed after the last waiter.

Background collectors run in bounded resumable batches. Artifact collection is
reference-aware and keeps live, recovery and quarantined snapshots. Event
retention applies both age and row count, deletes only a contiguous oldest
prefix, preserves producer high-water marks, and returns `evicted` plus the
oldest available sequence to stale readers. Control-record retention removes
completed mutations, fired timers, unreferenced destroyed tombstones, terminal
fleet operations, superseded assignments and legacy replay rows only after a
successful transaction. Nodes remove only owned orphan spill files at startup.

Diagnostics expose current use and configured maxima. Metrics count every quota
rejection, collector run/error and reclaimed object, byte or record.

## Consequences

Overload becomes a typed, observable failure instead of unbounded memory/disk
growth. Replay gaps are explicit and authority-bearing references survive GC.
Operators must choose limits and retention windows that match their recovery
and audit requirements.

The immutable managed-connector cache is capacity-bounded but has no automatic
age eviction in this version. Exhaustion fails closed until an operator clears
or replaces that node cache; future GC must preserve scope-private reference
semantics and avoid creating a cross-workspace cache-occupancy channel.
