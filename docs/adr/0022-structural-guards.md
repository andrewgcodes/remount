# ADR 0022: Structural guards ahead of the durable runtime

## Status

Accepted.

## Context

Phase 1 and later add memory checkpoints, remote blob storage and audit export.
Each of those is a second implementation of something that today has exactly
one: `Checkpointer` is satisfied only by the process and docker handles,
`artifact.Store` is the only blob store, and the event log is read only by
`Read`. Adding the second implementation later means touching every caller
that assumed the first, and that is where the "which kind of snapshot is
this?" and "did the export skip a hole?" bugs come from.

## Decision

Three contracts are fixed now, before any second implementation exists.

1. `workspace.CheckpointKind` (`fs`, `fs+mem`) is the vocabulary shared by
   `Caps.Snapshots`, the snapshot record and the restore path. A handle that
   captures memory implements `MemoryCheckpointer`; everyone else asks
   `KindOf`, which returns `fs` for any handle that does not make a known
   claim. A backend cannot be recorded as producing a memory snapshot by
   accident, and callers never type-assert on a concrete backend.
2. `artifact.BlobStore` (`Put`, `Open`, `Head`, `Delete`, `List`) is the
   content-addressed contract. Ids are computed by `Put`, so no caller can
   store bytes under a name that does not verify. `Head` exists so a caller
   can size a blob without opening it, which is what an object store's HEAD
   request is. `artifact.Store` is asserted to satisfy the interface at
   compile time.
3. `eventlog.Exporter.Export(ctx, from, to, sink)` is the one path an audit
   export takes. It pages through the store, stops at the first sink error
   and returns the last sequence delivered so the caller can resume. A `from`
   below retention is `CodeEvicted` with `Oldest`, the same signal `Read`
   gives a subscriber: an export never silently starts after a hole.

No caller changes. The interfaces are guards for the implementations that
Phase 1, Phase 2 and Phase 4 add.

## Consequences

- A new backend, blob store or export sink compiles against a contract that
  already has tests, and a mismatch is a compile error rather than a runtime
  surprise.
- `KindOf` is deliberately conservative: an unknown kind string degrades to
  `fs`. A backend that wants credit for a memory snapshot must say exactly
  `fs+mem`.
- `Export` holds no lock across the whole range, so a long export and live
  appends interleave. The `to` bound is fixed at the start when zero, so an
  export is a snapshot of the log at the moment it began.
