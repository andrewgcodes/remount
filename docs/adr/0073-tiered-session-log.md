# ADR 0073: Tiered session logs commit references before reclaiming disk

## Status

Accepted.

## Context

ADR 0003 made every session an append-only sequence and required explicit gaps
when retained output was gone. The original implementation bounded memory by
spilling to one bounded file, then rotated that file. Rotation was honest, but
a sufficiently long or noisy session eventually lost output. Keeping a larger
file only moved the threshold and made node disks the duration limit.

Session output must instead survive multi-day producers and remain replayable
from any sequence without making the session package depend on a concrete
artifact implementation. Publication also crosses a dangerous boundary: a
successful content-addressed upload is not yet a durable reference. Reclaiming
the local copy before the session record names the artifact lets ordinary blob
GC erase the only remaining copy.

## Decision

The log has three ordered tiers: bounded memory, a bounded local spill file,
and immutable `artifact.BlobStore` segments. The encoded segment limit is 64
MiB. A record is never split; configuration must leave room for one maximum
chunk and may select a smaller limit for tests. Closing a log flushes and seals
the remaining resident tail synchronously, so completion never abandons an
unjoined archival producer. The manager commits active-capacity accounting and
publishes `Session.Wait` while the log close boundary still excludes readers;
therefore EOF cannot race ahead of the capacity handoff.

Each artifact is represented by
`SegmentRef{First, Next, Artifact, Bytes}`. Ranges are monotonically contiguous,
the artifact id authenticates the encoded bytes, and the durable session record
also stores the format version and maximum chunk size required to validate the
records. `OpenArchivedLog` reconstructs a closed replay source using only that
record and a `BlobStore`. The reference list is capped at 65,536 segments
(four PiB at the default segment size); reaching the cap is an observable
storage failure rather than unbounded metadata growth.

Sealing uses this order:

1. sync the complete local segment;
2. `BlobStore.Put` it and verify the returned size with `Head`;
3. atomically replace the session record with the candidate reference set;
4. only after that commit succeeds, truncate the local spill bytes.

The record commit is idempotent. A failure before step 3 leaves the local range
authoritative and contiguous. A failure after the record commit may leave a
duplicate local copy, but never an unreferenced sole copy. Artifact objects are
not deleted by the session log because content addressing permits another
record to share them. Tenant retention first commits an empty reference set;
ordinary reference-aware artifact GC may collect the objects afterward. A
failed dereference keeps the retained session and its quota reservation and is
retried at a bounded cadence.

Blob reads occur without the append mutex. The complete immutable segment is
still parsed through EOF before any selected records are accepted, which both
validates sequence continuity and lets verifying `BlobStore` readers check the
content digest. Local spill records are likewise range- and sequence-checked.

`ErrTierUnavailable` names `blob` or `disk` and the exact unavailable
half-open sequence range. It unwraps to `ErrEvicted` for compatibility, so an
older node still emits a gap instead of silently ending replay. The protocol
gap body gains the tier name when the node integration lands. A configured and
available artifact tier therefore prevents capacity eviction; gaps represent
an unavailable tier or deliberate tenant retention, never silent rotation.

`ManagerOptions.RetentionForTenant` selects retention without trusting a
workspace. `BlobStoreForTenant` must return a tenant-scoped view (for example,
the encrypted store's tenant adapter); a fleet-wide unscoped encrypted store is
not accepted by this seam. `LogStats` exposes memory, disk and referenced blob
use plus the unavailable tier for node diagnostics and metrics.

## Alternatives rejected

- **Increase or rotate a single spill file.** This still creates a duration
  limit and permanent gaps under normal successful operation.
- **Store every output chunk as an artifact.** It turns one-byte PTY writes
  into unbounded object counts and request overhead.
- **Delete segment objects at session expiry.** Digest deduplication means the
  same object may be referenced elsewhere; only reference-aware GC can decide.
- **Upload asynchronously and close the session immediately.** Cancellation is
  not completion. It reintroduces an unjoined producer and makes `Wait` race
  archival publication.
- **Persist artifact ids after deleting disk bytes.** The crash window contains
  no authoritative copy and violates the central lifecycle commit rule.

## Invariant

A session-log range moves from disk to blob only after the complete immutable
artifact and its contiguous reference are durably committed. Retention drops
the reference before capacity is released. Replay returns byte-identical
ordered chunks or an explicit gap naming the unavailable tier and exact range;
it never reports incomplete output as complete.

## Verification

`internal/session` tests force many small memory/disk/blob transitions with a
fast producer, reopen from the durable record, replay from every sequence,
race concurrent append against remote replay, inject reference-commit and blob
read failures, and prove per-tenant retention does not release capacity or
references when the durable dereference fails.
