# ADR 0072: Chunked snapshots are canonical manifests over plaintext FastCDC chunks

## Status

Accepted.

## Context

ADR 0006 defines a snapshot as a portable deterministic filesystem artifact.
The original representation is one tar.gz blob. That is an excellent interop
format, but changing four kilobytes in a 500 MB workspace requires hashing and
uploading a new 500 MB object, and the per-object 8 GiB safety bound becomes a
workspace-size ceiling.

Chunking must not weaken the content-addressing or verification contract. A
manifest is durable authority only after every referenced chunk is present;
missing data, malformed metadata, a partial background fetch, or a digest
mismatch must never be reported as a successful restore.

## Decision

### Plaintext identity and chunking

Chunk and manifest ids use the existing `art_sha256:<sha256>` form and are
always calculated over canonical plaintext. `internal/artifact/chunked`
depends only on `artifact.BlobStore`; local filesystem, S3, and tenant-encrypted
stores remain independent implementations of that contract.

File content is divided by normalized FastCDC with fixed format parameters:

- minimum chunk: 16 KiB;
- target chunk: 64 KiB;
- maximum chunk: 256 KiB;
- deterministic gear table and normalization masks identified as
  `fastcdc-v1`.

The chunker is independent of source reader buffer sizes and holds at most one
maximum-sized scan buffer plus the returned chunk. Empty files have no chunk.

Snapshot checks `Head` for each unique chunk id and uploads only missing
plaintext chunks. An existing id with the wrong size is corruption. Repeated
chunks within a snapshot are checked once. The canonical manifest is uploaded
last and only after every chunk write completed with the expected id and size.
Content-addressed chunks uploaded by an ultimately failed snapshot are
unreferenced staging results: they remain inside the BlobStore's ordinary grace
window and reference-aware garbage collection, because deleting them locally
could race another publisher that already referenced the same digest.

### Canonical manifest

The manifest is canonical JSON with fixed struct field order and no maps. Its
own logical id is the SHA-256 of those exact plaintext bytes. A decoder rejects
unknown fields, trailing input, non-canonical re-encoding, unsupported chunker
parameters, duplicate or unsorted paths, unsafe paths and symlinks, absent
directory parents, invalid modes, unsorted extended attributes, bad chunk ids,
inconsistent sizes, and inconsistent aggregate counts.

Every entry records its slash-separated path, type, permission mode, nanosecond
mtime, and sorted extended attributes. Regular files additionally record size
and ordered chunk references; symlinks record their relative target. The
manifest carries a sorted hot set of regular files touched by the previous
session.

Regular-file and directory xattrs are captured and restored through opened
file descriptors on Linux and macOS. Symlink xattrs are excluded because the
portable file-descriptor boundary cannot read them without reopening a
path-race. A platform unable to apply a non-empty xattr set fails restore rather
than silently dropping metadata.

### Bounds

The snapshot is no longer bounded by the 8 GiB size of one tar object. Finite
manifest limits instead bound entries, chunk references, canonical manifest
bytes, path bytes, attributes per entry, per-attribute and aggregate attribute
bytes, bytes per file, and bytes per snapshot. Defaults permit 1 TiB files and
a 16 TiB snapshot while bounding the canonical manifest to 512 MiB and four
million chunk references. Counts and conservative encoded-size budgets are
enforced while building as well as while decoding.

BlobStore admission and retained capacity still apply independently to each
chunk and manifest. Callers retain the existing node-level concurrent snapshot
admission and frequency limits.

### Restore, export, and lazy prefetch

Eager restore fetches every referenced chunk, checks the declared size and
plaintext digest, fsyncs a private sibling tree, and atomically replaces the
destination only after the complete tree succeeds. Cancellation or any missing,
truncated, extended, corrupt, or unavailable chunk leaves the old destination
in place. Cleanup after the durable directory swap is maintenance rather than
permission to report the new tree as failed.

`ExportTar` streams the same deterministic full tar.gz representation produced
by `artifact.Snapshot`. It exists for deep diagnostics, compatibility, and
systems that have not negotiated `chunked-artifacts`. If export returns an
error, the caller discards any partial output.

Lazy operation is a safe prefetch boundary, not a partially materialized
filesystem. `StartPrefetch` verifies and caches the manifest and hot-set chunks
synchronously, then uses one bounded background worker for unique cold chunks.
`Cancel` only requests termination. `Wait` joins the worker and returns its
terminal error; publication or eager restore from the cache happens only after
`Wait` succeeds. The current `BlobStore` interface has no context parameter, so
a blocked backend read may delay cancellation; that delay remains observable
because `Wait` cannot complete until the read returns.

### Capability and observability

Snapshot results report plaintext bytes, manifest bytes, uploaded bytes, total
chunks, and uploaded chunks. Integration maps these to
`snapshot_bytes_uploaded` and `snapshot_dedupe_ratio` at the node boundary,
where tenant and operation labels are authoritative.

The `chunked-artifacts` capability is advertised only after node snapshot,
control-plane reference tracking, client upload, restore/move, garbage
collection, and `doctor --deep` all understand manifests and retain their
transitive chunk references. An older peer continues to use full tar artifacts.

## Consequences

- A small edit normally uploads only the chunks around the edit plus a new
  manifest. The generated 500 MB acceptance fixture uploads under 1 MiB on its
  second snapshot.
- Manifest ids differ from legacy full-tar ids, while both remain plaintext
  `art_sha256` artifacts and export represents the same filesystem bytes.
- Garbage collection must treat a live manifest as a transitive reference to
  every chunk it names. Collecting only the manifest id would destroy a live
  snapshot.
- Directory walks still observe a live filesystem unless the caller holds the
  authoritative workspace tree lock. Chunking does not change checkpoint
  quiescence or durable commit rules.
- Extended attributes improve fidelity but can make a cross-platform restore
  explicitly unavailable when the destination cannot represent them.
- Lazy prefetch improves transfer overlap but does not yet provide demand-paged
  filesystem reads; presenting an incomplete tree would violate the snapshot
  contract.

## Verification

Focused tests prove deterministic reader-independent FastCDC boundaries,
minimum/maximum chunk bounds, eager round-trip, deterministic tar byte identity,
Head-based deduplication, a generated sparse 500 MB fixture whose second
snapshot uploads less than 1 MiB, hot-set-first prefetch, cancel-versus-join
ordering, canonical-manifest refusal, corrupt manifest/chunk refusal, resource
bounds, and race-safe concurrent operation. A seeded fuzz target exercises
hostile canonical-manifest decoding.
