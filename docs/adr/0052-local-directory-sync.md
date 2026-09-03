# ADR 0052: Local directories travel as artifacts; overlays land by rename

## Status

Accepted.

## Context

Until now a workspace started empty or from a snapshot another workspace had
produced. The plan's first Phase 1 item is the everyday case: a developer has a
checkout on a laptop, wants an agent to work on it on some node, and wants the
result back. Three shapes were considered.

- **A file-by-file sync over `fs.write`.** Simple, but one round trip per
  file, no atomicity across the set, and every node would learn a new
  chunking scheme for large files.
- **A dedicated upload op on the node link.** Adds a second bulk-transfer
  path next to the artifact store, with its own limits and digest handling.
- **Reuse the artifact pipeline.** The local tree becomes exactly the archive a
  snapshot would produce; it goes through `PUT /v1/artifacts/{id}` with the
  same size limit and digest check; seeding is `restore_from`; updating is a
  new node op that applies the same archive as an overlay.

## Decision

1. **One archive format.** `localfs.Pack` filters a directory and hands the
   walk to `artifact.SnapshotFiltered`, the same writer `ws.snapshot` uses.
   Whatever the pack produces, `artifact.Restore` and `ApplyOverlay` accept
   under the same containment and expansion limits as a node restore. There is
   no second tar reader.

2. **Filtering is the developer's, not the node's.** `.gitignore` and
   `.remountignore` (same syntax, nested files honored, negation supported) are
   evaluated on the client, plus a default exclude list (`node_modules`,
   `.venv`, `target`, `dist`, `__pycache__`) that can be turned off. `.git` is
   included by default because agents commit; a `.git/objects/pack` over
   100 MiB is skipped with a warning rather than uploaded silently. `.remount/`
   is never packed: it is node-owned and rewritten on materialize.

3. **`Pack` writes to an `io.Writer`.** The plan sketched
   `Pack(dir, opts) (io.Reader, Manifest, error)`. A manifest is only known
   when the walk finishes, so a reader-returning API would have to buffer the
   whole archive or lie about counts. The CLI pipes the writer straight into
   `UploadArtifact`, which spools to a temporary file to compute the id, so
   memory stays flat regardless of tree size.

4. **`fs.apply_tar` validates everything, then renames.** The node fetches the
   artifact (before taking the tree lock, so a slow download never stalls other
   file operations), extracts it into a private staging directory under
   `.remount/`, and checks every destination: no `.remount/` writes, no
   symlinked parent, no file replacing a directory or directory replacing a
   file. Only then are directories created and files moved, each by one
   rename. A refusal on the last entry writes nothing. Paths the archive does
   not name are untouched: push is an overlay, not a mirror, because deleting
   what the agent created while the developer was editing is the surprising
   outcome.

5. **Pull never deletes either.** `localfs.Unpack` refuses a dirty Git
   checkout, or a non-empty non-Git directory, without `--force`; compares by
   type, mode, size and content hash; writes only what differs, each by
   rename; and reports local-only paths instead of removing them.

6. **Every written path is an event.** One `fs.apply_tar` summary (with
   `complete:false` if a rename failed after validation passed) and one
   `fs.write` per landed path, so an audit reader sees a push exactly the way
   it sees `fs.write` calls.

7. **Downloads are verified at EOF.** `client.DownloadArtifact` hashes as it
   streams and fails the final `Read` with `ErrDigestMismatch`, so a pull that
   consumed a substituted or corrupted body cannot finish cleanly.

## Consequences

- The artifact store's `MaxArtifactBytes` bounds pushes the same way it bounds
  snapshots; there is one knob.
- Overlays are file-atomic, not set-atomic: a rename that fails after
  validation (disk full, I/O error) leaves earlier files in place and the
  event says so. Set-atomicity would need a second tree and a directory swap,
  which the process backend cannot do under a live container mount.
- A developer who wants deletions to propagate should use a fresh workspace
  (`ws create --dir`) rather than `push`.

## Tests

`internal/localfs/localfs_test.go` (ignore semantics, determinism, large pack
warning, unpack identity, dirty refusal), `internal/artifact/overlay_test.go`
(hostile archives, late-entry refusal writes nothing, symlinked parents,
type conflicts), `internal/sim/localfs_test.go` (pack → upload → seed →
byte-identical read-back with exclusions absent → push overlay → idempotent
replay → pull → dirty refusal → events).
