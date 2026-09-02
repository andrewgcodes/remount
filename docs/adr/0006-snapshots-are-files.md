# 6. Snapshots are files, never memory

**Status:** accepted

## Context

"Move a running workspace" invites the reading: move the running processes too.
Live migration exists. CRIU checkpoints process trees, vMotion moves VMs,
Firecracker snapshots include guest memory, and several sandbox vendors resume
with RAM intact in under a second.

## Decision

A Remount snapshot is a deterministic tar.gz of the workspace filesystem. It
contains files, directories, symlinks, modes and mtimes. It never contains
process memory.

Deterministic means sorted entries, zeroed uid and gid, PAX format, so the same
tree always produces the same content-addressed id.

## Consequences

Snapshots are portable across nodes, operating systems, CPU architectures and
vendors, because a tarball is portable and a memory image is not. Moving from a
Mac to a Linux GPU box is the same operation as moving between two Linux boxes.

Processes do not survive a move. The harness restarts them, which it can do
because the event log tells it exactly where it was. This is the honest version
of the tradeoff, and it is stated in the protocol rather than hidden: a node
advertises `snapshots: fs`, and if a backend ever supports memory snapshots it
advertises `fs+mem` and the harness can know the difference.

Same-vendor pause and resume could later use a native mechanism for speed. That
is an optimization within a vendor, not the portable path, and the portable path
must never depend on it.
