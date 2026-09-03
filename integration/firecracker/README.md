# Firecracker host integration

This lane is intentionally unavailable on ordinary GitHub-hosted and macOS
runners. It needs a dedicated Linux host with KVM, a trusted Firecracker and
jailer installation, an explicit cgroup, and the Remount guest kernel/rootfs.

Run the prerequisite gate first:

```sh
REMOUNT_FIRECRACKER_KERNEL=/var/lib/remount/vmlinux \
REMOUNT_FIRECRACKER_ROOTFS=/var/lib/remount/rootfs.ext4 \
REMOUNT_FIRECRACKER_CGROUP=/sys/fs/cgroup/remount \
integration/firecracker/host-gate.sh
```

Exit 77 means unavailable, never pass. A release claim additionally requires
the host-gated end-to-end test below; this prerequisite script alone is not a
Firecracker conformance result.

The node now composes the jailed Firecracker API, reflink-backed root disk,
node-owned network namespace/TAP, full disk+state+memory bundle, and versioned
vsock guest filesystem/session bridge. Build the guest binary and its immutable
manifest before constructing the root image:

```sh
CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o remount-guest ./cmd/remount
./remount-guest guest-manifest --binary ./remount-guest --output ./guest-manifest.json
```

Install that exact binary in the image and start
`remount guest-agent --workspace /workspace` as root after mounting the root
disk at `/workspace`. The manifest SHA-256, protocol version, vsock port, and
workspace path are the image contract. The guest holds no reusable host
credential; the host accepts only the well-known host vsock CID.

Firecracker must be registered explicitly with `remount up --backend
firecracker` and the `--firecracker-*` flags (or matching
`REMOUNT_FIRECRACKER_*` variables). Registration fails rather than advertising
microVM capabilities when KVM, trusted binaries/images, cgroups, reflink,
network namespace recovery, or guest manifest validation is unavailable.

The full self-hosted KVM scenario remains a release gate: fresh boot, a guest
filesystem RPC hitting the block disk, exec and PTY, denied direct egress,
successful broker egress, synchronous revoke of an in-flight connection, full
checkpoint, retained-source fence, fresh pre-boot VMM restore, process marker
continuity, output reattachment, and cleanup. The current workflow deliberately
fails its final step until that host scenario is checked in; do not interpret
the unit/race suite as that proof.

Implementation constraints follow Firecracker's official
[snapshot support](https://github.com/firecracker-microvm/firecracker/blob/main/docs/snapshotting/snapshot-support.md),
[snapshot versioning](https://github.com/firecracker-microvm/firecracker/blob/main/docs/snapshotting/versioning.md),
[vsock](https://github.com/firecracker-microvm/firecracker/blob/main/docs/vsock.md), and
[jailer](https://github.com/firecracker-microvm/firecracker/blob/main/docs/jailer.md)
documentation. Restore always uses a fresh pre-boot VMM. Remount uses full
snapshots only; it does not advertise preview diff-snapshot or userfaultfd
restore behavior.
