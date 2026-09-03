# ADR 0063: Firecracker checkpoints compose four probed host boundaries

## Status

Accepted as an interface and lifecycle decision. The backend remains
unregistered and the KVM lane reports unavailable until all host adapters and
the guest image are integrated and exercised together.

## Context

The `multi_tenant` profile needs a microVM boundary, non-bypassable egress and
process-preserving checkpoints. Firecracker provides KVM isolation, file-backed
block devices, TAP networking and full/diff memory snapshots, but those pieces
do not by themselves satisfy Remount's `workspace.Handle` contract.

In particular, Firecracker has no virtio-fs device. A writable ext4 image
cannot be mounted by the host for `*fsops.FS` while the guest writes it. A guest
also needs a protocol behind `Prepare` for exec, PTY and port sessions. Treating
either missing bridge as implemented would advertise isolation over a backend
that cannot actually serve the workspace.

## Decision

1. The backend composes four explicit, probed interfaces: a jailer-only
   `MachineFactory`, a node-owned `NetworkProvider`, a coherent CoW
   `VolumeProvider`, and a `GuestExecutor`. `New` also requires explicit kernel
   and base-rootfs artifacts. Any missing or failed probe makes the backend
   unavailable; an unverified value advertises only `none/open/fs`.

2. The network provider reserves the host-veth address before the broker binds.
   `ApplyNetworkPolicy`, carrying the authoritative workspace generation,
   prepares a TAP inside ADR 0061's deny-first namespace. The VM is configured
   or restored while that link cannot egress, and the broker-only permit is
   activated last. Failure kills and joins the VM and synchronously revokes the
   network. Firewall logic is not duplicated in this backend.

3. A fresh VM is configured with an explicit kernel, its per-workspace CoW root
   drive, the prepared TAP, dirty-page tracking, CPU and memory limits; it then
   starts. A restored VM is fresh and otherwise unconfigured: it loads the
   saved VMM state and memory before resume, as required by the Firecracker API.
   Process preservation may be reported only after both load and resume
   succeed.

4. `Checkpoint` pauses the VM, asks Firecracker for a full VMM and memory
   snapshot, then asks the volume provider to archive the filesystem/block
   state and those files. The archive producer is synchronous and joined before
   resume or return. Resume failure is joined with the primary error. The
   resulting kind is `fs+mem`; `Snapshot` remains a live filesystem-only
   diagnostic operation.

5. Destruction orders synchronous network revoke, VMM kill/join, then volume
   removal. The protected resources are the TAP/netns, KVM VM, memory state and
   CoW disk. Workspace generation and node handle ownership fence stale work.
   The irreversible disk removal follows the node's durable lifecycle commit,
   not snapshot production. Observable completion is a terminated VMM, absent
   TAP and removed volume; ambiguity retains the later resources for retry.

6. Only a backend constructed after successful KVM, jailer, snapshot API,
   network, CoW volume and guest-agent probes advertises
   `microvm`, `fs+mem`, `enforced_gateway`, network/device/sibling isolation and
   multi-tenant support. The real lane is host-gated. Missing `/dev/kvm`,
   binaries, artifacts or adapters is `unavailable` and deliberately fails the
   opt-in workflow rather than rendering a green check.

## Consequences

- Root integration must implement the narrow TAP adapter on `internal/netns`,
  a reflink/overlay volume whose host and guest views are coherent, and the
  versioned vsock guest-executor protocol shipped in the base image.
- The node registers the backend only when `firecracker.New` succeeds and adds
  `/dev/kvm`, jailer/firecracker versions, artifact identity, reflink,
  snapshot/load and network probes to `doctor`.
- `ws.moved.processes="preserved"` is emitted only for a successfully restored
  `fs+mem` checkpoint. Every filesystem-only backend remains `restarted`.
- Diff snapshots remain a later optimization. The first correctness lane uses
  full snapshots; Firecracker's diff format is developer-preview and requires
  ordered base/layer merging.
