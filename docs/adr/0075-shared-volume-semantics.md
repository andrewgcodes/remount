# ADR 0075: Shared data volumes are immutable versions with read-only mounts

## Status

Accepted.

## Context

Agents commonly reuse datasets, dependency caches and generated indexes across
workspaces. Copying the same tree into every workspace wastes transfer and
storage, but exposing a live writable filesystem to several workspaces creates
a distributed-filesystem product: it adds cache-coherence, writer election,
network-partition and snapshot-consistency semantics to Remount's existing
workspace authority problem.

The workspace snapshot contract is deliberately simpler. An authoritative
checkpoint is one quiesced filesystem value committed at one generation. A
shared path whose bytes can change independently would make that checkpoint
ambiguous: restoring the workspace could silently produce different bytes.

Linux bind mounts also require care. A bind operation initially inherits the
underlying mount options; read-only must be applied as a per-mount remount, and
mount propagation must be isolated. The Linux `mount(2)` documentation defines
the `MS_BIND | MS_REMOUNT | MS_RDONLY` sequence, while the mount namespace
documentation explains why propagation and locked read-only flags matter:
[mount(2)](https://man7.org/linux/man-pages/man2/mount.2.html) and
[mount_namespaces(7)](https://man7.org/linux/man-pages/man7/mount_namespaces.7.html).
NFS may be the trusted host's storage substrate, but its client cache and
server consistency behavior do not become Remount lifecycle authority; Linux's
NFS export documentation explicitly describes weak cache-consistency metadata:
[Making Filesystems Exportable](https://docs.kernel.org/filesystems/nfs/exporting.html).

## Decision

### Model and authority

A logical volume is:

```text
Volume{id, tenant, current_artifact, current_version}
Version{number, artifact, publisher_workspace, publisher_generation}
VolumeMount{volume, version, artifact, workspace, generation, path, read_only}
```

The control-plane SQLite row is the distributed authority. The current artifact
and its version advance in the same `control.transact` transaction as the
`volume.created`, `volume.published`, `volume.removed`, `volume.attached` or
`volume.detached` event. Node-local `internal/volume.LocalBackend` state is a
crash-recovery/materialization record only; it must not be used to override a
control row or to infer tenant access. The volume capability is not advertised
until the protocol and control integration use that transactional boundary.

Every operation is tenant-scoped. Tenant isolation is checked before lookup,
so an id in another tenant is indistinguishable from an absent id. Mutations
carry an operation id. An exact replay returns the committed result even at
quota; reuse with changed arguments is a conflict. Workspace operations carry
the exact generation, and a node advances its local workspace fence before it
can serve a new generation. A stale generation cannot publish, attach or
detach.

### Immutable versions and the exclusive writer

There is no writable shared mount. Every attached version is an immutable,
tenant-scoped artifact and every workspace mount is read-only. The only write
operation is:

```text
volume publish WS PATH
```

When `PATH` is not the volume's current read-only mount point, the CLI requires
`--volume ID`. A publisher can therefore copy a pinned input to a writable
workspace directory, modify it, and publish that directory without ever making
the shared mount writable.

The node quiesces managed sessions, holds the workspace tree boundary, archives
`PATH` to the configured artifact store, verifies the artifact, and asks control to compare-and-swap
version `N` to `N+1` for that workspace and generation. The artifact upload is
content addressed, so losing the compare-and-swap leaves only an unreferenced
object inside the normal artifact GC grace window. Exactly one publisher can
win a version. There is no distributed writer lease and no last-write-wins
fallback.

Existing mounts remain pinned to their recorded version and artifact. A publish
therefore never changes bytes underneath a running process. New attachments
select the new current version. A caller that wants every workspace to see the
new value explicitly detaches and reattaches each workspace after the publish.

### Attach, detach and move

Control durably records desired attachment state before a node performs host
mount work. On the node, an attach has the following boundary:

1. Persist `{workspace, generation, volume, version, artifact, path,
   state=attaching}`.
2. Open the workspace root, every target component and the immutable source
   with descriptor-rooted `openat(O_NOFOLLOW)` traversal; reject `..`,
   non-canonical paths, `.remount`, symlinks and a non-empty target. Persist the
   opened target's device/inode identity with the attachment.
3. Install a private bind mount and verify that the exact target is mounted
   read-only. `nodev` and `nosuid` are also applied on Linux.
4. Persist `state=attached` before reporting success.

Detach persists `state=detaching`, reopens the target without following links,
requires its device/inode identity to match, synchronously unmounts and verifies
absence, then removes the local record. A missing, renamed or replaced path
retains the record and fails closed because the original mount may still exist.
Cancellation is not completion. An uncertain
attach/detach remains fenced in its intermediate state and is inspected on
restart. Recovery commits an `attaching` record only when the exact mount is
present and read-only; it otherwise unmounts or removes the incomplete record.
An `attached` record whose host mount disappeared is rebuilt from its pinned
artifact. A generation advance synchronously drains every older mount before
the new workspace generation becomes locally serviceable.

A workspace move preserves `{volume, version, artifact, path}` in the
authoritative workspace specification. The source node drains old-generation
mounts; the destination restores the workspace checkpoint without volume bytes,
then attaches the same pinned artifact under the new generation. Failure to
mount on the destination keeps `ws.ready` false. It does not roll forward to a
newer volume version and does not make the source safe to destroy.

Release and fleet quarantine persist a typed node-local recovery intent with
the control-derived tenant, backend and exact volume declarations. Phase one
does not acknowledge until the checkpoint producer and managed sessions have
joined and every pinned mount is synchronously absent. A normal release abort
reattaches and verifies those exact pins before the workspace or broker resumes.
Control retains the physical node and volume references through destroy until
the node's durable, idempotent destruction tombstone is acknowledged; only then
does one transaction clear them and emit pinned `volume.detached` events.

The built-in Linux mount engine currently composes only with the process
workspace backend. It accepts any verified host directory as a source; that
directory may live on local storage or on NFS mounted and managed by the
trusted host. Remount does not mount an arbitrary caller-supplied NFS endpoint,
place credentials in a workspace, or claim that NFS provides writer fencing.
Docker, gVisor and Firecracker do not advertise `readonly-volumes` until each
has an end-to-end proof that the mount is visible and read-only inside its own
isolation boundary. Non-Linux backends return `unsupported` unless they provide
an equivalent descriptor-rooted, enforceably read-only mount boundary.

### Snapshot boundary and retention

Volume bytes are excluded from workspace snapshots. A snapshot records only
the mount declaration and pinned `{volume, version, artifact}` reference in the
workspace/control record. Restore obtains and verifies that artifact before
`ws.ready`. This makes a restore deterministic even after later publishes.

Deleting a volume is refused while any active or intermediate attachment
references it. Deleting the catalog row dereferences its retained versions; the
artifact collector applies its ordinary grace period and must preserve versions
still pinned by workspaces, recovery records or quarantine. Version history is
bounded per volume. Oldest unpinned versions are pruned first; if every slot is
current or pinned, publish returns `resource_exhausted` rather than discarding a
live reference. Volume, attachment and idempotency journals have finite tenant
and process limits plus age retention for completed operations. Diagnostics
expose current counts, quota rejections, stale-generation rejections and
restart reconciliation counts.

### Lifecycle proof

| Boundary | Decision |
|---|---|
| Authority | control's tenant-scoped volume/version and workspace-generation rows |
| Resource at risk | an artifact reference, a host mount, or the only deterministic shared-data version |
| Fence | volume expected-version CAS plus workspace generation and operation id |
| Irreversible action | dereferencing a deleted version or removing the last source-node mount during a move |
| Durable commit point | artifact verification, then transactional control row/event commit; local attach/detach intent precedes host mount changes |
| Observable postcondition | `volume.*` event, current-version/mount inspection, exact read-only mount evidence and quota/reconciliation counters |

If artifact verification, control persistence, mount enforcement or
postcondition inspection is unavailable, the operation is unavailable or
failed—not healthy. A failed publish retains the old current version. A failed
move retains and fences the source according to the workspace lifecycle
protocol.

## Integration contract

The protocol and node wiring preserve these exact interfaces:

1. Add additive `Volume`, `VolumeVersion` and `VolumeMount` bodies; add
   `volume.create`, `volume.remove`, `volume.get`, `volume.list`,
   `volume.publish`, `volume.attach` and `volume.detach`; add
   `WorkspaceSpec.Volumes`. Every mutating
   body carries `idempotency_key`, `tenant` comes only from authenticated
   identity, and workspace mutations carry `generation`; node-facing publish
   requests additionally carry the generation-bound signed grant. Control
   re-verifies it and resolves its still-connected client identity, so the
   holding node cannot silently replace that actor with the workspace owner.
2. Store logical rows, version references and their events through
   `control.transact`. Enforce tenant/subject quotas before provider work and
   check idempotent replay before quota admission. Use expected-version CAS for
   publish.
3. On the node, call `SetWorkspaceGeneration` before attachments can be
   serviced. Materialize declared mounts after checkpoint restore and before
   `ws.ready`; detach them while the workspace tree boundary is still held on
   release, move, quarantine and destroy. Pass only a control-derived tenant,
   artifact and generation.
4. `volume publish` quiesces and joins managed sessions, archives the selected
   workspace subpath with `consistency=quiesced`, verifies its tenant-scoped
   artifact upload, then commits the CAS in control. It is not a failover
   checkpoint and therefore is not marked workspace-authoritative. The node
   keeps the tree boundary through the initial control commit.
5. Snapshot exclusion and artifact GC must understand mount targets and
   transitive pinned version references. `doctor --deep` verifies every pinned
   artifact and reports mount checks it cannot run as unavailable.
6. Advertise `readonly-volumes` only when create/publish, move/restore,
   tenant isolation, GC, diagnostics and a backend-enforced read-only mount all
   compose end to end. Process-mode directory permissions or a cooperative
   convention do not satisfy the capability.

`internal/volume.Backend` supplies the bounded reference lifecycle and
node-local recovery shape. `SourceResolver` must return an already verified,
immutable, tenant-scoped directory; `DirectoryResolver` provides symlink-safe
lookup under a local or host-mounted NFS root. `MountEngine` must operate on
opened descriptors and prove both presence and read-only state. The Linux
`BindMountEngine` is the built-in implementation.

## Consequences

- Many workspaces can share dataset or cache bytes without copying them into
  workspace snapshots.
- Publishers serialize through an explicit version CAS, and readers never see
  an in-place update.
- Users who require POSIX shared writes, distributed locks or immediate cache
  coherence need a separately operated distributed filesystem; Remount does
  not provide or imply those semantics.
- NFS availability and performance remain deployment properties. Its use as a
  host source does not relax artifact verification, tenant isolation, fencing
  or readiness.
- The local JSON recovery store assumes one node writer and atomic rename on a
  durable local filesystem. Distributed volume authority stays in control's
  SQLite transaction; placing the local recovery file itself on shared NFS does
  not create safe multi-writer authority.

## Verification

Focused tests cover tenant isolation, read-only refusal and post-mount proof,
single-winner concurrent publish, pinned-version behavior, attach-versus-delete
ordering, generation fencing and synchronous stale-mount drain, canonical path
validation, source and target symlink escape, idempotent replay at capacity,
volume/mount/version/operation bounds, reference-aware version retention,
stable list/inspect copies, and restart reconciliation for missing, attaching
and detaching mounts. The package is exercised repeatedly under the race
detector. The simulator publishes changed bytes as a new version, proves an
existing workspace remains pinned, moves that workspace between real process
nodes, verifies exact bytes and generation, confirms source teardown, and
confirms mounted data is excluded from the move snapshot. Privileged Linux
unit tests separately prove bind-mount visibility and read-only enforcement
when the host permits the mount operation; unsupported hosts skip that
host-gated proof.
