# ADR 0053: Bases are pinned snapshots resolved at create time

## Status

Accepted.

## Context

Teams want one prepared workspace (toolchain installed, repo cloned, caches
warm) to be the starting point for many agent runs. ADR 0052 made a snapshot
artifact the unit of seeding, so the missing piece was a stable name for one
and a guarantee that it stays around. Three shapes were considered.

- **A label on the artifact store entry.** Cheap, but the artifact store is
  content-addressed and knows nothing about tenants or ownership, so every
  policy question would have leaked into `internal/artifact`.
- **A template workspace kept in `sleeping`.** Costs a workspace slot and a
  node's attention forever, and a template that can be woken can be mutated.
- **A small control-plane resource that pins an artifact under a name.** The
  workspace path is unchanged: `ws create --base NAME` is `restore_from` with
  the id looked up on the server.

## Decision

1. **`Base` is a durable control-plane resource.** `{name, tenant, owner,
   artifact, workspace?, bytes, created_at}` in its own SQLite table, loaded on
   start, keyed by `(tenant, name)`. Names are CLI-safe
   (`^[A-Za-z0-9][A-Za-z0-9._-]{0,63}$`). `base.create` verifies the
   artifact's digest before pinning; a base can never name bytes the control
   plane does not hold.

2. **Pins are GC roots.** `Control.WithArtifactReferences` lists every base
   artifact alongside workspace `restore_from` and `last_snapshot` ids. That is
   the only thing a base adds to retention: `base rm` drops the pin, and the
   artifact then lives exactly as long as some workspace still references it.

3. **Resolution happens once, at `ws.create`.** The control plane rewrites
   `Spec.RestoreFrom = base.Artifact` and stores the resolved spec. Workspaces
   never depend on the base after creation, so removing or re-pointing a base
   cannot change what an existing workspace restores from after a move.

4. **The idempotency fingerprint covers the request as sent.** A client that
   retries `ws.create {base: golden}` after the base was removed must get its
   original workspace, not `not_found` and not a "different arguments"
   conflict. The mutation record therefore stores the pre-resolution request.

5. **Tenant-shared read, owner-gated remove.** Any member of the tenant may
   list a base or start from it — that is the point of a base. Only the owner
   or an admin may remove one, because removal changes retention for everyone
   in the tenant. A pluggable `Authorizer` sees `Resource{Kind: "base"}` for
   both decisions and is consulted outside `control.mu`; the resolved artifact
   is re-checked under the lock so a concurrent `base rm` cannot slip a stale
   id through.

6. **Bounded.** `MaxBasesPerTenant` (default 256) refuses the excess with
   `resource_exhausted` and counts `remount_base_quota_rejections_total`, per
   the rule that every retained collection has admission and an observable
   rejection.

7. **Events.** `base.created` and `base.removed` carry `tenant` and use the
   base name as `stream`; the originating workspace is payload only, so a base
   built from a since-destroyed workspace still has a coherent history.

## Consequences

- `remount ws snapshot WS --as-base NAME` is two operations: an upload
  snapshot, then `base.create`. If the second fails the snapshot artifact
  exists and is reported; the CLI says so rather than pretending the base was
  made.
- A base names an artifact, not a workspace: re-snapshotting the source does
  not move the base. Update a base by `base rm` + `--as-base` again.
- Bases pin storage indefinitely. Operators watch the base count per tenant
  and the quota metric; there is no time-based expiry by design.
