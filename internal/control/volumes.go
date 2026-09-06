package control

import (
	"context"
	"crypto/ed25519"
	"sort"
	"strings"

	"remount.dev/remount/internal/eventlog"
	"remount.dev/remount/internal/metrics"
	"remount.dev/remount/internal/proto"
)

func volumeKey(tenant, id string) string { return tenant + "\x00" + id }

func sameVolumeSubject(a, b Subject) bool {
	if a.ID != b.ID || a.Tenant != b.Tenant || len(a.Roles) != len(b.Roles) {
		return false
	}
	for i := range a.Roles {
		if a.Roles[i] != b.Roles[i] {
			return false
		}
	}
	return true
}

func volumeResource(volume *proto.Volume) Resource {
	return Resource{Kind: "volume", ID: volume.ID, Tenant: volume.Tenant, Owner: volume.Owner}
}

func cloneVolume(volume *proto.Volume) *proto.Volume {
	if volume == nil {
		return nil
	}
	cp := *volume
	cp.Versions = append([]proto.VolumeVersion(nil), volume.Versions...)
	return &cp
}

func (c *Control) volumeEvent(typ string, volume *proto.Volume, principal string, payload map[string]any) *proto.Event {
	if payload == nil {
		payload = map[string]any{}
	}
	payload["volume"] = volume.ID
	payload["artifact"] = volume.Artifact
	payload["version"] = volume.Version
	e := c.newEvent(typ, volume.ID, principal, "", payload)
	e.Tenant = volume.Tenant
	return e
}

func stampVolumeWorkspaceEvent(event *proto.Event, workspace *proto.Workspace, operationID string) *proto.Event {
	event.Stream = workspace.ID
	event.Workspace = workspace.ID
	event.Generation = workspace.Generation
	event.Node = workspace.Node
	event.OperationID = operationID
	return event
}

func (c *Control) destroyedVolumeDetachEvents(workspace *proto.Workspace, mounts []proto.VolumeMount, principal, node, operationID string) []*proto.Event {
	return c.volumeDetachEvents(workspace, mounts, principal, node, operationID, "workspace.destroy")
}

func (c *Control) fleetDestroyedVolumeDetachEvents(workspace *proto.Workspace, mounts []proto.VolumeMount, principal, node, operationID string) []*proto.Event {
	return c.volumeDetachEvents(workspace, mounts, principal, node, operationID, "fleet.quarantine")
}

func (c *Control) volumeDetachEvents(workspace *proto.Workspace, mounts []proto.VolumeMount, principal, node, operationID, reason string) []*proto.Event {
	events := make([]*proto.Event, 0, len(mounts))
	for _, mount := range mounts {
		volume := cloneVolume(c.volumes[volumeKey(workspace.Tenant, mount.ID)])
		if volume == nil {
			volume = &proto.Volume{ID: mount.ID, Tenant: workspace.Tenant, Artifact: mount.Artifact, Version: mount.Version}
		} else {
			volume.Artifact = mount.Artifact
			volume.Version = mount.Version
		}
		event := stampVolumeWorkspaceEvent(c.volumeEvent(proto.EvVolumeDetached, volume, principal,
			map[string]any{"ws": workspace.ID, "generation": workspace.Generation, "path": mount.Path, "reason": reason}), workspace, operationID)
		event.Node = node
		events = append(events, event)
	}
	return events
}

func (c *Control) volumeAttachEvents(workspace *proto.Workspace, mounts []proto.VolumeMount, principal, operationID, reason string) []*proto.Event {
	events := make([]*proto.Event, 0, len(mounts))
	for _, mount := range mounts {
		volume := cloneVolume(c.volumes[volumeKey(workspace.Tenant, mount.ID)])
		if volume == nil {
			volume = &proto.Volume{ID: mount.ID, Tenant: workspace.Tenant}
		}
		volume.Artifact = mount.Artifact
		volume.Version = mount.Version
		event := stampVolumeWorkspaceEvent(c.volumeEvent(proto.EvVolumeAttached, volume, principal,
			map[string]any{"ws": workspace.ID, "generation": workspace.Generation, "path": mount.Path, "reason": reason}), workspace, operationID)
		events = append(events, event)
	}
	return events
}

func validateVolumeID(id string) error {
	return proto.ValidateVolumeID(id)
}

func requireVolumeIdempotency(key string) error {
	if key == "" {
		return proto.Err(proto.CodeBadRequest, "volume mutation requires an idempotency key")
	}
	return nil
}

func (c *Control) verifyVolumeArtifact(tenant, id string) (int64, error) {
	store, err := c.artifactStoreForTenant(tenant)
	if err != nil {
		return 0, proto.Err(proto.CodeUnsupported, "artifact store is unavailable")
	}
	if err := store.Verify(id); err != nil {
		return 0, proto.Err(proto.CodeNotFound, "volume artifact %q is unavailable: %v", id, err)
	}
	r, size, err := store.Open(id)
	if err != nil {
		return 0, proto.Err(proto.CodeNotFound, "volume artifact %q is unavailable: %v", id, err)
	}
	if err := r.Close(); err != nil {
		return 0, proto.Err(proto.CodeInternal, "close volume artifact %q: %v", id, err)
	}
	return size, nil
}

func (c *Control) volumeArtifactPresent(tenant, id string) error {
	store, err := c.artifactStoreForTenant(tenant)
	if err != nil {
		return proto.Err(proto.CodeUnsupported, "artifact store is unavailable")
	}
	r, _, err := store.Open(id)
	if err != nil {
		return proto.Err(proto.CodeNotFound, "volume artifact %q disappeared before commit: %v", id, err)
	}
	if err := r.Close(); err != nil {
		return proto.Err(proto.CodeInternal, "close volume artifact %q: %v", id, err)
	}
	return nil
}

func (c *Control) volumeCreate(ctx context.Context, subject Subject, req *proto.VolumeCreateReq) (*proto.Volume, error) {
	if err := validateVolumeID(req.ID); err != nil {
		return nil, err
	}
	if req.Artifact == "" {
		return nil, proto.Err(proto.CodeBadRequest, "artifact id is required")
	}
	if err := requireVolumeIdempotency(req.IdempotencyKey); err != nil {
		return nil, err
	}
	if err := c.check(ctx, subject, ActionWrite, Resource{Kind: "volume", ID: req.ID, Tenant: subject.Tenant, Owner: subject.ID}); err != nil {
		return nil, err
	}
	scope := subject.Tenant + "|" + subject.ID + "|volume.create"
	unlock := c.lockMutation(scope, req.IdempotencyKey)
	defer unlock()
	var prior proto.Volume
	if hit, err := c.mutationLookup(scope, req.IdempotencyKey, proto.OpVolumeCreate, req, &prior); err != nil {
		return nil, err
	} else if hit {
		return cloneVolume(&prior), nil
	}
	if _, err := c.verifyVolumeArtifact(subject.Tenant, req.Artifact); err != nil {
		return nil, err
	}
	now := c.now().UnixMilli()
	volume := &proto.Volume{
		ID: req.ID, Tenant: subject.Tenant, Owner: subject.ID, Artifact: req.Artifact,
		Version: 1, Versions: []proto.VolumeVersion{{Number: 1, Artifact: req.Artifact, PublishedAt: now}},
		CreatedAt: now, UpdatedAt: now,
	}
	key := volumeKey(volume.Tenant, volume.ID)
	c.mu.Lock()
	defer c.mu.Unlock()
	if err := c.volumeArtifactPresent(subject.Tenant, req.Artifact); err != nil {
		return nil, err
	}
	if c.volumes[key] != nil {
		return nil, proto.Err(proto.CodeConflict, "volume %q already exists", req.ID)
	}
	count := 0
	for _, existing := range c.volumes {
		if existing.Tenant == subject.Tenant {
			count++
		}
	}
	if count >= c.opts.MaxVolumesPerTenant {
		metrics.VolumeQuotaRejected.Inc()
		return nil, proto.Err(proto.CodeResourceExhausted, "tenant volume limit %d reached", c.opts.MaxVolumesPerTenant)
	}
	if err := c.transact(func(tx *eventlog.Tx) error {
		if _, err := tx.Exec(`INSERT INTO volumes(tenant,id,data) VALUES(?,?,?)`, volume.Tenant, volume.ID, proto.MustMarshal(volume)); err != nil {
			return err
		}
		return c.insertMutationTx(tx.Tx, scope, req.IdempotencyKey, proto.OpVolumeCreate, req, volume)
	}, []*proto.Event{func() *proto.Event {
		e := c.volumeEvent(proto.EvVolumeCreated, volume, subject.ID, nil)
		e.OperationID = req.IdempotencyKey
		return e
	}()}); err != nil {
		return nil, err
	}
	c.volumes[key] = volume
	return cloneVolume(volume), nil
}

func (c *Control) volumeGet(ctx context.Context, subject Subject, id string) (*proto.Volume, error) {
	c.mu.Lock()
	volume := cloneVolume(c.volumes[volumeKey(subject.Tenant, id)])
	c.mu.Unlock()
	if volume == nil {
		return nil, proto.Err(proto.CodeNotFound, "volume %q is not defined", id)
	}
	if !c.volumeReadable(ctx, subject, volume) {
		return nil, proto.Err(proto.CodeDenied, "subject %s may not read volume %s", subject.ID, id)
	}
	return volume, nil
}

func (c *Control) volumeReadable(ctx context.Context, subject Subject, volume *proto.Volume) bool {
	if c.opts.Authorizer != nil {
		return c.check(ctx, subject, ActionRead, volumeResource(volume)) == nil
	}
	return hasRole(subject, "admin") || subject.Tenant == volume.Tenant
}

func (c *Control) volumeList(ctx context.Context, subject Subject) (*proto.VolumeListRes, error) {
	c.mu.Lock()
	volumes := make([]proto.Volume, 0, len(c.volumes))
	for _, volume := range c.volumes {
		if volume.Tenant == subject.Tenant || hasRole(subject, "admin") {
			volumes = append(volumes, *cloneVolume(volume))
		}
	}
	c.mu.Unlock()
	out := &proto.VolumeListRes{Volumes: make([]proto.Volume, 0, len(volumes))}
	for i := range volumes {
		// Volumes are tenant-shared read-only datasets. Authorizers still get
		// an explicit decision; the fallback check treats the caller as reader.
		if c.volumeReadable(ctx, subject, &volumes[i]) {
			out.Volumes = append(out.Volumes, volumes[i])
		}
	}
	sort.Slice(out.Volumes, func(i, j int) bool {
		if out.Volumes[i].Tenant != out.Volumes[j].Tenant {
			return out.Volumes[i].Tenant < out.Volumes[j].Tenant
		}
		return out.Volumes[i].ID < out.Volumes[j].ID
	})
	return out, nil
}

func (c *Control) volumeRemove(ctx context.Context, subject Subject, req *proto.VolumeRemoveReq) error {
	if err := validateVolumeID(req.ID); err != nil {
		return err
	}
	if err := requireVolumeIdempotency(req.IdempotencyKey); err != nil {
		return err
	}
	scope := subject.Tenant + "|" + subject.ID + "|volume.remove"
	unlock := c.lockMutation(scope, req.IdempotencyKey)
	defer unlock()
	if hit, err := c.mutationLookup(scope, req.IdempotencyKey, proto.OpVolumeRemove, req, nil); err != nil || hit {
		return err
	}
	key := volumeKey(subject.Tenant, req.ID)
	c.mu.Lock()
	original := c.volumes[key]
	volume := cloneVolume(original)
	c.mu.Unlock()
	if volume == nil {
		return proto.Err(proto.CodeNotFound, "volume %q is not defined", req.ID)
	}
	if err := c.check(ctx, subject, ActionWrite, volumeResource(volume)); err != nil {
		return err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	current := c.volumes[key]
	if current == nil || current != original || current.Version != volume.Version || current.Artifact != volume.Artifact || current.Owner != volume.Owner {
		return proto.Err(proto.CodeConflict, "volume %q changed while authorizing removal", req.ID)
	}
	for _, ws := range c.workspaces {
		if ws.Tenant != subject.Tenant {
			continue
		}
		for _, mount := range ws.Spec.Volumes {
			if mount.ID == req.ID {
				return proto.Err(proto.CodeConflict, "volume %q is attached to workspace %s", req.ID, ws.ID)
			}
		}
	}
	if err := c.transact(func(tx *eventlog.Tx) error {
		if _, err := tx.Exec(`DELETE FROM volumes WHERE tenant=? AND id=?`, volume.Tenant, volume.ID); err != nil {
			return err
		}
		return c.insertMutationTx(tx.Tx, scope, req.IdempotencyKey, proto.OpVolumeRemove, req, struct{}{})
	}, []*proto.Event{func() *proto.Event {
		e := c.volumeEvent(proto.EvVolumeRemoved, volume, subject.ID, nil)
		e.OperationID = req.IdempotencyKey
		return e
	}()}); err != nil {
		return err
	}
	delete(c.volumes, key)
	return nil
}

func (c *Control) volumePublish(ctx context.Context, subject Subject, req *proto.VolumePublishReq, publisherNode string) (*proto.Volume, error) {
	if err := validateVolumeID(req.ID); err != nil {
		return nil, err
	}
	if req.Workspace == "" || req.Generation == 0 || req.ExpectedVersion == 0 || req.Artifact == "" {
		return nil, proto.Err(proto.CodeBadRequest, "workspace, generation, expected_version and artifact are required")
	}
	if err := requireVolumeIdempotency(req.IdempotencyKey); err != nil {
		return nil, err
	}
	scope := subject.Tenant + "|" + subject.ID + "|volume.publish"
	unlock := c.lockMutation(scope, req.IdempotencyKey)
	defer unlock()
	canonical := *req
	canonical.Grant = nil
	var prior proto.Volume
	if hit, err := c.mutationLookup(scope, req.IdempotencyKey, proto.OpVolumePublish, &canonical, &prior); err != nil {
		return nil, err
	} else if hit {
		return cloneVolume(&prior), nil
	}
	if _, err := c.verifyVolumeArtifact(subject.Tenant, req.Artifact); err != nil {
		return nil, err
	}
	c.mu.Lock()
	originalVolume := c.volumes[volumeKey(subject.Tenant, req.ID)]
	volume := cloneVolume(originalVolume)
	originalWS := c.workspaces[req.Workspace]
	var workspace proto.Workspace
	if originalWS != nil {
		workspace = *originalWS
	}
	c.mu.Unlock()
	if volume == nil || originalWS == nil || workspace.Tenant != subject.Tenant {
		return nil, proto.Err(proto.CodeNotFound, "volume or workspace is not defined")
	}
	if err := c.check(ctx, subject, ActionWrite, volumeResource(volume)); err != nil {
		return nil, err
	}
	if err := c.check(ctx, subject, ActionWrite, workspaceResource(&workspace)); err != nil {
		return nil, err
	}
	if req.Grant != nil {
		if err := VerifyGrant(c.key.Public().(ed25519.PublicKey), req.Grant, c.now()); err != nil {
			return nil, err
		}
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if err := c.volumeArtifactPresent(subject.Tenant, req.Artifact); err != nil {
		return nil, err
	}
	current := c.volumes[volumeKey(subject.Tenant, req.ID)]
	currentWS := c.workspaces[req.Workspace]
	if current == nil || current != originalVolume || currentWS == nil || currentWS != originalWS ||
		currentWS.State != proto.WSClaimed ||
		currentWS.Generation != req.Generation || currentWS.AuthzRevision != workspace.AuthzRevision ||
		(req.Grant != nil && currentWS.AuthzRevision != req.Grant.Claims.AuthzRevision) ||
		(publisherNode != "" && currentWS.Node != publisherNode) {
		return nil, proto.Err(proto.CodeConflict, "stale workspace generation")
	}
	if req.Grant != nil {
		claims := req.Grant.Claims
		live, ok := c.subjects[claims.Client]
		if !ok || !sameVolumeSubject(live, subject) || c.now().UnixMilli() > claims.ExpiresAt {
			return nil, proto.Err(proto.CodeUnauthorized, "volume.publish grant principal is no longer current")
		}
	}
	if current.Version != req.ExpectedVersion {
		return nil, proto.Err(proto.CodeConflict, "volume version is %d, expected %d", current.Version, req.ExpectedVersion)
	}
	if current.Version == ^uint64(0) {
		return nil, proto.Err(proto.CodeResourceExhausted, "volume version space is exhausted")
	}
	if len(current.Versions) >= proto.MaxVolumeVersions {
		pinned := make(map[uint64]struct{})
		for _, candidate := range c.workspaces {
			if candidate.Tenant != current.Tenant {
				continue
			}
			for _, mount := range candidate.Spec.Volumes {
				if mount.ID == current.ID {
					pinned[mount.Version] = struct{}{}
				}
			}
		}
		prune := -1
		for i, version := range current.Versions {
			if version.Number != current.Version {
				if _, held := pinned[version.Number]; !held {
					prune = i
					break
				}
			}
		}
		if prune < 0 {
			return nil, proto.Err(proto.CodeResourceExhausted, "volume version limit %d reached; every old version is pinned", proto.MaxVolumeVersions)
		}
		current = cloneVolume(current)
		current.Versions = append(current.Versions[:prune:prune], current.Versions[prune+1:]...)
	}
	next := cloneVolume(current)
	next.Version++
	next.Artifact = req.Artifact
	next.UpdatedAt = c.now().UnixMilli()
	next.Versions = append(next.Versions, proto.VolumeVersion{
		Number: next.Version, Artifact: req.Artifact, PublishedBy: req.Workspace,
		Generation: req.Generation, PublishedAt: next.UpdatedAt,
	})
	if err := c.transact(func(tx *eventlog.Tx) error {
		if _, err := tx.Exec(`UPDATE volumes SET data=? WHERE tenant=? AND id=?`, proto.MustMarshal(next), next.Tenant, next.ID); err != nil {
			return err
		}
		return c.insertMutationTx(tx.Tx, scope, req.IdempotencyKey, proto.OpVolumePublish, &canonical, next)
	}, []*proto.Event{stampVolumeWorkspaceEvent(c.volumeEvent(proto.EvVolumePublished, next, subject.ID,
		map[string]any{"ws": req.Workspace, "generation": req.Generation}), currentWS, req.IdempotencyKey)}); err != nil {
		return nil, err
	}
	c.volumes[volumeKey(next.Tenant, next.ID)] = next
	return cloneVolume(next), nil
}

func (c *Control) volumeAttach(ctx context.Context, subject Subject, req *proto.VolumeAttachReq) (*proto.Workspace, error) {
	if err := proto.ValidateVolumeMount(proto.VolumeMount{ID: req.ID, Path: req.Path}); err != nil {
		return nil, err
	}
	if req.Workspace == "" {
		return nil, proto.Err(proto.CodeBadRequest, "workspace is required")
	}
	if err := requireVolumeIdempotency(req.IdempotencyKey); err != nil {
		return nil, err
	}
	return c.changeVolumeAttachment(ctx, subject, proto.OpVolumeAttach, req.IdempotencyKey, req, req.Workspace, req.Generation, func(ws *proto.Workspace, volume *proto.Volume) (string, error) {
		for _, mount := range ws.Spec.Volumes {
			if volumePathsOverlap(mount.Path, req.Path) {
				return "", proto.Err(proto.CodeConflict, "workspace volume paths %q and %q overlap", mount.Path, req.Path)
			}
		}
		if len(ws.Spec.Volumes) >= proto.MaxWorkspaceVolumes {
			return "", proto.Err(proto.CodeResourceExhausted, "workspace volume limit %d reached", proto.MaxWorkspaceVolumes)
		}
		ws.Spec.Volumes = append(ws.Spec.Volumes, proto.VolumeMount{ID: volume.ID, Path: req.Path, Version: volume.Version, Artifact: volume.Artifact})
		if ws.Spec.Requires.Backend != "" && ws.Spec.Requires.Backend != "process" {
			return "", proto.Err(proto.CodeUnsupported, "read-only shared volumes currently require the process backend")
		}
		ws.Spec.Requires.Backend = "process"
		if !contains(ws.Spec.Requires.Caps, proto.CapabilityReadOnlyVolumes) {
			ws.Spec.Requires.Caps = append(ws.Spec.Requires.Caps, proto.CapabilityReadOnlyVolumes)
		}
		return volume.ID, nil
	})
}

func volumePathsOverlap(a, b string) bool {
	return a == b || strings.HasPrefix(a, b+"/") || strings.HasPrefix(b, a+"/")
}

func (c *Control) volumeDetach(ctx context.Context, subject Subject, req *proto.VolumeDetachReq) (*proto.Workspace, error) {
	if err := proto.ValidateVolumeMount(proto.VolumeMount{ID: "detached", Path: req.Path}); err != nil {
		return nil, err
	}
	if req.Workspace == "" {
		return nil, proto.Err(proto.CodeBadRequest, "workspace is required")
	}
	if err := requireVolumeIdempotency(req.IdempotencyKey); err != nil {
		return nil, err
	}
	return c.changeVolumeAttachment(ctx, subject, proto.OpVolumeDetach, req.IdempotencyKey, req, req.Workspace, req.Generation, func(ws *proto.Workspace, _ *proto.Volume) (string, error) {
		for i, mount := range ws.Spec.Volumes {
			if mount.Path == req.Path {
				ws.Spec.Volumes = append(ws.Spec.Volumes[:i:i], ws.Spec.Volumes[i+1:]...)
				return mount.ID, nil
			}
		}
		return "", proto.Err(proto.CodeNotFound, "workspace path %q has no volume", req.Path)
	})
}

func (c *Control) changeVolumeAttachment(ctx context.Context, subject Subject, op, idem string, request any, workspaceID string, generation uint64, mutate func(*proto.Workspace, *proto.Volume) (string, error)) (*proto.Workspace, error) {
	scope := subject.Tenant + "|" + subject.ID + "|" + op
	unlock := c.lockMutation(scope, idem)
	defer unlock()
	var prior proto.Workspace
	if hit, err := c.mutationLookup(scope, idem, op, request, &prior); err != nil {
		return nil, err
	} else if hit {
		c.mu.Lock()
		current := c.workspaces[workspaceID]
		var currentSnapshot proto.Workspace
		if current != nil {
			currentSnapshot = *current
		}
		c.mu.Unlock()
		if current == nil || currentSnapshot.Tenant != subject.Tenant {
			return nil, proto.Err(proto.CodeNotFound, "workspace %s", workspaceID)
		}
		if err := c.check(ctx, subject, ActionWrite, workspaceResource(&currentSnapshot)); err != nil {
			return nil, err
		}
		return snapshotWorkspace(&prior), nil
	}
	c.mu.Lock()
	originalWS := c.workspaces[workspaceID]
	ws := originalWS
	var snapshot proto.Workspace
	var volumeSnapshot *proto.Volume
	var originalVolume *proto.Volume
	if ws != nil {
		snapshot = *ws
	}
	if attach, ok := request.(*proto.VolumeAttachReq); ok {
		originalVolume = c.volumes[volumeKey(subject.Tenant, attach.ID)]
		volumeSnapshot = cloneVolume(originalVolume)
	}
	c.mu.Unlock()
	if ws == nil || snapshot.Tenant != subject.Tenant {
		return nil, proto.Err(proto.CodeNotFound, "workspace %s", workspaceID)
	}
	if err := c.check(ctx, subject, ActionWrite, workspaceResource(&snapshot)); err != nil {
		return nil, err
	}
	if _, ok := request.(*proto.VolumeAttachReq); ok {
		if volumeSnapshot == nil {
			return nil, proto.Err(proto.CodeNotFound, "volume is not defined")
		}
		if !c.volumeReadable(ctx, subject, volumeSnapshot) {
			return nil, proto.Err(proto.CodeDenied, "subject %s may not read volume %s", subject.ID, volumeSnapshot.ID)
		}
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	ws = c.workspaces[workspaceID]
	if ws == nil || ws != originalWS || ws.Generation != generation || ws.AuthzRevision != snapshot.AuthzRevision {
		return nil, proto.Err(proto.CodeConflict, "stale workspace generation")
	}
	if ws.State != proto.WSPending && ws.State != proto.WSPaused {
		return nil, proto.Err(proto.CodeConflict, "workspace must be pending or paused to change volume mounts")
	}
	next := *ws
	next.Spec.Volumes = append([]proto.VolumeMount(nil), ws.Spec.Volumes...)
	next.Spec.Requires.Caps = append([]string(nil), ws.Spec.Requires.Caps...)
	var detachedMount *proto.VolumeMount
	if detach, ok := request.(*proto.VolumeDetachReq); ok {
		for _, mount := range ws.Spec.Volumes {
			if mount.Path == detach.Path {
				copyMount := mount
				detachedMount = &copyMount
				break
			}
		}
	}
	var volume *proto.Volume
	if attach, ok := request.(*proto.VolumeAttachReq); ok {
		volume = c.volumes[volumeKey(subject.Tenant, attach.ID)]
		if volume == nil || volume != originalVolume || volume.Version != volumeSnapshot.Version || volume.Artifact != volumeSnapshot.Artifact || volume.Owner != volumeSnapshot.Owner {
			return nil, proto.Err(proto.CodeNotFound, "volume %q is not defined", attach.ID)
		}
	}
	volumeID, err := mutate(&next, volume)
	if err != nil {
		return nil, err
	}
	var eventVolume *proto.Volume
	if volume != nil {
		eventVolume = cloneVolume(volume)
	} else {
		eventVolume = cloneVolume(c.volumes[volumeKey(subject.Tenant, volumeID)])
	}
	if eventVolume == nil {
		return nil, proto.Err(proto.CodeNotFound, "volume %q is not defined", volumeID)
	}
	typ := proto.EvVolumeAttached
	if op == proto.OpVolumeDetach {
		typ = proto.EvVolumeDetached
		if detachedMount != nil {
			eventVolume.Artifact = detachedMount.Artifact
			eventVolume.Version = detachedMount.Version
		}
	}
	next.UpdatedAt = c.now().UnixMilli()
	event := stampVolumeWorkspaceEvent(c.volumeEvent(typ, eventVolume, subject.ID,
		map[string]any{"ws": next.ID, "generation": next.Generation, "path": attachmentPath(request)}), &next, idem)
	if err := c.persistWSAndMutation(&next, scope, idem, op, request, &next, event); err != nil {
		return nil, err
	}
	*ws = next
	return c.snapshotWorkspaceLocked(ws), nil
}

func attachmentPath(request any) string {
	switch req := request.(type) {
	case *proto.VolumeAttachReq:
		return req.Path
	case *proto.VolumeDetachReq:
		return req.Path
	default:
		return ""
	}
}

func (c *Control) snapshotWorkspaceLocked(ws *proto.Workspace) *proto.Workspace {
	return snapshotWorkspace(ws)
}

func snapshotWorkspace(ws *proto.Workspace) *proto.Workspace {
	cp := *ws
	cp.Spec.Volumes = append([]proto.VolumeMount(nil), ws.Spec.Volumes...)
	cp.Spec.RestoreObjects = append([]string(nil), ws.Spec.RestoreObjects...)
	cp.LastSnapshotObjects = append([]string(nil), ws.LastSnapshotObjects...)
	cp.Spec.Requires.Caps = append([]string(nil), ws.Spec.Requires.Caps...)
	cp.Spec.ACL.Readers = append([]string(nil), ws.Spec.ACL.Readers...)
	cp.Spec.ACL.Writers = append([]string(nil), ws.Spec.ACL.Writers...)
	cp.Spec.Labels = cloneMap(ws.Spec.Labels)
	cp.Lease, cp.IdlePolicy, cp.LifecycleDeadline = ws.CloneLifecycle()
	return &cp
}
