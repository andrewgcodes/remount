package volume

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"remount.dev/remount/internal/artifact"
)

const (
	stateAttaching = "attaching"
	stateAttached  = "attached"
	stateDetaching = "detaching"
)

// LocalBackend is a single-writer durable catalog with descriptor-rooted bind
// mount materialization. It is suitable for one node process; shared control
// authority remains outside this package.
type LocalBackend struct {
	mu         sync.Mutex
	filename   string
	resolver   SourceResolver
	mounter    MountEngine
	opts       Options
	state      catalogState
	authorized map[string]uint64
	closed     bool
}

// OpenLocalBackend loads durable state without acting on persisted mount intent.
// Recovery waits for SetWorkspaceGeneration to supply current control authority.
func OpenLocalBackend(ctx context.Context, filename string, resolver SourceResolver, mounter MountEngine, opts Options) (*LocalBackend, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if filename == "" {
		return nil, errors.New("volume: state filename is required")
	}
	if err := normalizeOptions(&opts); err != nil {
		return nil, err
	}
	state, err := loadState(filename)
	if err != nil {
		return nil, err
	}
	b := &LocalBackend{filename: filename, resolver: resolver, mounter: mounter, opts: opts, state: state, authorized: make(map[string]uint64)}
	if err := b.validateLoadedState(); err != nil {
		return nil, err
	}
	return b, nil
}

func normalizeOptions(opts *Options) error {
	if opts.MaxVolumesPerTenant < 0 || opts.MaxMountsPerTenant < 0 || opts.MaxVersionsPerVolume < 0 ||
		opts.MaxVolumes < 0 || opts.MaxMounts < 0 || opts.MaxWorkspaceFences < 0 ||
		opts.MaxOperations < 0 || opts.OperationRetention < 0 {
		return errors.New("volume: limits must not be negative")
	}
	if opts.MaxVolumesPerTenant == 0 {
		opts.MaxVolumesPerTenant = 1024
	}
	if opts.MaxMountsPerTenant == 0 {
		opts.MaxMountsPerTenant = 8192
	}
	if opts.MaxVersionsPerVolume == 0 {
		opts.MaxVersionsPerVolume = 128
	}
	if opts.MaxVolumes == 0 {
		opts.MaxVolumes = 16384
	}
	if opts.MaxMounts == 0 {
		opts.MaxMounts = 32768
	}
	if opts.MaxWorkspaceFences == 0 {
		opts.MaxWorkspaceFences = 65536
	}
	if opts.MaxOperations == 0 {
		opts.MaxOperations = 65536
	}
	if opts.OperationRetention == 0 {
		opts.OperationRetention = 30 * 24 * time.Hour
	}
	if opts.Clock == nil {
		opts.Clock = time.Now
	}
	return nil
}

func (b *LocalBackend) validateLoadedState() error {
	volumesByTenant := make(map[string]int)
	mountsByTenant := make(map[string]int)
	for key, record := range b.state.Volumes {
		if volumeKey(record.Volume.Tenant, record.Volume.ID) != key || len(record.Versions) == 0 || record.Volume.Version == 0 {
			return errors.New("volume: corrupt volume state")
		}
		if err := validateTenant(record.Volume.Tenant); err != nil {
			return errors.New("volume: corrupt volume tenant")
		}
		if err := validateVolumeID(record.Volume.ID); err != nil {
			return errors.New("volume: corrupt volume id")
		}
		if _, err := artifact.Digest(record.Volume.Artifact); err != nil {
			return errors.New("volume: corrupt volume artifact")
		}
		previous := uint64(0)
		latestFound := false
		for _, version := range record.Versions {
			if version.Number <= previous {
				return errors.New("volume: unsorted volume versions")
			}
			if _, err := artifact.Digest(version.Artifact); err != nil {
				return errors.New("volume: corrupt version artifact")
			}
			if version.Number == record.Volume.Version {
				latestFound = version.Artifact == record.Volume.Artifact
			}
			previous = version.Number
		}
		if !latestFound {
			return errors.New("volume: current version is absent")
		}
		if len(record.Versions) > b.opts.MaxVersionsPerVolume {
			return fmt.Errorf("%w: retained versions exceed configured limit", ErrQuota)
		}
		volumesByTenant[record.Volume.Tenant]++
	}
	for key, mount := range b.state.Attachments {
		if attachmentKey(mount.Tenant, mount.Workspace, mount.Path) != key || (mount.State != stateAttaching && mount.State != stateAttached && mount.State != stateDetaching) {
			return errors.New("volume: corrupt attachment state")
		}
		if _, _, err := cleanMountPath(mount.Path); err != nil {
			return errors.New("volume: corrupt attachment path")
		}
		if !filepath.IsAbs(mount.WorkspaceRoot) || len(mount.WorkspaceRoot) > 4096 || !mount.ReadOnly || mount.Generation == 0 {
			return errors.New("volume: corrupt attachment boundary")
		}
		fence, ok := b.state.Fences[workspaceKey(mount.Tenant, mount.Workspace)]
		if !ok || mount.Generation > fence {
			return errors.New("volume: attachment exceeds workspace fence")
		}
		record, ok := b.state.Volumes[volumeKey(mount.Tenant, mount.VolumeID)]
		if !ok {
			return errors.New("volume: attachment references absent volume")
		}
		versionFound := false
		for _, version := range record.Versions {
			if version.Number == mount.VolumeVersion && version.Artifact == mount.Artifact {
				versionFound = true
				break
			}
		}
		if !versionFound {
			return errors.New("volume: attachment references absent version")
		}
		mountsByTenant[mount.Tenant]++
	}
	for tenant, count := range volumesByTenant {
		if count > b.opts.MaxVolumesPerTenant {
			return fmt.Errorf("%w: tenant %s has too many volumes", ErrQuota, tenant)
		}
	}
	for tenant, count := range mountsByTenant {
		if count > b.opts.MaxMountsPerTenant {
			return fmt.Errorf("%w: tenant %s has too many mounts", ErrQuota, tenant)
		}
	}
	if len(b.state.Volumes) > b.opts.MaxVolumes {
		return fmt.Errorf("%w: volume catalog exceeds configured limit", ErrQuota)
	}
	if len(b.state.Attachments) > b.opts.MaxMounts {
		return fmt.Errorf("%w: mount catalog exceeds configured limit", ErrQuota)
	}
	if len(b.state.Fences) > b.opts.MaxWorkspaceFences {
		return fmt.Errorf("%w: workspace fence catalog exceeds configured limit", ErrQuota)
	}
	for key, generation := range b.state.Fences {
		tenant, workspace, ok := splitCompositeKey(key)
		updated, hasUpdated := b.state.FenceUpdated[key]
		if !ok || generation == 0 || !hasUpdated || updated.IsZero() || validateTenant(tenant) != nil || validateSegment("workspace", workspace) != nil {
			return errors.New("volume: corrupt workspace fence")
		}
	}
	if len(b.state.FenceUpdated) != len(b.state.Fences) {
		return errors.New("volume: corrupt workspace fence timestamps")
	}
	if len(b.state.Operations) > b.opts.MaxOperations {
		return fmt.Errorf("%w: operation journal exceeds configured limit", ErrQuota)
	}
	return nil
}

// EnsureVersion reconciles one control-plane-authoritative immutable version
// into this node's local mount catalog. It never invents or advances control
// authority; it only makes an already pinned artifact attachable after a move.
func (b *LocalBackend) EnsureVersion(ctx context.Context, tenant, id string, number uint64, artifactID string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := validateTenant(tenant); err != nil {
		return err
	}
	if err := validateVolumeID(id); err != nil {
		return err
	}
	if number == 0 {
		return ErrConflict
	}
	if _, err := artifact.Digest(artifactID); err != nil {
		return err
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if err := b.ensureOpenLocked(); err != nil {
		return err
	}
	key := volumeKey(tenant, id)
	record, exists := b.state.Volumes[key]
	if !exists {
		evictKey, err := b.volumeEvictionCandidateLocked(tenant)
		if err != nil {
			return err
		}
		now := b.opts.Clock().UTC()
		record = volumeRecord{
			Volume:   Volume{ID: id, Tenant: tenant, Artifact: artifactID, Version: number, CreatedAt: now, UpdatedAt: now},
			Versions: []Version{{Number: number, Artifact: artifactID, PublishedAt: now}},
		}
		return b.mutateLocked(func() {
			if evictKey != "" {
				delete(b.state.Volumes, evictKey)
				b.state.Counters.PrunedVolumes++
			}
			b.state.Volumes[key] = record
		})
	}
	for _, version := range record.Versions {
		if version.Number == number {
			if version.Artifact != artifactID {
				return ErrConflict
			}
			return nil
		}
	}
	if err := b.makeVersionRoomLocked(&record); err != nil {
		return err
	}
	now := b.opts.Clock().UTC()
	record.Versions = append(record.Versions, Version{Number: number, Artifact: artifactID, PublishedAt: now})
	sort.Slice(record.Versions, func(i, j int) bool { return record.Versions[i].Number < record.Versions[j].Number })
	if number > record.Volume.Version {
		record.Volume.Version = number
		record.Volume.Artifact = artifactID
		record.Volume.UpdatedAt = now
	}
	return b.mutateLocked(func() { b.state.Volumes[key] = record })
}

// Create commits a new volume and its first immutable version atomically.
func (b *LocalBackend) Create(ctx context.Context, req CreateRequest) (Volume, error) {
	if err := ctx.Err(); err != nil {
		return Volume{}, err
	}
	if err := validateMutation(req.OperationID, req.Tenant, req.ID, req.Artifact); err != nil {
		return Volume{}, err
	}
	fp := fingerprint("create", req.Tenant, req.ID, req.Artifact)
	b.mu.Lock()
	defer b.mu.Unlock()
	if err := b.ensureOpenLocked(); err != nil {
		return Volume{}, err
	}
	if op, found, err := b.replayLocked(req.Tenant, req.OperationID, fp); found || err != nil {
		if err != nil {
			return Volume{}, err
		}
		return *op.Volume, nil
	}
	if _, ok := b.state.Volumes[volumeKey(req.Tenant, req.ID)]; ok {
		return Volume{}, ErrConflict
	}
	evictKey, err := b.volumeEvictionCandidateLocked(req.Tenant)
	if err != nil {
		return Volume{}, err
	}
	if err := b.admitOperationLocked(); err != nil {
		return Volume{}, err
	}
	now := b.opts.Clock().UTC()
	vol := Volume{ID: req.ID, Tenant: req.Tenant, Artifact: req.Artifact, Version: 1, CreatedAt: now, UpdatedAt: now}
	version := Version{Number: 1, Artifact: req.Artifact, PublishedAt: now}
	err = b.mutateLocked(func() {
		if evictKey != "" {
			delete(b.state.Volumes, evictKey)
			b.state.Counters.PrunedVolumes++
		}
		b.state.Volumes[volumeKey(req.Tenant, req.ID)] = volumeRecord{Volume: vol, Versions: []Version{version}}
		b.state.Operations[operationKey(req.Tenant, req.OperationID)] = completedOperation("create", fp, now, &vol, nil)
	})
	return vol, err
}

// Delete removes a volume only after every pending or active attachment is gone.
func (b *LocalBackend) Delete(ctx context.Context, req DeleteRequest) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := validateVolumeIdentity(req.OperationID, req.Tenant, req.ID); err != nil {
		return err
	}
	fp := fingerprint("delete", req.Tenant, req.ID)
	b.mu.Lock()
	defer b.mu.Unlock()
	if err := b.ensureOpenLocked(); err != nil {
		return err
	}
	if _, found, err := b.replayLocked(req.Tenant, req.OperationID, fp); found || err != nil {
		return err
	}
	key := volumeKey(req.Tenant, req.ID)
	if _, ok := b.state.Volumes[key]; !ok {
		return ErrNotFound
	}
	for _, mount := range b.state.Attachments {
		if mount.Tenant == req.Tenant && mount.VolumeID == req.ID {
			return ErrConflict
		}
	}
	if err := b.admitOperationLocked(); err != nil {
		return err
	}
	now := b.opts.Clock().UTC()
	return b.mutateLocked(func() {
		delete(b.state.Volumes, key)
		b.state.Operations[operationKey(req.Tenant, req.OperationID)] = completedOperation("delete", fp, now, nil, nil)
	})
}

// Publish advances the latest artifact with compare-and-swap and a current
// workspace-generation fence. Existing attachments stay pinned to old bytes.
func (b *LocalBackend) Publish(ctx context.Context, req PublishRequest) (Volume, error) {
	if err := ctx.Err(); err != nil {
		return Volume{}, err
	}
	if err := validateMutation(req.OperationID, req.Tenant, req.ID, req.Artifact); err != nil {
		return Volume{}, err
	}
	if err := validateSegment("workspace", req.Workspace); err != nil || req.Generation == 0 {
		return Volume{}, errors.Join(ErrStaleGeneration, err)
	}
	fp := fingerprint("publish", req.Tenant, req.ID, req.Artifact, req.Workspace, req.Generation, req.ExpectedVersion)
	b.mu.Lock()
	defer b.mu.Unlock()
	if err := b.ensureOpenLocked(); err != nil {
		return Volume{}, err
	}
	if op, found, err := b.replayLocked(req.Tenant, req.OperationID, fp); found || err != nil {
		if err != nil {
			return Volume{}, err
		}
		return *op.Volume, nil
	}
	if err := b.checkGenerationLocked(req.Tenant, req.Workspace, req.Generation); err != nil {
		return Volume{}, err
	}
	key := volumeKey(req.Tenant, req.ID)
	record, ok := b.state.Volumes[key]
	if !ok {
		return Volume{}, ErrNotFound
	}
	if record.Volume.Version != req.ExpectedVersion {
		return Volume{}, ErrConflict
	}
	if record.Volume.Version == ^uint64(0) {
		return Volume{}, ErrConflict
	}
	if err := b.makeVersionRoomLocked(&record); err != nil {
		return Volume{}, err
	}
	if err := b.admitOperationLocked(); err != nil {
		return Volume{}, err
	}
	now := b.opts.Clock().UTC()
	record.Volume.Version++
	record.Volume.Artifact = req.Artifact
	record.Volume.UpdatedAt = now
	record.Versions = append(record.Versions, Version{Number: record.Volume.Version, Artifact: req.Artifact, PublishedBy: req.Workspace, Generation: req.Generation, PublishedAt: now})
	vol := record.Volume
	err := b.mutateLocked(func() {
		b.state.Volumes[key] = record
		b.state.Operations[operationKey(req.Tenant, req.OperationID)] = completedOperation("publish", fp, now, &vol, nil)
	})
	return vol, err
}

// Attach durably records intent before invoking the mount engine and commits
// attached state only after read-only status is verified.
func (b *LocalBackend) Attach(ctx context.Context, req AttachRequest) (Attachment, error) {
	if !req.ReadOnly {
		return Attachment{}, ErrReadOnlyRequired
	}
	pathValue, _, err := cleanMountPath(req.Path)
	if err != nil {
		return Attachment{}, err
	}
	if err := validateVolumeIdentity(req.OperationID, req.Tenant, req.ID); err != nil {
		return Attachment{}, err
	}
	if err := validateSegment("workspace", req.Workspace); err != nil || req.Generation == 0 || !filepath.IsAbs(req.WorkspaceRoot) || len(req.WorkspaceRoot) > 4096 {
		return Attachment{}, ErrUnsafePath
	}
	fp := fingerprint("attach", req.Tenant, req.ID, req.Workspace, req.Generation, pathValue, req.WorkspaceRoot, true, req.Version, req.Artifact)
	key := attachmentKey(req.Tenant, req.Workspace, pathValue)
	b.mu.Lock()
	if err := b.ensureOpenLocked(); err != nil {
		b.mu.Unlock()
		return Attachment{}, err
	}
	if err := b.checkAuthorizedGenerationLocked(req.Tenant, req.Workspace, req.Generation); err != nil {
		b.mu.Unlock()
		return Attachment{}, err
	}
	opKey := operationKey(req.Tenant, req.OperationID)
	if op, found := b.state.Operations[opKey]; found {
		if op.Fingerprint != fp || op.Kind != "attach" || op.ResourceKey != key {
			b.mu.Unlock()
			return Attachment{}, ErrConflict
		}
		if op.Done {
			mount, attachmentPresent := b.state.Attachments[key]
			if op.Attachment == nil || !attachmentPresent || mount.State != stateAttached || !attachmentMatchesRequest(mount, req, pathValue) {
				b.mu.Unlock()
				return Attachment{}, ErrConflict
			}
			b.mu.Unlock()
			return b.recoverCommittedAttach(ctx, key, opKey, req.OperationID, fp, mount, op)
		}
		_, attachmentPresent := b.state.Attachments[key]
		if attachmentPresent && !op.MountAttempted && !op.ReplaceExisting {
			op.ReplaceExisting = true
			if err := b.mutateLocked(func() { b.state.Operations[opKey] = op }); err != nil {
				b.mu.Unlock()
				return Attachment{}, err
			}
		}
		b.mu.Unlock()
		if !attachmentPresent {
			return Attachment{}, ErrConflict
		}
		return b.performAttach(ctx, key, req.OperationID, true, op.MountAttempted, op.MountAttempted || op.ReplaceExisting)
	}
	if err := b.checkGenerationLocked(req.Tenant, req.Workspace, req.Generation); err != nil {
		b.mu.Unlock()
		return Attachment{}, err
	}
	record, ok := b.state.Volumes[volumeKey(req.Tenant, req.ID)]
	if !ok {
		b.mu.Unlock()
		return Attachment{}, ErrNotFound
	}
	if _, exists := b.state.Attachments[key]; exists {
		b.mu.Unlock()
		return Attachment{}, ErrConflict
	}
	if b.mountCountLocked(req.Tenant) >= b.opts.MaxMountsPerTenant || len(b.state.Attachments) >= b.opts.MaxMounts {
		b.rejectQuotaLocked()
		b.mu.Unlock()
		return Attachment{}, ErrQuota
	}
	if err := b.admitOperationLocked(); err != nil {
		b.mu.Unlock()
		return Attachment{}, err
	}
	versionNumber, artifactID := record.Volume.Version, record.Volume.Artifact
	if req.Version != 0 || req.Artifact != "" {
		if req.Version == 0 || req.Artifact == "" {
			b.mu.Unlock()
			return Attachment{}, ErrConflict
		}
		found := false
		for _, version := range record.Versions {
			if version.Number == req.Version && version.Artifact == req.Artifact {
				found = true
				break
			}
		}
		if !found {
			b.mu.Unlock()
			return Attachment{}, ErrNotFound
		}
		versionNumber, artifactID = req.Version, req.Artifact
	}
	mount := Attachment{Tenant: req.Tenant, Workspace: req.Workspace, Generation: req.Generation, VolumeID: req.ID, VolumeVersion: versionNumber, Artifact: artifactID, Path: pathValue, WorkspaceRoot: filepath.Clean(req.WorkspaceRoot), ReadOnly: true, State: stateAttaching}
	if err := b.mutateLocked(func() {
		b.state.Attachments[key] = mount
		b.state.Operations[operationKey(req.Tenant, req.OperationID)] = operation{Kind: "attach", Fingerprint: fp, ResourceKey: key}
	}); err != nil {
		b.mu.Unlock()
		return Attachment{}, err
	}
	b.mu.Unlock()
	return b.performAttach(ctx, key, req.OperationID, true, false, false)
}

func (b *LocalBackend) performAttach(ctx context.Context, key, operationID string, cancelOnFailure, resume, replaceExisting bool) (Attachment, error) {
	b.mu.Lock()
	mount, ok := b.state.Attachments[key]
	b.mu.Unlock()
	if !ok {
		return Attachment{}, ErrNotFound
	}
	if b.resolver == nil || b.mounter == nil {
		return Attachment{}, ErrUnsupported
	}
	source, err := b.resolver.OpenArtifact(ctx, mount.Tenant, mount.Artifact)
	if err != nil {
		if cancelOnFailure {
			b.cancelAttach(key, mount.Tenant, operationID)
		}
		return Attachment{}, err
	}
	defer source.Close()
	expectedSource, err := source.Stat()
	if err != nil {
		return Attachment{}, err
	}
	target, err := openTarget(mount, true)
	if err != nil {
		if cancelOnFailure {
			b.cancelAttach(key, mount.Tenant, operationID)
		}
		return Attachment{}, err
	}
	defer target.Close()
	status, err := b.mounter.Inspect(ctx, target)
	if err != nil {
		return Attachment{}, err
	}
	if status.Mounted {
		matchesSource := status.Source != nil && os.SameFile(expectedSource, status.Source)
		if resume && status.ReadOnly && matchesSource {
			return b.commitAttach(ctx, key, mount, operationID, target)
		}
		if !replaceExisting {
			if cancelOnFailure {
				b.cancelAttach(key, mount.Tenant, operationID)
			}
			return Attachment{}, ErrConflict
		}
		if err := b.mounter.Unmount(context.WithoutCancel(ctx), target); err != nil {
			return Attachment{}, err
		}
		status, err = b.mounter.Inspect(context.WithoutCancel(ctx), target)
		if err != nil || status.Mounted {
			if err == nil {
				err = errors.New("volume: writable mount remains after unmount")
			}
			return Attachment{}, err
		}
	}
	if err := ensureEmpty(target); err != nil {
		if cancelOnFailure {
			b.cancelAttach(key, mount.Tenant, operationID)
		}
		return Attachment{}, err
	}
	if operationID != "" {
		if err := b.markMountAttempted(key, mount.Tenant, operationID); err != nil {
			return Attachment{}, err
		}
	}
	if err := b.mounter.MountReadOnly(ctx, source, target); err != nil {
		status, inspectErr := b.mounter.Inspect(context.WithoutCancel(ctx), target)
		if inspectErr == nil && !status.Mounted && cancelOnFailure {
			b.cancelAttach(key, mount.Tenant, operationID)
		}
		return Attachment{}, err
	}
	status, err = b.mounter.Inspect(ctx, target)
	matchesSource := status.Source != nil && os.SameFile(expectedSource, status.Source)
	if err != nil || !status.Mounted || !status.ReadOnly || !matchesSource {
		if err == nil {
			err = errors.New("volume: mount engine did not establish read-only mount")
		}
		_ = b.mounter.Unmount(context.WithoutCancel(ctx), target)
		return Attachment{}, err
	}
	return b.commitAttach(ctx, key, mount, operationID, target)
}

func (b *LocalBackend) commitAttach(ctx context.Context, key string, mount Attachment, operationID string, target *os.File) (Attachment, error) {
	device, inode, identityErr := directoryIdentity(target)
	if identityErr != nil {
		_ = b.mounter.Unmount(context.WithoutCancel(ctx), target)
		return Attachment{}, identityErr
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	current, ok := b.state.Attachments[key]
	if !ok || current.State != stateAttaching || current.Generation != mount.Generation {
		_ = b.mounter.Unmount(context.WithoutCancel(ctx), target)
		return Attachment{}, ErrStaleGeneration
	}
	now := b.opts.Clock().UTC()
	current.State = stateAttached
	current.AttachedAt = now
	current.TargetDevice = device
	current.TargetInode = inode
	err := b.mutateLocked(func() {
		b.state.Attachments[key] = current
		if operationID != "" {
			opKey := operationKey(current.Tenant, operationID)
			op := b.state.Operations[opKey]
			op.Done, op.CompletedAt, op.Attachment = true, now, &current
			b.state.Operations[opKey] = op
		} else {
			b.state.Counters.ReconciledAttach++
		}
	})
	return current, err
}

func attachmentMatchesRequest(mount Attachment, req AttachRequest, pathValue string) bool {
	if mount.Tenant != req.Tenant || mount.Workspace != req.Workspace || mount.Generation != req.Generation ||
		mount.VolumeID != req.ID || mount.Path != pathValue || mount.WorkspaceRoot != filepath.Clean(req.WorkspaceRoot) || !mount.ReadOnly {
		return false
	}
	return (req.Version == 0 && req.Artifact == "") || (mount.VolumeVersion == req.Version && mount.Artifact == req.Artifact)
}

func (b *LocalBackend) liveMountMatches(ctx context.Context, mount Attachment) (bool, error) {
	if b.resolver == nil || b.mounter == nil {
		return false, ErrUnsupported
	}
	source, err := b.resolver.OpenArtifact(ctx, mount.Tenant, mount.Artifact)
	if err != nil {
		return false, err
	}
	defer source.Close()
	expected, err := source.Stat()
	if err != nil {
		return false, err
	}
	target, err := openTarget(mount, false)
	if errors.Is(err, os.ErrNotExist) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	status, inspectErr := b.mounter.Inspect(ctx, target)
	closeErr := target.Close()
	if inspectErr != nil || closeErr != nil {
		return false, errors.Join(inspectErr, closeErr)
	}
	return status.Mounted && status.ReadOnly && status.Source != nil && os.SameFile(expected, status.Source), nil
}

func (b *LocalBackend) recoverCommittedAttach(ctx context.Context, key, opKey, operationID, fingerprint string, mount Attachment, op operation) (Attachment, error) {
	matches, err := b.liveMountMatches(ctx, mount)
	if err != nil {
		return Attachment{}, err
	}
	if matches && op.MountAttempted {
		return *op.Attachment, nil
	}
	b.mu.Lock()
	if err := b.ensureOpenLocked(); err != nil {
		b.mu.Unlock()
		return Attachment{}, err
	}
	if err := b.checkAuthorizedGenerationLocked(mount.Tenant, mount.Workspace, mount.Generation); err != nil {
		b.mu.Unlock()
		return Attachment{}, err
	}
	current, attachmentPresent := b.state.Attachments[key]
	currentOp, operationPresent := b.state.Operations[opKey]
	if !attachmentPresent || current != mount || !operationPresent || !currentOp.Done || currentOp.Fingerprint != fingerprint {
		b.mu.Unlock()
		return Attachment{}, ErrConflict
	}
	err = b.mutateLocked(func() {
		current.State = stateAttaching
		b.state.Attachments[key] = current
		currentOp.Done = false
		currentOp.Attachment = nil
		currentOp.CompletedAt = time.Time{}
		currentOp.MountAttempted = false
		currentOp.ReplaceExisting = true
		b.state.Operations[opKey] = currentOp
	})
	b.mu.Unlock()
	if err != nil {
		return Attachment{}, err
	}
	return b.performAttach(ctx, key, operationID, true, false, true)
}

// Detach persists a detaching fence before synchronously unmounting.
func (b *LocalBackend) Detach(ctx context.Context, req DetachRequest) error {
	pathValue, _, err := cleanMountPath(req.Path)
	if err != nil {
		return err
	}
	if err := validateOperationTenant(req.OperationID, req.Tenant); err != nil {
		return err
	}
	if err := validateSegment("workspace", req.Workspace); err != nil {
		return err
	}
	fp := fingerprint("detach", req.Tenant, req.Workspace, req.Generation, pathValue)
	key := attachmentKey(req.Tenant, req.Workspace, pathValue)
	b.mu.Lock()
	if err := b.ensureOpenLocked(); err != nil {
		b.mu.Unlock()
		return err
	}
	opKey := operationKey(req.Tenant, req.OperationID)
	if op, found := b.state.Operations[opKey]; found {
		if op.Fingerprint != fp || op.Kind != "detach" || op.ResourceKey != key {
			b.mu.Unlock()
			return ErrConflict
		}
		if op.Done {
			if _, attachmentPresent := b.state.Attachments[key]; attachmentPresent {
				b.mu.Unlock()
				return ErrConflict
			}
			b.mu.Unlock()
			return nil
		}
		_, attachmentPresent := b.state.Attachments[key]
		b.mu.Unlock()
		if !attachmentPresent {
			return b.commitDetach(key, req.Tenant, req.OperationID)
		}
		return b.performDetach(ctx, key, req.OperationID)
	}
	if err := b.checkGenerationLocked(req.Tenant, req.Workspace, req.Generation); err != nil {
		b.mu.Unlock()
		return err
	}
	mount, ok := b.state.Attachments[key]
	if !ok {
		b.mu.Unlock()
		return ErrNotFound
	}
	if mount.Generation != req.Generation || mount.State != stateAttached {
		b.mu.Unlock()
		return ErrConflict
	}
	if err := b.admitOperationLocked(); err != nil {
		b.mu.Unlock()
		return err
	}
	if err := b.mutateLocked(func() {
		mount.State = stateDetaching
		b.state.Attachments[key] = mount
		b.state.Operations[operationKey(req.Tenant, req.OperationID)] = operation{Kind: "detach", Fingerprint: fp, ResourceKey: key}
	}); err != nil {
		b.mu.Unlock()
		return err
	}
	b.mu.Unlock()
	return b.performDetach(ctx, key, req.OperationID)
}

func (b *LocalBackend) performDetach(ctx context.Context, key, operationID string) error {
	b.mu.Lock()
	mount, ok := b.state.Attachments[key]
	b.mu.Unlock()
	if !ok {
		return nil
	}
	if b.mounter == nil {
		return ErrUnsupported
	}
	target, err := openTarget(mount, false)
	if errors.Is(err, os.ErrNotExist) {
		if mount.TargetInode != 0 {
			return ErrUnsafePath
		}
		return b.commitDetach(key, mount.Tenant, operationID)
	}
	if err != nil {
		return err
	}
	defer target.Close()
	if err := verifyDirectoryIdentity(target, mount.TargetDevice, mount.TargetInode); err != nil {
		return err
	}
	status, err := b.mounter.Inspect(ctx, target)
	if err != nil {
		return err
	}
	if status.Mounted {
		if err := b.mounter.Unmount(ctx, target); err != nil {
			return err
		}
		status, err = b.mounter.Inspect(ctx, target)
		if err != nil {
			return err
		}
		if status.Mounted {
			return errors.New("volume: mount remains after unmount")
		}
	}
	return b.commitDetach(key, mount.Tenant, operationID)
}

func (b *LocalBackend) commitDetach(key, tenant, operationID string) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	now := b.opts.Clock().UTC()
	return b.mutateLocked(func() {
		delete(b.state.Attachments, key)
		if operationID != "" {
			opKey := operationKey(tenant, operationID)
			op := b.state.Operations[opKey]
			op.Done, op.CompletedAt = true, now
			b.state.Operations[opKey] = op
		} else {
			b.completeRecoveredOperationLocked("detach", key, nil)
			b.removeRecoveredOperationLocked("attach", key)
			b.state.Counters.ReconciledDetach++
		}
	})
}

// SetWorkspaceGeneration advances the local authority fence and drains every
// older attachment before returning. It never accepts a generation rollback.
func (b *LocalBackend) SetWorkspaceGeneration(ctx context.Context, tenant, workspace string, generation uint64) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := validateTenant(tenant); err != nil {
		return err
	}
	if err := validateSegment("workspace", workspace); err != nil || generation == 0 {
		return ErrStaleGeneration
	}
	key := workspaceKey(tenant, workspace)
	b.mu.Lock()
	if err := b.ensureOpenLocked(); err != nil {
		b.mu.Unlock()
		return err
	}
	current := b.state.Fences[key]
	if generation < current {
		b.state.Counters.StaleRejections++
		_ = saveState(b.filename, b.state)
		b.mu.Unlock()
		return ErrStaleGeneration
	}
	delete(b.authorized, key)
	evictFence := ""
	if current == 0 && len(b.state.Fences) >= b.opts.MaxWorkspaceFences {
		var err error
		evictFence, err = b.fenceEvictionCandidateLocked()
		if err != nil {
			b.mu.Unlock()
			return err
		}
	}
	var stale []string
	needsCommit := generation != current
	for mountKey, mount := range b.state.Attachments {
		if mount.Tenant == tenant && mount.Workspace == workspace &&
			(mount.Generation < generation || (mount.Generation == generation && mount.State == stateDetaching)) {
			stale = append(stale, mountKey)
			if mount.State != stateDetaching {
				needsCommit = true
			}
		}
	}
	if !needsCommit && len(stale) == 0 {
		b.authorized[key] = generation
		b.mu.Unlock()
		return nil
	}
	err := b.mutateLocked(func() {
		if generation > current {
			if evictFence != "" {
				delete(b.state.Fences, evictFence)
				delete(b.state.FenceUpdated, evictFence)
				delete(b.authorized, evictFence)
				b.state.Counters.PrunedFences++
			}
			b.state.Fences[key] = generation
			b.state.FenceUpdated[key] = b.opts.Clock().UTC()
		}
		for _, mountKey := range stale {
			mount, ok := b.state.Attachments[mountKey]
			if ok && mount.State != stateDetaching {
				mount.State = stateDetaching
				b.state.Attachments[mountKey] = mount
			}
		}
	})
	b.mu.Unlock()
	if err != nil {
		return err
	}
	for _, mountKey := range stale {
		if err := b.performDetach(ctx, mountKey, ""); err != nil {
			return err
		}
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.state.Fences[key] != generation {
		return ErrStaleGeneration
	}
	b.authorized[key] = generation
	return nil
}

// List returns a stable tenant-filtered copy.
func (b *LocalBackend) List(ctx context.Context, tenant string) ([]Volume, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := validateTenant(tenant); err != nil {
		return nil, err
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if err := b.ensureOpenLocked(); err != nil {
		return nil, err
	}
	return sortedVolumes(b.state, tenant), nil
}

// Inspect returns version and attachment history only within tenant scope.
func (b *LocalBackend) Inspect(ctx context.Context, tenant, id string) (Detail, error) {
	if err := ctx.Err(); err != nil {
		return Detail{}, err
	}
	if err := validateVolumeIdentity("inspect", tenant, id); err != nil {
		return Detail{}, err
	}
	b.mu.Lock()
	if err := b.ensureOpenLocked(); err != nil {
		b.mu.Unlock()
		return Detail{}, err
	}
	record, ok := b.state.Volumes[volumeKey(tenant, id)]
	if !ok {
		b.mu.Unlock()
		return Detail{}, ErrNotFound
	}
	detail := Detail{Volume: record.Volume, Versions: append([]Version(nil), record.Versions...)}
	for _, mount := range b.state.Attachments {
		if mount.Tenant == tenant && mount.VolumeID == id {
			detail.Attachments = append(detail.Attachments, mount)
		}
	}
	b.mu.Unlock()
	for i := range detail.Attachments {
		mount := &detail.Attachments[i]
		mount.MountPresent = false
		mount.MountVerified = false
		if b.mounter == nil || b.resolver == nil {
			return Detail{}, ErrUnsupported
		}
		source, err := b.resolver.OpenArtifact(ctx, mount.Tenant, mount.Artifact)
		if err != nil {
			return Detail{}, err
		}
		expectedSource, err := source.Stat()
		closeSourceErr := source.Close()
		if err != nil || closeSourceErr != nil {
			return Detail{}, errors.Join(err, closeSourceErr)
		}
		target, err := openTarget(*mount, false)
		if errors.Is(err, os.ErrNotExist) {
			mount.ReadOnly = false
			continue
		}
		if err != nil {
			return Detail{}, err
		}
		if err := verifyDirectoryIdentity(target, mount.TargetDevice, mount.TargetInode); err != nil {
			target.Close()
			return Detail{}, err
		}
		status, inspectErr := b.mounter.Inspect(ctx, target)
		closeErr := target.Close()
		if inspectErr != nil || closeErr != nil {
			return Detail{}, errors.Join(inspectErr, closeErr)
		}
		mount.MountPresent = status.Mounted
		mount.MountVerified = status.Mounted && status.ReadOnly && status.Source != nil && os.SameFile(expectedSource, status.Source)
		mount.ReadOnly = mount.MountVerified
	}
	sort.Slice(detail.Attachments, func(i, j int) bool {
		if detail.Attachments[i].Workspace != detail.Attachments[j].Workspace {
			return detail.Attachments[i].Workspace < detail.Attachments[j].Workspace
		}
		return detail.Attachments[i].Path < detail.Attachments[j].Path
	})
	return detail, nil
}

// Stats returns retained use and persisted counters.
func (b *LocalBackend) Stats() Stats {
	b.mu.Lock()
	defer b.mu.Unlock()
	stats := b.state.Counters
	stats.Volumes = len(b.state.Volumes)
	stats.Attachments = len(b.state.Attachments)
	stats.WorkspaceFences = len(b.state.Fences)
	stats.Operations = len(b.state.Operations)
	return stats
}

// ArtifactReferences returns a stable, deduplicated snapshot of artifacts
// needed to recover active or intermediate attachments. Catalog history is a
// rehydratable control-plane cache and must not pin node-local blobs forever.
func (b *LocalBackend) ArtifactReferences() []string {
	b.mu.Lock()
	defer b.mu.Unlock()
	set := make(map[string]struct{})
	for _, mount := range b.state.Attachments {
		set[mount.Artifact] = struct{}{}
	}
	refs := make([]string, 0, len(set))
	for id := range set {
		refs = append(refs, id)
	}
	sort.Strings(refs)
	return refs
}

// Close prevents new operations. The resolver and mount engine remain owned by
// their caller because they may be shared with another local backend.
func (b *LocalBackend) Close() error {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.closed = true
	return nil
}

func openTarget(mount Attachment, create bool) (*os.File, error) {
	_, rel, err := cleanMountPath(mount.Path)
	if err != nil {
		return nil, err
	}
	return openDirectoryNoSymlink(mount.WorkspaceRoot, rel, create)
}

func verifyDirectoryIdentity(target *os.File, device, inode uint64) error {
	if inode == 0 {
		return nil // legacy attachment; the no-symlink open still applies.
	}
	actualDevice, actualInode, err := directoryIdentity(target)
	if err != nil {
		return err
	}
	if actualDevice != device || actualInode != inode {
		return ErrUnsafePath
	}
	return nil
}

func ensureEmpty(dir *os.File) error {
	entries, err := dir.ReadDir(1)
	if err != nil && !errors.Is(err, io.EOF) {
		return err
	}
	if len(entries) != 0 {
		return errors.New("volume: mount target is not empty")
	}
	return nil
}

func (b *LocalBackend) cancelAttach(key, tenant, operationID string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	_ = b.mutateLocked(func() {
		delete(b.state.Attachments, key)
		delete(b.state.Operations, operationKey(tenant, operationID))
	})
}

func (b *LocalBackend) markMountAttempted(key, tenant, operationID string) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	opKey := operationKey(tenant, operationID)
	op, ok := b.state.Operations[opKey]
	mount, mountOK := b.state.Attachments[key]
	if !ok || op.Kind != "attach" || op.ResourceKey != key || op.Done || !mountOK || mount.State != stateAttaching {
		return ErrStaleGeneration
	}
	if op.MountAttempted {
		return nil
	}
	return b.mutateLocked(func() {
		op.MountAttempted = true
		b.state.Operations[opKey] = op
	})
}

func (b *LocalBackend) mutateLocked(change func()) error {
	before, err := cloneState(b.state)
	if err != nil {
		return err
	}
	change()
	if err := saveState(b.filename, b.state); err != nil {
		b.state = before
		return err
	}
	return nil
}

func (b *LocalBackend) replayLocked(tenant, id, fingerprint string) (operation, bool, error) {
	op, ok := b.state.Operations[operationKey(tenant, id)]
	if !ok {
		return operation{}, false, nil
	}
	if op.Fingerprint != fingerprint {
		return operation{}, true, ErrConflict
	}
	if !op.Done {
		return operation{}, true, ErrConflict
	}
	return op, true, nil
}

func (b *LocalBackend) admitOperationLocked() error {
	b.pruneOperationsLocked()
	if len(b.state.Operations) >= b.opts.MaxOperations {
		b.rejectQuotaLocked()
		return ErrQuota
	}
	return nil
}

func (b *LocalBackend) pruneOperationsLocked() {
	cutoff := b.opts.Clock().UTC().Add(-b.opts.OperationRetention)
	for key, op := range b.state.Operations {
		if op.Done && !op.CompletedAt.IsZero() && op.CompletedAt.Before(cutoff) {
			delete(b.state.Operations, key)
		}
	}
}

func (b *LocalBackend) makeVersionRoomLocked(record *volumeRecord) error {
	for len(record.Versions) >= b.opts.MaxVersionsPerVolume {
		index := -1
		for i, version := range record.Versions {
			if version.Number == record.Volume.Version || b.versionPinnedLocked(record.Volume.Tenant, record.Volume.ID, version.Number) {
				continue
			}
			index = i
			break
		}
		if index < 0 {
			b.rejectQuotaLocked()
			return ErrQuota
		}
		record.Versions = append(record.Versions[:index], record.Versions[index+1:]...)
	}
	return nil
}

func (b *LocalBackend) versionPinnedLocked(tenant, id string, version uint64) bool {
	for _, mount := range b.state.Attachments {
		if mount.Tenant == tenant && mount.VolumeID == id && mount.VolumeVersion == version {
			return true
		}
	}
	return false
}

func (b *LocalBackend) checkGenerationLocked(tenant, workspace string, generation uint64) error {
	if b.state.Fences[workspaceKey(tenant, workspace)] != generation {
		b.state.Counters.StaleRejections++
		_ = saveState(b.filename, b.state)
		return ErrStaleGeneration
	}
	return nil
}

func (b *LocalBackend) checkAuthorizedGenerationLocked(tenant, workspace string, generation uint64) error {
	key := workspaceKey(tenant, workspace)
	if b.state.Fences[key] != generation || b.authorized[key] != generation {
		b.state.Counters.StaleRejections++
		_ = saveState(b.filename, b.state)
		return ErrStaleGeneration
	}
	return nil
}

func (b *LocalBackend) rejectQuotaLocked() {
	b.state.Counters.QuotaRejections++
	_ = saveState(b.filename, b.state)
}

func (b *LocalBackend) completeRecoveredOperationLocked(kind, resourceKey string, attachment *Attachment) {
	for key, op := range b.state.Operations {
		if op.Kind == kind && op.ResourceKey == resourceKey && !op.Done {
			op.Done = true
			op.CompletedAt = b.opts.Clock().UTC()
			op.Attachment = attachment
			b.state.Operations[key] = op
		}
	}
}

func (b *LocalBackend) removeRecoveredOperationLocked(kind, resourceKey string) {
	for key, op := range b.state.Operations {
		if op.Kind == kind && op.ResourceKey == resourceKey && !op.Done {
			delete(b.state.Operations, key)
		}
	}
}

func (b *LocalBackend) volumeCountLocked(tenant string) int {
	count := 0
	for _, record := range b.state.Volumes {
		if record.Volume.Tenant == tenant {
			count++
		}
	}
	return count
}

func (b *LocalBackend) mountCountLocked(tenant string) int {
	count := 0
	for _, mount := range b.state.Attachments {
		if mount.Tenant == tenant {
			count++
		}
	}
	return count
}

func (b *LocalBackend) volumeEvictionCandidateLocked(tenant string) (string, error) {
	tenantFull := b.volumeCountLocked(tenant) >= b.opts.MaxVolumesPerTenant
	globalFull := len(b.state.Volumes) >= b.opts.MaxVolumes
	if !tenantFull && !globalFull {
		return "", nil
	}
	attachedVolumes := make(map[string]struct{}, len(b.state.Attachments))
	for _, mount := range b.state.Attachments {
		attachedVolumes[volumeKey(mount.Tenant, mount.VolumeID)] = struct{}{}
	}
	candidate := ""
	var candidateTime time.Time
	for key, record := range b.state.Volumes {
		if tenantFull && record.Volume.Tenant != tenant {
			continue
		}
		if _, attached := attachedVolumes[key]; attached {
			continue
		}
		updated := record.Volume.UpdatedAt
		if candidate == "" || updated.Before(candidateTime) || (updated.Equal(candidateTime) && key < candidate) {
			candidate, candidateTime = key, updated
		}
	}
	if candidate == "" {
		b.rejectQuotaLocked()
		return "", ErrQuota
	}
	return candidate, nil
}

func (b *LocalBackend) fenceEvictionCandidateLocked() (string, error) {
	attachedWorkspaces := make(map[string]struct{}, len(b.state.Attachments))
	for _, mount := range b.state.Attachments {
		attachedWorkspaces[workspaceKey(mount.Tenant, mount.Workspace)] = struct{}{}
	}
	candidate := ""
	var candidateTime time.Time
	for key, updated := range b.state.FenceUpdated {
		tenant, workspace, ok := splitCompositeKey(key)
		if !ok {
			continue
		}
		if _, attached := attachedWorkspaces[workspaceKey(tenant, workspace)]; attached {
			continue
		}
		if candidate == "" || updated.Before(candidateTime) || (updated.Equal(candidateTime) && key < candidate) {
			candidate, candidateTime = key, updated
		}
	}
	if candidate == "" {
		b.rejectQuotaLocked()
		return "", ErrQuota
	}
	return candidate, nil
}

func (b *LocalBackend) ensureOpenLocked() error {
	if b.closed {
		return errors.New("volume: backend closed")
	}
	return nil
}

func validateOperationTenant(operationID, tenant string) error {
	if err := validateSegment("operation id", operationID); err != nil {
		return err
	}
	return validateTenant(tenant)
}

func validateVolumeIdentity(operationID, tenant, id string) error {
	if err := validateOperationTenant(operationID, tenant); err != nil {
		return err
	}
	return validateVolumeID(id)
}

func validateMutation(operationID, tenant, id, artifactID string) error {
	if err := validateVolumeIdentity(operationID, tenant, id); err != nil {
		return err
	}
	if _, err := artifact.Digest(artifactID); err != nil {
		return fmt.Errorf("volume: invalid artifact: %w", err)
	}
	return nil
}

func fingerprint(values ...any) string {
	body, _ := json.Marshal(values)
	sum := sha256.Sum256(body)
	return hex.EncodeToString(sum[:])
}

func completedOperation(kind, fp string, now time.Time, volume *Volume, attachment *Attachment) operation {
	return operation{Kind: kind, Fingerprint: fp, Done: true, Volume: volume, Attachment: attachment, CompletedAt: now}
}

func volumeKey(tenant, id string) string           { return tenant + "\x00" + id }
func workspaceKey(tenant, workspace string) string { return tenant + "\x00" + workspace }
func attachmentKey(tenant, workspace, path string) string {
	return strings.Join([]string{tenant, workspace, path}, "\x00")
}
func operationKey(tenant, id string) string { return tenant + "\x00" + id }

func splitCompositeKey(key string) (string, string, bool) {
	first, second, ok := strings.Cut(key, "\x00")
	return first, second, ok && !strings.Contains(second, "\x00")
}
