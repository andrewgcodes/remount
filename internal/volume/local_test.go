package volume

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"
)

type fakeMounter struct {
	mu         sync.Mutex
	mounted    map[string]fakeMount
	mountCalls int
	unmounts   int
	readOnly   bool
	mountErr   error
	unmountErr error
	entered    chan struct{}
	release    chan struct{}
}

type fakeMount struct {
	readOnly bool
	source   os.FileInfo
}

func newFakeMounter() *fakeMounter {
	return &fakeMounter{mounted: make(map[string]fakeMount), readOnly: true}
}

func (m *fakeMounter) MountReadOnly(ctx context.Context, source, target *os.File) error {
	if m.entered != nil {
		select {
		case m.entered <- struct{}{}:
		default:
		}
		select {
		case <-m.release:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	info, err := source.Stat()
	if err != nil {
		return err
	}
	m.mountCalls++
	m.mounted[target.Name()] = fakeMount{readOnly: m.readOnly, source: info}
	return m.mountErr
}

func (m *fakeMounter) Unmount(_ context.Context, target *os.File) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.unmountErr != nil {
		return m.unmountErr
	}
	delete(m.mounted, target.Name())
	m.unmounts++
	return nil
}

func (m *fakeMounter) Inspect(_ context.Context, target *os.File) (MountStatus, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	mount, ok := m.mounted[target.Name()]
	return MountStatus{Mounted: ok, ReadOnly: mount.readOnly, Source: mount.source}, nil
}

func (m *fakeMounter) forgetAll() {
	m.mu.Lock()
	defer m.mu.Unlock()
	clear(m.mounted)
}

func artifactID(c byte) string { return "art_sha256:" + strings.Repeat(string(c), 64) }

type fixture struct {
	backend  *LocalBackend
	resolver *DirectoryResolver
	mounter  *fakeMounter
	state    string
	sources  string
	root     string
}

func newFixture(t *testing.T, opts Options) *fixture {
	t.Helper()
	dir := t.TempDir()
	sources := filepath.Join(dir, "sources")
	root := filepath.Join(dir, "workspace")
	if err := os.MkdirAll(sources, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(root, 0o700); err != nil {
		t.Fatal(err)
	}
	resolver, err := OpenDirectoryResolver(sources)
	if err != nil {
		t.Fatal(err)
	}
	mounter := newFakeMounter()
	state := filepath.Join(dir, "state", "volumes.json")
	backend, err := OpenLocalBackend(context.Background(), state, resolver, mounter, opts)
	if err != nil {
		t.Fatal(err)
	}
	return &fixture{backend: backend, resolver: resolver, mounter: mounter, state: state, sources: sources, root: root}
}

func (f *fixture) source(t *testing.T, tenant, artifact string) {
	t.Helper()
	dir := filepath.Join(f.sources, tenant, artifact)
	if err := os.MkdirAll(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "payload"), []byte(artifact), 0o600); err != nil {
		t.Fatal(err)
	}
}

func createVolume(t *testing.T, f *fixture, tenant, id, artifact string) Volume {
	t.Helper()
	f.source(t, tenant, artifact)
	volume, err := f.backend.Create(context.Background(), CreateRequest{OperationID: "create-" + id, Tenant: tenant, ID: id, Artifact: artifact})
	if err != nil {
		t.Fatal(err)
	}
	return volume
}

func TestLocalBackendLifecyclePinsPublishedVersion(t *testing.T) {
	f := newFixture(t, Options{})
	ctx := context.Background()
	oldArtifact, newArtifact := artifactID('a'), artifactID('b')
	created := createVolume(t, f, "tenant1", "vol1", oldArtifact)
	if created.Version != 1 || created.Artifact != oldArtifact {
		t.Fatalf("created = %+v", created)
	}
	if replay, err := f.backend.Create(ctx, CreateRequest{OperationID: "create-vol1", Tenant: "tenant1", ID: "vol1", Artifact: oldArtifact}); err != nil || replay != created {
		t.Fatalf("create replay = %+v, %v", replay, err)
	}
	if _, err := f.backend.Create(ctx, CreateRequest{OperationID: "create-vol1", Tenant: "tenant1", ID: "different", Artifact: oldArtifact}); !errors.Is(err, ErrConflict) {
		t.Fatalf("argument-changing replay = %v", err)
	}
	if err := f.backend.SetWorkspaceGeneration(ctx, "tenant1", "ws1", 7); err != nil {
		t.Fatal(err)
	}
	attachReq := AttachRequest{OperationID: "attach-1", Tenant: "tenant1", ID: "vol1", Workspace: "ws1", Generation: 7, Path: "/data", WorkspaceRoot: f.root, ReadOnly: true}
	attached, err := f.backend.Attach(ctx, attachReq)
	if err != nil {
		t.Fatal(err)
	}
	if !attached.ReadOnly || attached.Artifact != oldArtifact || attached.VolumeVersion != 1 || attached.State != stateAttached {
		t.Fatalf("attachment = %+v", attached)
	}
	if replay, err := f.backend.Attach(ctx, attachReq); err != nil || replay != attached {
		t.Fatalf("attach replay = %+v, %v", replay, err)
	}
	if f.mounter.mountCalls != 1 {
		t.Fatalf("mount calls = %d, want 1", f.mounter.mountCalls)
	}
	f.source(t, "tenant1", newArtifact)
	published, err := f.backend.Publish(ctx, PublishRequest{OperationID: "publish-1", Tenant: "tenant1", ID: "vol1", Artifact: newArtifact, Workspace: "ws1", Generation: 7, ExpectedVersion: 1})
	if err != nil {
		t.Fatal(err)
	}
	if published.Version != 2 || published.Artifact != newArtifact {
		t.Fatalf("published = %+v", published)
	}
	detail, err := f.backend.Inspect(ctx, "tenant1", "vol1")
	if err != nil {
		t.Fatal(err)
	}
	if len(detail.Attachments) != 1 || detail.Attachments[0].Artifact != oldArtifact {
		t.Fatalf("publish changed mounted bytes: %+v", detail.Attachments)
	}
	if err := f.backend.Delete(ctx, DeleteRequest{OperationID: "delete-1", Tenant: "tenant1", ID: "vol1"}); !errors.Is(err, ErrConflict) {
		t.Fatalf("delete attached volume = %v", err)
	}
	if err := f.backend.Detach(ctx, DetachRequest{OperationID: "detach-1", Tenant: "tenant1", Workspace: "ws1", Generation: 7, Path: "/data"}); err != nil {
		t.Fatal(err)
	}
	if err := f.backend.Delete(ctx, DeleteRequest{OperationID: "delete-1", Tenant: "tenant1", ID: "vol1"}); err != nil {
		t.Fatal(err)
	}
	if err := f.backend.Delete(ctx, DeleteRequest{OperationID: "delete-1", Tenant: "tenant1", ID: "vol1"}); err != nil {
		t.Fatalf("delete replay = %v", err)
	}
}

func TestTenantIsolationAndReadOnlyAdmission(t *testing.T) {
	f := newFixture(t, Options{})
	ctx := context.Background()
	id := artifactID('c')
	createVolume(t, f, "tenant1", "same", id)
	createVolume(t, f, "tenant2", "same", id)
	if _, err := f.backend.Inspect(ctx, "tenant3", "same"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("cross-tenant inspect = %v", err)
	}
	if err := f.backend.SetWorkspaceGeneration(ctx, "tenant1", "ws1", 1); err != nil {
		t.Fatal(err)
	}
	_, err := f.backend.Attach(ctx, AttachRequest{OperationID: "writable", Tenant: "tenant1", ID: "same", Workspace: "ws1", Generation: 1, Path: "/data", WorkspaceRoot: f.root})
	if !errors.Is(err, ErrReadOnlyRequired) {
		t.Fatalf("writable attach = %v", err)
	}
	volumes, err := f.backend.List(ctx, "tenant1")
	if err != nil || len(volumes) != 1 || volumes[0].Tenant != "tenant1" {
		t.Fatalf("tenant list = %+v, %v", volumes, err)
	}
}

func TestVolumeIdentityUsesWireGrammar(t *testing.T) {
	for _, valid := range []string{"a", "build.cache_01", strings.Repeat("v", 64)} {
		if err := validateVolumeID(valid); err != nil {
			t.Errorf("valid volume id %q: %v", valid, err)
		}
	}
	for _, invalid := range []string{".hidden", "tenant:volume", "slash/name", strings.Repeat("v", 65)} {
		if err := validateVolumeID(invalid); err == nil {
			t.Errorf("invalid volume id %q accepted", invalid)
		}
	}
}

func TestEnsureVersionPinsControlResolvedHistory(t *testing.T) {
	f := newFixture(t, Options{})
	ctx := context.Background()
	oldID, newID := artifactID('1'), artifactID('2')
	f.source(t, "tenant1", oldID)
	f.source(t, "tenant1", newID)
	if err := f.backend.EnsureVersion(ctx, "tenant1", "dataset", 7, newID); err != nil {
		t.Fatal(err)
	}
	if err := f.backend.EnsureVersion(ctx, "tenant1", "dataset", 3, oldID); err != nil {
		t.Fatal(err)
	}
	if err := f.backend.SetWorkspaceGeneration(ctx, "tenant1", "ws1", 4); err != nil {
		t.Fatal(err)
	}
	attached, err := f.backend.Attach(ctx, AttachRequest{
		OperationID: "attach-pinned", Tenant: "tenant1", ID: "dataset", Workspace: "ws1", Generation: 4,
		Path: "/data", WorkspaceRoot: f.root, ReadOnly: true, Version: 3, Artifact: oldID,
	})
	if err != nil {
		t.Fatal(err)
	}
	if attached.VolumeVersion != 3 || attached.Artifact != oldID {
		t.Fatalf("attachment = %+v, want pinned version 3", attached)
	}
	if err := f.backend.EnsureVersion(ctx, "tenant1", "dataset", 3, newID); !errors.Is(err, ErrConflict) {
		t.Fatalf("conflicting version identity = %v, want conflict", err)
	}
}

func TestGenerationFenceSynchronouslyDrainsStaleMount(t *testing.T) {
	f := newFixture(t, Options{})
	ctx := context.Background()
	id := artifactID('d')
	createVolume(t, f, "tenant1", "vol1", id)
	if err := f.backend.SetWorkspaceGeneration(ctx, "tenant1", "ws1", 1); err != nil {
		t.Fatal(err)
	}
	_, err := f.backend.Attach(ctx, AttachRequest{OperationID: "attach-1", Tenant: "tenant1", ID: "vol1", Workspace: "ws1", Generation: 1, Path: "/data", WorkspaceRoot: f.root, ReadOnly: true})
	if err != nil {
		t.Fatal(err)
	}
	if err := f.backend.SetWorkspaceGeneration(ctx, "tenant1", "ws1", 2); err != nil {
		t.Fatal(err)
	}
	if f.mounter.unmounts != 1 {
		t.Fatalf("unmounts = %d, want 1", f.mounter.unmounts)
	}
	detail, err := f.backend.Inspect(ctx, "tenant1", "vol1")
	if err != nil || len(detail.Attachments) != 0 {
		t.Fatalf("stale attachments = %+v, %v", detail.Attachments, err)
	}
	_, err = f.backend.Attach(ctx, AttachRequest{OperationID: "attach-stale", Tenant: "tenant1", ID: "vol1", Workspace: "ws1", Generation: 1, Path: "/old", WorkspaceRoot: f.root, ReadOnly: true})
	if !errors.Is(err, ErrStaleGeneration) {
		t.Fatalf("stale attach = %v", err)
	}
	_, err = f.backend.Publish(ctx, PublishRequest{OperationID: "publish-stale", Tenant: "tenant1", ID: "vol1", Artifact: artifactID('e'), Workspace: "ws1", Generation: 1, ExpectedVersion: 1})
	if !errors.Is(err, ErrStaleGeneration) {
		t.Fatalf("stale publish = %v", err)
	}
	if err := f.backend.SetWorkspaceGeneration(ctx, "tenant1", "ws1", 1); !errors.Is(err, ErrStaleGeneration) {
		t.Fatalf("generation rollback = %v", err)
	}
}

func TestGenerationRetryDrainsFailedStaleDetach(t *testing.T) {
	f := newFixture(t, Options{})
	ctx := context.Background()
	id := artifactID('e')
	createVolume(t, f, "tenant1", "vol1", id)
	if err := f.backend.SetWorkspaceGeneration(ctx, "tenant1", "ws1", 1); err != nil {
		t.Fatal(err)
	}
	if _, err := f.backend.Attach(ctx, AttachRequest{OperationID: "attach-1", Tenant: "tenant1", ID: "vol1", Workspace: "ws1", Generation: 1, Path: "/data", WorkspaceRoot: f.root, ReadOnly: true}); err != nil {
		t.Fatal(err)
	}
	detachFailure := errors.New("injected unmount failure")
	f.mounter.mu.Lock()
	f.mounter.unmountErr = detachFailure
	f.mounter.mu.Unlock()
	if err := f.backend.SetWorkspaceGeneration(ctx, "tenant1", "ws1", 2); !errors.Is(err, detachFailure) {
		t.Fatalf("generation advance = %v, want injected failure", err)
	}
	detail, err := f.backend.Inspect(ctx, "tenant1", "vol1")
	if err != nil || len(detail.Attachments) != 1 || detail.Attachments[0].State != stateDetaching {
		t.Fatalf("failed drain state = %+v, %v", detail.Attachments, err)
	}
	f.mounter.mu.Lock()
	f.mounter.unmountErr = nil
	f.mounter.mu.Unlock()
	if err := f.backend.SetWorkspaceGeneration(ctx, "tenant1", "ws1", 2); err != nil {
		t.Fatalf("same-generation retry did not drain stale mount: %v", err)
	}
	detail, err = f.backend.Inspect(ctx, "tenant1", "vol1")
	if err != nil || len(detail.Attachments) != 0 {
		t.Fatalf("stale attachment survived retry: %+v, %v", detail.Attachments, err)
	}
}

func TestDetachRetryResumesPendingUnmount(t *testing.T) {
	f := newFixture(t, Options{})
	ctx := context.Background()
	id := artifactID('f')
	createVolume(t, f, "tenant1", "vol1", id)
	if err := f.backend.SetWorkspaceGeneration(ctx, "tenant1", "ws1", 1); err != nil {
		t.Fatal(err)
	}
	if _, err := f.backend.Attach(ctx, AttachRequest{OperationID: "attach-1", Tenant: "tenant1", ID: "vol1", Workspace: "ws1", Generation: 1, Path: "/data", WorkspaceRoot: f.root, ReadOnly: true}); err != nil {
		t.Fatal(err)
	}
	failure := errors.New("injected unmount failure")
	f.mounter.mu.Lock()
	f.mounter.unmountErr = failure
	f.mounter.mu.Unlock()
	req := DetachRequest{OperationID: "detach-1", Tenant: "tenant1", Workspace: "ws1", Generation: 1, Path: "/data"}
	if err := f.backend.Detach(ctx, req); !errors.Is(err, failure) {
		t.Fatalf("first detach = %v, want injected failure", err)
	}
	f.mounter.mu.Lock()
	f.mounter.unmountErr = nil
	f.mounter.mu.Unlock()
	if err := f.backend.Detach(ctx, req); err != nil {
		t.Fatalf("exact pending detach retry = %v", err)
	}
	if err := f.backend.Detach(ctx, req); err != nil {
		t.Fatalf("completed detach replay = %v", err)
	}
	detail, err := f.backend.Inspect(ctx, "tenant1", "vol1")
	if err != nil || len(detail.Attachments) != 0 {
		t.Fatalf("retried detach left attachment: %+v, %v", detail.Attachments, err)
	}
}

func TestAttachRetryCommitsAmbiguousReadOnlyMount(t *testing.T) {
	f := newFixture(t, Options{})
	ctx := context.Background()
	id := artifactID('0')
	createVolume(t, f, "tenant1", "vol1", id)
	if err := f.backend.SetWorkspaceGeneration(ctx, "tenant1", "ws1", 1); err != nil {
		t.Fatal(err)
	}
	failure := errors.New("injected ambiguous mount result")
	f.mounter.mu.Lock()
	f.mounter.mountErr = failure
	f.mounter.mu.Unlock()
	req := AttachRequest{OperationID: "attach-ambiguous", Tenant: "tenant1", ID: "vol1", Workspace: "ws1", Generation: 1, Path: "/data", WorkspaceRoot: f.root, ReadOnly: true}
	if _, err := f.backend.Attach(ctx, req); !errors.Is(err, failure) {
		t.Fatalf("first attach = %v, want injected ambiguity", err)
	}
	f.mounter.mu.Lock()
	f.mounter.mountErr = nil
	f.mounter.mu.Unlock()
	attached, err := f.backend.Attach(ctx, req)
	if err != nil || attached.State != stateAttached {
		t.Fatalf("pending attach retry = %+v, %v", attached, err)
	}
	if f.mounter.mountCalls != 1 {
		t.Fatalf("retry duplicated a verified mount: calls=%d", f.mounter.mountCalls)
	}
	if replay, err := f.backend.Attach(ctx, req); err != nil || replay != attached {
		t.Fatalf("completed attach replay = %+v, %v", replay, err)
	}
}

func TestAttachRetryNeverAdoptsPreexistingMount(t *testing.T) {
	f := newFixture(t, Options{})
	ctx := context.Background()
	id := artifactID('3')
	createVolume(t, f, "tenant1", "vol1", id)
	if err := f.backend.SetWorkspaceGeneration(ctx, "tenant1", "ws1", 1); err != nil {
		t.Fatal(err)
	}
	target, err := openTarget(Attachment{Path: "/data", WorkspaceRoot: f.root}, true)
	if err != nil {
		t.Fatal(err)
	}
	preexisting, err := target.Stat()
	if err != nil {
		t.Fatal(err)
	}
	f.mounter.mu.Lock()
	f.mounter.mounted[target.Name()] = fakeMount{readOnly: true, source: preexisting}
	f.mounter.mu.Unlock()
	target.Close()
	req := AttachRequest{OperationID: "attach-preexisting", Tenant: "tenant1", ID: "vol1", Workspace: "ws1", Generation: 1, Path: "/data", WorkspaceRoot: f.root, ReadOnly: true}
	for attempt := 0; attempt < 2; attempt++ {
		if _, err := f.backend.Attach(ctx, req); !errors.Is(err, ErrConflict) {
			t.Fatalf("attempt %d adopted preexisting mount: %v", attempt+1, err)
		}
	}
	detail, err := f.backend.Inspect(ctx, "tenant1", "vol1")
	if err != nil || len(detail.Attachments) != 0 || f.mounter.mountCalls != 0 {
		t.Fatalf("preexisting mount changed local authority: attachments=%+v calls=%d err=%v", detail.Attachments, f.mounter.mountCalls, err)
	}
}

func TestAttachReplayReplacesWrongReadOnlySource(t *testing.T) {
	for _, restart := range []bool{false, true} {
		t.Run(fmt.Sprintf("restart=%v", restart), func(t *testing.T) {
			f := newFixture(t, Options{})
			ctx := context.Background()
			id := artifactID('4')
			createVolume(t, f, "tenant1", "vol1", id)
			if err := f.backend.SetWorkspaceGeneration(ctx, "tenant1", "ws1", 1); err != nil {
				t.Fatal(err)
			}
			req := AttachRequest{OperationID: "attach-1", Tenant: "tenant1", ID: "vol1", Workspace: "ws1", Generation: 1, Path: "/data", WorkspaceRoot: f.root, ReadOnly: true}
			if _, err := f.backend.Attach(ctx, req); err != nil {
				t.Fatal(err)
			}
			target, err := openTarget(Attachment{Path: "/data", WorkspaceRoot: f.root}, false)
			if err != nil {
				t.Fatal(err)
			}
			wrongDir := t.TempDir()
			wrongSource, err := os.Stat(wrongDir)
			if err != nil {
				t.Fatal(err)
			}
			f.mounter.mu.Lock()
			f.mounter.mounted[target.Name()] = fakeMount{readOnly: true, source: wrongSource}
			f.mounter.mu.Unlock()
			target.Close()

			backend := f.backend
			if restart {
				backend, err = OpenLocalBackend(ctx, f.state, f.resolver, f.mounter, Options{})
				if err != nil {
					t.Fatal(err)
				}
				if f.mounter.mountCalls != 1 || f.mounter.unmounts != 0 {
					t.Fatalf("startup acted on wrong mount: calls=%d unmounts=%d", f.mounter.mountCalls, f.mounter.unmounts)
				}
				if err := backend.SetWorkspaceGeneration(ctx, "tenant1", "ws1", 1); err != nil {
					t.Fatal(err)
				}
			}
			detail, err := backend.Inspect(ctx, "tenant1", "vol1")
			if err != nil || len(detail.Attachments) != 1 || detail.Attachments[0].MountVerified {
				t.Fatalf("wrong source was reported verified: %+v, %v", detail.Attachments, err)
			}
			if _, err := backend.Attach(ctx, req); err != nil {
				t.Fatalf("authorized repair: %v", err)
			}
			if f.mounter.mountCalls != 2 || f.mounter.unmounts != 1 {
				t.Fatalf("wrong source was adopted: calls=%d unmounts=%d", f.mounter.mountCalls, f.mounter.unmounts)
			}
			detail, err = backend.Inspect(ctx, "tenant1", "vol1")
			if err != nil || !detail.Attachments[0].MountVerified {
				t.Fatalf("repaired source was not verified: %+v, %v", detail.Attachments, err)
			}
		})
	}
}

func TestAttachAndDeleteCannotCross(t *testing.T) {
	f := newFixture(t, Options{})
	ctx := context.Background()
	id := artifactID('f')
	createVolume(t, f, "tenant1", "vol1", id)
	if err := f.backend.SetWorkspaceGeneration(ctx, "tenant1", "ws1", 1); err != nil {
		t.Fatal(err)
	}
	f.mounter.entered = make(chan struct{}, 1)
	f.mounter.release = make(chan struct{})
	result := make(chan error, 1)
	go func() {
		_, err := f.backend.Attach(ctx, AttachRequest{OperationID: "attach-1", Tenant: "tenant1", ID: "vol1", Workspace: "ws1", Generation: 1, Path: "/data", WorkspaceRoot: f.root, ReadOnly: true})
		result <- err
	}()
	select {
	case <-f.mounter.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("attach did not reach mount boundary")
	}
	if err := f.backend.Delete(ctx, DeleteRequest{OperationID: "delete-1", Tenant: "tenant1", ID: "vol1"}); !errors.Is(err, ErrConflict) {
		t.Fatalf("delete crossed pending attach: %v", err)
	}
	close(f.mounter.release)
	if err := <-result; err != nil {
		t.Fatal(err)
	}
}

func TestGenerationAdvanceWinsAgainstInFlightAttach(t *testing.T) {
	f := newFixture(t, Options{})
	ctx := context.Background()
	id := artifactID('d')
	createVolume(t, f, "tenant1", "vol1", id)
	if err := f.backend.SetWorkspaceGeneration(ctx, "tenant1", "ws1", 1); err != nil {
		t.Fatal(err)
	}
	f.mounter.entered = make(chan struct{}, 1)
	f.mounter.release = make(chan struct{})
	result := make(chan error, 1)
	go func() {
		_, err := f.backend.Attach(ctx, AttachRequest{OperationID: "attach-1", Tenant: "tenant1", ID: "vol1", Workspace: "ws1", Generation: 1, Path: "/data", WorkspaceRoot: f.root, ReadOnly: true})
		result <- err
	}()
	select {
	case <-f.mounter.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("attach did not reach mount boundary")
	}
	if err := f.backend.SetWorkspaceGeneration(ctx, "tenant1", "ws1", 2); err != nil {
		t.Fatal(err)
	}
	close(f.mounter.release)
	if err := <-result; !errors.Is(err, ErrStaleGeneration) {
		t.Fatalf("in-flight stale attach = %v", err)
	}
	target, err := openTarget(Attachment{Path: "/data", WorkspaceRoot: f.root}, false)
	if err != nil {
		t.Fatal(err)
	}
	status, err := f.mounter.Inspect(ctx, target)
	target.Close()
	if err != nil || status.Mounted {
		t.Fatalf("stale mount survived: %+v, %v", status, err)
	}
}

func TestPublishHasOneCASWinner(t *testing.T) {
	f := newFixture(t, Options{})
	ctx := context.Background()
	createVolume(t, f, "tenant1", "vol1", artifactID('a'))
	if err := f.backend.SetWorkspaceGeneration(ctx, "tenant1", "ws1", 1); err != nil {
		t.Fatal(err)
	}
	type result struct {
		volume Volume
		err    error
	}
	results := make(chan result, 2)
	for i, candidate := range []string{artifactID('b'), artifactID('c')} {
		f.source(t, "tenant1", candidate)
		go func(i int, artifact string) {
			volume, err := f.backend.Publish(ctx, PublishRequest{OperationID: "publish-" + string(rune('a'+i)), Tenant: "tenant1", ID: "vol1", Artifact: artifact, Workspace: "ws1", Generation: 1, ExpectedVersion: 1})
			results <- result{volume: volume, err: err}
		}(i, candidate)
	}
	winners, conflicts := 0, 0
	for range 2 {
		result := <-results
		switch {
		case result.err == nil && result.volume.Version == 2:
			winners++
		case errors.Is(result.err, ErrConflict):
			conflicts++
		default:
			t.Fatalf("unexpected publish result: %+v", result)
		}
	}
	if winners != 1 || conflicts != 1 {
		t.Fatalf("winners=%d conflicts=%d", winners, conflicts)
	}
}

func TestPublishRefusesVersionOverflow(t *testing.T) {
	f := newFixture(t, Options{})
	ctx := context.Background()
	first, second := artifactID('a'), artifactID('b')
	createVolume(t, f, "tenant1", "vol1", first)
	f.source(t, "tenant1", second)
	if err := f.backend.SetWorkspaceGeneration(ctx, "tenant1", "ws1", 1); err != nil {
		t.Fatal(err)
	}
	f.backend.mu.Lock()
	record := f.backend.state.Volumes[volumeKey("tenant1", "vol1")]
	record.Volume.Version = ^uint64(0)
	record.Versions[len(record.Versions)-1].Number = ^uint64(0)
	err := f.backend.mutateLocked(func() { f.backend.state.Volumes[volumeKey("tenant1", "vol1")] = record })
	f.backend.mu.Unlock()
	if err != nil {
		t.Fatal(err)
	}
	_, err = f.backend.Publish(ctx, PublishRequest{
		OperationID: "overflow", Tenant: "tenant1", ID: "vol1", Artifact: second,
		Workspace: "ws1", Generation: 1, ExpectedVersion: ^uint64(0),
	})
	if !errors.Is(err, ErrConflict) {
		t.Fatalf("overflow publish = %v, want conflict", err)
	}
	detail, err := f.backend.Inspect(ctx, "tenant1", "vol1")
	if err != nil || detail.Volume.Version != ^uint64(0) || detail.Volume.Artifact != first {
		t.Fatalf("overflow mutated volume: %+v err=%v", detail.Volume, err)
	}
}

func TestPathAndSourceSymlinkEscapesAreRejected(t *testing.T) {
	f := newFixture(t, Options{})
	ctx := context.Background()
	id := artifactID('1')
	createVolume(t, f, "tenant1", "vol1", id)
	if err := f.backend.SetWorkspaceGeneration(ctx, "tenant1", "ws1", 1); err != nil {
		t.Fatal(err)
	}
	for i, mountPath := range []string{"data", "/", "/a/../escape", "/.remount/vol", "/a//b"} {
		_, err := f.backend.Attach(ctx, AttachRequest{OperationID: "bad-" + string(rune('a'+i)), Tenant: "tenant1", ID: "vol1", Workspace: "ws1", Generation: 1, Path: mountPath, WorkspaceRoot: f.root, ReadOnly: true})
		if !errors.Is(err, ErrUnsafePath) {
			t.Errorf("path %q = %v", mountPath, err)
		}
	}
	outside := t.TempDir()
	if err := os.Symlink(outside, filepath.Join(f.root, "escape")); err != nil {
		t.Fatal(err)
	}
	_, err := f.backend.Attach(ctx, AttachRequest{OperationID: "bad-symlink", Tenant: "tenant1", ID: "vol1", Workspace: "ws1", Generation: 1, Path: "/escape/data", WorkspaceRoot: f.root, ReadOnly: true})
	if err == nil {
		t.Fatal("target symlink escape succeeded")
	}
	entries, err := os.ReadDir(outside)
	if err != nil || len(entries) != 0 {
		t.Fatalf("outside target changed: %+v, %v", entries, err)
	}
	if err := os.Mkdir(filepath.Join(f.root, "secret"), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink("secret", filepath.Join(f.root, "data")); err != nil {
		t.Fatal(err)
	}
	_, err = f.backend.Attach(ctx, AttachRequest{OperationID: "bad-in-root-symlink", Tenant: "tenant1", ID: "vol1", Workspace: "ws1", Generation: 1, Path: "/data", WorkspaceRoot: f.root, ReadOnly: true})
	if !errors.Is(err, ErrUnsafePath) || f.mounter.mountCalls != 0 {
		t.Fatalf("in-root target symlink attach err=%v mount_calls=%d", err, f.mounter.mountCalls)
	}

	sources := t.TempDir()
	resolverRoot := filepath.Join(t.TempDir(), "resolver")
	if err := os.MkdirAll(filepath.Join(resolverRoot, "tenant1"), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(sources, filepath.Join(resolverRoot, "tenant1", id)); err != nil {
		t.Fatal(err)
	}
	resolver, err := OpenDirectoryResolver(resolverRoot)
	if err != nil {
		t.Fatal(err)
	}
	defer resolver.Close()
	if _, err := resolver.OpenArtifact(ctx, "tenant1", id); err == nil {
		t.Fatal("artifact symlink escape succeeded")
	}
}

func TestDetachRefusesRetargetedMountPathAndRetainsProof(t *testing.T) {
	f := newFixture(t, Options{})
	ctx := context.Background()
	id := artifactID('4')
	createVolume(t, f, "tenant1", "vol1", id)
	if err := f.backend.SetWorkspaceGeneration(ctx, "tenant1", "ws1", 1); err != nil {
		t.Fatal(err)
	}
	attach := AttachRequest{OperationID: "attach", Tenant: "tenant1", ID: "vol1", Workspace: "ws1", Generation: 1, Path: "/data", WorkspaceRoot: f.root, ReadOnly: true, Version: 1, Artifact: id}
	if _, err := f.backend.Attach(ctx, attach); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(filepath.Join(f.root, "data"), filepath.Join(f.root, "original-mount")); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(filepath.Join(f.root, "data"), 0o700); err != nil {
		t.Fatal(err)
	}
	err := f.backend.Detach(ctx, DetachRequest{OperationID: "detach", Tenant: "tenant1", Workspace: "ws1", Generation: 1, Path: "/data"})
	if !errors.Is(err, ErrUnsafePath) {
		t.Fatalf("retargeted detach = %v, want unsafe path", err)
	}
	if _, inspectErr := f.backend.Inspect(ctx, "tenant1", "vol1"); !errors.Is(inspectErr, ErrUnsafePath) {
		t.Fatalf("retargeted mount inspect = %v, want unsafe path", inspectErr)
	}
	f.backend.mu.Lock()
	retained := len(f.backend.state.Attachments)
	f.backend.mu.Unlock()
	if retained != 1 {
		t.Fatalf("retargeted mount lost durable attachment proof: %d attachments", retained)
	}
}

func TestVersionAndCatalogQuotasRetainPinnedArtifacts(t *testing.T) {
	f := newFixture(t, Options{MaxVolumesPerTenant: 1, MaxMountsPerTenant: 1, MaxVersionsPerVolume: 2})
	ctx := context.Background()
	a1, a2, a3 := artifactID('2'), artifactID('3'), artifactID('4')
	createVolume(t, f, "tenant1", "vol1", a1)
	if err := f.backend.SetWorkspaceGeneration(ctx, "tenant1", "ws1", 1); err != nil {
		t.Fatal(err)
	}
	if _, err := f.backend.Attach(ctx, AttachRequest{OperationID: "attach-1", Tenant: "tenant1", ID: "vol1", Workspace: "ws1", Generation: 1, Path: "/data", WorkspaceRoot: f.root, ReadOnly: true}); err != nil {
		t.Fatal(err)
	}
	if _, err := f.backend.Create(ctx, CreateRequest{OperationID: "create-vol2", Tenant: "tenant1", ID: "vol2", Artifact: a2}); !errors.Is(err, ErrQuota) {
		t.Fatalf("pinned volume quota = %v", err)
	}
	f.source(t, "tenant1", a2)
	if _, err := f.backend.Publish(ctx, PublishRequest{OperationID: "publish-2", Tenant: "tenant1", ID: "vol1", Artifact: a2, Workspace: "ws1", Generation: 1, ExpectedVersion: 1}); err != nil {
		t.Fatal(err)
	}
	f.source(t, "tenant1", a3)
	if _, err := f.backend.Publish(ctx, PublishRequest{OperationID: "publish-3", Tenant: "tenant1", ID: "vol1", Artifact: a3, Workspace: "ws1", Generation: 1, ExpectedVersion: 2}); !errors.Is(err, ErrQuota) {
		t.Fatalf("pinned version quota = %v", err)
	}
	if err := f.backend.Detach(ctx, DetachRequest{OperationID: "detach-1", Tenant: "tenant1", Workspace: "ws1", Generation: 1, Path: "/data"}); err != nil {
		t.Fatal(err)
	}
	if _, err := f.backend.Publish(ctx, PublishRequest{OperationID: "publish-3", Tenant: "tenant1", ID: "vol1", Artifact: a3, Workspace: "ws1", Generation: 1, ExpectedVersion: 2}); err != nil {
		t.Fatal(err)
	}
	detail, err := f.backend.Inspect(ctx, "tenant1", "vol1")
	if err != nil {
		t.Fatal(err)
	}
	var versions []uint64
	for _, version := range detail.Versions {
		versions = append(versions, version.Number)
	}
	if got := versions; len(got) != 2 || got[0] != 2 || got[1] != 3 {
		t.Fatalf("retained versions = %v, want [2 3]", got)
	}
	if f.backend.Stats().QuotaRejections < 2 {
		t.Fatalf("quota counter = %+v", f.backend.Stats())
	}
}

func TestOperationRetentionAndIdempotencyAtCapacity(t *testing.T) {
	now := time.Unix(1000, 0)
	f := newFixture(t, Options{MaxOperations: 1, OperationRetention: time.Hour, Clock: func() time.Time { return now }})
	ctx := context.Background()
	a1, a2 := artifactID('5'), artifactID('6')
	first := createVolume(t, f, "tenant1", "vol1", a1)
	if replay, err := f.backend.Create(ctx, CreateRequest{OperationID: "create-vol1", Tenant: "tenant1", ID: "vol1", Artifact: a1}); err != nil || replay != first {
		t.Fatalf("replay at capacity = %+v, %v", replay, err)
	}
	if _, err := f.backend.Create(ctx, CreateRequest{OperationID: "create-vol2", Tenant: "tenant1", ID: "vol2", Artifact: a2}); !errors.Is(err, ErrQuota) {
		t.Fatalf("operation capacity = %v", err)
	}
	now = now.Add(2 * time.Hour)
	if _, err := f.backend.Create(ctx, CreateRequest{OperationID: "create-vol2", Tenant: "tenant1", ID: "vol2", Artifact: a2}); err != nil {
		t.Fatalf("create after retention = %v", err)
	}
}

func TestRestartDefersRecoveryUntilControlGeneration(t *testing.T) {
	f := newFixture(t, Options{})
	ctx := context.Background()
	id := artifactID('7')
	createVolume(t, f, "tenant1", "vol1", id)
	if err := f.backend.SetWorkspaceGeneration(ctx, "tenant1", "ws1", 1); err != nil {
		t.Fatal(err)
	}
	req := AttachRequest{OperationID: "attach-1", Tenant: "tenant1", ID: "vol1", Workspace: "ws1", Generation: 1, Path: "/data", WorkspaceRoot: f.root, ReadOnly: true}
	_, err := f.backend.Attach(ctx, req)
	if err != nil {
		t.Fatal(err)
	}
	f.mounter.forgetAll()
	reopened, err := OpenLocalBackend(ctx, f.state, f.resolver, f.mounter, Options{})
	if err != nil {
		t.Fatal(err)
	}
	if f.mounter.mountCalls != 1 {
		t.Fatalf("restart acted before live authority: calls=%d", f.mounter.mountCalls)
	}
	if _, err := reopened.Attach(ctx, req); !errors.Is(err, ErrStaleGeneration) {
		t.Fatalf("attach before live authority = %v", err)
	}
	if err := reopened.SetWorkspaceGeneration(ctx, "tenant1", "ws1", 1); err != nil {
		t.Fatal(err)
	}
	if replay, err := reopened.Attach(ctx, req); err != nil || replay.State != stateAttached {
		t.Fatalf("replay after control authority = %+v, %v", replay, err)
	}
	if f.mounter.mountCalls != 2 {
		t.Fatalf("authorized recovery did not rebuild missing mount: calls=%d", f.mounter.mountCalls)
	}

	key := attachmentKey("tenant1", "ws1", "/data")
	reopened.mu.Lock()
	mount := reopened.state.Attachments[key]
	mount.State = stateDetaching
	detachFP := fingerprint("detach", "tenant1", "ws1", uint64(1), "/data")
	if err := reopened.mutateLocked(func() {
		reopened.state.Attachments[key] = mount
		reopened.state.Operations[operationKey("tenant1", "detach-crash")] = operation{Kind: "detach", Fingerprint: detachFP, ResourceKey: key}
	}); err != nil {
		reopened.mu.Unlock()
		t.Fatal(err)
	}
	reopened.mu.Unlock()
	afterCrash, err := OpenLocalBackend(ctx, f.state, f.resolver, f.mounter, Options{})
	if err != nil {
		t.Fatal(err)
	}
	detail, err := afterCrash.Inspect(ctx, "tenant1", "vol1")
	if err != nil || len(detail.Attachments) != 1 || detail.Attachments[0].State != stateDetaching {
		t.Fatalf("restart changed pending detach without authority: %+v, %v", detail.Attachments, err)
	}
	if err := afterCrash.SetWorkspaceGeneration(ctx, "tenant1", "ws1", 1); err != nil {
		t.Fatal(err)
	}
	detail, err = afterCrash.Inspect(ctx, "tenant1", "vol1")
	if err != nil || len(detail.Attachments) != 0 {
		t.Fatalf("authorized pending detach survived: %+v, %v", detail.Attachments, err)
	}
	if err := afterCrash.Detach(ctx, DetachRequest{OperationID: "detach-crash", Tenant: "tenant1", Workspace: "ws1", Generation: 1, Path: "/data"}); err != nil {
		t.Fatalf("recovered detach replay = %v", err)
	}
}

func TestRestartDrainsAttachmentOlderThanDurableFence(t *testing.T) {
	f := newFixture(t, Options{})
	ctx := context.Background()
	id := artifactID('4')
	createVolume(t, f, "tenant1", "vol1", id)
	if err := f.backend.SetWorkspaceGeneration(ctx, "tenant1", "ws1", 1); err != nil {
		t.Fatal(err)
	}
	if _, err := f.backend.Attach(ctx, AttachRequest{OperationID: "attach-1", Tenant: "tenant1", ID: "vol1", Workspace: "ws1", Generation: 1, Path: "/data", WorkspaceRoot: f.root, ReadOnly: true}); err != nil {
		t.Fatal(err)
	}
	f.backend.mu.Lock()
	if err := f.backend.mutateLocked(func() {
		f.backend.state.Fences[workspaceKey("tenant1", "ws1")] = 2
	}); err != nil {
		f.backend.mu.Unlock()
		t.Fatal(err)
	}
	f.backend.mu.Unlock()
	reopened, err := OpenLocalBackend(ctx, f.state, f.resolver, f.mounter, Options{})
	if err != nil {
		t.Fatal(err)
	}
	detail, err := reopened.Inspect(ctx, "tenant1", "vol1")
	if err != nil || len(detail.Attachments) != 1 {
		t.Fatalf("restart altered older-generation record before authority: %+v, %v", detail.Attachments, err)
	}
	if err := reopened.SetWorkspaceGeneration(ctx, "tenant1", "ws1", 2); err != nil {
		t.Fatal(err)
	}
	detail, err = reopened.Inspect(ctx, "tenant1", "vol1")
	if err != nil || len(detail.Attachments) != 0 {
		t.Fatalf("authorized generation retained stale mount: %+v, %v", detail.Attachments, err)
	}
	if f.mounter.unmounts != 1 || reopened.Stats().ReconciledDetach != 1 {
		t.Fatalf("restart drain evidence: unmounts=%d stats=%+v", f.mounter.unmounts, reopened.Stats())
	}
}

func TestRestartDoesNotAdoptUnattemptedPendingAttach(t *testing.T) {
	f := newFixture(t, Options{})
	ctx := context.Background()
	id := artifactID('9')
	createVolume(t, f, "tenant1", "vol1", id)
	if err := f.backend.SetWorkspaceGeneration(ctx, "tenant1", "ws1", 1); err != nil {
		t.Fatal(err)
	}
	req := AttachRequest{OperationID: "attach-crash", Tenant: "tenant1", ID: "vol1", Workspace: "ws1", Generation: 1, Path: "/data", WorkspaceRoot: f.root, ReadOnly: true}
	key := attachmentKey("tenant1", "ws1", "/data")
	fp := fingerprint("attach", "tenant1", "vol1", "ws1", uint64(1), "/data", f.root, true, uint64(0), "")
	mount := Attachment{Tenant: "tenant1", Workspace: "ws1", Generation: 1, VolumeID: "vol1", VolumeVersion: 1, Artifact: id, Path: "/data", WorkspaceRoot: f.root, ReadOnly: true, State: stateAttaching}
	f.backend.mu.Lock()
	if err := f.backend.mutateLocked(func() {
		f.backend.state.Attachments[key] = mount
		f.backend.state.Operations[operationKey("tenant1", "attach-crash")] = operation{Kind: "attach", Fingerprint: fp, ResourceKey: key}
	}); err != nil {
		f.backend.mu.Unlock()
		t.Fatal(err)
	}
	f.backend.mu.Unlock()
	source, err := f.resolver.OpenArtifact(ctx, "tenant1", id)
	if err != nil {
		t.Fatal(err)
	}
	target, err := openTarget(mount, true)
	if err != nil {
		t.Fatal(err)
	}
	if err := f.mounter.MountReadOnly(ctx, source, target); err != nil {
		t.Fatal(err)
	}
	source.Close()
	target.Close()
	reopened, err := OpenLocalBackend(ctx, f.state, f.resolver, f.mounter, Options{})
	if err != nil {
		t.Fatal(err)
	}
	if f.mounter.mountCalls != 1 || f.mounter.unmounts != 0 {
		t.Fatalf("restart touched pending mount before authority: calls=%d unmounts=%d", f.mounter.mountCalls, f.mounter.unmounts)
	}
	detail, err := reopened.Inspect(ctx, "tenant1", "vol1")
	if err != nil || len(detail.Attachments) != 1 || !detail.Attachments[0].MountPresent || !detail.Attachments[0].MountVerified {
		t.Fatalf("pending live evidence = %+v, %v", detail.Attachments, err)
	}
	if _, err := reopened.Attach(ctx, req); !errors.Is(err, ErrStaleGeneration) {
		t.Fatalf("pending attach before authority = %v", err)
	}
	if err := reopened.SetWorkspaceGeneration(ctx, "tenant1", "ws1", 1); err != nil {
		t.Fatal(err)
	}
	replay, err := reopened.Attach(ctx, req)
	if err != nil || replay.State != stateAttached {
		t.Fatalf("pending attach recovery = %+v, %v", replay, err)
	}
	if f.mounter.mountCalls != 2 || f.mounter.unmounts != 1 {
		t.Fatalf("unattempted mount was adopted instead of replaced: calls=%d unmounts=%d", f.mounter.mountCalls, f.mounter.unmounts)
	}
}

func TestMountEngineMustProveReadOnly(t *testing.T) {
	f := newFixture(t, Options{})
	f.mounter.readOnly = false
	ctx := context.Background()
	id := artifactID('a')
	createVolume(t, f, "tenant1", "vol1", id)
	if err := f.backend.SetWorkspaceGeneration(ctx, "tenant1", "ws1", 1); err != nil {
		t.Fatal(err)
	}
	_, err := f.backend.Attach(ctx, AttachRequest{OperationID: "attach-1", Tenant: "tenant1", ID: "vol1", Workspace: "ws1", Generation: 1, Path: "/data", WorkspaceRoot: f.root, ReadOnly: true})
	if err == nil {
		t.Fatal("writable mount reported success")
	}
	if f.mounter.unmounts != 1 {
		t.Fatalf("unsafe mount was not rolled back: %+v", f.mounter)
	}
}

func TestInspectRevalidatesLiveMountState(t *testing.T) {
	f := newFixture(t, Options{})
	ctx := context.Background()
	id := artifactID('b')
	createVolume(t, f, "tenant1", "vol1", id)
	if err := f.backend.SetWorkspaceGeneration(ctx, "tenant1", "ws1", 1); err != nil {
		t.Fatal(err)
	}
	if _, err := f.backend.Attach(ctx, AttachRequest{OperationID: "attach-1", Tenant: "tenant1", ID: "vol1", Workspace: "ws1", Generation: 1, Path: "/data", WorkspaceRoot: f.root, ReadOnly: true}); err != nil {
		t.Fatal(err)
	}
	detail, err := f.backend.Inspect(ctx, "tenant1", "vol1")
	if err != nil || len(detail.Attachments) != 1 || !detail.Attachments[0].MountPresent || !detail.Attachments[0].MountVerified || !detail.Attachments[0].ReadOnly {
		t.Fatalf("verified mount = %+v, %v", detail.Attachments, err)
	}
	f.mounter.forgetAll()
	detail, err = f.backend.Inspect(ctx, "tenant1", "vol1")
	if err != nil || len(detail.Attachments) != 1 || detail.Attachments[0].MountPresent || detail.Attachments[0].MountVerified || detail.Attachments[0].ReadOnly {
		t.Fatalf("missing live mount was reported verified: %+v, %v", detail.Attachments, err)
	}
	target, err := openTarget(detail.Attachments[0], false)
	if err != nil {
		t.Fatal(err)
	}
	f.mounter.mu.Lock()
	f.mounter.mounted[target.Name()] = fakeMount{readOnly: false}
	f.mounter.mu.Unlock()
	target.Close()
	detail, err = f.backend.Inspect(ctx, "tenant1", "vol1")
	if err != nil || len(detail.Attachments) != 1 || !detail.Attachments[0].MountPresent || detail.Attachments[0].MountVerified || detail.Attachments[0].ReadOnly {
		t.Fatalf("writable live mount was reported verified: %+v, %v", detail.Attachments, err)
	}
}

func TestGlobalCatalogAndFenceLimitsFailClosed(t *testing.T) {
	f := newFixture(t, Options{MaxVolumes: 1, MaxMounts: 1, MaxWorkspaceFences: 1})
	ctx := context.Background()
	a1, a2 := artifactID('c'), artifactID('d')
	createVolume(t, f, "tenant1", "vol1", a1)
	if err := f.backend.SetWorkspaceGeneration(ctx, "tenant1", "ws1", 1); err != nil {
		t.Fatal(err)
	}
	if err := f.backend.SetWorkspaceGeneration(ctx, "tenant1", "ws1", 2); err != nil {
		t.Fatalf("existing fence could not advance at capacity: %v", err)
	}
	if _, err := f.backend.Attach(ctx, AttachRequest{OperationID: "attach-1", Tenant: "tenant1", ID: "vol1", Workspace: "ws1", Generation: 2, Path: "/one", WorkspaceRoot: f.root, ReadOnly: true}); err != nil {
		t.Fatal(err)
	}
	if _, err := f.backend.Create(ctx, CreateRequest{OperationID: "create-vol2", Tenant: "tenant2", ID: "vol2", Artifact: a2}); !errors.Is(err, ErrQuota) {
		t.Fatalf("global pinned volume limit = %v", err)
	}
	if err := f.backend.SetWorkspaceGeneration(ctx, "tenant2", "ws2", 1); !errors.Is(err, ErrQuota) {
		t.Fatalf("pinned workspace fence limit = %v", err)
	}
	if _, err := f.backend.Attach(ctx, AttachRequest{OperationID: "attach-2", Tenant: "tenant1", ID: "vol1", Workspace: "ws1", Generation: 2, Path: "/two", WorkspaceRoot: f.root, ReadOnly: true}); !errors.Is(err, ErrQuota) {
		t.Fatalf("global mount limit = %v", err)
	}
	stats := f.backend.Stats()
	if stats.WorkspaceFences != 1 || stats.QuotaRejections < 3 {
		t.Fatalf("bounded-state diagnostics = %+v", stats)
	}
}

func TestCatalogChurnPrunesOldestUnattachedWithinPressureScope(t *testing.T) {
	now := time.Unix(10_000, 0)
	f := newFixture(t, Options{MaxVolumesPerTenant: 1, MaxVolumes: 2, Clock: func() time.Time { return now }})
	ctx := context.Background()
	other, old, current := artifactID('7'), artifactID('8'), artifactID('9')
	createVolume(t, f, "tenant2", "older-global", other)
	now = now.Add(time.Second)
	createVolume(t, f, "tenant1", "old-tenant", old)
	now = now.Add(time.Second)
	if _, err := f.backend.Create(ctx, CreateRequest{OperationID: "create-current", Tenant: "tenant1", ID: "current", Artifact: current}); err != nil {
		t.Fatalf("catalog churn = %v", err)
	}
	if _, err := f.backend.Inspect(ctx, "tenant1", "old-tenant"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("per-tenant pressure retained old tenant record: %v", err)
	}
	if _, err := f.backend.Inspect(ctx, "tenant2", "older-global"); err != nil {
		t.Fatalf("per-tenant pressure evicted another tenant: %v", err)
	}
	if f.backend.Stats().PrunedVolumes != 1 || f.backend.Stats().QuotaRejections != 0 {
		t.Fatalf("catalog churn stats = %+v", f.backend.Stats())
	}
}

func TestFenceChurnPrunesOldestUnattachedWorkspace(t *testing.T) {
	now := time.Unix(20_000, 0)
	f := newFixture(t, Options{MaxWorkspaceFences: 2, Clock: func() time.Time { return now }})
	ctx := context.Background()
	if err := f.backend.SetWorkspaceGeneration(ctx, "tenant1", "ws-z", 1); err != nil {
		t.Fatal(err)
	}
	now = now.Add(time.Second)
	if err := f.backend.SetWorkspaceGeneration(ctx, "tenant1", "ws-a", 1); err != nil {
		t.Fatal(err)
	}
	now = now.Add(time.Second)
	if err := f.backend.SetWorkspaceGeneration(ctx, "tenant1", "ws-new", 1); err != nil {
		t.Fatalf("fence churn = %v", err)
	}
	f.backend.mu.Lock()
	_, oldestPresent := f.backend.state.Fences[workspaceKey("tenant1", "ws-z")]
	_, newerPresent := f.backend.state.Fences[workspaceKey("tenant1", "ws-a")]
	_, currentPresent := f.backend.state.Fences[workspaceKey("tenant1", "ws-new")]
	f.backend.mu.Unlock()
	if oldestPresent || !newerPresent || !currentPresent {
		t.Fatalf("fence retention oldest=%v newer=%v current=%v", oldestPresent, newerPresent, currentPresent)
	}
	if f.backend.Stats().PrunedFences != 1 || f.backend.Stats().QuotaRejections != 0 {
		t.Fatalf("fence churn stats = %+v", f.backend.Stats())
	}
}

func TestLegacyFenceStateMigratesWithDeterministicAge(t *testing.T) {
	f := newFixture(t, Options{})
	ctx := context.Background()
	if err := f.backend.SetWorkspaceGeneration(ctx, "tenant1", "ws1", 3); err != nil {
		t.Fatal(err)
	}
	f.backend.mu.Lock()
	legacy := f.backend.state
	legacy.Format = 1
	legacy.FenceUpdated = nil
	if err := saveState(f.state, legacy); err != nil {
		f.backend.mu.Unlock()
		t.Fatal(err)
	}
	f.backend.mu.Unlock()
	reopened, err := OpenLocalBackend(ctx, f.state, f.resolver, f.mounter, Options{})
	if err != nil {
		t.Fatal(err)
	}
	reopened.mu.Lock()
	format := reopened.state.Format
	updated := reopened.state.FenceUpdated[workspaceKey("tenant1", "ws1")]
	reopened.mu.Unlock()
	if format != stateVersion || updated.IsZero() {
		t.Fatalf("legacy fence migration format=%d updated=%v", format, updated)
	}
}

func TestEnsureVersionPrunesUnpinnedLocalHistory(t *testing.T) {
	f := newFixture(t, Options{MaxVersionsPerVolume: 2})
	ctx := context.Background()
	for number, id := range []string{artifactID('e'), artifactID('f'), artifactID('0')} {
		if err := f.backend.EnsureVersion(ctx, "tenant1", "vol1", uint64(number+1), id); err != nil {
			t.Fatal(err)
		}
	}
	detail, err := f.backend.Inspect(ctx, "tenant1", "vol1")
	if err != nil {
		t.Fatal(err)
	}
	if len(detail.Versions) != 2 || detail.Versions[0].Number != 2 || detail.Versions[1].Number != 3 {
		t.Fatalf("local version history = %+v, want versions 2 and 3", detail.Versions)
	}
}

func TestArtifactReferencesAreFreshAndDeduplicated(t *testing.T) {
	f := newFixture(t, Options{})
	ctx := context.Background()
	a, b := artifactID('1'), artifactID('2')
	for number, id := range []string{b, a, b} {
		if err := f.backend.EnsureVersion(ctx, "tenant1", "vol1", uint64(number+1), id); err != nil {
			t.Fatal(err)
		}
	}
	var reporter ArtifactReferenceReporter = f.backend
	refs := reporter.ArtifactReferences()
	if len(refs) != 0 {
		t.Fatalf("detached catalog history pinned artifacts: %v", refs)
	}
	f.source(t, "tenant1", a)
	f.source(t, "tenant1", b)
	if err := f.backend.SetWorkspaceGeneration(ctx, "tenant1", "ws1", 1); err != nil {
		t.Fatal(err)
	}
	first := AttachRequest{OperationID: "attach-a", Tenant: "tenant1", ID: "vol1", Workspace: "ws1", Generation: 1, Path: "/a", WorkspaceRoot: f.root, ReadOnly: true, Version: 2, Artifact: a}
	second := AttachRequest{OperationID: "attach-b", Tenant: "tenant1", ID: "vol1", Workspace: "ws1", Generation: 1, Path: "/b", WorkspaceRoot: f.root, ReadOnly: true, Version: 3, Artifact: b}
	if _, err := f.backend.Attach(ctx, first); err != nil {
		t.Fatal(err)
	}
	if _, err := f.backend.Attach(ctx, second); err != nil {
		t.Fatal(err)
	}
	refs = reporter.ArtifactReferences()
	if len(refs) != 2 || refs[0] != a || refs[1] != b {
		t.Fatalf("attachment recovery references = %v", refs)
	}
	refs[0] = artifactID('3')
	again := reporter.ArtifactReferences()
	if len(again) != 2 || again[0] != a || again[1] != b {
		t.Fatalf("caller mutated backend references: %v", again)
	}
	if err := f.backend.Detach(ctx, DetachRequest{OperationID: "detach-a", Tenant: "tenant1", Workspace: "ws1", Generation: 1, Path: "/a"}); err != nil {
		t.Fatal(err)
	}
	if err := f.backend.Detach(ctx, DetachRequest{OperationID: "detach-b", Tenant: "tenant1", Workspace: "ws1", Generation: 1, Path: "/b"}); err != nil {
		t.Fatal(err)
	}
	if refs := reporter.ArtifactReferences(); len(refs) != 0 {
		t.Fatalf("detached versions still pin artifacts: %v", refs)
	}
}

func TestReleaseAbortScopesPreserveDelayedOperationReplay(t *testing.T) {
	f := newFixture(t, Options{})
	ctx := context.Background()
	artifact := artifactID('7')
	createVolume(t, f, "tenant1", "vol1", artifact)
	if err := f.backend.SetWorkspaceGeneration(ctx, "tenant1", "ws1", 1); err != nil {
		t.Fatal(err)
	}
	initialAttach := AttachRequest{
		OperationID: "attach-materialize", Tenant: "tenant1", ID: "vol1", Workspace: "ws1",
		Generation: 1, Path: "/data", WorkspaceRoot: f.root, ReadOnly: true, Version: 1, Artifact: artifact,
	}
	firstDetach := DetachRequest{OperationID: "detach-release-1", Tenant: "tenant1", Workspace: "ws1", Generation: 1, Path: "/data"}
	if _, err := f.backend.Attach(ctx, initialAttach); err != nil {
		t.Fatal(err)
	}
	if err := f.backend.Detach(ctx, firstDetach); err != nil {
		t.Fatal(err)
	}
	abortAttach := initialAttach
	abortAttach.OperationID = "attach-release-1-abort"
	if _, err := f.backend.Attach(ctx, abortAttach); err != nil {
		t.Fatalf("release abort could not attach the exact pinned version: %v", err)
	}
	if _, err := f.backend.Attach(ctx, initialAttach); err != nil {
		t.Fatalf("delayed initial attach replay lost its committed result: %v", err)
	}
	if err := f.backend.Detach(ctx, firstDetach); !errors.Is(err, ErrConflict) {
		t.Fatalf("delayed old detach touched the restored mount: %v", err)
	}
	secondDetach := firstDetach
	secondDetach.OperationID = "detach-release-2"
	if err := f.backend.Detach(ctx, secondDetach); err != nil {
		t.Fatalf("second release could not use its distinct durable scope: %v", err)
	}
	if f.mounter.mountCalls != 2 || f.mounter.unmounts != 2 {
		t.Fatalf("mount lifecycle calls mount=%d unmount=%d, want 2/2", f.mounter.mountCalls, f.mounter.unmounts)
	}
	detail, err := f.backend.Inspect(ctx, "tenant1", "vol1")
	if err != nil {
		t.Fatal(err)
	}
	if len(detail.Attachments) != 0 {
		t.Fatalf("second release retained attachment: %+v", detail.Attachments)
	}
}

func TestListAndInspectAreStableCopies(t *testing.T) {
	f := newFixture(t, Options{})
	a := artifactID('8')
	createVolume(t, f, "tenant1", "z", a)
	createVolume(t, f, "tenant1", "a", a)
	list, err := f.backend.List(context.Background(), "tenant1")
	if err != nil {
		t.Fatal(err)
	}
	ids := []string{list[0].ID, list[1].ID}
	if !sort.StringsAreSorted(ids) {
		t.Fatalf("list not sorted: %v", ids)
	}
	list[0].Artifact = "changed"
	detail, err := f.backend.Inspect(context.Background(), "tenant1", "a")
	if err != nil || detail.Volume.Artifact != a {
		t.Fatalf("live state escaped: %+v, %v", detail, err)
	}
}
