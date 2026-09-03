package control

import (
	"context"
	"errors"
	"path/filepath"
	"strings"
	"testing"

	"remount.dev/remount/internal/artifact"
	"remount.dev/remount/internal/proto"
)

func volumeArtifact(t *testing.T, store *artifact.Store, body string) string {
	t.Helper()
	id, _, err := store.Put(strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	return id
}

func TestVolumesPinVersionsAndFenceMutations(t *testing.T) {
	store, err := artifact.NewStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	first := volumeArtifact(t, store, "dataset-v1")
	second := volumeArtifact(t, store, "dataset-v2")
	path := filepath.Join(t.TempDir(), "control.db")
	withStore := func(opts *Options) { opts.Artifacts = store }
	f := newControlFixture(t, path, withStore)
	ctx := context.Background()
	owner := Subject{ID: "alice", Tenant: "tenant-a"}
	reader := Subject{ID: "bob", Tenant: "tenant-a"}

	created, err := f.c.volumeCreate(ctx, owner, &proto.VolumeCreateReq{ID: "dataset", Artifact: first, IdempotencyKey: "create"})
	if err != nil || created.Version != 1 || len(created.Versions) != 1 {
		t.Fatalf("create = %+v, %v", created, err)
	}
	replayed, err := f.c.volumeCreate(ctx, owner, &proto.VolumeCreateReq{ID: "dataset", Artifact: first, IdempotencyKey: "create"})
	if err != nil || replayed.Artifact != first {
		t.Fatalf("create replay = %+v, %v", replayed, err)
	}

	ws, err := f.c.wsCreate(ctx, reader, &proto.WSCreateReq{Spec: proto.WorkspaceSpec{
		Volumes: []proto.VolumeMount{{ID: "dataset", Path: "/datasets/main"}},
	}, IdempotencyKey: "workspace"})
	if err != nil {
		t.Fatal(err)
	}
	if got := ws.Spec.Volumes; len(got) != 1 || got[0].Version != 1 || got[0].Artifact != first {
		t.Fatalf("resolved mounts = %+v", got)
	}

	// Publishing requires write authority over both the volume and source
	// workspace. Give the owner's source an explicit generation fence.
	source, err := f.c.wsCreate(ctx, owner, &proto.WSCreateReq{IdempotencyKey: "source"})
	if err != nil {
		t.Fatal(err)
	}
	f.c.mu.Lock()
	f.c.workspaces[source.ID].Generation = 7
	f.c.workspaces[source.ID].State = proto.WSClaimed
	if err := f.c.persistWS(f.c.workspaces[source.ID]); err != nil {
		f.c.mu.Unlock()
		t.Fatal(err)
	}
	f.c.mu.Unlock()
	published, err := f.c.volumePublish(ctx, owner, &proto.VolumePublishReq{
		ID: "dataset", Workspace: source.ID, Generation: 7, Artifact: second,
		ExpectedVersion: 1, IdempotencyKey: "publish",
	}, "")
	if err != nil || published.Version != 2 || published.Artifact != second {
		t.Fatalf("publish = %+v, %v", published, err)
	}
	if got := f.c.snapshotWS(ws.ID).Spec.Volumes[0]; got.Version != 1 || got.Artifact != first {
		t.Fatalf("existing mount changed under workspace: %+v", got)
	}
	var protocolError *proto.Error
	_, err = f.c.volumePublish(ctx, owner, &proto.VolumePublishReq{
		ID: "dataset", Workspace: source.ID, Generation: 6, Artifact: first,
		ExpectedVersion: 2, IdempotencyKey: "stale-generation",
	}, "")
	if !errors.As(err, &protocolError) || protocolError.Code != proto.CodeConflict {
		t.Fatalf("stale generation = %v, want conflict", err)
	}
	_, err = f.c.volumePublish(ctx, owner, &proto.VolumePublishReq{
		ID: "dataset", Workspace: source.ID, Generation: 7, Artifact: first,
		ExpectedVersion: 1, IdempotencyKey: "stale-version",
	}, "")
	if !errors.As(err, &protocolError) || protocolError.Code != proto.CodeConflict {
		t.Fatalf("stale version = %v, want conflict", err)
	}

	var refs []string
	if err := f.c.WithArtifactReferences(func(ids []string) error { refs = append(refs, ids...); return nil }); err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{first, second} {
		found := false
		for _, got := range refs {
			found = found || got == want
		}
		if !found {
			t.Fatalf("artifact %s not pinned in %v", want, refs)
		}
	}
	if err := f.c.volumeRemove(ctx, owner, &proto.VolumeRemoveReq{ID: "dataset", IdempotencyKey: "remove-busy"}); !errors.As(err, &protocolError) || protocolError.Code != proto.CodeConflict {
		t.Fatalf("remove attached = %v, want conflict", err)
	}
	events, err := f.log.Read(ctx, 0, "", 1000)
	if err != nil {
		t.Fatal(err)
	}
	types := map[string]bool{}
	for _, event := range events {
		types[event.Type] = true
	}
	for _, want := range []string{proto.EvVolumeCreated, proto.EvVolumePublished, proto.EvWSCreated} {
		if !types[want] {
			t.Fatalf("missing transactional event %s in %v", want, types)
		}
	}
}

func TestVolumeAttachDetachAtomicDurableAndTenantScoped(t *testing.T) {
	store, err := artifact.NewStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	art := volumeArtifact(t, store, "shared")
	path := filepath.Join(t.TempDir(), "control.db")
	withStore := func(opts *Options) { opts.Artifacts = store }
	f := newControlFixture(t, path, withStore)
	ctx := context.Background()
	a := Subject{ID: "alice", Tenant: "tenant-a"}
	b := Subject{ID: "mallory", Tenant: "tenant-b"}
	if _, err := f.c.volumeCreate(ctx, a, &proto.VolumeCreateReq{ID: "cache", Artifact: art, IdempotencyKey: "create-cache"}); err != nil {
		t.Fatal(err)
	}
	ws, err := f.c.wsCreate(ctx, a, &proto.WSCreateReq{})
	if err != nil {
		t.Fatal(err)
	}
	attached, err := f.c.volumeAttach(ctx, a, &proto.VolumeAttachReq{ID: "cache", Workspace: ws.ID, Generation: 0, Path: "/cache", IdempotencyKey: "attach"})
	if err != nil || len(attached.Spec.Volumes) != 1 || attached.Spec.Volumes[0].Artifact != art {
		t.Fatalf("attach = %+v, %v", attached, err)
	}
	if _, err := f.c.volumeAttach(ctx, a, &proto.VolumeAttachReq{ID: "cache", Workspace: ws.ID, Generation: 1, Path: "/other", IdempotencyKey: "stale"}); err == nil {
		t.Fatal("stale attach succeeded")
	}
	if list, err := f.c.volumeList(ctx, b); err != nil || len(list.Volumes) != 0 {
		t.Fatalf("cross-tenant list = %+v, %v", list, err)
	}
	if _, err := f.c.volumeGet(ctx, b, "cache"); err == nil {
		t.Fatal("cross-tenant get succeeded")
	}
	detached, err := f.c.volumeDetach(ctx, a, &proto.VolumeDetachReq{Workspace: ws.ID, Generation: 0, Path: "/cache", IdempotencyKey: "detach"})
	if err != nil || len(detached.Spec.Volumes) != 0 {
		t.Fatalf("detach = %+v, %v", detached, err)
	}
	if err := f.c.volumeRemove(ctx, a, &proto.VolumeRemoveReq{ID: "cache", IdempotencyKey: "remove"}); err != nil {
		t.Fatal(err)
	}

	// Stop and reopen to prove the volume/workspace updates are durable, not
	// merely mutations of the in-memory maps.
	f.c.Stop()
	if err := f.log.Close(); err != nil {
		t.Fatal(err)
	}
	f2 := newControlFixture(t, path, withStore)
	if _, err := f2.c.volumeGet(ctx, a, "cache"); err == nil {
		t.Fatal("removed volume returned after restart")
	}
	if got := f2.c.snapshotWS(ws.ID); got == nil || len(got.Spec.Volumes) != 0 {
		t.Fatalf("workspace after restart = %+v", got)
	}
}

func TestWorkspaceVolumeDeclarationValidation(t *testing.T) {
	f := newControlFixture(t, "", nil)
	ctx := context.Background()
	subject := Subject{ID: "alice", Tenant: "tenant-a"}
	for _, spec := range []proto.WorkspaceSpec{
		{Volumes: []proto.VolumeMount{{ID: "bad/id", Path: "/data"}}},
		{Volumes: []proto.VolumeMount{{ID: "missing", Path: "relative"}}},
		{Volumes: []proto.VolumeMount{{ID: "missing", Path: "/data", Version: 1}}},
		{Volumes: []proto.VolumeMount{{ID: "a", Path: "/data"}, {ID: "b", Path: "/data"}}},
	} {
		if _, err := f.c.wsCreate(ctx, subject, &proto.WSCreateReq{Spec: spec}); err == nil {
			t.Fatalf("invalid volume spec accepted: %+v", spec)
		}
	}
}
