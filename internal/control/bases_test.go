package control

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"remount.dev/remount/internal/artifact"
	"remount.dev/remount/internal/proto"
)

func putTestSnapshot(t *testing.T, store artifact.BlobStore, content string) string {
	t.Helper()
	root := t.TempDir()
	if err := os.WriteFile(filepath.Join(root, "body"), []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
	id, _, err := artifact.SnapshotToStore(store, root, nil)
	if err != nil {
		t.Fatal(err)
	}
	return id
}

// TestBasesSurviveRestartAndReplayOutlivesRemoval: a base is durable state,
// so it must be re-pinned after a restart; and a `ws create --base` replay
// keyed on the request as sent must return its prior workspace even after the
// base it named has been removed.
func TestBasesSurviveRestartAndReplayOutlivesRemoval(t *testing.T) {
	store, err := artifact.NewStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	pinned := putTestSnapshot(t, store, "golden image")
	path := filepath.Join(t.TempDir(), "bases.db")
	withStore := func(opts *Options) { opts.Artifacts = store }
	f1 := newControlFixture(t, path, withStore)
	subject := Subject{ID: "alice", Tenant: "tenant-a"}
	ctx := context.Background()
	if _, err := f1.c.baseCreate(ctx, subject, &proto.BaseCreateReq{Name: "golden", Artifact: pinned, IdempotencyKey: "pin"}); err != nil {
		t.Fatal(err)
	}
	ws, err := f1.c.wsCreate(ctx, subject, &proto.WSCreateReq{Spec: proto.WorkspaceSpec{Base: "golden"}, IdempotencyKey: "from-base"})
	if err != nil {
		t.Fatal(err)
	}
	if ws.Spec.RestoreFrom != pinned {
		t.Fatalf("RestoreFrom = %q, want %q", ws.Spec.RestoreFrom, pinned)
	}
	f1.c.Stop()
	if err := f1.log.Close(); err != nil {
		t.Fatal(err)
	}

	f2 := newControlFixture(t, path, withStore)
	list, err := f2.c.baseList(ctx, subject)
	if err != nil || len(list.Bases) != 1 || list.Bases[0].Artifact != pinned {
		t.Fatalf("after restart: %+v, %v", list, err)
	}
	var refs []string
	if err := f2.c.WithArtifactReferences(func(references []string) error {
		refs = append(refs, references...)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	found := 0
	for _, ref := range refs {
		if ref == pinned {
			found++
		}
	}
	if found < 1 {
		t.Fatalf("pinned base missing from GC references: %v", refs)
	}
	if err := f2.c.baseRemove(ctx, subject, &proto.BaseRemoveReq{Name: "golden", IdempotencyKey: "unpin"}); err != nil {
		t.Fatal(err)
	}
	replay, err := f2.c.wsCreate(ctx, subject, &proto.WSCreateReq{Spec: proto.WorkspaceSpec{Base: "golden"}, IdempotencyKey: "from-base"})
	if err != nil || replay.ID != ws.ID {
		t.Fatalf("replay after base rm = %+v, %v; want workspace %s", replay, err, ws.ID)
	}
	var pe *proto.Error
	if _, err := f2.c.wsCreate(ctx, subject, &proto.WSCreateReq{Spec: proto.WorkspaceSpec{Base: "golden"}, IdempotencyKey: "fresh"}); !errors.As(err, &pe) || pe.Code != proto.CodeNotFound {
		t.Fatalf("fresh create from removed base: %v, want not_found", err)
	}
}

// TestBaseQuotaIsPerTenant: the pinned-base limit counts one tenant's bases
// only, and a rejection leaves no partial state behind.
func TestBaseQuotaIsPerTenant(t *testing.T) {
	store, err := artifact.NewStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	art := putTestSnapshot(t, store, "small")
	f := newControlFixture(t, "", func(opts *Options) { opts.Artifacts = store; opts.MaxBasesPerTenant = 1 })
	ctx := context.Background()
	a := Subject{ID: "alice", Tenant: "tenant-a"}
	b := Subject{ID: "bob", Tenant: "tenant-b"}
	if _, err := f.c.baseCreate(ctx, a, &proto.BaseCreateReq{Name: "one", Artifact: art}); err != nil {
		t.Fatal(err)
	}
	var pe *proto.Error
	if _, err := f.c.baseCreate(ctx, a, &proto.BaseCreateReq{Name: "two", Artifact: art}); !errors.As(err, &pe) || pe.Code != proto.CodeResourceExhausted {
		t.Fatalf("over quota: %v, want resource_exhausted", err)
	}
	if _, err := f.c.baseCreate(ctx, b, &proto.BaseCreateReq{Name: "one", Artifact: art}); err != nil {
		t.Fatalf("other tenant is not charged for tenant-a's bases: %v", err)
	}
	list, err := f.c.baseList(ctx, a)
	if err != nil || len(list.Bases) != 1 || list.Bases[0].Name != "one" || list.Bases[0].Tenant != "tenant-a" {
		t.Fatalf("tenant-a ls = %+v, %v", list, err)
	}
}
