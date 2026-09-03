package control

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"testing"

	"remount.dev/remount/internal/artifact"
	"remount.dev/remount/internal/artifact/chunked"
	"remount.dev/remount/internal/proto"
)

func TestFirecrackerFullSnapshotIsOneOpaqueRootedObject(t *testing.T) {
	store, err := artifact.NewStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	id, _, err := store.Put(bytes.NewReader([]byte("opaque full checkpoint")))
	if err != nil {
		t.Fatal(err)
	}
	resolver := &recordingTenantResolver{stores: map[string]*artifact.Store{"tenant-a": store}}
	f := newControlFixture(t, "", func(options *Options) { options.TenantArtifacts = resolver })
	objects, err := f.c.snapshotArtifactObjects(context.Background(), "tenant-a", id, proto.ArtifactFormatFirecrackerFullV1)
	if err != nil || len(objects) != 1 || objects[0] != id {
		t.Fatalf("opaque closure = %v, %v", objects, err)
	}
}

func TestChunkedAuthorityVerifiesAndRootsTenantLocalClosure(t *testing.T) {
	storeA, err := artifact.NewStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	storeB, err := artifact.NewStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	root := t.TempDir()
	if err := os.WriteFile(filepath.Join(root, "body"), make([]byte, 300<<10), 0o600); err != nil {
		t.Fatal(err)
	}
	snapshot, err := chunked.Snapshot(context.Background(), storeA, root, chunked.SnapshotOptions{})
	if err != nil {
		t.Fatal(err)
	}
	resolver := &recordingTenantResolver{stores: map[string]*artifact.Store{"tenant-a": storeA, "tenant-b": storeB}}
	f := newControlFixture(t, "", func(options *Options) { options.TenantArtifacts = resolver })
	objects, err := f.c.snapshotArtifactObjects(context.Background(), "tenant-a", snapshot.ManifestID, proto.ArtifactFormatChunkedV1)
	if err != nil || len(objects) < 2 {
		t.Fatalf("verified closure = %v, %v", objects, err)
	}
	if _, err := f.c.snapshotArtifactObjects(context.Background(), "tenant-b", snapshot.ManifestID, proto.ArtifactFormatChunkedV1); err == nil {
		t.Fatal("cross-tenant manifest lookup exposed tenant-a content")
	}
	f.c.mu.Lock()
	f.c.workspaces["ws-a"] = &proto.Workspace{
		ID: "ws-a", Tenant: "tenant-a", State: proto.WSPaused,
		LastSnapshot: snapshot.ManifestID, LastSnapshotFormat: proto.ArtifactFormatChunkedV1, LastSnapshotObjects: objects,
	}
	f.c.mu.Unlock()
	want := make(map[string]bool, len(objects))
	for _, id := range objects {
		want[id] = true
	}
	if err := f.c.WithTenantArtifactReferences(func(refs []artifact.TenantReference) error {
		for _, ref := range refs {
			if ref.Tenant == "tenant-a" {
				delete(want, ref.ID)
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if len(want) != 0 {
		t.Fatalf("chunk closure missing GC roots: %v", want)
	}

	manifest, err := chunked.LoadManifest(storeA, snapshot.ManifestID, chunked.Limits{})
	if err != nil {
		t.Fatal(err)
	}
	missing := manifest.Entries[len(manifest.Entries)-1].Chunks[0].ID
	if err := storeA.Delete(missing); err != nil {
		t.Fatal(err)
	}
	if _, err := f.c.snapshotArtifactObjects(context.Background(), "tenant-a", snapshot.ManifestID, proto.ArtifactFormatChunkedV1); err == nil {
		t.Fatal("manifest with a missing chunk became checkpoint authority")
	}
}
