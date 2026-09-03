package control

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"strings"
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

// closureFixture pins one chunked snapshot as a live workspace checkpoint so
// doctor's deep pass has an authoritative root to walk.
func closureFixture(t *testing.T) (*controlFixture, *artifact.Store, chunked.SnapshotResult) {
	t.Helper()
	store, err := artifact.NewStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	root := t.TempDir()
	if err := os.WriteFile(filepath.Join(root, "body"), make([]byte, 300<<10), 0o600); err != nil {
		t.Fatal(err)
	}
	snapshot, err := chunked.Snapshot(context.Background(), store, root, chunked.SnapshotOptions{})
	if err != nil {
		t.Fatal(err)
	}
	resolver := &recordingTenantResolver{stores: map[string]*artifact.Store{"tenant-a": store}}
	f := newControlFixture(t, "", func(options *Options) { options.TenantArtifacts = resolver })
	objects, err := f.c.snapshotArtifactObjects(context.Background(), "tenant-a", snapshot.ManifestID, proto.ArtifactFormatChunkedV1)
	if err != nil {
		t.Fatal(err)
	}
	f.c.mu.Lock()
	f.c.workspaces["ws-a"] = &proto.Workspace{
		ID: "ws-a", Tenant: "tenant-a", State: proto.WSPaused,
		LastSnapshot: snapshot.ManifestID, LastSnapshotFormat: proto.ArtifactFormatChunkedV1, LastSnapshotObjects: objects,
	}
	f.c.mu.Unlock()
	return f, store, snapshot
}

func findingsByCheck(findings []proto.Finding, check string) []proto.Finding {
	var out []proto.Finding
	for _, finding := range findings {
		if finding.Check == check {
			out = append(out, finding)
		}
	}
	return out
}

// A deleted chunk leaves every retained blob hashing correctly, so the digest
// pass has nothing to say. Only the closure walk can see it, and it must say
// which property failed rather than reporting generic corruption.
func TestDeepVerifyReportsAnIncompleteClosureDistinctly(t *testing.T) {
	f, store, snapshot := closureFixture(t)
	ctx := context.Background()
	healthy := f.c.verifyArtifactsForTenant(ctx, "tenant-a")
	if len(findingsByCheck(healthy, closureCheckChunks)) != 0 || len(findingsByCheck(healthy, closureCheckManifest)) != 0 {
		t.Fatalf("an intact closure produced damage findings: %+v", healthy)
	}
	if len(findingsByCheck(healthy, "artifact.closure_verified")) != 1 {
		t.Fatalf("no closure summary: %+v", healthy)
	}

	manifest, err := chunked.LoadManifest(store, snapshot.ManifestID, chunked.Limits{})
	if err != nil {
		t.Fatal(err)
	}
	missing := manifest.Entries[len(manifest.Entries)-1].Chunks[0].ID
	if err := store.Delete(missing); err != nil {
		t.Fatal(err)
	}
	damaged := f.c.verifyArtifactsForTenant(ctx, "tenant-a")
	closure := findingsByCheck(damaged, closureCheckChunks)
	if len(closure) != 1 || closure[0].Severity != "error" || closure[0].Subject != "ws-a" {
		t.Fatalf("closure findings=%+v", damaged)
	}
	if !strings.Contains(closure[0].Detail, missing) {
		t.Fatalf("the finding does not name the missing chunk: %+v", closure[0])
	}
	// Every retained blob still matches its content address, so a digest
	// finding here would mean the two checks had been conflated.
	if digests := findingsByCheck(damaged, "artifact.digest"); len(digests) != 0 {
		t.Fatalf("closure damage was reported as digest corruption: %+v", digests)
	}
	if manifests := findingsByCheck(damaged, closureCheckManifest); len(manifests) != 0 {
		t.Fatalf("a missing chunk was reported as a manifest problem: %+v", manifests)
	}
}

// A manifest can hash correctly and still be undecodable, because the content
// address covers the bytes and not their meaning.
func TestDeepVerifyReportsANonCanonicalManifestDistinctly(t *testing.T) {
	f, store, snapshot := closureFixture(t)
	ctx := context.Background()
	if err := store.Delete(snapshot.ManifestID); err != nil {
		t.Fatal(err)
	}
	// Republish under a different id whose bytes are not a canonical manifest,
	// then point the workspace at it: the blob is intact by digest and broken
	// by meaning.
	corrupt, _, err := store.Put(strings.NewReader(`{"schema":"remount.chunked.v1","entries":[{"path":"body"`))
	if err != nil {
		t.Fatal(err)
	}
	f.c.mu.Lock()
	f.c.workspaces["ws-a"].LastSnapshot = corrupt
	f.c.mu.Unlock()

	findings := f.c.verifyArtifactsForTenant(ctx, "tenant-a")
	manifests := findingsByCheck(findings, closureCheckManifest)
	if len(manifests) != 1 || manifests[0].Severity != "error" || manifests[0].Subject != "ws-a" {
		t.Fatalf("manifest findings=%+v", findings)
	}
	if !strings.Contains(manifests[0].Detail, corrupt) {
		t.Fatalf("the finding does not name the manifest: %+v", manifests[0])
	}
	if digests := findingsByCheck(findings, "artifact.digest"); len(digests) != 0 {
		t.Fatalf("a canonical-encoding failure was reported as digest corruption: %+v", digests)
	}
	if closure := findingsByCheck(findings, closureCheckChunks); len(closure) != 0 {
		t.Fatalf("an undecodable manifest was reported as a missing chunk: %+v", closure)
	}
}

// A walk that cannot run is unavailable, never healthy: the summary must not
// be the only thing an operator sees when the store is absent.
func TestClosureVerificationThatCannotRunIsUnavailable(t *testing.T) {
	f := newControlFixture(t, "", nil)
	f.c.mu.Lock()
	f.c.workspaces["ws-a"] = &proto.Workspace{
		ID: "ws-a", Tenant: "tenant-a", State: proto.WSPaused,
		LastSnapshot: "art_sha256:" + strings.Repeat("0", 64), LastSnapshotFormat: proto.ArtifactFormatChunkedV1,
	}
	f.c.mu.Unlock()
	findings := f.c.verifySnapshotClosures(context.Background(), "tenant-a")
	if len(findings) != 1 || findings[0].Check != "artifact.closure_unavailable" || findings[0].Severity != "warn" {
		t.Fatalf("findings=%+v", findings)
	}
}
