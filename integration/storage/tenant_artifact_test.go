package storage_test

import (
	"context"
	"crypto/sha256"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"remount.dev/remount/internal/artifact"
	"remount.dev/remount/internal/artifact/encrypted"
)

type localRuntime struct {
	resolver *encrypted.Resolver
	store    *encrypted.Store
	keys     *encrypted.DirectoryKeyProvider
}

func TestTenantArtifactGCQualifiesReferencesAndHonorsObservedGrace(t *testing.T) {
	runtime := openLocalRuntime(t, t.TempDir(), 10)
	a, _ := runtime.resolver.ResolveTenant("tenant-a")
	b, _ := runtime.resolver.ResolveTenant("tenant-b")
	id, _, err := a.Put(strings.NewReader("same plaintext"))
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := b.Put(strings.NewReader("same plaintext")); err != nil {
		t.Fatal(err)
	}
	now := time.Unix(1_800_000_000, 0)
	references := []artifact.TenantReference{{Tenant: "tenant-a", ID: id}}
	first, err := runtime.resolver.CollectTenants(context.Background(), references, now, now.Add(-time.Hour))
	if err != nil || first.Referenced != 1 || first.GraceRetained != 1 || first.Removed != 0 {
		t.Fatalf("first tenant GC = %+v, %v", first, err)
	}
	secondNow := now.Add(2 * time.Hour)
	second, err := runtime.resolver.CollectTenants(context.Background(), references, secondNow, secondNow.Add(-time.Hour))
	if err != nil || second.Referenced != 1 || second.Removed != 1 {
		t.Fatalf("second tenant GC = %+v, %v", second, err)
	}
	if _, err := a.Head(id); err != nil {
		t.Fatalf("qualified tenant-a reference was removed: %v", err)
	}
	if _, err := b.Head(id); err == nil {
		t.Fatal("unreferenced tenant-b copy survived its observed grace window")
	}
}

func openLocalRuntime(t *testing.T, root string, batch int) localRuntime {
	t.Helper()
	masterBytes := sha256.Sum256([]byte("persistent integration master key"))
	master, err := encrypted.NewAESMasterKey("master-v1", masterBytes[:])
	if err != nil {
		t.Fatal(err)
	}
	keys, err := encrypted.NewDirectoryKeyProvider(filepath.Join(root, "keys"), master, encrypted.DirectoryKeyOptions{MaxTenants: 8, MaxVersionsPerTenant: 4})
	if err != nil {
		t.Fatal(err)
	}
	objects, err := encrypted.NewFileStore(filepath.Join(root, "objects"), encrypted.FileStoreOptions{MaxBytes: 64 << 20, MaxObjects: 100})
	if err != nil {
		t.Fatal(err)
	}
	store, err := encrypted.NewStore(objects, keys, filepath.Join(root, "stage"), encrypted.Options{MaxPlaintextBytes: 8 << 20, MaxStagingBytes: 16 << 20, MaxConcurrentWrites: 2})
	if err != nil {
		t.Fatal(err)
	}
	resolver, err := encrypted.NewResolver(store, keys, encrypted.ResolverOptions{RewrapBatch: batch})
	if err != nil {
		t.Fatal(err)
	}
	return localRuntime{resolver: resolver, store: store, keys: keys}
}

func TestEncryptedTenantArtifactsRestartIsolationAndRotation(t *testing.T) {
	root := t.TempDir()
	runtime := openLocalRuntime(t, root, 1)
	a, err := runtime.resolver.ResolveTenant("tenant-a")
	if err != nil {
		t.Fatal(err)
	}
	b, err := runtime.resolver.ResolveTenant("tenant-b")
	if err != nil {
		t.Fatal(err)
	}
	sharedID, _, err := a.Put(strings.NewReader("same plaintext"))
	if err != nil {
		t.Fatal(err)
	}
	if id, _, err := b.Put(strings.NewReader("same plaintext")); err != nil || id != sharedID {
		t.Fatalf("tenant-b Put = %q, %v; want logical id %q", id, err, sharedID)
	}
	secondID, _, err := a.Put(strings.NewReader("second artifact"))
	if err != nil {
		t.Fatal(err)
	}

	files := make(map[string][]byte)
	err = filepath.WalkDir(filepath.Join(root, "objects"), func(path string, entry os.DirEntry, walkErr error) error {
		if walkErr != nil || entry.IsDir() {
			return walkErr
		}
		relative, err := filepath.Rel(filepath.Join(root, "objects"), path)
		if err != nil {
			return err
		}
		body, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		files[filepath.ToSlash(relative)] = body
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	var tenantA, tenantB []byte
	for path, body := range files {
		if strings.HasPrefix(path, "tenants/tenant-a/") && strings.HasSuffix(path, physicalArtifactSuffix(sharedID)) {
			tenantA = body
		}
		if strings.HasPrefix(path, "tenants/tenant-b/") && strings.HasSuffix(path, physicalArtifactSuffix(sharedID)) {
			tenantB = body
		}
	}
	if len(tenantA) == 0 || len(tenantB) == 0 || string(tenantA) == string(tenantB) || strings.Contains(string(tenantA), "same plaintext") || strings.Contains(string(tenantB), "same plaintext") {
		t.Fatal("same plaintext was not physically isolated as independently encrypted tenant objects")
	}

	// Reopen every durable layer to prove keys, ciphertext, and logical ids
	// survive process restart before rotation begins.
	runtime = openLocalRuntime(t, root, 1)
	a, err = runtime.resolver.ResolveTenant("tenant-a")
	if err != nil {
		t.Fatal(err)
	}
	reader, _, err := a.Open(sharedID)
	if err != nil {
		t.Fatal(err)
	}
	body, readErr := io.ReadAll(reader)
	closeErr := reader.Close()
	if readErr != nil || closeErr != nil || string(body) != "same plaintext" {
		t.Fatalf("restart read = %q, read=%v close=%v", body, readErr, closeErr)
	}

	rotation, err := runtime.resolver.Rotate(context.Background(), "tenant-a")
	if err != nil {
		t.Fatal(err)
	}
	if rotation.Migrated != 1 || rotation.Remaining != 1 {
		t.Fatalf("bounded first rotation = %+v, want one migrated and one remaining", rotation)
	}
	continued, err := runtime.resolver.ContinueRotation(context.Background(), "tenant-a")
	if err != nil {
		t.Fatal(err)
	}
	if continued.Version != rotation.Version || continued.Migrated != 1 || continued.Remaining != 0 {
		t.Fatalf("continued rotation = %+v after %+v", continued, rotation)
	}
	for _, id := range []string{sharedID, secondID} {
		if err := runtime.store.Verify(context.Background(), "tenant-a", id); err != nil {
			t.Fatalf("verify %s after rotation: %v", id, err)
		}
	}
	objects, err := runtime.store.Inventory(context.Background(), "tenant-a")
	if err != nil {
		t.Fatal(err)
	}
	if len(objects) != 2 {
		t.Fatalf("post-rotation objects = %+v", objects)
	}
	for _, object := range objects {
		if object.KeyVersion != rotation.Version || object.Retired || !object.KeyAvailable {
			t.Fatalf("post-rotation object = %+v", object)
		}
	}
}
