package encrypted

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"testing"

	"remount.dev/remount/internal/artifact"
)

type memoryObjects struct {
	mu      sync.Mutex
	objects map[string][]byte
}

func newMemoryObjects() *memoryObjects {
	return &memoryObjects{objects: make(map[string][]byte)}
}

func (s *memoryObjects) Create(_ context.Context, key string, body io.Reader, size int64) error {
	data, err := io.ReadAll(io.LimitReader(body, size+1))
	if err != nil {
		return err
	}
	if int64(len(data)) != size {
		return io.ErrUnexpectedEOF
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.objects[key]; ok {
		return ErrObjectExists
	}
	s.objects[key] = data
	return nil
}

func (s *memoryObjects) Open(_ context.Context, key string) (io.ReadCloser, int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	data, ok := s.objects[key]
	if !ok {
		return nil, 0, fs.ErrNotExist
	}
	copy := append([]byte(nil), data...)
	return io.NopCloser(bytes.NewReader(copy)), int64(len(copy)), nil
}

func (s *memoryObjects) Head(_ context.Context, key string) (int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	data, ok := s.objects[key]
	if !ok {
		return 0, fs.ErrNotExist
	}
	return int64(len(data)), nil
}

func (s *memoryObjects) Delete(_ context.Context, key string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.objects[key]; !ok {
		return fs.ErrNotExist
	}
	delete(s.objects, key)
	return nil
}

func (s *memoryObjects) List(_ context.Context, prefix string) ([]string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []string
	for key := range s.objects {
		if strings.HasPrefix(key, prefix) {
			out = append(out, key)
		}
	}
	sort.Strings(out)
	return out, nil
}

func (s *memoryObjects) mutate(key string, fn func([]byte)) {
	s.mu.Lock()
	defer s.mu.Unlock()
	fn(s.objects[key])
}

type rotatingKeys struct {
	mu      sync.Mutex
	current string
	keys    map[string][32]byte
}

func newRotatingKeys() *rotatingKeys {
	return &rotatingKeys{current: "v1", keys: map[string][32]byte{"v1": sha256.Sum256([]byte("tenant-key-v1"))}}
}

func (p *rotatingKeys) Current(_ context.Context, _ string) (string, [32]byte, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.current, p.keys[p.current], nil
}

func (p *rotatingKeys) Get(_ context.Context, _ string, version string) ([32]byte, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	key, ok := p.keys[version]
	if !ok {
		return [32]byte{}, ErrKeyUnavailable
	}
	return key, nil
}

func (p *rotatingKeys) Versions(_ context.Context, _ string) ([]KeyVersion, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	out := []KeyVersion{{Version: p.current}}
	for version := range p.keys {
		if version != p.current {
			out = append(out, KeyVersion{Version: version, Retired: true})
		}
	}
	return out, nil
}

func (p *rotatingKeys) rotate(version string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.keys[version] = sha256.Sum256([]byte("tenant-key-" + version))
	p.current = version
}

func newTestStore(t *testing.T, objects *memoryObjects, keys KeyProvider, opts Options) *Store {
	t.Helper()
	store, err := NewStore(objects, keys, t.TempDir(), opts)
	if err != nil {
		t.Fatal(err)
	}
	return store
}

func plaintextID(body []byte) string {
	sum := sha256.Sum256(body)
	return artifact.ID(sum[:])
}

func TestStorePreservesPlaintextIdentityAndEncryptsPhysicalObject(t *testing.T) {
	ctx := context.Background()
	objects := newMemoryObjects()
	store := newTestStore(t, objects, newRotatingKeys(), Options{ChunkSize: 4})
	body := []byte("plaintext that must not appear at rest")
	id, n, err := store.Put(ctx, "tenant-a", bytes.NewReader(body))
	if err != nil || n != int64(len(body)) || id != plaintextID(body) {
		t.Fatalf("Put = (%q, %d, %v)", id, n, err)
	}
	key, _ := ObjectKey("tenant-a", "v1", id)
	raw := objects.objects[key]
	if len(raw) == 0 || bytes.Contains(raw, body) {
		t.Fatal("physical object is absent or contains plaintext")
	}
	r, size, err := store.Open(ctx, "tenant-a", id)
	if err != nil || size != int64(len(body)) {
		t.Fatalf("Open = (%d, %v)", size, err)
	}
	got, err := io.ReadAll(r)
	closeErr := r.Close()
	if err != nil || closeErr != nil || !bytes.Equal(got, body) {
		t.Fatalf("read = %q, %v, close=%v", got, err, closeErr)
	}
	if err := store.Verify(ctx, "tenant-a", id); err != nil {
		t.Fatal(err)
	}
}

func TestCrossTenantDedupeIsRefused(t *testing.T) {
	ctx := context.Background()
	objects := newMemoryObjects()
	store := newTestStore(t, objects, newRotatingKeys(), Options{})
	body := []byte("same plaintext")
	idA, _, err := store.Put(ctx, "tenant-a", bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	idB, _, err := store.Put(ctx, "tenant-b", bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	if idA != idB {
		t.Fatal("logical plaintext identity changed across tenants")
	}
	keys, _ := objects.List(ctx, "tenants/")
	if len(keys) != 2 || !strings.Contains(keys[0], "tenant-a") || !strings.Contains(keys[1], "tenant-b") {
		t.Fatalf("physical tenant isolation = %v", keys)
	}
	if bytes.Equal(objects.objects[keys[0]], objects.objects[keys[1]]) {
		t.Fatal("two tenants shared ciphertext")
	}
	if _, _, err := store.Open(ctx, "tenant-c", idA); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("cross-tenant open = %v, want not exist", err)
	}
}

func TestRotationKeepsOldReadableAndRewrapMigratesAfterVerification(t *testing.T) {
	ctx := context.Background()
	objects := newMemoryObjects()
	keys := newRotatingKeys()
	store := newTestStore(t, objects, keys, Options{ChunkSize: 3})
	body := []byte("rotate without re-encrypting chunk ciphertext")
	id, _, err := store.Put(ctx, "tenant", bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	oldKey, _ := ObjectKey("tenant", "v1", id)
	oldRaw := append([]byte(nil), objects.objects[oldKey]...)
	oldHeader, oldHeaderBytes, err := readHeader(bytes.NewReader(oldRaw), int64(len(oldRaw)), "tenant", "v1", id)
	if err != nil || oldHeader.KeyVersion != "v1" {
		t.Fatal(err)
	}
	keys.rotate("v2")
	if err := store.Verify(ctx, "tenant", id); err != nil {
		t.Fatalf("old version stopped being readable: %v", err)
	}
	migrated, err := store.Rewrap(ctx, "tenant", id)
	if err != nil || !migrated {
		t.Fatalf("Rewrap = (%v, %v)", migrated, err)
	}
	newKey, _ := ObjectKey("tenant", "v2", id)
	newRaw := objects.objects[newKey]
	_, newHeaderBytes, err := readHeader(bytes.NewReader(newRaw), int64(len(newRaw)), "tenant", "v2", id)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(oldRaw[len(oldHeaderBytes):], newRaw[len(newHeaderBytes):]) {
		t.Fatal("rewrap changed data ciphertext instead of only the wrapped data key")
	}
	if _, ok := objects.objects[oldKey]; ok {
		t.Fatal("old physical version remained after verified migration")
	}
	if err := store.Verify(ctx, "tenant", id); err != nil {
		t.Fatal(err)
	}
}

func TestRewriteOnRotationAndBoundedBackgroundCleanup(t *testing.T) {
	ctx := context.Background()
	objects := newMemoryObjects()
	keys := newRotatingKeys()
	store := newTestStore(t, objects, keys, Options{ChunkSize: 5})
	ids := make([]string, 0, 2)
	for _, body := range []string{"first artifact", "second artifact"} {
		id, _, err := store.Put(ctx, "tenant", strings.NewReader(body))
		if err != nil {
			t.Fatal(err)
		}
		ids = append(ids, id)
	}
	keys.rotate("v2")
	// A repeated upload under a new active version creates a new physical
	// object instead of treating another version as a cross-version dedupe hit.
	if got, _, err := store.Put(ctx, "tenant", strings.NewReader("first artifact")); err != nil || got != ids[0] {
		t.Fatalf("rotated Put = (%q, %v)", got, err)
	}
	oldFirst, _ := ObjectKey("tenant", "v1", ids[0])
	newFirst, _ := ObjectKey("tenant", "v2", ids[0])
	if _, oldOK := objects.objects[oldFirst]; !oldOK {
		t.Fatal("old version disappeared before verified migration")
	}
	if _, newOK := objects.objects[newFirst]; !newOK {
		t.Fatal("new version was not written on repeated upload")
	}
	result, err := store.RewrapRetired(ctx, "tenant", 1)
	if err != nil || result.Scanned != 2 || result.Migrated != 1 || result.Remaining != 1 {
		t.Fatalf("first RewrapRetired = %+v, %v", result, err)
	}
	result, err = store.RewrapRetired(ctx, "tenant", 10)
	if err != nil || result.Migrated != 1 || result.Remaining != 0 {
		t.Fatalf("second RewrapRetired = %+v, %v", result, err)
	}
	for _, id := range ids {
		old, _ := ObjectKey("tenant", "v1", id)
		if _, ok := objects.objects[old]; ok {
			t.Fatalf("retired physical object remains for %s", id)
		}
	}
}

func TestInventoryMakesCorruptRetiredCopyObservable(t *testing.T) {
	ctx := context.Background()
	objects := newMemoryObjects()
	keys := newRotatingKeys()
	store := newTestStore(t, objects, keys, Options{ChunkSize: 4})
	body := "duplicate across rotation"
	id, _, err := store.Put(ctx, "tenant", strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	keys.rotate("v2")
	if _, _, err := store.Put(ctx, "tenant", strings.NewReader(body)); err != nil {
		t.Fatal(err)
	}
	old, _ := ObjectKey("tenant", "v1", id)
	objects.mutate(old, func(body []byte) { body[len(body)-1] ^= 1 })
	if err := store.Verify(ctx, "tenant", id); err != nil {
		t.Fatalf("logical current copy should remain readable: %v", err)
	}
	inventory, err := store.Inventory(ctx, "tenant")
	if err != nil || len(inventory) != 2 {
		t.Fatalf("Inventory = %+v, %v", inventory, err)
	}
	var sawRetired bool
	for _, object := range inventory {
		if object.KeyVersion == "v1" {
			sawRetired = object.Retired && object.KeyAvailable
			if err := store.VerifyVersion(ctx, "tenant", object.KeyVersion, object.ID); !errors.Is(err, ErrIntegrity) {
				t.Fatalf("retired corruption = %v", err)
			}
		}
	}
	if !sawRetired {
		t.Fatalf("retired copy not observable: %+v", inventory)
	}
}

func TestEmptyArtifactRoundTrip(t *testing.T) {
	ctx := context.Background()
	store := newTestStore(t, newMemoryObjects(), newRotatingKeys(), Options{ChunkSize: 1})
	id, n, err := store.Put(ctx, "tenant", bytes.NewReader(nil))
	if err != nil || n != 0 || id != plaintextID(nil) {
		t.Fatalf("Put empty = (%q, %d, %v)", id, n, err)
	}
	if err := store.Verify(ctx, "tenant", id); err != nil {
		t.Fatal(err)
	}
}

type corruptingCreateStore struct{ *memoryObjects }

func (s *corruptingCreateStore) Create(ctx context.Context, key string, body io.Reader, size int64) error {
	if err := s.memoryObjects.Create(ctx, key, body, size); err != nil {
		return err
	}
	s.mutate(key, func(body []byte) { body[len(body)-1] ^= 1 })
	return nil
}

func TestPutVerifiesPublishedCiphertextAndCleansCorruption(t *testing.T) {
	ctx := context.Background()
	objects := &corruptingCreateStore{newMemoryObjects()}
	store, err := NewStore(objects, newRotatingKeys(), t.TempDir(), Options{})
	if err != nil {
		t.Fatal(err)
	}
	id := plaintextID([]byte("body"))
	if _, _, err := store.Put(ctx, "tenant", strings.NewReader("body")); !errors.Is(err, ErrIntegrity) {
		t.Fatalf("Put = %v, want integrity failure", err)
	}
	physical, _ := ObjectKey("tenant", "v1", id)
	if _, ok := objects.objects[physical]; ok {
		t.Fatal("corrupt newly published object was retained")
	}
}

func TestConcurrentIdenticalPutPublishesOneVerifiedObject(t *testing.T) {
	ctx := context.Background()
	objects := newMemoryObjects()
	store := newTestStore(t, objects, newRotatingKeys(), Options{MaxConcurrentWrites: 8})
	const writers = 8
	start := make(chan struct{})
	errs := make(chan error, writers)
	for i := 0; i < writers; i++ {
		go func() {
			<-start
			_, _, err := store.Put(ctx, "tenant", strings.NewReader("identical"))
			errs <- err
		}()
	}
	close(start)
	for i := 0; i < writers; i++ {
		if err := <-errs; err != nil {
			t.Fatal(err)
		}
	}
	listed, err := objects.List(ctx, "tenants/tenant/")
	if err != nil || len(listed) != 1 {
		t.Fatalf("physical objects = %v, %v", listed, err)
	}
}

func TestCorruptionAndMissingKeyFailClosed(t *testing.T) {
	ctx := context.Background()
	for _, tc := range []struct {
		name   string
		mutate func([]byte)
	}{
		{"format", func(b []byte) { b[8] ^= 1 }},
		{"chunk-size", func(b []byte) { b[15] ^= 1 }},
		{"wrapped-key", func(b []byte) { b[100] ^= 1 }},
		{"ciphertext", func(b []byte) { b[len(b)-1] ^= 1 }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			objects := newMemoryObjects()
			keys := newRotatingKeys()
			store := newTestStore(t, objects, keys, Options{ChunkSize: 4})
			body := []byte("more than one encrypted chunk")
			id, _, err := store.Put(ctx, "tenant", bytes.NewReader(body))
			if err != nil {
				t.Fatal(err)
			}
			key, _ := ObjectKey("tenant", "v1", id)
			objects.mutate(key, tc.mutate)
			if err := store.Verify(ctx, "tenant", id); err == nil {
				t.Fatal("corruption verified")
			}
		})
	}
	objects := newMemoryObjects()
	keys := newRotatingKeys()
	store := newTestStore(t, objects, keys, Options{})
	id, _, err := store.Put(ctx, "tenant", strings.NewReader("key disappears"))
	if err != nil {
		t.Fatal(err)
	}
	keys.mu.Lock()
	delete(keys.keys, "v1")
	keys.mu.Unlock()
	if err := store.Verify(ctx, "tenant", id); !errors.Is(err, ErrKeyUnavailable) {
		t.Fatalf("missing key verify = %v", err)
	}
}

func TestCloseDrainsAndReportsLateCorruption(t *testing.T) {
	ctx := context.Background()
	objects := newMemoryObjects()
	store := newTestStore(t, objects, newRotatingKeys(), Options{ChunkSize: 4})
	id, _, err := store.Put(ctx, "tenant", strings.NewReader("first chunk and corrupted tail"))
	if err != nil {
		t.Fatal(err)
	}
	key, _ := ObjectKey("tenant", "v1", id)
	objects.mutate(key, func(b []byte) { b[len(b)-1] ^= 1 })
	r, _, err := store.Open(ctx, "tenant", id)
	if err != nil {
		t.Fatal(err)
	}
	buf := make([]byte, 2)
	if _, err := r.Read(buf); err != nil {
		t.Fatalf("first read reached late corruption: %v", err)
	}
	if err := r.Close(); !errors.Is(err, ErrIntegrity) {
		t.Fatalf("Close = %v, want integrity failure", err)
	}
}

func TestStagingBoundsAndCleanup(t *testing.T) {
	ctx := context.Background()
	objects := newMemoryObjects()
	stage := t.TempDir()
	store, err := NewStore(objects, newRotatingKeys(), stage, Options{
		MaxPlaintextBytes: 4, MaxStagingBytes: 4, MaxConcurrentWrites: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, n, err := store.Put(ctx, "tenant", strings.NewReader("12345")); !errors.Is(err, ErrTooLarge) || n != 5 {
		t.Fatalf("oversize Put = (%d, %v)", n, err)
	}
	entries, err := os.ReadDir(stage)
	if err != nil || len(entries) != 0 {
		t.Fatalf("staging leaked after rejection: %v, %v", entries, err)
	}
	stats := store.Stats()
	if stats.StagingBytes != 0 || stats.ConcurrentWrites != 0 {
		t.Fatalf("reservation leaked: %+v", stats)
	}
	crashFile := filepath.Join(stage, ".encrypt-crash")
	if err := os.WriteFile(crashFile, []byte("plaintext"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := NewStore(objects, newRotatingKeys(), stage, Options{}); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(crashFile); !errors.Is(err, fs.ErrNotExist) {
		t.Fatal("crash staging file was not removed")
	}
}

func TestTenantAdapterImplementsBlobStore(t *testing.T) {
	store := newTestStore(t, newMemoryObjects(), newRotatingKeys(), Options{})
	tenant, err := store.ForTenant("tenant")
	if err != nil {
		t.Fatal(err)
	}
	id, _, err := tenant.Put(strings.NewReader("adapter"))
	if err != nil {
		t.Fatal(err)
	}
	if err := tenant.Verify(id); err != nil {
		t.Fatal(err)
	}
	ids, err := tenant.List()
	if err != nil || len(ids) != 1 || ids[0] != id {
		t.Fatalf("List = %v, %v", ids, err)
	}
}
