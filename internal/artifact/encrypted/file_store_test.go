package encrypted

import (
	"bytes"
	"context"
	"errors"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestFileStoreCreateIsImmutableBoundedAndPersistent(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	store, err := NewFileStore(dir, FileStoreOptions{MaxBytes: 5, MaxObjects: 1})
	if err != nil {
		t.Fatal(err)
	}
	key := "tenants/tenant/v1/" + plaintextID([]byte("hello"))
	if err := store.Create(ctx, key, strings.NewReader("hello"), 5); err != nil {
		t.Fatal(err)
	}
	if err := store.Create(ctx, key, strings.NewReader("other"), 5); !errors.Is(err, ErrObjectExists) {
		t.Fatalf("direct create at capacity = %v", err)
	}
	r, size, err := store.Open(ctx, key)
	if err != nil || size != 5 {
		t.Fatalf("Open = (%d, %v)", size, err)
	}
	if err := store.Delete(ctx, key); err == nil {
		t.Fatal("pinned object was deleted")
	}
	body, err := io.ReadAll(r)
	if err != nil || string(body) != "hello" {
		t.Fatalf("read = %q, %v", body, err)
	}
	if err := r.Close(); err != nil {
		t.Fatal(err)
	}
	if err := store.Delete(ctx, key); err != nil {
		t.Fatal(err)
	}
	if stats := store.Stats(); stats.Bytes != 0 || stats.Objects != 0 || stats.ReservedBytes != 0 || stats.ReservedObjects != 0 {
		t.Fatalf("released stats = %+v", stats)
	}
	if err := store.Create(ctx, key, strings.NewReader("short"), 4); err == nil {
		t.Fatal("long body was published under a short declared size")
	}
	if _, err := os.Stat(filepath.Join(dir, filepath.FromSlash(key))); !errors.Is(err, fs.ErrNotExist) {
		t.Fatal("failed create left a visible object")
	}
}

func TestFileStoreConcurrentReservationAndCrashCleanup(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	store, err := NewFileStore(dir, FileStoreOptions{MaxBytes: 6, MaxObjects: 2})
	if err != nil {
		t.Fatal(err)
	}
	gate := make(chan struct{})
	started := make(chan struct{})
	firstKey := "tenants/tenant/v1/" + plaintextID([]byte("1234"))
	result := make(chan error, 1)
	go func() {
		result <- store.Create(ctx, firstKey, &blockingReader{body: []byte("1234"), started: started, gate: gate}, 4)
	}()
	<-started
	secondKey := "tenants/tenant/v1/" + plaintextID([]byte("abc"))
	if err := store.Create(ctx, secondKey, strings.NewReader("abc"), 3); !errors.Is(err, ErrPhysicalStoreFull) {
		t.Fatalf("concurrent overcommit = %v", err)
	}
	close(gate)
	if err := <-result; err != nil {
		t.Fatal(err)
	}
	if stats := store.Stats(); stats.Bytes != 4 || stats.Objects != 1 || stats.ReservedBytes != 0 || stats.ReservedObjects != 0 {
		t.Fatalf("stats = %+v", stats)
	}
	crash := filepath.Join(dir, ".create-crash")
	if err := os.WriteFile(crash, []byte("partial ciphertext"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := NewFileStore(dir, FileStoreOptions{MaxBytes: 6, MaxObjects: 2}); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(crash); !errors.Is(err, fs.ErrNotExist) {
		t.Fatal("crash staging was not removed")
	}
}

type blockingReader struct {
	body    []byte
	started chan<- struct{}
	gate    <-chan struct{}
	done    bool
}

func (r *blockingReader) Read(p []byte) (int, error) {
	if len(r.body) != 0 {
		n := copy(p, r.body)
		r.body = r.body[n:]
		return n, nil
	}
	if !r.done {
		r.done = true
		close(r.started)
		<-r.gate
	}
	return 0, io.EOF
}

func TestEncryptedStoreWithFileStore(t *testing.T) {
	ctx := context.Background()
	objects, err := NewFileStore(t.TempDir(), FileStoreOptions{MaxBytes: 1 << 20, MaxObjects: 10})
	if err != nil {
		t.Fatal(err)
	}
	store, err := NewStore(objects, newRotatingKeys(), t.TempDir(), Options{ChunkSize: 4, MaxPlaintextBytes: 1024, MaxStagingBytes: 1024})
	if err != nil {
		t.Fatal(err)
	}
	id, _, err := store.Put(ctx, "tenant", bytes.NewReader([]byte("filesystem round trip")))
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Verify(ctx, "tenant", id); err != nil {
		t.Fatal(err)
	}
}
