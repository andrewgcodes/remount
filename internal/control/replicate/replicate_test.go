package replicate

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"errors"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	_ "modernc.org/sqlite"
	"remount.dev/remount/internal/artifact/s3"
)

type fakeObject struct {
	data []byte
	info s3.ObjectInfo
}
type fakeStore struct {
	mu      sync.Mutex
	objects map[string]fakeObject
	next    uint64
}

func newFakeStore() *fakeStore                                    { return &fakeStore{objects: make(map[string]fakeObject)} }
func (f *fakeStore) CheckConditionalWrites(context.Context) error { return nil }
func (f *fakeStore) PutObject(_ context.Context, key string, reader io.Reader, size int64, opts s3.PutOptions) (s3.ObjectInfo, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	old, exists := f.objects[key]
	if opts.IfNoneMatch == "*" && exists {
		return s3.ObjectInfo{}, s3.ErrPreconditionFailed
	}
	if opts.IfMatch != "" && (!exists || old.info.ETag != opts.IfMatch) {
		return s3.ObjectInfo{}, s3.ErrPreconditionFailed
	}
	data, err := io.ReadAll(io.LimitReader(reader, size+1))
	if err != nil {
		return s3.ObjectInfo{}, err
	}
	if int64(len(data)) != size {
		return s3.ObjectInfo{}, io.ErrUnexpectedEOF
	}
	sum := sha256.Sum256(data)
	if opts.ContentSHA256 != "" && opts.ContentSHA256 != hex.EncodeToString(sum[:]) {
		return s3.ObjectInfo{}, errors.New("digest")
	}
	f.next++
	info := s3.ObjectInfo{Key: key, Size: size, ETag: strconv.FormatUint(f.next, 10), LastModified: time.Unix(1, 0), Metadata: opts.Metadata}
	f.objects[key] = fakeObject{data: data, info: info}
	return info, nil
}
func (f *fakeStore) OpenObject(_ context.Context, key string) (io.ReadCloser, s3.ObjectInfo, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	o, ok := f.objects[key]
	if !ok {
		return nil, s3.ObjectInfo{}, &s3.Error{StatusCode: http.StatusNotFound, Code: "NoSuchKey"}
	}
	return io.NopCloser(bytes.NewReader(append([]byte(nil), o.data...))), o.info, nil
}
func (f *fakeStore) HeadObject(_ context.Context, key string) (s3.ObjectInfo, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	o, ok := f.objects[key]
	if !ok {
		return s3.ObjectInfo{}, &s3.Error{StatusCode: http.StatusNotFound}
	}
	return o.info, nil
}
func (f *fakeStore) ListObjects(_ context.Context, prefix string) ([]s3.ObjectInfo, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	var out []s3.ObjectInfo
	for key, o := range f.objects {
		if len(key) >= len(prefix) && key[:len(prefix)] == prefix {
			out = append(out, o.info)
		}
	}
	return out, nil
}
func (f *fakeStore) DeleteObject(_ context.Context, key string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	delete(f.objects, key)
	return nil
}

func TestConditionalLeaseElectsOneWriterAndSelfFences(t *testing.T) {
	store := newFakeStore()
	now := time.Unix(100, 0)
	opts := Options{LeaseTTL: 3 * time.Second, RenewInterval: time.Second, Now: func() time.Time { return now }}
	const candidates = 12
	var won atomic.Int32
	start := make(chan struct{})
	var wg sync.WaitGroup
	for i := 0; i < candidates; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			c, _ := NewCoordinator(store, "c"+strconv.Itoa(i), opts)
			<-start
			if _, _, err := c.Acquire(context.Background()); err == nil {
				won.Add(1)
			} else if !errors.Is(err, ErrLeaseHeld) {
				t.Errorf("acquire: %v", err)
			}
		}(i)
	}
	close(start)
	wg.Wait()
	if got := won.Load(); got != 1 {
		t.Fatalf("winners=%d, want 1", got)
	}
	winner, _ := NewCoordinator(store, "later", opts)
	now = now.Add(4 * time.Second)
	epoch, previous, err := winner.Acquire(context.Background())
	if err != nil || epoch != 2 || previous != 1 {
		t.Fatalf("promotion=(%d,%d,%v)", epoch, previous, err)
	}
	now = now.Add(3 * time.Second)
	if _, err := winner.CanDecide(); !errors.Is(err, ErrFenced) {
		t.Fatalf("CanDecide error=%v", err)
	}
}

func TestSQLiteShipAndPromoteRestoresCommittedWAL(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	sourcePath := filepath.Join(dir, "source.db")
	db, err := sql.Open("sqlite", sourcePath)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	db.SetMaxOpenConns(1)
	if _, err := db.Exec(`PRAGMA journal_mode=WAL`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`CREATE TABLE records (id INTEGER PRIMARY KEY, value TEXT); INSERT INTO records(value) VALUES ('snapshot')`); err != nil {
		t.Fatal(err)
	}
	store := newFakeStore()
	now := time.Unix(200, 0)
	opts := Options{LeaseTTL: 3 * time.Second, RenewInterval: time.Second, ShipInterval: time.Second, SnapshotInterval: time.Hour, OrphanGrace: 3 * time.Second, MaxManifestBytes: 1 << 20, MaxSnapshotBytes: 16 << 20, MaxWALBytes: 16 << 20, Now: func() time.Time { return now }}
	active, _ := NewCoordinator(store, "active", opts)
	if _, _, err := active.Acquire(ctx); err != nil {
		t.Fatal(err)
	}
	source, err := NewSQLiteSource(db, sourcePath, dir, opts.MaxSnapshotBytes, opts.MaxWALBytes, func() time.Time { return now })
	if err != nil {
		t.Fatal(err)
	}
	shipper, _ := NewShipper(store, source, active, opts)
	if _, err := shipper.Ship(ctx, 1, true); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO records(value) VALUES ('wal')`); err != nil {
		t.Fatal(err)
	}
	now = now.Add(time.Second)
	manifest, err := shipper.Ship(ctx, 2, false)
	if err != nil {
		t.Fatal(err)
	}
	if manifest.WAL == nil {
		t.Fatal("second recovery point omitted WAL")
	}
	now = now.Add(4 * time.Second)
	standby, _ := NewCoordinator(store, "standby", opts)
	restorer, _ := NewRestorer(store, standby, opts)
	destination := filepath.Join(dir, "restored.db")
	recovery, err := restorer.Promote(ctx, destination)
	if err != nil {
		t.Fatal(err)
	}
	if recovery.Epoch != 2 || !recovery.NeedsReconcile || recovery.LostWindow != 4*time.Second {
		t.Fatalf("recovery=%+v", recovery)
	}
	restored, err := sql.Open("sqlite", destination)
	if err != nil {
		t.Fatal(err)
	}
	defer restored.Close()
	var count int
	if err := restored.QueryRow(`SELECT count(*) FROM records`).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 2 {
		t.Fatalf("restored rows=%d, want 2", count)
	}
	orphan := []byte("orphan")
	if _, err := store.PutObject(ctx, "control/replication/epochs/orphan.bin", bytes.NewReader(orphan), int64(len(orphan)), s3.PutOptions{}); err != nil {
		t.Fatal(err)
	}
	if deleted, err := shipper.Collect(ctx); err != nil || deleted != 1 {
		t.Fatalf("Collect = (%d, %v), want one old orphan", deleted, err)
	}
}

func TestPromoteRejectsCorruptPayloadWithoutReplacingDestination(t *testing.T) {
	// Covered end-to-end by mutating immutable bytes after a valid publication.
	ctx := context.Background()
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "source.db")
	db, _ := sql.Open("sqlite", dbPath)
	defer db.Close()
	db.SetMaxOpenConns(1)
	_, _ = db.Exec(`PRAGMA journal_mode=WAL`)
	_, _ = db.Exec(`CREATE TABLE x (v INTEGER)`)
	store := newFakeStore()
	now := time.Unix(300, 0)
	opts := Options{LeaseTTL: 3 * time.Second, RenewInterval: time.Second, SnapshotInterval: time.Hour, MaxSnapshotBytes: 16 << 20, MaxWALBytes: 16 << 20, Now: func() time.Time { return now }}
	active, _ := NewCoordinator(store, "a", opts)
	_, _, _ = active.Acquire(ctx)
	source, _ := NewSQLiteSource(db, dbPath, dir, opts.MaxSnapshotBytes, opts.MaxWALBytes, func() time.Time { return now })
	shipper, _ := NewShipper(store, source, active, opts)
	manifest, err := shipper.Ship(ctx, 0, true)
	if err != nil {
		t.Fatal(err)
	}
	store.mu.Lock()
	broken := store.objects[manifest.Snapshot.Key]
	broken.data[0] ^= 0xff
	store.objects[manifest.Snapshot.Key] = broken
	store.mu.Unlock()
	destination := filepath.Join(dir, "live.db")
	if err := os.WriteFile(destination, []byte("sentinel"), 0o600); err != nil {
		t.Fatal(err)
	}
	now = now.Add(4 * time.Second)
	standby, _ := NewCoordinator(store, "b", opts)
	restorer, _ := NewRestorer(store, standby, opts)
	if _, err := restorer.Promote(ctx, destination); !errors.Is(err, ErrCorruptRecoveryPoint) {
		t.Fatalf("error=%v", err)
	}
	got, _ := os.ReadFile(destination)
	if string(got) != "sentinel" {
		t.Fatalf("destination replaced on failed validation")
	}
}
