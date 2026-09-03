package chunked

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"remount.dev/remount/internal/artifact"
)

func newBlobStore(t *testing.T) *artifact.Store {
	t.Helper()
	store, err := artifact.NewStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	return store
}

func TestFastCDCDeterministicAndBounded(t *testing.T) {
	data := make([]byte, 4<<20)
	for i := range data {
		data[i] = byte((i*31 + i/97) % 251)
	}
	chunk := func(reader io.Reader) [][]byte {
		var out [][]byte
		chunker := NewChunker(reader)
		for {
			part, err := chunker.Next()
			if errors.Is(err, io.EOF) {
				break
			}
			if err != nil {
				t.Fatal(err)
			}
			out = append(out, part)
		}
		return out
	}
	a := chunk(bytes.NewReader(data))
	b := chunk(&oneByteReader{body: data})
	if len(a) != len(b) {
		t.Fatalf("reader segmentation changed boundaries: %d != %d", len(a), len(b))
	}
	for i := range a {
		if !bytes.Equal(a[i], b[i]) {
			t.Fatalf("chunk %d differs", i)
		}
		if i < len(a)-1 && (len(a[i]) < MinChunkSize || len(a[i]) > MaxChunkSize) {
			t.Fatalf("chunk %d size %d outside bounds", i, len(a[i]))
		}
	}
}

type oneByteReader struct{ body []byte }

func (r *oneByteReader) Read(p []byte) (int, error) {
	if len(r.body) == 0 {
		return 0, io.EOF
	}
	p[0] = r.body[0]
	r.body = r.body[1:]
	return 1, nil
}

func TestSnapshotRestoreAndTarInteropAreByteIdentical(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	if err := os.Mkdir(filepath.Join(root, "dir"), 0o751); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "dir", "file.txt"), []byte(strings.Repeat("content-", 20_000)), 0o640); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "empty"), nil, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink("dir/file.txt", filepath.Join(root, "link")); err != nil {
		t.Fatal(err)
	}
	mtime := time.Unix(1_700_000_000, 123_456_789)
	for _, path := range []string{filepath.Join(root, "dir", "file.txt"), filepath.Join(root, "empty"), filepath.Join(root, "dir")} {
		if err := os.Chtimes(path, mtime, mtime); err != nil {
			t.Fatal(err)
		}
	}
	file, err := os.OpenFile(filepath.Join(root, "dir", "file.txt"), os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}
	xattrSet := true
	if err := applyXattrs(file, []Xattr{{Name: "user.remount-test", Value: []byte("xattr-value")}}); err != nil {
		xattrSet = false
		_ = file.Close()
		t.Logf("extended attributes unavailable: %v", err)
	} else if err := file.Close(); err != nil {
		t.Fatal(err)
	}

	var originalTar bytes.Buffer
	if err := artifact.Snapshot(root, nil, &originalTar); err != nil {
		t.Fatal(err)
	}
	store := newBlobStore(t)
	result, err := Snapshot(ctx, store, root, SnapshotOptions{HotPaths: []string{"dir/file.txt"}})
	if err != nil {
		t.Fatal(err)
	}
	if result.ManifestID == "" || result.PlaintextBytes == 0 || result.Chunks == 0 {
		t.Fatalf("snapshot result = %+v", result)
	}
	var exported bytes.Buffer
	if err := ExportTar(ctx, store, result.ManifestID, &exported, Limits{}); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(exported.Bytes(), originalTar.Bytes()) {
		t.Fatal("chunked full-tar export differs from artifact.Snapshot")
	}
	restored := filepath.Join(t.TempDir(), "restored")
	if err := os.Mkdir(restored, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := Restore(ctx, store, result.ManifestID, restored, Limits{}); err != nil {
		t.Fatal(err)
	}
	restoredBody, err := os.ReadFile(filepath.Join(restored, "dir", "file.txt"))
	if err != nil || string(restoredBody) != strings.Repeat("content-", 20_000) {
		t.Fatal("eager restored file differs")
	}
	target, err := os.Readlink(filepath.Join(restored, "link"))
	if err != nil || filepath.ToSlash(target) != "dir/file.txt" {
		t.Fatalf("restored symlink = %q, %v", target, err)
	}
	if xattrSet {
		restoredFile, err := os.Open(filepath.Join(restored, "dir", "file.txt"))
		if err != nil {
			t.Fatal(err)
		}
		attrs, attrErr := readXattrs(restoredFile, normalizedLimits(Limits{}))
		closeErr := restoredFile.Close()
		if attrErr != nil || closeErr != nil {
			t.Fatal(errors.Join(attrErr, closeErr))
		}
		found := false
		for _, attr := range attrs {
			found = found || attr.Name == "user.remount-test" && string(attr.Value) == "xattr-value"
		}
		if !found {
			t.Fatal("restored file lost its extended attribute")
		}
	}
}

func TestSnapshotUsesHeadAndSecondLargeSnapshotUploadsUnderOneMiB(t *testing.T) {
	if testing.Short() {
		t.Skip("500 MB generated acceptance fixture")
	}
	ctx := context.Background()
	root := t.TempDir()
	path := filepath.Join(root, "fixture.bin")
	const fixtureSize = int64(500 << 20)
	if err := os.WriteFile(path, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Truncate(path, fixtureSize); err != nil {
		t.Fatal(err)
	}
	store := newBlobStore(t)
	first, err := Snapshot(ctx, store, root, SnapshotOptions{})
	if err != nil {
		t.Fatal(err)
	}
	file, err := os.OpenFile(path, os.O_WRONLY, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := file.WriteAt(bytes.Repeat([]byte{0x5a}, 4096), fixtureSize/2); err != nil {
		_ = file.Close()
		t.Fatal(err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}
	second, err := Snapshot(ctx, store, root, SnapshotOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if first.ManifestID == second.ManifestID {
		t.Fatal("changed fixture retained the same manifest")
	}
	if second.BytesUploaded >= 1<<20 {
		t.Fatalf("second snapshot uploaded %d bytes, want < 1 MiB", second.BytesUploaded)
	}
	t.Logf("500 MB fixture second upload: %d bytes", second.BytesUploaded)
}

type blockingStore struct {
	artifact.BlobStore
	blockID string
	started chan struct{}
	release chan struct{}
	once    sync.Once
}

func (s *blockingStore) Open(id string) (io.ReadCloser, int64, error) {
	if id == s.blockID {
		s.once.Do(func() { close(s.started) })
		<-s.release
	}
	return s.BlobStore.Open(id)
}

func TestLazyPrefetchHotSetAndCancellationMustBeJoined(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	if err := os.WriteFile(filepath.Join(root, "hot"), bytes.Repeat([]byte("h"), MaxChunkSize+17), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "cold"), bytes.Repeat([]byte("c"), MaxChunkSize+31), 0o600); err != nil {
		t.Fatal(err)
	}
	remote := newBlobStore(t)
	result, err := Snapshot(ctx, remote, root, SnapshotOptions{HotPaths: []string{"hot"}})
	if err != nil {
		t.Fatal(err)
	}
	manifest, err := LoadManifest(remote, result.ManifestID, Limits{})
	if err != nil {
		t.Fatal(err)
	}
	var hotChunk, coldChunk string
	for _, entry := range manifest.Entries {
		if entry.Path == "hot" {
			hotChunk = entry.Chunks[0].ID
		}
		if entry.Path == "cold" {
			coldChunk = entry.Chunks[0].ID
		}
	}
	blocked := &blockingStore{BlobStore: remote, blockID: coldChunk, started: make(chan struct{}), release: make(chan struct{})}
	cache := newBlobStore(t)
	job, err := StartPrefetch(ctx, blocked, cache, result.ManifestID, Limits{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := cache.Head(hotChunk); err != nil {
		t.Fatal("hot chunk was not fetched synchronously")
	}
	<-blocked.started
	job.Cancel()
	waited := make(chan error, 1)
	go func() { waited <- job.Wait() }()
	select {
	case <-waited:
		t.Fatal("Cancel was reported as completion before the blocked read exited")
	case <-time.After(20 * time.Millisecond):
	}
	close(blocked.release)
	if err := <-waited; !errors.Is(err, context.Canceled) {
		t.Fatalf("Wait = %v, want context cancellation", err)
	}
}

func TestLazyPrefetchCompletionMakesCacheRestorable(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	if err := os.WriteFile(filepath.Join(root, "hot"), []byte("hot body"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "cold"), bytes.Repeat([]byte("cold"), 100_000), 0o600); err != nil {
		t.Fatal(err)
	}
	remote := newBlobStore(t)
	result, err := Snapshot(ctx, remote, root, SnapshotOptions{HotPaths: []string{"hot"}})
	if err != nil {
		t.Fatal(err)
	}
	cache := newBlobStore(t)
	job, err := StartPrefetch(ctx, remote, cache, result.ManifestID, Limits{})
	if err != nil {
		t.Fatal(err)
	}
	if err := job.Wait(); err != nil {
		t.Fatal(err)
	}
	destination := filepath.Join(t.TempDir(), "destination")
	if err := os.Mkdir(destination, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := Restore(ctx, cache, result.ManifestID, destination, Limits{}); err != nil {
		t.Fatal(err)
	}
	if body, err := os.ReadFile(filepath.Join(destination, "cold")); err != nil || len(body) != 400_000 {
		t.Fatalf("restored cold file = %d bytes, %v", len(body), err)
	}
}

func TestManifestCorruptionAndNonCanonicalEncodingFailClosed(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	if err := os.WriteFile(filepath.Join(root, "file"), []byte("body"), 0o600); err != nil {
		t.Fatal(err)
	}
	store := newBlobStore(t)
	result, err := Snapshot(ctx, store, root, SnapshotOptions{})
	if err != nil {
		t.Fatal(err)
	}
	r, _, err := store.Open(result.ManifestID)
	if err != nil {
		t.Fatal(err)
	}
	body, _ := io.ReadAll(r)
	_ = r.Close()
	body = append(body, ' ')
	nonCanonicalID, _, err := store.Put(bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := LoadManifest(store, nonCanonicalID, Limits{}); err == nil {
		t.Fatal("non-canonical manifest was accepted")
	}
	bad := &lyingStore{BlobStore: store, id: result.ManifestID, body: []byte("corrupt")}
	if _, err := LoadManifest(bad, result.ManifestID, Limits{}); !errors.Is(err, artifact.ErrDigestMismatch) {
		t.Fatalf("corrupt manifest = %v", err)
	}
}

func TestCorruptChunkDoesNotReplaceDestination(t *testing.T) {
	ctx := context.Background()
	source := t.TempDir()
	if err := os.WriteFile(filepath.Join(source, "file"), bytes.Repeat([]byte("payload"), 10_000), 0o600); err != nil {
		t.Fatal(err)
	}
	store := newBlobStore(t)
	result, err := Snapshot(ctx, store, source, SnapshotOptions{})
	if err != nil {
		t.Fatal(err)
	}
	manifest, err := LoadManifest(store, result.ManifestID, Limits{})
	if err != nil {
		t.Fatal(err)
	}
	chunk := manifest.Entries[0].Chunks[0]
	corrupt := bytes.Repeat([]byte{0xff}, chunk.Size)
	bad := &lyingStore{BlobStore: store, id: chunk.ID, body: corrupt}
	destination := filepath.Join(t.TempDir(), "destination")
	if err := os.Mkdir(destination, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(destination, "sentinel"), []byte("old-authority"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := Restore(ctx, bad, result.ManifestID, destination, Limits{}); !errors.Is(err, artifact.ErrDigestMismatch) {
		t.Fatalf("Restore = %v, want digest mismatch", err)
	}
	body, err := os.ReadFile(filepath.Join(destination, "sentinel"))
	if err != nil || string(body) != "old-authority" {
		t.Fatal("failed restore changed the authoritative destination")
	}
}

func TestManifestAndSnapshotResourceBounds(t *testing.T) {
	bad := Manifest{
		Version: manifestVersion,
		Chunker: ChunkSpec{Algorithm: "fastcdc-v1", Min: MinChunkSize, Average: AvgChunkSize, Max: MaxChunkSize},
		Entries: []Entry{{Path: "missing-parent/file", Type: "file", Mode: 0o600}},
	}
	if _, err := canonicalManifest(bad, normalizedLimits(Limits{})); err == nil {
		t.Fatal("manifest with an absent directory parent was accepted")
	}
	root := t.TempDir()
	if err := os.WriteFile(filepath.Join(root, "large"), bytes.Repeat([]byte{0x9d}, MaxChunkSize*3), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := Snapshot(context.Background(), newBlobStore(t), root, SnapshotOptions{Limits: Limits{MaxChunks: 1}}); err == nil {
		t.Fatal("snapshot chunk limit was not enforced")
	}
}

func TestConcurrentSnapshotsConvergeOnOneManifest(t *testing.T) {
	root := t.TempDir()
	if err := os.WriteFile(filepath.Join(root, "file"), bytes.Repeat([]byte("concurrent"), 100_000), 0o600); err != nil {
		t.Fatal(err)
	}
	store := newBlobStore(t)
	const workers = 4
	start := make(chan struct{})
	results := make(chan SnapshotResult, workers)
	errs := make(chan error, workers)
	for i := 0; i < workers; i++ {
		go func() {
			<-start
			result, err := Snapshot(context.Background(), store, root, SnapshotOptions{})
			results <- result
			errs <- err
		}()
	}
	close(start)
	var id string
	for i := 0; i < workers; i++ {
		result := <-results
		if err := <-errs; err != nil {
			t.Fatal(err)
		}
		if id == "" {
			id = result.ManifestID
		} else if result.ManifestID != id {
			t.Fatalf("concurrent manifest ids differ: %q != %q", result.ManifestID, id)
		}
	}
}

type lyingStore struct {
	artifact.BlobStore
	id   string
	body []byte
}

func (s *lyingStore) Open(id string) (io.ReadCloser, int64, error) {
	if id == s.id {
		return io.NopCloser(bytes.NewReader(s.body)), int64(len(s.body)), nil
	}
	return s.BlobStore.Open(id)
}
