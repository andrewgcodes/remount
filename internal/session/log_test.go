package session

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"io"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"testing"
	"time"

	"remount.dev/remount/internal/artifact"
)

func TestLogAppendReadCursor(t *testing.T) {
	l, err := NewLog(LogOptions{MemBytes: 1 << 20})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = l.Release() })
	if s, _ := l.Append(1, []byte("a")); s != 0 {
		t.Fatal(s)
	}
	if s, _ := l.Append(2, []byte("b")); s != 1 {
		t.Fatal(s)
	}
	chunks, err := l.Read(0, 0)
	if err != nil || len(chunks) != 2 || string(chunks[1].Data) != "b" || chunks[1].Stream != 2 {
		t.Fatalf("%v %+v", err, chunks)
	}
	chunks, _ = l.Read(1, 0)
	if len(chunks) != 1 || chunks[0].Seq != 1 {
		t.Fatalf("%+v", chunks)
	}
	chunks, _ = l.Read(2, 0)
	if len(chunks) != 0 {
		t.Fatalf("%+v", chunks)
	}

	// Cursor: live tail wakes on append, EOF after close.
	c := l.CursorAt(0)
	got, err := c.Next(context.Background(), 0)
	if err != nil || len(got) != 2 || c.Seq() != 2 {
		t.Fatalf("%v %d", err, c.Seq())
	}
	done := make(chan []Chunk, 1)
	go func() {
		g, _ := c.Next(context.Background(), 0)
		done <- g
	}()
	time.Sleep(10 * time.Millisecond)
	l.Append(1, []byte("c"))
	select {
	case g := <-done:
		if len(g) != 1 || string(g[0].Data) != "c" {
			t.Fatalf("%+v", g)
		}
	case <-time.After(time.Second):
		t.Fatal("cursor did not wake")
	}
	l.Close()
	if _, err := c.Next(context.Background(), 0); !errors.Is(err, io.EOF) {
		t.Fatalf("want EOF, got %v", err)
	}
	if _, err := l.Append(1, []byte("x")); !errors.Is(err, ErrLogClosed) {
		t.Fatal(err)
	}
	// Cursor ctx cancellation.
	l2, _ := NewLog(LogOptions{})
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if _, err := l2.CursorAt(0).Next(ctx, 0); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal(err)
	}
}

func TestLogSplitsLargeAppends(t *testing.T) {
	l, _ := NewLog(LogOptions{MaxChunk: 10, MemBytes: 1 << 20})
	data := bytes.Repeat([]byte("z"), 35)
	l.Append(1, data)
	chunks, _ := l.Read(0, 0)
	if len(chunks) != 4 || len(chunks[3].Data) != 5 || l.Next() != 4 {
		t.Fatalf("%d chunks, next=%d", len(chunks), l.Next())
	}
	var joined []byte
	for _, c := range chunks {
		joined = append(joined, c.Data...)
	}
	if !bytes.Equal(joined, data) {
		t.Fatal("reassembly mismatch")
	}
	// Empty append still gets a seq (used for exit/info markers).
	if s, _ := l.Append(3, nil); s != 4 {
		t.Fatal(s)
	}
}

func TestLogEvictsToSpillAndReplays(t *testing.T) {
	dir := t.TempDir()
	l, err := NewLog(LogOptions{MemBytes: 100, MaxChunk: 10, SpillBytes: 1 << 20, SpillPath: filepath.Join(dir, "spill")})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = l.closeLocal() })
	var want []byte
	for i := 0; i < 50; i++ {
		piece := bytes.Repeat([]byte{byte('a' + i%26)}, 10)
		want = append(want, piece...)
		l.Append(1, piece)
	}
	if l.Oldest() != 0 {
		t.Fatalf("spill should retain everything; oldest=%d", l.Oldest())
	}
	chunks, err := l.Read(0, 0)
	if err != nil {
		t.Fatal(err)
	}
	var got []byte
	prev := uint64(0)
	for i, c := range chunks {
		if i > 0 && c.Seq != prev+1 {
			t.Fatalf("seq gap at %d: %d after %d", i, c.Seq, prev)
		}
		prev = c.Seq
		got = append(got, c.Data...)
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("replay mismatch: %d vs %d bytes", len(got), len(want))
	}
	// Partial replay that straddles spill and memory.
	chunks, _ = l.Read(45, 0)
	if len(chunks) != 5 || chunks[0].Seq != 45 {
		t.Fatalf("%d %d", len(chunks), chunks[0].Seq)
	}
	// Cursor over the whole thing, batched.
	c := l.CursorAt(0)
	total := 0
	for {
		b, err := c.Next(context.Background(), 7)
		if err != nil {
			t.Fatal(err)
		}
		total += len(b)
		if c.Seq() == l.Next() {
			break
		}
	}
	if total != 50 {
		t.Fatal(total)
	}
}

func TestLogEvictedWithoutSpill(t *testing.T) {
	l, _ := NewLog(LogOptions{MemBytes: 30, MaxChunk: 10})
	for i := 0; i < 10; i++ {
		l.Append(1, bytes.Repeat([]byte("q"), 10))
	}
	oldest := l.Oldest()
	if oldest == 0 {
		t.Fatal("expected eviction")
	}
	_, err := l.Read(0, 0)
	var ev *ErrEvicted
	if !errors.As(err, &ev) || ev.Oldest != oldest {
		t.Fatalf("%v", err)
	}
	c := l.CursorAt(0)
	if _, err := c.Next(context.Background(), 0); !errors.As(err, &ev) {
		t.Fatal(err)
	}
	c.Skip(ev.Oldest)
	got, err := c.Next(context.Background(), 0)
	if err != nil || got[0].Seq != oldest {
		t.Fatalf("%v %+v", err, got)
	}
}

func TestLogSpillRotation(t *testing.T) {
	dir := t.TempDir()
	// Spill holds ~ (13+10)*4 = 92 bytes -> rotation after ~4 evicted chunks.
	l, _ := NewLog(LogOptions{MemBytes: 20, MaxChunk: 10, SpillBytes: 100, SpillPath: filepath.Join(dir, "spill")})
	for i := 0; i < 20; i++ {
		l.Append(1, bytes.Repeat([]byte("r"), 10))
	}
	oldest := l.Oldest()
	if oldest == 0 || oldest >= 18 {
		t.Fatalf("oldest=%d", oldest)
	}
	chunks, err := l.Read(oldest, 0)
	if err != nil {
		t.Fatal(err)
	}
	for i := 1; i < len(chunks); i++ {
		if chunks[i].Seq != chunks[i-1].Seq+1 {
			t.Fatal("gap after rotation")
		}
	}
	if chunks[len(chunks)-1].Seq != 19 {
		t.Fatal(chunks[len(chunks)-1].Seq)
	}
	l.Release()
}

func TestLogEvictsByChunkCount(t *testing.T) {
	dir := t.TempDir()
	l, _ := NewLog(LogOptions{MemBytes: 1 << 20, MaxChunks: 8, SpillBytes: 1 << 20, SpillPath: filepath.Join(dir, "spill")})
	t.Cleanup(func() { _ = l.Release() })
	for i := 0; i < 100; i++ {
		l.Append(1, []byte{byte(i)})
	}
	l.mu.Lock()
	n := len(l.chunks)
	l.mu.Unlock()
	if n > 8 {
		t.Fatalf("ring holds %d chunks", n)
	}
	chunks, err := l.Read(0, 0)
	if err != nil || len(chunks) != 100 {
		t.Fatalf("%v %d", err, len(chunks))
	}
	for i, c := range chunks {
		if c.Seq != uint64(i) || c.Data[0] != byte(i) {
			t.Fatalf("chunk %d: %+v", i, c)
		}
	}
}

func TestSpillFailurePreservesContiguousMemoryAndReturnsError(t *testing.T) {
	l, err := NewLog(LogOptions{
		MemBytes: 20, MaxChunk: 10, SpillBytes: 1 << 20,
		SpillPath: filepath.Join(t.TempDir(), "spill"),
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = l.Release() })
	for i := 0; i < 3; i++ {
		if _, err := l.Append(1, bytes.Repeat([]byte{byte(i)}, 10)); err != nil {
			t.Fatal(err)
		}
	}
	if err := l.spill.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := l.Append(1, bytes.Repeat([]byte{3}, 10)); err == nil {
		t.Fatal("spill write failure was hidden")
	}
	oldest := l.Oldest()
	if oldest != 1 {
		t.Fatalf("oldest=%d, want 1", oldest)
	}
	if _, err := l.Read(0, 0); err == nil {
		t.Fatal("lost spill history was not reported as evicted")
	}
	chunks, err := l.Read(oldest, 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(chunks) != 3 {
		t.Fatalf("got %d retained chunks", len(chunks))
	}
	for i, chunk := range chunks {
		if chunk.Seq != oldest+uint64(i) {
			t.Fatalf("gap at index %d: seq %d", i, chunk.Seq)
		}
	}
}

func TestReadSpillHonorsBatchLimit(t *testing.T) {
	l, err := NewLog(LogOptions{
		MemBytes: 10, MaxChunk: 1, SpillBytes: 1 << 20,
		SpillPath: filepath.Join(t.TempDir(), "spill"),
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = l.closeLocal() })
	for i := 0; i < 100; i++ {
		if _, err := l.Append(1, []byte{byte(i)}); err != nil {
			t.Fatal(err)
		}
	}
	l.mu.Lock()
	chunks, err := l.readSpillLocked(0, 7)
	l.mu.Unlock()
	if err != nil {
		t.Fatal(err)
	}
	if len(chunks) != 7 || chunks[0].Seq != 0 || chunks[6].Seq != 6 {
		t.Fatalf("unexpected batch: %+v", chunks)
	}
}

type switchBlobStore struct {
	artifact.BlobStore
	mu      sync.Mutex
	putErr  error
	openErr error
}

type memoryBlobStore struct {
	mu   sync.Mutex
	data map[string][]byte
}

func newMemoryBlobStore() *memoryBlobStore { return &memoryBlobStore{data: map[string][]byte{}} }

func (s *memoryBlobStore) Put(r io.Reader) (string, int64, error) {
	data, err := io.ReadAll(r)
	if err != nil {
		return "", 0, err
	}
	sum := sha256.Sum256(data)
	id := artifact.ID(sum[:])
	s.mu.Lock()
	s.data[id] = append([]byte(nil), data...)
	s.mu.Unlock()
	return id, int64(len(data)), nil
}

func (s *memoryBlobStore) Open(id string) (io.ReadCloser, int64, error) {
	s.mu.Lock()
	data, ok := s.data[id]
	s.mu.Unlock()
	if !ok {
		return nil, 0, os.ErrNotExist
	}
	copyData := append([]byte(nil), data...)
	return io.NopCloser(bytes.NewReader(copyData)), int64(len(copyData)), nil
}

func (s *memoryBlobStore) Head(id string) (int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	data, ok := s.data[id]
	if !ok {
		return 0, os.ErrNotExist
	}
	return int64(len(data)), nil
}

func (s *memoryBlobStore) Delete(id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.data[id]; !ok {
		return os.ErrNotExist
	}
	delete(s.data, id)
	return nil
}

func (s *memoryBlobStore) List() ([]string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	ids := make([]string, 0, len(s.data))
	for id := range s.data {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	return ids, nil
}

func (s *switchBlobStore) Put(r io.Reader) (string, int64, error) {
	s.mu.Lock()
	err := s.putErr
	s.mu.Unlock()
	if err != nil {
		return "", 0, err
	}
	return s.BlobStore.Put(r)
}

func (s *switchBlobStore) Open(id string) (io.ReadCloser, int64, error) {
	s.mu.Lock()
	err := s.openErr
	s.mu.Unlock()
	if err != nil {
		return nil, 0, err
	}
	return s.BlobStore.Open(id)
}

func newTestBlobStore(t *testing.T) artifact.BlobStore {
	t.Helper()
	store, err := artifact.NewStore(filepath.Join(t.TempDir(), "artifacts"))
	if err != nil {
		t.Fatal(err)
	}
	return store
}

func TestTieredLogSealsOnlyAfterDurableRecordCommitAndReopens(t *testing.T) {
	store := newTestBlobStore(t)
	var mu sync.Mutex
	var durable LogRecord
	commit := func(record LogRecord) error {
		mu.Lock()
		defer mu.Unlock()
		durable = LogRecord{Version: record.Version, MaxChunk: record.MaxChunk, Segments: append([]SegmentRef(nil), record.Segments...)}
		return nil
	}
	l, err := NewLog(LogOptions{
		MemBytes: 32, SpillBytes: 256, SpillPath: filepath.Join(t.TempDir(), "session.log"),
		MaxChunk: 32, MaxChunks: 4, BlobStore: store, SegmentBytes: 90, CommitRecord: commit,
	})
	if err != nil {
		t.Fatal(err)
	}
	var want []Chunk
	for i := 0; i < 40; i++ {
		data := bytes.Repeat([]byte{byte(i)}, 32)
		if _, err := l.Append(1, data); err != nil {
			t.Fatal(err)
		}
		want = append(want, Chunk{Seq: uint64(i), Stream: 1, Data: data})
	}
	if err := l.Close(); err != nil {
		t.Fatal(err)
	}
	stats := l.Stats()
	if stats.MemoryBytes != 0 || stats.DiskBytes != 0 || stats.BlobSegments < 2 || !stats.Closed {
		t.Fatalf("unexpected tier stats: %+v", stats)
	}
	for _, segment := range l.SegmentRefs() {
		if segment.Bytes > 90 {
			t.Fatalf("segment exceeded seal size: %+v", segment)
		}
	}
	mu.Lock()
	record := durable
	mu.Unlock()
	if len(record.Segments) != stats.BlobSegments {
		t.Fatalf("durable record has %d segments, log has %d", len(record.Segments), stats.BlobSegments)
	}
	reopened, err := OpenArchivedLog(store, record)
	if err != nil {
		t.Fatal(err)
	}
	for from := range want {
		got, err := reopened.Read(uint64(from), 1)
		if err != nil {
			t.Fatalf("read from %d: %v", from, err)
		}
		if len(got) != 1 || got[0].Seq != uint64(from) || !bytes.Equal(got[0].Data, want[from].Data) {
			t.Fatalf("read from %d = %+v", from, got)
		}
	}
	if err := l.Forget(); err != nil {
		t.Fatal(err)
	}
	mu.Lock()
	released := durable
	mu.Unlock()
	if len(released.Segments) != 0 {
		t.Fatalf("retention did not release durable refs: %+v", released)
	}
}

func TestTieredLogRecordFailureRetainsContiguousLocalCopy(t *testing.T) {
	store := newTestBlobStore(t)
	commitErr := errors.New("record database unavailable")
	l, err := NewLog(LogOptions{
		MemBytes: 16, SpillBytes: 256, SpillPath: filepath.Join(t.TempDir(), "session.log"),
		MaxChunk: 16, MaxChunks: 4, BlobStore: store, SegmentBytes: 58,
		CommitRecord: func(LogRecord) error { return commitErr },
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = l.closeLocal() })
	for i := 0; i < 2; i++ {
		if _, err := l.Append(1, bytes.Repeat([]byte{byte(i)}, 16)); err != nil {
			t.Fatal(err)
		}
	}
	_, err = l.Append(1, bytes.Repeat([]byte{2}, 16))
	var unavailable *ErrTierUnavailable
	if !errors.As(err, &unavailable) || unavailable.Tier != TierBlob || !errors.Is(unavailable.Cause, commitErr) {
		t.Fatalf("append error = %#v", err)
	}
	if len(l.SegmentRefs()) != 0 {
		t.Fatal("uncommitted segment became authoritative")
	}
	before := l.Next()
	if _, retryErr := l.Append(1, []byte("must-not-grow")); retryErr == nil || l.Next() != before {
		t.Fatalf("producer continued after storage failure: err=%v next=%d want=%d", retryErr, l.Next(), before)
	}
	got, err := l.Read(0, 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 3 {
		t.Fatalf("retained chunks = %d, want 3", len(got))
	}
	for i, chunk := range got {
		if chunk.Seq != uint64(i) || !bytes.Equal(chunk.Data, bytes.Repeat([]byte{byte(i)}, 16)) {
			t.Fatalf("chunk %d = %+v", i, chunk)
		}
	}
}

func TestTieredLogNamesUnavailableBlobRange(t *testing.T) {
	base := newTestBlobStore(t)
	store := &switchBlobStore{BlobStore: base}
	var record LogRecord
	l, err := NewLog(LogOptions{
		MemBytes: 8, SpillBytes: 128, SpillPath: filepath.Join(t.TempDir(), "session.log"),
		MaxChunk: 8, MaxChunks: 2, BlobStore: store, SegmentBytes: 42,
		CommitRecord: func(next LogRecord) error { record = next; return nil },
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = l.closeLocal() })
	for i := 0; i < 6; i++ {
		if _, err := l.Append(1, bytes.Repeat([]byte{byte(i)}, 8)); err != nil {
			t.Fatal(err)
		}
	}
	if err := l.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := OpenArchivedLog(store, record)
	if err != nil {
		t.Fatal(err)
	}
	store.mu.Lock()
	store.openErr = os.ErrNotExist
	store.mu.Unlock()
	_, err = reopened.Read(0, 1)
	var unavailable *ErrTierUnavailable
	var evicted *ErrEvicted
	if !errors.As(err, &unavailable) || unavailable.Tier != TierBlob || unavailable.Requested != 0 || unavailable.Oldest != record.Segments[0].Next {
		t.Fatalf("unavailable = %#v", err)
	}
	if !errors.As(err, &evicted) || evicted.Oldest != unavailable.Oldest {
		t.Fatalf("compatibility eviction missing: %v", err)
	}
}

func TestE11FastProducerReplaysEverySequenceAcrossTiers(t *testing.T) {
	store := newMemoryBlobStore()
	var record LogRecord
	l, err := NewLog(LogOptions{
		MemBytes: 256, SpillBytes: 4096, SpillPath: filepath.Join(t.TempDir(), "session.log"),
		MaxChunk: 64, MaxChunks: 8, BlobStore: store, SegmentBytes: 1001,
		CommitRecord: func(next LogRecord) error { record = next; return nil },
	})
	if err != nil {
		t.Fatal(err)
	}
	// E11's six hours/2 GiB are a duration and volume, not a distinct code
	// path. Small segment thresholds force the same transitions quickly.
	const chunks = 512
	for i := 0; i < chunks; i++ {
		data := bytes.Repeat([]byte{byte(i), byte(i >> 8)}, 32)
		if _, err := l.Append(1, data); err != nil {
			t.Fatalf("append %d: %v", i, err)
		}
	}
	if err := l.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := OpenArchivedLog(store, record)
	if err != nil {
		t.Fatal(err)
	}
	for from := 0; from < chunks; from++ {
		got, err := reopened.Read(uint64(from), 1)
		if err != nil {
			t.Fatalf("reattach from %d: %v", from, err)
		}
		want := bytes.Repeat([]byte{byte(from), byte(from >> 8)}, 32)
		if len(got) != 1 || got[0].Seq != uint64(from) || !bytes.Equal(got[0].Data, want) {
			t.Fatalf("reattach from %d returned %+v", from, got)
		}
	}
}

func TestTieredLogConcurrentAppendAndRemoteReplay(t *testing.T) {
	store := newMemoryBlobStore()
	var recordMu sync.Mutex
	var record LogRecord
	l, err := NewLog(LogOptions{
		MemBytes: 64, SpillBytes: 1024, SpillPath: filepath.Join(t.TempDir(), "session.log"),
		MaxChunk: 16, MaxChunks: 8, BlobStore: store, SegmentBytes: 116,
		CommitRecord: func(next LogRecord) error {
			recordMu.Lock()
			record = next
			recordMu.Unlock()
			return nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = l.closeLocal() })
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() {
		cursor := l.CursorAt(0)
		for expected := uint64(0); expected < 250; {
			batch, err := cursor.Next(ctx, 7)
			if err != nil {
				done <- err
				return
			}
			for _, chunk := range batch {
				if chunk.Seq != expected {
					done <- errors.New("non-contiguous concurrent replay")
					return
				}
				expected++
			}
		}
		done <- nil
	}()
	for i := 0; i < 250; i++ {
		if _, err := l.Append(1, bytes.Repeat([]byte{byte(i)}, 16)); err != nil {
			t.Fatal(err)
		}
	}
	if err := l.Close(); err != nil {
		t.Fatal(err)
	}
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	recordMu.Lock()
	segments := len(record.Segments)
	recordMu.Unlock()
	if segments == 0 {
		t.Fatal("concurrent producer created no remote segments")
	}
}
