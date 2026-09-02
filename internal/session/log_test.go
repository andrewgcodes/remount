package session

import (
	"bytes"
	"context"
	"errors"
	"io"
	"path/filepath"
	"testing"
	"time"
)

func TestLogAppendReadCursor(t *testing.T) {
	l, err := NewLog(LogOptions{MemBytes: 1 << 20})
	if err != nil {
		t.Fatal(err)
	}
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
