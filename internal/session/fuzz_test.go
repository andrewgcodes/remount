package session

import (
	"bytes"
	"testing"
)

func FuzzLogCursorRanges(f *testing.F) {
	f.Add(uint64(0), int(0))
	f.Add(uint64(1), int(1))
	f.Add(^uint64(0), int(-1))
	f.Fuzz(func(t *testing.T, from uint64, max int) {
		log, err := NewLog(LogOptions{MemBytes: 128, MaxChunk: 32, MaxChunks: 8})
		if err != nil {
			t.Fatal(err)
		}
		defer log.Release()
		for i := 0; i < 16; i++ {
			if _, err := log.Append(1, []byte("0123456789")); err != nil {
				t.Fatal(err)
			}
		}
		// Keep hostile integers bounded to the API's meaningful domain while
		// retaining zero, negative, and overflow-adjacent cursor cases.
		if max > 1024 {
			max = 1024
		}
		if max < -1024 {
			max = -1024
		}
		chunks, _ := log.Read(from, max)
		for i := 1; i < len(chunks); i++ {
			if chunks[i].Seq != chunks[i-1].Seq+1 {
				t.Fatalf("non-contiguous result: %+v", chunks)
			}
		}
	})
}

func FuzzArchivedLogSegments(f *testing.F) {
	f.Add([]byte{})
	f.Add([]byte("not a segment"))
	f.Add([]byte{0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0})
	f.Fuzz(func(t *testing.T, encoded []byte) {
		if len(encoded) > 4096 {
			encoded = encoded[:4096]
		}
		if len(encoded) == 0 {
			encoded = []byte{0}
		}
		store := newMemoryBlobStore()
		id, size, err := store.Put(bytes.NewReader(encoded))
		if err != nil {
			t.Fatal(err)
		}
		// The content is arbitrary but the outer durable record is valid. The
		// decoder must reject corrupt lengths/sequences without panicking or
		// returning non-contiguous chunks.
		record := LogRecord{Version: logRecordVersion, MaxChunk: 32, Segments: []SegmentRef{{
			First: 0, Next: 1, Artifact: id, Bytes: size,
		}}}
		log, err := OpenArchivedLog(store, record)
		if err != nil {
			t.Fatal(err)
		}
		chunks, _ := log.Read(0, 8)
		for i, chunk := range chunks {
			if chunk.Seq != uint64(i) {
				t.Fatalf("non-contiguous decoded chunk: %+v", chunks)
			}
		}
	})
}
