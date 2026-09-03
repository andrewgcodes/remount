package session

import "testing"

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
