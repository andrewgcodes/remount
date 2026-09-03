package encrypted

import (
	"bytes"
	"context"
	"io"
	"testing"
)

func FuzzEncryptedObjectFailsClosed(f *testing.F) {
	objects := newMemoryObjects()
	keys := newRotatingKeys()
	store, err := NewStore(objects, keys, f.TempDir(), Options{ChunkSize: 32, MaxPlaintextBytes: 1 << 20, MaxStagingBytes: 1 << 20})
	if err != nil {
		f.Fatal(err)
	}
	id, _, err := store.Put(context.Background(), "tenant", bytes.NewReader([]byte("seed plaintext")))
	if err != nil {
		f.Fatal(err)
	}
	physical, _ := ObjectKey("tenant", "v1", id)
	seed := append([]byte(nil), objects.objects[physical]...)
	f.Add(seed)
	f.Add([]byte("not an encrypted artifact"))

	f.Fuzz(func(t *testing.T, input []byte) {
		if len(input) > 1<<20 {
			t.Skip()
		}
		local := newMemoryObjects()
		local.objects[physical] = append([]byte(nil), input...)
		candidate, err := NewStore(local, keys, t.TempDir(), Options{ChunkSize: 32, MaxPlaintextBytes: 1 << 20, MaxStagingBytes: 1 << 20})
		if err != nil {
			t.Fatal(err)
		}
		r, _, err := candidate.Open(context.Background(), "tenant", id)
		if err != nil {
			return
		}
		_, _ = io.Copy(io.Discard, r)
		_ = r.Close()
	})
}
