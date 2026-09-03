package chunked

import (
	"bytes"
	"testing"
)

func FuzzManifestDecoder(f *testing.F) {
	seed := Manifest{
		Version: manifestVersion,
		Chunker: ChunkSpec{Algorithm: "fastcdc-v1", Min: MinChunkSize, Average: AvgChunkSize, Max: MaxChunkSize},
	}
	body, err := canonicalManifest(seed, normalizedLimits(Limits{MaxManifestBytes: 1 << 20}))
	if err != nil {
		f.Fatal(err)
	}
	f.Add(body)
	f.Add([]byte(`{"version":1}`))
	f.Fuzz(func(t *testing.T, input []byte) {
		if len(input) > 1<<20 {
			t.Skip()
		}
		id := digestID(input)
		_, _, _ = decodeManifest(bytes.NewReader(input), id, normalizedLimits(Limits{MaxManifestBytes: 1 << 20, MaxEntries: 1000, MaxChunks: 1000}))
	})
}
