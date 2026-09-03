package ids

import (
	"strings"
	"testing"
)

func FuzzPrefix(f *testing.F) {
	for _, seed := range []string{"ws_01", "", "no-separator", "_suffix", "a_b_c", "a\x00_b"} {
		f.Add(seed)
	}
	f.Fuzz(func(t *testing.T, id string) {
		if len(id) > 1<<20 {
			t.Skip()
		}
		prefix := Prefix(id)
		index := strings.IndexByte(id, '_')
		if index < 0 && prefix != "" {
			t.Fatalf("Prefix(%q) = %q without separator", id, prefix)
		}
		if index >= 0 && prefix != id[:index] {
			t.Fatalf("Prefix(%q) = %q", id, prefix)
		}
	})
}
