package fsops

import (
	"path/filepath"
	"strings"
	"testing"
)

func FuzzResolve(f *testing.F) {
	filesystem, err := New(f.TempDir())
	if err != nil {
		f.Fatal(err)
	}
	defer filesystem.Close()
	for _, seed := range []string{"", ".", "a/b", "../escape", "/absolute", "a\x00b", "a/../../b"} {
		f.Add(seed)
	}
	f.Fuzz(func(t *testing.T, path string) {
		if len(path) > 8192 {
			t.Skip()
		}
		resolved, err := filesystem.Resolve(path)
		if err == nil {
			relative, relErr := filepath.Rel(filesystem.Root(), resolved)
			if relErr != nil || relative == ".." || strings.HasPrefix(relative, ".."+string(filepath.Separator)) {
				t.Fatalf("Resolve(%q) escaped root: %q", path, resolved)
			}
		}
	})
}
