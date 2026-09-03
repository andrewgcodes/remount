package artifact

import (
	"archive/tar"
	"bytes"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
)

func TestApplyOverlayReplacesOnlyNamedEntries(t *testing.T) {
	root := t.TempDir()
	must := func(err error) {
		t.Helper()
		if err != nil {
			t.Fatal(err)
		}
	}
	must(os.MkdirAll(filepath.Join(root, ".remount"), 0o755))
	must(os.WriteFile(filepath.Join(root, ".remount", "env"), []byte("REMOUNT_BROKER=x"), 0o600))
	must(os.WriteFile(filepath.Join(root, "keep.txt"), []byte("untouched"), 0o644))
	must(os.WriteFile(filepath.Join(root, "replace.txt"), []byte("old"), 0o644))
	must(os.MkdirAll(filepath.Join(root, "dir"), 0o755))
	must(os.WriteFile(filepath.Join(root, "dir", "old.txt"), []byte("stays"), 0o644))

	archive := buildTar(t, map[string]string{
		"replace.txt":  "new",
		"dir/new.txt":  "added",
		"deep/a/b.txt": "deep",
	})
	res, err := ApplyOverlay(root, bytes.NewReader(archive), RestoreLimits{})
	must(err)
	if !slices.Equal(res.Paths, []string{"deep/a/b.txt", "dir/new.txt", "replace.txt"}) {
		t.Fatalf("paths %q", res.Paths)
	}
	if res.Bytes != int64(len("new")+len("added")+len("deep")) {
		t.Fatalf("bytes %d", res.Bytes)
	}
	for rel, want := range map[string]string{
		"keep.txt": "untouched", "replace.txt": "new", "dir/old.txt": "stays",
		"dir/new.txt": "added", "deep/a/b.txt": "deep", ".remount/env": "REMOUNT_BROKER=x",
	} {
		got, err := os.ReadFile(filepath.Join(root, filepath.FromSlash(rel)))
		if err != nil || string(got) != want {
			t.Fatalf("%s = %q (%v), want %q", rel, got, err, want)
		}
	}
	entries, err := os.ReadDir(filepath.Join(root, ".remount"))
	must(err)
	if len(entries) != 1 {
		t.Fatalf("staging directory left behind: %v", entries)
	}
}

func TestApplyOverlayRefusesRemountDirAndHostileArchives(t *testing.T) {
	root := t.TempDir()
	if err := os.WriteFile(filepath.Join(root, "safe.txt"), []byte("safe"), 0o644); err != nil {
		t.Fatal(err)
	}
	env := buildTar(t, map[string]string{".remount/env": "REMOUNT_BROKER=attacker"})
	if _, err := ApplyOverlay(root, bytes.NewReader(env), RestoreLimits{}); err == nil || !strings.Contains(err.Error(), ".remount") {
		t.Fatalf("overlay into .remount accepted: %v", err)
	}
	if _, err := os.Stat(filepath.Join(root, ".remount", "env")); !os.IsNotExist(err) {
		t.Fatal(".remount/env was written")
	}
	escape := buildTar(t, map[string]string{"../evil": "bad"})
	if _, err := ApplyOverlay(root, bytes.NewReader(escape), RestoreLimits{}); err == nil {
		t.Fatal("escaping entry accepted")
	}
	if _, err := os.Stat(filepath.Join(filepath.Dir(root), "evil")); err == nil {
		t.Fatal("evil file written")
	}
	// A truncated archive changes nothing: validation completes before any
	// rename.
	good := buildTar(t, map[string]string{"safe.txt": "clobbered", "other.txt": "x"})
	if _, err := ApplyOverlay(root, bytes.NewReader(good[:len(good)/2]), RestoreLimits{}); err == nil {
		t.Fatal("truncated archive accepted")
	}
	if got, _ := os.ReadFile(filepath.Join(root, "safe.txt")); string(got) != "safe" {
		t.Fatalf("safe.txt = %q after failed overlay", got)
	}
	if _, err := os.Stat(filepath.Join(root, "other.txt")); !os.IsNotExist(err) {
		t.Fatal("partial archive wrote a file")
	}
	// A refusal discovered on a late entry (sorted after valid ones) still
	// writes nothing: every destination is checked before the first rename.
	if err := os.MkdirAll(filepath.Join(root, "zdir"), 0o755); err != nil {
		t.Fatal(err)
	}
	late := buildTar(t, map[string]string{"aaa.txt": "first", "safe.txt": "clobbered", "zdir": "now a file"})
	if _, err := ApplyOverlay(root, bytes.NewReader(late), RestoreLimits{}); err == nil || !strings.Contains(err.Error(), "replace a directory") {
		t.Fatalf("file over directory accepted: %v", err)
	}
	if got, _ := os.ReadFile(filepath.Join(root, "safe.txt")); string(got) != "safe" {
		t.Fatalf("safe.txt = %q after refused overlay", got)
	}
	if _, err := os.Stat(filepath.Join(root, "aaa.txt")); !os.IsNotExist(err) {
		t.Fatal("refused overlay wrote an earlier entry")
	}
	if err := os.WriteFile(filepath.Join(root, "afile"), []byte("f"), 0o644); err != nil {
		t.Fatal(err)
	}
	dirOverFile := buildTar(t, map[string]string{"afile/child.txt": "x", "zzz.txt": "y"})
	if _, err := ApplyOverlay(root, bytes.NewReader(dirOverFile), RestoreLimits{}); err == nil || !strings.Contains(err.Error(), "replace a file with a directory") {
		t.Fatalf("directory over file accepted: %v", err)
	}
	if _, err := os.Stat(filepath.Join(root, "zzz.txt")); !os.IsNotExist(err) {
		t.Fatal("refused overlay wrote a sibling entry")
	}
}

func TestApplyOverlayRefusesWritingThroughSymlinkedDirectory(t *testing.T) {
	parent := t.TempDir()
	root := filepath.Join(parent, "ws")
	outside := filepath.Join(parent, "outside")
	for _, d := range []string{root, outside} {
		if err := os.MkdirAll(d, 0o755); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.Symlink(outside, filepath.Join(root, "link")); err != nil {
		t.Fatal(err)
	}
	archive := buildTarEntries(t,
		testTarEntry{header: tar.Header{Name: "link/", Typeflag: tar.TypeDir, Mode: 0o755}},
		testTarEntry{header: tar.Header{Name: "link/victim", Typeflag: tar.TypeReg, Mode: 0o644}, body: "PWNED"},
	)
	if _, err := ApplyOverlay(root, bytes.NewReader(archive), RestoreLimits{}); err == nil {
		t.Fatal("write through symlinked directory accepted")
	}
	if _, err := os.Stat(filepath.Join(outside, "victim")); !os.IsNotExist(err) {
		t.Fatalf("archive wrote outside root: %v", err)
	}
	// A file that would replace a directory is refused by name.
	if err := os.MkdirAll(filepath.Join(root, "isdir"), 0o755); err != nil {
		t.Fatal(err)
	}
	clash := buildTar(t, map[string]string{"isdir": "now a file"})
	if _, err := ApplyOverlay(root, bytes.NewReader(clash), RestoreLimits{}); err == nil || !strings.Contains(err.Error(), "directory") {
		t.Fatalf("file over directory accepted: %v", err)
	}
}
