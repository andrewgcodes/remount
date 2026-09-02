package artifact

import (
	"archive/tar"
	"bytes"
	"crypto/sha256"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func digestFor(body string) string {
	sum := sha256.Sum256([]byte(body))
	return ID(sum[:])
}

func TestStorePutOpenVerify(t *testing.T) {
	s, err := NewStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	id, n, err := s.Put(strings.NewReader("hello"))
	if err != nil || n != 5 {
		t.Fatal(err, n)
	}
	if !strings.HasPrefix(id, Prefix) || !s.Has(id) {
		t.Fatal(id)
	}
	id2, _, _ := s.Put(strings.NewReader("hello"))
	if id2 != id {
		t.Fatal("content addressing broken")
	}
	r, size, err := s.Open(id)
	if err != nil || size != 5 {
		t.Fatal(err, size)
	}
	b, _ := readAll(r)
	r.Close()
	if string(b) != "hello" {
		t.Fatal(string(b))
	}
	if err := s.Verify(id); err != nil {
		t.Fatal(err)
	}
	ids, _ := s.List()
	if len(ids) != 1 || ids[0] != id {
		t.Fatal(ids)
	}
	if _, err := Digest("art_sha256:zz"); err == nil {
		t.Fatal("bad digest accepted")
	}
	if _, _, err := s.Open("art_sha256:" + strings.Repeat("0", 64)); err == nil {
		t.Fatal("missing blob opened")
	}
	if err := s.Delete(id); err != nil || s.Has(id) {
		t.Fatal("delete")
	}
}

func TestStorePutExpectedNeverPublishesOrDeletesMismatch(t *testing.T) {
	s, err := NewStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	body := "already-valid-body"
	bodyID, _, err := s.Put(strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	wanted := digestFor("different expected bytes")
	n, err := s.PutExpected(wanted, strings.NewReader(body), 1<<20)
	if !errors.Is(err, ErrDigestMismatch) || n != int64(len(body)) {
		t.Fatalf("PutExpected mismatch = (%d, %v)", n, err)
	}
	if !s.Has(bodyID) {
		t.Fatal("mismatched upload deleted an existing valid blob")
	}
	if s.Has(wanted) {
		t.Fatal("mismatched upload was published under the requested id")
	}
	ids, err := s.List()
	if err != nil || len(ids) != 1 || ids[0] != bodyID {
		t.Fatalf("store contents after mismatch = %v, %v", ids, err)
	}
}

func TestStorePutLimitRejectsBeforePublication(t *testing.T) {
	s, err := NewStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	id, n, err := s.PutLimit(strings.NewReader("12345"), 4)
	if !errors.Is(err, ErrTooLarge) || id != "" || n != 5 {
		t.Fatalf("PutLimit = (%q, %d, %v)", id, n, err)
	}
	ids, listErr := s.List()
	if listErr != nil || len(ids) != 0 {
		t.Fatalf("oversized input was published: %v, %v", ids, listErr)
	}
}

func readAll(r interface{ Read([]byte) (int, error) }) ([]byte, error) {
	var buf bytes.Buffer
	tmp := make([]byte, 1024)
	for {
		n, err := r.Read(tmp)
		buf.Write(tmp[:n])
		if err != nil {
			if err.Error() == "EOF" {
				return buf.Bytes(), nil
			}
			return buf.Bytes(), err
		}
	}
}

func TestSnapshotRestoreRoundTrip(t *testing.T) {
	src := t.TempDir()
	os.MkdirAll(filepath.Join(src, "src", "pkg"), 0o755)
	os.MkdirAll(filepath.Join(src, "node_modules", "x"), 0o755)
	os.MkdirAll(filepath.Join(src, "build"), 0o755)
	os.WriteFile(filepath.Join(src, "src", "main.go"), []byte("package main"), 0o644)
	os.WriteFile(filepath.Join(src, "src", "pkg", "run.sh"), []byte("#!/bin/sh"), 0o755)
	os.WriteFile(filepath.Join(src, "node_modules", "x", "i.js"), []byte("junk"), 0o644)
	os.WriteFile(filepath.Join(src, "build", "a.o"), []byte("obj"), 0o644)
	os.WriteFile(filepath.Join(src, "build", "keep.txt"), []byte("keep"), 0o644)
	os.Symlink("src/main.go", filepath.Join(src, "link"))
	mt := time.Date(2024, 1, 2, 3, 4, 5, 0, time.UTC)
	os.Chtimes(filepath.Join(src, "src", "main.go"), mt, mt)

	store, _ := NewStore(t.TempDir())
	id, n, err := SnapshotToStore(store, src, []string{"node_modules", "build/*.o"})
	if err != nil || n == 0 {
		t.Fatal(err)
	}
	// Deterministic: same tree, same id.
	id2, _, _ := SnapshotToStore(store, src, []string{"node_modules", "build/*.o"})
	if id != id2 {
		t.Fatal("snapshot not deterministic")
	}

	dst := t.TempDir()
	if err := RestoreFromStore(store, id, dst); err != nil {
		t.Fatal(err)
	}
	b, err := os.ReadFile(filepath.Join(dst, "src", "main.go"))
	if err != nil || string(b) != "package main" {
		t.Fatal(err)
	}
	st, _ := os.Stat(filepath.Join(dst, "src", "pkg", "run.sh"))
	if st.Mode().Perm() != 0o755 {
		t.Fatalf("mode %o", st.Mode().Perm())
	}
	st, _ = os.Stat(filepath.Join(dst, "src", "main.go"))
	if !st.ModTime().Equal(mt) {
		t.Fatalf("mtime %v", st.ModTime())
	}
	if _, err := os.Stat(filepath.Join(dst, "node_modules")); !os.IsNotExist(err) {
		t.Fatal("node_modules not excluded")
	}
	if _, err := os.Stat(filepath.Join(dst, "build", "a.o")); !os.IsNotExist(err) {
		t.Fatal("build/*.o not excluded")
	}
	if _, err := os.Stat(filepath.Join(dst, "build", "keep.txt")); err != nil {
		t.Fatal("build/keep.txt missing")
	}
	target, err := os.Readlink(filepath.Join(dst, "link"))
	if err != nil || target != "src/main.go" {
		t.Fatal(err, target)
	}
}

func TestRestoreRejectsEscape(t *testing.T) {
	var buf bytes.Buffer
	// Hand-build a tar.gz with a "../evil" entry.
	src := t.TempDir()
	os.WriteFile(filepath.Join(src, "ok"), []byte("x"), 0o644)
	if err := Snapshot(src, nil, &buf); err != nil {
		t.Fatal(err)
	}
	// Tamper: a fresh archive with an escaping name.
	evil := buildTar(t, map[string]string{"../evil": "bad"})
	dst := t.TempDir()
	if err := Restore(dst, bytes.NewReader(evil)); err == nil {
		t.Fatal("escape accepted")
	}
	if _, err := os.Stat(filepath.Join(filepath.Dir(dst), "evil")); err == nil {
		t.Fatal("evil file written")
	}
}

func TestRestoreRejectsSymlinkWriteThrough(t *testing.T) {
	parent := t.TempDir()
	root := filepath.Join(parent, "workspace")
	outside := filepath.Join(parent, "outside")
	if err := os.MkdirAll(root, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(outside, 0o755); err != nil {
		t.Fatal(err)
	}
	archive := buildTarEntries(t,
		testTarEntry{header: tar.Header{Name: "escape", Typeflag: tar.TypeSymlink, Linkname: outside, Mode: 0o777}},
		testTarEntry{header: tar.Header{Name: "escape/victim", Typeflag: tar.TypeReg, Mode: 0o644}, body: "PWNED"},
	)
	if err := Restore(root, bytes.NewReader(archive)); err == nil {
		t.Fatal("absolute symlink target accepted")
	}
	if _, err := os.Stat(filepath.Join(outside, "victim")); !os.IsNotExist(err) {
		t.Fatalf("archive wrote outside root: %v", err)
	}
}

func TestRestoreRejectsRelativeSymlinkEscapeAndHardLinks(t *testing.T) {
	for name, archive := range map[string][]byte{
		"relative symlink": buildTarEntries(t,
			testTarEntry{header: tar.Header{Name: "dir/escape", Typeflag: tar.TypeSymlink, Linkname: "../../outside", Mode: 0o777}},
		),
		"hard link": buildTarEntries(t,
			testTarEntry{header: tar.Header{Name: "hard", Typeflag: tar.TypeLink, Linkname: "target", Mode: 0o644}},
		),
	} {
		t.Run(name, func(t *testing.T) {
			if err := Restore(t.TempDir(), bytes.NewReader(archive)); err == nil {
				t.Fatal("hostile entry accepted")
			}
		})
	}
}

func TestRestoreIsTransactionalAndBounded(t *testing.T) {
	root := t.TempDir()
	marker := filepath.Join(root, "keep")
	if err := os.WriteFile(marker, []byte("original"), 0o644); err != nil {
		t.Fatal(err)
	}
	archive := buildTarEntries(t,
		testTarEntry{header: tar.Header{Name: "one", Typeflag: tar.TypeReg, Mode: 0o644}, body: "1234"},
		testTarEntry{header: tar.Header{Name: "two", Typeflag: tar.TypeReg, Mode: 0o644}, body: "5678"},
	)
	limits := RestoreLimits{MaxExpandedBytes: 7, MaxEntries: 10, MaxPathBytes: 128, MaxDepth: 8}
	if err := RestoreWithLimits(root, bytes.NewReader(archive), limits); err == nil {
		t.Fatal("expanded byte limit was not enforced")
	}
	got, err := os.ReadFile(marker)
	if err != nil || string(got) != "original" {
		t.Fatalf("failed restore changed destination: %q, %v", got, err)
	}

	limits.MaxExpandedBytes = 100
	limits.MaxEntries = 1
	if err := RestoreWithLimits(root, bytes.NewReader(archive), limits); err == nil {
		t.Fatal("entry limit was not enforced")
	}
	if _, err := os.Stat(marker); err != nil {
		t.Fatalf("failed restore removed destination: %v", err)
	}
}

func TestExcluded(t *testing.T) {
	cases := []struct {
		rel  string
		pats []string
		want bool
	}{
		{"node_modules/a/b.js", []string{"node_modules"}, true},
		{"a/node_modules/b.js", []string{"node_modules"}, true},
		{"src/a.go", []string{"node_modules"}, false},
		{"build/x.o", []string{"build/*.o"}, true},
		{"build/sub/x.o", []string{"build/*.o"}, false},
		{".venv/lib", []string{".venv", "*.pyc"}, true},
		{"a/b.pyc", []string{".venv", "*.pyc"}, true},
		{"a/b.py", []string{".venv", "*.pyc"}, false},
	}
	for _, c := range cases {
		if got := Excluded(c.rel, c.pats); got != c.want {
			t.Errorf("%q %v: got %v", c.rel, c.pats, got)
		}
	}
}
