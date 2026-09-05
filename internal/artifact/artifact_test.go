package artifact

import (
	"archive/tar"
	"bytes"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
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

func TestStoreNeverTreatsCorruptedExistingBlobAsIdempotent(t *testing.T) {
	s, err := NewStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	body := "valid-body"
	id, _, err := s.Put(strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	digest, _ := Digest(id)
	path := s.pathFor(digest)
	if err := os.Chmod(path, 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte("corruptxxx"), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := s.PutExpected(id, strings.NewReader(body), int64(len(body))); !errors.Is(err, ErrDigestMismatch) {
		t.Fatalf("corrupt idempotent target = %v, want ErrDigestMismatch", err)
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

func TestStoreQuotaIsAtomicAndIdempotentAtCapacity(t *testing.T) {
	s, err := NewStoreWithOptions(t.TempDir(), StoreOptions{MaxBytes: 5, MaxObjects: 1})
	if err != nil {
		t.Fatal(err)
	}
	body := "12345"
	id := digestFor(body)
	if n, err := s.PutExpected(id, strings.NewReader(body), 5); err != nil || n != 5 {
		t.Fatalf("initial PutExpected = (%d, %v)", n, err)
	}
	// A retry of an existing digest must still work when retained storage is
	// exactly at quota; it verifies the entire supplied body without staging a
	// second disk copy.
	if n, err := s.PutExpected(id, strings.NewReader(body), 5); err != nil || n != 5 {
		t.Fatalf("idempotent PutExpected at capacity = (%d, %v)", n, err)
	}
	if _, _, err := s.Put(strings.NewReader("x")); !errors.Is(err, ErrStoreFull) {
		t.Fatalf("second object error = %v, want ErrStoreFull", err)
	}
	stats := s.Stats()
	if stats.Bytes != 5 || stats.Objects != 1 || stats.ReservedBytes != 0 || stats.ReservedObjects != 0 {
		t.Fatalf("stats after rejection = %+v", stats)
	}
	if err := s.Delete(id); err != nil {
		t.Fatal(err)
	}
	if _, _, err := s.Put(strings.NewReader("x")); err != nil {
		t.Fatalf("quota was not released after delete: %v", err)
	}
}

type gatedReader struct {
	first   []byte
	gate    <-chan struct{}
	once    sync.Once
	started chan<- struct{}
}

func (r *gatedReader) Read(p []byte) (int, error) {
	if len(r.first) > 0 {
		n := copy(p, r.first)
		r.first = r.first[n:]
		return n, nil
	}
	r.once.Do(func() { close(r.started) })
	<-r.gate
	return 0, io.EOF
}

func TestStoreQuotaCountsConcurrentStagingBytes(t *testing.T) {
	s, err := NewStoreWithOptions(t.TempDir(), StoreOptions{MaxBytes: 6, MaxObjects: 4})
	if err != nil {
		t.Fatal(err)
	}
	gate := make(chan struct{})
	started := make(chan struct{})
	result := make(chan error, 1)
	go func() {
		_, _, err := s.Put(&gatedReader{first: []byte("1234"), gate: gate, started: started})
		result <- err
	}()
	select {
	case <-started:
	case <-time.After(5 * time.Second):
		t.Fatal("first upload did not reach its staging barrier")
	}
	deadline := time.Now().Add(5 * time.Second)
	for s.Stats().ReservedBytes != 4 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if got := s.Stats().ReservedBytes; got != 4 {
		t.Fatalf("first upload reservation = %d, want 4", got)
	}
	if _, _, err := s.Put(strings.NewReader("abc")); !errors.Is(err, ErrStoreFull) {
		t.Fatalf("concurrent overcommit error = %v, want ErrStoreFull", err)
	}
	close(gate)
	if err := <-result; err != nil {
		t.Fatalf("first upload failed: %v", err)
	}
	stats := s.Stats()
	if stats.Bytes != 4 || stats.Objects != 1 || stats.ReservedBytes != 0 || stats.ReservedObjects != 0 {
		t.Fatalf("final stats = %+v", stats)
	}
}

func TestStoreCollectDoesNotRaceIdempotentUpload(t *testing.T) {
	s, err := NewStoreWithOptions(t.TempDir(), StoreOptions{MaxBytes: 5, MaxObjects: 1})
	if err != nil {
		t.Fatal(err)
	}
	body := "12345"
	id := digestFor(body)
	if _, err := s.PutExpected(id, strings.NewReader(body), int64(len(body))); err != nil {
		t.Fatal(err)
	}
	digest, _ := Digest(id)
	past := time.Now().Add(-2 * time.Hour)
	if err := os.Chtimes(s.pathFor(digest), past, past); err != nil {
		t.Fatal(err)
	}
	gate := make(chan struct{})
	started := make(chan struct{})
	result := make(chan error, 1)
	go func() {
		_, err := s.PutExpected(id, &gatedReader{first: []byte(body), gate: gate, started: started}, int64(len(body)))
		result <- err
	}()
	select {
	case <-started:
	case <-time.After(5 * time.Second):
		t.Fatal("idempotent upload did not reach its body barrier")
	}
	gc, err := s.Collect(nil, time.Now().Add(-time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	if gc.Removed != 0 || !s.Has(id) {
		t.Fatalf("GC removed pinned upload target: result=%+v present=%v", gc, s.Has(id))
	}
	close(gate)
	if err := <-result; err != nil {
		t.Fatalf("idempotent upload failed: %v", err)
	}
}

func TestStoreInventoryCleansCrashStagingAndRestoresUsage(t *testing.T) {
	dir := t.TempDir()
	s, err := NewStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	id, _, err := s.Put(strings.NewReader("persisted"))
	if err != nil {
		t.Fatal(err)
	}
	stale := filepath.Join(dir, ".put-crashed")
	if err := os.WriteFile(stale, []byte("partial"), 0o600); err != nil {
		t.Fatal(err)
	}
	reopened, err := NewStoreWithOptions(dir, StoreOptions{MaxBytes: 9, MaxObjects: 1})
	if err != nil {
		t.Fatal(err)
	}
	if stats := reopened.Stats(); stats.Bytes != 9 || stats.Objects != 1 {
		t.Fatalf("reopened stats = %+v", stats)
	}
	if _, err := os.Lstat(stale); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("crash staging file survived inventory: %v", err)
	}
	if !reopened.Has(id) {
		t.Fatal("inventory lost durable blob")
	}
}

func TestStoreInventoryRejectsUnaccountedFiles(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "unaccounted"), []byte("bytes outside quota"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := NewStoreWithOptions(dir, StoreOptions{MaxBytes: 1, MaxObjects: 1}); err == nil {
		t.Fatal("store accepted an unaccounted file that bypasses capacity tracking")
	}
}

func TestStoreCollectIsReferenceAwareGracefulAndResumable(t *testing.T) {
	dir := t.TempDir()
	s, err := NewStoreWithOptions(dir, StoreOptions{MaxBytes: 1 << 20, MaxObjects: 10})
	if err != nil {
		t.Fatal(err)
	}
	put := func(body string) string {
		t.Helper()
		id, _, err := s.Put(strings.NewReader(body))
		if err != nil {
			t.Fatal(err)
		}
		return id
	}
	keep := put("referenced")
	old := put("old orphan")
	young := put("young orphan")
	past := time.Now().Add(-2 * time.Hour)
	for _, id := range []string{keep, old} {
		digest, _ := Digest(id)
		if err := os.Chtimes(s.pathFor(digest), past, past); err != nil {
			t.Fatal(err)
		}
	}
	before := s.Stats()
	if _, err := s.Collect([]string{"not-an-artifact"}, time.Now()); err == nil {
		t.Fatal("GC accepted an invalid reference set")
	}
	if after := s.Stats(); after != before {
		t.Fatalf("invalid-reference GC mutated usage: before=%+v after=%+v", before, after)
	}
	result, err := s.Collect([]string{keep}, time.Now().Add(-time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	if result.Scanned != 3 || result.Referenced != 1 || result.GraceRetained != 1 || result.Removed != 1 || result.RemovedBytes != int64(len("old orphan")) {
		t.Fatalf("GC result = %+v", result)
	}
	if !s.Has(keep) || s.Has(old) || !s.Has(young) {
		t.Fatalf("GC retained wrong objects: keep=%v old=%v young=%v", s.Has(keep), s.Has(old), s.Has(young))
	}
	// A second pass is a no-op, which is the recovery behavior after a process
	// exits between individual unlinks.
	result, err = s.Collect([]string{keep}, time.Now().Add(-time.Hour))
	if err != nil || result.Removed != 0 {
		t.Fatalf("resumed GC = %+v, %v", result, err)
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
	if runtime.GOOS != "windows" && st.Mode().Perm() != 0o755 {
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
	if err != nil || filepath.Clean(target) != filepath.Clean("src/main.go") {
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
		"mixed-separator symlink": buildTarEntries(t,
			testTarEntry{header: tar.Header{Name: "escape", Typeflag: tar.TypeSymlink, Linkname: `safe\child/../..`, Mode: 0o777}},
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

func TestSnapshotRewritesInternalAbsoluteSymlink(t *testing.T) {
	src := t.TempDir()
	if err := os.MkdirAll(filepath.Join(src, ".npm", "bin"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Join(src, ".codex", "tmp", "arg0"), 0o755); err != nil {
		t.Fatal(err)
	}
	target := filepath.Join(src, ".npm", "bin", "tool")
	if err := os.WriteFile(target, []byte("portable"), 0o755); err != nil {
		t.Fatal(err)
	}
	linkPath := filepath.Join(src, ".codex", "tmp", "arg0", "apply_patch")
	if err := os.Symlink(target, linkPath); err != nil {
		if runtime.GOOS == "windows" {
			t.Skipf("symlink unavailable: %v", err)
		}
		t.Fatal(err)
	}
	for _, prefix := range []string{"", "workspace"} {
		t.Run(prefix, func(t *testing.T) {
			var archive bytes.Buffer
			if _, err := SnapshotTrees([]Tree{{Prefix: prefix, Root: src}}, &archive); err != nil {
				t.Fatal(err)
			}
			dst := t.TempDir()
			if err := Restore(dst, bytes.NewReader(archive.Bytes())); err != nil {
				t.Fatal(err)
			}
			restoredLink := filepath.Join(dst, prefix, ".codex", "tmp", "arg0", "apply_patch")
			link, err := os.Readlink(restoredLink)
			if err != nil {
				t.Fatal(err)
			}
			if filepath.IsAbs(link) {
				t.Fatalf("restored link is absolute: %q", link)
			}
			body, err := os.ReadFile(restoredLink)
			if err != nil || string(body) != "portable" {
				t.Fatalf("restored link body = %q, %v", body, err)
			}
		})
	}
}

func TestSnapshotRejectsExternalAbsoluteSymlink(t *testing.T) {
	src := t.TempDir()
	external := filepath.Join(t.TempDir(), "secret")
	if err := os.WriteFile(external, []byte("outside"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(external, filepath.Join(src, "escape")); err != nil {
		if runtime.GOOS == "windows" {
			t.Skipf("symlink unavailable: %v", err)
		}
		t.Fatal(err)
	}
	if err := Snapshot(src, nil, io.Discard); err == nil || !strings.Contains(err.Error(), "invalid target") {
		t.Fatalf("Snapshot external absolute symlink error = %v", err)
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

func TestRestoreRejectsEveryArchiveResourceBoundary(t *testing.T) {
	largeBody := strings.Repeat("a", 2<<20)
	largeArchive := buildTarEntries(t,
		testTarEntry{header: tar.Header{Name: "large", Typeflag: tar.TypeReg, Mode: 0o644}, body: largeBody},
	)
	tests := []struct {
		name    string
		archive []byte
		limits  RestoreLimits
		match   string
	}{
		{
			name: "compressed bytes", archive: largeArchive,
			limits: RestoreLimits{MaxCompressedBytes: int64(len(largeArchive) - 1)}, match: "compressed data exceeds limit",
		},
		{
			name: "single file", archive: buildTar(t, map[string]string{"five": "12345"}),
			limits: RestoreLimits{MaxFileBytes: 4}, match: "file \"five\" exceeds 4 bytes",
		},
		{
			name: "compression ratio", archive: largeArchive,
			limits: RestoreLimits{MaxCompressionRatio: 2, MaxExpandedBytes: 4 << 20, MaxFileBytes: 4 << 20}, match: "expansion ratio exceeds 2:1",
		},
		{
			name: "path length", archive: buildTar(t, map[string]string{"12345": "x"}),
			limits: RestoreLimits{MaxPathBytes: 4}, match: "path exceeds 4 bytes",
		},
		{
			name: "path depth", archive: buildTar(t, map[string]string{"a/b/c": "x"}),
			limits: RestoreLimits{MaxDepth: 2}, match: "path exceeds depth 2",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			root := t.TempDir()
			marker := filepath.Join(root, "marker")
			if err := os.WriteFile(marker, []byte("original"), 0o600); err != nil {
				t.Fatal(err)
			}
			err := RestoreWithLimits(root, bytes.NewReader(tc.archive), tc.limits)
			if err == nil || !strings.Contains(err.Error(), tc.match) {
				t.Fatalf("RestoreWithLimits error = %v, want substring %q", err, tc.match)
			}
			if got, err := os.ReadFile(marker); err != nil || string(got) != "original" {
				t.Fatalf("failed restore changed live tree: %q, %v", got, err)
			}
		})
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
		{"shared/data", []string{"/shared"}, true},
		{"src/shared/data", []string{"/shared"}, false},
		{"data[1]/file", []string{"/data[1]"}, true},
		{"data1/file", []string{"/data[1]"}, false},
	}
	for _, c := range cases {
		if got := Excluded(c.rel, c.pats); got != c.want {
			t.Errorf("%q %v: got %v", c.rel, c.pats, got)
		}
	}
}

func TestStoreHeadMatchesOpenAndIsNotExistForUnknown(t *testing.T) {
	var bs BlobStore
	s, err := NewStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	bs = s
	id, size, err := bs.Put(strings.NewReader("hello blob"))
	if err != nil {
		t.Fatal(err)
	}
	n, err := bs.Head(id)
	if err != nil || n != size {
		t.Fatalf("Head = %d, %v; want %d", n, err, size)
	}
	if _, err := bs.Head(digestFor("absent")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("Head of unknown id = %v; want ErrNotExist", err)
	}
	if _, err := bs.Head("not-a-digest"); err == nil {
		t.Fatal("Head must reject malformed ids")
	}
}

// TestConcurrentPublishersOfOneDigestCreateItExactlyOnce is the correctness
// property the store-wide lock used to provide and the per-digest lock has to
// keep providing.
//
// publish decides "did I create this object?" by looking at the filesystem and
// then updating the store's counters. If two goroutines uploading identical
// content can both observe "absent" and both count a creation, the store's
// object and byte totals drift upward forever, and a store with a quota
// eventually refuses writes it has room for. Content addressing makes the
// racing renames harmless — both write the same bytes to the same path — so
// the accounting is the only thing at risk, and the only thing this asserts.
func TestConcurrentPublishersOfOneDigestCreateItExactlyOnce(t *testing.T) {
	store, err := NewStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	payload := bytes.Repeat([]byte("same-content-every-time"), 512)

	const racers = 24
	var wg sync.WaitGroup
	created := make([]bool, racers)
	errs := make([]error, racers)
	start := make(chan struct{})
	for i := 0; i < racers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			// put reports creation through the store's counters, so compare
			// those rather than a return value the API does not expose.
			_, _, err := store.PutLimit(bytes.NewReader(payload), 0)
			errs[i] = err
			created[i] = err == nil
		}(i)
	}
	close(start)
	wg.Wait()

	for i, err := range errs {
		if err != nil {
			t.Fatalf("racer %d: %v", i, err)
		}
	}
	store.mu.Lock()
	objects, storedBytes, reservedObjects, reservedBytes := store.objects, store.bytes, store.reservedObjects, store.reservedBytes
	store.mu.Unlock()

	if objects != 1 {
		t.Errorf("%d concurrent uploads of one digest left the store counting %d objects; identical content is one object", racers, objects)
	}
	if storedBytes != int64(len(payload)) {
		t.Errorf("store counts %d bytes for one %d-byte object", storedBytes, len(payload))
	}
	// Every reservation must be settled, whether its publisher created the
	// object or found it already there. A leaked reservation is a quota the
	// store can never reclaim.
	if reservedObjects != 0 || reservedBytes != 0 {
		t.Errorf("reservations leaked: %d objects, %d bytes still reserved", reservedObjects, reservedBytes)
	}
}

// TestConcurrentPublishersOfDistinctDigestsAllLand covers the other side: the
// per-digest lock must not make two different objects exclude each other.
func TestConcurrentPublishersOfDistinctDigestsAllLand(t *testing.T) {
	store, err := NewStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	const racers = 24
	var wg sync.WaitGroup
	ids := make([]string, racers)
	errs := make([]error, racers)
	for i := 0; i < racers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			ids[i], _, errs[i] = store.PutLimit(bytes.NewReader([]byte(fmt.Sprintf("distinct-%03d", i))), 0)
		}(i)
	}
	wg.Wait()

	unique := map[string]struct{}{}
	for i, err := range errs {
		if err != nil {
			t.Fatalf("racer %d: %v", i, err)
		}
		unique[ids[i]] = struct{}{}
	}
	if len(unique) != racers {
		t.Fatalf("%d distinct payloads produced %d distinct ids", racers, len(unique))
	}
	store.mu.Lock()
	objects := store.objects
	store.mu.Unlock()
	if objects != racers {
		t.Errorf("store counts %d objects after %d distinct uploads", objects, racers)
	}
	for _, id := range ids {
		if _, err := store.Head(id); err != nil {
			t.Errorf("Head(%s): %v", id, err)
		}
	}
}

// TestACachedPutIsTheSameObjectAsADurableOne pins that PutLimitCached differs
// from PutLimit only in durability. If it ever produced different bytes, a
// different digest, or an object the store could not serve, a node would
// upload something the control plane did not expect.
func TestACachedPutIsTheSameObjectAsADurableOne(t *testing.T) {
	durable, err := NewStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	cached, err := NewStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	payload := bytes.Repeat([]byte("cache-put-parity"), 1000)

	wantID, wantSize, err := durable.PutLimit(bytes.NewReader(payload), 0)
	if err != nil {
		t.Fatal(err)
	}
	gotID, gotSize, err := cached.PutLimitCached(bytes.NewReader(payload), 0)
	if err != nil {
		t.Fatal(err)
	}
	if gotID != wantID || gotSize != wantSize {
		t.Fatalf("cached put produced %s/%d, durable put produced %s/%d", gotID, gotSize, wantID, wantSize)
	}
	// Readable immediately, which is the property the node depends on: it
	// reopens the object to upload it in the very next statement.
	r, size, err := cached.Open(gotID)
	if err != nil {
		t.Fatalf("a cached object could not be reopened: %v", err)
	}
	defer r.Close()
	got, err := io.ReadAll(r)
	if err != nil {
		t.Fatal(err)
	}
	if size != int64(len(payload)) || !bytes.Equal(got, payload) {
		t.Fatalf("cached object served %d bytes, want %d", len(got), len(payload))
	}
	if err := cached.Verify(gotID); err != nil {
		t.Fatalf("a cached object failed its own digest verification: %v", err)
	}
}
