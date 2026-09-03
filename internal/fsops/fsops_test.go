package fsops

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"remount.dev/remount/internal/proto"
)

func newFS(t *testing.T) (*FS, string) {
	t.Helper()
	dir := t.TempDir()
	f, err := New(dir)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = f.Close() })
	return f, f.Root()
}

func codeOf(err error) string {
	var pe *proto.Error
	if errors.As(err, &pe) {
		return pe.Code
	}
	return ""
}

func TestJailBlocksEscapes(t *testing.T) {
	f, root := newFS(t)
	outside := filepath.Dir(root)
	for _, p := range []string{"../x", "/../x", "a/../../x", "..", "/.."} {
		if _, err := f.Resolve(p); err == nil {
			h, _ := f.Resolve(p)
			if h == root || filepath.HasPrefix(h, root+string(filepath.Separator)) {
				continue // Clean() collapsed it inside the root, which is fine
			}
			t.Fatalf("%q escaped to %q", p, h)
		}
	}
	// Symlink pointing outside must be refused.
	if err := os.Symlink(outside, filepath.Join(root, "esc")); err != nil {
		t.Fatal(err)
	}
	if _, err := f.Read("esc/anything", 0, 0); codeOf(err) != proto.CodeDenied {
		t.Fatalf("symlink escape not denied: %v", err)
	}
	if _, err := f.List("esc"); codeOf(err) != proto.CodeDenied {
		t.Fatalf("symlink escape not denied: %v", err)
	}
	// Symlink inside is fine.
	os.MkdirAll(filepath.Join(root, "real"), 0o755)
	os.WriteFile(filepath.Join(root, "real", "f.txt"), []byte("ok"), 0o644)
	os.Symlink("real", filepath.Join(root, "link"))
	r, err := f.Read("link/f.txt", 0, 0)
	if err != nil || string(r.Data) != "ok" {
		t.Fatalf("%v %+v", err, r)
	}
	// Writing a new file under a symlinked dir inside the root.
	if err := f.Write("link/new.txt", []byte("n"), 0, false, false); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(root, "real", "new.txt")); err != nil {
		t.Fatal(err)
	}
}

func TestRemoveUnlinksSymlinkInsteadOfTarget(t *testing.T) {
	f, root := newFS(t)
	if err := os.MkdirAll(filepath.Join(root, "real"), 0o755); err != nil {
		t.Fatal(err)
	}
	target := filepath.Join(root, "real", "keep")
	if err := os.WriteFile(target, []byte("keep"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink("real", filepath.Join(root, "link")); err != nil {
		t.Fatal(err)
	}
	if err := f.Remove("link", true); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Lstat(filepath.Join(root, "link")); !os.IsNotExist(err) {
		t.Fatalf("symlink still exists: %v", err)
	}
	if got, err := os.ReadFile(target); err != nil || string(got) != "keep" {
		t.Fatalf("symlink target was changed: %q, %v", got, err)
	}
}

func TestInvalidAndSpecialPathsFailPromptly(t *testing.T) {
	f, root := newFS(t)
	if _, err := f.Read("bad\x00name", 0, 0); codeOf(err) != proto.CodeBadRequest {
		t.Fatalf("NUL path code = %q: %v", codeOf(err), err)
	}
	if err := os.WriteFile(filepath.Join(root, "regular"), []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink("regular", filepath.Join(root, "link")); err != nil {
		t.Fatal(err)
	}
	if _, err := f.Read("link", 0, 0); codeOf(err) != proto.CodeBadRequest {
		t.Fatalf("symlink read code = %q: %v", codeOf(err), err)
	}
}

func TestReadWriteListStat(t *testing.T) {
	f, root := newFS(t)
	if err := f.Write("/a/b/c.txt", []byte("hello world"), 0, false, true); err != nil {
		t.Fatal(err)
	}
	r, err := f.Read("a/b/c.txt", 6, 3)
	if err != nil || string(r.Data) != "wor" || r.Size != 11 || r.EOF {
		t.Fatalf("%v %+v", err, r)
	}
	r, _ = f.Read("a/b/c.txt", 6, 0)
	if string(r.Data) != "world" || !r.EOF {
		t.Fatalf("%+v", r)
	}
	if err := f.Write("a/b/c.txt", []byte("!"), 0, true, false); err != nil {
		t.Fatal(err)
	}
	r, _ = f.Read("a/b/c.txt", 0, 0)
	if string(r.Data) != "hello world!" {
		t.Fatalf("%q", r.Data)
	}
	// Mode preserved on overwrite; explicit mode honored.
	os.Chmod(filepath.Join(root, "a/b/c.txt"), 0o600)
	f.Write("a/b/c.txt", []byte("x"), 0, false, false)
	st, _ := f.Stat("a/b/c.txt")
	if st.Mode != 0o600 || st.Size != 1 || st.IsDir {
		t.Fatalf("%+v", st)
	}
	f.Write("exe.sh", []byte("#!/bin/sh"), 0o755, false, false)
	st, _ = f.Stat("exe.sh")
	if st.Mode != 0o755 {
		t.Fatalf("%o", st.Mode)
	}
	ents, err := f.List("/")
	if err != nil || len(ents) != 2 || ents[0].Name != "a" || !ents[0].IsDir || ents[1].Name != "exe.sh" {
		t.Fatalf("%v %+v", err, ents)
	}
	if _, err := f.Read("nope", 0, 0); codeOf(err) != proto.CodeNotFound {
		t.Fatalf("%v", err)
	}
	if _, err := f.Read("a", 0, 0); codeOf(err) != proto.CodeBadRequest {
		t.Fatalf("%v", err)
	}
	if err := f.Write("a", []byte("x"), 0, false, false); codeOf(err) != proto.CodeBadRequest {
		t.Fatalf("%v", err)
	}
	// No temp files left behind.
	m, _ := filepath.Glob(filepath.Join(root, "a/b/.remount-*"))
	if len(m) != 0 {
		t.Fatalf("temp files left: %v", m)
	}
}

func TestMkdirRemoveRename(t *testing.T) {
	f, _ := newFS(t)
	if err := f.Mkdir("x/y/z"); err != nil {
		t.Fatal(err)
	}
	f.Write("x/y/z/f", []byte("1"), 0, false, false)
	if err := f.Remove("x/y", false); err == nil {
		t.Fatal("non-recursive remove of non-empty dir should fail")
	}
	if err := f.Rename("x/y", "x/w"); err != nil {
		t.Fatal(err)
	}
	if _, err := f.Stat("x/w/z/f"); err != nil {
		t.Fatal(err)
	}
	if err := f.Remove("x", true); err != nil {
		t.Fatal(err)
	}
	if err := f.Remove("/", true); codeOf(err) != proto.CodeDenied {
		t.Fatalf("root removal: %v", err)
	}
	if err := f.Remove(".", true); codeOf(err) != proto.CodeDenied {
		t.Fatalf("root removal: %v", err)
	}
}

func TestSearch(t *testing.T) {
	f, root := newFS(t)
	f.Write("src/a.go", []byte("package a\nfunc Hello() {}\n// TODO: fix\n"), 0, false, true)
	f.Write("src/b.txt", []byte("TODO: also\nnothing\n"), 0, false, true)
	f.Write("bin.dat", append([]byte("TODO"), 0, 1, 2), 0, false, true)
	os.MkdirAll(filepath.Join(root, ".git"), 0o755)
	os.WriteFile(filepath.Join(root, ".git", "x"), []byte("TODO in git"), 0o644)
	res, err := f.Search("/", `TODO`, "", 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Matches) != 2 || res.Truncated {
		t.Fatalf("%+v", res)
	}
	if res.Matches[0].Path != "/src/a.go" || res.Matches[0].Line != 3 {
		t.Fatalf("%+v", res.Matches[0])
	}
	res, _ = f.Search("/", `TODO`, "*.go", 0)
	if len(res.Matches) != 1 {
		t.Fatalf("%+v", res)
	}
	res, _ = f.Search("/", `.`, "", 2)
	if len(res.Matches) != 2 || !res.Truncated {
		t.Fatalf("%+v", res)
	}
	if _, err := f.Search("/", `(`, "", 0); codeOf(err) != proto.CodeBadRequest {
		t.Fatal(err)
	}
	if _, err := f.Search("/", `x`, "[", 0); codeOf(err) != proto.CodeBadRequest {
		t.Fatal(err)
	}
	if err := f.Write("long.txt", append(bytes.Repeat([]byte{'x'}, (1<<20)+1), '\n'), 0, false, false); err != nil {
		t.Fatal(err)
	}
	if _, err := f.Search("long.txt", `x`, "", 0); codeOf(err) != proto.CodeResourceExhausted {
		t.Fatalf("overlong search line error = %v", err)
	}
}

func TestEditAtomic(t *testing.T) {
	f, _ := newFS(t)
	f.Write("m.go", []byte("a b a c"), 0o600, false, false)
	n, err := f.Edit("m.go", []proto.FSEdit{{Old: "b", New: "B"}, {Old: "a", New: "A", All: true}})
	if err != nil || n != 3 {
		t.Fatalf("%v %d", err, n)
	}
	r, _ := f.Read("m.go", 0, 0)
	if string(r.Data) != "A B A c" {
		t.Fatalf("%q", r.Data)
	}
	st, _ := f.Stat("m.go")
	if st.Mode != 0o600 {
		t.Fatalf("mode not preserved: %o", st.Mode)
	}
	// Ambiguous edit: nothing applied.
	_, err = f.Edit("m.go", []proto.FSEdit{{Old: "c", New: "C"}, {Old: "A", New: "x"}})
	if codeOf(err) != proto.CodeConflict {
		t.Fatalf("%v", err)
	}
	r, _ = f.Read("m.go", 0, 0)
	if string(r.Data) != "A B A c" {
		t.Fatalf("partial edit applied: %q", r.Data)
	}
	_, err = f.Edit("m.go", []proto.FSEdit{{Old: "zzz", New: "y"}})
	if codeOf(err) != proto.CodeConflict {
		t.Fatalf("%v", err)
	}
	_, err = f.Edit("m.go", []proto.FSEdit{{Old: "", New: "y"}})
	if codeOf(err) != proto.CodeBadRequest {
		t.Fatalf("%v", err)
	}
}
