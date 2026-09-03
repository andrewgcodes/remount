package localfs

import (
	"bytes"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"testing"

	"remount.dev/remount/internal/artifact"
)

func writeFile(t *testing.T, root, rel, content string) {
	t.Helper()
	p := filepath.Join(root, filepath.FromSlash(rel))
	if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(p, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
}

func fixture(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	writeFile(t, dir, ".gitignore", "*.log\n/build/\nsecret-*\n!secret-keep.txt\ndocs/**/*.tmp\n# comment\n\n")
	writeFile(t, dir, ".remountignore", "!important.log\n")
	writeFile(t, dir, "main.go", "package main\n")
	writeFile(t, dir, "debug.log", "noise")
	writeFile(t, dir, "important.log", "keep me")
	writeFile(t, dir, "build/out.bin", "bin")
	writeFile(t, dir, "sub/build/keep.txt", "anchored build/ only excludes the root one")
	writeFile(t, dir, "secret-a.txt", "x")
	writeFile(t, dir, "secret-keep.txt", "y")
	writeFile(t, dir, "docs/a/b/c.tmp", "tmp")
	writeFile(t, dir, "docs/a/b/c.md", "md")
	writeFile(t, dir, "node_modules/x/index.js", "js")
	writeFile(t, dir, "sub/.gitignore", "local-only\n")
	writeFile(t, dir, "sub/local-only", "nested ignore")
	writeFile(t, dir, "sub/kept", "nested kept")
	writeFile(t, dir, ".remount/env", "REMOUNT_BROKER=nope")
	writeFile(t, dir, ".git/HEAD", "ref: refs/heads/main\n")
	if err := os.Symlink("main.go", filepath.Join(dir, "link.go")); err != nil {
		t.Fatal(err)
	}
	return dir
}

func entries(t *testing.T, archive []byte) []string {
	t.Helper()
	out := t.TempDir()
	if err := artifact.Restore(out, bytes.NewReader(archive)); err != nil {
		t.Fatal(err)
	}
	var names []string
	err := filepath.WalkDir(out, func(p string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if p == out {
			return nil
		}
		rel, _ := filepath.Rel(out, p)
		names = append(names, filepath.ToSlash(rel))
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	slices.Sort(names)
	return names
}

func TestPackHonorsIgnoreFilesAndDefaults(t *testing.T) {
	dir := fixture(t)
	var buf bytes.Buffer
	m, err := Pack(dir, PackOptions{}, &buf)
	if err != nil {
		t.Fatal(err)
	}
	got := entries(t, buf.Bytes())
	want := []string{
		".git", ".git/HEAD", ".gitignore", ".remountignore",
		"docs", "docs/a", "docs/a/b", "docs/a/b/c.md",
		"important.log", "link.go", "main.go", "secret-keep.txt",
		"sub", "sub/.gitignore", "sub/build", "sub/build/keep.txt", "sub/kept",
	}
	if !slices.Equal(got, want) {
		t.Fatalf("entries:\n got %q\nwant %q", got, want)
	}
	if m.Files != 10 || m.Symlinks != 1 || m.Excluded == 0 {
		t.Fatalf("manifest %+v", m)
	}
	if len(m.Warnings) != 0 {
		t.Fatalf("unexpected warnings %q", m.Warnings)
	}
	// Byte identity: the restored tree matches the source for included files.
	out := t.TempDir()
	if err := artifact.Restore(out, bytes.NewReader(buf.Bytes())); err != nil {
		t.Fatal(err)
	}
	for _, rel := range []string{"main.go", "docs/a/b/c.md", "sub/kept", "important.log"} {
		a, _ := os.ReadFile(filepath.Join(dir, rel))
		b, err := os.ReadFile(filepath.Join(out, rel))
		if err != nil || !bytes.Equal(a, b) {
			t.Fatalf("%s differs after restore: %v", rel, err)
		}
	}
	if link, err := os.Readlink(filepath.Join(out, "link.go")); err != nil || link != "main.go" {
		t.Fatalf("symlink not preserved: %q %v", link, err)
	}
}

func TestPackIsDeterministicAndOptionsApply(t *testing.T) {
	dir := fixture(t)
	var a, b bytes.Buffer
	if _, err := Pack(dir, PackOptions{}, &a); err != nil {
		t.Fatal(err)
	}
	if _, err := Pack(dir, PackOptions{}, &b); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(a.Bytes(), b.Bytes()) {
		t.Fatal("two packs of the same tree differ")
	}
	var c bytes.Buffer
	if _, err := Pack(dir, PackOptions{ExcludeGit: true, NoIgnoreFiles: true, NoDefaultExcludes: true, Excludes: []string{"docs"}}, &c); err != nil {
		t.Fatal(err)
	}
	got := entries(t, c.Bytes())
	if slices.Contains(got, ".git") || slices.Contains(got, "docs") {
		t.Fatalf(".git/docs should be excluded: %q", got)
	}
	for _, must := range []string{"debug.log", "build/out.bin", "node_modules/x/index.js", "secret-a.txt"} {
		if !slices.Contains(got, must) {
			t.Fatalf("%s should be included with ignore processing off: %q", must, got)
		}
	}
	if slices.Contains(got, ".remount") || slices.Contains(got, ".remount/env") {
		t.Fatalf(".remount must never travel: %q", got)
	}
}

func TestPackWarnsAndSkipsLargeGitPack(t *testing.T) {
	dir := t.TempDir()
	writeFile(t, dir, "a.txt", "a")
	writeFile(t, dir, ".git/HEAD", "ref: refs/heads/main\n")
	pack := filepath.Join(dir, ".git", "objects", "pack", "pack-x.pack")
	if err := os.MkdirAll(filepath.Dir(pack), 0o755); err != nil {
		t.Fatal(err)
	}
	f, err := os.Create(pack)
	if err != nil {
		t.Fatal(err)
	}
	if err := f.Truncate(GitPackWarnBytes + 1); err != nil {
		t.Fatal(err)
	}
	f.Close()
	var buf bytes.Buffer
	m, err := Pack(dir, PackOptions{}, &buf)
	if err != nil {
		t.Fatal(err)
	}
	if len(m.Warnings) != 1 {
		t.Fatalf("want one warning, got %q", m.Warnings)
	}
	got := entries(t, buf.Bytes())
	if slices.Contains(got, ".git/objects/pack") || !slices.Contains(got, ".git/HEAD") {
		t.Fatalf("pack dir should be skipped, HEAD kept: %q", got)
	}
}

func TestIgnorePatterns(t *testing.T) {
	cases := []struct {
		pattern, rel string
		isDir, want  bool
	}{
		{"*.log", "a.log", false, true},
		{"*.log", "deep/er/a.log", false, true},
		{"*.log", "a.log.txt", false, false},
		{"/build", "build", true, true},
		{"/build", "sub/build", true, false},
		{"build/", "build", false, false},
		{"build/", "sub/build", true, true},
		{"doc/*.txt", "doc/a.txt", false, true},
		{"doc/*.txt", "doc/sub/a.txt", false, false},
		{"doc/**/*.txt", "doc/sub/a.txt", false, true},
		{"doc/**/*.txt", "doc/a.txt", false, true},
		{"**/foo", "a/b/foo", false, true},
		{"abc/**", "abc/x/y", false, true},
		{"a?c", "abc", false, true},
		{"a[!b]c", "abc", false, false},
		{"a[!b]c", "axc", false, true},
		{`\#hash`, "#hash", false, true},
		{"foo", "foo/inside.txt", false, true},
	}
	for _, tc := range cases {
		p, ok := parseLine(tc.pattern)
		if !ok {
			t.Fatalf("pattern %q did not parse", tc.pattern)
		}
		got := p.re.MatchString(tc.rel) && (!p.dirOnly || tc.isDir)
		if got != tc.want {
			t.Errorf("%q vs %q (dir=%v): got %v want %v (re %s)", tc.pattern, tc.rel, tc.isDir, got, tc.want, p.re)
		}
	}
}

func TestUnpackWritesDiffAndRefusesDirtyTree(t *testing.T) {
	src := t.TempDir()
	writeFile(t, src, "a.txt", "new a")
	writeFile(t, src, "b/c.txt", "c")
	writeFile(t, src, ".remount/env", "nope")
	var buf bytes.Buffer
	if _, err := Pack(src, PackOptions{NoIgnoreFiles: true}, &buf); err != nil {
		t.Fatal(err)
	}
	// Pack never includes .remount; simulate a node snapshot that does not
	// either, then check Unpack on an empty dir, a same dir, and a dirty dir.
	dst := t.TempDir()
	res, err := Unpack(dst, bytes.NewReader(buf.Bytes()), UnpackOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(res.Written, []string{"a.txt", "b/c.txt"}) || res.Unchanged != 0 {
		t.Fatalf("first unpack %+v", res)
	}
	// Non-git, non-empty directory is dirty without --force.
	if _, err := Unpack(dst, bytes.NewReader(buf.Bytes()), UnpackOptions{}); !errors.Is(err, ErrDirty) {
		t.Fatalf("want ErrDirty, got %v", err)
	}
	writeFile(t, dst, "a.txt", "local edit")
	writeFile(t, dst, "extra.txt", "mine")
	res, err = Unpack(dst, bytes.NewReader(buf.Bytes()), UnpackOptions{Force: true})
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(res.Written, []string{"a.txt"}) || res.Unchanged != 1 || !res.Dirty {
		t.Fatalf("forced unpack %+v", res)
	}
	if !slices.Equal(res.Extra, []string{"extra.txt"}) {
		t.Fatalf("extra %q", res.Extra)
	}
	if got, _ := os.ReadFile(filepath.Join(dst, "extra.txt")); string(got) != "mine" {
		t.Fatal("extra local file was touched")
	}
	if got, _ := os.ReadFile(filepath.Join(dst, "a.txt")); string(got) != "new a" {
		t.Fatalf("a.txt = %q", got)
	}
}

func TestUnpackUsesGitStatusForCheckouts(t *testing.T) {
	git, err := exec.LookPath("git")
	if err != nil {
		t.Skip("git not installed")
	}
	dst := t.TempDir()
	run := func(args ...string) {
		t.Helper()
		cmd := exec.Command(git, append([]string{"-C", dst}, args...)...)
		cmd.Env = append(os.Environ(), "GIT_AUTHOR_NAME=t", "GIT_AUTHOR_EMAIL=t@t", "GIT_COMMITTER_NAME=t", "GIT_COMMITTER_EMAIL=t@t", "HOME="+dst)
		if out, err := cmd.CombinedOutput(); err != nil {
			t.Fatalf("git %v: %v\n%s", args, err, out)
		}
	}
	run("init", "-q")
	writeFile(t, dst, "tracked.txt", "v1")
	run("add", "tracked.txt")
	run("commit", "-q", "-m", "init")
	src := t.TempDir()
	writeFile(t, src, "tracked.txt", "v2")
	var buf bytes.Buffer
	if _, err := Pack(src, PackOptions{}, &buf); err != nil {
		t.Fatal(err)
	}
	// Clean checkout: proceeds without force even though the dir is non-empty.
	res, err := Unpack(dst, bytes.NewReader(buf.Bytes()), UnpackOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(res.Written, []string{"tracked.txt"}) || res.Dirty {
		t.Fatalf("%+v", res)
	}
	// Now the tree is modified relative to HEAD: dirty.
	if _, err := Unpack(dst, bytes.NewReader(buf.Bytes()), UnpackOptions{}); !errors.Is(err, ErrDirty) {
		t.Fatalf("want ErrDirty, got %v", err)
	}
}

func TestPackExtraCarriesHomeStateAlongsideCheckout(t *testing.T) {
	dir := t.TempDir()
	writeFile(t, dir, "main.go", "package main\n")
	writeFile(t, dir, ".claude/settings.json", "{}")
	home := t.TempDir()
	writeFile(t, home, ".claude/projects/p1/session.jsonl", "{}\n")
	writeFile(t, home, ".claude.json", "{\"a\":1}")
	writeFile(t, home, ".local/share/opencode/db.sqlite", "sqlite")
	writeFile(t, home, "unrelated.txt", "never packed")

	var buf bytes.Buffer
	m, err := Pack(dir, PackOptions{Extra: []ExtraTree{
		{Local: filepath.Join(home, ".claude"), Archive: ".claude"},
		{Local: filepath.Join(home, ".claude.json"), Archive: ".claude.json"},
		{Local: filepath.Join(home, ".local/share/opencode"), Archive: ".local/share/opencode"},
		{Local: filepath.Join(home, ".codex"), Archive: ".codex"},
	}}, &buf)
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(m.Missing, []string{".codex"}) {
		t.Fatalf("missing = %v", m.Missing)
	}
	got := entries(t, buf.Bytes())
	want := []string{
		".claude", ".claude.json", ".claude/projects", ".claude/projects/p1", ".claude/projects/p1/session.jsonl", ".claude/settings.json",
		".local", ".local/share", ".local/share/opencode", ".local/share/opencode/db.sqlite", "main.go",
	}
	if !slices.Equal(got, want) {
		t.Fatalf("entries:\n got %v\nwant %v", got, want)
	}

	// A file at the same archive path from two trees is a conflict.
	writeFile(t, home, ".claude/settings.json", "home copy")
	if _, err := Pack(dir, PackOptions{Extra: []ExtraTree{{Local: filepath.Join(home, ".claude"), Archive: ".claude"}}}, &bytes.Buffer{}); err == nil {
		t.Fatal("conflicting file must fail the pack")
	}
	for _, bad := range []ExtraTree{
		{Local: home, Archive: ".remount/env"},
		{Local: home, Archive: "../x"},
		{Local: home, Archive: ""},
		{Local: filepath.Join(home, ".claude.json"), Archive: "renamed.json"},
	} {
		if _, err := Pack(dir, PackOptions{Extra: []ExtraTree{bad}}, &bytes.Buffer{}); err == nil {
			t.Fatalf("extra %+v must be refused", bad)
		}
	}
}
