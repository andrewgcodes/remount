package localfs

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"testing"

	"remount.dev/remount/internal/artifact"
	"remount.dev/remount/internal/artifact/chunked"
)

// selectionFixture is one checkout with every filter Pack applies represented:
// default excludes, an ignore file, an explicit --exclude target, .git, and
// the node-owned staging directory.
func selectionFixture(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	files := map[string]string{
		".gitignore":                "ignored.txt\nbuild/\n",
		"keep.txt":                  "keep",
		"ignored.txt":               "ignored by .gitignore",
		"noisy.log":                 "excluded by flag",
		"build/out.bin":             "generated",
		"sub/nested.txt":            "nested",
		"sub/.remountignore":        "!ignored.txt\n",
		"sub/ignored.txt":           "re-included by .remountignore",
		"node_modules/pkg/index.js": "reproducible",
		".venv/pyvenv.cfg":          "reproducible",
		".git/HEAD":                 "ref: refs/heads/main\n",
		".git/objects/pack/p.pack":  "packfile",
		".remount/env":              "REMOUNT_BROKER=127.0.0.1:1",
	}
	for rel, body := range files {
		path := filepath.Join(dir, filepath.FromSlash(rel))
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, []byte(body), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	return dir
}

// tarPaths lists what Pack actually archived, with directory entries
// normalized to the manifest's spelling.
func tarPaths(t *testing.T, dir string, opts PackOptions) []string {
	t.Helper()
	var buf bytes.Buffer
	if _, err := Pack(dir, opts, &buf); err != nil {
		t.Fatalf("pack: %v", err)
	}
	gz, err := gzip.NewReader(&buf)
	if err != nil {
		t.Fatal(err)
	}
	defer gz.Close()
	var out []string
	tr := tar.NewReader(gz)
	for {
		hdr, err := tr.Next()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		out = append(out, strings.TrimSuffix(hdr.Name, "/"))
	}
	sort.Strings(out)
	return out
}

// chunkedPaths lists what a chunked snapshot driven by Select archived.
func chunkedPaths(t *testing.T, dir string, opts PackOptions) []string {
	t.Helper()
	store, err := artifact.NewStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	sel, err := Select(dir, opts)
	if err != nil {
		t.Fatal(err)
	}
	defer sel.Close()
	result, err := chunked.Snapshot(context.Background(), store, sel.Dir, chunked.SnapshotOptions{Skip: sel.Skip})
	if err != nil {
		t.Fatalf("chunked snapshot: %v", err)
	}
	manifest, err := chunked.LoadManifest(store, result.ManifestID, chunked.Limits{})
	if err != nil {
		t.Fatal(err)
	}
	out := make([]string, 0, len(manifest.Entries))
	for _, entry := range manifest.Entries {
		out = append(out, entry.Path)
	}
	sort.Strings(out)
	return out
}

// TestSelectorGivesChunkedAndTarPushesOneFileSet is the reason Select exists.
// A chunked push that filtered differently would silently upload a developer's
// node_modules, or silently drop a file the tar push carried, and neither
// would be visible in the push output.
func TestSelectorGivesChunkedAndTarPushesOneFileSet(t *testing.T) {
	dir := selectionFixture(t)
	cases := map[string]PackOptions{
		"defaults":        {},
		"exclude flag":    {Excludes: []string{"*.log"}},
		"no git":          {ExcludeGit: true},
		"no ignore files": {NoIgnoreFiles: true, NoDefaultExcludes: true},
	}
	for name, opts := range cases {
		t.Run(name, func(t *testing.T) {
			tarSet, chunkSet := tarPaths(t, dir, opts), chunkedPaths(t, dir, opts)
			if !reflect.DeepEqual(tarSet, chunkSet) {
				t.Fatalf("selection differs:\ntar:     %v\nchunked: %v", tarSet, chunkSet)
			}
		})
	}
}

// TestSelectorAppliesEveryPackFilter pins the individual decisions, so a
// future change that breaks one of them fails here rather than only showing up
// as two representations agreeing on the wrong answer.
func TestSelectorAppliesEveryPackFilter(t *testing.T) {
	dir := selectionFixture(t)
	paths := tarPaths(t, dir, PackOptions{Excludes: []string{"*.log"}})
	present := map[string]bool{}
	for _, p := range paths {
		present[p] = true
	}
	for _, want := range []string{"keep.txt", "sub/nested.txt", "sub/ignored.txt", ".git/HEAD"} {
		if !present[want] {
			t.Errorf("%q was left out of the archive: %v", want, paths)
		}
	}
	for _, unwanted := range []string{
		"ignored.txt", "build", "build/out.bin", "noisy.log",
		"node_modules", "node_modules/pkg/index.js", ".venv", ".venv/pyvenv.cfg",
		".remount", ".remount/env",
	} {
		if present[unwanted] {
			t.Errorf("%q was archived: %v", unwanted, paths)
		}
	}
	if got := chunkedPaths(t, dir, PackOptions{Excludes: []string{"*.log"}}); !reflect.DeepEqual(got, paths) {
		t.Fatalf("chunked selection = %v, want %v", got, paths)
	}
}

// TestSelectorSkipsOversizedGitPackWithTheSameWarning proves the one filter
// that depends on the tree's size rather than its names crosses to the chunked
// path with its operator-visible warning intact.
func TestSelectorSkipsOversizedGitPackWithTheSameWarning(t *testing.T) {
	dir := selectionFixture(t)
	pack := filepath.Join(dir, ".git", "objects", "pack", "big.pack")
	if err := os.WriteFile(pack, nil, 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.Truncate(pack, GitPackWarnBytes+1); err != nil {
		t.Fatal(err)
	}
	sel, err := Select(dir, PackOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer sel.Close()
	if len(sel.Warnings) != 1 || !strings.Contains(sel.Warnings[0], ".git/objects/pack") {
		t.Fatalf("warnings = %v", sel.Warnings)
	}
	if !sel.Skip(".git/objects/pack", true) || sel.Skip(".git/HEAD", false) {
		t.Fatal("oversized pack skip did not apply to the pack alone")
	}
	var buf bytes.Buffer
	m, err := Pack(dir, PackOptions{}, &buf)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(m.Warnings, sel.Warnings) {
		t.Fatalf("Pack warnings = %v, selector warnings = %v", m.Warnings, sel.Warnings)
	}
	for _, p := range chunkedPaths(t, dir, PackOptions{}) {
		if strings.HasPrefix(p, ".git/objects/pack") {
			t.Fatalf("chunked push carried the oversized pack: %q", p)
		}
	}
}
