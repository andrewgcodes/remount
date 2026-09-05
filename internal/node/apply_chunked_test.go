package node

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"testing"

	"remount.dev/remount/internal/artifact"
	"remount.dev/remount/internal/artifact/chunked"
	"remount.dev/remount/internal/fsops"
	"remount.dev/remount/internal/proto"
)

// applyFixture is one node holding one serviceable workspace whose tree is a
// real directory, which is all fs.apply_tar needs.
func applyFixture(t *testing.T, id string) (*Node, *ws, string) {
	t.Helper()
	n := newTestNode(t, nil)
	root := filepath.Join(t.TempDir(), "workspace")
	if err := os.MkdirAll(root, 0o755); err != nil {
		t.Fatal(err)
	}
	jail, err := fsops.New(root)
	if err != nil {
		t.Fatal(err)
	}
	w := &ws{
		Workspace: proto.Workspace{ID: id, Tenant: "local", Generation: 1, State: proto.WSClaimed},
		handle:    &failingHandle{id: id, fs: jail},
	}
	n.mu.Lock()
	n.workspaces[w.ID] = w
	n.mu.Unlock()
	return n, w, root
}

// writeTree materializes rel -> body under dir, creating parents.
func writeTree(t *testing.T, dir string, files map[string]string) {
	t.Helper()
	for rel, body := range files {
		path := filepath.Join(dir, filepath.FromSlash(rel))
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, []byte(body), 0o644); err != nil {
			t.Fatal(err)
		}
	}
}

// storeChunked publishes dir as a chunked manifest in the node's own store and
// returns the manifest id.
func storeChunked(t *testing.T, n *Node, dir string) string {
	t.Helper()
	result, err := chunked.Snapshot(context.Background(), n.store, dir, chunked.SnapshotOptions{})
	if err != nil {
		t.Fatalf("chunked snapshot: %v", err)
	}
	return result.ManifestID
}

// storeTar publishes dir as the legacy tar.gz artifact in the node's store.
func storeTar(t *testing.T, n *Node, dir string) string {
	t.Helper()
	var buf bytes.Buffer
	if err := artifact.Snapshot(dir, nil, &buf); err != nil {
		t.Fatalf("tar snapshot: %v", err)
	}
	id, _, err := n.store.Put(&buf)
	if err != nil {
		t.Fatal(err)
	}
	return id
}

// treeShape is every entry of root with the facts an overlay is responsible
// for: kind, permissions and content. Modification times are deliberately
// excluded; the two representations carry them differently and neither is the
// workspace content.
func treeShape(t *testing.T, root string) []string {
	t.Helper()
	var out []string
	rr, err := os.OpenRoot(root)
	if err != nil {
		t.Fatal(err)
	}
	defer rr.Close()
	err = fs.WalkDir(rr.FS(), ".", func(p string, d fs.DirEntry, walkErr error) error {
		if walkErr != nil || p == "." {
			return walkErr
		}
		info, err := rr.Lstat(p)
		if err != nil {
			return err
		}
		switch {
		case info.IsDir():
			out = append(out, fmt.Sprintf("dir %s %04o", p, info.Mode().Perm()))
		case info.Mode()&os.ModeSymlink != 0:
			target, err := rr.Readlink(p)
			if err != nil {
				return err
			}
			out = append(out, fmt.Sprintf("link %s -> %s", p, target))
		default:
			f, err := rr.Open(p)
			if err != nil {
				return err
			}
			h := sha256.New()
			_, copyErr := io.Copy(h, f)
			closeErr := f.Close()
			if copyErr != nil || closeErr != nil {
				return fmt.Errorf("hash %s: %v %v", p, copyErr, closeErr)
			}
			out = append(out, fmt.Sprintf("file %s %04o %s", p, info.Mode().Perm(), hex.EncodeToString(h.Sum(nil))))
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	sort.Strings(out)
	return out
}

// TestApplyChunkedAndTarProduceIdenticalTrees is the compatibility proof for
// the new representation: choosing --chunked must change what crosses the
// network, never what the workspace ends up holding.
func TestApplyChunkedAndTarProduceIdenticalTrees(t *testing.T) {
	source := t.TempDir()
	writeTree(t, source, map[string]string{
		"README.md":             "# hello\n",
		"src/main.go":           strings.Repeat("package main\n", 4096),
		"src/deep/nested/a.txt": "a",
		"empty/.keep":           "",
	})
	if err := os.Chmod(filepath.Join(source, "src", "main.go"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink("README.md", filepath.Join(source, "link.md")); err != nil {
		t.Fatal(err)
	}

	ctx := context.Background()
	tarNode, tarWS, tarRoot := applyFixture(t, "ws_tar")
	chunkNode, chunkWS, chunkRoot := applyFixture(t, "ws_chunked")
	for _, root := range []string{tarRoot, chunkRoot} {
		writeTree(t, root, map[string]string{"README.md": "stale", "local-only.txt": "kept"})
	}

	tarRaw, err := tarNode.applyTar(ctx, tarWS, storeTar(t, tarNode, source), proto.ArtifactFormatTar, "", "c1")
	if err != nil {
		t.Fatalf("tar apply: %v", err)
	}
	chunkRaw, err := chunkNode.applyTar(ctx, chunkWS, storeChunked(t, chunkNode, source), proto.ArtifactFormatChunkedV1, "", "c1")
	if err != nil {
		t.Fatalf("chunked apply: %v", err)
	}
	var tarRes, chunkRes proto.FSApplyTarRes
	if err := proto.Unmarshal(tarRaw, &tarRes); err != nil {
		t.Fatal(err)
	}
	if err := proto.Unmarshal(chunkRaw, &chunkRes); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(tarRes, chunkRes) {
		t.Fatalf("apply result differs: tar=%+v chunked=%+v", tarRes, chunkRes)
	}
	if tarRes.Files == 0 {
		t.Fatalf("fixture applied nothing: %+v", tarRes)
	}
	if got, want := treeShape(t, chunkRoot), treeShape(t, tarRoot); !reflect.DeepEqual(got, want) {
		t.Fatalf("chunked overlay produced a different tree:\nchunked: %v\ntar:     %v", got, want)
	}
	// The overlay adds; it never removes what the archive does not name.
	if body, err := os.ReadFile(filepath.Join(chunkRoot, "local-only.txt")); err != nil || string(body) != "kept" {
		t.Fatalf("local-only.txt = %q (%v)", body, err)
	}
}

// TestApplyRefusesUnsupportedFormatWithoutTouchingTree is the fail-closed
// proof. A node asked for a representation it cannot reconstruct must say so;
// reading the body as though it were some other representation, or applying
// part of it, would both be worse than refusing.
func TestApplyRefusesUnsupportedFormatWithoutTouchingTree(t *testing.T) {
	source := t.TempDir()
	writeTree(t, source, map[string]string{"payload.txt": "would have landed"})
	n, w, root := applyFixture(t, "ws_format")
	writeTree(t, root, map[string]string{"existing.txt": "untouched"})
	before := treeShape(t, root)
	id := storeChunked(t, n, source)

	for _, format := range []string{proto.ArtifactFormatFirecrackerFullV1, "zip-v9", "chunked-v2"} {
		_, err := n.applyTar(context.Background(), w, id, format, "", "c1")
		if err == nil {
			t.Fatalf("format %q was accepted", format)
		}
		if !errors.Is(err, &proto.Error{Code: proto.CodeUnsupported}) {
			t.Fatalf("format %q error = %v, want a typed unsupported error", format, err)
		}
		if !strings.Contains(err.Error(), format) {
			t.Fatalf("format %q refusal does not name the format: %v", format, err)
		}
	}
	if got := treeShape(t, root); !reflect.DeepEqual(got, before) {
		t.Fatalf("refused format changed the tree:\nafter:  %v\nbefore: %v", got, before)
	}
	if _, err := os.Stat(filepath.Join(root, artifact.OverlayStageDir)); !os.IsNotExist(err) {
		t.Fatalf("refused format created the staging directory: %v", err)
	}
}

// TestChunkedApplyKeepsOverlaySafetyProperties mirrors the tar overlay's
// refusals on the chunked path. The chunked branch reuses ApplyOverlay
// precisely so these hold; the test is what makes a second, weaker overlay
// implementation impossible to land unnoticed.
func TestChunkedApplyKeepsOverlaySafetyProperties(t *testing.T) {
	ctx := context.Background()

	t.Run("remount dir", func(t *testing.T) {
		source := t.TempDir()
		writeTree(t, source, map[string]string{".remount/env": "REMOUNT_BROKER=attacker"})
		n, w, root := applyFixture(t, "ws_env")
		writeTree(t, root, map[string]string{"safe.txt": "safe"})
		_, err := n.applyTar(ctx, w, storeChunked(t, n, source), proto.ArtifactFormatChunkedV1, "", "c1")
		if err == nil || !strings.Contains(err.Error(), artifact.OverlayStageDir) {
			t.Fatalf("chunked overlay into .remount accepted: %v", err)
		}
		if _, err := os.Stat(filepath.Join(root, artifact.OverlayStageDir, "env")); !os.IsNotExist(err) {
			t.Fatal(".remount/env was written by a chunked overlay")
		}
	})

	t.Run("symlinked parent", func(t *testing.T) {
		source := t.TempDir()
		writeTree(t, source, map[string]string{"link/victim": "PWNED"})
		n, w, root := applyFixture(t, "ws_symlink")
		outside := filepath.Join(filepath.Dir(root), "outside")
		if err := os.MkdirAll(outside, 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.Symlink(outside, filepath.Join(root, "link")); err != nil {
			t.Fatal(err)
		}
		if _, err := n.applyTar(ctx, w, storeChunked(t, n, source), proto.ArtifactFormatChunkedV1, "", "c1"); err == nil {
			t.Fatal("chunked write through a symlinked directory accepted")
		}
		if _, err := os.Stat(filepath.Join(outside, "victim")); !os.IsNotExist(err) {
			t.Fatalf("chunked overlay wrote outside the workspace root: %v", err)
		}
	})

	t.Run("file over directory", func(t *testing.T) {
		source := t.TempDir()
		writeTree(t, source, map[string]string{"aaa.txt": "first", "isdir": "now a file"})
		n, w, root := applyFixture(t, "ws_clash")
		if err := os.MkdirAll(filepath.Join(root, "isdir"), 0o755); err != nil {
			t.Fatal(err)
		}
		_, err := n.applyTar(ctx, w, storeChunked(t, n, source), proto.ArtifactFormatChunkedV1, "", "c1")
		if err == nil || !strings.Contains(err.Error(), "replace a directory") {
			t.Fatalf("chunked file over directory accepted: %v", err)
		}
		// Validation precedes the first rename, so the entry sorted before the
		// refused one did not land either.
		if _, err := os.Stat(filepath.Join(root, "aaa.txt")); !os.IsNotExist(err) {
			t.Fatal("refused chunked overlay wrote an earlier entry")
		}
	})

	t.Run("directory over file", func(t *testing.T) {
		source := t.TempDir()
		writeTree(t, source, map[string]string{"afile/child.txt": "x", "zzz.txt": "y"})
		n, w, root := applyFixture(t, "ws_dirclash")
		writeTree(t, root, map[string]string{"afile": "a regular file"})
		_, err := n.applyTar(ctx, w, storeChunked(t, n, source), proto.ArtifactFormatChunkedV1, "", "c1")
		if err == nil || !strings.Contains(err.Error(), "replace a file with a directory") {
			t.Fatalf("chunked directory over file accepted: %v", err)
		}
		if _, err := os.Stat(filepath.Join(root, "zzz.txt")); !os.IsNotExist(err) {
			t.Fatal("refused chunked overlay wrote a sibling entry")
		}
	})
}

// TestChunkedApplyRejectsAMissingManifest keeps the not-found path typed: a
// manifest the node cannot fetch is refused before the tree boundary, like an
// absent tar.
func TestChunkedApplyRejectsAMissingManifest(t *testing.T) {
	n, w, root := applyFixture(t, "ws_missing")
	writeTree(t, root, map[string]string{"safe.txt": "safe"})
	before := treeShape(t, root)
	absent := artifact.ID(make([]byte, 32))
	_, err := n.applyTar(context.Background(), w, absent, proto.ArtifactFormatChunkedV1, "", "c1")
	if err == nil || !errors.Is(err, &proto.Error{Code: proto.CodeNotFound}) {
		t.Fatalf("missing manifest error = %v, want not_found", err)
	}
	if got := treeShape(t, root); !reflect.DeepEqual(got, before) {
		t.Fatalf("missing manifest changed the tree: %v", got)
	}
}
