package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"

	"remount.dev/remount/internal/artifact"
	"remount.dev/remount/internal/client"
	"remount.dev/remount/internal/proto"
)

// syncFixture is the tree both directions of the sync commands carry. The
// bulk file is large enough that FastCDC cuts it into many chunks, which is
// what makes the deduplication assertions meaningful.
func syncFixture() map[string][]byte {
	return map[string][]byte{
		"README.md":           []byte("# workspace\n"),
		"src/main.go":         []byte("package main\n\nfunc main() {}\n"),
		"src/deep/nested.txt": []byte("nested\n"),
		"data/bulk.bin":       bytes.Repeat([]byte("the quick brown fox jumps over the lazy dog\n"), 40_000),
		"data/notes.md":       []byte("v1\n"),
	}
}

// claimedWorkspace creates one workspace on the embedded standalone server and
// waits until a node is actually serving it.
func claimedWorkspace(ctx context.Context, t *testing.T, cl *client.Client) string {
	t.Helper()
	ws, err := cl.CreateWorkspace(ctx, proto.WorkspaceSpec{})
	if err != nil {
		t.Fatal(err)
	}
	ws, err = cl.WaitClaimed(ctx, ws.ID)
	if err != nil {
		t.Fatal(err)
	}
	return ws.ID
}

// workspaceTree reads every file the workspace holds, so a push can be checked
// against what the caller intended rather than against a byte count the node
// reported about its own work.
func workspaceTree(ctx context.Context, t *testing.T, cl *client.Client, wsID string) []string {
	t.Helper()
	var out []string
	var walk func(dir string)
	walk = func(dir string) {
		entries, err := cl.ListDir(ctx, wsID, dir)
		if err != nil {
			t.Fatalf("list %s: %v", dir, err)
		}
		for _, entry := range entries {
			path := entry.Name
			if dir != "." {
				path = dir + "/" + entry.Name
			}
			switch {
			case path == artifact.OverlayStageDir:
				// Node-owned; it never travels in either direction.
			case entry.IsDir:
				walk(path)
			default:
				body, err := cl.ReadFile(ctx, wsID, path)
				if err != nil {
					t.Fatalf("read %s: %v", path, err)
				}
				sum := sha256.Sum256(body)
				out = append(out, fmt.Sprintf("%s %d %s", path, len(body), hex.EncodeToString(sum[:8])))
			}
		}
	}
	walk(".")
	sort.Strings(out)
	return out
}

// localTree is workspaceTree's counterpart for a directory on this machine.
func localTree(t *testing.T, dir string) []string {
	t.Helper()
	var out []string
	err := filepath.WalkDir(dir, func(path string, d fs.DirEntry, err error) error {
		if err != nil || d.IsDir() {
			return err
		}
		rel, err := filepath.Rel(dir, path)
		if err != nil {
			return err
		}
		body, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		sum := sha256.Sum256(body)
		out = append(out, fmt.Sprintf("%s %d %s", filepath.ToSlash(rel), len(body), hex.EncodeToString(sum[:8])))
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	sort.Strings(out)
	return out
}

// pullResult is the shape `remount pull --json` prints.
type pullResult struct {
	WS       string `json:"ws"`
	Artifact string `json:"artifact"`
	Format   string `json:"format"`
}

// pushResult is the shape `remount push --json` prints.
type pushResult struct {
	Artifact string `json:"artifact"`
	Format   string `json:"format"`
	Chunked  *struct {
		ManifestID     string `json:"manifest"`
		PlaintextBytes int64  `json:"plaintext_bytes"`
		BytesUploaded  int64  `json:"bytes_uploaded"`
		Chunks         int    `json:"chunks"`
		ChunksUploaded int    `json:"chunks_uploaded"`
	} `json:"chunked"`
	Applied proto.FSApplyTarRes `json:"applied"`
}

func runJSON[T any](t *testing.T, run func() error) T {
	t.Helper()
	var err error
	out := captureStdout(t, func() { err = run() })
	if err != nil {
		t.Fatalf("command failed: %v\noutput: %s", err, out)
	}
	var decoded T
	if jsonErr := json.Unmarshal([]byte(out), &decoded); jsonErr != nil {
		t.Fatalf("decode command output: %v\noutput: %s", jsonErr, out)
	}
	return decoded
}

// TestPullReconstructsAChunkedSnapshotThroughTheCommand covers the gap that
// chunked pull had only package-level coverage: the command itself never ran
// against a real server, so a break in cmdPull's format plumbing would have
// been invisible.
func TestPullReconstructsAChunkedSnapshotThroughTheCommand(t *testing.T) {
	if os.Getenv("REMOUNT_TEST_LOCAL_SERVER") == "" && testing.Short() {
		t.Skip("short")
	}
	t.Setenv("REMOUNT_AUTOSTART", "off")
	serverURL := standaloneForTest(t)
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	c := common{server: serverURL}
	cl := c.client()
	defer cl.Close()
	wsID := claimedWorkspace(ctx, t, cl)
	for path, body := range syncFixture() {
		if err := cl.WriteFile(ctx, wsID, path, body, 0o644); err != nil {
			t.Fatalf("write %s: %v", path, err)
		}
	}

	dir := t.TempDir()
	pulled := runJSON[pullResult](t, func() error {
		return cmdPull(ctx, []string{"--server", serverURL, "--json", wsID, "--dir", dir})
	})
	// Asserted, never assumed: a tar snapshot would exercise none of the
	// chunked reconstruction this test exists for.
	if pulled.Format != proto.ArtifactFormatChunkedV1 {
		t.Fatalf("pull used format %q, want %q", pulled.Format, proto.ArtifactFormatChunkedV1)
	}
	if pulled.WS != wsID || pulled.Artifact == "" {
		t.Fatalf("pull result = %+v", pulled)
	}
	if got, want := localTree(t, dir), workspaceTree(ctx, t, cl, wsID); !reflect.DeepEqual(got, want) {
		t.Fatalf("pulled tree differs:\nlocal:     %v\nworkspace: %v", got, want)
	}
}

// TestPushChunkedTransfersOnlyNewContentAndLandsIdenticalBytes is the
// command-level proof for the other direction: the same tree lands the same
// way as a tar push, and a second push of a mostly-unchanged tree moves a
// fraction of the bytes.
func TestPushChunkedTransfersOnlyNewContentAndLandsIdenticalBytes(t *testing.T) {
	if os.Getenv("REMOUNT_TEST_LOCAL_SERVER") == "" && testing.Short() {
		t.Skip("short")
	}
	t.Setenv("REMOUNT_AUTOSTART", "off")
	serverURL := standaloneForTest(t)
	ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
	defer cancel()

	c := common{server: serverURL}
	cl := c.client()
	defer cl.Close()

	dir := t.TempDir()
	for path, body := range syncFixture() {
		full := filepath.Join(dir, filepath.FromSlash(path))
		if err := os.MkdirAll(filepath.Dir(full), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(full, body, 0o644); err != nil {
			t.Fatal(err)
		}
	}
	// Content the tar path correctly ignores. If --chunked selected its own
	// file set, this is what would silently travel.
	if err := os.MkdirAll(filepath.Join(dir, "node_modules", "pkg"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "node_modules", "pkg", "index.js"), []byte("reproducible"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, ".gitignore"), []byte("secret.txt\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "secret.txt"), []byte("ignored"), 0o644); err != nil {
		t.Fatal(err)
	}

	tarWS := claimedWorkspace(ctx, t, cl)
	chunkWS := claimedWorkspace(ctx, t, cl)

	tarPush := runJSON[pushResult](t, func() error {
		return cmdPush(ctx, []string{"--server", serverURL, "--json", tarWS, "--dir", dir})
	})
	if tarPush.Format != proto.ArtifactFormatTar || tarPush.Chunked != nil {
		t.Fatalf("default push = %+v, want the legacy tar representation", tarPush)
	}
	first := runJSON[pushResult](t, func() error {
		return cmdPush(ctx, []string{"--server", serverURL, "--json", "--chunked", chunkWS, "--dir", dir})
	})
	if first.Format != proto.ArtifactFormatChunkedV1 || first.Chunked == nil {
		t.Fatalf("chunked push = %+v", first)
	}
	if first.Chunked.Chunks < 2 || first.Chunked.ChunksUploaded != first.Chunked.Chunks {
		t.Fatalf("first chunked push = %+v, want every chunk transferred to an empty store", first.Chunked)
	}
	if first.Artifact != first.Chunked.ManifestID {
		t.Fatalf("chunked push named artifact %q but manifest %q", first.Artifact, first.Chunked.ManifestID)
	}
	if !reflect.DeepEqual(first.Applied, tarPush.Applied) {
		t.Fatalf("apply result differs: chunked=%+v tar=%+v", first.Applied, tarPush.Applied)
	}

	tarTree, chunkTree := workspaceTree(ctx, t, cl, tarWS), workspaceTree(ctx, t, cl, chunkWS)
	if !reflect.DeepEqual(chunkTree, tarTree) {
		t.Fatalf("chunked push landed a different tree:\nchunked: %v\ntar:     %v", chunkTree, tarTree)
	}
	for _, entry := range chunkTree {
		if strings.HasPrefix(entry, "node_modules/") || strings.HasPrefix(entry, "secret.txt ") {
			t.Fatalf("chunked push carried a path the tar push ignores: %q", entry)
		}
	}
	if len(chunkTree) == 0 {
		t.Fatal("push landed nothing")
	}

	// The second push changes one small file. Everything else is already in
	// the store, so almost nothing may cross the network.
	if err := os.WriteFile(filepath.Join(dir, "data", "notes.md"), []byte("v2\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	second := runJSON[pushResult](t, func() error {
		return cmdPush(ctx, []string{"--server", serverURL, "--json", "--chunked", chunkWS, "--dir", dir})
	})
	if second.Chunked == nil || second.Chunked.ManifestID == first.Chunked.ManifestID {
		t.Fatalf("changed tree republished the previous manifest: %+v", second)
	}
	if second.Chunked.Chunks != first.Chunked.Chunks {
		t.Fatalf("chunk count changed unexpectedly: %d -> %d", first.Chunked.Chunks, second.Chunked.Chunks)
	}
	if second.Chunked.ChunksUploaded != 1 {
		t.Fatalf("second push sent %d of %d chunks, want only the changed one",
			second.Chunked.ChunksUploaded, second.Chunked.Chunks)
	}
	if second.Chunked.BytesUploaded*20 > first.Chunked.BytesUploaded {
		t.Fatalf("second push moved %d bytes against the first's %d; deduplication did not happen",
			second.Chunked.BytesUploaded, first.Chunked.BytesUploaded)
	}
	if got, want := workspaceTree(ctx, t, cl, chunkWS), localTree(t, dir); !treesAgreeOnPushedPaths(got, want) {
		t.Fatalf("second chunked push did not land the local tree:\nworkspace: %v\nlocal:     %v", got, want)
	}
}

// treesAgreeOnPushedPaths reports whether every workspace entry matches the
// local tree. The local tree also holds the ignored paths a push deliberately
// leaves behind, so it is a superset rather than an equal.
func treesAgreeOnPushedPaths(workspace, local []string) bool {
	have := map[string]bool{}
	for _, entry := range local {
		have[entry] = true
	}
	for _, entry := range workspace {
		if !have[entry] {
			return false
		}
	}
	return len(workspace) > 0
}
