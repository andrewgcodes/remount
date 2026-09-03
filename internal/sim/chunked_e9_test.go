package sim

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"remount.dev/remount/internal/artifact"
	"remount.dev/remount/internal/artifact/chunked"
	"remount.dev/remount/internal/node"
	"remount.dev/remount/internal/proto"
)

// TestE9ChunkedSnapshotMoveDeduplicates500MBLogicalFixture composes the real
// client, control, node artifact-proof HTTP path, chunk store and a second
// backend. The large dependency tree is sparse but real and excluded exactly
// as production recommends; included content still traverses FastCDC and is
// byte-compared after handoff and against the canonical tar exporter.
func TestE9ChunkedSnapshotMoveDeduplicates500MBLogicalFixture(t *testing.T) {
	w := newWorld(t)
	n1Dir, n2Dir := t.TempDir(), t.TempDir()
	n1 := w.nodeWith("e9-n1", func(options *node.Options) {
		options.DataDir = n1Dir
		options.Labels = map[string]string{"e9": "yes"}
		options.SnapshotMinInterval = time.Nanosecond
	})
	n2 := w.nodeWith("e9-n2", func(options *node.Options) {
		options.DataDir = n2Dir
		options.Labels = map[string]string{"e9": "yes"}
	})
	c := w.client("e9-client")
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	ws := mustWS(t, c, proto.WorkspaceSpec{
		Placement: proto.Placement{Node: n1.ID()}, Exclude: []string{"node_modules"},
	})

	_, stderr, exit, err := c.Run(ctx, ws.ID, "sh", "-c", "mkdir -p node_modules && truncate -s 524288000 node_modules/generated.bin")
	if err != nil || exit == nil || exit.Code != 0 {
		t.Fatalf("create 500MB fixture: exit=%+v stderr=%q err=%v", exit, stderr, err)
	}
	original := bytes.Repeat([]byte("0123456789abcdef"), 32<<10) // 512 KiB.
	if err := c.WriteFile(ctx, ws.ID, "included.bin", original, 0o640); err != nil {
		t.Fatal(err)
	}
	first, err := c.Checkpoint(ctx, ws.ID)
	if err != nil {
		t.Fatal(err)
	}
	if first.Format != proto.ArtifactFormatChunkedV1 || first.Chunks < 2 {
		t.Fatalf("first snapshot did not use chunked runtime: %+v", first)
	}

	changed := append([]byte(nil), original...)
	copy(changed[len(changed)/2:len(changed)/2+4096], bytes.Repeat([]byte{0x5a}, 4096))
	if err := c.WriteFile(ctx, ws.ID, "included.bin", changed, 0o640); err != nil {
		t.Fatal(err)
	}
	second, err := c.Checkpoint(ctx, ws.ID)
	if err != nil {
		t.Fatal(err)
	}
	if second.Format != proto.ArtifactFormatChunkedV1 || second.UploadedBytes >= 1<<20 {
		t.Fatalf("second snapshot = %+v, want chunked and <1 MiB uploaded", second)
	}

	started := time.Now()
	moved, err := c.MoveWorkspace(ctx, ws.ID, nil, &proto.Placement{Node: n2.ID()})
	if err != nil {
		t.Fatal(err)
	}
	moved, err = c.WaitClaimed(ctx, moved.ID)
	if err != nil {
		t.Fatal(err)
	}
	if elapsed := time.Since(started); elapsed >= 3*time.Second {
		t.Fatalf("chunked move took %s, want <3s", elapsed)
	}
	if moved.Node != n2.ID() || moved.LastSnapshotFormat != proto.ArtifactFormatChunkedV1 {
		t.Fatalf("moved workspace = %+v", moved)
	}
	got, err := c.ReadFile(ctx, ws.ID, "included.bin")
	if err != nil || !bytes.Equal(got, changed) {
		t.Fatalf("restored included file differs: bytes=%d err=%v", len(got), err)
	}
	if _, err := c.ReadFile(ctx, ws.ID, "node_modules/generated.bin"); err == nil {
		t.Fatal("excluded node_modules content traveled in the checkpoint")
	}

	root := filepath.Join(n2Dir, "ws", ws.ID)
	var tarPath, exported bytes.Buffer
	if err := artifact.Snapshot(root, []string{".remount", "node_modules"}, &tarPath); err != nil {
		t.Fatal(err)
	}
	if err := chunked.ExportTar(ctx, w.srv.Store, moved.LastSnapshot, &exported, chunked.Limits{}); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(tarPath.Bytes(), exported.Bytes()) {
		t.Fatalf("restored tar path differs from manifest export: tar=%d export=%d", tarPath.Len(), exported.Len())
	}
	if info, err := os.Stat(filepath.Join(n1Dir, "ws", ws.ID)); err == nil && info.IsDir() {
		t.Fatal("source workspace tree remained after durable move commit")
	}
}
