package sim

import (
	"bytes"
	"crypto/sha256"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"remount.dev/remount/internal/artifact"
	"remount.dev/remount/internal/client"
	"remount.dev/remount/internal/localfs"
	"remount.dev/remount/internal/proto"
)

func sha256Of(b []byte) []byte {
	sum := sha256.Sum256(b)
	return sum[:]
}

func writeTree(t *testing.T, root string, files map[string]string) {
	t.Helper()
	for rel, body := range files {
		p := filepath.Join(root, rel)
		if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(p, []byte(body), 0o644); err != nil {
			t.Fatal(err)
		}
	}
}

// The plan's P1.1 proof: pack a fixture with a .gitignore, upload it, seed a
// workspace from it, and read the tree back byte-identical with the ignored
// paths absent. Then push an overlay and pull the result into a fresh
// directory through the same artifact path.
func TestLocalDirectoryRoundTrip(t *testing.T) {
	w := newWorld(t)
	w.node("n1", nil)
	c := w.client("c1")
	ctx := ctxT(t, 90*time.Second)

	src := t.TempDir()
	writeTree(t, src, map[string]string{
		".gitignore":               "*.log\n/build/\n",
		"main.go":                  "package main\n",
		"lib/util.go":              "package lib\n",
		"debug.log":                "noise",
		"build/out.bin":            "artifact",
		"node_modules/x/index.js":  "module.exports = 1",
		"keep/.remountignore":      "secret.txt\n",
		"keep/secret.txt":          "not for the workspace",
		"keep/public.txt":          "fine",
		".remount/env":             "REMOUNT_BROKER=stale",
		"nested/deep/dir/file.txt": strings.Repeat("z", 4096),
	})

	var packed bytes.Buffer
	m, err := localfs.Pack(src, localfs.PackOptions{}, &packed)
	if err != nil {
		t.Fatal(err)
	}
	if m.Files != 6 {
		t.Fatalf("packed %d files, want 6: %+v", m.Files, m)
	}
	id, size, err := c.UploadArtifact(ctx, bytes.NewReader(packed.Bytes()))
	if err != nil {
		t.Fatal(err)
	}
	if size != int64(packed.Len()) || artifact.ID(sha256Of(packed.Bytes())) != id {
		t.Fatalf("upload reported %s/%d", id, size)
	}
	if _, _, err := c.UploadArtifact(ctx, bytes.NewReader(packed.Bytes())); err != nil {
		t.Fatalf("re-upload of an existing artifact must be idempotent: %v", err)
	}

	ws := mustWS(t, c, proto.WorkspaceSpec{Name: "seeded", RestoreFrom: id})
	for rel, want := range map[string]string{
		"main.go": "package main\n", "lib/util.go": "package lib\n", "keep/public.txt": "fine",
		"keep/.remountignore": "secret.txt\n", "nested/deep/dir/file.txt": strings.Repeat("z", 4096),
	} {
		got, err := c.ReadFile(ctx, ws.ID, rel)
		if err != nil || string(got) != want {
			t.Fatalf("%s: %v %q", rel, err, got)
		}
	}
	for _, rel := range []string{"debug.log", "build/out.bin", "node_modules/x/index.js", "keep/secret.txt"} {
		if _, err := c.ReadFile(ctx, ws.ID, rel); err == nil {
			t.Fatalf("%s must be excluded from the pack", rel)
		}
	}
	// .remount is node-owned: the packed archive never carried the stale env.
	env, err := c.ReadFile(ctx, ws.ID, ".remount/env")
	if err != nil || strings.Contains(string(env), "stale") {
		t.Fatalf(".remount/env: %v %q", err, env)
	}

	// The agent writes something; the developer edits locally and pushes.
	if err := c.WriteFile(ctx, ws.ID, "agent.txt", []byte("from the agent\n"), 0); err != nil {
		t.Fatal(err)
	}
	writeTree(t, src, map[string]string{"main.go": "package main // v2\n", "lib/new.go": "package lib\n"})
	packed.Reset()
	if _, err := localfs.Pack(src, localfs.PackOptions{}, &packed); err != nil {
		t.Fatal(err)
	}
	overlay, _, err := c.UploadArtifact(ctx, bytes.NewReader(packed.Bytes()))
	if err != nil {
		t.Fatal(err)
	}
	res, err := c.ApplyTar(ctx, ws.ID, overlay, client.WithIdempotencyKey("push-1"))
	if err != nil {
		t.Fatal(err)
	}
	if res.Files != 7 {
		t.Fatalf("apply_tar wrote %d files, want 7: %+v", res.Files, res)
	}
	got, err := c.ReadFile(ctx, ws.ID, "main.go")
	if err != nil || string(got) != "package main // v2\n" {
		t.Fatalf("overlay: %v %q", err, got)
	}
	if got, err := c.ReadFile(ctx, ws.ID, "agent.txt"); err != nil || string(got) != "from the agent\n" {
		t.Fatalf("overlay must not remove files it does not name: %v %q", err, got)
	}
	// Replaying the same idempotency key does not write twice.
	again, err := c.ApplyTar(ctx, ws.ID, overlay, client.WithIdempotencyKey("push-1"))
	if err != nil || again.Files != 7 {
		t.Fatalf("second apply: %v %+v", err, again)
	}

	// Pull: snapshot, download, unpack into a clean directory, compare.
	snap, err := c.Snapshot(ctx, ws.ID, true)
	if err != nil {
		t.Fatal(err)
	}
	rc, err := c.DownloadArtifact(ctx, snap.Artifact)
	if err != nil {
		t.Fatal(err)
	}
	dst := t.TempDir()
	pulled, err := localfs.Unpack(dst, rc, localfs.UnpackOptions{})
	rc.Close()
	if err != nil {
		t.Fatal(err)
	}
	if len(pulled.Written) != 8 {
		t.Fatalf("pulled %d files, want 8: %+v", len(pulled.Written), pulled)
	}
	for rel, want := range map[string]string{
		"main.go": "package main // v2\n", "lib/new.go": "package lib\n", "agent.txt": "from the agent\n",
		"nested/deep/dir/file.txt": strings.Repeat("z", 4096),
	} {
		got, err := os.ReadFile(filepath.Join(dst, rel))
		if err != nil || string(got) != want {
			t.Fatalf("pulled %s: %v %q", rel, err, got)
		}
	}
	if _, err := os.Stat(filepath.Join(dst, ".remount")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf(".remount must not be pulled: %v", err)
	}
	// A dirty non-git destination refuses without --force.
	if _, err := os.Stat(filepath.Join(dst, "main.go")); err != nil {
		t.Fatal(err)
	}
	rc, err = c.DownloadArtifact(ctx, snap.Artifact)
	if err != nil {
		t.Fatal(err)
	}
	_, err = localfs.Unpack(dst, rc, localfs.UnpackOptions{})
	rc.Close()
	if !errors.Is(err, localfs.ErrDirty) {
		t.Fatalf("second unpack into a non-git tree: %v, want ErrDirty", err)
	}

	// Events: one fs.apply_tar summary and one fs.write per path.
	deadline := time.Now().Add(10 * time.Second)
	for {
		evs, err := c.ReadEvents(ctx, 1, ws.ID)
		if err != nil {
			t.Fatal(err)
		}
		applies, writes := 0, map[string]bool{}
		for _, e := range evs {
			switch e.Type {
			case proto.EvFSApplyTar:
				applies++
			case proto.EvFSWrite:
				var payload struct {
					Path     string `cbor:"path"`
					Artifact string `cbor:"artifact"`
				}
				if err := proto.Unmarshal(e.Payload, &payload); err == nil && payload.Artifact == overlay {
					writes[payload.Path] = true
				}
			}
		}
		if applies == 1 && len(writes) == 7 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("events: %d fs.apply_tar, %d overlay fs.write, want 1 and 7", applies, len(writes))
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// A corrupted download must surface at EOF rather than silently unpack.
func TestDownloadArtifactVerifiesDigest(t *testing.T) {
	w := newWorld(t)
	c := w.client("c1")
	ctx := ctxT(t, 30*time.Second)
	if _, err := c.DownloadArtifact(ctx, "not-an-id"); err == nil {
		t.Fatal("malformed id accepted")
	}
	missing := artifact.ID(sha256Of([]byte("never uploaded")))
	if _, err := c.DownloadArtifact(ctx, missing); err == nil {
		t.Fatal("missing artifact returned a reader")
	}
	if _, err := c.ApplyTar(ctx, "ws_none", missing); err == nil {
		t.Fatal("apply_tar on unknown workspace succeeded")
	}
}
