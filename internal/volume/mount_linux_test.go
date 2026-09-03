//go:build linux

package volume

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"golang.org/x/sys/unix"
)

func TestBindMountEngineRealReadOnlyLifecycle(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	sourcePath := filepath.Join(root, "source")
	targetPath := filepath.Join(root, "target")
	if err := os.Mkdir(sourcePath, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(targetPath, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(sourcePath, "visible"), []byte("immutable"), 0o600); err != nil {
		t.Fatal(err)
	}

	source, err := os.Open(sourcePath)
	if err != nil {
		t.Fatal(err)
	}
	defer source.Close()
	target, err := os.Open(targetPath)
	if err != nil {
		t.Fatal(err)
	}
	defer target.Close()

	engine := BindMountEngine{}
	ctx := context.Background()
	if err := engine.MountReadOnly(ctx, source, target); err != nil {
		if errors.Is(err, unix.EPERM) || errors.Is(err, ErrUnsupported) {
			t.Skipf("real bind mounts unavailable on this Linux host: %v", err)
		}
		t.Fatalf("mount read-only: %v", err)
	}
	mounted := true
	t.Cleanup(func() {
		if mounted {
			if err := engine.Unmount(context.Background(), target); err != nil {
				t.Errorf("cleanup unmount: %v", err)
			}
		}
	})

	status, err := engine.Inspect(ctx, target)
	if err != nil {
		t.Fatalf("inspect mounted target: %v", err)
	}
	sourceInfo, err := source.Stat()
	if err != nil {
		t.Fatal(err)
	}
	if !status.Mounted || !status.ReadOnly || status.Source == nil || !os.SameFile(sourceInfo, status.Source) {
		t.Fatalf("inspect = %+v, want exact read-only source", status)
	}
	got, err := os.ReadFile(filepath.Join(targetPath, "visible"))
	if err != nil {
		t.Fatalf("read through bind: %v", err)
	}
	if string(got) != "immutable" {
		t.Fatalf("visible content = %q, want immutable", got)
	}
	if err := os.WriteFile(filepath.Join(targetPath, "forbidden"), []byte("write"), 0o600); !errors.Is(err, unix.EROFS) {
		t.Fatalf("write through read-only bind = %v, want EROFS", err)
	}

	if err := engine.Unmount(ctx, target); err != nil {
		t.Fatalf("unmount: %v", err)
	}
	mounted = false
	status, err = engine.Inspect(ctx, target)
	if err != nil {
		t.Fatalf("inspect detached target: %v", err)
	}
	if status.Mounted {
		t.Fatalf("inspect after detach = %+v, want absent", status)
	}
	if _, err := os.Stat(filepath.Join(targetPath, "visible")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("source still visible after detach: %v", err)
	}
}
