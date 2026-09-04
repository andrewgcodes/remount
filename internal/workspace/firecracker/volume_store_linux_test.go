//go:build linux

package firecracker

import (
	"bytes"
	"crypto/rand"
	"errors"
	"io"
	"os"
	"path/filepath"
	"syscall"
	"testing"
)

func TestDiskFullRestoreStagingFailsClosedAndCleansUp(t *testing.T) {
	if os.Getenv("REMOUNT_FIRECRACKER_INTEGRATION") != "1" {
		t.Skip("unavailable: set REMOUNT_FIRECRACKER_INTEGRATION=1 for the privileged disk-full staging proof")
	}
	if os.Geteuid() != 0 {
		t.Skip("unavailable: disk-full staging proof requires root to mount a bounded tmpfs")
	}

	source, base := testVolumeProvider(t)
	raw, err := source.Create(t.Context(), "ws_disk_full_source", base, nil)
	if err != nil {
		t.Fatal(err)
	}
	state := filepath.Join(t.TempDir(), "state")
	memory := filepath.Join(t.TempDir(), "memory")
	for _, path := range []string{state, memory} {
		data := make([]byte, 128<<10)
		if _, err := rand.Read(data); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, data, 0o600); err != nil {
			t.Fatal(err)
		}
	}
	var bundle bytes.Buffer
	if err := raw.Checkpoint(t.Context(), nil, SnapshotFiles{
		State: state, Memory: memory, Compatibility: testCompatibility(),
	}, 1, &bundle); err != nil {
		t.Fatal(err)
	}
	if err := raw.Destroy(t.Context()); err != nil {
		t.Fatal(err)
	}

	stageRoot := t.TempDir()
	if err := syscall.Mount("tmpfs", stageRoot, "tmpfs", 0, "size=64k,mode=0700"); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := syscall.Unmount(stageRoot, 0); err != nil {
			t.Errorf("unmount staging tmpfs: %v", err)
		}
	})

	imageRoot := t.TempDir()
	store, err := newReflinkStore(ReflinkStoreOptions{
		Dir: filepath.Join(imageRoot, "images"), MaxImages: 2, MaxLogicalBytes: 1 << 20,
		OwnerUID: os.Getuid(), OwnerGID: os.Getgid(),
	}, func(source, target string) error {
		in, err := os.Open(source)
		if err != nil {
			return err
		}
		defer in.Close()
		out, err := os.OpenFile(target, os.O_WRONLY|os.O_TRUNC, 0)
		if err != nil {
			return err
		}
		_, copyErr := io.Copy(out, in)
		return errors.Join(copyErr, out.Close())
	})
	if err != nil {
		t.Fatal(err)
	}
	destination, err := NewCoWVolumeProvider(CoWVolumeOptions{
		Dir: stageRoot, Images: store, Firecracker: "unused", MaxBundleBytes: 1 << 20,
	})
	if err != nil {
		t.Fatal(err)
	}
	destination.compat = testCompatibility()

	if _, err := destination.Create(t.Context(), "ws_disk_full_restore", base, bytes.NewReader(bundle.Bytes())); !errors.Is(err, syscall.ENOSPC) {
		t.Fatalf("disk-full staging error = %v, want ENOSPC", err)
	}
	stages, err := filepath.Glob(filepath.Join(stageRoot, ".restore-*"))
	if err != nil {
		t.Fatal(err)
	}
	if len(stages) != 0 {
		t.Fatalf("disk-full restore retained staging paths: %v", stages)
	}
	if restored, err := destination.Create(t.Context(), "ws_disk_full_restore", base, nil); err != nil {
		t.Fatalf("disk-full restore retained image admission: %v", err)
	} else if err := restored.Destroy(t.Context()); err != nil {
		t.Fatal(err)
	}
}
