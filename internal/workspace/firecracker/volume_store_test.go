//go:build !windows

package firecracker

import (
	"bytes"
	"context"
	"io"
	"os"
	"path/filepath"
	"testing"
)

func testCompatibility() Compatibility {
	return Compatibility{Architecture: "x86_64", CPUFingerprint: "sha256:test", HostKernel: "6.1", Firecracker: "v1", Snapshot: "v1", GuestProtocol: GuestProtocolVersion}
}

func testVolumeProvider(t *testing.T) (*CoWVolumeProvider, string) {
	t.Helper()
	dir := t.TempDir()
	base := filepath.Join(dir, "base.ext4")
	if err := os.WriteFile(base, []byte("base-disk-bytes"), 0o600); err != nil {
		t.Fatal(err)
	}
	store, err := newReflinkStore(ReflinkStoreOptions{Dir: filepath.Join(dir, "images"), MaxImages: 4, MaxLogicalBytes: 1 << 20, OwnerUID: os.Getuid(), OwnerGID: os.Getgid()}, func(source, target string) error {
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
		return errorsJoin(copyErr, out.Close())
	})
	if err != nil {
		t.Fatal(err)
	}
	provider, err := NewCoWVolumeProvider(CoWVolumeOptions{Dir: filepath.Join(dir, "snapshots"), Images: store, Firecracker: "unused", MaxBundleBytes: 1 << 20})
	if err != nil {
		t.Fatal(err)
	}
	provider.compat = testCompatibility()
	return provider, base
}

func errorsJoin(values ...error) error {
	for _, err := range values {
		if err != nil {
			return err
		}
	}
	return nil
}

func TestFullBundleRoundTripAndCompatibilityFence(t *testing.T) {
	provider, base := testVolumeProvider(t)
	raw, err := provider.Create(t.Context(), "ws_bundle", base, nil)
	if err != nil {
		t.Fatal(err)
	}
	volume := raw.(*cowVolume)
	if err := os.WriteFile(volume.ImagePath(), []byte("guest-disk-data"), 0o600); err != nil {
		t.Fatal(err)
	}
	state := filepath.Join(t.TempDir(), "state")
	memory := filepath.Join(t.TempDir(), "memory")
	if err := os.WriteFile(state, []byte("vm-state"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(memory, []byte("vm-memory"), 0o600); err != nil {
		t.Fatal(err)
	}
	files := SnapshotFiles{State: state, Memory: memory, Compatibility: testCompatibility()}
	var bundle bytes.Buffer
	if err := volume.Checkpoint(t.Context(), []string{".remount"}, files, 7, &bundle); err != nil {
		t.Fatal(err)
	}
	if err := volume.Destroy(context.Background()); err != nil {
		t.Fatal(err)
	}
	restored, err := provider.Create(t.Context(), "ws_bundle", base, bytes.NewReader(bundle.Bytes()))
	if err != nil {
		t.Fatal(err)
	}
	disk, err := os.ReadFile(restored.ImagePath())
	if err != nil || string(disk) != "guest-disk-data" {
		t.Fatalf("disk = %q, %v", disk, err)
	}
	gotFiles, generation, ok := restored.MemorySnapshot()
	if !ok || generation != 7 {
		t.Fatalf("memory snapshot = %+v gen=%d ok=%v", gotFiles, generation, ok)
	}
	provider.compat.HostKernel = "different"
	if err := restored.(*cowVolume).restore(t.Context(), bytes.NewReader(bundle.Bytes())); err == nil {
		t.Fatal("incompatible host accepted full checkpoint")
	}
}

func TestFullBundleRejectsUnsupportedExclusion(t *testing.T) {
	provider, base := testVolumeProvider(t)
	raw, err := provider.Create(t.Context(), "ws_exclude", base, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := raw.Checkpoint(t.Context(), []string{"node_modules"}, SnapshotFiles{}, 1, io.Discard); err == nil {
		t.Fatal("process-preserving checkpoint silently omitted an arbitrary path")
	}
}
