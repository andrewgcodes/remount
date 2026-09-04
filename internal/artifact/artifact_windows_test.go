//go:build windows

package artifact

import (
	"archive/tar"
	"bytes"
	"io"
	"os"
	"path/filepath"
	"testing"
)

func TestWindowsArchiveAliasesCannotTargetReservedWorkspaceState(t *testing.T) {
	root := t.TempDir()
	if err := os.MkdirAll(filepath.Join(root, ".remount"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, ".remount", "env"), []byte("safe"), 0o600); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{".remount./env", ".remount /env", "file.txt:stream", "CON", "nul.txt"} {
		t.Run(name, func(t *testing.T) {
			archive := buildTar(t, map[string]string{name: "attacker"})
			if _, err := ApplyOverlay(root, bytes.NewReader(archive), RestoreLimits{}); err == nil {
				t.Fatalf("Windows-ambiguous archive entry %q accepted", name)
			}
			got, err := os.ReadFile(filepath.Join(root, ".remount", "env"))
			if err != nil || string(got) != "safe" {
				t.Fatalf("reserved state changed to %q: %v", got, err)
			}
		})
	}
}

func TestWindowsOverlayOpenDestinationIsAtomic(t *testing.T) {
	root := t.TempDir()
	path := filepath.Join(root, "open.txt")
	if err := os.WriteFile(path, []byte("old"), 0o644); err != nil {
		t.Fatal(err)
	}
	reader, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	archive := buildTar(t, map[string]string{"open.txt": "new"})
	_, applyErr := ApplyOverlay(root, bytes.NewReader(archive), RestoreLimits{})
	current, readErr := os.ReadFile(path)
	if readErr != nil {
		t.Fatal(readErr)
	}
	if applyErr == nil {
		if string(current) != "new" {
			t.Fatalf("successful overlay left %q", current)
		}
	} else if string(current) != "old" {
		t.Fatalf("failed overlay exposed %q instead of old bytes: %v", current, applyErr)
	}
	if _, err := reader.Seek(0, io.SeekStart); err != nil {
		t.Fatal(err)
	}
	opened, err := io.ReadAll(reader)
	if err != nil {
		t.Fatal(err)
	}
	if string(opened) != "old" {
		t.Fatalf("open handle observed %q instead of old bytes", opened)
	}
}

func TestWindowsSnapshotPreservesLineEndingBytes(t *testing.T) {
	root := t.TempDir()
	want := []byte("one\ntwo\r\nthree\n")
	if err := os.WriteFile(filepath.Join(root, "mixed.txt"), want, 0o644); err != nil {
		t.Fatal(err)
	}
	var archive bytes.Buffer
	if err := Snapshot(root, nil, &archive); err != nil {
		t.Fatal(err)
	}
	restored := filepath.Join(t.TempDir(), "restored")
	if err := os.Mkdir(restored, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := Restore(restored, bytes.NewReader(archive.Bytes())); err != nil {
		t.Fatal(err)
	}
	got, err := os.ReadFile(filepath.Join(restored, "mixed.txt"))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("snapshot changed bytes: got %q want %q", got, want)
	}
}

func TestWindowsRestorePreservesReadOnlyAttribute(t *testing.T) {
	root := filepath.Join(t.TempDir(), "restored")
	if err := os.Mkdir(root, 0o755); err != nil {
		t.Fatal(err)
	}
	archive := buildTarEntries(t, testTarEntry{
		header: tar.Header{Name: "readonly.txt", Typeflag: tar.TypeReg, Mode: 0o444},
		body:   "fixed",
	})
	if err := Restore(root, bytes.NewReader(archive)); err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(filepath.Join(root, "readonly.txt"))
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode().Perm()&0o200 != 0 {
		t.Fatalf("restored file is writable: mode %s", info.Mode())
	}
}
