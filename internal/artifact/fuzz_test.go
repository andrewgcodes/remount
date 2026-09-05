package artifact

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"os"
	"path"
	"path/filepath"
	"strings"
	"testing"
)

func FuzzRestore(f *testing.F) {
	f.Add(fuzzArchiveSeed())
	f.Add([]byte("not a gzip stream"))
	f.Add([]byte{0x1f, 0x8b, 0x08})
	f.Fuzz(func(t *testing.T, archive []byte) {
		if len(archive) > 1<<20 {
			t.Skip()
		}
		root := t.TempDir()
		limits := RestoreLimits{
			MaxCompressedBytes: 1 << 20, MaxExpandedBytes: 2 << 20,
			MaxFileBytes: 1 << 20, MaxEntries: 128, MaxPathBytes: 256,
			MaxDepth: 16, MaxCompressionRatio: 100,
		}
		_ = RestoreWithLimits(root, bytes.NewReader(archive), limits)
		if info, err := os.Lstat(root); err != nil || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
			t.Fatalf("restore root lost containment: info=%v err=%v", info, err)
		}
	})
}

func fuzzArchiveSeed() []byte {
	var output bytes.Buffer
	gzipWriter := gzip.NewWriter(&output)
	tarWriter := tar.NewWriter(gzipWriter)
	body := []byte("seed")
	if err := tarWriter.WriteHeader(&tar.Header{Name: "dir/file.txt", Mode: 0o600, Size: int64(len(body)), Typeflag: tar.TypeReg}); err != nil {
		panic(err)
	}
	if _, err := tarWriter.Write(body); err != nil {
		panic(err)
	}
	if err := tarWriter.Close(); err != nil {
		panic(err)
	}
	if err := gzipWriter.Close(); err != nil {
		panic(err)
	}
	return output.Bytes()
}

func FuzzPortableSymlinkTarget(f *testing.F) {
	sourceRoot, err := filepath.Abs("fuzz-workspace")
	if err != nil {
		f.Fatal(err)
	}
	f.Add(".codex/tmp/arg0/apply_patch", "../../.npm/bin/tool", "")
	f.Add(".codex/tmp/arg0/apply_patch", filepath.Join(sourceRoot, ".npm", "bin", "tool"), "")
	f.Add("bin/tool", filepath.Join(filepath.Dir(sourceRoot), "external"), "")
	f.Add("workspace/bin/tool", filepath.Join(sourceRoot, "lib", "tool"), "workspace")
	f.Add("escape", "../../outside", "")
	f.Add("escape", `safe\child/../..`, "")
	f.Fuzz(func(t *testing.T, name, target, archivePrefix string) {
		portable, err := PortableSymlinkTarget(name, target, sourceRoot, archivePrefix)
		if err != nil {
			return
		}
		if filepath.IsAbs(portable) || path.IsAbs(portable) || filepath.VolumeName(portable) != "" {
			t.Fatalf("portable target is absolute: %q", portable)
		}
		resolved := path.Clean(path.Join(path.Dir(filepath.ToSlash(name)), portable))
		if resolved == ".." || strings.HasPrefix(resolved, "../") {
			t.Fatalf("portable target escaped archive: name=%q target=%q result=%q", name, target, portable)
		}
		absolute := filepath.IsAbs(target) || path.IsAbs(target) || filepath.VolumeName(target) != ""
		if !absolute {
			if portable != filepath.ToSlash(target) {
				t.Fatalf("relative target changed: target=%q result=%q", target, portable)
			}
			return
		}
		targetRel, relErr := filepath.Rel(sourceRoot, filepath.Clean(target))
		if relErr != nil || targetRel == ".." || strings.HasPrefix(targetRel, ".."+string(filepath.Separator)) {
			t.Fatalf("external absolute target accepted: %q", target)
		}
		want := filepath.ToSlash(targetRel)
		if archivePrefix != "" {
			want = path.Join(archivePrefix, want)
		}
		if resolved != path.Clean(want) {
			t.Fatalf("absolute target changed: name=%q target=%q prefix=%q result=%q resolved=%q want=%q",
				name, target, archivePrefix, portable, resolved, want)
		}
	})
}

// FuzzApplyOverlay checks that no archive can make an overlay write outside
// the root, into .remount, or leave anything behind in the staging area.
func FuzzApplyOverlay(f *testing.F) {
	f.Add(fuzzArchiveSeed())
	f.Add([]byte("not a gzip stream"))
	f.Add([]byte{0x1f, 0x8b, 0x08})
	f.Fuzz(func(t *testing.T, archive []byte) {
		if len(archive) > 1<<20 {
			t.Skip()
		}
		parent := t.TempDir()
		root := filepath.Join(parent, "ws")
		if err := os.MkdirAll(filepath.Join(root, OverlayStageDir), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(root, OverlayStageDir, "env"), []byte("keep"), 0o600); err != nil {
			t.Fatal(err)
		}
		limits := RestoreLimits{
			MaxCompressedBytes: 1 << 20, MaxExpandedBytes: 2 << 20,
			MaxFileBytes: 1 << 20, MaxEntries: 128, MaxPathBytes: 256,
			MaxDepth: 16, MaxCompressionRatio: 100,
		}
		_, _ = ApplyOverlay(root, bytes.NewReader(archive), limits)
		entries, err := os.ReadDir(parent)
		if err != nil || len(entries) != 1 || entries[0].Name() != "ws" {
			t.Fatalf("overlay escaped the root: %v %v", entries, err)
		}
		if got, err := os.ReadFile(filepath.Join(root, OverlayStageDir, "env")); err != nil || string(got) != "keep" {
			t.Fatalf(".remount/env changed: %q %v", got, err)
		}
		stage, err := os.ReadDir(filepath.Join(root, OverlayStageDir))
		if err != nil || len(stage) != 1 {
			t.Fatalf("staging area not cleaned: %v %v", stage, err)
		}
	})
}
