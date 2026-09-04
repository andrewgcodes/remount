//go:build windows

package fsops

import (
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"golang.org/x/sys/windows"

	"remount.dev/remount/internal/proto"
)

func TestWindowsHostilePathsAreRejectedBeforeFilesystemAccess(t *testing.T) {
	f, root := newFS(t)
	for _, name := range []string{
		`C:relative`, `C:\absolute`, `C:/absolute`, `\\server\share\file`,
		`\\?\C:\device`, `CON`, `con.txt`, `NUL`, `AUX.log`, `COM1`, `LPT9.txt`,
		`file.txt:stream`, `.remount.`, `.remount `, `dir.\file`, `dir \file`,
	} {
		t.Run(strings.ReplaceAll(name, "/", "_"), func(t *testing.T) {
			if err := f.Write(name, []byte("forbidden"), 0, false, true); codeOf(err) != proto.CodeBadRequest {
				t.Fatalf("Write(%q) code = %q: %v", name, codeOf(err), err)
			}
		})
	}
	entries, err := os.ReadDir(root)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 0 {
		t.Fatalf("hostile paths created entries: %v", entries)
	}
}

func TestWindowsJunctionParentCannotEscapeRoot(t *testing.T) {
	parent := t.TempDir()
	root := filepath.Join(parent, "workspace")
	outside := filepath.Join(parent, "outside")
	for _, dir := range []string{root, outside} {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			t.Fatal(err)
		}
	}
	link := filepath.Join(root, "junction")
	out, err := exec.Command("cmd", "/c", "mklink", "/J", link, outside).CombinedOutput()
	if err != nil {
		t.Skipf("Windows junction unavailable: %v: %s", err, out)
	}
	f, err := New(root)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	if err := f.Write(`junction\victim`, []byte("escaped"), 0, false, false); codeOf(err) != proto.CodeDenied {
		t.Fatalf("junction escape code = %q: %v", codeOf(err), err)
	}
	if _, err := os.Stat(filepath.Join(outside, "victim")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("junction escape wrote outside root: %v", err)
	}
}

func TestWindowsCaseAliasesStayInsideRoot(t *testing.T) {
	f, root := newFS(t)
	if err := f.Write(`Mixed\Name.txt`, []byte("safe"), 0, false, true); err != nil {
		t.Fatal(err)
	}
	got, err := f.Read(`mixed/name.TXT`, 0, 0)
	if err != nil || string(got.Data) != "safe" {
		t.Fatalf("case-insensitive read = %q, %v", got.Data, err)
	}
	resolved, err := f.Resolve(`MIXED\NAME.txt`)
	if err != nil {
		t.Fatal(err)
	}
	rel, err := filepath.Rel(root, resolved)
	if err != nil || rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
		t.Fatalf("case alias escaped root: %q, %v", resolved, err)
	}
}

func TestWindowsShortNameAliasStaysInsideRoot(t *testing.T) {
	f, root := newFS(t)
	longDir := "LongDirectoryNameForShortAlias"
	if err := f.Write(longDir+`\file.txt`, []byte("safe"), 0, false, true); err != nil {
		t.Fatal(err)
	}
	longPath, err := windows.UTF16PtrFromString(filepath.Join(root, longDir))
	if err != nil {
		t.Fatal(err)
	}
	buffer := make([]uint16, windows.MAX_PATH)
	n, err := windows.GetShortPathName(longPath, &buffer[0], uint32(len(buffer)))
	if err != nil {
		t.Skipf("unavailable: Windows short-name lookup failed: %v", err)
	}
	shortDir := filepath.Base(windows.UTF16ToString(buffer[:n]))
	if !strings.Contains(shortDir, "~") {
		t.Skip("unavailable: NTFS 8.3 short-name generation is disabled")
	}
	got, err := f.Read(shortDir+`\file.txt`, 0, 0)
	if err != nil || string(got.Data) != "safe" {
		t.Fatalf("short-name read = %q, %v", got.Data, err)
	}
	resolved, err := f.Resolve(shortDir + `\file.txt`)
	if err != nil {
		t.Fatal(err)
	}
	rel, err := filepath.Rel(root, resolved)
	if err != nil || rel == ".." || strings.HasPrefix(rel, `..\`) {
		t.Fatalf("short-name alias escaped root: %q, %v", resolved, err)
	}
}

func TestWindowsOverwritePreservesReadOnlyAttribute(t *testing.T) {
	f, root := newFS(t)
	if err := f.Write("readonly.txt", []byte("old"), 0, false, false); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(root, "readonly.txt")
	if err := os.Chmod(path, 0o444); err != nil {
		t.Fatal(err)
	}
	if err := f.Write("readonly.txt", []byte("new"), 0, false, false); codeOf(err) != proto.CodeDenied {
		t.Fatalf("read-only replacement code = %q: %v", codeOf(err), err)
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode().Perm()&0o200 != 0 {
		t.Fatalf("failed replacement made read-only file writable: mode %s", info.Mode())
	}
	got, err := os.ReadFile(path)
	if err != nil || string(got) != "old" {
		t.Fatalf("failed replacement changed bytes to %q: %v", got, err)
	}
}
