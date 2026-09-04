package installs

import (
	"debug/elf"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

// TestB32TheStaticBinaryInstallsAndPassesTheBlackBoxSmoke installs the file
// `make dist` produced onto a prefix of its own and judges it there.
func TestB32TheStaticBinaryInstallsAndPassesTheBlackBoxSmoke(t *testing.T) {
	if testing.Short() {
		t.Skip("unavailable: the install lanes link and run real artifacts; skipped under -short")
	}
	root := repoRoot(t)
	prefix := filepath.Join(t.TempDir(), "opt", "remount")
	goos, goarch := hostPlatform()
	bin := installBinary(t, prefix, goos, goarch)

	// A binary that names the checkout is a binary that only runs on this
	// machine. -trimpath is what keeps it out; this is what proves it worked.
	requireNoSourceTree(t, bin, root)

	version := runClean(t, prefix, cleanEnv(t), bin, "version")
	if !strings.HasPrefix(version, "remount ") {
		t.Fatalf("the installed binary answered %q to `version`", version)
	}
	t.Logf("installed %s: %s", bin, strings.TrimSpace(version))

	conformanceSmoke(t, startInstalled(t, bin), "")
}

// TestB32TheSourceTreeScanCatchesAnUntrimmedBinary is the control for
// requireNoSourceTree. The scan above passes; a scan that always passes would
// too. Dropping -trimpath is the exact defect the scan exists to catch, so the
// same scan must find it here.
func TestB32TheSourceTreeScanCatchesAnUntrimmedBinary(t *testing.T) {
	if testing.Short() {
		t.Skip("unavailable: skipped under -short")
	}
	root := repoRoot(t)
	untrimmed := filepath.Join(t.TempDir(), "remount-untrimmed")
	cmd := exec.Command("go", "build", "-o", untrimmed, "./cmd/remount")
	cmd.Dir = root
	cmd.Env = append(os.Environ(), "CGO_ENABLED=0")
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("build without -trimpath: %v\n%s", err, out)
	}
	if !containsSourceTree(t, untrimmed, root) {
		t.Fatalf("a binary built without -trimpath does not carry %s; the source-tree scan cannot distinguish anything", root)
	}
}

// TestB32TheLinuxArtifactsAreStaticallyLinked is the other half of "install it
// anywhere": a dynamically linked binary needs a loader and libraries the
// destination may not have. Go on darwin always links libSystem, so the claim
// is only checkable — and only made — for the ELF artifacts.
func TestB32TheLinuxArtifactsAreStaticallyLinked(t *testing.T) {
	if testing.Short() {
		t.Skip("unavailable: skipped under -short")
	}
	dist := distDir(t)
	for _, arch := range []string{"amd64", "arm64"} {
		path := filepath.Join(dist, distName("linux", arch))
		f, err := elf.Open(path)
		if err != nil {
			t.Fatalf("open %s: %v", path, err)
		}
		for _, prog := range f.Progs {
			if prog.Type == elf.PT_INTERP {
				t.Errorf("%s names a dynamic loader; it is not installable on a host without one", path)
			}
		}
		needed, err := f.DynString(elf.DT_NEEDED)
		// A fully static ELF has no dynamic section at all, which DynString
		// reports as an error rather than an empty list.
		if err == nil && len(needed) > 0 {
			t.Errorf("%s needs shared libraries %v", path, needed)
		}
		if err := f.Close(); err != nil {
			t.Fatal(err)
		}
	}
}
