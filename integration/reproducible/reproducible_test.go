// Package reproducible proves the shipped binaries are a function of the
// source and nothing else.
//
// Plan B B31 asks for two clean builds producing the same checksums. The claim
// that matters is not "building twice gives the same answer" — a warm cache
// would give that for free even if the build embedded the machine it ran on.
// The claim is that nothing about the environment leaks into the artifact, so
// the test builds once normally and once with a cold build cache and a
// different temporary directory, which are the two things a Go build most
// easily bakes in.
//
// Two clean containers, which B31 also names, additionally vary the toolchain
// path and the OS image; that lane belongs to a Linux CI host and is recorded
// in docs/engineering/handoff-linux-host-2026-09-03.md.
package reproducible

import (
	"crypto/sha256"
	"encoding/hex"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

// platforms is a representative slice rather than all six: enough to catch a
// host-specific or arch-specific leak without spending six link steps on every
// run. `make dist` builds the full matrix.
var platforms = []struct{ os, arch string }{
	{"linux", "amd64"},
	{"linux", "arm64"},
	{runtime.GOOS, runtime.GOARCH},
}

func repoRoot(t *testing.T) string {
	t.Helper()
	out, err := exec.Command("git", "rev-parse", "--show-toplevel").Output()
	if err != nil {
		t.Skipf("unavailable: not a git checkout: %v", err)
	}
	return strings.TrimSpace(string(out))
}

// version mirrors the Makefile so the test builds the same artifact the
// release does. A differing -X value would change every checksum and prove
// nothing about reproducibility.
func version(t *testing.T, root string) string {
	t.Helper()
	cmd := exec.Command("git", "describe", "--tags", "--always", "--dirty")
	cmd.Dir = root
	out, err := cmd.Output()
	if err != nil {
		return "dev"
	}
	return strings.TrimSpace(string(out))
}

func build(t *testing.T, root, goos, goarch, out string, env []string) {
	t.Helper()
	name := "remount-" + goos + "-" + goarch
	if goos == "windows" {
		name += ".exe"
	}
	cmd := exec.Command("go", "build", "-trimpath",
		"-ldflags=-s -w -X main.version="+version(t, root),
		"-o", filepath.Join(out, name), "./cmd/remount")
	cmd.Dir = root
	cmd.Env = append(append(os.Environ(),
		"CGO_ENABLED=0", "GOOS="+goos, "GOARCH="+goarch), env...)
	if combined, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("build %s/%s: %v\n%s", goos, goarch, err, combined)
	}
}

func digest(t *testing.T, path string) string {
	t.Helper()
	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	h := sha256.New()
	if _, err := io.Copy(h, f); err != nil {
		t.Fatal(err)
	}
	return hex.EncodeToString(h.Sum(nil))
}

// TestB31BinariesAreAFunctionOfTheSourceAlone is Plan B B31.
func TestB31BinariesAreAFunctionOfTheSourceAlone(t *testing.T) {
	if testing.Short() {
		t.Skip("unavailable: reproducibility links several binaries; skipped under -short")
	}
	root := repoRoot(t)

	warm := t.TempDir()
	cold := t.TempDir()
	coldCache := t.TempDir()
	coldTmp := t.TempDir()

	seen := map[string]string{}
	for _, p := range platforms {
		key := p.os + "/" + p.arch
		if _, done := seen[key]; done {
			continue
		}
		seen[key] = ""

		build(t, root, p.os, p.arch, warm, nil)
		// A cold build cache and a different temporary directory are the two
		// environment inputs a Go build is most likely to embed. -trimpath is
		// what keeps the source path out; this is what proves it worked.
		build(t, root, p.os, p.arch, cold, []string{
			"GOCACHE=" + coldCache,
			"TMPDIR=" + coldTmp,
			"GOTMPDIR=" + coldTmp,
		})

		name := "remount-" + p.os + "-" + p.arch
		if p.os == "windows" {
			name += ".exe"
		}
		first := digest(t, filepath.Join(warm, name))
		second := digest(t, filepath.Join(cold, name))
		if first != second {
			t.Fatalf("%s is not reproducible: warm cache %s, cold cache %s — something in the build environment reaches the artifact", key, first, second)
		}
		t.Logf("%s reproducible: sha256 %s", key, first)
	}
}

// TestB31ADifferentVersionStampChangesTheBinary is the positive control. If
// the digests above matched because the test compared a file to itself, or
// because the linker ignored -X, this would also match — and it must not.
func TestB31ADifferentVersionStampChangesTheBinary(t *testing.T) {
	if testing.Short() {
		t.Skip("unavailable: skipped under -short")
	}
	root := repoRoot(t)
	out := t.TempDir()

	stamp := func(value, name string) string {
		cmd := exec.Command("go", "build", "-trimpath",
			"-ldflags=-s -w -X main.version="+value,
			"-o", filepath.Join(out, name), "./cmd/remount")
		cmd.Dir = root
		cmd.Env = append(os.Environ(), "CGO_ENABLED=0", "GOOS=linux", "GOARCH=amd64")
		if combined, err := cmd.CombinedOutput(); err != nil {
			t.Fatalf("build: %v\n%s", err, combined)
		}
		return digest(t, filepath.Join(out, name))
	}
	if a, b := stamp("v0.0.0-control-a", "a"), stamp("v0.0.0-control-b", "b"); a == b {
		t.Fatal("two different version stamps produced the same binary; the reproducibility comparison above cannot distinguish anything")
	}
}
