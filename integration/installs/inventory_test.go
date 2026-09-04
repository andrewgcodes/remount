package installs

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// distArtifacts is the release file set: the six platform binaries `make dist`
// builds, and nothing else that happens to be in the output directory.
func distArtifacts(t *testing.T) []string {
	t.Helper()
	dir := distDir(t)
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	var names []string
	for _, e := range entries {
		if !e.IsDir() && strings.HasPrefix(e.Name(), "remount-") {
			names = append(names, e.Name())
		}
	}
	if len(names) < 6 {
		t.Fatalf("dist holds %d platform artifacts, want the six the Makefile builds: %v", len(names), names)
	}
	return names
}

func sha256File(t *testing.T, path string) string {
	t.Helper()
	body, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256(body)
	return hex.EncodeToString(sum[:])
}

// TestB32AChecksumManifestTravelsWithTheArtifactsAndCatchesDamage installs the
// artifacts and their checksums the way a release publishes them — the
// `sha256sum` line format .github/workflows/release.yml writes — and verifies
// them where they landed rather than where they were built.
func TestB32AChecksumManifestTravelsWithTheArtifactsAndCatchesDamage(t *testing.T) {
	if testing.Short() {
		t.Skip("unavailable: skipped under -short")
	}
	dist := distDir(t)
	install := t.TempDir()

	manifest := &strings.Builder{}
	for _, name := range distArtifacts(t) {
		body, err := os.ReadFile(filepath.Join(dist, name))
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(install, name), body, 0o755); err != nil {
			t.Fatal(err)
		}
		sum := sha256.Sum256(body)
		fmt.Fprintf(manifest, "%s  %s\n", hex.EncodeToString(sum[:]), name)
	}
	if err := os.WriteFile(filepath.Join(install, "checksums.txt"), []byte(manifest.String()), 0o644); err != nil {
		t.Fatal(err)
	}

	verify := func() error {
		body, err := os.ReadFile(filepath.Join(install, "checksums.txt"))
		if err != nil {
			return err
		}
		for _, line := range strings.Split(strings.TrimSpace(string(body)), "\n") {
			want, name, ok := strings.Cut(line, "  ")
			if !ok {
				return fmt.Errorf("malformed checksum line %q", line)
			}
			if got := sha256File(t, filepath.Join(install, name)); got != want {
				return fmt.Errorf("%s: manifest says %s, the installed file is %s", name, want, got)
			}
		}
		return nil
	}
	if err := verify(); err != nil {
		t.Fatalf("the installed artifacts do not match the manifest that travelled with them: %v", err)
	}

	// The control. A verifier that reports success on damaged bytes is worse
	// than none, so one artifact is damaged and the same verifier must object.
	damaged := filepath.Join(install, distArtifacts(t)[0])
	body, err := os.ReadFile(damaged)
	if err != nil {
		t.Fatal(err)
	}
	body[len(body)/2] ^= 0xff
	if err := os.WriteFile(damaged, body, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := verify(); err == nil {
		t.Fatal("the checksum verifier accepted a byte-flipped artifact")
	}
}

// buildInfo is one `go version -m` line set: the component inventory the Go
// toolchain can read back out of a finished binary.
type buildInfo struct {
	main string
	deps map[string]string
}

// readBuildInfo reads the installed binary's own inventory. It runs where the
// binary was installed, with cleanEnv, so what it reports comes from the file
// rather than from the module cache or the checkout.
func readBuildInfo(t *testing.T, bin, workdir string) buildInfo {
	t.Helper()
	out := runClean(t, workdir, cleanEnv(t), "go", "version", "-m", bin)
	info := buildInfo{deps: map[string]string{}}
	for _, line := range strings.Split(out, "\n") {
		fields := strings.Fields(strings.TrimSpace(line))
		if len(fields) < 3 {
			continue
		}
		switch fields[0] {
		case "mod":
			info.main = fields[1]
		case "dep":
			info.deps[fields[1]] = fields[2]
		}
	}
	return info
}

// TestB32TheInstalledBinaryCarriesItsOwnComponentInventory is B32's SBOM lane
// as this host can honestly serve it.
//
// The release SBOM is an SPDX document produced by syft, which is not
// installed here; TestB32TheSPDXSBOMLaneNeedsSyft records that as unavailable
// rather than passing something else off as it. What is available without any
// third-party tool is the inventory the Go toolchain embeds in the artifact
// itself: every module, its version, and the hash it was built from. It is
// read out of the installed file with no source tree in sight, which is the
// property B32 cares about — an SBOM you can only produce next to the
// checkout describes the checkout, not the artifact.
func TestB32TheInstalledBinaryCarriesItsOwnComponentInventory(t *testing.T) {
	if testing.Short() {
		t.Skip("unavailable: skipped under -short")
	}
	root := repoRoot(t)
	prefix := filepath.Join(t.TempDir(), "opt", "remount")
	goos, goarch := hostPlatform()
	bin := installBinary(t, prefix, goos, goarch)

	info := readBuildInfo(t, bin, prefix)
	if info.main != "remount.dev/remount" {
		t.Fatalf("the installed binary names main module %q", info.main)
	}

	// Every module go.mod requires directly must appear, at the version this
	// module pins. An inventory that omits a dependency is not an inventory.
	var mod struct {
		Require []struct {
			Path     string
			Version  string
			Indirect bool
		}
	}
	if err := json.Unmarshal([]byte(runClean(t, root, os.Environ(), "go", "mod", "edit", "-json")), &mod); err != nil {
		t.Fatal(err)
	}
	direct := 0
	for _, req := range mod.Require {
		if req.Indirect {
			continue
		}
		direct++
		got, ok := info.deps[req.Path]
		if !ok {
			// creack/pty and golang.org/x/term are platform-conditional, so a
			// missing module is only a defect when the build could reach it.
			t.Logf("the inventory does not name %s; it may be excluded by build constraints on %s/%s", req.Path, goos, goarch)
			continue
		}
		if got != req.Version {
			t.Errorf("the installed binary reports %s %s, go.mod requires %s", req.Path, got, req.Version)
		}
	}
	if direct == 0 {
		t.Fatal("go.mod declares no direct requirement; the comparison above proves nothing")
	}
	if len(info.deps) < direct {
		t.Fatalf("the inventory names %d modules, fewer than the %d go.mod requires directly", len(info.deps), direct)
	}
	t.Logf("inventory of %s: main %s, %d modules", filepath.Base(bin), info.main, len(info.deps))
}

// TestB32TheSPDXSBOMLaneNeedsSyft is the lane the release actually uses.
// Plan B §6.2 has no skipped-success: on a host without syft this is
// `unavailable` and names why, and it runs for real wherever syft exists.
func TestB32TheSPDXSBOMLaneNeedsSyft(t *testing.T) {
	if testing.Short() {
		t.Skip("unavailable: skipped under -short")
	}
	syft := requireTool(t, "syft")
	prefix := filepath.Join(t.TempDir(), "opt", "remount")
	goos, goarch := hostPlatform()
	bin := installBinary(t, prefix, goos, goarch)

	out := runClean(t, prefix, cleanEnv(t), syft, "scan", "file:"+bin, "-o", "spdx-json")
	if !strings.Contains(out, "remount.dev/remount") {
		t.Fatalf("the SPDX document does not name the main module:\n%s", out)
	}
}
