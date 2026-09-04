package installs

import (
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

// offlineMarkers are the ways a package manager says it could not reach its
// index. A lane that cannot fetch its dependencies is `unavailable` with that
// reason; every other failure is a failure, so this list is deliberately
// narrow rather than a catch-all that would swallow real breakage.
var offlineMarkers = []string{
	"Temporary failure in name resolution",
	"Network is unreachable",
	"Could not fetch URL",
	"Connection refused",
	"Failed to establish a new connection",
	"getaddrinfo",
	"ENOTFOUND",
	"ECONNREFUSED",
	"network timeout",
}

func skipIfOffline(t *testing.T, what, out string, err error) {
	t.Helper()
	if err == nil {
		return
	}
	for _, marker := range offlineMarkers {
		if strings.Contains(out, marker) {
			t.Skipf("unavailable: %s could not reach its package index: %s", what, marker)
		}
	}
	t.Fatalf("%s: %v\n%s", what, err, out)
}

// venvPython creates a virtual environment and answers with its interpreter.
func venvPython(t *testing.T, dir string) string {
	t.Helper()
	python3 := requireTool(t, "python3")
	// pip is not on this host's PATH; `python3 -m pip` and `python3 -m venv`
	// are the supported spellings and the ones an install document should
	// use, so the lane proves those rather than assuming a `pip` shim.
	out, err := runCleanErr(t, filepath.Dir(dir), cleanEnv(t), python3, "-m", "venv", dir)
	if err != nil {
		t.Skipf("unavailable: python3 -m venv failed: %v\n%s", err, out)
	}
	bin := "bin"
	if runtime.GOOS == "windows" {
		bin = "Scripts"
	}
	return filepath.Join(dir, bin, "python")
}

// pythonSmoke asks the installed package where it came from and then uses it
// for a whole workspace lifecycle against the installed server. Reporting
// __file__ is the source-tree detector for this lane: a wheel install answers
// from inside the environment, an editable install answers from the checkout.
const pythonSmoke = `import asyncio, sys
import remount
from remount import Client

print(remount.__file__)

async def main() -> None:
    async with Client(sys.argv[1], "") as client:
        ws = await client.create_workspace({"name": "b32-wheel-consumer", "security": {"profile": "local"}})
        while True:
            current = await client.get_workspace(ws["id"])
            if current["state"] == "claimed":
                break
            if current["state"] in ("failed", "destroyed"):
                raise SystemExit(f"workspace reached {current['state']}")
            await asyncio.sleep(0.1)
        session = await client.exec(ws["id"], ["/bin/echo", "installed-from-the-wheel"])
        out = b""
        async for chunk in session:
            if chunk.stream == 1:
                out += chunk.data
        await client.destroy_workspace(ws["id"])
        sys.stdout.write(out.decode())

asyncio.run(main())
`

// TestB32ThePythonWheelInstallsIntoAFreshVenvAndDrivesTheInstalledServer is
// B32's Python lane: build the wheel from sdk/python, install it into an
// environment that has never seen this repository, and use it.
func TestB32ThePythonWheelInstallsIntoAFreshVenvAndDrivesTheInstalledServer(t *testing.T) {
	if testing.Short() {
		t.Skip("unavailable: skipped under -short")
	}
	root := repoRoot(t)
	sandbox := t.TempDir()

	build := venvPython(t, filepath.Join(sandbox, "buildenv"))
	wheels := filepath.Join(sandbox, "wheels")
	out, err := runCleanErr(t, sandbox, cleanEnv(t), build, "-m", "pip", "--disable-pip-version-check", "--no-input",
		"wheel", "--no-deps", "--wheel-dir", wheels, filepath.Join(root, "sdk", "python"))
	skipIfOffline(t, "building the wheel", out, err)

	built, err := filepath.Glob(filepath.Join(wheels, "remount-*.whl"))
	if err != nil || len(built) != 1 {
		t.Fatalf("expected exactly one wheel in %s, got %v (%v)", wheels, built, err)
	}
	// The wheel is the artifact from here on: it is moved away from the tree
	// that built it before anything installs it.
	wheel := filepath.Join(sandbox, filepath.Base(built[0]))
	copyFile(t, built[0], wheel, 0o644)

	run := venvPython(t, filepath.Join(sandbox, "runenv"))
	out, err = runCleanErr(t, sandbox, cleanEnv(t), run, "-m", "pip", "--disable-pip-version-check", "--no-input",
		"install", wheel)
	skipIfOffline(t, "installing the wheel", out, err)

	script := filepath.Join(sandbox, "smoke.py")
	if err := os.WriteFile(script, []byte(pythonSmoke), 0o644); err != nil {
		t.Fatal(err)
	}

	prefix := filepath.Join(t.TempDir(), "opt", "remount")
	goos, goarch := hostPlatform()
	endpoint := startInstalled(t, installBinary(t, prefix, goos, goarch))

	out = runClean(t, sandbox, cleanEnv(t), run, script, endpoint)
	location, echoed, ok := strings.Cut(strings.TrimSpace(out), "\n")
	if !ok {
		t.Fatalf("the smoke printed no output beyond %q", out)
	}
	if strings.HasPrefix(location, root+string(os.PathSeparator)) {
		t.Fatalf("the installed package resolved to %s, inside the checkout", location)
	}
	if !strings.HasPrefix(location, realPath(t, sandbox)) {
		t.Fatalf("the installed package resolved to %s, outside the environment it was installed into", location)
	}
	if strings.TrimSpace(echoed) != "installed-from-the-wheel" {
		t.Fatalf("the workspace produced %q", echoed)
	}
	t.Logf("wheel %s installed at %s drove %s", filepath.Base(wheel), location, endpoint)
}

// TestB32AnEditablePythonInstallIsCaught is the control for the location
// assertion above. An editable install is the ordinary way a Python package
// keeps depending on its source tree, and it must be visible to the same check
// that the wheel install passes.
func TestB32AnEditablePythonInstallIsCaught(t *testing.T) {
	if testing.Short() {
		t.Skip("unavailable: skipped under -short")
	}
	root := repoRoot(t)
	sandbox := t.TempDir()
	python := venvPython(t, filepath.Join(sandbox, "editable"))

	out, err := runCleanErr(t, sandbox, cleanEnv(t), python, "-m", "pip", "--disable-pip-version-check", "--no-input",
		"install", "--no-deps", "-e", filepath.Join(root, "sdk", "python"))
	skipIfOffline(t, "installing editable", out, err)

	// find_spec locates the package without importing it, so the control does
	// not need the SDK's own dependencies installed to answer the only
	// question it asks.
	location := strings.TrimSpace(runClean(t, sandbox, cleanEnv(t), python, "-c",
		"import importlib.util; print(importlib.util.find_spec('remount').origin)"))
	if !strings.HasPrefix(location, root+string(os.PathSeparator)) {
		t.Fatalf("an editable install of sdk/python reported %s, outside the checkout %s; the location check cannot tell a wheel from a source tree", location, root)
	}
}
