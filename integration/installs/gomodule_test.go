package installs

import (
	"archive/zip"
	"encoding/json"
	"fmt"
	"net/url"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

// The version the module artifact is published under inside this test. It is
// a local, disposable coordinate: nothing here tags a commit or pushes
// anything, which Plan B §18 cuts.
const moduleVersion = "v0.1.0-b32install"

const modulePath = "remount.dev/remount"

// packagedFiles is the file set a published module carries: what git tracks,
// read from the working tree, minus the directories that are their own
// modules. Using the tracked set rather than the directory means dist/, the
// data directories and anything else a working checkout accumulates cannot
// smuggle itself into the artifact.
func packagedFiles(t *testing.T, root string) []string {
	t.Helper()
	cmd := exec.Command("git", "ls-files", "-z")
	cmd.Dir = root
	out, err := cmd.Output()
	if err != nil {
		t.Skipf("unavailable: git ls-files: %v", err)
	}
	var tracked []string
	for _, name := range strings.Split(strings.TrimRight(string(out), "\x00"), "\x00") {
		if name != "" {
			tracked = append(tracked, name)
		}
	}
	var nested []string
	for _, name := range tracked {
		if path.Base(name) == "go.mod" && name != "go.mod" {
			nested = append(nested, path.Dir(name)+"/")
		}
	}
	var packaged []string
	for _, name := range tracked {
		skip := false
		for _, dir := range nested {
			if strings.HasPrefix(name, dir) {
				skip = true
				break
			}
		}
		// A module zip holds regular files only; a symlink would resolve
		// against whatever tree it was extracted next to.
		if info, err := os.Lstat(filepath.Join(root, name)); err != nil || !info.Mode().IsRegular() {
			skip = true
		}
		if !skip {
			packaged = append(packaged, name)
		}
	}
	return packaged
}

// localProxy writes the module artifact into a directory laid out as a Go
// module proxy. This is how a module is installed without publishing it: the
// consumer resolves the same coordinates it would resolve from a registry,
// against a file:// origin that exists only for this test.
func localProxy(t *testing.T, root string) string {
	t.Helper()
	proxy := t.TempDir()
	dir := filepath.Join(proxy, filepath.FromSlash(modulePath), "@v")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}

	zipPath := filepath.Join(dir, moduleVersion+".zip")
	f, err := os.Create(zipPath)
	if err != nil {
		t.Fatal(err)
	}
	w := zip.NewWriter(f)
	prefix := modulePath + "@" + moduleVersion + "/"
	for _, name := range packagedFiles(t, root) {
		body, err := os.ReadFile(filepath.Join(root, name))
		if err != nil {
			t.Fatal(err)
		}
		entry, err := w.Create(prefix + name)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := entry.Write(body); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}

	gomod, err := os.ReadFile(filepath.Join(root, "go.mod"))
	if err != nil {
		t.Fatal(err)
	}
	write := func(name, body string) {
		if err := os.WriteFile(filepath.Join(dir, name), []byte(body), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	write(moduleVersion+".mod", string(gomod))
	write(moduleVersion+".info", fmt.Sprintf(`{"Version":%q,"Time":"2026-09-03T00:00:00Z"}`, moduleVersion))
	write("list", moduleVersion+"\n")
	return proxy
}

// consumerEnv is a Go environment with no memory of this machine's checkout:
// its own module cache and build cache, an empty `go env` file so the user's
// defaults cannot apply, and a proxy chain that serves the module artifact
// first and this host's existing download cache second. The second entry is
// how the third-party dependencies resolve without a network; it holds no
// Remount code, and the assertions below prove the consumer took Remount from
// the artifact.
func consumerEnv(t *testing.T, sandbox, proxy string) []string {
	t.Helper()
	hostCache := strings.TrimSpace(runClean(t, sandbox, os.Environ(), "go", "env", "GOMODCACHE"))
	if hostCache == "" {
		t.Skip("unavailable: this host reports no GOMODCACHE, so dependencies cannot be resolved offline")
	}
	goenv := filepath.Join(sandbox, "goenv")
	if err := os.WriteFile(goenv, nil, 0o644); err != nil {
		t.Fatal(err)
	}
	fileURL := func(dir string) string {
		slash := filepath.ToSlash(dir)
		if runtime.GOOS == "windows" {
			slash = "/" + slash
		}
		return (&url.URL{Scheme: "file", Path: slash}).String()
	}
	env := cleanEnv(t,
		"GOENV="+goenv,
		"GOMODCACHE="+filepath.Join(sandbox, "modcache"),
		"GOCACHE="+filepath.Join(sandbox, "buildcache"),
		"GOPROXY="+fileURL(proxy)+","+fileURL(filepath.Join(hostCache, "cache", "download")),
		"GOSUMDB=off",
		"GOFLAGS=-mod=mod",
		"GOTOOLCHAIN=local",
		"CGO_ENABLED=0",
	)
	// A module cache is written read-only, so the disposable environment has
	// to be disposed of deliberately rather than deleted.
	t.Cleanup(func() {
		if out, err := runCleanErr(t, sandbox, env, "go", "clean", "-modcache"); err != nil {
			t.Errorf("the consumer's module cache did not clean up: %v\n%s", err, out)
		}
	})
	return env
}

// consumerProgram is a whole agent-facing use of the SDK: create a workspace,
// wait for it to be claimed, run a command in it, and destroy it. It imports
// only the two public packages, so it also re-proves what integration/publicsdk
// proves — with the difference that matters to B32: publicsdk reaches the
// module through a `replace` into this checkout, and this one cannot, because
// the checkout is not on its GOPROXY.
const consumerProgram = `package main

import (
	"context"
	"fmt"
	"os"
	"runtime"
	"time"

	"remount.dev/remount/api"
	"remount.dev/remount/client"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "consumer:", err)
		os.Exit(1)
	}
}

func run() error {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	c, err := client.New(client.Options{Server: os.Args[1], Retries: 3})
	if err != nil {
		return err
	}
	defer c.Close()

	ws, err := c.CreateWorkspace(ctx, api.WorkspaceSpec{
		Name:     "b32-go-module-consumer",
		Security: api.SecuritySpec{Profile: api.SecurityLocal},
	}, client.WithIdempotencyKey("b32-consumer-create"))
	if err != nil {
		return err
	}
	if _, err := c.WaitClaimed(ctx, ws.ID); err != nil {
		return err
	}
	program := []string{"/bin/echo", "installed-from-the-module-artifact"}
	if runtime.GOOS == "windows" {
		program = []string{"cmd.exe", "/d", "/s", "/c", "echo installed-from-the-module-artifact"}
	}
	stdout, _, exit, err := c.Run(ctx, ws.ID, program...)
	if err != nil {
		return err
	}
	if exit == nil || exit.Code != 0 {
		return fmt.Errorf("the command did not exit cleanly: %+v", exit)
	}
	if err := c.DestroyWorkspace(ctx, ws.ID, client.WithIdempotencyKey("b32-consumer-destroy")); err != nil {
		return err
	}
	fmt.Print(string(stdout))
	return nil
}
`

// moduleOrigin is where the consumer's build actually took Remount from.
// `go list -m` answers with the resolved directory and any replacement, which
// is the one question B32 asks of a Go install: did this come from the
// artifact, or from the tree next door?
type moduleOrigin struct {
	Dir     string
	Version string
	Replace *struct {
		Path string
		Dir  string
	}
}

func moduleOriginOf(t *testing.T, dir string, env []string) moduleOrigin {
	t.Helper()
	out := runClean(t, dir, env, "go", "list", "-m", "-json", modulePath)
	var origin moduleOrigin
	if err := json.Unmarshal([]byte(out), &origin); err != nil {
		t.Fatalf("decode `go list -m -json %s`: %v\n%s", modulePath, err, out)
	}
	return origin
}

// newConsumer writes a module outside the repository that requires Remount at
// the packaged version.
func newConsumer(t *testing.T, sandbox, extra string) string {
	t.Helper()
	dir := filepath.Join(sandbox, "consumer")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	gomod := fmt.Sprintf("module example.com/b32-consumer\n\ngo 1.27.1\n\nrequire %s %s\n%s", modulePath, moduleVersion, extra)
	if err := os.WriteFile(filepath.Join(dir, "go.mod"), []byte(gomod), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "main.go"), []byte(consumerProgram), 0o644); err != nil {
		t.Fatal(err)
	}
	return dir
}

// TestB32AGoModuleConsumerBuildsFromTheArtifactAndDrivesTheInstalledServer is
// the Go install lane: a module that is not in this repository, resolving
// Remount from the packaged module artifact rather than from the checkout, and
// using it to drive a server installed from the dist binary.
func TestB32AGoModuleConsumerBuildsFromTheArtifactAndDrivesTheInstalledServer(t *testing.T) {
	if testing.Short() {
		t.Skip("unavailable: skipped under -short")
	}
	root := repoRoot(t)
	sandbox := t.TempDir()
	env := consumerEnv(t, sandbox, localProxy(t, root))
	dir := newConsumer(t, sandbox, "")

	runClean(t, dir, env, "go", "mod", "tidy")
	if body, err := os.ReadFile(filepath.Join(dir, "go.mod")); err != nil {
		t.Fatal(err)
	} else if strings.Contains(string(body), "replace") {
		t.Fatalf("the consumer acquired a replace directive:\n%s", body)
	}

	origin := moduleOriginOf(t, dir, env)
	if origin.Replace != nil {
		t.Fatalf("%s was replaced by %+v; the build did not use the artifact", modulePath, origin.Replace)
	}
	if strings.HasPrefix(origin.Dir, root+string(os.PathSeparator)) || origin.Dir == root {
		t.Fatalf("%s resolved to %s, inside the checkout", modulePath, origin.Dir)
	}
	if !strings.HasPrefix(origin.Dir, sandbox) {
		t.Fatalf("%s resolved to %s, outside this test's module cache", modulePath, origin.Dir)
	}

	bin := filepath.Join(sandbox, "consumer.bin")
	runClean(t, dir, env, "go", "build", "-o", bin, ".")
	// Built without -trimpath on purpose: the paths it bakes in are this
	// test's sandbox, and a byte of the checkout in here would mean the
	// compiler read the checkout.
	requireNoSourceTree(t, bin, root)

	prefix := filepath.Join(t.TempDir(), "opt", "remount")
	goos, goarch := hostPlatform()
	endpoint := startInstalled(t, installBinary(t, prefix, goos, goarch))

	out := runClean(t, sandbox, cleanEnv(t), bin, endpoint)
	if strings.TrimSpace(out) != "installed-from-the-module-artifact" {
		t.Fatalf("the consumer printed %q", out)
	}
	t.Logf("consumer built from %s %s drove %s", modulePath, origin.Version, endpoint)
}

// TestB32AReplaceIntoTheCheckoutIsCaught is the control for the lane above.
// The assertions there pass; assertions that cannot fail would also pass. This
// reintroduces exactly the dependency B32 forbids — a `replace` pointing at the
// source tree, which is how integration/publicsdk builds — and requires the
// same checks to catch it.
func TestB32AReplaceIntoTheCheckoutIsCaught(t *testing.T) {
	if testing.Short() {
		t.Skip("unavailable: skipped under -short")
	}
	root := repoRoot(t)
	sandbox := t.TempDir()
	env := consumerEnv(t, sandbox, localProxy(t, root))
	dir := newConsumer(t, sandbox, fmt.Sprintf("\nreplace %s => %s\n", modulePath, root))

	runClean(t, dir, env, "go", "mod", "tidy")
	origin := moduleOriginOf(t, dir, env)
	if origin.Replace == nil {
		t.Fatal("a consumer with a replace into the checkout reported no replacement; moduleOriginOf cannot detect one")
	}
	if origin.Dir != root {
		t.Fatalf("the replaced module resolved to %s, not the checkout %s; the directory check proves nothing", origin.Dir, root)
	}

	bin := filepath.Join(sandbox, "replaced.bin")
	runClean(t, dir, env, "go", "build", "-o", bin, ".")
	if !containsSourceTree(t, bin, root) {
		t.Fatalf("a binary compiled from %s carries no reference to it; the source-tree scan cannot distinguish a clean build", root)
	}
}
