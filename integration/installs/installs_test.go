// Package installs proves the artifacts this repository ships can be
// installed and used somewhere the source tree does not exist.
//
// Plan B B32 asks for every shipped artifact to be installed into a clean,
// disposable environment and judged by the black-box conformance suite. The
// claim that matters is not "the install command exited zero": a machine that
// already holds the source tree will install almost anything successfully, and
// the install would still be worthless on a machine that does not. The claim
// is that the artifact carries what it needs. So every lane installs into a
// temporary directory, runs with an allow-listed environment that cannot name
// the source tree, and asserts the installed thing never reaches back into it.
// Every such detector has a control test that deliberately reintroduces the
// dependency and requires the detector to catch it, because a detector nobody
// has seen fail is not yet a detector.
//
// Nothing here publishes. Plan B §18 cuts a public release, tag, image push,
// package publish or signing-identity action; these lanes build locally,
// install locally, and delete what they made.
package installs

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"
)

// inheritedPaths are the environment variables that can hand an installed
// artifact a path back into a source tree or an ambient package directory.
// cleanEnv builds its environment from an allow list rather than deleting
// these, so the list is the documentation and TestCleanEnvDropsInheritedPaths
// is the proof; a variable nobody remembered to delete is exactly how a source
// tree survives a "clean" install.
var inheritedPaths = []string{
	"GOPATH", "GOFLAGS", "GOBIN", "GOPRIVATE", "GO111MODULE", "GOMODCACHE", "GOCACHE", "GOWORK",
	"PYTHONPATH", "PYTHONHOME", "VIRTUAL_ENV",
	"NODE_PATH", "NODE_OPTIONS", "npm_config_prefix",
	"REMOUNT_SERVER", "REMOUNT_TOKEN", "REMOUNT_DATA",
}

// cleanEnv is the environment a disposable machine would have: a search path,
// a home for package-manager caches and credentials, and a temporary
// directory. HOME stays because a disposable environment still has a user and
// the caches under it are not the source tree; nothing that could name the
// source tree survives.
func cleanEnv(t *testing.T, extra ...string) []string {
	t.Helper()
	env := []string{
		"PATH=" + os.Getenv("PATH"),
		"HOME=" + os.Getenv("HOME"),
		"TMPDIR=" + os.TempDir(),
		"LANG=C",
	}
	env = append(env, extra...)
	allowed := map[string]bool{}
	for _, kv := range extra {
		if name, _, ok := strings.Cut(kv, "="); ok {
			allowed[name] = true
		}
	}
	for _, name := range inheritedPaths {
		if allowed[name] {
			continue
		}
		for _, kv := range env {
			if strings.HasPrefix(kv, name+"=") {
				t.Fatalf("cleanEnv leaked %s into an install environment", name)
			}
		}
	}
	return env
}

// TestCleanEnvDropsInheritedPaths is the control for cleanEnv itself. Every
// lane below trusts it, so it must be shown refusing a variable that is set in
// the parent process.
func TestCleanEnvDropsInheritedPaths(t *testing.T) {
	t.Setenv("PYTHONPATH", "/somewhere/with/a/source/tree")
	t.Setenv("GOFLAGS", "-mod=mod")
	t.Setenv("NODE_PATH", "/somewhere/else")
	for _, kv := range cleanEnv(t) {
		name, _, _ := strings.Cut(kv, "=")
		for _, forbidden := range inheritedPaths {
			if name == forbidden {
				t.Fatalf("cleanEnv passed %s through", name)
			}
		}
	}
}

// realPath resolves a temporary directory the way a child process reports it.
// On macOS /var is a symlink to /private/var, so a path comparison against a
// raw t.TempDir() would fail for a reason that has nothing to do with the
// property under test.
func realPath(t *testing.T, dir string) string {
	t.Helper()
	resolved, err := filepath.EvalSymlinks(dir)
	if err != nil {
		t.Fatal(err)
	}
	return resolved
}

func repoRootErr() (string, error) {
	out, err := exec.Command("git", "rev-parse", "--show-toplevel").Output()
	if err != nil {
		return "", fmt.Errorf("not a git checkout: %w", err)
	}
	root, err := filepath.EvalSymlinks(strings.TrimSpace(string(out)))
	if err != nil {
		return "", err
	}
	return root, nil
}

func repoRoot(t *testing.T) string {
	t.Helper()
	root, err := repoRootErr()
	if err != nil {
		t.Skipf("unavailable: %v", err)
	}
	return root
}

// requireTool skips the lane by name when the host has no such command. §6.2
// has no skipped-success: a missing tool is `unavailable` and says so.
func requireTool(t *testing.T, name string) string {
	t.Helper()
	path, err := exec.LookPath(name)
	if err != nil {
		t.Skipf("unavailable: %s is not on PATH: %v", name, err)
	}
	return path
}

// runClean runs a command the way an operator on a fresh machine would: in a
// directory that is not the source tree, with cleanEnv, and with the whole
// output attached to any failure.
func runClean(t *testing.T, dir string, env []string, name string, args ...string) string {
	t.Helper()
	out, err := runCleanErr(t, dir, env, name, args...)
	if err != nil {
		t.Fatalf("%s %s in %s: %v\n%s", name, strings.Join(args, " "), dir, err, out)
	}
	return out
}

func runCleanErr(t *testing.T, dir string, env []string, name string, args ...string) (string, error) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Minute)
	defer cancel()
	cmd := exec.CommandContext(ctx, name, args...)
	cmd.Dir = dir
	cmd.Env = env
	out, err := cmd.CombinedOutput()
	return string(out), err
}

// ---- the artifacts ------------------------------------------------------

// distOnce runs `make dist` at most once per test binary. The lanes install
// the files that command produced rather than a build of their own: an
// artifact test that builds its own artifact is testing a build, not a
// release. dist/ is the Makefile's own output directory and is not tracked.
var distOnce = sync.OnceValues(func() (string, error) {
	root, err := repoRootErr()
	if err != nil {
		return "", err
	}
	if _, err := exec.LookPath("make"); err != nil {
		return "", fmt.Errorf("make is not on PATH: %w", err)
	}
	cmd := exec.Command("make", "dist")
	cmd.Dir = root
	if out, err := cmd.CombinedOutput(); err != nil {
		return "", fmt.Errorf("make dist: %v\n%s", err, out)
	}
	return filepath.Join(root, "dist"), nil
})

func distDir(t *testing.T) string {
	t.Helper()
	dir, err := distOnce()
	if err != nil {
		t.Skipf("unavailable: %v", err)
	}
	return dir
}

// distName is the `make dist` file name for one platform.
func distName(goos, goarch string) string {
	name := "remount-" + goos + "-" + goarch
	if goos == "windows" {
		name += ".exe"
	}
	return name
}

// installBinary copies one dist artifact into a prefix that looks like a
// machine's own, far from the checkout. Copying rather than symlinking is the
// point: a symlink into dist/ would keep the source tree load-bearing.
func installBinary(t *testing.T, prefix, goos, goarch string) string {
	t.Helper()
	src := filepath.Join(distDir(t), distName(goos, goarch))
	body, err := os.ReadFile(src)
	if err != nil {
		t.Fatalf("read the dist artifact: %v", err)
	}
	bin := filepath.Join(prefix, "bin")
	if err := os.MkdirAll(bin, 0o755); err != nil {
		t.Fatal(err)
	}
	dst := filepath.Join(bin, "remount")
	if err := os.WriteFile(dst, body, 0o755); err != nil {
		t.Fatal(err)
	}
	return dst
}

// ---- the source-tree detector -------------------------------------------

// containsSourceTree reports whether a file carries the checkout's path. It is
// the crudest possible detector and that is why it works on any artifact: a
// binary, an archive member or a generated script that names /Users/…/remount
// is a file that only runs where that directory exists.
func containsSourceTree(t *testing.T, path, root string) bool {
	t.Helper()
	body, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	return bytes.Contains(body, []byte(root))
}

func requireNoSourceTree(t *testing.T, path, root string) {
	t.Helper()
	if containsSourceTree(t, path, root) {
		t.Fatalf("%s carries the source tree path %s; the artifact only works where the checkout exists", path, root)
	}
}

// ---- a running installed server -----------------------------------------

func freeLoopbackAddr(t *testing.T) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := l.Addr().String()
	if err := l.Close(); err != nil {
		t.Fatal(err)
	}
	return addr
}

func waitHealthy(t *testing.T, endpoint, token string, within time.Duration) {
	t.Helper()
	deadline := time.Now().Add(within)
	var last error
	for time.Now().Before(deadline) {
		req, err := http.NewRequest(http.MethodGet, endpoint+"/healthz", nil)
		if err != nil {
			t.Fatal(err)
		}
		if token != "" {
			req.Header.Set("Authorization", "Bearer "+token)
		}
		resp, err := http.DefaultClient.Do(req)
		if err == nil {
			body, _ := readAllAndClose(resp)
			if resp.StatusCode == http.StatusOK && strings.Contains(body, `"serving":true`) {
				return
			}
			last = fmt.Errorf("healthz %d: %s", resp.StatusCode, body)
		} else {
			last = err
		}
		time.Sleep(200 * time.Millisecond)
	}
	t.Fatalf("%s never became healthy: %v", endpoint, last)
}

func readAllAndClose(resp *http.Response) (string, error) {
	defer func() { _ = resp.Body.Close() }()
	buf := &bytes.Buffer{}
	_, err := buf.ReadFrom(resp.Body)
	return buf.String(), err
}

// startInstalled runs an installed binary in standalone mode from a directory
// that is not the checkout, with cleanEnv, and returns its endpoint.
func startInstalled(t *testing.T, bin string) string {
	t.Helper()
	addr := freeLoopbackAddr(t)
	work := t.TempDir()
	data := filepath.Join(work, "state")
	log := &lockedBuffer{}
	cmd := exec.Command(bin, "standalone", "--listen", addr, "--data", data)
	cmd.Dir = work
	cmd.Env = cleanEnv(t)
	cmd.Stdout = log
	cmd.Stderr = log
	if err := cmd.Start(); err != nil {
		t.Fatalf("start the installed binary: %v", err)
	}
	t.Cleanup(func() {
		_ = cmd.Process.Kill()
		_, _ = cmd.Process.Wait()
		if t.Failed() {
			t.Logf("installed server log:\n%s", log.String())
		}
	})
	endpoint := "http://" + addr
	waitHealthy(t, endpoint, "", 60*time.Second)
	return endpoint
}

// lockedBuffer collects a child's output without racing the test goroutine
// that reads it after a failure.
type lockedBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *lockedBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *lockedBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

// ---- the black-box smoke ------------------------------------------------

// conformanceReport is the subset of cmd/conformance's JSON report these lanes
// judge. It is declared here rather than imported from internal/conformance
// because the suite's whole value is that it shares nothing with the
// implementation it judges; a test that reached into it to read a verdict
// would be re-linking exactly what B4 separated.
type conformanceReport struct {
	Results []struct {
		Requirement struct {
			ID   string `json:"id"`
			Tier string `json:"tier"`
		} `json:"requirement"`
		Status string `json:"status"`
		Reason string `json:"reason"`
	} `json:"results"`
	Cleanup string `json:"cleanup"`
	Aborted string `json:"aborted"`
}

// conformanceSmoke is B32's judgement: `go run ./cmd/conformance --endpoint URL
// --external` against something this test installed. --external is the honest
// mode for an installed artifact — the runner may assume nothing beyond the
// public protocol, HTTP and event surfaces, which is what makes the verdict a
// statement about the install rather than about the checkout that produced it.
func conformanceSmoke(t *testing.T, endpoint, token string) {
	t.Helper()
	root := repoRoot(t)
	reportPath := filepath.Join(t.TempDir(), "report.json")
	args := []string{"run", "./cmd/conformance", "--endpoint", endpoint, "--external", "--report", reportPath}
	if token != "" {
		args = append(args, "--token", token)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	// The judge is repository tooling and runs in the repository; the target
	// it judges is the installed artifact, which is what B32 constrains.
	cmd := exec.CommandContext(ctx, "go", args...)
	cmd.Dir = root
	summary := &bytes.Buffer{}
	cmd.Stdout = summary
	cmd.Stderr = &bytes.Buffer{}
	runErr := cmd.Run()

	body, err := os.ReadFile(reportPath)
	if err != nil {
		t.Fatalf("conformance wrote no report (%v): %v\n%s", runErr, err, summary)
	}
	var report conformanceReport
	if err := json.Unmarshal(body, &report); err != nil {
		t.Fatalf("decode the conformance report: %v", err)
	}
	if report.Aborted != "" {
		t.Fatalf("the conformance run could not judge %s: %s", endpoint, report.Aborted)
	}
	var passed, failed, unavailable int
	for _, res := range report.Results {
		if res.Requirement.Tier != "required" {
			continue
		}
		switch res.Status {
		case "passed":
			passed++
		case "failed":
			failed++
			t.Errorf("required %s failed: %s", res.Requirement.ID, res.Reason)
		case "unavailable":
			unavailable++
			// §6.2 has no skipped-success, and Conformant() already refuses
			// this; naming the row keeps the failure readable.
			t.Errorf("required %s could not be observed: %s", res.Requirement.ID, res.Reason)
		}
	}
	if runErr != nil {
		t.Fatalf("conformance against %s: %v\n%s", endpoint, runErr, summary)
	}
	if !strings.HasPrefix(summary.String(), "CONFORMANT") {
		t.Fatalf("conformance did not lead with CONFORMANT:\n%s", summary)
	}
	if report.Cleanup != "verified" {
		t.Fatalf("conformance left state behind: cleanup %q", report.Cleanup)
	}
	if passed == 0 {
		t.Fatal("the conformance report contains no required rows at all")
	}
	t.Logf("CONFORMANT against %s: required %d passed, %d failed, %d unavailable; cleanup %s",
		endpoint, passed, failed, unavailable, report.Cleanup)
}

// hostPlatform is the only platform whose dist artifact this host can run.
func hostPlatform() (string, string) { return runtime.GOOS, runtime.GOARCH }
