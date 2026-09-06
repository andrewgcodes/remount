// Package examples proves the runnable public examples still run.
//
// An example nobody executes is documentation that decays silently, and the
// two examples covered here are the ones a new user is told to start with. So
// each test boots a whole Remount system — control plane, relay, artifact
// store and one process-backend node — in this process on a free loopback
// port, builds the example exactly as `go run ./examples/...` would, runs the
// resulting binary against that server with nothing in its environment but
// REMOUNT_SERVER, and asserts on what it printed.
//
// No credential, binding or outbound network access is involved: an example
// that quietly grew a dependency on either would fail here rather than on a
// reader's laptop.
package examples

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"remount.dev/remount/internal/node"
	"remount.dev/remount/internal/server"
	"remount.dev/remount/internal/transport"
	"remount.dev/remount/internal/workspace"
)

// TestMinimalShellExample runs examples/minimal-shell end to end.
func TestMinimalShellExample(t *testing.T) {
	requirePOSIXShell(t)
	endpoint := standalone(t)
	stdout := runExample(t, "./examples/minimal-shell", endpoint, 3*time.Minute)

	for _, want := range []string{"hello from ", "exit 0", "read back 27 bytes: written by the Remount SDK"} {
		if !strings.Contains(stdout, want) {
			t.Errorf("minimal-shell output is missing %q:\n%s", want, stdout)
		}
	}
	if matches := workspaceID.FindAllString(stdout, -1); len(matches) == 0 {
		t.Errorf("minimal-shell never named a workspace:\n%s", stdout)
	} else if !strings.Contains(stdout, "workspace "+matches[0]+" destroyed") {
		t.Errorf("minimal-shell left workspace %s behind:\n%s", matches[0], stdout)
	}
}

// TestArtifactTransferExample runs examples/artifact-transfer end to end. Its
// own final line is the byte-for-byte claim, so asserting the line is
// asserting the round trip.
func TestArtifactTransferExample(t *testing.T) {
	requirePOSIXShell(t)
	endpoint := standalone(t)
	stdout := runExample(t, "./examples/artifact-transfer", endpoint, 5*time.Minute)

	for _, want := range []string{
		"source ", "uploaded 3 files into ws_", "snapshot art_sha256:",
		"digest verified", "verified 3 files byte for byte in ws_",
	} {
		if !strings.Contains(stdout, want) {
			t.Errorf("artifact-transfer output is missing %q:\n%s", want, stdout)
		}
	}
	// Both workspaces are destroyed, and they are not the same workspace: a
	// restore that silently reused the origin would still print every other
	// line above.
	destroyed := destroyedWorkspace.FindAllStringSubmatch(stdout, -1)
	if len(destroyed) != 2 {
		t.Fatalf("artifact-transfer destroyed %d workspaces, want 2:\n%s", len(destroyed), stdout)
	}
	if destroyed[0][1] == destroyed[1][1] {
		t.Errorf("artifact-transfer restored into the workspace it snapshotted:\n%s", stdout)
	}
}

// TestLongRunningAutosleepExample runs examples/long-running-autosleep end to
// end. The example takes a twenty-second hold, renews once, and then waits for
// Remount's own control plane to act — so unlike the other two, most of its
// runtime is a deadline nobody in this process is holding.
//
// The example fails itself if the exit reason or an expected event is missing,
// so a green run is already the assertion. The checks below name the claims out
// loud anyway: what would silently rot here is not the exit status but whether
// the durable half still happens.
func TestLongRunningAutosleepExample(t *testing.T) {
	requirePOSIXShell(t)
	endpoint := standalone(t)
	stdout := runExample(t, "./examples/long-running-autosleep", endpoint, 5*time.Minute)

	for _, want := range []string{
		"background job started",
		"(renewals 1)",
		"pending deadline: sleep at ",
		"is paused, checkpoint art_sha256:",
		`reason "lifecycle_deadline_expired"`,
		"event ws.lifecycle.expired action=sleep source=lease",
		"event ws.lease.expired ",
		"filesystem survived: written before the hold expired",
		"processes after wake: processes-gone",
	} {
		if !strings.Contains(stdout, want) {
			t.Errorf("long-running-autosleep output is missing %q:\n%s", want, stdout)
		}
	}
	// The hold expired on its own schedule rather than being cancelled: the
	// example never calls CancelLease, so a cancelled hold here would mean the
	// deadline was retired by something else.
	if strings.Contains(stdout, "reason=cancelled") {
		t.Errorf("the hold was cancelled rather than expiring at its deadline:\n%s", stdout)
	}
	if matches := workspaceID.FindAllString(stdout, -1); len(matches) == 0 {
		t.Errorf("long-running-autosleep never named a workspace:\n%s", stdout)
	} else if !strings.Contains(stdout, "workspace "+matches[0]+" destroyed") {
		t.Errorf("long-running-autosleep left workspace %s behind:\n%s", matches[0], stdout)
	}
}

var (
	workspaceID        = regexp.MustCompile(`ws_[a-z0-9]+`)
	destroyedWorkspace = regexp.MustCompile(`workspace (ws_[a-z0-9]+) destroyed`)
)

// requirePOSIXShell skips where the examples' `sh -c` and POSIX tooling do not
// exist. A check that cannot run is reported as unavailable, never as a pass.
func requirePOSIXShell(t *testing.T) {
	t.Helper()
	if runtime.GOOS == "windows" {
		t.Skip("unavailable: the examples run their command through a POSIX shell")
	}
}

// repoRoot is the checkout the examples are built from.
func repoRoot(t *testing.T) string {
	t.Helper()
	wd, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	return filepath.Dir(filepath.Dir(wd))
}

// runExample builds one example package and runs the binary against endpoint,
// returning its stdout. Building first rather than `go run` keeps the go
// tool's own output out of the program's.
func runExample(t *testing.T, pkg, endpoint string, timeout time.Duration) string {
	t.Helper()
	root := repoRoot(t)
	binary := filepath.Join(t.TempDir(), "example")
	if runtime.GOOS == "windows" {
		binary += ".exe"
	}
	build := exec.Command("go", "build", "-o", binary, pkg)
	build.Dir = root
	if out, err := build.CombinedOutput(); err != nil {
		t.Fatalf("build %s: %v\n%s", pkg, err, out)
	}

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	cmd := exec.CommandContext(ctx, binary)
	cmd.Dir = t.TempDir()
	// Only the server address. No token, no provider key, no proxy.
	cmd.Env = append(os.Environ(), "REMOUNT_SERVER="+endpoint, "REMOUNT_TOKEN=")
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	err := cmd.Run()
	if stderr.Len() > 0 {
		t.Logf("%s stderr:\n%s", pkg, stderr.String())
	}
	if err != nil {
		t.Fatalf("run %s: %v\nstdout:\n%s", pkg, err, stdout.String())
	}
	return stdout.String()
}

// standalone runs a control plane and a process-backend node in this process,
// the way `remount standalone` does, and returns the server's base URL.
func standalone(t *testing.T) string {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	data := t.TempDir()
	quiet := slog.New(slog.NewTextHandler(io.Discard, nil))

	srv, err := server.New(server.Options{
		DataDir: filepath.Join(data, "server"), Logger: quiet, Mode: server.ModeStandalone,
	})
	if err != nil {
		cancel()
		t.Fatal(err)
	}
	// Port zero, resolved by WaitReady: a test that hard coded 7443 would
	// fight the demo server a developer already has running.
	go func() { _ = srv.Serve(ctx, "127.0.0.1:0") }()
	readyCtx, readyCancel := context.WithTimeout(ctx, 30*time.Second)
	addr, err := srv.WaitReady(readyCtx)
	readyCancel()
	if err != nil {
		cancel()
		srv.Close()
		t.Fatal(err)
	}
	endpoint := "http://" + addr

	nodeDir := filepath.Join(data, "node")
	process, err := workspace.NewProcess(filepath.Join(nodeDir, "ws"))
	if err != nil {
		cancel()
		srv.Close()
		t.Fatal(err)
	}
	link := strings.Replace(endpoint, "http://", "ws://", 1) + "/v1/link"
	n, err := node.New(node.Options{
		DataDir: nodeDir, Dialer: transport.DialFunc(func(ctx context.Context) (transport.Conn, error) {
			return transport.DialWS(ctx, link, nil)
		}),
		Labels: map[string]string{"standalone": "true"}, Backends: workspace.NewRegistry(process),
		ArtifactURL: endpoint + "/v1/artifacts", Logger: quiet, Version: "test",
	})
	if err != nil {
		cancel()
		srv.Close()
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	wg.Add(1)
	go func() { defer wg.Done(); _ = n.Run(ctx) }()
	t.Cleanup(func() {
		cancel()
		wg.Wait()
		srv.Close()
	})
	waitHealthy(t, endpoint, 60*time.Second)
	return endpoint
}

func waitHealthy(t *testing.T, endpoint string, within time.Duration) {
	t.Helper()
	deadline := time.Now().Add(within)
	var last error
	for time.Now().Before(deadline) {
		response, err := http.Get(endpoint + "/healthz")
		if err == nil {
			body, _ := io.ReadAll(response.Body)
			_ = response.Body.Close()
			if response.StatusCode == http.StatusOK && strings.Contains(string(body), `"serving":true`) {
				return
			}
			last = fmt.Errorf("healthz %d: %s", response.StatusCode, body)
		} else {
			last = err
		}
		time.Sleep(200 * time.Millisecond)
	}
	t.Fatalf("%s never became healthy: %v", endpoint, last)
}
