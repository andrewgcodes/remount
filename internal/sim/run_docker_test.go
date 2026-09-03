package sim

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"remount.dev/remount/internal/client"
	"remount.dev/remount/internal/control"
	"remount.dev/remount/internal/launch"
	"remount.dev/remount/internal/node"
	"remount.dev/remount/internal/proto"
	"remount.dev/remount/internal/workspace"
)

// TestRunOpenCodeDockerIntegration is the E1 integration lane: the real
// OpenCode harness installed and run inside a docker workspace, talking to
// the real OpenAI API through the broker with a key the container never
// holds. The docker backend cooperates rather than enforces egress, so the
// profile is local and the recipe's hosts come from the node allow list, as
// they would under `remount standalone --allow`. It runs only when a daemon
// is up and REMOUNT_INTEGRATION_OPENAI_KEY is set; everything else about the
// run path is covered by the in-process tests.
func TestRunOpenCodeDockerIntegration(t *testing.T) {
	key := os.Getenv("REMOUNT_INTEGRATION_OPENAI_KEY")
	if key == "" {
		t.Skip("REMOUNT_INTEGRATION_OPENAI_KEY not set")
	}
	image := os.Getenv("REMOUNT_INTEGRATION_IMAGE")
	if image == "" {
		image = "node:22-bookworm-slim"
	}
	recipe, err := launch.Load("opencode")
	if err != nil {
		t.Fatal(err)
	}
	d, _ := workspace.NewDocker(filepath.Join(t.TempDir(), "d"), image)
	if err := d.Available(ctxT(t, 30*time.Second)); err != nil {
		t.Skipf("docker: %v", err)
	}
	w := newWorld(t, control.Binding{ID: "b_openai", Secret: key, Destinations: []string{"api.openai.com"}, TTLSec: 600})
	pb, _ := workspace.NewProcess(filepath.Join(t.TempDir(), "p"))
	w.nodeWith("dn", func(o *node.Options) {
		o.Backends = workspace.NewRegistry(pb, d)
		o.Allow = append(o.Allow, recipe.Hosts...)
	})
	c := w.client("c1")
	ctx := ctxT(t, 8*time.Minute)
	binding, err := launch.ParseBinding("b_openai")
	if err != nil {
		t.Fatal(err)
	}
	res, err := launch.Start(ctx, c, launch.Options{
		Recipe:   recipe,
		Task:     "Create a file named GREETING.txt containing exactly the word hello. Do nothing else.",
		Backend:  "docker",
		Image:    image,
		Bindings: []launch.Binding{binding},
		Security: proto.SecurityLocal,
		Sandbox:  launch.SandboxWorkspaceWrite,
		Model:    "openai/gpt-4o-mini",
		Timeout:  6 * time.Minute,
		Stderr:   io.Discard,
	})
	if res != nil && res.Workspace != nil {
		registerDockerWorkspaceCleanup(t, c, d, res.Workspace.ID)
	}
	if err != nil {
		fatalWithoutToken(t, err, key)
	}
	ws := res.Workspace
	var out []byte
	for ch := range res.Session.Chunks() {
		if ch.Stream == proto.StreamStdout || ch.Stream == proto.StreamStderr {
			out = append(out, ch.Data...)
		}
	}
	if err := res.Session.Err(); err != nil {
		fatalWithoutToken(t, err, key)
	}
	if strings.Contains(string(out), key) {
		t.Fatal("session output carries the provider secret")
	}
	if exit := res.Session.Exit(); exit == nil || exit.Code != 0 {
		t.Fatalf("opencode exit = %+v\n%s\n%s", exit, out, egressLog(t, c, ws.ID, key))
	}
	greeting, err := c.ReadFile(ctx, ws.ID, "GREETING.txt")
	if err != nil {
		t.Fatalf("GREETING.txt: %v\n%s", err, out)
	}
	if strings.TrimSpace(string(greeting)) != "hello" {
		t.Fatalf("GREETING.txt = %q", greeting)
	}
	cfg, err := c.ReadFile(ctx, ws.ID, ".remount/launch/opencode.json")
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(cfg), key) {
		t.Fatal("opencode.json carries the provider secret")
	}
	if !strings.Contains(string(cfg), `"apiKey": "ref:b_openai"`) {
		t.Fatalf("opencode.json:\n%s", cfg)
	}
	if _, err := c.ReadFile(ctx, ws.ID, "opencode.json"); err == nil {
		t.Fatal("recipe wrote opencode.json into the project root")
	}
	// The key is in zero workspace files, including the harness's own state
	// and .remount/env; the planted canary proves the scan reads them. The
	// container wrote as root, so the tree is scanned after the snapshot
	// pass that re-owns it to the node, exactly as it would travel.
	if _, err := c.Snapshot(ctx, ws.ID, false); err != nil {
		t.Fatal(err)
	}
	info, err := c.WorkspaceInfo(ctx, ws.ID)
	if err != nil {
		t.Fatal(err)
	}
	// ws.info reports MountPathOf: the root the *workspace* sees, which under
	// docker is the in-container mount, not a host directory. Scanning it as a
	// local path is exactly the confusion HostFileSystem warns against, so the
	// host tree is reached through the backend's own layout instead.
	if info.Root != proto.DefaultMountPath {
		t.Fatalf("docker ws.info root = %q, want the in-container mount path %q", info.Root, proto.DefaultMountPath)
	}
	hostRoot := filepath.Join(d.Dir, ws.ID)
	if _, err := os.Stat(hostRoot); err != nil {
		t.Fatalf("docker workspace host tree: %v", err)
	}
	assertTokenAbsent(t, hostRoot, key)
	// Scanner self-validation must never write the provider credential merely
	// to prove the scan works. This synthetic value is unique to the temporary
	// workspace and has no authority outside the test.
	scanCanary := "remount-nonsecret-scan-canary-" + ws.ID
	if err := c.WriteFile(ctx, ws.ID, "canary.txt", []byte("x "+scanCanary+" y\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if hits := scanForToken(t, hostRoot, scanCanary); len(hits) != 1 || filepath.Base(hits[0]) != "canary.txt" {
		t.Fatalf("scan did not find the planted canary: %v", hits)
	}
	assertTokenAbsent(t, hostRoot, key)

	time.Sleep(300 * time.Millisecond)
	evs, err := c.ReadEvents(ctx, 1, ws.ID)
	if err != nil {
		t.Fatal(err)
	}
	var started, finished, used, denied int
	for _, e := range evs {
		if strings.Contains(string(e.Payload), key) {
			t.Fatalf("event %s carries the provider secret", e.Type)
		}
		switch e.Type {
		case proto.EvRunStarted:
			started++
		case proto.EvRunFinished:
			finished++
		case proto.EvCredUsed:
			if strings.Contains(string(e.Payload), "api.openai.com") {
				used++
			}
		case proto.EvEgressDenied:
			// A denial naming the provider would mean the placeholder went
			// somewhere its binding does not cover.
			if strings.Contains(string(e.Payload), "api.openai.com") {
				denied++
			}
		}
	}
	if started != 1 || finished != 1 || used == 0 || denied != 0 {
		t.Fatalf("events: run.started=%d run.finished=%d cred.used(openai)=%d egress.denied(openai)=%d\n%s", started, finished, used, denied, egressLog(t, c, ws.ID, key))
	}
}

func fatalWithoutToken(t *testing.T, err error, token string) {
	t.Helper()
	if strings.Contains(err.Error(), token) {
		t.Fatal("error carries the provider secret")
	}
	t.Fatal(err)
}

func assertTokenAbsent(t *testing.T, root, token string) {
	t.Helper()
	if hits := scanForToken(t, root, token); len(hits) != 0 {
		t.Fatalf("provider key on workspace disk: %v", hits)
	}
}

// registerDockerWorkspaceCleanup keeps cleanup observable while retaining a
// direct backend fallback for a failed control-plane teardown. The final
// inventory and filesystem checks make a leaked local sandbox fail the lane.
func registerDockerWorkspaceCleanup(t *testing.T, c *client.Client, d *workspace.Docker, wsID string) {
	t.Helper()
	root := filepath.Join(d.Dir, wsID)
	container := "remount-" + strings.ReplaceAll(wsID, "_", "-")
	t.Cleanup(func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), time.Minute)
		defer cancel()
		if err := c.DestroyWorkspace(cleanupCtx, wsID); err != nil {
			t.Errorf("destroy docker workspace: %v", err)
			if handle, adoptErr := d.Adopt(cleanupCtx, wsID); adoptErr == nil {
				if destroyErr := handle.Destroy(cleanupCtx); destroyErr != nil {
					t.Errorf("fallback destroy docker workspace: %v", destroyErr)
				}
			}
		}

		out, err := exec.CommandContext(cleanupCtx, d.Binary, "ps", "-aq", "--filter", "label=remount.workspace="+wsID).CombinedOutput()
		if err != nil {
			t.Errorf("verify docker workspace cleanup: %v", err)
			emergencyRemoveDockerContainer(t, d.Binary, container)
		} else if strings.TrimSpace(string(out)) != "" {
			t.Errorf("docker workspace container still present after destroy")
			emergencyRemoveDockerContainer(t, d.Binary, container)
		}
		if _, err := os.Stat(root); err == nil {
			t.Errorf("docker workspace root still present after destroy")
			if removeErr := os.RemoveAll(root); removeErr != nil {
				t.Errorf("emergency remove docker workspace root: %v", removeErr)
			}
		} else if !errors.Is(err, os.ErrNotExist) {
			t.Errorf("verify docker workspace root cleanup: %v", err)
		}
		if _, err := os.Stat(root); !errors.Is(err, os.ErrNotExist) {
			t.Errorf("docker workspace root remains after cleanup: %v", err)
		}
	})
}

func emergencyRemoveDockerContainer(t *testing.T, binary, container string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if out, err := exec.CommandContext(ctx, binary, "rm", "-f", container).CombinedOutput(); err != nil && !strings.Contains(string(out), "No such container") {
		t.Errorf("emergency docker cleanup: %v (%s)", err, strings.TrimSpace(string(out)))
	}
}

// egressLog renders the broker's decisions for ws so a failed run explains
// itself; the provider key is asserted absent rather than redacted.
func egressLog(t *testing.T, c *client.Client, ws, key string) string {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	evs, err := c.ReadEvents(ctx, 1, ws)
	if err != nil {
		return "events: " + err.Error()
	}
	var b strings.Builder
	for _, e := range evs {
		if strings.Contains(string(e.Payload), key) {
			t.Errorf("event %s carries the provider secret", e.Type)
			continue
		}
		if strings.HasPrefix(e.Type, "egress.") || strings.HasPrefix(e.Type, "cred.") || strings.HasPrefix(e.Type, "run.") {
			fmt.Fprintf(&b, "%s %s\n", e.Type, e.Payload)
		}
	}
	return b.String()
}
