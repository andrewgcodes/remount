package sim

import (
	"context"
	"fmt"
	"os"
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
		Stderr:   os.Stderr,
	})
	if err != nil {
		t.Fatal(err)
	}
	ws := res.Workspace
	defer func() {
		dctx, cancel := context.WithTimeout(context.Background(), time.Minute)
		defer cancel()
		_ = c.DestroyWorkspace(dctx, ws.ID)
	}()
	var out []byte
	for ch := range res.Session.Chunks() {
		if ch.Stream == proto.StreamStdout || ch.Stream == proto.StreamStderr {
			out = append(out, ch.Data...)
		}
	}
	if err := res.Session.Err(); err != nil {
		t.Fatal(err)
	}
	if exit := res.Session.Exit(); exit == nil || exit.Code != 0 {
		t.Fatalf("opencode exit = %+v\n%s\n%s", exit, out, egressLog(t, c, ws.ID, key))
	}
	if strings.Contains(string(out), key) {
		t.Fatal("session output carries the provider secret")
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
	if strings.Contains(string(cfg), key) || !strings.Contains(string(cfg), `"apiKey": "ref:b_openai"`) {
		t.Fatalf("opencode.json:\n%s", cfg)
	}
	if _, err := c.ReadFile(ctx, ws.ID, "opencode.json"); err == nil {
		t.Fatal("recipe wrote opencode.json into the project root")
	}

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
