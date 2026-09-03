package sim

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"remount.dev/remount/internal/control"
	"remount.dev/remount/internal/launch"
	"remount.dev/remount/internal/node"
	"remount.dev/remount/internal/proto"
	"remount.dev/remount/internal/workspace"
)

// dockerLane returns the OpenCode recipe, a docker backend and the provider
// key for the real-harness lanes, skipping when either the key or a daemon
// is absent.
func dockerLane(t *testing.T) (*launch.Recipe, *workspace.Docker, string, string) {
	t.Helper()
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
	return recipe, d, image, key
}

func dockerNode(t *testing.T, w *world, name string, recipe *launch.Recipe, d *workspace.Docker) *node.Node {
	t.Helper()
	pb, _ := workspace.NewProcess(filepath.Join(t.TempDir(), name+"-p"))
	return w.nodeWith(name, func(o *node.Options) {
		o.Backends = workspace.NewRegistry(pb, d)
		o.Allow = append(o.Allow, recipe.Hosts...)
	})
}

// TestRunOpenCodeHandoffAcrossNodesDockerIntegration is the E2 lane with the
// API-key auth variant: two turns of a real OpenCode conversation on one
// docker node, a move to a second node, and a third turn there that must
// still have the first two in context. The state lives in the recipe's
// state_dirs under the workspace, so it travels in the snapshot; nothing on
// either node's host is consulted.
func TestRunOpenCodeHandoffAcrossNodesDockerIntegration(t *testing.T) {
	recipe, d, image, key := dockerLane(t)
	w := newWorld(t, control.Binding{ID: "b_openai", Secret: key, Destinations: []string{"api.openai.com"}, TTLSec: 600})
	first := dockerNode(t, w, "dn1", recipe, d)
	c := w.client("c1")
	ctx := ctxT(t, 12*time.Minute)
	binding, err := launch.ParseBinding("b_openai")
	if err != nil {
		t.Fatal(err)
	}
	var progress bytes.Buffer
	res, err := launch.Start(ctx, c, launch.Options{
		Recipe:   recipe,
		Task:     "Remember this: the codeword is PELICAN-7. Reply with exactly OK and nothing else. Do not create or edit files.",
		Backend:  "docker",
		Image:    image,
		Bindings: []launch.Binding{binding},
		Security: proto.SecurityLocal,
		Sandbox:  launch.SandboxWorkspaceWrite,
		Model:    "openai/gpt-4o-mini",
		Timeout:  5 * time.Minute,
		Stderr:   &progress,
	})
	if err != nil {
		t.Fatalf("%v\n%s", err, progress.String())
	}
	ws := res.Workspace
	defer func() {
		dctx, cancel := context.WithTimeout(context.Background(), time.Minute)
		defer cancel()
		_ = c.DestroyWorkspace(dctx, ws.ID)
	}()
	if ws.Node != first.ID() {
		t.Fatalf("claimed by %s, want %s", ws.Node, first.ID())
	}
	turn := func(task, what string) string {
		t.Helper()
		rr, err := launch.Resume(ctx, c, launch.ResumeOptions{WS: ws.ID, Task: task, Timeout: 5 * time.Minute, Stderr: &progress})
		if err != nil {
			t.Fatalf("%s: %v\n%s", what, err, progress.String())
		}
		out := readOut(t, rr.Session)
		if exit := rr.Session.Exit(); exit == nil || exit.Code != 0 {
			t.Fatalf("%s: exit %+v\n%s\n%s", what, exit, out, egressLog(t, c, ws.ID, key))
		}
		if strings.Contains(out, key) {
			t.Fatalf("%s: output carries the provider secret", what)
		}
		return out
	}
	out1 := readOut(t, res.Session)
	if exit := res.Session.Exit(); exit == nil || exit.Code != 0 {
		t.Fatalf("turn 1 exit %+v\n%s\n%s", exit, out1, egressLog(t, c, ws.ID, key))
	}
	turn("Also remember: the color is teal. Reply with exactly OK and nothing else.", "turn 2")
	entries, err := c.ListDir(ctx, ws.ID, recipe.StateDirs[0])
	if err != nil || len(entries) == 0 {
		t.Fatalf("harness state %s after two turns: %v %+v", recipe.StateDirs[0], err, entries)
	}

	// The "second machine": a node that appears only now, so nothing about
	// the conversation can have been cached there.
	second := dockerNode(t, w, "dn2", recipe, d)
	moved, err := c.MoveWorkspace(ctx, ws.ID, nil, &proto.Placement{Node: second.ID()})
	if err != nil {
		t.Fatal(err)
	}
	if moved, err = c.WaitClaimed(ctx, moved.ID); err != nil {
		t.Fatal(err)
	}
	if moved.Node != second.ID() || moved.Generation < 2 {
		t.Fatalf("after move: %+v", moved)
	}
	out3 := turn("What is the codeword and what is the color from earlier in this conversation? Answer in one line.", "turn 3")
	lower := strings.ToLower(out3)
	if !strings.Contains(lower, "pelican") || !strings.Contains(lower, "teal") {
		t.Fatalf("third turn lost the conversation:\n%s", out3)
	}

	time.Sleep(300 * time.Millisecond)
	evs, err := c.ReadEvents(ctx, 1, ws.ID)
	if err != nil {
		t.Fatal(err)
	}
	var started, finished, used, resident int
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
			used++
		case proto.EvAuthWSResident:
			resident++
		}
	}
	if started != 3 || finished != 3 || used < 3 || resident != 0 {
		t.Fatalf("events: run.started=%d run.finished=%d cred.used=%d workspace_resident=%d\n%s", started, finished, used, resident, egressLog(t, c, ws.ID, key))
	}
}

// TestRunOpenCodeQueueDockerIntegration is the E3 lane: several tasks run
// in order through one docker workspace by a real harness, with the tree
// checkpointed between them and the queue cursor in the control plane. The
// overnight sleep is exercised by the in-process queue tests; here the
// tasks follow each other at once so the lane stays short.
func TestRunOpenCodeQueueDockerIntegration(t *testing.T) {
	recipe, d, image, key := dockerLane(t)
	w := newWorld(t, control.Binding{ID: "b_openai", Secret: key, Destinations: []string{"api.openai.com"}, TTLSec: 600})
	dockerNode(t, w, "dn1", recipe, d)
	c := w.client("c1")
	ctx := ctxT(t, 12*time.Minute)
	binding, err := launch.ParseBinding("b_openai")
	if err != nil {
		t.Fatal(err)
	}
	var progress, output bytes.Buffer
	qr, err := launch.RunQueue(ctx, c, launch.QueueOptions{
		Tasks: []string{
			"Create a file named ONE.txt containing exactly the word alpha. Do nothing else.",
			"Create a file named TWO.txt containing exactly the word beta. Do nothing else.",
		},
		Run: launch.Options{
			Recipe:   recipe,
			Backend:  "docker",
			Image:    image,
			Bindings: []launch.Binding{binding},
			Security: proto.SecurityLocal,
			Sandbox:  launch.SandboxWorkspaceWrite,
			Model:    "openai/gpt-4o-mini",
			Timeout:  5 * time.Minute,
			Stderr:   &progress,
		},
		Output: &output,
	})
	if qr != nil && qr.Workspace != nil {
		defer func() {
			dctx, cancel := context.WithTimeout(context.Background(), time.Minute)
			defer cancel()
			_ = c.DestroyWorkspace(dctx, qr.Workspace.ID)
		}()
	}
	if err != nil {
		t.Fatalf("%v\n%s\n%s", err, progress.String(), output.String())
	}
	if strings.Contains(output.String(), key) {
		t.Fatal("queue output carries the provider secret")
	}
	ws := qr.Workspace
	if qr.Queue.Status != proto.QueueDone || qr.Queue.Cursor != 2 || len(qr.Sessions) != 2 {
		t.Fatalf("queue = %+v sessions=%d", qr.Queue, len(qr.Sessions))
	}
	for path, want := range map[string]string{"ONE.txt": "alpha", "TWO.txt": "beta"} {
		b, err := c.ReadFile(ctx, ws.ID, path)
		if err != nil || strings.TrimSpace(string(b)) != want {
			t.Fatalf("%s = %q, %v\n%s", path, b, err, output.String())
		}
	}

	time.Sleep(300 * time.Millisecond)
	evs, err := c.ReadEvents(ctx, 1, ws.ID)
	if err != nil {
		t.Fatal(err)
	}
	var runs, queueTask, snapshots int
	for _, e := range evs {
		if strings.Contains(string(e.Payload), key) {
			t.Fatalf("event %s carries the provider secret", e.Type)
		}
		if strings.Contains(string(e.Payload), "ONE.txt") || strings.Contains(string(e.Payload), "TWO.txt") {
			t.Fatalf("event %s carries task text: %s", e.Type, e.Payload)
		}
		switch e.Type {
		case proto.EvRunFinished:
			runs++
		case proto.EvQueueAdvanced:
			queueTask++
		case proto.EvWSSnapshot:
			snapshots++
		}
	}
	if runs != 2 || queueTask != 2 || snapshots == 0 {
		t.Fatalf("events: run.finished=%d queue.advanced=%d ws.snapshot=%d\n%s", runs, queueTask, snapshots, egressLog(t, c, ws.ID, key))
	}
}
