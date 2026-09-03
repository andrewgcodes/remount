package sim

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"remount.dev/remount/internal/client"
	"remount.dev/remount/internal/launch"
	"remount.dev/remount/internal/proto"
)

// fakeHarness is a recipe whose "conversation" is a counter file under its
// state dir: every resume reads it, prints it and bumps it. That makes a
// lost or duplicated state hand-off observable as a wrong number.
func fakeHarness(t *testing.T, pathKeyed bool) *launch.Recipe {
	t.Helper()
	script := `mkdir -p .fake; n=$(cat .fake/counter 2>/dev/null || echo 0); echo "$((n+1))" > .fake/counter; echo "mode=$0 counter=$n task=$1"; [ "$1" = wait ] && sleep 60; exit 0`
	r := &launch.Recipe{
		Name:          "fake",
		Auth:          launch.AuthWorkspaceResident,
		Command:       []string{"sh", "-c", script, "start", "{{.Task}}"},
		ResumeCommand: []string{"sh", "-c", script, "resume", "{{.Task}}"},
		StateDirs:     []string{".fake", ".fake.json"},
		PathKeyed:     pathKeyed,
	}
	if err := r.Validate(); err != nil {
		t.Fatal(err)
	}
	return r
}

func readOut(t *testing.T, s *client.Session) string {
	t.Helper()
	var out []byte
	for ch := range s.Chunks() {
		if ch.Stream == proto.StreamStdout || ch.Stream == proto.StreamStderr {
			out = append(out, ch.Data...)
		}
	}
	if err := s.Err(); err != nil {
		t.Fatal(err)
	}
	return string(out)
}

// TestHandoffThenResumeCarriesHarnessState is the P1.4 proof: a local
// checkout and the harness's ~/.state travel together into one workspace,
// the resume command sees the counter where it left off, `remount resume`
// continues it (waking a sleeping workspace when needed) and reattaches to a
// live one rather than starting a second, and a path-keyed harness on a node
// without a mount namespace is refused with a clear reason rather than
// symlinked into place.
func TestHandoffThenResumeCarriesHarnessState(t *testing.T) {
	w := newWorld(t)
	w.node("n1", nil)
	c := w.client("c1")
	ctx := ctxT(t, 120*time.Second)

	dir := filepath.Join(t.TempDir(), "proj")
	home := filepath.Join(t.TempDir(), "home")
	for _, d := range []string{filepath.Join(dir, ".git"), filepath.Join(home, ".fake"), filepath.Join(home, ".other-harness")} {
		if err := os.MkdirAll(d, 0o755); err != nil {
			t.Fatal(err)
		}
	}
	os.WriteFile(filepath.Join(dir, "main.go"), []byte("package main\n"), 0o644)
	os.WriteFile(filepath.Join(dir, ".git", "HEAD"), []byte("ref: refs/heads/main\n"), 0o644)
	os.WriteFile(filepath.Join(home, ".fake", "counter"), []byte("7\n"), 0o644)
	os.WriteFile(filepath.Join(home, ".fake", "secretish.json"), []byte(`{"session":"abc"}`), 0o600)

	fake := fakeHarness(t, false)
	other := fakeHarness(t, false)
	other.Name, other.StateDirs = "other", []string{".other-harness"}
	if _, err := launch.DetectRecipe(home, []*launch.Recipe{fake, other}); err == nil || !strings.Contains(err.Error(), "several") {
		t.Fatalf("ambiguous detection: %v", err)
	}
	if got, err := launch.DetectRecipe(home, []*launch.Recipe{fake}); err != nil || got != fake {
		t.Fatalf("detect: %v %v", got, err)
	}
	if _, err := launch.DetectRecipe(t.TempDir(), []*launch.Recipe{fake, other}); err == nil {
		t.Fatal("empty home detected a harness")
	}

	var progress bytes.Buffer
	res, err := launch.Handoff(ctx, c, launch.HandoffOptions{
		Dir: dir, Home: home, Candidates: []*launch.Recipe{fake},
		Run: launch.Options{Task: "finish the refactor", Name: "handoff", Stderr: &progress},
	})
	if err != nil {
		t.Fatalf("%v\n%s", err, progress.String())
	}
	if res.Recipe != fake || len(res.StateCarried) != 1 || res.StateCarried[0] != ".fake" || len(res.StateMissing) != 1 || res.StateMissing[0] != ".fake.json" {
		t.Fatalf("handoff = %+v", res)
	}
	if res.MountPath != "" || res.Workspace.Spec.MountPath != "" {
		t.Fatalf("non path-keyed recipe pinned a mount path: %+v", res.Workspace.Spec)
	}
	ws := res.Workspace
	// The origin label is the canonical path (symlinks resolved), which is
	// what a later resume from the same directory computes too.
	canonical, err := filepath.EvalSymlinks(dir)
	if err != nil {
		t.Fatal(err)
	}
	if ws.Spec.Labels[launch.LabelRecipe] != "fake" || ws.Spec.Labels[launch.LabelOrigin] != canonical || ws.Spec.RestoreFrom != res.Artifact {
		t.Fatalf("labels %+v restore %q", ws.Spec.Labels, ws.Spec.RestoreFrom)
	}
	out := readOut(t, res.Session)
	if !strings.Contains(out, "mode=resume counter=7 task=finish the refactor") {
		t.Fatalf("resume output %q", out)
	}
	for path, want := range map[string]string{"main.go": "package main\n", ".fake/counter": "8\n", ".fake/secretish.json": `{"session":"abc"}`, ".git/HEAD": "ref: refs/heads/main\n"} {
		b, err := c.ReadFile(ctx, ws.ID, path)
		if err != nil || string(b) != want {
			t.Fatalf("%s = %q, %v; want %q", path, b, err, want)
		}
	}
	if _, err := c.ReadFile(ctx, ws.ID, ".other-harness"); err == nil {
		t.Fatal("another harness's state was carried along")
	}

	// remount resume on a claimed workspace with no live harness starts one.
	rr, err := launch.Resume(ctx, c, launch.ResumeOptions{WS: ws.ID, Recipe: fake, Task: "next step", Attach: true, Stderr: &progress})
	if err != nil {
		t.Fatalf("%v\n%s", err, progress.String())
	}
	if rr.Attached || rr.Run == nil {
		t.Fatalf("resume = %+v", rr)
	}
	if out := readOut(t, rr.Session); !strings.Contains(out, "mode=resume counter=8 task=next step") {
		t.Fatalf("second resume output %q", out)
	}

	// Sleep it; resume must wake it and continue from the checkpointed state.
	if _, err := c.SleepWorkspace(ctx, proto.WSSleepReq{ID: ws.ID, OnEvent: "human.returned"}); err != nil {
		t.Fatal(err)
	}
	if got, _ := c.GetWorkspace(ctx, ws.ID); got.State != proto.WSPaused {
		t.Fatalf("state %s", got.State)
	}
	rr, err = launch.Resume(ctx, c, launch.ResumeOptions{WS: ws.ID, Recipe: fake, Stderr: &progress})
	if err != nil {
		t.Fatalf("%v\n%s", err, progress.String())
	}
	if out := readOut(t, rr.Session); !strings.Contains(out, "mode=resume counter=9 task="+launch.DefaultResumeTask) {
		t.Fatalf("resume after sleep output %q", out)
	}

	// A live harness session is joined, not duplicated.
	live, err := launch.Resume(ctx, c, launch.ResumeOptions{WS: ws.ID, Recipe: fake, Task: "wait", Stderr: &progress})
	if err != nil {
		t.Fatal(err)
	}
	// Wait for the harness to have bumped the counter before joining it.
	started := make(chan struct{})
	go func() {
		var seen []byte
		signaled := false
		for ch := range live.Session.Chunks() {
			seen = append(seen, ch.Data...)
			if !signaled && bytes.Contains(seen, []byte("task=wait")) {
				signaled = true
				close(started)
			}
		}
	}()
	select {
	case <-started:
	case <-time.After(20 * time.Second):
		t.Fatal("wait session did not start")
	}
	deadline := time.Now().Add(20 * time.Second)
	for {
		sessions, err := c.ListSessions(ctx, ws.ID)
		if err != nil {
			t.Fatal(err)
		}
		running := 0
		for _, s := range sessions {
			if s.Info.Run != nil && !s.Exited {
				running++
			}
		}
		if running == 1 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("live run session not visible: %+v", sessions)
		}
		time.Sleep(50 * time.Millisecond)
	}
	joined, err := launch.Resume(ctx, c, launch.ResumeOptions{WS: ws.ID, Recipe: fake, Attach: true, Stderr: &progress})
	if err != nil {
		t.Fatal(err)
	}
	if !joined.Attached || joined.Session == nil || joined.Session.ID != live.Session.ID || joined.Run != nil {
		t.Fatalf("attach = %+v (live %s)", joined, live.Session.ID)
	}
	if err := live.Session.Close(ctx, true); err != nil {
		t.Fatal(err)
	}
	if b, _ := c.ReadFile(ctx, ws.ID, ".fake/counter"); string(b) != "11\n" {
		t.Fatalf("counter after attach = %q; a second resume ran", b)
	}

	// Path-keyed harness: the mount path is pinned to the checkout, no
	// process-backend node may claim it, and the failure says why.
	keyed := fakeHarness(t, true)
	progress.Reset()
	bad, err := launch.Handoff(ctx, c, launch.HandoffOptions{
		Dir: dir, Home: home, Recipe: keyed,
		Run: launch.Options{Task: "x", Name: "keyed", Stderr: &progress, WaitClaimed: 3 * time.Second},
	})
	if err == nil || !strings.Contains(err.Error(), "namespaced backend") {
		t.Fatalf("path-keyed handoff onto a process node: %v", err)
	}
	if bad == nil || bad.MountPath != canonical || bad.Workspace == nil || bad.Workspace.Spec.MountPath != canonical {
		t.Fatalf("path-keyed handoff = %+v", bad)
	}
	if got, _ := c.GetWorkspace(ctx, bad.Workspace.ID); got.State != proto.WSPending || got.Node != "" {
		t.Fatalf("unclaimable workspace: state %s node %q", got.State, got.Node)
	}
	// Forcing the process backend is refused before anything is created.
	if _, err := launch.Handoff(ctx, c, launch.HandoffOptions{
		Dir: dir, Home: home, Recipe: keyed,
		Run: launch.Options{Task: "x", Backend: "process", WaitClaimed: time.Second},
	}); err == nil || !strings.Contains(err.Error(), "mount namespace") {
		t.Fatalf("forced process backend: %v", err)
	}
	if _, err := launch.Resume(ctx, c, launch.ResumeOptions{WS: "ws_nope", Recipe: fake}); err == nil {
		t.Fatal("resume of an unknown workspace")
	}
	var pe *proto.Error
	if err := c.DestroyWorkspace(ctx, bad.Workspace.ID); err != nil && !(errors.As(err, &pe) && pe.Code == proto.CodeNotFound) {
		t.Fatal(err)
	}
}

// TestQueueRunsTasksAcrossSleepAndMove is the P1.4 proof for
// `remount run --queue`: tasks run in order in one workspace, the workspace
// sleeps on the control plane's timer between them, a failing task stops the
// queue with the cursor on it, and the same queue is continued on another
// node after the first one is gone with nothing about the cursor stored in
// the tree. Events carry indexes and exits, never the task text.
func TestQueueRunsTasksAcrossSleepAndMove(t *testing.T) {
	w := newWorld(t)
	w.node("n1", nil)
	c := w.client("c1")
	ctx := ctxT(t, 180*time.Second)

	r := &launch.Recipe{
		Name:    "queued",
		Auth:    launch.AuthWorkspaceResident,
		Command: []string{"sh", "-c", `echo "$1" >> log.txt; echo "ran $1 on $(cat .remount/node 2>/dev/null || echo ?)"; case "$1" in *fails*) [ -f .retry ] || exit 3;; esac`, "queued", "{{.Task}}"},
	}
	if err := r.Validate(); err != nil {
		t.Fatal(err)
	}
	tasks, err := launch.ParseQueueFile(strings.NewReader("# the plan\nalpha\n\nbravo one \\\n  bravo two\ncharlie-fails\ndelta\n"))
	if err != nil {
		t.Fatal(err)
	}
	if len(tasks) != 4 || tasks[1] != "bravo one \n  bravo two" {
		t.Fatalf("tasks = %q", tasks)
	}
	var progress, output bytes.Buffer
	res, err := launch.RunQueue(ctx, c, launch.QueueOptions{
		Tasks: tasks, SleepAfter: time.Second, Output: &output,
		Run: launch.Options{Recipe: r, Name: "queue", Stderr: &progress},
	})
	if err == nil || !strings.Contains(err.Error(), "task 3 exited 3") {
		t.Fatalf("queue error = %v\n%s", err, progress.String())
	}
	if res.Queue == nil || res.Queue.Status != proto.QueueFailed || res.Queue.Cursor != 2 || res.Queue.Items[2].Exit != 3 || len(res.Sessions) != 3 {
		t.Fatalf("queue = %+v sessions %v", res.Queue, res.Sessions)
	}
	ws := res.Workspace
	if b, _ := c.ReadFile(ctx, ws.ID, "log.txt"); string(b) != "alpha\nbravo one \n  bravo two\ncharlie-fails\n" {
		t.Fatalf("log = %q", b)
	}
	timers, _ := c.ListTimers(ctx)
	fired := 0
	for _, tm := range timers {
		if tm.WS == ws.ID && tm.Fired {
			fired++
		}
	}
	if fired != 2 {
		t.Fatalf("sleep timers fired = %d, want one between each pair of tasks", fired)
	}
	if _, err := launch.RunQueue(ctx, c, launch.QueueOptions{
		Tasks: []string{"x"}, Run: launch.Options{Recipe: r, WS: ws.ID, Stderr: &progress},
	}); err == nil {
		t.Fatal("second unfinished queue on one workspace was accepted")
	}

	// Park it, lose the node, continue elsewhere.
	if err := c.WriteFile(ctx, ws.ID, ".retry", []byte("ok"), 0); err != nil {
		t.Fatal(err)
	}
	if _, err := c.SleepWorkspace(ctx, proto.WSSleepReq{ID: ws.ID, OnEvent: "operator.back"}); err != nil {
		t.Fatal(err)
	}
	w.stopNode("n1")
	n2 := w.node("n2", nil)
	progress.Reset()
	cont, err := launch.RunQueue(ctx, c, launch.QueueOptions{
		Continue: res.Queue.ID, Output: &output,
		Run: launch.Options{Recipe: r, Stderr: &progress},
	})
	if err != nil {
		t.Fatalf("%v\n%s", err, progress.String())
	}
	if cont.Queue.Status != proto.QueueDone || cont.Queue.Cursor != 4 || cont.Queue.Items[2].Attempts != 2 || cont.Queue.Items[3].Attempts != 1 {
		t.Fatalf("continued queue = %+v", cont.Queue)
	}
	if cont.Workspace.Node != n2.ID() {
		t.Fatalf("continued on %q", cont.Workspace.Node)
	}
	if b, _ := c.ReadFile(ctx, ws.ID, "log.txt"); string(b) != "alpha\nbravo one \n  bravo two\ncharlie-fails\ncharlie-fails\ndelta\n" {
		t.Fatalf("log after move = %q", b)
	}
	if !strings.Contains(progress.String(), "sleeping "+ws.ID+" for 1s") {
		t.Fatalf("continued driver did not honor the recorded sleep:\n%s", progress.String())
	}
	if _, err := launch.RunQueue(ctx, c, launch.QueueOptions{Continue: res.Queue.ID, Run: launch.Options{Recipe: r}}); err == nil || !strings.Contains(err.Error(), "is done") {
		t.Fatalf("continue of a done queue: %v", err)
	}
	listed, err := c.ListQueues(ctx, ws.ID)
	if err != nil || len(listed) != 1 || listed[0].ID != res.Queue.ID {
		t.Fatalf("list = %+v %v", listed, err)
	}

	evs, err := c.ReadEvents(ctx, 1, ws.ID)
	if err != nil {
		t.Fatal(err)
	}
	created, advanced := 0, 0
	var indexes []int
	for _, e := range evs {
		text := string(e.Payload)
		for _, task := range tasks {
			if strings.Contains(text, strings.Split(task, " ")[0]) {
				t.Fatalf("event %s leaks task text: %s", e.Type, text)
			}
		}
		switch e.Type {
		case proto.EvQueueCreated:
			created++
		case proto.EvQueueAdvanced:
			advanced++
			var p struct {
				Queue  string `cbor:"queue"`
				Index  int    `cbor:"index"`
				Exit   int    `cbor:"exit"`
				Status string `cbor:"status"`
			}
			if err := proto.Unmarshal(e.Payload, &p); err != nil {
				t.Fatal(err)
			}
			if p.Queue != res.Queue.ID {
				t.Fatalf("advanced payload %+v", p)
			}
			indexes = append(indexes, p.Index)
			if p.Index == 2 && p.Exit == 3 && p.Status != proto.QueueFailed {
				t.Fatalf("failed advance status %+v", p)
			}
		}
	}
	if created != 1 || advanced != 5 || len(indexes) != 5 || indexes[2] != 2 || indexes[3] != 2 || indexes[4] != 3 {
		t.Fatalf("created %d advanced %d indexes %v", created, advanced, indexes)
	}
	if _, err := c.ReadFile(ctx, ws.ID, ".remount/queue"); err == nil {
		t.Fatal("queue state written into the workspace")
	}
}
