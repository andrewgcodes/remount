package sim

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"remount.dev/remount/internal/client"
	"remount.dev/remount/internal/control"
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
	if runtime.GOOS == "windows" {
		t.Skip("unavailable: recipe launchers require a POSIX shell in process workspaces")
	}
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

func handoffFixtureFile(t *testing.T, home, name, content string, modified time.Time) string {
	t.Helper()
	p := filepath.Join(home, filepath.FromSlash(name))
	if err := os.MkdirAll(filepath.Dir(p), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(p, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
	if !modified.IsZero() {
		if err := os.Chtimes(p, modified, modified); err != nil {
			t.Fatal(err)
		}
	}
	return p
}

func handoffFixtureSession(t *testing.T, home, recipe, dir, id string, modified time.Time) (string, string) {
	t.Helper()
	cwd, err := json.Marshal(dir)
	if err != nil {
		t.Fatal(err)
	}
	var name, content string
	if recipe == "claude" {
		key := strings.Map(func(r rune) rune {
			if r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || r >= '0' && r <= '9' {
				return r
			}
			return '-'
		}, dir)
		name = ".claude/projects/" + key + "/" + id + ".jsonl"
		content = fmt.Sprintf("{\"type\":\"user\",\"cwd\":%s,\"sessionId\":%q,\"message\":{\"role\":\"user\",\"content\":\"remember synthetic conversation\"}}\n", cwd, id)
	} else {
		name = ".codex/sessions/2026/09/04/rollout-2026-09-04T12-00-00-" + id + ".jsonl"
		content = fmt.Sprintf("{\"type\":\"session_meta\",\"payload\":{\"cwd\":%s,\"id\":%q}}\n{\"type\":\"response_item\",\"payload\":{\"type\":\"message\",\"role\":\"user\",\"content\":[{\"type\":\"input_text\",\"text\":\"remember synthetic conversation\"}]}}\n", cwd, id)
	}
	handoffFixtureFile(t, home, name, content, modified)
	return name, content
}

func handoffFixtureRecipe(t *testing.T, name string) (*launch.Recipe, launch.Binding) {
	t.Helper()
	r, err := launch.Load(name)
	if err != nil {
		t.Fatal(err)
	}
	r.Install, r.PathKeyed, r.Configure = "", false, nil
	r.ResumeCommand = []string{"sh", "-c", "echo synthetic-resume"}
	preset := "openai"
	if name == "claude" {
		preset = "anthropic"
	}
	b, err := launch.ParseBinding("b_handoff:" + preset)
	if err != nil {
		t.Fatal(err)
	}
	return r, b
}

func TestBuiltinHandoffSelectsOnlyLatestCheckoutConversation(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("unavailable: recipe launchers require a POSIX shell in process workspaces")
	}
	for _, recipe := range []string{"claude", "codex"} {
		t.Run(recipe, func(t *testing.T) {
			r, binding := handoffFixtureRecipe(t, recipe)
			w := newWorld(t, control.Binding{ID: binding.ID, Secret: "synthetic-provider-secret", Destinations: binding.Preset.Hosts, TTLSec: 60})
			w.node("n1", nil)
			c := w.client("c1")
			ctx := ctxT(t, 60*time.Second)
			dir, err := filepath.EvalSymlinks(t.TempDir())
			if err != nil {
				t.Fatal(err)
			}
			home := t.TempDir()
			now := time.Now()
			latest, latestContent := handoffFixtureSession(t, home, recipe, dir, "11111111-1111-4111-8111-111111111111", now)
			older, _ := handoffFixtureSession(t, home, recipe, dir, "22222222-2222-4222-8222-222222222222", now.Add(-time.Hour))
			other, _ := handoffFixtureSession(t, home, recipe, filepath.Join(dir, "other-project"), "33333333-3333-4333-8333-333333333333", now.Add(time.Hour))
			for _, p := range []string{".claude.json", ".claude/.credentials.json", ".claude/settings.json", ".codex/auth.json", ".codex/config.toml", ".env", ".env.local", "nested/.env.production", "nested/.claude/config.json", "nested/.codex/auth.json", "nested/.claude.json", ".git/.env", ".git/.claude/auth.json"} {
				handoffFixtureFile(t, dir, p, "checkout-private-sentinel", time.Time{})
				handoffFixtureFile(t, home, p, "home-private-sentinel", time.Time{})
			}
			handoffFixtureFile(t, dir, latest, "checkout-private-sentinel", time.Time{})
			handoffFixtureFile(t, dir, "main.go", "package main\n", time.Time{})
			kept := map[string]string{latest: latestContent, "main.go": "package main\n"}
			if recipe == "claude" {
				sub := strings.TrimSuffix(latest, ".jsonl") + "/subagents/agent-worker.jsonl"
				handoffFixtureFile(t, home, sub, latestContent, time.Time{})
				kept[sub] = latestContent
				handoffFixtureFile(t, home, strings.TrimSuffix(latest, ".jsonl")+"/.env", "home-private-sentinel", time.Time{})
			}
			res, err := launch.Handoff(ctx, c, launch.HandoffOptions{Dir: dir, Home: home, Recipe: r, Run: launch.Options{Auth: launch.AuthAPIKey, Bindings: []launch.Binding{binding}}})
			if err != nil {
				t.Fatal(err)
			}
			if out := readOut(t, res.Session); !strings.Contains(out, "synthetic-resume") {
				t.Fatalf("resume output %q", out)
			}
			for p, want := range kept {
				got, err := c.ReadFile(ctx, res.Workspace.ID, p)
				if err != nil || string(got) != want {
					t.Errorf("carried %s = %q, %v; want %q", p, got, err, want)
				}
			}
			for _, p := range []string{older, other, ".claude.json", ".claude/.credentials.json", ".claude/settings.json", ".codex/auth.json", ".codex/config.toml", ".env", ".env.local", "nested/.env.production", "nested/.claude/config.json", "nested/.codex/auth.json", "nested/.claude.json", ".git/.env", ".git/.claude/auth.json"} {
				if got, err := c.ReadFile(ctx, res.Workspace.ID, p); err == nil {
					t.Errorf("forbidden path %s carried: %q", p, got)
				}
			}
			req, err := http.NewRequestWithContext(ctx, http.MethodGet, w.http.URL+"/v1/artifacts/"+res.Artifact, nil)
			if err != nil {
				t.Fatal(err)
			}
			req.Header.Set("Authorization", "Bearer tok")
			resp, err := http.DefaultClient.Do(req)
			if err != nil {
				t.Fatal(err)
			}
			defer resp.Body.Close()
			if resp.StatusCode != http.StatusOK {
				t.Fatalf("artifact response: %s", resp.Status)
			}
			gz, err := gzip.NewReader(resp.Body)
			if err != nil {
				t.Fatal(err)
			}
			defer gz.Close()
			tr := tar.NewReader(gz)
			for {
				h, err := tr.Next()
				if err == io.EOF {
					break
				}
				if err != nil {
					t.Fatal(err)
				}
				data, err := io.ReadAll(tr)
				if err != nil {
					t.Fatal(err)
				}
				if bytes.Contains(data, []byte("private-sentinel")) {
					t.Errorf("private data uploaded in %s", h.Name)
				}
			}
		})
	}
}

func TestBuiltinHandoffPinsConversationAcrossResume(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("unavailable: recipe launchers require POSIX sh")
	}
	const id = "11111111-1111-4111-8111-111111111111"
	for _, name := range []string{"claude", "codex"} {
		t.Run(name, func(t *testing.T) {
			r, binding := handoffFixtureRecipe(t, name)
			w := newWorld(t, control.Binding{ID: binding.ID, Secret: "synthetic-provider-secret", Destinations: binding.Preset.Hosts, TTLSec: 60})
			w.node("n1", nil)
			c := w.client("c1")
			ctx := ctxT(t, 90*time.Second)
			dir, err := filepath.EvalSymlinks(t.TempDir())
			if err != nil {
				t.Fatal(err)
			}
			home := t.TempDir()
			transcript, contents := handoffFixtureSession(t, home, name, dir, id, time.Now())
			if name == "codex" {
				contents = strings.Replace(contents, `"payload":{`, `"payload":{"model_provider":"handoff",`, 1)
				handoffFixtureFile(t, home, transcript, contents, time.Time{})
			}
			r.ResumeCommand = []string{"sh", "-c", `if [ "$1" = "11111111-1111-4111-8111-111111111111" ]; then echo recalled-selected; else echo fresh-provider-filtered; fi`, "mock", "{{.Conversation}}"}
			res, err := launch.Handoff(ctx, c, launch.HandoffOptions{Dir: dir, Home: home, Recipe: r, Run: launch.Options{Auth: launch.AuthAPIKey, Bindings: []launch.Binding{binding}}})
			if err != nil {
				t.Fatal(err)
			}
			if got := res.Workspace.Spec.Labels["remount.conversation"]; got != id {
				t.Errorf("conversation label = %q, want %q", got, id)
			}
			if out := readOut(t, res.Session); !strings.Contains(out, "recalled-selected") {
				t.Fatalf("initial handoff lost conversation: %q", out)
			}
			if _, err := c.SleepWorkspace(ctx, proto.WSSleepReq{ID: res.Workspace.ID, OnEvent: "test.resume"}); err != nil {
				t.Fatal(err)
			}
			rr, err := launch.Resume(ctx, c, launch.ResumeOptions{WS: res.Workspace.ID, Recipe: r})
			if err != nil {
				t.Fatal(err)
			}
			if out := readOut(t, rr.Session); !strings.Contains(out, "recalled-selected") {
				t.Fatalf("durable resume lost conversation: %q", out)
			}
			for _, damaged := range []string{"", "malformed", strings.ReplaceAll(contents, id, "22222222-2222-4222-8222-222222222222")} {
				if damaged == "" {
					err = c.Remove(ctx, res.Workspace.ID, transcript, false)
				} else {
					err = c.WriteFile(ctx, res.Workspace.ID, transcript, []byte(damaged), 0o600)
				}
				if err != nil {
					t.Fatal(err)
				}
				if _, err := launch.Resume(ctx, c, launch.ResumeOptions{WS: res.Workspace.ID, Recipe: r}); err == nil {
					t.Fatal("missing or invalid selected transcript silently resumed")
				}
			}
		})
	}
}

func TestBuiltinHandoffUnavailableBackendDoesNotUpload(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("unavailable: path-keyed workspaces require a POSIX mount path")
	}
	for _, recipe := range []string{"claude", "codex"} {
		t.Run(recipe, func(t *testing.T) {
			r, binding := handoffFixtureRecipe(t, recipe)
			r.PathKeyed = true
			w := newWorld(t, control.Binding{ID: binding.ID, Secret: "synthetic-provider-secret", Destinations: binding.Preset.Hosts, TTLSec: 60})
			w.node("n1", nil)
			var uploads atomic.Int32
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
				uploads.Add(1)
				http.Error(w, "upload must not happen", http.StatusForbidden)
			}))
			defer srv.Close()
			c := client.New(client.Options{Dialer: w.dialer("c1"), Token: "tok", Principal: "a_c1", ArtifactURL: srv.URL})
			defer c.Close()
			dir, err := filepath.EvalSymlinks(t.TempDir())
			if err != nil {
				t.Fatal(err)
			}
			home := t.TempDir()
			handoffFixtureSession(t, home, recipe, dir, "11111111-1111-4111-8111-111111111111", time.Now())
			_, err = launch.Handoff(ctxT(t, 30*time.Second), c, launch.HandoffOptions{Dir: dir, Home: home, Recipe: r, Run: launch.Options{Auth: launch.AuthAPIKey, Bindings: []launch.Binding{binding}}})
			if err == nil || !strings.Contains(err.Error(), "namespaced backend") || uploads.Load() != 0 {
				t.Fatalf("backend preflight: %v; uploads %d", err, uploads.Load())
			}
		})
	}
}

func TestBuiltinHandoffRejectsBeforeUpload(t *testing.T) {
	for _, recipe := range []string{"claude", "codex"} {
		for _, scenario := range []string{"no-binding", "wrong-provider", "no-state", "wrong-cwd", "wrong-id", "invalid-uuid", "exclude-state", "exclude-transcript", "symlink-file", "symlink-state", "backend", "mount-path"} {
			t.Run(recipe+"/"+scenario, func(t *testing.T) {
				if runtime.GOOS == "windows" && (strings.HasPrefix(scenario, "symlink") || scenario == "backend") {
					t.Skip("unavailable: symlinks need Windows privileges and path-keyed workspaces need POSIX mount paths")
				}
				r, binding := handoffFixtureRecipe(t, recipe)
				dir, err := filepath.EvalSymlinks(t.TempDir())
				if err != nil {
					t.Fatal(err)
				}
				home := t.TempDir()
				cwd := dir
				if scenario == "wrong-cwd" {
					cwd = filepath.Join(dir, "elsewhere")
				}
				name, content := handoffFixtureSession(t, home, recipe, cwd, "11111111-1111-4111-8111-111111111111", time.Now())
				local := filepath.Join(home, filepath.FromSlash(name))
				if scenario == "wrong-id" {
					handoffFixtureFile(t, home, name, strings.ReplaceAll(content, "11111111-1111-4111-8111-111111111111", "22222222-2222-4222-8222-222222222222"), time.Time{})
				}
				if scenario == "invalid-uuid" {
					bad := strings.Repeat("z", 36)
					handoffFixtureFile(t, home, strings.ReplaceAll(name, "11111111-1111-4111-8111-111111111111", bad), strings.ReplaceAll(content, "11111111-1111-4111-8111-111111111111", bad), time.Time{})
					if err := os.Remove(local); err != nil {
						t.Fatal(err)
					}
				}
				if scenario == "no-state" {
					if err := os.Remove(local); err != nil {
						t.Fatal(err)
					}
				}
				if scenario == "symlink-file" {
					outside := handoffFixtureFile(t, t.TempDir(), filepath.Base(local), content, time.Time{})
					if err := os.Remove(local); err != nil {
						t.Fatal(err)
					}
					if err := os.Symlink(outside, local); err != nil {
						t.Fatal(err)
					}
				}
				if scenario == "symlink-state" {
					state := filepath.Join(home, "."+recipe)
					outside := filepath.Join(t.TempDir(), "state")
					if err := os.Rename(state, outside); err != nil {
						t.Fatal(err)
					}
					if err := os.Symlink(outside, state); err != nil {
						t.Fatal(err)
					}
				}
				run := launch.Options{Auth: launch.AuthAPIKey, Bindings: []launch.Binding{binding}}
				want := "saved"
				switch scenario {
				case "exclude-state":
					run.Exclude, want = []string{"." + recipe}, "exclude"
				case "exclude-transcript":
					run.Exclude, want = []string{name}, "exclude"
				case "no-binding":
					run.Bindings, want = nil, "binding"
				case "wrong-provider":
					wrong, err := launch.ParseBinding("b_wrong:google")
					if err != nil {
						t.Fatal(err)
					}
					run.Bindings, want = []launch.Binding{wrong}, "does not consume"
				case "backend":
					r.PathKeyed, run.Backend, want = true, "process", "mount namespace"
				case "mount-path":
					r.PathKeyed, run.MountPath, want = true, "/different", "must stay"
				}
				var uploads atomic.Int32
				srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
					uploads.Add(1)
					http.Error(w, "upload must not happen", http.StatusForbidden)
				}))
				defer srv.Close()
				c := client.New(client.Options{ArtifactURL: srv.URL})
				_, err = launch.Handoff(ctxT(t, 15*time.Second), c, launch.HandoffOptions{Dir: dir, Home: home, Recipe: r, Run: run})
				if err == nil || !strings.Contains(err.Error(), want) {
					t.Errorf("error = %v; want %q", err, want)
				}
				if uploads.Load() != 0 {
					t.Errorf("invalid handoff uploaded %d artifacts", uploads.Load())
				}
			})
		}
	}
}

// TestQueueRunsTasksAcrossSleepAndMove is the P1.4 proof for
// `remount run --queue`: tasks run in order in one workspace, the workspace
// sleeps on the control plane's timer between them, a failing task stops the
// queue with the cursor on it, and the same queue is continued on another
// node after the first one is gone with nothing about the cursor stored in
// the tree. Events carry indexes and exits, never the task text.
func TestQueueRunsTasksAcrossSleepAndMove(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("unavailable: recipe launchers require a POSIX shell in process workspaces")
	}
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
