package sim

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"context"
	"crypto/x509"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
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

// TestRunCustomRecipeEndToEnd is the P1.3 proof for `remount run custom`:
// the launcher is written under .remount/launch, sources .remount/env at
// exec time (so the broker address is discovered, not baked), the harness
// sees only a placeholder key while the upstream sees the real one through
// the broker, the run is bracketed by run.started/run.finished carrying a
// task hash but never the task text, and the launcher never travels in a
// snapshot.
func TestRunCustomRecipeEndToEnd(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("unavailable: recipe launchers require a POSIX shell in process workspaces")
	}
	var gotAuth atomic.Value
	up := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotAuth.Store(r.Header.Get("Authorization"))
		io.WriteString(w, `{"ok":true}`)
	}))
	defer up.Close()
	upHost := strings.TrimPrefix(up.URL, "https://")
	w := newWorld(t, control.Binding{ID: "b_llm", Secret: "sk-REAL-SECRET", Destinations: []string{upHost}, TTLSec: 60})
	roots := x509.NewCertPool()
	roots.AddCert(up.Certificate())
	w.nodeWithBrokerRoots("n1", nil, roots)
	c := w.client("c1")
	ctx := ctxT(t, 90*time.Second)

	recipe, err := launch.Load("custom")
	if err != nil {
		t.Fatal(err)
	}
	// The preset's host list is what a real launch would put in a typed
	// egress rule; the sim upstream is reached by its own host, so the
	// command talks to it through the broker's /d/ prefix directly.
	binding, err := launch.ParseBinding("b_llm:openai")
	if err != nil {
		t.Fatal(err)
	}
	secretText := "secret task text \"quoted\" $HOME `id`; rm -rf /"
	var progress bytes.Buffer
	res, err := launch.Start(ctx, c, launch.Options{
		Recipe:   recipe,
		Args:     []string{"sh", "-c", `printf '%s\n' "$OPENAI_API_KEY" "$REMOUNT_WORKSPACE" "$REMOUNT_RECIPE" "$REMOUNT_SANDBOX"; curl -s -H "Authorization: Bearer $OPENAI_API_KEY" "$REMOUNT_BROKER/d/` + upHost + `/v1/models"; echo; echo "$1"`, "task", secretText},
		Name:     "run-custom",
		Bindings: []launch.Binding{binding},
		Stderr:   &progress,
	})
	if err != nil {
		t.Fatal(err)
	}
	if !res.Created || res.Workspace == nil || res.Session == nil || res.Auth != launch.AuthAPIKey {
		t.Fatalf("result = %+v", res)
	}
	ws := res.Workspace
	var out []byte
	for ch := range res.Session.Chunks() {
		if ch.Stream == proto.StreamStdout {
			out = append(out, ch.Data...)
		}
	}
	if err := res.Session.Err(); err != nil {
		t.Fatal(err)
	}
	exit := res.Session.Exit()
	if exit == nil || exit.Code != 0 {
		t.Fatalf("exit = %+v, out=%q", exit, out)
	}
	lines := strings.Split(strings.TrimRight(string(out), "\n"), "\n")
	if len(lines) != 6 {
		t.Fatalf("out = %q", out)
	}
	if lines[0] != "ref:b_llm" {
		t.Fatalf("harness saw key %q, want the placeholder", lines[0])
	}
	if lines[1] != ws.ID || lines[2] != "custom" || lines[3] != launch.SandboxWorkspaceWrite {
		t.Fatalf("launcher env = %q", lines[1:4])
	}
	if lines[4] != `{"ok":true}` {
		t.Fatalf("broker round trip = %q", lines[4])
	}
	if gotAuth.Load() != "Bearer sk-REAL-SECRET" {
		t.Fatalf("upstream saw %v", gotAuth.Load())
	}
	if lines[5] != secretText {
		t.Fatalf("argv not delivered byte-for-byte: %q", lines[5])
	}
	if !strings.Contains(progress.String(), "workspace "+ws.ID+" created") {
		t.Fatalf("progress = %q", progress.String())
	}

	// The launcher lives under .remount and the spec carries the binding, the
	// placeholder env and the recipe label, but never the secret.
	if !contains(ws.Spec.Bindings, "b_llm") || ws.Spec.Env["OPENAI_API_KEY"] != "ref:b_llm" || ws.Spec.Labels["remount.recipe"] != "custom" {
		t.Fatalf("spec = %+v", ws.Spec)
	}
	if !strings.HasPrefix(ws.Spec.Env["OPENAI_BASE_URL"], "${REMOUNT_BROKER}/d/api.openai.com") {
		t.Fatalf("base url = %q", ws.Spec.Env["OPENAI_BASE_URL"])
	}
	script, err := c.ReadFile(ctx, ws.ID, recipe.LauncherPath())
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(script), ". ./.remount/env") || strings.Contains(string(script), "REMOUNT_BROKER=") || strings.Contains(string(script), "REAL-SECRET") {
		t.Fatalf("launcher:\n%s", script)
	}
	snap, err := c.Snapshot(ctx, ws.ID, true)
	if err != nil {
		t.Fatal(err)
	}
	for _, name := range snapshotNames(t, c, ctx, snap.Artifact, snap.Format) {
		if strings.Contains(name, ".remount") {
			t.Fatalf("snapshot carries %s", name)
		}
	}

	// Events: run.started and run.finished bracket the session, carry the
	// recipe and a hash, and never the task or argv.
	time.Sleep(200 * time.Millisecond)
	evs, err := c.ReadEvents(ctx, 1, ws.ID)
	if err != nil {
		t.Fatal(err)
	}
	var started, finished, resident int
	for _, e := range evs {
		if bytes.Contains(e.Payload, []byte("secret task")) || bytes.Contains(e.Payload, []byte("REAL-SECRET")) {
			t.Fatalf("event %s leaks the task or secret: %s", e.Type, e.Payload)
		}
		switch e.Type {
		case proto.EvRunStarted:
			started++
			var p struct {
				S        string `cbor:"s"`
				Recipe   string `cbor:"recipe"`
				TaskHash string `cbor:"task_hash"`
				Sandbox  string `cbor:"sandbox"`
				Auth     string `cbor:"auth"`
			}
			if err := proto.Unmarshal(e.Payload, &p); err != nil {
				t.Fatal(err)
			}
			if p.S != res.Session.ID || p.Recipe != "custom" || p.TaskHash != res.Run.TaskHash || p.Sandbox != launch.SandboxWorkspaceWrite || p.Auth != proto.RunAuthAPIKey || e.Session != res.Session.ID {
				t.Fatalf("run.started = %+v (session %q)", p, e.Session)
			}
		case proto.EvRunFinished:
			finished++
			var p struct {
				Recipe string `cbor:"recipe"`
				Exit   int    `cbor:"exit"`
			}
			if err := proto.Unmarshal(e.Payload, &p); err != nil {
				t.Fatal(err)
			}
			if p.Recipe != "custom" || p.Exit != 0 {
				t.Fatalf("run.finished = %+v", p)
			}
		case proto.EvAuthWSResident:
			resident++
		}
	}
	if started != 1 || finished != 1 || resident != 0 {
		t.Fatalf("run events: started=%d finished=%d resident=%d", started, finished, resident)
	}
}

// TestRunReusesWorkspaceAndReplaysIdempotently covers --ws: the launcher
// is rewritten in an existing workspace, a binding the workspace does not
// carry is refused before anything is written, and an idempotent replay of
// the open does not emit a second run.started.
func TestRunReusesWorkspaceAndReplaysIdempotently(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("unavailable: recipe launchers require a POSIX shell in process workspaces")
	}
	w := newWorld(t, control.Binding{ID: "b_llm", Secret: "x", Destinations: []string{"api.openai.com"}, TTLSec: 60})
	w.node("n1", nil)
	c := w.client("c1")
	ctx := ctxT(t, 60*time.Second)
	ws := mustWS(t, c, proto.WorkspaceSpec{Name: "reuse"})
	recipe, _ := launch.Load("custom")
	binding, _ := launch.ParseBinding("b_llm:openai")

	if _, err := launch.Start(ctx, c, launch.Options{Recipe: recipe, WS: ws.ID, Args: []string{"true"}, Bindings: []launch.Binding{binding}}); err == nil || !strings.Contains(err.Error(), "does not carry binding") {
		t.Fatalf("foreign binding: %v", err)
	}
	if _, err := c.ReadFile(ctx, ws.ID, recipe.LauncherPath()); err == nil {
		t.Fatal("launcher written before validation failed")
	}

	res, err := launch.Start(ctx, c, launch.Options{Recipe: recipe, WS: ws.ID, Args: []string{"sh", "-c", "echo hi"}})
	if err != nil {
		t.Fatal(err)
	}
	if res.Created || res.Auth != launch.AuthWorkspaceResident {
		t.Fatalf("result = %+v", res)
	}
	if exit, err := res.Session.Wait(ctx); err != nil || exit.Code != 0 {
		t.Fatalf("%v %+v", err, exit)
	}

	// Replay: the same open request with the same idempotency key returns the
	// same session and emits nothing new.
	run := res.Run
	req := proto.SOpenReq{WS: ws.ID, Kind: proto.SessionExec, Program: []string{"/bin/sh", recipe.LauncherPath()}, IdempotencyKey: "run-replay", Run: &run}
	s1, err := c.Exec(ctx, req)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s1.Wait(ctx); err != nil {
		t.Fatal(err)
	}
	s2, err := c.Exec(ctx, req)
	if err != nil {
		t.Fatal(err)
	}
	if s2.ID != s1.ID {
		t.Fatalf("replay opened a new session %s != %s", s2.ID, s1.ID)
	}
	time.Sleep(200 * time.Millisecond)
	evs, _ := c.ReadEvents(ctx, 1, ws.ID)
	var started, resident int
	for _, e := range evs {
		switch e.Type {
		case proto.EvRunStarted:
			started++
		case proto.EvAuthWSResident:
			resident++
		}
	}
	// Two distinct runs (Start, then the explicit open); the replay adds none.
	if started != 2 || resident != 2 {
		t.Fatalf("run.started=%d auth.workspace_resident=%d, want 2/2", started, resident)
	}

	// Malformed run metadata is refused at the node, fail-closed.
	var pe *proto.Error
	if _, err := c.Exec(ctx, proto.SOpenReq{WS: ws.ID, Kind: proto.SessionExec, Program: []string{"true"}, Run: &proto.RunInfo{Recipe: "custom", Auth: "magic"}}); !errors.As(err, &pe) || pe.Code != proto.CodeBadRequest {
		t.Fatalf("bad run auth: %v, want bad_request", err)
	}
}

func snapshotNames(t *testing.T, c *client.Client, ctx context.Context, id, format string) []string {
	t.Helper()
	rc, err := c.DownloadSnapshot(ctx, id, format)
	if err != nil {
		t.Fatal(err)
	}
	defer rc.Close()
	gz, err := gzip.NewReader(rc)
	if err != nil {
		t.Fatal(err)
	}
	var names []string
	tr := tar.NewReader(gz)
	for {
		h, err := tr.Next()
		if err == io.EOF {
			return names
		}
		if err != nil {
			t.Fatal(err)
		}
		names = append(names, h.Name)
	}
}

func contains(list []string, s string) bool {
	for _, x := range list {
		if x == s {
			return true
		}
	}
	return false
}
