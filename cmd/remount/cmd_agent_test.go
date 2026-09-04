package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"remount.dev/remount/internal/acp"
	"remount.dev/remount/internal/acp/acptest"
	"remount.dev/remount/internal/client"
	"remount.dev/remount/internal/launch"
	"remount.dev/remount/internal/proto"
	"remount.dev/remount/internal/server"
	"remount.dev/remount/internal/workspace"
)

// The CLI tests re-exec their own binary as the ACP harness, like the sim.
const (
	cliFakeACPEnv = "REMOUNT_CLI_FAKE_ACP"
	cliFakeACPArg = "-remount-cli-fake-acp"
)

func TestMain(m *testing.M) {
	if len(os.Args) == 2 && os.Args[1] == cliFakeACPArg {
		acptest.Main(acptest.Config{Info: acp.Implementation{Name: "cli-fake", Version: "t"}})
		return
	}
	if os.Getenv(cliFakeACPEnv) == "1" {
		acptest.Main(acptest.Config{Info: acp.Implementation{Name: "cli-fake", Version: "t"}})
		return
	}
	os.Exit(m.Run())
}

// standaloneForTest runs a control plane and a process-backend node in this
// process, the way `remount standalone` does, and returns the server URL.
func standaloneForTest(t *testing.T) string {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	data := t.TempDir()
	srv, err := server.New(server.Options{DataDir: filepath.Join(data, "server"), Logger: slog.New(slog.NewTextHandler(io.Discard, nil)), Mode: server.ModeStandalone})
	if err != nil {
		t.Fatal(err)
	}
	go func() { _ = srv.Serve(ctx, "127.0.0.1:0") }()
	readyCtx, readyCancel := context.WithTimeout(ctx, 20*time.Second)
	addr, err := srv.WaitReady(readyCtx)
	readyCancel()
	if err != nil {
		cancel()
		t.Fatal(err)
	}
	c := common{server: "http://" + addr}
	n, err := buildNode(filepath.Join(data, "node"), "", c, map[string]string{"standalone": "true"}, "process", workspace.DefaultImage(version), nil, []string{"127.0.0.1", "localhost"}, nodeResourceOptions{})
	if err != nil {
		cancel()
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
	return c.server
}

func fakeACPRecipe(t *testing.T) (string, *launch.Recipe, string) {
	t.Helper()
	exe, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	yaml := "name: fakeacp\nauth: workspace_resident\ncommand: [\"/bin/true\"]\nacp:\n  command: [\"" + filepath.ToSlash(exe) + "\", \"" + cliFakeACPArg + "\"]\n"
	file := filepath.Join(t.TempDir(), "fakeacp.yaml")
	if err := os.WriteFile(file, []byte(yaml), 0o600); err != nil {
		t.Fatal(err)
	}
	r, y, err := loadRecipe("fakeacp", file)
	if err != nil {
		t.Fatal(err)
	}
	return file, r, y
}

// TestAgentCreateWatchConversation drives the whole CLI path: plan and
// create an Agent from a recipe file, watch its transcript as a conversation,
// type a follow-up on stdin, and see it finish by --max-turns with the
// harness's echoed replies in order.
func TestAgentCreateWatchConversation(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("unavailable: recipe ACP launcher scripts require a POSIX shell")
	}
	if os.Getenv("REMOUNT_TEST_LOCAL_SERVER") == "" && testing.Short() {
		t.Skip("short")
	}
	t.Setenv("REMOUNT_AUTOSTART", "off")
	serverURL := standaloneForTest(t)
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	_, recipe, yaml := fakeACPRecipe(t)
	c := common{server: serverURL}
	f := agentSeedFlags{sandbox: launch.SandboxWorkspaceWrite, approve: launch.ApproveNever, maxTurns: 2, name: "conv"}
	p, err := planAgent(ctx, &c, &f, recipe, yaml, "hello there")
	if err != nil {
		t.Fatal(err)
	}
	if p.policy.MaxTurns != 2 || p.policy.Approve != proto.ApproveNever {
		t.Fatalf("policy=%+v", p.policy)
	}
	cl := c.client()
	defer cl.Close()
	a, err := p.create(ctx, cl, nil, "cli-create-1")
	if err != nil {
		t.Fatal(err)
	}
	again, err := p.create(ctx, cl, nil, "cli-create-1")
	if err != nil || again.ID != a.ID {
		t.Fatalf("idempotent create: %v / %v", again, err)
	}
	if a.Spec.Recipe != "fakeacp" || a.Spec.RecipeYAML == "" || a.Mode != proto.AgentModeACP || a.Name != "conv" {
		t.Fatalf("agent=%+v", a)
	}

	in, inw := io.Pipe()
	var out, errOut bytes.Buffer
	watchErr := make(chan error, 1)
	go func() {
		watchErr <- watchAgent(ctx, cl, a.ID, 0, watchOptions{follow: true, interactive: true, showStderr: true, stdin: in, stdout: &out, stderr: &errOut})
	}()
	// The first turn echoes the task; once the agent waits for input a typed
	// line becomes the second (and last) turn.
	waitFor(t, ctx, func() bool {
		ag, err := cl.GetAgent(ctx, a.ID)
		return err == nil && ag.Status == proto.AgentWaitingInput
	})
	if _, err := io.WriteString(inw, "second turn\n"); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-watchErr:
		if err != nil {
			t.Fatalf("watch: %v\nstdout:\n%s\nstderr:\n%s", err, out.String(), errOut.String())
		}
	case <-ctx.Done():
		t.Fatalf("watch did not return\nstdout:\n%s\nstderr:\n%s", out.String(), errOut.String())
	}
	_ = inw.Close()
	got := out.String()
	for _, want := range []string{"> hello there", "hello there", "> second turn", "second turn"} {
		if !strings.Contains(got, want) {
			t.Fatalf("stdout lacks %q:\n%s\nstderr:\n%s", want, got, errOut.String())
		}
	}
	if strings.Index(got, "> hello there") > strings.Index(got, "> second turn") {
		t.Fatalf("turns out of order:\n%s", got)
	}
	final, err := cl.GetAgent(ctx, a.ID)
	if err != nil || final.Status != proto.AgentFinished || final.Turns != 2 {
		t.Fatalf("final=%+v err=%v", final, err)
	}
	if !strings.Contains(errOut.String(), "[finished: max_turns 2 reached]") {
		t.Fatalf("stderr lacks the finish line:\n%s", errOut.String())
	}

	// Finishing by policy stops the harness; the run closes and its exit
	// chunk lands in the mirror shortly after the status flips.
	waitFor(t, ctx, func() bool {
		ag, err := cl.GetAgent(ctx, a.ID)
		return err == nil && len(ag.Runs) > 0 && ag.Runs[len(ag.Runs)-1].State == proto.AgentRunDone
	})

	// --no-follow replays the durable transcript from any cursor, and --raw
	// exposes decoded frames without the renderer.
	var replay bytes.Buffer
	if err := watchAgent(ctx, cl, a.ID, 0, watchOptions{stdout: &replay, stderr: io.Discard}); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(replay.String(), "> second turn") {
		t.Fatalf("replay:\n%s", replay.String())
	}
	var raw bytes.Buffer
	if err := watchAgent(ctx, cl, a.ID, 0, watchOptions{raw: true, stdout: &raw, stderr: io.Discard}); err != nil {
		t.Fatal(err)
	}
	dec := json.NewDecoder(&raw)
	var frames, exits int
	for dec.More() {
		var rec map[string]any
		if err := dec.Decode(&rec); err != nil {
			t.Fatal(err)
		}
		if _, ok := rec["frame"]; ok {
			frames++
		}
		if _, ok := rec["exit"]; ok {
			exits++
		}
	}
	if frames == 0 || exits == 0 {
		t.Fatalf("raw: frames=%d exits=%d", frames, exits)
	}

	// The diff of an untouched checkout-less workspace is a clean git error
	// surfaced with a stable code, never a crash.
	if _, err := cl.Diff(ctx, a.ID, false); !errors.Is(err, &proto.Error{Code: proto.CodeUnreachable}) {
		t.Fatalf("diff without a repository: %v", err)
	}
}

// TestAgentWatchPTYModeAndRenderer covers the renderer on synthesized records:
// prompts, streamed text, tool calls, plans, permission requests, thoughts,
// stderr, exits and gaps.
func TestAgentTranscriptRenderer(t *testing.T) {
	var out, errOut bytes.Buffer
	r := newTranscriptRenderer(&out, &errOut, watchOptions{showStderr: true})
	frame := func(stream uint8, v any) *proto.TranscriptRecord {
		raw, _ := json.Marshal(v)
		return &proto.TranscriptRecord{Stream: stream, Data: proto.MustMarshal(proto.ACPFrameRecord{Frame: raw})}
	}
	title := "read main.go"
	recs := []*proto.TranscriptRecord{
		frame(proto.StreamACPOut, map[string]any{"jsonrpc": "2.0", "id": 1, "method": acp.MethodSessionPrompt, "params": acp.PromptRequest{SessionID: "s", Prompt: textBlocks("fix the bug")}}),
		frame(proto.StreamACPIn, map[string]any{"jsonrpc": "2.0", "method": acp.MethodSessionUpdate, "params": map[string]any{"sessionId": "s", "update": map[string]any{"sessionUpdate": "agent_thought_chunk", "content": map[string]any{"type": "text", "text": "hmm"}}}}),
		frame(proto.StreamACPIn, map[string]any{"jsonrpc": "2.0", "method": acp.MethodSessionUpdate, "params": map[string]any{"sessionId": "s", "update": map[string]any{"sessionUpdate": "tool_call", "toolCallId": "tc1", "title": title, "kind": "read"}}}),
		frame(proto.StreamACPIn, map[string]any{"jsonrpc": "2.0", "method": acp.MethodSessionUpdate, "params": map[string]any{"sessionId": "s", "update": map[string]any{"sessionUpdate": "agent_message_chunk", "content": map[string]any{"type": "text", "text": "Looking"}}}}),
		frame(proto.StreamACPIn, map[string]any{"jsonrpc": "2.0", "method": acp.MethodSessionUpdate, "params": map[string]any{"sessionId": "s", "update": map[string]any{"sessionUpdate": "tool_call_update", "toolCallId": "tc1", "status": "completed"}}}),
		frame(proto.StreamACPIn, map[string]any{"jsonrpc": "2.0", "id": 7, "method": acp.MethodSessionRequestPermission, "params": map[string]any{"sessionId": "s", "toolCall": map[string]any{"toolCallId": "tc1"}, "options": []any{}}}),
		frame(proto.StreamACPIn, map[string]any{"jsonrpc": "2.0", "method": acp.MethodSessionUpdate, "params": map[string]any{"sessionId": "s", "update": map[string]any{"sessionUpdate": "plan", "entries": []any{map[string]any{"content": "step one", "priority": "high", "status": "completed"}, map[string]any{"content": "step two", "priority": "low", "status": "in_progress"}}}}}),
		frame(proto.StreamACPIn, map[string]any{"jsonrpc": "2.0", "method": acp.MethodSessionUpdate, "params": map[string]any{"sessionId": "s", "update": map[string]any{"sessionUpdate": "agent_message_chunk", "content": map[string]any{"type": "text", "text": " done.\n"}}}}),
		frame(proto.StreamACPIn, map[string]any{"jsonrpc": "2.0", "id": 1, "result": acp.PromptResponse{StopReason: acp.StopReasonMaxTokens}}),
		{Stream: proto.StreamStderr, Data: []byte("warn: slow\n")},
		{Stream: proto.StreamExit, Data: proto.MustMarshal(proto.ExitInfo{Code: 3})},
	}
	// A replayed frame (session/load history) is not rendered twice.
	replayed := frame(proto.StreamACPIn, map[string]any{"jsonrpc": "2.0", "method": acp.MethodSessionUpdate, "params": map[string]any{"sessionId": "s", "update": map[string]any{"sessionUpdate": "agent_message_chunk", "content": map[string]any{"type": "text", "text": "OLD"}}}})
	var rep proto.ACPFrameRecord
	_ = proto.Unmarshal(replayed.Data, &rep)
	rep.Replayed = true
	replayed.Data = proto.MustMarshal(rep)
	recs = append(recs, replayed)
	r.gap(3, 5)
	for _, rec := range recs {
		r.record(rec)
	}
	r.flush()
	got, errs := out.String(), errOut.String()
	if !strings.Contains(got, "> fix the bug") || !strings.Contains(got, "Looking\n") || !strings.Contains(got, " done.\n") || strings.Contains(got, "OLD") {
		t.Fatalf("stdout:\n%s", got)
	}
	for _, want := range []string{"[transcript records 3..4 were evicted]", "* read main.go (read)", "= read main.go: done", "? permission: read main.go", "[x] step one", "[>] step two", "[turn ended: max_tokens]", "  | warn: slow", "[harness exited: exit status 3]"} {
		if !strings.Contains(errs, want) {
			t.Fatalf("stderr lacks %q:\n%s", want, errs)
		}
	}
	if strings.Contains(errs, "hmm") {
		t.Fatalf("thoughts shown without --thoughts:\n%s", errs)
	}
	// Streamed text mid-line is ended before an interleaved note.
	if !strings.Contains(got, "Looking\n") && !strings.Contains(got, "Looking done.\n") {
		t.Fatalf("midline handling:\n%s", got)
	}
}

func TestAgentCommandsValidateBeforeDialing(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("unavailable: recipe ACP launcher scripts require a POSIX shell")
	}
	ctx := context.Background()
	t.Setenv("REMOUNT_AUTOSTART", "off")
	file, _, _ := fakeACPRecipe(t)
	cases := []struct {
		name string
		args []string
		want string
	}{
		{"no sub", nil, "agent create"},
		{"unknown sub", []string{"frobnicate"}, "unknown subcommand"},
		{"create no recipe", []string{"create"}, "recipes:"},
		{"create non-acp recipe", []string{"create", "custom", "--", "x"}, "no acp command"},
		{"create task required", []string{"create", "fakeacp", "--recipe-file", file}, "needs a task"},
		{"create bad approve", []string{"create", "fakeacp", "--recipe-file", file, "--approve", "always", "--", "x"}, "--approve"},
		{"create negative turns", []string{"create", "fakeacp", "--recipe-file", file, "--max-turns", "-1", "--", "x"}, "--max-turns"},
		{"create dir and ws", []string{"create", "fakeacp", "--recipe-file", file, "--dir", ".", "--ws", "ws_a", "--", "x"}, "mutually exclusive"},
		{"get arity", []string{"get"}, "agent get ID"},
		{"get extra", []string{"get", "a", "b"}, "agent get ID"},
		{"open arity", []string{"open"}, "agent open ID"},
		{"message no id", []string{"message"}, "agent message ID"},
		{"cancel arity", []string{"cancel"}, "agent cancel ID"},
		{"wake arity", []string{"wake", "a", "b"}, "agent wake ID"},
		{"fork no id", []string{"fork"}, "agent fork ID"},
		{"watch arity", []string{"watch"}, "agent watch ID"},
		{"diff arity", []string{"diff"}, "agent diff ID"},
		{"approvals arity", []string{"approvals"}, "agent approvals ID"},
		{"ls extra", []string{"ls", "x"}, "agent ls"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := cmdAgent(ctx, tc.args)
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("args=%q error=%v want %q", tc.args, err, tc.want)
			}
		})
	}
}

func TestRunUsesAgentForACPRecipes(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("unavailable: recipe ACP launcher scripts require a POSIX shell")
	}
	t.Setenv("REMOUNT_AUTOSTART", "off")
	file, _, _ := fakeACPRecipe(t)
	// An ACP recipe run without a task fails in the agent planner, proving
	// the run command took the agent path; --pty takes the session path.
	err := cmdRun(context.Background(), []string{"fakeacp", "--recipe-file", file})
	if err == nil || !strings.Contains(err.Error(), "needs a task after --") {
		t.Fatalf("agent path: %v", err)
	}
	err = cmdRun(context.Background(), []string{"fakeacp", "--recipe-file", file, "--pty", "--max-turns", "1", "--", "x"})
	if err == nil || !strings.Contains(err.Error(), "--max-turns applies to agents") {
		t.Fatalf("pty path: %v", err)
	}
}

func waitFor(t *testing.T, ctx context.Context, pred func() bool) {
	t.Helper()
	for !pred() {
		select {
		case <-ctx.Done():
			t.Fatal("condition never held")
		case <-time.After(100 * time.Millisecond):
		}
	}
}

var _ = client.New

func textBlocks(text string) []acp.ContentBlock {
	raw, _ := json.Marshal(map[string]string{"type": "text", "text": text})
	var b acp.ContentBlock
	_ = json.Unmarshal(raw, &b)
	return []acp.ContentBlock{b}
}

func TestParseStartAt(t *testing.T) {
	now := time.Date(2026, 9, 3, 17, 0, 0, 0, time.UTC)
	cases := map[string]time.Time{
		"90m":                  now.Add(90 * time.Minute),
		"09:00":                time.Date(2026, 9, 4, 9, 0, 0, 0, time.UTC),
		"18:30":                time.Date(2026, 9, 3, 18, 30, 0, 0, time.UTC),
		"2026-09-05T08:00:00Z": time.Date(2026, 9, 5, 8, 0, 0, 0, time.UTC),
	}
	for in, want := range cases {
		got, err := parseStartAt(in, now)
		if err != nil || !got.Equal(want) {
			t.Errorf("parseStartAt(%q) = %v, %v; want %v", in, got, err, want)
		}
	}
	for _, in := range []string{"", "yesterday", "-5m", "0s", "2026-09-01T00:00:00Z", "25:00"} {
		if _, err := parseStartAt(in, now); err == nil {
			t.Errorf("parseStartAt(%q) accepted", in)
		}
	}
}
