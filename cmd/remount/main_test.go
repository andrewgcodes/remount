package main

import (
	"bytes"
	"context"
	"errors"
	"flag"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"remount.dev/remount/internal/client"
	"remount.dev/remount/internal/proto"
)

func TestEgressRuleFlagParsesInlineAndFileStrictly(t *testing.T) {
	var rules egressRuleFlag
	inline := `{"id":"api-read","protocol":"HTTPS","hosts":["API.EXAMPLE:443"],"methods":["get"],"path_prefixes":["/v1"]}`
	if err := rules.Set(inline); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "rule.json")
	if err := os.WriteFile(path, []byte(`{"id":"packages","connector":"package","protocol":"https","hosts":["registry.example"]}`), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := rules.Set("@" + path); err != nil {
		t.Fatal(err)
	}
	if len(rules) != 2 || rules[0].ID != "api-read" || rules[1].ID != "packages" {
		t.Fatalf("rules=%#v", rules)
	}
	security, err := proto.NormalizeSecurity(proto.SecuritySpec{
		Network: proto.NetworkPolicy{Rules: rules},
	})
	if err != nil {
		t.Fatal(err)
	}
	if security.Network.Default != proto.NetworkDefaultDeny ||
		!reflect.DeepEqual(security.Network.Rules[0].Methods, []string{"GET"}) ||
		security.Network.Rules[1].Connector != proto.EgressConnectorPackage ||
		security.Network.Rules[1].SharedState != proto.SharedStateImmutableRead ||
		!reflect.DeepEqual(security.Network.Rules[1].Methods, []string{"GET", "HEAD"}) {
		t.Fatalf("normalized=%#v", security)
	}
	if err := rules.Set(`{"id":"bad","protocol":"https","hosts":["example.com"],"typo":true}`); err == nil {
		t.Fatal("unknown rule field was silently accepted")
	}
	if len(rules) != 2 {
		t.Fatalf("failed parse mutated rules: %#v", rules)
	}
}

func TestParseAllowsFlagsAfterPositionals(t *testing.T) {
	fs := flag.NewFlagSet("test", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	value := fs.String("value", "", "")
	parse(fs, []string{"workspace", "--value", "set"})
	if *value != "set" || fs.NArg() != 1 || fs.Arg(0) != "workspace" {
		t.Fatalf("value=%q args=%v", *value, fs.Args())
	}
}

func TestAbsFlagPathResolvesRelativeAndNamesFlag(t *testing.T) {
	dir := t.TempDir()
	wd, _ := os.Getwd()
	if err := os.Chdir(dir); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chdir(wd) })
	got, err := absFlagPath("data", "./remount-data")
	if err != nil {
		t.Fatal(err)
	}
	want, _ := filepath.EvalSymlinks(dir)
	gotReal, _ := filepath.EvalSymlinks(filepath.Dir(got))
	if !filepath.IsAbs(got) || gotReal != want || filepath.Base(got) != "remount-data" {
		t.Fatalf("got %q want under %q", got, want)
	}
	if _, err := absFlagPath("data", "  "); err == nil || !strings.Contains(err.Error(), "--data") {
		t.Fatalf("empty path error must name the flag: %v", err)
	}
	if _, err := buildNode("relative/node", common{}, nil, "process", "", nil, nil, nodeResourceOptions{}); err == nil || !strings.Contains(err.Error(), "absolute") {
		t.Fatalf("buildNode accepted a relative root: %v", err)
	}
}

func TestArityRejectsExtraPositionalsAndSuggestsImage(t *testing.T) {
	ctx := context.Background()
	err := cmdWS(ctx, []string{"create", "python:3.12", "--wait=false"})
	if err == nil || !strings.Contains(err.Error(), "--image python:3.12") {
		t.Fatalf("image-like positional: %v", err)
	}
	err = cmdWS(ctx, []string{"create", "extra"})
	if err == nil || !strings.Contains(err.Error(), `unexpected argument "extra"`) {
		t.Fatalf("plain extra positional: %v", err)
	}
	err = cmdWS(ctx, []string{"get", "ws_1", "ws_2"})
	if err == nil || !strings.Contains(err.Error(), `unexpected argument "ws_2"`) || strings.Contains(err.Error(), "--image") {
		t.Fatalf("ws get arity: %v", err)
	}
	err = cmdWS(ctx, []string{"destroy"})
	if err == nil || !strings.Contains(err.Error(), "missing argument: ws destroy WS") {
		t.Fatalf("ws destroy arity: %v", err)
	}
	err = cmdFS(ctx, []string{"mv", "ws_1", "a"})
	if err == nil || !strings.Contains(err.Error(), "fs mv WS FROM TO") {
		t.Fatalf("fs mv arity: %v", err)
	}
	err = cmdFS(ctx, []string{"ls", "ws_1", "/", "extra"})
	if err == nil || !strings.Contains(err.Error(), `unexpected argument "extra"`) {
		t.Fatalf("fs ls arity: %v", err)
	}
	err = cmdAttach(ctx, []string{"ws_1"})
	if err == nil || !strings.Contains(err.Error(), "attach WS SESSION") {
		t.Fatalf("attach arity: %v", err)
	}
	err = cmdPort(ctx, []string{"ws_1", "70000"})
	if err == nil || !strings.Contains(err.Error(), "1-65535") {
		t.Fatalf("port range: %v", err)
	}
	err = cmdFleet(ctx, []string{"get"})
	if err == nil || !strings.Contains(err.Error(), "fleet get OPERATION") {
		t.Fatalf("fleet get arity: %v", err)
	}
	if !looksLikeImage("ghcr.io/org/img") || !looksLikeImage("node:22") || looksLikeImage("ws_abc") || looksLikeImage("plain") {
		t.Fatal("looksLikeImage heuristics")
	}
}

func TestGlobalFlagsAcceptedBeforeCommand(t *testing.T) {
	globals, rest := splitGlobalFlags([]string{"--json", "--server", "http://x:1", "--token=t", "ws", "get", "ws_1"})
	if !reflect.DeepEqual(globals, []string{"--json", "--server", "http://x:1", "--token=t"}) ||
		!reflect.DeepEqual(rest, []string{"ws", "get", "ws_1"}) {
		t.Fatalf("globals=%v rest=%v", globals, rest)
	}
	globals, rest = splitGlobalFlags([]string{"--follow", "events"})
	if len(globals) != 0 || len(rest) != 2 {
		t.Fatalf("non-global flag consumed: %v %v", globals, rest)
	}
	// Globals ahead of a nested subcommand still reach its FlagSet: arity
	// runs after parse, so the positional count proves --json was not
	// mistaken for a positional.
	err := run(context.Background(), []string{"--json", "ws", "get", "ws_1", "ws_2"})
	if err == nil || !strings.Contains(err.Error(), `unexpected argument "ws_2"`) {
		t.Fatalf("run with leading global: %v", err)
	}
	err = run(context.Background(), []string{"--json", "server"})
	if err == nil || !strings.Contains(err.Error(), "does not accept --json") {
		t.Fatalf("server should refuse client globals: %v", err)
	}
}

// fakeSession stands in for client.Session so drive's interrupt handling can
// be exercised without a node.
type fakeSession struct {
	chunks  chan client.Chunk
	mu      sync.Mutex
	signals []string
	closed  *bool // kill flag of the Close call, nil if never closed
	exit    *proto.ExitInfo
}

func newFakeSession() *fakeSession { return &fakeSession{chunks: make(chan client.Chunk, 16)} }

func (f *fakeSession) Chunks() <-chan client.Chunk { return f.chunks }
func (f *fakeSession) Exit() *proto.ExitInfo {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.exit
}
func (f *fakeSession) Input(context.Context, []byte, bool) error { return nil }
func (f *fakeSession) Resize(context.Context, uint16, uint16) error {
	return nil
}
func (f *fakeSession) Signal(_ context.Context, sig string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.signals = append(f.signals, sig)
	return nil
}
func (f *fakeSession) Close(_ context.Context, kill bool) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.closed = &kill
	return nil
}
func (f *fakeSession) finish(code int) {
	f.mu.Lock()
	f.exit = &proto.ExitInfo{Code: code}
	f.mu.Unlock()
	close(f.chunks)
}

func TestDriveInterruptDetachesByDefault(t *testing.T) {
	s := newFakeSession()
	s.chunks <- client.Chunk{Stream: proto.StreamStdout, Data: []byte("building...\n")}
	interrupts := make(chan os.Signal, 1)
	var stdout, stderr bytes.Buffer
	interrupts <- os.Interrupt
	err := drive(context.Background(), s, driveOptions{
		WS: "ws_1", Session: "s_1", stdin: strings.NewReader(""), stdout: &stdout, stderr: &stderr, interrupts: interrupts,
	})
	if err != nil {
		t.Fatalf("detach should not be an error: %v", err)
	}
	if s.closed == nil || *s.closed {
		t.Fatalf("expected Close(kill=false), got %v", s.closed)
	}
	if len(s.signals) != 0 {
		t.Fatalf("no signal should reach the remote process: %v", s.signals)
	}
	if !strings.Contains(stderr.String(), "reattach with: remount attach ws_1 s_1") {
		t.Fatalf("stderr=%q", stderr.String())
	}
	if !strings.Contains(stdout.String(), "building...") {
		t.Fatalf("output before interrupt lost: %q", stdout.String())
	}
}

func TestDriveKillOnInterruptForwardsSIGINTThenDetaches(t *testing.T) {
	s := newFakeSession()
	interrupts := make(chan os.Signal, 2)
	var stderr bytes.Buffer
	done := make(chan error, 1)
	go func() {
		done <- drive(context.Background(), s, driveOptions{
			WS: "ws_1", Session: "s_1", KillOnInterrupt: true,
			stdin: strings.NewReader(""), stdout: io.Discard, stderr: &stderr, interrupts: interrupts,
		})
	}()
	interrupts <- os.Interrupt
	deadline := time.Now().Add(5 * time.Second)
	for {
		s.mu.Lock()
		n := len(s.signals)
		s.mu.Unlock()
		if n == 1 || time.Now().After(deadline) {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	if s.signals[0] != "INT" {
		t.Fatalf("signals=%v", s.signals)
	}
	// The process honours SIGINT; the CLI reports its exit code.
	s.finish(130)
	err := <-done
	var ee exitError
	if !errors.As(err, &ee) || int(ee) != 130 {
		t.Fatalf("expected exit 130, got %v", err)
	}
	if s.closed != nil {
		t.Fatal("a process that exits must not also be detached")
	}

	// A process that ignores SIGINT must not trap the operator: the second
	// Ctrl-C detaches.
	s2 := newFakeSession()
	interrupts2 := make(chan os.Signal, 2)
	interrupts2 <- os.Interrupt
	interrupts2 <- os.Interrupt
	err = drive(context.Background(), s2, driveOptions{
		WS: "ws_1", Session: "s_2", KillOnInterrupt: true,
		stdin: strings.NewReader(""), stdout: io.Discard, stderr: io.Discard, interrupts: interrupts2,
	})
	if err != nil || len(s2.signals) != 1 || s2.closed == nil || *s2.closed {
		t.Fatalf("err=%v signals=%v closed=%v", err, s2.signals, s2.closed)
	}
}

func TestExecTimeoutIsNotClamped(t *testing.T) {
	fs := flag.NewFlagSet("exec", flag.ContinueOnError)
	timeout := fs.Duration("timeout", 0, "")
	parse(fs, []string{"ws_1", "--timeout", "6h", "--", "sleep", "1"})
	if int64(timeout.Seconds()) != 6*3600 {
		t.Fatalf("timeout=%v", *timeout)
	}
	if fs.NArg() != 4 || fs.Arg(1) != "--" {
		t.Fatalf("args=%v", fs.Args())
	}
}

func TestWorkspaceCreateRejectsClientSelectedPrincipalBeforeDial(t *testing.T) {
	err := cmdWS(context.Background(), []string{"create", "--principal", "a_attacker", "--wait=false"})
	if err == nil || !strings.Contains(err.Error(), "caller identity is authoritative") {
		t.Fatalf("error=%v", err)
	}
}

func TestEventsSessionFilterAndJSONShape(t *testing.T) {
	opened := proto.Event{Seq: 1, Type: proto.EvSOpened, Stream: "ws_1", Workspace: "ws_1", Generation: 2, Session: "s_1",
		Payload: proto.MustMarshal(map[string]any{"s": "s_1"})}
	exited := proto.Event{Seq: 2, Type: proto.EvSExited, Stream: "ws_1", Session: "s_2"}
	created := proto.Event{Seq: 3, Type: proto.EvWSCreated, Stream: "ws_1"}
	f := eventFilter{Session: "s_1"}
	if !f.match(opened) || f.match(exited) || f.match(created) {
		t.Fatal("--session must select only events attributed to that session")
	}
	if (eventFilter{}).match(created) != true {
		t.Fatal("no filter matches everything")
	}
	got := eventJSON(opened)
	if got["session"] != "s_1" || got["workspace"] != "ws_1" || got["generation"] != uint64(2) {
		t.Fatalf("json shape %+v", got)
	}
	if _, ok := eventJSON(created)["session"]; ok {
		t.Fatal("unattributed events must not carry a session key")
	}
}

func TestWorkspaceCreateRejectsDirWithRestoreFrom(t *testing.T) {
	err := cmdWS(context.Background(), []string{"create", "--dir", t.TempDir(), "--restore-from", "art_sha256:00", "--wait=false"})
	if err == nil || !strings.Contains(err.Error(), "mutually exclusive") {
		t.Fatalf("error=%v", err)
	}
}

func TestPushAndPullRequireExactlyOneWorkspace(t *testing.T) {
	if err := cmdPush(context.Background(), nil); err == nil {
		t.Fatal("push without WS accepted")
	}
	if err := cmdPull(context.Background(), []string{"ws_a", "ws_b"}); err == nil {
		t.Fatal("pull with two positionals accepted")
	}
}

func TestBaseSubcommandsValidateBeforeDialing(t *testing.T) {
	if err := cmdBase(context.Background(), nil); err == nil {
		t.Fatal("base without subcommand accepted")
	}
	if err := cmdBase(context.Background(), []string{"rm"}); err == nil {
		t.Fatal("base rm without NAME accepted")
	}
	if err := cmdBase(context.Background(), []string{"rm", "a", "b"}); err == nil {
		t.Fatal("base rm with two names accepted")
	}
	if err := cmdBase(context.Background(), []string{"mk", "x"}); err == nil || !strings.Contains(err.Error(), "unknown base subcommand") {
		t.Fatalf("error=%v", err)
	}
	err := cmdWS(context.Background(), []string{"create", "--base", "golden", "--restore-from", "art_sha256:00", "--wait=false"})
	if err == nil || !strings.Contains(err.Error(), "mutually exclusive") {
		t.Fatalf("error=%v", err)
	}
	err = cmdWS(context.Background(), []string{"snapshot", "ws_x", "--upload=false", "--as-base", "golden"})
	if err == nil || !strings.Contains(err.Error(), "as-base") {
		t.Fatalf("error=%v", err)
	}
}

func TestRunValidatesBeforeDialing(t *testing.T) {
	ctx := context.Background()
	cases := []struct {
		name string
		args []string
		want string
	}{
		{"no recipe", nil, "recipes:"},
		{"unknown recipe", []string{"nope", "--", "x"}, "nope"},
		{"custom needs argv", []string{"custom"}, "after --"},
		{"task required", []string{"opencode", "--binding", "b_openai"}, "needs a task"},
		{"bad sandbox", []string{"opencode", "--binding", "b_openai", "--sandbox", "loose", "--", "x"}, "--sandbox"},
		{"bad approve", []string{"opencode", "--binding", "b_openai", "--approve", "always", "--", "x"}, "--approve"},
		{"bad security", []string{"opencode", "--binding", "b_openai", "--security", "paranoid", "--", "x"}, "--security"},
		{"bad preset", []string{"opencode", "--binding", "b_x:nopreset", "--", "x"}, "nopreset"},
		{"foreign provider", []string{"claude", "--binding", "b_openai", "--", "x"}, "does not consume"},
		{"dir and ws", []string{"custom", "--dir", ".", "--ws", "ws_a", "--", "true"}, "mutually exclusive"},
		{"dir and base", []string{"custom", "--dir", ".", "--base", "b", "--", "true"}, "mutually exclusive"},
		{"repo unsupported", []string{"custom", "--repo", "https://x/y.git", "--", "true"}, "git connector"},
		{"negative timeout", []string{"custom", "--timeout", "-1s", "--", "true"}, "negative"},
		{"resident under isolated", []string{"claude", "--security", "isolated", "--", "x"}, "inside the workspace"},
		{"duplicate binding", []string{"opencode", "--binding", "b_openai", "--binding", "b_openai", "--", "x"}, "twice"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := cmdRun(ctx, tc.args)
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("args=%q error=%v want %q", tc.args, err, tc.want)
			}
		})
	}
}

func TestRunRecipeFileMustMatchAndParse(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	good := filepath.Join(dir, "mine.yaml")
	if err := os.WriteFile(good, []byte("name: mine\ncommand: [sh, -c, 'echo \"$TASK\"']\ncommand_from_args: false\nproviders: [openai]\nauth: api_key\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	err := cmdRun(ctx, []string{"other", "--recipe-file", good, "--", "x"})
	if err == nil || !strings.Contains(err.Error(), `declares recipe "mine"`) {
		t.Fatalf("error=%v", err)
	}
	bad := filepath.Join(dir, "bad.yaml")
	if err := os.WriteFile(bad, []byte("name: [\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := cmdRun(ctx, []string{"bad", "--recipe-file", bad, "--", "x"}); err == nil || !strings.Contains(err.Error(), "bad.yaml") {
		t.Fatalf("error=%v", err)
	}
	if err := cmdRun(ctx, []string{"x", "--recipe-file", filepath.Join(dir, "missing.yaml"), "--", "x"}); err == nil {
		t.Fatal("missing recipe file accepted")
	}
}

func TestBindingPresetLsIsTheOnlySubcommand(t *testing.T) {
	ctx := context.Background()
	for _, args := range [][]string{nil, {"preset"}, {"ls"}, {"preset", "rm"}} {
		if err := cmdBinding(ctx, args); err == nil {
			t.Fatalf("binding %q accepted", args)
		}
	}
	if err := cmdBinding(ctx, []string{"preset", "ls", "extra"}); err == nil {
		t.Fatal("extra positional accepted")
	}
	out := captureStdout(t, func() {
		if err := cmdBinding(ctx, []string{"preset", "ls", "--json"}); err != nil {
			t.Fatal(err)
		}
	})
	for _, want := range []string{`"name": "openai"`, `"name": "anthropic"`, `"hosts"`, `"key_env"`} {
		if !strings.Contains(out, want) {
			t.Fatalf("preset ls --json missing %s:\n%s", want, out)
		}
	}
}

// captureStdout runs f with os.Stdout redirected to a pipe and returns what
// it wrote. Commands print with printJSON and tabWriter, both of which write
// to os.Stdout directly.
func captureStdout(t *testing.T, f func()) string {
	t.Helper()
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	old := os.Stdout
	os.Stdout = w
	done := make(chan string)
	go func() {
		b, _ := io.ReadAll(r)
		done <- string(b)
	}()
	f()
	os.Stdout = old
	_ = w.Close()
	return <-done
}
