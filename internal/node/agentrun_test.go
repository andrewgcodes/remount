package node

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"remount.dev/remount/internal/session"
	"strings"
	"sync"
	"testing"
	"time"

	"remount.dev/remount/internal/acp"
	"remount.dev/remount/internal/acp/acptest"
	"remount.dev/remount/internal/proto"
	"remount.dev/remount/internal/workspace"
)

// The fake ACP agent runs as a child process: the test binary re-execs
// itself with fakeACPEnv set and serves ACP on its stdio. What it does per
// prompt is selected by fakeACPModeEnv so one binary covers every script.
const (
	fakeACPEnv     = "REMOUNT_TEST_FAKE_ACP"
	fakeACPModeEnv = "REMOUNT_TEST_FAKE_ACP_MODE"
)

func TestMain(m *testing.M) {
	if os.Getenv(fakeACPEnv) == "1" {
		acptest.Main(fakeACPConfig(os.Getenv(fakeACPModeEnv)))
		return
	}
	os.Exit(m.Run())
}

func fakeACPConfig(mode string) acptest.Config {
	cfg := acptest.Config{Info: acp.Implementation{Name: "fake", Version: "t"}}
	switch mode {
	case "permission":
		cfg.Turn = func(ctx context.Context, s *acptest.Session, req acp.PromptRequest) (acp.StopReason, error) {
			title := "rm -rf build"
			kind := acp.ToolKindDelete
			var res acp.RequestPermissionResponse
			err := s.Call(ctx, acp.MethodSessionRequestPermission, acp.RequestPermissionRequest{
				SessionID: s.ID,
				ToolCall:  acp.ToolCallUpdate{ToolCallID: "tc_1", Title: &title, Kind: &kind},
				Options: []acp.PermissionOption{
					{OptionID: "yes", Name: "Allow", Kind: acp.PermissionOptionKindAllowOnce},
					{OptionID: "no", Name: "Reject", Kind: acp.PermissionOptionKindRejectOnce},
				},
			}, &res)
			if err != nil {
				return "", err
			}
			if res.Outcome.Kind != "selected" {
				return acp.StopReasonEndTurn, s.Text("outcome:" + res.Outcome.Kind)
			}
			sel, err := res.Outcome.AsSelected()
			if err != nil {
				return "", err
			}
			return acp.StopReasonEndTurn, s.Text("selected:" + string(sel.OptionID))
		}
	case "terminal":
		cfg.Turn = func(ctx context.Context, s *acptest.Session, req acp.PromptRequest) (acp.StopReason, error) {
			var created acp.CreateTerminalResponse
			if err := s.Call(ctx, acp.MethodTerminalCreate, acp.CreateTerminalRequest{
				SessionID: s.ID, Command: "/bin/sh", Args: []string{"-c", "echo hello-from-terminal; echo REMOUNT_WORKSPACE=$REMOUNT_WORKSPACE; exit 3"},
			}, &created); err != nil {
				return "", err
			}
			var exit acp.WaitForTerminalExitResponse
			if err := s.Call(ctx, acp.MethodTerminalWaitForExit, acp.WaitForTerminalExitRequest{SessionID: s.ID, TerminalID: created.TerminalID}, &exit); err != nil {
				return "", err
			}
			var out acp.TerminalOutputResponse
			if err := s.Call(ctx, acp.MethodTerminalOutput, acp.TerminalOutputRequest{SessionID: s.ID, TerminalID: created.TerminalID}, &out); err != nil {
				return "", err
			}
			if err := s.Call(ctx, acp.MethodTerminalRelease, acp.ReleaseTerminalRequest{SessionID: s.ID, TerminalID: created.TerminalID}, &acp.ReleaseTerminalResponse{}); err != nil {
				return "", err
			}
			code := int64(-1)
			if exit.ExitCode != nil {
				code = *exit.ExitCode
			}
			return acp.StopReasonEndTurn, s.Text(fmt.Sprintf("exit=%d output=%q", code, out.Output))
		}
	case "fs":
		cfg.Turn = func(ctx context.Context, s *acptest.Session, req acp.PromptRequest) (acp.StopReason, error) {
			inside := filepath.Join(s.Cwd, "notes", "a.txt")
			if err := s.Call(ctx, acp.MethodFsWriteTextFile, acp.WriteTextFileRequest{SessionID: s.ID, Path: inside, Content: "one\ntwo\nthree\n"}, &acp.WriteTextFileResponse{}); err != nil {
				return "", err
			}
			line := int64(2)
			limit := int64(1)
			var read acp.ReadTextFileResponse
			if err := s.Call(ctx, acp.MethodFsReadTextFile, acp.ReadTextFileRequest{SessionID: s.ID, Path: inside, Line: &line, Limit: &limit}, &read); err != nil {
				return "", err
			}
			outside := "outside:ok"
			if err := s.Call(ctx, acp.MethodFsReadTextFile, acp.ReadTextFileRequest{SessionID: s.ID, Path: "/etc/hostname"}, &acp.ReadTextFileResponse{}); err != nil {
				outside = "outside:denied"
			}
			escape := "escape:ok"
			if err := s.Call(ctx, acp.MethodFsWriteTextFile, acp.WriteTextFileRequest{SessionID: s.ID, Path: filepath.Join(s.Cwd, "..", "escaped.txt"), Content: "x"}, &acp.WriteTextFileResponse{}); err != nil {
				escape = "escape:denied"
			}
			return acp.StopReasonEndTurn, s.Text("read=" + strings.TrimSpace(read.Content) + " " + outside + " " + escape)
		}
	case "hang":
		cfg.Turn = func(ctx context.Context, s *acptest.Session, req acp.PromptRequest) (acp.StopReason, error) {
			_ = s.Text("working")
			<-ctx.Done()
			return acp.StopReasonCancelled, nil
		}
	case "crash":
		cfg.Turn = func(ctx context.Context, s *acptest.Session, req acp.PromptRequest) (acp.StopReason, error) {
			_ = s.Text("about to die")
			os.Exit(7)
			return "", nil
		}
	case "trailer":
		// Ends the turn, then writes one more frame and exits at once: the
		// frame is in the pipe when the process is gone.
		cfg.Turn = func(ctx context.Context, s *acptest.Session, req acp.PromptRequest) (acp.StopReason, error) {
			go func() {
				time.Sleep(50 * time.Millisecond)
				_ = s.Text("trailing-frame-after-the-turn")
				os.Exit(0)
			}()
			return acp.StopReasonEndTurn, nil
		}
	case "load":
		cfg.Capabilities = acp.AgentCapabilities{LoadSession: true}
		cfg.Rewind = func(ctx context.Context, s *acptest.Session, req acp.LoadSessionRequest) error {
			return s.Text("replayed-history-for-" + string(req.SessionID))
		}
	case "resume":
		cfg.Capabilities = acp.AgentCapabilities{SessionCapabilities: &acp.SessionCapabilities{Resume: &acp.SessionResumeCapabilities{}}}
	case "leakvalues":
		cfg.Turn = func(ctx context.Context, s *acptest.Session, req acp.PromptRequest) (acp.StopReason, error) {
			// Values alone, no names: nothing for the shape patterns to key
			// on, so only a literal the redactor was told about is caught.
			var values []string
			for _, kv := range os.Environ() {
				if _, v, ok := strings.Cut(kv, "="); ok {
					values = append(values, v)
				}
			}
			return acp.StopReasonEndTurn, s.Text("values:" + strings.Join(values, " "))
		}
	case "leak":
		cfg.Turn = func(ctx context.Context, s *acptest.Session, req acp.PromptRequest) (acp.StopReason, error) {
			// A careless harness: echoes its whole environment and the env
			// file into the conversation.
			env, _ := os.ReadFile(filepath.Join(s.Cwd, ".remount", "env"))
			return acp.StopReasonEndTurn, s.Text("env:" + strings.Join(os.Environ(), " ") + " file:" + string(env))
		}
	}
	return cfg
}

// agentFixture is a node with one process-backend workspace and a report
// sink; runs start the test binary as the fake harness.
type agentFixture struct {
	n       *Node
	w       *ws
	reports chan *proto.AgentReport
	mu      sync.Mutex
	all     []*proto.AgentReport
}

func newAgentFixture(t *testing.T) *agentFixture {
	t.Helper()
	n := newTestNode(t, nil)
	f := &agentFixture{n: n, reports: make(chan *proto.AgentReport, 256)}
	n.agentReportSink = func(ctx context.Context, rep *proto.AgentReport) error {
		f.mu.Lock()
		f.all = append(f.all, rep)
		f.mu.Unlock()
		select {
		case f.reports <- rep:
		case <-ctx.Done():
			return ctx.Err()
		}
		return nil
	}
	backend, err := workspace.NewProcess(filepath.Join(t.TempDir(), "ws"))
	if err != nil {
		t.Fatal(err)
	}
	h, err := backend.Create(context.Background(), "ws_agent", proto.WorkspaceSpec{}, nil)
	if err != nil {
		t.Fatal(err)
	}
	w := &ws{
		Workspace: proto.Workspace{ID: "ws_agent", Generation: 3, State: proto.WSClaimed, Spec: proto.WorkspaceSpec{Env: map[string]string{
			"LEAK_TOKEN": "tok-canary-from-workspace-env-9f8e7d",
		}}},
		handle: h,
		leases: []proto.BindingLease{{ID: "b_x", Secret: "lease-secret-canary-ABCDEF"}},
	}
	// The env file a real materialize writes, with a capability token in the
	// broker URL the way a real one carries it.
	if err := h.FS().Mkdir(EnvFileDir); err != nil {
		t.Fatal(err)
	}
	if err := h.FS().Write(EnvFilePath, []byte("REMOUNT_WORKSPACE=ws_agent\nREMOUNT_BROKER=http://127.0.0.1:1/c/brokertokencanary0123456789/\n"), 0o644, false, true); err != nil {
		t.Fatal(err)
	}
	n.mu.Lock()
	n.workspaces[w.ID] = w
	n.deadlines[w.ID] = n.started.AddDate(1, 0, 0)
	n.mu.Unlock()
	f.w = w
	return f
}

func (f *agentFixture) start(t *testing.T, mode string, msgs ...string) proto.AgentRunReq {
	t.Helper()
	req := f.request(t, mode, msgs...)
	if _, err := f.n.agentRunStart(context.Background(), nil, &req); err != nil {
		t.Fatal(err)
	}
	return req
}

func (f *agentFixture) request(t *testing.T, mode string, msgs ...string) proto.AgentRunReq {
	t.Helper()
	exe, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	req := proto.AgentRunReq{
		Agent: "ag_1", Run: "run_" + mode, Attempt: 1, WS: f.w.ID, Gen: f.w.Generation, Tenant: "t", Owner: "u",
		Spec:   proto.AgentSpec{ACPCommand: []string{"/usr/bin/env", fakeACPEnv + "=1", fakeACPModeEnv + "=" + mode, exe}},
		Policy: proto.AgentPolicy{Approve: proto.ApproveOnRequest}, Mode: proto.AgentModeACP,
	}
	for i, m := range msgs {
		req.Messages = append(req.Messages, proto.AgentMessage{ID: fmt.Sprintf("m%d", i+1), Kind: proto.AgentMessageFollowUp, Text: m})
	}
	return req
}

func (f *agentFixture) next(t *testing.T, kind string) *proto.AgentReport {
	t.Helper()
	deadline := time.After(30 * time.Second)
	for {
		select {
		case rep := <-f.reports:
			if rep.Kind == kind {
				return rep
			}
			if rep.Kind == proto.AgentReportFinished {
				t.Fatalf("run finished (err=%q exit=%d cancelled=%v) before %s", rep.Error, rep.ExitCode, rep.Cancelled, kind)
			}
		case <-deadline:
			t.Fatalf("no %s report", kind)
		}
	}
}

func (f *agentFixture) waitDone(t *testing.T) {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	for !f.n.agentRunsIdle() {
		if time.Now().After(deadline) {
			t.Fatal("run did not leave the live set")
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// transcript returns every byte of both ACP streams of the run's transcript.
func (f *agentFixture) transcript(t *testing.T, id string) []byte {
	t.Helper()
	s, ok := f.n.sessions.Get(id)
	if !ok {
		t.Fatalf("transcript %s is gone", id)
	}
	chunks, err := s.Log.Read(0, 0)
	if err != nil {
		t.Fatal(err)
	}
	var b bytes.Buffer
	for _, c := range chunks {
		if c.Stream == proto.StreamACPIn || c.Stream == proto.StreamACPOut {
			b.Write(c.Data)
		}
	}
	return b.Bytes()
}

func TestAgentRunEchoTurnsAndCancel(t *testing.T) {
	f := newAgentFixture(t)
	req := f.start(t, "echo", "first prompt", "second prompt")
	started := f.next(t, proto.AgentReportStarted)
	if started.Seq != 1 || started.Transcript == "" || started.Gen != 3 {
		t.Fatalf("started = %+v", started)
	}
	sess := f.next(t, proto.AgentReportSession)
	if sess.ACPSessionID == "" || sess.Loaded {
		t.Fatalf("session = %+v", sess)
	}
	for _, id := range []string{"m1", "m2"} {
		ts := f.next(t, proto.AgentReportTurnStarted)
		if ts.Message != id {
			t.Fatalf("turn_started for %q, want %q", ts.Message, id)
		}
		tf := f.next(t, proto.AgentReportTurnFinished)
		if tf.Message != id || tf.StopReason != string(acp.StopReasonEndTurn) {
			t.Fatalf("turn_finished = %+v", tf)
		}
	}
	// A message delivered while idle starts a turn without a respawn.
	if err := f.n.agentRunDeliver(&proto.AgentDeliverReq{Agent: req.Agent, Run: req.Run, Message: proto.AgentMessage{ID: "m3", Kind: proto.AgentMessageFollowUp, Text: "third"}}); err != nil {
		t.Fatal(err)
	}
	if err := f.n.agentRunDeliver(&proto.AgentDeliverReq{Agent: req.Agent, Run: req.Run, Message: proto.AgentMessage{ID: "m3", Kind: proto.AgentMessageFollowUp, Text: "third"}}); err != nil {
		t.Fatalf("duplicate delivery = %v", err)
	}
	if f.next(t, proto.AgentReportTurnFinished).Message != "m3" {
		t.Fatal("m3 did not run")
	}
	// A replayed agent.run for the live run returns the same transcript.
	res, err := f.n.agentRunStart(context.Background(), nil, &req)
	if err != nil || res.(proto.AgentRunRes).Transcript != started.Transcript {
		t.Fatalf("replayed agent.run = %v, %v", res, err)
	}
	if err := f.n.agentRunCancel(&proto.AgentRunCancelReq{Agent: req.Agent, Run: req.Run, Reason: "test"}); err != nil {
		t.Fatal(err)
	}
	fin := f.next(t, proto.AgentReportFinished)
	if !fin.Cancelled || fin.Error != "" {
		t.Fatalf("finished = %+v", fin)
	}
	f.waitDone(t)
	// Once finished, a replayed agent.run is refused rather than restarted.
	if _, err := f.n.agentRunStart(context.Background(), nil, &req); err == nil {
		t.Fatal("agent.run for a finished run was accepted")
	}
	tr := f.transcript(t, started.Transcript)
	if !bytes.Contains(tr, []byte("first prompt")) || !bytes.Contains(tr, []byte("agent_message_chunk")) {
		t.Fatalf("transcript lacks the conversation: %s", tr)
	}
	s, _ := f.n.sessions.Get(started.Transcript)
	if exit := s.ExitInfo(); exit == nil || exit.Reason != "cancelled" {
		t.Fatalf("transcript exit = %+v", exit)
	}
	f.assertMirrored(t, s)
}

// assertMirrored checks the transcript reports the node sent reproduce the
// session log exactly and in order, within the report bound, and all before
// finished.
func (f *agentFixture) assertMirrored(t *testing.T, s *session.Session) {
	t.Helper()
	chunks, err := s.Log.Read(0, 0)
	if err != nil {
		t.Fatal(err)
	}
	want := map[uint64][]byte{}
	for _, c := range chunks {
		if c.Stream == proto.StreamACPIn || c.Stream == proto.StreamACPOut || c.Stream == proto.StreamStderr || c.Stream == proto.StreamExit {
			want[c.Seq] = c.Data
		}
	}
	if exit := s.ExitInfo(); exit == nil {
		t.Fatal("assertMirrored before the session exited")
	}
	f.mu.Lock()
	all := append([]*proto.AgentReport(nil), f.all...)
	f.mu.Unlock()
	var (
		lastSeq  uint64
		got      int
		finished bool
		lastRep  uint64
	)
	for _, rep := range all {
		if rep.Seq <= lastRep {
			t.Fatalf("report seq %d after %d", rep.Seq, lastRep)
		}
		lastRep = rep.Seq
		if rep.Kind == proto.AgentReportFinished {
			finished = true
			continue
		}
		if rep.Kind != proto.AgentReportTranscript {
			continue
		}
		if finished {
			t.Fatal("transcript report after finished")
		}
		size := 0
		for _, ch := range rep.Chunks {
			size += len(ch.Data)
			if ch.Seq <= lastSeq {
				t.Fatalf("chunk seq %d after %d", ch.Seq, lastSeq)
			}
			lastSeq = ch.Seq
			if !bytes.Equal(want[ch.Seq], ch.Data) {
				t.Fatalf("chunk seq %d differs from the session log", ch.Seq)
			}
			got++
		}
		if size > proto.MaxTranscriptReportBytes {
			t.Fatalf("transcript report of %d bytes exceeds the bound", size)
		}
	}
	if got != len(want) || got == 0 {
		t.Fatalf("mirrored %d of %d transcript records", got, len(want))
	}
}

func TestAgentRunPermissionParksApproval(t *testing.T) {
	f := newAgentFixture(t)
	req := f.start(t, "permission", "delete it")
	started := f.next(t, proto.AgentReportStarted)
	perm := f.next(t, proto.AgentReportPermission)
	if perm.Approval == nil || perm.Approval.Kind != proto.ApprovalToolCall || perm.Approval.ToolCall != "tc_1" || perm.ToolKind != "delete" || len(perm.Approval.Options) != 2 || !strings.HasPrefix(perm.Approval.ID, "ap_") {
		t.Fatalf("permission = %+v approval=%+v", perm, perm.Approval)
	}
	select {
	case rep := <-f.reports:
		t.Fatalf("run progressed while waiting for approval: %+v", rep)
	case <-time.After(200 * time.Millisecond):
	}
	if err := f.n.agentApprovalDecided(&proto.AgentApprovalDecidedReq{Agent: req.Agent, Run: req.Run, Approval: "ap_unknown", Decision: proto.ApprovalDecision{Option: "yes"}}); err == nil {
		t.Fatal("decision for an unknown approval was accepted")
	}
	if err := f.n.agentApprovalDecided(&proto.AgentApprovalDecidedReq{Agent: req.Agent, Run: req.Run, Approval: perm.Approval.ID, Decision: proto.ApprovalDecision{Option: "yes"}}); err != nil {
		t.Fatal(err)
	}
	tf := f.next(t, proto.AgentReportTurnFinished)
	if tf.Message != "m1" {
		t.Fatalf("turn_finished = %+v", tf)
	}
	if !bytes.Contains(f.transcript(t, started.Transcript), []byte("selected:yes")) {
		t.Fatal("harness did not see the selected option")
	}
	_ = f.n.agentRunCancel(&proto.AgentRunCancelReq{Agent: req.Agent, Run: req.Run})
	f.next(t, proto.AgentReportFinished)
	f.waitDone(t)
}

func TestAgentRunPolicyAutoAndNeverAnswerWithoutParking(t *testing.T) {
	for _, tc := range []struct{ policy, want string }{{proto.ApproveAuto, "selected:yes"}, {proto.ApproveNever, "selected:no"}} {
		t.Run(tc.policy, func(t *testing.T) {
			f := newAgentFixture(t)
			exe, _ := os.Executable()
			req := proto.AgentRunReq{
				Agent: "ag_1", Run: "run_" + tc.policy, Attempt: 1, WS: f.w.ID, Gen: f.w.Generation,
				Spec:     proto.AgentSpec{ACPCommand: []string{"/usr/bin/env", fakeACPEnv + "=1", fakeACPModeEnv + "=permission", exe}},
				Policy:   proto.AgentPolicy{Approve: tc.policy},
				Messages: []proto.AgentMessage{{ID: "m1", Kind: proto.AgentMessageFollowUp, Text: "go"}},
			}
			if _, err := f.n.agentRunStart(context.Background(), nil, &req); err != nil {
				t.Fatal(err)
			}
			started := f.next(t, proto.AgentReportStarted)
			f.next(t, proto.AgentReportTurnFinished)
			if !bytes.Contains(f.transcript(t, started.Transcript), []byte(tc.want)) {
				t.Fatalf("policy %s did not answer %s", tc.policy, tc.want)
			}
			_ = f.n.agentRunCancel(&proto.AgentRunCancelReq{Agent: req.Agent, Run: req.Run})
			f.next(t, proto.AgentReportFinished)
			f.waitDone(t)
		})
	}
}

func TestAgentRunTerminalThroughSessionManager(t *testing.T) {
	f := newAgentFixture(t)
	req := f.start(t, "terminal", "run it")
	started := f.next(t, proto.AgentReportStarted)
	f.next(t, proto.AgentReportTurnFinished)
	tr := f.transcript(t, started.Transcript)
	if !bytes.Contains(tr, []byte(`exit=3`)) || !bytes.Contains(tr, []byte("hello-from-terminal")) || !bytes.Contains(tr, []byte("REMOUNT_WORKSPACE=ws_agent")) {
		t.Fatalf("terminal round trip missing from transcript: %s", tr)
	}
	// The terminal was released: only the transcript remains for the ws.
	for _, s := range f.n.sessions.List(f.w.ID) {
		if s.ID != started.Transcript {
			t.Fatalf("terminal session %s leaked", s.ID)
		}
	}
	_ = f.n.agentRunCancel(&proto.AgentRunCancelReq{Agent: req.Agent, Run: req.Run})
	f.next(t, proto.AgentReportFinished)
	f.waitDone(t)
}

func TestAgentRunFilesystemStaysInJail(t *testing.T) {
	f := newAgentFixture(t)
	req := f.start(t, "fs", "edit")
	started := f.next(t, proto.AgentReportStarted)
	f.next(t, proto.AgentReportTurnFinished)
	tr := f.transcript(t, started.Transcript)
	if !bytes.Contains(tr, []byte("read=two outside:denied escape:denied")) {
		t.Fatalf("fs round trip: %s", tr)
	}
	if _, err := os.Stat(filepath.Join(mustHostFS(t, f.w.handle).Root(), "notes", "a.txt")); err != nil {
		t.Fatalf("file not written inside the jail: %v", err)
	}
	if _, err := os.Stat(filepath.Join(mustHostFS(t, f.w.handle).Root(), "..", "escaped.txt")); err == nil {
		t.Fatal("harness wrote outside the jail")
	}
	_ = f.n.agentRunCancel(&proto.AgentRunCancelReq{Agent: req.Agent, Run: req.Run})
	f.next(t, proto.AgentReportFinished)
	f.waitDone(t)
}

func TestAgentRunCancelMidTurnIsNotATurnFinish(t *testing.T) {
	f := newAgentFixture(t)
	req := f.start(t, "hang", "never ends")
	f.next(t, proto.AgentReportTurnStarted)
	time.Sleep(100 * time.Millisecond)
	if err := f.n.agentRunCancel(&proto.AgentRunCancelReq{Agent: req.Agent, Run: req.Run, Reason: "user"}); err != nil {
		t.Fatal(err)
	}
	for {
		rep := <-f.reports
		if rep.Kind == proto.AgentReportTurnFinished {
			t.Fatal("a cancelled turn was reported finished; the inbox message would be lost")
		}
		if rep.Kind == proto.AgentReportFinished {
			if !rep.Cancelled {
				t.Fatalf("finished = %+v", rep)
			}
			break
		}
	}
	f.waitDone(t)
}

func TestAgentRunHarnessCrashKeepsMessage(t *testing.T) {
	f := newAgentFixture(t)
	f.start(t, "crash", "die")
	f.next(t, proto.AgentReportTurnStarted)
	fin := f.next(t, proto.AgentReportFinished)
	if fin.Cancelled || fin.ExitCode != 7 || fin.Error == "" {
		t.Fatalf("finished after crash = %+v", fin)
	}
	f.waitDone(t)
	f.n.mu.Lock()
	_, done := f.n.agentRunsDone[agentRunKey("ag_1", "run_crash")]
	f.n.mu.Unlock()
	if !done {
		t.Fatal("crashed run not remembered as finished")
	}
}

func TestAgentRunTrailingFrameReachesTranscript(t *testing.T) {
	f := newAgentFixture(t)
	f.start(t, "trailer", "go")
	started := f.next(t, proto.AgentReportStarted)
	f.next(t, proto.AgentReportTurnFinished)
	fin := f.next(t, proto.AgentReportFinished)
	if fin.Cancelled || fin.ExitCode != 0 {
		t.Fatalf("finished = %+v", fin)
	}
	f.waitDone(t)
	if tr := f.transcript(t, started.Transcript); !bytes.Contains(tr, []byte("trailing-frame-after-the-turn")) {
		t.Fatalf("frame written just before exit is missing from the transcript: %s", tr)
	}
	s, ok := f.n.sessions.Get(started.Transcript)
	if !ok {
		t.Fatal("transcript gone")
	}
	f.assertMirrored(t, s)
}

func TestAgentRunTranscriptRedactsSecrets(t *testing.T) {
	f := newAgentFixture(t)
	// The prompt itself carries the lease secret (a user pasted it); the
	// harness then echoes its environment and the env file.
	req := f.start(t, "leak", "use lease-secret-canary-ABCDEF and sk-proj-abcdefghijklmnopqrstuvwxyz0123456789")
	started := f.next(t, proto.AgentReportStarted)
	f.next(t, proto.AgentReportTurnFinished)
	tr := f.transcript(t, started.Transcript)
	for _, canary := range []string{
		"lease-secret-canary-ABCDEF",           // lease secret, literal
		"tok-canary-from-workspace-env-9f8e7d", // workspace env var with a secret-shaped name
		"sk-proj-abcdefghijklmnopqrstuvwxyz01", // provider key shape
		"brokertokencanary0123456789",          // capability token in the broker URL
	} {
		if bytes.Contains(tr, []byte(canary)) {
			t.Fatalf("transcript leaks %q", canary)
		}
	}
	if !bytes.Contains(tr, []byte(redactedMark)) {
		t.Fatal("nothing was redacted; the canaries did not reach the transcript")
	}
	// The harness did see the broker (through the env file) and the
	// workspace env: redaction is on the record, not on the process.
	if !bytes.Contains(tr, []byte("REMOUNT_BROKER=")) || !bytes.Contains(tr, []byte("LEAK_TOKEN=")) {
		t.Fatalf("harness environment not visible in transcript: %s", tr)
	}
	_ = f.n.agentRunCancel(&proto.AgentRunCancelReq{Agent: req.Agent, Run: req.Run})
	f.next(t, proto.AgentReportFinished)
	f.waitDone(t)
}

func TestAgentRunReopensSessionAcrossRuns(t *testing.T) {
	for _, mode := range []string{"load", "resume"} {
		t.Run(mode, func(t *testing.T) {
			f := newAgentFixture(t)
			first := f.start(t, mode, "one")
			sess := f.next(t, proto.AgentReportSession)
			if sess.Loaded || sess.ACPSessionID == "" {
				t.Fatalf("first session = %+v", sess)
			}
			f.next(t, proto.AgentReportTurnFinished)
			_ = f.n.agentRunCancel(&proto.AgentRunCancelReq{Agent: first.Agent, Run: first.Run})
			f.next(t, proto.AgentReportFinished)
			f.waitDone(t)

			// The next run (a wake after sleep, a move) carries the harness
			// session id and continues it instead of starting over.
			second := f.request(t, mode, "two")
			second.Run = "run_second"
			second.ACPSessionID = sess.ACPSessionID
			if _, err := f.n.agentRunStart(context.Background(), nil, &second); err != nil {
				t.Fatal(err)
			}
			started := f.next(t, proto.AgentReportStarted)
			again := f.next(t, proto.AgentReportSession)
			if !again.Loaded || again.ACPSessionID != sess.ACPSessionID {
				t.Fatalf("second session = %+v", again)
			}
			if again.Capabilities.LoadSession != (mode == "load") || again.Capabilities.ResumeSession != (mode == "resume") {
				t.Fatalf("capabilities = %+v", again.Capabilities)
			}
			f.next(t, proto.AgentReportTurnFinished)
			tr := f.transcript(t, started.Transcript)
			replayed := bytes.Contains(tr, []byte("replayed-history-for-"))
			if replayed != (mode == "load") {
				t.Fatalf("mode %s: replayed history present=%v", mode, replayed)
			}
			if mode == "load" && !bytes.Contains(tr, []byte("replayed")) {
				t.Fatal("replayed frames are not marked")
			}
			_ = f.n.agentRunCancel(&proto.AgentRunCancelReq{Agent: second.Agent, Run: second.Run})
			f.next(t, proto.AgentReportFinished)
			f.waitDone(t)
		})
	}
}

func TestAgentRunRefusesWrongGenerationAndSecondRun(t *testing.T) {
	f := newAgentFixture(t)
	req := f.start(t, "echo")
	f.next(t, proto.AgentReportSession)
	stale := req
	stale.Run = "run_other"
	stale.Gen = 2
	if _, err := f.n.agentRunStart(context.Background(), nil, &stale); err == nil {
		t.Fatal("stale generation accepted")
	}
	second := req
	second.Run = "run_other"
	if _, err := f.n.agentRunStart(context.Background(), nil, &second); err == nil {
		t.Fatal("a second live run for the same agent was accepted")
	}
	// Killing the workspace's sessions hard-stops the run and joins it.
	f.n.stopWorkspaceSessions(f.w)
	fin := f.next(t, proto.AgentReportFinished)
	if !fin.Cancelled {
		t.Fatalf("finished after kill = %+v", fin)
	}
	f.waitDone(t)
}

// installRecipe is a recipe whose install script records each run in the
// workspace and whose ACP command is the fake harness the fixture uses.
func installRecipe(t *testing.T, mode, install string) string {
	t.Helper()
	exe, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	return fmt.Sprintf(`name: inst
auth: workspace_resident
command: ["true"]
install: |
  %s
acp:
  command: ["/usr/bin/env", "%s=1", "%s=%s", %q]
`, install, fakeACPEnv, fakeACPModeEnv, mode, exe)
}

func TestAgentRunInstallsRecipeOncePerGeneration(t *testing.T) {
	f := newAgentFixture(t)
	req := f.request(t, "echo", "hello")
	req.Spec = proto.AgentSpec{Recipe: "inst", RecipeYAML: installRecipe(t, "echo", `echo "installed key=${OPENAI_API_KEY:-none}" >&2; echo run >> installs.txt`)}
	if _, err := f.n.agentRunStart(context.Background(), nil, &req); err != nil {
		t.Fatal(err)
	}
	started := f.next(t, proto.AgentReportStarted)
	f.next(t, proto.AgentReportTurnFinished)
	if err := f.n.agentRunCancel(&proto.AgentRunCancelReq{Agent: req.Agent, Run: req.Run, Reason: "test"}); err != nil {
		t.Fatal(err)
	}
	f.next(t, proto.AgentReportFinished)
	f.waitDone(t)
	res, err := f.w.handle.FS().Read("installs.txt", 0, 0)
	if err != nil || string(res.Data) != "run\n" {
		t.Fatalf("installs.txt = %+v, %v", res, err)
	}
	// Install output reaches the transcript, on the stderr stream.
	s, _ := f.n.sessions.Get(started.Transcript)
	chunks, err := s.Log.Read(0, 0)
	if err != nil {
		t.Fatal(err)
	}
	var stderr []byte
	for _, c := range chunks {
		if c.Stream == proto.StreamStderr {
			stderr = append(stderr, c.Data...)
		}
	}
	if !bytes.Contains(stderr, []byte("installed key=")) {
		t.Fatalf("install output missing from transcript: %q", stderr)
	}

	// Same generation: a second run does not install again.
	req.Run = "run_second"
	if _, err := f.n.agentRunStart(context.Background(), nil, &req); err != nil {
		t.Fatal(err)
	}
	f.next(t, proto.AgentReportTurnFinished)
	_ = f.n.agentRunCancel(&proto.AgentRunCancelReq{Agent: req.Agent, Run: req.Run, Reason: "test"})
	f.next(t, proto.AgentReportFinished)
	f.waitDone(t)
	if res, err := f.w.handle.FS().Read("installs.txt", 0, 0); err != nil || string(res.Data) != "run\n" {
		t.Fatalf("second run reinstalled: %+v, %v", res, err)
	}

	// A new generation (the workspace moved) installs again.
	f.n.mu.Lock()
	f.w.Generation++
	f.n.mu.Unlock()
	req.Run, req.Gen = "run_third", f.w.Generation
	if _, err := f.n.agentRunStart(context.Background(), nil, &req); err != nil {
		t.Fatal(err)
	}
	f.next(t, proto.AgentReportTurnFinished)
	_ = f.n.agentRunCancel(&proto.AgentRunCancelReq{Agent: req.Agent, Run: req.Run, Reason: "test"})
	f.next(t, proto.AgentReportFinished)
	f.waitDone(t)
	if res, err := f.w.handle.FS().Read("installs.txt", 0, 0); err != nil || string(res.Data) != "run\nrun\n" {
		t.Fatalf("new generation did not reinstall: %+v, %v", res, err)
	}
}

func TestAgentRunInstallFailureFailsTheRun(t *testing.T) {
	f := newAgentFixture(t)
	req := f.request(t, "echo", "hello")
	req.Spec = proto.AgentSpec{Recipe: "inst", RecipeYAML: installRecipe(t, "echo", `echo "no network" >&2; exit 3`)}
	if _, err := f.n.agentRunStart(context.Background(), nil, &req); err != nil {
		t.Fatal(err)
	}
	fin := f.next(t, proto.AgentReportFinished)
	if fin.Cancelled || fin.ExitCode != -1 || !strings.Contains(fin.Error, "install inst exited 3") {
		t.Fatalf("finished = %+v", fin)
	}
	f.waitDone(t)
	if _, err := f.w.handle.FS().Read(".remount/launch/inst.installed", 0, 0); err == nil {
		t.Fatal("a failed install left a marker")
	}
}

// envRewritingHandle behaves like the docker backend's Prepare: the
// workspace's variables move out of spec.Env into the command line, and
// spec.Env becomes the host environment the container CLI itself needs.
type envRewritingHandle struct {
	workspace.Handle
}

func (h envRewritingHandle) Prepare(spec *session.Spec) error {
	if err := h.Handle.Prepare(spec); err != nil {
		return err
	}
	spec.Program = append(append([]string{"/usr/bin/env"}, spec.Env...), spec.Program...)
	spec.Env = []string{"PATH=" + os.Getenv("PATH"), "HOME=" + os.Getenv("HOME")}
	return nil
}

// The redactor learns the workspace's secrets from the environment the
// workspace declared, not from whatever the backend hands the host after it
// rewrote the spec. With the docker shape of Prepare the old order saw only
// the host environment and let a workspace token through.
func TestAgentRunRedactsWorkspaceEnvWhenBackendRewritesSpec(t *testing.T) {
	f := newAgentFixture(t)
	f.n.mu.Lock()
	f.w.handle = envRewritingHandle{f.w.handle}
	f.n.mu.Unlock()
	req := f.start(t, "leakvalues", "echo the environment")
	started := f.next(t, proto.AgentReportStarted)
	f.next(t, proto.AgentReportTurnFinished)
	tr := f.transcript(t, started.Transcript)
	if !bytes.Contains(tr, []byte("values:")) || !bytes.Contains(tr, []byte(redactedMark)) {
		t.Fatalf("the harness did not echo its environment, or nothing was redacted: %s", tr)
	}
	if bytes.Contains(tr, []byte("tok-canary-from-workspace-env-9f8e7d")) {
		t.Fatalf("transcript leaks the workspace token after a backend env rewrite: %s", tr)
	}
	_ = f.n.agentRunCancel(&proto.AgentRunCancelReq{Agent: req.Agent, Run: req.Run})
	f.next(t, proto.AgentReportFinished)
	f.waitDone(t)
}

// A report the control plane refuses as not authoritative (the run row is
// closed, the agent is gone, the workspace moved) stops the harness: it
// would otherwise keep working, with brokered credentials, on a run nobody
// accounts for. A finished report refused the same way is simply dropped.
func TestAgentRunStopsWhenControlPlaneDisownsIt(t *testing.T) {
	f := newAgentFixture(t)
	inner := f.n.agentReportSink
	f.n.agentReportSink = func(ctx context.Context, rep *proto.AgentReport) error {
		if rep.Kind == proto.AgentReportTurnStarted {
			return proto.Err(proto.CodeConflict, "run is finished; report is not authoritative")
		}
		return inner(ctx, rep)
	}
	req := f.start(t, "hang", "work forever")
	fin := f.next(t, proto.AgentReportFinished)
	if !fin.Cancelled {
		t.Fatalf("finished after disown = %+v", fin)
	}
	f.waitDone(t)
	f.n.mu.Lock()
	_, done := f.n.agentRunsDone[agentRunKey(req.Agent, req.Run)]
	f.n.mu.Unlock()
	if !done {
		t.Fatal("disowned run not remembered as finished")
	}
}

// A newer run for an agent whose older run is still live here is refused,
// and the older run is cancelled: the control plane closed it without the
// node hearing, so the retry of the launch finds the harness gone.
func TestAgentRunSupersedesOlderLiveRun(t *testing.T) {
	f := newAgentFixture(t)
	req := f.start(t, "hang", "work forever")
	f.next(t, proto.AgentReportTurnStarted)
	newer := req
	newer.Run = "run_newer"
	newer.Attempt = 2
	_, err := f.n.agentRunStart(context.Background(), nil, &newer)
	var pe *proto.Error
	if !errorsAs(err, &pe) || pe.Code != proto.CodeConflict {
		t.Fatalf("newer run while older is live: err = %v, want conflict", err)
	}
	fin := f.next(t, proto.AgentReportFinished)
	if fin.Run != req.Run || !fin.Cancelled {
		t.Fatalf("older run after supersede = %+v", fin)
	}
	f.waitDone(t)
	if _, err := f.n.agentRunStart(context.Background(), nil, &newer); err != nil {
		t.Fatalf("retry of the newer run after the older stopped: %v", err)
	}
	if got := f.next(t, proto.AgentReportStarted); got.Run != newer.Run {
		t.Fatalf("started %s, want %s", got.Run, newer.Run)
	}
	_ = f.n.agentRunCancel(&proto.AgentRunCancelReq{Agent: newer.Agent, Run: newer.Run})
	f.next(t, proto.AgentReportFinished)
	f.waitDone(t)
}

func errorsAs(err error, target **proto.Error) bool {
	for err != nil {
		if pe, ok := err.(*proto.Error); ok {
			*target = pe
			return true
		}
		u, ok := err.(interface{ Unwrap() error })
		if !ok {
			return false
		}
		err = u.Unwrap()
	}
	return false
}

// The per-run report queue is bounded. Past the bound the oldest transcript
// report behind the in-flight head is dropped; lifecycle reports and the
// head never are. The loss is reported as a gap chunk placed where the
// dropped records were, so the mirror stays in seq order and shows the hole.
func TestAgentReporterBoundsQueueAndPlacesGapInOrder(t *testing.T) {
	r := &agentReporter{run: &agentRun{req: proto.AgentRunReq{Agent: "ag", Run: "run", WS: "ws", Gen: 1}}, kick: make(chan struct{}, 1)}
	transcript := func(seq uint64) *proto.AgentReport {
		return &proto.AgentReport{Kind: proto.AgentReportTranscript, Chunks: []proto.TranscriptChunk{{Seq: seq, Stream: proto.StreamACPOut, Data: []byte("x")}}}
	}
	r.mu.Lock()
	r.enqueueLocked(&proto.AgentReport{Kind: proto.AgentReportStarted}) // head, in flight
	for seq := uint64(1); seq <= uint64(agentReportQueueMax)+9; seq++ {
		if seq == 5 {
			r.enqueueLocked(&proto.AgentReport{Kind: proto.AgentReportTurnStarted})
		}
		r.enqueueLocked(transcript(seq))
	}
	q := append([]*proto.AgentReport(nil), r.q...)
	pending := r.gap
	r.mu.Unlock()
	if len(q) != agentReportQueueMax {
		t.Fatalf("queue length = %d, want %d", len(q), agentReportQueueMax)
	}
	if q[0].Kind != proto.AgentReportStarted || q[1].Kind != proto.AgentReportTurnStarted {
		t.Fatalf("head or lifecycle report dropped: %s, %s", q[0].Kind, q[1].Kind)
	}
	if pending != nil {
		t.Fatalf("gap pending for later flush = %+v, want it placed in the queue", pending)
	}
	// Two lifecycle reports plus max+9 transcript reports overflow by 11:
	// seqs 1..11 were dropped and the first surviving transcript report
	// opens with one gap covering exactly them.
	first := q[2]
	if first.Kind != proto.AgentReportTranscript || len(first.Chunks) != 2 || first.Chunks[0].Stream != proto.StreamGap {
		t.Fatalf("first surviving transcript report = %+v", first)
	}
	var g proto.Gap
	if err := proto.Unmarshal(first.Chunks[0].Data, &g); err != nil {
		t.Fatal(err)
	}
	if g.From != 1 || g.To != 11 || first.Chunks[1].Seq != 12 {
		t.Fatalf("gap = %+v before seq %d, want 1..11 before 12", g, first.Chunks[1].Seq)
	}
	for i := 3; i < len(q); i++ {
		for _, ch := range q[i].Chunks {
			if ch.Stream == proto.StreamGap {
				t.Fatalf("stray gap chunk in report %d", i)
			}
		}
	}
	// Bytes: a big transcript report tips the byte bound and the victim is
	// the newest transcript report, so the gap waits for the next flush.
	r2 := &agentReporter{run: r.run, kick: make(chan struct{}, 1)}
	r2.mu.Lock()
	r2.enqueueLocked(&proto.AgentReport{Kind: proto.AgentReportStarted})
	big := &proto.AgentReport{Kind: proto.AgentReportTranscript, Chunks: []proto.TranscriptChunk{{Seq: 7, Stream: proto.StreamACPOut, Data: make([]byte, agentReportQueueBytes)}}}
	r2.enqueueLocked(big)
	if len(r2.q) != 1 || r2.gap == nil || r2.gap.From != 7 || r2.gap.To != 7 {
		r2.mu.Unlock()
		t.Fatalf("after oversize report: len=%d gap=%+v", len(r2.q), r2.gap)
	}
	r2.chunks = append(r2.chunks, proto.TranscriptChunk{Seq: 8, Stream: proto.StreamACPOut, Data: []byte("y")})
	r2.flushChunksLocked()
	next := r2.q[len(r2.q)-1]
	r2.mu.Unlock()
	if len(next.Chunks) != 2 || next.Chunks[0].Stream != proto.StreamGap || next.Chunks[0].Seq != 7 || next.Chunks[1].Seq != 8 {
		t.Fatalf("flushed report after a pending gap = %+v", next.Chunks)
	}
	// A pending gap with no further transcript records still ships, ahead of
	// the finished report, so a run that ends right after the trim does not
	// take the hole to its grave.
	r3 := &agentReporter{run: r.run, kick: make(chan struct{}, 1)}
	r3.mu.Lock()
	r3.enqueueLocked(&proto.AgentReport{Kind: proto.AgentReportStarted})
	r3.enqueueLocked(&proto.AgentReport{Kind: proto.AgentReportTranscript, Chunks: []proto.TranscriptChunk{{Seq: 3, Stream: proto.StreamACPOut, Data: make([]byte, agentReportQueueBytes)}}})
	r3.mu.Unlock()
	r3.report(proto.AgentReportFinished, nil)
	r3.mu.Lock()
	q3 := append([]*proto.AgentReport(nil), r3.q...)
	pending3 := r3.gap
	r3.mu.Unlock()
	if pending3 != nil || len(q3) != 3 || q3[1].Kind != proto.AgentReportTranscript || q3[2].Kind != proto.AgentReportFinished {
		kinds := make([]string, len(q3))
		for i, rep := range q3 {
			kinds[i] = rep.Kind
		}
		t.Fatalf("queue at finish = %v, pending gap = %+v; want started, transcript(gap), finished", kinds, pending3)
	}
	if len(q3[1].Chunks) != 1 || q3[1].Chunks[0].Stream != proto.StreamGap || q3[1].Chunks[0].Seq != 3 {
		t.Fatalf("gap-only transcript report = %+v", q3[1].Chunks)
	}
}

// A soft agent.run.cancel that lands while the recipe is still installing
// ends the run then, not when the install script gives up. The install has
// no ACP turn to cancel, so it is the run's own cancel that has to reach it.
func TestAgentRunCancelInterruptsInstall(t *testing.T) {
	f := newAgentFixture(t)
	req := f.request(t, "echo", "hello")
	req.Spec = proto.AgentSpec{Recipe: "inst", RecipeYAML: installRecipe(t, "echo", `echo installing >&2; sleep 60`)}
	if _, err := f.n.agentRunStart(context.Background(), nil, &req); err != nil {
		t.Fatal(err)
	}
	f.next(t, proto.AgentReportStarted)
	time.Sleep(200 * time.Millisecond)
	begin := time.Now()
	if err := f.n.agentRunCancel(&proto.AgentRunCancelReq{Agent: req.Agent, Run: req.Run, Reason: "test"}); err != nil {
		t.Fatal(err)
	}
	fin := f.next(t, proto.AgentReportFinished)
	if took := time.Since(begin); took > 10*time.Second {
		t.Fatalf("cancel waited on the install script for %s", took)
	}
	if !fin.Cancelled {
		t.Fatalf("finished after cancel during install = %+v", fin)
	}
	f.waitDone(t)
	if _, err := f.w.handle.FS().Read(".remount/launch/inst.installed", 0, 0); err == nil {
		t.Fatal("a cancelled install left a marker")
	}
}

// harnessExit is bounded: a harness that closed its stdout and lingers, or
// whose children hold the group open, is killed and then abandoned rather
// than pinning the run slot for as long as the tree lives.
func TestHarnessExitIsBounded(t *testing.T) {
	if !posixProcessGroups {
		t.Skip("unavailable: this asserts a POSIX process-group property and uses /bin/sh")
	}
	cmd := exec.Command("/bin/sh", "-c", "exec >/dev/null 2>&1; trap '' TERM; sleep 60 & wait")
	session.ConfigureProcessGroup(cmd)
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	begin := time.Now()
	code := harnessExit(cmd, 300*time.Millisecond)
	if took := time.Since(begin); took > 5*time.Second {
		t.Fatalf("harnessExit blocked for %s", took)
	}
	if code == 0 {
		t.Fatalf("exit code = %d for a harness that had to be killed", code)
	}
	if cmd.ProcessState == nil {
		t.Fatal("process not reaped after the bound")
	}
	// The orphaned child is a zombie until init reaps it; give that a moment.
	gone := false
	for i := 0; i < 200 && !gone; i++ {
		gone = !processGroupAlive(cmd.Process.Pid)
		if !gone {
			time.Sleep(10 * time.Millisecond)
		}
	}
	if !gone {
		t.Fatal("the harness process group is still alive after harnessExit")
	}

	quick := exec.Command("/bin/sh", "-c", "exit 7")
	session.ConfigureProcessGroup(quick)
	if err := quick.Start(); err != nil {
		t.Fatal(err)
	}
	if code := harnessExit(quick, 5*time.Second); code != 7 {
		t.Fatalf("exit code = %d, want the harness's own 7", code)
	}
}
