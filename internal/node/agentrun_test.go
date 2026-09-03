package node

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
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
	case "load":
		cfg.Capabilities = acp.AgentCapabilities{LoadSession: true}
		cfg.Rewind = func(ctx context.Context, s *acptest.Session, req acp.LoadSessionRequest) error {
			return s.Text("replayed-history-for-" + string(req.SessionID))
		}
	case "resume":
		cfg.Capabilities = acp.AgentCapabilities{SessionCapabilities: &acp.SessionCapabilities{Resume: &acp.SessionResumeCapabilities{}}}
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
}

func newAgentFixture(t *testing.T) *agentFixture {
	t.Helper()
	n := newTestNode(t, nil)
	f := &agentFixture{n: n, reports: make(chan *proto.AgentReport, 256)}
	n.agentReportSink = func(ctx context.Context, rep *proto.AgentReport) error {
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
	if _, err := os.Stat(filepath.Join(f.w.handle.FS().Root(), "notes", "a.txt")); err != nil {
		t.Fatalf("file not written inside the jail: %v", err)
	}
	if _, err := os.Stat(filepath.Join(f.w.handle.FS().Root(), "..", "escaped.txt")); err == nil {
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
	done := f.n.agentRunsDone[agentRunKey("ag_1", "run_crash")]
	f.n.mu.Unlock()
	if !done {
		t.Fatal("crashed run not remembered as finished")
	}
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
