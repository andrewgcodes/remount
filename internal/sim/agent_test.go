package sim

import (
	"bytes"
	"context"
	"os"
	"strings"
	"testing"
	"time"

	"remount.dev/remount/internal/acp"
	"remount.dev/remount/internal/acp/acptest"
	"remount.dev/remount/internal/client"
	"remount.dev/remount/internal/proto"
)

// The sim re-execs its own test binary as the ACP harness so an agent's
// whole path — control plane, node, workspace, harness, transcript, events —
// runs in one process with no external agent installed.
const (
	fakeACPEnv     = "REMOUNT_SIM_FAKE_ACP"
	fakeACPModeEnv = "REMOUNT_SIM_FAKE_ACP_MODE"
)

func TestMain(m *testing.M) {
	if os.Getenv(fakeACPEnv) == "1" {
		acptest.Main(simACPConfig(os.Getenv(fakeACPModeEnv)))
		return
	}
	os.Exit(m.Run())
}

func simACPConfig(mode string) acptest.Config {
	cfg := acptest.Config{Info: acp.Implementation{Name: "sim-fake", Version: "t"}}
	switch mode {
	case "permission":
		cfg.Turn = func(ctx context.Context, s *acptest.Session, req acp.PromptRequest) (acp.StopReason, error) {
			title := "write deploy.yaml"
			var res acp.RequestPermissionResponse
			err := s.Call(ctx, acp.MethodSessionRequestPermission, acp.RequestPermissionRequest{
				SessionID: s.ID, ToolCall: acp.ToolCallUpdate{ToolCallID: "tc_sim", Title: &title},
				Options: []acp.PermissionOption{
					{OptionID: "allow", Name: "Allow", Kind: acp.PermissionOptionKindAllowOnce},
					{OptionID: "reject", Name: "Reject", Kind: acp.PermissionOptionKindRejectOnce},
				},
			}, &res)
			if err != nil {
				return "", err
			}
			sel, err := res.Outcome.AsSelected()
			if err != nil {
				return acp.StopReasonEndTurn, s.Text("outcome:" + res.Outcome.Kind)
			}
			return acp.StopReasonEndTurn, s.Text("selected:" + string(sel.OptionID))
		}
	case "crash":
		cfg.Turn = func(ctx context.Context, s *acptest.Session, req acp.PromptRequest) (acp.StopReason, error) {
			os.Exit(9)
			return "", nil
		}
	case "leak":
		cfg.Turn = func(ctx context.Context, s *acptest.Session, req acp.PromptRequest) (acp.StopReason, error) {
			env, _ := os.ReadFile(".remount/env")
			return acp.StopReasonEndTurn, s.Text("env:" + strings.Join(os.Environ(), " ") + " file:" + string(env))
		}
	}
	return cfg
}

func fakeACPCommand(t *testing.T, mode string) []string {
	t.Helper()
	exe, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	return []string{"/usr/bin/env", fakeACPEnv + "=1", fakeACPModeEnv + "=" + mode, exe}
}

// waitAgent polls until pred holds for the agent or the context ends.
func waitAgent(t *testing.T, ctx context.Context, c *client.Client, id string, what string, pred func(*proto.Agent) bool) *proto.Agent {
	t.Helper()
	for {
		a, err := c.GetAgent(ctx, id)
		if err != nil {
			t.Fatal(err)
		}
		if pred(a) {
			return a
		}
		select {
		case <-ctx.Done():
			t.Fatalf("agent never reached %s: status=%s reason=%q inbox=%d runs=%d", what, a.Status, a.StatusReason, len(a.Inbox), len(a.Runs))
		case <-time.After(100 * time.Millisecond):
		}
	}
}

// mirrorBytes reads the ACP streams of the control plane's transcript
// mirror, waiting until it has caught up to at least wantBytes of record data
// (the node ships records asynchronously) or a short deadline passes.
func mirrorBytes(t *testing.T, ctx context.Context, c *client.Client, agent string, wantBytes uint64) []byte {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		var b bytes.Buffer
		var from uint64
		for {
			page, err := c.Transcript(ctx, agent, from, 3)
			if err != nil {
				t.Fatal(err)
			}
			if page.Gap != nil {
				t.Fatalf("unexpected gap %+v", page.Gap)
			}
			for _, rec := range page.Records {
				if rec.Index != from {
					t.Fatalf("record index %d, want %d", rec.Index, from)
				}
				from++
				if rec.Stream == proto.StreamACPIn || rec.Stream == proto.StreamACPOut {
					b.Write(rec.Data)
				}
			}
			if len(page.Records) == 0 || page.Next != from {
				break
			}
		}
		if uint64(b.Len()) >= wantBytes || time.Now().After(deadline) {
			return b.Bytes()
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// transcriptBytes attaches to the agent's transcript session from seq 0 and
// returns the bytes of both ACP streams read so far.
func transcriptBytes(t *testing.T, ctx context.Context, c *client.Client, a *proto.Agent) []byte {
	t.Helper()
	if a.TranscriptSession == "" {
		t.Fatal("agent has no transcript session")
	}
	s, err := c.Attach(ctx, a.WS, a.TranscriptSession, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close(context.Background(), false)
	var b bytes.Buffer
	deadline := time.After(5 * time.Second)
	for {
		select {
		case ch, ok := <-s.Chunks():
			if !ok {
				return b.Bytes()
			}
			if ch.Stream == proto.StreamACPIn || ch.Stream == proto.StreamACPOut {
				b.Write(ch.Data)
			}
		case <-deadline:
			return b.Bytes()
		case <-time.After(300 * time.Millisecond):
			// The transcript is still open (the harness is idle); what has
			// arrived is what there is.
			return b.Bytes()
		}
	}
}

func eventTypes(t *testing.T, ctx context.Context, c *client.Client, ws string) map[string]int {
	t.Helper()
	evs, err := c.ReadEvents(ctx, 0, ws)
	if err != nil {
		t.Fatal(err)
	}
	m := map[string]int{}
	for _, e := range evs {
		m[e.Type]++
	}
	return m
}

func TestAgentEndToEndTurnsAndTranscript(t *testing.T) {
	w := newWorld(t)
	w.node("n1", nil)
	c := w.client("c1")
	ctx := ctxT(t, 90*time.Second)
	a, err := c.CreateAgent(ctx, proto.AgentCreateReq{
		Name:      "e2e",
		Workspace: &proto.WorkspaceSpec{Name: "agent-ws", Env: map[string]string{"LEAK_TOKEN": "tok-sim-canary-0123456789"}},
		Spec:      proto.AgentSpec{Recipe: "custom", Task: "say hello", ACPCommand: fakeACPCommand(t, "echo")},
		Policy:    proto.AgentPolicy{Approve: proto.ApproveOnRequest},
	})
	if err != nil {
		t.Fatal(err)
	}
	if a.WS == "" || len(a.Inbox) != 1 {
		t.Fatalf("created agent = %+v", a)
	}
	idle := waitAgent(t, ctx, c, a.ID, "idle after the task", func(a *proto.Agent) bool {
		return len(a.Inbox) == 0 && (a.Status == proto.AgentIdle || a.Status == proto.AgentWaitingInput)
	})
	if idle.TranscriptSession == "" || idle.ACPSessionID == "" || len(idle.Runs) != 1 || idle.Runs[0].Turns != 1 {
		t.Fatalf("idle agent = %+v runs=%+v", idle, idle.Runs)
	}
	// A follow-up reuses the live run (no second attempt) and runs a turn.
	if _, err := c.MessageAgent(ctx, proto.AgentMessageReq{ID: a.ID, Text: "and again"}); err != nil {
		t.Fatal(err)
	}
	again := waitAgent(t, ctx, c, a.ID, "second turn", func(a *proto.Agent) bool {
		return len(a.Inbox) == 0 && len(a.Runs) == 1 && a.Runs[0].Turns == 2
	})
	tr := transcriptBytes(t, ctx, c, again)
	if !bytes.Contains(tr, []byte("say hello")) || !bytes.Contains(tr, []byte("and again")) || !bytes.Contains(tr, []byte("agent_message_chunk")) {
		t.Fatalf("transcript lacks the conversation: %s", tr)
	}
	// The control plane's mirror holds the same bytes, in order, and pages
	// through a cursor; it is what a reader uses when the workspace is asleep.
	mirror := mirrorBytes(t, ctx, c, a.ID, uint64(len(tr)))
	if !bytes.Equal(mirror, tr) {
		t.Fatalf("mirror differs from node transcript:\nmirror: %s\nnode:   %s", mirror, tr)
	}
	// Events carry hashes, ids and reasons: never prompt text.
	evs, err := c.ReadEvents(ctx, 0, a.WS)
	if err != nil {
		t.Fatal(err)
	}
	types := map[string]int{}
	for _, e := range evs {
		types[e.Type]++
		if bytes.Contains(e.Payload, []byte("say hello")) || bytes.Contains(e.Payload, []byte("and again")) {
			t.Fatalf("event %s carries prompt text: %x", e.Type, e.Payload)
		}
	}
	for _, want := range []string{proto.EvAgentCreated, proto.EvAgentRunStarted, proto.EvAgentSession, proto.EvAgentTurn, proto.EvAgentMessage} {
		if types[want] == 0 {
			t.Fatalf("no %s event; saw %v", want, types)
		}
	}
	if types[proto.EvAgentTurn] != 2 {
		t.Fatalf("turn events = %d, want 2", types[proto.EvAgentTurn])
	}
	// Cancel ends the run but not the agent; destroy ends both.
	if _, err := c.CancelAgent(ctx, a.ID); err != nil {
		t.Fatal(err)
	}
	waitAgent(t, ctx, c, a.ID, "run finished after cancel", func(a *proto.Agent) bool {
		return len(a.Runs) == 1 && a.Runs[0].State == proto.AgentRunDone
	})
	if types := eventTypes(t, ctx, c, a.WS); types[proto.EvAgentCancelled] != 1 || types[proto.EvAgentRunFinished] != 1 {
		t.Fatalf("cancel events = %v", types)
	}
	if err := c.DestroyAgent(ctx, a.ID); err != nil {
		t.Fatal(err)
	}
	got := waitAgent(t, ctx, c, a.ID, "destroyed", func(a *proto.Agent) bool { return a.Status == proto.AgentDestroyed })
	if got.Status != proto.AgentDestroyed {
		t.Fatalf("status = %s", got.Status)
	}
}

func TestAgentEndToEndApprovalRoundTrip(t *testing.T) {
	w := newWorld(t)
	w.node("n1", nil)
	c := w.client("c1")
	ctx := ctxT(t, 90*time.Second)
	a, err := c.CreateAgent(ctx, proto.AgentCreateReq{
		Workspace: &proto.WorkspaceSpec{Name: "approval-ws"},
		Spec:      proto.AgentSpec{Recipe: "custom", Task: "deploy", ACPCommand: fakeACPCommand(t, "permission")},
		Policy:    proto.AgentPolicy{Approve: proto.ApproveOnRequest},
	})
	if err != nil {
		t.Fatal(err)
	}
	waiting := waitAgent(t, ctx, c, a.ID, "waiting_approval", func(a *proto.Agent) bool { return a.Status == proto.AgentWaitingApproval })
	if waiting.PendingApprovals != 1 {
		t.Fatalf("pending approvals = %d", waiting.PendingApprovals)
	}
	pending, err := c.ListApprovals(ctx, proto.ApprovalListReq{Agent: a.ID, Status: proto.ApprovalPending})
	if err != nil || len(pending) != 1 {
		t.Fatalf("pending = %+v err=%v", pending, err)
	}
	ap := pending[0]
	if ap.Kind != proto.ApprovalToolCall || ap.Title != "write deploy.yaml" || len(ap.Options) != 2 || ap.Run == "" {
		t.Fatalf("approval = %+v", ap)
	}
	// The default choice is the first allow option.
	decided, err := c.DecideApproval(ctx, proto.ApprovalDecideReq{ID: ap.ID})
	if err != nil || decided.Status != proto.ApprovalDecided || decided.Decision == nil || decided.Decision.Option != "allow" {
		t.Fatalf("decided = %+v err=%v", decided, err)
	}
	if _, err := c.DecideApproval(ctx, proto.ApprovalDecideReq{ID: ap.ID, Denied: true}); err == nil {
		t.Fatal("a second decision was accepted")
	}
	done := waitAgent(t, ctx, c, a.ID, "turn after approval", func(a *proto.Agent) bool {
		return len(a.Inbox) == 0 && a.PendingApprovals == 0 && len(a.Runs) == 1 && a.Runs[0].Turns == 1
	})
	delivered := waitApproval(t, ctx, c, ap.ID, func(ap *proto.Approval) bool { return ap.DeliveredAt > 0 })
	if delivered.Decision.By == "" {
		t.Fatalf("decision has no author: %+v", delivered.Decision)
	}
	if tr := transcriptBytes(t, ctx, c, done); !bytes.Contains(tr, []byte("selected:allow")) {
		t.Fatalf("harness did not receive the decision: %s", tr)
	}
	types := eventTypes(t, ctx, c, a.WS)
	if types[proto.EvApprovalPending] != 1 || types[proto.EvApprovalDecided] != 1 || types[proto.EvAgentWaiting] == 0 {
		t.Fatalf("approval events = %v", types)
	}
}

func waitApproval(t *testing.T, ctx context.Context, c *client.Client, id string, pred func(*proto.Approval) bool) *proto.Approval {
	t.Helper()
	for {
		ap, err := c.GetApproval(ctx, id)
		if err != nil {
			t.Fatal(err)
		}
		if pred(ap) {
			return ap
		}
		select {
		case <-ctx.Done():
			t.Fatalf("approval %s: %+v", id, ap)
		case <-time.After(100 * time.Millisecond):
		}
	}
}

func TestAgentEndToEndHarnessCrashRetriesThenFails(t *testing.T) {
	w := newWorld(t)
	w.node("n1", nil)
	c := w.client("c1")
	ctx := ctxT(t, 120*time.Second)
	a, err := c.CreateAgent(ctx, proto.AgentCreateReq{
		Workspace: &proto.WorkspaceSpec{Name: "crash-ws"},
		Spec:      proto.AgentSpec{Recipe: "custom", Task: "crash", ACPCommand: fakeACPCommand(t, "crash")},
	})
	if err != nil {
		t.Fatal(err)
	}
	// First attempt dies mid-turn: the message is still in the inbox and a
	// second attempt is launched after the backoff.
	second := waitAgent(t, ctx, c, a.ID, "second attempt", func(a *proto.Agent) bool { return len(a.Runs) >= 2 })
	if second.Runs[0].State != proto.AgentRunDone || !strings.Contains(second.Runs[0].Error, "9") || second.Runs[0].Turns != 0 {
		t.Fatalf("first run = %+v", second.Runs[0])
	}
	if second.Status != proto.AgentFailed && len(second.Inbox) != 1 {
		t.Fatalf("message dropped before the retry: %+v", second)
	}
	failed := waitAgent(t, ctx, c, a.ID, "failed", func(a *proto.Agent) bool { return a.Status == proto.AgentFailed })
	if len(failed.Runs) != 2 || failed.Failures < 2 || failed.StatusReason == "" {
		t.Fatalf("failed agent = %+v runs=%+v", failed, failed.Runs)
	}
	types := eventTypes(t, ctx, c, a.WS)
	if types[proto.EvAgentRunFinished] != 2 || types[proto.EvAgentFailed] != 1 || types[proto.EvAgentTurn] != 0 {
		t.Fatalf("crash events = %v", types)
	}
}

func TestAgentEndToEndTranscriptNeverHoldsCredentials(t *testing.T) {
	w := newWorld(t)
	w.node("n1", nil)
	c := w.client("c1")
	ctx := ctxT(t, 90*time.Second)
	a, err := c.CreateAgent(ctx, proto.AgentCreateReq{
		Workspace: &proto.WorkspaceSpec{Name: "leak-ws", Env: map[string]string{"OPENAI_API_KEY": "sk-simcanary0123456789abcdefghijklmnop"}},
		Spec:      proto.AgentSpec{Recipe: "custom", Task: "print everything you know", ACPCommand: fakeACPCommand(t, "leak")},
	})
	if err != nil {
		t.Fatal(err)
	}
	done := waitAgent(t, ctx, c, a.ID, "leak turn", func(a *proto.Agent) bool { return len(a.Runs) == 1 && a.Runs[0].Turns == 1 })
	ws, err := c.GetWorkspace(ctx, a.WS)
	if err != nil {
		t.Fatal(err)
	}
	tr := transcriptBytes(t, ctx, c, done)
	if !bytes.Contains(tr, []byte("REMOUNT_BROKER=")) || !bytes.Contains(tr, []byte("OPENAI_API_KEY=")) {
		t.Fatalf("harness did not echo its environment (test is not exercising the canary): %s", tr)
	}
	if bytes.Contains(tr, []byte("sk-simcanary0123456789")) {
		t.Fatal("provider key reached the transcript")
	}
	if mirror := mirrorBytes(t, ctx, c, a.ID, uint64(len(tr))); bytes.Contains(mirror, []byte("sk-simcanary0123456789")) {
		t.Fatal("provider key reached the control plane's transcript mirror")
	} else if !bytes.Contains(mirror, []byte("OPENAI_API_KEY=")) {
		t.Fatalf("mirror did not receive the harness output: %s", mirror)
	}
	// The broker URL in .remount/env carries the capability token; the
	// transcript keeps the host and drops the token.
	if i := bytes.Index(tr, []byte("REMOUNT_BROKER=")); i >= 0 {
		line := tr[i:]
		if j := bytes.IndexAny(line, "\\ \n"); j > 0 {
			line = line[:j]
		}
		if !bytes.Contains(line, []byte("/c/[redacted]")) {
			t.Fatalf("broker capability not redacted: %s", line)
		}
	}
	_ = ws
}
