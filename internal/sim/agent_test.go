package sim

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"runtime"
	"strings"
	"testing"
	"time"

	"remount.dev/remount/internal/acp"
	"remount.dev/remount/internal/acp/acptest"
	"remount.dev/remount/internal/client"
	"remount.dev/remount/internal/proto"
	"remount.dev/remount/internal/server"
)

// The sim re-execs its own test binary as the ACP harness so an agent's
// whole path — control plane, node, workspace, harness, transcript, events —
// runs in one process with no external agent installed.
const (
	fakeACPEnv     = "REMOUNT_SIM_FAKE_ACP"
	fakeACPModeEnv = "REMOUNT_SIM_FAKE_ACP_MODE"
	fakeACPArg     = "-remount-sim-fake-acp"
)

// newAgentWorld starts a world with a production-sized lease. Agent tests
// spawn and kill real harness processes, and on a slow hosted runner under
// the race detector that work can stall the node's renewals past the
// 1.33-second fence window a 2-second lease leaves, so the node fences a
// workspace the test still holds. Tests that need a lease to expire, such
// as TestAgentSurvivesNodeLoss, keep newWorld and its short lease.
func newAgentWorld(t *testing.T) *world {
	t.Helper()
	return newWorldWith(t, func(o *server.Options) { o.LeaseSec = 30 })
}

func TestMain(m *testing.M) {
	if len(os.Args) == 3 && os.Args[1] == fakeACPArg {
		acptest.Main(simACPConfig(os.Args[2]))
		return
	}
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
	case "mode":
		cfg.Modes = &acp.SessionModeState{
			CurrentModeID: "default",
			AvailableModes: []acp.SessionMode{
				{ID: "default", Name: "Manual"},
				{ID: "acceptEdits", Name: "Accept edits"},
			},
		}
		cfg.Turn = func(ctx context.Context, s *acptest.Session, req acp.PromptRequest) (acp.StopReason, error) {
			return acp.StopReasonEndTurn, s.Text("mode:" + string(s.Mode()))
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
	return []string{exe, fakeACPArg, mode}
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
	w := newAgentWorld(t)
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

func TestAgentOmittedSandboxAppliesWorkspaceWriteACPMode(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("unavailable: recipe ACP launcher scripts require a POSIX shell")
	}
	w := newAgentWorld(t)
	w.node("n1", nil)
	c := w.client("c1")
	ctx := ctxT(t, 90*time.Second)
	exe, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	a, err := c.CreateAgent(ctx, proto.AgentCreateReq{
		Workspace: &proto.WorkspaceSpec{Name: "default-sandbox-ws"},
		Spec: proto.AgentSpec{
			Recipe: "claude",
			Task:   "check mode",
			RecipeYAML: fmt.Sprintf(`name: claude
auth: workspace_resident
command: ["unused"]
acp:
  command: [%q, %q, "mode"]
  sandbox_modes:
    workspace-write: acceptEdits
`, exe, fakeACPArg),
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if a.Spec.Sandbox != proto.AgentSandboxWorkspaceWrite {
		t.Fatalf("sandbox = %q, want workspace-write", a.Spec.Sandbox)
	}
	idle := waitAgent(t, ctx, c, a.ID, "workspace-write turn", func(a *proto.Agent) bool {
		return len(a.Inbox) == 0 && (a.Status == proto.AgentIdle || a.Status == proto.AgentWaitingInput)
	})
	if tr := transcriptBytes(t, ctx, c, idle); !bytes.Contains(tr, []byte(`"text":"mode:acceptEdits"`)) {
		t.Fatalf("transcript does not show workspace-write ACP mode: %s", tr)
	}
}

func TestAgentEndToEndApprovalRoundTrip(t *testing.T) {
	w := newAgentWorld(t)
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
	w := newAgentWorld(t)
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
	w := newAgentWorld(t)
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

// TestAgentMaxTurnsStopsHarnessAndMirrorsExit: finishing by policy is not
// just a status flip. The harness the node keeps up for follow-ups is
// stopped, its run closes with a finished report, and the durable mirror
// ends with the exit chunk so a reader that never saw the node knows the
// conversation ended. Before this the harness idled on the node until the
// workspace was destroyed.
func TestAgentMaxTurnsStopsHarnessAndMirrorsExit(t *testing.T) {
	w := newAgentWorld(t)
	w.node("n1", nil)
	c := w.client("c1")
	ctx := ctxT(t, 90*time.Second)
	a, err := c.CreateAgent(ctx, proto.AgentCreateReq{
		Name:           "bounded",
		Workspace:      &proto.WorkspaceSpec{Name: "bounded-ws", Labels: map[string]string{"team": "sim"}},
		Spec:           proto.AgentSpec{Recipe: "custom", Task: "only turn", ACPCommand: fakeACPCommand(t, "echo")},
		Policy:         proto.AgentPolicy{Approve: proto.ApproveNever, MaxTurns: 1},
		IdempotencyKey: "bounded-1",
	})
	if err != nil {
		t.Fatal(err)
	}
	// The label map on the request is fingerprinted for idempotency; the
	// control plane must not write its own label through it.
	again, err := c.CreateAgent(ctx, proto.AgentCreateReq{
		Name:           "bounded",
		Workspace:      &proto.WorkspaceSpec{Name: "bounded-ws", Labels: map[string]string{"team": "sim"}},
		Spec:           proto.AgentSpec{Recipe: "custom", Task: "only turn", ACPCommand: fakeACPCommand(t, "echo")},
		Policy:         proto.AgentPolicy{Approve: proto.ApproveNever, MaxTurns: 1},
		IdempotencyKey: "bounded-1",
	})
	if err != nil || again.ID != a.ID {
		t.Fatalf("idempotent create = %+v, %v", again, err)
	}
	done := waitAgent(t, ctx, c, a.ID, "finished with the run closed", func(a *proto.Agent) bool {
		return a.Status == proto.AgentFinished && len(a.Runs) == 1 && a.Runs[0].State == proto.AgentRunDone
	})
	if done.Turns != 1 || done.StatusReason != "max_turns 1 reached" {
		t.Fatalf("finished agent = %+v", done)
	}
	types := eventTypes(t, ctx, c, a.WS)
	if types[proto.EvAgentFinished] != 1 || types[proto.EvAgentRunFinished] != 1 {
		t.Fatalf("events = %v", types)
	}
	// The harness's transcript session has exited on the node and the mirror
	// holds that exit as its last record.
	if s, err := c.Attach(ctx, a.WS, done.TranscriptSession, 0); err == nil {
		var exited bool
		deadline := time.After(5 * time.Second)
	read:
		for {
			select {
			case ch, ok := <-s.Chunks():
				if !ok {
					break read
				}
				if ch.Stream == proto.StreamExit {
					exited = true
					break read
				}
			case <-deadline:
				break read
			}
		}
		s.Close(context.Background(), false)
		if !exited {
			t.Fatal("transcript session still open after max_turns")
		}
	}
	var last *proto.TranscriptRecord
	for deadline := time.Now().Add(5 * time.Second); ; {
		page, err := c.Transcript(ctx, a.ID, 0, proto.MaxTranscriptPage)
		if err != nil {
			t.Fatal(err)
		}
		if n := len(page.Records); n > 0 && page.Records[n-1].Stream == proto.StreamExit {
			last = &page.Records[n-1]
			if !page.Done {
				t.Fatal("mirror has the exit but is not done")
			}
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("mirror never received the exit chunk: %d records", len(page.Records))
		}
		time.Sleep(50 * time.Millisecond)
	}
	var exit proto.ExitInfo
	if err := proto.Unmarshal(last.Data, &exit); err != nil || exit.Reason != "cancelled" {
		t.Fatalf("mirrored exit = %+v, %v", exit, err)
	}
}

// TestAgentSurvivesNodeLoss: the node running the harness dies for good.
// The workspace fails over to another node from its last snapshot, the
// agent's run is closed as lost, and the next message starts a fresh
// attempt on the new node with the same ACP session id. The durable mirror
// keeps every record from before the loss at its original index and
// continues after it, so a reader that only ever saw the control plane sees
// one conversation, not two.
func TestAgentSurvivesNodeLoss(t *testing.T) {
	w := newWorld(t)
	nodes := map[string]string{}
	for _, name := range []string{"n1", "n2"} {
		nodes[w.node(name, map[string]string{"zone": "a"}).ID()] = name
	}
	c := w.client("c1")
	ctx := ctxT(t, 150*time.Second)
	a, err := c.CreateAgent(ctx, proto.AgentCreateReq{
		Name:      "durable",
		Workspace: &proto.WorkspaceSpec{Name: "durable-ws", Placement: proto.Placement{Allow: map[string]string{"zone": "a"}}},
		Spec:      proto.AgentSpec{Recipe: "custom", Task: "first turn", ACPCommand: fakeACPCommand(t, "echo")},
		Policy:    proto.AgentPolicy{Approve: proto.ApproveNever},
	})
	if err != nil {
		t.Fatal(err)
	}
	idle := waitAgent(t, ctx, c, a.ID, "idle after the first turn", func(a *proto.Agent) bool {
		return len(a.Inbox) == 0 && len(a.Runs) == 1 && a.Runs[0].Turns == 1
	})
	if err := c.WriteFile(ctx, a.WS, "progress.txt", []byte("turn 1"), 0); err != nil {
		t.Fatal(err)
	}
	// A graceful move records the snapshot a failover restores from. The
	// run does not survive it (the workspace is released and re-claimed at a
	// new generation) but the agent does: no failure is counted.
	if _, err := c.MoveWorkspace(ctx, a.WS, nil, &proto.Placement{Allow: map[string]string{"zone": "a"}}); err != nil {
		t.Fatal(err)
	}
	moved, err := c.WaitClaimed(ctx, a.WS)
	if err != nil {
		t.Fatal(err)
	}
	afterMove := waitAgent(t, ctx, c, a.ID, "run closed and awake after the move", func(a *proto.Agent) bool {
		return len(a.Runs) == 1 && a.Runs[0].State == proto.AgentRunDone && a.Status != proto.AgentSleeping
	})
	if agentTerminalStatus(afterMove.Status) || afterMove.Failures != 0 {
		t.Fatalf("after move = %+v runs=%+v", afterMove, afterMove.Runs)
	}
	// A turn on the moved workspace leaves a live, idle harness on the node
	// that is about to die.
	if _, err := c.MessageAgent(ctx, proto.AgentMessageReq{ID: a.ID, Text: "turn after the move"}); err != nil {
		t.Fatal(err)
	}
	live := waitAgent(t, ctx, c, a.ID, "turn on the moved workspace", func(a *proto.Agent) bool {
		return len(a.Inbox) == 0 && len(a.Runs) == 2 && a.Runs[1].Turns == 1 && a.Runs[1].State == proto.AgentRunActive
	})
	if live.Runs[1].Node != moved.Node || live.Runs[1].Generation != moved.Generation {
		t.Fatalf("live run = %+v, workspace on %s gen %d", live.Runs[1], moved.Node, moved.Generation)
	}
	before := mirrorRecords(t, ctx, c, a.ID)
	if len(before) == 0 {
		t.Fatal("mirror empty before the loss")
	}

	// The node holding the workspace and the live harness dies permanently.
	holder := nodes[moved.Node]
	if holder == "" {
		t.Fatalf("unknown holder %s", moved.Node)
	}
	w.stopNode(holder)
	var failedOver *proto.Workspace
	for deadline := time.Now().Add(60 * time.Second); time.Now().Before(deadline); time.Sleep(200 * time.Millisecond) {
		ws, err := c.GetWorkspace(ctx, a.WS)
		if err != nil {
			t.Fatal(err)
		}
		if ws.State == proto.WSClaimed && ws.Node != moved.Node {
			failedOver = ws
			break
		}
	}
	if failedOver == nil {
		t.Fatal("workspace did not fail over")
	}
	if b, err := c.ReadFile(ctx, a.WS, "progress.txt"); err != nil || string(b) != "turn 1" {
		t.Fatalf("restored file = %q, %v", b, err)
	}

	// The live run is closed as lost, and the next message launches attempt
	// 3 on the surviving node, in the same ACP session; the turn completes.
	lost := waitAgent(t, ctx, c, a.ID, "live run closed as lost", func(a *proto.Agent) bool {
		return len(a.Runs) == 2 && a.Runs[1].State == proto.AgentRunDone
	})
	if lost.Runs[1].Error == "" || agentTerminalStatus(lost.Status) {
		t.Fatalf("lost run = %+v status=%s", lost.Runs[1], lost.Status)
	}
	if _, err := c.MessageAgent(ctx, proto.AgentMessageReq{ID: a.ID, Text: "second turn after the loss"}); err != nil {
		t.Fatal(err)
	}
	resumed := waitAgent(t, ctx, c, a.ID, "turn on the new node", func(a *proto.Agent) bool {
		return len(a.Inbox) == 0 && len(a.Runs) == 3 && a.Runs[2].Turns == 1
	})
	if resumed.Runs[2].Node != failedOver.Node || resumed.Runs[2].Attempt != 3 || resumed.Runs[2].Generation != failedOver.Generation {
		t.Fatalf("third run = %+v (workspace on %s gen %d)", resumed.Runs[2], failedOver.Node, failedOver.Generation)
	}
	if resumed.ACPSessionID != idle.ACPSessionID || resumed.Turns != 3 {
		t.Fatalf("resumed agent = %+v", resumed)
	}
	after := mirrorRecords(t, ctx, c, a.ID)
	if len(after) <= len(before) {
		t.Fatalf("mirror did not grow after the loss: %d -> %d", len(before), len(after))
	}
	for i, rec := range before {
		if after[i].Index != rec.Index || after[i].Run != rec.Run || after[i].Seq != rec.Seq || !bytes.Equal(after[i].Data, rec.Data) {
			t.Fatalf("mirror record %d changed across the loss: %+v -> %+v", i, rec, after[i])
		}
	}
	var text bytes.Buffer
	runs := map[string]bool{}
	for _, rec := range after {
		runs[rec.Run] = true
		if rec.Stream == proto.StreamACPIn || rec.Stream == proto.StreamACPOut {
			text.Write(rec.Data)
		}
	}
	if len(runs) != 3 || !bytes.Contains(text.Bytes(), []byte("first turn")) || !bytes.Contains(text.Bytes(), []byte("turn after the move")) || !bytes.Contains(text.Bytes(), []byte("second turn after the loss")) {
		t.Fatalf("mirror does not span all runs (%d runs): %s", len(runs), text.Bytes())
	}
	// Each run is announced twice (dispatched pending, then active); the
	// moved and the lost run each finished once, and the agent never failed.
	types := eventTypes(t, ctx, c, a.WS)
	if types[proto.EvAgentRunStarted] != 6 || types[proto.EvAgentRunFinished] != 2 || types[proto.EvAgentFailed] != 0 || types[proto.EvAgentTurn] != 3 || types["ws.lease_expired"] != 1 {
		t.Fatalf("events = %v", types)
	}
}

// mirrorRecords reads the whole durable mirror through the cursor.
func mirrorRecords(t *testing.T, ctx context.Context, c *client.Client, agent string) []proto.TranscriptRecord {
	t.Helper()
	var out []proto.TranscriptRecord
	var from uint64
	for {
		page, err := c.Transcript(ctx, agent, from, proto.MaxTranscriptPage)
		if err != nil {
			t.Fatal(err)
		}
		if page.Gap != nil {
			t.Fatalf("unexpected gap %+v", page.Gap)
		}
		out = append(out, page.Records...)
		if len(page.Records) == 0 || page.Next == from {
			return out
		}
		from = page.Next
	}
}

func agentTerminalStatus(s string) bool {
	return s == proto.AgentFailed || s == proto.AgentFinished || s == proto.AgentDestroyed
}

// A child agent inherits its parent's caps and never exceeds them; when it
// finishes, the parent hears about it as a prompt turn and an event (1A.8,
// E22 with the fake harness).
func TestAgentChildReportsToParent(t *testing.T) {
	w := newAgentWorld(t)
	w.node("n1", nil)
	c := w.client("c1")
	ctx := ctxT(t, 120*time.Second)
	parent, err := c.CreateAgent(ctx, proto.AgentCreateReq{
		Name: "parent",
		Workspace: &proto.WorkspaceSpec{Name: "parent-ws", Security: proto.SecuritySpec{Network: proto.NetworkPolicy{Rules: []proto.EgressRule{{
			ID: "docs", Protocol: proto.EgressProtocolHTTPS, Hosts: []string{"docs.example.com"}, Methods: []string{"GET"},
		}}}}},
		Spec:   proto.AgentSpec{Recipe: "custom", Task: "coordinate", ACPCommand: fakeACPCommand(t, "echo"), Sandbox: proto.AgentSandboxReadOnly},
		Policy: proto.AgentPolicy{Approve: proto.ApproveNever, MaxTurns: 4},
	})
	if err != nil {
		t.Fatal(err)
	}
	waitAgent(t, ctx, c, parent.ID, "parent waiting", func(a *proto.Agent) bool { return a.Status == proto.AgentWaitingInput })

	// A child may not hold what the parent does not: providers, workspace
	// bindings, a looser policy, an egress rule the parent lacks.
	for name, req := range map[string]proto.AgentCreateReq{
		"providers": {Parent: parent.ID, Workspace: &proto.WorkspaceSpec{}, Spec: proto.AgentSpec{Recipe: "custom", Task: "x", ACPCommand: fakeACPCommand(t, "echo"), Providers: []string{"openai"}}},
		"bindings":  {Parent: parent.ID, Workspace: &proto.WorkspaceSpec{Bindings: []string{"b_secret"}}, Spec: proto.AgentSpec{Recipe: "custom", Task: "x", ACPCommand: fakeACPCommand(t, "echo")}},
		"turns":     {Parent: parent.ID, Workspace: &proto.WorkspaceSpec{}, Spec: proto.AgentSpec{Recipe: "custom", Task: "x", ACPCommand: fakeACPCommand(t, "echo")}, Policy: proto.AgentPolicy{MaxTurns: 10}},
		"approve":   {Parent: parent.ID, Workspace: &proto.WorkspaceSpec{}, Spec: proto.AgentSpec{Recipe: "custom", Task: "x", ACPCommand: fakeACPCommand(t, "echo")}, Policy: proto.AgentPolicy{Approve: proto.ApproveOnRequest, MaxTurns: 1}},
		"egress":    {Parent: parent.ID, Workspace: &proto.WorkspaceSpec{Security: proto.SecuritySpec{Network: proto.NetworkPolicy{Rules: []proto.EgressRule{{ID: "docs", Protocol: proto.EgressProtocolHTTPS, Hosts: []string{"docs.example.com"}}}}}}, Spec: proto.AgentSpec{Recipe: "custom", Task: "x", ACPCommand: fakeACPCommand(t, "echo")}},
		"allow":     {Parent: parent.ID, Workspace: &proto.WorkspaceSpec{Security: proto.SecuritySpec{Network: proto.NetworkPolicy{Default: proto.NetworkDefaultAllow}}}, Spec: proto.AgentSpec{Recipe: "custom", Task: "x", ACPCommand: fakeACPCommand(t, "echo")}},
	} {
		if _, err := c.CreateAgent(ctx, req); !errors.Is(err, &proto.Error{Code: proto.CodeDenied}) {
			t.Fatalf("%s: child create error = %v, want denied", name, err)
		}
	}

	child, err := c.CreateAgent(ctx, proto.AgentCreateReq{
		Name:      "child",
		Parent:    parent.ID,
		Workspace: &proto.WorkspaceSpec{Name: "child-ws"},
		Spec:      proto.AgentSpec{Recipe: "custom", Task: "do one thing", ACPCommand: fakeACPCommand(t, "echo")},
		Policy:    proto.AgentPolicy{Approve: proto.ApproveNever, MaxTurns: 1},
	})
	if err != nil {
		t.Fatal(err)
	}
	if child.Parent != parent.ID || child.Spec.Sandbox != proto.AgentSandboxReadOnly || child.WS == parent.WS {
		t.Fatalf("child = %+v", child)
	}
	if cws, err := c.GetWorkspace(ctx, child.WS); err != nil {
		t.Fatal(err)
	} else if cws.Spec.Security.Network.Default != proto.NetworkDefaultDeny || len(cws.Spec.Security.Network.Rules) != 1 || cws.Spec.Security.Network.Rules[0].ID != "docs" {
		t.Fatalf("child security = %+v, want the parent's egress contract", cws.Spec.Security)
	}
	waitAgent(t, ctx, c, child.ID, "child finished", func(a *proto.Agent) bool { return a.Status == proto.AgentFinished })

	// The parent's next turn is the child's summary, and only once.
	p := waitAgent(t, ctx, c, parent.ID, "parent consumed the child summary", func(a *proto.Agent) bool {
		return a.Turns == 2 && len(a.Inbox) == 0
	})
	text := string(mirrorBytes(t, ctx, c, parent.ID, 0))
	if !strings.Contains(text, child.ID) || !strings.Contains(text, `\"status\":\"finished\"`) && !strings.Contains(text, `"status":"finished"`) {
		t.Fatalf("parent transcript lacks the child summary: %s", text)
	}
	types := eventTypes(t, ctx, c, p.WS)
	if types[proto.EvAgentChildDone] != 1 {
		t.Fatalf("parent events = %v", types)
	}
	got := waitAgent(t, ctx, c, child.ID, "child marked notified", func(a *proto.Agent) bool { return a.ParentNotified })
	if got.Status != proto.AgentFinished {
		t.Fatalf("child = %+v", got)
	}
	// A restart of the reconciler does not notify twice.
	time.Sleep(1500 * time.Millisecond)
	if again, _ := c.GetAgent(ctx, parent.ID); again.Turns != 2 || len(again.Inbox) != 0 {
		t.Fatalf("parent notified twice: %+v", again)
	}
	kids, err := c.ListAgents(ctx, proto.AgentListReq{Parent: parent.ID})
	if err != nil || len(kids) != 1 || kids[0].ID != child.ID {
		t.Fatalf("children = %+v, %v", kids, err)
	}
}

// A scheduled agent provisions its workspace and holds the inbox until
// Policy.StartAt, then runs; messages before the start are kept, not
// delivered (1A.8).
func TestAgentScheduledStartHoldsUntilDue(t *testing.T) {
	w := newAgentWorld(t)
	w.node("n1", nil)
	c := w.client("c1")
	ctx := ctxT(t, 90*time.Second)
	startAt := time.Now().Add(4 * time.Second)
	a, err := c.CreateAgent(ctx, proto.AgentCreateReq{
		Name:      "later",
		Workspace: &proto.WorkspaceSpec{Name: "later-ws"},
		Spec:      proto.AgentSpec{Recipe: "custom", Task: "first", ACPCommand: fakeACPCommand(t, "echo")},
		Policy:    proto.AgentPolicy{Approve: proto.ApproveNever, StartAt: startAt.UnixMilli()},
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := c.CreateAgent(ctx, proto.AgentCreateReq{
		Workspace: &proto.WorkspaceSpec{}, Spec: proto.AgentSpec{Recipe: "custom", Task: "x", ACPCommand: fakeACPCommand(t, "echo")},
		Policy: proto.AgentPolicy{StartAt: time.Now().Add(400 * 24 * time.Hour).UnixMilli()},
	}); !errors.Is(err, &proto.Error{Code: proto.CodeBadRequest}) {
		t.Fatalf("far future start_at error = %v", err)
	}
	if _, err := c.WaitClaimed(ctx, a.WS); err != nil {
		t.Fatal(err)
	}
	if _, err := c.MessageAgent(ctx, proto.AgentMessageReq{ID: a.ID, Text: "second"}); err != nil {
		t.Fatal(err)
	}
	held, err := c.GetAgent(ctx, a.ID)
	if err != nil {
		t.Fatal(err)
	}
	if held.Status != proto.AgentScheduled || len(held.Runs) != 0 || len(held.Inbox) != 2 {
		t.Fatalf("before start: %+v", held)
	}
	// Both messages are delivered in one run, in order, after the start.
	done := waitAgent(t, ctx, c, a.ID, "both turns after the start", func(a *proto.Agent) bool {
		return a.Turns == 2 && len(a.Inbox) == 0
	})
	if len(done.Runs) != 1 || done.Runs[0].StartedAt < startAt.UnixMilli() {
		t.Fatalf("run started early: %+v (start_at %d)", done.Runs, startAt.UnixMilli())
	}
	text := string(mirrorBytes(t, ctx, c, a.ID, 0))
	if !strings.Contains(text, "first") || strings.Index(text, "first") > strings.Index(text, "second") {
		t.Fatalf("turn order: %s", text)
	}
}
