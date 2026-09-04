package sim

import (
	"bytes"
	"context"
	"encoding/json"
	"regexp"
	"strings"
	"testing"
	"time"

	"remount.dev/remount/internal/client"
	"remount.dev/remount/internal/launch"
	"remount.dev/remount/internal/modelfake"
	"remount.dev/remount/internal/proto"
)

// TestPlanBOpenCodeAgentTranscriptApprovalAndResume is Plan B B2, B4 and B5.
// The same pinned OpenCode runs as an ACP agent against the same local model:
// the transcript carries the scripted tool call and the terminal result (B2);
// an automated approval is durably committed before the approved tool's
// result becomes observable anywhere (B5); and after sleep and restore the
// harness reloads the prior conversation rather than starting a new one (B4).
//
// It needs no credential and no environment variable.
func TestPlanBOpenCodeAgentTranscriptApprovalAndResume(t *testing.T) {
	fake, host, roots := planBUpstreamServer(t)
	recipe, err := launch.Load("opencode")
	if err != nil {
		t.Fatal(err)
	}
	_, c, d, _, image := planBWorld(t, host, roots, recipe.Hosts)
	binding := launch.Binding{ID: "b_openai", Preset: planBPreset(host)}
	ctx := ctxT(t, 15*time.Minute)

	// The workspace carries no remount.bindings label on purpose: a label is
	// re-resolved against the global preset table, which would put the real
	// api.openai.com base URL over the workspace env and send the lane at the
	// provider. The placeholder and the broker path come from the env instead.
	ws := mustWS(t, c, proto.WorkspaceSpec{
		Name: "planb-acp", Image: image,
		Requires: proto.Requires{Backend: "docker"},
		Bindings: []string{binding.ID},
		Env:      planBEnv(binding),
	})
	registerDockerWorkspaceCleanup(t, c, d, ws.ID)
	planBInstallOpenCode(t, ctx, c, ws.ID)
	planBWriteCatalog(t, ctx, c, ws.ID)
	// B5 needs the shell tool to ask. The project config is the last layer
	// OpenCode merges, so it overrides the recipe's sandbox default without
	// touching the broker wiring the recipe owns.
	if err := c.WriteFile(ctx, ws.ID, "opencode.json", []byte(`{"permission":{"bash":"ask"}}`+"\n"), 0o644); err != nil {
		t.Fatal(err)
	}

	a, err := c.CreateAgent(ctx, proto.AgentCreateReq{
		Name: "planb", WS: ws.ID,
		Spec: proto.AgentSpec{
			Recipe: "opencode", Task: planBTask, Mode: proto.AgentModeACP,
			Providers: []string{"openai"}, Primary: "openai",
			Model: "openai/" + planBModel, Sandbox: launch.SandboxWorkspaceWrite, Auth: launch.AuthAPIKey,
		},
		Policy: proto.AgentPolicy{Approve: proto.ApproveOnRequest},
	})
	if err != nil {
		t.Fatal(err)
	}

	// B5. The harness parks at the shell tool and the decision has not been
	// made, so the approved tool's result exists nowhere yet: not in the
	// transcript, and not in any request the upstream saw.
	waiting := waitAgent(t, ctx, c, a.ID, "waiting_approval", func(a *proto.Agent) bool {
		return a.Status == proto.AgentWaitingApproval
	})
	if waiting.PendingApprovals != 1 {
		t.Fatalf("pending approvals = %d", waiting.PendingApprovals)
	}
	pending, err := c.ListApprovals(ctx, proto.ApprovalListReq{Agent: a.ID, Status: proto.ApprovalPending})
	if err != nil || len(pending) != 1 {
		t.Fatalf("pending approvals = %+v err=%v", pending, err)
	}
	ap := pending[0]
	if ap.Kind != proto.ApprovalToolCall {
		t.Fatalf("approval = %+v", ap)
	}
	before := transcriptBytes(t, ctx, c, waiting)
	if bytes.Contains(before, []byte(planBShellMark)) {
		t.Fatal("the approved tool's result is in the transcript before the decision")
	}
	if fake.Saw(planBShellMark) {
		t.Fatal("the approved tool's result reached the model before the decision")
	}
	requestsAtDecision := fake.Count()
	pendingSeq := planBEventSeq(t, ctx, c, ws.ID, proto.EvApprovalPending)
	if pendingSeq == 0 {
		t.Fatal("the pending approval was not committed to the event log")
	}

	decided, err := c.DecideApproval(ctx, proto.ApprovalDecideReq{ID: ap.ID})
	if err != nil || decided.Status != proto.ApprovalDecided || decided.Decision == nil {
		t.Fatalf("decided = %+v err=%v", decided, err)
	}
	idle := waitAgent(t, ctx, c, a.ID, "idle after the approved turn", func(a *proto.Agent) bool {
		return len(a.Inbox) == 0 && a.PendingApprovals == 0 &&
			(a.Status == proto.AgentIdle || a.Status == proto.AgentWaitingInput)
	})
	// The decision is durable before the result is observable: its event
	// commits after the pending event and before any byte of the tool result
	// reached the model or the transcript.
	decidedSeq := planBEventSeq(t, ctx, c, ws.ID, proto.EvApprovalDecided)
	if decidedSeq <= pendingSeq {
		t.Fatalf("approval order pending=%d decided=%d", pendingSeq, decidedSeq)
	}
	resultRequest := planBFirstRequestWith(fake, planBShellMark)
	if resultRequest < 0 {
		t.Fatal("the approved tool's result never reached the model")
	}
	if resultRequest < requestsAtDecision {
		t.Fatalf("the approved tool's result reached the model as request %d, before the decision at request %d", resultRequest, requestsAtDecision)
	}

	// B2. The transcript is the ACP conversation, not a byte stream: it
	// carries the scripted tool call and the terminal result.
	tr := transcriptBytes(t, ctx, c, idle)
	// These are also the transcript scan's positive control: it is only worth
	// asserting that the upstream value is absent from a body of bytes the
	// same search has just been shown to read. The command appears
	// JSON-escaped, so the assertion uses the part no escape touches.
	for _, want := range []string{`"sessionUpdate":"tool_call"`, "GREETING.txt", "remount-planb-%s-ok", planBShellMark, `"stopReason":"end_turn"`} {
		if !bytes.Contains(tr, []byte(want)) {
			t.Fatalf("transcript lacks %q:\n%s", want, tr)
		}
	}
	// The terminal result arrives as streamed deltas, so it is asserted on the
	// reassembled message rather than on a contiguous run of bytes.
	if message := planBAgentMessage(tr); !strings.Contains(message, planBDone) {
		t.Fatalf("transcript agent message = %q, want the terminal result", message)
	}
	if bytes.Contains(tr, []byte(planBUpstream)) {
		t.Fatal("the transcript carries the synthetic upstream value")
	}
	// The control plane's mirror holds the same bytes; it is what a reader
	// sees once the workspace is asleep.
	if mirror := mirrorBytes(t, ctx, c, a.ID, uint64(len(tr))); !bytes.Equal(mirror, tr) {
		t.Fatalf("mirror differs from the node transcript: %d vs %d bytes", len(mirror), len(tr))
	}
	greeting, err := c.ReadFile(ctx, ws.ID, "GREETING.txt")
	if err != nil || string(bytes.TrimSpace(greeting)) != "hello" {
		t.Fatalf("GREETING.txt = %q err=%v", greeting, err)
	}
	planBAssertUpstreamReached(t, fake)

	// B4. Sleep releases the workspace and restores it from a checkpoint; the
	// follow-up must reload the prior conversation through session/load, not
	// start a fresh one.
	if _, err := c.SleepAgent(ctx, a.ID); err != nil {
		t.Fatal(err)
	}
	waitAgent(t, ctx, c, a.ID, "sleeping", func(a *proto.Agent) bool { return a.Status == proto.AgentSleeping })
	requestsBeforeResume := fake.Count()
	if _, err := c.MessageAgent(ctx, proto.AgentMessageReq{ID: a.ID, Text: "Confirm the file you created."}); err != nil {
		t.Fatal(err)
	}
	resumed := waitAgent(t, ctx, c, a.ID, "a second run after restore", func(a *proto.Agent) bool {
		return len(a.Inbox) == 0 && len(a.Runs) == 2 && a.Runs[1].State != proto.AgentRunPending &&
			(a.Status == proto.AgentIdle || a.Status == proto.AgentWaitingInput)
	})
	if !resumed.Runs[1].Loaded {
		t.Fatalf("the restored run did not replay history through session/load: %+v", resumed.Runs[1])
	}
	// The restored harness sent the earlier conversation upstream, so the
	// retention is the harness's own state and not a control-plane summary.
	carried := false
	for _, r := range fake.Requests()[requestsBeforeResume:] {
		if r.Model == planBModel && bytes.Contains([]byte(r.Body), []byte(planBShellMark)) &&
			bytes.Contains([]byte(r.Body), []byte("GREETING.txt")) {
			carried = true
		}
	}
	if !carried {
		t.Fatalf("no request after the restore carried the prior conversation (%d requests before, %d after)", requestsBeforeResume, fake.Count())
	}
	if fake.Saw("ref:b_openai") {
		t.Fatal("the workspace placeholder reached the upstream after the restore")
	}
}

// planBChunkText matches one streamed assistant text delta in the ACP
// transcript. The model streams in fixed-size pieces, so a marker is only
// visible once the deltas are reassembled.
var planBChunkText = regexp.MustCompile(`"sessionUpdate":"agent_message_chunk"[^\n]*?"text":"((?:[^"\\]|\\.)*)"`)

// planBAgentMessage reassembles the assistant text the transcript carries.
func planBAgentMessage(transcript []byte) string {
	var b strings.Builder
	for _, m := range planBChunkText.FindAllSubmatch(transcript, -1) {
		var text string
		if json.Unmarshal([]byte(`"`+string(m[1])+`"`), &text) == nil {
			b.WriteString(text)
		}
	}
	return b.String()
}

// planBEventSeq returns the sequence of the last event of typ on ws, or zero.
func planBEventSeq(t *testing.T, ctx context.Context, c *client.Client, ws, typ string) uint64 {
	t.Helper()
	evs, err := c.ReadEvents(ctx, 1, ws)
	if err != nil {
		t.Fatal(err)
	}
	var seq uint64
	for _, e := range evs {
		if e.Type == typ {
			seq = e.Seq
		}
	}
	return seq
}

// planBFirstRequestWith returns the index of the first recorded request whose
// body carries needle, or -1.
func planBFirstRequestWith(fake *modelfake.Server, needle string) int {
	for i, r := range fake.Requests() {
		if bytes.Contains([]byte(r.Body), []byte(needle)) {
			return i
		}
	}
	return -1
}
