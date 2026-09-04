package conformance

import (
	"context"
	"fmt"
	"strings"
	"time"
)

// ---- agent inbox, transcript, wake and resume ----------------------------

// scheduledAgent creates an agent whose first run is held in the future.
// §6.1: while policy.start_at lies ahead the workspace is materialized, the
// inbox is held and nothing launches. That is exactly the state the inbox,
// transcript, sleep and wake requirements describe, and reaching it needs no
// harness binary, so these checks judge the protocol rather than the host's
// software inventory.
func (s *Session) scheduledAgent(ctx context.Context, name string) (Agent, error) {
	req := AgentCreateReq{
		Name:      name,
		Workspace: &WorkspaceSpec{Name: name, Requires: Requires{Backend: s.Target.Backend}},
		Spec:      AgentSpec{Recipe: "custom", Task: "conformance: this agent never launches a harness", ACPCommand: []string{"/bin/true"}},
		Policy:    AgentPolicy{StartAt: time.Now().Add(6 * time.Hour).UnixMilli()},
		Idem:      s.Idem("agent"),
	}
	var a Agent
	if err := s.Call(ctx, "agent.create", req, &a); err != nil {
		return a, err
	}
	s.TrackAgent(a.ID)
	if a.WS != "" {
		s.track(a.WS)
	}
	return a, nil
}

func checkAgentCreateShape(ctx context.Context, s *Session) error {
	a, err := s.scheduledAgent(ctx, "conformance-agent-shape")
	if err != nil {
		return err
	}
	switch {
	case a.ID == "":
		return failf("agent.create returned no id")
	case a.WS == "":
		return failf("agent %s names no workspace; §6.1 makes an agent a workspace plus a conversation plus a policy", a.ID)
	case a.Status == "":
		return failf("agent %s has no status; §6.1 derives one and never leaves it unset", a.ID)
	case a.CreatedAt == 0:
		return failf("agent %s has no creation time", a.ID)
	case a.Mode == "":
		return failf("agent %s declares no mode", a.ID)
	}
	var got Agent
	if err := s.Call(ctx, "agent.get", AgentGetReq{ID: a.ID}, &got); err != nil {
		return failf("an agent that was just created could not be read back: %v", err)
	}
	if got.ID != a.ID || got.WS != a.WS {
		return failf("agent.get returned %s/%s for the agent created as %s/%s", got.ID, got.WS, a.ID, a.WS)
	}
	return nil
}

func checkAgentInboxAppends(ctx context.Context, s *Session) error {
	a, err := s.scheduledAgent(ctx, "conformance-agent-inbox")
	if err != nil {
		return err
	}
	const text = "conformance follow-up"
	var res AgentMessageRes
	if err := s.Call(ctx, "agent.message", AgentMessageReq{ID: a.ID, Text: text, Idem: s.Idem("msg")}, &res); err != nil {
		return err
	}
	if res.Message.ID == "" {
		return failf("agent.message returned no message id")
	}
	if res.Message.Text != text {
		return failf("agent.message echoed %q for a message sent as %q", res.Message.Text, text)
	}
	var got Agent
	if err := s.Call(ctx, "agent.get", AgentGetReq{ID: a.ID}, &got); err != nil {
		return err
	}
	for _, m := range got.Inbox {
		if m.ID == res.Message.ID && m.Text == text {
			return nil
		}
	}
	return failf("the message agent.message accepted is not in %s's inbox; §6.1 keeps it there until a turn consumes it", a.ID)
}

func checkAgentInboxBounded(ctx context.Context, s *Session) error {
	a, err := s.scheduledAgent(ctx, "conformance-agent-bound")
	if err != nil {
		return err
	}
	// The bound is an implementation choice; that there is one is not. A
	// generous ceiling keeps this from becoming a test of a specific number.
	const ceiling = 512
	for i := range ceiling {
		err := s.Call(ctx, "agent.message", AgentMessageReq{
			ID: a.ID, Text: fmt.Sprintf("conformance bound probe %d", i), Idem: s.Idem("bound"),
		}, nil)
		if err == nil {
			continue
		}
		if code := CodeOf(err); code != "resource_exhausted" {
			return failf("the %dth message was refused with %q, not %q (%v)", i+1, code, "resource_exhausted", err)
		}
		return nil
	}
	return failf("%d messages were accepted into one inbox without a refusal; §6.1 bounds the inbox and rejects the excess", ceiling)
}

func checkAgentTranscriptPage(ctx context.Context, s *Session) error {
	a, err := s.scheduledAgent(ctx, "conformance-agent-transcript")
	if err != nil {
		return err
	}
	var page AgentTranscriptRes
	if err := s.Call(ctx, "agent.transcript", AgentTranscriptReq{ID: a.ID, From: 0, Limit: 100}, &page); err != nil {
		return failf("agent.transcript on an agent with no records failed rather than returning an empty page: %v", err)
	}
	if page.Gap != nil {
		return failf("an empty transcript reported an evicted range %+v", *page.Gap)
	}
	for i, r := range page.Records {
		if r.Index != page.Records[0].Index+uint64(i) {
			return failf("transcript record %d carries index %d, breaking the contiguous agent-wide index §6.2 requires", i, r.Index)
		}
	}
	if uint64(len(page.Records)) > page.Next {
		return failf("agent.transcript returned %d records but a continuation cursor of %d", len(page.Records), page.Next)
	}
	// Reading the mirror must not wake the workspace.
	var ws Workspace
	if err := s.Call(ctx, "ws.get", WSGetReq{ID: a.WS}, &ws); err != nil {
		return err
	}
	if !containsString(WorkspaceStates, ws.State) {
		return failf("the agent's workspace reports state %q after a transcript read", ws.State)
	}
	return nil
}

// checkAgentTranscriptGap needs a mirror that has evicted records, which
// needs a harness producing tens of megabytes of transcript.
func checkAgentTranscriptGap(_ context.Context, s *Session) error {
	return Unavailablef("target %q declares transcript eviction reachable, but the runner launches no harness and therefore produces no transcript records to evict", s.Target.Name)
}

func checkAgentSleepAndWake(ctx context.Context, s *Session) error {
	a, err := s.scheduledAgent(ctx, "conformance-agent-sleep")
	if err != nil {
		return err
	}
	if _, err := s.WaitClaimed(ctx, a.WS); err != nil {
		return err
	}
	var slept Agent
	if err := s.Call(ctx, "agent.sleep", AgentGetReq{ID: a.ID, Idem: s.Idem("sleep")}, &slept); err != nil {
		return err
	}
	if err := s.waitState(ctx, a.WS, WSPaused); err != nil {
		return failf("agent.sleep did not pause the workspace: %v", err)
	}
	var res AgentMessageRes
	if err := s.Call(ctx, "agent.message", AgentMessageReq{ID: a.ID, Text: "conformance wake", Idem: s.Idem("wake")}, &res); err != nil {
		return err
	}
	if !res.Woken {
		return failf("a message to a sleeping agent did not report woken:true; §6.1 says it wakes the workspace and says so")
	}
	if _, err := s.WaitClaimed(ctx, a.WS); err != nil {
		return failf("the woken agent's workspace never returned to claimed: %v", err)
	}
	// Resume: the conversation is still there afterwards.
	var got Agent
	if err := s.Call(ctx, "agent.get", AgentGetReq{ID: a.ID}, &got); err != nil {
		return err
	}
	found := false
	for _, m := range got.Inbox {
		if m.ID == res.Message.ID {
			found = true
		}
	}
	if !found {
		return failf("the message that woke %s is not in its inbox after the wake; the conversation did not survive the sleep", a.ID)
	}
	return nil
}

func checkAgentUnknownIsNotFound(ctx context.Context, s *Session) error {
	err := s.Call(ctx, "agent.get", AgentGetReq{ID: "a_conformance_absent"}, nil)
	if err == nil {
		return failf("agent.get of an absent id succeeded")
	}
	if code := CodeOf(err); code != "not_found" {
		return failf("agent.get of an absent id answered %q, not %q (%v)", code, "not_found", err)
	}
	return nil
}

func checkAgentDestroyIsTerminal(ctx context.Context, s *Session) error {
	a, err := s.scheduledAgent(ctx, "conformance-agent-destroy")
	if err != nil {
		return err
	}
	// Destroy a settled agent. Whether a destroy that races materialization
	// is safe is a different obligation from whether a destroyed agent stays
	// destroyed, and conflating them would report one failure as the other.
	if _, err := s.WaitClaimed(ctx, a.WS); err != nil {
		return err
	}
	if err := s.Call(ctx, "agent.destroy", AgentGetReq{ID: a.ID, Idem: s.Idem("destroy")}, nil); err != nil {
		return err
	}
	var got Agent
	err = s.Call(ctx, "agent.get", AgentGetReq{ID: a.ID}, &got)
	switch {
	case IsCode(err, "not_found"):
	case err != nil:
		return failf("agent.get after destroy answered %q (%v)", CodeOf(err), err)
	case got.Status != "destroyed":
		return failf("agent.get after destroy reports status %q; §6.1 says a terminal agent is never resurrected", got.Status)
	}
	if err := s.Call(ctx, "agent.message", AgentMessageReq{ID: a.ID, Text: "resurrect", Idem: s.Idem("resurrect")}, nil); err == nil {
		return failf("a message to a destroyed agent was accepted")
	}
	return nil
}

// ---- canonical event ordering and export cursor behaviour ----------------

func checkEvtSeqIncreasing(ctx context.Context, s *Session) error {
	events, err := s.Tail(ctx, 1, "")
	if err != nil {
		return err
	}
	if len(events) == 0 {
		return failf("the canonical log is empty; a control plane that has served this run has emitted events")
	}
	for i := 1; i < len(events); i++ {
		if events[i].Seq <= events[i-1].Seq {
			return failf("event %d carries seq %d after seq %d; §11 makes seq the only total order", i, events[i].Seq, events[i-1].Seq)
		}
	}
	return nil
}

func checkEvtSeqDense(ctx context.Context, s *Session) error {
	events, err := s.Tail(ctx, 1, "")
	if err != nil {
		return err
	}
	if len(events) == 0 {
		return failf("the canonical log is empty")
	}
	for i := 1; i < len(events); i++ {
		if events[i].Seq != events[i-1].Seq+1 {
			return failf("the log jumps from seq %d to %d with no eviction reported; §11.1 deletes only a contiguous oldest prefix", events[i-1].Seq, events[i].Seq)
		}
	}
	return nil
}

func checkEvtStreamFilter(ctx context.Context, s *Session) error {
	f, err := s.NewFixture(ctx, WorkspaceSpec{Name: "conformance-stream"})
	if err != nil {
		return err
	}
	events, err := s.Tail(ctx, 1, f.WS.ID)
	if err != nil {
		return err
	}
	if len(events) == 0 {
		return failf("creating and claiming %s produced no events on its own stream", f.WS.ID)
	}
	for _, e := range events {
		if e.Stream != f.WS.ID && e.Workspace != f.WS.ID {
			return failf("a tail filtered to %s returned event %d on stream %q for workspace %q", f.WS.ID, e.Seq, e.Stream, e.Workspace)
		}
	}
	return nil
}

func checkEvtLifecycleOrder(ctx context.Context, s *Session) error {
	f, err := s.NewFixture(ctx, WorkspaceSpec{Name: "conformance-lifecycle"})
	if err != nil {
		return err
	}
	events, err := s.Tail(ctx, 1, f.WS.ID)
	if err != nil {
		return err
	}
	want := []string{"ws.created", "ws.claiming", "ws.claimed"}
	i := 0
	var order []string
	for _, e := range events {
		order = append(order, e.Type)
		if i < len(want) && e.Type == want[i] {
			i++
		}
	}
	if i != len(want) {
		return failf("the lifecycle of %s emitted %v; §5 and §11 require %v in that order", f.WS.ID, order, want)
	}
	return nil
}

func checkEvtCanonicalTypes(ctx context.Context, s *Session) error {
	f, err := s.NewFixture(ctx, WorkspaceSpec{Name: "conformance-types"})
	if err != nil {
		return err
	}
	if err := s.seed(ctx, f, "types"); err != nil {
		return err
	}
	events, err := s.Tail(ctx, 1, f.WS.ID)
	if err != nil {
		return err
	}
	if len(events) == 0 {
		return failf("a workspace that was created, claimed and written to produced no events")
	}
	var unknown []string
	for _, e := range events {
		if containsString(CanonicalEventTypes, e.Type) {
			continue
		}
		// §11 permits namespaced extensions; a bare name that is not
		// canonical is drift in the baseline vocabulary.
		if strings.HasPrefix(e.Type, "webhook.") || strings.HasPrefix(e.Type, "x-") {
			continue
		}
		unknown = append(unknown, e.Type)
	}
	if len(unknown) > 0 {
		return failf("workspace %s emitted non-canonical, non-namespaced event types %v; a reader of another implementation's log would not recognise them", f.WS.ID, unknown)
	}
	return nil
}

func checkEvtTailBeyondHead(ctx context.Context, s *Session) error {
	head, err := s.headSequence(ctx)
	if err != nil {
		return err
	}
	events, err := s.Tail(ctx, head+1_000_000, "")
	if err != nil {
		return failf("a tail from a sequence beyond the head failed with %v rather than returning nothing", err)
	}
	if len(events) != 0 {
		return failf("a tail from seq %d, past the head at %d, returned %d events", head+1_000_000, head, len(events))
	}
	return nil
}

func checkEvtOutOfBandAppend(ctx context.Context, s *Session) error {
	head, err := s.headSequence(ctx)
	if err != nil {
		return err
	}
	status, body, err := s.PostEvent(ctx, `{"type":"conformance.probe","payload":{"marker":"conformance"}}`)
	if err != nil {
		return err
	}
	if status != 202 {
		return failf("an out-of-band append answered %d %s, not 202", status, strings.TrimSpace(string(body)))
	}
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		events, err := s.Tail(ctx, head+1, "")
		if err != nil {
			return err
		}
		for _, e := range events {
			if strings.Contains(e.Type, "conformance.probe") {
				if e.Seq <= head {
					return failf("the appended event was assigned seq %d, at or below the head it was appended after (%d)", e.Seq, head)
				}
				return nil
			}
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(200 * time.Millisecond):
		}
	}
	return failf("an event appended out of band never became visible in the canonical order")
}

func checkEvtUnknownFieldRefused(ctx context.Context, s *Session) error {
	status, body, err := s.PostEvent(ctx, `{"type":"conformance.probe","conformance_unknown_field":true}`)
	if err != nil {
		return err
	}
	if status == 202 {
		return failf("an out-of-band append carrying an unknown field was accepted; §11 answers 400 rather than half-applying it")
	}
	if status != 400 {
		return failf("an out-of-band append carrying an unknown field answered %d %s, not 400", status, strings.TrimSpace(string(body)))
	}
	return nil
}

// checkEvtExportCursor asserts an extension: §11 defines outbound
// notification as operator configuration rather than a protocol request, so
// an implementation that ships none still conforms.
func checkEvtExportCursor(_ context.Context, s *Session) error {
	return Unavailablef("target %q declares a notification destination, but the runner configures none and will not assert a cursor it cannot see advance", s.Target.Name)
}

func checkEvtEvictedNamesOldest(_ context.Context, s *Session) error {
	return Unavailablef("target %q declares its retention watermark reachable, but the runner has no supported way to drive its log past it", s.Target.Name)
}
