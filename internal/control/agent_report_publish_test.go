package control

import (
	"context"
	"encoding/json"
	"path/filepath"
	"reflect"
	"sort"
	"testing"
	"time"

	"remount.dev/remount/internal/proto"
)

// The node report path is a state transition the control plane publishes
// twice: once into memory, once into SQLite. These tests pin the order. A
// report whose durable commit fails must leave memory exactly as it was, or
// the run's report watermark acknowledges the node's retry without the turn,
// approval or session ever reaching the database; the next restart then
// rebuilds an agent that never saw the report.

// agentReportState is everything a report can change, as one comparable
// value: the live row, the durable row, the approvals in both places, the
// volatile scheduling marks, and the audit log and transcript mirror sizes.
type agentReportState struct {
	agent            string // canonical JSON of the live agent
	durableAgent     string // canonical JSON of the agents row, "" when absent
	approvals        string // canonical JSON of the live approvals in id order
	durableApprovals string // canonical JSON of the approvals rows in id order
	dirtyApprovals   int
	delivered        []string // inbox ids with a delivery mark
	retryAt          bool     // agentRetry holds the agent
	events           []string // every event type in the log, in order
	transcripts      int
}

// canonicalJSON renders v the way the database would hand it back, so a
// live value and its decoded row compare equal when they carry the same
// state (nil and empty slices, unset pointers).
func canonicalJSON(t *testing.T, v any) string {
	t.Helper()
	switch x := v.(type) {
	case *proto.Agent:
		var out proto.Agent
		if err := proto.Unmarshal(proto.MustMarshal(x), &out); err != nil {
			t.Fatal(err)
		}
		if out.Inbox == nil {
			out.Inbox = []proto.AgentMessage{}
		}
		if out.Runs == nil {
			out.Runs = []proto.AgentRun{}
		}
		v = &out
	case []*proto.Approval:
		out := make([]*proto.Approval, 0, len(x))
		for _, ap := range x {
			var cp proto.Approval
			if err := proto.Unmarshal(proto.MustMarshal(ap), &cp); err != nil {
				t.Fatal(err)
			}
			out = append(out, &cp)
		}
		v = out
	}
	b, err := json.Marshal(v)
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}

func snapshotAgentReportState(t *testing.T, f *controlFixture, id string) agentReportState {
	t.Helper()
	var s agentReportState
	f.c.mu.Lock()
	if a := f.c.agents[id]; a != nil {
		s.agent = canonicalJSON(t, copyAgent(a))
	}
	var aps []*proto.Approval
	for _, ap := range f.c.approvals {
		if ap.Agent == id {
			aps = append(aps, copyApproval(ap))
		}
	}
	sort.Slice(aps, func(i, j int) bool { return aps[i].ID < aps[j].ID })
	s.approvals = canonicalJSON(t, aps)
	s.dirtyApprovals = len(f.c.dirtyApprovals)
	for m := range f.c.agentDelivered {
		s.delivered = append(s.delivered, m)
	}
	sort.Strings(s.delivered)
	_, s.retryAt = f.c.agentRetry[id]
	f.c.mu.Unlock()

	var raw []byte
	if err := f.c.db.QueryRow(`SELECT data FROM agents WHERE id=?`, id).Scan(&raw); err == nil {
		var a proto.Agent
		if err := proto.Unmarshal(raw, &a); err != nil {
			t.Fatal(err)
		}
		s.durableAgent = canonicalJSON(t, &a)
	}
	rows, err := f.c.db.Query(`SELECT data FROM approvals ORDER BY id`)
	if err != nil {
		t.Fatal(err)
	}
	var durable []*proto.Approval
	for rows.Next() {
		var b []byte
		if err := rows.Scan(&b); err != nil {
			t.Fatal(err)
		}
		var ap proto.Approval
		if err := proto.Unmarshal(b, &ap); err != nil {
			t.Fatal(err)
		}
		if ap.Agent == id {
			durable = append(durable, &ap)
		}
	}
	if err := rows.Close(); err != nil {
		t.Fatal(err)
	}
	s.durableApprovals = canonicalJSON(t, durable)
	if err := f.c.db.QueryRow(`SELECT count(*) FROM transcripts WHERE agent=?`, id).Scan(&s.transcripts); err != nil {
		t.Fatal(err)
	}
	all, err := f.log.Read(context.Background(), 1, "", 10000)
	if err != nil {
		t.Fatal(err)
	}
	s.events = make([]string, 0, len(all))
	for _, ev := range all {
		s.events = append(s.events, ev.Type)
	}
	return s
}

// failAgentWrites makes the next agents-row write fail inside its
// transaction, the way a full disk or a corrupt page would, and returns the
// function that lets writes through again.
func failAgentWrites(t *testing.T, f *controlFixture) func() {
	t.Helper()
	if _, err := f.c.db.Exec(`CREATE TRIGGER fail_agent_write BEFORE INSERT ON agents BEGIN SELECT RAISE(FAIL, 'injected durable write failure'); END`); err != nil {
		t.Fatal(err)
	}
	return func() {
		if _, err := f.c.db.Exec(`DROP TRIGGER IF EXISTS fail_agent_write`); err != nil {
			t.Fatal(err)
		}
	}
}

func toolCallApproval(id, kind string) *proto.Approval {
	return &proto.Approval{
		ID: id, Kind: kind, Title: "write file", ToolCall: "tc-" + id,
		Options: []proto.ApprovalOption{{ID: "once", Kind: "allow_once", Name: "Allow once"}, {ID: "no", Kind: "reject_once", Name: "Reject"}},
		Detail:  json.RawMessage(`{"path":"notes.txt"}`),
	}
}

func countEvents(t *testing.T, af *agentFixture, typ string) int {
	t.Helper()
	return len(eventsOfType(t, af.log, typ))
}

// reportPublishCase drives the fixture to the state a report kind needs and
// names the postcondition that only a committed report may produce.
type reportPublishCase struct {
	name  string
	prep  func(t *testing.T, af *agentFixture) (*proto.Agent, proto.AgentRunReq, proto.AgentReport)
	check func(t *testing.T, af *agentFixture, got *proto.Agent, run *proto.AgentRun)
}

func reportPublishCases() []reportPublishCase {
	started := func(t *testing.T, af *agentFixture) (*proto.Agent, proto.AgentRunReq, string) {
		t.Helper()
		a, run := af.start(t, localSubject(), "task")
		msg := af.agent(t, a.ID).Inbox[0].ID
		return a, run, msg
	}
	return []reportPublishCase{
		{
			name: "started",
			prep: func(t *testing.T, af *agentFixture) (*proto.Agent, proto.AgentRunReq, proto.AgentReport) {
				a := af.create(t, localSubject(), proto.AgentCreateReq{Name: "worker", Spec: agentSpec("task")})
				af.claim(t, a.WS)
				af.c.agentReconcile(context.Background())
				return a, af.lastRun(t), proto.AgentReport{Seq: 1, Kind: proto.AgentReportStarted, Transcript: "s_acp"}
			},
			check: func(t *testing.T, af *agentFixture, got *proto.Agent, run *proto.AgentRun) {
				if run.State != proto.AgentRunActive || run.Transcript != "s_acp" || got.TranscriptSession != "s_acp" || got.TranscriptNode != "n_one" {
					t.Fatalf("started: run=%+v agent transcript=%q/%q", run, got.TranscriptSession, got.TranscriptNode)
				}
				// One for the launch, one for the node's started report.
				if n := countEvents(t, af, proto.EvAgentRunStarted); n != 2 {
					t.Fatalf("agent.run_started events = %d", n)
				}
			},
		},
		{
			name: "session",
			prep: func(t *testing.T, af *agentFixture) (*proto.Agent, proto.AgentRunReq, proto.AgentReport) {
				a, run, _ := started(t, af)
				return a, run, proto.AgentReport{Seq: 2, Kind: proto.AgentReportSession, ACPSessionID: "acp-1", Loaded: true, Capabilities: &proto.ACPCapabilities{LoadSession: true}}
			},
			check: func(t *testing.T, af *agentFixture, got *proto.Agent, run *proto.AgentRun) {
				if got.ACPSessionID != "acp-1" || !run.Loaded || got.Capabilities == nil || !got.Capabilities.LoadSession {
					t.Fatalf("session: agent=%+v run=%+v", got, run)
				}
				if n := countEvents(t, af, proto.EvAgentSession); n != 1 {
					t.Fatalf("agent.session events = %d", n)
				}
			},
		},
		{
			name: "turn_started",
			prep: func(t *testing.T, af *agentFixture) (*proto.Agent, proto.AgentRunReq, proto.AgentReport) {
				a, run, msg := started(t, af)
				return a, run, proto.AgentReport{Seq: 2, Kind: proto.AgentReportTurnStarted, Message: msg}
			},
			check: func(t *testing.T, af *agentFixture, got *proto.Agent, run *proto.AgentRun) {
				if run.TurnMessage == "" || len(got.Inbox) != 1 || run.TurnMessage != got.Inbox[0].ID {
					t.Fatalf("turn_started: run=%+v inbox=%+v", run, got.Inbox)
				}
			},
		},
		{
			name: "turn_finished",
			prep: func(t *testing.T, af *agentFixture) (*proto.Agent, proto.AgentRunReq, proto.AgentReport) {
				a, run, msg := started(t, af)
				af.report(t, run, 2, proto.AgentReport{Kind: proto.AgentReportTurnStarted, Message: msg})
				return a, run, proto.AgentReport{Seq: 3, Kind: proto.AgentReportTurnFinished, Message: msg, StopReason: "end_turn", Usage: &proto.AgentUsage{Input: 10, Output: 20}}
			},
			check: func(t *testing.T, af *agentFixture, got *proto.Agent, run *proto.AgentRun) {
				if got.Turns != 1 || run.Turns != 1 || len(got.Inbox) != 0 || run.TurnMessage != "" || got.Status != proto.AgentWaitingInput {
					t.Fatalf("turn_finished: turns=%d/%d inbox=%d turn_message=%q status=%s", got.Turns, run.Turns, len(got.Inbox), run.TurnMessage, got.Status)
				}
				af.c.mu.Lock()
				delivered := len(af.c.agentDelivered)
				af.c.mu.Unlock()
				if delivered != 0 {
					t.Fatalf("delivery marks after the turn = %d, want none", delivered)
				}
				if n := countEvents(t, af, proto.EvAgentTurn); n != 1 {
					t.Fatalf("agent.turn events = %d", n)
				}
			},
		},
		{
			name: "turn_finished_max_turns",
			prep: func(t *testing.T, af *agentFixture) (*proto.Agent, proto.AgentRunReq, proto.AgentReport) {
				a := af.create(t, localSubject(), proto.AgentCreateReq{Name: "worker", Spec: agentSpec("task"), Policy: proto.AgentPolicy{MaxTurns: 1}})
				af.claim(t, a.WS)
				af.c.agentReconcile(context.Background())
				run := af.lastRun(t)
				af.report(t, run, 1, proto.AgentReport{Kind: proto.AgentReportStarted, Transcript: "s_acp"})
				msg := af.agent(t, a.ID).Inbox[0].ID
				af.report(t, run, 2, proto.AgentReport{Kind: proto.AgentReportTurnStarted, Message: msg})
				return a, run, proto.AgentReport{Seq: 3, Kind: proto.AgentReportTurnFinished, Message: msg, StopReason: "end_turn"}
			},
			check: func(t *testing.T, af *agentFixture, got *proto.Agent, run *proto.AgentRun) {
				if got.Status != proto.AgentFinished || len(got.Inbox) != 0 || got.Turns != 1 {
					t.Fatalf("max_turns: %+v", got)
				}
				if n := countEvents(t, af, proto.EvAgentFinished); n != 1 {
					t.Fatalf("agent.finished events = %d", n)
				}
				// The harness is only told to stop once the finish is durable.
				af.waitOps(proto.OpAgentRunCancel, 1)
				if n := af.opCount(proto.OpAgentRunCancel); n != 1 {
					t.Fatalf("agent.run.cancel sent %d times after the committed finish", n)
				}
			},
		},
		{
			name: "tool_call",
			prep: func(t *testing.T, af *agentFixture) (*proto.Agent, proto.AgentRunReq, proto.AgentReport) {
				a, run, _ := started(t, af)
				return a, run, proto.AgentReport{Seq: 2, Kind: proto.AgentReportToolCall, ToolCall: "tc1", ToolKind: "execute", ToolTitle: "ls", ToolStatus: "completed", Locations: []string{"/w"}}
			},
			check: func(t *testing.T, af *agentFixture, got *proto.Agent, run *proto.AgentRun) {
				if n := countEvents(t, af, proto.EvAgentToolCall); n != 1 {
					t.Fatalf("agent.tool_call events = %d", n)
				}
			},
		},
		{
			name: "permission",
			prep: func(t *testing.T, af *agentFixture) (*proto.Agent, proto.AgentRunReq, proto.AgentReport) {
				a, run, msg := started(t, af)
				af.report(t, run, 2, proto.AgentReport{Kind: proto.AgentReportTurnStarted, Message: msg})
				return a, run, proto.AgentReport{Seq: 3, Kind: proto.AgentReportPermission, Approval: toolCallApproval("ap_1", proto.ApprovalToolCall)}
			},
			check: checkParked("ap_1", proto.ApprovalToolCall),
		},
		{
			name: "elicitation",
			prep: func(t *testing.T, af *agentFixture) (*proto.Agent, proto.AgentRunReq, proto.AgentReport) {
				a, run, msg := started(t, af)
				af.report(t, run, 2, proto.AgentReport{Kind: proto.AgentReportTurnStarted, Message: msg})
				return a, run, proto.AgentReport{Seq: 3, Kind: proto.AgentReportElicitation, Approval: toolCallApproval("ap_2", proto.ApprovalElicitation)}
			},
			check: checkParked("ap_2", proto.ApprovalElicitation),
		},
		{
			name: "finished_expires_approvals",
			prep: func(t *testing.T, af *agentFixture) (*proto.Agent, proto.AgentRunReq, proto.AgentReport) {
				a, run, msg := started(t, af)
				af.report(t, run, 2, proto.AgentReport{Kind: proto.AgentReportTurnStarted, Message: msg})
				af.report(t, run, 3, proto.AgentReport{Kind: proto.AgentReportPermission, Approval: toolCallApproval("ap_1", proto.ApprovalToolCall)})
				return a, run, proto.AgentReport{Seq: 4, Kind: proto.AgentReportFinished, StopReason: "exit"}
			},
			check: func(t *testing.T, af *agentFixture, got *proto.Agent, run *proto.AgentRun) {
				if run.State != proto.AgentRunDone || run.StopReason != "exit" || got.PendingApprovals != 0 || len(got.Inbox) != 1 || got.Status != proto.AgentRunning {
					t.Fatalf("finished: run=%+v agent=%+v", run, got)
				}
				af.c.mu.Lock()
				ap := copyApproval(af.c.approvals["ap_1"])
				af.c.mu.Unlock()
				if ap.Status != proto.ApprovalExpired {
					t.Fatalf("approval after the run finished = %s, want expired", ap.Status)
				}
				if n := countEvents(t, af, proto.EvAgentRunFinished); n != 1 {
					t.Fatalf("agent.run_finished events = %d", n)
				}
				if n := countEvents(t, af, proto.EvApprovalExpired); n != 1 {
					t.Fatalf("approval.expired events = %d", n)
				}
			},
		},
		{
			name: "finished_error_schedules_retry",
			prep: func(t *testing.T, af *agentFixture) (*proto.Agent, proto.AgentRunReq, proto.AgentReport) {
				a, run, msg := started(t, af)
				af.report(t, run, 2, proto.AgentReport{Kind: proto.AgentReportTurnStarted, Message: msg})
				return a, run, proto.AgentReport{Seq: 3, Kind: proto.AgentReportFinished, ExitCode: 1}
			},
			check: func(t *testing.T, af *agentFixture, got *proto.Agent, run *proto.AgentRun) {
				if run.State != proto.AgentRunDone || run.Error != "harness exited 1" || got.Failures != 1 || len(got.Inbox) != 1 || got.Status != proto.AgentRunning {
					t.Fatalf("crash: run=%+v agent=%+v", run, got)
				}
				af.c.mu.Lock()
				_, backoff := af.c.agentRetry[got.ID]
				af.c.mu.Unlock()
				if !backoff {
					t.Fatal("a committed crash schedules the retry backoff")
				}
			},
		},
		{
			name: "finished_second_failure_fails_agent",
			prep: func(t *testing.T, af *agentFixture) (*proto.Agent, proto.AgentRunReq, proto.AgentReport) {
				a, run, msg := started(t, af)
				af.report(t, run, 2, proto.AgentReport{Kind: proto.AgentReportTurnStarted, Message: msg})
				af.report(t, run, 3, proto.AgentReport{Kind: proto.AgentReportFinished, ExitCode: 1})
				af.advance(agentRetryBackoff + time.Second)
				af.c.agentReconcile(context.Background())
				retry := af.lastRun(t)
				if retry.Run == run.Run {
					t.Fatal("retry run was not launched")
				}
				af.report(t, retry, 1, proto.AgentReport{Kind: proto.AgentReportStarted, Transcript: "s_acp_2"})
				return a, retry, proto.AgentReport{Seq: 2, Kind: proto.AgentReportFinished, Error: "session/load: no such session"}
			},
			check: func(t *testing.T, af *agentFixture, got *proto.Agent, run *proto.AgentRun) {
				if got.Status != proto.AgentFailed || got.Failures != 2 || len(got.Inbox) != 0 || run.State != proto.AgentRunDone {
					t.Fatalf("second failure: agent=%+v run=%+v", got, run)
				}
				af.c.mu.Lock()
				delivered := len(af.c.agentDelivered)
				af.c.mu.Unlock()
				if delivered != 0 {
					t.Fatalf("delivery marks after the agent failed = %d, want none", delivered)
				}
				if n := countEvents(t, af, proto.EvAgentFailed); n != 1 {
					t.Fatalf("agent.failed events = %d", n)
				}
			},
		},
		{
			name: "transcript",
			prep: func(t *testing.T, af *agentFixture) (*proto.Agent, proto.AgentRunReq, proto.AgentReport) {
				a, run, _ := started(t, af)
				return a, run, proto.AgentReport{Seq: 2, Kind: proto.AgentReportTranscript, Chunks: []proto.TranscriptChunk{
					{Seq: 0, Stream: 1, At: 1, Data: []byte("hello")},
					{Seq: 1, Stream: 1, At: 2, Data: []byte("world")},
				}}
			},
			check: func(t *testing.T, af *agentFixture, got *proto.Agent, run *proto.AgentRun) {
				if got.TranscriptNext != 2 || got.TranscriptBytes != 10 {
					t.Fatalf("transcript bounds = next %d bytes %d", got.TranscriptNext, got.TranscriptBytes)
				}
				var rows int
				if err := af.c.db.QueryRow(`SELECT count(*) FROM transcripts WHERE agent=?`, got.ID).Scan(&rows); err != nil || rows != 2 {
					t.Fatalf("transcript rows = %d, %v", rows, err)
				}
			},
		},
	}
}

func checkParked(id, kind string) func(*testing.T, *agentFixture, *proto.Agent, *proto.AgentRun) {
	return func(t *testing.T, af *agentFixture, got *proto.Agent, run *proto.AgentRun) {
		if got.PendingApprovals != 1 || got.Status != proto.AgentWaitingApproval {
			t.Fatalf("parked: pending=%d status=%s", got.PendingApprovals, got.Status)
		}
		af.c.mu.Lock()
		live := af.c.approvals[id]
		var ap *proto.Approval
		if live != nil {
			ap = copyApproval(live)
		}
		af.c.mu.Unlock()
		if ap == nil || ap.Status != proto.ApprovalPending || ap.Kind != kind || ap.Agent != got.ID || ap.Run != run.ID {
			t.Fatalf("approval %s after the committed report = %+v", id, ap)
		}
		if n := countEvents(t, af, proto.EvApprovalPending); n != 1 {
			t.Fatalf("approval.pending events = %d", n)
		}
	}
}

// TestAgentReportPublishesOnlyAfterDurableCommit is the forbidden result for
// every report kind: a report whose commit fails must change nothing the
// control plane can observe; the node's retry of the same sequence must then
// be applied, not deduplicated; memory and the database must agree; and a
// restart must rebuild the same agent and approvals from the rows alone.
func TestAgentReportPublishesOnlyAfterDurableCommit(t *testing.T) {
	for _, tc := range reportPublishCases() {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			path := filepath.Join(t.TempDir(), "control.db")
			af := newAgentFixture(t, path, func(o *Options) { o.MaxApprovalsPerAgent = 4 })
			a, run, rep := tc.prep(t, af)
			rep.Agent, rep.Run, rep.WS, rep.Gen = run.Agent, run.Run, run.WS, run.Gen

			before := snapshotAgentReportState(t, af.controlFixture, a.ID)
			allow := failAgentWrites(t, af.controlFixture)
			firstErr := af.c.agentReport(ctx, "n_one", &rep)
			allow()
			if firstErr == nil {
				t.Fatal("injected durable write failure was not reached")
			}
			after := snapshotAgentReportState(t, af.controlFixture, a.ID)
			if !reflect.DeepEqual(before, after) {
				t.Fatalf("a failed commit changed observable state\nbefore: %+v\nafter:  %+v", before, after)
			}
			if n := af.opCount(proto.OpAgentRunCancel); n != 0 {
				t.Fatalf("agent.run.cancel sent %d times for an uncommitted report", n)
			}

			if err := af.c.agentReport(ctx, "n_one", &rep); err != nil {
				t.Fatalf("retry of the same report: %v", err)
			}
			committed := snapshotAgentReportState(t, af.controlFixture, a.ID)
			if committed.agent != committed.durableAgent {
				t.Fatalf("after the retry memory and the agents row disagree\nmemory:  %s\ndurable: %s", committed.agent, committed.durableAgent)
			}
			if committed.approvals != committed.durableApprovals {
				t.Fatalf("after the retry memory and the approvals rows disagree\nmemory:  %s\ndurable: %s", committed.approvals, committed.durableApprovals)
			}
			if committed.dirtyApprovals != 0 {
				t.Fatalf("dirty approvals left after a committed report = %d", committed.dirtyApprovals)
			}
			if len(committed.events) <= len(before.events) && rep.Kind != proto.AgentReportTranscript && rep.Kind != proto.AgentReportTurnStarted {
				t.Fatalf("the committed report staged no events (%d before, %d after)", len(before.events), len(committed.events))
			}
			got := af.agent(t, a.ID)
			r := findRun(got, rep.Run)
			if r == nil || r.LastReport != rep.Seq {
				t.Fatalf("run watermark after the retry = %+v, want seq %d", r, rep.Seq)
			}
			tc.check(t, af, got, r)

			af.c.Stop()
			_ = af.log.Close()
			again := newControlFixture(t, path, func(o *Options) { o.PublicURL = "https://remount.example" })
			restarted := snapshotAgentReportState(t, again, a.ID)
			if restarted.agent != committed.agent {
				t.Fatalf("restart rebuilt a different agent\nbefore: %s\nafter:  %s", committed.agent, restarted.agent)
			}
			if restarted.approvals != committed.approvals {
				t.Fatalf("restart rebuilt different approvals\nbefore: %s\nafter:  %s", committed.approvals, restarted.approvals)
			}
			// The log is append-only: restart may add its own events after
			// the committed ones but never loses or reorders them.
			if len(restarted.events) < len(committed.events) || !reflect.DeepEqual(restarted.events[:len(committed.events)], committed.events) {
				t.Fatalf("restart changed the committed events\nbefore: %v\nafter:  %v", committed.events, restarted.events)
			}
			if restarted.transcripts != committed.transcripts {
				t.Fatalf("restart: transcript rows %d->%d", committed.transcripts, restarted.transcripts)
			}
		})
	}
}

// TestAgentReportRetryAfterCommitFailure is the handoff reproducer: a
// turn_finished whose commit fails, retried by the node. The retry has to
// land the turn durably rather than be acknowledged from a watermark the
// failed attempt advanced in memory.
func TestAgentReportRetryAfterCommitFailure(t *testing.T) {
	ctx := context.Background()
	af := newAgentFixture(t, "", nil)
	a, run := af.start(t, localSubject(), "task")
	message := af.agent(t, a.ID).Inbox[0].ID
	af.report(t, run, 2, proto.AgentReport{Kind: proto.AgentReportTurnStarted, Message: message})
	rep := &proto.AgentReport{Agent: a.ID, Run: run.Run, WS: run.WS, Gen: run.Gen, Seq: 3, Kind: proto.AgentReportTurnFinished, Message: message}
	allow := failAgentWrites(t, af.controlFixture)
	firstErr := af.c.agentReport(ctx, "n_one", rep)
	allow()
	if firstErr == nil {
		t.Fatal("injected commit failure was not reached")
	}
	if live := af.agent(t, a.ID); live.Turns != 0 || len(live.Inbox) != 1 || live.Runs[0].LastReport != 2 {
		t.Fatalf("failed commit published state: turns=%d inbox=%d report=%d", live.Turns, len(live.Inbox), live.Runs[0].LastReport)
	}
	if err := af.c.agentReport(ctx, "n_one", rep); err != nil {
		t.Fatalf("retry: %v", err)
	}
	var raw []byte
	if err := af.c.db.QueryRow(`SELECT data FROM agents WHERE id=?`, a.ID).Scan(&raw); err != nil {
		t.Fatal(err)
	}
	var persisted proto.Agent
	if err := proto.Unmarshal(raw, &persisted); err != nil {
		t.Fatal(err)
	}
	live := af.agent(t, a.ID)
	if persisted.Turns != 1 || len(persisted.Inbox) != 0 || persisted.Runs[0].LastReport != 3 {
		t.Fatalf("retry acknowledged uncommitted turn: memory turns=%d inbox=%d report=%d; durable turns=%d inbox=%d report=%d",
			live.Turns, len(live.Inbox), live.Runs[0].LastReport, persisted.Turns, len(persisted.Inbox), persisted.Runs[0].LastReport)
	}
	if n := len(eventsOfType(t, af.log, proto.EvAgentTurn)); n != 1 {
		t.Fatalf("agent.turn events = %d, want exactly one for the committed retry", n)
	}
}

// TestAgentReportRejectedReportLeavesWatermark: a report the control plane
// refuses on validation (a permission without an approval) is not applied,
// so the node's corrected report with the same sequence must still be
// accepted rather than deduplicated.
func TestAgentReportRejectedReportLeavesWatermark(t *testing.T) {
	ctx := context.Background()
	af := newAgentFixture(t, "", nil)
	a, run := af.start(t, localSubject(), "task")
	message := af.agent(t, a.ID).Inbox[0].ID
	af.report(t, run, 2, proto.AgentReport{Kind: proto.AgentReportTurnStarted, Message: message})
	bad := proto.AgentReport{Agent: a.ID, Run: run.Run, WS: run.WS, Gen: run.Gen, Seq: 3, Kind: proto.AgentReportPermission}
	if err := af.c.agentReport(ctx, "n_one", &bad); codeOf(err) != proto.CodeBadRequest {
		t.Fatalf("permission without approval = %v", err)
	}
	if live := af.agent(t, a.ID); live.Runs[0].LastReport != 2 {
		t.Fatalf("a rejected report advanced the watermark to %d", live.Runs[0].LastReport)
	}
	good := bad
	good.Approval = toolCallApproval("ap_1", proto.ApprovalToolCall)
	if err := af.c.agentReport(ctx, "n_one", &good); err != nil {
		t.Fatalf("corrected report with the same seq: %v", err)
	}
	if live := af.agent(t, a.ID); live.PendingApprovals != 1 || live.Runs[0].LastReport != 3 {
		t.Fatalf("corrected report not applied: %+v", live)
	}
}
