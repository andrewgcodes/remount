package control

import (
	"context"
	"reflect"
	"testing"

	"remount.dev/remount/internal/proto"
)

type agentLifecycleState struct {
	agent            string
	durableAgent     string
	approvals        string
	durableApprovals string
	dirtyApprovals   int
	delivered        []string
	retryAt          bool
	counters         map[string]uint64
	events           map[string]int
}

func snapshotAgentLifecycleState(t *testing.T, af *agentFixture, id string) agentLifecycleState {
	t.Helper()
	s := snapshotAgentReportState(t, af.controlFixture, id)
	events := map[string]int{}
	for _, typ := range []string{
		proto.EvAgentSlept,
		proto.EvAgentDestroyed,
		proto.EvAgentRunFinished,
		proto.EvApprovalExpired,
	} {
		events[typ] = countEvents(t, af, typ)
	}
	return agentLifecycleState{
		agent: s.agent, durableAgent: s.durableAgent,
		approvals: s.approvals, durableApprovals: s.durableApprovals,
		dirtyApprovals: s.dirtyApprovals, delivered: s.delivered,
		retryAt: s.retryAt, counters: s.counters, events: events,
	}
}

func requireAgentLifecycleUnchanged(t *testing.T, before, after agentLifecycleState) {
	t.Helper()
	if !reflect.DeepEqual(before, after) {
		t.Fatalf("failed agent commit changed observable state\nbefore: %+v\nafter:  %+v", before, after)
	}
}

func agentWithPendingApproval(t *testing.T, af *agentFixture, adopted bool) (*proto.Agent, proto.AgentRunReq) {
	t.Helper()
	var a *proto.Agent
	var run proto.AgentRunReq
	if adopted {
		ws := createWorkspace(t, af.c, localSubject(), proto.WorkspaceSpec{Name: "adopted"})
		a = af.create(t, localSubject(), proto.AgentCreateReq{Name: "worker", Spec: agentSpec("task"), WS: ws.ID})
		af.claim(t, a.WS)
		af.c.agentReconcile(context.Background())
		run = af.lastRun(t)
		af.report(t, run, 1, proto.AgentReport{Kind: proto.AgentReportStarted, Transcript: "s_acp"})
	} else {
		a, run = af.start(t, localSubject(), "task")
	}
	af.report(t, run, 2, proto.AgentReport{Kind: proto.AgentReportTurnStarted, Message: run.Messages[0].ID})
	af.report(t, run, 3, proto.AgentReport{
		Kind:     proto.AgentReportPermission,
		Approval: toolCallApproval("ap_lifecycle", proto.ApprovalToolCall),
	})
	return a, run
}

func flushUnrelatedApprovals(t *testing.T, af *agentFixture) {
	t.Helper()
	af.c.mu.Lock()
	err := af.c.persistApprovalsOnly(nil)
	af.c.mu.Unlock()
	if err != nil {
		t.Fatal(err)
	}
}

func requirePendingApproval(t *testing.T, af *agentFixture) {
	t.Helper()
	ap, err := af.c.approvalCopy("ap_lifecycle")
	if err != nil || ap.Status != proto.ApprovalPending {
		t.Fatalf("live approval after unrelated flush = %+v, %v", ap, err)
	}
	if ap := durableApproval(t, af.controlFixture, "ap_lifecycle"); ap.Status != proto.ApprovalPending {
		t.Fatalf("durable approval after unrelated flush = %+v", ap)
	}
	if status := dirtyApprovalStatus(af.controlFixture, "ap_lifecycle"); status != "" {
		t.Fatalf("approval queued for an eventless later flush as %q", status)
	}
}

func TestAgentSleepPublishesApprovalsOnlyAfterCommit(t *testing.T) {
	af := newAgentFixture(t, "", nil)
	a, _ := agentWithPendingApproval(t, af, false)
	before := snapshotAgentLifecycleState(t, af, a.ID)
	allow := failAgentWrites(t, af.controlFixture)
	_, err := af.c.sleepAgent(context.Background(), localSubject().ID, a.ID, "test", "sleep-once")
	allow()
	if err == nil {
		t.Fatal("injected agent-row failure was not reached")
	}
	requireAgentLifecycleUnchanged(t, before, snapshotAgentLifecycleState(t, af, a.ID))
	flushUnrelatedApprovals(t, af)
	requirePendingApproval(t, af)

	if _, err := af.c.sleepAgent(context.Background(), localSubject().ID, a.ID, "test", "sleep-once"); err != nil {
		t.Fatalf("retry: %v", err)
	}
	if ap, _ := af.c.approvalCopy("ap_lifecycle"); ap.Status != proto.ApprovalExpired {
		t.Fatalf("approval after committed retry = %+v", ap)
	}
}

func TestAgentDestroyPublishesApprovalsOnlyAfterCommit(t *testing.T) {
	af := newAgentFixture(t, "", nil)
	a, _ := agentWithPendingApproval(t, af, true)
	req := &proto.AgentGetReq{ID: a.ID, IdempotencyKey: "destroy-once"}
	before := snapshotAgentLifecycleState(t, af, a.ID)
	allow := failAgentWrites(t, af.controlFixture)
	err := af.c.agentDestroy(context.Background(), localSubject(), req)
	allow()
	if err == nil {
		t.Fatal("injected agent-row failure was not reached")
	}
	requireAgentLifecycleUnchanged(t, before, snapshotAgentLifecycleState(t, af, a.ID))
	flushUnrelatedApprovals(t, af)
	requirePendingApproval(t, af)

	if err := af.c.agentDestroy(context.Background(), localSubject(), req); err != nil {
		t.Fatalf("retry: %v", err)
	}
	if ap, _ := af.c.approvalCopy("ap_lifecycle"); ap.Status != proto.ApprovalExpired {
		t.Fatalf("approval after committed retry = %+v", ap)
	}
}

func TestAgentReconcilePublishesApprovalsOnlyAfterCommit(t *testing.T) {
	af := newAgentFixture(t, "", nil)
	a, _ := agentWithPendingApproval(t, af, false)
	af.c.mu.Lock()
	af.c.workspaces[a.WS].Generation++
	af.c.mu.Unlock()
	before := snapshotAgentLifecycleState(t, af, a.ID)
	allow := failAgentWrites(t, af.controlFixture)
	if work := af.c.agentDecide(); len(work) != 0 {
		t.Fatalf("failed reconcile scheduled %d work items", len(work))
	}
	allow()
	requireAgentLifecycleUnchanged(t, before, snapshotAgentLifecycleState(t, af, a.ID))
	flushUnrelatedApprovals(t, af)
	requirePendingApproval(t, af)

	af.c.agentDecide()
	if ap, _ := af.c.approvalCopy("ap_lifecycle"); ap.Status != proto.ApprovalExpired {
		t.Fatalf("approval after committed retry = %+v", ap)
	}
}

func TestLaunchFailurePublishesApprovalsOnlyAfterCommit(t *testing.T) {
	af := newAgentFixture(t, "", nil)
	a, dispatch := agentWithPendingApproval(t, af, false)
	af.c.mu.Lock()
	run := findRun(af.c.agents[a.ID], dispatch.Run)
	run.State = proto.AgentRunPending
	if err := af.c.persistAgent(af.c.agents[a.ID]); err != nil {
		af.c.mu.Unlock()
		t.Fatal(err)
	}
	af.c.mu.Unlock()
	before := snapshotAgentLifecycleState(t, af, a.ID)
	af.fail.Store(true)
	allow := failAgentWrites(t, af.controlFixture)
	af.c.launchRun(context.Background(), agentDispatch{node: "n_one", req: dispatch})
	allow()
	requireAgentLifecycleUnchanged(t, before, snapshotAgentLifecycleState(t, af, a.ID))
	flushUnrelatedApprovals(t, af)
	requirePendingApproval(t, af)

	af.c.launchRun(context.Background(), agentDispatch{node: "n_one", req: dispatch})
	if ap, _ := af.c.approvalCopy("ap_lifecycle"); ap.Status != proto.ApprovalExpired {
		t.Fatalf("approval after committed retry = %+v", ap)
	}
}
