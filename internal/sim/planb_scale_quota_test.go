package sim

// Plan B B30 §15.4 item "quota rejection and recovery", proving the §15.5
// property: every rejected unit moves a counter and returns an explicit
// result. A rejection nobody counted is indistinguishable from a silent drop.
//
// Every assertion here is a delta around the burst, because metrics.Default is
// process-global. Each burst is also run twice so that a rejection path that
// retains state per refused request shows up as growth rather than as a
// harmless constant.

import (
	"fmt"
	"testing"
	"time"

	"remount.dev/remount/internal/client"
	"remount.dev/remount/internal/node"
	"remount.dev/remount/internal/proto"
	"remount.dev/remount/internal/server"
)

// TestPlanBScaleWorkspaceQuotaRejectionIsCountedAndRecovers drives the
// control-plane workspace ceiling past its limit twice and asserts that the
// refusals are counted exactly, are typed CodeResourceExhausted, name the
// limit in an event, and that freeing capacity restores admission.
func TestPlanBScaleWorkspaceQuotaRejectionIsCountedAndRecovers(t *testing.T) {
	const limit = 4
	rejects := planbScaleSize(t, 24, 6)

	w := newWorldWith(t, func(o *server.Options) {
		o.MaxWorkspacesPerTenant = limit
		o.MaxWorkspacesPerSubject = limit
	})
	w.node("n1", nil)
	c := w.client("c1")
	ctx := ctxT(t, 120*time.Second)

	admitted := make([]string, 0, limit)
	for i := 0; i < limit; i++ {
		ws, err := c.CreateWorkspace(ctx, proto.WorkspaceSpec{Name: fmt.Sprintf("quota-%d", i)})
		if err != nil {
			t.Fatalf("create %d below the limit: %v", i, err)
		}
		admitted = append(admitted, ws.ID)
	}

	// Two identical over-limit bursts. The first establishes the counter
	// delta; the second proves the refusal path is itself bounded.
	var firstFloor planbScaleSample
	for cycle := 1; cycle <= 2; cycle++ {
		before := planbScaleMetrics()
		beforeEvents, err := c.ReadEvents(ctx, 1, "")
		if err != nil {
			t.Fatal(err)
		}
		for i := 0; i < rejects; i++ {
			_, err := c.CreateWorkspace(ctx, proto.WorkspaceSpec{Name: fmt.Sprintf("over-%d-%d", cycle, i)})
			if err == nil {
				t.Fatalf("cycle %d create %d was admitted above the %d workspace limit", cycle, i, limit)
			}
			if got := codeOf(err); got != proto.CodeResourceExhausted {
				t.Fatalf("cycle %d create %d error code = %q, want %q (%v)", cycle, i, got, proto.CodeResourceExhausted, err)
			}
		}
		after := planbScaleMetrics()
		planbScaleCountedRejections(t, before, after, "remount_workspace_quota_rejections_total", rejects)

		// A counter alone leaves an operator with a number and no subject.
		// The rejection must also be an explicit, attributable record.
		afterEvents, err := c.ReadEvents(ctx, 1, "")
		if err != nil {
			t.Fatal(err)
		}
		quota := 0
		for _, event := range afterEvents[len(beforeEvents):] {
			if event.Type == proto.EvQuotaExceeded {
				quota++
			}
		}
		if quota != rejects {
			t.Errorf("cycle %d: %d %s events for %d rejections", cycle, quota, proto.EvQuotaExceeded, rejects)
		}

		floor := planbScaleFloor(5 * time.Second)
		t.Logf("cycle %d after %d rejections: %s", cycle, rejects, floor)
		if cycle == 1 {
			firstFloor = floor
			continue
		}
		if floor.Goroutines > firstFloor.Goroutines+8 {
			t.Errorf("goroutines grew across identical rejection bursts: %d then %d", firstFloor.Goroutines, floor.Goroutines)
		}
		if growth := planbScaleGrowth(firstFloor.HeapBytes, floor.HeapBytes); growth > 0.5 {
			t.Errorf("heap grew %.0f%% across identical rejection bursts: %s then %s",
				growth*100, planbScaleBytes(firstFloor.HeapBytes), planbScaleBytes(floor.HeapBytes))
		}
	}

	// Recovery: freeing one slot admits exactly one more workspace.
	if err := c.DestroyWorkspace(ctx, admitted[0]); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(20 * time.Second)
	var recovered *proto.Workspace
	for {
		ws, err := c.CreateWorkspace(ctx, proto.WorkspaceSpec{Name: "recovered"})
		if err == nil {
			recovered = ws
			break
		}
		if codeOf(err) != proto.CodeResourceExhausted {
			t.Fatalf("recovery create: %v", err)
		}
		if time.Now().After(deadline) {
			t.Fatal("quota never recovered after a workspace was destroyed")
		}
		time.Sleep(50 * time.Millisecond)
	}
	if recovered.ID == "" {
		t.Fatal("recovered workspace has no id")
	}
	if _, err := c.CreateWorkspace(ctx, proto.WorkspaceSpec{Name: "still-over"}); codeOf(err) != proto.CodeResourceExhausted {
		t.Fatalf("the ceiling did not close again after recovery: %v", err)
	}
}

// TestPlanBScaleSessionQuotaRejectionIsCountedAndRecovers is the same
// property at the node's session ceiling: refused opens are counted, typed,
// and leave no retained session behind, and an exit returns the slot.
func TestPlanBScaleSessionQuotaRejectionIsCountedAndRecovers(t *testing.T) {
	const limit = 3
	rejects := planbScaleSize(t, 24, 6)

	// The ceiling under test is the live-process one. MaxSessions bounds
	// *retained* logs, which an exit deliberately does not release, so it is
	// left wide: a recovery assertion against it would be asserting the wrong
	// contract.
	w := newWorld(t)
	w.nodeWith("n1", func(o *node.Options) {
		o.MaxSessions = 64
		o.MaxSessionsPerWorkspace = 64
		o.MaxSessionsPerPrincipal = 64
		o.MaxActiveSessions = limit
	})
	c := w.client("c1")
	ws := mustWS(t, c, proto.WorkspaceSpec{})
	ctx := ctxT(t, 120*time.Second)

	held := make([]*client.Session, 0, limit)
	for i := 0; i < limit; i++ {
		s, err := c.Exec(ctx, proto.SOpenReq{WS: ws.ID, Program: []string{"sh", "-c", "read line; echo done"}, Stdin: true})
		if err != nil {
			t.Fatalf("open %d below the limit: %v", i, err)
		}
		held = append(held, s)
	}

	before := planbScaleMetrics()
	for i := 0; i < rejects; i++ {
		if _, err := c.Exec(ctx, proto.SOpenReq{WS: ws.ID, Program: []string{"true"}}); err == nil {
			t.Fatalf("open %d was admitted above the %d session limit", i, limit)
		} else if got := codeOf(err); got != proto.CodeResourceExhausted {
			t.Fatalf("open %d error code = %q, want %q (%v)", i, got, proto.CodeResourceExhausted, err)
		}
	}
	after := planbScaleMetrics()
	planbScaleCountedRejections(t, before, after, "remount_session_quota_rejections_total", rejects)

	// A refused open must not have consumed a slot of its own: the node still
	// reports exactly the sessions that were actually admitted.
	statuses, err := c.ListSessions(ctx, ws.ID)
	if err != nil {
		t.Fatal(err)
	}
	if len(statuses) != limit {
		t.Fatalf("node retains %d sessions after %d refused opens, want %d", len(statuses), rejects, limit)
	}

	// Recovery is the capacity handoff: once a session exits, the slot is
	// reusable without polling for bookkeeping to catch up.
	if err := held[0].Input(ctx, []byte("go\n"), true); err != nil {
		t.Fatal(err)
	}
	if exit := client.Copy(held[0], nil, nil); exit == nil || exit.Code != 0 {
		t.Fatalf("held session exit = %+v", exit)
	}
	replacement, err := c.Exec(ctx, proto.SOpenReq{WS: ws.ID, Program: []string{"sh", "-c", "read line; echo done"}, Stdin: true})
	if err != nil {
		t.Fatalf("session quota did not recover after an exit: %v", err)
	}
	if _, err := c.Exec(ctx, proto.SOpenReq{WS: ws.ID, Program: []string{"true"}}); codeOf(err) != proto.CodeResourceExhausted {
		t.Fatalf("the session ceiling did not close again after recovery: %v", err)
	}
	if err := replacement.Input(ctx, []byte("go\n"), true); err != nil {
		t.Fatal(err)
	}
	client.Copy(replacement, nil, nil)
	for _, s := range held[1:] {
		_ = s.Input(ctx, []byte("go\n"), true)
		client.Copy(s, nil, nil)
	}
}
