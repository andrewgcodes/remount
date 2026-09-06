package sim

import (
	"strings"
	"testing"
	"time"

	"remount.dev/remount/internal/proto"
)

// TestLiveSessionReceivesLifecycleExitChunk is the half of ADR 0090's exit
// story that a reattached replay cannot prove.
//
// TestLongCommandAutoSleptReplaysExplicitExitReason shows that the durable log
// records the reason and that a client which comes back later reads it. This
// test is about the client that never went away: it opened `sleep 300`, is
// still streaming when the deadline fires, and must be told the session ended.
//
// It was written against a real failure. Running the long-running-autosleep
// example against a standalone server, the workspace reached `paused` and the
// node emitted `s.exited{reason: lifecycle_deadline_expired}`, but the example
// hung forever in client.Copy: releasePrepare cancelled every subscriber for
// the workspace before terminating its sessions, so the exit chunk the
// graceful stop produced had no subscriber left to receive. A stream that goes
// silent and never closes is worse than either an exit or an explicit gap,
// because the caller cannot tell it from work still in progress.
func TestLiveSessionReceivesLifecycleExitChunk(t *testing.T) {
	w := newWorld(t)
	w.node("n1", nil)
	c := w.client("c1")
	ws := mustWS(t, c, proto.WorkspaceSpec{})
	ctx := ctxT(t, 120*time.Second)

	live, err := c.Exec(ctx, proto.SOpenReq{WS: ws.ID, Program: []string{"sh", "-c", "echo started; sleep 300"}})
	if err != nil {
		t.Fatal(err)
	}
	select {
	case chunk, ok := <-live.Chunks():
		if !ok || !strings.Contains(string(chunk.Data), "started") {
			t.Fatalf("first chunk %q ok=%v", chunk.Data, ok)
		}
	case <-time.After(30 * time.Second):
		t.Fatal("session produced no output")
	}

	if _, err := c.LeaseWorkspace(ctx, proto.WSLeaseReq{ID: ws.ID, MaxAliveSec: 2, Reason: "long_command"}); err != nil {
		t.Fatal(err)
	}

	// Drain the still-attached stream. The channel closes when delivery ends,
	// with or without an exit; the bound is what turns "hung forever" into a
	// failure instead of a timeout panic in an unrelated test.
	drained := make(chan struct{})
	go func() {
		defer close(drained)
		for range live.Chunks() {
		}
	}()
	select {
	case <-drained:
	case <-time.After(60 * time.Second):
		t.Fatalf("the attached session stream never ended after the lifecycle deadline fired (workspace %s)", ws.ID)
	}

	exit := live.Exit()
	if exit == nil {
		t.Fatalf("attached session ended with no exit chunk: %v", live.Err())
	}
	if exit.Reason != proto.ReasonLifecycleDeadlineExpired {
		t.Fatalf("attached session exit reason %q, want %q (exit %+v)", exit.Reason, proto.ReasonLifecycleDeadlineExpired, exit)
	}
	waitWorkspaceState(t, c, ws.ID, proto.WSPaused, 45*time.Second)
}
