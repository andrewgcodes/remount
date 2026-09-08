package sim

// Plan B B18: a queue resumes on another node without repeating a completed
// item.
//
// TestQueueRunsTasksAcrossSleepAndMove already continues a queue on a second
// node, but it does so from a clean stop: the workspace is asleep and every
// task has a recorded outcome. This scenario is the one the failure matrix
// asks for. One item is durably complete, the next is still running when its
// node dies, and its outcome is therefore unknown. The completed item must
// never run again, and the unknown one must never be recorded as anything —
// it goes back to being due, which is the only truthful answer.

import (
	"bytes"
	"context"
	"strings"
	"testing"
	"time"

	"remount.dev/remount/internal/client"
	"remount.dev/remount/internal/proto"
)

func TestPlanBContinuityQueueResumesWithoutRepeatingACompletedItem(t *testing.T) {
	w := newWorldExpiring(t)
	names := map[string]string{}
	for _, name := range []string{"b18-n1", "b18-n2"} {
		names[w.node(name, map[string]string{"zone": "b18"}).ID()] = name
	}
	c := w.client("b18-c1")
	ctx := ctxT(t, 6*time.Minute)
	placement := proto.Placement{Allow: map[string]string{"zone": "b18"}}
	created, err := c.CreateWorkspace(ctx, proto.WorkspaceSpec{Name: "b18", Placement: placement})
	if err != nil {
		t.Fatal(err)
	}
	before, err := c.WaitClaimed(ctx, created.ID)
	if err != nil {
		t.Fatal(err)
	}
	holder := names[before.Node]
	if holder == "" {
		t.Fatalf("unknown holder in %s", continuityRow(before))
	}

	queue, err := c.CreateQueue(ctx, proto.QueueCreateReq{
		WS: before.ID, Tasks: []string{"alpha", "bravo", "charlie"},
	}, client.WithIdempotencyKey("b18-queue"))
	if err != nil {
		t.Fatal(err)
	}
	if queue.Cursor != 0 || queue.Status != proto.QueueRunning || len(queue.Items) != 3 {
		t.Fatalf("new queue = %+v", queue)
	}

	// Item 0 runs to a known outcome and its result is committed with the
	// cursor in one transaction.
	session := planBRunQueueTask(t, ctx, c, before.ID, "alpha")
	queue, err = c.AdvanceQueue(ctx, proto.QueueAdvanceReq{ID: queue.ID, Index: 0, Session: session, Exit: 0},
		client.WithIdempotencyKey("b18-advance-0"))
	if err != nil {
		t.Fatal(err)
	}
	if queue.Cursor != 1 || queue.Status != proto.QueueRunning || queue.Items[0].Attempts != 1 || queue.Items[0].Session != session {
		t.Fatalf("queue after item 0 = %+v", queue)
	}

	// Exactly once, from both directions. A retry of the same advance replays
	// its recorded result, and a second advance of the same index under a new
	// key is a conflict that names where the cursor actually is.
	replayed, err := c.AdvanceQueue(ctx, proto.QueueAdvanceReq{ID: queue.ID, Index: 0, Session: session, Exit: 0},
		client.WithIdempotencyKey("b18-advance-0"))
	if err != nil {
		t.Fatalf("replaying a recorded advance: %v", err)
	}
	if replayed.Cursor != 1 || replayed.Items[0].Attempts != 1 {
		t.Fatalf("replayed advance double-counted the attempt: %+v", replayed)
	}
	_, err = c.AdvanceQueue(ctx, proto.QueueAdvanceReq{ID: queue.ID, Index: 0, Session: session, Exit: 0},
		client.WithIdempotencyKey("b18-advance-0-again"))
	if code := continuityCode(err); code != proto.CodeConflict {
		t.Fatalf("re-recording a completed item returned %v (code %q), want conflict", err, code)
	}

	// The commit point for the completed item's effect on the filesystem.
	checkpoint, err := c.Checkpoint(ctx, before.ID, client.WithIdempotencyKey("b18-commit"))
	if err != nil {
		t.Fatal(err)
	}
	if !checkpoint.Authoritative || checkpoint.Artifact == "" {
		t.Fatalf("checkpoint = %+v", checkpoint)
	}
	committed, err := c.GetWorkspace(ctx, before.ID)
	if err != nil {
		t.Fatal(err)
	}
	if committed.LastSnapshot != checkpoint.Artifact {
		t.Fatalf("checkpoint %s is not authoritative: %s", checkpoint.Artifact, continuityRow(committed))
	}

	// Item 1 starts and its node dies while it runs. Its outcome is unknown:
	// it may have written anything and it may have been about to succeed.
	running, err := c.Exec(ctx, proto.SOpenReq{
		WS: before.ID, Kind: proto.SessionExec, IdempotencyKey: "b18-bravo-attempt-1",
		// The task stays resident in the shell rather than replacing itself
		// with a sleep: the node has to tear a live managed session down when
		// it dies, and a shell that keeps its own pid does that predictably.
		Program: []string{"sh", "-c", "echo bravo-partial >> ran.txt; echo started; while [ ! -f never.txt ]; do sleep 1; done"},
	})
	if err != nil {
		t.Fatal(err)
	}
	var startup bytes.Buffer
	for ch := range running.Chunks() {
		if ch.Stream == proto.StreamStdout {
			startup.Write(ch.Data)
		}
		if strings.Contains(startup.String(), "started\n") {
			break
		}
	}
	if !strings.Contains(startup.String(), "started\n") {
		t.Fatalf("item 1 never started: %q", startup.String())
	}
	w.stopNode(holder)

	// The unknown outcome stays unknown. Nothing about item 1 is recorded,
	// and the cursor still points at it.
	interrupted, err := c.GetQueue(ctx, queue.ID)
	if err != nil {
		t.Fatal(err)
	}
	if interrupted.Cursor != 1 || interrupted.Status != proto.QueueRunning {
		t.Fatalf("queue after the node loss = %+v", interrupted)
	}
	item := interrupted.Items[1]
	if item.Attempts != 0 || item.FinishedAt != 0 || item.Session != "" || item.Exit != 0 || item.Signal != "" {
		t.Fatalf("an interrupted item was given an outcome: %+v", item)
	}
	if got := planBQueueAdvancedIndexes(t, ctx, c, before.ID, queue.ID); len(got) != 1 || got[0] != 0 {
		t.Fatalf("queue.advanced indexes = %v, want only the one committed item", got)
	}

	// The workspace comes back on the other node from the committed
	// checkpoint, so the completed item's effect is there exactly once and
	// the interrupted item's partial write is not.
	resumed := awaitWorkspace(t, ctx, c, before.ID, "claimed by a replacement node", 2*time.Minute,
		func(ws *proto.Workspace) bool { return ws.State == proto.WSClaimed && ws.Node != committed.Node })
	if resumed.Generation != committed.Generation+2 || resumed.LastSnapshot != checkpoint.Artifact {
		t.Fatalf("resumed on %s, want generation %d restored from %s", continuityRow(resumed), committed.Generation+2, checkpoint.Artifact)
	}
	log, err := c.ReadFile(ctx, before.ID, "ran.txt")
	if err != nil || string(log) != "alpha\n" {
		t.Fatalf("restored log = %q (%v); want only the committed item's effect", log, err)
	}

	// A driver picking the queue up continues from the cursor the control
	// plane holds, which is the only place it is written down.
	if _, err := c.ReadFile(ctx, before.ID, ".remount/queue"); err == nil {
		t.Fatal("queue state was written into the workspace tree")
	}
	for index := interrupted.Cursor; index < len(interrupted.Items); index++ {
		task := interrupted.Items[index].Task
		session := planBRunQueueTask(t, ctx, c, before.ID, task)
		queue, err = c.AdvanceQueue(ctx, proto.QueueAdvanceReq{ID: queue.ID, Index: index, Session: session, Exit: 0},
			client.WithIdempotencyKey("b18-resume-"+task))
		if err != nil {
			t.Fatalf("advance item %d (%s): %v", index, task, err)
		}
	}
	if queue.Status != proto.QueueDone || queue.Cursor != len(queue.Items) {
		t.Fatalf("resumed queue = %+v", queue)
	}
	if queue.Items[0].Attempts != 1 {
		t.Fatalf("the completed item ran again: %+v", queue.Items[0])
	}
	if queue.Items[1].Attempts != 1 {
		t.Fatalf("the interrupted item recorded %d attempts, want the one that actually finished: %+v",
			queue.Items[1].Attempts, queue.Items[1])
	}
	log, err = c.ReadFile(ctx, before.ID, "ran.txt")
	if err != nil || string(log) != "alpha\nbravo\ncharlie\n" {
		t.Fatalf("final log = %q (%v)", log, err)
	}
	if got := planBQueueAdvancedIndexes(t, ctx, c, before.ID, queue.ID); len(got) != 3 ||
		got[0] != 0 || got[1] != 1 || got[2] != 2 {
		t.Fatalf("queue.advanced indexes = %v, want each item recorded once in order", got)
	}
}

// planBRunQueueTask runs one queued task in the workspace and returns the
// session id its outcome is attributed to.
func planBRunQueueTask(t *testing.T, ctx context.Context, c *client.Client, ws, task string) string {
	t.Helper()
	s, err := c.Exec(ctx, proto.SOpenReq{
		WS: ws, Kind: proto.SessionExec,
		Program: []string{"sh", "-c", "echo \"$1\" >> ran.txt", "queued", task},
	})
	if err != nil {
		t.Fatalf("run %q: %v", task, err)
	}
	exit := client.Copy(s, nil, nil)
	if err := s.Err(); err != nil {
		t.Fatalf("run %q: %v", task, err)
	}
	if exit == nil || exit.Code != 0 {
		t.Fatalf("run %q exit = %+v", task, exit)
	}
	return s.ID
}

// planBQueueAdvancedIndexes returns the item indexes the event log records as
// advanced, in order. The event log is the independent witness that a
// completed item was recorded once and an interrupted one not at all.
func planBQueueAdvancedIndexes(t *testing.T, ctx context.Context, c *client.Client, ws, queue string) []int {
	t.Helper()
	evs, err := c.ReadEvents(ctx, 0, ws)
	if err != nil {
		t.Fatal(err)
	}
	var out []int
	for _, e := range evs {
		if e.Type != proto.EvQueueAdvanced {
			continue
		}
		var payload struct {
			Queue string `cbor:"queue"`
			Index int    `cbor:"index"`
		}
		if err := proto.Unmarshal(e.Payload, &payload); err != nil {
			t.Fatalf("decode %s at seq %d: %v", e.Type, e.Seq, err)
		}
		if payload.Queue == queue {
			out = append(out, payload.Index)
		}
	}
	return out
}
