package control

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"remount.dev/remount/internal/proto"
)

// TestQueueIsDurableOrderedAndIdempotent: a queue survives a control-plane
// restart with its cursor, refuses out-of-order and duplicate advances, stops
// on a failing task so the cursor can be retried, and commits queue.advanced
// in the same transaction as the cursor.
func TestQueueIsDurableOrderedAndIdempotent(t *testing.T) {
	path := filepath.Join(t.TempDir(), "queues.db")
	f := newControlFixture(t, path, nil)
	ctx := context.Background()
	alice := Subject{ID: "alice", Tenant: "tenant-a"}
	ws := createWorkspace(t, f.c, alice, proto.WorkspaceSpec{})

	for _, bad := range []*proto.QueueCreateReq{
		{WS: ws.ID},
		{WS: ws.ID, Tasks: []string{""}},
		{WS: ws.ID, Tasks: []string{"a"}, SleepUntil: "25:00"},
		{WS: ws.ID, Tasks: []string{"a"}, SleepUntil: "09:00", SleepAfterSec: 5},
		{WS: ws.ID, Tasks: []string{"a"}, SleepAfterSec: -1},
		{WS: "ws_missing", Tasks: []string{"a"}},
	} {
		if _, err := f.c.queueCreate(ctx, alice, bad); err == nil {
			t.Fatalf("queue.create accepted %+v", bad)
		}
	}
	req := &proto.QueueCreateReq{WS: ws.ID, Recipe: "custom", Tasks: []string{"one", "two", "three"}, SleepAfterSec: 30, IdempotencyKey: "q1"}
	q, err := f.c.queueCreate(ctx, alice, req)
	if err != nil {
		t.Fatal(err)
	}
	if q.Status != proto.QueueRunning || q.Cursor != 0 || len(q.Items) != 3 || q.WS != ws.ID || q.Owner != "alice" {
		t.Fatalf("queue = %+v", q)
	}
	again, err := f.c.queueCreate(ctx, alice, req)
	if err != nil || again.ID != q.ID {
		t.Fatalf("replay: %+v %v", again, err)
	}
	if _, err := f.c.queueCreate(ctx, alice, &proto.QueueCreateReq{WS: ws.ID, Tasks: []string{"x"}, IdempotencyKey: "q2"}); !isCode(err, proto.CodeConflict) {
		t.Fatalf("second unfinished queue on one workspace: %v", err)
	}
	mallory := Subject{ID: "mallory", Tenant: "tenant-b"}
	if _, err := f.c.queueGet(ctx, mallory, q.ID); !isCode(err, proto.CodeDenied) {
		t.Fatalf("cross-tenant get: %v", err)
	}
	if _, err := f.c.queueAdvance(ctx, mallory, &proto.QueueAdvanceReq{ID: q.ID, Index: 0, IdempotencyKey: "m"}); !isCode(err, proto.CodeDenied) {
		t.Fatalf("cross-tenant advance: %v", err)
	}
	if _, err := f.c.queueAdvance(ctx, alice, &proto.QueueAdvanceReq{ID: q.ID, Index: 1, IdempotencyKey: "skip"}); !isCode(err, proto.CodeConflict) {
		t.Fatalf("advance past cursor: %v", err)
	}
	adv := &proto.QueueAdvanceReq{ID: q.ID, Index: 0, Session: "s_1", Exit: 0, IdempotencyKey: "a0"}
	q, err = f.c.queueAdvance(ctx, alice, adv)
	if err != nil || q.Cursor != 1 || q.Status != proto.QueueRunning || q.Items[0].Session != "s_1" || q.Items[0].Attempts != 1 {
		t.Fatalf("after first advance: %+v %v", q, err)
	}
	if q, err = f.c.queueAdvance(ctx, alice, adv); err != nil || q.Cursor != 1 {
		t.Fatalf("replayed advance moved the cursor: %+v %v", q, err)
	}
	if _, err := f.c.queueAdvance(ctx, alice, &proto.QueueAdvanceReq{ID: q.ID, Index: 0, Exit: 1, IdempotencyKey: "a0"}); !isCode(err, proto.CodeConflict) {
		t.Fatalf("idempotency key reuse with other arguments: %v", err)
	}
	q, err = f.c.queueAdvance(ctx, alice, &proto.QueueAdvanceReq{ID: q.ID, Index: 1, Exit: 2, IdempotencyKey: "a1"})
	if err != nil || q.Cursor != 1 || q.Status != proto.QueueFailed || q.Items[1].Exit != 2 {
		t.Fatalf("after failure: %+v %v", q, err)
	}

	// Restart: cursor, status and the failed item are what was committed.
	f.c.Stop()
	if err := f.log.Close(); err != nil {
		t.Fatal(err)
	}
	f = newControlFixture(t, path, nil)
	q, err = f.c.queueGet(ctx, alice, q.ID)
	if err != nil || q.Cursor != 1 || q.Status != proto.QueueFailed || q.Items[1].Attempts != 1 || q.SleepAfterSec != 30 {
		t.Fatalf("after restart: %+v %v", q, err)
	}
	// A retry of the failed task succeeds and the queue runs to done.
	q, err = f.c.queueAdvance(ctx, alice, &proto.QueueAdvanceReq{ID: q.ID, Index: 1, Exit: 0, IdempotencyKey: "a1-retry"})
	if err != nil || q.Cursor != 2 || q.Status != proto.QueueRunning || q.Items[1].Attempts != 2 {
		t.Fatalf("after retry: %+v %v", q, err)
	}
	q, err = f.c.queueAdvance(ctx, alice, &proto.QueueAdvanceReq{ID: q.ID, Index: 2, Exit: 0, IdempotencyKey: "a2"})
	if err != nil || q.Cursor != 3 || q.Status != proto.QueueDone {
		t.Fatalf("after last: %+v %v", q, err)
	}
	if _, err := f.c.queueAdvance(ctx, alice, &proto.QueueAdvanceReq{ID: q.ID, Index: 3, IdempotencyKey: "a3"}); !isCode(err, proto.CodeConflict) {
		t.Fatalf("advance of a done queue: %v", err)
	}
	// Done queues no longer block a new one on the workspace.
	if _, err := f.c.queueCreate(ctx, alice, &proto.QueueCreateReq{WS: ws.ID, Tasks: []string{"x"}, IdempotencyKey: "q3"}); err != nil {
		t.Fatal(err)
	}
	list, err := f.c.queueList(ctx, alice, &proto.QueueListReq{WS: ws.ID})
	if err != nil || len(list.Queues) != 2 || list.Queues[0].ID != q.ID {
		t.Fatalf("list: %+v %v", list, err)
	}
	if list, err = f.c.queueList(ctx, mallory, &proto.QueueListReq{}); err != nil || len(list.Queues) != 0 {
		t.Fatalf("mallory sees %+v", list)
	}

	events, err := f.log.Read(ctx, 1, ws.ID, 100)
	if err != nil {
		t.Fatal(err)
	}
	advanced := 0
	for _, e := range events {
		if e.Type == proto.EvQueueAdvanced {
			advanced++
			if string(e.Payload) == "" || e.Workspace != ws.ID || e.Tenant != "tenant-a" {
				t.Fatalf("queue.advanced not attributed: %+v", e)
			}
			var payload struct {
				Queue  string `cbor:"queue"`
				Index  int    `cbor:"index"`
				Exit   int    `cbor:"exit"`
				Status string `cbor:"status"`
				Task   string `cbor:"task"`
			}
			if err := proto.Unmarshal(e.Payload, &payload); err != nil {
				t.Fatal(err)
			}
			if payload.Queue != q.ID || payload.Task != "" {
				t.Fatalf("payload %+v", payload)
			}
		}
	}
	// four successful/failed advances committed; the replay committed none.
	if advanced != 4 {
		t.Fatalf("queue.advanced events = %d", advanced)
	}

	// Pruning a destroyed workspace takes its queues along.
	if err := f.c.wsDestroy(ctx, "alice", ws.ID, "destroy"); err != nil {
		t.Fatal(err)
	}
	if _, err := f.c.PruneRecords(ctx, time.Now().Add(24*time.Hour), 1000); err != nil {
		t.Fatal(err)
	}
	if _, err := f.c.queueGet(ctx, alice, q.ID); !isCode(err, proto.CodeNotFound) {
		t.Fatalf("queue outlived its pruned workspace: %v", err)
	}
}

func isCode(err error, code string) bool {
	var pe *proto.Error
	return errors.As(err, &pe) && pe.Code == code
}
