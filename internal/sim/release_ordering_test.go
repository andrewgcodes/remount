package sim

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"remount.dev/remount/internal/client"
	"remount.dev/remount/internal/proto"
)

func TestDelayedReleaseFromOlderAbortCannotFenceRestoredWorkspace(t *testing.T) {
	w := newWorld(t)
	const nodeName = "release-order-node"
	const delayedFrameID = 1 << 62
	var (
		mu       sync.Mutex
		requests []proto.WSReleaseReq
		first    *proto.Frame
	)
	w.mu.Lock()
	w.serverHooks[nodeName] = func(frame *proto.Frame) bool {
		if frame.T != proto.KindReq || frame.Op != proto.OpWSRelease {
			return true
		}
		var request proto.WSReleaseReq
		if err := frame.Decode(&request); err != nil {
			t.Errorf("decode release request: %v", err)
			return true
		}
		if frame.ID == delayedFrameID {
			return true
		}
		mu.Lock()
		requests = append(requests, request)
		if first == nil {
			copyFrame := *frame
			copyFrame.Body = append([]byte(nil), frame.Body...)
			first = &copyFrame
		}
		mu.Unlock()
		return true
	}
	w.peerHooks[nodeName] = func(frame *proto.Frame) bool {
		if frame.T == proto.KindRes && frame.Op == proto.OpWSRelease && frame.Err == nil {
			frame.Err = proto.Err(proto.CodeInternal, "injected lost release result")
		}
		return true
	}
	w.mu.Unlock()
	node := w.node(nodeName, nil)
	c := w.client("release-order-client")
	ctx := ctxT(t, 2*time.Minute)
	ws := mustWS(t, c, proto.WorkspaceSpec{Placement: proto.Placement{Node: node.ID()}})
	if err := c.WriteFile(ctx, ws.ID, "state.txt", []byte("live"), 0o644); err != nil {
		t.Fatal(err)
	}

	for cycle := 1; cycle <= 2; cycle++ {
		_, err := c.MoveWorkspace(ctx, ws.ID, nil, nil,
			client.WithIdempotencyKey(fmt.Sprintf("release-order-%d", cycle)))
		if err == nil {
			t.Fatalf("release cycle %d succeeded despite injected response failure", cycle)
		}
		current, getErr := c.GetWorkspace(ctx, ws.ID)
		if getErr != nil {
			t.Fatal(getErr)
		}
		if current.State != proto.WSClaimed || current.Generation != ws.Generation || current.Node != node.ID() {
			t.Fatalf("release cycle %d did not restore authority: %+v", cycle, current)
		}
	}

	session, err := c.Exec(ctx, proto.SOpenReq{
		WS: ws.ID, Kind: proto.SessionExec, Program: []string{"sh", "-c", "sleep 30"},
	})
	if err != nil {
		t.Fatal(err)
	}
	mu.Lock()
	delayed := *first
	delayed.ID = delayedFrameID
	if len(requests) < 2 || requests[0].OperationID == requests[1].OperationID {
		mu.Unlock()
		t.Fatalf("release operations were not distinct: %+v", requests)
	}
	mu.Unlock()
	if err := w.sendToPeer(context.Background(), nodeName, &delayed); err != nil {
		t.Fatal(err)
	}
	if err := c.WriteFile(ctx, ws.ID, "after-delayed.txt", []byte("still-live"), 0o644); err != nil {
		t.Fatalf("delayed release fenced the restored workspace: %v", err)
	}
	if got, err := c.ReadFile(ctx, ws.ID, "state.txt"); err != nil || string(got) != "live" {
		t.Fatalf("delayed release changed retained source: %q, %v", got, err)
	}
	select {
	case <-session.Done():
		t.Fatalf("delayed release stopped a fresh session: exit=%+v err=%v", session.Exit(), session.Err())
	default:
	}
	if err := session.Close(ctx, true); err != nil {
		t.Fatal(err)
	}

	_, err = c.MoveWorkspace(ctx, ws.ID, nil, nil,
		client.WithIdempotencyKey("release-order-3"))
	if err == nil {
		t.Fatal("third release cycle succeeded despite injected response failure")
	}
	current, err := c.GetWorkspace(ctx, ws.ID)
	if err != nil {
		t.Fatal(err)
	}
	if current.State != proto.WSClaimed || current.Generation != ws.Generation || current.Node != node.ID() {
		t.Fatalf("third release cycle did not restore authority: %+v", current)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(requests) < 3 {
		t.Fatalf("captured %d release operations, want at least 3", len(requests))
	}
	for index, request := range requests[:3] {
		want := uint64(index + 1)
		if request.ReleaseEpoch != want {
			t.Fatalf("release operation %d epoch=%d want=%d: %+v", index+1, request.ReleaseEpoch, want, request)
		}
	}
	if requests[2].OperationID == requests[0].OperationID || requests[2].OperationID == requests[1].OperationID {
		t.Fatalf("third release operation was not fresh: %+v", requests[:3])
	}
}
