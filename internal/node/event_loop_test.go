package node

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"remount.dev/remount/internal/proto"
	"remount.dev/remount/internal/transport"
)

// controlThatPosts stands in for control's events.post handler. decide is
// called per batch and returns the error to answer with, or nil to accept.
func controlThatPosts(t *testing.T, n *Node, decide func(req proto.EventPost) *proto.Error) <-chan proto.EventPost {
	t.Helper()
	nodeConn, controlConn := transport.Pipe(8)
	accepted := make(chan proto.EventPost, 16)
	controlPeer := transport.NewPeer(controlConn, transport.HandlerFunc(func(ctx context.Context, p *transport.Peer, frame *proto.Frame) {
		if frame.Op != proto.OpEventsPost {
			_ = p.RespondErr(ctx, frame, proto.Err(proto.CodeBadRequest, "unexpected operation %s", frame.Op))
			return
		}
		var req proto.EventPost
		if err := frame.Decode(&req); err != nil {
			t.Errorf("decode events.post: %v", err)
			return
		}
		if err := decide(req); err != nil {
			_ = p.RespondErr(ctx, frame, err)
			return
		}
		accepted <- req
		_ = p.Respond(ctx, frame, struct{}{})
	}))
	nodePeer := transport.NewPeer(nodeConn, nil)
	t.Cleanup(func() {
		_ = nodePeer.Close()
		_ = controlPeer.Close()
	})
	n.mu.Lock()
	n.peer = nodePeer
	n.mu.Unlock()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	n.wg.Add(1)
	go n.eventLoop(ctx)
	return accepted
}

func newTestNodeAt(t *testing.T, dir string) *Node {
	t.Helper()
	return newTestNode(t, func(o *Options) { o.DataDir = dir })
}

// emitLastSeq is the watermark the node has persisted for its own events.
func (n *Node) emitLastSeq() uint64 {
	n.producerSeqMu.Lock()
	defer n.producerSeqMu.Unlock()
	return n.producerSeqHigh
}

func collectTypes(t *testing.T, accepted <-chan proto.EventPost, want int) map[string]uint64 {
	t.Helper()
	seen := map[string]uint64{}
	deadline := time.After(10 * time.Second)
	for len(seen) < want {
		select {
		case req := <-accepted:
			for _, e := range req.Events {
				seen[e.Type] = e.Seq
			}
		case <-deadline:
			t.Fatalf("control received %v, want %d event types", seen, want)
		}
	}
	return seen
}

// Control answered "sequence already names a different event": the node's
// numbering collided with a previous life. The batch must be re-issued under
// sequences past the collision, not retried verbatim forever.
func TestEventLoopReissuesABatchAfterASequenceCollision(t *testing.T) {
	n := newTestNode(t, nil)
	// Emitted before the forwarder starts, so its first backfill carries both
	// and the collision answer applies to one deterministic batch.
	n.emit("test.first", "ws_collide", "p", map[string]any{"k": 1})
	n.emit("test.second", "ws_collide", "p", nil)
	var posts atomic.Int64
	accepted := controlThatPosts(t, n, func(proto.EventPost) *proto.Error {
		if posts.Add(1) == 1 {
			return proto.Err(proto.CodeConflict, "node event sequence 1 is out of order or changed")
		}
		return nil
	})
	seen := collectTypes(t, accepted, 2)
	if seen["test.first"] < producerSeqRecoveryStride || seen["test.second"] <= seen["test.first"] {
		t.Fatalf("re-issued events did not move past the collided sequences: %v", seen)
	}
	if got := n.emitLastSeq(); got != seen["test.second"] {
		t.Fatalf("persisted watermark %d != last re-issued sequence %d", got, seen["test.second"])
	}
}

// One event control rejects outright must not hold the events behind it.
func TestEventLoopDropsOnlyTheEventControlRejects(t *testing.T) {
	n := newTestNode(t, nil)
	n.emit("test.before", "ws_ok", "p", nil)
	n.emit("test.poison", "ws_poison", "p", nil)
	n.emit("test.after", "ws_ok", "p", nil)
	accepted := controlThatPosts(t, n, func(req proto.EventPost) *proto.Error {
		for _, e := range req.Events {
			if e.Type == "test.poison" {
				return proto.Err(proto.CodeDenied, "node does not own event workspace ws_poison")
			}
		}
		return nil
	})
	seen := collectTypes(t, accepted, 2)
	if _, ok := seen["test.poison"]; ok {
		t.Fatal("the rejected event was recorded anyway")
	}
	if _, ok := seen["test.before"]; !ok {
		t.Fatal("the event before the rejected one was lost")
	}
	if _, ok := seen["test.after"]; !ok {
		t.Fatal("the event after the rejected one was held back")
	}
}

// A restarted node continues its producer numbering from the persisted
// watermark rather than beginning again at one.
func TestProducerSequenceContinuesAcrossRestart(t *testing.T) {
	dir := t.TempDir()
	first := newTestNodeAt(t, dir)
	first.emit("test.a", "ws_x", "p", nil)
	first.emit("test.b", "ws_x", "p", nil)
	last := first.emitLastSeq()
	if last != 2 {
		t.Fatalf("first life persisted watermark %d, want 2", last)
	}
	second := newTestNodeAt(t, dir)
	second.emit("test.c", "ws_x", "p", nil)
	if got := second.emitLastSeq(); got != 3 {
		t.Fatalf("restarted node numbered its first event %d, want 3", got)
	}
}
