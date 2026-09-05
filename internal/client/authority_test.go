package client

import (
	"context"
	"testing"
	"time"

	"remount.dev/remount/internal/proto"
	"remount.dev/remount/internal/transport"
)

func TestChunkAuthorityIncludesBufferedWorkspaceAndNode(t *testing.T) {
	c := New(Options{})
	t.Cleanup(func() { c.Close() })
	// handle only reads this peer's epoch; no connection is needed here.
	p := &transport.Peer{}
	frame := func(from, ws string, seq uint64, data string) *proto.Frame {
		f := chunkFrame(seq, proto.StreamStdout, []byte(data))
		f.From, f.WS = from, ws
		return f
	}
	for _, from := range []string{"", "client_evil", "node_evil"} {
		c.handle(context.Background(), p, frame(from, "ws_test", 0, "forged"))
	}
	// No cached grant is required: reconnect clears it while an in-flight
	// open may still return successfully using the grant it already holds.
	// This node is authorized for a different workspace; its frame must not
	// become trusted merely because an open later returns the same session id.
	c.handle(context.Background(), p, frame("node_other", "ws_other", 0, "wrong workspace"))
	c.handle(context.Background(), p, frame("node_good", "ws_test", 0, "early"))
	s := c.newSession("", "ws_test", "exec")
	s.bindNode("node_good")
	c.register("s_test", s)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	select {
	case got := <-s.Chunks():
		if string(got.Data) != "early" {
			t.Fatalf("early=%q", got.Data)
		}
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	// Reattachment transfers observation authority; a formerly valid node
	// cannot inject even a malformed body after the new grant has been bound.
	s.bindNode("node_new")
	for _, from := range []string{"", "client_evil", "node_good"} {
		f := frame(from, "ws_test", 1, "forged")
		f.Body = []byte{0xff}
		c.handle(ctx, p, f)
	}
	c.handle(ctx, p, frame("node_new", "ws_test", 1, "current"))
	select {
	case got := <-s.Chunks():
		if string(got.Data) != "current" {
			t.Fatalf("live=%q", got.Data)
		}
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
}

func TestLogEventsOnlyComeFromControl(t *testing.T) {
	c := New(Options{})
	t.Cleanup(func() { c.Close() })
	p := &transport.Peer{}
	sub := newEventSubscription(c, "sub_test", 0, "")
	c.eventSubs[sub.id] = sub
	for _, from := range []string{"client_evil", "node_evil", "", proto.PeerControl} {
		c.handle(context.Background(), p, &proto.Frame{
			T: proto.KindEvent, From: from, Op: "log",
			Body: proto.MustMarshal(proto.EventPost{Events: []proto.Event{{Seq: 1, Type: from}}}),
		})
	}
	select {
	case got := <-sub.out:
		if got.Type != proto.PeerControl {
			t.Fatalf("forged log event: %+v", got)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("control event not delivered")
	}
}
