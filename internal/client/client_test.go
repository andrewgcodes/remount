package client

import (
	"context"
	"errors"
	"testing"
	"time"

	"remount.dev/remount/internal/proto"
	"remount.dev/remount/internal/transport"
)

func chunkFrame(seq uint64, stream uint8, data []byte) *proto.Frame {
	return &proto.Frame{
		V: proto.Version, T: proto.KindChunk, S: "s_test", Seq: seq,
		Body: proto.MustMarshal(proto.ChunkBody{Stream: stream, Data: data}),
	}
}

func TestSessionDeliveryReordersWithoutTransportBlocking(t *testing.T) {
	c := New(Options{})
	s := c.newSession("s_test", "ws_test", proto.SessionExec)
	c.mu.Lock()
	c.sessions[s.ID] = s
	c.mu.Unlock()

	s.enqueue(chunkFrame(1, proto.StreamStdout, []byte("data")))
	s.enqueue(chunkFrame(0, proto.StreamInfo, proto.MustMarshal(proto.SessionInfo{ID: s.ID})))
	s.enqueue(chunkFrame(2, proto.StreamExit, proto.MustMarshal(proto.ExitInfo{Code: 0})))

	var got []Chunk
	for chunk := range s.Chunks() {
		got = append(got, chunk)
	}
	if len(got) != 3 || got[0].Seq != 0 || got[1].Seq != 1 || got[2].Seq != 2 {
		t.Fatalf("delivery order: %+v", got)
	}
	if s.Exit() == nil || s.Exit().Code != 0 || s.Err() != nil {
		t.Fatalf("exit=%+v err=%v", s.Exit(), s.Err())
	}
}

func TestSlowSessionConsumerFailsOnlyThatSession(t *testing.T) {
	c := New(Options{})
	s := &Session{
		c: c, ID: "s_test", WS: "ws_test", pending: map[uint64]*proto.Frame{},
		out: make(chan Chunk), in: make(chan *proto.Frame, 1),
		stop: make(chan struct{}), exited: make(chan struct{}),
	}
	c.mu.Lock()
	c.sessions[s.ID] = s
	c.mu.Unlock()
	go s.runDelivery()
	s.enqueue(chunkFrame(0, proto.StreamInfo, proto.MustMarshal(proto.SessionInfo{ID: s.ID})))
	deadline := time.Now().Add(time.Second)
	for len(s.in) != 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	// The pump is now blocked on the unconsumed public channel. One frame can
	// queue; the next must fail the session immediately instead of blocking.
	s.enqueue(chunkFrame(1, proto.StreamStdout, []byte("queued")))
	start := time.Now()
	s.enqueue(chunkFrame(2, proto.StreamStdout, []byte("overflow")))
	if time.Since(start) > 100*time.Millisecond {
		t.Fatal("enqueue blocked on a slow application consumer")
	}
	select {
	case <-s.Done():
	case <-time.After(time.Second):
		t.Fatal("overflow did not wake the session")
	}
	var pe *proto.Error
	if !errors.As(s.Err(), &pe) || pe.Code != proto.CodeResourceExhausted {
		t.Fatalf("unexpected terminal error: %v", s.Err())
	}
}

func TestSessionCloseWakesBlockedDelivery(t *testing.T) {
	c := New(Options{Retries: 1})
	c.mu.Lock()
	c.closed = true
	c.mu.Unlock()
	s := &Session{
		c: c, ID: "s_test", WS: "ws_test", pending: map[uint64]*proto.Frame{},
		out: make(chan Chunk), in: make(chan *proto.Frame, 1),
		stop: make(chan struct{}), exited: make(chan struct{}),
	}
	go s.runDelivery()
	s.enqueue(chunkFrame(0, proto.StreamInfo, proto.MustMarshal(proto.SessionInfo{ID: s.ID})))
	deadline := time.Now().Add(time.Second)
	for len(s.in) != 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := s.Close(ctx, false); !errors.Is(err, transport.ErrClosed) {
		t.Fatalf("Close error = %v", err)
	}
	select {
	case <-s.Done():
	case <-time.After(time.Second):
		t.Fatal("Close did not wake blocked delivery")
	}
}
