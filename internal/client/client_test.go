package client

import (
	"context"
	"errors"
	"sync/atomic"
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

func TestCallerSuppliedIdempotencyKey(t *testing.T) {
	key, configured := operationKey([]OperationOption{WithIdempotencyKey("logical-operation")})
	if key != "logical-operation" || !configured {
		t.Fatalf("operation key = %q, configured=%v", key, configured)
	}
	generated, configured := operationKey(nil)
	if generated == "" || configured {
		t.Fatalf("generated key = %q, configured=%v", generated, configured)
	}
}

func TestOnlyGrantAuthorityStalenessIsRetryable(t *testing.T) {
	for _, test := range []struct {
		name   string
		reason string
		want   bool
	}{
		{name: "authorization revision or tenant", reason: proto.ReasonRevoked, want: true},
		{name: "controller epoch", reason: proto.ReasonGenerationMismatch, want: true},
		{name: "different client, workspace or node", reason: ""},
		{name: "bad grant signature", reason: ""},
		{name: "expired grant is not converged by a retry", reason: proto.ReasonGrantExpired},
		{name: "an unknown future reason is not retried", reason: "invented_reason"},
	} {
		err := &proto.Error{Code: proto.CodeUnauthorized, Reason: test.reason, Msg: test.name}
		if got := staleGrantAuthority(err); got != test.want {
			t.Fatalf("staleGrantAuthority(%s reason=%q) = %v, want %v", test.name, test.reason, got, test.want)
		}
	}
	// Message text is no longer authority: the same words without a reason
	// must not resurrect the old match.
	legacy := &proto.Error{Code: proto.CodeUnauthorized, Msg: "grant authorization revision is stale"}
	if staleGrantAuthority(legacy) {
		t.Fatal("message text still drives the retry decision")
	}
}

func TestAppendWithinLimit(t *testing.T) {
	destination := []byte("ab")
	if !appendWithinLimit(&destination, []byte("cd"), 2, 4) || string(destination) != "abcd" {
		t.Fatalf("boundary append = %q", destination)
	}
	if appendWithinLimit(&destination, []byte("e"), 4, 4) || string(destination) != "abcd" {
		t.Fatalf("over-limit append mutated output: %q", destination)
	}
	if appendWithinLimit(&destination, []byte("x"), 5, 4) {
		t.Fatal("invalid current size was accepted")
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

func TestNodeCallWaitsForAuthorizationPush(t *testing.T) {
	clientConn, serverConn := transport.Pipe(8)
	clientPeer := transport.NewPeer(clientConn, nil)
	var nodeCalls atomic.Int64
	serverPeer := transport.NewPeer(serverConn, transport.HandlerFunc(func(ctx context.Context, peer *transport.Peer, frame *proto.Frame) {
		switch frame.Op {
		case proto.OpGrant:
			_ = peer.Respond(ctx, frame, proto.Grant{
				Node: "n_test",
				Claims: proto.GrantClaims{
					Client: "c_test", WS: "ws_test", Node: "n_test", AuthzRevision: 2,
				},
			})
		case "test.authz":
			if nodeCalls.Add(1) < 3 {
				_ = peer.RespondErr(ctx, frame, proto.Err(proto.CodeConflict, "authorization push pending"))
				return
			}
			_ = peer.Respond(ctx, frame, struct{}{})
		default:
			_ = peer.RespondErr(ctx, frame, proto.Err(proto.CodeUnsupported, "unexpected operation"))
		}
	}))
	t.Cleanup(func() {
		_ = clientPeer.Close()
		_ = serverPeer.Close()
	})
	c := New(Options{Retries: 1})
	c.mu.Lock()
	c.peer = clientPeer
	c.id = "c_test"
	c.mu.Unlock()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := c.nodeCall(ctx, "ws_test", "test.authz", func(grant *proto.Grant) any {
		return struct {
			Grant *proto.Grant `cbor:"grant"`
		}{Grant: grant}
	}, &struct{}{}); err != nil {
		t.Fatal(err)
	}
	if calls := nodeCalls.Load(); calls != 3 {
		t.Fatalf("node calls = %d", calls)
	}
}

// A destroy that control declined to commit because the node's uplink was
// reconnecting must be retried, not surfaced. Control says "remains
// uncommitted" in that refusal, so nothing was applied and the retry is safe.
func TestDestroyWorkspaceRetriesWhileTheNodeUplinkReconnects(t *testing.T) {
	clientConn, serverConn := transport.Pipe(8)
	clientPeer := transport.NewPeer(clientConn, nil)
	var destroys atomic.Int64
	serverPeer := transport.NewPeer(serverConn, transport.HandlerFunc(func(ctx context.Context, peer *transport.Peer, frame *proto.Frame) {
		if frame.Op != proto.OpWSDestroy {
			_ = peer.RespondErr(ctx, frame, proto.Err(proto.CodeUnsupported, "unexpected operation"))
			return
		}
		if destroys.Add(1) < 3 {
			_ = peer.RespondErr(ctx, frame, proto.Err(proto.CodeUnreachable,
				"workspace source node n_test is unavailable; destroy remains uncommitted"))
			return
		}
		_ = peer.Respond(ctx, frame, struct{}{})
	}))
	t.Cleanup(func() {
		_ = clientPeer.Close()
		_ = serverPeer.Close()
	})
	c := New(Options{Retries: 1})
	c.mu.Lock()
	c.peer = clientPeer
	c.id = "c_test"
	c.mu.Unlock()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := c.DestroyWorkspace(ctx, "ws_test"); err != nil {
		t.Fatal(err)
	}
	if got := destroys.Load(); got != 3 {
		t.Fatalf("destroy attempts = %d, want 3", got)
	}
}

// A node that is genuinely gone must not hold the caller past the budget.
func TestDestroyWorkspaceGivesUpOnAPermanentlyUnreachableNode(t *testing.T) {
	clientConn, serverConn := transport.Pipe(8)
	clientPeer := transport.NewPeer(clientConn, nil)
	var destroys atomic.Int64
	serverPeer := transport.NewPeer(serverConn, transport.HandlerFunc(func(ctx context.Context, peer *transport.Peer, frame *proto.Frame) {
		destroys.Add(1)
		_ = peer.RespondErr(ctx, frame, proto.Err(proto.CodeUnreachable,
			"workspace source node n_test is unavailable; destroy remains uncommitted"))
	}))
	t.Cleanup(func() {
		_ = clientPeer.Close()
		_ = serverPeer.Close()
	})
	c := New(Options{Retries: 1})
	c.mu.Lock()
	c.peer = clientPeer
	c.id = "c_test"
	c.mu.Unlock()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	start := time.Now()
	err := c.DestroyWorkspace(ctx, "ws_test")
	elapsed := time.Since(start)
	if err == nil {
		t.Fatal("destroy against a permanently unreachable node returned nil")
	}
	if elapsed > nodeCallRetryBudget+2*time.Second {
		t.Fatalf("destroy held the caller for %s, past the %s budget", elapsed, nodeCallRetryBudget)
	}
	if destroys.Load() < 2 {
		t.Fatalf("destroy attempts = %d, want more than one", destroys.Load())
	}
}
