package relay

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"remount.dev/remount/internal/proto"
	"remount.dev/remount/internal/transport"
)

type testController struct{}

func (testController) Authenticate(context.Context, *proto.Hello) (string, *proto.HelloOK, error) {
	return "", nil, nil
}
func (testController) HandleFrame(context.Context, *proto.Frame)           {}
func (testController) PeerConnected(context.Context, string, *proto.Hello) {}
func (testController) PeerGone(context.Context, string)                    {}

type testConn struct {
	closed chan struct{}
	once   sync.Once
}

func newTestConn() *testConn { return &testConn{closed: make(chan struct{})} }

func (c *testConn) Send(ctx context.Context, _ *proto.Frame) error {
	select {
	case <-c.closed:
		return transport.ErrClosed
	case <-ctx.Done():
		return ctx.Err()
	default:
		return nil
	}
}

func (c *testConn) Recv(ctx context.Context) (*proto.Frame, error) {
	select {
	case <-c.closed:
		return nil, transport.ErrClosed
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (c *testConn) Close() error {
	c.once.Do(func() { close(c.closed) })
	return nil
}

func TestRouteRemoveAndReplacementAreSynchronized(t *testing.T) {
	r := New(testController{})
	a := transport.NewPeer(newTestConn(), nil)
	b := transport.NewPeer(newTestConn(), nil)
	defer a.Close()
	defer b.Close()
	r.peers["a"] = a
	r.peers["b"] = b

	ctx := context.Background()
	var wg sync.WaitGroup
	for worker := 0; worker < 8; worker++ {
		wg.Add(1)
		go func(worker int) {
			defer wg.Done()
			for i := 0; i < 1000; i++ {
				from, to := "a", "b"
				if (i+worker)%2 == 0 {
					from, to = to, from
				}
				r.route(ctx, &proto.Frame{V: proto.Version, T: proto.KindEvent, From: from, To: to})
			}
		}(worker)
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 1000; i++ {
			r.remove(ctx, "b", b)
			r.mu.Lock()
			r.peers["b"] = b
			r.mu.Unlock()
		}
	}()
	wg.Wait()
}

func TestControlRequestResponseIsBoundToPeerAndOperation(t *testing.T) {
	r := New(testController{})
	ch := make(chan *proto.Frame, 1)
	r.pending[7] = relayPending{from: "node", op: "expected", ch: ch}
	r.route(context.Background(), &proto.Frame{V: proto.Version, T: proto.KindRes, ID: 7, From: "attacker", Op: "expected"})
	r.route(context.Background(), &proto.Frame{V: proto.Version, T: proto.KindRes, ID: 7, From: "node", Op: "wrong"})
	select {
	case <-ch:
		t.Fatal("forged response satisfied request")
	default:
	}
	want := &proto.Frame{V: proto.Version, T: proto.KindRes, ID: 7, From: "node", Op: "expected"}
	r.route(context.Background(), want)
	select {
	case got := <-ch:
		if got != want {
			t.Fatalf("got %p, want %p", got, want)
		}
	case <-time.After(time.Second):
		t.Fatal("valid response was not delivered")
	}
}

func TestCloseFailsControlRequests(t *testing.T) {
	r := New(testController{})
	p := transport.NewPeer(newTestConn(), nil)
	r.peers["node"] = p
	done := make(chan error, 1)
	go func() {
		done <- r.Request(context.Background(), "node", "hang", nil, nil)
	}()
	deadline := time.After(time.Second)
	for {
		r.pendMu.Lock()
		waiting := len(r.pending) == 1
		r.pendMu.Unlock()
		if waiting {
			break
		}
		select {
		case <-deadline:
			t.Fatal("request did not become pending")
		default:
		}
	}
	r.Close()
	select {
	case err := <-done:
		if !errors.Is(err, transport.ErrClosed) {
			t.Fatalf("got %v, want ErrClosed", err)
		}
	case <-time.After(time.Second):
		t.Fatal("request remained blocked after close")
	}
}
