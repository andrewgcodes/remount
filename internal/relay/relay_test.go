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

func TestCloseJoinsPreAuthenticationServeAndRejectsNewConnections(t *testing.T) {
	r := New(testController{})
	conn := newTestConn()
	done := make(chan error, 1)
	go func() { done <- r.Serve(context.Background(), conn) }()

	deadline := time.Now().Add(time.Second)
	for {
		r.mu.RLock()
		active := len(r.active)
		r.mu.RUnlock()
		if active == 1 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("Serve was not registered")
		}
		time.Sleep(time.Millisecond)
	}

	r.Close()
	select {
	case err := <-done:
		if !errors.Is(err, transport.ErrClosed) {
			t.Fatalf("Serve returned %v, want ErrClosed", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Close returned without joining Serve")
	}

	rejected := newTestConn()
	if err := r.Serve(context.Background(), rejected); !errors.Is(err, transport.ErrClosed) {
		t.Fatalf("Serve after Close returned %v, want ErrClosed", err)
	}
	select {
	case <-rejected.closed:
	default:
		t.Fatal("rejected connection was not closed")
	}
}

func TestHelloReturnsDeepCopy(t *testing.T) {
	r := New(testController{})
	r.hellos["node"] = &proto.Hello{
		Peer: "node", Caps: []string{"v1"}, PubKey: []byte{1}, Nonce: []byte{2}, Proof: []byte{3},
		Labels: map[string]string{"zone": "a"},
		Node: &proto.NodeInfo{Backends: []string{"process"}, Caps: []string{"gpu"}, Connectors: []string{"package"},
			BackendDescriptors: []proto.BackendDescriptor{{Name: "process"}}},
	}

	got := r.Hello("node")
	got.Caps[0] = "changed"
	got.PubKey[0], got.Nonce[0], got.Proof[0] = 9, 9, 9
	got.Labels["zone"] = "changed"
	got.Node.Backends[0] = "changed"
	got.Node.Caps[0] = "changed"
	got.Node.Connectors[0] = "changed"
	got.Node.BackendDescriptors[0].Name = "changed"

	want := r.Hello("node")
	if want.Caps[0] != "v1" || want.PubKey[0] != 1 || want.Nonce[0] != 2 || want.Proof[0] != 3 ||
		want.Labels["zone"] != "a" || want.Node.Backends[0] != "process" || want.Node.Caps[0] != "gpu" ||
		want.Node.Connectors[0] != "package" ||
		want.Node.BackendDescriptors[0].Name != "process" {
		t.Fatalf("stored hello was mutated through returned value: %+v", want)
	}
}
