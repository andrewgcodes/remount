package transport

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"remount.dev/remount/internal/proto"
)

type echoHandler struct{}

func (echoHandler) HandleFrame(ctx context.Context, p *Peer, f *proto.Frame) {
	switch f.T {
	case proto.KindReq:
		if f.Op == "fail" {
			_ = p.RespondErr(ctx, f, proto.Err(proto.CodeBadRequest, "nope"))
			return
		}
		var in map[string]string
		_ = f.Decode(&in)
		_ = p.Respond(ctx, f, map[string]string{"echo": in["msg"]})
	}
}

func TestPeerRequestResponsePipe(t *testing.T) {
	a, b := Pipe(8)
	pa := NewPeer(a, nil)
	pb := NewPeer(b, echoHandler{})
	defer pa.Close()
	defer pb.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	var out map[string]string
	if err := pa.Call(ctx, "b", "echo", map[string]string{"msg": "hi"}, &out); err != nil {
		t.Fatal(err)
	}
	if out["echo"] != "hi" {
		t.Fatalf("got %v", out)
	}
	_, err := pa.Request(ctx, "b", "fail", nil)
	var pe *proto.Error
	if !errors.As(err, &pe) || pe.Code != proto.CodeBadRequest {
		t.Fatalf("want bad_request, got %v", err)
	}
	if rtt, err := pa.Ping(ctx); err != nil || rtt < 0 {
		t.Fatalf("ping: %v", err)
	}
}

func TestPeerConcurrentRequests(t *testing.T) {
	a, b := Pipe(64)
	pa := NewPeer(a, nil)
	pb := NewPeer(b, echoHandler{})
	defer pa.Close()
	defer pb.Close()
	ctx := context.Background()
	var wg sync.WaitGroup
	for i := 0; i < 200; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			msg := strings.Repeat("x", i%7) + "m"
			var out map[string]string
			if err := pa.Call(ctx, "b", "echo", map[string]string{"msg": msg}, &out); err != nil {
				t.Error(err)
				return
			}
			if out["echo"] != msg {
				t.Errorf("mismatch %q %q", out["echo"], msg)
			}
		}(i)
	}
	wg.Wait()
}

func TestPeerFailsPendingOnClose(t *testing.T) {
	a, b := Pipe(1)
	pa := NewPeer(a, nil)
	_ = NewPeer(b, HandlerFunc(func(ctx context.Context, p *Peer, f *proto.Frame) {
		// never respond; simulate a hang, then the link dies
		go func() { time.Sleep(20 * time.Millisecond); p.Close() }()
	}))
	_, err := pa.Request(context.Background(), "b", "hang", nil)
	if !errors.Is(err, ErrClosed) {
		t.Fatalf("want ErrClosed, got %v", err)
	}
	<-pa.Done()
}

func TestPipeHookDrops(t *testing.T) {
	a, b := Pipe(8)
	dropped := 0
	SetHook(a, func(f *proto.Frame) bool {
		if f.T == proto.KindEvent {
			dropped++
			return false
		}
		return true
	})
	ctx := context.Background()
	_ = a.Send(ctx, proto.NewEvent("b", "x", nil))
	_ = a.Send(ctx, &proto.Frame{T: proto.KindPing, ID: 1})
	f, err := b.Recv(ctx)
	if err != nil || f.T != proto.KindPing || dropped != 1 {
		t.Fatalf("%v %v %d", f, err, dropped)
	}
}

func TestWebSocketRoundTrip(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c, err := AcceptWS(w, r)
		if err != nil {
			return
		}
		p := NewPeer(c, echoHandler{})
		<-p.Done()
	}))
	defer srv.Close()
	url := "ws" + strings.TrimPrefix(srv.URL, "http") + "/v1/link"
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	c, err := DialWS(ctx, url, nil)
	if err != nil {
		t.Fatal(err)
	}
	p := NewPeer(c, nil)
	var out map[string]string
	if err := p.Call(ctx, "srv", "echo", map[string]string{"msg": "over ws"}, &out); err != nil {
		t.Fatal(err)
	}
	if out["echo"] != "over ws" {
		t.Fatal(out)
	}
	big := strings.Repeat("y", 1<<20)
	if err := p.Call(ctx, "srv", "echo", map[string]string{"msg": big}, &out); err != nil {
		t.Fatal(err)
	}
	if len(out["echo"]) != len(big) {
		t.Fatal("big frame mismatch")
	}
	p.Close()
	<-p.Done()
	if _, err := p.Request(ctx, "srv", "echo", nil); !errors.Is(err, ErrClosed) {
		t.Fatalf("after close: %v", err)
	}
}
