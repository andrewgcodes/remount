package relay

import (
	"context"
	"sync"
	"testing"
	"time"

	"remount.dev/remount/internal/proto"
	"remount.dev/remount/internal/trace"
	"remount.dev/remount/internal/transport"
)

// capturingConn records the frames the relay actually put on the wire.
type capturingConn struct {
	*testConn
	mu     sync.Mutex
	frames []*proto.Frame
}

func newCapturingConn() *capturingConn { return &capturingConn{testConn: newTestConn()} }

func (c *capturingConn) Send(ctx context.Context, f *proto.Frame) error {
	if err := c.testConn.Send(ctx, f); err != nil {
		return err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.frames = append(c.frames, f)
	return nil
}

func (c *capturingConn) last(t *testing.T) *proto.Frame {
	t.Helper()
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.frames) == 0 {
		t.Fatal("the relay sent nothing")
	}
	return c.frames[len(c.frames)-1]
}

type discardExporter struct{}

func (discardExporter) Enqueue(trace.Record) {}

// TestControlRequestCarriesTheCallersTraceContext proves the propagation a
// node relies on to make its span a child of the control span that caused it,
// and that a deployment with no collector configured stamps nothing at all.
func TestControlRequestCarriesTheCallersTraceContext(t *testing.T) {
	r := New(testController{})
	conn := newCapturingConn()
	r.peers["node"] = transport.NewPeer(conn, nil)

	untraced, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	_ = r.Request(untraced, "node", "fs.write", map[string]string{}, nil)
	if f := conn.last(t); f.Trace != "" || f.Span != "" {
		t.Fatalf("an untraced deployment stamped a trace context: %q %q", f.Trace, f.Span)
	}

	tracer := trace.NewTracer(discardExporter{})
	spanCtx, span := tracer.Start(context.Background(), "ws.create")
	traced, cancelTraced := context.WithTimeout(spanCtx, 50*time.Millisecond)
	defer cancelTraced()
	_ = r.Request(traced, "node", "fs.write", map[string]string{}, nil)
	sent := conn.last(t)
	if sent.Trace != span.TraceID() || sent.Span != span.SpanID() {
		t.Fatalf("frame carried (%q,%q), the control span is (%q,%q)",
			sent.Trace, sent.Span, span.TraceID(), span.SpanID())
	}

	_, child := tracer.StartRemote(context.Background(), sent.Op, sent.Trace, sent.Span)
	if child.TraceID() != span.TraceID() {
		t.Fatalf("the node's span joined trace %q, not the control trace %q", child.TraceID(), span.TraceID())
	}
	if child.SpanID() == span.SpanID() {
		t.Fatal("the node reused the control span's id instead of becoming its child")
	}
}
