package sim

// Plan B B30 §15.4 item "event and export backpressure", judged by §15.5:
// "Every dropped or rejected unit needs a metric and an explicit result."
//
// Three properties are separated here on purpose, because they fail
// independently:
//
//  1. the retained event log has a ceiling, and what it drops is counted and
//     reported to a reader as an explicit eviction rather than a short read;
//  2. a subscriber that cannot keep up loses events only with a metric and a
//     result its consumer can act on; and
//  3. the number of live subscriptions one requester may hold is itself
//     bounded, since each one costs a goroutine and a fan-out slot for the
//     lifetime of the connection.

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"remount.dev/remount/internal/eventlog"
	"remount.dev/remount/internal/proto"
	"remount.dev/remount/internal/server"
	"remount.dev/remount/internal/transport"
)

type planbScaleExportSink struct {
	maxBatch int
	rejectAt int
	calls    int
	accepted int
	rejected int
	oversize int
}

func (s *planbScaleExportSink) Send(_ context.Context, events []proto.Event) error {
	s.calls++
	if len(events) > s.maxBatch {
		s.oversize++
	}
	if s.calls == s.rejectAt {
		s.rejected += len(events)
		return errors.New("destination backpressure")
	}
	s.accepted += len(events)
	return nil
}

func TestPlanBScaleExportBackpressureKeepsBoundedBatchAndCursor(t *testing.T) {
	events := planbScaleSize(t, 1200, 400)
	const batchEvents = 64

	log := eventlog.New(eventlog.NewMemory(0))
	defer log.Close()
	for i := 0; i < events; i++ {
		if err := log.Append(context.Background(), &proto.Event{
			Type: "planb.scale.export", Payload: proto.MustMarshal(map[string]int{"i": i}),
		}); err != nil {
			t.Fatal(err)
		}
	}
	cursors, err := eventlog.NewMemoryCursorStore(1)
	if err != nil {
		t.Fatal(err)
	}
	blocked := &planbScaleExportSink{maxBatch: batchEvents, rejectAt: 4}
	last, err := eventlog.RunExport(context.Background(), log, blocked, cursors, eventlog.RunOptions{
		Name: "planb-scale", From: 1, BatchEvents: batchEvents, BatchBytes: 64 << 10,
	})
	if err == nil || err.Error() != "destination backpressure" {
		t.Fatalf("backpressured export error = %v", err)
	}
	if last != 3*batchEvents || blocked.accepted != 3*batchEvents || blocked.rejected != batchEvents || blocked.oversize != 0 {
		t.Fatalf("backpressured export last=%d accepted=%d rejected=%d oversize=%d",
			last, blocked.accepted, blocked.rejected, blocked.oversize)
	}
	cursor, err := cursors.Load(context.Background(), "planb-scale")
	if err != nil {
		t.Fatal(err)
	}
	if cursor.Next != last+1 {
		t.Fatalf("cursor advanced past rejected batch: %+v after last=%d", cursor, last)
	}

	recovered := &planbScaleExportSink{maxBatch: batchEvents}
	last, err = eventlog.RunExport(context.Background(), log, recovered, cursors, eventlog.RunOptions{
		Name: "planb-scale", From: 1, BatchEvents: batchEvents, BatchBytes: 64 << 10,
	})
	if err != nil || last != uint64(events) {
		t.Fatalf("recovered export = (%d, %v), want (%d, nil)", last, err, events)
	}
	if recovered.accepted != events-3*batchEvents || recovered.rejected != 0 || recovered.oversize != 0 {
		t.Fatalf("recovered export accepted=%d rejected=%d oversize=%d",
			recovered.accepted, recovered.rejected, recovered.oversize)
	}
	t.Logf("%d events exported in batches of at most %d; %d rejected events were explicit and retried from cursor %d",
		events, batchEvents, blocked.rejected, cursor.Next)
}

// TestPlanBScaleEventLogRetentionCeilingIsCountedAndExplicit drives the
// canonical log past its retained-event ceiling and requires the eviction to
// be both counted and visible: a reader asking for an evicted range gets
// CodeEvicted naming the oldest surviving sequence, never a silently
// truncated answer.
func TestPlanBScaleEventLogRetentionCeilingIsCountedAndExplicit(t *testing.T) {
	const ceiling = 200
	posts := planbScaleSize(t, 1200, 400)

	w := newWorldWith(t, func(o *server.Options) {
		o.MaxEvents = ceiling
		o.EventGCInterval = 25 * time.Millisecond
		o.EventRetention = time.Hour // size, not age, is the ceiling under test
	})
	c := w.client("c1")
	ctx := ctxT(t, 120*time.Second)

	before := planbScaleMetrics()
	for i := 0; i < posts; i++ {
		if err := c.PostEvent(ctx, proto.Event{Type: "planb.scale.backpressure", Payload: proto.MustMarshal(map[string]int{"i": i})}); err != nil {
			t.Fatalf("post %d: %v", i, err)
		}
	}

	// The retention pass is asynchronous, so wait for the ceiling to bind
	// rather than assuming the last post already triggered it.
	deadline := time.Now().Add(30 * time.Second)
	var pruned float64
	for {
		pruned = planbScaleDelta(before, planbScaleMetrics(), "remount_events_pruned_total")
		if pruned >= float64(posts-ceiling) {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("posted %d events under a %d-event ceiling and only %g were pruned and counted", posts, ceiling, pruned)
		}
		time.Sleep(25 * time.Millisecond)
	}
	t.Logf("%d events posted under a %d-event ceiling: remount_events_pruned_total +%g", posts, ceiling, pruned)

	// Every removed event must be counted, and nothing beyond the ceiling may
	// survive: the retained set is the bound, the counter is the evidence.
	if pruned > float64(posts) {
		t.Errorf("pruned %g events after posting %d: the counter over-reports", pruned, posts)
	}

	// The explicit result: an evicted range is an error naming the oldest
	// sequence that still exists, not a shorter answer that reads as complete.
	_, err := c.ReadEvents(ctx, 1, "")
	if err == nil {
		t.Fatal("reading from an evicted sequence succeeded: eviction was reported as a complete read")
	}
	if got := codeOf(err); got != proto.CodeEvicted {
		t.Fatalf("read of an evicted range: code %q, want %q (%v)", got, proto.CodeEvicted, err)
	}
	var pe *proto.Error
	if !errors.As(err, &pe) || pe.Oldest == 0 {
		t.Fatalf("evicted read did not name the oldest surviving sequence: %+v", err)
	}
	t.Logf("evicted read reports oldest=%d", pe.Oldest)

	// Recovery: the asynchronous retention pass may advance again between
	// requests while the final posts are still being collected. Follow each
	// explicit watermark until the retained window stabilizes.
	oldest := pe.Oldest
	var events []proto.Event
	for {
		events, err = c.ReadEvents(ctx, oldest, "")
		if err == nil {
			break
		}
		if !errors.As(err, &pe) || pe.Code != proto.CodeEvicted || pe.Oldest <= oldest {
			t.Fatalf("read from reported oldest sequence %d: %v", oldest, err)
		}
		oldest = pe.Oldest
	}
	if len(events) == 0 {
		t.Fatal("read from the reported oldest sequence returned nothing")
	}
}

// TestPlanBScaleSlowEventSubscriberLossIsCountedAndExplicit is the hard half
// of the §15.5 property. A subscriber that stops reading is backpressure; the
// system may drop its events, but the drop has to be counted and the consumer
// has to be able to tell that it happened.
func TestPlanBScaleSlowEventSubscriberLossIsCountedAndExplicit(t *testing.T) {
	posts := planbScaleSize(t, 4000, 1500)

	w := newWorld(t)
	c := w.client("c1")
	ctx := ctxT(t, 120*time.Second)

	tail, err := c.TailEvents(ctx, 0, "")
	if err != nil {
		t.Fatal(err)
	}

	// Deliberately read nothing while the log runs away from the subscriber.
	before := planbScaleMetrics()
	for i := 0; i < posts; i++ {
		if err := c.PostEvent(ctx, proto.Event{Type: "planb.scale.slow-tail", Payload: proto.MustMarshal(map[string]int{"i": i})}); err != nil {
			t.Fatalf("post %d: %v", i, err)
		}
	}

	// Now drain whatever survived, and find out how the stream ended.
	delivered, closed := planbScaleDrainTail(tail, 20*time.Second)
	after := planbScaleMetrics()
	t.Logf("slow subscriber: posted %d, delivered %d, stream closed=%v", posts, delivered, closed)

	if delivered >= posts {
		t.Skipf("the subscriber kept up with %d events; raise REMOUNT_SCALE_N to create backpressure", posts)
	}

	// Something was lost. §15.5 requires two things of that loss, and the
	// system currently provides neither, so both are asserted separately.
	dropped := 0.0
	for name, value := range after {
		if value == before[name] {
			continue
		}
		switch name {
		case "remount_event_subscriber_drops_total", "remount_event_subscribers_dropped_total",
			"remount_events_dropped_total", "remount_notification_unavailable_total":
			dropped += value - before[name]
		}
	}
	if dropped == 0 {
		t.Errorf("a slow subscriber lost %d of %d events and no counter moved: "+
			"an uncounted drop is indistinguishable from delivery", posts-delivered, posts)
	}
	if closed {
		t.Errorf("the subscription was terminated after losing %d of %d events, and a closed channel "+
			"is the same result a caller sees after cancelling: the loss is not distinguishable from a clean end",
			posts-delivered, posts)
	}
}

// TestPlanBScaleEventTailSubscriptionsAreBounded checks the retained-state
// ceiling behind those subscriptions. Every live tail costs the control plane
// a goroutine, an event-log fan-out slot and a 256-event buffered channel for
// as long as the connection lives, and one authenticated requester can open
// as many as it likes simply by naming a new subscription id. That is
// retained state, so it needs admission, an accounting, a rejection code and
// a metric, exactly as every other retained collection in this system has.
//
// The tails are opened over a raw peer rather than through the SDK so the
// measurement is the control plane's own cost per subscription, with none of
// the client's per-subscription buffers mixed in.
func TestPlanBScaleEventTailSubscriptionsAreBounded(t *testing.T) {
	tails := planbScaleSize(t, 2000, 400)

	w := newWorld(t)
	ctx := ctxT(t, 300*time.Second)
	peer := planbScaleRawPeer(t, ctx, w, "planb-scale-tails")

	baseline := planbScaleFloor(5 * time.Second)
	before := planbScaleMetrics()

	opened, refused := 0, 0
	var firstRefusal error
	for i := 0; i < tails; i++ {
		// One peer, one connection, a fresh subscription id each time: the
		// shape that makes this retained state rather than per-connection.
		call, cancel := context.WithTimeout(ctx, 15*time.Second)
		err := peer.Call(call, proto.PeerControl, proto.OpEventsTail, proto.EventsTailReq{
			Follow: true, Subscription: fmt.Sprintf("planb-scale-%05d", i),
		}, nil)
		cancel()
		if err == nil {
			opened++
			continue
		}
		refused++
		if firstRefusal == nil {
			firstRefusal = err
		}
	}
	peak := planbScaleMeasure()
	after := planbScaleMetrics()
	perTail := 0.0
	if opened > 0 {
		perTail = float64(peak.HeapInUse-baseline.HeapInUse) / float64(opened)
	}
	t.Logf("one peer opened %d control-plane event tails (%d refused): %s; +%d goroutines and %s of heap per tail",
		opened, refused, peak, (peak.Goroutines-baseline.Goroutines)/max(opened, 1), planbScaleBytes(uint64(perTail)))

	if refused == 0 {
		t.Errorf("one authenticated peer opened %d simultaneous event-tail subscriptions on one connection and none was refused: "+
			"retained subscription state has no ceiling, no rejection code and no metric (each tail costs a control-plane "+
			"goroutine, a 256-event buffered channel and an event-log fan-out slot, so every appended event is fanned out "+
			"to all %d of them under one lock)", opened, opened)
	}
	if refused > 0 {
		if code := codeOf(firstRefusal); code != proto.CodeResourceExhausted {
			t.Errorf("refused tail code = %q, want %q (%v)", code, proto.CodeResourceExhausted, firstRefusal)
		}
		planbScaleCountedRejections(t, before, after, "remount_event_tail_quota_rejections_total", refused)
	}
}

// planbScaleRawPeer dials an authenticated client peer whose handler discards
// everything the control plane pushes at it, so a test can hold many
// server-side subscriptions without the SDK's own bookkeeping.
func planbScaleRawPeer(t *testing.T, ctx context.Context, w *world, name string) *transport.Peer {
	t.Helper()
	conn, err := w.dialer(name).Dial(ctx)
	if err != nil {
		t.Fatalf("dial %s: %v", name, err)
	}
	peer := transport.NewPeer(conn, transport.HandlerFunc(func(context.Context, *transport.Peer, *proto.Frame) {}))
	hctx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	if _, err := transport.Hello(hctx, peer, proto.Hello{
		Peer: "c_" + name, Role: proto.RoleClient, Token: "tok", Caps: []string{proto.CapabilityV1},
	}); err != nil {
		t.Fatalf("hello %s: %v", name, err)
	}
	t.Cleanup(func() { _ = peer.Close() })
	return peer
}

// planbScaleDrainTail reads a tail until it stops producing or closes, and
// reports how many events it yielded and whether it ended by closing.
func planbScaleDrainTail(tail <-chan proto.Event, idle time.Duration) (int, bool) {
	delivered := 0
	for {
		select {
		case _, ok := <-tail:
			if !ok {
				return delivered, true
			}
			delivered++
		case <-time.After(idle):
			return delivered, false
		}
	}
}
