package sim

// Plan B B30 §15.4 item "many simultaneous reconnecting cursors", measured
// against the §15.5 ceilings: goroutines, memory, descriptors and retained
// per-peer state.
//
// Scale chosen and why. Plan B names 10,000 simultaneous cursors. In this
// simulator a cursor is not a cheap handle: every one is a real client with
// its own in-memory pipe, transport peer, relay Serve goroutine, control-plane
// peer registration and a node-side subscriber goroutine pumping a session
// log. Measured here, each cursor costs six live goroutines and the pair of
// 256-frame pipe buffers behind it, so 10,000 simultaneous cursors is roughly
// 60,000 goroutines: a machine-sized run, not a laptop-sized one. A number
// that cannot be run is worth less than a smaller one that is.
//
// The default is 400 simultaneous cursors driven through three identical
// connect / attach / partition / reattach / close cycles. What that proves is
// a per-cycle *slope*, not an absolute number: if 400 cursors establish and
// release without the later cycles costing more goroutines, heap or
// descriptors than the earlier ones, per-cursor state is released on teardown,
// and 10,000 differs only by the constant per-cursor cost this test prints.
// Set REMOUNT_SCALE_N to run the same code at any width; the assertions are
// width-independent.
//
// The cursors deliberately do not use world.dialer. That helper appends every
// connection it ever handed out to world.conns and only removes the ones a
// test cuts by name, so a client that is closed rather than cut leaves a
// closed pipe and its two frame buffers retained by the harness. That is test
// scaffolding, not product state, and at this width it is large enough to be
// mistaken for a leak, so these cursors dial through a fleet that forgets a
// connection when it closes it.

import (
	"context"
	"fmt"
	"math/rand"
	"sync"
	"testing"
	"time"

	"remount.dev/remount/internal/client"
	"remount.dev/remount/internal/node"
	"remount.dev/remount/internal/proto"
	"remount.dev/remount/internal/server"
	"remount.dev/remount/internal/transport"
)

// planbScaleFleet owns a generation of cursor clients and every connection
// they dialed, so a cycle can be torn down leaving nothing behind.
type planbScaleFleet struct {
	world   *world
	mu      sync.Mutex
	conns   []transport.Conn
	clients []*client.Client
	cursors []*client.Session
}

func newPlanbScaleFleet(w *world) *planbScaleFleet { return &planbScaleFleet{world: w} }

// dial hands out a connection into the running server and records it so cut
// can sever it. Closing it removes it from the fleet's memory as well.
func (f *planbScaleFleet) dialer() transport.Dialer {
	return transport.DialFunc(func(ctx context.Context) (transport.Conn, error) {
		a, b := transport.Pipe(256)
		go f.world.srv.AcceptConn(f.world.ctx, b)
		f.mu.Lock()
		f.conns = append(f.conns, a)
		f.mu.Unlock()
		return a, nil
	})
}

// client adds one cursor client to the fleet.
func (f *planbScaleFleet) client(name string) *client.Client {
	c := client.New(client.Options{
		Dialer: f.dialer(), Token: "tok", Principal: "a_" + name,
		ArtifactURL: f.world.http.URL + "/v1/artifacts",
	})
	f.mu.Lock()
	f.clients = append(f.clients, c)
	f.mu.Unlock()
	return c
}

// cut severs a seeded random subset of the fleet's live connections, modelling
// a partition that takes most but not all cursors. Leaving some connected is
// deliberate: it exercises the path where a node releases one client's
// subscriber while other subscribers on the same session stay live, which a
// cut-everything test never reaches. The clients that were cut redial.
func (f *planbScaleFleet) cut(rng *rand.Rand, fraction float64) int {
	f.mu.Lock()
	conns := f.conns
	f.conns = nil
	f.mu.Unlock()
	rng.Shuffle(len(conns), func(i, j int) { conns[i], conns[j] = conns[j], conns[i] })
	sever := int(float64(len(conns)) * fraction)
	if sever < 1 && len(conns) > 0 {
		sever = 1
	}
	for _, c := range conns[:sever] {
		_ = c.Close()
	}
	// Whatever survived is still the fleet's to release at close.
	f.mu.Lock()
	f.conns = append(f.conns, conns[sever:]...)
	f.mu.Unlock()
	return sever
}

// close detaches every cursor, closes every client, and releases every
// connection the fleet is still holding.
func (f *planbScaleFleet) close(ctx context.Context) {
	f.mu.Lock()
	cursors, clients, conns := f.cursors, f.clients, f.conns
	f.cursors, f.clients, f.conns = nil, nil, nil
	f.mu.Unlock()
	for _, s := range cursors {
		// Detach, never kill: the producer must survive for the next cycle.
		_ = s.Close(ctx, false)
	}
	for _, c := range clients {
		_ = c.Close()
	}
	for _, c := range conns {
		_ = c.Close()
	}
}

// TestPlanBScaleReconnectingCursorsReleaseTheirState opens N cursors on one
// live session, partitions a seeded three-quarters of them mid-stream, lets
// them reattach beside the cursors that never lost their connection, then
// closes them all. Three cycles; the later ones must not cost more than the
// first.
func TestPlanBScaleReconnectingCursorsReleaseTheirState(t *testing.T) {
	if testing.Short() {
		t.Skip("simultaneous-cursor scale evidence is not a short test")
	}
	cursors := planbScaleSizeFromEnv(t, "REMOUNT_SCALE_CURSORS", 400, 40)
	rng := planbScaleRand(t)

	w := newWorldWith(t, func(o *server.Options) {
		o.LeaseSec = 300
	})
	w.nodeWith("n1", func(o *node.Options) {
		o.MaxSessions = 64
		o.MaxActiveSessions = 16
		o.MaxSessionsPerWorkspace = 64
		o.MaxSessionsPerPrincipal = 64
		o.MaxConcurrentRequests = 256
	})
	driver := w.client("cursor-driver")
	ws := mustWS(t, driver, proto.WorkspaceSpec{})
	ctx := ctxT(t, 20*time.Minute)

	// One long-lived producer emitting a slow heartbeat. Every cursor attaches
	// to this one log, so the work each cursor does is identical between
	// cycles and the log itself grows by only a few hundred bytes across the
	// whole run, which keeps it out of the heap comparison.
	producer, err := driver.Exec(ctx, proto.SOpenReq{WS: ws.ID, NoSubscribe: true, Program: []string{"sh", "-c",
		"i=0; while [ $i -lt 4000 ]; do echo cursor-line-$i; i=$((i+1)); sleep 0.1; done"}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		stop, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		_ = producer.Close(stop, true)
	})

	baseline := planbScaleFloor(5 * time.Second)
	basePeers := planbScaleMetrics()["remount_peers_connected"]
	t.Logf("baseline before any cursor: %s peers=%g", baseline, basePeers)

	var cycles []planbScaleSample
	var perCursorGoroutines int
	for cycle := 1; cycle <= 3; cycle++ {
		before := planbScaleMetrics()
		fleet := newPlanbScaleFleet(w)
		planbScaleOpenCursors(t, fleet, ws.ID, producer.ID, cycle, cursors)

		peak := planbScaleMeasure()
		perCursorGoroutines = (peak.Goroutines - baseline.Goroutines) / cursors
		t.Logf("cycle %d with %d cursors attached: %s peers=%g (%d goroutines per cursor)",
			cycle, cursors, peak, planbScaleMetrics()["remount_peers_connected"], perCursorGoroutines)

		// Partition most of the fleet. The client SDK redials and reattaches
		// from its own cursor position; the node must release the subscriber
		// belonging to each connection that died, while the subscribers of the
		// cursors that stayed connected keep streaming untouched.
		severed := fleet.cut(rng, 0.75)
		if severed == 0 {
			t.Fatalf("cycle %d severed no cursor connections", cycle)
		}
		t.Logf("cycle %d severed %d of %d cursor connections", cycle, severed, cursors)
		planbScaleAwaitCursorChunks(t, ctx, fleet, cycle, "after cut")

		fleet.close(ctx)

		// Teardown is asynchronous: wait for the relay to observe every
		// disconnect before concluding anything about retained state.
		planbScaleAwaitPeers(t, basePeers, 60*time.Second)
		settled := planbScaleFloor(20 * time.Second)
		after := planbScaleMetrics()
		t.Logf("cycle %d settled after closing %d cursors: %s peers=%g dropped_frames=+%g gaps=+%g",
			cycle, cursors, settled, after["remount_peers_connected"],
			planbScaleDelta(before, after, "remount_frames_dropped_total"),
			planbScaleDelta(before, after, "remount_session_gaps_total"))
		cycles = append(cycles, settled)
	}

	// Cycle one pays one-off costs (grants, arenas, SQLite pages). The leak
	// question is whether cycles two and three keep paying them.
	first, last := cycles[1], cycles[len(cycles)-1]
	if last.Goroutines > first.Goroutines+16 {
		t.Errorf("goroutines grew across identical %d-cursor cycles: %d then %d (a per-cursor leak would be ~%d)",
			cursors, first.Goroutines, last.Goroutines, cursors*perCursorGoroutines)
	}
	if growth := planbScaleGrowth(first.HeapInUse, last.HeapInUse); growth > 0.20 {
		t.Errorf("heap in use grew %.0f%% across identical %d-cursor cycles: %s then %s",
			growth*100, cursors, planbScaleBytes(first.HeapInUse), planbScaleBytes(last.HeapInUse))
	}
	if first.Descriptor > 0 && last.Descriptor > first.Descriptor+16 {
		t.Errorf("descriptors grew across identical %d-cursor cycles: %d then %d",
			cursors, first.Descriptor, last.Descriptor)
	}
	if last.Goroutines > baseline.Goroutines+32 {
		t.Errorf("goroutines never returned to the pre-cursor floor: baseline %d, after %d cycles %d",
			baseline.Goroutines, len(cycles), last.Goroutines)
	}
}

// planbScaleOpenCursors connects count cursors and proves each one is really
// streaming before the test does anything else to them.
func planbScaleOpenCursors(t *testing.T, f *planbScaleFleet, wsID, sessionID string, cycle, count int) {
	t.Helper()
	ctx := ctxT(t, 10*time.Minute)
	for i := 0; i < count; i++ {
		cl := f.client(fmt.Sprintf("cur-%d-%04d", cycle, i))
		s, err := cl.Attach(ctx, wsID, sessionID, 0)
		if err != nil {
			t.Fatalf("cycle %d cursor %d attach: %v", cycle, i, err)
		}
		f.mu.Lock()
		f.cursors = append(f.cursors, s)
		f.mu.Unlock()
	}
	planbScaleAwaitCursorChunks(t, ctx, f, cycle, "after attach")
}

// planbScaleAwaitCursorChunks requires every cursor to deliver at least one
// chunk. Counting connections would prove only that a socket exists; a
// delivered chunk proves the whole path is live, which is what makes the
// teardown measurement afterwards mean anything.
func planbScaleAwaitCursorChunks(t *testing.T, ctx context.Context, f *planbScaleFleet, cycle int, stage string) {
	t.Helper()
	f.mu.Lock()
	cursors := append([]*client.Session(nil), f.cursors...)
	f.mu.Unlock()
	deadline, cancel := context.WithTimeout(ctx, 120*time.Second)
	defer cancel()
	for i, s := range cursors {
		select {
		case chunk, ok := <-s.Chunks():
			if !ok {
				t.Fatalf("cycle %d cursor %d log closed %s", cycle, i, stage)
			}
			if chunk.Stream == proto.StreamGap {
				t.Fatalf("cycle %d cursor %d got a gap %s: the live log was evicted under it", cycle, i, stage)
			}
		case <-deadline.Done():
			t.Fatalf("cycle %d cursor %d delivered no chunk %s", cycle, i, stage)
		}
	}
}

// planbScaleAwaitPeers waits for the relay's connected-peer gauge to come back
// to want. It is the system's own statement about how much per-connection
// state it still holds.
func planbScaleAwaitPeers(t *testing.T, want float64, within time.Duration) {
	t.Helper()
	deadline := time.Now().Add(within)
	for {
		got := planbScaleMetrics()["remount_peers_connected"]
		if got <= want {
			return
		}
		if time.Now().After(deadline) {
			t.Errorf("relay still reports %g connected peers, want %g: per-connection state was not released", got, want)
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
}
