package sim

// Plan B B17: restart the control plane with claimed workspaces; stale
// grants, stale nodes and stale ready messages are refused.
//
// The point is not that a restart survives — internal/control already has
// durable-state tests for that. It is that after the restart the control
// plane's authority still comes from its own committed rows, so a peer that
// speaks for a superseded assignment is refused through the protocol rather
// than believed because it sounds plausible.

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"remount.dev/remount/internal/client"
	"remount.dev/remount/internal/node"
	"remount.dev/remount/internal/proto"
	"remount.dev/remount/internal/server"
	"remount.dev/remount/internal/transport"
)

// planBControlGeneration is one running control plane process.
type planBControlGeneration struct {
	server  *server.Server
	handler http.Handler
}

// planBControlWorld is a world whose control plane can be restarted over its
// own durable state. The shared sim world binds one server for the lifetime
// of the test, which is right for every other scenario and wrong for this
// one; the topology is otherwise the same: real nodes and clients over
// in-memory pipes.
type planBControlWorld struct {
	t      *testing.T
	opts   server.Options
	http   *httptest.Server
	ctx    context.Context
	cancel context.CancelFunc

	live atomic.Pointer[planBControlGeneration]

	mu       sync.Mutex
	closing  bool
	conns    []transport.Conn
	clients  []*client.Client
	cancels  map[string]context.CancelFunc
	done     map[string]<-chan error
	acceptWG sync.WaitGroup
}

// newPlanBControlWorld starts a control plane whose lease is long enough that
// nothing in this scenario expires by accident: B17 is about a restart, and a
// lease-expiry failover in the middle of it would prove something else.
func newPlanBControlWorld(t *testing.T, leaseSec int64) *planBControlWorld {
	t.Helper()
	opts := server.Options{
		DataDir: filepath.Join(t.TempDir(), "control"), Token: "tok", LeaseSec: leaseSec,
		Logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
	}
	srv, err := server.New(opts)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	w := &planBControlWorld{
		t: t, opts: opts, ctx: ctx, cancel: cancel,
		cancels: map[string]context.CancelFunc{}, done: map[string]<-chan error{},
	}
	w.live.Store(&planBControlGeneration{server: srv, handler: srv.Handler()})
	w.http = httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, req *http.Request) {
		live := w.live.Load()
		if live == nil {
			http.Error(rw, "control plane is restarting", http.StatusServiceUnavailable)
			return
		}
		live.handler.ServeHTTP(rw, req)
	}))
	t.Cleanup(w.close)
	return w
}

func (w *planBControlWorld) dialer() transport.Dialer {
	return transport.DialFunc(func(context.Context) (transport.Conn, error) {
		live := w.live.Load()
		if live == nil {
			return nil, errors.New("control plane is restarting")
		}
		a, b := transport.Pipe(256)
		w.mu.Lock()
		if w.closing {
			w.mu.Unlock()
			_ = a.Close()
			return nil, transport.ErrClosed
		}
		w.conns = append(w.conns, a)
		w.acceptWG.Add(1)
		w.mu.Unlock()
		go func(srv *server.Server) {
			defer w.acceptWG.Done()
			_ = srv.AcceptConn(w.ctx, b)
		}(live.server)
		return a, nil
	})
}

func (w *planBControlWorld) node(name string, labels map[string]string) *node.Node {
	w.t.Helper()
	n, err := node.New(node.Options{
		DataDir: filepath.Join(w.t.TempDir(), name), Dialer: w.dialer(), Token: w.opts.Token,
		ArtifactURL: w.http.URL + "/v1/artifacts", Labels: labels,
		Allow: []string{"127.0.0.1"}, AllowPrivate: []string{"127.0.0.1", "localhost"},
	})
	if err != nil {
		w.t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(w.ctx)
	done := make(chan error, 1)
	w.mu.Lock()
	w.cancels[name], w.done[name] = cancel, done
	w.mu.Unlock()
	go func() {
		done <- n.Run(ctx)
		close(done)
	}()
	select {
	case <-n.Online():
	case <-time.After(15 * time.Second):
		w.t.Fatalf("node %s never came online", name)
	}
	return n
}

func (w *planBControlWorld) client(name string) *client.Client {
	c := client.New(client.Options{
		Dialer: w.dialer(), Token: w.opts.Token, Principal: "a_" + name,
		ArtifactURL: w.http.URL + "/v1/artifacts", Retries: 20,
	})
	w.mu.Lock()
	w.clients = append(w.clients, c)
	w.mu.Unlock()
	return c
}

// restartControl closes the running control plane and opens a new one over
// the same SQLite state. Peers redial and re-announce themselves; none of the
// authority they hold is re-derived from what they say.
func (w *planBControlWorld) restartControl() {
	w.t.Helper()
	previous := w.live.Swap(nil)
	if previous == nil {
		w.t.Fatal("control plane is not running")
	}
	w.mu.Lock()
	stale := w.conns
	w.conns = nil
	w.mu.Unlock()
	if err := previous.server.Close(); err != nil {
		w.t.Fatalf("close control plane: %v", err)
	}
	for _, conn := range stale {
		_ = conn.Close()
	}
	next, err := server.New(w.opts)
	if err != nil {
		w.t.Fatalf("reopen durable control state: %v", err)
	}
	w.live.Store(&planBControlGeneration{server: next, handler: next.Handler()})
}

func (w *planBControlWorld) close() {
	w.mu.Lock()
	w.closing = true
	conns, clients := w.conns, w.clients
	w.conns, w.clients = nil, nil
	cancels := make(map[string]context.CancelFunc, len(w.cancels))
	dones := make(map[string]<-chan error, len(w.done))
	for name, cancel := range w.cancels {
		cancels[name] = cancel
	}
	for name, done := range w.done {
		dones[name] = done
	}
	w.mu.Unlock()

	live := w.live.Swap(nil)
	w.cancel()
	for _, c := range clients {
		_ = c.Close()
	}
	for _, cancel := range cancels {
		cancel()
	}
	for _, conn := range conns {
		_ = conn.Close()
	}
	if live != nil {
		_ = live.server.Close()
	}
	w.http.Close()
	for name, done := range dones {
		select {
		case err := <-done:
			if err != nil {
				w.t.Errorf("node %s shutdown: %v", name, err)
			}
		case <-time.After(15 * time.Second):
			w.t.Errorf("node %s did not finish shutdown", name)
		}
	}
	w.acceptWG.Wait()
}

// planBRawClientPeer is the fixed identity the B17 raw client redials under.
// A grant names the client it was issued to, so a reconnect that changed the
// peer id would make every later refusal meaningless.
const planBRawClientPeer = "c_b17raw"

func TestPlanBContinuityControlRestartRefusesStaleAuthority(t *testing.T) {
	w := newPlanBControlWorld(t, 60)
	holderNode := w.node("b17-n1", map[string]string{"zone": "b17"})
	otherNode := w.node("b17-n2", map[string]string{"zone": "b17"})
	c := w.client("b17-c1")
	ctx := ctxT(t, 5*time.Minute)
	placement := proto.Placement{Allow: map[string]string{"zone": "b17"}}
	created, err := c.CreateWorkspace(ctx, proto.WorkspaceSpec{Name: "b17", Placement: placement})
	if err != nil {
		t.Fatal(err)
	}
	before, err := c.WaitClaimed(ctx, created.ID)
	if err != nil {
		t.Fatal(err)
	}
	if before.Node != holderNode.ID() && before.Node != otherNode.ID() {
		t.Fatalf("unknown holder in %s", continuityRow(before))
	}
	if err := c.WriteFile(ctx, before.ID, "state.txt", []byte("v1"), 0); err != nil {
		t.Fatal(err)
	}

	// A raw client peer that never refreshes its grant, so the fence has to
	// answer rather than the SDK's retry.
	raw := continuityRawClient(t, ctx, w.dialer(), "tok", planBRawClientPeer)
	grant := continuityGrant(t, ctx, raw, before.ID)
	if grant.Claims.Gen != before.Generation || grant.Claims.Node != before.Node {
		t.Fatalf("grant %+v does not match %s", grant.Claims, continuityRow(before))
	}
	if body, err := continuityReadWithGrant(ctx, raw, before.Node, before.ID, "state.txt", grant); err != nil || string(body) != "v1" {
		t.Fatalf("grant did not work before the restart: %q %v", body, err)
	}

	w.restartControl()

	// Durable authority survives: the same node holds the same generation,
	// and the workspace never passed through the other eligible node.
	after := awaitWorkspace(t, ctx, c, before.ID, "claimed again after the control restart", 2*time.Minute,
		func(ws *proto.Workspace) bool { return ws.State == proto.WSClaimed })
	if after.Node != before.Node || after.Generation != before.Generation {
		t.Fatalf("restart moved the authoritative row: %s want %s", continuityRow(after), continuityRow(before))
	}
	// Nothing became stale that should not have: the control key and the
	// assignment are both durable, so a grant issued before the restart still
	// names live authority. The raw peer redials under the same id, which is
	// what the grant is bound to.
	raw = continuityRawClient(t, ctx, w.dialer(), "tok", planBRawClientPeer)
	if body, err := continuityReadWithGrant(ctx, raw, after.Node, before.ID, "state.txt", grant); err != nil || string(body) != "v1" {
		t.Fatalf("a grant for the unchanged assignment was refused after the restart: %q %v", body, err)
	}

	// A node peer speaking for an assignment it does not hold. It advertises
	// no backend, so it can never be placed on; its only job is to say the
	// things a stale node says.
	stale := continuityRawNode(t, ctx, w.dialer(), "tok", "n_b17stale000000000000000000")
	readyCases := []struct {
		what string
		gen  uint64
	}{
		{"the current generation", after.Generation},
		{"a generation that never existed", after.Generation + 5},
		{"the pre-claim generation", 0},
	}
	for _, tc := range readyCases {
		err := stale.Call(ctx, proto.PeerControl, proto.OpWSReady, proto.WSReadyReq{ID: before.ID, Gen: tc.gen}, nil)
		if code := continuityCode(err); code != proto.CodeConflict {
			t.Fatalf("ws.ready for %s from a node that does not hold %s returned %v (code %q), want conflict",
				tc.what, before.ID, err, code)
		}
	}
	claimErr := stale.Call(ctx, proto.PeerControl, proto.OpWSClaim, proto.WSClaimReq{ID: before.ID}, &proto.WSClaimRes{})
	if code := continuityCode(claimErr); code != proto.CodeConflict {
		t.Fatalf("a claim of an already-claimed workspace returned %v (code %q), want conflict", claimErr, code)
	}
	// Renewal is the standing authority question, and the answer to a node
	// that is not the holder is to fence, never to continue.
	var renewed proto.WSRenewRes
	if err := stale.Call(ctx, proto.PeerControl, proto.OpWSRenew, proto.WSRenewReq{
		IDs: []string{before.ID}, Gen: map[string]uint64{before.ID: after.Generation},
	}, &renewed); err != nil {
		t.Fatalf("ws.renew from a stale node: %v", err)
	}
	if len(renewed.Results) != 1 {
		t.Fatalf("renew results = %+v", renewed.Results)
	}
	if result := renewed.Results[0]; result.Accepted || result.Action != "fence" || result.AuthoritativeGen != after.Generation {
		t.Fatalf("stale renew = %+v, want accepted=false action=fence authoritative_gen=%d", result, after.Generation)
	}
	// None of it moved the row.
	requireStableWorkspace(t, ctx, c, "stale authority messages must change nothing", 3*time.Second, after)

	// Now advance the generation through the public path. The move is pinned
	// to the node that already holds the workspace, so the grant's client,
	// workspace and node all still match and the generation is the only thing
	// that changed: a refusal can then be nothing but the generation fence.
	// The grant is also still unexpired, because this world's lease outlives
	// the move — asserted, since an expired grant would be refused for a
	// reason that proves nothing about generations.
	moved, err := c.MoveWorkspace(ctx, before.ID, nil, &proto.Placement{Node: after.Node},
		client.WithIdempotencyKey("b17-move"))
	if err != nil {
		t.Fatal(err)
	}
	current, err := c.WaitClaimed(ctx, moved.ID)
	if err != nil {
		t.Fatal(err)
	}
	if current.Node != after.Node || current.Generation != after.Generation+1 {
		t.Fatalf("move produced %s, want node %s at generation %d", continuityRow(current), after.Node, after.Generation+1)
	}
	if grant.Claims.ExpiresAt <= time.Now().UnixMilli() {
		t.Fatal("the grant expired before the generation advanced; this run proves nothing about generations")
	}
	_, err = continuityReadWithGrant(ctx, raw, current.Node, before.ID, "state.txt", grant)
	if code := continuityCode(err); code != proto.CodeConflict {
		t.Fatalf("grant at gen %d against %s returned %v (code %q), want the generation fence",
			grant.Claims.Gen, continuityRow(current), err, code)
	}
	// Positive control: a fresh grant on the same raw peer works.
	refreshed := continuityGrant(t, ctx, raw, before.ID)
	if refreshed.Claims.Gen != current.Generation {
		t.Fatalf("refreshed grant %+v does not match %s", refreshed.Claims, continuityRow(current))
	}
	body, err := continuityReadWithGrant(ctx, raw, current.Node, before.ID, "state.txt", refreshed)
	if err != nil || string(body) != "v1" {
		t.Fatalf("current grant read = %q, %v", body, err)
	}
}
