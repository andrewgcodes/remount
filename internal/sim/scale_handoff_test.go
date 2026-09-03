package sim

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
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

const (
	handoffScaleNodes            = 200
	handoffScaleWorkspaces       = 2_000
	handoffWorkspacesPerNode     = handoffScaleWorkspaces / handoffScaleNodes
	handoffScaleOperationTimeout = 8 * time.Minute
)

// TestHandoffScaleAndControlFailover exercises the Phase 6.4 target without
// replacing peers with mocks. The process backend is deliberate: Docker and
// gVisor need separately gated hosts, while this scenario must run under the
// race detector on an ordinary development machine.
func TestHandoffScaleAndControlFailover(t *testing.T) {
	if testing.Short() {
		t.Skip("200-node/2,000-workspace scale evidence is not a short test")
	}

	baseline := runtime.NumGoroutine()
	t.Run("real-peers-and-durable-restart", func(t *testing.T) {
		w, err := newHandoffScaleWorld(t)
		if err != nil {
			t.Fatal(err)
		}
		defer func() {
			if err := w.Close(); err != nil {
				t.Errorf("close scale world: %v", err)
			}
		}()

		ctx, cancel := context.WithTimeout(context.Background(), handoffScaleOperationTimeout)
		defer cancel()

		nodes, err := w.StartNodes(ctx, handoffScaleNodes)
		if err != nil {
			t.Fatal(err)
		}
		clients := w.StartClients(handoffScaleNodes)

		workspaceIDs := make([][]string, handoffScaleNodes)
		claimLatency := make([]time.Duration, handoffScaleWorkspaces)
		err = runHandoffParallel(handoffScaleNodes, func(i int) error {
			workspaceIDs[i] = make([]string, handoffWorkspacesPerNode)
			for j := 0; j < handoffWorkspacesPerNode; j++ {
				started := time.Now()
				ws, createErr := clients[i].CreateWorkspace(ctx, proto.WorkspaceSpec{
					Name:      fmt.Sprintf("scale-%03d-%02d", i, j),
					Requires:  proto.Requires{Backend: "process"},
					Placement: proto.Placement{Node: nodes[i].ID()},
				})
				if createErr != nil {
					return fmt.Errorf("node %d workspace %d create: %w", i, j, createErr)
				}
				ws, createErr = clients[i].WaitClaimed(ctx, ws.ID)
				if createErr != nil {
					return fmt.Errorf("node %d workspace %d claim: %w", i, j, createErr)
				}
				if ws.Node != nodes[i].ID() || ws.Generation != 1 {
					return fmt.Errorf("node %d workspace %d claimed by %s at generation %d", i, j, ws.Node, ws.Generation)
				}
				workspaceIDs[i][j] = ws.ID
				claimLatency[i*handoffWorkspacesPerNode+j] = time.Since(started)
			}
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
		logHandoffLatency(t, "claim", claimLatency)

		if err := waitForHandoffPeers(ctx, w.CurrentServer(), handoffScaleNodes*2); err != nil {
			t.Fatal(err)
		}
		if err := verifyHandoffInventory(ctx, clients[0], nodes, workspaceIDs, false); err != nil {
			t.Fatal(err)
		}

		execLatency := make([]time.Duration, handoffScaleNodes)
		err = runHandoffParallel(handoffScaleNodes, func(i int) error {
			started := time.Now()
			stdout, stderr, exit, runErr := clients[i].Run(ctx, workspaceIDs[i][0], "sh", "-c", "printf 'scale-exec'")
			execLatency[i] = time.Since(started)
			if runErr != nil {
				return fmt.Errorf("exec %d: %w", i, runErr)
			}
			if string(stdout) != "scale-exec" || len(stderr) != 0 || exit == nil || exit.Code != 0 {
				return fmt.Errorf("exec %d: stdout=%q stderr=%q exit=%+v", i, stdout, stderr, exit)
			}
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
		logHandoffLatency(t, "exec_round_trip", execLatency)

		moveLatency := make([]time.Duration, handoffScaleNodes)
		err = runHandoffParallel(handoffScaleNodes, func(i int) error {
			target := nodes[(i+1)%handoffScaleNodes].ID()
			started := time.Now()
			if _, moveErr := clients[i].MoveWorkspace(ctx, workspaceIDs[i][0], nil, &proto.Placement{Node: target}); moveErr != nil {
				return fmt.Errorf("move %d submit: %w", i, moveErr)
			}
			ws, moveErr := clients[i].WaitClaimed(ctx, workspaceIDs[i][0])
			moveLatency[i] = time.Since(started)
			if moveErr != nil {
				return fmt.Errorf("move %d claim: %w", i, moveErr)
			}
			if ws.Node != target || ws.Generation != 2 {
				return fmt.Errorf("move %d: node=%s want=%s generation=%d", i, ws.Node, target, ws.Generation)
			}
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
		logHandoffLatency(t, "move", moveLatency)

		sessions := make([]*client.Session, handoffScaleNodes)
		err = runHandoffParallel(handoffScaleNodes, func(i int) error {
			s, openErr := clients[i].Exec(ctx, proto.SOpenReq{
				WS:      workspaceIDs[i][0],
				Kind:    proto.SessionExec,
				Program: []string{"sh", "-c", "printf 'ready\\n'; while IFS= read -r line; do printf 'echo:%s\\n' \"$line\"; [ \"$line\" = stop ] && exit 0; done"},
				Stdin:   true,
			})
			if openErr != nil {
				return fmt.Errorf("open resumable exec %d: %w", i, openErr)
			}
			sessions[i] = s
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}

		collectors := make([]*handoffSessionCollector, handoffScaleNodes)
		for i, s := range sessions {
			collectors[i] = newHandoffSessionCollector(s)
		}
		for i, collector := range collectors {
			select {
			case <-collector.ready:
			case <-ctx.Done():
				t.Fatalf("session %d did not emit its pre-failover readiness marker: %v", i, ctx.Err())
			}
		}

		failoverStarted := time.Now()
		restartLatency, next, err := w.RestartControlPlane()
		if err != nil {
			t.Fatal(err)
		}
		t.Logf("control_restart durable_store=shared_sqlite elapsed=%s", restartLatency)
		if err := waitForHandoffPeers(ctx, next, handoffScaleNodes*2); err != nil {
			t.Fatal(err)
		}
		// Peer hello precedes node resync. Wait for each live session's
		// workspace to pass through claiming/ready before exercising the
		// post-failover grant and input path.
		err = runHandoffParallel(handoffScaleNodes, func(i int) error {
			ws, claimErr := clients[i].WaitClaimed(ctx, workspaceIDs[i][0])
			if claimErr != nil {
				return fmt.Errorf("post-failover claim %d: %w", i, claimErr)
			}
			if ws.Node != nodes[(i+1)%handoffScaleNodes].ID() || ws.Generation != 2 {
				return fmt.Errorf("post-failover claim %d: node=%s generation=%d", i, ws.Node, ws.Generation)
			}
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}

		err = runHandoffParallel(handoffScaleNodes, func(i int) error {
			payload := fmt.Sprintf("resume-%03d\nstop\n", i)
			if inputErr := sessions[i].Input(ctx, []byte(payload), true); inputErr != nil {
				return fmt.Errorf("post-failover input %d: %w", i, inputErr)
			}
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}

		reattachLatency := make([]time.Duration, handoffScaleNodes)
		for i, collector := range collectors {
			result, collectErr := collector.Wait(ctx)
			if collectErr != nil {
				t.Fatalf("session %d collection: %v", i, collectErr)
			}
			want := fmt.Sprintf("ready\necho:resume-%03d\necho:stop\n", i)
			if result.stdout != want || result.stderr != "" || result.gaps != 0 || result.exit == nil || result.exit.Code != 0 {
				t.Fatalf("session %d after failover: stdout=%q want=%q stderr=%q gaps=%d exit=%+v", i, result.stdout, want, result.stderr, result.gaps, result.exit)
			}
			if result.resumeObserved.IsZero() {
				t.Fatalf("session %d never observed its unique post-failover marker", i)
			}
			reattachLatency[i] = result.resumeObserved.Sub(failoverStarted)
		}
		logHandoffLatency(t, "reattach_after_control_restart", reattachLatency)

		if err := verifyHandoffInventory(ctx, clients[0], nodes, workspaceIDs, true); err != nil {
			t.Fatal(err)
		}
		t.Logf("scale_evidence nodes=%d workspaces=%d clients=%d backend=process reconnect_peers=%d", handoffScaleNodes, handoffScaleWorkspaces, handoffScaleNodes, len(next.Relay.Peers()))
		t.Log("backend_evidence docker_benchmark=unavailable gvisor_benchmark=unavailable reason=host-specific runtimes are not modeled by internal/sim; run integration/chaos/backend-gates.sh on each named benchmark host")
	})

	deadline := time.Now().Add(30 * time.Second)
	for runtime.NumGoroutine() > baseline+40 && time.Now().Before(deadline) {
		runtime.GC()
		time.Sleep(100 * time.Millisecond)
	}
	if remaining := runtime.NumGoroutine(); remaining > baseline+40 {
		t.Fatalf("scale scenario leaked goroutines: before=%d after=%d", baseline, remaining)
	}
}

type handoffServerGeneration struct {
	server  *server.Server
	handler http.Handler
}

type handoffScaleWorld struct {
	root   string
	opts   server.Options
	ctx    context.Context
	cancel context.CancelFunc
	http   *httptest.Server

	generation atomic.Pointer[handoffServerGeneration]
	closeOnce  sync.Once
	closeErr   error

	mu          sync.Mutex
	closing     bool
	connections []transport.Conn
	clients     []*client.Client
	nodeCancels []context.CancelFunc
	nodeRunErrs []error

	nodeWG   sync.WaitGroup
	acceptWG sync.WaitGroup
}

func newHandoffScaleWorld(t *testing.T) (*handoffScaleWorld, error) {
	root := t.TempDir()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	opts := server.Options{
		DataDir: filepath.Join(root, "control"),
		Token:   "scale-token",
		// Scale setup intentionally cannot expire the earliest claims while
		// 2,000 SQLite transactions and artifact moves contend for the host.
		LeaseSec:                300,
		MaxConcurrentRequests:   2_048,
		MaxWorkspacesPerTenant:  handoffScaleWorkspaces + 100,
		MaxWorkspacesPerSubject: handoffScaleWorkspaces + 100,
		MaxMutationRecords:      handoffScaleWorkspaces * 4,
		ArtifactGCInterval:      -1,
		EventGCInterval:         -1,
		RecordGCInterval:        -1,
		Logger:                  logger,
	}
	srv, err := server.New(opts)
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithCancel(context.Background())
	w := &handoffScaleWorld{root: root, opts: opts, ctx: ctx, cancel: cancel}
	w.generation.Store(&handoffServerGeneration{server: srv, handler: srv.Handler()})
	w.http = httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, req *http.Request) {
		generation := w.generation.Load()
		if generation == nil {
			http.Error(rw, "control plane unavailable", http.StatusServiceUnavailable)
			return
		}
		generation.handler.ServeHTTP(rw, req)
	}))
	t.Cleanup(func() { _ = w.Close() })
	return w, nil
}

func (w *handoffScaleWorld) CurrentServer() *server.Server {
	generation := w.generation.Load()
	if generation == nil {
		return nil
	}
	return generation.server
}

func (w *handoffScaleWorld) dialer() transport.Dialer {
	return transport.DialFunc(func(context.Context) (transport.Conn, error) {
		generation := w.generation.Load()
		if generation == nil {
			return nil, errors.New("scale control plane is restarting")
		}
		a, b := transport.Pipe(512)
		w.mu.Lock()
		if w.closing {
			w.mu.Unlock()
			_ = a.Close()
			return nil, transport.ErrClosed
		}
		w.connections = append(w.connections, a)
		w.acceptWG.Add(1)
		w.mu.Unlock()
		go func(srv *server.Server) {
			defer w.acceptWG.Done()
			_ = srv.AcceptConn(w.ctx, b)
		}(generation.server)
		return a, nil
	})
}

func (w *handoffScaleWorld) StartNodes(ctx context.Context, count int) ([]*node.Node, error) {
	nodes := make([]*node.Node, count)
	w.nodeRunErrs = make([]error, count)
	w.nodeCancels = make([]context.CancelFunc, count)
	for i := 0; i < count; i++ {
		n, err := node.New(node.Options{
			DataDir:               filepath.Join(w.root, "nodes", fmt.Sprintf("%03d", i)),
			Dialer:                w.dialer(),
			Token:                 w.opts.Token,
			ArtifactURL:           w.http.URL + "/v1/artifacts",
			Allow:                 []string{"127.0.0.1"},
			AllowPrivate:          []string{"127.0.0.1", "localhost"},
			MaxConcurrentRequests: 64,
			MaxSessions:           64,
			MaxActiveSessions:     8,
			Logger:                w.opts.Logger,
		})
		if err != nil {
			return nil, fmt.Errorf("new node %d: %w", i, err)
		}
		nodes[i] = n
		nodeCtx, cancel := context.WithCancel(w.ctx)
		w.nodeCancels[i] = cancel
		w.nodeWG.Add(1)
		go func(index int) {
			defer w.nodeWG.Done()
			w.nodeRunErrs[index] = nodes[index].Run(nodeCtx)
		}(i)
	}
	for i, n := range nodes {
		select {
		case <-n.Online():
		case <-ctx.Done():
			return nil, fmt.Errorf("node %d online: %w", i, ctx.Err())
		}
	}
	return nodes, nil
}

func (w *handoffScaleWorld) StartClients(count int) []*client.Client {
	w.clients = make([]*client.Client, count)
	for i := range w.clients {
		w.clients[i] = client.New(client.Options{
			Dialer:      w.dialer(),
			Token:       w.opts.Token,
			Principal:   fmt.Sprintf("a_scale_%03d", i),
			Retries:     20,
			ArtifactURL: w.http.URL + "/v1/artifacts",
		})
	}
	return w.clients
}

func (w *handoffScaleWorld) RestartControlPlane() (time.Duration, *server.Server, error) {
	started := time.Now()
	old := w.generation.Swap(nil)
	if old == nil {
		return 0, nil, errors.New("scale control plane is not running")
	}
	if err := old.server.Close(); err != nil {
		return time.Since(started), nil, fmt.Errorf("close old control plane: %w", err)
	}
	next, err := server.New(w.opts)
	if err != nil {
		return time.Since(started), nil, fmt.Errorf("open durable control plane state: %w", err)
	}
	w.generation.Store(&handoffServerGeneration{server: next, handler: next.Handler()})
	return time.Since(started), next, nil
}

func (w *handoffScaleWorld) Close() error {
	w.closeOnce.Do(func() {
		w.mu.Lock()
		w.closing = true
		current := w.generation.Swap(nil)
		connections := append([]transport.Conn(nil), w.connections...)
		w.connections = nil
		w.mu.Unlock()
		w.cancel()
		for _, c := range w.clients {
			if c != nil {
				_ = c.Close()
			}
		}
		for _, cancel := range w.nodeCancels {
			if cancel != nil {
				cancel()
			}
		}
		for _, connection := range connections {
			_ = connection.Close()
		}
		if current != nil {
			w.closeErr = errors.Join(w.closeErr, current.server.Close())
		}
		w.http.Close()
		w.nodeWG.Wait()
		w.acceptWG.Wait()
		for i, err := range w.nodeRunErrs {
			if err != nil {
				w.closeErr = errors.Join(w.closeErr, fmt.Errorf("node %d run: %w", i, err))
			}
		}
	})
	return w.closeErr
}

type handoffSessionResult struct {
	stdout         string
	stderr         string
	gaps           int
	exit           *proto.ExitInfo
	resumeObserved time.Time
}

type handoffSessionCollector struct {
	ready chan struct{}
	done  chan struct{}
	mu    sync.Mutex
	res   handoffSessionResult
	err   error
}

func newHandoffSessionCollector(session *client.Session) *handoffSessionCollector {
	collector := &handoffSessionCollector{ready: make(chan struct{}), done: make(chan struct{})}
	go func() {
		defer close(collector.done)
		var stdout, stderr bytes.Buffer
		readyOnce := sync.Once{}
		for chunk := range session.Chunks() {
			switch chunk.Stream {
			case proto.StreamStdout:
				_, _ = stdout.Write(chunk.Data)
				if strings.Contains(stdout.String(), "ready\n") {
					readyOnce.Do(func() { close(collector.ready) })
				}
				if strings.Contains(stdout.String(), "echo:resume-") && collector.res.resumeObserved.IsZero() {
					collector.res.resumeObserved = time.Now()
				}
			case proto.StreamStderr:
				_, _ = stderr.Write(chunk.Data)
			case proto.StreamGap:
				collector.res.gaps++
			}
		}
		readyOnce.Do(func() { close(collector.ready) })
		collector.mu.Lock()
		collector.res.stdout = stdout.String()
		collector.res.stderr = stderr.String()
		collector.res.exit = session.Exit()
		collector.err = session.Err()
		collector.mu.Unlock()
	}()
	return collector
}

func (c *handoffSessionCollector) Wait(ctx context.Context) (handoffSessionResult, error) {
	select {
	case <-c.done:
		c.mu.Lock()
		defer c.mu.Unlock()
		return c.res, c.err
	case <-ctx.Done():
		return handoffSessionResult{}, ctx.Err()
	}
}

func runHandoffParallel(count int, fn func(int) error) error {
	errs := make(chan error, count)
	var wg sync.WaitGroup
	for i := 0; i < count; i++ {
		wg.Add(1)
		go func(index int) {
			defer wg.Done()
			if err := fn(index); err != nil {
				errs <- err
			}
		}(i)
	}
	wg.Wait()
	close(errs)
	var result error
	for err := range errs {
		result = errors.Join(result, err)
	}
	return result
}

func waitForHandoffPeers(ctx context.Context, srv *server.Server, want int) error {
	ticker := time.NewTicker(25 * time.Millisecond)
	defer ticker.Stop()
	for {
		if srv != nil && len(srv.Relay.Peers()) >= want {
			return nil
		}
		select {
		case <-ctx.Done():
			got := 0
			if srv != nil {
				got = len(srv.Relay.Peers())
			}
			return fmt.Errorf("wait for %d peers (got %d): %w", want, got, ctx.Err())
		case <-ticker.C:
		}
	}
}

func verifyHandoffInventory(ctx context.Context, c *client.Client, nodes []*node.Node, ids [][]string, moved bool) error {
	workspaces, err := c.ListWorkspaces(ctx)
	if err != nil {
		return fmt.Errorf("list workspaces: %w", err)
	}
	if len(workspaces) != handoffScaleWorkspaces {
		return fmt.Errorf("workspace inventory: got %d want %d", len(workspaces), handoffScaleWorkspaces)
	}
	type expectedWorkspace struct {
		node       string
		generation uint64
	}
	want := make(map[string]expectedWorkspace, handoffScaleWorkspaces)
	for i, row := range ids {
		for j, id := range row {
			nodeIndex, generation := i, uint64(1)
			if moved && j == 0 {
				nodeIndex, generation = (i+1)%len(nodes), 2
			}
			want[id] = expectedWorkspace{node: nodes[nodeIndex].ID(), generation: generation}
		}
	}
	for _, ws := range workspaces {
		expected, ok := want[ws.ID]
		if !ok {
			return fmt.Errorf("unexpected workspace %s", ws.ID)
		}
		if ws.State != proto.WSClaimed {
			return fmt.Errorf("workspace %s state=%s", ws.ID, ws.State)
		}
		if ws.Node != expected.node || ws.Generation != expected.generation {
			return fmt.Errorf("workspace %s node=%s generation=%d want node=%s generation=%d", ws.ID, ws.Node, ws.Generation, expected.node, expected.generation)
		}
	}
	statuses, err := c.ListNodes(ctx)
	if err != nil {
		return fmt.Errorf("list nodes: %w", err)
	}
	if len(statuses) != len(nodes) {
		return fmt.Errorf("node inventory: got %d want %d", len(statuses), len(nodes))
	}
	for _, status := range statuses {
		if !status.Online {
			return fmt.Errorf("node %s is offline", status.ID)
		}
	}
	return nil
}

func logHandoffLatency(t *testing.T, operation string, samples []time.Duration) {
	t.Helper()
	ordered := append([]time.Duration(nil), samples...)
	sort.Slice(ordered, func(i, j int) bool { return ordered[i] < ordered[j] })
	percentile := func(p int) time.Duration {
		index := (p*len(ordered)+99)/100 - 1
		if index < 0 {
			index = 0
		}
		return ordered[index]
	}
	t.Logf("latency operation=%s samples=%d p50=%s p99=%s", operation, len(ordered), percentile(50), percentile(99))
}
