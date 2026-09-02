// Package sim runs the whole system in one process: a server (control +
// relay + artifacts), one or more nodes, and clients, connected through
// in-memory pipes with fault injection. Every row of the failure model in
// docs/design.md has a test here.
package sim

import (
	"bytes"
	"context"
	"crypto/x509"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"remount.dev/remount/internal/client"
	"remount.dev/remount/internal/control"
	"remount.dev/remount/internal/node"
	"remount.dev/remount/internal/proto"
	"remount.dev/remount/internal/server"
	"remount.dev/remount/internal/transport"
	"remount.dev/remount/internal/workspace"
)

// world is one simulated deployment.
type world struct {
	t           *testing.T
	artifactDir string
	srv         *server.Server
	http        *httptest.Server
	ctx         context.Context
	cancel      context.CancelFunc

	mu          sync.Mutex
	conns       []*fault // every live pipe end handed to a dialer
	nodeCancels map[string]context.CancelFunc
}

// fault wraps a pipe end so tests can cut it.
type fault struct {
	transport.Conn
	who string
}

func newWorld(t *testing.T, bindings ...control.Binding) *world {
	t.Helper()
	dataDir := filepath.Join(t.TempDir(), "server")
	srv, err := server.New(server.Options{DataDir: dataDir, Token: "tok", Bindings: bindings, LeaseSec: 2})
	if err != nil {
		t.Fatal(err)
	}
	hs := httptest.NewServer(srv.Handler())
	ctx, cancel := context.WithCancel(context.Background())
	w := &world{
		t: t, artifactDir: filepath.Join(dataDir, "artifacts"), srv: srv,
		http: hs, ctx: ctx, cancel: cancel, nodeCancels: make(map[string]context.CancelFunc),
	}
	t.Cleanup(func() {
		cancel()
		hs.Close()
		srv.Close()
	})
	return w
}

// dialer returns an in-memory dialer whose connections the test can cut.
func (w *world) dialer(who string) transport.Dialer {
	return transport.DialFunc(func(ctx context.Context) (transport.Conn, error) {
		a, b := transport.Pipe(256)
		go w.srv.AcceptConn(w.ctx, b)
		f := &fault{Conn: a, who: who}
		w.mu.Lock()
		w.conns = append(w.conns, f)
		w.mu.Unlock()
		return f, nil
	})
}

// cut closes every live connection belonging to who.
func (w *world) cut(who string) int {
	w.mu.Lock()
	defer w.mu.Unlock()
	n := 0
	kept := w.conns[:0]
	for _, c := range w.conns {
		if c.who == who {
			c.Close()
			n++
			continue
		}
		kept = append(kept, c)
	}
	w.conns = kept
	return n
}

// stopNode models a process death: it prevents reconnects and cuts any
// connection that was already established. Repeated calls are harmless.
func (w *world) stopNode(name string) {
	w.mu.Lock()
	cancel := w.nodeCancels[name]
	delete(w.nodeCancels, name)
	w.mu.Unlock()
	if cancel != nil {
		cancel()
	}
	w.cut(name)
}

func (w *world) node(name string, labels map[string]string) *node.Node {
	return w.nodeWithBrokerRoots(name, labels, nil)
}

func (w *world) nodeWithBrokerRoots(name string, labels map[string]string, roots *x509.CertPool) *node.Node {
	w.t.Helper()
	dir := filepath.Join(w.t.TempDir(), name)
	n, err := node.New(node.Options{
		DataDir: dir, Dialer: w.dialer(name), Token: "tok", Labels: labels,
		ArtifactURL: w.http.URL + "/v1/artifacts", Allow: []string{"127.0.0.1"}, AllowPrivate: []string{"127.0.0.1", "localhost"},
		BrokerRootCAs: roots,
	})
	if err != nil {
		w.t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(w.ctx)
	w.mu.Lock()
	w.nodeCancels[name] = cancel
	w.mu.Unlock()
	go n.Run(ctx)
	w.t.Cleanup(func() { w.stopNode(name) })
	select {
	case <-n.Online():
	case <-time.After(5 * time.Second):
		w.t.Fatal("node never came online")
	}
	return n
}

func (w *world) client(name string) *client.Client {
	c := client.New(client.Options{Dialer: w.dialer(name), Token: "tok", Principal: "a_" + name})
	w.t.Cleanup(func() { c.Close() })
	return c
}

// syncBuf is a bytes.Buffer safe for a collector goroutine plus a reader.
type syncBuf struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (s *syncBuf) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.Write(p)
}

func (s *syncBuf) String() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.String()
}

func ctxT(t *testing.T, d time.Duration) context.Context {
	ctx, cancel := context.WithTimeout(context.Background(), d)
	t.Cleanup(cancel)
	return ctx
}

func mustWS(t *testing.T, c *client.Client, spec proto.WorkspaceSpec) *proto.Workspace {
	t.Helper()
	ctx := ctxT(t, 60*time.Second)
	ws, err := c.CreateWorkspace(ctx, spec)
	if err != nil {
		t.Fatal(err)
	}
	ws, err = c.WaitClaimed(ctx, ws.ID)
	if err != nil {
		t.Fatal(err)
	}
	return ws
}

// ---------------------------------------------------------------------------

func TestExecEndToEnd(t *testing.T) {
	w := newWorld(t)
	n := w.node("n1", nil)
	c := w.client("c1")
	ws := mustWS(t, c, proto.WorkspaceSpec{Name: "hello"})
	if ws.Node != n.ID() || ws.Generation != 1 {
		t.Fatalf("%+v", ws)
	}
	ctx := ctxT(t, 60*time.Second)
	out, errb, exit, err := c.Run(ctx, ws.ID, "sh", "-c", "echo hi; echo bad >&2; exit 7")
	if err != nil || string(out) != "hi\n" || string(errb) != "bad\n" || exit.Code != 7 {
		t.Fatalf("%v %q %q %+v", err, out, errb, exit)
	}
	// Filesystem round trip and search.
	if err := c.WriteFile(ctx, ws.ID, "src/a.txt", []byte("alpha\nbeta\n"), 0); err != nil {
		t.Fatal(err)
	}
	b, err := c.ReadFile(ctx, ws.ID, "/src/a.txt")
	if err != nil || string(b) != "alpha\nbeta\n" {
		t.Fatal(err, string(b))
	}
	res, err := c.Search(ctx, ws.ID, "/", "bet", "", 0)
	if err != nil || len(res.Matches) != 1 || res.Matches[0].Line != 2 {
		t.Fatalf("%v %+v", err, res)
	}
	nrep, err := c.Edit(ctx, ws.ID, "src/a.txt", []proto.FSEdit{{Old: "beta", New: "gamma"}})
	if err != nil || nrep != 1 {
		t.Fatal(err, nrep)
	}
	out, _, _, _ = c.Run(ctx, ws.ID, "cat", "src/a.txt")
	if string(out) != "alpha\ngamma\n" {
		t.Fatalf("%q", out)
	}
	// The root holds what the agent created plus .remount, the node-local
	// env file a harness reads to find the broker after a move.
	ents, _ := c.ListDir(ctx, ws.ID, "/")
	names := map[string]bool{}
	for _, e := range ents {
		names[e.Name] = true
	}
	if !names["src"] || !names[".remount"] || len(ents) != 2 {
		t.Fatalf("%+v", ents)
	}
	// Events made it to the control plane's canonical log.
	evs, err := c.ReadEvents(ctx, 1, ws.ID)
	if err != nil {
		t.Fatal(err)
	}
	types := map[string]int{}
	for _, e := range evs {
		types[e.Type]++
	}
	for _, want := range []string{proto.EvWSCreated, proto.EvWSClaimed, proto.EvSOpened, proto.EvSExited, proto.EvFSWrite, proto.EvFSEdit} {
		if types[want] == 0 {
			t.Errorf("missing event %s in %v", want, types)
		}
	}
	// Node status and lists.
	nodes, _ := c.ListNodes(ctx)
	if len(nodes) != 1 || !nodes[0].Online || len(nodes[0].Workspaces) != 1 {
		t.Fatalf("%+v", nodes)
	}
	list, _ := c.ListWorkspaces(ctx)
	if len(list) != 1 {
		t.Fatal(len(list))
	}
	if err := c.DestroyWorkspace(ctx, ws.ID); err != nil {
		t.Fatal(err)
	}
	time.Sleep(100 * time.Millisecond)
	if _, err := c.ReadFile(ctx, ws.ID, "src/a.txt"); err == nil {
		t.Fatal("destroyed workspace still serves files")
	}
}

func TestPTYAndStdin(t *testing.T) {
	w := newWorld(t)
	w.node("n1", nil)
	c := w.client("c1")
	ws := mustWS(t, c, proto.WorkspaceSpec{})
	ctx := ctxT(t, 60*time.Second)
	s, err := c.Exec(ctx, proto.SOpenReq{WS: ws.ID, Kind: proto.SessionPTY, Program: []string{"sh", "-c", "read x; echo got:$x; stty size"}, Rows: 20, Cols: 90})
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Resize(ctx, 33, 111); err != nil {
		t.Fatal(err)
	}
	if err := s.Input(ctx, []byte("ping\n"), false); err != nil {
		t.Fatal(err)
	}
	var out bytes.Buffer
	exit := client.Copy(s, &out, &out)
	if !strings.Contains(out.String(), "got:ping") || !strings.Contains(out.String(), "33 111") || exit.Code != 0 {
		t.Fatalf("%q %+v", out.String(), exit)
	}
	// exec with stdin + EOF
	s, _ = c.Exec(ctx, proto.SOpenReq{WS: ws.ID, Program: []string{"wc", "-c"}, Stdin: true})
	s.Input(ctx, []byte("12345"), true)
	out.Reset()
	client.Copy(s, &out, nil)
	if strings.TrimSpace(out.String()) != "5" {
		t.Fatalf("%q", out.String())
	}
	// signal
	s, _ = c.Exec(ctx, proto.SOpenReq{WS: ws.ID, Program: []string{"sleep", "30"}})
	time.Sleep(50 * time.Millisecond)
	if err := s.Signal(ctx, "TERM"); err != nil {
		t.Fatal(err)
	}
	exit, err = s.Wait(ctx)
	if err != nil || exit.Signal == "" {
		t.Fatalf("%v %+v", err, exit)
	}
	// session list shows the finished sessions with exit records
	sessions, _ := c.ListSessions(ctx, ws.ID)
	if len(sessions) < 3 {
		t.Fatal(len(sessions))
	}
}

// R4: kill the client's connection mid-stream; output is byte-identical.
func TestReconnectMidStreamIsLossless(t *testing.T) {
	w := newWorld(t)
	w.node("n1", nil)
	c := w.client("c1")
	ws := mustWS(t, c, proto.WorkspaceSpec{})
	ctx := ctxT(t, 60*time.Second)
	// 2000 numbered lines with small pauses so the cut lands mid-stream.
	s, err := c.Exec(ctx, proto.SOpenReq{WS: ws.ID, Program: []string{"sh", "-c", "i=0; while [ $i -lt 2000 ]; do echo line-$i; i=$((i+1)); if [ $((i % 200)) -eq 0 ]; then sleep 0.02; fi; done"}})
	if err != nil {
		t.Fatal(err)
	}
	var got bytes.Buffer
	cuts := 0
	var seqs []uint64
	for ch := range s.Chunks() {
		if ch.Stream == proto.StreamStdout {
			got.Write(ch.Data)
			seqs = append(seqs, ch.Seq)
		}
		if ch.Stream == proto.StreamGap {
			t.Fatal("unexpected gap")
		}
		if got.Len() > 2000*(cuts+1) && cuts < 3 {
			cuts++
			w.cut("c1")
		}
	}
	if cuts == 0 {
		t.Fatal("test never cut the connection")
	}
	var want bytes.Buffer
	for i := 0; i < 2000; i++ {
		fmt.Fprintf(&want, "line-%d\n", i)
	}
	if got.String() != want.String() {
		t.Fatalf("output differs after %d cuts: got %d bytes want %d", cuts, got.Len(), want.Len())
	}
	for i := 1; i < len(seqs); i++ {
		if seqs[i] <= seqs[i-1] {
			t.Fatalf("seq not increasing at %d: %d after %d", i, seqs[i], seqs[i-1])
		}
	}
	if s.Exit() == nil || s.Exit().Code != 0 {
		t.Fatalf("%+v", s.Exit())
	}
}

// Node uplink flaps: the session keeps running on the node; the client
// re-attaches once the node is back.
func TestNodeUplinkFlapKeepsSessionRunning(t *testing.T) {
	w := newWorld(t)
	w.node("n1", nil)
	c := w.client("c1")
	ws := mustWS(t, c, proto.WorkspaceSpec{})
	ctx := ctxT(t, 60*time.Second)
	s, err := c.Exec(ctx, proto.SOpenReq{WS: ws.ID, Program: []string{"sh", "-c", "for i in 1 2 3 4 5 6; do echo tick-$i; sleep 0.3; done"}})
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(400 * time.Millisecond)
	if w.cut("n1") == 0 {
		t.Fatal("no node connection to cut")
	}
	// Give the node time to reconnect (its backoff starts at 1s); the lease is 2s.
	var out syncBuf
	go func() {
		for ch := range s.Chunks() {
			if ch.Stream == proto.StreamStdout {
				out.Write(ch.Data)
			}
		}
	}()
	// Our subscription died with the node's connection; re-attach from where we were.
	time.Sleep(1500 * time.Millisecond)
	s2, err := c.Attach(ctx, ws.ID, s.ID, s.Next())
	if err != nil {
		t.Fatal(err)
	}
	var out2 bytes.Buffer
	client.Copy(s2, &out2, nil)
	all := out.String() + out2.String()
	for i := 1; i <= 6; i++ {
		if strings.Count(all, fmt.Sprintf("tick-%d\n", i)) != 1 {
			t.Fatalf("tick-%d count wrong in %q", i, all)
		}
	}
	// Same workspace, same generation: the node reconnected within its lease.
	got, _ := c.GetWorkspace(ctx, ws.ID)
	if got.Generation != ws.Generation || got.State != proto.WSClaimed {
		t.Fatalf("%+v", got)
	}
}

// R5: node dies; its lease expires; another eligible node claims the
// workspace and restores it from the last snapshot. Also exercises an
// explicit graceful move first (which is what records LastSnapshot).
func TestNodeDeathMovesWorkspaceFromSnapshot(t *testing.T) {
	w := newWorld(t)
	nodes := map[string]*node.Node{}
	nodes["n1"] = w.node("n1", map[string]string{"zone": "a"})
	nodes["n2"] = w.node("n2", map[string]string{"zone": "a"})
	byID := func() map[string]string {
		m := map[string]string{}
		for name, n := range nodes {
			m[n.ID()] = name
		}
		return m
	}
	c := w.client("c1")
	ctx := ctxT(t, 90*time.Second)
	ws, err := c.CreateWorkspace(ctx, proto.WorkspaceSpec{Placement: proto.Placement{Allow: map[string]string{"zone": "a"}}})
	if err != nil {
		t.Fatal(err)
	}
	ws, err = c.WaitClaimed(ctx, ws.ID)
	if err != nil {
		t.Fatal(err)
	}
	c.WriteFile(ctx, ws.ID, "state.txt", []byte("v1"), 0)

	// Graceful move establishes a LastSnapshot and demonstrates cross-node
	// portability. Placement stays label-based so any zone=a node qualifies.
	moved, err := c.MoveWorkspace(ctx, ws.ID, nil, &proto.Placement{Allow: map[string]string{"zone": "a"}})
	if err != nil {
		t.Fatal(err)
	}
	moved, err = c.WaitClaimed(ctx, moved.ID)
	if err != nil {
		t.Fatal(err)
	}
	if moved.Generation != 2 || moved.LastSnapshot == "" {
		t.Fatalf("%+v", moved)
	}
	b, err := c.ReadFile(ctx, ws.ID, "state.txt")
	if err != nil || string(b) != "v1" {
		t.Fatalf("after move: %v %q", err, b)
	}

	// Crash the node that currently holds it, permanently.
	holderName := byID()[moved.Node]
	if holderName == "" {
		t.Fatalf("unknown holder %s", moved.Node)
	}
	w.stopNode(holderName)

	// The other zone=a node must pick it up (lease is 2s) at a higher gen.
	deadline := time.Now().Add(40 * time.Second)
	var got *proto.Workspace
	for time.Now().Before(deadline) {
		got, _ = c.GetWorkspace(ctx, ws.ID)
		if got.State == proto.WSClaimed && got.Node != moved.Node {
			break
		}
		time.Sleep(200 * time.Millisecond)
	}
	if got == nil || got.Node == moved.Node || got.State != proto.WSClaimed || got.Generation < 3 {
		t.Fatalf("workspace did not fail over from %s: %+v", holderName, got)
	}
	b, err = c.ReadFile(ctx, ws.ID, "state.txt")
	if err != nil || string(b) != "v1" {
		t.Fatalf("restored content wrong: %v %q", err, b)
	}
	evs, _ := c.ReadEvents(ctx, 1, ws.ID)
	seen := map[string]bool{}
	for _, e := range evs {
		seen[e.Type] = true
	}
	if !seen[proto.EvWSLeaseExpired] || !seen[proto.EvWSRestored] {
		t.Fatalf("events: %v", seen)
	}
}

// R6: sleep for a timer, wake, files intact; also wake on posted event.
func TestSleepAndWake(t *testing.T) {
	w := newWorld(t)
	w.node("n1", nil)
	c := w.client("c1")
	ws := mustWS(t, c, proto.WorkspaceSpec{})
	ctx := ctxT(t, 60*time.Second)
	c.WriteFile(ctx, ws.ID, "memo.txt", []byte("remember me"), 0)
	timer, err := c.SleepWorkspace(ctx, proto.WSSleepReq{ID: ws.ID, AfterSec: 1})
	if err != nil || timer.ID == "" {
		t.Fatal(err)
	}
	got, _ := c.GetWorkspace(ctx, ws.ID)
	if got.State != proto.WSPaused || got.Node != "" || got.LastSnapshot == "" {
		t.Fatalf("%+v", got)
	}
	if _, err := c.ReadFile(ctx, ws.ID, "memo.txt"); err == nil {
		t.Fatal("paused workspace should not be reachable")
	}
	got, err = c.WaitClaimed(ctx, ws.ID)
	if err != nil {
		t.Fatal(err)
	}
	b, err := c.ReadFile(ctx, ws.ID, "memo.txt")
	if err != nil || string(b) != "remember me" {
		t.Fatalf("%v %q", err, b)
	}
	timers, _ := c.ListTimers(ctx)
	if len(timers) != 1 || !timers[0].Fired {
		t.Fatalf("%+v", timers)
	}
	// Event wake.
	_, err = c.SleepWorkspace(ctx, proto.WSSleepReq{ID: ws.ID, OnEvent: "github.pr.merged"})
	if err != nil {
		t.Fatal(err)
	}
	got, _ = c.GetWorkspace(ctx, ws.ID)
	if got.State != proto.WSPaused {
		t.Fatalf("%+v", got)
	}
	// Webhook via HTTP.
	resp, err := http.Post(w.http.URL+"/v1/events", "application/json", strings.NewReader(`{"type":"github.pr.merged","payload":{"pr":42}}`))
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("webhook without token accepted: %d", resp.StatusCode)
	}
	req, _ := http.NewRequest("POST", w.http.URL+"/v1/events", strings.NewReader(`{"type":"github.pr.merged","payload":{"pr":42}}`))
	req.Header.Set("Authorization", "Bearer tok")
	resp, err = http.DefaultClient.Do(req)
	if err != nil || resp.StatusCode != http.StatusAccepted {
		t.Fatal(err, resp.StatusCode)
	}
	if _, err := c.WaitClaimed(ctx, ws.ID); err != nil {
		t.Fatal(err)
	}
	b, _ = c.ReadFile(ctx, ws.ID, "memo.txt")
	if string(b) != "remember me" {
		t.Fatalf("%q", b)
	}
}

// The node publishes where the broker is, refreshes it on every move, and
// never lets that node-local truth travel inside a snapshot.
func TestWorkspaceEnvFileIsRefreshedAndNotSnapshotted(t *testing.T) {
	w := newWorld(t)
	a := w.node("na", map[string]string{"n": "a"})
	w.node("nb", map[string]string{"n": "b"})
	c := w.client("c1")
	ctx := ctxT(t, 60*time.Second)
	ws := mustWS(t, c, proto.WorkspaceSpec{Placement: proto.Placement{Node: a.ID()}})

	first, err := c.ReadFile(ctx, ws.ID, node.EnvFilePath)
	if err != nil {
		t.Fatal(err)
	}
	info, _ := c.WorkspaceInfo(ctx, ws.ID)
	if !strings.Contains(string(first), "REMOUNT_BROKER="+info.Broker) {
		t.Fatalf("env file does not name the live broker:\n%s", first)
	}
	if !strings.Contains(string(first), "REMOUNT_WORKSPACE="+ws.ID) {
		t.Fatalf("%s", first)
	}

	// A session can source it and see the same broker.
	out, _, exit, err := c.Run(ctx, ws.ID, "sh", "-c", ". ./"+node.EnvFilePath+" && echo $REMOUNT_BROKER")
	if err != nil || exit.Code != 0 || strings.TrimSpace(string(out)) != info.Broker {
		t.Fatalf("%v %+v %q vs %q", err, exit, out, info.Broker)
	}

	// Move it. The file must be rewritten for the new node, not restored stale.
	moved, err := c.MoveWorkspace(ctx, ws.ID, nil, &proto.Placement{Node: ""})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = c.WaitClaimed(ctx, moved.ID); err != nil {
		t.Fatal(err)
	}
	info2, _ := c.WorkspaceInfo(ctx, ws.ID)
	second, err := c.ReadFile(ctx, ws.ID, node.EnvFilePath)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(second), "REMOUNT_BROKER="+info2.Broker) {
		t.Fatalf("env file not refreshed after move:\n%s\nwant broker %s", second, info2.Broker)
	}
	if info.Broker == info2.Broker {
		t.Skip("broker address happened not to change; nothing to prove")
	}
	if strings.Contains(string(second), info.Broker) {
		t.Fatalf("stale broker address survived the move:\n%s", second)
	}
}

// R3/R10: the workspace never sees the secret; the broker substitutes it for
// the bound host and blocks it elsewhere; every decision is an event.
func TestSecretBlindWorkspace(t *testing.T) {
	var gotAuth atomic.Value
	up := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotAuth.Store(r.Header.Get("Authorization"))
		io.WriteString(w, "ok")
	}))
	defer up.Close()
	upHost := strings.TrimPrefix(up.URL, "https://")
	w := newWorld(t, control.Binding{ID: "b_api", Secret: "sk-REAL-SECRET", Destinations: []string{upHost}, TTLSec: 60})
	roots := x509.NewCertPool()
	roots.AddCert(up.Certificate())
	w.nodeWithBrokerRoots("n1", nil, roots)
	c := w.client("c1")
	ws := mustWS(t, c, proto.WorkspaceSpec{
		Bindings: []string{"b_api"},
		Env:      map[string]string{"API_KEY": "ref:b_api", "API_URL": "${REMOUNT_BROKER}/d/" + upHost},
	})
	ctx := ctxT(t, 60*time.Second)
	// 1. The environment holds only the placeholder.
	out, _, _, _ := c.Run(ctx, ws.ID, "sh", "-c", `echo "$API_KEY"; env | grep -c REAL || true`)
	if !strings.HasPrefix(string(out), "ref:b_api\n") || !strings.HasSuffix(strings.TrimSpace(string(out)), "0") {
		t.Fatalf("secret leaked into env: %q", out)
	}
	// 2. Through the broker the upstream receives the real secret.
	out, errb, exit, _ := c.Run(ctx, ws.ID, "sh", "-c", `curl -s -H "Authorization: Bearer $API_KEY" "$API_URL/v1/thing"`)
	if exit.Code != 0 || string(out) != "ok" {
		t.Fatalf("curl failed: %d %q %q", exit.Code, out, errb)
	}
	if gotAuth.Load() != "Bearer sk-REAL-SECRET" {
		t.Fatalf("upstream saw %v", gotAuth.Load())
	}
	// 3. Same placeholder to a foreign host is blocked (leak attempt).
	other := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { io.WriteString(w, "evil") }))
	defer other.Close()
	otherHost := strings.TrimPrefix(other.URL, "http://")
	out, _, _, _ = c.Run(ctx, ws.ID, "sh", "-c", `curl -s -o /dev/null -w "%{http_code}" -H "Authorization: Bearer $API_KEY" "$REMOUNT_BROKER/http/`+otherHost+`/x"`)
	if string(out) != "403" {
		t.Fatalf("leak not blocked: %q", out)
	}
	// 4. The audit trail is in the canonical log.
	time.Sleep(200 * time.Millisecond)
	evs, _ := c.ReadEvents(ctx, 1, ws.ID)
	var used, denied int
	for _, e := range evs {
		switch e.Type {
		case proto.EvCredUsed:
			used++
		case proto.EvEgressDenied:
			denied++
		}
	}
	if used != 1 || denied != 1 {
		t.Fatalf("cred.used=%d egress.denied=%d", used, denied)
	}
	// 5. The workspace disk holds no secret either.
	info, _ := c.WorkspaceInfo(ctx, ws.ID)
	found := false
	filepath.Walk(info.Root, func(p string, fi os.FileInfo, err error) error {
		if err == nil && fi.Mode().IsRegular() {
			b, _ := os.ReadFile(p)
			if bytes.Contains(b, []byte("sk-REAL-SECRET")) {
				found = true
			}
		}
		return nil
	})
	if found {
		t.Fatal("secret on workspace disk")
	}
}

// Grants: a client cannot talk to a node about a workspace without a valid
// grant, and a grant for another client is rejected.
func TestGrantsAreEnforced(t *testing.T) {
	w := newWorld(t)
	n := w.node("n1", nil)
	c := w.client("c1")
	ws := mustWS(t, c, proto.WorkspaceSpec{})
	ctx := ctxT(t, 60*time.Second)
	// Raw peer without any grant.
	conn, _ := w.dialer("raw").Dial(ctx)
	p := transport.NewPeer(conn, nil)
	if _, err := transport.Hello(ctx, p, proto.Hello{Role: proto.RoleClient, Token: "tok"}); err != nil {
		t.Fatal(err)
	}
	var res proto.FSReadRes
	err := p.Call(ctx, n.ID(), proto.OpFSRead, proto.FSReadReq{WS: ws.ID, Path: "x"}, &res)
	var pe *proto.Error
	if !errors.As(err, &pe) || pe.Code != proto.CodeUnauthorized {
		t.Fatalf("no-grant request allowed: %v", err)
	}
	// A forged grant (bad signature).
	forged := &proto.Grant{Claims: proto.GrantClaims{Client: p.Name, WS: ws.ID, Node: n.ID(), ExpiresAt: time.Now().Add(time.Hour).UnixMilli(), Gen: 1}, Signature: make([]byte, 64)}
	err = p.Call(ctx, n.ID(), proto.OpFSRead, proto.FSReadReq{WS: ws.ID, Path: "x", Grant: forged}, &res)
	if !errors.As(err, &pe) || pe.Code != proto.CodeUnauthorized {
		t.Fatalf("forged grant allowed: %v", err)
	}
	// Bad token at hello.
	conn2, _ := w.dialer("raw2").Dial(ctx)
	p2 := transport.NewPeer(conn2, nil)
	if _, err := transport.Hello(ctx, p2, proto.Hello{Role: proto.RoleClient, Token: "wrong"}); err == nil {
		t.Fatal("bad token accepted")
	}
	// Unknown workspace -> not found.
	err = c.Mkdir(ctx, "ws_doesnotexist", "x")
	if !errors.As(err, &pe) || pe.Code != proto.CodeNotFound {
		t.Fatalf("%v", err)
	}
}

// Port forwarding: a server inside the workspace is reachable through a
// port session.
func TestPortForward(t *testing.T) {
	w := newWorld(t)
	w.node("n1", nil)
	c := w.client("c1")
	ws := mustWS(t, c, proto.WorkspaceSpec{})
	ctx := ctxT(t, 60*time.Second)
	// Confirm python3 is available in the workspace; skip if not.
	_, _, exit, err := c.Run(ctx, ws.ID, "sh", "-c", "command -v python3")
	if err != nil || exit.Code != 0 {
		t.Skip("python3 not available in workspace")
	}
	c.WriteFile(ctx, ws.ID, "index.html", []byte("<h1>remount</h1>"), 0)
	srv, err := c.Exec(ctx, proto.SOpenReq{WS: ws.ID, Program: []string{"python3", "-m", "http.server", "18081", "--bind", "127.0.0.1"}})
	if err != nil {
		t.Fatal(err)
	}
	defer srv.Close(ctx, true)
	var s *client.Session
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		s, err = c.OpenPort(ctx, ws.ID, 18081)
		if err == nil {
			break
		}
		time.Sleep(200 * time.Millisecond)
	}
	if err != nil || s == nil {
		t.Fatalf("port open: %v", err)
	}
	s.Input(ctx, []byte("GET /index.html HTTP/1.0\r\nHost: x\r\n\r\n"), true)
	var out bytes.Buffer
	client.Copy(s, &out, nil)
	if !strings.Contains(out.String(), "200 OK") || !strings.Contains(out.String(), "<h1>remount</h1>") {
		t.Fatalf("%q", out.String())
	}
}

// Placement: a workspace requiring a label only lands on a matching node,
// and waits in pending until one exists.
func TestPlacementWaitsForEligibleNode(t *testing.T) {
	w := newWorld(t)
	w.node("cpu", map[string]string{"gpu": "no"})
	c := w.client("c1")
	ctx := ctxT(t, 60*time.Second)
	ws, err := c.CreateWorkspace(ctx, proto.WorkspaceSpec{Placement: proto.Placement{Allow: map[string]string{"gpu": "yes"}}})
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(500 * time.Millisecond)
	got, _ := c.GetWorkspace(ctx, ws.ID)
	if got.State != proto.WSPending {
		t.Fatalf("claimed by ineligible node: %+v", got)
	}
	gpu := w.node("gpu", map[string]string{"gpu": "yes"})
	got, err = c.WaitClaimed(ctx, ws.ID)
	if err != nil || got.Node != gpu.ID() {
		t.Fatalf("%v %+v", err, got)
	}
	// Unsupported backend requirement is never claimed.
	ws2, _ := c.CreateWorkspace(ctx, proto.WorkspaceSpec{Requires: proto.Requires{Backend: "firecracker"}})
	time.Sleep(300 * time.Millisecond)
	got, _ = c.GetWorkspace(ctx, ws2.ID)
	if got.State != proto.WSPending {
		t.Fatalf("%+v", got)
	}
}

// Idempotent exec: an exact retry returns one process, while reusing the key
// with different arguments is an explicit conflict rather than silently
// returning a semantically unrelated session.
func TestIdempotentExec(t *testing.T) {
	w := newWorld(t)
	w.node("n1", nil)
	c := w.client("c1")
	ws := mustWS(t, c, proto.WorkspaceSpec{})
	ctx := ctxT(t, 60*time.Second)
	key := "same-key"
	a, err := c.Exec(ctx, proto.SOpenReq{WS: ws.ID, Program: []string{"sh", "-c", "echo $$ > pid; sleep 0.2"}, IdempotencyKey: key})
	if err != nil {
		t.Fatal(err)
	}
	b, err := c.Exec(ctx, proto.SOpenReq{WS: ws.ID, Program: []string{"sh", "-c", "echo $$ > pid; sleep 0.2"}, IdempotencyKey: key})
	if err != nil {
		t.Fatal(err)
	}
	if a.ID != b.ID {
		t.Fatalf("two sessions for one key: %s %s", a.ID, b.ID)
	}
	if _, err := c.Exec(ctx, proto.SOpenReq{WS: ws.ID, Program: []string{"sh", "-c", "echo second > pid"}, IdempotencyKey: key}); err == nil {
		t.Fatal("idempotency key reuse with different arguments succeeded")
	} else {
		var pe *proto.Error
		if !errors.As(err, &pe) || pe.Code != proto.CodeConflict {
			t.Fatalf("key reuse error = %v, want conflict", err)
		}
	}
	client.Copy(a, nil, nil)
	out, _ := c.ReadFile(ctx, ws.ID, "pid")
	if strings.TrimSpace(string(out)) == "second" {
		t.Fatal("second process ran")
	}
}

func TestFilesystemMutationsAreDurablyIdempotent(t *testing.T) {
	w := newWorld(t)
	w.node("n1", nil)
	c := w.client("c1")
	ws := mustWS(t, c, proto.WorkspaceSpec{})
	ctx := ctxT(t, 30*time.Second)

	if err := c.Mkdir(ctx, ws.ID, "dir", client.WithIdempotencyKey("mkdir-one")); err != nil {
		t.Fatal(err)
	}
	if err := c.WriteFile(ctx, ws.ID, "dir/file", []byte("value"), 0,
		client.WithIdempotencyKey("write-one")); err != nil {
		t.Fatal(err)
	}
	if err := c.Rename(ctx, ws.ID, "dir/file", "dir/renamed",
		client.WithIdempotencyKey("rename-one")); err != nil {
		t.Fatal(err)
	}
	// The source no longer exists, so a second success proves replay rather
	// than accidental filesystem idempotency.
	if err := c.Rename(ctx, ws.ID, "dir/file", "dir/renamed",
		client.WithIdempotencyKey("rename-one")); err != nil {
		t.Fatalf("rename replay: %v", err)
	}
	if err := c.Remove(ctx, ws.ID, "dir/renamed", false,
		client.WithIdempotencyKey("remove-one")); err != nil {
		t.Fatal(err)
	}
	if err := c.Remove(ctx, ws.ID, "dir/renamed", false,
		client.WithIdempotencyKey("remove-one")); err != nil {
		t.Fatalf("remove replay: %v", err)
	}
	if err := c.Remove(ctx, ws.ID, "different", false,
		client.WithIdempotencyKey("remove-one")); err == nil {
		t.Fatal("idempotency key reuse with changed arguments succeeded")
	}
}

// Node restart: workspaces on disk are adopted back under a new generation.
func TestNodeRestartAdoptsLocalWorkspaces(t *testing.T) {
	w := newWorld(t)
	dir := filepath.Join(t.TempDir(), "n1")
	mk := func() (*node.Node, context.CancelFunc) {
		n, err := node.New(node.Options{DataDir: dir, Dialer: w.dialer("n1"), Token: "tok", ArtifactURL: w.http.URL + "/v1/artifacts"})
		if err != nil {
			t.Fatal(err)
		}
		ctx, cancel := context.WithCancel(w.ctx)
		go n.Run(ctx)
		<-n.Online()
		return n, cancel
	}
	n, cancel := mk()
	c := w.client("c1")
	ws := mustWS(t, c, proto.WorkspaceSpec{})
	ctx := ctxT(t, 60*time.Second)
	c.WriteFile(ctx, ws.ID, "keep.txt", []byte("survives restart"), 0)
	cancel()
	time.Sleep(200 * time.Millisecond)
	n2, cancel2 := mk()
	defer cancel2()
	if n2.ID() != n.ID() {
		t.Fatal("identity not persisted")
	}
	got, err := c.WaitClaimed(ctxT(t, 20*time.Second), ws.ID)
	if err != nil {
		t.Fatal(err)
	}
	// The same node re-adopts what it still holds: the generation is
	// unchanged (outstanding grants stay valid) and the files survive.
	if got.Node != n2.ID() || got.State != proto.WSClaimed {
		t.Fatalf("%+v", got)
	}
	b, err := c.ReadFile(ctx, ws.ID, "keep.txt")
	if err != nil || string(b) != "survives restart" {
		t.Fatalf("%v %q", err, b)
	}
}

func TestConcurrentEventTailsAreIndependent(t *testing.T) {
	w := newWorld(t)
	c := w.client("c1")
	ctx1, cancel1 := context.WithCancel(w.ctx)
	ctx2, cancel2 := context.WithCancel(w.ctx)
	defer cancel2()
	first, err := c.TailEvents(ctx1, 0, "")
	if err != nil {
		t.Fatal(err)
	}
	second, err := c.TailEvents(ctx2, 0, "")
	if err != nil {
		t.Fatal(err)
	}
	want := "test.concurrent-tail.first"
	if err := c.PostEvent(ctxT(t, 5*time.Second), proto.Event{Type: want}); err != nil {
		t.Fatal(err)
	}
	waitFor := func(ch <-chan proto.Event, typ string) {
		t.Helper()
		deadline := time.After(5 * time.Second)
		for {
			select {
			case event, ok := <-ch:
				if !ok {
					t.Fatalf("tail closed before %s", typ)
				}
				if event.Type == typ {
					return
				}
			case <-deadline:
				t.Fatalf("timed out waiting for %s", typ)
			}
		}
	}
	waitFor(first, want)
	waitFor(second, want)
	cancel1()
	closed := false
	deadline := time.After(5 * time.Second)
	for !closed {
		select {
		case _, ok := <-first:
			closed = !ok
		case <-deadline:
			t.Fatal("cancelled tail did not close")
		}
	}
	want = "test.concurrent-tail.second"
	if err := c.PostEvent(ctxT(t, 5*time.Second), proto.Event{Type: want}); err != nil {
		t.Fatal(err)
	}
	waitFor(second, want)
}

// The docker backend is exercised end-to-end only when a daemon is present.
func TestDockerWorkspaceIfAvailable(t *testing.T) {
	d, _ := workspace.NewDocker(filepath.Join(t.TempDir(), "d"), "alpine:3.20")
	if err := d.Available(ctxT(t, 30*time.Second)); err != nil {
		t.Skipf("docker: %v", err)
	}
	w := newWorld(t)
	dir := filepath.Join(t.TempDir(), "dn")
	pb, _ := workspace.NewProcess(filepath.Join(dir, "ws"))
	n, err := node.New(node.Options{DataDir: dir, Dialer: w.dialer("dn"), Token: "tok", Backends: workspace.NewRegistry(pb, d)})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(w.ctx)
	defer cancel()
	go n.Run(ctx)
	<-n.Online()
	c := w.client("c1")
	ws := mustWS(t, c, proto.WorkspaceSpec{Requires: proto.Requires{Backend: "docker"}})
	tctx := ctxT(t, 2*time.Minute)
	out, _, exit, err := c.Run(tctx, ws.ID, "sh", "-c", "cat /etc/alpine-release && pwd")
	if err != nil || exit.Code != 0 || !strings.Contains(string(out), "/work") {
		t.Fatalf("%v %+v %q", err, exit, out)
	}
	c.DestroyWorkspace(tctx, ws.ID)
}
