// Package node is the supervisor that runs on every machine: it enrolls with
// the control plane over one outbound connection, claims workspaces, runs
// sessions, serves filesystem operations, brokers credentials, snapshots,
// and streams session output to whichever clients are attached.
//
// Everything the agent controls (the workspace) is untrusted; the node is
// the trusted computing base together with the broker
// (docs/adr/0010-trust-boundaries.md).
package node

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"remount.dev/remount/internal/artifact"
	"remount.dev/remount/internal/broker"
	"remount.dev/remount/internal/control"
	"remount.dev/remount/internal/eventlog"
	"remount.dev/remount/internal/ids"
	"remount.dev/remount/internal/metrics"
	"remount.dev/remount/internal/proto"
	"remount.dev/remount/internal/session"
	"remount.dev/remount/internal/transport"
	"remount.dev/remount/internal/workspace"
)

// Options configure a node.
type Options struct {
	DataDir  string           // identity, workspaces, spill, artifacts
	Dialer   transport.Dialer // how to reach the relay (redialed on failure)
	Token    string
	Labels   map[string]string
	Backends *workspace.Registry
	// ArtifactURL is the control plane's artifact store base (http://host/v1/artifacts).
	// Empty disables uploads (snapshots stay local).
	ArtifactURL string
	HTTPClient  *http.Client
	Logger      *slog.Logger
	// Allow lists hosts every workspace on this node may reach without a credential.
	Allow []string
	// AllowPrivate lists hosts that may resolve to private addresses (local models).
	AllowPrivate []string
	Version      string
	// Caps advertises extra capabilities (display, gpu …).
	Caps []string
}

// Node is the supervisor.
type Node struct {
	opts   Options
	id     string
	priv   ed25519.PrivateKey
	logger *slog.Logger

	sessions *session.Manager
	store    *artifact.Store
	events   *eventlog.Log

	mu         sync.Mutex
	peer       *transport.Peer
	ctrlPub    ed25519.PublicKey
	leaseSec   int64
	workspaces map[string]*ws
	// materializing holds workspaces this node has claimed but not finished
	// restoring: their leases must be renewed too, or a slow restore loses
	// the claim it is working on.
	materializing map[string]uint64       // ws -> generation
	grants        map[string]*proto.Grant // client|ws -> grant
	subs          map[string]*subscriber  // client|session -> active stream

	started time.Time
	stop    chan struct{}
	wg      sync.WaitGroup
	online  chan struct{} // closed when first connected
	onceOn  sync.Once
}

// ws is a claimed workspace on this node.
type ws struct {
	proto.Workspace
	handle workspace.Handle
	broker *broker.Broker
	leases []proto.BindingLease
}

// subscriber streams one session's log to one client.
type subscriber struct {
	cancel context.CancelFunc
}

// New loads or creates the node identity and prepares runtime state.
func New(opts Options) (*Node, error) {
	if opts.DataDir == "" {
		return nil, errors.New("node: DataDir required")
	}
	if opts.Logger == nil {
		opts.Logger = slog.Default()
	}
	if opts.HTTPClient == nil {
		opts.HTTPClient = &http.Client{Timeout: 10 * time.Minute}
	}
	for _, d := range []string{"", "ws", "spill", "artifacts"} {
		if err := os.MkdirAll(filepath.Join(opts.DataDir, d), 0o700); err != nil {
			return nil, err
		}
	}
	id, priv, err := loadIdentity(filepath.Join(opts.DataDir, "identity.json"))
	if err != nil {
		return nil, err
	}
	if opts.Backends == nil {
		pb, err := workspace.NewProcess(filepath.Join(opts.DataDir, "ws"))
		if err != nil {
			return nil, err
		}
		opts.Backends = workspace.NewRegistry(pb)
	}
	store, err := artifact.NewStore(filepath.Join(opts.DataDir, "artifacts"))
	if err != nil {
		return nil, err
	}
	n := &Node{
		opts: opts, id: id, priv: priv, logger: opts.Logger.With("node", id),
		store: store, events: eventlog.New(eventlog.NewMemory(10000)),
		workspaces: map[string]*ws{}, materializing: map[string]uint64{},
		grants: map[string]*proto.Grant{}, subs: map[string]*subscriber{},
		started: time.Now(), stop: make(chan struct{}), online: make(chan struct{}),
	}
	n.sessions = session.NewManager(session.ManagerOptions{
		SpillDir: filepath.Join(opts.DataDir, "spill"), MemBytes: 2 << 20, SpillBytes: 128 << 20,
		OnExit: func(s *session.Session, info proto.ExitInfo) {
			n.emit(proto.EvSExited, s.WS, s.Principal, map[string]any{"s": s.ID, "code": info.Code, "signal": info.Signal})
		},
	})
	return n, nil
}

// ID returns the node id.
func (n *Node) ID() string { return n.id }

// Online is closed once the node has completed its first hello.
func (n *Node) Online() <-chan struct{} { return n.online }

// Events exposes the node-local event log (mirrored to the control plane).
func (n *Node) Events() *eventlog.Log { return n.events }

type identityFile struct {
	ID   string `json:"id"`
	Priv []byte `json:"priv"`
}

func loadIdentity(path string) (string, ed25519.PrivateKey, error) {
	b, err := os.ReadFile(path)
	if err == nil {
		var f identityFile
		if err := json.Unmarshal(b, &f); err == nil && len(f.Priv) == ed25519.PrivateKeySize && strings.HasPrefix(f.ID, "n_") {
			return f.ID, ed25519.PrivateKey(f.Priv), nil
		}
	}
	_, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		return "", nil, err
	}
	f := identityFile{ID: ids.New("n"), Priv: priv}
	b, _ = json.Marshal(f)
	if err := os.WriteFile(path, b, 0o600); err != nil {
		return "", nil, err
	}
	return f.ID, priv, nil
}

// emit records an event locally and forwards it to the control plane.
func (n *Node) emit(typ, stream, principal string, payload any) {
	ctx := context.Background()
	e, err := n.events.Emit(ctx, typ, stream, principal, n.id, payload, 0)
	if err != nil {
		return
	}
	n.mu.Lock()
	p := n.peer
	n.mu.Unlock()
	if p != nil {
		go func() {
			cctx, cancel := context.WithTimeout(ctx, 10*time.Second)
			defer cancel()
			_ = p.Call(cctx, proto.PeerControl, proto.OpEventsPost, proto.EventPost{Events: []proto.Event{*e}}, nil)
		}()
	}
}

// ---------------------------------------------------------------------------
// uplink
// ---------------------------------------------------------------------------

// Run connects (and reconnects) to the relay until ctx ends.
func (n *Node) Run(ctx context.Context) error {
	n.wg.Add(1)
	go n.renewLoop(ctx)
	defer n.wg.Wait()
	defer n.shutdown()
	backoff := time.Second
	for {
		err := n.connectOnce(ctx)
		if ctx.Err() != nil {
			return nil
		}
		n.logger.Warn("uplink lost; reconnecting", "err", err, "backoff", backoff)
		select {
		case <-time.After(backoff):
		case <-ctx.Done():
			return nil
		}
		backoff *= 2
		if backoff > 30*time.Second {
			backoff = 30 * time.Second
		}
	}
}

func (n *Node) shutdown() {
	close(n.stop)
	n.sessions.Close()
	n.mu.Lock()
	for _, w := range n.workspaces {
		if w.broker != nil {
			w.broker.Close()
		}
	}
	n.mu.Unlock()
}

func (n *Node) connectOnce(ctx context.Context) error {
	conn, err := n.opts.Dialer.Dial(ctx)
	if err != nil {
		return err
	}
	peer := transport.NewPeer(conn, transport.HandlerFunc(n.handle))
	hello := proto.Hello{
		Peer: n.id, Role: proto.RoleNode, Token: n.opts.Token, Caps: []string{"v1"},
		PubKey: n.priv.Public().(ed25519.PublicKey), Labels: n.opts.Labels,
	}
	info := workspace.HostInfo(n.opts.Backends.Names())
	info.Version = n.opts.Version
	info.Caps = n.opts.Caps
	hello.Node = &info
	return n.helloAndServe(ctx, peer, hello)
}

func (n *Node) helloAndServe(ctx context.Context, peer *transport.Peer, hello proto.Hello) error {
	hctx, cancel := context.WithTimeout(ctx, 15*time.Second)
	ok, err := transport.Hello(hctx, peer, hello)
	cancel()
	if err != nil {
		peer.Close()
		return err
	}
	n.mu.Lock()
	n.peer = peer
	n.ctrlPub = ed25519.PublicKey(ok.PubKey)
	n.leaseSec = ok.LeaseSec
	n.grants = map[string]*proto.Grant{}
	n.mu.Unlock()
	n.onceOn.Do(func() { close(n.online) })
	n.logger.Info("uplink established", "server", ok.Server)
	// Announce the workspaces we are already serving (a reconnect demoted
	// them out of WSClaimed), then re-claim anything still on disk from a
	// previous process.
	n.resync(ctx)
	n.reclaimLocal(ctx)
	select {
	case <-peer.Done():
	case <-ctx.Done():
		peer.Close()
	}
	n.mu.Lock()
	if n.peer == peer {
		n.peer = nil
	}
	for k, s := range n.subs {
		s.cancel()
		delete(n.subs, k)
	}
	n.mu.Unlock()
	return peer.Err()
}

// renewLoop keeps this node's claims alive. The interval tracks the lease the
// control plane handed us: renewing every 5s under a 2s lease would drop
// workspaces we are actively serving.
func (n *Node) renewLoop(ctx context.Context) {
	defer n.wg.Done()
	t := time.NewTicker(250 * time.Millisecond)
	defer t.Stop()
	var last time.Time
	for {
		select {
		case <-ctx.Done():
			return
		case <-n.stop:
			return
		case <-t.C:
			n.mu.Lock()
			lease := n.leaseSec
			n.mu.Unlock()
			interval := 5 * time.Second
			if lease > 0 {
				interval = time.Duration(lease) * time.Second / 3
			}
			if interval < 250*time.Millisecond {
				interval = 250 * time.Millisecond
			}
			if time.Since(last) >= interval {
				n.renew(ctx)
				last = time.Now()
			}
		}
	}
}

func (n *Node) renew(ctx context.Context) {
	n.mu.Lock()
	p := n.peer
	req := proto.WSRenewReq{Gen: map[string]uint64{}}
	for id, w := range n.workspaces {
		req.IDs = append(req.IDs, id)
		req.Gen[id] = w.Generation
	}
	for id, gen := range n.materializing {
		if _, done := n.workspaces[id]; !done {
			req.IDs = append(req.IDs, id)
			req.Gen[id] = gen
		}
	}
	// Refresh broker leases that are within a minute of expiry.
	var refresh []*ws
	for _, w := range n.workspaces {
		for _, l := range w.leases {
			if l.ExpiresAt-time.Now().UnixMilli() < 60_000 {
				refresh = append(refresh, w)
				break
			}
		}
	}
	n.mu.Unlock()
	if p == nil {
		return
	}
	if len(req.IDs) > 0 {
		rctx, cancel := context.WithTimeout(ctx, 10*time.Second)
		if err := p.Call(rctx, proto.PeerControl, proto.OpWSRenew, req, nil); err != nil {
			n.logger.Warn("renew failed", "err", err)
		}
		cancel()
	}
	for _, w := range refresh {
		n.refreshLeases(ctx, w)
	}
}

func (n *Node) refreshLeases(ctx context.Context, w *ws) {
	n.mu.Lock()
	p := n.peer
	n.mu.Unlock()
	if p == nil {
		return
	}
	var res proto.BindingLeaseRes
	rctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	if err := p.Call(rctx, proto.PeerControl, proto.OpBindingLease, proto.BindingLeaseReq{WS: w.ID}, &res); err != nil {
		n.logger.Warn("lease refresh failed; broker will fail closed at expiry", "ws", w.ID, "err", err)
		return
	}
	n.mu.Lock()
	w.leases = res.Leases
	if w.broker != nil {
		w.broker.SetLeases(res.Leases)
	}
	n.mu.Unlock()
}

// resync re-declares the workspaces this node is already serving. A control
// plane that saw us disconnect has demoted them; ws.ready promotes them back.
// A conflict means the workspace moved on without us, so the local copy is
// dropped.
func (n *Node) resync(ctx context.Context) {
	n.mu.Lock()
	p := n.peer
	held := make([]*ws, 0, len(n.workspaces))
	for _, w := range n.workspaces {
		held = append(held, w)
	}
	n.mu.Unlock()
	if p == nil {
		return
	}
	for _, w := range held {
		rctx, cancel := context.WithTimeout(ctx, 15*time.Second)
		err := p.Call(rctx, proto.PeerControl, proto.OpWSReady, proto.WSReadyReq{ID: w.ID, Gen: w.Generation}, nil)
		cancel()
		if err == nil {
			continue
		}
		var pe *proto.Error
		if errors.As(err, &pe) && (pe.Code == proto.CodeConflict || pe.Code == proto.CodeNotFound) {
			n.logger.Info("dropping workspace we no longer own", "ws", w.ID, "reason", pe.Code)
			n.dropWorkspace(ctx, w.ID)
			continue
		}
		n.logger.Warn("resync failed", "ws", w.ID, "err", err)
	}
}

// EnvFileDir is the workspace-relative directory the node writes its
// per-materialization environment into. It is excluded from snapshots,
// because its contents are true only for the node currently holding the
// workspace.
const EnvFileDir = ".remount"

// EnvFilePath is the sourceable env file inside a workspace.
const EnvFilePath = EnvFileDir + "/env"

// writeWorkspaceEnv records where the broker is and who is hosting, so a
// harness can rediscover them after a move without the client re-configuring
// anything.
func writeWorkspaceEnv(handle workspace.Handle, w *ws) error {
	var b strings.Builder
	b.WriteString("# Written by the Remount node each time this workspace is materialized.\n")
	b.WriteString("# It changes when the workspace moves. Source it at start-up rather\n")
	b.WriteString("# than copying its values into a config file.\n")
	fmt.Fprintf(&b, "REMOUNT_WORKSPACE=%s\n", w.ID)
	fmt.Fprintf(&b, "REMOUNT_NODE_BACKEND=%s\n", handle.Backend())
	if w.broker != nil {
		fmt.Fprintf(&b, "REMOUNT_BROKER=%s\n", w.broker.BaseURL())
	}
	for _, l := range w.leases {
		// The placeholder, never the secret.
		fmt.Fprintf(&b, "REMOUNT_REF_%s=%s\n", strings.ToUpper(strings.TrimPrefix(l.ID, "b_")), broker.Placeholder(l))
	}
	if err := handle.FS().Mkdir(EnvFileDir); err != nil {
		return err
	}
	return handle.FS().Write(EnvFilePath, []byte(b.String()), 0o644, false, true)
}

// dropWorkspace tears down a workspace locally without telling the control
// plane (it already knows, or no longer cares).
func (n *Node) dropWorkspace(ctx context.Context, id string) {
	n.mu.Lock()
	w := n.workspaces[id]
	delete(n.workspaces, id)
	for k, s := range n.subs {
		if strings.HasSuffix(k, "|"+id) {
			s.cancel()
			delete(n.subs, k)
		}
	}
	n.mu.Unlock()
	if w == nil {
		return
	}
	n.sessions.KillWorkspace(id)
	if w.broker != nil {
		w.broker.Close()
	}
	if err := w.handle.Destroy(ctx); err != nil {
		n.logger.Warn("destroy failed", "ws", id, "err", err)
	}
}

// reclaimLocal re-claims workspaces whose directories are still on disk.
func (n *Node) reclaimLocal(ctx context.Context) {
	entries, err := os.ReadDir(filepath.Join(n.opts.DataDir, "ws"))
	if err != nil {
		return
	}
	for _, e := range entries {
		if !e.IsDir() || !strings.HasPrefix(e.Name(), "ws_") {
			continue
		}
		n.mu.Lock()
		_, have := n.workspaces[e.Name()]
		n.mu.Unlock()
		if have {
			continue
		}
		n.tryClaim(ctx, e.Name(), true)
	}
}

// ---------------------------------------------------------------------------
// inbound frames
// ---------------------------------------------------------------------------

func (n *Node) handle(ctx context.Context, p *transport.Peer, f *proto.Frame) {
	switch f.T {
	case proto.KindEvent:
		n.handleEvent(ctx, f)
	case proto.KindReq:
		go n.handleReq(ctx, p, f)
	}
}

func (n *Node) handleEvent(ctx context.Context, f *proto.Frame) {
	switch f.Op {
	case proto.EvWSOffer:
		var req proto.WSGetReq
		if err := f.Decode(&req); err == nil {
			go n.tryClaim(ctx, req.ID, false)
		}
	case proto.EvPeerGone:
		var body map[string]string
		_ = f.Decode(&body)
		n.dropClient(body["peer"])
	}
}

func (n *Node) dropClient(client string) {
	n.mu.Lock()
	defer n.mu.Unlock()
	for k, s := range n.subs {
		if strings.HasPrefix(k, client+"|") {
			s.cancel()
			delete(n.subs, k)
		}
	}
	for k := range n.grants {
		if strings.HasPrefix(k, client+"|") {
			delete(n.grants, k)
		}
	}
}

func (n *Node) handleReq(ctx context.Context, p *transport.Peer, f *proto.Frame) {
	body, err := n.dispatch(ctx, p, f)
	if err != nil {
		var pe *proto.Error
		if !errors.As(err, &pe) {
			pe = proto.Err(proto.CodeInternal, "%v", err)
		}
		_ = p.RespondErr(ctx, f, pe)
		return
	}
	_ = p.Respond(ctx, f, body)
}

func decode[T any](f *proto.Frame) (*T, error) {
	var v T
	if err := f.Decode(&v); err != nil {
		return nil, proto.Err(proto.CodeBadRequest, "decode %s: %v", f.Op, err)
	}
	return &v, nil
}

// authorize checks the grant for (client, ws); grants are cached per connection.
func (n *Node) authorize(client, wsID string, g *proto.Grant) (*ws, error) {
	n.mu.Lock()
	defer n.mu.Unlock()
	w := n.workspaces[wsID]
	if w == nil {
		return nil, proto.Err(proto.CodeNotFound, "workspace %s is not on this node", wsID)
	}
	key := client + "|" + wsID
	if g == nil {
		g = n.grants[key]
	}
	if err := control.VerifyGrant(n.ctrlPub, g, time.Now()); err != nil {
		return nil, err
	}
	if g.Claims.Client != client || g.Claims.WS != wsID || g.Claims.Node != n.id {
		return nil, proto.Err(proto.CodeUnauthorized, "grant is for a different client, workspace or node")
	}
	if g.Claims.Gen != w.Generation {
		return nil, proto.Err(proto.CodeConflict, "grant generation %d != workspace generation %d (workspace moved?)", g.Claims.Gen, w.Generation)
	}
	n.grants[key] = g
	return w, nil
}

func (n *Node) dispatch(ctx context.Context, p *transport.Peer, f *proto.Frame) (any, error) {
	if f.From == proto.PeerControl {
		switch f.Op {
		case proto.OpWSRelease:
			req, err := decode[proto.WSReleaseReq](f)
			if err != nil {
				return nil, err
			}
			return n.release(ctx, req)
		}
		return nil, proto.Err(proto.CodeUnsupported, "unknown control op %q", f.Op)
	}
	switch f.Op {
	case proto.OpNodeStatus:
		return n.status(), nil
	case proto.OpNodeDiag:
		// Diagnostics expose workspace roots and session programs, so the
		// caller must prove entitlement to a workspace here. A node holding
		// nothing has nothing to protect.
		req, err := decode[proto.NodeDiagReq](f)
		if err != nil {
			return nil, err
		}
		n.mu.Lock()
		anyWS := len(n.workspaces) > 0
		n.mu.Unlock()
		if anyWS {
			if req.WS == "" {
				return nil, proto.Err(proto.CodeUnauthorized,
					"node.diag needs a workspace and grant; this node holds workspaces")
			}
			if _, err := n.authorize(f.From, req.WS, req.Grant); err != nil {
				return nil, err
			}
		}
		d := n.Diag(ctx)
		if req.Verify {
			d.Findings = append(d.Findings, n.VerifyArtifacts()...)
		}
		return d, nil
	case proto.OpSOpen:
		req, err := decode[proto.SOpenReq](f)
		if err != nil {
			return nil, err
		}
		w, err := n.authorize(f.From, req.WS, req.Grant)
		if err != nil {
			return nil, err
		}
		return n.sOpen(ctx, p, f.From, w, req)
	case proto.OpPortOpen:
		req, err := decode[proto.PortOpenReq](f)
		if err != nil {
			return nil, err
		}
		w, err := n.authorize(f.From, req.WS, req.Grant)
		if err != nil {
			return nil, err
		}
		return n.portOpen(ctx, p, f.From, w, req)
	case proto.OpSAttach:
		req, err := decode[proto.SAttachReq](f)
		if err != nil {
			return nil, err
		}
		s, err := n.sessionFor(f.From, req.S, req.Grant)
		if err != nil {
			return nil, err
		}
		n.subscribe(p, f.From, s, req.From)
		return proto.SOpenRes{S: s.ID, Next: s.Log.Next()}, nil
	case proto.OpSInput:
		req, err := decode[proto.SInputReq](f)
		if err != nil {
			return nil, err
		}
		s, err := n.sessionFor(f.From, req.S, nil)
		if err != nil {
			return nil, err
		}
		return struct{}{}, s.Input(req.ISeq, req.Data, req.EOF)
	case proto.OpSResize:
		req, err := decode[proto.SResizeReq](f)
		if err != nil {
			return nil, err
		}
		s, err := n.sessionFor(f.From, req.S, nil)
		if err != nil {
			return nil, err
		}
		return struct{}{}, s.Resize(req.Rows, req.Cols)
	case proto.OpSSignal:
		req, err := decode[proto.SSignalReq](f)
		if err != nil {
			return nil, err
		}
		s, err := n.sessionFor(f.From, req.S, nil)
		if err != nil {
			return nil, err
		}
		return struct{}{}, s.Signal(req.Signal)
	case proto.OpSAck:
		return struct{}{}, nil // liveness only in v0; cursors are per-subscriber
	case proto.OpSClose:
		req, err := decode[proto.SCloseReq](f)
		if err != nil {
			return nil, err
		}
		s, err := n.sessionFor(f.From, req.S, nil)
		if err != nil {
			return nil, err
		}
		n.unsubscribe(f.From, s.ID)
		if req.Kill {
			n.sessions.Remove(s.ID, true)
		}
		return struct{}{}, nil
	case proto.OpSWait:
		req, err := decode[proto.SWaitReq](f)
		if err != nil {
			return nil, err
		}
		s, err := n.sessionFor(f.From, req.S, nil)
		if err != nil {
			return nil, err
		}
		wctx := ctx
		var cancel context.CancelFunc
		if req.TimeoutSec > 0 {
			wctx, cancel = context.WithTimeout(ctx, time.Duration(req.TimeoutSec)*time.Second)
			defer cancel()
		}
		info, err := s.Wait(wctx)
		if err != nil {
			return proto.SWaitRes{Exited: false}, nil
		}
		return proto.SWaitRes{Exited: true, Exit: info}, nil
	case proto.OpSList:
		req, err := decode[proto.SListReq](f)
		if err != nil {
			return nil, err
		}
		if req.WS != "" {
			if _, err := n.authorize(f.From, req.WS, nil); err != nil {
				return nil, err
			}
		}
		res := proto.SListRes{}
		for _, s := range n.sessions.List(req.WS) {
			if req.WS == "" {
				if _, err := n.authorize(f.From, s.WS, nil); err != nil {
					continue
				}
			}
			res.Sessions = append(res.Sessions, proto.SessionStatus{Info: s.Info, Exited: s.Exited(), Exit: s.ExitInfo(), Next: s.Log.Next(), Oldest: s.Log.Oldest()})
		}
		return res, nil
	case proto.OpFSRead:
		req, err := decode[proto.FSReadReq](f)
		if err != nil {
			return nil, err
		}
		w, err := n.authorize(f.From, req.WS, req.Grant)
		if err != nil {
			return nil, err
		}
		return w.handle.FS().Read(req.Path, req.Offset, req.Limit)
	case proto.OpFSWrite:
		req, err := decode[proto.FSWriteReq](f)
		if err != nil {
			return nil, err
		}
		w, err := n.authorize(f.From, req.WS, req.Grant)
		if err != nil {
			return nil, err
		}
		if err := w.handle.FS().Write(req.Path, req.Data, req.Mode, req.Append, req.MkdirP); err != nil {
			return nil, err
		}
		n.emit(proto.EvFSWrite, w.ID, w.Spec.Principal, map[string]any{"path": req.Path, "bytes": len(req.Data), "client": f.From})
		return struct{}{}, nil
	case proto.OpFSList:
		req, err := decode[proto.FSListReq](f)
		if err != nil {
			return nil, err
		}
		w, err := n.authorize(f.From, req.WS, req.Grant)
		if err != nil {
			return nil, err
		}
		ents, err := w.handle.FS().List(req.Path)
		if err != nil {
			return nil, err
		}
		return proto.FSListRes{Entries: ents}, nil
	case proto.OpFSStat:
		req, err := decode[proto.FSStatReq](f)
		if err != nil {
			return nil, err
		}
		w, err := n.authorize(f.From, req.WS, req.Grant)
		if err != nil {
			return nil, err
		}
		e, err := w.handle.FS().Stat(req.Path)
		if err != nil {
			return nil, err
		}
		return proto.FSStatRes{Entry: *e}, nil
	case proto.OpFSMkdir:
		req, err := decode[proto.FSMkdirReq](f)
		if err != nil {
			return nil, err
		}
		w, err := n.authorize(f.From, req.WS, req.Grant)
		if err != nil {
			return nil, err
		}
		return struct{}{}, w.handle.FS().Mkdir(req.Path)
	case proto.OpFSRemove:
		req, err := decode[proto.FSRemoveReq](f)
		if err != nil {
			return nil, err
		}
		w, err := n.authorize(f.From, req.WS, req.Grant)
		if err != nil {
			return nil, err
		}
		if err := w.handle.FS().Remove(req.Path, req.Recursive); err != nil {
			return nil, err
		}
		n.emit(proto.EvFSRemove, w.ID, w.Spec.Principal, map[string]any{"path": req.Path, "client": f.From})
		return struct{}{}, nil
	case proto.OpFSRename:
		req, err := decode[proto.FSRenameReq](f)
		if err != nil {
			return nil, err
		}
		w, err := n.authorize(f.From, req.WS, req.Grant)
		if err != nil {
			return nil, err
		}
		return struct{}{}, w.handle.FS().Rename(req.From, req.To)
	case proto.OpFSSearch:
		req, err := decode[proto.FSSearchReq](f)
		if err != nil {
			return nil, err
		}
		w, err := n.authorize(f.From, req.WS, req.Grant)
		if err != nil {
			return nil, err
		}
		return w.handle.FS().Search(req.Path, req.Pattern, req.Glob, req.MaxResults)
	case proto.OpFSEdit:
		req, err := decode[proto.FSEditReq](f)
		if err != nil {
			return nil, err
		}
		w, err := n.authorize(f.From, req.WS, req.Grant)
		if err != nil {
			return nil, err
		}
		nrep, err := w.handle.FS().Edit(req.Path, req.Edits)
		if err != nil {
			return nil, err
		}
		n.emit(proto.EvFSEdit, w.ID, w.Spec.Principal, map[string]any{"path": req.Path, "replacements": nrep, "client": f.From})
		return proto.FSEditRes{Replacements: nrep}, nil
	case proto.OpWSSnapshot:
		req, err := decode[proto.WSSnapshotReq](f)
		if err != nil {
			return nil, err
		}
		w, err := n.authorize(f.From, req.WS, req.Grant)
		if err != nil {
			return nil, err
		}
		id, size, err := n.snapshot(ctx, w, req.Upload)
		if err != nil {
			return nil, err
		}
		return proto.WSSnapshotRes{Artifact: id, Bytes: size}, nil
	case proto.OpWSInfo:
		req, err := decode[proto.WSGetReq](f)
		if err != nil {
			return nil, err
		}
		w, err := n.authorize(f.From, req.ID, nil)
		if err != nil {
			return nil, err
		}
		info := proto.WSInfoRes{WS: w.ID, Backend: w.handle.Backend(), Root: w.handle.FS().Root()}
		if w.broker != nil {
			info.Broker = w.broker.BaseURL()
		}
		for _, s := range n.sessions.List(w.ID) {
			info.Sessions = append(info.Sessions, s.ID)
		}
		return info, nil
	}
	return nil, proto.Err(proto.CodeUnsupported, "unknown node op %q", f.Op)
}

// sessionFor looks up a session and checks the client is authorized for its workspace.
func (n *Node) sessionFor(client, sid string, g *proto.Grant) (*session.Session, error) {
	s, ok := n.sessions.Get(sid)
	if !ok {
		return nil, proto.Err(proto.CodeNotFound, "session %s", sid)
	}
	if _, err := n.authorize(client, s.WS, g); err != nil {
		return nil, err
	}
	return s, nil
}

func (n *Node) status() proto.NodeStatus {
	info := workspace.HostInfo(n.opts.Backends.Names())
	info.Version = n.opts.Version
	st := proto.NodeStatus{ID: n.id, Labels: n.opts.Labels, Info: info, Online: true, LastSeen: time.Now().UnixMilli()}
	n.mu.Lock()
	for id := range n.workspaces {
		st.Workspaces = append(st.Workspaces, id)
	}
	n.mu.Unlock()
	// lint:locks-ok st is a local value, never published
	sort.Strings(st.Workspaces)
	return st
}

// ---------------------------------------------------------------------------
// sessions and streaming
// ---------------------------------------------------------------------------

func (n *Node) sOpen(ctx context.Context, p *transport.Peer, client string, w *ws, req *proto.SOpenReq) (any, error) {
	env := map[string]string{}
	for k, v := range w.Spec.Env {
		env[k] = v
	}
	for k, v := range req.Env {
		env[k] = v
	}
	base := ""
	if w.broker != nil {
		base = w.broker.BaseURL()
	}
	env = broker.ResolveEnv(env, base, w.leases)
	var envList []string
	if w.broker != nil {
		envList = append(envList, w.broker.EnvFor()...)
	}
	envList = append(envList, workspace.MapEnv(env)...)
	spec := session.Spec{
		WS: w.ID, Kind: req.Kind, Program: req.Program, Cwd: req.Cwd, Env: envList,
		Rows: req.Rows, Cols: req.Cols, Stdin: req.Stdin, IdempotencyKey: req.IdempotencyKey,
		Principal: w.Spec.Principal,
	}
	if req.IdempotencyKey != "" {
		spec.IdempotencyKey = w.ID + "|" + req.IdempotencyKey
	}
	if req.TimeoutSec > 0 {
		spec.Timeout = time.Duration(req.TimeoutSec) * time.Second
	}
	if err := w.handle.Prepare(&spec); err != nil {
		return nil, err
	}
	s, err := n.sessions.Open(spec)
	if err != nil {
		return nil, err
	}
	n.emit(proto.EvSOpened, w.ID, w.Spec.Principal, map[string]any{"s": s.ID, "kind": req.Kind, "program": req.Program, "client": client})
	if !req.NoSubscribe {
		n.subscribe(p, client, s, 0)
	}
	return proto.SOpenRes{S: s.ID, Next: s.Log.Next()}, nil
}

func (n *Node) portOpen(ctx context.Context, p *transport.Peer, client string, w *ws, req *proto.PortOpenReq) (any, error) {
	spec := session.Spec{WS: w.ID, Kind: proto.SessionPort, Host: req.Host, Port: req.Port, Principal: w.Spec.Principal}
	if err := w.handle.Prepare(&spec); err != nil {
		return nil, err
	}
	s, err := n.sessions.Open(spec)
	if err != nil {
		return nil, err
	}
	// A port session is a connection, not a program: if the dial failed there
	// is nothing to attach to, so report it as an error the caller can retry
	// rather than handing back a session that is already dead.
	if s.Exited() {
		info := s.ExitInfo()
		n.sessions.Remove(s.ID, true)
		msg := "connection closed immediately"
		if info != nil && info.Error != "" {
			msg = info.Error
		}
		return nil, proto.Err(proto.CodeUnreachable, "port %d: %s", req.Port, msg)
	}
	n.subscribe(p, client, s, 0)
	return proto.SOpenRes{S: s.ID, Next: s.Log.Next()}, nil
}

// subscribe streams s's log to client from seq `from` until the client
// detaches, the connection dies, or the log ends. One stream per
// (client, session): a re-attach replaces the previous cursor.
func (n *Node) subscribe(p *transport.Peer, client string, s *session.Session, from uint64) {
	key := client + "|" + s.ID
	ctx, cancel := context.WithCancel(context.Background())
	n.mu.Lock()
	if old, ok := n.subs[key]; ok {
		old.cancel()
	}
	n.subs[key] = &subscriber{cancel: cancel}
	n.mu.Unlock()
	go func() {
		defer func() {
			n.mu.Lock()
			if cur, ok := n.subs[key]; ok && cur.cancel != nil {
				// only remove if it's still ours (compare by ctx doneness)
				select {
				case <-ctx.Done():
					delete(n.subs, key)
				default:
				}
			}
			n.mu.Unlock()
		}()
		cur := s.Log.CursorAt(from)
		for {
			chunks, err := cur.Next(ctx, 64)
			if err != nil {
				var ev *session.ErrEvicted
				if errors.As(err, &ev) {
					// Truthful gap, then continue from what exists.
					metrics.GapsReported.Inc()
					gap := proto.MustMarshal(proto.Gap{From: ev.Requested, To: ev.Oldest - 1})
					f := &proto.Frame{V: proto.Version, T: proto.KindChunk, To: client, S: s.ID, WS: s.WS, Seq: ev.Requested, Body: proto.MustMarshal(proto.ChunkBody{Stream: proto.StreamGap, Data: gap})}
					if p.Send(ctx, f) != nil {
						return
					}
					cur.Skip(ev.Oldest)
					continue
				}
				return // EOF or cancelled
			}
			for _, c := range chunks {
				f := &proto.Frame{V: proto.Version, T: proto.KindChunk, To: client, S: s.ID, WS: s.WS, Seq: c.Seq, Body: proto.MustMarshal(proto.ChunkBody{Stream: c.Stream, Data: c.Data})}
				if err := p.Send(ctx, f); err != nil {
					return
				}
			}
		}
	}()
}

func (n *Node) unsubscribe(client, sid string) {
	n.mu.Lock()
	defer n.mu.Unlock()
	key := client + "|" + sid
	if s, ok := n.subs[key]; ok {
		s.cancel()
		delete(n.subs, key)
	}
}

// ---------------------------------------------------------------------------
// claims, restore, snapshot, release
// ---------------------------------------------------------------------------

func (n *Node) tryClaim(ctx context.Context, wsID string, adopt bool) {
	// Reserve the workspace before talking to the control plane: offers can
	// arrive more than once and two concurrent claims on the same node would
	// both succeed (the second looks like a re-adoption) and then fight over
	// the same directory.
	n.mu.Lock()
	p := n.peer
	_, have := n.workspaces[wsID]
	_, busy := n.materializing[wsID]
	if have || busy || p == nil {
		n.mu.Unlock()
		return
	}
	n.materializing[wsID] = 0
	n.mu.Unlock()
	defer func() {
		n.mu.Lock()
		delete(n.materializing, wsID)
		n.mu.Unlock()
	}()
	var res proto.WSClaimRes
	cctx, cancel := context.WithTimeout(ctx, 15*time.Second)
	err := p.Call(cctx, proto.PeerControl, proto.OpWSClaim, proto.WSClaimReq{ID: wsID}, &res)
	cancel()
	if err != nil {
		if adopt {
			// Nobody wants what we have; it was destroyed or lives elsewhere now.
			var pe *proto.Error
			if errors.As(err, &pe) && (pe.Code == proto.CodeNotFound || pe.Code == proto.CodeConflict) {
				n.logger.Info("dropping local workspace copy", "ws", wsID, "reason", pe.Code)
				if be, err := n.opts.Backends.Get(""); err == nil {
					if h, err := be.Adopt(ctx, wsID); err == nil {
						_ = h.Destroy(ctx)
					}
				}
			}
		}
		return
	}
	n.mu.Lock()
	n.materializing[wsID] = res.Workspace.Generation
	n.mu.Unlock()
	if err := n.materialize(ctx, res.Workspace, adopt); err != nil {
		n.logger.Error("materialize failed; releasing", "ws", wsID, "err", err)
		_ = p.Call(ctx, proto.PeerControl, proto.OpWSReleased, proto.WSReleasedReq{ID: wsID, Gen: res.Workspace.Generation, Reason: "materialize failed: " + err.Error()}, nil)
	}
}

func (n *Node) materialize(ctx context.Context, w proto.Workspace, adopt bool) error {
	be, err := n.opts.Backends.Get(w.Spec.Requires.Backend)
	if err != nil {
		return err
	}
	var handle workspace.Handle
	if adopt {
		handle, err = be.Adopt(ctx, w.ID)
		if err != nil {
			adopt = false
		}
	}
	if !adopt {
		create := func() (workspace.Handle, error) {
			var restore io.Reader
			var closer io.Closer
			if w.Spec.RestoreFrom != "" {
				r, ferr := n.fetchArtifact(ctx, w.Spec.RestoreFrom)
				if ferr != nil {
					return nil, fmt.Errorf("fetch %s: %w", w.Spec.RestoreFrom, ferr)
				}
				restore, closer = r, r
			}
			h, cerr := be.Create(ctx, w.ID, w.Spec, restore)
			if closer != nil {
				closer.Close()
			}
			return h, cerr
		}
		handle, err = create()
		var pe *proto.Error
		if err != nil && errors.As(err, &pe) && pe.Code == proto.CodeConflict {
			// A directory left over from a previous life. The snapshot the
			// control plane named is authoritative, so replace it; with no
			// snapshot the leftover is the only copy, so adopt it.
			if w.Spec.RestoreFrom != "" {
				if stale, aerr := be.Adopt(ctx, w.ID); aerr == nil {
					_ = stale.Destroy(ctx)
				}
				handle, err = create()
			} else {
				handle, err = be.Adopt(ctx, w.ID)
			}
		}
		if err != nil {
			return err
		}
	}
	entry := &ws{Workspace: w, handle: handle}
	// Broker: one per workspace, always on, so every session has an egress path.
	var leases []proto.BindingLease
	if len(w.Spec.Bindings) > 0 {
		var res proto.BindingLeaseRes
		n.mu.Lock()
		p := n.peer
		n.mu.Unlock()
		if p != nil {
			lctx, cancel := context.WithTimeout(ctx, 10*time.Second)
			if err := p.Call(lctx, proto.PeerControl, proto.OpBindingLease, proto.BindingLeaseReq{WS: w.ID}, &res); err != nil {
				cancel()
				_ = handle.Destroy(ctx)
				return fmt.Errorf("binding lease: %w", err)
			}
			cancel()
			leases = res.Leases
		}
	}
	entry.leases = leases
	entry.broker = broker.New(broker.Options{
		WS: w.ID, Principal: w.Spec.Principal, Leases: leases, Allow: n.opts.Allow, AllowPrivate: n.opts.AllowPrivate,
		Audit: func(a broker.Audit) {
			typ := proto.EvEgressAllowed
			switch a.Decision {
			case broker.DecisionSubstituted:
				typ = proto.EvCredUsed
			case broker.DecisionDenied, broker.DecisionLeakBlocked, broker.DecisionExpired:
				typ = proto.EvEgressDenied
			}
			n.emit(typ, a.WS, a.Principal, map[string]any{"decision": a.Decision, "binding": a.Binding, "host": a.Host, "method": a.Method, "path": a.Path, "reason": a.Reason, "status": a.Status})
		},
	})
	if _, err := entry.broker.Start(); err != nil {
		_ = handle.Destroy(ctx)
		return err
	}
	// Drop a sourceable env file into the workspace. The broker's address
	// changes every time a workspace is materialized, so anything that bakes
	// it into a config file goes stale after a move. Reading this file at
	// start-up is the portable way to find it.
	if err := writeWorkspaceEnv(handle, entry); err != nil {
		n.logger.Warn("could not write .remount/env", "ws", w.ID, "err", err)
	}
	n.mu.Lock()
	n.workspaces[w.ID] = entry
	n.mu.Unlock()
	if w.Spec.RestoreFrom != "" && !adopt {
		metrics.RestoresDone.Inc()
		n.emit(proto.EvWSRestored, w.ID, w.Spec.Principal, map[string]any{"from": w.Spec.RestoreFrom, "backend": be.Name()})
	}
	n.logger.Info("workspace claimed", "ws", w.ID, "gen", w.Generation, "backend", be.Name(), "restore", w.Spec.RestoreFrom, "adopted", adopt)
	// Only now is the workspace serviceable; tell the control plane so a
	// client waiting on it is not handed a node that is still restoring.
	n.mu.Lock()
	p := n.peer
	n.mu.Unlock()
	if p != nil {
		rctx, cancel := context.WithTimeout(ctx, 15*time.Second)
		defer cancel()
		if err := p.Call(rctx, proto.PeerControl, proto.OpWSReady, proto.WSReadyReq{ID: w.ID, Gen: w.Generation}, nil); err != nil {
			n.logger.Warn("ws.ready failed", "ws", w.ID, "err", err)
		}
	}
	return nil
}

func (n *Node) fetchArtifact(ctx context.Context, id string) (io.ReadCloser, error) {
	if n.store.Has(id) {
		r, _, err := n.store.Open(id)
		return r, err
	}
	if n.opts.ArtifactURL == "" {
		return nil, fmt.Errorf("artifact %s not local and no artifact URL configured", id)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, strings.TrimSuffix(n.opts.ArtifactURL, "/")+"/"+id, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Authorization", "Bearer "+n.opts.Token)
	resp, err := n.opts.HTTPClient.Do(req)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode != http.StatusOK {
		resp.Body.Close()
		return nil, fmt.Errorf("artifact %s: HTTP %d", id, resp.StatusCode)
	}
	// Cache locally while streaming through, verifying the digest.
	got, _, err := n.store.Put(resp.Body)
	resp.Body.Close()
	if err != nil {
		return nil, err
	}
	if got != id {
		_ = n.store.Delete(got)
		return nil, fmt.Errorf("artifact %s: digest mismatch (%s)", id, got)
	}
	r, _, err := n.store.Open(id)
	return r, err
}

func (n *Node) snapshot(ctx context.Context, w *ws, upload bool) (string, int64, error) {
	// .remount is node-local truth and must never travel with the workspace.
	excludes := append([]string{EnvFileDir}, w.Spec.Exclude...)
	pr, pw := io.Pipe()
	go func() { pw.CloseWithError(w.handle.Snapshot(ctx, excludes, pw)) }()
	id, size, err := n.store.Put(pr)
	if err != nil {
		pr.CloseWithError(err)
		return "", 0, err
	}
	if upload && n.opts.ArtifactURL != "" {
		if err := n.upload(ctx, id); err != nil {
			return "", 0, fmt.Errorf("upload: %w", err)
		}
	}
	metrics.SnapshotsTaken.Inc()
	metrics.SnapshotBytes.Add(uint64(size))
	n.emit(proto.EvWSSnapshot, w.ID, w.Spec.Principal, map[string]any{"artifact": id, "bytes": size, "uploaded": upload})
	return id, size, nil
}

func (n *Node) upload(ctx context.Context, id string) error {
	r, size, err := n.store.Open(id)
	if err != nil {
		return err
	}
	defer r.Close()
	req, err := http.NewRequestWithContext(ctx, http.MethodPut, strings.TrimSuffix(n.opts.ArtifactURL, "/")+"/"+id, r)
	if err != nil {
		return err
	}
	req.ContentLength = size
	req.Header.Set("Authorization", "Bearer "+n.opts.Token)
	resp, err := n.opts.HTTPClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode/100 != 2 {
		b, _ := io.ReadAll(io.LimitReader(resp.Body, 1024))
		return fmt.Errorf("HTTP %d: %s", resp.StatusCode, strings.TrimSpace(string(b)))
	}
	return nil
}

// release gives a workspace back to the control plane.
func (n *Node) release(ctx context.Context, req *proto.WSReleaseReq) (any, error) {
	n.mu.Lock()
	w := n.workspaces[req.WS]
	if w == nil {
		n.mu.Unlock()
		return nil, proto.Err(proto.CodeNotFound, "workspace %s not here", req.WS)
	}
	if w.Generation != req.Gen {
		n.mu.Unlock()
		return nil, proto.Err(proto.CodeConflict, "generation mismatch")
	}
	delete(n.workspaces, req.WS)
	for k, s := range n.subs {
		if strings.HasSuffix(k, "|"+req.WS) {
			s.cancel()
		}
	}
	n.mu.Unlock()
	// Stop processes first so the snapshot is quiescent.
	n.sessions.KillWorkspace(req.WS)
	out := proto.WSReleasedReq{ID: req.WS, Gen: req.Gen, Reason: req.Reason}
	if req.Snapshot {
		id, _, err := n.snapshot(ctx, w, true)
		if err != nil {
			n.logger.Error("snapshot on release failed", "ws", req.WS, "err", err)
		} else {
			out.Snapshot = id
		}
	}
	if w.broker != nil {
		w.broker.Close()
	}
	if err := w.handle.Destroy(ctx); err != nil {
		n.logger.Warn("destroy failed", "ws", req.WS, "err", err)
	}
	return out, nil
}
