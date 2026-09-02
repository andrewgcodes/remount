// Package control is the Remount control plane: identity, the workspace
// claim queue, leases, timers, secret bindings, grants and the canonical
// event log. It carries coordination only; session bytes flow between
// clients and nodes through the relay and never pass through here.
//
// The scheduler is a claim queue, not a placement engine
// (docs/adr/0009-claim-queue.md): a pending workspace is offered to eligible
// nodes, the first ws.claim wins a single-row compare-and-swap, the claim
// carries a lease, and an expired lease returns the workspace to pending
// with restore_from set to its last snapshot. Split brain is impossible by
// construction because Generation bumps on every claim and nodes act only
// on their own generation.
package control

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/subtle"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"sort"
	"strings"
	"sync"
	"time"

	"remount.dev/remount/internal/eventlog"
	"remount.dev/remount/internal/ids"
	"remount.dev/remount/internal/metrics"
	"remount.dev/remount/internal/proto"
	"remount.dev/remount/internal/relay"
)

// Binding is a secret the control plane can lease to nodes.
type Binding struct {
	ID           string   `json:"id"`
	Secret       string   `json:"secret"`
	Destinations []string `json:"destinations"`
	Principals   []string `json:"principals,omitempty"` // empty = any principal
	Placeholder  string   `json:"placeholder,omitempty"`
	TTLSec       int64    `json:"ttl_sec,omitempty"` // default 600
}

// Options configure the control plane.
type Options struct {
	DB         *sql.DB            // shared with the event log; required
	Log        *eventlog.Log      // required
	Token      string             // shared bearer token; empty = open (standalone)
	SigningKey ed25519.PrivateKey // grants; generated if nil
	LeaseSec   int64              // claim lease; default 30
	Bindings   []Binding
	Logger     *slog.Logger
	// Artifacts lets the control plane report on and verify its blob store.
	// Optional: without it, artifact checks report as unavailable rather
	// than as passing.
	Artifacts ArtifactStore
	Now       func() time.Time // injectable clock for tests
}

// ArtifactStore is the part of the blob store the control plane inspects.
type ArtifactStore interface {
	List() ([]string, error)
	Verify(id string) error
	Open(id string) (io.ReadCloser, int64, error)
}

// Control implements relay.Controller.
type Control struct {
	opts   Options
	db     *sql.DB
	log    *eventlog.Log
	key    ed25519.PrivateKey
	send   relay.Sender
	logger *slog.Logger
	now    func() time.Time

	mu         sync.Mutex
	workspaces map[string]*proto.Workspace
	timers     map[string]*proto.Timer
	nodes      map[string]*nodeState
	clients    map[string]*proto.Hello
	idem       map[string]string
	bindings   map[string]Binding
	tails      map[string]context.CancelFunc // events.tail per requester

	started time.Time
	stop    chan struct{}
	wg      sync.WaitGroup
}

type nodeState struct {
	Status proto.NodeStatus
	PubKey []byte
}

// New creates a control plane. Call Attach with the relay before Serve.
func New(opts Options) (*Control, error) {
	if opts.DB == nil || opts.Log == nil {
		return nil, errors.New("control: DB and Log are required")
	}
	if opts.LeaseSec == 0 {
		opts.LeaseSec = 30
	}
	if opts.Logger == nil {
		opts.Logger = slog.Default()
	}
	if opts.Now == nil {
		opts.Now = time.Now
	}
	c := &Control{
		opts: opts, db: opts.DB, log: opts.Log, logger: opts.Logger, now: opts.Now,
		workspaces: map[string]*proto.Workspace{}, timers: map[string]*proto.Timer{},
		nodes: map[string]*nodeState{}, clients: map[string]*proto.Hello{}, idem: map[string]string{},
		bindings: map[string]Binding{}, tails: map[string]context.CancelFunc{},
		started: opts.Now(), stop: make(chan struct{}),
	}
	for _, b := range opts.Bindings {
		c.bindings[b.ID] = b
	}
	if err := c.migrate(); err != nil {
		return nil, err
	}
	key := opts.SigningKey
	if key == nil {
		k, err := c.loadOrCreateKey()
		if err != nil {
			return nil, err
		}
		key = k
	}
	c.key = key
	if err := c.load(); err != nil {
		return nil, err
	}
	return c, nil
}

// Attach wires the relay used to reach peers.
func (c *Control) Attach(s relay.Sender) { c.send = s }

// PublicKey returns the grant-signing key.
func (c *Control) PublicKey() ed25519.PublicKey { return c.key.Public().(ed25519.PublicKey) }

// Start runs the lease/timer/offer loops.
func (c *Control) Start() {
	c.wg.Add(1)
	go c.loop()
}

// Stop halts loops.
func (c *Control) Stop() {
	close(c.stop)
	c.wg.Wait()
}

// ---------------------------------------------------------------------------
// persistence
// ---------------------------------------------------------------------------

func (c *Control) migrate() error {
	_, err := c.db.Exec(`
CREATE TABLE IF NOT EXISTS workspaces (id TEXT PRIMARY KEY, data BLOB NOT NULL);
CREATE TABLE IF NOT EXISTS timers (id TEXT PRIMARY KEY, data BLOB NOT NULL);
CREATE TABLE IF NOT EXISTS nodes (id TEXT PRIMARY KEY, pubkey BLOB, data BLOB NOT NULL);
CREATE TABLE IF NOT EXISTS idem (key TEXT PRIMARY KEY, ws TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS keys (name TEXT PRIMARY KEY, priv BLOB NOT NULL);
`)
	return err
}

func (c *Control) loadOrCreateKey() (ed25519.PrivateKey, error) {
	var priv []byte
	err := c.db.QueryRow(`SELECT priv FROM keys WHERE name='grant'`).Scan(&priv)
	if err == nil && len(priv) == ed25519.PrivateKeySize {
		return ed25519.PrivateKey(priv), nil
	}
	_, k, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		return nil, err
	}
	if _, err := c.db.Exec(`INSERT OR REPLACE INTO keys(name, priv) VALUES('grant', ?)`, []byte(k)); err != nil {
		return nil, err
	}
	return k, nil
}

func (c *Control) load() error {
	rows, err := c.db.Query(`SELECT data FROM workspaces`)
	if err != nil {
		return err
	}
	for rows.Next() {
		var b []byte
		if err := rows.Scan(&b); err != nil {
			rows.Close()
			return err
		}
		var ws proto.Workspace
		if err := proto.Unmarshal(b, &ws); err != nil {
			rows.Close()
			return err
		}
		// Nothing is online after a restart: claimed workspaces are pending
		// again with their last snapshot. Nodes that still hold them will
		// reclaim with a fresh generation and Adopt their local copy.
		if held(ws.State) {
			ws.State = proto.WSPending
			ws.Node = ""
			ws.LeaseUntil = 0
			ws.Spec.RestoreFrom = ws.LastSnapshot
		}
		c.workspaces[ws.ID] = &ws
	}
	rows.Close()
	rows, err = c.db.Query(`SELECT data FROM timers`)
	if err != nil {
		return err
	}
	for rows.Next() {
		var b []byte
		if err := rows.Scan(&b); err != nil {
			rows.Close()
			return err
		}
		var t proto.Timer
		if err := proto.Unmarshal(b, &t); err != nil {
			rows.Close()
			return err
		}
		c.timers[t.ID] = &t
	}
	rows.Close()
	rows, err = c.db.Query(`SELECT id, pubkey, data FROM nodes`)
	if err != nil {
		return err
	}
	for rows.Next() {
		var id string
		var pub, b []byte
		if err := rows.Scan(&id, &pub, &b); err != nil {
			rows.Close()
			return err
		}
		var st proto.NodeStatus
		_ = proto.Unmarshal(b, &st)
		st.Online = false
		c.nodes[id] = &nodeState{Status: st, PubKey: pub}
	}
	rows.Close()
	rows, err = c.db.Query(`SELECT key, ws FROM idem`)
	if err != nil {
		return err
	}
	for rows.Next() {
		var k, w string
		if err := rows.Scan(&k, &w); err == nil {
			c.idem[k] = w
		}
	}
	rows.Close()
	return nil
}

func (c *Control) saveWS(ws *proto.Workspace) {
	ws.UpdatedAt = c.now().UnixMilli()
	if _, err := c.db.Exec(`INSERT OR REPLACE INTO workspaces(id, data) VALUES(?,?)`, ws.ID, proto.MustMarshal(ws)); err != nil {
		c.logger.Error("save workspace", "err", err)
	}
}

func (c *Control) saveTimer(t *proto.Timer) {
	if _, err := c.db.Exec(`INSERT OR REPLACE INTO timers(id, data) VALUES(?,?)`, t.ID, proto.MustMarshal(t)); err != nil {
		c.logger.Error("save timer", "err", err)
	}
}

func (c *Control) saveNode(id string, n *nodeState) {
	if _, err := c.db.Exec(`INSERT OR REPLACE INTO nodes(id, pubkey, data) VALUES(?,?,?)`, id, n.PubKey, proto.MustMarshal(n.Status)); err != nil {
		c.logger.Error("save node", "err", err)
	}
}

func (c *Control) emit(ctx context.Context, typ, stream, principal, node string, payload any) uint64 {
	e, err := c.log.Emit(ctx, typ, stream, principal, node, payload, 0)
	if err != nil {
		c.logger.Error("emit", "type", typ, "err", err)
		return 0
	}
	return e.Seq
}

// ---------------------------------------------------------------------------
// relay.Controller
// ---------------------------------------------------------------------------

// Authenticate checks the token and assigns/validates the peer id.
func (c *Control) Authenticate(ctx context.Context, h *proto.Hello) (string, *proto.HelloOK, error) {
	if c.opts.Token != "" && subtle.ConstantTimeCompare([]byte(h.Token), []byte(c.opts.Token)) != 1 {
		return "", nil, proto.Err(proto.CodeUnauthorized, "bad token")
	}
	ok := &proto.HelloOK{Caps: []string{"v1"}, Server: "remount", Now: c.now().UnixMilli(), PubKey: c.PublicKey(), LeaseSec: c.opts.LeaseSec}
	switch h.Role {
	case proto.RoleNode:
		if h.Peer == "" || !strings.HasPrefix(h.Peer, "n_") || len(h.PubKey) != ed25519.PublicKeySize {
			return "", nil, proto.Err(proto.CodeBadRequest, "node hello needs an n_ id and an ed25519 public key")
		}
		c.mu.Lock()
		defer c.mu.Unlock()
		if n, exists := c.nodes[h.Peer]; exists && len(n.PubKey) > 0 && subtle.ConstantTimeCompare(n.PubKey, h.PubKey) != 1 {
			return "", nil, proto.Err(proto.CodeUnauthorized, "node id %s is registered to a different key", h.Peer)
		}
		ok.Peer = h.Peer
		return h.Peer, ok, nil
	case proto.RoleClient:
		id := h.Peer
		if id == "" || !strings.HasPrefix(id, "c_") {
			id = ids.New("c")
		}
		ok.Peer = id
		return id, ok, nil
	default:
		return "", nil, proto.Err(proto.CodeBadRequest, "unknown role %q", h.Role)
	}
}

// PeerConnected records a node/client coming online and offers pending work.
func (c *Control) PeerConnected(ctx context.Context, id string, h *proto.Hello) {
	ctx = context.WithoutCancel(ctx)
	c.mu.Lock()
	if h.Role == proto.RoleNode {
		n := c.nodes[id]
		fresh := n == nil
		if fresh {
			n = &nodeState{}
			c.nodes[id] = n
		}
		n.PubKey = h.PubKey
		n.Status.ID = id
		n.Status.Labels = h.Labels
		if h.Node != nil {
			n.Status.Info = *h.Node
		}
		n.Status.Online = true
		n.Status.LastSeen = c.now().UnixMilli()
		c.saveNode(id, n)
		c.mu.Unlock()
		if fresh {
			c.emit(ctx, proto.EvNodeEnrolled, id, "", id, h.Labels)
		}
		c.emit(ctx, proto.EvNodeOnline, id, "", id, nil)
		c.offerPending(ctx)
		return
	}
	c.clients[id] = h
	c.mu.Unlock()
}

// PeerGone marks a node offline. Its leases keep running until they expire,
// so a brief reconnect loses nothing.
func (c *Control) PeerGone(ctx context.Context, id string) {
	// Lifecycle bookkeeping must outlive the connection that triggered it.
	ctx = context.WithoutCancel(ctx)
	c.mu.Lock()
	if n, ok := c.nodes[id]; ok {
		n.Status.Online = false
		n.Status.LastSeen = c.now().UnixMilli()
		// Its workspaces are still leased to it, but they are not serving
		// until it comes back and reports ready, so demote them out of
		// WSClaimed. Clients waiting on a workspace therefore wait for the
		// node that will actually answer them.
		var demoted []string
		for _, ws := range c.workspaces {
			if ws.Node == id && ws.State == proto.WSClaimed {
				ws.State = proto.WSClaiming
				c.saveWS(ws)
				demoted = append(demoted, ws.ID)
			}
		}
		c.mu.Unlock()
		c.emit(ctx, proto.EvNodeOffline, id, "", id, map[string]any{"workspaces": demoted})
		return
	}
	delete(c.clients, id)
	if cancel, ok := c.tails[id]; ok {
		cancel()
		delete(c.tails, id)
	}
	c.mu.Unlock()
}

// HandleFrame dispatches control-plane requests.
func (c *Control) HandleFrame(ctx context.Context, f *proto.Frame) {
	if f.T != proto.KindReq {
		return // control ignores stray events/chunks
	}
	go func() {
		body, err := c.dispatch(ctx, f)
		if err != nil {
			var pe *proto.Error
			if !errors.As(err, &pe) {
				pe = proto.Err(proto.CodeInternal, "%v", err)
			}
			_ = c.send.Send(ctx, proto.NewErrRes(f, pe))
			return
		}
		_ = c.send.Send(ctx, proto.NewRes(f, body))
	}()
}

func (c *Control) principalOf(from string) string {
	c.mu.Lock()
	defer c.mu.Unlock()
	if h, ok := c.clients[from]; ok && h.Principal != "" {
		return h.Principal
	}
	return from
}

func (c *Control) isNode(from string) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	_, ok := c.nodes[from]
	return ok
}

func decode[T any](f *proto.Frame) (*T, error) {
	var v T
	if err := f.Decode(&v); err != nil {
		return nil, proto.Err(proto.CodeBadRequest, "decode %s: %v", f.Op, err)
	}
	return &v, nil
}

func (c *Control) dispatch(ctx context.Context, f *proto.Frame) (any, error) {
	switch f.Op {
	case proto.OpWSCreate:
		req, err := decode[proto.WSCreateReq](f)
		if err != nil {
			return nil, err
		}
		return c.wsCreate(ctx, c.principalOf(f.From), req)
	case proto.OpWSGet, proto.OpWSInfo:
		req, err := decode[proto.WSGetReq](f)
		if err != nil {
			return nil, err
		}
		return c.wsGet(req.ID)
	case proto.OpWSList:
		return c.wsList(), nil
	case proto.OpWSDestroy:
		req, err := decode[proto.WSGetReq](f)
		if err != nil {
			return nil, err
		}
		return struct{}{}, c.wsDestroy(ctx, c.principalOf(f.From), req.ID)
	case proto.OpWSMove:
		req, err := decode[proto.WSMoveReq](f)
		if err != nil {
			return nil, err
		}
		return c.wsMove(ctx, c.principalOf(f.From), req)
	case proto.OpWSSleep:
		req, err := decode[proto.WSSleepReq](f)
		if err != nil {
			return nil, err
		}
		return c.wsSleep(ctx, c.principalOf(f.From), req)
	case proto.OpWSWake:
		req, err := decode[proto.WSGetReq](f)
		if err != nil {
			return nil, err
		}
		return c.wsWake(ctx, c.principalOf(f.From), req.ID, "")
	case proto.OpWSClaim:
		if !c.isNode(f.From) {
			return nil, proto.Err(proto.CodeUnauthorized, "only nodes claim")
		}
		req, err := decode[proto.WSClaimReq](f)
		if err != nil {
			return nil, err
		}
		return c.wsClaim(ctx, f.From, req.ID)
	case proto.OpWSRenew:
		if !c.isNode(f.From) {
			return nil, proto.Err(proto.CodeUnauthorized, "only nodes renew")
		}
		req, err := decode[proto.WSRenewReq](f)
		if err != nil {
			return nil, err
		}
		return struct{}{}, c.wsRenew(ctx, f.From, req)
	case proto.OpWSReady:
		if !c.isNode(f.From) {
			return nil, proto.Err(proto.CodeUnauthorized, "only nodes report ready")
		}
		req, err := decode[proto.WSReadyReq](f)
		if err != nil {
			return nil, err
		}
		return struct{}{}, c.wsReady(ctx, f.From, req)
	case proto.OpWSReleased:
		if !c.isNode(f.From) {
			return nil, proto.Err(proto.CodeUnauthorized, "only nodes release")
		}
		req, err := decode[proto.WSReleasedReq](f)
		if err != nil {
			return nil, err
		}
		return struct{}{}, c.wsReleased(ctx, f.From, req)
	case proto.OpNodeList:
		return c.nodeList(), nil
	case proto.OpEventsTail:
		req, err := decode[proto.EventsTailReq](f)
		if err != nil {
			return nil, err
		}
		return c.eventsTail(ctx, f.From, req)
	case "events.stop":
		c.mu.Lock()
		if cancel, ok := c.tails[f.From]; ok {
			cancel()
			delete(c.tails, f.From)
		}
		c.mu.Unlock()
		return struct{}{}, nil
	case proto.OpEventsPost:
		req, err := decode[proto.EventPost](f)
		if err != nil {
			return nil, err
		}
		return struct{}{}, c.eventsPost(ctx, f.From, req)
	case proto.OpBindingLease:
		if !c.isNode(f.From) {
			return nil, proto.Err(proto.CodeUnauthorized, "only nodes lease bindings")
		}
		req, err := decode[proto.BindingLeaseReq](f)
		if err != nil {
			return nil, err
		}
		return c.bindingLease(ctx, f.From, req.WS)
	case proto.OpGrant:
		req, err := decode[proto.GrantReq](f)
		if err != nil {
			return nil, err
		}
		return c.grant(f.From, c.principalOf(f.From), req.WS)
	case proto.OpTimerList:
		return c.timerList(), nil
	case proto.OpDiag:
		req, err := decode[proto.DiagReq](f)
		if err != nil {
			return nil, err
		}
		d := c.Diag(ctx)
		d.DBIntegrity = c.DBIntegrity(ctx)
		if req.Verify {
			d.Findings = append(d.Findings, c.verifyArtifacts()...)
		}
		return d, nil
	}
	return nil, proto.Err(proto.CodeUnsupported, "unknown control op %q", f.Op)
}

// ---------------------------------------------------------------------------
// workspaces
// ---------------------------------------------------------------------------

func (c *Control) wsCreate(ctx context.Context, principal string, req *proto.WSCreateReq) (*proto.Workspace, error) {
	c.mu.Lock()
	if req.IdempotencyKey != "" {
		if id, ok := c.idem[req.IdempotencyKey]; ok {
			// Copy: everything returned from here is serialized by the
			// caller without the lock, so it must not alias live state.
			cp := *c.workspaces[id]
			c.mu.Unlock()
			return &cp, nil
		}
	}
	for _, b := range req.Spec.Bindings {
		if _, ok := c.bindings[b]; !ok {
			c.mu.Unlock()
			return nil, proto.Err(proto.CodeNotFound, "binding %q is not defined", b)
		}
	}
	if req.Spec.Principal == "" {
		req.Spec.Principal = principal
	}
	now := c.now().UnixMilli()
	ws := &proto.Workspace{ID: ids.New("ws"), Spec: req.Spec, State: proto.WSPending, CreatedAt: now, UpdatedAt: now}
	c.workspaces[ws.ID] = ws
	if req.IdempotencyKey != "" {
		c.idem[req.IdempotencyKey] = ws.ID
		_, _ = c.db.Exec(`INSERT OR REPLACE INTO idem(key, ws) VALUES(?,?)`, req.IdempotencyKey, ws.ID)
	}
	c.saveWS(ws)
	// The workspace is in the shared map now, so a node can claim and mutate
	// it the moment the lock is released. Copy what the event needs first.
	id, spec := ws.ID, ws.Spec
	c.mu.Unlock()
	metrics.WSCreated.Inc()
	c.emit(ctx, proto.EvWSCreated, id, principal, "", spec)
	c.offerPending(ctx)
	return c.snapshotWS(id), nil
}

func (c *Control) snapshotWS(id string) *proto.Workspace {
	c.mu.Lock()
	defer c.mu.Unlock()
	ws := c.workspaces[id]
	if ws == nil {
		return nil
	}
	cp := *ws
	return &cp
}

func (c *Control) wsGet(id string) (*proto.Workspace, error) {
	ws := c.snapshotWS(id)
	if ws == nil {
		return nil, proto.Err(proto.CodeNotFound, "workspace %s", id)
	}
	return ws, nil
}

func (c *Control) wsList() *proto.WSListRes {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := &proto.WSListRes{}
	for _, ws := range c.workspaces {
		if ws.State != proto.WSDestroyed {
			out.Workspaces = append(out.Workspaces, *ws)
		}
	}
	sort.Slice(out.Workspaces, func(i, j int) bool { return out.Workspaces[i].ID < out.Workspaces[j].ID })
	return out
}

func (c *Control) wsDestroy(ctx context.Context, principal, id string) error {
	c.mu.Lock()
	ws := c.workspaces[id]
	if ws == nil {
		c.mu.Unlock()
		return proto.Err(proto.CodeNotFound, "workspace %s", id)
	}
	node, gen := ws.Node, ws.Generation
	ws.State = proto.WSDestroyed
	ws.Node = ""
	c.saveWS(ws)
	for _, t := range c.timers {
		if t.WS == id && !t.Fired {
			t.Fired = true
			c.saveTimer(t)
		}
	}
	c.mu.Unlock()
	if node != "" && c.send != nil && c.send.Online(node) {
		rctx, cancel := context.WithTimeout(ctx, 30*time.Second)
		_ = c.send.Request(rctx, node, proto.OpWSRelease, proto.WSReleaseReq{WS: id, Gen: gen, Snapshot: false, Reason: "destroy"}, nil)
		cancel()
	}
	metrics.WSDestroyed.Inc()
	c.emit(ctx, proto.EvWSDestroyed, id, principal, node, nil)
	return nil
}

// release asks the current node to give a workspace up (optionally with a
// snapshot) and moves it to the given state. It is used by move, sleep and
// destroy. Returns the snapshot artifact id if one was taken.
func (c *Control) release(ctx context.Context, id string, snapshot bool, reason string) (string, error) {
	c.mu.Lock()
	ws := c.workspaces[id]
	if ws == nil {
		c.mu.Unlock()
		return "", proto.Err(proto.CodeNotFound, "workspace %s", id)
	}
	if !held(ws.State) {
		last := ws.LastSnapshot
		c.mu.Unlock()
		return last, nil
	}
	node, gen := ws.Node, ws.Generation
	ws.State = proto.WSReleased
	c.saveWS(ws)
	c.mu.Unlock()
	var res proto.WSReleasedReq
	rctx, cancel := context.WithTimeout(ctx, 10*time.Minute)
	defer cancel()
	if err := c.send.Request(rctx, node, proto.OpWSRelease, proto.WSReleaseReq{WS: id, Gen: gen, Snapshot: snapshot, Reason: reason}, &res); err != nil {
		// Node unreachable: treat like a lease expiry; the last snapshot is what we have.
		c.mu.Lock()
		last := ws.LastSnapshot
		c.mu.Unlock()
		c.logger.Warn("release request failed", "ws", id, "node", node, "err", err)
		return last, nil
	}
	c.mu.Lock()
	if res.Snapshot != "" {
		ws.LastSnapshot = res.Snapshot
	}
	last := ws.LastSnapshot
	c.saveWS(ws)
	c.mu.Unlock()
	c.emit(ctx, proto.EvWSReleased, id, "", node, map[string]any{"reason": reason, "snapshot": res.Snapshot})
	return last, nil
}

func (c *Control) wsMove(ctx context.Context, principal string, req *proto.WSMoveReq) (*proto.Workspace, error) {
	if _, err := c.wsGet(req.ID); err != nil {
		return nil, err
	}
	snap, err := c.release(ctx, req.ID, true, "move")
	if err != nil {
		return nil, err
	}
	c.mu.Lock()
	ws := c.workspaces[req.ID]
	if req.Requires != nil {
		ws.Spec.Requires = *req.Requires
	}
	if req.Placement != nil {
		ws.Spec.Placement = *req.Placement
	}
	ws.Spec.RestoreFrom = snap
	ws.State = proto.WSPending
	ws.Node = ""
	ws.LeaseUntil = 0
	c.saveWS(ws)
	c.mu.Unlock()
	metrics.WSMoved.Inc()
	c.emit(ctx, proto.EvWSMoved, req.ID, principal, "", map[string]any{"restore_from": snap})
	c.offerPending(ctx)
	return c.snapshotWS(req.ID), nil
}

func (c *Control) wsSleep(ctx context.Context, principal string, req *proto.WSSleepReq) (*proto.Timer, error) {
	if _, err := c.wsGet(req.ID); err != nil {
		return nil, err
	}
	if req.AfterSec == 0 && req.AtMillis == 0 && req.OnEvent == "" {
		return nil, proto.Err(proto.CodeBadRequest, "sleep needs after_sec, at or on")
	}
	t := &proto.Timer{ID: ids.New("t"), WS: req.ID, Action: "resume", CreatedAt: c.now().UnixMilli(), OnEvent: req.OnEvent}
	if req.AfterSec > 0 {
		t.At = c.now().Add(time.Duration(req.AfterSec) * time.Second).UnixMilli()
	} else if req.AtMillis > 0 {
		t.At = req.AtMillis
	}
	c.mu.Lock()
	c.timers[t.ID] = t
	c.saveTimer(t)
	tcp := *t
	c.mu.Unlock()
	c.emit(ctx, proto.EvTimerSet, req.ID, principal, "", tcp)
	snap, err := c.release(ctx, req.ID, true, "sleep")
	if err != nil {
		return nil, err
	}
	c.mu.Lock()
	ws := c.workspaces[req.ID]
	ws.Spec.RestoreFrom = snap
	ws.State = proto.WSPaused
	ws.Node = ""
	ws.LeaseUntil = 0
	c.saveWS(ws)
	c.mu.Unlock()
	c.emit(ctx, proto.EvWSPaused, req.ID, principal, "", map[string]any{"timer": tcp.ID, "snapshot": snap})
	return &tcp, nil
}

func (c *Control) wsWake(ctx context.Context, principal, id, timerID string) (*proto.Workspace, error) {
	c.mu.Lock()
	ws := c.workspaces[id]
	if ws == nil {
		c.mu.Unlock()
		return nil, proto.Err(proto.CodeNotFound, "workspace %s", id)
	}
	if ws.State == proto.WSDestroyed {
		c.mu.Unlock()
		return nil, proto.Err(proto.CodeConflict, "workspace destroyed")
	}
	if ws.State != proto.WSPaused {
		cp := *ws
		c.mu.Unlock()
		return &cp, nil
	}
	ws.State = proto.WSPending
	c.saveWS(ws)
	c.mu.Unlock()
	c.emit(ctx, proto.EvWSResumed, id, principal, "", map[string]any{"timer": timerID})
	c.offerPending(ctx)
	return c.snapshotWS(id), nil
}

func (c *Control) wsClaim(ctx context.Context, node, id string) (*proto.WSClaimRes, error) {
	c.mu.Lock()
	ws := c.workspaces[id]
	if ws == nil {
		c.mu.Unlock()
		return nil, proto.Err(proto.CodeNotFound, "workspace %s", id)
	}
	if held(ws.State) && ws.Node == node {
		// Re-adoption: the same node reconnecting still holds this workspace.
		// Keep the generation so the client's outstanding grants stay valid,
		// but go back through claiming so waiters do not race the restore.
		ws.State = proto.WSClaiming
		ws.LeaseUntil = c.now().Add(time.Duration(c.opts.LeaseSec) * time.Second).UnixMilli()
		c.saveWS(ws)
		cp := *ws
		c.mu.Unlock()
		return &proto.WSClaimRes{Workspace: cp, LeaseSec: c.opts.LeaseSec}, nil
	}
	if ws.State != proto.WSPending {
		// Read the state before releasing the lock: formatting the message
		// after the unlock is a read of shared memory.
		state := ws.State
		c.mu.Unlock()
		return nil, proto.Err(proto.CodeConflict, "workspace %s is %s", id, state)
	}
	n := c.nodes[node]
	if n == nil || !c.eligibleLocked(ws, n) {
		c.mu.Unlock()
		metrics.WSClaimDenied.Inc()
		return nil, proto.Err(proto.CodeDenied, "node %s is not eligible for %s", node, id)
	}
	// The compare-and-swap: state was pending under the lock; now it's ours.
	// It becomes WSClaimed only once the node reports ws.ready, so a client
	// never talks to a node that is still restoring the filesystem.
	ws.State = proto.WSClaiming
	ws.Node = node
	ws.Generation++
	ws.LeaseUntil = c.now().Add(time.Duration(c.opts.LeaseSec) * time.Second).UnixMilli()
	c.saveWS(ws)
	cp := *ws
	c.mu.Unlock()
	metrics.WSClaims.Inc()
	c.emit(ctx, proto.EvWSClaiming, id, "", node, map[string]any{"gen": cp.Generation, "restore_from": cp.Spec.RestoreFrom})
	return &proto.WSClaimRes{Workspace: cp, LeaseSec: c.opts.LeaseSec}, nil
}

// wsReady is the node reporting that a claimed workspace is materialized.
func (c *Control) wsReady(ctx context.Context, node string, req *proto.WSReadyReq) error {
	c.mu.Lock()
	ws := c.workspaces[req.ID]
	if ws == nil {
		c.mu.Unlock()
		return proto.Err(proto.CodeNotFound, "workspace %s", req.ID)
	}
	if ws.Node != node || ws.Generation != req.Gen {
		c.mu.Unlock()
		return proto.Err(proto.CodeConflict, "stale ready for %s", req.ID)
	}
	if ws.State == proto.WSClaimed {
		c.mu.Unlock()
		return nil
	}
	ws.State = proto.WSClaimed
	c.saveWS(ws)
	gen := ws.Generation
	restore := ws.Spec.RestoreFrom
	c.mu.Unlock()
	c.emit(ctx, proto.EvWSClaimed, req.ID, "", node, map[string]any{"gen": gen, "restore_from": restore})
	return nil
}

func (c *Control) wsRenew(ctx context.Context, node string, req *proto.WSRenewReq) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	until := c.now().Add(time.Duration(c.opts.LeaseSec) * time.Second).UnixMilli()
	for _, id := range req.IDs {
		ws := c.workspaces[id]
		if ws == nil || ws.Node != node || !held(ws.State) {
			continue
		}
		if g, ok := req.Gen[id]; ok && g != ws.Generation {
			continue // stale generation must not extend a lease
		}
		ws.LeaseUntil = until
		c.saveWS(ws)
	}
	if n := c.nodes[node]; n != nil {
		n.Status.LastSeen = c.now().UnixMilli()
	}
	return nil
}

func (c *Control) wsReleased(ctx context.Context, node string, req *proto.WSReleasedReq) error {
	c.mu.Lock()
	ws := c.workspaces[req.ID]
	if ws == nil {
		c.mu.Unlock()
		return proto.Err(proto.CodeNotFound, "workspace %s", req.ID)
	}
	if ws.Node != node || ws.Generation != req.Gen {
		c.mu.Unlock()
		return proto.Err(proto.CodeConflict, "stale release for %s", req.ID)
	}
	if req.Snapshot != "" {
		ws.LastSnapshot = req.Snapshot
		ws.Spec.RestoreFrom = req.Snapshot
	}
	if held(ws.State) {
		ws.State = proto.WSPending
	}
	ws.Node = ""
	ws.LeaseUntil = 0
	c.saveWS(ws)
	state := ws.State
	c.mu.Unlock()
	c.emit(ctx, proto.EvWSReleased, req.ID, "", node, map[string]any{"reason": req.Reason, "snapshot": req.Snapshot})
	if state == proto.WSPending {
		c.offerPending(ctx)
	}
	return nil
}

// eligibleLocked applies Requires and Placement against a node.
func (c *Control) eligibleLocked(ws *proto.Workspace, n *nodeState) bool {
	if !n.Status.Online {
		return false
	}
	r := ws.Spec.Requires
	if r.Backend != "" && !contains(n.Status.Info.Backends, r.Backend) {
		return false
	}
	if r.OS != "" && n.Status.Info.OS != r.OS {
		return false
	}
	if r.Arch != "" && n.Status.Info.Arch != r.Arch {
		return false
	}
	if r.CPU > 0 && n.Status.Info.CPU < r.CPU {
		return false
	}
	if r.MemMiB > 0 && n.Status.Info.MemMiB > 0 && n.Status.Info.MemMiB < r.MemMiB {
		return false
	}
	for _, cap := range r.Caps {
		if !contains(n.Status.Info.Caps, cap) {
			return false
		}
	}
	p := ws.Spec.Placement
	if p.Node != "" && p.Node != n.Status.ID {
		return false
	}
	for k, v := range p.Allow {
		if n.Status.Labels[k] != v {
			return false
		}
	}
	return true
}

// held reports whether a node currently owns the workspace (materializing
// or serving). Both states carry a lease.
func held(state string) bool { return state == proto.WSClaimed || state == proto.WSClaiming }

func contains(list []string, s string) bool {
	for _, x := range list {
		if x == s {
			return true
		}
	}
	return false
}

// offerPending sends ws.offer to every eligible online node for every
// pending workspace. Nodes race to claim; the CAS in wsClaim picks one.
func (c *Control) offerPending(ctx context.Context) {
	if c.send == nil {
		return
	}
	type offer struct{ node, ws string }
	var offers []offer
	c.mu.Lock()
	for _, ws := range c.workspaces {
		if ws.State != proto.WSPending {
			continue
		}
		for id, n := range c.nodes {
			if c.eligibleLocked(ws, n) && c.send.Online(id) {
				offers = append(offers, offer{id, ws.ID})
			}
		}
	}
	c.mu.Unlock()
	for _, o := range offers {
		_ = c.send.Send(ctx, proto.NewEvent(o.node, proto.EvWSOffer, proto.WSGetReq{ID: o.ws}))
	}
}

// ---------------------------------------------------------------------------
// nodes, events, bindings, grants, timers
// ---------------------------------------------------------------------------

func (c *Control) nodeList() *proto.NodeListRes {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := &proto.NodeListRes{}
	for id, n := range c.nodes {
		st := n.Status
		st.Workspaces = nil
		for _, ws := range c.workspaces {
			if ws.Node == id && held(ws.State) {
				st.Workspaces = append(st.Workspaces, ws.ID)
			}
		}
		sort.Strings(st.Workspaces)
		out.Nodes = append(out.Nodes, st)
	}
	sort.Slice(out.Nodes, func(i, j int) bool { return out.Nodes[i].ID < out.Nodes[j].ID })
	return out
}

// eventsTail streams events to the requester as ev frames (Op "log") until
// events.stop or disconnect. Historical events are delivered first.
func (c *Control) eventsTail(ctx context.Context, from string, req *proto.EventsTailReq) (any, error) {
	if !req.Follow {
		evs, err := c.log.Read(ctx, req.From, req.WS, 1000)
		if err != nil {
			return nil, err
		}
		return proto.EventPost{Events: evs}, nil
	}
	c.mu.Lock()
	if cancel, ok := c.tails[from]; ok {
		cancel()
	}
	tctx, cancel := context.WithCancel(context.Background())
	c.tails[from] = cancel
	c.mu.Unlock()
	go func() {
		defer cancel()
		sub := c.log.Subscribe(req.From, req.WS)
		defer sub.Close()
		for {
			evs, err := sub.Next(tctx)
			if err != nil {
				return
			}
			if err := c.send.Send(tctx, proto.NewEvent(from, "log", proto.EventPost{Events: evs})); err != nil {
				return
			}
		}
	}()
	return struct{}{}, nil
}

// eventsPost appends node-originated events (node id enforced) and fires
// any timer waiting on the event type.
func (c *Control) eventsPost(ctx context.Context, from string, req *proto.EventPost) error {
	isNode := c.isNode(from)
	for i := range req.Events {
		e := req.Events[i]
		e.Seq = 0
		if isNode {
			e.Node = from
		} else if e.Principal == "" {
			e.Principal = c.principalOf(from)
		}
		if err := c.log.Append(ctx, &e); err != nil {
			return err
		}
		c.fireEventTimers(ctx, e.Type)
	}
	return nil
}

// PostEvents appends externally originated events (webhooks, integrations)
// to the canonical log and fires any timer waiting on their type. It must
// not be given a request-scoped context: the caller returns before the
// timers and offers finish.
func (c *Control) PostEvents(ctx context.Context, principal string, events []proto.Event) error {
	for i := range events {
		e := events[i]
		e.Seq = 0
		if e.Principal == "" {
			e.Principal = principal
		}
		if err := c.log.Append(ctx, &e); err != nil {
			return err
		}
		c.fireEventTimers(ctx, e.Type)
	}
	return nil
}

func (c *Control) fireEventTimers(ctx context.Context, typ string) {
	c.mu.Lock()
	var fire []*proto.Timer
	for _, t := range c.timers {
		if !t.Fired && t.OnEvent != "" && t.OnEvent == typ {
			fire = append(fire, t)
		}
	}
	c.mu.Unlock()
	for _, t := range fire {
		c.fireTimer(ctx, t)
	}
}

func (c *Control) fireTimer(ctx context.Context, t *proto.Timer) {
	c.mu.Lock()
	if t.Fired {
		c.mu.Unlock()
		return
	}
	t.Fired = true
	c.saveTimer(t)
	cp := *t
	c.mu.Unlock()
	metrics.TimersFired.Inc()
	c.emit(ctx, proto.EvTimerFired, cp.WS, "", "", cp)
	_, _ = c.wsWake(ctx, "", cp.WS, cp.ID)
}

func (c *Control) bindingLease(ctx context.Context, node, wsID string) (*proto.BindingLeaseRes, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	ws := c.workspaces[wsID]
	if ws == nil || ws.Node != node || !held(ws.State) {
		return nil, proto.Err(proto.CodeDenied, "workspace %s is not claimed by %s", wsID, node)
	}
	out := &proto.BindingLeaseRes{}
	for _, id := range ws.Spec.Bindings {
		b, ok := c.bindings[id]
		if !ok {
			continue
		}
		if len(b.Principals) > 0 && !contains(b.Principals, ws.Spec.Principal) {
			continue
		}
		ttl := b.TTLSec
		if ttl == 0 {
			ttl = 600
		}
		out.Leases = append(out.Leases, proto.BindingLease{
			ID: b.ID, Secret: b.Secret, Destinations: b.Destinations, Principals: b.Principals,
			Placeholder: b.Placeholder, ExpiresAt: c.now().Add(time.Duration(ttl) * time.Second).UnixMilli(),
		})
	}
	return out, nil
}

func (c *Control) grant(client, principal, wsID string) (*proto.Grant, error) {
	c.mu.Lock()
	ws := c.workspaces[wsID]
	if ws == nil {
		c.mu.Unlock()
		return nil, proto.Err(proto.CodeNotFound, "workspace %s", wsID)
	}
	if ws.State != proto.WSClaimed {
		st := ws.State
		c.mu.Unlock()
		return nil, proto.Err(proto.CodeConflict, "workspace %s is %s, not claimed", wsID, st)
	}
	claims := proto.GrantClaims{Client: client, WS: wsID, Node: ws.Node, Principal: principal, ExpiresAt: c.now().Add(time.Hour).UnixMilli(), Gen: ws.Generation}
	node := ws.Node
	c.mu.Unlock()
	sig := ed25519.Sign(c.key, proto.MustMarshal(claims))
	return &proto.Grant{Claims: claims, Signature: sig, Node: node}, nil
}

// VerifyGrant checks a grant against the control plane's public key. Nodes
// call this with the key from HelloOK.
func VerifyGrant(pub ed25519.PublicKey, g *proto.Grant, now time.Time) error {
	if g == nil {
		return proto.Err(proto.CodeUnauthorized, "missing grant")
	}
	if !ed25519.Verify(pub, proto.MustMarshal(g.Claims), g.Signature) {
		return proto.Err(proto.CodeUnauthorized, "bad grant signature")
	}
	if now.UnixMilli() > g.Claims.ExpiresAt {
		return proto.Err(proto.CodeUnauthorized, "grant expired")
	}
	return nil
}

func (c *Control) timerList() *proto.TimerListRes {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := &proto.TimerListRes{}
	for _, t := range c.timers {
		out.Timers = append(out.Timers, *t)
	}
	sort.Slice(out.Timers, func(i, j int) bool { return out.Timers[i].ID < out.Timers[j].ID })
	return out
}

// ---------------------------------------------------------------------------
// background loop: leases, timers, re-offers
// ---------------------------------------------------------------------------

func (c *Control) loop() {
	defer c.wg.Done()
	tick := time.NewTicker(time.Second)
	defer tick.Stop()
	for {
		select {
		case <-c.stop:
			return
		case <-tick.C:
			c.Tick(context.Background())
		}
	}
}

// Tick runs one iteration of the background work (exported for tests with
// an injected clock).
func (c *Control) Tick(ctx context.Context) {
	now := c.now().UnixMilli()
	var expired []*proto.Workspace
	var fire []*proto.Timer
	c.mu.Lock()
	for _, ws := range c.workspaces {
		if held(ws.State) && ws.LeaseUntil > 0 && ws.LeaseUntil < now {
			ws.State = proto.WSPending
			ws.Spec.RestoreFrom = ws.LastSnapshot
			lost := ws.Node
			ws.Node = ""
			ws.LeaseUntil = 0
			c.saveWS(ws)
			cp := *ws
			cp.Node = lost
			expired = append(expired, &cp)
		}
	}
	for _, t := range c.timers {
		if !t.Fired && t.At > 0 && t.At <= now {
			fire = append(fire, t)
		}
	}
	c.mu.Unlock()
	// lint:locks-ok expired holds per-iteration copies, not shared workspaces
	for _, ws := range expired {
		metrics.WSLeaseExpired.Inc()
		c.emit(ctx, proto.EvWSLeaseExpired, ws.ID, "", ws.Node, map[string]any{"restore_from": ws.Spec.RestoreFrom})
	}
	for _, t := range fire {
		c.fireTimer(ctx, t)
	}
	c.offerPending(ctx)
}

// Bindings returns the configured binding ids (for the CLI).
func (c *Control) Bindings() []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([]string, 0, len(c.bindings))
	for id := range c.bindings {
		out = append(out, id)
	}
	sort.Strings(out)
	return out
}

// String for logs.
func (c *Control) String() string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return fmt.Sprintf("control{workspaces=%d nodes=%d timers=%d}", len(c.workspaces), len(c.nodes), len(c.timers))
}
