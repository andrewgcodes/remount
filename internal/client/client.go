// Package client is the Go SDK: one connection to a relay, typed calls to
// the control plane and to nodes, and Session, a reconnecting cursor over a
// remote session's log.
//
// A Client survives connection loss: every call that fails with
// transport.ErrClosed is retried on a fresh connection with the same
// idempotency key, and an attached Session resumes from the last seq it
// delivered, so the caller sees one unbroken stream.
package client

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sync"
	"sync/atomic"
	"time"

	"remount.dev/remount/internal/ids"
	"remount.dev/remount/internal/proto"
	"remount.dev/remount/internal/transport"
)

// Options configure a client.
type Options struct {
	Dialer    transport.Dialer
	Token     string
	Principal string
	// Retries bounds reconnect attempts per call (default 5).
	Retries int
}

// Client is a connection to a Remount relay.
type Client struct {
	opts Options

	dialMu   sync.Mutex // serializes reconnect dials
	mu       sync.Mutex
	peer     *transport.Peer
	id       string
	grants   map[string]*proto.Grant
	sessions map[string]*Session
	// orphans holds chunks for sessions whose open response has not arrived
	// yet (the node starts streaming before it replies). Bounded.
	orphans map[string][]*proto.Frame
	events  chan proto.Event
	closed  bool
	gen     uint64 // connection generation
}

// New creates a client; it connects lazily.
func New(opts Options) *Client {
	if opts.Retries == 0 {
		opts.Retries = 5
	}
	return &Client{opts: opts, grants: map[string]*proto.Grant{}, sessions: map[string]*Session{}, orphans: map[string][]*proto.Frame{}}
}

// ID returns the peer id assigned by the relay (after first connect).
func (c *Client) ID() string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.id
}

// Close disconnects.
func (c *Client) Close() error {
	c.mu.Lock()
	c.closed = true
	p := c.peer
	c.peer = nil
	c.mu.Unlock()
	if p != nil {
		p.Close()
	}
	return nil
}

// Connect ensures a live connection and returns it.
func (c *Client) Connect(ctx context.Context) (*transport.Peer, error) {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return nil, transport.ErrClosed
	}
	if p := c.peer; p != nil {
		select {
		case <-p.Done():
		default:
			c.mu.Unlock()
			return p, nil
		}
	}
	c.mu.Unlock()
	c.dialMu.Lock()
	defer c.dialMu.Unlock()
	// Re-check: another goroutine may have connected while we waited.
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return nil, transport.ErrClosed
	}
	if p := c.peer; p != nil {
		select {
		case <-p.Done():
		default:
			c.mu.Unlock()
			return p, nil
		}
	}
	c.mu.Unlock()
	conn, err := c.opts.Dialer.Dial(ctx)
	if err != nil {
		return nil, err
	}
	p := transport.NewPeer(conn, transport.HandlerFunc(c.handle))
	hctx, cancel := context.WithTimeout(ctx, 15*time.Second)
	ok, err := transport.Hello(hctx, p, proto.Hello{Peer: c.ID(), Role: proto.RoleClient, Token: c.opts.Token, Caps: []string{"v1"}, Principal: c.opts.Principal})
	cancel()
	if err != nil {
		p.Close()
		return nil, err
	}
	c.mu.Lock()
	if c.peer != nil {
		c.peer.Close()
	}
	c.peer = p
	c.id = ok.Peer
	c.gen++
	c.grants = map[string]*proto.Grant{} // nodes cache grants per connection
	sessions := make([]*Session, 0, len(c.sessions))
	for _, s := range c.sessions {
		sessions = append(sessions, s)
	}
	c.mu.Unlock()
	// Re-attach live sessions on the new connection, and supervise the
	// connection so a drop while sessions are streaming triggers a
	// reconnect even when the caller is only ranging over chunks.
	for _, s := range sessions {
		go s.reattach(context.Background())
	}
	go c.supervise(p)
	return p, nil
}

// supervise reconnects after p dies, as long as the client is open and has
// live sessions to resume. Each successful Connect installs a fresh
// supervisor, so this forms a chain of exactly one watcher per connection.
func (c *Client) supervise(p *transport.Peer) {
	<-p.Done()
	backoff := 100 * time.Millisecond
	for {
		c.mu.Lock()
		closed, current, n := c.closed, c.peer == p, len(c.sessions)
		c.mu.Unlock()
		if closed || !current || n == 0 {
			return
		}
		if _, err := c.Connect(context.Background()); err == nil {
			return
		}
		time.Sleep(backoff)
		if backoff < 5*time.Second {
			backoff *= 2
		}
	}
}

func (c *Client) handle(ctx context.Context, p *transport.Peer, f *proto.Frame) {
	switch f.T {
	case proto.KindChunk:
		c.mu.Lock()
		s := c.sessions[f.S]
		if s == nil {
			if len(c.orphans) < 256 && len(c.orphans[f.S]) < 4096 {
				c.orphans[f.S] = append(c.orphans[f.S], f)
			}
		}
		c.mu.Unlock()
		if s != nil {
			s.deliver(f)
		}
	case proto.KindEvent:
		if f.Op == "log" {
			var post proto.EventPost
			if err := f.Decode(&post); err == nil {
				c.mu.Lock()
				ch := c.events
				c.mu.Unlock()
				if ch != nil {
					for _, e := range post.Events {
						select {
						case ch <- e:
						case <-ctx.Done():
						}
					}
				}
			}
		}
	}
}

// call performs a request with reconnect-and-retry on connection loss.
func (c *Client) call(ctx context.Context, to, op string, body, out any) error {
	var lastErr error
	for attempt := 0; attempt < c.opts.Retries; attempt++ {
		p, err := c.Connect(ctx)
		if err != nil {
			lastErr = err
		} else {
			err = p.Call(ctx, to, op, body, out)
			if err == nil || !errors.Is(err, transport.ErrClosed) {
				return err
			}
			lastErr = err
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(time.Duration(attempt+1) * 200 * time.Millisecond):
		}
	}
	return lastErr
}

// ---------------------------------------------------------------------------
// control plane
// ---------------------------------------------------------------------------

// CreateWorkspace submits a workspace spec.
func (c *Client) CreateWorkspace(ctx context.Context, spec proto.WorkspaceSpec) (*proto.Workspace, error) {
	var ws proto.Workspace
	err := c.call(ctx, proto.PeerControl, proto.OpWSCreate, proto.WSCreateReq{Spec: spec, IdempotencyKey: ids.New("idem")}, &ws)
	return &ws, err
}

// GetWorkspace fetches one workspace.
func (c *Client) GetWorkspace(ctx context.Context, id string) (*proto.Workspace, error) {
	var ws proto.Workspace
	err := c.call(ctx, proto.PeerControl, proto.OpWSGet, proto.WSGetReq{ID: id}, &ws)
	return &ws, err
}

// WaitClaimed blocks until the workspace is claimed by a node.
func (c *Client) WaitClaimed(ctx context.Context, id string) (*proto.Workspace, error) {
	for {
		ws, err := c.GetWorkspace(ctx, id)
		if err != nil {
			return nil, err
		}
		switch ws.State {
		case proto.WSClaimed:
			return ws, nil
		case proto.WSDestroyed:
			return nil, proto.Err(proto.CodeConflict, "workspace destroyed")
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(150 * time.Millisecond):
		}
	}
}

// ListWorkspaces lists all workspaces.
func (c *Client) ListWorkspaces(ctx context.Context) ([]proto.Workspace, error) {
	var res proto.WSListRes
	err := c.call(ctx, proto.PeerControl, proto.OpWSList, nil, &res)
	return res.Workspaces, err
}

// DestroyWorkspace destroys a workspace.
func (c *Client) DestroyWorkspace(ctx context.Context, id string) error {
	return c.call(ctx, proto.PeerControl, proto.OpWSDestroy, proto.WSGetReq{ID: id}, nil)
}

// MoveWorkspace snapshots and re-queues a workspace with new requirements.
func (c *Client) MoveWorkspace(ctx context.Context, id string, req *proto.Requires, placement *proto.Placement) (*proto.Workspace, error) {
	var ws proto.Workspace
	err := c.call(ctx, proto.PeerControl, proto.OpWSMove, proto.WSMoveReq{ID: id, Requires: req, Placement: placement, IdempotencyKey: ids.New("idem")}, &ws)
	c.forgetGrant(id)
	return &ws, err
}

// SleepWorkspace pauses a workspace until a timer or event.
func (c *Client) SleepWorkspace(ctx context.Context, req proto.WSSleepReq) (*proto.Timer, error) {
	var t proto.Timer
	if req.IdempotencyKey == "" {
		req.IdempotencyKey = ids.New("idem")
	}
	err := c.call(ctx, proto.PeerControl, proto.OpWSSleep, req, &t)
	c.forgetGrant(req.ID)
	return &t, err
}

// WakeWorkspace resumes a paused workspace.
func (c *Client) WakeWorkspace(ctx context.Context, id string) (*proto.Workspace, error) {
	var ws proto.Workspace
	err := c.call(ctx, proto.PeerControl, proto.OpWSWake, proto.WSGetReq{ID: id}, &ws)
	return &ws, err
}

// ListNodes lists nodes.
func (c *Client) ListNodes(ctx context.Context) ([]proto.NodeStatus, error) {
	var res proto.NodeListRes
	err := c.call(ctx, proto.PeerControl, proto.OpNodeList, nil, &res)
	return res.Nodes, err
}

// ListTimers lists timers.
func (c *Client) ListTimers(ctx context.Context) ([]proto.Timer, error) {
	var res proto.TimerListRes
	err := c.call(ctx, proto.PeerControl, proto.OpTimerList, nil, &res)
	return res.Timers, err
}

// PostEvent appends an event to the canonical log (webhook-style wake).
func (c *Client) PostEvent(ctx context.Context, e proto.Event) error {
	return c.call(ctx, proto.PeerControl, proto.OpEventsPost, proto.EventPost{Events: []proto.Event{e}}, nil)
}

// ReadEvents returns historical events.
func (c *Client) ReadEvents(ctx context.Context, from uint64, ws string) ([]proto.Event, error) {
	var res proto.EventPost
	err := c.call(ctx, proto.PeerControl, proto.OpEventsTail, proto.EventsTailReq{From: from, WS: ws}, &res)
	return res.Events, err
}

// TailEvents streams events on the returned channel until ctx ends.
func (c *Client) TailEvents(ctx context.Context, from uint64, ws string) (<-chan proto.Event, error) {
	ch := make(chan proto.Event, 256)
	c.mu.Lock()
	c.events = ch
	c.mu.Unlock()
	if err := c.call(ctx, proto.PeerControl, proto.OpEventsTail, proto.EventsTailReq{From: from, WS: ws, Follow: true}, nil); err != nil {
		return nil, err
	}
	go func() {
		<-ctx.Done()
		c.mu.Lock()
		c.events = nil
		c.mu.Unlock()
		_ = c.call(context.Background(), proto.PeerControl, "events.stop", nil, nil)
		close(ch)
	}()
	return ch, nil
}

// grant obtains (and caches per connection) a grant for a workspace.
func (c *Client) grant(ctx context.Context, wsID string) (*proto.Grant, error) {
	c.mu.Lock()
	g := c.grants[wsID]
	c.mu.Unlock()
	if g != nil && time.Now().UnixMilli() < g.Claims.ExpiresAt-60_000 {
		return g, nil
	}
	var ng proto.Grant
	if err := c.call(ctx, proto.PeerControl, proto.OpGrant, proto.GrantReq{WS: wsID}, &ng); err != nil {
		return nil, err
	}
	c.mu.Lock()
	c.grants[wsID] = &ng
	c.mu.Unlock()
	return &ng, nil
}

func (c *Client) forgetGrant(wsID string) {
	c.mu.Lock()
	delete(c.grants, wsID)
	c.mu.Unlock()
}

// nodeCall performs a request against the node holding wsID, attaching a
// grant. A stale grant (workspace moved) is refreshed once.
func (c *Client) nodeCall(ctx context.Context, wsID, op string, body func(g *proto.Grant) any, out any) error {
	for attempt := 0; attempt < 2; attempt++ {
		g, err := c.grant(ctx, wsID)
		if err != nil {
			return err
		}
		err = c.call(ctx, g.Node, op, body(g), out)
		var pe *proto.Error
		if errors.As(err, &pe) && (pe.Code == proto.CodeConflict || pe.Code == proto.CodeUnreachable || pe.Code == proto.CodeUnauthorized) && attempt == 0 {
			c.forgetGrant(wsID)
			continue
		}
		return err
	}
	return nil
}

// ---------------------------------------------------------------------------
// filesystem
// ---------------------------------------------------------------------------

// ReadFile reads a whole file (bounded by the node's read limit).
func (c *Client) ReadFile(ctx context.Context, wsID, path string) ([]byte, error) {
	var res proto.FSReadRes
	err := c.nodeCall(ctx, wsID, proto.OpFSRead, func(g *proto.Grant) any { return proto.FSReadReq{WS: wsID, Path: path, Grant: g} }, &res)
	return res.Data, err
}

// WriteFile writes a file, creating parents.
func (c *Client) WriteFile(ctx context.Context, wsID, path string, data []byte, mode uint32) error {
	return c.nodeCall(ctx, wsID, proto.OpFSWrite, func(g *proto.Grant) any {
		return proto.FSWriteReq{WS: wsID, Path: path, Data: data, Mode: mode, MkdirP: true, IdempotencyKey: ids.New("idem"), Grant: g}
	}, nil)
}

// ListDir lists a directory.
func (c *Client) ListDir(ctx context.Context, wsID, path string) ([]proto.FSEntry, error) {
	var res proto.FSListRes
	err := c.nodeCall(ctx, wsID, proto.OpFSList, func(g *proto.Grant) any { return proto.FSListReq{WS: wsID, Path: path, Grant: g} }, &res)
	return res.Entries, err
}

// Stat stats a path.
func (c *Client) Stat(ctx context.Context, wsID, path string) (*proto.FSEntry, error) {
	var res proto.FSStatRes
	err := c.nodeCall(ctx, wsID, proto.OpFSStat, func(g *proto.Grant) any { return proto.FSStatReq{WS: wsID, Path: path, Grant: g} }, &res)
	return &res.Entry, err
}

// Mkdir creates a directory.
func (c *Client) Mkdir(ctx context.Context, wsID, path string) error {
	return c.nodeCall(ctx, wsID, proto.OpFSMkdir, func(g *proto.Grant) any { return proto.FSMkdirReq{WS: wsID, Path: path, Grant: g} }, nil)
}

// Remove deletes a path.
func (c *Client) Remove(ctx context.Context, wsID, path string, recursive bool) error {
	return c.nodeCall(ctx, wsID, proto.OpFSRemove, func(g *proto.Grant) any {
		return proto.FSRemoveReq{WS: wsID, Path: path, Recursive: recursive, Grant: g}
	}, nil)
}

// Rename moves a path.
func (c *Client) Rename(ctx context.Context, wsID, from, to string) error {
	return c.nodeCall(ctx, wsID, proto.OpFSRename, func(g *proto.Grant) any { return proto.FSRenameReq{WS: wsID, From: from, To: to, Grant: g} }, nil)
}

// Search greps.
func (c *Client) Search(ctx context.Context, wsID, path, pattern, glob string, max int) (*proto.FSSearchRes, error) {
	var res proto.FSSearchRes
	err := c.nodeCall(ctx, wsID, proto.OpFSSearch, func(g *proto.Grant) any {
		return proto.FSSearchReq{WS: wsID, Path: path, Pattern: pattern, Glob: glob, MaxResults: max, Grant: g}
	}, &res)
	return &res, err
}

// Edit applies atomic find/replace edits.
func (c *Client) Edit(ctx context.Context, wsID, path string, edits []proto.FSEdit) (int, error) {
	var res proto.FSEditRes
	err := c.nodeCall(ctx, wsID, proto.OpFSEdit, func(g *proto.Grant) any {
		return proto.FSEditReq{WS: wsID, Path: path, Edits: edits, IdempotencyKey: ids.New("idem"), Grant: g}
	}, &res)
	return res.Replacements, err
}

// Snapshot takes a snapshot; upload pushes it to the control plane store.
func (c *Client) Snapshot(ctx context.Context, wsID string, upload bool) (*proto.WSSnapshotRes, error) {
	var res proto.WSSnapshotRes
	err := c.nodeCall(ctx, wsID, proto.OpWSSnapshot, func(g *proto.Grant) any { return proto.WSSnapshotReq{WS: wsID, Upload: upload, Grant: g} }, &res)
	return &res, err
}

// WorkspaceInfo asks the node about a workspace.
func (c *Client) WorkspaceInfo(ctx context.Context, wsID string) (*proto.WSInfoRes, error) {
	var res proto.WSInfoRes
	err := c.nodeCall(ctx, wsID, proto.OpWSInfo, func(g *proto.Grant) any { return proto.WSGetReq{ID: wsID} }, &res)
	return &res, err
}

// ListSessions lists sessions on a workspace.
func (c *Client) ListSessions(ctx context.Context, wsID string) ([]proto.SessionStatus, error) {
	var res proto.SListRes
	err := c.nodeCall(ctx, wsID, proto.OpSList, func(g *proto.Grant) any { return proto.SListReq{WS: wsID} }, &res)
	return res.Sessions, err
}

// ---------------------------------------------------------------------------
// sessions
// ---------------------------------------------------------------------------

// Chunk is one delivered piece of session output.
type Chunk struct {
	Seq    uint64
	Stream uint8
	Data   []byte
}

// Session is a client-side cursor over a remote session.
type Session struct {
	c    *Client
	ID   string
	WS   string
	Kind string

	mu       sync.Mutex
	next     uint64 // next seq expected
	pending  map[uint64]*proto.Frame
	out      chan Chunk
	exit     *proto.ExitInfo
	exited   chan struct{}
	closed   bool
	iseq     atomic.Uint64
	attached bool
}

// Exec starts an exec/pty session. Chunks arrive on Session.Chunks().
func (c *Client) Exec(ctx context.Context, req proto.SOpenReq) (*Session, error) {
	if req.Kind == "" {
		req.Kind = proto.SessionExec
	}
	if req.IdempotencyKey == "" {
		req.IdempotencyKey = ids.New("idem")
	}
	var res proto.SOpenRes
	// Register a placeholder before the call so chunks arriving before the
	// response are not lost.
	s := c.newSession("", req.WS, req.Kind)
	c.mu.Lock()
	c.sessions["pending:"+req.IdempotencyKey] = s
	c.mu.Unlock()
	err := c.nodeCall(ctx, req.WS, proto.OpSOpen, func(g *proto.Grant) any { r := req; r.Grant = g; return r }, &res)
	c.mu.Lock()
	delete(c.sessions, "pending:"+req.IdempotencyKey)
	c.mu.Unlock()
	if err != nil {
		return nil, err
	}
	s.mu.Lock()
	s.attached = true
	s.mu.Unlock()
	return c.register(res.S, s), nil
}

// OpenPort opens a TCP forward to a port inside the workspace.
func (c *Client) OpenPort(ctx context.Context, wsID string, port int) (*Session, error) {
	var res proto.SOpenRes
	s := c.newSession("", wsID, proto.SessionPort)
	err := c.nodeCall(ctx, wsID, proto.OpPortOpen, func(g *proto.Grant) any { return proto.PortOpenReq{WS: wsID, Port: port, Grant: g} }, &res)
	if err != nil {
		return nil, err
	}
	s.mu.Lock()
	s.attached = true
	s.mu.Unlock()
	return c.register(res.S, s), nil
}

// Attach subscribes to an existing session from seq `from`.
func (c *Client) Attach(ctx context.Context, wsID, sid string, from uint64) (*Session, error) {
	s := c.newSession(sid, wsID, "")
	s.next = from
	c.mu.Lock()
	c.sessions[sid] = s
	c.mu.Unlock()
	var res proto.SOpenRes
	err := c.nodeCall(ctx, wsID, proto.OpSAttach, func(g *proto.Grant) any { return proto.SAttachReq{S: sid, From: from, Grant: g} }, &res)
	if err != nil {
		c.mu.Lock()
		delete(c.sessions, sid)
		c.mu.Unlock()
		return nil, err
	}
	s.mu.Lock()
	s.attached = true
	s.mu.Unlock()
	return s, nil
}

// register binds a session id to s and delivers any chunks that arrived
// early. If a live Session already exists for id (an idempotent re-open
// returned the same server session), that one is returned instead so there
// is exactly one cursor per (client, session).
func (c *Client) register(id string, s *Session) *Session {
	c.mu.Lock()
	if existing, ok := c.sessions[id]; ok && existing != s {
		c.mu.Unlock()
		return existing
	}
	s.ID = id
	c.sessions[id] = s
	early := c.orphans[id]
	delete(c.orphans, id)
	c.mu.Unlock()
	for _, f := range early {
		s.deliver(f)
	}
	return s
}

func (c *Client) newSession(id, ws, kind string) *Session {
	return &Session{c: c, ID: id, WS: ws, Kind: kind, pending: map[uint64]*proto.Frame{}, out: make(chan Chunk, 1024), exited: make(chan struct{})}
}

// Chunks delivers output in seq order. The channel closes after the exit chunk.
func (s *Session) Chunks() <-chan Chunk { return s.out }

// Exit returns the exit record once the session has ended.
func (s *Session) Exit() *proto.ExitInfo {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.exit
}

// Done is closed when the exit chunk has been delivered.
func (s *Session) Done() <-chan struct{} { return s.exited }

// Next is the next seq the session expects (for external resume bookkeeping).
func (s *Session) Next() uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.next
}

// deliver reorders chunks by seq and pushes them out.
func (s *Session) deliver(f *proto.Frame) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return
	}
	var body proto.ChunkBody
	if err := f.Decode(&body); err != nil {
		return
	}
	if body.Stream == proto.StreamGap {
		var gap proto.Gap
		_ = proto.Unmarshal(body.Data, &gap)
		if gap.To+1 > s.next {
			s.next = gap.To + 1
		}
		s.out <- Chunk{Seq: f.Seq, Stream: body.Stream, Data: body.Data}
		return
	}
	if f.Seq < s.next {
		return // duplicate from a re-attach
	}
	if f.Seq > s.next {
		s.pending[f.Seq] = f
		return
	}
	s.push(f.Seq, body)
	for {
		nf, ok := s.pending[s.next]
		if !ok {
			break
		}
		delete(s.pending, s.next)
		var nb proto.ChunkBody
		_ = nf.Decode(&nb)
		s.push(nf.Seq, nb)
	}
}

func (s *Session) push(seq uint64, body proto.ChunkBody) {
	s.next = seq + 1
	s.out <- Chunk{Seq: seq, Stream: body.Stream, Data: body.Data}
	if body.Stream == proto.StreamExit {
		var info proto.ExitInfo
		_ = proto.Unmarshal(body.Data, &info)
		s.exit = &info
		s.closed = true
		close(s.out)
		close(s.exited)
		s.c.mu.Lock()
		delete(s.c.sessions, s.ID)
		s.c.mu.Unlock()
	}
}

// reattach re-subscribes after a reconnect from the last delivered seq.
func (s *Session) reattach(ctx context.Context) {
	s.mu.Lock()
	if s.closed || !s.attached || s.ID == "" {
		s.mu.Unlock()
		return
	}
	from := s.next
	s.mu.Unlock()
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	var res proto.SOpenRes
	_ = s.c.nodeCall(ctx, s.WS, proto.OpSAttach, func(g *proto.Grant) any { return proto.SAttachReq{S: s.ID, From: from, Grant: g} }, &res)
}

// Input sends bytes to the process (or socket). Retries are idempotent.
func (s *Session) Input(ctx context.Context, data []byte, eof bool) error {
	iseq := s.iseq.Add(1)
	return s.c.nodeCall(ctx, s.WS, proto.OpSInput, func(g *proto.Grant) any { return proto.SInputReq{S: s.ID, ISeq: iseq, Data: data, EOF: eof} }, nil)
}

// Resize resizes a pty.
func (s *Session) Resize(ctx context.Context, rows, cols uint16) error {
	return s.c.nodeCall(ctx, s.WS, proto.OpSResize, func(g *proto.Grant) any { return proto.SResizeReq{S: s.ID, Rows: rows, Cols: cols} }, nil)
}

// Signal sends a signal by name.
func (s *Session) Signal(ctx context.Context, sig string) error {
	return s.c.nodeCall(ctx, s.WS, proto.OpSSignal, func(g *proto.Grant) any { return proto.SSignalReq{S: s.ID, Signal: sig} }, nil)
}

// Close detaches; kill also terminates the process.
func (s *Session) Close(ctx context.Context, kill bool) error {
	s.c.mu.Lock()
	delete(s.c.sessions, s.ID)
	s.c.mu.Unlock()
	s.mu.Lock()
	if !s.closed {
		s.closed = true
		close(s.out)
	}
	s.mu.Unlock()
	return s.c.nodeCall(ctx, s.WS, proto.OpSClose, func(g *proto.Grant) any { return proto.SCloseReq{S: s.ID, Kill: kill} }, nil)
}

// Wait blocks until exit (server-side wait plus local delivery).
func (s *Session) Wait(ctx context.Context) (*proto.ExitInfo, error) {
	select {
	case <-s.exited:
		return s.Exit(), nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// Run is a convenience: exec a command, collect stdout/stderr, return exit.
func (c *Client) Run(ctx context.Context, wsID string, program ...string) (stdout, stderr []byte, exit *proto.ExitInfo, err error) {
	s, err := c.Exec(ctx, proto.SOpenReq{WS: wsID, Kind: proto.SessionExec, Program: program})
	if err != nil {
		return nil, nil, nil, err
	}
loop:
	for {
		select {
		case ch, ok := <-s.Chunks():
			if !ok {
				break loop
			}
			switch ch.Stream {
			case proto.StreamStdout:
				stdout = append(stdout, ch.Data...)
			case proto.StreamStderr:
				stderr = append(stderr, ch.Data...)
			}
		case <-ctx.Done():
			_ = s.Close(context.Background(), true)
			return stdout, stderr, nil, ctx.Err()
		}
	}
	exit = s.Exit()
	if exit == nil {
		return stdout, stderr, nil, fmt.Errorf("session ended without exit record")
	}
	return stdout, stderr, exit, nil
}

// Copy pumps a session's stdout/stderr into writers until exit.
func Copy(s *Session, stdout, stderr io.Writer) *proto.ExitInfo {
	for ch := range s.Chunks() {
		switch ch.Stream {
		case proto.StreamStdout:
			if stdout != nil {
				_, _ = stdout.Write(ch.Data)
			}
		case proto.StreamStderr:
			if stderr != nil {
				_, _ = stderr.Write(ch.Data)
			}
		case proto.StreamGap:
			var gap proto.Gap
			_ = proto.Unmarshal(ch.Data, &gap)
			if stderr != nil {
				fmt.Fprintf(stderr, "\n[remount: output seq %d-%d elided]\n", gap.From, gap.To)
			}
		}
	}
	return s.Exit()
}

// ---------------------------------------------------------------------------
// diagnostics
// ---------------------------------------------------------------------------

// Diag asks the control plane for its own health, including any problems it
// can see without contacting a node.
func (c *Client) Diag(ctx context.Context, verify bool) (*proto.ControlDiag, error) {
	var d proto.ControlDiag
	err := c.call(ctx, proto.PeerControl, proto.OpDiag, proto.DiagReq{Verify: verify}, &d)
	return &d, err
}

// NodeDiag asks one node for its deep state: workspaces on disk, session log
// positions, free space, and its own findings. wsID names a workspace on that
// node the caller is entitled to; it may be empty only for a node holding
// nothing.
func (c *Client) NodeDiag(ctx context.Context, nodeID, wsID string, verify bool) (*proto.NodeDiag, error) {
	req := proto.NodeDiagReq{WS: wsID, Verify: verify}
	if wsID != "" {
		g, err := c.grant(ctx, wsID)
		if err != nil {
			return nil, err
		}
		req.Grant = g
	}
	var d proto.NodeDiag
	err := c.call(ctx, nodeID, proto.OpNodeDiag, req, &d)
	return &d, err
}

// NodeStatus asks one node for its summary.
func (c *Client) NodeStatus(ctx context.Context, nodeID string) (*proto.NodeStatus, error) {
	var st proto.NodeStatus
	err := c.call(ctx, nodeID, proto.OpNodeStatus, struct{}{}, &st)
	return &st, err
}
