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
	"crypto/sha256"
	"errors"
	"fmt"
	"hash"
	"io"
	"io/fs"
	"net/http"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"remount.dev/remount/internal/artifact"
	"remount.dev/remount/internal/artifact/chunked"
	"remount.dev/remount/internal/ids"
	"remount.dev/remount/internal/metrics"
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
	// MaxReadBytes bounds the allocation made by ReadFile. Default 64 MiB.
	MaxReadBytes int64
	// MaxRunOutputBytes bounds stdout+stderr collected by Run. Streaming a
	// Session through Chunks is unaffected. Default 64 MiB.
	MaxRunOutputBytes int64
	// ArtifactURL is the control plane's blob endpoint
	// (http://host/v1/artifacts). Required by UploadArtifact and
	// DownloadArtifact; the WebSocket link carries everything else.
	ArtifactURL string
	// HTTPClient performs artifact transfers. Default http.DefaultClient.
	HTTPClient *http.Client
}

// OperationOption configures one logical mutating operation. Reuse the same
// idempotency key when retrying after an ambiguous timeout or client restart.
type OperationOption func(*operationOptions)

type operationOptions struct {
	idempotencyKey string
	keySet         bool
}

// WithIdempotencyKey supplies a stable caller-owned key for a logical
// mutation. An empty key falls back to a generated key.
func WithIdempotencyKey(key string) OperationOption {
	return func(opts *operationOptions) {
		opts.idempotencyKey = key
		opts.keySet = true
	}
}

func operationKey(options []OperationOption) (string, bool) {
	var configured operationOptions
	for _, option := range options {
		if option != nil {
			option(&configured)
		}
	}
	if configured.idempotencyKey == "" {
		return ids.New("idem"), configured.keySet
	}
	return configured.idempotencyKey, configured.keySet
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
	orphans     map[string][]*proto.Frame
	orphanCount int
	eventSubs   map[string]*eventSubscription
	closed      bool
	gen         uint64 // connection generation
}

// New creates a client; it connects lazily.
func New(opts Options) *Client {
	if opts.Retries == 0 {
		opts.Retries = 5
	}
	if opts.MaxReadBytes <= 0 {
		opts.MaxReadBytes = 64 << 20
	}
	if opts.MaxRunOutputBytes <= 0 {
		opts.MaxRunOutputBytes = 64 << 20
	}
	return &Client{
		opts: opts, grants: map[string]*proto.Grant{}, sessions: map[string]*Session{},
		orphans: map[string][]*proto.Frame{}, eventSubs: map[string]*eventSubscription{},
	}
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
	sessions := make([]*Session, 0, len(c.sessions))
	for _, s := range c.sessions {
		sessions = append(sessions, s)
	}
	subs := make([]*eventSubscription, 0, len(c.eventSubs))
	for _, sub := range c.eventSubs {
		subs = append(subs, sub)
	}
	c.mu.Unlock()
	if p != nil {
		p.Close()
	}
	for _, s := range sessions {
		s.fail(transport.ErrClosed)
	}
	for _, sub := range subs {
		sub.close()
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
	ok, err := transport.Hello(hctx, p, proto.Hello{Peer: c.ID(), Role: proto.RoleClient, Token: c.opts.Token, Caps: proto.PeerCapabilities(), Principal: c.opts.Principal})
	cancel()
	if err != nil {
		p.Close()
		return nil, err
	}
	if !proto.HasCapability(ok.Caps, proto.CapabilityV1) {
		p.Close()
		return nil, proto.Err(proto.CodeUnsupported, "server did not negotiate required capability %q", proto.CapabilityV1)
	}
	if proto.HasCapability(ok.Caps, proto.CapabilityControllerEpoch) {
		if ok.ControllerEpoch == 0 {
			p.Close()
			return nil, proto.Err(proto.CodeConflict, "server negotiated controller-epoch without an epoch")
		}
		p.SetControllerEpoch(ok.ControllerEpoch)
	}
	c.mu.Lock()
	if c.peer != nil {
		c.peer.Close()
	}
	c.peer = p
	c.id = ok.Peer
	c.gen++
	gen := c.gen
	c.grants = map[string]*proto.Grant{} // nodes cache grants per connection
	// Unknown chunks from an older connection are replayable from the session
	// log. Keeping them forever only poisons the bounded orphan cache.
	c.orphans = map[string][]*proto.Frame{}
	c.orphanCount = 0
	sessions := make([]*Session, 0, len(c.sessions))
	for _, s := range c.sessions {
		sessions = append(sessions, s)
	}
	subs := make([]*eventSubscription, 0, len(c.eventSubs))
	for _, sub := range c.eventSubs {
		subs = append(subs, sub)
	}
	c.mu.Unlock()
	// Re-attach live sessions on the new connection, and supervise the
	// connection so a drop while sessions are streaming triggers a
	// reconnect even when the caller is only ranging over chunks.
	for _, s := range sessions {
		go s.reattach(context.Background(), gen)
	}
	for _, sub := range subs {
		go sub.reattach(gen)
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
		closed, current, n := c.closed, c.peer == p, len(c.sessions)+len(c.eventSubs)
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
	if epoch := p.ControllerEpoch(); epoch != 0 && f.ControllerEpoch < epoch {
		return
	}
	switch f.T {
	case proto.KindChunk:
		c.mu.Lock()
		s := c.sessions[f.S]
		overflow := false
		if s == nil {
			_, known := c.orphans[f.S]
			if (!known && len(c.orphans) >= 256) || len(c.orphans[f.S]) >= 4096 || c.orphanCount >= 16384 {
				// Force a reconnect and replay rather than silently discarding an
				// unregistered session's first output.
				overflow = true
				c.orphans = map[string][]*proto.Frame{}
				c.orphanCount = 0
			} else {
				c.orphans[f.S] = append(c.orphans[f.S], f)
				c.orphanCount++
			}
		}
		c.mu.Unlock()
		if overflow {
			p.Close()
			return
		}
		if s != nil {
			s.enqueue(f)
		}
	case proto.KindEvent:
		if f.Op == "log" {
			var post proto.EventPost
			if err := f.Decode(&post); err == nil {
				c.mu.Lock()
				var subscribers []*eventSubscription
				if post.Subscription != "" {
					if sub := c.eventSubs[post.Subscription]; sub != nil {
						subscribers = append(subscribers, sub)
					}
				} else {
					// Compatibility with an older control plane that does not echo
					// subscription ids: local subscribers still filter and dedupe.
					for _, sub := range c.eventSubs {
						subscribers = append(subscribers, sub)
					}
				}
				c.mu.Unlock()
				for _, sub := range subscribers {
					for _, e := range post.Events {
						sub.enqueue(e)
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
		if attempt+1 >= c.opts.Retries {
			break
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
func (c *Client) CreateWorkspace(ctx context.Context, spec proto.WorkspaceSpec, options ...OperationOption) (*proto.Workspace, error) {
	var ws proto.Workspace
	idem, _ := operationKey(options)
	err := c.call(ctx, proto.PeerControl, proto.OpWSCreate, proto.WSCreateReq{Spec: spec, IdempotencyKey: idem}, &ws)
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
func (c *Client) DestroyWorkspace(ctx context.Context, id string, options ...OperationOption) error {
	idem, _ := operationKey(options)
	return c.call(ctx, proto.PeerControl, proto.OpWSDestroy, proto.WSGetReq{ID: id, IdempotencyKey: idem}, nil)
}

// MoveWorkspace snapshots and re-queues a workspace with new requirements.
func (c *Client) MoveWorkspace(ctx context.Context, id string, req *proto.Requires, placement *proto.Placement, options ...OperationOption) (*proto.Workspace, error) {
	var ws proto.Workspace
	idem, _ := operationKey(options)
	err := c.call(ctx, proto.PeerControl, proto.OpWSMove, proto.WSMoveReq{ID: id, Requires: req, Placement: placement, IdempotencyKey: idem}, &ws)
	c.forgetGrant(id)
	return &ws, err
}

// SetWorkspaceACL replaces who else may use a workspace. Only its owner or an
// administrator may call it. Principals removed by the change lose access
// within one node renew interval; everyone else's grants refresh transparently.
func (c *Client) SetWorkspaceACL(ctx context.Context, id string, acl proto.WorkspaceACL, options ...OperationOption) (*proto.Workspace, error) {
	var ws proto.Workspace
	idem, _ := operationKey(options)
	err := c.call(ctx, proto.PeerControl, proto.OpWSACL, proto.WSACLReq{ID: id, ACL: acl, IdempotencyKey: idem}, &ws)
	c.forgetGrant(id)
	return &ws, err
}

// SleepWorkspace pauses a workspace until a timer or event.
func (c *Client) SleepWorkspace(ctx context.Context, req proto.WSSleepReq, options ...OperationOption) (*proto.Timer, error) {
	var t proto.Timer
	idem, configured := operationKey(options)
	if configured && req.IdempotencyKey != "" && req.IdempotencyKey != idem {
		return nil, proto.Err(proto.CodeBadRequest, "conflicting idempotency keys")
	}
	if req.IdempotencyKey == "" || configured {
		req.IdempotencyKey = idem
	}
	err := c.call(ctx, proto.PeerControl, proto.OpWSSleep, req, &t)
	c.forgetGrant(req.ID)
	return &t, err
}

// WakeWorkspace resumes a paused workspace.
func (c *Client) WakeWorkspace(ctx context.Context, id string, options ...OperationOption) (*proto.Workspace, error) {
	var ws proto.Workspace
	idem, _ := operationKey(options)
	err := c.call(ctx, proto.PeerControl, proto.OpWSWake, proto.WSGetReq{ID: id, IdempotencyKey: idem}, &ws)
	return &ws, err
}

// ListNodes lists nodes.
func (c *Client) ListNodes(ctx context.Context) ([]proto.NodeStatus, error) {
	var res proto.NodeListRes
	err := c.call(ctx, proto.PeerControl, proto.OpNodeList, nil, &res)
	return res.Nodes, err
}

// QuarantineFleet creates a durable incident-containment operation. The
// returned object may still be running; use WaitFleetOperation or GetFleetOperation
// to observe per-target acknowledgement.
func (c *Client) QuarantineFleet(ctx context.Context, req proto.FleetQuarantineReq) (*proto.FleetOperation, error) {
	if req.IdempotencyKey == "" {
		req.IdempotencyKey = ids.New("idem")
	}
	var operation proto.FleetOperation
	err := c.call(ctx, proto.PeerControl, proto.OpFleetQuarantine, req, &operation)
	return &operation, err
}

// CreateBase pins an uploaded artifact under a tenant-scoped name so future
// workspaces can start from it with WorkspaceSpec.Base. The artifact is
// excluded from garbage collection until RemoveBase.
func (c *Client) CreateBase(ctx context.Context, req proto.BaseCreateReq, options ...OperationOption) (*proto.Base, error) {
	if key, set := operationKey(options); set {
		req.IdempotencyKey = key
	} else if req.IdempotencyKey == "" {
		req.IdempotencyKey = ids.New("idem")
	}
	var base proto.Base
	err := c.call(ctx, proto.PeerControl, proto.OpBaseCreate, req, &base)
	return &base, err
}

// ListBases returns the bases visible to the caller, sorted by tenant and name.
func (c *Client) ListBases(ctx context.Context) ([]proto.Base, error) {
	var response proto.BaseListRes
	err := c.call(ctx, proto.PeerControl, proto.OpBaseList, nil, &response)
	return response.Bases, err
}

// RemoveBase unpins a base. Workspaces already created from it are unaffected.
func (c *Client) RemoveBase(ctx context.Context, name string, options ...OperationOption) error {
	idem, set := operationKey(options)
	if !set {
		idem = ids.New("idem")
	}
	return c.call(ctx, proto.PeerControl, proto.OpBaseRemove, proto.BaseRemoveReq{Name: name, IdempotencyKey: idem}, nil)
}

// CreateVolume creates a tenant-scoped immutable shared-data volume from an
// already uploaded artifact.
func (c *Client) CreateVolume(ctx context.Context, req proto.VolumeCreateReq, options ...OperationOption) (*proto.Volume, error) {
	if key, set := operationKey(options); set {
		req.IdempotencyKey = key
	} else if req.IdempotencyKey == "" {
		req.IdempotencyKey = ids.New("idem")
	}
	var volume proto.Volume
	err := c.call(ctx, proto.PeerControl, proto.OpVolumeCreate, req, &volume)
	return &volume, err
}

// GetVolume returns one tenant-scoped shared-data volume.
func (c *Client) GetVolume(ctx context.Context, id string) (*proto.Volume, error) {
	var volume proto.Volume
	err := c.call(ctx, proto.PeerControl, proto.OpVolumeGet, proto.VolumeGetReq{ID: id}, &volume)
	return &volume, err
}

// ListVolumes returns shared-data volumes visible to the caller.
func (c *Client) ListVolumes(ctx context.Context) ([]proto.Volume, error) {
	var response proto.VolumeListRes
	err := c.call(ctx, proto.PeerControl, proto.OpVolumeList, nil, &response)
	return response.Volumes, err
}

// RemoveVolume removes an unattached volume and releases its artifact pins.
func (c *Client) RemoveVolume(ctx context.Context, id string, options ...OperationOption) error {
	idem, set := operationKey(options)
	if !set {
		idem = ids.New("idem")
	}
	return c.call(ctx, proto.PeerControl, proto.OpVolumeRemove, proto.VolumeRemoveReq{ID: id, IdempotencyKey: idem}, nil)
}

// AttachVolume pins the current volume version in a workspace declaration.
// A running workspace must first be paused so the next materialization can
// enforce the read-only mount.
func (c *Client) AttachVolume(ctx context.Context, req proto.VolumeAttachReq, options ...OperationOption) (*proto.Workspace, error) {
	if key, set := operationKey(options); set {
		req.IdempotencyKey = key
	} else if req.IdempotencyKey == "" {
		req.IdempotencyKey = ids.New("idem")
	}
	var ws proto.Workspace
	err := c.call(ctx, proto.PeerControl, proto.OpVolumeAttach, req, &ws)
	return &ws, err
}

// DetachVolume removes a pinned mount declaration from a non-running
// workspace generation.
func (c *Client) DetachVolume(ctx context.Context, req proto.VolumeDetachReq, options ...OperationOption) (*proto.Workspace, error) {
	if key, set := operationKey(options); set {
		req.IdempotencyKey = key
	} else if req.IdempotencyKey == "" {
		req.IdempotencyKey = ids.New("idem")
	}
	var ws proto.Workspace
	err := c.call(ctx, proto.PeerControl, proto.OpVolumeDetach, req, &ws)
	return &ws, err
}

// ArchiveVolume snapshots one jailed workspace directory and uploads the
// immutable artifact without changing the workspace's authoritative snapshot.
func (c *Client) ArchiveVolume(ctx context.Context, wsID, path string, options ...OperationOption) (*proto.WSSnapshotRes, error) {
	return c.ArchiveVolumeWithUpload(ctx, wsID, path, true, options...)
}

// ArchiveVolumeWithUpload controls whether the node also publishes the
// resulting blob to the configured control-plane artifact service.
func (c *Client) ArchiveVolumeWithUpload(ctx context.Context, wsID, path string, upload bool, options ...OperationOption) (*proto.WSSnapshotRes, error) {
	idem, set := operationKey(options)
	if !set {
		idem = ids.New("idem")
	}
	var result proto.WSSnapshotRes
	err := c.nodeCall(ctx, wsID, proto.OpVolumeArchive, func(g *proto.Grant) any {
		return proto.VolumeArchiveReq{WS: wsID, Path: path, Upload: upload, IdempotencyKey: idem, Grant: g}
	}, &result)
	return &result, err
}

// PublishVolumePath snapshots one workspace directory and keeps the node tree
// boundary held until the control plane commits the generation/version CAS.
func (c *Client) PublishVolumePath(ctx context.Context, wsID, path, volumeID string, expectedVersion uint64, options ...OperationOption) (*proto.Volume, error) {
	idem, set := operationKey(options)
	if !set {
		idem = ids.New("idem")
	}
	var result proto.Volume
	err := c.nodeCall(ctx, wsID, proto.OpVolumePublish, func(g *proto.Grant) any {
		return proto.VolumePublishPathReq{WS: wsID, Path: path, Volume: volumeID, ExpectedVersion: expectedVersion, IdempotencyKey: idem, Grant: g}
	}, &result)
	return &result, err
}

// CreatePool records tenant-scoped desired node capacity. Provider
// credentials remain server-side and are not part of the request.
func (c *Client) CreatePool(ctx context.Context, spec proto.PoolSpec, options ...OperationOption) (*proto.Pool, error) {
	idem, set := operationKey(options)
	if !set {
		idem = ids.New("idem")
	}
	var pool proto.Pool
	err := c.call(ctx, proto.PeerControl, proto.OpPoolCreate, proto.PoolCreateReq{Spec: spec, IdempotencyKey: idem}, &pool)
	return &pool, err
}

// GetPool returns a tenant-scoped pool by name.
func (c *Client) GetPool(ctx context.Context, name string) (*proto.Pool, error) {
	var pool proto.Pool
	err := c.call(ctx, proto.PeerControl, proto.OpPoolGet, proto.PoolGetReq{Name: name}, &pool)
	return &pool, err
}

// ListPools returns the caller's tenant-scoped pools sorted by name.
func (c *Client) ListPools(ctx context.Context) ([]proto.Pool, error) {
	var response proto.PoolListRes
	err := c.call(ctx, proto.PeerControl, proto.OpPoolList, nil, &response)
	return response.Pools, err
}

// RemovePool removes an empty pool resource. Provider machines are never
// destroyed as an implicit side effect of this call.
func (c *Client) RemovePool(ctx context.Context, name string, options ...OperationOption) error {
	idem, set := operationKey(options)
	if !set {
		idem = ids.New("idem")
	}
	return c.call(ctx, proto.PeerControl, proto.OpPoolRemove, proto.PoolRemoveReq{Name: name, IdempotencyKey: idem}, nil)
}

// CreateQueue records a durable task list for a workspace (ADR 0041). A
// missing idempotency key is generated so a retry cannot create two queues.
func (c *Client) CreateQueue(ctx context.Context, req proto.QueueCreateReq, options ...OperationOption) (*proto.Queue, error) {
	if key, set := operationKey(options); set {
		req.IdempotencyKey = key
	} else if req.IdempotencyKey == "" {
		req.IdempotencyKey = ids.New("idem")
	}
	var q proto.Queue
	err := c.call(ctx, proto.PeerControl, proto.OpQueueCreate, req, &q)
	return &q, err
}

// GetQueue returns one queue.
func (c *Client) GetQueue(ctx context.Context, id string) (*proto.Queue, error) {
	var q proto.Queue
	err := c.call(ctx, proto.PeerControl, proto.OpQueueGet, proto.QueueGetReq{ID: id}, &q)
	return &q, err
}

// ListQueues returns the caller's queues, optionally only those of one
// workspace, oldest first.
func (c *Client) ListQueues(ctx context.Context, wsID string) ([]proto.Queue, error) {
	var res proto.QueueListRes
	err := c.call(ctx, proto.PeerControl, proto.OpQueueList, proto.QueueListReq{WS: wsID}, &res)
	return res.Queues, err
}

// AdvanceQueue records the outcome of the task at the queue's cursor.
func (c *Client) AdvanceQueue(ctx context.Context, req proto.QueueAdvanceReq, options ...OperationOption) (*proto.Queue, error) {
	if key, set := operationKey(options); set {
		req.IdempotencyKey = key
	} else if req.IdempotencyKey == "" {
		req.IdempotencyKey = ids.New("idem")
	}
	var q proto.Queue
	err := c.call(ctx, proto.PeerControl, proto.OpQueueAdvance, req, &q)
	return &q, err
}

// GetFleetOperation returns one durable containment operation.
func (c *Client) GetFleetOperation(ctx context.Context, id string) (*proto.FleetOperation, error) {
	var operation proto.FleetOperation
	err := c.call(ctx, proto.PeerControl, proto.OpFleetGet, proto.FleetGetReq{ID: id}, &operation)
	return &operation, err
}

// ListFleetOperations returns containment operations visible to the caller.
func (c *Client) ListFleetOperations(ctx context.Context) ([]proto.FleetOperation, error) {
	var response proto.FleetListRes
	err := c.call(ctx, proto.PeerControl, proto.OpFleetList, nil, &response)
	return response.Operations, err
}

// WaitFleetOperation waits for completed or partial state. Partial means at
// least one target failed or remained unreachable at the operation deadline.
func (c *Client) WaitFleetOperation(ctx context.Context, id string) (*proto.FleetOperation, error) {
	for {
		operation, err := c.GetFleetOperation(ctx, id)
		if err != nil {
			return nil, err
		}
		if operation.State == proto.FleetStateCompleted || operation.State == proto.FleetStatePartial {
			return operation, nil
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(200 * time.Millisecond):
		}
	}
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
	cursor := from
	var all []proto.Event
	for {
		events, err := c.ReadEventPage(ctx, cursor, ws)
		if err != nil {
			return nil, err
		}
		if len(events) == 0 {
			return all, nil
		}
		all = append(all, events...)
		next := events[len(events)-1].Seq + 1
		if next <= cursor {
			return nil, proto.Err(proto.CodeInternal, "event pagination did not advance")
		}
		cursor = next
		if len(events) < 1000 {
			return all, nil
		}
	}
}

// ReadEventPage returns at most one control-plane page of historical events.
// Exporters use it to avoid retaining the complete audit history in memory.
func (c *Client) ReadEventPage(ctx context.Context, from uint64, ws string) ([]proto.Event, error) {
	var response proto.EventPost
	err := c.call(ctx, proto.PeerControl, proto.OpEventsTail, proto.EventsTailReq{From: from, WS: ws}, &response)
	return response.Events, err
}

// TailEvents streams events on the returned channel until ctx ends.
func (c *Client) TailEvents(ctx context.Context, from uint64, ws string) (<-chan proto.Event, error) {
	sub := newEventSubscription(c, ids.New("sub"), from, ws)
	c.mu.Lock()
	c.eventSubs[sub.id] = sub
	c.mu.Unlock()
	if err := c.call(ctx, proto.PeerControl, proto.OpEventsTail, proto.EventsTailReq{
		From: from, WS: ws, Follow: true, Subscription: sub.id,
	}, nil); err != nil {
		c.removeEventSubscription(sub)
		sub.close()
		return nil, err
	}
	go func() {
		<-ctx.Done()
		c.stopEventSubscription(sub)
	}()
	return sub.out, nil
}

type eventSubscription struct {
	c          *Client
	id         string
	ws         string
	mu         sync.Mutex
	next       uint64
	in         chan proto.Event
	out        chan proto.Event
	stop       chan struct{}
	stopOnce   sync.Once
	remoteOnce sync.Once
	// dropped records that this subscription ended because the consumer could
	// not keep up, rather than because it was cancelled. Without it the two are
	// indistinguishable, and a consumer that silently lost events looks exactly
	// like one that asked to stop.
	dropped atomic.Bool
}

// Lagged reports whether this subscription was ended because its consumer fell
// behind. Events are replayable, so the recovery is to re-tail from the last
// sequence the consumer processed; what must never happen is that the loss is
// invisible.
func (s *eventSubscription) Lagged() bool { return s.dropped.Load() }

func newEventSubscription(c *Client, id string, from uint64, ws string) *eventSubscription {
	s := &eventSubscription{
		c: c, id: id, ws: ws, next: from,
		in: make(chan proto.Event, 512), out: make(chan proto.Event, 256), stop: make(chan struct{}),
	}
	go s.run()
	return s
}

func (s *eventSubscription) run() {
	defer func() {
		s.c.removeEventSubscription(s)
		close(s.out)
	}()
	for {
		select {
		case <-s.stop:
			return
		case event := <-s.in:
			s.mu.Lock()
			if event.Seq < s.next || (s.ws != "" && event.Stream != s.ws) {
				s.mu.Unlock()
				continue
			}
			s.mu.Unlock()
			select {
			case s.out <- event:
				s.mu.Lock()
				if event.Seq >= s.next {
					s.next = event.Seq + 1
				}
				s.mu.Unlock()
			case <-s.stop:
				return
			}
		}
	}
}

func (s *eventSubscription) enqueue(event proto.Event) {
	select {
	case <-s.stop:
		return
	case s.in <- event:
	default:
		// The consumer is behind. Blocking here would stall the peer reader for
		// everyone, so this event is dropped — but the subscription stays open.
		//
		// Ending it instead was worse than the loss: a closed channel is exactly
		// what a caller sees after cancelling, so a subscriber that silently
		// lost 3,744 of 4,000 events could not tell that from its own clean
		// shutdown. Staying open leaves the loss observable the way this system
		// reports loss everywhere else — as a visible gap. Events carry a
		// monotonic Seq, so a consumer sees the discontinuity, Lagged reports
		// it directly, and the counter makes it visible to an operator who is
		// not reading the stream at all.
		s.dropped.Store(true)
		metrics.EventSubscribersDropped.Inc()
	}
}

func (s *eventSubscription) close() {
	s.stopOnce.Do(func() { close(s.stop) })
}

func (s *eventSubscription) cursor() uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.next
}

func (s *eventSubscription) reattach(generation uint64) {
	select {
	case <-s.stop:
		return
	default:
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	err := s.c.call(ctx, proto.PeerControl, proto.OpEventsTail, proto.EventsTailReq{
		From: s.cursor(), WS: s.ws, Follow: true, Subscription: s.id,
	}, nil)
	if err != nil && s.c.generation() == generation {
		s.close()
	}
}

func (c *Client) removeEventSubscription(sub *eventSubscription) {
	c.mu.Lock()
	if c.eventSubs[sub.id] == sub {
		delete(c.eventSubs, sub.id)
	}
	c.mu.Unlock()
}

func (c *Client) stopEventSubscription(sub *eventSubscription) {
	c.removeEventSubscription(sub)
	sub.close()
	sub.remoteOnce.Do(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = c.call(ctx, proto.PeerControl, proto.OpEventsStop, proto.EventsStopReq{Subscription: sub.id}, nil)
	})
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
// grant. A stale grant or a node still applying a newer authorization
// revision is refreshed and retried within a bounded interval.
func (c *Client) nodeCall(ctx context.Context, wsID, op string, body func(g *proto.Grant) any, out any) error {
	const attempts = 5
	for attempt := 0; attempt < attempts; attempt++ {
		g, err := c.grant(ctx, wsID)
		if err != nil {
			return err
		}
		err = c.call(ctx, g.Node, op, body(g), out)
		var pe *proto.Error
		retryable := errors.As(err, &pe) &&
			(pe.Code == proto.CodeConflict || pe.Code == proto.CodeUnreachable || pe.Code == proto.CodeUnauthorized)
		if retryable && attempt+1 < attempts {
			c.forgetGrant(wsID)
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(time.Duration(attempt+1) * 100 * time.Millisecond):
			}
			continue
		}
		return err
	}
	return nil
}

// ---------------------------------------------------------------------------
// filesystem
// ---------------------------------------------------------------------------

const maxInlineMutationBytes = 3 << 20

// ReadFile reads a whole file in bounded wire-sized pages. It returns an
// explicit resource_exhausted error instead of silently returning a prefix.
func (c *Client) ReadFile(ctx context.Context, wsID, path string) ([]byte, error) {
	var data []byte
	var offset int64
	for {
		var res proto.FSReadRes
		err := c.nodeCall(ctx, wsID, proto.OpFSRead, func(g *proto.Grant) any {
			return proto.FSReadReq{WS: wsID, Path: path, Offset: offset, Limit: 1 << 20, Grant: g}
		}, &res)
		if err != nil {
			return nil, err
		}
		if res.Size > c.opts.MaxReadBytes || int64(len(data))+int64(len(res.Data)) > c.opts.MaxReadBytes {
			return nil, proto.Err(proto.CodeResourceExhausted, "file exceeds ReadFile limit of %d bytes", c.opts.MaxReadBytes)
		}
		data = append(data, res.Data...)
		offset += int64(len(res.Data))
		if res.EOF {
			return data, nil
		}
		if len(res.Data) == 0 {
			return nil, proto.Err(proto.CodeInternal, "file pagination made no progress")
		}
	}
}

// WriteFile writes a file, creating parents.
func (c *Client) WriteFile(ctx context.Context, wsID, path string, data []byte, mode uint32, options ...OperationOption) error {
	if len(data) > maxInlineMutationBytes {
		return proto.Err(proto.CodeResourceExhausted, "inline write exceeds %d bytes", maxInlineMutationBytes)
	}
	idem, _ := operationKey(options)
	return c.nodeCall(ctx, wsID, proto.OpFSWrite, func(g *proto.Grant) any {
		return proto.FSWriteReq{WS: wsID, Path: path, Data: data, Mode: mode, MkdirP: true, IdempotencyKey: idem, Grant: g}
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
func (c *Client) Mkdir(ctx context.Context, wsID, path string, options ...OperationOption) error {
	idem, _ := operationKey(options)
	return c.nodeCall(ctx, wsID, proto.OpFSMkdir, func(g *proto.Grant) any {
		return proto.FSMkdirReq{WS: wsID, Path: path, IdempotencyKey: idem, Grant: g}
	}, nil)
}

// Remove deletes a path.
func (c *Client) Remove(ctx context.Context, wsID, path string, recursive bool, options ...OperationOption) error {
	idem, _ := operationKey(options)
	return c.nodeCall(ctx, wsID, proto.OpFSRemove, func(g *proto.Grant) any {
		return proto.FSRemoveReq{WS: wsID, Path: path, Recursive: recursive, IdempotencyKey: idem, Grant: g}
	}, nil)
}

// Rename moves a path.
func (c *Client) Rename(ctx context.Context, wsID, from, to string, options ...OperationOption) error {
	idem, _ := operationKey(options)
	return c.nodeCall(ctx, wsID, proto.OpFSRename, func(g *proto.Grant) any {
		return proto.FSRenameReq{WS: wsID, From: from, To: to, IdempotencyKey: idem, Grant: g}
	}, nil)
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
func (c *Client) Edit(ctx context.Context, wsID, path string, edits []proto.FSEdit, options ...OperationOption) (int, error) {
	requestBytes := 0
	for _, edit := range edits {
		requestBytes += len(edit.Old) + len(edit.New)
	}
	if requestBytes > maxInlineMutationBytes {
		return 0, proto.Err(proto.CodeResourceExhausted, "inline edit exceeds %d bytes", maxInlineMutationBytes)
	}
	var res proto.FSEditRes
	idem, _ := operationKey(options)
	err := c.nodeCall(ctx, wsID, proto.OpFSEdit, func(g *proto.Grant) any {
		return proto.FSEditReq{WS: wsID, Path: path, Edits: edits, IdempotencyKey: idem, Grant: g}
	}, &res)
	return res.Replacements, err
}

// ApplyTar overlays an artifact previously stored with UploadArtifact onto the
// workspace tree. Files land one rename at a time; nothing else is removed.
func (c *Client) ApplyTar(ctx context.Context, wsID, artifactID string, options ...OperationOption) (*proto.FSApplyTarRes, error) {
	return c.ApplyArtifact(ctx, wsID, artifactID, proto.ArtifactFormatTar, options...)
}

// ApplyArtifact is ApplyTar for an artifact in a named representation. The
// node reconstructs the archive from the format it is told about; it never
// guesses one from the id, and refuses a representation it cannot restore
// without touching the tree.
func (c *Client) ApplyArtifact(ctx context.Context, wsID, artifactID, format string, options ...OperationOption) (*proto.FSApplyTarRes, error) {
	format, err := proto.NormalizeArtifactFormat(format)
	if err != nil {
		return nil, err
	}
	if format == proto.ArtifactFormatTar {
		// The legacy representation stays off the wire so a request from this
		// release is byte-identical to one from a release without formats.
		format = ""
	}
	var res proto.FSApplyTarRes
	idem, _ := operationKey(options)
	err = c.nodeCall(ctx, wsID, proto.OpFSApplyTar, func(g *proto.Grant) any {
		return proto.FSApplyTarReq{WS: wsID, Artifact: artifactID, Format: format, IdempotencyKey: idem, Grant: g}
	}, &res)
	return &res, err
}

// ErrNoArtifactURL means Options.ArtifactURL was not configured.
var ErrNoArtifactURL = errors.New("client: ArtifactURL not configured")

// UploadArtifact stores r as a content-addressed artifact in the control
// plane and returns its id. The stream is spooled to a temporary file so the
// digest can be computed before the single PUT; the file is removed before
// return.
func (c *Client) UploadArtifact(ctx context.Context, r io.Reader) (string, int64, error) {
	if c.opts.ArtifactURL == "" {
		return "", 0, ErrNoArtifactURL
	}
	spool, err := os.CreateTemp("", "remount-upload-*")
	if err != nil {
		return "", 0, err
	}
	defer func() {
		spool.Close()
		os.Remove(spool.Name())
	}()
	h := sha256.New()
	size, err := io.Copy(io.MultiWriter(spool, h), r)
	if err != nil {
		return "", 0, err
	}
	if _, err := spool.Seek(0, io.SeekStart); err != nil {
		return "", 0, err
	}
	id := artifact.ID(h.Sum(nil))
	req, err := http.NewRequestWithContext(ctx, http.MethodPut, c.artifactURL(id), spool)
	if err != nil {
		return "", 0, err
	}
	req.ContentLength = size
	req.Header.Set("Authorization", "Bearer "+c.opts.Token)
	req.Header.Set("Content-Type", "application/gzip")
	resp, err := c.httpClient().Do(req)
	if err != nil {
		return "", 0, err
	}
	defer resp.Body.Close()
	if resp.StatusCode/100 != 2 {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return "", 0, fmt.Errorf("client: upload artifact: HTTP %d: %s", resp.StatusCode, strings.TrimSpace(string(body)))
	}
	return id, size, nil
}

// DownloadArtifact streams an artifact from the control plane. The returned
// reader fails with artifact.ErrDigestMismatch at EOF if the bytes do not
// hash to id, so callers that consume the whole stream never act on a
// corrupted or substituted archive without seeing an error.
func (c *Client) DownloadArtifact(ctx context.Context, id string) (io.ReadCloser, error) {
	r, _, err := c.DownloadArtifactWithSize(ctx, id)
	return r, err
}

// DownloadSnapshot returns a deterministic tar.gz stream for a snapshot in
// either the legacy tar or chunked representation. Callers must pass the
// format returned by Snapshot or stored on the workspace; downloading a
// chunked manifest as though it were a tar archive is an invalid header.
func (c *Client) DownloadSnapshot(ctx context.Context, id, format string) (io.ReadCloser, error) {
	format, err := proto.NormalizeArtifactFormat(format)
	if err != nil {
		return nil, err
	}
	if format != proto.ArtifactFormatChunkedV1 {
		return c.DownloadArtifact(ctx, id)
	}
	pr, pw := io.Pipe()
	go func() {
		err := chunked.ExportTar(ctx, clientArtifactReadStore{ctx: ctx, client: c}, id, pw, chunked.Limits{})
		_ = pw.CloseWithError(err)
	}()
	return pr, nil
}

type clientArtifactReadStore struct {
	ctx    context.Context
	client *Client
}

func (clientArtifactReadStore) Put(io.Reader) (string, int64, error) {
	return "", 0, errors.New("client artifact view is read-only")
}

func (s clientArtifactReadStore) Open(id string) (io.ReadCloser, int64, error) {
	return s.client.DownloadArtifactWithSize(s.ctx, id)
}

func (s clientArtifactReadStore) Head(id string) (int64, error) {
	return s.client.HeadArtifact(s.ctx, id)
}

func (clientArtifactReadStore) Delete(string) error {
	return errors.New("client artifact view is read-only")
}

func (clientArtifactReadStore) List() ([]string, error) {
	return nil, errors.New("client artifact enumeration is unavailable")
}

var _ artifact.BlobStore = clientArtifactReadStore{}

// clientArtifactStore adds publication to the read view. It is the store a
// chunked push runs against: Head asks the control plane what it already
// holds so an unchanged chunk is never sent, and Put publishes the ones it
// does not.
type clientArtifactStore struct {
	clientArtifactReadStore
}

func (s clientArtifactStore) Put(r io.Reader) (string, int64, error) {
	return s.client.UploadArtifact(s.ctx, r)
}

var _ artifact.BlobStore = clientArtifactStore{}

// HeadArtifact reports an artifact's stored size without transferring it, and
// returns an error satisfying errors.Is(err, fs.ErrNotExist) when the control
// plane does not hold it. Deduplication depends on this being a real HEAD:
// answering the same question with GET would download every chunk in order to
// discover it was already there, which costs more than uploading everything.
func (c *Client) HeadArtifact(ctx context.Context, id string) (int64, error) {
	if c.opts.ArtifactURL == "" {
		return 0, ErrNoArtifactURL
	}
	if _, err := artifact.Digest(id); err != nil {
		return 0, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodHead, c.artifactURL(id), nil)
	if err != nil {
		return 0, err
	}
	req.Header.Set("Authorization", "Bearer "+c.opts.Token)
	resp, err := c.httpClient().Do(req)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusNotFound {
		return 0, fmt.Errorf("client: artifact %s: %w", id, fs.ErrNotExist)
	}
	if resp.StatusCode != http.StatusOK {
		return 0, fmt.Errorf("client: head artifact %s: HTTP %d", id, resp.StatusCode)
	}
	if resp.ContentLength < 0 {
		return 0, fmt.Errorf("client: head artifact %s: missing Content-Length", id)
	}
	return resp.ContentLength, nil
}

// UploadChunkedSnapshot publishes dir as a chunked manifest, transferring only
// the chunks the control plane reports missing. The returned result names the
// manifest id to pass to ApplyArtifact and reports the deduplication actually
// achieved, which is the only reason to choose this representation over a tar.
func (c *Client) UploadChunkedSnapshot(ctx context.Context, dir string, opts chunked.SnapshotOptions) (chunked.SnapshotResult, error) {
	if c.opts.ArtifactURL == "" {
		return chunked.SnapshotResult{}, ErrNoArtifactURL
	}
	return chunked.Snapshot(ctx, clientArtifactStore{clientArtifactReadStore{ctx: ctx, client: c}}, dir, opts)
}

// DownloadArtifactWithSize is DownloadArtifact plus the authenticated
// plaintext Content-Length. Chunked snapshot exporters use the size to apply
// manifest and segment bounds before consuming a remote object.
func (c *Client) DownloadArtifactWithSize(ctx context.Context, id string) (io.ReadCloser, int64, error) {
	if c.opts.ArtifactURL == "" {
		return nil, 0, ErrNoArtifactURL
	}
	if _, err := artifact.Digest(id); err != nil {
		return nil, 0, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.artifactURL(id), nil)
	if err != nil {
		return nil, 0, err
	}
	req.Header.Set("Authorization", "Bearer "+c.opts.Token)
	resp, err := c.httpClient().Do(req)
	if err != nil {
		return nil, 0, err
	}
	if resp.StatusCode != http.StatusOK {
		resp.Body.Close()
		return nil, 0, fmt.Errorf("client: download artifact %s: HTTP %d", id, resp.StatusCode)
	}
	if resp.ContentLength < 0 {
		resp.Body.Close()
		return nil, 0, fmt.Errorf("client: download artifact %s: missing Content-Length", id)
	}
	return &verifyingReader{body: resp.Body, want: id, h: sha256.New()}, resp.ContentLength, nil
}

type verifyingReader struct {
	body io.ReadCloser
	want string
	h    hash.Hash
}

func (v *verifyingReader) Read(p []byte) (int, error) {
	n, err := v.body.Read(p)
	if n > 0 {
		v.h.Write(p[:n])
	}
	if errors.Is(err, io.EOF) && artifact.ID(v.h.Sum(nil)) != v.want {
		return n, artifact.ErrDigestMismatch
	}
	return n, err
}

func (v *verifyingReader) Close() error { return v.body.Close() }

func (c *Client) artifactURL(id string) string {
	return strings.TrimSuffix(c.opts.ArtifactURL, "/") + "/" + id
}

func (c *Client) httpClient() *http.Client {
	if c.opts.HTTPClient != nil {
		return c.opts.HTTPClient
	}
	return http.DefaultClient
}

// Snapshot takes a live, crash-inconsistent snapshot; upload pushes it to the
// control-plane store but never makes it authoritative failover state.
func (c *Client) Snapshot(ctx context.Context, wsID string, upload bool, options ...OperationOption) (*proto.WSSnapshotRes, error) {
	var res proto.WSSnapshotRes
	idem, _ := operationKey(options)
	err := c.nodeCall(ctx, wsID, proto.OpWSSnapshot, func(g *proto.Grant) any {
		return proto.WSSnapshotReq{WS: wsID, Upload: upload, IdempotencyKey: idem, Grant: g}
	}, &res)
	return &res, err
}

// Checkpoint fences managed execution, takes and uploads a quiesced snapshot,
// and commits it as the workspace's authoritative failover state. On the
// local process backend, fencing terminates all workspace sessions.
func (c *Client) Checkpoint(ctx context.Context, wsID string, options ...OperationOption) (*proto.WSSnapshotRes, error) {
	var res proto.WSSnapshotRes
	idem, _ := operationKey(options)
	err := c.nodeCall(ctx, wsID, proto.OpWSSnapshot, func(g *proto.Grant) any {
		return proto.WSSnapshotReq{WS: wsID, Upload: true, Authoritative: true, IdempotencyKey: idem, Grant: g}
	}, &res)
	return &res, err
}

// WorkspaceInfo asks the node about a workspace.
func (c *Client) WorkspaceInfo(ctx context.Context, wsID string) (*proto.WSInfoRes, error) {
	var res proto.WSInfoRes
	err := c.nodeCall(ctx, wsID, proto.OpWSInfo, func(g *proto.Grant) any { return proto.WSGetReq{ID: wsID, Grant: g} }, &res)
	return &res, err
}

// ListSessions lists sessions on a workspace.
func (c *Client) ListSessions(ctx context.Context, wsID string) ([]proto.SessionStatus, error) {
	var res proto.SListRes
	err := c.nodeCall(ctx, wsID, proto.OpSList, func(g *proto.Grant) any { return proto.SListReq{WS: wsID, Grant: g} }, &res)
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

	mu           sync.Mutex
	next         uint64 // next seq expected
	pending      map[uint64]*proto.Frame
	pendingBytes int
	out          chan Chunk
	// unsubscribed records that this session was opened with NoSubscribe, so
	// no chunk stream will arrive and Wait must ask the node directly.
	unsubscribed bool
	in           chan *proto.Frame
	stop         chan struct{}
	stopOnce     sync.Once
	exit         *proto.ExitInfo
	err          error
	exited       chan struct{}
	closed       bool
	attached     bool
	attachMu     sync.Mutex
	inputMu      sync.Mutex
	iseq         uint64
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
		s.fail(err)
		return nil, err
	}
	s.mu.Lock()
	s.attached = true
	s.mu.Unlock()
	s.seedInputSeq(res.LastInputSeq)
	registered := c.register(res.S, s)
	if req.NoSubscribe {
		// The caller asked not to receive output and the node honoured it, so
		// reattaching here would subscribe anyway and quietly undo the request.
		// It also fills this session's delivery queue with chunks nobody reads,
		// which fails the session outright once the producer is fast enough.
		// Wait falls back to the s.wait operation, which is what that operation
		// exists for.
		registered.mu.Lock()
		registered.unsubscribed = true
		registered.mu.Unlock()
		return registered, nil
	}
	go registered.reattach(context.Background(), c.generation())
	return registered, nil
}

// OpenPort opens a TCP forward to a port inside the workspace.
func (c *Client) OpenPort(ctx context.Context, wsID string, port int, options ...OperationOption) (*Session, error) {
	var res proto.SOpenRes
	s := c.newSession("", wsID, proto.SessionPort)
	pendingID := "pending:" + ids.New("idem")
	c.mu.Lock()
	c.sessions[pendingID] = s
	c.mu.Unlock()
	idem, _ := operationKey(options)
	err := c.nodeCall(ctx, wsID, proto.OpPortOpen, func(g *proto.Grant) any {
		return proto.PortOpenReq{WS: wsID, Port: port, IdempotencyKey: idem, Grant: g}
	}, &res)
	c.mu.Lock()
	delete(c.sessions, pendingID)
	c.mu.Unlock()
	if err != nil {
		s.fail(err)
		return nil, err
	}
	s.mu.Lock()
	s.attached = true
	s.mu.Unlock()
	s.seedInputSeq(res.LastInputSeq)
	registered := c.register(res.S, s)
	go registered.reattach(context.Background(), c.generation())
	return registered, nil
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
		s.fail(err)
		return nil, err
	}
	s.mu.Lock()
	s.attached = true
	s.mu.Unlock()
	s.seedInputSeq(res.LastInputSeq)
	go s.reattach(context.Background(), c.generation())
	return s, nil
}

// register binds a session id to s and delivers any chunks that arrived
// early. If a live Session already exists for id (an idempotent re-open
// returned the same server session), that one is returned instead so there
// is exactly one cursor per (client, session).
func (c *Client) register(id string, s *Session) *Session {
	s.mu.Lock()
	s.ID = id
	s.mu.Unlock()
	c.mu.Lock()
	if existing, ok := c.sessions[id]; ok && existing != s {
		early := c.orphans[id]
		c.orphanCount -= len(early)
		if c.orphanCount < 0 {
			c.orphanCount = 0
		}
		delete(c.orphans, id)
		c.mu.Unlock()
		s.fail(proto.Err(proto.CodeClosed, "session cursor superseded by idempotent open"))
		for _, f := range early {
			existing.enqueue(f)
		}
		return existing
	}
	c.sessions[id] = s
	early := c.orphans[id]
	c.orphanCount -= len(early)
	if c.orphanCount < 0 {
		c.orphanCount = 0
	}
	delete(c.orphans, id)
	c.mu.Unlock()
	for _, f := range early {
		s.enqueue(f)
	}
	return s
}

func (c *Client) generation() uint64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.gen
}

func (c *Client) newSession(id, ws, kind string) *Session {
	s := &Session{
		c: c, ID: id, WS: ws, Kind: kind,
		pending: map[uint64]*proto.Frame{}, out: make(chan Chunk, 1024),
		in: make(chan *proto.Frame, 1024), stop: make(chan struct{}), exited: make(chan struct{}),
	}
	go s.runDelivery()
	return s
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

const (
	maxPendingChunks = 4096
	maxPendingBytes  = 16 << 20
)

// enqueue never waits for the application. A session-local delivery pump owns
// reordering and the public channel, so one slow consumer cannot stall the
// transport reader or unrelated sessions.
func (s *Session) enqueue(f *proto.Frame) {
	s.mu.Lock()
	closed := s.closed
	s.mu.Unlock()
	if closed {
		return
	}
	select {
	case s.in <- f:
	default:
		s.fail(proto.Err(proto.CodeResourceExhausted, "session delivery queue is full"))
	}
}

func (s *Session) runDelivery() {
	defer func() {
		s.mu.Lock()
		s.closed = true
		s.mu.Unlock()
		s.c.mu.Lock()
		for key, current := range s.c.sessions {
			if current == s {
				delete(s.c.sessions, key)
			}
		}
		s.c.mu.Unlock()
		close(s.out)
		close(s.exited)
	}()
	for {
		select {
		case <-s.stop:
			return
		case f := <-s.in:
			chunks, terminal, err := s.accept(f)
			if err != nil {
				s.fail(err)
				return
			}
			for _, chunk := range chunks {
				select {
				case s.out <- chunk:
				case <-s.stop:
					return
				}
			}
			if terminal {
				s.stopOnce.Do(func() { close(s.stop) })
				return
			}
		}
	}
}

// accept reorders one wire frame and returns the contiguous chunks now ready
// for delivery. It performs no channel send while holding s.mu.
func (s *Session) accept(f *proto.Frame) ([]Chunk, bool, error) {
	var body proto.ChunkBody
	if err := f.Decode(&body); err != nil {
		return nil, false, proto.Err(proto.CodeBadRequest, "decode session chunk: %v", err)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil, false, nil
	}
	var ready []Chunk
	if body.Stream == proto.StreamGap {
		var gap proto.Gap
		if err := proto.Unmarshal(body.Data, &gap); err != nil || gap.To < gap.From {
			return nil, false, proto.Err(proto.CodeBadRequest, "invalid session gap")
		}
		if gap.To+1 > s.next {
			s.next = gap.To + 1
			ready = append(ready, Chunk{Seq: f.Seq, Stream: body.Stream, Data: body.Data})
		}
		for seq, pending := range s.pending {
			if seq < s.next {
				s.pendingBytes -= len(pending.Body)
				delete(s.pending, seq)
			}
		}
	} else {
		if f.Seq < s.next {
			return nil, false, nil // duplicate from a re-attach
		}
		if f.Seq > s.next {
			if old := s.pending[f.Seq]; old == nil {
				s.pending[f.Seq] = f
				s.pendingBytes += len(f.Body)
			}
			if len(s.pending) > maxPendingChunks || s.pendingBytes > maxPendingBytes {
				return nil, false, proto.Err(proto.CodeResourceExhausted, "session reorder buffer is full")
			}
			return nil, false, nil
		}
		terminal, err := s.appendReadyLocked(f.Seq, body, &ready)
		if err != nil || terminal {
			return ready, terminal, err
		}
	}
	for {
		nf := s.pending[s.next]
		if nf == nil {
			break
		}
		delete(s.pending, s.next)
		s.pendingBytes -= len(nf.Body)
		var nextBody proto.ChunkBody
		if err := nf.Decode(&nextBody); err != nil {
			return nil, false, proto.Err(proto.CodeBadRequest, "decode buffered session chunk: %v", err)
		}
		terminal, err := s.appendReadyLocked(nf.Seq, nextBody, &ready)
		if err != nil || terminal {
			return ready, terminal, err
		}
	}
	return ready, false, nil
}

func (s *Session) appendReadyLocked(seq uint64, body proto.ChunkBody, ready *[]Chunk) (bool, error) {
	s.next = seq + 1
	*ready = append(*ready, Chunk{Seq: seq, Stream: body.Stream, Data: body.Data})
	if body.Stream != proto.StreamExit {
		return false, nil
	}
	var info proto.ExitInfo
	if err := proto.Unmarshal(body.Data, &info); err != nil {
		return false, proto.Err(proto.CodeBadRequest, "decode session exit: %v", err)
	}
	s.exit = &info
	s.closed = true
	return true, nil
}

func (s *Session) fail(err error) {
	s.mu.Lock()
	if s.err == nil && err != nil {
		s.err = err
	}
	s.closed = true
	s.mu.Unlock()
	s.stopOnce.Do(func() { close(s.stop) })
}

// Err reports why delivery ended without a normal exit record.
func (s *Session) Err() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.err
}

func (s *Session) seedInputSeq(seq uint64) {
	s.inputMu.Lock()
	if seq > s.iseq {
		s.iseq = seq
	}
	s.inputMu.Unlock()
}

// reattach re-subscribes after a reconnect from the last delivered seq.
func (s *Session) reattach(ctx context.Context, generation uint64) {
	s.attachMu.Lock()
	defer s.attachMu.Unlock()
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	backoff := 100 * time.Millisecond
	var lastErr error
	for {
		s.mu.Lock()
		if s.closed || !s.attached || s.ID == "" {
			s.mu.Unlock()
			return
		}
		from, id, ws := s.next, s.ID, s.WS
		s.mu.Unlock()
		var res proto.SOpenRes
		err := s.c.nodeCall(ctx, ws, proto.OpSAttach, func(g *proto.Grant) any {
			return proto.SAttachReq{S: id, From: from, Grant: g}
		}, &res)
		if err == nil {
			s.seedInputSeq(res.LastInputSeq)
			return
		}
		lastErr = err
		var pe *proto.Error
		if errors.As(err, &pe) && pe.Code == proto.CodeNotFound {
			s.fail(err)
			return
		}
		if s.c.generation() != generation {
			return // a newer connection installed its own reattach attempt
		}
		select {
		case <-ctx.Done():
			if lastErr == nil {
				lastErr = ctx.Err()
			}
			s.fail(fmt.Errorf("session reattach: %w", lastErr))
			return
		case <-s.stop:
			return
		case <-time.After(backoff):
			if backoff < 2*time.Second {
				backoff *= 2
			}
		}
	}
}

// Input sends bytes to the process (or socket). Retries are idempotent.
func (s *Session) Input(ctx context.Context, data []byte, eof bool) error {
	s.inputMu.Lock()
	defer s.inputMu.Unlock()
	s.mu.Lock()
	if s.closed {
		err := s.err
		if err == nil {
			err = proto.Err(proto.CodeClosed, "session closed")
		}
		s.mu.Unlock()
		return err
	}
	id, ws, iseq := s.ID, s.WS, s.iseq+1
	s.mu.Unlock()
	err := s.c.nodeCall(ctx, ws, proto.OpSInput, func(g *proto.Grant) any {
		return proto.SInputReq{S: id, ISeq: iseq, Data: data, EOF: eof, Grant: g}
	}, nil)
	if err == nil {
		s.iseq = iseq
	}
	return err
}

// Resize resizes a pty.
func (s *Session) Resize(ctx context.Context, rows, cols uint16) error {
	s.mu.Lock()
	id, ws := s.ID, s.WS
	s.mu.Unlock()
	return s.c.nodeCall(ctx, ws, proto.OpSResize, func(g *proto.Grant) any {
		return proto.SResizeReq{S: id, Rows: rows, Cols: cols, Grant: g}
	}, nil)
}

// Signal sends a signal by name.
func (s *Session) Signal(ctx context.Context, sig string) error {
	s.mu.Lock()
	id, ws := s.ID, s.WS
	s.mu.Unlock()
	return s.c.nodeCall(ctx, ws, proto.OpSSignal, func(g *proto.Grant) any {
		return proto.SSignalReq{S: id, Signal: sig, Grant: g}
	}, nil)
}

// Close detaches; kill also terminates the process.
func (s *Session) Close(ctx context.Context, kill bool) error {
	s.mu.Lock()
	id, ws := s.ID, s.WS
	s.mu.Unlock()
	s.fail(proto.Err(proto.CodeClosed, "session detached"))
	return s.c.nodeCall(ctx, ws, proto.OpSClose, func(g *proto.Grant) any {
		return proto.SCloseReq{S: id, Kill: kill, Grant: g}
	}, nil)
}

// Wait blocks until exit (server-side wait plus local delivery).
// waitRemote asks the node for the exit status instead of waiting for an exit
// chunk. A session opened with NoSubscribe has no chunk stream, so the stream
// is not a place its exit can arrive.
func (s *Session) waitRemote(ctx context.Context) (*proto.ExitInfo, error) {
	for {
		var res proto.SWaitRes
		err := s.c.nodeCall(ctx, s.WS, proto.OpSWait, func(g *proto.Grant) any {
			return proto.SWaitReq{S: s.ID, TimeoutSec: 30, Grant: g}
		}, &res)
		if err != nil {
			return nil, err
		}
		if res.Exited {
			if res.Exit == nil {
				return nil, proto.Err(proto.CodeClosed, "session closed without exit")
			}
			s.mu.Lock()
			if s.exit == nil {
				s.exit = res.Exit
			}
			s.closed = true
			exit := s.exit
			s.mu.Unlock()
			s.stopOnce.Do(func() { close(s.stop) })
			return exit, nil
		}
		// The node's own wait timed out rather than the session ending. Respect
		// the caller's deadline and ask again.
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}
	}
}

func (s *Session) Wait(ctx context.Context) (*proto.ExitInfo, error) {
	s.mu.Lock()
	unsubscribed := s.unsubscribed
	s.mu.Unlock()
	if unsubscribed {
		return s.waitRemote(ctx)
	}
	select {
	case <-s.exited:
		if exit := s.Exit(); exit != nil {
			return exit, nil
		}
		if err := s.Err(); err != nil {
			return nil, err
		}
		return nil, proto.Err(proto.CodeClosed, "session closed without exit")
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
				if !appendWithinLimit(&stdout, ch.Data, int64(len(stdout)+len(stderr)), c.opts.MaxRunOutputBytes) {
					killCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
					_ = s.Close(killCtx, true)
					cancel()
					return stdout, stderr, nil, proto.Err(proto.CodeResourceExhausted,
						"Run output exceeds %d bytes; use Session.Chunks for streaming", c.opts.MaxRunOutputBytes)
				}
			case proto.StreamStderr:
				if !appendWithinLimit(&stderr, ch.Data, int64(len(stdout)+len(stderr)), c.opts.MaxRunOutputBytes) {
					killCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
					_ = s.Close(killCtx, true)
					cancel()
					return stdout, stderr, nil, proto.Err(proto.CodeResourceExhausted,
						"Run output exceeds %d bytes; use Session.Chunks for streaming", c.opts.MaxRunOutputBytes)
				}
			case proto.StreamGap:
				var gap proto.Gap
				if decodeErr := proto.Unmarshal(ch.Data, &gap); decodeErr != nil || gap.To < gap.From {
					killCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
					_ = s.Close(killCtx, true)
					cancel()
					return stdout, stderr, nil, proto.Err(proto.CodeInternal, "Run received an invalid output gap")
				}
				killCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				_ = s.Close(killCtx, true)
				cancel()
				return stdout, stderr, nil, proto.Err(proto.CodeEvicted,
					"Run output sequence %d-%d is no longer available; use Session.Chunks for streaming", gap.From, gap.To)
			}
		case <-ctx.Done():
			killCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			_ = s.Close(killCtx, true)
			cancel()
			return stdout, stderr, nil, ctx.Err()
		}
	}
	exit = s.Exit()
	if exit == nil {
		if sessionErr := s.Err(); sessionErr != nil {
			return stdout, stderr, nil, sessionErr
		}
		return stdout, stderr, nil, fmt.Errorf("session ended without exit record")
	}
	return stdout, stderr, exit, nil
}

func appendWithinLimit(destination *[]byte, data []byte, current, limit int64) bool {
	if current < 0 || limit < 0 || current > limit || int64(len(data)) > limit-current {
		return false
	}
	*destination = append(*destination, data...)
	return true
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
