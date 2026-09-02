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
	"crypto/sha256"
	"crypto/x509"
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
	// MaxArtifactBytes bounds compressed snapshots and remote downloads.
	// Zero selects 8 GiB.
	MaxArtifactBytes int64
	Logger           *slog.Logger
	// Allow lists hosts every workspace on this node may reach without a credential.
	Allow []string
	// AllowPrivate lists hosts that may resolve to private addresses (local models).
	AllowPrivate []string
	// BrokerRootCAs augments trust for broker-reoriginated TLS (primarily
	// private providers and deterministic integration tests).
	BrokerRootCAs *x509.CertPool
	Version       string
	// Caps advertises extra capabilities (display, gpu …).
	Caps []string
	// MaxConcurrentRequests bounds request handlers independently of relay
	// connection count. Zero selects 128.
	MaxConcurrentRequests int
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
	materializing map[string]*materialization
	deadlines     map[string]time.Time    // monotonic local self-fence deadline
	quarantined   map[string]struct{}     // local bytes retained but never served
	grants        map[string]*proto.Grant // client|ws -> grant
	subs          map[string]*subscriber  // client|session -> active stream
	prepared      map[string]*preparedRelease
	committed     map[string]uint64 // idempotent release commits by workspace
	eventClaims   map[string]eventClaim

	mutationMu   sync.Mutex
	mutations    map[string]*mutationEntry
	mutationPath string

	requestMu      sync.Mutex
	requestSlots   chan struct{}
	overloadSlots  chan struct{}
	requestWG      sync.WaitGroup
	requestCtx     context.Context
	requestCancel  context.CancelFunc
	acceptRequests bool

	started  time.Time
	stop     chan struct{}
	stopOnce sync.Once
	wg       sync.WaitGroup
	online   chan struct{} // closed when first connected
	onceOn   sync.Once
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
	client  string
	ws      string
	session string
	cancel  context.CancelFunc
}

type preparedRelease struct {
	workspace *ws
	request   proto.WSReleaseReq
	response  proto.WSReleasedReq
	done      chan struct{}
	err       error
}

// eventClaim retains the authenticated assignment metadata needed to forward
// late node events after a workspace has been fenced or released locally.
// The control plane still verifies this tuple against durable assignment
// history; these values are hints, never authority by themselves.
type eventClaim struct {
	tenant     string
	generation uint64
}

type materialization struct {
	generation uint64
	deadline   time.Time
	cancel     context.CancelFunc
}

type mutationEntry struct {
	Fingerprint  [32]byte
	State        string
	Result       []byte
	CompletedAt  int64
	done         chan struct{}
	err          error
	needsPersist bool
}

type persistedMutation struct {
	Fingerprint []byte `cbor:"fingerprint"`
	State       string `cbor:"state,omitempty"`
	Result      []byte `cbor:"result"`
	CompletedAt int64  `cbor:"completed_at"`
}

const (
	mutationPending    = "pending"
	mutationCompleted  = "completed"
	maxMutationRecords = 10_000
)

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
	if opts.MaxArtifactBytes <= 0 {
		opts.MaxArtifactBytes = 8 << 30
	}
	if opts.MaxConcurrentRequests <= 0 {
		opts.MaxConcurrentRequests = 128
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
	mutationPath := filepath.Join(opts.DataDir, "mutations.cbor")
	mutations, err := loadMutations(mutationPath)
	if err != nil {
		return nil, err
	}
	requestCtx, requestCancel := context.WithCancel(context.Background())
	overloadLimit := opts.MaxConcurrentRequests / 8
	if overloadLimit < 8 {
		overloadLimit = 8
	}
	if overloadLimit > 64 {
		overloadLimit = 64
	}
	n := &Node{
		opts: opts, id: id, priv: priv, logger: opts.Logger.With("node", id),
		store: store, events: eventlog.New(eventlog.NewMemory(10000)),
		workspaces: map[string]*ws{}, materializing: map[string]*materialization{},
		deadlines: map[string]time.Time{}, quarantined: map[string]struct{}{},
		grants: map[string]*proto.Grant{}, subs: map[string]*subscriber{},
		prepared: map[string]*preparedRelease{}, committed: map[string]uint64{},
		eventClaims: map[string]eventClaim{},
		mutations:   mutations, mutationPath: mutationPath,
		requestSlots: make(chan struct{}, opts.MaxConcurrentRequests), overloadSlots: make(chan struct{}, overloadLimit),
		requestCtx: requestCtx, requestCancel: requestCancel, acceptRequests: true,
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

func loadMutations(path string) (map[string]*mutationEntry, error) {
	out := map[string]*mutationEntry{}
	b, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return out, nil
	}
	if err != nil {
		return nil, err
	}
	var stored map[string]persistedMutation
	if err := proto.Unmarshal(b, &stored); err != nil {
		return nil, fmt.Errorf("node: corrupt mutation journal: %w", err)
	}
	for key, record := range stored {
		if len(record.Fingerprint) != sha256.Size {
			return nil, fmt.Errorf("node: corrupt mutation journal fingerprint for %q", key)
		}
		state := record.State
		if state == "" {
			// Journals written before states were introduced contain only
			// completed records.
			state = mutationCompleted
		}
		if state != mutationPending && state != mutationCompleted {
			return nil, fmt.Errorf("node: corrupt mutation journal state %q for %q", state, key)
		}
		entry := &mutationEntry{
			State: state, Result: append([]byte(nil), record.Result...),
			CompletedAt: record.CompletedAt, done: make(chan struct{}),
		}
		copy(entry.Fingerprint[:], record.Fingerprint)
		if state == mutationPending {
			entry.err = proto.Err(proto.CodeConflict,
				"mutation outcome is unknown after node restart; automatic replay is refused")
		}
		close(entry.done)
		out[key] = entry
	}
	return out, nil
}

func (n *Node) persistMutationsLocked() error {
	stored := make(map[string]persistedMutation, len(n.mutations))
	for key, entry := range n.mutations {
		if entry.State != mutationPending && entry.State != mutationCompleted {
			return fmt.Errorf("invalid mutation state %q", entry.State)
		}
		stored[key] = persistedMutation{
			Fingerprint: append([]byte(nil), entry.Fingerprint[:]...), State: entry.State,
			Result: append([]byte(nil), entry.Result...), CompletedAt: entry.CompletedAt,
		}
	}
	b, err := proto.Marshal(stored)
	if err != nil {
		return err
	}
	tmp, err := os.CreateTemp(filepath.Dir(n.mutationPath), ".mutations-*")
	if err != nil {
		return err
	}
	tmpName := tmp.Name()
	defer os.Remove(tmpName)
	if err := tmp.Chmod(0o600); err == nil {
		_, err = tmp.Write(b)
	}
	if err == nil {
		err = tmp.Sync()
	}
	if closeErr := tmp.Close(); err == nil {
		err = closeErr
	}
	if err == nil {
		err = os.Rename(tmpName, n.mutationPath)
	}
	if err == nil {
		err = syncParentDir(filepath.Dir(n.mutationPath))
	}
	if err == nil {
		for _, entry := range n.mutations {
			if entry.needsPersist {
				entry.needsPersist = false
				if entry.State == mutationCompleted {
					entry.err = nil
				}
			}
		}
	}
	return err
}

func (n *Node) runMutation(ctx context.Context, key string, request any, apply func() ([]byte, error)) ([]byte, error) {
	if key == "" {
		return apply()
	}
	fingerprint := sha256.Sum256(proto.MustMarshal(request))
	n.mutationMu.Lock()
	if existing := n.mutations[key]; existing != nil {
		if existing.Fingerprint != fingerprint {
			n.mutationMu.Unlock()
			return nil, proto.Err(proto.CodeConflict, "idempotency key was reused with different arguments")
		}
		done := existing.done
		n.mutationMu.Unlock()
		select {
		case <-done:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
		n.mutationMu.Lock()
		if existing.State == mutationCompleted && existing.needsPersist {
			if err := n.persistMutationsLocked(); err != nil {
				existing.err = proto.Err(proto.CodeInternal,
					"mutation completed but its result is not durable: %v", err)
			}
		}
		result, err := append([]byte(nil), existing.Result...), existing.err
		n.mutationMu.Unlock()
		if err != nil {
			return nil, err
		}
		return result, err
	}
	if len(n.mutations) >= maxMutationRecords {
		n.mutationMu.Unlock()
		return nil, proto.Err(proto.CodeResourceExhausted,
			"node mutation journal contains %d records", maxMutationRecords)
	}
	entry := &mutationEntry{
		Fingerprint: fingerprint, State: mutationPending, CompletedAt: time.Now().UnixMilli(),
		done: make(chan struct{}), needsPersist: true,
	}
	n.mutations[key] = entry
	if err := n.persistMutationsLocked(); err != nil {
		delete(n.mutations, key)
		entry.err = proto.Err(proto.CodeInternal, "persist mutation intent before execution: %v", err)
		close(entry.done)
		n.mutationMu.Unlock()
		return nil, entry.err
	}
	n.mutationMu.Unlock()

	result, err := apply()
	n.mutationMu.Lock()
	if err != nil {
		// Failed operations are retryable only after removing the durable
		// intent. If that removal cannot be committed, retain an ambiguous
		// pending record so a restart cannot repeat a possibly partial effect.
		delete(n.mutations, key)
		if persistErr := n.persistMutationsLocked(); persistErr != nil {
			n.mutations[key] = entry
			entry.err = proto.Err(proto.CodeInternal,
				"mutation failed and its durable intent could not be cleared; outcome is ambiguous: %v", persistErr)
		} else {
			entry.err = err
		}
		close(entry.done)
		n.mutationMu.Unlock()
		return nil, entry.err
	}
	entry.State = mutationCompleted
	entry.Result = append([]byte(nil), result...)
	entry.CompletedAt = time.Now().UnixMilli()
	entry.needsPersist = true
	if persistErr := n.persistMutationsLocked(); persistErr != nil {
		entry.err = proto.Err(proto.CodeInternal,
			"mutation completed but its result could not be durably recorded: %v", persistErr)
	}
	close(entry.done)
	result, err = append([]byte(nil), entry.Result...), entry.err
	n.mutationMu.Unlock()
	if err != nil {
		return nil, err
	}
	return result, err
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
		return "", nil, fmt.Errorf("node: identity file %s is corrupt; refusing to replace enrolled identity", path)
	}
	if !errors.Is(err, os.ErrNotExist) {
		return "", nil, err
	}
	_, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		return "", nil, err
	}
	f := identityFile{ID: ids.New("n"), Priv: priv}
	b, _ = json.Marshal(f)
	tmp, err := os.CreateTemp(filepath.Dir(path), ".identity-*")
	if err != nil {
		return "", nil, err
	}
	tmpName := tmp.Name()
	defer os.Remove(tmpName)
	if err := tmp.Chmod(0o600); err == nil {
		_, err = tmp.Write(b)
	}
	if err == nil {
		err = tmp.Sync()
	}
	if closeErr := tmp.Close(); err == nil {
		err = closeErr
	}
	if err == nil {
		err = os.Rename(tmpName, path)
	}
	if err != nil {
		return "", nil, err
	}
	return f.ID, priv, nil
}

// emit records an event locally and forwards it to the control plane.
func (n *Node) emit(typ, stream, principal string, payload any) {
	e := &proto.Event{Type: typ, Stream: stream, Principal: principal, Node: n.id, Origin: "node", Actor: n.id}
	if payload != nil {
		e.Payload = proto.MustMarshal(payload)
	}
	if strings.HasPrefix(stream, "ws_") {
		n.mu.Lock()
		claim := n.eventClaims[stream]
		n.mu.Unlock()
		e.Workspace, e.Tenant, e.Generation = stream, claim.tenant, claim.generation
	}
	if err := n.events.Append(context.Background(), e); err != nil {
		n.logger.Error("append node event", "type", typ, "workspace", stream, "err", err)
	}
}

// ---------------------------------------------------------------------------
// uplink
// ---------------------------------------------------------------------------

// Run connects (and reconnects) to the relay until ctx ends.
func (n *Node) Run(ctx context.Context) error {
	n.wg.Add(3)
	go n.renewLoop(ctx)
	go n.fenceLoop(ctx)
	go n.eventLoop(ctx)
	defer n.wg.Wait()
	defer n.shutdown()
	backoff := 100 * time.Millisecond
	for {
		connectedAt := time.Now()
		err := n.connectOnce(ctx)
		if ctx.Err() != nil {
			return nil
		}
		// A connection that remained healthy for a while starts a fresh retry
		// epoch. Without this reset, unrelated flaps days apart eventually wait
		// the full cap and unnecessarily cross local lease safety deadlines.
		if time.Since(connectedAt) >= 30*time.Second {
			backoff = 100 * time.Millisecond
		}
		delay := backoff
		n.mu.Lock()
		holding := len(n.workspaces) > 0 || len(n.materializing) > 0
		if holding {
			maxDelay := n.localLeaseWindowLocked() / 4
			if maxDelay < 25*time.Millisecond {
				maxDelay = 25 * time.Millisecond
			}
			if delay > maxDelay {
				delay = maxDelay
			}
		}
		n.mu.Unlock()
		n.logger.Warn("uplink lost; reconnecting", "err", err, "backoff", delay)
		select {
		case <-time.After(delay):
		case <-ctx.Done():
			return nil
		}
		backoff *= 2
		if backoff > 30*time.Second {
			backoff = 30 * time.Second
		}
	}
}

// eventLoop is the node event outbox. One ordered producer retains an
// unacknowledged batch across reconnects; control deduplicates retries using
// the node log's sequence number.
func (n *Node) eventLoop(ctx context.Context) {
	defer n.wg.Done()
	sub := n.events.Subscribe(1, "")
	defer sub.Close()
	var pending []proto.Event
	for {
		if len(pending) == 0 {
			events, err := sub.Next(ctx)
			if err != nil {
				return
			}
			pending = events
		}
		n.mu.Lock()
		p := n.peer
		n.mu.Unlock()
		if p != nil {
			cctx, cancel := context.WithTimeout(ctx, 10*time.Second)
			err := p.Call(cctx, proto.PeerControl, proto.OpEventsPost, proto.EventPost{Events: pending}, nil)
			cancel()
			if err == nil {
				pending = nil
				continue
			}
		}
		select {
		case <-ctx.Done():
			return
		case <-n.stop:
			return
		case <-time.After(100 * time.Millisecond):
		}
	}
}

func (n *Node) shutdown() {
	n.requestMu.Lock()
	n.acceptRequests = false
	n.requestMu.Unlock()
	n.requestCancel()
	n.stopOnce.Do(func() { close(n.stop) })
	n.requestWG.Wait()
	n.sessions.Close()
	n.mu.Lock()
	for _, w := range n.workspaces {
		if w.broker != nil {
			w.broker.Close()
		}
		_ = w.handle.FS().Close()
	}
	for _, prepared := range n.prepared {
		_ = prepared.workspace.handle.FS().Close()
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
	info := workspace.HostInfoForRegistry(n.opts.Backends)
	info.Version = n.opts.Version
	info.Caps = n.opts.Caps
	hello.Node = &info
	hello.IssuedAt = time.Now().UnixMilli()
	hello.Nonce = make([]byte, 32)
	if _, err := rand.Read(hello.Nonce); err != nil {
		return err
	}
	hello.Proof = ed25519.Sign(n.priv, proto.HelloProofBytes(hello))
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

// fenceLoop is deliberately independent of renewal I/O. A wedged control RPC
// must not postpone the local safety deadline it is supposed to enforce.
func (n *Node) fenceLoop(ctx context.Context) {
	defer n.wg.Done()
	t := time.NewTicker(100 * time.Millisecond)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-n.stop:
			return
		case now := <-t.C:
			n.fenceExpired(ctx, now)
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
	for id, materializing := range n.materializing {
		if _, done := n.workspaces[id]; !done && materializing.generation != 0 {
			req.IDs = append(req.IDs, id)
			req.Gen[id] = materializing.generation
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
		var res proto.WSRenewRes
		rctx, cancel := context.WithTimeout(ctx, 10*time.Second)
		if err := p.Call(rctx, proto.PeerControl, proto.OpWSRenew, req, &res); err != nil {
			n.logger.Warn("renew failed", "err", err)
		} else {
			n.applyRenewResults(req, &res)
		}
		cancel()
	}
	for _, w := range refresh {
		n.refreshLeases(ctx, w)
	}
}

func (n *Node) applyRenewResults(req proto.WSRenewReq, res *proto.WSRenewRes) {
	results := make(map[string]proto.WSRenewResult, len(res.Results))
	for _, result := range res.Results {
		results[result.ID] = result
	}
	for _, id := range req.IDs {
		result, ok := results[id]
		if !ok || !result.Accepted || result.Generation != req.Gen[id] {
			reason := "renewal result missing"
			if ok {
				reason = fmt.Sprintf("renewal rejected: action=%s authoritative_gen=%d", result.Action, result.AuthoritativeGen)
			}
			n.fenceWorkspace(context.Background(), id, reason)
			continue
		}
		deadline := time.Now().Add(n.localLeaseWindow())
		n.mu.Lock()
		if w := n.workspaces[id]; w != nil && w.Generation == result.Generation {
			n.deadlines[id] = deadline
			w.LeaseUntil = result.LeaseUntil
		}
		if materializing := n.materializing[id]; materializing != nil && materializing.generation == result.Generation {
			materializing.deadline = deadline
		}
		n.mu.Unlock()
	}
}

func (n *Node) localLeaseWindow() time.Duration {
	n.mu.Lock()
	d := n.localLeaseWindowLocked()
	n.mu.Unlock()
	return d
}

func (n *Node) localLeaseWindowLocked() time.Duration {
	seconds := n.leaseSec
	if seconds <= 0 {
		seconds = 30
	}
	// Fence with one third of the authoritative lease still remaining.
	return time.Duration(seconds) * time.Second * 2 / 3
}

func (n *Node) fenceExpired(ctx context.Context, now time.Time) {
	n.mu.Lock()
	var expired []string
	for id, deadline := range n.deadlines {
		if !deadline.IsZero() && !now.Before(deadline) {
			expired = append(expired, id)
		}
	}
	for id, materializing := range n.materializing {
		if !materializing.deadline.IsZero() && !now.Before(materializing.deadline) {
			expired = append(expired, id)
		}
	}
	n.mu.Unlock()
	for _, id := range expired {
		n.fenceWorkspace(ctx, id, "local lease safety deadline elapsed")
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
// fenced and retained for reconciliation.
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
			n.mu.Lock()
			if current := n.workspaces[w.ID]; current == w && current.Generation == w.Generation {
				n.deadlines[w.ID] = time.Now().Add(n.localLeaseWindowLocked())
			}
			n.mu.Unlock()
			continue
		}
		var pe *proto.Error
		if errors.As(err, &pe) && (pe.Code == proto.CodeConflict || pe.Code == proto.CodeNotFound) {
			n.logger.Warn("fencing workspace pending reconciliation", "ws", w.ID, "reason", pe.Code)
			n.fenceWorkspace(ctx, w.ID, "ready rejected during reconnect: "+pe.Code)
			go n.tryClaim(context.WithoutCancel(ctx), w.ID, true)
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

// fenceWorkspace stops all execution and egress but preserves the filesystem.
// Authority disagreement is not permission to delete the only current copy.
func (n *Node) fenceWorkspace(ctx context.Context, id, reason string) {
	n.mu.Lock()
	w := n.workspaces[id]
	materializing := n.materializing[id]
	if w == nil && materializing == nil {
		n.mu.Unlock()
		return
	}
	delete(n.workspaces, id)
	delete(n.deadlines, id)
	n.quarantined[id] = struct{}{}
	if materializing != nil {
		materializing.cancel()
	}
	for k, s := range n.subs {
		if s.ws == id {
			s.cancel()
			delete(n.subs, k)
		}
	}
	n.mu.Unlock()
	n.sessions.KillWorkspace(id)
	if w != nil {
		if w.broker != nil {
			w.broker.Close()
		}
		_ = w.handle.FS().Close()
	}
	n.logger.Warn("workspace fenced; local filesystem retained", "ws", id, "reason", reason)
	n.emit(proto.EvWSFenced, id, "", map[string]any{"reason": reason})
}

// quarantineMaterialization makes a partially prepared filesystem inert while
// retaining its bytes for reconciliation. Setup failure is not proof that the
// local tree is disposable: it may be the only copy left after a restart.
func (n *Node) quarantineMaterialization(id string, handle workspace.Handle, b *broker.Broker, reason string) {
	if b != nil {
		b.Close()
	}
	if handle != nil {
		_ = handle.FS().Close()
	}
	n.mu.Lock()
	n.quarantined[id] = struct{}{}
	n.mu.Unlock()
	n.logger.Warn("workspace materialization quarantined; local filesystem retained", "ws", id, "reason", reason)
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
		n.scheduleRequest(ctx, p, f)
	}
}

func (n *Node) scheduleRequest(ctx context.Context, p *transport.Peer, f *proto.Frame) {
	n.requestMu.Lock()
	if !n.acceptRequests {
		n.requestMu.Unlock()
		return
	}
	select {
	case n.requestSlots <- struct{}{}:
		n.requestWG.Add(1)
		n.requestMu.Unlock()
		metrics.NodeRequestsActive.Add(1)
		go func() {
			defer func() {
				metrics.NodeRequestsActive.Add(-1)
				<-n.requestSlots
				n.requestWG.Done()
			}()
			n.handleReq(n.requestCtx, p, f)
		}()
		return
	default:
		metrics.NodeRequestsRejected.Inc()
	}
	select {
	case n.overloadSlots <- struct{}{}:
		n.requestWG.Add(1)
		n.requestMu.Unlock()
		go func() {
			defer func() {
				<-n.overloadSlots
				n.requestWG.Done()
			}()
			rctx, cancel := context.WithTimeout(n.requestCtx, time.Second)
			defer cancel()
			if err := p.RespondErr(rctx, f,
				proto.Err(proto.CodeResourceExhausted, "node request capacity exhausted")); err != nil {
				_ = p.Close()
			}
		}()
	default:
		n.requestMu.Unlock()
		// The peer is producing requests faster than even bounded rejection
		// responses can be written. Disconnect it to release all associated
		// transport state without allocating another goroutine.
		_ = p.Close()
	}
	_ = ctx // inbound transport contexts do not carry a remote deadline
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
		if s.client == client {
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
	if g.Claims.Tenant != w.Tenant || g.Claims.AuthzRevision != w.AuthzRevision {
		return nil, proto.Err(proto.CodeUnauthorized, "grant authorization revision or tenant is stale")
	}
	n.grants[key] = g
	return w, nil
}

// mutationKey scopes caller-selected keys to the authenticated subject,
// tenant, workspace and operation. The relay peer id is deliberately not in
// the durable key: it changes across client processes, while the subject does
// not. authorize has already verified and cached this grant.
func (n *Node) mutationKey(client, wsID, op, key string) string {
	if key == "" {
		return ""
	}
	n.mu.Lock()
	g := n.grants[client+"|"+wsID]
	n.mu.Unlock()
	principal, tenant := client, ""
	if g != nil {
		principal, tenant = g.Claims.Principal, g.Claims.Tenant
	}
	sum := sha256.Sum256(proto.MustMarshal(struct {
		Tenant, Principal, Workspace, Operation, Key string
	}{tenant, principal, wsID, op, key}))
	return fmt.Sprintf("mutation:%x", sum[:])
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
		case proto.OpWSReleaseCommit:
			req, err := decode[proto.WSReleaseCommitReq](f)
			if err != nil {
				return nil, err
			}
			return struct{}{}, n.releaseCommit(ctx, req)
		case proto.OpWSReleaseAbort:
			req, err := decode[proto.WSReleaseCommitReq](f)
			if err != nil {
				return nil, err
			}
			return struct{}{}, n.releaseAbort(req)
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
		return proto.SOpenRes{S: s.ID, Next: s.Log.Next(), LastInputSeq: s.LastInputSeq()}, nil
	case proto.OpSInput:
		req, err := decode[proto.SInputReq](f)
		if err != nil {
			return nil, err
		}
		s, err := n.sessionFor(f.From, req.S, req.Grant)
		if err != nil {
			return nil, err
		}
		return struct{}{}, s.Input(req.ISeq, req.Data, req.EOF)
	case proto.OpSResize:
		req, err := decode[proto.SResizeReq](f)
		if err != nil {
			return nil, err
		}
		s, err := n.sessionFor(f.From, req.S, req.Grant)
		if err != nil {
			return nil, err
		}
		return struct{}{}, s.Resize(req.Rows, req.Cols)
	case proto.OpSSignal:
		req, err := decode[proto.SSignalReq](f)
		if err != nil {
			return nil, err
		}
		s, err := n.sessionFor(f.From, req.S, req.Grant)
		if err != nil {
			return nil, err
		}
		return struct{}{}, s.Signal(req.Signal)
	case proto.OpSAck:
		req, err := decode[proto.SAckReq](f)
		if err != nil {
			return nil, err
		}
		if _, err := n.sessionFor(f.From, req.S, req.Grant); err != nil {
			return nil, err
		}
		return struct{}{}, nil // liveness only in v0; cursors are per-subscriber
	case proto.OpSClose:
		req, err := decode[proto.SCloseReq](f)
		if err != nil {
			return nil, err
		}
		s, err := n.sessionFor(f.From, req.S, req.Grant)
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
		s, err := n.sessionFor(f.From, req.S, req.Grant)
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
			if _, err := n.authorize(f.From, req.WS, req.Grant); err != nil {
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
		clean := *req
		clean.Grant = nil
		key := n.mutationKey(f.From, w.ID, proto.OpFSWrite, req.IdempotencyKey)
		_, err = n.runMutation(ctx, key, clean, func() ([]byte, error) {
			if err := w.handle.FS().Write(req.Path, req.Data, req.Mode, req.Append, req.MkdirP); err != nil {
				return nil, err
			}
			n.emit(proto.EvFSWrite, w.ID, w.Spec.Principal, map[string]any{"path": req.Path, "bytes": len(req.Data), "client": f.From})
			return proto.Marshal(struct{}{})
		})
		return struct{}{}, err
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
		clean := *req
		clean.Grant = nil
		key := n.mutationKey(f.From, w.ID, proto.OpFSMkdir, req.IdempotencyKey)
		_, err = n.runMutation(ctx, key, clean, func() ([]byte, error) {
			if err := w.handle.FS().Mkdir(req.Path); err != nil {
				return nil, err
			}
			n.emit(proto.EvFSMkdir, w.ID, w.Spec.Principal, map[string]any{"path": req.Path, "client": f.From})
			return proto.Marshal(struct{}{})
		})
		return struct{}{}, err
	case proto.OpFSRemove:
		req, err := decode[proto.FSRemoveReq](f)
		if err != nil {
			return nil, err
		}
		w, err := n.authorize(f.From, req.WS, req.Grant)
		if err != nil {
			return nil, err
		}
		clean := *req
		clean.Grant = nil
		key := n.mutationKey(f.From, w.ID, proto.OpFSRemove, req.IdempotencyKey)
		_, err = n.runMutation(ctx, key, clean, func() ([]byte, error) {
			if err := w.handle.FS().Remove(req.Path, req.Recursive); err != nil {
				return nil, err
			}
			n.emit(proto.EvFSRemove, w.ID, w.Spec.Principal, map[string]any{"path": req.Path, "client": f.From})
			return proto.Marshal(struct{}{})
		})
		return struct{}{}, err
	case proto.OpFSRename:
		req, err := decode[proto.FSRenameReq](f)
		if err != nil {
			return nil, err
		}
		w, err := n.authorize(f.From, req.WS, req.Grant)
		if err != nil {
			return nil, err
		}
		clean := *req
		clean.Grant = nil
		key := n.mutationKey(f.From, w.ID, proto.OpFSRename, req.IdempotencyKey)
		_, err = n.runMutation(ctx, key, clean, func() ([]byte, error) {
			if err := w.handle.FS().Rename(req.From, req.To); err != nil {
				return nil, err
			}
			n.emit(proto.EvFSRename, w.ID, w.Spec.Principal,
				map[string]any{"from": req.From, "to": req.To, "client": f.From})
			return proto.Marshal(struct{}{})
		})
		return struct{}{}, err
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
		clean := *req
		clean.Grant = nil
		key := n.mutationKey(f.From, w.ID, proto.OpFSEdit, req.IdempotencyKey)
		raw, err := n.runMutation(ctx, key, clean, func() ([]byte, error) {
			nrep, err := w.handle.FS().Edit(req.Path, req.Edits)
			if err != nil {
				return nil, err
			}
			n.emit(proto.EvFSEdit, w.ID, w.Spec.Principal, map[string]any{"path": req.Path, "replacements": nrep, "client": f.From})
			return proto.Marshal(proto.FSEditRes{Replacements: nrep})
		})
		if err != nil {
			return nil, err
		}
		var result proto.FSEditRes
		if err := proto.Unmarshal(raw, &result); err != nil {
			return nil, err
		}
		return result, nil
	case proto.OpWSSnapshot:
		req, err := decode[proto.WSSnapshotReq](f)
		if err != nil {
			return nil, err
		}
		w, err := n.authorize(f.From, req.WS, req.Grant)
		if err != nil {
			return nil, err
		}
		clean := *req
		clean.Grant = nil
		key := n.mutationKey(f.From, w.ID, proto.OpWSSnapshot, req.IdempotencyKey)
		raw, err := n.runMutation(ctx, key, clean, func() ([]byte, error) {
			id, size, err := n.snapshot(ctx, w, req.Upload)
			if err != nil {
				return nil, err
			}
			if req.Upload {
				cctx, cancel := context.WithTimeout(ctx, 30*time.Second)
				err = p.Call(cctx, proto.PeerControl, proto.OpWSSnapshotCommit, proto.WSSnapshotCommitReq{ID: w.ID, Gen: w.Generation, Snapshot: id}, nil)
				cancel()
				if err != nil {
					return nil, fmt.Errorf("commit snapshot: %w", err)
				}
			}
			return proto.Marshal(proto.WSSnapshotRes{Artifact: id, Bytes: size})
		})
		if err != nil {
			return nil, err
		}
		var result proto.WSSnapshotRes
		if err := proto.Unmarshal(raw, &result); err != nil {
			return nil, err
		}
		return result, nil
	case proto.OpWSInfo:
		req, err := decode[proto.WSGetReq](f)
		if err != nil {
			return nil, err
		}
		w, err := n.authorize(f.From, req.ID, req.Grant)
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
	info := workspace.HostInfoForRegistry(n.opts.Backends)
	info.Version = n.opts.Version
	info.Caps = append([]string(nil), n.opts.Caps...)
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
		spec.IdempotencyKey = client + "|" + w.ID + "|" + req.IdempotencyKey
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
	return proto.SOpenRes{S: s.ID, Next: s.Log.Next(), LastInputSeq: s.LastInputSeq()}, nil
}

func (n *Node) portOpen(ctx context.Context, p *transport.Peer, client string, w *ws, req *proto.PortOpenReq) (any, error) {
	if req.Host != "" {
		return nil, proto.Err(proto.CodeDenied, "port.open host is backend-controlled")
	}
	if req.Port < 1 || req.Port > 65535 {
		return nil, proto.Err(proto.CodeBadRequest, "port must be between 1 and 65535")
	}
	spec := session.Spec{WS: w.ID, Kind: proto.SessionPort, Host: req.Host, Port: req.Port, Principal: w.Spec.Principal}
	if req.IdempotencyKey != "" {
		spec.IdempotencyKey = client + "|" + w.ID + "|port|" + req.IdempotencyKey
	}
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
	return proto.SOpenRes{S: s.ID, Next: s.Log.Next(), LastInputSeq: s.LastInputSeq()}, nil
}

// subscribe streams s's log to client from seq `from` until the client
// detaches, the connection dies, or the log ends. One stream per
// (client, session): a re-attach replaces the previous cursor.
func (n *Node) subscribe(p *transport.Peer, client string, s *session.Session, from uint64) {
	key := client + "|" + s.ID
	ctx, cancel := context.WithCancel(context.Background())
	sub := &subscriber{client: client, ws: s.WS, session: s.ID, cancel: cancel}
	n.mu.Lock()
	if old, ok := n.subs[key]; ok {
		old.cancel()
	}
	n.subs[key] = sub
	n.mu.Unlock()
	go func() {
		defer func() {
			n.mu.Lock()
			if n.subs[key] == sub {
				delete(n.subs, key)
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
	_, prepared := n.prepared[wsID]
	_, retained := n.quarantined[wsID]
	if have || busy || prepared || p == nil {
		n.mu.Unlock()
		return
	}
	adopt = adopt || retained
	mctx, materializeCancel := context.WithCancel(ctx)
	materializing := &materialization{cancel: materializeCancel}
	n.materializing[wsID] = materializing
	n.mu.Unlock()
	defer func() {
		materializeCancel()
		n.mu.Lock()
		if n.materializing[wsID] == materializing {
			delete(n.materializing, wsID)
		}
		n.mu.Unlock()
	}()
	var res proto.WSClaimRes
	cctx, cancel := context.WithTimeout(mctx, 15*time.Second)
	err := p.Call(cctx, proto.PeerControl, proto.OpWSClaim, proto.WSClaimReq{ID: wsID}, &res)
	cancel()
	if err != nil {
		if adopt {
			// Authority disagreement fences the local copy; it does not prove
			// those bytes are obsolete or authorize their destruction.
			var pe *proto.Error
			if errors.As(err, &pe) && (pe.Code == proto.CodeNotFound || pe.Code == proto.CodeConflict) {
				n.mu.Lock()
				n.quarantined[wsID] = struct{}{}
				n.mu.Unlock()
				n.logger.Warn("local workspace retained in quarantine", "ws", wsID, "reason", pe.Code)
			}
		}
		return
	}
	n.mu.Lock()
	materializing.generation = res.Workspace.Generation
	materializing.deadline = time.Now().Add(n.localLeaseWindowLocked())
	n.eventClaims[wsID] = eventClaim{tenant: res.Workspace.Tenant, generation: res.Workspace.Generation}
	n.mu.Unlock()
	if err := n.materialize(mctx, res.Workspace, adopt); err != nil {
		n.logger.Error("materialize failed; releasing", "ws", wsID, "err", err)
		_ = p.Call(ctx, proto.PeerControl, proto.OpWSReleased, proto.WSReleasedReq{ID: wsID, Gen: res.Workspace.Generation, Reason: "materialize failed: " + err.Error()}, nil)
	}
}

func (n *Node) materialize(ctx context.Context, w proto.Workspace, adopt bool) error {
	be, err := n.opts.Backends.Get(w.Spec.Requires.Backend)
	if err != nil {
		return err
	}
	descriptor, err := n.opts.Backends.Descriptor(be.Name())
	if err != nil {
		return err
	}
	if err := proto.ValidateBackendSecurity(w.Spec.Security, descriptor); err != nil {
		return proto.Err(proto.CodeDenied, "backend %s no longer satisfies workspace security policy: %v", be.Name(), err)
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
			// A local tree may contain bytes newer than the last control-plane
			// snapshot. Adopt it; never destroy it merely because a restore was
			// also named.
			handle, err = be.Adopt(ctx, w.ID)
		}
		if err != nil {
			return err
		}
	}
	entry := &ws{Workspace: w, handle: handle}
	retainOnError := func(err error) error {
		n.quarantineMaterialization(w.ID, handle, entry.broker, err.Error())
		return err
	}
	if err := ctx.Err(); err != nil {
		return retainOnError(err)
	}
	// Broker: one per workspace, always on, so every session has an egress path.
	var leases []proto.BindingLease
	if len(w.Spec.Bindings) > 0 {
		var res proto.BindingLeaseRes
		n.mu.Lock()
		p := n.peer
		n.mu.Unlock()
		if p == nil {
			return retainOnError(proto.Err(proto.CodeUnreachable, "control connection lost before binding lease"))
		}
		lctx, cancel := context.WithTimeout(ctx, 10*time.Second)
		err := p.Call(lctx, proto.PeerControl, proto.OpBindingLease, proto.BindingLeaseReq{WS: w.ID}, &res)
		cancel()
		if err != nil {
			return retainOnError(fmt.Errorf("binding lease: %w", err))
		}
		leases = res.Leases
	}
	entry.leases = leases
	brokerOpts := broker.Options{
		WS: w.ID, Principal: w.Spec.Principal, Leases: leases, Allow: n.opts.Allow, AllowPrivate: n.opts.AllowPrivate,
		RootCAs: n.opts.BrokerRootCAs,
		Audit: func(a broker.Audit) {
			typ := proto.EvEgressAllowed
			switch a.Decision {
			case broker.DecisionSubstituted:
				typ = proto.EvCredUsed
			case broker.DecisionDenied, broker.DecisionLeakBlocked, broker.DecisionExpired, broker.DecisionUnauthenticated:
				typ = proto.EvEgressDenied
			}
			n.emit(typ, a.WS, a.Principal, map[string]any{"decision": a.Decision, "binding": a.Binding, "host": a.Host, "method": a.Method, "path": a.Path, "reason": a.Reason, "status": a.Status})
		},
	}
	if handle.Backend() == "docker" {
		// Containers cannot reach a host loopback listener. The random
		// per-workspace capability authenticates this host-gateway listener.
		brokerOpts.Listen = "0.0.0.0:0"
		brokerOpts.AdvertiseHost = "host.docker.internal"
	}
	entry.broker = broker.New(brokerOpts)
	if _, err := entry.broker.Start(); err != nil {
		return retainOnError(err)
	}
	// Drop a sourceable env file into the workspace. The broker's address
	// changes every time a workspace is materialized, so anything that bakes
	// it into a config file goes stale after a move. Reading this file at
	// start-up is the portable way to find it.
	if err := writeWorkspaceEnv(handle, entry); err != nil {
		return retainOnError(fmt.Errorf("write workspace environment: %w", err))
	}
	n.mu.Lock()
	materializing := n.materializing[w.ID]
	if materializing == nil || materializing.generation != w.Generation || ctx.Err() != nil {
		n.mu.Unlock()
		return retainOnError(proto.Err(proto.CodeConflict, "claim expired while workspace was materializing"))
	}
	deadline := materializing.deadline
	if deadline.IsZero() {
		deadline = time.Now().Add(n.localLeaseWindowLocked())
	}
	n.workspaces[w.ID] = entry
	n.deadlines[w.ID] = deadline
	delete(n.quarantined, w.ID)
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
	if p == nil {
		n.fenceWorkspace(ctx, w.ID, "control connection lost before ws.ready")
		return proto.Err(proto.CodeUnreachable, "control connection lost before ws.ready")
	}
	rctx, cancel := context.WithTimeout(ctx, 15*time.Second)
	err = p.Call(rctx, proto.PeerControl, proto.OpWSReady, proto.WSReadyReq{ID: w.ID, Gen: w.Generation}, nil)
	cancel()
	if err != nil {
		n.fenceWorkspace(ctx, w.ID, "ws.ready rejected: "+err.Error())
		return fmt.Errorf("ws.ready: %w", err)
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
	if resp.ContentLength > n.opts.MaxArtifactBytes {
		resp.Body.Close()
		return nil, fmt.Errorf("artifact %s: %w", id, artifact.ErrTooLarge)
	}
	// Cache locally while streaming through, verifying the digest.
	_, err = n.store.PutExpected(id, resp.Body, n.opts.MaxArtifactBytes)
	resp.Body.Close()
	if err != nil {
		return nil, err
	}
	r, _, err := n.store.Open(id)
	return r, err
}

func (n *Node) snapshot(ctx context.Context, w *ws, upload bool) (string, int64, error) {
	// .remount is node-local truth and must never travel with the workspace.
	excludes := append([]string{EnvFileDir}, w.Spec.Exclude...)
	pr, pw := io.Pipe()
	go func() { pw.CloseWithError(w.handle.Snapshot(ctx, excludes, pw)) }()
	id, size, err := n.store.PutLimit(pr, n.opts.MaxArtifactBytes)
	if err != nil {
		pr.CloseWithError(err)
		return "", 0, err
	}
	if upload && n.opts.ArtifactURL != "" {
		if err := n.upload(ctx, id); err != nil {
			return id, size, fmt.Errorf("upload artifact %s: %w", id, err)
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
	if existing := n.prepared[req.WS]; existing != nil {
		if existing.request.Gen != req.Gen || existing.request.Snapshot != req.Snapshot || existing.request.Reason != req.Reason {
			n.mu.Unlock()
			return nil, proto.Err(proto.CodeConflict, "release retry does not match prepared operation")
		}
		n.mu.Unlock()
		select {
		case <-existing.done:
			if existing.err != nil {
				return nil, existing.err
			}
			return existing.response, nil
		default:
			return proto.WSReleasedReq{ID: req.WS, Gen: req.Gen, Reason: req.Reason, Preparing: true}, nil
		}
	}
	w := n.workspaces[req.WS]
	if w == nil {
		n.mu.Unlock()
		return nil, proto.Err(proto.CodeNotFound, "workspace %s not here", req.WS)
	}
	if w.Generation != req.Gen {
		n.mu.Unlock()
		return nil, proto.Err(proto.CodeConflict, "generation mismatch")
	}
	prepared := &preparedRelease{workspace: w, request: *req, done: make(chan struct{})}
	n.prepared[req.WS] = prepared
	delete(n.workspaces, req.WS)
	delete(n.deadlines, req.WS)
	for k, s := range n.subs {
		if s.ws == req.WS {
			s.cancel()
			delete(n.subs, k)
		}
	}
	n.mu.Unlock()
	// Stop processes first so the snapshot is quiescent.
	n.sessions.KillWorkspace(req.WS)
	out := proto.WSReleasedReq{ID: req.WS, Gen: req.Gen, Reason: req.Reason}
	if req.Snapshot {
		id, _, err := n.snapshot(ctx, w, true)
		if err != nil {
			prepared.err = fmt.Errorf("checkpoint %s: %w", id, err)
			n.mu.Lock()
			n.workspaces[req.WS] = w
			delete(n.prepared, req.WS)
			close(prepared.done)
			n.mu.Unlock()
			return nil, prepared.err
		}
		out.Snapshot = id
	}
	if w.broker != nil {
		w.broker.Suspend()
	}
	prepared.response = out
	n.mu.Lock()
	close(prepared.done)
	n.mu.Unlock()
	return out, nil
}

func (n *Node) releaseCommit(ctx context.Context, req *proto.WSReleaseCommitReq) error {
	n.mu.Lock()
	if n.committed[req.ID] == req.Gen {
		n.mu.Unlock()
		return nil
	}
	prepared := n.prepared[req.ID]
	if prepared == nil {
		n.mu.Unlock()
		return proto.Err(proto.CodeConflict, "workspace %s has no prepared release", req.ID)
	}
	if prepared.response.Gen != req.Gen || prepared.response.Snapshot != req.Snapshot {
		n.mu.Unlock()
		return proto.Err(proto.CodeConflict, "release commit does not match prepared checkpoint")
	}
	done := prepared.done
	n.mu.Unlock()
	select {
	case <-done:
	case <-ctx.Done():
		return ctx.Err()
	}
	if prepared.err != nil {
		return prepared.err
	}
	if prepared.workspace.broker != nil {
		_ = prepared.workspace.broker.Close()
	}
	if err := prepared.workspace.handle.Destroy(ctx); err != nil {
		return err
	}
	n.mu.Lock()
	if n.prepared[req.ID] == prepared {
		delete(n.prepared, req.ID)
		n.committed[req.ID] = req.Gen
	}
	n.mu.Unlock()
	return nil
}

func (n *Node) releaseAbort(req *proto.WSReleaseCommitReq) error {
	n.mu.Lock()
	defer n.mu.Unlock()
	prepared := n.prepared[req.ID]
	if prepared == nil {
		if current := n.workspaces[req.ID]; current != nil && current.Generation == req.Gen {
			return nil // duplicate abort after successful restoration
		}
		return proto.Err(proto.CodeConflict, "workspace %s has no prepared release", req.ID)
	}
	if prepared.response.Gen != req.Gen {
		return proto.Err(proto.CodeConflict, "release abort generation mismatch")
	}
	select {
	case <-prepared.done:
	default:
		return proto.Err(proto.CodeTimeout, "release prepare is still running")
	}
	if prepared.workspace.broker != nil {
		prepared.workspace.broker.Resume()
	}
	n.workspaces[req.ID] = prepared.workspace
	n.deadlines[req.ID] = time.Now().Add(n.localLeaseWindowLocked())
	delete(n.prepared, req.ID)
	return nil
}
