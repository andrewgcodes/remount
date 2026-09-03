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
	"net"
	"net/http"
	"os"
	"path/filepath"
	"slices"
	"sort"
	"strings"
	"sync"
	"time"

	"remount.dev/remount/internal/artifact"
	"remount.dev/remount/internal/broker"
	"remount.dev/remount/internal/connector"
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
	// MaxArtifactStoreBytes and MaxArtifactObjects bound this node's snapshot
	// cache, including concurrent staging. Zero selects 32 GiB / 50,000.
	MaxArtifactStoreBytes int64
	MaxArtifactObjects    int
	// ArtifactRetention is the minimum age of an unreferenced cache entry;
	// ArtifactGCInterval controls background reference-aware collection. Zero
	// selects 24 hours and ten minutes.
	ArtifactRetention  time.Duration
	ArtifactGCInterval time.Duration
	// Package connector cache limits. Zero values select conservative node,
	// workspace, and object defaults in connector.NewStore.
	MaxConnectorCacheBytes       int64
	MaxConnectorWorkspaceBytes   int64
	MaxConnectorObjectBytes      int64
	MaxConnectorObjects          int64
	MaxConnectorWorkspaceObjects int64
	Logger                       *slog.Logger
	// Allow lists hosts every workspace on this node may reach without a credential.
	Allow []string
	// AllowPrivate lists hosts that may resolve to private addresses (local models).
	AllowPrivate []string
	// BrokerRootCAs augments trust for broker-reoriginated TLS (primarily
	// private providers and deterministic integration tests).
	BrokerRootCAs *x509.CertPool
	// BrokerAdvertiseHost overrides the host workspaces dial to reach their
	// broker. Docker workspaces default to host.docker.internal; an IP
	// literal here is also the address the broker listens on.
	BrokerAdvertiseHost string
	Version             string
	// Caps advertises extra capabilities (display, gpu …).
	Caps []string
	// MaxConcurrentRequests bounds request handlers independently of relay
	// connection count. Zero selects 128.
	MaxConcurrentRequests int
	// Session limits bound retained logs and live processes/connections.
	MaxSessions             int
	MaxActiveSessions       int
	MaxSessionsPerWorkspace int
	MaxSessionsPerPrincipal int
	// Session log limits are per retained session. Their product with
	// MaxSessions is the node-wide worst-case retained-memory/spill bound.
	SessionMemoryBytes     int
	SessionSpillBytes      int64
	SessionMaxChunkBytes   int
	SessionMaxMemoryChunks int
	// MutationRetention is the replay window for completed node-side
	// idempotency results. Pending/ambiguous intents are never pruned. Zero
	// selects 30 days; MaxMutationRecords defaults to 10,000.
	MutationRetention  time.Duration
	MaxMutationRecords int
	// Snapshot admission bounds concurrent archive construction across the
	// node and user-requested snapshot frequency per workspace. Lifecycle
	// checkpoints bypass the frequency limit but still share the concurrency
	// budget. Zero selects 4 concurrent snapshots and a one-second interval.
	MaxConcurrentSnapshots int
	SnapshotMinInterval    time.Duration
}

// Node is the supervisor.
type Node struct {
	opts   Options
	id     string
	priv   ed25519.PrivateKey
	logger *slog.Logger

	sessions   *session.Manager
	store      *artifact.Store
	connectors *connector.Store
	events     *eventlog.Log

	mu         sync.Mutex
	peer       *transport.Peer
	protocol   []string // capabilities negotiated with the current uplink
	ctrlPub    ed25519.PublicKey
	leaseSec   int64
	workspaces map[string]*ws
	// agentRuns are the live Agent run attempts keyed by agent|run;
	// agentRunsDone remembers the ones that finished on this node (and
	// when) so a replayed agent.run for a dead attempt is refused rather
	// than restarted; entries age out, see pruneAgentRunsDoneLocked.
	agentRuns     map[string]*agentRun
	agentRunsDone map[string]time.Time
	// agentReportSink replaces the uplink for agent reports in tests.
	agentReportSink func(context.Context, *proto.AgentReport) error
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

	mutationMu    sync.Mutex
	mutations     map[string]*mutationEntry
	mutationPath  string
	snapshotMu    sync.Mutex
	snapshotSlots chan struct{}

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
	handle       workspace.Handle
	broker       *broker.Broker
	leases       []proto.BindingLease
	lastSnapshot time.Time
	// treeMu serializes node filesystem mutations/session startup with archive
	// construction. checkpointing is guarded by Node.mu and rejects newly
	// authorized work while an authoritative checkpoint fences sessions.
	treeMu        sync.RWMutex
	checkpointing bool
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
	done       chan struct{}
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
	mutationPending   = "pending"
	mutationCompleted = "completed"
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
	if opts.MaxArtifactBytes < 0 || opts.MaxArtifactStoreBytes < 0 || opts.MaxArtifactObjects < 0 ||
		opts.MaxConnectorCacheBytes < 0 || opts.MaxConnectorWorkspaceBytes < 0 || opts.MaxConnectorObjectBytes < 0 ||
		opts.MaxConnectorObjects < 0 || opts.MaxConnectorWorkspaceObjects < 0 ||
		opts.MaxSessions < 0 || opts.MaxActiveSessions < 0 || opts.MaxSessionsPerWorkspace < 0 ||
		opts.MaxSessionsPerPrincipal < 0 || opts.SessionMemoryBytes < 0 || opts.SessionSpillBytes < 0 ||
		opts.SessionMaxChunkBytes < 0 || opts.SessionMaxMemoryChunks < 0 || opts.MaxConcurrentRequests < 0 ||
		opts.MutationRetention < 0 || opts.MaxMutationRecords < 0 || opts.MaxConcurrentSnapshots < 0 ||
		opts.SnapshotMinInterval < 0 || opts.ArtifactRetention < 0 || opts.ArtifactGCInterval < 0 {
		return nil, errors.New("node: resource limits must not be negative")
	}
	if opts.MaxArtifactBytes == 0 {
		opts.MaxArtifactBytes = 8 << 30
	}
	if opts.MaxArtifactStoreBytes == 0 {
		opts.MaxArtifactStoreBytes = 32 << 30
	}
	if opts.MaxArtifactObjects == 0 {
		opts.MaxArtifactObjects = 50_000
	}
	if opts.ArtifactRetention == 0 {
		opts.ArtifactRetention = 24 * time.Hour
	}
	if opts.ArtifactGCInterval == 0 {
		opts.ArtifactGCInterval = 10 * time.Minute
	}
	if opts.MaxConnectorCacheBytes == 0 {
		opts.MaxConnectorCacheBytes = 16 << 30
	}
	if opts.MaxConnectorWorkspaceBytes == 0 {
		opts.MaxConnectorWorkspaceBytes = min(int64(2<<30), opts.MaxConnectorCacheBytes)
	}
	if opts.MaxConnectorObjectBytes == 0 {
		opts.MaxConnectorObjectBytes = min(int64(512<<20), opts.MaxConnectorWorkspaceBytes)
	}
	if opts.MaxConnectorObjects == 0 {
		opts.MaxConnectorObjects = 100_000
	}
	if opts.MaxConnectorWorkspaceObjects == 0 {
		opts.MaxConnectorWorkspaceObjects = min(int64(4_096), opts.MaxConnectorObjects)
	}
	if opts.MaxConcurrentRequests <= 0 {
		opts.MaxConcurrentRequests = 128
	}
	if opts.SessionMemoryBytes == 0 {
		opts.SessionMemoryBytes = 2 << 20
	}
	if opts.SessionSpillBytes == 0 {
		opts.SessionSpillBytes = 128 << 20
	}
	if opts.SessionMaxChunkBytes == 0 {
		opts.SessionMaxChunkBytes = 32 << 10
	}
	if opts.SessionMaxMemoryChunks == 0 {
		opts.SessionMaxMemoryChunks = 16_384
	}
	if opts.MutationRetention == 0 {
		opts.MutationRetention = 30 * 24 * time.Hour
	}
	if opts.MaxMutationRecords == 0 {
		opts.MaxMutationRecords = 10_000
	}
	if opts.MaxConcurrentSnapshots == 0 {
		opts.MaxConcurrentSnapshots = 4
	}
	if opts.SnapshotMinInterval == 0 {
		opts.SnapshotMinInterval = time.Second
	}
	for _, d := range []string{"", "ws", "spill", "artifacts"} {
		if err := os.MkdirAll(filepath.Join(opts.DataDir, d), 0o700); err != nil {
			return nil, err
		}
	}
	if err := cleanupOrphanSpills(filepath.Join(opts.DataDir, "spill")); err != nil {
		return nil, err
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
	store, err := artifact.NewStoreWithOptions(filepath.Join(opts.DataDir, "artifacts"), artifact.StoreOptions{
		MaxBytes: opts.MaxArtifactStoreBytes, MaxObjects: opts.MaxArtifactObjects,
	})
	if err != nil {
		return nil, err
	}
	connectorStore, err := connector.NewStore(filepath.Join(opts.DataDir, "connectors"), connector.StoreOptions{
		MaxBytes: opts.MaxConnectorCacheBytes, MaxBytesPerScope: opts.MaxConnectorWorkspaceBytes,
		MaxObjectBytes: opts.MaxConnectorObjectBytes, MaxObjects: opts.MaxConnectorObjects,
		MaxObjectsPerScope: opts.MaxConnectorWorkspaceObjects,
	})
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
		store: store, connectors: connectorStore, events: eventlog.New(eventlog.NewMemory(10000)),
		workspaces: map[string]*ws{}, materializing: map[string]*materialization{},
		agentRuns: map[string]*agentRun{}, agentRunsDone: map[string]time.Time{},
		deadlines: map[string]time.Time{}, quarantined: map[string]struct{}{},
		grants: map[string]*proto.Grant{}, subs: map[string]*subscriber{},
		prepared: map[string]*preparedRelease{}, committed: map[string]uint64{},
		eventClaims: map[string]eventClaim{},
		mutations:   mutations, mutationPath: mutationPath,
		snapshotSlots: make(chan struct{}, opts.MaxConcurrentSnapshots),
		requestSlots:  make(chan struct{}, opts.MaxConcurrentRequests), overloadSlots: make(chan struct{}, overloadLimit),
		requestCtx: requestCtx, requestCancel: requestCancel, acceptRequests: true,
		started: time.Now(), stop: make(chan struct{}), online: make(chan struct{}),
	}
	n.sessions = session.NewManager(session.ManagerOptions{
		SpillDir: filepath.Join(opts.DataDir, "spill"), MemBytes: opts.SessionMemoryBytes, SpillBytes: opts.SessionSpillBytes,
		MaxChunk: opts.SessionMaxChunkBytes, MaxChunks: opts.SessionMaxMemoryChunks,
		MaxSessions: opts.MaxSessions, MaxActive: opts.MaxActiveSessions, MaxSessionsPerWorkspace: opts.MaxSessionsPerWorkspace,
		MaxSessionsPerPrincipal: opts.MaxSessionsPerPrincipal,
		OnExit: func(s *session.Session, info proto.ExitInfo) {
			n.emitSession(proto.EvSExited, s.WS, s.Principal, s.ID, map[string]any{"s": s.ID, "code": info.Code, "signal": info.Signal})
			if run := s.Info.Run; run != nil {
				n.emitSession(proto.EvRunFinished, s.WS, s.Principal, s.ID, map[string]any{"s": s.ID, "recipe": run.Recipe, "exit": info.Code, "signal": info.Signal})
			}
		},
	})
	return n, nil
}

func cleanupOrphanSpills(directory string) error {
	entries, err := os.ReadDir(directory)
	if err != nil {
		return fmt.Errorf("node: read spill directory: %w", err)
	}
	var cleanupErrors []error
	for _, entry := range entries {
		name := entry.Name()
		if !strings.HasPrefix(name, "s_") || !strings.HasSuffix(name, ".log") || entry.Type()&os.ModeSymlink != 0 {
			continue
		}
		info, err := entry.Info()
		if err != nil {
			cleanupErrors = append(cleanupErrors, err)
			continue
		}
		if !info.Mode().IsRegular() {
			continue
		}
		if err := os.Remove(filepath.Join(directory, name)); err != nil {
			cleanupErrors = append(cleanupErrors, err)
			continue
		}
		metrics.OrphanSpillsRemoved.Inc()
	}
	if err := errors.Join(cleanupErrors...); err != nil {
		return fmt.Errorf("node: remove orphan session spills: %w", err)
	}
	return nil
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
	legacyCompletedAt := time.Now().UnixMilli()
	if info, statErr := os.Stat(path); statErr == nil {
		legacyCompletedAt = info.ModTime().UnixMilli()
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
		if state == mutationCompleted && record.CompletedAt == 0 {
			record.CompletedAt = legacyCompletedAt
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

func (n *Node) pruneExpiredMutationsLocked(now time.Time) map[string]*mutationEntry {
	retention := n.opts.MutationRetention
	if retention <= 0 {
		retention = 30 * 24 * time.Hour
	}
	cutoff := now.Add(-retention).UnixMilli()
	removed := make(map[string]*mutationEntry)
	for key, entry := range n.mutations {
		if entry.State == mutationCompleted && entry.CompletedAt > 0 && entry.CompletedAt < cutoff {
			removed[key] = entry
			delete(n.mutations, key)
		}
	}
	return removed
}

func (n *Node) restorePrunedMutationsLocked(removed map[string]*mutationEntry) {
	for key, entry := range removed {
		n.mutations[key] = entry
	}
}

func recordMutationPrune(removed map[string]*mutationEntry) {
	if len(removed) > 0 {
		metrics.MutationsPruned.Add(uint64(len(removed)))
	}
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
	if err = tmp.Chmod(0o600); err == nil {
		var written int
		written, err = tmp.Write(b)
		if err == nil && written != len(b) {
			err = io.ErrShortWrite
		}
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
	pruned := n.pruneExpiredMutationsLocked(time.Now())
	if existing := n.mutations[key]; existing != nil {
		if existing.Fingerprint != fingerprint {
			if len(pruned) > 0 {
				if err := n.persistMutationsLocked(); err != nil {
					n.restorePrunedMutationsLocked(pruned)
					n.mutationMu.Unlock()
					return nil, proto.Err(proto.CodeInternal, "persist mutation retention: %v", err)
				}
				recordMutationPrune(pruned)
			}
			n.mutationMu.Unlock()
			return nil, proto.Err(proto.CodeConflict, "idempotency key was reused with different arguments")
		}
		if len(pruned) > 0 {
			if err := n.persistMutationsLocked(); err != nil {
				n.restorePrunedMutationsLocked(pruned)
				n.mutationMu.Unlock()
				return nil, proto.Err(proto.CodeInternal, "persist mutation retention: %v", err)
			}
			recordMutationPrune(pruned)
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
	maxRecords := n.opts.MaxMutationRecords
	if maxRecords <= 0 {
		maxRecords = 10_000
	}
	if len(n.mutations) >= maxRecords {
		if len(pruned) > 0 {
			if err := n.persistMutationsLocked(); err != nil {
				n.restorePrunedMutationsLocked(pruned)
				n.mutationMu.Unlock()
				return nil, proto.Err(proto.CodeInternal, "persist mutation retention: %v", err)
			}
			recordMutationPrune(pruned)
		}
		n.mutationMu.Unlock()
		metrics.MutationQuotaRejected.Inc()
		return nil, proto.Err(proto.CodeResourceExhausted,
			"node mutation journal contains %d records", maxRecords)
	}
	entry := &mutationEntry{
		Fingerprint: fingerprint, State: mutationPending, CompletedAt: time.Now().UnixMilli(),
		done: make(chan struct{}), needsPersist: true,
	}
	n.mutations[key] = entry
	if err := n.persistMutationsLocked(); err != nil {
		delete(n.mutations, key)
		n.restorePrunedMutationsLocked(pruned)
		entry.err = proto.Err(proto.CodeInternal, "persist mutation intent before execution: %v", err)
		close(entry.done)
		n.mutationMu.Unlock()
		return nil, entry.err
	}
	recordMutationPrune(pruned)
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

// Protocol returns the capabilities negotiated with the current uplink, or
// nil before the first hello. The slice is a fresh copy.
func (n *Node) Protocol() []string {
	n.mu.Lock()
	defer n.mu.Unlock()
	return append([]string(nil), n.protocol...)
}

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
	if err = tmp.Chmod(0o600); err == nil {
		var written int
		written, err = tmp.Write(b)
		if err == nil && written != len(b) {
			err = io.ErrShortWrite
		}
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
	n.emitSession(typ, stream, principal, "", payload)
}

// emitSession records an event attributed to one session so a reader can
// follow a single command's history without parsing payloads.
func (n *Node) emitSession(typ, stream, principal, session string, payload any) {
	e := &proto.Event{Type: typ, Stream: stream, Principal: principal, Session: session, Node: n.id, Origin: "node", Actor: n.id}
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
	n.wg.Add(4)
	go n.renewLoop(ctx)
	go n.fenceLoop(ctx)
	go n.eventLoop(ctx)
	go n.artifactGCLoop(ctx)
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

func (n *Node) artifactGCLoop(ctx context.Context) {
	defer n.wg.Done()
	ticker := time.NewTicker(n.opts.ArtifactGCInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-n.stop:
			return
		case now := <-ticker.C:
			if _, err := n.CollectArtifacts(now); err != nil {
				metrics.ArtifactGCErrors.Inc()
				n.logger.Error("collect node artifact cache", "err", err)
			}
		}
	}
}

// CollectArtifacts removes old cache entries that are not referenced by a
// current workspace or a prepared two-phase release. The control-plane store,
// not this cache, remains the durability authority.
func (n *Node) CollectArtifacts(now time.Time) (artifact.GCResult, error) {
	n.mu.Lock()
	references := make(map[string]struct{})
	for _, workspace := range n.workspaces {
		if workspace.LastSnapshot != "" {
			references[workspace.LastSnapshot] = struct{}{}
		}
		if workspace.Spec.RestoreFrom != "" {
			references[workspace.Spec.RestoreFrom] = struct{}{}
		}
	}
	for _, prepared := range n.prepared {
		if prepared.response.Snapshot != "" {
			references[prepared.response.Snapshot] = struct{}{}
		}
	}
	n.mu.Unlock()
	ids := make([]string, 0, len(references))
	for id := range references {
		ids = append(ids, id)
	}
	result, err := n.store.Collect(ids, now.Add(-n.opts.ArtifactRetention))
	if err != nil {
		return result, err
	}
	metrics.ArtifactGCRuns.Inc()
	metrics.ArtifactGCObjects.Add(uint64(result.Removed))
	metrics.ArtifactGCBytes.Add(uint64(result.RemovedBytes))
	return result, nil
}

func (n *Node) shutdown() {
	n.requestMu.Lock()
	n.acceptRequests = false
	n.requestMu.Unlock()
	n.requestCancel()
	n.stopOnce.Do(func() { close(n.stop) })
	n.requestWG.Wait()
	n.mu.Lock()
	held := make(map[*ws]struct{}, len(n.workspaces)+len(n.prepared))
	for _, w := range n.workspaces {
		held[w] = struct{}{}
	}
	for _, prepared := range n.prepared {
		held[prepared.workspace] = struct{}{}
	}
	n.mu.Unlock()
	for w := range held {
		if w.broker != nil {
			w.broker.Suspend()
		}
	}
	for w := range held {
		if err := n.revokeWorkspaceNetwork(context.Background(), w); err != nil {
			n.logger.Error("revoke workspace network during shutdown", "ws", w.ID, "err", err)
		}
	}
	n.sessions.Close()
	for w := range held {
		if w.broker != nil {
			_ = w.broker.Close()
		}
		_ = w.handle.FS().Close()
	}
}

func (n *Node) connectOnce(ctx context.Context) error {
	conn, err := n.opts.Dialer.Dial(ctx)
	if err != nil {
		return err
	}
	peer := transport.NewPeer(conn, transport.HandlerFunc(n.handle))
	hello := proto.Hello{
		Peer: n.id, Role: proto.RoleNode, Token: n.opts.Token, Caps: proto.PeerCapabilities(),
		PubKey: n.priv.Public().(ed25519.PublicKey), Labels: n.opts.Labels,
	}
	info := workspace.HostInfoForRegistry(n.opts.Backends)
	info.Version = n.opts.Version
	info.Caps = n.opts.Caps
	info.Connectors = []string{proto.EgressConnectorPackage, proto.EgressConnectorGit}
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
	if !proto.HasCapability(ok.Caps, proto.CapabilityV1) {
		peer.Close()
		return proto.Err(proto.CodeUnsupported, "server did not negotiate required capability %q", proto.CapabilityV1)
	}
	n.mu.Lock()
	n.peer = peer
	n.protocol = ok.Caps
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
	req := proto.WSRenewReq{Gen: map[string]uint64{}, Authz: map[string]uint64{}}
	for id, w := range n.workspaces {
		req.IDs = append(req.IDs, id)
		req.Gen[id] = w.Generation
		req.Authz[id] = w.AuthzRevision
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
		var revoke *revocation
		if w := n.workspaces[id]; w != nil && w.Generation == result.Generation {
			n.deadlines[id] = deadline
			w.LeaseUntil = result.LeaseUntil
			revoke = n.applyAuthzLocked(w, result)
		}
		if materializing := n.materializing[id]; materializing != nil && materializing.generation == result.Generation {
			materializing.deadline = deadline
		}
		n.mu.Unlock()
		if revoke != nil {
			n.closeRevokedSessions(revoke)
		}
	}
}

// revocation is the work an authorization push leaves for after n.mu is
// released: whose sessions to end and what to record.
type revocation struct {
	ws         *ws
	revision   uint64
	principals []string
	reset      bool
}

// applyAuthzLocked adopts the authoritative authorization revision pushed on
// renew (authz-push). Every cached grant minted under an older revision stops
// verifying at once; the sessions of principals control names as revoked are
// collected for closure. A reset means control could not name them, so every
// session of the workspace is closed and still-authorized principals reopen
// with fresh grants. Callers hold n.mu.
func (n *Node) applyAuthzLocked(w *ws, result proto.WSRenewResult) *revocation {
	if result.AuthzRevision <= w.AuthzRevision {
		return nil
	}
	w.AuthzRevision = result.AuthzRevision
	for key, g := range n.grants {
		if g.Claims.WS == w.ID && g.Claims.AuthzRevision != w.AuthzRevision {
			delete(n.grants, key)
		}
	}
	if len(result.Revoked) == 0 && !result.AuthzReset {
		return nil
	}
	return &revocation{ws: w, revision: result.AuthzRevision, principals: result.Revoked, reset: result.AuthzReset}
}

// confirmOpenAuthz closes the window between authorizing an open and the
// session existing. A revision pushed in that window has already listed the
// workspace's sessions, so a session started under the older grant would
// outlive its principal's access; end it and answer as the grant check would.
func (n *Node) confirmOpenAuthz(w *ws, claims proto.GrantClaims, s *session.Session) error {
	n.mu.Lock()
	current := w.AuthzRevision
	n.mu.Unlock()
	if claims.AuthzRevision == current {
		return nil
	}
	n.sessions.Terminate(s.ID, proto.ExitReasonRevoked)
	return proto.Err(proto.CodeUnauthorized, "grant authorization revision is stale")
}

func (n *Node) closeRevokedSessions(r *revocation) {
	var closed []string
	for _, s := range n.sessions.List(r.ws.ID) {
		if !r.reset && !slices.Contains(r.principals, s.Principal) {
			continue
		}
		if n.sessions.Terminate(s.ID, proto.ExitReasonRevoked) {
			closed = append(closed, s.ID)
		}
	}
	n.emit(proto.EvAuthzRevoked, r.ws.ID, r.ws.Spec.Principal, map[string]any{
		"authz_revision": r.revision, "principals": r.principals, "reset": r.reset, "sessions": closed,
	})
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
		fmt.Fprintf(&b, "REMOUNT_PACKAGE_CONNECTOR=%s\n", w.broker.PackageURL())
		fmt.Fprintf(&b, "REMOUNT_GIT_CONNECTOR=%s\n", w.broker.GitURL())
	}
	for _, l := range w.leases {
		// The placeholder, never the secret.
		fmt.Fprintf(&b, "REMOUNT_REF_%s=%s\n", strings.ToUpper(strings.TrimPrefix(l.ID, "b_")), broker.Placeholder(l))
	}
	for _, kv := range gitConfigEnv(w.broker, w.Spec.Repo, w.leases) {
		// GIT_CONFIG_* routes the declared repository through the broker for
		// any shell that sources this file; values carry spaces, so quote.
		key, value, _ := strings.Cut(kv, "=")
		fmt.Fprintf(&b, "%s='%s'\n", key, strings.ReplaceAll(value, "'", `'\''`))
	}
	if err := handle.FS().Mkdir(EnvFileDir); err != nil {
		return err
	}
	return handle.FS().Write(EnvFilePath, []byte(b.String()), 0o644, false, true)
}

func (n *Node) revokeWorkspaceNetwork(ctx context.Context, w *ws) error {
	controller, ok := w.handle.(workspace.NetworkController)
	if !ok {
		policy, err := proto.NormalizeSecurity(w.Spec.Security)
		if err != nil {
			return err
		}
		if policy.RequireEnforcedEgress {
			return fmt.Errorf("backend %s has no required network controller", w.handle.Backend())
		}
		return nil
	}
	rctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
	defer cancel()
	return controller.RevokeNetwork(rctx)
}

func errorString(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}

func appendWarning(existing, warning string) string {
	if existing == "" {
		return warning
	}
	if warning == "" {
		return existing
	}
	return existing + "; " + warning
}

// stopWorkspaceSessions drains session creation that was already inside the
// workspace tree boundary before ownership was removed, then terminates every
// resulting session. Callers must remove the workspace from the serving map
// first so queued starters fail their post-lock serviceability check.
func (n *Node) stopWorkspaceSessions(w *ws) error {
	w.treeMu.Lock()
	err := n.sessions.KillWorkspace(w.ID)
	w.treeMu.Unlock()
	// Agent runs join after the tree boundary is released: a run still
	// waiting to spawn needs the boundary to observe the workspace is gone.
	n.stopAgentRuns(w.ID, "workspace sessions stopped")
	return err
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
	if materializing != nil && materializing.cancel != nil {
		materializing.cancel()
	}
	for k, s := range n.subs {
		if s.ws == id {
			s.cancel()
			delete(n.subs, k)
		}
	}
	n.mu.Unlock()
	if w != nil {
		if w.broker != nil {
			w.broker.Suspend()
		}
	}
	var networkErr error
	if w != nil {
		networkErr = n.revokeWorkspaceNetwork(ctx, w)
		if networkErr != nil {
			n.logger.Error("revoke workspace network while fencing", "ws", id, "err", networkErr)
		}
	}
	var sessionErr error
	if w != nil {
		sessionErr = n.stopWorkspaceSessions(w)
	} else {
		sessionErr = n.sessions.KillWorkspace(id)
	}
	if sessionErr != nil {
		n.logger.Error("stop workspace sessions while fencing", "ws", id, "err", sessionErr)
	}
	if w != nil {
		if w.broker != nil {
			_ = w.broker.Close()
		}
		// Wait for an operation already inside the tree critical section and
		// prevent a pre-authorized waiter from racing the close. Such a waiter
		// revalidates after it acquires treeMu and observes removal above.
		w.treeMu.Lock()
		_ = w.handle.FS().Close()
		w.treeMu.Unlock()
	}
	n.logger.Warn("workspace execution fenced; local filesystem retained", "ws", id, "reason", reason,
		"network_revoked", networkErr == nil, "network_error", errorString(networkErr),
		"sessions_stopped", sessionErr == nil, "session_error", errorString(sessionErr))
	n.emit(proto.EvWSFenced, id, "", map[string]any{
		"reason": reason, "network_revoked": networkErr == nil, "network_error": errorString(networkErr),
		"sessions_stopped": sessionErr == nil, "session_error": errorString(sessionErr),
	})
}

// quarantine is the node half of a durable fleet containment operation. It
// removes the workspace from the serving map before any slow checkpoint I/O,
// revokes its broker, stops its sessions and retains the filesystem. Control
// advances the authoritative generation after this acknowledgement.
func (n *Node) quarantine(ctx context.Context, req *proto.WSQuarantineReq) (*proto.WSQuarantineRes, error) {
	if req.OperationID == "" || req.WS == "" || req.Gen == 0 {
		return nil, proto.Err(proto.CodeBadRequest, "quarantine requires operation, workspace and generation")
	}
	switch req.Action {
	case proto.FleetActionFreeze, proto.FleetActionRevokeEgress, proto.FleetActionCheckpoint,
		proto.FleetActionStop, proto.FleetActionDestroy:
	default:
		return nil, proto.Err(proto.CodeBadRequest, "unknown quarantine action %q", req.Action)
	}
	key := "fleet:" + req.OperationID + ":" + req.WS
	raw, err := n.runMutation(ctx, key, *req, func() ([]byte, error) {
		n.mu.Lock()
		w := n.workspaces[req.WS]
		materializing := n.materializing[req.WS]
		if w == nil {
			_, retained := n.quarantined[req.WS]
			if materializing == nil && !retained {
				n.mu.Unlock()
				return nil, proto.Err(proto.CodeNotFound, "workspace %s not here", req.WS)
			}
			if materializing != nil && materializing.generation != 0 && materializing.generation != req.Gen {
				n.mu.Unlock()
				return nil, proto.Err(proto.CodeConflict, "generation mismatch")
			}
			if materializing != nil && materializing.cancel != nil {
				materializing.cancel()
			}
			n.quarantined[req.WS] = struct{}{}
			delete(n.deadlines, req.WS)
			n.mu.Unlock()

			res := proto.WSQuarantineRes{
				Fenced: true, Generation: req.Gen, Action: req.Action, Backend: req.Backend,
			}
			if materializing != nil {
				if materializing.done == nil {
					res.Fenced = false
					res.Warning = "materialization cancellation cannot be confirmed"
				} else {
					select {
					case <-materializing.done:
					case <-ctx.Done():
						return nil, ctx.Err()
					}
				}
			}

			if req.Backend == "" {
				res.Fenced = false
				res.Warning = appendWarning(res.Warning, "retained workspace has no backend identity for network revocation")
			} else if backend, backendErr := n.opts.Backends.Get(req.Backend); backendErr != nil {
				res.Fenced = false
				res.Warning = appendWarning(res.Warning, backendErr.Error())
			} else if handle, adoptErr := backend.Adopt(ctx, req.WS); adoptErr != nil {
				res.Fenced = false
				res.Warning = appendWarning(res.Warning, adoptErr.Error())
			} else {
				retainedWorkspace := &ws{
					Workspace: proto.Workspace{
						ID: req.WS, Generation: req.Gen,
						Spec: proto.WorkspaceSpec{
							Exclude: append([]string(nil), req.Exclude...), Security: req.Security,
						},
					},
					handle: handle,
				}
				if revokeErr := n.revokeWorkspaceNetwork(ctx, retainedWorkspace); revokeErr != nil {
					res.Fenced = false
					res.Warning = appendWarning(res.Warning, "network revocation failed: "+revokeErr.Error())
				}
				if req.Action == proto.FleetActionCheckpoint || req.Action == proto.FleetActionDestroy {
					id, _, snapshotErr := n.snapshot(ctx, retainedWorkspace, true)
					if snapshotErr != nil {
						res.Warning = appendWarning(res.Warning, snapshotErr.Error())
					} else {
						res.Snapshot = id
					}
				}
				_ = handle.FS().Close()
			}
			n.emit(proto.EvWSFenced, req.WS, "", map[string]any{
				"operation": req.OperationID, "action": req.Action, "materializing": materializing != nil,
				"snapshot": res.Snapshot, "warning": res.Warning, "network_revoked": res.Fenced,
			})
			return proto.Marshal(res)
		}
		if w.Generation != req.Gen {
			n.mu.Unlock()
			return nil, proto.Err(proto.CodeConflict, "generation mismatch")
		}
		delete(n.workspaces, req.WS)
		delete(n.deadlines, req.WS)
		n.quarantined[req.WS] = struct{}{}
		for key, grant := range n.grants {
			if grant != nil && grant.Claims.WS == req.WS {
				delete(n.grants, key)
			}
		}
		for key, sub := range n.subs {
			if sub.ws == req.WS {
				sub.cancel()
				delete(n.subs, key)
			}
		}
		n.mu.Unlock()

		if w.broker != nil {
			w.broker.Suspend()
		}
		networkErr := n.revokeWorkspaceNetwork(ctx, w)
		if w.broker != nil {
			_ = w.broker.Close()
		}
		sessionErr := n.stopWorkspaceSessions(w)
		res := proto.WSQuarantineRes{
			Fenced: true, Generation: req.Gen, Action: req.Action, Backend: w.handle.Backend(),
		}
		if sessionErr != nil {
			res.Fenced = false
			res.Warning = appendWarning(res.Warning, "session fencing failed: "+sessionErr.Error())
		}
		if networkErr != nil {
			res.Fenced = false
			res.Warning = appendWarning(res.Warning, "network revocation failed: "+networkErr.Error())
		}
		if sessionErr == nil && (req.Action == proto.FleetActionCheckpoint || req.Action == proto.FleetActionDestroy) {
			id, _, snapshotErr := n.snapshot(ctx, w, true)
			if snapshotErr != nil {
				res.Warning = appendWarning(res.Warning, snapshotErr.Error())
			} else {
				res.Snapshot = id
			}
		}
		w.treeMu.Lock()
		_ = w.handle.FS().Close()
		w.treeMu.Unlock()
		n.emit(proto.EvWSFenced, req.WS, w.Spec.Principal, map[string]any{
			"operation": req.OperationID, "action": req.Action, "snapshot": res.Snapshot,
			"warning": res.Warning, "network_revoked": res.Fenced,
		})
		return proto.Marshal(res)
	})
	if err != nil {
		return nil, err
	}
	var res proto.WSQuarantineRes
	if err := proto.Unmarshal(raw, &res); err != nil {
		return nil, err
	}
	return &res, nil
}

// quarantineCommit performs the destructive second phase only after control
// has durably committed the fenced state and snapshot reference. It is itself
// journaled, so a lost acknowledgement can be retried without deleting an
// unrelated replacement.
func (n *Node) quarantineCommit(ctx context.Context, req *proto.WSQuarantineCommitReq) error {
	if req.OperationID == "" || req.WS == "" || req.Gen == 0 || req.Backend == "" || req.Snapshot == "" {
		return proto.Err(proto.CodeBadRequest,
			"quarantine commit requires operation, workspace, generation, backend and snapshot")
	}
	if err := n.verifyQuarantineProof(ctx, req); err != nil {
		return err
	}
	key := "fleet:" + req.OperationID + ":destroy:" + req.WS
	_, err := n.runMutation(ctx, key, *req, func() ([]byte, error) {
		n.mu.Lock()
		if current := n.workspaces[req.WS]; current != nil {
			n.mu.Unlock()
			return nil, proto.Err(proto.CodeConflict, "workspace %s is still serviceable", req.WS)
		}
		n.mu.Unlock()
		backend, err := n.opts.Backends.Get(req.Backend)
		if err != nil {
			return nil, err
		}
		handle, err := backend.Adopt(ctx, req.WS)
		var protocolErr *proto.Error
		if errors.As(err, &protocolErr) && protocolErr.Code == proto.CodeNotFound {
			n.mu.Lock()
			delete(n.quarantined, req.WS)
			n.mu.Unlock()
			return proto.Marshal(struct{}{})
		}
		if err != nil {
			return nil, err
		}
		if err := handle.Destroy(ctx); err != nil {
			return nil, err
		}
		n.mu.Lock()
		delete(n.quarantined, req.WS)
		n.mu.Unlock()
		return proto.Marshal(struct{}{})
	})
	return err
}

// verifyQuarantineProof prevents a destructive phase-two request from becoming
// authority by itself. The exact snapshot and backend must have been returned
// by a successfully completed, durable phase-one quarantine on this node.
func (n *Node) verifyQuarantineProof(ctx context.Context, req *proto.WSQuarantineCommitReq) error {
	key := "fleet:" + req.OperationID + ":" + req.WS
	n.mutationMu.Lock()
	entry := n.mutations[key]
	if entry == nil {
		n.mutationMu.Unlock()
		return proto.Err(proto.CodeConflict, "destroy commit has no quarantine proof")
	}
	done := entry.done
	n.mutationMu.Unlock()
	select {
	case <-done:
	case <-ctx.Done():
		return ctx.Err()
	}

	n.mutationMu.Lock()
	if entry.State == mutationCompleted && entry.needsPersist {
		if err := n.persistMutationsLocked(); err != nil {
			entry.err = proto.Err(proto.CodeInternal,
				"quarantine proof completed but is not durable: %v", err)
		}
	}
	state := entry.State
	result := append([]byte(nil), entry.Result...)
	entryErr := entry.err
	n.mutationMu.Unlock()
	if state != mutationCompleted || entryErr != nil {
		return proto.Err(proto.CodeConflict, "destroy commit quarantine proof is not durably complete")
	}
	var proof proto.WSQuarantineRes
	if err := proto.Unmarshal(result, &proof); err != nil {
		return proto.Err(proto.CodeConflict, "destroy commit quarantine proof is invalid: %v", err)
	}
	if !proof.Fenced || proof.Generation != req.Gen || proof.Action != proto.FleetActionDestroy ||
		proof.Backend != req.Backend || proof.Snapshot != req.Snapshot {
		return proto.Err(proto.CodeConflict, "destroy commit does not match quarantine proof")
	}
	return nil
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

// authorizeClaims checks the grant for (client, ws) and returns its signed
// actor identity. Grants are cached per connection.
func (n *Node) authorizeClaims(client, wsID string, g *proto.Grant) (*ws, proto.GrantClaims, error) {
	n.mu.Lock()
	defer n.mu.Unlock()
	w := n.workspaces[wsID]
	if w == nil {
		return nil, proto.GrantClaims{}, proto.Err(proto.CodeNotFound, "workspace %s is not on this node", wsID)
	}
	if w.checkpointing {
		return nil, proto.GrantClaims{}, proto.Err(proto.CodeConflict, "workspace %s is checkpointing", wsID)
	}
	key := client + "|" + wsID
	if g == nil {
		g = n.grants[key]
	}
	if err := control.VerifyGrant(n.ctrlPub, g, time.Now()); err != nil {
		return nil, proto.GrantClaims{}, err
	}
	if g.Claims.Client != client || g.Claims.WS != wsID || g.Claims.Node != n.id {
		return nil, proto.GrantClaims{}, proto.Err(proto.CodeUnauthorized, "grant is for a different client, workspace or node")
	}
	if g.Claims.Gen != w.Generation {
		return nil, proto.GrantClaims{}, proto.Err(proto.CodeConflict, "grant generation %d != workspace generation %d (workspace moved?)", g.Claims.Gen, w.Generation)
	}
	if g.Claims.Tenant != w.Tenant || g.Claims.AuthzRevision != w.AuthzRevision {
		return nil, proto.GrantClaims{}, proto.Err(proto.CodeUnauthorized, "grant authorization revision or tenant is stale")
	}
	n.grants[key] = g
	return w, g.Claims, nil
}

func (n *Node) authorize(client, wsID string, g *proto.Grant) (*ws, error) {
	w, _, err := n.authorizeClaims(client, wsID, g)
	return w, err
}

// lockWorkspaceTree closes the authorization-to-operation race. A lifecycle
// transition can remove or checkpoint a workspace after authorize returns but
// before an operation reaches treeMu. Revalidate only after acquiring the tree
// lock so an operation queued behind release/quarantine/checkpoint cannot touch
// a stale or already-archived handle.
func (n *Node) lockWorkspaceTree(w *ws, exclusive bool) (func(), error) {
	if exclusive {
		w.treeMu.Lock()
	} else {
		w.treeMu.RLock()
	}
	unlock := func() {
		if exclusive {
			w.treeMu.Unlock()
		} else {
			w.treeMu.RUnlock()
		}
	}
	n.mu.Lock()
	serviceable := n.workspaces[w.ID] == w && !w.checkpointing
	n.mu.Unlock()
	if !serviceable {
		unlock()
		return nil, proto.Err(proto.CodeConflict, "workspace %s is no longer serviceable", w.ID)
	}
	return unlock, nil
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

func sessionOpenKey(claims proto.GrantClaims, wsID, kind, key string) string {
	// Authenticator-produced tenant and principal strings are opaque. Hash a
	// structured encoding instead of joining them with a delimiter, which
	// would let distinct identities alias if a custom authenticator admitted
	// that delimiter.
	sum := sha256.Sum256(proto.MustMarshal(struct {
		Tenant, Principal, Workspace, Kind, Key string
	}{claims.Tenant, claims.Principal, wsID, kind, key}))
	return fmt.Sprintf("session:%x", sum[:])
}

func (n *Node) dispatch(ctx context.Context, p *transport.Peer, f *proto.Frame) (any, error) {
	if f.From == proto.PeerControl {
		switch f.Op {
		case proto.OpWSQuarantine:
			req, err := decode[proto.WSQuarantineReq](f)
			if err != nil {
				return nil, err
			}
			return n.quarantine(ctx, req)
		case proto.OpWSQuarantineCommit:
			req, err := decode[proto.WSQuarantineCommitReq](f)
			if err != nil {
				return nil, err
			}
			return struct{}{}, n.quarantineCommit(ctx, req)
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
		case proto.OpWSSnapshot:
			req, err := decode[proto.WSSnapshotReq](f)
			if err != nil {
				return nil, err
			}
			return n.controlSnapshot(ctx, p, req)
		case proto.OpAgentRun:
			req, err := decode[proto.AgentRunReq](f)
			if err != nil {
				return nil, err
			}
			return n.agentRunStart(ctx, p, req)
		case proto.OpAgentDeliver:
			req, err := decode[proto.AgentDeliverReq](f)
			if err != nil {
				return nil, err
			}
			return struct{}{}, n.agentRunDeliver(req)
		case proto.OpAgentRunCancel:
			req, err := decode[proto.AgentRunCancelReq](f)
			if err != nil {
				return nil, err
			}
			return struct{}{}, n.agentRunCancel(req)
		case proto.OpAgentApprovalDecided:
			req, err := decode[proto.AgentApprovalDecidedReq](f)
			if err != nil {
				return nil, err
			}
			return struct{}{}, n.agentApprovalDecided(req)
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
		w, claims, err := n.authorizeClaims(f.From, req.WS, req.Grant)
		if err != nil {
			return nil, err
		}
		return n.sOpen(ctx, p, f.From, claims, w, req)
	case proto.OpPortOpen:
		req, err := decode[proto.PortOpenReq](f)
		if err != nil {
			return nil, err
		}
		w, claims, err := n.authorizeClaims(f.From, req.WS, req.Grant)
		if err != nil {
			return nil, err
		}
		return n.portOpen(ctx, p, f.From, claims, w, req)
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
		unlock, err := n.lockWorkspaceTree(w, false)
		if err != nil {
			return nil, err
		}
		defer unlock()
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
			unlock, err := n.lockWorkspaceTree(w, true)
			if err != nil {
				return nil, err
			}
			defer unlock()
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
		unlock, err := n.lockWorkspaceTree(w, false)
		if err != nil {
			return nil, err
		}
		defer unlock()
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
		unlock, err := n.lockWorkspaceTree(w, false)
		if err != nil {
			return nil, err
		}
		defer unlock()
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
			unlock, err := n.lockWorkspaceTree(w, true)
			if err != nil {
				return nil, err
			}
			defer unlock()
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
			unlock, err := n.lockWorkspaceTree(w, true)
			if err != nil {
				return nil, err
			}
			defer unlock()
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
			unlock, err := n.lockWorkspaceTree(w, true)
			if err != nil {
				return nil, err
			}
			defer unlock()
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
		unlock, err := n.lockWorkspaceTree(w, false)
		if err != nil {
			return nil, err
		}
		defer unlock()
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
			unlock, err := n.lockWorkspaceTree(w, true)
			if err != nil {
				return nil, err
			}
			defer unlock()
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
	case proto.OpFSApplyTar:
		req, err := decode[proto.FSApplyTarReq](f)
		if err != nil {
			return nil, err
		}
		w, err := n.authorize(f.From, req.WS, req.Grant)
		if err != nil {
			return nil, err
		}
		if _, err := artifact.Digest(req.Artifact); err != nil {
			return nil, proto.Err(proto.CodeBadRequest, "fs.apply_tar: %v", err)
		}
		clean := *req
		clean.Grant = nil
		key := n.mutationKey(f.From, w.ID, proto.OpFSApplyTar, req.IdempotencyKey)
		raw, err := n.runMutation(ctx, key, clean, func() ([]byte, error) {
			return n.applyTar(ctx, w, req.Artifact, f.From)
		})
		if err != nil {
			return nil, err
		}
		var result proto.FSApplyTarRes
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
			result, err := n.snapshotExplicit(ctx, w, req.Upload, req.Authoritative, func(id string) error {
				cctx, cancel := context.WithTimeout(ctx, 30*time.Second)
				defer cancel()
				return p.Call(cctx, proto.PeerControl, proto.OpWSSnapshotCommit,
					proto.WSSnapshotCommitReq{ID: w.ID, Gen: w.Generation, Snapshot: id}, nil)
			})
			if err != nil {
				return nil, err
			}
			return proto.Marshal(result)
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
	info.Connectors = []string{proto.EgressConnectorPackage, proto.EgressConnectorGit}
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

// sessionEnv builds the environment a process in w starts with: the
// workspace's env, then extra, with binding references resolved to
// placeholders and ${REMOUNT_BROKER} expanded, plus the broker's own
// variables. Real secrets are never in the result; they stay in the broker.
func (n *Node) sessionEnv(w *ws, extra map[string]string) []string {
	env := map[string]string{}
	for k, v := range w.Spec.Env {
		env[k] = v
	}
	for k, v := range extra {
		env[k] = v
	}
	base := ""
	if w.broker != nil {
		base = w.broker.BaseURL()
	}
	n.mu.Lock()
	leases := slices.Clone(w.leases)
	n.mu.Unlock()
	env = broker.ResolveEnv(env, base, leases)
	var envList []string
	if w.broker != nil {
		envList = append(envList, w.broker.EnvFor()...)
		envList = append(envList, gitConfigEnv(w.broker, w.Spec.Repo, leases)...)
	}
	return append(envList, workspace.MapEnv(env)...)
}

func (n *Node) sOpen(ctx context.Context, p *transport.Peer, client string, claims proto.GrantClaims, w *ws, req *proto.SOpenReq) (any, error) {
	if err := req.Run.Validate(); err != nil {
		return nil, err
	}
	if req.Run != nil && req.Run.Auth == proto.RunAuthWorkspaceResident && w.Spec.Security.Profile != "" && w.Spec.Security.Profile != proto.SecurityLocal {
		// The node is the authority on the workspace's security profile; a
		// client cannot talk it into a login it would never see.
		return nil, proto.Err(proto.CodeDenied, "workspace-resident harness auth is refused under security profile %s", w.Spec.Security.Profile)
	}
	unlock, err := n.lockWorkspaceTree(w, false)
	if err != nil {
		return nil, err
	}
	defer unlock()
	envList := n.sessionEnv(w, req.Env)
	spec := session.Spec{
		WS: w.ID, Kind: req.Kind, Program: req.Program, Cwd: req.Cwd, Env: envList,
		Rows: req.Rows, Cols: req.Cols, Stdin: req.Stdin, IdempotencyKey: req.IdempotencyKey,
		Principal: claims.Principal, Tenant: claims.Tenant, Run: req.Run,
	}
	if req.IdempotencyKey != "" {
		spec.IdempotencyKey = sessionOpenKey(claims, w.ID, proto.SessionExec, req.IdempotencyKey)
	}
	if req.TimeoutSec > 0 {
		spec.Timeout = time.Duration(req.TimeoutSec) * time.Second
	}
	if err := w.handle.Prepare(&spec); err != nil {
		return nil, err
	}
	s, created, err := n.sessions.OpenOrReplay(spec)
	if err != nil {
		return nil, err
	}
	if err := n.confirmOpenAuthz(w, claims, s); err != nil {
		return nil, err
	}
	n.emitSession(proto.EvSOpened, w.ID, claims.Principal, s.ID, map[string]any{"s": s.ID, "kind": req.Kind, "program": req.Program, "client": client})
	if run := req.Run; run != nil && created {
		n.emitSession(proto.EvRunStarted, w.ID, claims.Principal, s.ID, map[string]any{"s": s.ID, "recipe": run.Recipe, "task_hash": run.TaskHash, "sandbox": run.Sandbox, "auth": run.Auth})
		if run.Auth == proto.RunAuthWorkspaceResident {
			n.emitSession(proto.EvAuthWSResident, w.ID, claims.Principal, s.ID, map[string]any{"s": s.ID, "recipe": run.Recipe})
		}
	}
	if !req.NoSubscribe {
		n.subscribe(p, client, s, 0)
	}
	return proto.SOpenRes{S: s.ID, Next: s.Log.Next(), LastInputSeq: s.LastInputSeq()}, nil
}

func (n *Node) portOpen(ctx context.Context, p *transport.Peer, client string, claims proto.GrantClaims, w *ws, req *proto.PortOpenReq) (any, error) {
	unlock, err := n.lockWorkspaceTree(w, false)
	if err != nil {
		return nil, err
	}
	defer unlock()
	if req.Host != "" {
		return nil, proto.Err(proto.CodeDenied, "port.open host is backend-controlled")
	}
	if req.Port < 1 || req.Port > 65535 {
		return nil, proto.Err(proto.CodeBadRequest, "port must be between 1 and 65535")
	}
	spec := session.Spec{WS: w.ID, Kind: proto.SessionPort, Host: req.Host, Port: req.Port, Principal: claims.Principal, Tenant: claims.Tenant}
	if req.IdempotencyKey != "" {
		spec.IdempotencyKey = sessionOpenKey(claims, w.ID, proto.SessionPort, req.IdempotencyKey)
	}
	if err := w.handle.Prepare(&spec); err != nil {
		return nil, err
	}
	s, err := n.sessions.Open(spec)
	if err != nil {
		return nil, err
	}
	if err := n.confirmOpenAuthz(w, claims, s); err != nil {
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
	materializing := &materialization{cancel: materializeCancel, done: make(chan struct{})}
	n.materializing[wsID] = materializing
	n.mu.Unlock()
	defer func() {
		materializeCancel()
		n.mu.Lock()
		if n.materializing[wsID] == materializing {
			delete(n.materializing, wsID)
		}
		n.mu.Unlock()
		close(materializing.done)
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
		rctx, releaseCancel := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
		_ = p.Call(rctx, proto.PeerControl, proto.OpWSReleased, proto.WSReleasedReq{ID: wsID, Gen: res.Workspace.Generation, Reason: "materialize failed: " + err.Error(), Failed: true}, nil)
		releaseCancel()
	}
}

// requireUplinkCapabilities fails closed when the control plane never
// negotiated a capability the workspace's security profile depends on. An
// older control plane cannot deliver the property the profile promises, and
// serving the workspace anyway would be exactly the silent weakening named
// capabilities exist to prevent (ADR 0040).
func (n *Node) requireUplinkCapabilities(profile string) error {
	n.mu.Lock()
	negotiated := n.protocol
	n.mu.Unlock()
	missing := proto.MissingCapabilities(negotiated, proto.SecurityCapabilities(profile))
	if len(missing) == 0 {
		return nil
	}
	return proto.Err(proto.CodeUnsupported, "control plane lacks protocol capabilities %s required by security profile %q", strings.Join(missing, ","), profile)
}

func (n *Node) materialize(ctx context.Context, w proto.Workspace, adopt bool) error {
	if err := n.requireUplinkCapabilities(w.Spec.Security.Profile); err != nil {
		return err
	}
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
		} else if needsClone(w.Spec) && !cloneCompleted(handle) {
			// The tree exists but its clone never finished (the node died
			// mid-fetch or the clone failed). It was never ws.ready, so no
			// client has written to it: nothing is lost by starting over,
			// and serving it would hand out an empty checkout.
			n.logger.Warn("discarding tree whose clone never completed", "ws", w.ID)
			if err := handle.Destroy(ctx); err != nil {
				return fmt.Errorf("discard incomplete clone: %w", err)
			}
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
		if revokeErr := n.revokeWorkspaceNetwork(ctx, entry); revokeErr != nil {
			n.logger.Error("revoke network after materialization failure", "ws", w.ID, "err", revokeErr)
		}
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
		WS: w.ID, Generation: w.Generation, Principal: w.Spec.Principal, Tenant: w.Tenant, Leases: leases,
		Network: w.Spec.Security.Network, Allow: n.opts.Allow, AllowPrivate: n.opts.AllowPrivate,
		RootCAs: n.opts.BrokerRootCAs, ConnectorStore: n.connectors, Repo: w.Spec.Repo,
		Audit: func(a broker.Audit) {
			typ := proto.EvEgressAllowed
			switch a.Decision {
			case broker.DecisionSubstituted:
				typ = proto.EvCredUsed
			case broker.DecisionDenied, broker.DecisionLeakBlocked, broker.DecisionExpired,
				broker.DecisionUnauthenticated, broker.DecisionLimitExceeded:
				typ = proto.EvEgressDenied
			case broker.DecisionRedacted:
				typ = proto.EvEgressRedacted
			}
			n.emit(typ, a.WS, a.Principal, map[string]any{
				"generation": a.Generation, "decision": a.Decision, "binding": a.Binding,
				"rule": a.Rule, "protocol": a.Protocol, "shared_state": a.SharedState,
				"connector": a.Connector, "op": a.Op, "repo": a.Repo, "digest": a.Digest, "cached": a.Cached,
				"host": a.Host, "method": a.Method, "path": a.Path, "reason": a.Reason,
				"status": a.Status, "error": a.Error, "request_bytes": a.RequestBytes, "response_bytes": a.ResponseBytes,
				"redactions": a.Redactions,
			})
		},
	}
	if advertise := n.brokerAdvertiseHost(handle); advertise != "" {
		// Containers cannot reach a host loopback listener. The random
		// per-workspace capability authenticates this host-gateway listener.
		brokerOpts.Listen = "0.0.0.0:0"
		if ip := net.ParseIP(advertise); ip != nil {
			brokerOpts.Listen = net.JoinHostPort(advertise, "0")
		}
		brokerOpts.AdvertiseHost = advertise
	}
	entry.broker = broker.New(brokerOpts)
	if _, err := entry.broker.Start(); err != nil {
		return retainOnError(err)
	}
	if descriptor.Security.EgressMode == "enforced_gateway" {
		controller, ok := handle.(workspace.NetworkController)
		if !ok {
			return retainOnError(proto.Err(proto.CodeDenied,
				"backend %s advertises enforced egress without a network controller", be.Name()))
		}
		if err := controller.ApplyNetworkPolicy(ctx, w.Spec.Security.Network, workspace.NetworkEndpoint{
			Workspace: w.ID, Generation: w.Generation,
			ReverseProxyURL: entry.broker.BaseURL(), ForwardProxyURL: entry.broker.ProxyURL(),
		}); err != nil {
			return retainOnError(fmt.Errorf("apply enforced network policy: %w", err))
		}
	}
	// A declared repository is cloned into a fresh tree before the workspace
	// is ever serviceable. A restore already carries the checkout, and an
	// adopted tree is whatever the previous claim left; only a first
	// materialization fetches. A failed clone is not quarantined like a
	// failed restore: the tree was created moments ago and holds nothing of
	// anyone's, so it is destroyed and the next offer starts clean instead of
	// adopting an empty checkout.
	cloned := ""
	if needsClone(w.Spec) && !adopt {
		commit, err := n.cloneRepo(ctx, entry)
		if err == nil {
			err = markCloneCompleted(handle, commit)
		}
		if err != nil {
			metrics.RepoCloneFailures.Inc()
			if revokeErr := n.revokeWorkspaceNetwork(ctx, entry); revokeErr != nil {
				n.logger.Error("revoke network after clone failure", "ws", w.ID, "err", revokeErr)
			}
			entry.broker.Close()
			if destroyErr := handle.Destroy(context.WithoutCancel(ctx)); destroyErr != nil {
				n.logger.Error("discard tree after clone failure", "ws", w.ID, "err", destroyErr)
			}
			return fmt.Errorf("clone %s: %w", w.Spec.Repo.URL, err)
		}
		cloned = commit
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
	if cloned != "" {
		metrics.RepoClones.Inc()
		n.emit(proto.EvRepoCloned, w.ID, w.Spec.Principal, cloneEventPayload(w.Spec.Repo, cloned, be.Name()))
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

func (n *Node) brokerAdvertiseHost(handle workspace.Handle) string {
	if advertiser, ok := handle.(workspace.BrokerAdvertiser); ok {
		if address := advertiser.BrokerAdvertiseHost(); address != "" {
			return address
		}
	}
	if n.opts.BrokerAdvertiseHost != "" {
		return n.opts.BrokerAdvertiseHost
	}
	if handle.Backend() == "docker" {
		return "host.docker.internal"
	}
	return ""
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

// applyTar overlays an uploaded artifact onto a workspace tree. The fetch
// happens before the tree lock so a slow download never blocks other file
// operations; the overlay itself holds the lock because every rename must be
// ordered against concurrent fs.* mutations.
func (n *Node) applyTar(ctx context.Context, w *ws, id, client string) ([]byte, error) {
	rc, err := n.fetchArtifact(ctx, id)
	if err != nil {
		return nil, proto.Err(proto.CodeNotFound, "fs.apply_tar: %v", err)
	}
	defer rc.Close()
	unlock, err := n.lockWorkspaceTree(w, true)
	if err != nil {
		return nil, err
	}
	defer unlock()
	limits := artifact.RestoreLimits{MaxCompressedBytes: n.opts.MaxArtifactBytes}
	res, err := artifact.ApplyOverlay(w.handle.FS().Root(), rc, limits)
	// Whatever landed before a mid-archive refusal is real state; record it
	// before reporting the failure.
	if len(res.Paths) > 0 || res.Dirs > 0 {
		n.emit(proto.EvFSApplyTar, w.ID, w.Spec.Principal, map[string]any{
			"artifact": id, "files": len(res.Paths), "dirs": res.Dirs, "bytes": res.Bytes, "client": client,
			"complete": err == nil,
		})
		for _, p := range res.Paths {
			n.emit(proto.EvFSWrite, w.ID, w.Spec.Principal, map[string]any{"path": p, "artifact": id, "client": client})
		}
	}
	if err != nil {
		return nil, proto.Err(proto.CodeBadRequest, "fs.apply_tar: %v", err)
	}
	return proto.Marshal(proto.FSApplyTarRes{Files: len(res.Paths), Dirs: res.Dirs, Bytes: res.Bytes})
}

func (n *Node) snapshot(ctx context.Context, w *ws, upload bool) (string, int64, error) {
	release, err := n.acquireSnapshot(ctx, w, false)
	if err != nil {
		return "", 0, err
	}
	defer release()
	w.treeMu.Lock()
	defer w.treeMu.Unlock()
	result, err := n.snapshotRaw(ctx, w, upload, proto.SnapshotConsistencyQuiesced)
	return result.Artifact, result.Bytes, err
}

// controlSnapshot serves a snapshot the control plane asks for on its own
// authority (an agent fork). There is no client grant to verify; the request
// is fenced to the generation the control plane believes this node holds so a
// request that crossed a move cannot archive the wrong tree.
func (n *Node) controlSnapshot(ctx context.Context, p *transport.Peer, req *proto.WSSnapshotReq) (any, error) {
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
	if w.checkpointing {
		n.mu.Unlock()
		return nil, proto.Err(proto.CodeConflict, "workspace %s is checkpointing", req.WS)
	}
	n.mu.Unlock()
	clean := *req
	key := n.mutationKey(proto.PeerControl, w.ID, proto.OpWSSnapshot, req.IdempotencyKey)
	raw, err := n.runMutation(ctx, key, clean, func() ([]byte, error) {
		result, err := n.snapshotExplicit(ctx, w, req.Upload, req.Authoritative, func(id string) error {
			cctx, cancel := context.WithTimeout(ctx, 30*time.Second)
			defer cancel()
			return p.Call(cctx, proto.PeerControl, proto.OpWSSnapshotCommit,
				proto.WSSnapshotCommitReq{ID: w.ID, Gen: w.Generation, Snapshot: id}, nil)
		})
		if err != nil {
			return nil, err
		}
		return proto.Marshal(result)
	})
	if err != nil {
		return nil, err
	}
	var result proto.WSSnapshotRes
	if err := proto.Unmarshal(raw, &result); err != nil {
		return nil, err
	}
	return result, nil
}

func (n *Node) snapshotExplicit(ctx context.Context, w *ws, upload, authoritative bool, commit func(string) error) (proto.WSSnapshotRes, error) {
	if authoritative && !upload {
		return proto.WSSnapshotRes{}, proto.Err(proto.CodeBadRequest, "an authoritative checkpoint must be uploaded")
	}
	if authoritative && n.opts.ArtifactURL == "" {
		return proto.WSSnapshotRes{}, proto.Err(proto.CodeUnsupported,
			"authoritative checkpoints require a configured control-plane artifact store")
	}
	if authoritative && commit == nil {
		return proto.WSSnapshotRes{}, proto.Err(proto.CodeInternal,
			"authoritative checkpoints require a control-plane commit callback")
	}
	release, err := n.acquireSnapshot(ctx, w, true)
	if err != nil {
		return proto.WSSnapshotRes{}, err
	}
	defer release()

	if authoritative {
		n.mu.Lock()
		if n.workspaces[w.ID] != w || w.checkpointing {
			n.mu.Unlock()
			return proto.WSSnapshotRes{}, proto.Err(proto.CodeConflict, "workspace %s is not available for checkpoint", w.ID)
		}
		w.checkpointing = true
		n.mu.Unlock()
		defer func() {
			n.mu.Lock()
			w.checkpointing = false
			n.mu.Unlock()
		}()
	}

	// The exclusive tree lock drains any node-mediated filesystem mutation or
	// session startup that authorized just before checkpointing was published.
	w.treeMu.Lock()
	defer w.treeMu.Unlock()
	n.mu.Lock()
	serviceable := n.workspaces[w.ID] == w
	if !authoritative {
		serviceable = serviceable && !w.checkpointing
	}
	n.mu.Unlock()
	if !serviceable {
		return proto.WSSnapshotRes{}, proto.Err(proto.CodeConflict,
			"workspace %s is no longer serviceable", w.ID)
	}
	consistency := proto.SnapshotConsistencyLive
	if authoritative {
		// Process backends cannot suspend arbitrary escaped host processes, so
		// their documented local-mode contract is limited to Remount-managed
		// writers. KillWorkspace joins every such session before archiving.
		if err := n.sessions.KillWorkspace(w.ID); err != nil {
			return proto.WSSnapshotRes{}, proto.Err(proto.CodeTimeout, "quiesce workspace sessions: %v", err)
		}
		consistency = proto.SnapshotConsistencyQuiesced
	}
	result, err := n.snapshotRaw(ctx, w, upload, consistency)
	if err != nil {
		return result, err
	}
	// Keep checkpointing published and treeMu exclusive until control has
	// durably accepted this exact generation/digest. An operation that passed
	// authorization just before checkpointing may be waiting on treeMu; letting
	// it mutate between archive completion and commit would acknowledge data not
	// represented by the new authoritative checkpoint.
	if authoritative {
		if err := commit(result.Artifact); err != nil {
			return result, fmt.Errorf("commit snapshot: %w", err)
		}
	}
	return result, nil
}

func (n *Node) acquireSnapshot(ctx context.Context, w *ws, explicit bool) (func(), error) {
	if explicit {
		select {
		case n.snapshotSlots <- struct{}{}:
		default:
			metrics.SnapshotQuotaRejected.Inc()
			return nil, proto.Err(proto.CodeResourceExhausted,
				"node concurrent snapshot limit %d reached", cap(n.snapshotSlots))
		}
	} else {
		select {
		case n.snapshotSlots <- struct{}{}:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	if explicit {
		now := time.Now()
		n.snapshotMu.Lock()
		if !w.lastSnapshot.IsZero() && now.Sub(w.lastSnapshot) < n.opts.SnapshotMinInterval {
			n.snapshotMu.Unlock()
			<-n.snapshotSlots
			metrics.SnapshotQuotaRejected.Inc()
			return nil, proto.Err(proto.CodeResourceExhausted,
				"workspace snapshot interval %s has not elapsed", n.opts.SnapshotMinInterval)
		}
		w.lastSnapshot = now
		n.snapshotMu.Unlock()
	}
	return func() { <-n.snapshotSlots }, nil
}

// snapshotRaw requires w.treeMu to be held exclusively. Quiesced snapshots
// call the backend's Checkpoint contract; live snapshots are explicitly
// labeled and are never eligible to update control-plane failover state.
func (n *Node) snapshotRaw(ctx context.Context, w *ws, upload bool, consistency string) (proto.WSSnapshotRes, error) {
	// .remount is node-local truth and must never travel with the workspace.
	excludes := append([]string{EnvFileDir}, w.Spec.Exclude...)
	pr, pw := io.Pipe()
	producerDone := make(chan error, 1)
	go func() {
		var err error
		if consistency == proto.SnapshotConsistencyQuiesced {
			err = w.handle.Checkpoint(ctx, excludes, pw)
		} else {
			err = w.handle.Snapshot(ctx, excludes, pw)
		}
		_ = pw.CloseWithError(err)
		producerDone <- err
	}()
	id, size, err := n.store.PutLimit(pr, n.opts.MaxArtifactBytes)
	if err != nil {
		pr.CloseWithError(err)
		// Do not release treeMu while the archive producer can still be walking
		// the workspace. Closing the read end wakes a blocked writer; joining it
		// also prevents one failed snapshot from leaking a goroutine.
		<-producerDone
		return proto.WSSnapshotRes{}, err
	}
	if producerErr := <-producerDone; producerErr != nil {
		return proto.WSSnapshotRes{}, producerErr
	}
	if upload && n.opts.ArtifactURL != "" {
		if err := n.upload(ctx, id); err != nil {
			return proto.WSSnapshotRes{Artifact: id, Bytes: size, Consistency: consistency}, fmt.Errorf("upload artifact %s: %w", id, err)
		}
	}
	metrics.SnapshotsTaken.Inc()
	metrics.SnapshotBytes.Add(uint64(size))
	authoritative := consistency == proto.SnapshotConsistencyQuiesced && upload
	n.emit(proto.EvWSSnapshot, w.ID, w.Spec.Principal, map[string]any{
		"artifact": id, "bytes": size, "uploaded": upload,
		"consistency": consistency, "authoritative": authoritative,
	})
	return proto.WSSnapshotRes{
		Artifact: id, Bytes: size, Consistency: consistency, Authoritative: authoritative,
	}, nil
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
	if w.checkpointing {
		n.mu.Unlock()
		return nil, proto.Err(proto.CodeConflict, "workspace %s is checkpointing", req.WS)
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
	// Drain any session startup already inside the tree boundary, then stop all
	// resulting processes so the snapshot (or destroy) is quiescent.
	if err := n.stopWorkspaceSessions(w); err != nil {
		prepared.err = fmt.Errorf("quiesce workspace sessions: %w", err)
		n.mu.Lock()
		n.workspaces[req.WS] = w
		n.deadlines[req.WS] = time.Now().Add(n.localLeaseWindowLocked())
		delete(n.prepared, req.WS)
		close(prepared.done)
		n.mu.Unlock()
		return nil, prepared.err
	}
	out := proto.WSReleasedReq{ID: req.WS, Gen: req.Gen, Reason: req.Reason}
	if req.Snapshot {
		id, _, err := n.snapshot(ctx, w, true)
		if err != nil {
			prepared.err = fmt.Errorf("checkpoint %s: %w", id, err)
			n.mu.Lock()
			n.workspaces[req.WS] = w
			n.deadlines[req.WS] = time.Now().Add(n.localLeaseWindowLocked())
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
