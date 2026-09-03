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
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"sort"
	"strings"
	"sync"
	"time"

	"remount.dev/remount/internal/artifact"
	"remount.dev/remount/internal/eventlog"
	"remount.dev/remount/internal/ids"
	"remount.dev/remount/internal/metrics"
	"remount.dev/remount/internal/proto"
	"remount.dev/remount/internal/relay"
	"remount.dev/remount/internal/transport"
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

// Subject is an authenticated user/service identity.
type Subject struct {
	ID     string   `json:"id"`
	Tenant string   `json:"tenant"`
	Roles  []string `json:"roles,omitempty"`
}

// Credential is the authentication input supplied by a relay peer.
type Credential struct {
	Token string
	Role  string
	Peer  string
}

// Authenticator resolves a credential to server-authoritative identity.
type Authenticator interface {
	Authenticate(context.Context, Credential) (Subject, error)
}

// Authorizer decides an action on a resource after authentication.
type Authorizer interface {
	Check(context.Context, Subject, string, Resource) error
}

type Resource struct {
	Kind    string
	ID      string
	Tenant  string
	Owner   string
	Readers []string
	Writers []string
}

const (
	ActionRead    = "read"
	ActionWrite   = "write"
	ActionAdmin   = "admin"
	ActionExecute = "execute"
	// ActionACL is changing who may use a resource. Only its owner or an
	// administrator may do that; a writer may not widen or narrow the ACL.
	ActionACL = "acl"
)

// StaticAuthenticator is useful for small deployments and integration tests.
// Map keys are bearer credentials; values, not Hello fields, are authoritative.
type StaticAuthenticator map[string]Subject

func (a StaticAuthenticator) Authenticate(_ context.Context, c Credential) (Subject, error) {
	s, ok := a[c.Token]
	if !ok || s.ID == "" || s.Tenant == "" {
		return Subject{}, proto.Err(proto.CodeUnauthorized, "invalid credential")
	}
	return s, nil
}

// NodeApproval binds enrollment claims to an operator-approved identity.
type NodeApproval struct {
	PubKey []byte
	Labels map[string]string
	Info   proto.NodeInfo
}

// Options configure the control plane.
type Options struct {
	DB         *sql.DB            // shared with the event log; required
	Log        *eventlog.Log      // required
	Token      string             // shared bearer token; empty = open (standalone)
	SigningKey ed25519.PrivateKey // grants; generated if nil
	LeaseSec   int64              // claim lease; default 30
	// RecoveryGraceSec is how long a restarted control plane reserves prior
	// assignments for their recorded holders to reconnect. Default: 2 leases.
	RecoveryGraceSec int64
	Bindings         []Binding
	Logger           *slog.Logger
	// Artifacts lets the control plane report on and verify its blob store.
	// Optional: without it, artifact checks report as unavailable rather
	// than as passing.
	Artifacts     ArtifactStore
	Now           func() time.Time // injectable clock for tests
	Authenticator Authenticator
	Authorizer    Authorizer
	ApprovedNodes map[string]NodeApproval
	// SharedSubject is used by the legacy single-token/standalone profile.
	// Client-selected Hello.Principal is always ignored.
	SharedSubject Subject
	// SecurityProfileFloor lets a deployment strengthen every requested
	// workspace policy (isolated or multi_tenant in production modes).
	SecurityProfileFloor string
	// MaxConcurrentRequests bounds request handlers independently of relay
	// connection count. Zero selects 128.
	MaxConcurrentRequests int
	// MaxEvents is reported for capacity diagnostics; retention enforcement is
	// owned by the server around this control plane.
	MaxEvents int
	// Workspace quotas count every non-destroyed workspace. Zero selects 1,000
	// per tenant and 100 per owning subject.
	MaxWorkspacesPerTenant  int
	MaxWorkspacesPerSubject int
	// Durable control-record quotas bound idempotency results and wake timers.
	// Zero selects 100,000 total mutation records, 100,000 total timers, and
	// 128 timers per workspace. Fired timers remain visible until retention GC.
	MaxMutationRecords    int
	MaxTimers             int
	MaxTimersPerWorkspace int
	// MaxBasesPerTenant bounds pinned base images, each of which holds an
	// artifact out of garbage collection. Zero selects 256.
	MaxBasesPerTenant int
	// MaxQueuesPerTenant bounds task queues, which are pruned with their
	// workspace. Zero selects 4096.
	MaxQueuesPerTenant int
	// MaxAgentsPerTenant bounds live (non-terminal) agents. Zero selects 1024.
	MaxAgentsPerTenant int
	// MaxApprovalsPerAgent bounds parked approvals per agent. Zero selects 64.
	MaxApprovalsPerAgent int
	// MaxTranscriptBytesPerAgent bounds the transcript mirror kept per agent;
	// oldest records are evicted past it and readers see a gap. Zero selects
	// proto.MaxTranscriptBytesPerAgent.
	MaxTranscriptBytesPerAgent int64
	// PublicURL is the server's externally reachable base (https://host), used
	// to mint stable agent URLs. Empty leaves Agent.URL empty.
	PublicURL string
}

// ArtifactStore is the part of the blob store the control plane inspects.
type ArtifactStore interface {
	List() ([]string, error)
	Verify(id string) error
	Open(id string) (io.ReadCloser, int64, error)
	Stats() artifact.StoreStats
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

	mu                    sync.Mutex
	workspaces            map[string]*proto.Workspace
	timers                map[string]*proto.Timer
	nodes                 map[string]*nodeState
	clients               map[string]*proto.Hello
	subjects              map[string]Subject
	bindings              map[string]Binding
	tails                 map[string]map[string]*tailState // requester -> subscription -> tail
	lifecycle             map[string]*keyedMutex           // serializes long-running mutations per workspace
	proofs                map[string]int64                 // recently accepted node proof -> expiry
	producerSeq           map[string]uint64                // authenticated node -> last accepted event seq
	producerLocks         map[string]*keyedMutex           // serialize batches from one node
	mutationLocks         map[string]*keyedMutex           // serialize duplicate logical mutations
	fleetOps              map[string]*proto.FleetOperation
	fleetLocks            map[string]*keyedMutex
	bases                 map[string]*proto.Base // baseKey(tenant, name) -> pinned snapshot
	pools                 map[string]*proto.Pool // poolKey(tenant, name) -> desired node capacity
	queues                map[string]*proto.Queue
	agents                map[string]*proto.Agent
	approvals             map[string]*proto.Approval
	dirtyApprovals        map[string]*proto.Approval // touched under c.mu, flushed by the next agent commit
	transcriptWaiters     map[string]chan struct{}   // agent -> closed when its mirror grows or it ends
	agentRetry            map[string]time.Time       // agent -> no launch before
	agentDelivered        map[string]time.Time       // inbox message -> last deliver attempt
	agentBusy             map[string]struct{}        // agents with reconcile work in flight on a node
	agentKick             chan struct{}
	retries               map[string]*materializeRetry // ws -> hold-back after a failed materialization
	fleetWake             chan struct{}
	timerReservations     int
	timerReservationsByWS map[string]int

	requestMu      sync.Mutex
	requestSlots   chan struct{}
	overloadSlots  chan struct{}
	requestWG      sync.WaitGroup
	requestCtx     context.Context
	requestCancel  context.CancelFunc
	acceptRequests bool

	started   time.Time
	stop      chan struct{}
	wg        sync.WaitGroup
	startOnce sync.Once
	stopOnce  sync.Once
}

type nodeState struct {
	Status proto.NodeStatus
	PubKey []byte
}

// requireDeploymentCapabilities refuses a peer that negotiated fewer
// capabilities than the deployment's security floor requires. The floor is
// the profile every workspace here is strengthened to, so a peer lacking one
// of its capabilities could never serve any workspace and would only weaken
// the property the profile promises (ADR 0040).
func (c *Control) requireDeploymentCapabilities(role string, negotiated []string) error {
	missing := proto.MissingCapabilities(negotiated, proto.SecurityCapabilities(c.opts.SecurityProfileFloor))
	if len(missing) == 0 {
		return nil
	}
	return proto.Err(proto.CodeUnsupported, "%s lacks protocol capabilities %s required by security profile %q", role, strings.Join(missing, ","), c.opts.SecurityProfileFloor)
}

type tailState struct {
	cancel context.CancelFunc
}

type keyedMutex struct {
	mu   sync.Mutex
	refs int
}

// RecordPruneResult reports one resumable control-record retention pass.
type RecordPruneResult struct {
	Mutations       int64
	Timers          int64
	Workspaces      int64
	FleetOperations int64
	Assignments     int64
	LegacyIdem      int64
}

type mutationWorkspaceResult struct {
	ID string `cbor:"id"`
}

type mutationFleetResult struct {
	ID string `cbor:"id"`
}

// New creates a control plane. Call Attach with the relay before Serve.
func New(opts Options) (*Control, error) {
	if opts.DB == nil || opts.Log == nil {
		return nil, errors.New("control: DB and Log are required")
	}
	if opts.LeaseSec == 0 {
		opts.LeaseSec = 30
	}
	if opts.RecoveryGraceSec == 0 {
		opts.RecoveryGraceSec = 2 * opts.LeaseSec
	}
	if opts.Logger == nil {
		opts.Logger = slog.Default()
	}
	if opts.Now == nil {
		opts.Now = time.Now
	}
	if opts.MaxConcurrentRequests < 0 || opts.MaxEvents < 0 || opts.MaxWorkspacesPerTenant < 0 || opts.MaxWorkspacesPerSubject < 0 || opts.MaxBasesPerTenant < 0 || opts.MaxQueuesPerTenant < 0 ||
		opts.MaxMutationRecords < 0 || opts.MaxTimers < 0 || opts.MaxTimersPerWorkspace < 0 {
		return nil, errors.New("control: resource limits must not be negative")
	}
	if opts.MaxConcurrentRequests <= 0 {
		opts.MaxConcurrentRequests = 128
	}
	if opts.MaxWorkspacesPerTenant <= 0 {
		opts.MaxWorkspacesPerTenant = 1000
	}
	if opts.MaxWorkspacesPerSubject <= 0 {
		opts.MaxWorkspacesPerSubject = 100
	}
	if opts.MaxMutationRecords <= 0 {
		opts.MaxMutationRecords = 100_000
	}
	if opts.MaxTimers <= 0 {
		opts.MaxTimers = 100_000
	}
	if opts.MaxTimersPerWorkspace <= 0 {
		opts.MaxTimersPerWorkspace = 128
	}
	if opts.MaxBasesPerTenant <= 0 {
		opts.MaxBasesPerTenant = 256
	}
	if opts.MaxQueuesPerTenant <= 0 {
		opts.MaxQueuesPerTenant = 4096
	}
	if opts.MaxAgentsPerTenant <= 0 {
		opts.MaxAgentsPerTenant = 1024
	}
	if opts.MaxApprovalsPerAgent <= 0 {
		opts.MaxApprovalsPerAgent = 64
	}
	if opts.MaxTranscriptBytesPerAgent <= 0 {
		opts.MaxTranscriptBytesPerAgent = proto.MaxTranscriptBytesPerAgent
	}
	requestCtx, requestCancel := context.WithCancel(context.Background())
	overloadLimit := opts.MaxConcurrentRequests / 8
	if overloadLimit < 8 {
		overloadLimit = 8
	}
	if overloadLimit > 64 {
		overloadLimit = 64
	}
	c := &Control{
		opts: opts, db: opts.DB, log: opts.Log, logger: opts.Logger, now: opts.Now,
		workspaces: map[string]*proto.Workspace{}, timers: map[string]*proto.Timer{},
		nodes: map[string]*nodeState{}, clients: map[string]*proto.Hello{}, subjects: map[string]Subject{},
		bindings: map[string]Binding{}, tails: map[string]map[string]*tailState{},
		lifecycle: map[string]*keyedMutex{}, proofs: map[string]int64{}, producerSeq: map[string]uint64{},
		producerLocks: map[string]*keyedMutex{}, mutationLocks: map[string]*keyedMutex{},
		fleetOps: map[string]*proto.FleetOperation{}, fleetLocks: map[string]*keyedMutex{}, fleetWake: make(chan struct{}, 1),
		bases:                 map[string]*proto.Base{},
		pools:                 map[string]*proto.Pool{},
		queues:                map[string]*proto.Queue{},
		agents:                map[string]*proto.Agent{},
		approvals:             map[string]*proto.Approval{},
		dirtyApprovals:        map[string]*proto.Approval{},
		transcriptWaiters:     map[string]chan struct{}{},
		agentRetry:            map[string]time.Time{},
		agentDelivered:        map[string]time.Time{},
		agentBusy:             map[string]struct{}{},
		agentKick:             make(chan struct{}, 1),
		retries:               map[string]*materializeRetry{},
		timerReservationsByWS: map[string]int{},
		requestSlots:          make(chan struct{}, opts.MaxConcurrentRequests), overloadSlots: make(chan struct{}, overloadLimit),
		requestCtx: requestCtx, requestCancel: requestCancel, acceptRequests: true,
		started: opts.Now(), stop: make(chan struct{}),
	}
	for _, b := range opts.Bindings {
		if b.ID == "" || b.Secret == "" || len(b.Destinations) == 0 {
			return nil, fmt.Errorf("control: binding %q needs a non-empty id, secret and destinations", b.ID)
		}
		if _, exists := c.bindings[b.ID]; exists {
			return nil, fmt.Errorf("control: duplicate binding %q", b.ID)
		}
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
	// A previous process may have committed a resource and died before its
	// staged event reached the log; deliver it before anyone reads either.
	if err := c.log.DrainOutbox(context.Background(), c.db); err != nil {
		return nil, fmt.Errorf("control: drain event outbox: %w", err)
	}
	return c, nil
}

// Attach wires the relay used to reach peers.
func (c *Control) Attach(s relay.Sender) { c.send = s }

// PublicKey returns the grant-signing key.
func (c *Control) PublicKey() ed25519.PublicKey { return c.key.Public().(ed25519.PublicKey) }

// Start runs the lease/timer/offer loops.
func (c *Control) Start() {
	c.startOnce.Do(func() {
		c.wg.Add(2)
		go c.loop()
		go c.fleetLoop()
	})
}

// Stop halts loops.
func (c *Control) Stop() {
	c.stopOnce.Do(func() {
		c.requestMu.Lock()
		c.acceptRequests = false
		c.requestMu.Unlock()
		c.requestCancel()
		close(c.stop)
	})
	c.requestWG.Wait()
	c.wg.Wait()
}

// ---------------------------------------------------------------------------
// persistence
// ---------------------------------------------------------------------------

func (c *Control) migrate() error {
	if err := eventlog.CreateOutbox(c.db); err != nil {
		return err
	}
	_, err := c.db.Exec(`
CREATE TABLE IF NOT EXISTS workspaces (id TEXT PRIMARY KEY, data BLOB NOT NULL);
CREATE TABLE IF NOT EXISTS timers (id TEXT PRIMARY KEY, data BLOB NOT NULL);
CREATE TABLE IF NOT EXISTS nodes (id TEXT PRIMARY KEY, pubkey BLOB, data BLOB NOT NULL);
CREATE TABLE IF NOT EXISTS idem (key TEXT PRIMARY KEY, ws TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS mutations (
	scope TEXT NOT NULL,
	key TEXT NOT NULL,
	op TEXT NOT NULL,
	fingerprint BLOB NOT NULL,
	result BLOB NOT NULL,
	completed_at INTEGER NOT NULL,
	PRIMARY KEY(scope, key)
);
CREATE INDEX IF NOT EXISTS mutations_completed_at ON mutations(completed_at);
CREATE TABLE IF NOT EXISTS assignments (
	workspace TEXT NOT NULL,
	generation INTEGER NOT NULL,
	node TEXT NOT NULL,
	tenant TEXT NOT NULL,
	created_at INTEGER NOT NULL,
	PRIMARY KEY(workspace, generation, node)
);
CREATE TABLE IF NOT EXISTS fleet_operations (id TEXT PRIMARY KEY, data BLOB NOT NULL);
CREATE TABLE IF NOT EXISTS bases (tenant TEXT NOT NULL, name TEXT NOT NULL, data BLOB NOT NULL, PRIMARY KEY(tenant, name));
CREATE TABLE IF NOT EXISTS pools (tenant TEXT NOT NULL, name TEXT NOT NULL, data BLOB NOT NULL, PRIMARY KEY(tenant, name));
CREATE TABLE IF NOT EXISTS queues (id TEXT PRIMARY KEY, data BLOB NOT NULL);
CREATE TABLE IF NOT EXISTS agents (id TEXT PRIMARY KEY, data BLOB NOT NULL);
CREATE TABLE IF NOT EXISTS approvals (id TEXT PRIMARY KEY, data BLOB NOT NULL);
CREATE TABLE IF NOT EXISTS transcripts (
	agent TEXT NOT NULL,
	idx INTEGER NOT NULL,
	run TEXT NOT NULL,
	seq INTEGER NOT NULL,
	stream INTEGER NOT NULL,
	at INTEGER NOT NULL,
	data BLOB NOT NULL,
	PRIMARY KEY(agent, idx)
);
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
	type repair struct {
		workspace *proto.Workspace
		from      string
	}
	var repairs []repair
	type assignment struct {
		workspace  string
		generation uint64
		node       string
		tenant     string
	}
	var assignments []assignment
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
		// Preserve the recorded holder across a control restart. Publishing
		// the workspace as pending here can hand an empty/stale snapshot to a
		// different node before the only node with current bytes reconnects.
		changed := false
		originalState := ws.State
		if ws.Tenant == "" {
			ws.Tenant = "local"
			ws.Owner = ws.Spec.Principal
			if ws.Owner == "" {
				ws.Owner = "local-user"
			}
			ws.Spec.Principal = ws.Owner
			ws.AuthzRevision = 1
			changed = true
		}
		switch ws.State {
		case proto.WSClaimed, proto.WSClaiming:
			next, transitionErr := transitionWorkspace(&ws, lifecycleTransition{
				operation: transitionRecover, actor: actorRecovery, to: proto.WSClaiming,
			})
			if transitionErr != nil {
				rows.Close()
				return transitionErr
			}
			ws = next
			ws.LeaseUntil = c.now().Add(time.Duration(c.opts.RecoveryGraceSec) * time.Second).UnixMilli()
			changed = true
		case proto.WSQuiescing, proto.WSCheckpointing, proto.WSDestroying:
			// Without a persisted operation result we cannot safely infer that
			// either destruction or resumption completed. Keep it terminal and
			// operator-visible instead of reviving or deleting data.
			next, transitionErr := transitionWorkspace(&ws, lifecycleTransition{
				operation: transitionRecover, actor: actorRecovery, to: proto.WSFailed,
			})
			if transitionErr != nil {
				rows.Close()
				return transitionErr
			}
			ws = next
			ws.LeaseUntil = 0
			changed = true
		case proto.WSReleased:
			// The release checkpoint was committed before WSReleased became
			// durable. It is safe to re-offer from that snapshot after restart.
			next, transitionErr := transitionWorkspace(&ws, lifecycleTransition{
				operation: transitionRecover, actor: actorRecovery, to: proto.WSPending,
			})
			if transitionErr != nil {
				rows.Close()
				return transitionErr
			}
			ws = next
			ws.Node = ""
			ws.LeaseUntil = 0
			ws.Spec.RestoreFrom = ws.LastSnapshot
			changed = true
		}
		c.workspaces[ws.ID] = &ws
		if ws.Node != "" && ws.Generation > 0 {
			assignments = append(assignments, assignment{ws.ID, ws.Generation, ws.Node, ws.Tenant})
		}
		if changed {
			repairs = append(repairs, repair{workspace: &ws, from: originalState})
		}
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return err
	}
	if err := rows.Close(); err != nil {
		return err
	}
	for _, repair := range repairs {
		if err := c.persistWS(repair.workspace, c.transitionEvent(repair.from, repair.workspace, lifecycleTransition{
			operation: transitionRecover, actor: actorRecovery, to: repair.workspace.State,
		})); err != nil {
			return fmt.Errorf("persist recovered workspace %s: %w", repair.workspace.ID, err)
		}
	}
	for _, a := range assignments {
		if _, err := c.db.Exec(`INSERT OR IGNORE INTO assignments(workspace, generation, node, tenant, created_at) VALUES(?,?,?,?,?)`,
			a.workspace, a.generation, a.node, a.tenant, c.now().UnixMilli()); err != nil {
			return fmt.Errorf("persist recovered assignment %s/%d: %w", a.workspace, a.generation, err)
		}
	}
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
	if err := rows.Err(); err != nil {
		rows.Close()
		return err
	}
	if err := rows.Close(); err != nil {
		return err
	}
	rows, err = c.db.Query(`SELECT data FROM fleet_operations`)
	if err != nil {
		return err
	}
	for rows.Next() {
		var b []byte
		if err := rows.Scan(&b); err != nil {
			rows.Close()
			return err
		}
		var operation proto.FleetOperation
		if err := proto.Unmarshal(b, &operation); err != nil {
			rows.Close()
			return fmt.Errorf("decode fleet operation: %w", err)
		}
		c.fleetOps[operation.ID] = &operation
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return err
	}
	if err := rows.Close(); err != nil {
		return err
	}
	rows, err = c.db.Query(`SELECT data FROM bases`)
	if err != nil {
		return err
	}
	for rows.Next() {
		var b []byte
		if err := rows.Scan(&b); err != nil {
			rows.Close()
			return err
		}
		var base proto.Base
		if err := proto.Unmarshal(b, &base); err != nil {
			rows.Close()
			return fmt.Errorf("decode base: %w", err)
		}
		c.bases[baseKey(base.Tenant, base.Name)] = &base
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return err
	}
	if err := rows.Close(); err != nil {
		return err
	}
	if err := c.loadPools(); err != nil {
		return err
	}
	rows, err = c.db.Query(`SELECT data FROM queues`)
	if err != nil {
		return err
	}
	for rows.Next() {
		var b []byte
		if err := rows.Scan(&b); err != nil {
			rows.Close()
			return err
		}
		var q proto.Queue
		if err := proto.Unmarshal(b, &q); err != nil {
			rows.Close()
			return fmt.Errorf("decode queue: %w", err)
		}
		c.queues[q.ID] = &q
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return err
	}
	if err := rows.Close(); err != nil {
		return err
	}
	if err := c.loadAgents(); err != nil {
		return err
	}
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
	if err := rows.Err(); err != nil {
		rows.Close()
		return err
	}
	if err := rows.Close(); err != nil {
		return err
	}
	rows, err = c.db.Query(`SELECT node, MAX(producer_seq) FROM (
		SELECT node, producer_seq FROM events WHERE node != '' AND producer_seq > 0
		UNION ALL
		SELECT node, producer_seq FROM event_producers WHERE node != '' AND producer_seq > 0
	) GROUP BY node`)
	if err != nil {
		return err
	}
	for rows.Next() {
		var node string
		var seq uint64
		if err := rows.Scan(&node, &seq); err != nil {
			rows.Close()
			return err
		}
		c.producerSeq[node] = seq
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return err
	}
	return rows.Close()
}

// transact commits resource rows and the events describing them as one
// SQLite transaction (ADR 0050). Callers hold c.mu; the lock order is
// c.mu then the log's mutex, and nothing acquires them the other way round.
func (c *Control) transact(fn func(tx *eventlog.Tx) error, events []*proto.Event) error {
	err := c.log.Transact(context.Background(), c.db, func(tx *eventlog.Tx) error {
		if err := fn(tx); err != nil {
			return err
		}
		return stageAll(tx, events)
	})
	var deferred *eventlog.DrainError
	if errors.As(err, &deferred) {
		// The rows and their events are durable; only delivery to the log
		// lagged. Tick retries so subscribers see the event without a restart.
		c.logger.Error("event outbox drain deferred", "err", deferred.Err)
		return nil
	}
	return err
}

func stageAll(tx *eventlog.Tx, events []*proto.Event) error {
	for _, e := range events {
		if e == nil {
			continue
		}
		if err := tx.Emit(e); err != nil {
			return err
		}
	}
	return nil
}

func (c *Control) persistWS(ws *proto.Workspace, events ...*proto.Event) error {
	ws.UpdatedAt = c.now().UnixMilli()
	return c.transact(func(tx *eventlog.Tx) error {
		_, err := tx.Exec(`INSERT OR REPLACE INTO workspaces(id, data) VALUES(?,?)`, ws.ID, proto.MustMarshal(ws))
		return err
	}, events)
}

func (c *Control) persistClaim(ws *proto.Workspace, events ...*proto.Event) error {
	ws.UpdatedAt = c.now().UnixMilli()
	return c.transact(func(tx *eventlog.Tx) error {
		if _, err := tx.Exec(`INSERT OR REPLACE INTO workspaces(id, data) VALUES(?,?)`, ws.ID, proto.MustMarshal(ws)); err != nil {
			return err
		}
		_, err := tx.Exec(`INSERT OR IGNORE INTO assignments(workspace, generation, node, tenant, created_at) VALUES(?,?,?,?,?)`,
			ws.ID, ws.Generation, ws.Node, ws.Tenant, c.now().UnixMilli())
		return err
	}, events)
}

func (c *Control) persistFleetOperation(operation *proto.FleetOperation, events ...*proto.Event) error {
	operation.UpdatedAt = c.now().UnixMilli()
	return c.transact(func(tx *eventlog.Tx) error {
		_, err := tx.Exec(`INSERT OR REPLACE INTO fleet_operations(id, data) VALUES(?,?)`,
			operation.ID, proto.MustMarshal(operation))
		return err
	}, events)
}

// WithArtifactReferences invokes fn while workspace and fleet-operation
// references are stable. Garbage collection uses this critical section so an
// artifact cannot become referenced between the mark check and its unlink.
// The callback must not call back into Control.
func (c *Control) WithArtifactReferences(fn func([]string) error) error {
	if fn == nil {
		return errors.New("control: artifact reference callback is required")
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	set := make(map[string]struct{})
	for _, ws := range c.workspaces {
		if ws.State == proto.WSDestroyed {
			continue
		}
		if ws.Spec.RestoreFrom != "" {
			set[ws.Spec.RestoreFrom] = struct{}{}
		}
		if ws.LastSnapshot != "" {
			set[ws.LastSnapshot] = struct{}{}
		}
	}
	for _, operation := range c.fleetOps {
		for _, target := range operation.Results {
			if target.Snapshot != "" {
				set[target.Snapshot] = struct{}{}
			}
		}
	}
	for _, base := range c.bases {
		set[base.Artifact] = struct{}{}
	}
	references := make([]string, 0, len(set))
	for id := range set {
		references = append(references, id)
	}
	sort.Strings(references)
	return fn(references)
}

// PruneRecords removes completed idempotency results, fired timers, terminal
// fleet operations, unreferenced destroyed tombstones, superseded assignment
// history older than before, and drains legacy idempotency rows that have no
// timestamp and are no longer read. Pending authority is never eligible. The
// bounded pass is safe to resume after interruption because database and
// in-memory indexes change only after the transaction commits.
func (c *Control) PruneRecords(ctx context.Context, before time.Time, limit int) (RecordPruneResult, error) {
	if limit <= 0 {
		limit = 10_000
	}
	type timerCandidate struct {
		id string
		at int64
	}
	type workspaceCandidate struct {
		id string
		at int64
	}
	type fleetCandidate struct {
		id string
		at int64
	}
	type assignmentKey struct {
		workspace  string
		generation uint64
		node       string
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	cutoff := before.UnixMilli()
	timerCandidates := make([]timerCandidate, 0)
	for _, timer := range c.timers {
		if !timer.Fired {
			continue
		}
		at := timer.FiredAt
		if at == 0 {
			// Compatibility with records written before FiredAt existed.
			at = timer.CreatedAt
		}
		if at < cutoff {
			timerCandidates = append(timerCandidates, timerCandidate{id: timer.ID, at: at})
		}
	}
	sort.Slice(timerCandidates, func(i, j int) bool {
		if timerCandidates[i].at == timerCandidates[j].at {
			return timerCandidates[i].id < timerCandidates[j].id
		}
		return timerCandidates[i].at < timerCandidates[j].at
	})
	if len(timerCandidates) > limit {
		timerCandidates = timerCandidates[:limit]
	}

	fleetCandidates := make([]fleetCandidate, 0)
	prunedFleet := make(map[string]struct{})
	for id, operation := range c.fleetOps {
		terminal := operation.State == proto.FleetStateCompleted ||
			(operation.State == proto.FleetStatePartial && !fleetHasPending(operation))
		if terminal && operation.UpdatedAt < cutoff {
			fleetCandidates = append(fleetCandidates, fleetCandidate{id: id, at: operation.UpdatedAt})
		}
	}
	sort.Slice(fleetCandidates, func(i, j int) bool {
		if fleetCandidates[i].at == fleetCandidates[j].at {
			return fleetCandidates[i].id < fleetCandidates[j].id
		}
		return fleetCandidates[i].at < fleetCandidates[j].at
	})
	if len(fleetCandidates) > limit {
		fleetCandidates = fleetCandidates[:limit]
	}
	for _, candidate := range fleetCandidates {
		prunedFleet[candidate.id] = struct{}{}
	}

	protectedWorkspaces := make(map[string]struct{})
	protectedAssignments := make(map[assignmentKey]struct{})
	for id, operation := range c.fleetOps {
		if _, pruning := prunedFleet[id]; pruning {
			continue
		}
		for _, target := range operation.Results {
			protectedWorkspaces[target.Workspace] = struct{}{}
			if target.Node != "" && target.Generation != 0 {
				protectedAssignments[assignmentKey{target.Workspace, target.Generation, target.Node}] = struct{}{}
			}
		}
	}
	workspaceCandidates := make([]workspaceCandidate, 0)
	for id, workspace := range c.workspaces {
		if workspace.Node != "" && workspace.Generation != 0 {
			protectedAssignments[assignmentKey{id, workspace.Generation, workspace.Node}] = struct{}{}
		}
		if workspace.State != proto.WSDestroyed || workspace.UpdatedAt >= cutoff {
			continue
		}
		if _, protected := protectedWorkspaces[id]; !protected {
			workspaceCandidates = append(workspaceCandidates, workspaceCandidate{id: id, at: workspace.UpdatedAt})
		}
	}
	sort.Slice(workspaceCandidates, func(i, j int) bool {
		if workspaceCandidates[i].at == workspaceCandidates[j].at {
			return workspaceCandidates[i].id < workspaceCandidates[j].id
		}
		return workspaceCandidates[i].at < workspaceCandidates[j].at
	})
	if len(workspaceCandidates) > limit {
		workspaceCandidates = workspaceCandidates[:limit]
	}
	tx, err := c.db.BeginTx(ctx, nil)
	if err != nil {
		return RecordPruneResult{}, err
	}
	committed := false
	defer func() {
		if !committed {
			_ = tx.Rollback()
		}
	}()
	for _, candidate := range timerCandidates {
		if _, err := tx.ExecContext(ctx, `DELETE FROM timers WHERE id=?`, candidate.id); err != nil {
			return RecordPruneResult{}, err
		}
	}
	for _, candidate := range fleetCandidates {
		if _, err := tx.ExecContext(ctx, `DELETE FROM fleet_operations WHERE id=?`, candidate.id); err != nil {
			return RecordPruneResult{}, err
		}
	}
	// Queues go with their workspace: they are meaningless without the
	// tree they drove, and pruning them here keeps the map bounded.
	var queueCandidates []string
	for _, candidate := range workspaceCandidates {
		if _, err := tx.ExecContext(ctx, `DELETE FROM workspaces WHERE id=?`, candidate.id); err != nil {
			return RecordPruneResult{}, err
		}
		for id, q := range c.queues {
			if q.WS == candidate.id {
				queueCandidates = append(queueCandidates, id)
			}
		}
	}
	for _, id := range queueCandidates {
		if _, err := tx.ExecContext(ctx, `DELETE FROM queues WHERE id=?`, id); err != nil {
			return RecordPruneResult{}, err
		}
	}
	var agentCandidates, approvalCandidates []string
	for _, candidate := range workspaceCandidates {
		for id, a := range c.agents {
			if a.WS == candidate.id {
				agentCandidates = append(agentCandidates, id)
			}
		}
		for id, ap := range c.approvals {
			if ap.WS == candidate.id {
				approvalCandidates = append(approvalCandidates, id)
			}
		}
	}
	for _, id := range agentCandidates {
		if _, err := tx.ExecContext(ctx, `DELETE FROM agents WHERE id=?`, id); err != nil {
			return RecordPruneResult{}, err
		}
		if _, err := tx.ExecContext(ctx, `DELETE FROM transcripts WHERE agent=?`, id); err != nil {
			return RecordPruneResult{}, err
		}
	}
	for _, id := range approvalCandidates {
		if _, err := tx.ExecContext(ctx, `DELETE FROM approvals WHERE id=?`, id); err != nil {
			return RecordPruneResult{}, err
		}
	}
	res, err := tx.ExecContext(ctx, `DELETE FROM mutations WHERE rowid IN (
		SELECT rowid FROM mutations WHERE completed_at < ? ORDER BY completed_at, rowid LIMIT ?
	)`, cutoff, limit)
	if err != nil {
		return RecordPruneResult{}, err
	}
	mutations, err := res.RowsAffected()
	if err != nil {
		return RecordPruneResult{}, err
	}
	type assignmentCandidate struct {
		assignmentKey
		rowid int64
	}
	// Put the small live-authority set in a temporary table and let SQLite
	// exclude it before applying LIMIT. Merely over-fetching a fixed multiple
	// can permanently starve old deletable rows behind protected assignments.
	if _, err := tx.ExecContext(ctx, `CREATE TEMP TABLE IF NOT EXISTS prune_protected_assignments (
		workspace TEXT NOT NULL,
		generation INTEGER NOT NULL,
		node TEXT NOT NULL,
		PRIMARY KEY(workspace, generation, node)
	) WITHOUT ROWID`); err != nil {
		return RecordPruneResult{}, err
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM prune_protected_assignments`); err != nil {
		return RecordPruneResult{}, err
	}
	protect, err := tx.PrepareContext(ctx, `INSERT OR IGNORE INTO prune_protected_assignments(workspace, generation, node) VALUES(?,?,?)`)
	if err != nil {
		return RecordPruneResult{}, err
	}
	for assignment := range protectedAssignments {
		if _, err := protect.ExecContext(ctx, assignment.workspace, assignment.generation, assignment.node); err != nil {
			protect.Close()
			return RecordPruneResult{}, err
		}
	}
	if err := protect.Close(); err != nil {
		return RecordPruneResult{}, err
	}
	rows, err := tx.QueryContext(ctx, `SELECT a.rowid, a.workspace, a.generation, a.node
		FROM assignments AS a
		WHERE a.created_at < ? AND NOT EXISTS (
			SELECT 1 FROM prune_protected_assignments AS p
			WHERE p.workspace=a.workspace AND p.generation=a.generation AND p.node=a.node
		)
		ORDER BY a.created_at, a.rowid LIMIT ?`, cutoff, limit)
	if err != nil {
		return RecordPruneResult{}, err
	}
	assignmentCandidates := make([]assignmentCandidate, 0, limit)
	for rows.Next() {
		var candidate assignmentCandidate
		if err := rows.Scan(&candidate.rowid, &candidate.workspace, &candidate.generation, &candidate.node); err != nil {
			rows.Close()
			return RecordPruneResult{}, err
		}
		if _, protected := protectedAssignments[candidate.assignmentKey]; !protected {
			assignmentCandidates = append(assignmentCandidates, candidate)
			if len(assignmentCandidates) == limit {
				break
			}
		}
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return RecordPruneResult{}, err
	}
	if err := rows.Close(); err != nil {
		return RecordPruneResult{}, err
	}
	for _, candidate := range assignmentCandidates {
		if _, err := tx.ExecContext(ctx, `DELETE FROM assignments WHERE rowid=?`, candidate.rowid); err != nil {
			return RecordPruneResult{}, err
		}
	}
	legacyResult, err := tx.ExecContext(ctx, `DELETE FROM idem WHERE rowid IN (SELECT rowid FROM idem LIMIT ?)`, limit)
	if err != nil {
		return RecordPruneResult{}, err
	}
	legacyIdem, err := legacyResult.RowsAffected()
	if err != nil {
		return RecordPruneResult{}, err
	}
	if err := tx.Commit(); err != nil {
		return RecordPruneResult{}, err
	}
	committed = true
	for _, candidate := range timerCandidates {
		delete(c.timers, candidate.id)
	}
	for _, candidate := range fleetCandidates {
		delete(c.fleetOps, candidate.id)
	}
	for _, candidate := range workspaceCandidates {
		delete(c.workspaces, candidate.id)
		delete(c.retries, candidate.id)
	}
	for _, id := range queueCandidates {
		delete(c.queues, id)
	}
	for _, id := range agentCandidates {
		if a := c.agents[id]; a != nil {
			c.dropInboxLocked(a)
		}
		delete(c.agents, id)
		delete(c.agentRetry, id)
		delete(c.agentBusy, id)
	}
	for _, id := range approvalCandidates {
		delete(c.approvals, id)
	}
	result := RecordPruneResult{
		Mutations: mutations, Timers: int64(len(timerCandidates)),
		Workspaces: int64(len(workspaceCandidates)), FleetOperations: int64(len(fleetCandidates)),
		Assignments: int64(len(assignmentCandidates)), LegacyIdem: legacyIdem,
	}
	if result.Mutations > 0 {
		metrics.MutationsPruned.Add(uint64(result.Mutations))
	}
	if result.Timers > 0 {
		metrics.TimersPruned.Add(uint64(result.Timers))
	}
	if result.Workspaces > 0 {
		metrics.WorkspacesPruned.Add(uint64(result.Workspaces))
	}
	if result.FleetOperations > 0 {
		metrics.FleetOperationsPruned.Add(uint64(result.FleetOperations))
	}
	if result.Assignments > 0 {
		metrics.AssignmentsPruned.Add(uint64(result.Assignments))
	}
	return result, nil
}

func (c *Control) persistFleetOperationAndMutation(operation *proto.FleetOperation, scope, key string, request any, events ...*proto.Event) error {
	operation.UpdatedAt = c.now().UnixMilli()
	return c.transact(func(tx *eventlog.Tx) error {
		if _, err := tx.Exec(`INSERT INTO fleet_operations(id, data) VALUES(?,?)`,
			operation.ID, proto.MustMarshal(operation)); err != nil {
			return err
		}
		return c.insertMutationTx(tx.Tx, scope, key, proto.OpFleetQuarantine, request,
			mutationFleetResult{ID: operation.ID})
	}, events)
}

func (c *Control) persistWorkspaceAndFleetOperation(ws *proto.Workspace, operation *proto.FleetOperation, events ...*proto.Event) error {
	ws.UpdatedAt = c.now().UnixMilli()
	operation.UpdatedAt = c.now().UnixMilli()
	return c.transact(func(tx *eventlog.Tx) error {
		if _, err := tx.Exec(`INSERT OR REPLACE INTO workspaces(id, data) VALUES(?,?)`,
			ws.ID, proto.MustMarshal(ws)); err != nil {
			return err
		}
		_, err := tx.Exec(`INSERT OR REPLACE INTO fleet_operations(id, data) VALUES(?,?)`,
			operation.ID, proto.MustMarshal(operation))
		return err
	}, events)
}

func (c *Control) assignmentTenant(workspace string, generation uint64, node string) (string, bool, error) {
	var tenant string
	err := c.db.QueryRow(`SELECT tenant FROM assignments WHERE workspace=? AND generation=? AND node=?`, workspace, generation, node).Scan(&tenant)
	if errors.Is(err, sql.ErrNoRows) {
		return "", false, nil
	}
	if err != nil {
		return "", false, err
	}
	return tenant, true, nil
}

func (c *Control) persistWSAndMutation(ws *proto.Workspace, scope, key, op string, request, result any, events ...*proto.Event) error {
	return c.persistWorkspaceTimersAndMutation(ws, nil, scope, key, op, request, result, events...)
}

func (c *Control) persistWorkspaceTimerAndMutation(ws *proto.Workspace, timer *proto.Timer, scope, key, op string, request, result any, events ...*proto.Event) error {
	var timers []*proto.Timer
	if timer != nil {
		timers = []*proto.Timer{timer}
	}
	return c.persistWorkspaceTimersAndMutation(ws, timers, scope, key, op, request, result, events...)
}

func (c *Control) persistWorkspaceTimersAndMutation(ws *proto.Workspace, timers []*proto.Timer, scope, key, op string, request, result any, events ...*proto.Event) error {
	ws.UpdatedAt = c.now().UnixMilli()
	return c.transact(func(tx *eventlog.Tx) error {
		if _, err := tx.Exec(`INSERT OR REPLACE INTO workspaces(id, data) VALUES(?,?)`, ws.ID, proto.MustMarshal(ws)); err != nil {
			return err
		}
		for _, timer := range timers {
			if _, err := tx.Exec(`INSERT OR REPLACE INTO timers(id, data) VALUES(?,?)`, timer.ID, proto.MustMarshal(timer)); err != nil {
				return err
			}
		}
		return c.insertMutationTx(tx.Tx, scope, key, op, request, result)
	}, events)
}

func (c *Control) saveNode(id string, n *nodeState, events ...*proto.Event) {
	err := c.transact(func(tx *eventlog.Tx) error {
		_, err := tx.Exec(`INSERT OR REPLACE INTO nodes(id, pubkey, data) VALUES(?,?,?)`, id, n.PubKey, proto.MustMarshal(n.Status))
		return err
	}, events)
	if err != nil {
		c.logger.Error("save node", "err", err)
	}
}

// newEvent builds a control-originated event. It takes no lock; the caller,
// which holds c.mu, attributes the event to a workspace with stampWS and
// stages it in the transaction that commits the resource (ADR 0050).
func (c *Control) newEvent(typ, stream, principal, node string, payload any) *proto.Event {
	now := c.now().UnixMilli()
	e := &proto.Event{
		EventID: ids.New("ev"), ReceivedAt: now, Origin: "control", Actor: principal,
		Type: typ, Stream: stream, Principal: principal, Node: node,
	}
	if payload != nil {
		e.Payload = proto.MustMarshal(payload)
	}
	return e
}

// stampWS attributes e to the workspace whose row commits alongside it.
func stampWS(e *proto.Event, ws *proto.Workspace) *proto.Event {
	if e != nil && ws != nil {
		e.Tenant, e.Workspace, e.Generation = ws.Tenant, ws.ID, ws.Generation
	}
	return e
}

// wsEvent is newEvent plus stampWS for the common case of an event about the
// workspace being persisted.
func (c *Control) wsEvent(ws *proto.Workspace, typ, principal, node string, payload any) *proto.Event {
	return stampWS(c.newEvent(typ, ws.ID, principal, node, payload), ws)
}

// fleetEvent builds an event attributed to a fleet operation. ws, when not
// nil, is the target workspace committing in the same transaction.
func (c *Control) fleetEvent(typ, stream, node string, operation *proto.FleetOperation, ws *proto.Workspace, payload any) *proto.Event {
	e := &proto.Event{
		EventID: ids.New("ev"), ReceivedAt: c.now().UnixMilli(), Origin: "control", Actor: operation.RequestedBy,
		Principal: operation.RequestedBy, Tenant: operation.Tenant, OperationID: operation.ID,
		Type: typ, Stream: stream, Node: node,
	}
	if payload != nil {
		e.Payload = proto.MustMarshal(payload)
	}
	return stampWS(e, ws)
}

// ---------------------------------------------------------------------------
// relay.Controller
// ---------------------------------------------------------------------------

// Authenticate checks the token and assigns/validates the peer id.
func (c *Control) Authenticate(ctx context.Context, h *proto.Hello) (string, *proto.HelloOK, error) {
	caps, err := proto.NegotiateCapabilities(h.Caps)
	if err != nil {
		return "", nil, err
	}
	if err := c.requireDeploymentCapabilities(h.Role, caps); err != nil {
		return "", nil, err
	}
	ok := &proto.HelloOK{Caps: caps, Server: "remount", Now: c.now().UnixMilli(), PubKey: c.PublicKey(), LeaseSec: c.opts.LeaseSec}
	switch h.Role {
	case proto.RoleNode:
		if c.opts.Token != "" && subtle.ConstantTimeCompare([]byte(h.Token), []byte(c.opts.Token)) != 1 {
			return "", nil, proto.Err(proto.CodeUnauthorized, "bad node token")
		}
		if h.Peer == "" || !strings.HasPrefix(h.Peer, "n_") || len(h.PubKey) != ed25519.PublicKeySize {
			return "", nil, proto.Err(proto.CodeBadRequest, "node hello needs an n_ id and an ed25519 public key")
		}
		if err := c.verifyNodeProofLocked(h); err != nil {
			return "", nil, err
		}
		c.mu.Lock()
		defer c.mu.Unlock()
		if approval, required := c.opts.ApprovedNodes[h.Peer]; required || c.opts.ApprovedNodes != nil {
			if !required || subtle.ConstantTimeCompare(approval.PubKey, h.PubKey) != 1 {
				return "", nil, proto.Err(proto.CodeUnauthorized, "node is not operator-approved")
			}
			h.Labels = cloneMap(approval.Labels)
			info := approval.Info
			h.Node = &info
		}
		if n, exists := c.nodes[h.Peer]; exists && len(n.PubKey) > 0 && subtle.ConstantTimeCompare(n.PubKey, h.PubKey) != 1 {
			return "", nil, proto.Err(proto.CodeUnauthorized, "node id %s is registered to a different key", h.Peer)
		}
		ok.Peer = h.Peer
		return h.Peer, ok, nil
	case proto.RoleClient:
		var subject Subject
		var err error
		if c.opts.Authenticator != nil {
			subject, err = c.opts.Authenticator.Authenticate(ctx, Credential{Token: h.Token, Role: h.Role, Peer: h.Peer})
		} else {
			if c.opts.Token != "" && subtle.ConstantTimeCompare([]byte(h.Token), []byte(c.opts.Token)) != 1 {
				return "", nil, proto.Err(proto.CodeUnauthorized, "bad token")
			}
			subject = c.opts.SharedSubject
			if subject.ID == "" {
				subject = Subject{ID: "local-user", Tenant: "local", Roles: []string{"admin"}}
			}
		}
		if err != nil || subject.ID == "" || subject.Tenant == "" {
			return "", nil, proto.Err(proto.CodeUnauthorized, "authentication failed")
		}
		id := h.Peer
		if id == "" || !strings.HasPrefix(id, "c_") {
			id = ids.New("c")
		}
		ok.Peer = id
		ok.Subject, ok.Tenant = subject.ID, subject.Tenant
		c.mu.Lock()
		c.subjects[id] = subject
		c.mu.Unlock()
		return id, ok, nil
	default:
		return "", nil, proto.Err(proto.CodeBadRequest, "unknown role %q", h.Role)
	}
}

func (c *Control) verifyNodeProofLocked(h *proto.Hello) error {
	now := c.now().UnixMilli()
	if len(h.Nonce) < 16 || len(h.Proof) != ed25519.SignatureSize || h.IssuedAt < now-60_000 || h.IssuedAt > now+60_000 {
		return proto.Err(proto.CodeUnauthorized, "missing or stale node proof")
	}
	if !ed25519.Verify(ed25519.PublicKey(h.PubKey), proto.HelloProofBytes(*h), h.Proof) {
		return proto.Err(proto.CodeUnauthorized, "invalid node proof")
	}
	proofID := h.Peer + "|" + hex.EncodeToString(h.Nonce)
	c.mu.Lock()
	defer c.mu.Unlock()
	for id, expires := range c.proofs {
		if expires < now {
			delete(c.proofs, id)
		}
	}
	if _, replayed := c.proofs[proofID]; replayed {
		return proto.Err(proto.CodeUnauthorized, "replayed node proof")
	}
	c.proofs[proofID] = now + 120_000
	return nil
}

func cloneMap(in map[string]string) map[string]string {
	out := make(map[string]string, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
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
		// Authenticate already refused a hello without v1, so the negotiated
		// set is exactly what both sides implement.
		n.Status.Protocol, _ = proto.NegotiateCapabilities(h.Caps)
		if h.Node != nil {
			n.Status.Info = *h.Node
		}
		n.Status.Online = true
		n.Status.LastSeen = c.now().UnixMilli()
		var events []*proto.Event
		if fresh {
			events = append(events, c.newEvent(proto.EvNodeEnrolled, id, "", id, h.Labels))
		}
		events = append(events, c.newEvent(proto.EvNodeOnline, id, "", id, nil))
		c.saveNode(id, n, events...)
		c.mu.Unlock()
		c.offerPending(ctx)
		return
	}
	c.clients[id] = h
	c.mu.Unlock()
}

// PeerGone marks a node offline. Its leases keep running until they expire,
// so a brief reconnect loses nothing.
func (c *Control) PeerGone(_ context.Context, id string) {
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
				transition := lifecycleTransition{
					operation: transitionReconnect, actor: actorControl, to: proto.WSClaiming,
					expectGeneration: true, generation: ws.Generation,
					expectNode: true, node: id,
				}
				next, transitionErr := transitionWorkspace(ws, transition)
				if transitionErr != nil {
					c.logger.Error("validate node-offline demotion", "ws", ws.ID, "err", transitionErr)
					continue
				}
				if err := c.persistWS(&next, c.transitionEvent(proto.WSClaimed, &next, transition)); err != nil {
					c.logger.Error("persist node-offline demotion", "ws", ws.ID, "err", err)
					continue
				}
				*ws = next
				demoted = append(demoted, ws.ID)
			}
		}
		c.saveNode(id, n, c.newEvent(proto.EvNodeOffline, id, "", id, map[string]any{"workspaces": demoted}))
		c.mu.Unlock()
		return
	}
	delete(c.clients, id)
	delete(c.subjects, id)
	if tails := c.tails[id]; tails != nil {
		for _, tail := range tails {
			tail.cancel()
		}
		delete(c.tails, id)
	}
	c.mu.Unlock()
}

// HandleFrame dispatches control-plane requests.
func (c *Control) HandleFrame(ctx context.Context, f *proto.Frame) {
	if f.T != proto.KindReq {
		return // control ignores stray events/chunks
	}
	c.requestMu.Lock()
	if !c.acceptRequests {
		c.requestMu.Unlock()
		return
	}
	select {
	case c.requestSlots <- struct{}{}:
		c.requestWG.Add(1)
		c.requestMu.Unlock()
		metrics.ControlRequestsActive.Add(1)
		go func() {
			defer func() {
				metrics.ControlRequestsActive.Add(-1)
				<-c.requestSlots
				c.requestWG.Done()
			}()
			body, err := c.dispatch(c.requestCtx, f)
			if err != nil {
				var pe *proto.Error
				if !errors.As(err, &pe) {
					pe = proto.Err(proto.CodeInternal, "%v", err)
				}
				_ = c.send.Send(c.requestCtx, proto.NewErrRes(f, pe))
				return
			}
			_ = c.send.Send(c.requestCtx, proto.NewRes(f, body))
		}()
		return
	default:
		metrics.ControlRequestsRejected.Inc()
	}
	select {
	case c.overloadSlots <- struct{}{}:
		c.requestWG.Add(1)
		c.requestMu.Unlock()
		go func() {
			defer func() {
				<-c.overloadSlots
				c.requestWG.Done()
			}()
			rctx, cancel := context.WithTimeout(c.requestCtx, time.Second)
			defer cancel()
			_ = c.send.Send(rctx, proto.NewErrRes(f,
				proto.Err(proto.CodeResourceExhausted, "control request capacity exhausted")))
		}()
	default:
		// The bounded overload responders are saturated too. Dropping this
		// frame is preferable to allowing an attacker to create unbounded
		// goroutines; the caller's request deadline remains authoritative.
		c.requestMu.Unlock()
	}
	_ = ctx // inbound transport contexts do not carry a remote deadline
}

func (c *Control) principalOf(from string) string {
	c.mu.Lock()
	defer c.mu.Unlock()
	if subject, ok := c.subjects[from]; ok {
		return subject.ID
	}
	return from
}

func (c *Control) subjectOf(from string) (Subject, error) {
	c.mu.Lock()
	subject, ok := c.subjects[from]
	c.mu.Unlock()
	if !ok {
		return Subject{}, proto.Err(proto.CodeUnauthorized, "peer %s has no authenticated subject", from)
	}
	return subject, nil
}

func hasRole(subject Subject, role string) bool {
	for _, candidate := range subject.Roles {
		if candidate == role {
			return true
		}
	}
	return false
}

func (c *Control) check(ctx context.Context, subject Subject, action string, resource Resource) error {
	if c.opts.Authorizer != nil {
		if err := c.opts.Authorizer.Check(ctx, subject, action, resource); err != nil {
			return proto.Err(proto.CodeDenied, "authorization denied: %v", err)
		}
		return nil
	}
	if hasRole(subject, "admin") || (hasRole(subject, "tenant_admin") && subject.Tenant == resource.Tenant) {
		return nil
	}
	if action == ActionAdmin {
		return proto.Err(proto.CodeDenied, "administrator role required")
	}
	if subject.Tenant == "" || subject.Tenant != resource.Tenant {
		return proto.Err(proto.CodeDenied, "resource belongs to another tenant")
	}
	if subject.ID == resource.Owner {
		return nil
	}
	if action == ActionACL {
		return proto.Err(proto.CodeDenied, "subject %s does not own %s %s", subject.ID, resource.Kind, resource.ID)
	}
	if action == ActionRead {
		if contains(resource.Readers, subject.ID) || contains(resource.Writers, subject.ID) {
			return nil
		}
	} else if contains(resource.Writers, subject.ID) {
		return nil
	}
	return proto.Err(proto.CodeDenied, "subject %s may not %s %s %s", subject.ID, action, resource.Kind, resource.ID)
}

func workspaceResource(ws *proto.Workspace) Resource {
	return Resource{
		Kind: "workspace", ID: ws.ID, Tenant: ws.Tenant, Owner: ws.Owner,
		Readers: append([]string(nil), ws.Spec.ACL.Readers...), Writers: append([]string(nil), ws.Spec.ACL.Writers...),
	}
}

func (c *Control) authorizeWorkspace(ctx context.Context, from, id, action string) (*proto.Workspace, error) {
	subject, err := c.subjectOf(from)
	if err != nil {
		return nil, err
	}
	ws, err := c.wsGet(id)
	if err != nil {
		return nil, err
	}
	if err := c.check(ctx, subject, action, workspaceResource(ws)); err != nil {
		return nil, err
	}
	return ws, nil
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
		subject, err := c.subjectOf(f.From)
		if err != nil {
			return nil, err
		}
		return c.wsCreate(ctx, subject, req)
	case proto.OpWSGet, proto.OpWSInfo:
		req, err := decode[proto.WSGetReq](f)
		if err != nil {
			return nil, err
		}
		return c.authorizeWorkspace(ctx, f.From, req.ID, ActionRead)
	case proto.OpWSList:
		return c.wsListAuthorized(ctx, f.From)
	case proto.OpWSDestroy:
		req, err := decode[proto.WSGetReq](f)
		if err != nil {
			return nil, err
		}
		if _, err := c.authorizeWorkspace(ctx, f.From, req.ID, ActionWrite); err != nil {
			return nil, err
		}
		return struct{}{}, c.wsDestroy(ctx, c.principalOf(f.From), req.ID, req.IdempotencyKey)
	case proto.OpWSMove:
		req, err := decode[proto.WSMoveReq](f)
		if err != nil {
			return nil, err
		}
		if _, err := c.authorizeWorkspace(ctx, f.From, req.ID, ActionWrite); err != nil {
			return nil, err
		}
		return c.wsMove(ctx, c.principalOf(f.From), req)
	case proto.OpWSSleep:
		req, err := decode[proto.WSSleepReq](f)
		if err != nil {
			return nil, err
		}
		if _, err := c.authorizeWorkspace(ctx, f.From, req.ID, ActionWrite); err != nil {
			return nil, err
		}
		return c.wsSleep(ctx, c.principalOf(f.From), req)
	case proto.OpWSWake:
		req, err := decode[proto.WSGetReq](f)
		if err != nil {
			return nil, err
		}
		if _, err := c.authorizeWorkspace(ctx, f.From, req.ID, ActionWrite); err != nil {
			return nil, err
		}
		return c.wsWake(ctx, c.principalOf(f.From), req.ID, "", req.IdempotencyKey)
	case proto.OpWSACL:
		req, err := decode[proto.WSACLReq](f)
		if err != nil {
			return nil, err
		}
		if _, err := c.authorizeWorkspace(ctx, f.From, req.ID, ActionACL); err != nil {
			return nil, err
		}
		return c.wsACL(ctx, c.principalOf(f.From), req)
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
		return c.wsRenew(ctx, f.From, req)
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
	case proto.OpWSSnapshotCommit:
		if !c.isNode(f.From) {
			return nil, proto.Err(proto.CodeUnauthorized, "only nodes commit snapshots")
		}
		req, err := decode[proto.WSSnapshotCommitReq](f)
		if err != nil {
			return nil, err
		}
		return struct{}{}, c.wsSnapshotCommit(ctx, f.From, req)
	case proto.OpFleetQuarantine:
		req, err := decode[proto.FleetQuarantineReq](f)
		if err != nil {
			return nil, err
		}
		subject, err := c.subjectOf(f.From)
		if err != nil {
			return nil, err
		}
		resourceTenant := req.Selector.Tenant
		if resourceTenant == "" {
			resourceTenant = subject.Tenant
		}
		if err := c.check(ctx, subject, ActionAdmin, Resource{Kind: "fleet", Tenant: resourceTenant}); err != nil {
			return nil, err
		}
		return c.fleetQuarantine(ctx, subject, req)
	case proto.OpFleetGet:
		req, err := decode[proto.FleetGetReq](f)
		if err != nil {
			return nil, err
		}
		subject, err := c.subjectOf(f.From)
		if err != nil {
			return nil, err
		}
		return c.fleetGet(ctx, subject, req.ID)
	case proto.OpFleetList:
		subject, err := c.subjectOf(f.From)
		if err != nil {
			return nil, err
		}
		return c.fleetList(ctx, subject)
	case proto.OpBaseCreate:
		req, err := decode[proto.BaseCreateReq](f)
		if err != nil {
			return nil, err
		}
		subject, err := c.subjectOf(f.From)
		if err != nil {
			return nil, err
		}
		return c.baseCreate(ctx, subject, req)
	case proto.OpBaseList:
		subject, err := c.subjectOf(f.From)
		if err != nil {
			return nil, err
		}
		return c.baseList(ctx, subject)
	case proto.OpBaseRemove:
		req, err := decode[proto.BaseRemoveReq](f)
		if err != nil {
			return nil, err
		}
		subject, err := c.subjectOf(f.From)
		if err != nil {
			return nil, err
		}
		return struct{}{}, c.baseRemove(ctx, subject, req)
	case proto.OpQueueCreate:
		req, err := decode[proto.QueueCreateReq](f)
		if err != nil {
			return nil, err
		}
		subject, err := c.subjectOf(f.From)
		if err != nil {
			return nil, err
		}
		return c.queueCreate(ctx, subject, req)
	case proto.OpQueueGet:
		req, err := decode[proto.QueueGetReq](f)
		if err != nil {
			return nil, err
		}
		subject, err := c.subjectOf(f.From)
		if err != nil {
			return nil, err
		}
		return c.queueGet(ctx, subject, req.ID)
	case proto.OpQueueList:
		req, err := decode[proto.QueueListReq](f)
		if err != nil {
			return nil, err
		}
		subject, err := c.subjectOf(f.From)
		if err != nil {
			return nil, err
		}
		return c.queueList(ctx, subject, req)
	case proto.OpQueueAdvance:
		req, err := decode[proto.QueueAdvanceReq](f)
		if err != nil {
			return nil, err
		}
		subject, err := c.subjectOf(f.From)
		if err != nil {
			return nil, err
		}
		return c.queueAdvance(ctx, subject, req)
	case proto.OpPoolCreate:
		req, err := decode[proto.PoolCreateReq](f)
		if err != nil {
			return nil, err
		}
		subject, err := c.subjectOf(f.From)
		if err != nil {
			return nil, err
		}
		return c.poolCreate(ctx, subject, req)
	case proto.OpPoolGet:
		req, err := decode[proto.PoolGetReq](f)
		if err != nil {
			return nil, err
		}
		subject, err := c.subjectOf(f.From)
		if err != nil {
			return nil, err
		}
		return c.poolGet(ctx, subject, req.Name)
	case proto.OpPoolList:
		subject, err := c.subjectOf(f.From)
		if err != nil {
			return nil, err
		}
		return c.poolList(ctx, subject)
	case proto.OpPoolRemove:
		req, err := decode[proto.PoolRemoveReq](f)
		if err != nil {
			return nil, err
		}
		subject, err := c.subjectOf(f.From)
		if err != nil {
			return nil, err
		}
		return struct{}{}, c.poolRemove(ctx, subject, req)
	case proto.OpAgentCreate:
		req, err := decode[proto.AgentCreateReq](f)
		if err != nil {
			return nil, err
		}
		subject, err := c.subjectOf(f.From)
		if err != nil {
			return nil, err
		}
		return c.agentCreate(ctx, subject, req)
	case proto.OpAgentGet:
		req, err := decode[proto.AgentGetReq](f)
		if err != nil {
			return nil, err
		}
		subject, err := c.subjectOf(f.From)
		if err != nil {
			return nil, err
		}
		return c.agentGet(ctx, subject, req.ID)
	case proto.OpAgentList:
		req, err := decode[proto.AgentListReq](f)
		if err != nil {
			return nil, err
		}
		subject, err := c.subjectOf(f.From)
		if err != nil {
			return nil, err
		}
		return c.agentList(ctx, subject, req)
	case proto.OpAgentMessage:
		req, err := decode[proto.AgentMessageReq](f)
		if err != nil {
			return nil, err
		}
		subject, err := c.subjectOf(f.From)
		if err != nil {
			return nil, err
		}
		return c.agentMessage(ctx, subject, req)
	case proto.OpAgentCancel:
		req, err := decode[proto.AgentGetReq](f)
		if err != nil {
			return nil, err
		}
		subject, err := c.subjectOf(f.From)
		if err != nil {
			return nil, err
		}
		return c.agentCancel(ctx, subject, req)
	case proto.OpAgentSleep:
		req, err := decode[proto.AgentGetReq](f)
		if err != nil {
			return nil, err
		}
		subject, err := c.subjectOf(f.From)
		if err != nil {
			return nil, err
		}
		return c.agentSleep(ctx, subject, req)
	case proto.OpAgentFork:
		req, err := decode[proto.AgentForkReq](f)
		if err != nil {
			return nil, err
		}
		subject, err := c.subjectOf(f.From)
		if err != nil {
			return nil, err
		}
		return c.agentFork(ctx, subject, req)
	case proto.OpAgentDestroy:
		req, err := decode[proto.AgentGetReq](f)
		if err != nil {
			return nil, err
		}
		subject, err := c.subjectOf(f.From)
		if err != nil {
			return nil, err
		}
		return struct{}{}, c.agentDestroy(ctx, subject, req)
	case proto.OpAgentTranscript:
		req, err := decode[proto.AgentTranscriptReq](f)
		if err != nil {
			return nil, err
		}
		subject, err := c.subjectOf(f.From)
		if err != nil {
			return nil, err
		}
		return c.agentTranscript(ctx, subject, req)
	case proto.OpAgentWake:
		req, err := decode[proto.AgentWakeReq](f)
		if err != nil {
			return nil, err
		}
		subject, err := c.subjectOf(f.From)
		if err != nil {
			return nil, err
		}
		return c.agentWakeRequest(ctx, subject, req)
	case proto.OpApprovalList:
		req, err := decode[proto.ApprovalListReq](f)
		if err != nil {
			return nil, err
		}
		subject, err := c.subjectOf(f.From)
		if err != nil {
			return nil, err
		}
		return c.approvalList(ctx, subject, req)
	case proto.OpApprovalGet:
		req, err := decode[proto.ApprovalGetReq](f)
		if err != nil {
			return nil, err
		}
		subject, err := c.subjectOf(f.From)
		if err != nil {
			return nil, err
		}
		return c.approvalGet(ctx, subject, req.ID)
	case proto.OpApprovalDecide:
		req, err := decode[proto.ApprovalDecideReq](f)
		if err != nil {
			return nil, err
		}
		subject, err := c.subjectOf(f.From)
		if err != nil {
			return nil, err
		}
		return c.approvalDecide(ctx, subject, req)
	case proto.OpAgentReport:
		if !c.isNode(f.From) {
			return nil, proto.Err(proto.CodeUnauthorized, "only nodes report agent runs")
		}
		req, err := decode[proto.AgentReport](f)
		if err != nil {
			return nil, err
		}
		return struct{}{}, c.agentReport(ctx, f.From, req)
	case proto.OpNodeList:
		subject, err := c.subjectOf(f.From)
		if err != nil {
			return nil, err
		}
		if err := c.check(ctx, subject, ActionAdmin, Resource{Kind: "fleet", Tenant: subject.Tenant}); err != nil {
			return nil, err
		}
		return c.nodeList(), nil
	case proto.OpEventsTail:
		req, err := decode[proto.EventsTailReq](f)
		if err != nil {
			return nil, err
		}
		subject, err := c.subjectOf(f.From)
		if err != nil {
			return nil, err
		}
		if req.WS != "" {
			if _, err := c.authorizeWorkspace(ctx, f.From, req.WS, ActionRead); err != nil {
				return nil, err
			}
		}
		return c.eventsTail(ctx, f.From, subject, req)
	case proto.OpEventsStop:
		req, err := decode[proto.EventsStopReq](f)
		if err != nil {
			return nil, err
		}
		c.stopEventTail(f.From, req.Subscription)
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
		subject, err := c.subjectOf(f.From)
		if err != nil {
			return nil, err
		}
		return c.grant(ctx, f.From, subject, req.WS)
	case proto.OpTimerList:
		return c.timerListAuthorized(ctx, f.From)
	case proto.OpDiag:
		req, err := decode[proto.DiagReq](f)
		if err != nil {
			return nil, err
		}
		subject, err := c.subjectOf(f.From)
		if err != nil {
			return nil, err
		}
		if err := c.check(ctx, subject, ActionAdmin, Resource{Kind: "control", Tenant: subject.Tenant}); err != nil {
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

func (c *Control) wsCreate(ctx context.Context, subject Subject, req *proto.WSCreateReq) (*proto.Workspace, error) {
	security, err := proto.NormalizeSecurity(req.Spec.Security)
	if err != nil {
		return nil, err
	}
	if c.opts.SecurityProfileFloor != "" {
		security, err = proto.StrengthenSecurity(security, c.opts.SecurityProfileFloor)
		if err != nil {
			return nil, err
		}
	}
	if len(req.Spec.Bindings) > 0 && security.SecretMode == "" {
		security.SecretMode = "brokered"
	}
	for _, rule := range security.Network.Rules {
		action := ActionExecute
		switch rule.SharedState {
		case proto.SharedStateImmutableRead:
			action = ActionRead
		case proto.SharedStateScopedWrite:
			action = ActionWrite
		case proto.SharedStateGlobalWrite:
			action = ActionAdmin
		}
		if err := c.check(ctx, subject, action, Resource{
			Kind: "egress-rule", ID: rule.ID, Tenant: subject.Tenant, Owner: subject.ID,
		}); err != nil {
			return nil, err
		}
	}
	req.Spec.Security = security
	// Authority comes only from the authenticated subject. A client may not
	// select a different identity to gain bindings or ACL access.
	req.Spec.Principal = subject.ID
	if req.Spec.Base != "" && req.Spec.RestoreFrom != "" {
		return nil, proto.Err(proto.CodeBadRequest, "base and restore_from are mutually exclusive")
	}
	if err := proto.ValidateMountPath(req.Spec.MountPath); err != nil {
		return nil, err
	}
	if req.Spec.MountPath == proto.DefaultMountPath {
		req.Spec.MountPath = ""
	}
	if req.Spec.Repo.URL != "" || req.Spec.Repo.Ref != "" || req.Spec.Repo.Depth != 0 {
		repo, err := req.Spec.Repo.Normalize()
		if err != nil {
			return nil, err
		}
		if req.Spec.Base != "" || req.Spec.RestoreFrom != "" {
			// A clone needs an empty tree; a base or restore already carries
			// one. Refusing here is better than a clone that silently never
			// happens because the restore path won the race.
			return nil, proto.Err(proto.CodeBadRequest, "repo cannot be combined with base or restore_from")
		}
		req.Spec.Repo = repo
	}
	scope := subject.Tenant + "|" + subject.ID + "|workspace.create"
	unlockMutation := c.lockMutation(scope, req.IdempotencyKey)
	defer unlockMutation()
	var prior mutationWorkspaceResult
	if hit, err := c.mutationLookup(scope, req.IdempotencyKey, proto.OpWSCreate, req, &prior); err != nil {
		return nil, err
	} else if hit {
		return c.wsGet(prior.ID)
	}
	// The fingerprint must cover the request as the caller sent it: base
	// resolution below rewrites RestoreFrom, and a replay after `base rm`
	// must still find its prior result rather than a mismatch.
	asSent := *req
	// Authorize the base outside c.mu (an external Authorizer may block),
	// then re-check under the lock that the same artifact is still pinned.
	var baseArtifact string
	if req.Spec.Base != "" {
		base, err := c.baseGet(subject.Tenant, req.Spec.Base)
		if err != nil {
			return nil, err
		}
		if !c.baseReadable(ctx, subject, base) {
			return nil, proto.Err(proto.CodeDenied, "subject %s may not read base %s", subject.ID, req.Spec.Base)
		}
		baseArtifact = base.Artifact
	}
	c.mu.Lock()
	for _, b := range req.Spec.Bindings {
		if _, ok := c.bindings[b]; !ok {
			c.mu.Unlock()
			return nil, proto.Err(proto.CodeNotFound, "binding %q is not defined", b)
		}
	}
	if req.Spec.Base != "" {
		base := c.bases[baseKey(subject.Tenant, req.Spec.Base)]
		if base == nil || base.Artifact != baseArtifact {
			c.mu.Unlock()
			return nil, proto.Err(proto.CodeNotFound, "base %q is not defined", req.Spec.Base)
		}
		req.Spec.RestoreFrom = baseArtifact
	}
	if req.Spec.RestoreFrom != "" {
		if c.opts.Artifacts == nil {
			c.mu.Unlock()
			return nil, proto.Err(proto.CodeUnsupported, "artifact restore is unavailable")
		}
		r, _, err := c.opts.Artifacts.Open(req.Spec.RestoreFrom)
		if err != nil {
			c.mu.Unlock()
			return nil, proto.Err(proto.CodeNotFound, "restore artifact %q is unavailable: %v", req.Spec.RestoreFrom, err)
		}
		if err := r.Close(); err != nil {
			c.mu.Unlock()
			return nil, proto.Err(proto.CodeInternal, "close restore artifact %q: %v", req.Spec.RestoreFrom, err)
		}
	}
	tenantWorkspaces, subjectWorkspaces := 0, 0
	for _, existing := range c.workspaces {
		if existing.State == proto.WSDestroyed || existing.Tenant != subject.Tenant {
			continue
		}
		tenantWorkspaces++
		if existing.Owner == subject.ID {
			subjectWorkspaces++
		}
	}
	if tenantWorkspaces >= c.opts.MaxWorkspacesPerTenant {
		c.mu.Unlock()
		metrics.WorkspaceQuotaRejected.Inc()
		return nil, proto.Err(proto.CodeResourceExhausted, "tenant workspace limit %d reached", c.opts.MaxWorkspacesPerTenant)
	}
	if subjectWorkspaces >= c.opts.MaxWorkspacesPerSubject {
		c.mu.Unlock()
		metrics.WorkspaceQuotaRejected.Inc()
		return nil, proto.Err(proto.CodeResourceExhausted, "subject workspace limit %d reached", c.opts.MaxWorkspacesPerSubject)
	}
	now := c.now().UnixMilli()
	ws := &proto.Workspace{
		ID: ids.New("ws"), Spec: req.Spec, State: proto.WSPending, CreatedAt: now, UpdatedAt: now,
		Tenant: subject.Tenant, Owner: subject.ID, AuthzRevision: 1,
	}
	if err := c.persistWSAndMutation(ws, scope, req.IdempotencyKey, proto.OpWSCreate, &asSent, mutationWorkspaceResult{ID: ws.ID},
		c.wsEvent(ws, proto.EvWSCreated, subject.ID, "", req.Spec)); err != nil {
		c.mu.Unlock()
		return nil, err
	}
	c.workspaces[ws.ID] = ws
	id := ws.ID
	c.mu.Unlock()
	metrics.WSCreated.Inc()
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

func (c *Control) wsListAuthorized(ctx context.Context, from string) (*proto.WSListRes, error) {
	subject, err := c.subjectOf(from)
	if err != nil {
		return nil, err
	}
	c.mu.Lock()
	var candidates []proto.Workspace
	for _, ws := range c.workspaces {
		if ws.State != proto.WSDestroyed {
			candidates = append(candidates, *ws)
		}
	}
	c.mu.Unlock()
	out := &proto.WSListRes{}
	for i := range candidates {
		if c.check(ctx, subject, ActionRead, workspaceResource(&candidates[i])) == nil {
			out.Workspaces = append(out.Workspaces, candidates[i])
		}
	}
	sort.Slice(out.Workspaces, func(i, j int) bool { return out.Workspaces[i].ID < out.Workspaces[j].ID })
	return out, nil
}

func (c *Control) lockLifecycle(id string) func() {
	return c.lockKeyed(c.lifecycle, id)
}

func (c *Control) lockProducer(id string) func() {
	return c.lockKeyed(c.producerLocks, id)
}

func (c *Control) lockKeyed(locks map[string]*keyedMutex, id string) func() {
	c.mu.Lock()
	lock := locks[id]
	if lock == nil {
		lock = &keyedMutex{}
		locks[id] = lock
	}
	lock.refs++
	c.mu.Unlock()
	lock.mu.Lock()
	return func() {
		lock.mu.Unlock()
		c.mu.Lock()
		lock.refs--
		if lock.refs == 0 && locks[id] == lock {
			delete(locks, id)
		}
		c.mu.Unlock()
	}
}

func (c *Control) lockMutation(scope, key string) func() {
	if key == "" {
		return func() {}
	}
	name := scope + "\x00" + key
	c.mu.Lock()
	lock := c.mutationLocks[name]
	if lock == nil {
		lock = &keyedMutex{}
		c.mutationLocks[name] = lock
	}
	lock.refs++
	c.mu.Unlock()
	lock.mu.Lock()
	return func() {
		lock.mu.Unlock()
		c.mu.Lock()
		lock.refs--
		if lock.refs == 0 && c.mutationLocks[name] == lock {
			delete(c.mutationLocks, name)
		}
		c.mu.Unlock()
	}
}

func (c *Control) reserveTimer(workspace string) (func(), error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.timers)+c.timerReservations >= c.opts.MaxTimers {
		metrics.TimerQuotaRejected.Inc()
		return nil, proto.Err(proto.CodeResourceExhausted,
			"control timer limit %d reached", c.opts.MaxTimers)
	}
	workspaceTimers := c.timerReservationsByWS[workspace]
	for _, existing := range c.timers {
		if existing.WS == workspace {
			workspaceTimers++
		}
	}
	if workspaceTimers >= c.opts.MaxTimersPerWorkspace {
		metrics.TimerQuotaRejected.Inc()
		return nil, proto.Err(proto.CodeResourceExhausted,
			"workspace timer limit %d reached", c.opts.MaxTimersPerWorkspace)
	}
	c.timerReservations++
	c.timerReservationsByWS[workspace]++
	return func() {
		c.mu.Lock()
		c.timerReservations--
		if c.timerReservationsByWS[workspace] <= 1 {
			delete(c.timerReservationsByWS, workspace)
		} else {
			c.timerReservationsByWS[workspace]--
		}
		c.mu.Unlock()
	}, nil
}

func mutationFingerprint(request any) []byte {
	sum := sha256.Sum256(proto.MustMarshal(request))
	return sum[:]
}

func (c *Control) mutationLookup(scope, key, op string, request, out any) (bool, error) {
	if key == "" {
		return false, nil
	}
	var storedOp string
	var fingerprint, result []byte
	err := c.db.QueryRow(`SELECT op, fingerprint, result FROM mutations WHERE scope=? AND key=?`, scope, key).
		Scan(&storedOp, &fingerprint, &result)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	if storedOp != op || !bytes.Equal(fingerprint, mutationFingerprint(request)) {
		return false, proto.Err(proto.CodeConflict, "idempotency key was reused for a different operation or arguments")
	}
	if out != nil {
		if err := proto.Unmarshal(result, out); err != nil {
			return false, fmt.Errorf("decode idempotent %s result: %w", op, err)
		}
	}
	return true, nil
}

func (c *Control) insertMutationTx(tx *sql.Tx, scope, key, op string, request, result any) error {
	if key == "" {
		return nil
	}
	var count int
	if err := tx.QueryRow(`SELECT COUNT(*) FROM mutations`).Scan(&count); err != nil {
		return err
	}
	if count >= c.opts.MaxMutationRecords {
		metrics.MutationQuotaRejected.Inc()
		return proto.Err(proto.CodeResourceExhausted,
			"control idempotency record limit %d reached", c.opts.MaxMutationRecords)
	}
	_, err := tx.Exec(`INSERT INTO mutations(scope, key, op, fingerprint, result, completed_at) VALUES(?,?,?,?,?,?)`,
		scope, key, op, mutationFingerprint(request), proto.MustMarshal(result), c.now().UnixMilli())
	return err
}

const (
	releaseAttemptTimeout = 15 * time.Second
	releaseAbortTimeout   = 30 * time.Second
	releaseMaxAttempts    = 8
)

// prepareRelease retries the exact same generation-scoped request after an
// ambiguous transport failure. A duplicate request against a node that is
// still checkpointing returns Preparing, so polling cannot accumulate a set
// of request handlers all waiting for the same long snapshot.
func (c *Control) prepareRelease(ctx context.Context, node string, req proto.WSReleaseReq) (proto.WSReleasedReq, error) {
	var (
		res               proto.WSReleasedReq
		lastErr           error
		backoff           = 50 * time.Millisecond
		transientFailures int
	)
	for {
		if err := ctx.Err(); err != nil {
			if lastErr != nil {
				return proto.WSReleasedReq{}, errors.Join(lastErr, err)
			}
			return proto.WSReleasedReq{}, err
		}
		res = proto.WSReleasedReq{}
		attemptCtx, cancel := context.WithTimeout(ctx, releaseAttemptTimeout)
		err := c.send.Request(attemptCtx, node, proto.OpWSRelease, req, &res)
		cancel()
		if err == nil && !res.Preparing {
			return res, nil
		}
		if err == nil {
			lastErr = proto.Err(proto.CodeTimeout, "release preparation is still running")
		} else {
			lastErr = err
			if !retryableReleaseError(err) {
				return proto.WSReleasedReq{}, err
			}
			transientFailures++
			if transientFailures >= releaseMaxAttempts {
				return proto.WSReleasedReq{}, lastErr
			}
		}
		timer := time.NewTimer(backoff)
		select {
		case <-timer.C:
		case <-ctx.Done():
			if !timer.Stop() {
				<-timer.C
			}
			return proto.WSReleasedReq{}, errors.Join(lastErr, ctx.Err())
		}
		if backoff < 2*time.Second {
			backoff *= 2
			if backoff > 2*time.Second {
				backoff = 2 * time.Second
			}
		}
	}
}

func retryableReleaseError(err error) bool {
	if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, transport.ErrClosed) || errors.Is(err, io.EOF) {
		return true
	}
	var pe *proto.Error
	if !errors.As(err, &pe) {
		return false
	}
	switch pe.Code {
	case proto.CodeUnreachable, proto.CodeTimeout, proto.CodeClosed:
		return true
	default:
		return false
	}
}

// abortPreparedRelease reconciles every prepare-side failure. Only an
// acknowledged abort permits the control plane to publish the old assignment
// as claimed again; otherwise the workspace becomes failed and cannot be
// reassigned from an older snapshot while a prepared source may still exist.
func (c *Control) abortPreparedRelease(ctx context.Context, id, node string, gen uint64, expectedState string, cause error) error {
	abortCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), releaseAbortTimeout)
	defer cancel()
	abortReq := proto.WSReleaseCommitReq{ID: id, Gen: gen}
	var abortErr error
	backoff := 50 * time.Millisecond
	for {
		abortErr = c.send.Request(abortCtx, node, proto.OpWSReleaseAbort, abortReq, nil)
		if abortErr == nil || !retryableReleaseError(abortErr) {
			break
		}
		if abortCtx.Err() != nil {
			abortErr = errors.Join(abortErr, abortCtx.Err())
			break
		}
		timer := time.NewTimer(backoff)
		select {
		case <-timer.C:
		case <-abortCtx.Done():
			if !timer.Stop() {
				<-timer.C
			}
			abortErr = errors.Join(abortErr, abortCtx.Err())
		}
		if backoff < 2*time.Second {
			backoff *= 2
			if backoff > 2*time.Second {
				backoff = 2 * time.Second
			}
		}
		if abortCtx.Err() != nil {
			break
		}
	}

	c.mu.Lock()
	ws := c.workspaces[id]
	if ws == nil || ws.Node != node || ws.Generation != gen || ws.State != expectedState {
		state := "missing"
		if ws != nil {
			state = ws.State
		}
		c.mu.Unlock()
		return errors.Join(cause, abortErr, proto.Err(proto.CodeConflict, "workspace changed during release abort: %s", state))
	}
	to := proto.WSFailed
	if abortErr == nil {
		to = proto.WSClaimed
	}
	next, transitionErr := transitionWorkspace(ws, lifecycleTransition{
		operation: transitionReleaseAbort, actor: actorControl, to: to,
		expectGeneration: true, generation: gen, expectNode: true, node: node,
	})
	if transitionErr != nil {
		c.mu.Unlock()
		return errors.Join(cause, abortErr, transitionErr)
	}
	if abortErr == nil {
		next.LeaseUntil = c.now().Add(time.Duration(c.opts.LeaseSec) * time.Second).UnixMilli()
	} else {
		next.LeaseUntil = 0
	}
	if err := c.persistWS(&next, c.transitionEvent(expectedState, &next, lifecycleTransition{
		operation: transitionReleaseAbort, actor: actorControl, to: next.State,
		expectGeneration: true, generation: gen, expectNode: true, node: node,
	})); err != nil {
		c.mu.Unlock()
		return errors.Join(cause, abortErr, fmt.Errorf("persist release reconciliation: %w", err))
	}
	*ws = next
	c.mu.Unlock()
	if abortErr != nil {
		return errors.Join(cause, fmt.Errorf("release abort was not acknowledged; source fenced for reconciliation: %w", abortErr))
	}
	return cause
}

func (c *Control) wsDestroy(ctx context.Context, principal, id, idem string) error {
	unlock := c.lockLifecycle(id)
	defer unlock()
	scope := principal + "|" + id + "|workspace.destroy"
	unlockMutation := c.lockMutation(scope, idem)
	defer unlockMutation()
	request := proto.WSGetReq{ID: id, IdempotencyKey: idem}
	var prior struct{}
	if hit, err := c.mutationLookup(scope, idem, proto.OpWSDestroy, request, &prior); err != nil {
		return err
	} else if hit {
		return nil
	}
	c.mu.Lock()
	ws := c.workspaces[id]
	if ws == nil {
		c.mu.Unlock()
		return proto.Err(proto.CodeNotFound, "workspace %s", id)
	}
	if ws.State == proto.WSDestroyed {
		c.mu.Unlock()
		return nil
	}
	if ws.State == proto.WSFailed {
		c.mu.Unlock()
		return proto.Err(proto.CodeConflict, "workspace requires reconciliation before destroy")
	}
	node, gen := ws.Node, ws.Generation
	wasHeld := held(ws.State)
	beforeDestroy := ws.State
	if wasHeld {
		transition := lifecycleTransition{
			operation: transitionDestroyBegin, actor: actorControl, to: proto.WSDestroying,
			expectGeneration: true, generation: gen, expectNode: true, node: node,
		}
		next, transitionErr := transitionWorkspace(ws, transition)
		if transitionErr != nil {
			c.mu.Unlock()
			return transitionErr
		}
		if err := c.persistWS(&next, c.transitionEvent(beforeDestroy, &next, transition)); err != nil {
			c.mu.Unlock()
			return err
		}
		*ws = next
	}
	c.mu.Unlock()
	var prepared proto.WSReleasedReq
	if node != "" && c.send != nil && c.send.Online(node) {
		rctx, cancel := context.WithTimeout(ctx, 30*time.Second)
		var err error
		prepared, err = c.prepareRelease(rctx, node, proto.WSReleaseReq{WS: id, Gen: gen, Snapshot: false, Reason: "destroy"})
		cancel()
		if err != nil {
			return c.abortPreparedRelease(ctx, id, node, gen, proto.WSDestroying, err)
		}
		if prepared.ID != id || prepared.Gen != gen {
			err := proto.Err(proto.CodeConflict, "node returned a destroy result for a different workspace or generation")
			return c.abortPreparedRelease(ctx, id, node, gen, proto.WSDestroying, err)
		}
	}
	c.mu.Lock()
	if ws.Generation != gen || (wasHeld && ws.State != proto.WSDestroying) {
		state := ws.State
		c.mu.Unlock()
		return proto.Err(proto.CodeConflict, "workspace changed during destroy: %s", state)
	}
	destroyed, transitionErr := transitionWorkspace(ws, lifecycleTransition{
		operation: transitionDestroyCommit, actor: actorControl, to: proto.WSDestroyed,
		expectGeneration: true, generation: gen,
	})
	if transitionErr != nil {
		c.mu.Unlock()
		return transitionErr
	}
	destroyed.Node = ""
	destroyed.LeaseUntil = 0
	var timerUpdates []*proto.Timer
	for _, timer := range c.timers {
		if timer.WS == id && !timer.Fired {
			copyTimer := *timer
			copyTimer.Fired = true
			copyTimer.FiredAt = c.now().UnixMilli()
			timerUpdates = append(timerUpdates, &copyTimer)
		}
	}
	if err := c.persistWorkspaceTimersAndMutation(&destroyed, timerUpdates, scope, idem, proto.OpWSDestroy, request, struct{}{},
		c.wsEvent(&destroyed, proto.EvWSDestroyed, principal, node, nil)); err != nil {
		c.mu.Unlock()
		if prepared.ID != "" {
			return c.abortPreparedRelease(ctx, id, node, gen, proto.WSDestroying, err)
		}
		return err
	}
	*ws = destroyed
	for _, timer := range timerUpdates {
		*c.timers[timer.ID] = *timer
	}
	c.mu.Unlock()
	if prepared.ID != "" {
		commitCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 30*time.Second)
		err := c.send.Request(commitCtx, node, proto.OpWSReleaseCommit, proto.WSReleaseCommitReq{ID: id, Gen: gen}, nil)
		cancel()
		if err != nil {
			c.logger.Warn("destroy commit not acknowledged; fenced source retained", "ws", id, "node", node, "err", err)
		}
	}
	metrics.WSDestroyed.Inc()
	return nil
}

// release asks the current node to give a workspace up (optionally with a
// snapshot) and moves it to the given state. It is used by move, sleep and
// destroy. Returns the snapshot artifact id if one was taken.
func (c *Control) release(ctx context.Context, id string, snapshot bool, reason string) (string, error) {
	if c.send == nil {
		return "", proto.Err(proto.CodeUnreachable, "relay is not attached")
	}
	c.mu.Lock()
	ws := c.workspaces[id]
	if ws == nil {
		c.mu.Unlock()
		return "", proto.Err(proto.CodeNotFound, "workspace %s", id)
	}
	if ws.State != proto.WSClaimed {
		switch ws.State {
		case proto.WSPending, proto.WSReleased, proto.WSPaused:
			last := ws.LastSnapshot
			c.mu.Unlock()
			return last, nil
		case proto.WSDestroyed:
			c.mu.Unlock()
			return "", proto.Err(proto.CodeConflict, "workspace destroyed")
		default:
			state := ws.State
			c.mu.Unlock()
			return "", proto.Err(proto.CodeConflict, "workspace %s requires reconciliation from state %s", id, state)
		}
	}
	node, gen := ws.Node, ws.Generation
	begin := lifecycleTransition{
		operation: transitionReleaseBegin, actor: actorControl, to: proto.WSQuiescing,
		expectGeneration: true, generation: gen, expectNode: true, node: node,
	}
	next, transitionErr := transitionWorkspace(ws, begin)
	if transitionErr != nil {
		c.mu.Unlock()
		return "", transitionErr
	}
	if err := c.persistWS(&next, c.transitionEvent(proto.WSClaimed, &next, begin)); err != nil {
		c.mu.Unlock()
		return "", err
	}
	*ws = next
	c.mu.Unlock()
	var res proto.WSReleasedReq
	rctx, cancel := context.WithTimeout(ctx, 10*time.Minute)
	defer cancel()
	var err error
	res, err = c.prepareRelease(rctx, node, proto.WSReleaseReq{WS: id, Gen: gen, Snapshot: snapshot, Reason: reason})
	if err != nil {
		err = c.abortPreparedRelease(ctx, id, node, gen, proto.WSQuiescing, err)
		c.logger.Warn("release request failed", "ws", id, "node", node, "err", err)
		return "", err
	}
	if res.ID != id || res.Gen != gen {
		err := proto.Err(proto.CodeConflict, "node returned a release result for a different workspace or generation")
		return "", c.abortPreparedRelease(ctx, id, node, gen, proto.WSQuiescing, err)
	}
	if snapshot && res.Snapshot == "" {
		err := proto.Err(proto.CodeConflict, "node prepared release without required snapshot")
		return "", c.abortPreparedRelease(ctx, id, node, gen, proto.WSQuiescing, err)
	}
	if res.Snapshot != "" && c.opts.Artifacts != nil {
		if err := c.opts.Artifacts.Verify(res.Snapshot); err != nil {
			cause := proto.Err(proto.CodeConflict, "release snapshot verification failed: %v", err)
			return "", c.abortPreparedRelease(ctx, id, node, gen, proto.WSQuiescing, cause)
		}
	}
	c.mu.Lock()
	if ws.Node != node || ws.Generation != gen || ws.State != proto.WSQuiescing {
		state := ws.State
		c.mu.Unlock()
		return "", proto.Err(proto.CodeConflict, "workspace changed during release: %s", state)
	}
	committed, transitionErr := transitionWorkspace(ws, lifecycleTransition{
		operation: transitionReleaseCommit, actor: actorControl, to: proto.WSReleased,
		expectGeneration: true, generation: gen, expectNode: true, node: node,
	})
	if transitionErr != nil {
		c.mu.Unlock()
		return "", transitionErr
	}
	if res.Snapshot != "" {
		committed.LastSnapshot = res.Snapshot
		committed.Spec.RestoreFrom = res.Snapshot
	}
	last := committed.LastSnapshot
	if err := c.persistWS(&committed, c.wsEvent(&committed, proto.EvWSReleased, "", node, map[string]any{"reason": reason, "snapshot": res.Snapshot})); err != nil {
		c.mu.Unlock()
		return "", c.abortPreparedRelease(ctx, id, node, gen, proto.WSQuiescing, err)
	}
	*ws = committed
	c.mu.Unlock()
	commitCtx, commitCancel := context.WithTimeout(context.WithoutCancel(ctx), 30*time.Second)
	commitErr := c.send.Request(commitCtx, node, proto.OpWSReleaseCommit, proto.WSReleaseCommitReq{ID: id, Gen: gen, Snapshot: res.Snapshot}, nil)
	commitCancel()
	if commitErr != nil {
		// The checkpoint is already durable and authoritative. Retaining an
		// extra fenced source is safe; the node can reconcile it later.
		c.logger.Warn("release commit not acknowledged; source retained", "ws", id, "node", node, "err", commitErr)
	}
	return last, nil
}

func (c *Control) wsMove(ctx context.Context, principal string, req *proto.WSMoveReq) (*proto.Workspace, error) {
	unlock := c.lockLifecycle(req.ID)
	defer unlock()
	scope := principal + "|" + req.ID + "|workspace.move"
	unlockMutation := c.lockMutation(scope, req.IdempotencyKey)
	defer unlockMutation()
	var prior proto.Workspace
	if hit, err := c.mutationLookup(scope, req.IdempotencyKey, proto.OpWSMove, req, &prior); err != nil {
		return nil, err
	} else if hit {
		return &prior, nil
	}
	current, err := c.wsGet(req.ID)
	if err != nil {
		return nil, err
	}
	if current.State == proto.WSDestroyed {
		return nil, proto.Err(proto.CodeConflict, "workspace destroyed")
	}
	snap, err := c.release(ctx, req.ID, true, "move")
	if err != nil {
		return nil, err
	}
	c.mu.Lock()
	ws := c.workspaces[req.ID]
	if ws == nil || ws.State == proto.WSDestroyed {
		c.mu.Unlock()
		return nil, proto.Err(proto.CodeConflict, "workspace destroyed during move")
	}
	if ws.Generation != current.Generation {
		c.mu.Unlock()
		return nil, proto.Err(proto.CodeConflict, "workspace generation changed during move")
	}
	if held(current.State) && ws.State != proto.WSReleased {
		state := ws.State
		c.mu.Unlock()
		return nil, proto.Err(proto.CodeConflict, "workspace changed during move: %s", state)
	}
	if !held(current.State) && ws.State != current.State {
		state := ws.State
		c.mu.Unlock()
		return nil, proto.Err(proto.CodeConflict, "workspace changed during move: %s", state)
	}
	next, transitionErr := transitionWorkspace(ws, lifecycleTransition{
		operation: transitionMove, actor: actorControl, to: proto.WSPending,
		expectGeneration: true, generation: current.Generation,
	})
	if transitionErr != nil {
		c.mu.Unlock()
		return nil, transitionErr
	}
	if req.Requires != nil {
		next.Spec.Requires = *req.Requires
	}
	if req.Placement != nil {
		next.Spec.Placement = *req.Placement
	}
	next.Spec.RestoreFrom = snap
	next.Node = ""
	next.LeaseUntil = 0
	if err := c.persistWSAndMutation(&next, scope, req.IdempotencyKey, proto.OpWSMove, req, &next,
		c.wsEvent(&next, proto.EvWSMoved, principal, "", map[string]any{"restore_from": snap})); err != nil {
		c.mu.Unlock()
		return nil, err
	}
	*ws = next
	c.mu.Unlock()
	metrics.WSMoved.Inc()
	c.offerPending(ctx)
	return c.snapshotWS(req.ID), nil
}

func (c *Control) wsSleep(ctx context.Context, principal string, req *proto.WSSleepReq) (*proto.Timer, error) {
	unlock := c.lockLifecycle(req.ID)
	defer unlock()
	scope := principal + "|" + req.ID + "|workspace.sleep"
	unlockMutation := c.lockMutation(scope, req.IdempotencyKey)
	defer unlockMutation()
	var prior proto.Timer
	if hit, err := c.mutationLookup(scope, req.IdempotencyKey, proto.OpWSSleep, req, &prior); err != nil {
		return nil, err
	} else if hit {
		return &prior, nil
	}
	current, err := c.wsGet(req.ID)
	if err != nil {
		return nil, err
	}
	if current.State == proto.WSDestroyed {
		return nil, proto.Err(proto.CodeConflict, "workspace destroyed")
	}
	if req.AfterSec == 0 && req.AtMillis == 0 && req.OnEvent == "" {
		return nil, proto.Err(proto.CodeBadRequest, "sleep needs after_sec, at or on")
	}
	releaseTimerReservation, err := c.reserveTimer(req.ID)
	if err != nil {
		return nil, err
	}
	defer releaseTimerReservation()
	snap, err := c.release(ctx, req.ID, true, "sleep")
	if err != nil {
		return nil, err
	}
	t := &proto.Timer{ID: ids.New("t"), WS: req.ID, Action: "resume", CreatedAt: c.now().UnixMilli(), OnEvent: req.OnEvent}
	if req.AfterSec > 0 {
		t.At = c.now().Add(time.Duration(req.AfterSec) * time.Second).UnixMilli()
	} else if req.AtMillis > 0 {
		t.At = req.AtMillis
	}
	c.mu.Lock()
	ws := c.workspaces[req.ID]
	if ws == nil || ws.State == proto.WSDestroyed {
		c.mu.Unlock()
		return nil, proto.Err(proto.CodeConflict, "workspace destroyed during sleep")
	}
	if ws.Generation != current.Generation {
		c.mu.Unlock()
		return nil, proto.Err(proto.CodeConflict, "workspace generation changed during sleep")
	}
	if held(current.State) && ws.State != proto.WSReleased {
		state := ws.State
		c.mu.Unlock()
		return nil, proto.Err(proto.CodeConflict, "workspace changed during sleep: %s", state)
	}
	if !held(current.State) && ws.State != current.State {
		state := ws.State
		c.mu.Unlock()
		return nil, proto.Err(proto.CodeConflict, "workspace changed during sleep: %s", state)
	}
	next, transitionErr := transitionWorkspace(ws, lifecycleTransition{
		operation: transitionSleep, actor: actorControl, to: proto.WSPaused,
		expectGeneration: true, generation: current.Generation,
	})
	if transitionErr != nil {
		c.mu.Unlock()
		return nil, transitionErr
	}
	next.Spec.RestoreFrom = snap
	next.Node = ""
	next.LeaseUntil = 0
	tcp := *t
	if err := c.persistWorkspaceTimerAndMutation(&next, t, scope, req.IdempotencyKey, proto.OpWSSleep, req, t,
		c.wsEvent(&next, proto.EvTimerSet, principal, "", tcp),
		c.wsEvent(&next, proto.EvWSPaused, principal, "", map[string]any{"timer": tcp.ID, "snapshot": snap})); err != nil {
		c.mu.Unlock()
		return nil, err
	}
	*ws = next
	c.timers[t.ID] = t
	c.mu.Unlock()
	return &tcp, nil
}

// wsACL replaces the ACL and advances the authorization revision so every
// outstanding grant stops verifying. Principals that lost access are recorded
// so nodes learn whose sessions to close on their next renew; everyone else
// simply fetches a fresh grant.
func (c *Control) wsACL(ctx context.Context, principal string, req *proto.WSACLReq) (*proto.Workspace, error) {
	for _, name := range append(append([]string(nil), req.ACL.Readers...), req.ACL.Writers...) {
		if name == "" {
			return nil, proto.Err(proto.CodeBadRequest, "acl entries must name a principal")
		}
	}
	scope := principal + "|" + req.ID + "|workspace.acl"
	unlockMutation := c.lockMutation(scope, req.IdempotencyKey)
	defer unlockMutation()
	var prior proto.Workspace
	if hit, err := c.mutationLookup(scope, req.IdempotencyKey, proto.OpWSACL, req, &prior); err != nil {
		return nil, err
	} else if hit {
		return &prior, nil
	}
	c.mu.Lock()
	ws := c.workspaces[req.ID]
	if ws == nil {
		c.mu.Unlock()
		return nil, proto.Err(proto.CodeNotFound, "workspace %s", req.ID)
	}
	if ws.State == proto.WSDestroyed {
		c.mu.Unlock()
		return nil, proto.Err(proto.CodeConflict, "workspace destroyed")
	}
	next := *ws
	next.Spec.ACL = proto.WorkspaceACL{
		Readers: append([]string(nil), req.ACL.Readers...),
		Writers: append([]string(nil), req.ACL.Writers...),
	}
	next.AuthzRevision++
	revoked := revokedPrincipals(ws.Spec.ACL, next.Spec.ACL, ws.Owner)
	next.Revocations = append([]proto.AuthzRevocation(nil), ws.Revocations...)
	for _, name := range revoked {
		next.Revocations = append(next.Revocations, proto.AuthzRevocation{Revision: next.AuthzRevision, Principal: name})
	}
	for len(next.Revocations) > proto.MaxRetainedRevocations {
		next.RevocationFloor = next.Revocations[0].Revision
		next.Revocations = next.Revocations[1:]
	}
	payload := map[string]any{
		"readers": next.Spec.ACL.Readers, "writers": next.Spec.ACL.Writers,
		"revoked": revoked, "authz_revision": next.AuthzRevision,
	}
	if err := c.persistWorkspaceTimerAndMutation(&next, nil, scope, req.IdempotencyKey, proto.OpWSACL, req, &next,
		c.wsEvent(&next, proto.EvWSACL, principal, "", payload)); err != nil {
		c.mu.Unlock()
		return nil, err
	}
	*ws = next
	result := next
	c.mu.Unlock()
	return &result, nil
}

// revokedPrincipals names the subjects that could use the workspace under
// before and cannot under after. The owner never appears: ownership is not an
// ACL entry.
func revokedPrincipals(before, after proto.WorkspaceACL, owner string) []string {
	var revoked []string
	seen := map[string]bool{}
	for _, name := range append(append([]string(nil), before.Readers...), before.Writers...) {
		if name == owner || seen[name] || contains(after.Readers, name) || contains(after.Writers, name) {
			continue
		}
		seen[name] = true
		revoked = append(revoked, name)
	}
	return revoked
}

func (c *Control) wsWake(ctx context.Context, principal, id, timerID, idem string) (*proto.Workspace, error) {
	unlock := c.lockLifecycle(id)
	defer unlock()
	scope := principal + "|" + id + "|workspace.wake"
	unlockMutation := c.lockMutation(scope, idem)
	defer unlockMutation()
	request := proto.WSGetReq{ID: id, IdempotencyKey: idem}
	var prior proto.Workspace
	if hit, err := c.mutationLookup(scope, idem, proto.OpWSWake, request, &prior); err != nil {
		return nil, err
	} else if hit {
		return &prior, nil
	}
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
	var nextTimer *proto.Timer
	if timerID != "" {
		timer := c.timers[timerID]
		if timer == nil || timer.WS != id {
			c.mu.Unlock()
			return nil, proto.Err(proto.CodeNotFound, "timer %s", timerID)
		}
		copyTimer := *timer
		copyTimer.Fired = true
		copyTimer.FiredAt = c.now().UnixMilli()
		nextTimer = &copyTimer
	}
	next := *ws
	resumed := ws.State == proto.WSPaused
	if resumed {
		var transitionErr error
		next, transitionErr = transitionWorkspace(ws, lifecycleTransition{
			operation: transitionWake, actor: actorControl, to: proto.WSPending,
			expectGeneration: true, generation: ws.Generation,
		})
		if transitionErr != nil {
			c.mu.Unlock()
			return nil, transitionErr
		}
	}
	if !resumed && (nextTimer == nil || c.timers[timerID].Fired) {
		cp := next
		c.mu.Unlock()
		return &cp, nil
	}
	var events []*proto.Event
	if nextTimer != nil {
		events = append(events, c.wsEvent(&next, proto.EvTimerFired, principal, "", *nextTimer))
	}
	if resumed {
		events = append(events, c.wsEvent(&next, proto.EvWSResumed, principal, "", map[string]any{"timer": timerID}))
	}
	if err := c.persistWorkspaceTimerAndMutation(&next, nextTimer, scope, idem, proto.OpWSWake, request, &next, events...); err != nil {
		c.mu.Unlock()
		return nil, err
	}
	*ws = next
	if nextTimer != nil {
		*c.timers[timerID] = *nextTimer
	}
	cp := next
	c.mu.Unlock()
	if nextTimer != nil {
		metrics.TimersFired.Inc()
	}
	if resumed {
		c.offerPending(ctx)
	}
	return &cp, nil
}

func (c *Control) wsClaim(ctx context.Context, node, id string) (*proto.WSClaimRes, error) {
	c.mu.Lock()
	ws := c.workspaces[id]
	if ws == nil {
		c.mu.Unlock()
		return nil, proto.Err(proto.CodeNotFound, "workspace %s", id)
	}
	if (ws.State == proto.WSClaimed || ws.State == proto.WSClaiming) && ws.Node == node {
		// Re-adoption: the same node reconnecting still holds this workspace.
		// Keep the generation so the client's outstanding grants stay valid,
		// but go back through claiming so waiters do not race the restore.
		before := ws.State
		readopt := lifecycleTransition{
			operation: transitionClaim, actor: actorNode, to: proto.WSClaiming,
			expectGeneration: true, generation: ws.Generation,
			expectNode: true, node: node,
		}
		next, transitionErr := transitionWorkspace(ws, readopt)
		if transitionErr != nil {
			c.mu.Unlock()
			return nil, transitionErr
		}
		next.LeaseUntil = c.now().Add(time.Duration(c.opts.LeaseSec) * time.Second).UnixMilli()
		if err := c.persistClaim(&next, c.transitionEvent(before, &next, readopt)); err != nil {
			c.mu.Unlock()
			return nil, err
		}
		*ws = next
		c.mu.Unlock()
		return &proto.WSClaimRes{Workspace: next, LeaseSec: c.opts.LeaseSec}, nil
	}
	if ws.State != proto.WSPending {
		// Read the state before releasing the lock: formatting the message
		// after the unlock is a read of shared memory.
		state := ws.State
		c.mu.Unlock()
		return nil, proto.Err(proto.CodeConflict, "workspace %s is %s", id, state)
	}
	n := c.nodes[node]
	backend, eligible := c.eligibleBackendLocked(ws, n)
	if n == nil || !eligible {
		c.mu.Unlock()
		metrics.WSClaimDenied.Inc()
		return nil, proto.Err(proto.CodeDenied, "node %s is not eligible for %s", node, id)
	}
	// The compare-and-swap: state was pending under the lock; now it's ours.
	// It becomes WSClaimed only once the node reports ws.ready, so a client
	// never talks to a node that is still restoring the filesystem.
	next, transitionErr := transitionWorkspace(ws, lifecycleTransition{
		operation: transitionClaim, actor: actorNode, to: proto.WSClaiming,
		expectGeneration: true, generation: ws.Generation,
	})
	if transitionErr != nil {
		c.mu.Unlock()
		return nil, transitionErr
	}
	if next.Spec.Requires.Backend == "" {
		next.Spec.Requires.Backend = backend
	}
	next.Node = node
	next.Generation, transitionErr = nextWorkspaceGeneration(next.Generation)
	if transitionErr != nil {
		c.mu.Unlock()
		return nil, transitionErr
	}
	next.LeaseUntil = c.now().Add(time.Duration(c.opts.LeaseSec) * time.Second).UnixMilli()
	if err := c.persistClaim(&next, c.wsEvent(&next, proto.EvWSClaiming, "", node, map[string]any{"gen": next.Generation, "restore_from": next.Spec.RestoreFrom})); err != nil {
		c.mu.Unlock()
		return nil, err
	}
	*ws = next
	c.mu.Unlock()
	metrics.WSClaims.Inc()
	return &proto.WSClaimRes{Workspace: next, LeaseSec: c.opts.LeaseSec}, nil
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
		next, transitionErr := transitionWorkspace(ws, lifecycleTransition{
			operation: transitionReady, actor: actorNode, to: proto.WSClaimed,
			expectGeneration: true, generation: req.Gen,
			expectNode: true, node: node,
		})
		if transitionErr != nil {
			c.mu.Unlock()
			return transitionErr
		}
		next.LeaseUntil = c.now().Add(time.Duration(c.opts.LeaseSec) * time.Second).UnixMilli()
		if err := c.persistWS(&next); err != nil {
			c.mu.Unlock()
			return err
		}
		*ws = next
		c.mu.Unlock()
		return nil
	}
	if ws.State != proto.WSClaiming {
		state := ws.State
		c.mu.Unlock()
		return proto.Err(proto.CodeConflict, "workspace %s cannot become ready from %s", req.ID, state)
	}
	next, transitionErr := transitionWorkspace(ws, lifecycleTransition{
		operation: transitionReady, actor: actorNode, to: proto.WSClaimed,
		expectGeneration: true, generation: req.Gen,
		expectNode: true, node: node,
	})
	if transitionErr != nil {
		c.mu.Unlock()
		return transitionErr
	}
	next.LeaseUntil = c.now().Add(time.Duration(c.opts.LeaseSec) * time.Second).UnixMilli()
	if err := c.persistWS(&next, c.wsEvent(&next, proto.EvWSClaimed, "", node, map[string]any{"gen": next.Generation, "restore_from": next.Spec.RestoreFrom})); err != nil {
		c.mu.Unlock()
		return err
	}
	*ws = next
	delete(c.retries, ws.ID)
	c.mu.Unlock()
	return nil
}

// materializeRetry holds a workspace out of placement after a node reported
// that materializing it failed. Without it a clone or restore that cannot
// succeed is re-offered the instant it is released and the fleet burns
// generations in a tight loop; with it the retry cadence doubles from one
// second to a thirty second ceiling and resets on the first successful claim.
type materializeRetry struct {
	failures  int
	notBefore int64 // unix ms
}

const (
	materializeRetryBase = time.Second
	materializeRetryMax  = 30 * time.Second
)

// holdBackLocked records one failed materialization and returns the delay
// before the workspace may be offered again. Caller holds c.mu.
func (c *Control) holdBackLocked(wsID string) time.Duration {
	r := c.retries[wsID]
	if r == nil {
		r = &materializeRetry{}
		c.retries[wsID] = r
	}
	delay := materializeRetryBase << r.failures
	if delay > materializeRetryMax || delay <= 0 {
		delay = materializeRetryMax
	}
	if r.failures < 16 {
		r.failures++
	}
	r.notBefore = c.now().Add(delay).UnixMilli()
	return delay
}

func (c *Control) wsRenew(ctx context.Context, node string, req *proto.WSRenewReq) (*proto.WSRenewRes, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	until := c.now().Add(time.Duration(c.opts.LeaseSec) * time.Second).UnixMilli()
	res := &proto.WSRenewRes{Results: make([]proto.WSRenewResult, 0, len(req.IDs))}
	for _, id := range req.IDs {
		requested := req.Gen[id]
		result := proto.WSRenewResult{ID: id, Generation: requested, Action: "fence"}
		ws := c.workspaces[id]
		if ws == nil {
			result.Action = "destroy"
			res.Results = append(res.Results, result)
			continue
		}
		result.AuthoritativeGen = ws.Generation
		renewable := ws.State == proto.WSClaimed || ws.State == proto.WSClaiming
		if ws.Node != node || !renewable || requested == 0 || requested != ws.Generation {
			res.Results = append(res.Results, result)
			continue
		}
		next := *ws
		next.LeaseUntil = until
		if err := c.persistWS(&next); err != nil {
			return nil, err
		}
		*ws = next
		result.Accepted = true
		result.LeaseUntil = until
		result.Action = "continue"
		result.AuthzRevision = ws.AuthzRevision
		if known, ok := req.Authz[id]; ok && known < ws.AuthzRevision {
			result.Revoked, result.AuthzReset = revokedSince(ws, known)
		}
		res.Results = append(res.Results, result)
	}
	if n := c.nodes[node]; n != nil {
		n.Status.LastSeen = c.now().UnixMilli()
	}
	return res, nil
}

// revokedSince lists principals revoked after revision known, or reports a
// reset when the retained history no longer reaches back that far.
func revokedSince(ws *proto.Workspace, known uint64) ([]string, bool) {
	if known < ws.RevocationFloor {
		return nil, true
	}
	var revoked []string
	for _, r := range ws.Revocations {
		if r.Revision > known && !contains(revoked, r.Principal) {
			revoked = append(revoked, r.Principal)
		}
	}
	return revoked, false
}

func (c *Control) wsSnapshotCommit(ctx context.Context, node string, req *proto.WSSnapshotCommitReq) error {
	if req.Snapshot == "" {
		return proto.Err(proto.CodeBadRequest, "snapshot id is required")
	}
	if c.opts.Artifacts != nil {
		if err := c.opts.Artifacts.Verify(req.Snapshot); err != nil {
			return proto.Err(proto.CodeConflict, "snapshot verification failed: %v", err)
		}
	}
	c.mu.Lock()
	ws := c.workspaces[req.ID]
	if ws == nil {
		c.mu.Unlock()
		return proto.Err(proto.CodeNotFound, "workspace %s", req.ID)
	}
	// Client-initiated authoritative checkpoints are only valid while the
	// workspace is steadily claimed. Once a release, fleet checkpoint, or
	// destroy transition begins, that lifecycle operation owns the next
	// authoritative snapshot and a concurrent client commit must lose.
	if ws.Node != node || ws.Generation != req.Gen || ws.State != proto.WSClaimed {
		c.mu.Unlock()
		return proto.Err(proto.CodeConflict, "stale snapshot commit for %s", req.ID)
	}
	next := *ws
	next.LastSnapshot = req.Snapshot
	if err := c.persistWS(&next, c.wsEvent(&next, proto.EvWSSnapshot, "", node, map[string]any{"artifact": req.Snapshot, "committed": true})); err != nil {
		c.mu.Unlock()
		return err
	}
	*ws = next
	c.mu.Unlock()
	return nil
}

func (c *Control) wsReleased(ctx context.Context, node string, req *proto.WSReleasedReq) error {
	if req.Snapshot != "" && c.opts.Artifacts != nil {
		if err := c.opts.Artifacts.Verify(req.Snapshot); err != nil {
			return proto.Err(proto.CodeConflict, "released snapshot verification failed: %v", err)
		}
	}
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
	if ws.State == proto.WSDestroyed {
		c.mu.Unlock()
		return proto.Err(proto.CodeConflict, "workspace %s is destroyed", req.ID)
	}
	next := *ws
	if req.Snapshot != "" {
		next.LastSnapshot = req.Snapshot
		next.Spec.RestoreFrom = req.Snapshot
	}
	if held(next.State) {
		var transitionErr error
		next, transitionErr = transitionWorkspace(ws, lifecycleTransition{
			operation: transitionNodeReleased, actor: actorNode, to: proto.WSPending,
			expectGeneration: true, generation: req.Gen,
			expectNode: true, node: node,
		})
		if transitionErr != nil {
			c.mu.Unlock()
			return transitionErr
		}
	}
	next.Node = ""
	next.LeaseUntil = 0
	payload := map[string]any{"reason": req.Reason, "snapshot": req.Snapshot}
	if req.Failed {
		payload["retry_after_ms"] = c.holdBackLocked(ws.ID).Milliseconds()
	}
	if err := c.persistWS(&next, c.wsEvent(&next, proto.EvWSReleased, "", node, payload)); err != nil {
		c.mu.Unlock()
		return err
	}
	*ws = next
	state := next.State
	c.mu.Unlock()
	if state == proto.WSPending {
		c.offerPending(ctx)
	}
	return nil
}

// eligibleLocked applies Requires and Placement against a node.
func (c *Control) eligibleLocked(ws *proto.Workspace, n *nodeState) bool {
	_, ok := c.eligibleBackendLocked(ws, n)
	return ok
}

// eligibleBackendLocked returns the exact backend that satisfies both
// runtime requirements and the security policy. Keeping this decision
// backend-specific prevents a process backend from inheriting properties of
// a stronger backend registered on the same node.
func (c *Control) eligibleBackendLocked(ws *proto.Workspace, n *nodeState) (string, bool) {
	if n == nil || !n.Status.Online {
		return "", false
	}
	r := ws.Spec.Requires
	if r.OS != "" && n.Status.Info.OS != r.OS {
		return "", false
	}
	if r.Arch != "" && n.Status.Info.Arch != r.Arch {
		return "", false
	}
	if r.CPU > 0 && n.Status.Info.CPU < r.CPU {
		return "", false
	}
	if r.MemMiB > 0 && n.Status.Info.MemMiB < r.MemMiB {
		return "", false
	}
	for _, cap := range r.Caps {
		if !contains(n.Status.Info.Caps, cap) {
			return "", false
		}
	}
	if len(proto.MissingCapabilities(n.Status.Protocol, proto.SecurityCapabilities(ws.Spec.Security.Profile))) > 0 {
		return "", false
	}
	for _, rule := range ws.Spec.Security.Network.Rules {
		if rule.Connector != "" && !contains(n.Status.Info.Connectors, rule.Connector) {
			return "", false
		}
	}
	p := ws.Spec.Placement
	if p.Node != "" && p.Node != n.Status.ID {
		return "", false
	}
	for k, v := range p.Allow {
		if n.Status.Labels[k] != v {
			return "", false
		}
	}
	descriptors := n.Status.Info.BackendDescriptors
	if len(descriptors) == 0 {
		// Compatibility for v1 peers: absence is evidence only for a local,
		// non-isolating backend. Never infer stronger security properties.
		for _, name := range n.Status.Info.Backends {
			descriptors = append(descriptors, proto.BackendDescriptor{
				Name: name, Security: proto.BackendSecurityCaps{Isolation: "none", EgressMode: "open", BrokerIdentity: "none"},
				Runtime: proto.RuntimeCaps{Snapshots: n.Status.Info.Snapshots},
			})
		}
	}
	for _, descriptor := range descriptors {
		if r.Backend != "" && descriptor.Name != r.Backend {
			continue
		}
		// A backend without its own mount namespace would have to symlink a
		// host path into the jail to honor MountPath; refuse instead.
		if ws.Spec.MountPath != "" && !descriptor.Runtime.MountPath {
			continue
		}
		if err := proto.ValidateBackendSecurity(ws.Spec.Security, descriptor); err == nil {
			return descriptor.Name, true
		}
	}
	return "", false
}

// held reports whether a node currently owns the workspace (materializing
// or serving). Both states carry a lease.
func held(state string) bool {
	switch state {
	case proto.WSClaimed, proto.WSClaiming, proto.WSQuiescing, proto.WSCheckpointing, proto.WSDestroying:
		return true
	default:
		return false
	}
}

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
	now := c.now().UnixMilli()
	c.mu.Lock()
	for _, ws := range c.workspaces {
		if ws.State != proto.WSPending {
			continue
		}
		if r := c.retries[ws.ID]; r != nil && r.notBefore > now {
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
// durable fleet quarantine
// ---------------------------------------------------------------------------

func cloneFleetOperation(in *proto.FleetOperation) *proto.FleetOperation {
	if in == nil {
		return nil
	}
	out := *in
	out.Selector.Labels = cloneMap(in.Selector.Labels)
	out.Results = append([]proto.FleetOperationResult(nil), in.Results...)
	return &out
}

func selectorSpecified(selector proto.WorkspaceSelector) bool {
	return selector.All || selector.Tenant != "" || selector.Principal != "" || selector.Run != "" ||
		selector.Node != "" || selector.Model != "" || selector.Backend != "" || len(selector.Labels) > 0 ||
		selector.CreatedAfter != 0 || selector.CreatedBefore != 0
}

func validFleetAction(action string) bool {
	switch action {
	case proto.FleetActionFreeze, proto.FleetActionRevokeEgress, proto.FleetActionCheckpoint,
		proto.FleetActionStop, proto.FleetActionDestroy:
		return true
	default:
		return false
	}
}

const maxFleetOperationDuration = 24 * time.Hour

func fleetStateTerminal(state string) bool {
	return state == proto.FleetStateCompleted || state == proto.FleetStatePartial
}

func fleetHasPending(operation *proto.FleetOperation) bool {
	for _, result := range operation.Results {
		if result.State == proto.FleetTargetPending {
			return true
		}
	}
	return false
}

func fleetTarget(operation *proto.FleetOperation, workspace string) (proto.FleetOperationResult, bool) {
	if operation != nil {
		for _, result := range operation.Results {
			if result.Workspace == workspace {
				return result, true
			}
		}
	}
	return proto.FleetOperationResult{}, false
}

func (c *Control) fleetRPCContext(parent context.Context, operation *proto.FleetOperation) (context.Context, context.CancelFunc) {
	timeout := 20 * time.Second
	if operation.State != proto.FleetStatePartial {
		remainingMillis := operation.Deadline - c.now().UnixMilli()
		if remainingMillis <= 0 {
			ctx, cancel := context.WithCancel(parent)
			cancel()
			return ctx, func() {}
		}
		if remainingMillis < timeout.Milliseconds() {
			timeout = time.Duration(remainingMillis) * time.Millisecond
		}
	}
	return context.WithTimeout(parent, timeout)
}

func workspaceMatches(selector proto.WorkspaceSelector, ws *proto.Workspace, backend string) bool {
	if ws == nil || ws.State == proto.WSDestroyed {
		return false
	}
	if selector.Tenant != "" && selector.Tenant != ws.Tenant {
		return false
	}
	if selector.Principal != "" && selector.Principal != ws.Spec.Principal && selector.Principal != ws.Owner {
		return false
	}
	if selector.Run != "" && selector.Run != ws.Spec.Run {
		return false
	}
	if selector.Node != "" && selector.Node != ws.Node {
		return false
	}
	if selector.Model != "" && selector.Model != ws.Spec.Model {
		return false
	}
	if selector.Backend != "" && selector.Backend != backend {
		return false
	}
	if selector.CreatedAfter != 0 && ws.CreatedAt < selector.CreatedAfter {
		return false
	}
	if selector.CreatedBefore != 0 && ws.CreatedAt > selector.CreatedBefore {
		return false
	}
	for key, value := range selector.Labels {
		if ws.Spec.Labels[key] != value {
			return false
		}
	}
	return true
}

func (c *Control) fleetQuarantine(ctx context.Context, subject Subject, request *proto.FleetQuarantineReq) (*proto.FleetOperation, error) {
	req := *request
	req.Selector.Labels = cloneMap(request.Selector.Labels)
	if req.Action == "" {
		req.Action = proto.FleetActionFreeze
	}
	if !validFleetAction(req.Action) {
		return nil, proto.Err(proto.CodeBadRequest, "unknown fleet action %q", req.Action)
	}
	if req.IdempotencyKey == "" {
		return nil, proto.Err(proto.CodeBadRequest, "fleet quarantine requires an idempotency key")
	}
	if !selectorSpecified(req.Selector) {
		return nil, proto.Err(proto.CodeBadRequest, "fleet quarantine requires a selector or selector.all=true")
	}
	if req.Selector.CreatedAfter != 0 && req.Selector.CreatedBefore != 0 &&
		req.Selector.CreatedAfter > req.Selector.CreatedBefore {
		return nil, proto.Err(proto.CodeBadRequest, "fleet selector creation-time range is inverted")
	}
	if !hasRole(subject, "admin") {
		if req.Selector.Tenant == "" {
			req.Selector.Tenant = subject.Tenant
		} else if req.Selector.Tenant != subject.Tenant {
			return nil, proto.Err(proto.CodeDenied, "tenant administrator cannot quarantine another tenant")
		}
	}
	now := c.now().UnixMilli()
	if req.DeadlineMillis != 0 && req.TimeoutMillis != 0 {
		return nil, proto.Err(proto.CodeBadRequest, "fleet quarantine accepts deadline or timeout, not both")
	}
	maxDurationMillis := maxFleetOperationDuration.Milliseconds()
	deadline := req.DeadlineMillis
	if req.TimeoutMillis < 0 || req.TimeoutMillis > maxDurationMillis {
		return nil, proto.Err(proto.CodeBadRequest, "fleet quarantine timeout must be between 1ms and %s", maxFleetOperationDuration)
	}
	if req.TimeoutMillis > 0 {
		deadline = now + req.TimeoutMillis
	} else if deadline == 0 {
		deadline = now + (5 * time.Minute).Milliseconds()
	}
	if deadline <= now || deadline-now > maxDurationMillis {
		return nil, proto.Err(proto.CodeBadRequest, "fleet quarantine deadline must be in the future")
	}
	scope := subject.Tenant + "|" + subject.ID + "|fleet.quarantine"
	unlockMutation := c.lockMutation(scope, req.IdempotencyKey)
	defer unlockMutation()
	var prior mutationFleetResult
	if hit, err := c.mutationLookup(scope, req.IdempotencyKey, proto.OpFleetQuarantine, req, &prior); err != nil {
		return nil, err
	} else if hit {
		return c.fleetGet(ctx, subject, prior.ID)
	}

	operationTenant := req.Selector.Tenant
	if operationTenant == "" && !hasRole(subject, "admin") {
		operationTenant = subject.Tenant
	}
	operation := &proto.FleetOperation{
		ID: ids.New("fleet"), Selector: req.Selector, Action: req.Action,
		RequestedBy: subject.ID, Tenant: operationTenant, State: proto.FleetStatePending,
		CreatedAt: now, UpdatedAt: now, Deadline: deadline,
	}
	c.mu.Lock()
	for _, ws := range c.workspaces {
		backend := ws.Spec.Requires.Backend
		if backend == "" && ws.Node != "" {
			backend, _ = c.eligibleBackendLocked(ws, c.nodes[ws.Node])
		}
		if workspaceMatches(req.Selector, ws, backend) {
			operation.Results = append(operation.Results, proto.FleetOperationResult{
				Workspace: ws.ID, Node: ws.Node, Backend: backend,
				Generation: ws.Generation, State: proto.FleetTargetPending, UpdatedAt: now,
			})
		}
	}
	sort.Slice(operation.Results, func(i, j int) bool {
		return operation.Results[i].Workspace < operation.Results[j].Workspace
	})
	if len(operation.Results) == 0 {
		operation.State = proto.FleetStateCompleted
	}
	events := []*proto.Event{c.fleetEvent(proto.EvFleetRequested, operation.ID, "", operation, nil, map[string]any{
		"action": operation.Action, "selector": operation.Selector, "targets": len(operation.Results),
	})}
	if operation.State == proto.FleetStateCompleted {
		events = append(events, c.fleetEvent(proto.EvFleetCompleted, operation.ID, "", operation, nil, map[string]any{
			"state": operation.State, "targets": 0,
		}))
	}
	if err := c.persistFleetOperationAndMutation(operation, scope, req.IdempotencyKey, req, events...); err != nil {
		c.mu.Unlock()
		return nil, err
	}
	c.fleetOps[operation.ID] = operation
	out := cloneFleetOperation(operation)
	c.mu.Unlock()
	metrics.FleetOperations.Inc()
	if operation.State == proto.FleetStateCompleted {
		metrics.FleetOperationsCompleted.Inc()
	} else {
		c.signalFleet()
	}
	return out, nil
}

func (c *Control) fleetGet(ctx context.Context, subject Subject, id string) (*proto.FleetOperation, error) {
	c.mu.Lock()
	operation := cloneFleetOperation(c.fleetOps[id])
	c.mu.Unlock()
	if operation == nil {
		return nil, proto.Err(proto.CodeNotFound, "fleet operation %s", id)
	}
	if err := c.check(ctx, subject, ActionAdmin,
		Resource{Kind: "fleet-operation", ID: id, Tenant: operation.Tenant, Owner: operation.RequestedBy}); err != nil {
		return nil, err
	}
	return operation, nil
}

func (c *Control) fleetList(ctx context.Context, subject Subject) (*proto.FleetListRes, error) {
	c.mu.Lock()
	operations := make([]*proto.FleetOperation, 0, len(c.fleetOps))
	for _, operation := range c.fleetOps {
		operations = append(operations, cloneFleetOperation(operation))
	}
	c.mu.Unlock()
	out := &proto.FleetListRes{}
	for _, operation := range operations {
		if c.check(ctx, subject, ActionAdmin, Resource{
			Kind: "fleet-operation", ID: operation.ID, Tenant: operation.Tenant, Owner: operation.RequestedBy,
		}) == nil {
			out.Operations = append(out.Operations, *operation)
		}
	}
	sort.Slice(out.Operations, func(i, j int) bool { return out.Operations[i].CreatedAt < out.Operations[j].CreatedAt })
	return out, nil
}

func (c *Control) signalFleet() {
	select {
	case c.fleetWake <- struct{}{}:
	default:
	}
}

func (c *Control) lockFleet(id string) func() {
	return c.lockKeyed(c.fleetLocks, id)
}

func (c *Control) fleetLoop() {
	defer c.wg.Done()
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		c.runFleetPending(c.requestCtx)
		select {
		case <-c.stop:
			return
		case <-c.fleetWake:
		case <-ticker.C:
		}
	}
}

func (c *Control) runFleetPending(ctx context.Context) {
	c.mu.Lock()
	var ids []string
	for id, operation := range c.fleetOps {
		if operation.State == proto.FleetStatePending || operation.State == proto.FleetStateRunning ||
			(operation.State == proto.FleetStatePartial && fleetHasPending(operation)) {
			ids = append(ids, id)
		}
	}
	c.mu.Unlock()
	sort.Strings(ids)
	for _, id := range ids {
		if ctx.Err() != nil {
			return
		}
		c.runFleetOperation(ctx, id)
	}
}

func (c *Control) runFleetOperation(ctx context.Context, id string) {
	unlock := c.lockFleet(id)
	defer unlock()
	c.mu.Lock()
	operation := cloneFleetOperation(c.fleetOps[id])
	c.mu.Unlock()
	if operation == nil || (operation.State != proto.FleetStatePending && operation.State != proto.FleetStateRunning &&
		(operation.State != proto.FleetStatePartial || !fleetHasPending(operation))) {
		return
	}
	startedState := operation.State
	if operation.State != proto.FleetStateRunning {
		if operation.State == proto.FleetStatePending {
			operation.State = proto.FleetStateRunning
			if err := c.persistFleetOperation(operation); err != nil {
				c.logger.Error("persist fleet operation start", "operation", id, "err", err)
				return
			}
			c.mu.Lock()
			c.fleetOps[id] = operation
			c.mu.Unlock()
		}
	}
	for index := range operation.Results {
		if ctx.Err() != nil {
			return
		}
		if startedState != proto.FleetStatePartial && c.now().UnixMilli() >= operation.Deadline {
			break
		}
		c.processFleetTarget(ctx, id, index)
	}

	c.mu.Lock()
	operation = cloneFleetOperation(c.fleetOps[id])
	c.mu.Unlock()
	if operation == nil {
		return
	}
	pending, failed := false, false
	for _, result := range operation.Results {
		switch result.State {
		case proto.FleetTargetPending:
			pending = true
		case proto.FleetTargetFailed:
			failed = true
		}
	}
	terminal := false
	switch {
	case !pending && !failed:
		operation.State, terminal = proto.FleetStateCompleted, true
	case !pending:
		operation.State, terminal = proto.FleetStatePartial, true
	case c.now().UnixMilli() >= operation.Deadline:
		operation.State, terminal = proto.FleetStatePartial, true
	default:
		operation.State = proto.FleetStateRunning
	}
	firstTerminal := !fleetStateTerminal(startedState) && terminal
	var completed *proto.Event
	if firstTerminal || (startedState == proto.FleetStatePartial && operation.State == proto.FleetStateCompleted) {
		completed = c.fleetEvent(proto.EvFleetCompleted, id, "", operation, nil, map[string]any{
			"state": operation.State, "targets": len(operation.Results),
		})
	}
	c.mu.Lock()
	if err := c.persistFleetOperation(operation, completed); err != nil {
		c.mu.Unlock()
		c.logger.Error("persist fleet operation result", "operation", id, "err", err)
		return
	}
	c.fleetOps[id] = operation
	c.mu.Unlock()
	if firstTerminal {
		metrics.FleetOperationsCompleted.Inc()
	}
}

func (c *Control) processFleetTarget(ctx context.Context, operationID string, index int) {
	c.mu.Lock()
	operation := cloneFleetOperation(c.fleetOps[operationID])
	if operation == nil || index >= len(operation.Results) || operation.Results[index].State != proto.FleetTargetPending {
		c.mu.Unlock()
		return
	}
	target := operation.Results[index]
	c.mu.Unlock()

	unlock := c.lockLifecycle(target.Workspace)
	defer unlock()
	if operation.Action == proto.FleetActionDestroy && target.Fenced {
		c.commitFleetDestroy(ctx, operation, index)
		return
	}

	c.mu.Lock()
	ws := c.workspaces[target.Workspace]
	if ws == nil {
		target.State = proto.FleetTargetFailed
		target.Error = "workspace disappeared"
		target.UpdatedAt = c.now().UnixMilli()
		operation.Results[index] = target
		if err := c.persistFleetOperation(operation); err == nil {
			c.fleetOps[operationID] = operation
		}
		c.mu.Unlock()
		return
	}
	previousOperationID := ws.QuarantineOperation
	if previousOperationID != "" && previousOperationID != operationID {
		previousOperation := c.fleetOps[previousOperationID]
		previousTarget, found := fleetTarget(previousOperation, target.Workspace)
		if previousOperation == nil || !fleetStateTerminal(previousOperation.State) || !found {
			target.State = proto.FleetTargetFailed
			target.Error = "workspace belongs to another active or inconsistent quarantine operation"
			target.UpdatedAt = c.now().UnixMilli()
			operation.Results[index] = target
			if err := c.persistFleetOperation(operation); err == nil {
				c.fleetOps[operationID] = operation
			}
			c.mu.Unlock()
			return
		}
		// Preserve the generation and physical holder that the earlier fence
		// targeted. Control may already have advanced its authoritative
		// generation, while the retained source still identifies the old one.
		target.Node = previousTarget.Node
		target.Generation = previousTarget.Generation
		target.Backend = previousTarget.Backend
	}
	if ws.QuarantineOperation != operationID {
		if previousOperationID == "" {
			target.Node, target.Generation = ws.Node, ws.Generation
			target.Backend = ws.Spec.Requires.Backend
			if target.Backend == "" && target.Node != "" {
				target.Backend, _ = c.eligibleBackendLocked(ws, c.nodes[target.Node])
			}
		}
		next := *ws
		next.QuarantineOperation = operationID
		if next.QuarantinedAt == 0 {
			next.QuarantinedAt = c.now().UnixMilli()
		}
		if target.Node == "" {
			finalState := proto.WSFailed
			authorityErr := canAdvanceWorkspaceAuthority(ws)
			if operation.Action == proto.FleetActionDestroy && authorityErr == nil {
				finalState = proto.WSDestroyed
			}
			var transitionErr error
			next, transitionErr = transitionWorkspace(ws, lifecycleTransition{
				operation: transitionFleetComplete, actor: actorControl, to: finalState,
				expectGeneration: true, generation: ws.Generation,
			})
			if transitionErr != nil {
				c.logger.Error("validate offline fleet transition", "ws", ws.ID, "err", transitionErr)
				c.mu.Unlock()
				return
			}
			next.QuarantineOperation = operationID
			if next.QuarantinedAt == 0 {
				next.QuarantinedAt = c.now().UnixMilli()
			}
			if authorityErr == nil {
				next.Generation++
				next.AuthzRevision++
			}
			next.LeaseUntil = 0
			if finalState == proto.WSDestroyed {
				next.Node = ""
			}
			target.Snapshot = ws.LastSnapshot
			target.Fenced = true
			if authorityErr != nil {
				target.State = proto.FleetTargetFailed
				target.Error = authorityErr.Error() + "; workspace left failed for operator reconciliation"
			} else {
				target.Acknowledged = true
				target.State = proto.FleetTargetAcknowledged
			}
			target.UpdatedAt = c.now().UnixMilli()
			operation.Results[index] = target
			if err := c.persistWorkspaceAndFleetOperation(&next, operation, c.fleetEvent(proto.EvFleetTarget, target.Workspace, target.Node, operation, &next,
				map[string]any{"operation": operationID, "action": operation.Action, "acknowledged": true})); err != nil {
				c.mu.Unlock()
				return
			}
			*ws = next
			c.fleetOps[operationID] = operation
			c.mu.Unlock()
			metrics.FleetTargetsFenced.Inc()
			return
		}
		var transitionErr error
		fleetBegin := lifecycleTransition{
			operation: transitionFleetBegin, actor: actorControl, to: proto.WSQuiescing,
			expectGeneration: true, generation: ws.Generation,
			expectNode: true, node: target.Node,
		}
		before := ws.State
		next, transitionErr = transitionWorkspace(ws, fleetBegin)
		if transitionErr != nil {
			c.logger.Error("validate fleet quarantine transition", "ws", ws.ID, "err", transitionErr)
			c.mu.Unlock()
			return
		}
		next.QuarantineOperation = operationID
		if next.QuarantinedAt == 0 {
			next.QuarantinedAt = c.now().UnixMilli()
		}
		operation.Results[index] = target
		if err := c.persistWorkspaceAndFleetOperation(&next, operation, c.transitionEvent(before, &next, fleetBegin)); err != nil {
			c.mu.Unlock()
			return
		}
		*ws = next
		c.fleetOps[operationID] = operation
	}
	exclude := append([]string(nil), ws.Spec.Exclude...)
	security := ws.Spec.Security
	c.mu.Unlock()

	var response proto.WSQuarantineRes
	request := proto.WSQuarantineReq{
		OperationID: operationID, WS: target.Workspace, Gen: target.Generation, Action: operation.Action,
		Backend: target.Backend, Exclude: exclude, Security: security,
	}
	var requestErr error
	if c.send == nil || !c.send.Online(target.Node) {
		requestErr = proto.Err(proto.CodeUnreachable, "node %s is offline", target.Node)
	} else {
		rctx, cancel := c.fleetRPCContext(ctx, operation)
		requestErr = c.send.Request(rctx, target.Node, proto.OpWSQuarantine, request, &response)
		cancel()
	}
	if requestErr != nil {
		var protocolErr *proto.Error
		if errors.As(requestErr, &protocolErr) && protocolErr.Code == proto.CodeNotFound {
			response.Fenced = true
			response.Generation = request.Gen
			response.Action = request.Action
			response.Backend = request.Backend
			requestErr = nil
		}
	}

	c.mu.Lock()
	operation = cloneFleetOperation(c.fleetOps[operationID])
	ws = c.workspaces[target.Workspace]
	if operation == nil || ws == nil || index >= len(operation.Results) || ws.QuarantineOperation != operationID {
		c.mu.Unlock()
		return
	}
	target = operation.Results[index]
	advanceAuthority := ws.Generation == target.Generation
	finalState := proto.WSFailed
	committedSnapshot := ""
	target.UpdatedAt = c.now().UnixMilli()
	if requestErr != nil {
		target.Error = requestErr.Error()
		target.State = proto.FleetTargetPending
	} else if !response.Fenced {
		target.Error = "node did not affirm fencing"
		if response.Warning != "" {
			target.Error += ": " + response.Warning
		}
		target.State = proto.FleetTargetFailed
	} else if response.Generation != target.Generation || response.Action != operation.Action {
		target.Error = "node quarantine acknowledgement does not match request"
		target.State = proto.FleetTargetFailed
	} else {
		target.Fenced = true
		if response.Backend != "" {
			target.Backend = response.Backend
		}
		target.Snapshot = response.Snapshot
		if response.Warning != "" {
			target.Error = response.Warning
			target.State = proto.FleetTargetFailed
		} else if (operation.Action == proto.FleetActionCheckpoint || operation.Action == proto.FleetActionDestroy) && response.Snapshot == "" {
			target.Error = "node did not return a checkpoint"
			target.State = proto.FleetTargetFailed
		} else if response.Snapshot != "" {
			if c.opts.Artifacts != nil {
				if err := c.opts.Artifacts.Verify(response.Snapshot); err != nil {
					target.Error = "checkpoint verification failed: " + err.Error()
					target.State = proto.FleetTargetFailed
				}
			}
			if target.State != proto.FleetTargetFailed {
				committedSnapshot = response.Snapshot
			}
		}
		if target.State == proto.FleetTargetPending {
			if operation.Action == proto.FleetActionDestroy {
				finalState = proto.WSDestroyed
				target.Error = "fenced and checkpointed; destruction acknowledgement pending"
			} else {
				target.Acknowledged = true
				target.State = proto.FleetTargetAcknowledged
				target.Error = ""
			}
		}
	}
	if advanceAuthority {
		if authorityErr := canAdvanceWorkspaceAuthority(ws); authorityErr != nil {
			advanceAuthority = false
			finalState = proto.WSFailed
			target.State = proto.FleetTargetFailed
			target.Acknowledged = false
			target.Error = authorityErr.Error() + "; workspace left failed for operator reconciliation"
		}
	}
	next, transitionErr := transitionWorkspace(ws, lifecycleTransition{
		operation: transitionFleetComplete, actor: actorControl, to: finalState,
		expectGeneration: true, generation: ws.Generation,
	})
	if transitionErr != nil {
		c.logger.Error("validate fleet completion transition", "ws", ws.ID, "err", transitionErr)
		c.mu.Unlock()
		return
	}
	if advanceAuthority {
		next.Generation++
		next.AuthzRevision++
	}
	next.LeaseUntil = 0
	if committedSnapshot != "" {
		next.LastSnapshot = committedSnapshot
		next.Spec.RestoreFrom = committedSnapshot
	}
	if finalState == proto.WSDestroyed {
		next.Node = ""
	}
	operation.Results[index] = target
	if err := c.persistWorkspaceAndFleetOperation(&next, operation, c.fleetEvent(proto.EvFleetTarget, target.Workspace, target.Node, operation, &next, map[string]any{
		"operation": operationID, "action": operation.Action, "acknowledged": target.Acknowledged,
		"state": target.State, "error": target.Error,
	})); err != nil {
		c.mu.Unlock()
		return
	}
	*ws = next
	c.fleetOps[operationID] = operation
	c.mu.Unlock()
	metrics.FleetTargetsFenced.Inc()
	if operation.Action == proto.FleetActionDestroy && target.Fenced && target.State == proto.FleetTargetPending {
		c.commitFleetDestroy(ctx, operation, index)
	}
}

func (c *Control) commitFleetDestroy(ctx context.Context, operation *proto.FleetOperation, index int) {
	target := operation.Results[index]
	if c.send == nil || !c.send.Online(target.Node) {
		target.Error = fmt.Sprintf("node %s is unreachable; fenced source destruction remains pending", target.Node)
		target.UpdatedAt = c.now().UnixMilli()
		operation.Results[index] = target
		c.persistFleetOnly(operation)
		return
	}
	request := proto.WSQuarantineCommitReq{
		OperationID: operation.ID, WS: target.Workspace, Gen: target.Generation,
		Backend: target.Backend, Snapshot: target.Snapshot,
	}
	rctx, cancel := c.fleetRPCContext(ctx, operation)
	err := c.send.Request(rctx, target.Node, proto.OpWSQuarantineCommit, request, nil)
	cancel()
	if err != nil {
		target.Error = "destroy acknowledgement pending: " + err.Error()
	} else {
		target.Acknowledged = true
		target.State = proto.FleetTargetAcknowledged
		target.Error = ""
	}
	target.UpdatedAt = c.now().UnixMilli()
	operation.Results[index] = target
	c.persistFleetOnly(operation)
}

func (c *Control) persistFleetOnly(operation *proto.FleetOperation) {
	if err := c.persistFleetOperation(operation); err != nil {
		c.logger.Error("persist fleet target", "operation", operation.ID, "err", err)
		return
	}
	c.mu.Lock()
	c.fleetOps[operation.ID] = operation
	c.mu.Unlock()
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
func (c *Control) eventsTail(ctx context.Context, from string, subject Subject, req *proto.EventsTailReq) (any, error) {
	if !req.Follow {
		evs, err := c.readAuthorizedEvents(ctx, subject, req.From, req.WS, 1000)
		if err != nil {
			return nil, err
		}
		return proto.EventPost{Events: evs}, nil
	}
	if req.From != 0 {
		first, err := c.log.First(ctx)
		if err != nil {
			return nil, err
		}
		if req.From < first {
			return nil, &proto.Error{Code: proto.CodeEvicted, Msg: "requested events are older than retention", Oldest: first}
		}
	}
	subID := req.Subscription
	if subID == "" {
		subID = "default"
	}
	c.mu.Lock()
	byID := c.tails[from]
	if byID == nil {
		byID = map[string]*tailState{}
		c.tails[from] = byID
	}
	if previous := byID[subID]; previous != nil {
		previous.cancel()
	}
	tctx, cancel := context.WithCancel(context.Background())
	tail := &tailState{cancel: cancel}
	byID[subID] = tail
	c.mu.Unlock()
	go func() {
		defer func() {
			cancel()
			c.mu.Lock()
			if current := c.tails[from][subID]; current == tail {
				delete(c.tails[from], subID)
				if len(c.tails[from]) == 0 {
					delete(c.tails, from)
				}
			}
			c.mu.Unlock()
		}()
		sub := c.log.Subscribe(req.From, req.WS)
		defer sub.Close()
		for {
			evs, err := sub.Next(tctx)
			if err != nil {
				return
			}
			filtered := evs[:0]
			for _, event := range evs {
				if c.eventVisible(subject, event) {
					filtered = append(filtered, event)
				}
			}
			if len(filtered) == 0 {
				continue
			}
			if err := c.send.Send(tctx, proto.NewEvent(from, "log", proto.EventPost{Events: filtered, Subscription: req.Subscription})); err != nil {
				return
			}
		}
	}()
	return struct{}{}, nil
}

func (c *Control) eventVisible(subject Subject, event proto.Event) bool {
	if hasRole(subject, "admin") {
		return true
	}
	return event.Tenant != "" && event.Tenant == subject.Tenant
}

func (c *Control) readAuthorizedEvents(ctx context.Context, subject Subject, from uint64, ws string, limit int) ([]proto.Event, error) {
	cursor := from
	var out []proto.Event
	for len(out) < limit {
		events, err := c.log.Read(ctx, cursor, ws, 1000)
		if err != nil {
			return nil, err
		}
		if len(events) == 0 {
			break
		}
		for _, event := range events {
			if c.eventVisible(subject, event) {
				out = append(out, event)
				if len(out) == limit {
					break
				}
			}
		}
		next := events[len(events)-1].Seq + 1
		if next <= cursor || len(events) < 1000 {
			break
		}
		cursor = next
	}
	return out, nil
}

func (c *Control) stopEventTail(from, subID string) {
	if subID == "" {
		subID = "default"
	}
	c.mu.Lock()
	if tail := c.tails[from][subID]; tail != nil {
		tail.cancel()
		delete(c.tails[from], subID)
	}
	if len(c.tails[from]) == 0 {
		delete(c.tails, from)
	}
	c.mu.Unlock()
}

// eventsPost appends node-originated events (node id enforced) and fires
// any timer waiting on the event type.
func (c *Control) eventsPost(ctx context.Context, from string, req *proto.EventPost) error {
	isNode := c.isNode(from)
	if len(req.Events) > 512 {
		return proto.Err(proto.CodeResourceExhausted, "event batch exceeds 512 entries")
	}
	if isNode {
		unlock := c.lockProducer(from)
		defer unlock()
	}
	var subject Subject
	if !isNode {
		var err error
		subject, err = c.subjectOf(from)
		if err != nil {
			return err
		}
	}
	for i := range req.Events {
		e := req.Events[i]
		observedAt, producerSeq := e.ObservedAt, e.ProducerSeq
		if observedAt == 0 {
			observedAt = e.At
		}
		if producerSeq == 0 {
			producerSeq = e.Seq
		}
		hintedWorkspace, hintedGeneration := e.Workspace, e.Generation
		if hintedWorkspace == "" && strings.HasPrefix(e.Stream, "ws_") {
			hintedWorkspace = e.Stream
		}
		if hintedWorkspace != "" {
			if strings.HasPrefix(e.Stream, "ws_") && e.Stream != hintedWorkspace {
				return proto.Err(proto.CodeBadRequest, "event stream and workspace disagree")
			}
			if e.Stream == "" {
				e.Stream = hintedWorkspace
			}
		}
		e.Seq = 0
		e.At = 0
		e.EventID = ids.New("ev")
		e.ReceivedAt = c.now().UnixMilli()
		e.ObservedAt = observedAt
		e.ProducerSeq = producerSeq
		e.OperationID = ""
		if isNode {
			if producerSeq == 0 {
				return proto.Err(proto.CodeBadRequest, "node events require a producer sequence")
			}
			tenant, workspace, generation, err := c.authorizeNodeEvent(from, hintedWorkspace, hintedGeneration)
			if err != nil {
				return err
			}
			principal := c.workspacePrincipal(workspace)
			c.mu.Lock()
			last := c.producerSeq[from]
			c.mu.Unlock()
			if producerSeq <= last {
				found, duplicate, err := c.sameNodeEvent(ctx, from, producerSeq, e.Type, e.Stream, e.Payload, observedAt, workspace, generation)
				if err != nil {
					return err
				}
				if duplicate {
					continue
				}
				if !found {
					// Retention may have removed the original after its producer
					// watermark became durable. Acknowledge the retry without
					// changing canonical history so the node can clear its outbox.
					continue
				}
				return proto.Err(proto.CodeConflict, "node event sequence %d is out of order or changed", producerSeq)
			}
			if producerSeq > last+1 {
				gap := proto.Event{
					EventID:    fmt.Sprintf("gap:%s:%d:%d", from, last+1, producerSeq-1),
					ReceivedAt: c.now().UnixMilli(), ObservedAt: observedAt,
					Origin: "control", Actor: "control", Node: from,
					Tenant: tenant, Workspace: workspace, Generation: generation, Principal: principal,
					Stream: from, Type: proto.EvEventGap, ProducerSeq: producerSeq - 1,
					Payload: proto.MustMarshal(map[string]any{"producer": from, "missing_from": last + 1, "missing_through": producerSeq - 1}),
				}
				if err := c.log.Append(ctx, &gap); err != nil {
					return err
				}
				c.mu.Lock()
				c.producerSeq[from] = producerSeq - 1
				c.mu.Unlock()
			}
			e.EventID = fmt.Sprintf("%s:%d", from, producerSeq)
			e.Origin = "node"
			e.Actor = from
			e.Node = from
			// A node is authoritative for observations, never for user
			// identity. Derive the workspace principal from control state.
			e.Principal = principal
			e.Tenant, e.Workspace, e.Generation = tenant, workspace, generation
		} else {
			e.Origin = "client"
			e.Actor = subject.ID
			e.Principal = subject.ID
			e.Node = ""
			e.Tenant = subject.Tenant
			e.Generation = 0
			e.Session = "" // only the node that runs a session may attribute to it
			if e.Stream != "" {
				ws, err := c.authorizeWorkspace(ctx, from, e.Stream, ActionWrite)
				if err != nil {
					return err
				}
				e.Workspace, e.Tenant = ws.ID, ws.Tenant
			}
		}
		if err := c.log.Append(ctx, &e); err != nil {
			return err
		}
		if isNode {
			c.mu.Lock()
			if producerSeq > c.producerSeq[from] {
				c.producerSeq[from] = producerSeq
			}
			c.mu.Unlock()
		}
		c.fireEventTimers(ctx, e.Type, e.Tenant, e.Stream)
	}
	return nil
}

func (c *Control) workspacePrincipal(workspace string) string {
	if workspace == "" {
		return ""
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if ws := c.workspaces[workspace]; ws != nil {
		return ws.Owner
	}
	return ""
}

func (c *Control) authorizeNodeEvent(node, workspace string, generation uint64) (string, string, uint64, error) {
	if workspace == "" {
		return "", "", 0, nil
	}
	if generation == 0 {
		// Compatibility with a node that predates assignment hints is safe only
		// for the assignment that is currently held by that node.
		c.mu.Lock()
		ws := c.workspaces[workspace]
		if ws == nil || ws.Node != node || !held(ws.State) {
			c.mu.Unlock()
			return "", "", 0, proto.Err(proto.CodeDenied, "node does not own event workspace %s", workspace)
		}
		tenant, gen := ws.Tenant, ws.Generation
		c.mu.Unlock()
		return tenant, workspace, gen, nil
	}
	tenant, ok, err := c.assignmentTenant(workspace, generation, node)
	if err != nil {
		return "", "", 0, err
	}
	if !ok {
		return "", "", 0, proto.Err(proto.CodeDenied, "node has no assignment for workspace %s generation %d", workspace, generation)
	}
	return tenant, workspace, generation, nil
}

func (c *Control) sameNodeEvent(ctx context.Context, node string, producerSeq uint64, typ, stream string, payload []byte, observedAt int64, workspace string, generation uint64) (bool, bool, error) {
	var storedType, storedStream, storedWorkspace string
	var storedPayload []byte
	var storedObserved int64
	var storedGeneration uint64
	err := c.db.QueryRowContext(ctx, `SELECT type, stream, payload, observed_at, workspace, generation
		FROM events WHERE origin='node' AND event_id=?`, fmt.Sprintf("%s:%d", node, producerSeq)).
		Scan(&storedType, &storedStream, &storedPayload, &storedObserved, &storedWorkspace, &storedGeneration)
	if errors.Is(err, sql.ErrNoRows) {
		return false, false, nil
	}
	if err != nil {
		return false, false, err
	}
	return true, storedType == typ && storedStream == stream && bytes.Equal(storedPayload, payload) &&
		storedObserved == observedAt && storedWorkspace == workspace && storedGeneration == generation, nil
}

// PostEvents appends externally originated events (webhooks, integrations)
// to the canonical log and fires any timer waiting on their type. It must
// not be given a request-scoped context: the caller returns before the
// timers and offers finish.
func (c *Control) PostEvents(ctx context.Context, principal string, events []proto.Event) error {
	for i := range events {
		e := events[i]
		e.Seq = 0
		e.At = 0
		e.EventID = ids.New("ev")
		e.ReceivedAt = c.now().UnixMilli()
		e.Origin, e.Actor, e.Principal, e.Node = "webhook", principal, principal, ""
		if e.Tenant == "" {
			e.Tenant = "local"
		}
		if e.Stream != "" {
			c.mu.Lock()
			if ws := c.workspaces[e.Stream]; ws != nil {
				e.Workspace, e.Tenant = ws.ID, ws.Tenant
			}
			c.mu.Unlock()
		}
		if err := c.log.Append(ctx, &e); err != nil {
			return err
		}
		c.fireEventTimers(ctx, e.Type, e.Tenant, e.Stream)
	}
	return nil
}

func (c *Control) fireEventTimers(ctx context.Context, typ, tenant, stream string) {
	c.mu.Lock()
	var fire []*proto.Timer
	for _, t := range c.timers {
		if !t.Fired && t.OnEvent != "" && t.OnEvent == typ {
			ws := c.workspaces[t.WS]
			if ws != nil && ws.Tenant == tenant && (stream == "" || stream == ws.ID) {
				fire = append(fire, t)
			}
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
	id, ws := t.ID, t.WS
	c.mu.Unlock()
	if _, err := c.wsWake(ctx, "", ws, id, ""); err != nil {
		c.logger.Error("fire timer", "timer", id, "ws", ws, "err", err)
	}
}

func (c *Control) bindingLease(ctx context.Context, node, wsID string) (*proto.BindingLeaseRes, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	ws := c.workspaces[wsID]
	leaseable := ws != nil && (ws.State == proto.WSClaimed || ws.State == proto.WSClaiming)
	if !leaseable || ws.Node != node {
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

func (c *Control) grant(ctx context.Context, client string, subject Subject, wsID string) (*proto.Grant, error) {
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
	resource := workspaceResource(ws)
	generation, assignedNode := ws.Generation, ws.Node
	c.mu.Unlock()
	if err := c.check(ctx, subject, ActionExecute, resource); err != nil {
		return nil, err
	}
	c.mu.Lock()
	ws = c.workspaces[wsID]
	if ws == nil || ws.Generation != generation || ws.Node != assignedNode || ws.State != proto.WSClaimed {
		c.mu.Unlock()
		return nil, proto.Err(proto.CodeConflict, "workspace changed while granting access")
	}
	expires := c.now().Add(time.Hour).UnixMilli()
	if ws.LeaseUntil > 0 && ws.LeaseUntil < expires {
		expires = ws.LeaseUntil
	}
	claims := proto.GrantClaims{
		Client: client, WS: wsID, Node: ws.Node, Principal: subject.ID, Tenant: subject.Tenant,
		AuthzRevision: ws.AuthzRevision, ExpiresAt: expires, Gen: ws.Generation,
	}
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
	if len(pub) != ed25519.PublicKeySize || len(g.Signature) != ed25519.SignatureSize {
		return proto.Err(proto.CodeUnauthorized, "malformed grant key or signature")
	}
	if !ed25519.Verify(pub, proto.MustMarshal(g.Claims), g.Signature) {
		return proto.Err(proto.CodeUnauthorized, "bad grant signature")
	}
	if now.UnixMilli() > g.Claims.ExpiresAt {
		return proto.Err(proto.CodeUnauthorized, "grant expired")
	}
	return nil
}

func (c *Control) timerListAuthorized(ctx context.Context, from string) (*proto.TimerListRes, error) {
	subject, err := c.subjectOf(from)
	if err != nil {
		return nil, err
	}
	c.mu.Lock()
	var timers []proto.Timer
	resources := make(map[string]Resource)
	for _, timer := range c.timers {
		if ws := c.workspaces[timer.WS]; ws != nil {
			timers = append(timers, *timer)
			resources[timer.WS] = workspaceResource(ws)
		}
	}
	c.mu.Unlock()
	out := &proto.TimerListRes{}
	for _, timer := range timers {
		if c.check(ctx, subject, ActionRead, resources[timer.WS]) == nil {
			out.Timers = append(out.Timers, timer)
		}
	}
	sort.Slice(out.Timers, func(i, j int) bool { return out.Timers[i].ID < out.Timers[j].ID })
	return out, nil
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
		case <-c.agentKick:
			c.agentReconcileAsync(c.requestCtx)
		}
	}
}

// Tick runs one iteration of the background work (exported for tests with
// an injected clock).
func (c *Control) Tick(ctx context.Context) {
	if err := c.log.DrainOutbox(ctx, c.db); err != nil {
		c.logger.Error("event outbox drain", "err", err)
	}
	now := c.now().UnixMilli()
	var expired uint64
	var fire []*proto.Timer
	c.mu.Lock()
	for _, ws := range c.workspaces {
		// Transitional states are owned by an in-flight control operation. The
		// operation either commits them or, after a crash, load() marks them
		// failed for reconciliation; lease expiry must not reassign mid-snapshot.
		leaseManaged := ws.State == proto.WSClaimed || ws.State == proto.WSClaiming
		if leaseManaged && ws.LeaseUntil > 0 && ws.LeaseUntil < now {
			// Advancing the authoritative generation revokes every grant made
			// under the expired assignment before another node may claim.
			nextGeneration, generationErr := nextWorkspaceGeneration(ws.Generation)
			operation, targetState := transitionLeaseExpired, proto.WSPending
			if generationErr != nil {
				// Never wrap a fence. A terminal failed workspace cannot be
				// reassigned until an operator creates a fresh identity.
				operation, targetState = transitionAuthorityExhausted, proto.WSFailed
			}
			next, transitionErr := transitionWorkspace(ws, lifecycleTransition{
				operation: operation, actor: actorControl, to: targetState,
				expectGeneration: true, generation: ws.Generation,
				expectNode: true, node: ws.Node,
			})
			if transitionErr != nil {
				c.logger.Error("validate lease expiry transition", "ws", ws.ID, "err", transitionErr)
				continue
			}
			if generationErr == nil {
				next.Generation = nextGeneration
				next.Spec.RestoreFrom = next.LastSnapshot
			}
			lost := ws.Node
			if generationErr == nil {
				next.Node = ""
			}
			next.LeaseUntil = 0
			var expiryError string
			if generationErr != nil {
				expiryError = generationErr.Error()
			}
			if err := c.persistWS(&next, c.wsEvent(&next, proto.EvWSLeaseExpired, "", lost, map[string]any{
				"restore_from": next.Spec.RestoreFrom,
				"state":        next.State, "error": expiryError,
			})); err != nil {
				c.logger.Error("persist lease expiry", "ws", ws.ID, "err", err)
				continue
			}
			*ws = next
			expired++
		}
	}
	for _, t := range c.timers {
		if !t.Fired && t.At > 0 && t.At <= now {
			fire = append(fire, t)
		}
	}
	c.mu.Unlock()
	metrics.WSLeaseExpired.Add(uint64(expired))
	for _, t := range fire {
		c.fireTimer(ctx, t)
	}
	c.offerPending(ctx)
	c.agentReconcileAsync(c.requestCtx)
	c.expireStaleApprovals(ctx)
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
