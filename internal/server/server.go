// Package server hosts the relay, the control plane and the artifact store
// behind one HTTP listener: /v1/link (WebSocket for nodes and clients),
// /v1/artifacts/{id} (PUT/GET blobs), /v1/events (webhook wake), /healthz.
package server

import (
	"context"
	"crypto/subtle"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"log/slog"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"remount.dev/remount/internal/artifact"
	"remount.dev/remount/internal/artifact/encrypted"
	"remount.dev/remount/internal/control"
	"remount.dev/remount/internal/eventlog"
	"remount.dev/remount/internal/identity"
	"remount.dev/remount/internal/metrics"
	"remount.dev/remount/internal/notifier"
	nodepool "remount.dev/remount/internal/pool"
	"remount.dev/remount/internal/proto"
	"remount.dev/remount/internal/provision"
	"remount.dev/remount/internal/relay"
	"remount.dev/remount/internal/secretsource"
	"remount.dev/remount/internal/tenant"
	"remount.dev/remount/internal/transport"
	webui "remount.dev/remount/web"
)

// Options configure a server.
type Options struct {
	DataDir  string // sqlite db + artifacts; "" = in-memory (tests / --standalone)
	Token    string
	Bindings []control.Binding
	// SecretResolver resolves Binding.Source only at lease time. Production
	// modes require sources for every binding and refuse literal values.
	SecretResolver secretsource.Resolver
	LeaseSec       int64
	// MaxArtifactBytes bounds one uploaded compressed artifact. Default 8 GiB.
	MaxArtifactBytes int64
	// MaxArtifactStoreBytes and MaxArtifactObjects bound retained artifacts and
	// concurrent staging. Defaults are 64 GiB and 100,000 objects.
	MaxArtifactStoreBytes int64
	MaxArtifactObjects    int
	// TenantArtifacts resolves the authenticated tenant to an isolated logical
	// artifact namespace. Production modes require a resolver that asserts
	// encryption at rest; standalone explicitly falls back to the legacy shared
	// directory store.
	TenantArtifacts artifact.TenantResolver
	// ArtifactStartupTimeout bounds the resolver readiness probe. Zero selects
	// 30 seconds. A probe that cannot run makes production startup unavailable.
	ArtifactStartupTimeout time.Duration
	// NodeArtifactAuthorizer validates a node's signed workspace-generation
	// assignment proof for each artifact transfer. Client credentials are
	// authorized through Authorizer. Production node requests fail closed when
	// this hook is absent; a tenant header is never accepted as authority.
	NodeArtifactAuthorizer NodeArtifactAuthorizer
	// ArtifactGracePeriod protects uploads while a workspace operation commits
	// its reference. ArtifactGCInterval controls reference-aware collection.
	// Negative intervals disable the background collector (primarily for tests).
	ArtifactGracePeriod time.Duration
	ArtifactGCInterval  time.Duration
	// EventRetention is the durable audit window. Pruning removes only a
	// contiguous sequence prefix, and readers below it receive CodeEvicted.
	// Negative intervals disable the background collector.
	EventRetention  time.Duration
	EventGCInterval time.Duration
	// MaxEvents bounds retained rows even during a high-volume interval that
	// has not reached EventRetention age. Zero selects 1,000,000.
	MaxEvents int
	// RecordRetention is the replay window for completed idempotency results
	// and the visibility window for fired timers. RecordGCInterval controls
	// bounded pruning passes. Negative intervals disable background pruning.
	RecordRetention  time.Duration
	RecordGCInterval time.Duration
	Logger           *slog.Logger
	// Mode declares the deployment trust posture.
	Mode          string
	Authenticator control.Authenticator
	Authorizer    control.Authorizer
	// PrincipalAuthority backs production onboarding and short-lived token
	// issuance. The built-in identity manager is used when nil.
	PrincipalAuthority control.PrincipalAuthority
	// SessionCapabilityTTL controls short-lived signed broker proofs. The
	// node keeps the process-visible handle stable and rotates proofs early.
	SessionCapabilityTTL time.Duration
	// NodeAuthenticator atomically enrolls or verifies node keys. Production
	// modes require it and do not accept a shared node token.
	NodeAuthenticator control.NodeAuthenticator
	ApprovedNodes     map[string]control.NodeApproval
	// MaxConcurrentRequests bounds control-plane request handlers. Zero
	// selects 128.
	MaxConcurrentRequests int
	// Workspace quotas are enforced atomically by the control plane.
	MaxWorkspacesPerTenant  int
	MaxWorkspacesPerSubject int
	MaxMutationRecords      int
	MaxTimers               int
	MaxTimersPerWorkspace   int
	// PublicURL is the externally reachable base (https://host) that agent
	// URLs are minted under; empty leaves Agent.URL empty.
	PublicURL string
	// AgentURLBase, when set, makes GET /a/{id} redirect to an operator UI:
	// "{id}" in it is replaced, otherwise the id is appended. Empty serves
	// the agent as JSON.
	AgentURLBase string
	// CORSOrigins lists browser origins allowed to call the API; "*" allows
	// any origin without credentials. Empty disables CORS headers.
	CORSOrigins []string
	// MaxAPIClients bounds distinct credentials with an open in-process SDK
	// client at once; APIClientIdle closes one unused that long. Defaults
	// 256 and 10 minutes.
	MaxAPIClients int
	APIClientIdle time.Duration
	// WebhookSecret lets POST /v1/events authenticate with an HMAC-SHA256 of
	// the body (X-Remount-Signature or X-Hub-Signature-256: sha256=<hex>)
	// instead of a bearer. WebhookToken is the credential a signed request's
	// agent actions run as; empty leaves signed requests append-only.
	WebhookSecret string
	WebhookToken  string
	// WebhookProviders configures provider-native verification for POST
	// /v1/events. Secrets are used only while authenticating the raw request
	// and are never copied into the canonical event log.
	WebhookProviders WebhookProviderConfig
	// Notifications are preconfigured outbound routes. Each tenant gets its
	// own durable export cursor and serialized worker.
	Notifications                   []notifier.Subscription
	NotifierBatchEvents             int
	NotifierBatchBytes              int
	NotifierAttempts                int
	NotifierRetryBase               time.Duration
	NotifierRetryMax                time.Duration
	NotifierHTTPTimeout             time.Duration
	MaxNotifierSubscriptions        int
	MaxNotifierDeadLettersPerTenant int
	NotifierDeadLetterRetention     time.Duration
	NotifierDeadLetterGCInterval    time.Duration
	// ProvisionDrivers are explicitly configured whole-node providers. Pools
	// are enabled only with a node authenticator that can consume the minted
	// one-time enrollment credentials.
	ProvisionDrivers     []provision.Driver
	PoolEnrollmentSource nodepool.EnrollmentSource
	PoolBootstrap        control.PoolBootstrap
	PoolOptions          nodepool.Options
	// Replication enables fenced active/passive control-plane durability. Its
	// coordinator must already hold the writer lease; a standby restores first.
	Replication *ReplicationOptions
}

const (
	// ArtifactWorkspaceHeader identifies the workspace in a signed node
	// artifact assignment proof. The value is not authority by itself.
	ArtifactWorkspaceHeader = proto.ArtifactWorkspaceHeader
	// ArtifactGenerationHeader identifies the generation in a signed node
	// artifact assignment proof.
	ArtifactGenerationHeader = proto.ArtifactGenerationHeader
	// ArtifactProofHeader carries the control-plane-signed assignment proof.
	ArtifactProofHeader = proto.ArtifactProofHeader
)

// NodeArtifactAuthorizer revalidates live assignment after node credential
// authentication. It must not derive Tenant from an HTTP header.
type NodeArtifactAuthorizer interface {
	AuthorizeNodeArtifact(ctx context.Context, node, tenant, workspace string, generation uint64, method, artifactID, proof string) error
}

const (
	ModeStandalone             = "standalone"
	ModeProductionSingleTenant = "production-single-tenant"
	ModeProductionMultiTenant  = "production-multi-tenant"
)

// Server is a running Remount server.
type Server struct {
	opts      Options
	Control   *control.Control
	Identity  *identity.Manager
	Tenants   *tenant.Store
	Relay     *relay.Relay
	Store     *artifact.Store
	artifacts artifact.TenantResolver
	Log       *eventlog.Log
	db        *sql.DB
	http      *http.Server
	ln        net.Listener
	logger    *slog.Logger

	mu          sync.RWMutex
	ready       chan struct{}
	readyOnce   sync.Once
	readyErr    error
	serving     bool
	serveCalled bool
	closed      bool
	closeOnce   sync.Once
	closeErr    error
	tempArtDir  string
	gcCtx       context.Context
	gcCancel    context.CancelFunc
	gcWG        sync.WaitGroup
	// lifetime ends at Close; in-process API clients and their relay sides
	// live on it rather than on any one request.
	lifetime        context.Context
	lifetimeCancel  context.CancelFunc
	clients         *clientPool
	notifiers       []*notifier.Runner
	notifierDLQ     *notifier.SQLiteDeadLetterStore
	notifierTenants []string
	console         http.Handler
	replication     *controlReplication
}

// New builds a server. Call Serve or Handler.
func New(opts Options) (*Server, error) {
	if opts.Logger == nil {
		opts.Logger = slog.Default()
	}
	console, err := webui.NewHandler()
	if err != nil {
		return nil, fmt.Errorf("server: %w", err)
	}
	if opts.MaxArtifactBytes < 0 || opts.MaxArtifactStoreBytes < 0 || opts.MaxArtifactObjects < 0 || opts.MaxEvents < 0 || opts.ArtifactStartupTimeout < 0 {
		return nil, errors.New("server: artifact and event limits must not be negative")
	}
	if opts.MaxConcurrentRequests < 0 || opts.MaxWorkspacesPerTenant < 0 || opts.MaxWorkspacesPerSubject < 0 || opts.MaxMutationRecords < 0 ||
		opts.MaxTimers < 0 || opts.MaxTimersPerWorkspace < 0 {
		return nil, errors.New("server: control-plane quotas must not be negative")
	}
	if opts.WebhookProviders.SlackReplayWindow < 0 {
		return nil, errors.New("server: Slack webhook replay window must not be negative")
	}
	if opts.NotifierBatchEvents < 0 || opts.NotifierBatchBytes < 0 || opts.NotifierAttempts < 0 ||
		opts.MaxNotifierSubscriptions < 0 || opts.MaxNotifierDeadLettersPerTenant < 0 ||
		opts.NotifierRetryBase < 0 || opts.NotifierRetryMax < 0 || opts.NotifierHTTPTimeout < 0 ||
		opts.NotifierDeadLetterRetention < 0 {
		return nil, errors.New("server: notifier limits must not be negative")
	}
	if opts.NotifierDeadLetterGCInterval == 0 {
		opts.NotifierDeadLetterGCInterval = 10 * time.Minute
	}
	if opts.NotifierDeadLetterRetention == 0 {
		opts.NotifierDeadLetterRetention = 30 * 24 * time.Hour
	}
	if opts.MaxNotifierSubscriptions == 0 {
		opts.MaxNotifierSubscriptions = 128
	}
	if opts.MaxNotifierDeadLettersPerTenant == 0 {
		opts.MaxNotifierDeadLettersPerTenant = 10_000
	}
	if len(opts.Notifications) > opts.MaxNotifierSubscriptions {
		return nil, errors.New("server: notifier subscription capacity exceeded")
	}
	if opts.MaxArtifactBytes == 0 {
		opts.MaxArtifactBytes = 8 << 30
	}
	if opts.MaxArtifactStoreBytes == 0 {
		opts.MaxArtifactStoreBytes = 64 << 30
	}
	if opts.MaxArtifactObjects == 0 {
		opts.MaxArtifactObjects = 100_000
	}
	if opts.ArtifactGracePeriod == 0 {
		opts.ArtifactGracePeriod = 24 * time.Hour
	}
	if opts.ArtifactGracePeriod < 0 {
		return nil, errors.New("server: artifact grace period must not be negative")
	}
	if opts.ArtifactGCInterval == 0 {
		opts.ArtifactGCInterval = 10 * time.Minute
	}
	if opts.EventRetention == 0 {
		opts.EventRetention = 30 * 24 * time.Hour
	}
	if opts.EventRetention < 0 {
		return nil, errors.New("server: event retention must not be negative")
	}
	if opts.EventGCInterval == 0 {
		opts.EventGCInterval = 10 * time.Minute
	}
	if opts.MaxEvents == 0 {
		opts.MaxEvents = 1_000_000
	}
	if opts.RecordRetention == 0 {
		opts.RecordRetention = 30 * 24 * time.Hour
	}
	if opts.RecordRetention < 0 {
		return nil, errors.New("server: control-record retention must not be negative")
	}
	if opts.RecordGCInterval == 0 {
		opts.RecordGCInterval = 10 * time.Minute
	}
	if opts.Mode == "" {
		opts.Mode = ModeStandalone
	}
	if opts.ArtifactStartupTimeout == 0 {
		opts.ArtifactStartupTimeout = 30 * time.Second
	}
	if opts.Mode != ModeStandalone && (opts.TenantArtifacts == nil || !opts.TenantArtifacts.EncryptedAtRest()) {
		return nil, errors.New("server: production mode requires encrypted tenant artifact storage")
	}
	dbPath := ":memory:"
	artDir := ""
	if opts.DataDir != "" {
		if err := os.MkdirAll(opts.DataDir, 0o700); err != nil {
			return nil, err
		}
		dbPath = filepath.Join(opts.DataDir, "control.db")
		artDir = filepath.Join(opts.DataDir, "artifacts")
	}
	sq, err := eventlog.OpenSQLite(dbPath)
	if err != nil {
		return nil, err
	}
	if artDir == "" {
		d, err := os.MkdirTemp("", "remount-artifacts-")
		if err != nil {
			_ = sq.Close()
			return nil, err
		}
		artDir = d
	}
	store, err := artifact.NewStoreWithOptions(artDir, artifact.StoreOptions{
		MaxBytes: opts.MaxArtifactStoreBytes, MaxObjects: opts.MaxArtifactObjects,
	})
	if err != nil {
		_ = sq.Close()
		if opts.DataDir == "" {
			_ = os.RemoveAll(artDir)
		}
		return nil, err
	}
	if opts.TenantArtifacts == nil {
		opts.TenantArtifacts, err = artifact.NewSharedTenantResolver(store)
		if err != nil {
			_ = sq.Close()
			if opts.DataDir == "" {
				_ = os.RemoveAll(artDir)
			}
			return nil, err
		}
	}
	readyCtx, readyCancel := context.WithTimeout(context.Background(), opts.ArtifactStartupTimeout)
	err = opts.TenantArtifacts.Ready(readyCtx)
	readyCancel()
	if err != nil {
		_ = sq.Close()
		if opts.DataDir == "" {
			_ = os.RemoveAll(artDir)
		}
		return nil, fmt.Errorf("server: tenant artifact storage unavailable: %w", err)
	}
	log := eventlog.New(sq)
	var tenantStore *tenant.Store
	if opts.Mode == ModeProductionMultiTenant {
		tenantStore, err = tenant.NewStore(sq.DB(), log, tenant.Options{})
		if err != nil {
			_ = log.Close()
			if opts.DataDir == "" {
				_ = os.RemoveAll(artDir)
			}
			return nil, err
		}
	}
	var identityManager *identity.Manager
	if opts.Mode == ModeProductionSingleTenant || opts.Mode == ModeProductionMultiTenant {
		configured := opts.Authenticator != nil || opts.Authorizer != nil || opts.NodeAuthenticator != nil
		complete := opts.Authenticator != nil && opts.Authorizer != nil && opts.NodeAuthenticator != nil
		if configured && !complete {
			_ = log.Close()
			if opts.DataDir == "" {
				_ = os.RemoveAll(artDir)
			}
			return nil, errors.New("server: production identity overrides must provide authenticator, authorizer, and node authenticator together")
		}
		if !configured {
			identityStore, err := identity.NewSQLiteStore(sq.DB(), log)
			if err != nil {
				_ = log.Close()
				if opts.DataDir == "" {
					_ = os.RemoveAll(artDir)
				}
				return nil, err
			}
			identityKey, err := identityStore.LoadOrCreateSigningKey(context.Background())
			if err != nil {
				_ = log.Close()
				if opts.DataDir == "" {
					_ = os.RemoveAll(artDir)
				}
				return nil, err
			}
			identityManager, err = identity.New(identity.Options{PrivateKey: identityKey, Store: identityStore, SessionTTL: opts.SessionCapabilityTTL})
			if err != nil {
				_ = log.Close()
				if opts.DataDir == "" {
					_ = os.RemoveAll(artDir)
				}
				return nil, err
			}
			opts.Authenticator, opts.Authorizer, opts.NodeAuthenticator = identityManager, identityManager, identityManager
		}
	}
	floor, err := validateSecurityMode(opts)
	if err != nil {
		_ = log.Close()
		if opts.DataDir == "" {
			_ = os.RemoveAll(artDir)
		}
		return nil, err
	}
	var poolReconciler control.PoolReconciler
	if len(opts.ProvisionDrivers) > 0 {
		if opts.NodeAuthenticator == nil {
			_ = log.Close()
			if opts.DataDir == "" {
				_ = os.RemoveAll(artDir)
			}
			return nil, errors.New("server: provision drivers require one-time node enrollment authentication")
		}
		source := opts.PoolEnrollmentSource
		if source == nil {
			source, _ = opts.NodeAuthenticator.(nodepool.EnrollmentSource)
		}
		if source == nil {
			_ = log.Close()
			if opts.DataDir == "" {
				_ = os.RemoveAll(artDir)
			}
			return nil, errors.New("server: provision drivers require an enrollment source")
		}
		template := provision.Bootstrap{ServerURL: opts.PoolBootstrap.ServerURL, BinaryURL: opts.PoolBootstrap.BinaryURL,
			Backend: "configured-per-pool", DataDir: opts.PoolBootstrap.DataDir}
		if err := template.ValidateTemplate(); err != nil {
			_ = log.Close()
			if opts.DataDir == "" {
				_ = os.RemoveAll(artDir)
			}
			return nil, fmt.Errorf("server: invalid pool bootstrap: %w", err)
		}
		reconciler, err := nodepool.New(opts.ProvisionDrivers, source, opts.PoolOptions)
		if err != nil {
			_ = log.Close()
			if opts.DataDir == "" {
				_ = os.RemoveAll(artDir)
			}
			return nil, err
		}
		poolReconciler = reconciler
	}
	var legacyControlArtifacts control.ArtifactStore = store
	if opts.Mode != ModeStandalone {
		legacyControlArtifacts = nil
	}
	var sessionCapabilities control.SessionCapabilityAuthority
	var principalRevocations control.PrincipalRevocationAuthority
	principalAuthority := opts.PrincipalAuthority
	if identityManager != nil {
		sessionCapabilities = identityManager
		principalRevocations = identityManager
		principalAuthority = identityManager
	}
	var authority control.ControllerAuthority
	controllerRole := "active"
	var recoveryState *control.RecoveryState
	if opts.Replication != nil {
		authority = opts.Replication.Coordinator
		if opts.Replication.Recovery != nil {
			controllerRole = "promoted"
			recovery := opts.Replication.Recovery
			recoveryState = &control.RecoveryState{
				PreviousEpoch: recovery.PreviousEpoch, RestoredEventSeq: recovery.Manifest.SourceEventSeq,
				LastReplicatedAt: recovery.LastReplicatedAt, PromotedAt: recovery.PromotedAt, LostWindow: recovery.LostWindow,
			}
		}
	}
	ctrl, err := control.New(control.Options{DB: sq.DB(), Log: log, Token: opts.Token,
		Bindings: opts.Bindings, SecretResolver: opts.SecretResolver, LeaseSec: opts.LeaseSec, Logger: opts.Logger,
		Artifacts: legacyControlArtifacts, TenantArtifacts: opts.TenantArtifacts,
		Authenticator: opts.Authenticator, Authorizer: opts.Authorizer, NodeAuthenticator: opts.NodeAuthenticator, ApprovedNodes: opts.ApprovedNodes,
		SessionCapabilities: sessionCapabilities, PrincipalRevocations: principalRevocations,
		Principals:           principalAuthority,
		SessionCapabilityTTL: opts.SessionCapabilityTTL,
		Tenants:              tenantStore,
		SecurityProfileFloor: floor, MaxWorkspacesPerTenant: opts.MaxWorkspacesPerTenant,
		MaxWorkspacesPerSubject: opts.MaxWorkspacesPerSubject, MaxMutationRecords: opts.MaxMutationRecords,
		MaxTimers: opts.MaxTimers, MaxTimersPerWorkspace: opts.MaxTimersPerWorkspace,
		MaxConcurrentRequests: opts.MaxConcurrentRequests, MaxEvents: opts.MaxEvents, PublicURL: opts.PublicURL,
		PoolReconciler: poolReconciler, PoolBootstrap: opts.PoolBootstrap,
		ControllerAuthority: authority, ControllerRole: controllerRole, Recovery: recoveryState})
	if err != nil {
		_ = log.Close()
		if opts.DataDir == "" {
			_ = os.RemoveAll(artDir)
		}
		return nil, err
	}
	if opts.NodeArtifactAuthorizer == nil {
		opts.NodeArtifactAuthorizer = ctrl
	}
	r := relay.New(ctrl)
	ctrl.Attach(r)
	ctrl.Start()
	replication, err := newControlReplication(sq.DB(), dbPath, opts.DataDir, log, ctrl, opts.Replication)
	if err != nil {
		r.Close()
		ctrl.Stop()
		_ = log.Close()
		if opts.DataDir == "" {
			_ = os.RemoveAll(artDir)
		}
		return nil, err
	}
	if replication != nil && recoveryState == nil {
		shipCtx, shipCancel := context.WithTimeout(context.Background(), 30*time.Second)
		err = replication.initialShip(shipCtx)
		shipCancel()
		if err != nil {
			replication.close()
			r.Close()
			ctrl.Stop()
			_ = log.Close()
			if opts.DataDir == "" {
				_ = os.RemoveAll(artDir)
			}
			return nil, fmt.Errorf("server: initial control replication failed: %w", err)
		}
	}
	if replication != nil {
		replication.startKeepAlive()
		if recoveryState == nil {
			replication.startShipping()
		}
	}
	s := &Server{
		opts: opts, Control: ctrl, Identity: identityManager, Tenants: tenantStore, Relay: r, Store: store, Log: log, db: sq.DB(), logger: opts.Logger,
		artifacts: opts.TenantArtifacts, ready: make(chan struct{}), console: console, replication: replication,
	}
	if opts.DataDir == "" {
		s.tempArtDir = artDir
	}
	s.lifetime, s.lifetimeCancel = context.WithCancel(context.Background())
	if opts.MaxAPIClients <= 0 {
		opts.MaxAPIClients = 256
	}
	if opts.APIClientIdle <= 0 {
		opts.APIClientIdle = 10 * time.Minute
	}
	s.opts.MaxAPIClients, s.opts.APIClientIdle = opts.MaxAPIClients, opts.APIClientIdle
	s.clients = newClientPool(s, opts.MaxAPIClients, opts.APIClientIdle)
	if err := s.configureNotifiers(); err != nil {
		s.clients.close()
		s.lifetimeCancel()
		s.Relay.Close()
		s.Control.Stop()
		_ = s.Log.Close()
		if s.tempArtDir != "" {
			_ = os.RemoveAll(s.tempArtDir)
		}
		return nil, err
	}
	s.gcWG.Add(1)
	go s.clientSweepLoop(s.lifetime)
	if opts.ArtifactGCInterval > 0 || opts.EventGCInterval > 0 || opts.RecordGCInterval > 0 ||
		(len(s.notifiers) > 0 && opts.NotifierDeadLetterGCInterval > 0) {
		gcCtx, gcCancel := context.WithCancel(context.Background())
		s.gcCtx = gcCtx
		s.gcCancel = gcCancel
	}
	if opts.ArtifactGCInterval > 0 {
		s.gcWG.Add(1)
		go s.artifactGCLoop(s.gcCtx)
	}
	if opts.EventGCInterval > 0 {
		s.gcWG.Add(1)
		go s.eventGCLoop(s.gcCtx)
	}
	if opts.RecordGCInterval > 0 {
		s.gcWG.Add(1)
		go s.recordGCLoop(s.gcCtx)
	}
	if len(s.notifiers) > 0 && opts.NotifierDeadLetterGCInterval > 0 {
		s.gcWG.Add(1)
		go s.notifierDeadLetterGCLoop(s.gcCtx)
	}
	for index, runner := range s.notifiers {
		s.gcWG.Add(1)
		go s.notifierLoop(s.lifetime, runner, s.notifierTenants[index])
	}
	return s, nil
}

func (s *Server) configureNotifiers() error {
	if len(s.opts.Notifications) == 0 {
		return nil
	}
	deadLetters, err := notifier.NewSQLiteDeadLetterStore(s.db, s.opts.MaxNotifierDeadLettersPerTenant)
	if err != nil {
		return fmt.Errorf("server: notifier dead-letter store unavailable: %w", err)
	}
	byTenant := make(map[string][]notifier.Subscription)
	for _, subscription := range s.opts.Notifications {
		byTenant[subscription.Tenant] = append(byTenant[subscription.Tenant], subscription)
	}
	tenants := make([]string, 0, len(byTenant))
	for tenant := range byTenant {
		tenants = append(tenants, tenant)
	}
	sort.Strings(tenants)
	for _, tenant := range tenants {
		cursors, err := s.Control.ExportCursors(tenant)
		if err != nil {
			return fmt.Errorf("server: notifier cursor unavailable: %w", err)
		}
		runner, err := notifier.New(notifier.Options{
			Source: s.Log, Cursors: cursors, CursorName: "notifier-v1",
			Subscriptions: byTenant[tenant], DeadLetters: deadLetters,
			BatchEvents: s.opts.NotifierBatchEvents, BatchBytes: s.opts.NotifierBatchBytes,
			Attempts: s.opts.NotifierAttempts, RetryBase: s.opts.NotifierRetryBase,
			RetryMax: s.opts.NotifierRetryMax, HTTPTimeout: s.opts.NotifierHTTPTimeout,
			MaxSubs: s.opts.MaxNotifierSubscriptions, Signal: s.recordNotifierSignal,
		})
		if err != nil {
			return fmt.Errorf("server: invalid notifier configuration: %w", err)
		}
		s.notifiers = append(s.notifiers, runner)
		s.notifierTenants = append(s.notifierTenants, tenant)
	}
	s.notifierDLQ = deadLetters
	return nil
}

func (s *Server) recordNotifierSignal(ctx context.Context, signal notifier.Signal) error {
	typ := proto.EvNotifyUnavailable
	if signal.Kind == notifier.SignalDeadLettered {
		typ = proto.EvNotifyDeadLetter
	}
	return s.Control.PostEvents(ctx, "notifier", []proto.Event{{
		Type: typ, Tenant: signal.Tenant,
		Payload: proto.MustMarshal(map[string]any{
			"subscription": signal.SubscriptionID, "first_seq": signal.FirstSeq,
			"last_seq": signal.LastSeq, "attempts": signal.Attempts, "reason": signal.Reason,
		}),
	}})
}

func (s *Server) notifierLoop(ctx context.Context, runner *notifier.Runner, tenant string) {
	defer s.gcWG.Done()
	defer runner.CloseIdleConnections()
	backoff := time.Second
	for {
		err := runner.Run(ctx)
		if ctx.Err() != nil {
			return
		}
		// Never log the lower error: transports must not make a configured URL
		// or credential observable.
		s.logger.Error("notifier unavailable", "tenant", tenant)
		metrics.NotificationUnavailable.Inc()
		signalCtx, signalCancel := context.WithTimeout(context.Background(), 5*time.Second)
		_ = s.recordNotifierSignal(signalCtx, notifier.Signal{
			Kind: notifier.SignalUnavailable, Tenant: tenant, Reason: "destination_unavailable",
		})
		signalCancel()
		timer := time.NewTimer(backoff)
		select {
		case <-ctx.Done():
			if !timer.Stop() {
				<-timer.C
			}
			return
		case <-timer.C:
		}
		if err == nil {
			backoff = time.Second
		} else if backoff < time.Minute {
			backoff *= 2
			if backoff > time.Minute {
				backoff = time.Minute
			}
		}
	}
}

func (s *Server) notifierDeadLetterGCLoop(ctx context.Context) {
	defer s.gcWG.Done()
	ticker := time.NewTicker(s.opts.NotifierDeadLetterGCInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case now := <-ticker.C:
			for _, tenant := range s.notifierTenants {
				removed, err := s.notifierDLQ.Prune(ctx, tenant, now.Add(-s.opts.NotifierDeadLetterRetention).UnixMilli(), 1000)
				if err != nil {
					metrics.NotificationUnavailable.Inc()
					_ = s.recordNotifierSignal(ctx, notifier.Signal{Kind: notifier.SignalUnavailable, Tenant: tenant, Reason: "dead_letter_store_failed"})
					continue
				}
				if removed > 0 {
					metrics.NotificationDeadLettersPruned.Add(uint64(removed))
					_ = s.Control.PostEvents(ctx, "notifier", []proto.Event{{
						Type: proto.EvNotifyDLQPruned, Tenant: tenant,
						Payload: proto.MustMarshal(map[string]any{"removed": removed}),
					}})
				}
			}
		}
	}
}

// NotificationDeadLetters returns a bounded page of sanitized failed-delivery
// evidence. It is unavailable when no notifier is configured.
func (s *Server) NotificationDeadLetters(ctx context.Context, tenant string, afterID int64, limit int) ([]notifier.DeadLetterRecord, error) {
	if s.notifierDLQ == nil {
		return nil, errors.New("server: notifier unavailable")
	}
	return s.notifierDLQ.List(ctx, tenant, afterID, limit)
}

func (s *Server) clientSweepLoop(ctx context.Context) {
	defer s.gcWG.Done()
	interval := s.opts.APIClientIdle / 4
	if interval < time.Second {
		interval = time.Second
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case now := <-ticker.C:
			s.clients.sweep(now)
		}
	}
}

func (s *Server) artifactGCLoop(ctx context.Context) {
	defer s.gcWG.Done()
	ticker := time.NewTicker(s.opts.ArtifactGCInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case now := <-ticker.C:
			if _, err := s.CollectArtifacts(now); err != nil {
				metrics.ArtifactGCErrors.Inc()
				s.logger.Error("artifact garbage collection failed", "error", err)
			}
		}
	}
}

func (s *Server) eventGCLoop(ctx context.Context) {
	defer s.gcWG.Done()
	ticker := time.NewTicker(s.opts.EventGCInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case now := <-ticker.C:
			if _, err := s.PruneEvents(ctx, now); err != nil && !errors.Is(err, context.Canceled) {
				metrics.EventGCErrors.Inc()
				s.logger.Error("event retention failed", "error", err)
			}
			// Residency drift is a tenant-policy fact with the same bounded
			// maintenance cadence as retention: placement refuses a new
			// assignment, but a policy tightened under a running workspace is
			// only discoverable by looking.
			if denied := s.Control.EnforceTenantResidency(ctx); denied > 0 {
				s.logger.Warn("workspaces are held outside their tenant residency policy", "workspaces", denied)
			}
		}
	}
}

func (s *Server) recordGCLoop(ctx context.Context) {
	defer s.gcWG.Done()
	ticker := time.NewTicker(s.opts.RecordGCInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case now := <-ticker.C:
			if _, err := s.PruneControlRecords(ctx, now); err != nil && !errors.Is(err, context.Canceled) {
				metrics.RecordGCErrors.Inc()
				s.logger.Error("control-record retention failed", "error", err)
			}
		}
	}
}

// CollectArtifacts performs one reference-aware GC pass. Control keeps the
// reference set stable until Store has finished unlinking candidates, while
// the grace window protects freshly uploaded bytes not yet committed to a
// workspace or fleet-operation record.
func (s *Server) CollectArtifacts(now time.Time) (artifact.GCResult, error) {
	var result artifact.GCResult
	if collector, ok := s.artifacts.(artifact.TenantCollector); ok {
		// A tenant whose artifact retention is stricter than the shared grace
		// window collects sooner, and only inside its own namespace. The plan
		// is resolved before the reference set is frozen so no tenant policy
		// read happens while collection holds the control mutex.
		cutoffs := s.tenantArtifactCutoffs(now)
		retention, perTenant := collector.(artifact.TenantRetentionCollector)
		err := s.Control.WithTenantArtifactReferences(func(references []artifact.TenantReference) error {
			var err error
			if perTenant && len(cutoffs) > 0 {
				result, err = retention.CollectTenantRetention(s.lifetime, references, now, now.Add(-s.opts.ArtifactGracePeriod), cutoffs)
				return err
			}
			result, err = collector.CollectTenants(s.lifetime, references, now, now.Add(-s.opts.ArtifactGracePeriod))
			return err
		})
		metrics.ArtifactGCRuns.Inc()
		return result, err
	}
	err := s.Control.WithArtifactReferences(func(references []string) error {
		var err error
		result, err = s.Store.Collect(references, now.Add(-s.opts.ArtifactGracePeriod))
		return err
	})
	metrics.ArtifactGCRuns.Inc()
	return result, err
}

// tenantArtifactCutoffs resolves each tenant's own artifact collection cutoff.
// A failure returns no cutoffs rather than a global one: a policy that cannot
// be read must not be approximated by sweeping every namespace on one tenant's
// schedule, and `doctor` reports the unenforced policy separately.
func (s *Server) tenantArtifactCutoffs(now time.Time) map[string]time.Time {
	plan, err := s.Control.TenantRetentionPlan(s.lifetime, now, s.opts.EventRetention, s.opts.ArtifactGracePeriod)
	if err != nil {
		metrics.TenantRetentionFailed.Inc()
		s.logger.Error("tenant artifact retention could not be resolved", "error", err)
		return nil
	}
	if len(plan.ArtifactCutoffs) > 0 {
		metrics.TenantRetentionEnforced.Inc()
	}
	return plan.ArtifactCutoffs
}

// PruneEvents removes every complete batch in the expired sequence prefix.
// It yields the event-log mutex between batches so current appends can make
// progress during a large first retention pass.
func (s *Server) PruneEvents(ctx context.Context, now time.Time) (int64, error) {
	const batch = 10_000
	var total int64
	plan, err := s.Control.TenantRetentionPlan(ctx, now, s.opts.EventRetention, s.opts.ArtifactGracePeriod)
	if err != nil {
		metrics.TenantRetentionFailed.Inc()
		return 0, err
	}
	// Content first, then the prefix. A tenant whose own retention expired
	// earlier than the floor loses its event payloads now; waiting for the
	// floor would hold that content for the longest-lived tenant's window.
	if _, err := s.Control.EnforceTenantEventRetention(ctx, plan); err != nil {
		return 0, err
	}
	// Age and count limits both delete only an oldest contiguous prefix. Run
	// age first so a quiet deployment still honors its audit-window policy.
	for {
		if err := ctx.Err(); err != nil {
			return total, err
		}
		n, err := s.Log.Prune(ctx, plan.PruneBefore.UnixMilli(), batch)
		total += n
		if err != nil {
			return total, err
		}
		if n < batch {
			break
		}
	}
	for {
		if err := ctx.Err(); err != nil {
			return total, err
		}
		n, err := s.Log.PruneSize(ctx, s.opts.MaxEvents, batch)
		total += n
		if err != nil {
			return total, err
		}
		if n < batch {
			metrics.EventGCRuns.Inc()
			return total, nil
		}
	}
}

// PruneControlRecords removes expired replay results and fired timers in
// bounded, resumable transactions. Pending timers are never collected.
func (s *Server) PruneControlRecords(ctx context.Context, now time.Time) (control.RecordPruneResult, error) {
	const batch = 10_000
	var total control.RecordPruneResult
	for {
		if err := ctx.Err(); err != nil {
			return total, err
		}
		result, err := s.Control.PruneRecords(ctx, now.Add(-s.opts.RecordRetention), batch)
		total.Mutations += result.Mutations
		total.Timers += result.Timers
		total.Workspaces += result.Workspaces
		total.FleetOperations += result.FleetOperations
		total.Assignments += result.Assignments
		total.LegacyIdem += result.LegacyIdem
		if err != nil {
			return total, err
		}
		if result.Mutations < batch && result.Timers < batch && result.Workspaces < batch &&
			result.FleetOperations < batch && result.Assignments < batch && result.LegacyIdem < batch {
			metrics.RecordGCRuns.Inc()
			return total, nil
		}
	}
}

func validateSecurityMode(opts Options) (string, error) {
	switch opts.Mode {
	case ModeStandalone:
		return proto.SecurityLocal, nil
	case ModeProductionSingleTenant, ModeProductionMultiTenant:
		for _, binding := range opts.Bindings {
			if binding.Secret != "" || secretsource.ValidateSource(binding.Source) != nil || opts.SecretResolver == nil {
				return "", fmt.Errorf("server: %s binding %q requires a valid external secret source and resolver", opts.Mode, binding.ID)
			}
		}
		if opts.Token != "" {
			return "", fmt.Errorf("server: %s refuses shared tokens", opts.Mode)
		}
		if opts.Authenticator == nil || opts.Authorizer == nil || opts.NodeAuthenticator == nil {
			return "", fmt.Errorf("server: %s requires principal and node identity", opts.Mode)
		}
		profile := proto.SecurityIsolated
		if opts.Mode == ModeProductionMultiTenant {
			profile = proto.SecurityMultiTenant
		}
		return profile, nil
	default:
		return "", fmt.Errorf("server: unknown security mode %q", opts.Mode)
	}
}

// Handler returns the HTTP mux.
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.Handle("/console/", s.console)
	mux.Handle("/console", http.RedirectHandler("/console/", http.StatusPermanentRedirect))
	mux.HandleFunc("/v1/link", s.handleLink)
	mux.HandleFunc("/v1/artifacts/", s.handleArtifact)
	mux.HandleFunc("/v1/events", s.handleEvents)
	mux.HandleFunc("/metrics", func(w http.ResponseWriter, r *http.Request) {
		if !s.authed(r) {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		var sb strings.Builder
		metrics.Default.Write(&sb)
		w.Header().Set("Content-Type", "text/plain; version=0.0.4")
		_, _ = io.WriteString(w, sb.String())
	})
	mux.HandleFunc("/healthz", s.handleHealth)
	mux.HandleFunc("/readyz", s.handleHealth)
	mux.HandleFunc("/livez", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{"ok": true})
	})
	s.apiRoutes(mux)
	s.identityRoutes(mux)
	return s.cors(mux)
}

// Serve listens on addr until ctx ends.
func (s *Server) Serve(ctx context.Context, addr string) error {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return errors.New("server: closed")
	}
	if s.serveCalled {
		s.mu.Unlock()
		return errors.New("server: Serve called more than once")
	}
	s.serveCalled = true
	s.mu.Unlock()
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		s.mu.Lock()
		s.readyErr = err
		s.mu.Unlock()
		s.readyOnce.Do(func() { close(s.ready) })
		return err
	}
	httpServer := &http.Server{Handler: s.Handler(), ReadHeaderTimeout: 30 * time.Second}
	s.mu.Lock()
	if s.closed {
		closedErr := errors.New("server: closed before listen became ready")
		s.readyErr = closedErr
		s.mu.Unlock()
		_ = ln.Close()
		s.readyOnce.Do(func() { close(s.ready) })
		return closedErr
	}
	s.ln = ln
	s.http = httpServer
	s.serving = true
	s.mu.Unlock()
	s.readyOnce.Do(func() { close(s.ready) })
	if s.Control.RecoveryPending() {
		s.gcWG.Add(1)
		go func() {
			defer s.gcWG.Done()
			s.reconcileController(s.lifetime)
		}()
	}
	go func() {
		<-ctx.Done()
		sctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = httpServer.Shutdown(sctx)
	}()
	err = httpServer.Serve(ln)
	if errors.Is(err, http.ErrServerClosed) {
		err = nil
	}
	s.mu.Lock()
	s.serving = false
	if err != nil {
		s.readyErr = err
	}
	s.mu.Unlock()
	return err
}

func (s *Server) reconcileController(ctx context.Context) {
	for ctx.Err() == nil && s.Control.RecoveryPending() {
		if err := s.Control.ReconcileRecovery(ctx); err != nil {
			s.logger.Warn("controller recovery reconciliation deferred", "err", err)
			select {
			case <-ctx.Done():
				return
			case <-time.After(250 * time.Millisecond):
			}
			continue
		}
		if s.replication != nil {
			shipCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
			err := s.replication.initialShip(shipCtx)
			cancel()
			if err != nil {
				s.replication.fail(fmt.Errorf("publish reconciled controller state: %w", err))
				return
			}
			s.replication.markRecovered()
			s.replication.startShipping()
		}
	}
}

// Addr returns the bound address after Serve.
func (s *Server) Addr() string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.ln == nil {
		return ""
	}
	return s.ln.Addr().String()
}

// WaitReady waits until Serve has bound its listener or failed.
func (s *Server) WaitReady(ctx context.Context) (string, error) {
	select {
	case <-s.ready:
		s.mu.RLock()
		defer s.mu.RUnlock()
		if s.readyErr != nil {
			return "", s.readyErr
		}
		if s.ln == nil {
			return "", errors.New("server: listener unavailable")
		}
		return s.ln.Addr().String(), nil
	case <-ctx.Done():
		return "", ctx.Err()
	}
}

// Close stops everything.
func (s *Server) Close() error {
	s.closeOnce.Do(func() {
		s.mu.Lock()
		s.closed = true
		httpServer := s.http
		if !s.serveCalled {
			s.readyErr = errors.New("server: closed before Serve")
			s.readyOnce.Do(func() { close(s.ready) })
		}
		s.mu.Unlock()
		if s.gcCancel != nil {
			s.gcCancel()
		}
		s.lifetimeCancel()
		s.clients.close()
		s.gcWG.Wait()
		var errs []error
		if httpServer != nil {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			errs = append(errs, httpServer.Shutdown(ctx))
			cancel()
		}
		s.Relay.Close()
		s.Control.Stop()
		if s.replication != nil {
			s.replication.close()
		}
		errs = append(errs, s.Log.Close())
		if s.tempArtDir != "" {
			errs = append(errs, os.RemoveAll(s.tempArtDir))
		}
		s.closeErr = errors.Join(errs...)
	})
	return s.closeErr
}

// AcceptConn serves an already-established transport (tests, embedded use).
func (s *Server) AcceptConn(ctx context.Context, conn transport.Conn) error {
	s.mu.RLock()
	closed := s.closed
	s.mu.RUnlock()
	if closed {
		_ = conn.Close()
		return errors.New("server: closed")
	}
	return s.Relay.Serve(ctx, conn)
}

func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	s.mu.RLock()
	closed := s.closed
	serving := s.serving
	serveCalled := s.serveCalled
	s.mu.RUnlock()
	dbErr := s.db.PingContext(r.Context())
	ready := !closed && dbErr == nil && (!serveCalled || serving)
	var replicationErr error
	if s.replication != nil {
		replicationErr = s.replication.ready()
		ready = ready && replicationErr == nil && !s.Control.RecoveryPending()
	}
	w.Header().Set("Content-Type", "application/json")
	if !ready {
		w.WriteHeader(http.StatusServiceUnavailable)
	}
	body := map[string]any{"ok": ready, "peers": len(s.Relay.Peers()), "serving": serving, "security_mode": s.opts.Mode, "security_ready": ready}
	if dbErr != nil {
		body["database"] = dbErr.Error()
	}
	if replicationErr != nil {
		body["replication"] = replicationErr.Error()
	}
	if s.Control.RecoveryPending() {
		body["recovery"] = "reconciling"
	}
	_ = json.NewEncoder(w).Encode(body)
}

func (s *Server) handleLink(w http.ResponseWriter, r *http.Request) {
	conn, err := transport.AcceptWS(w, r)
	if err != nil {
		return
	}
	_ = s.Relay.Serve(r.Context(), conn)
}

func (s *Server) authed(r *http.Request) bool {
	tok, bearer := strings.CutPrefix(r.Header.Get("Authorization"), "Bearer ")
	if !bearer || tok == "" {
		return s.opts.Token == "" && s.opts.Authenticator == nil
	}
	// Standalone deployments may layer tenant-aware client authentication on
	// top of the shared node token. The node still needs that configured token
	// for artifact transfer; production modes forbid configuring it at all.
	if s.opts.Token != "" && subtle.ConstantTimeCompare([]byte(tok), []byte(s.opts.Token)) == 1 {
		return true
	}
	if s.opts.Authenticator != nil {
		_, err := s.opts.Authenticator.Authenticate(r.Context(), control.Credential{Token: tok})
		return err == nil
	}
	return false
}

func (s *Server) handleArtifact(w http.ResponseWriter, r *http.Request) {
	subject, legacy, ok := s.artifactSubject(r)
	if !ok {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}
	id := strings.TrimPrefix(r.URL.Path, "/v1/artifacts/")
	if _, err := artifact.Digest(id); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	action := control.ActionRead
	if r.Method == http.MethodPut {
		action = control.ActionWrite
	}
	if !legacy {
		if hasRole(subject.Roles, identity.RoleNode) {
			generation, err := strconv.ParseUint(r.Header.Get(ArtifactGenerationHeader), 10, 64)
			if err != nil || generation == 0 || s.opts.NodeArtifactAuthorizer == nil ||
				s.opts.NodeArtifactAuthorizer.AuthorizeNodeArtifact(r.Context(), subject.ID, subject.Tenant,
					r.Header.Get(ArtifactWorkspaceHeader), generation, r.Method, id, r.Header.Get(ArtifactProofHeader)) != nil {
				http.Error(w, "not found", http.StatusNotFound)
				return
			}
		} else if s.opts.Authorizer != nil {
			resource := control.Resource{Kind: "artifact", ID: id, Tenant: subject.Tenant, Owner: subject.ID}
			if err := s.opts.Authorizer.Check(r.Context(), subject, action, resource); err != nil {
				http.Error(w, "not found", http.StatusNotFound)
				return
			}
		}
	}
	store, err := s.artifacts.ResolveTenant(subject.Tenant)
	if err != nil {
		http.Error(w, "not found", http.StatusNotFound)
		return
	}
	switch r.Method {
	case http.MethodPut:
		if r.ContentLength > s.opts.MaxArtifactBytes {
			http.Error(w, artifact.ErrTooLarge.Error(), http.StatusRequestEntityTooLarge)
			return
		}
		n, err := store.PutExpected(id, r.Body, s.opts.MaxArtifactBytes)
		if err != nil {
			status := http.StatusInternalServerError
			switch {
			case errors.Is(err, artifact.ErrTooLarge), errors.Is(err, encrypted.ErrTooLarge):
				status = http.StatusRequestEntityTooLarge
			case errors.Is(err, artifact.ErrStoreFull), errors.Is(err, encrypted.ErrStagingFull), errors.Is(err, encrypted.ErrPhysicalStoreFull):
				status = http.StatusInsufficientStorage
			case errors.Is(err, artifact.ErrDigestMismatch):
				status = http.StatusBadRequest
			}
			http.Error(w, http.StatusText(status), status)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{"id": id, "bytes": n})
	case http.MethodHead:
		size, err := store.Head(id)
		if err != nil {
			writeArtifactReadError(w, err)
			return
		}
		w.Header().Set("Content-Type", "application/gzip")
		w.Header().Set("Content-Length", strconv.FormatInt(size, 10))
	case http.MethodGet:
		rc, size, err := store.Open(id)
		if err != nil {
			writeArtifactReadError(w, err)
			return
		}
		defer rc.Close()
		w.Header().Set("Content-Type", "application/gzip")
		w.Header().Set("Content-Length", strconv.FormatInt(size, 10))
		_, _ = io.Copy(w, rc)
	default:
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}

func writeArtifactReadError(w http.ResponseWriter, err error) {
	status := http.StatusInternalServerError
	if errors.Is(err, fs.ErrNotExist) {
		status = http.StatusNotFound
	} else if errors.Is(err, encrypted.ErrKeyUnavailable) {
		status = http.StatusServiceUnavailable
	}
	http.Error(w, http.StatusText(status), status)
}

func (s *Server) artifactSubject(r *http.Request) (control.Subject, bool, bool) {
	token, bearer := strings.CutPrefix(r.Header.Get("Authorization"), "Bearer ")
	// A standalone server with no shared token and no authenticator has nothing
	// to check a credential against, and every other surface — the relay hello
	// and the console — already admits any bearer in that configuration. This
	// endpoint used to admit only the *absence* of one, so presenting a
	// credential made a caller less authorized than presenting none: an
	// operator with REMOUNT_TOKEN exported saw `remount push` fail with 401
	// against their own laptop, and the repository's own move benchmark could
	// not run against a standalone server at all. Deciding the mode before
	// reading the header keeps the two answers the same.
	unauthenticated := s.opts.Mode == ModeStandalone && s.opts.Token == "" && s.opts.Authenticator == nil
	if unauthenticated {
		subject := control.Subject{ID: "local-user", Tenant: "local", Roles: []string{"admin"}}
		return subject, true, subject.Tenant != ""
	}
	if !bearer || token == "" {
		return control.Subject{}, false, false
	}
	if s.opts.Token != "" && subtle.ConstantTimeCompare([]byte(token), []byte(s.opts.Token)) == 1 {
		return control.Subject{ID: "legacy-node", Tenant: "local", Roles: []string{identity.RoleNode}}, true, true
	}
	if s.opts.Authenticator == nil {
		return control.Subject{}, false, false
	}
	subject, err := s.opts.Authenticator.Authenticate(r.Context(), control.Credential{Token: token})
	if err != nil || subject.ID == "" || subject.Tenant == "" || subject.Tenant == "*" {
		return control.Subject{}, false, false
	}
	return subject, false, true
}

func hasRole(roles []string, want string) bool {
	for _, role := range roles {
		if role == want {
			return true
		}
	}
	return false
}
