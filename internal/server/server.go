// Package server hosts the relay, the control plane and the artifact store
// behind one HTTP listener: /v1/link (WebSocket for nodes and clients),
// /v1/artifacts/{id} (PUT/GET blobs), /v1/events (webhook wake), /healthz.
package server

import (
	"context"
	"crypto/ed25519"
	"crypto/subtle"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"remount.dev/remount/internal/artifact"
	"remount.dev/remount/internal/control"
	"remount.dev/remount/internal/eventlog"
	"remount.dev/remount/internal/metrics"
	"remount.dev/remount/internal/proto"
	"remount.dev/remount/internal/relay"
	"remount.dev/remount/internal/transport"
)

// Options configure a server.
type Options struct {
	DataDir  string // sqlite db + artifacts; "" = in-memory (tests / --standalone)
	Token    string
	Bindings []control.Binding
	LeaseSec int64
	// MaxArtifactBytes bounds one uploaded compressed artifact. Default 8 GiB.
	MaxArtifactBytes int64
	// MaxArtifactStoreBytes and MaxArtifactObjects bound retained artifacts and
	// concurrent staging. Defaults are 64 GiB and 100,000 objects.
	MaxArtifactStoreBytes int64
	MaxArtifactObjects    int
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
	ApprovedNodes map[string]control.NodeApproval
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
}

const (
	ModeStandalone             = "standalone"
	ModeProductionSingleTenant = "production-single-tenant"
	ModeProductionMultiTenant  = "production-multi-tenant"
)

// Server is a running Remount server.
type Server struct {
	opts    Options
	Control *control.Control
	Relay   *relay.Relay
	Store   *artifact.Store
	Log     *eventlog.Log
	db      *sql.DB
	http    *http.Server
	ln      net.Listener
	logger  *slog.Logger

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
	lifetime       context.Context
	lifetimeCancel context.CancelFunc
	clients        *clientPool
}

// New builds a server. Call Serve or Handler.
func New(opts Options) (*Server, error) {
	if opts.Logger == nil {
		opts.Logger = slog.Default()
	}
	if opts.MaxArtifactBytes < 0 || opts.MaxArtifactStoreBytes < 0 || opts.MaxArtifactObjects < 0 || opts.MaxEvents < 0 {
		return nil, errors.New("server: artifact and event limits must not be negative")
	}
	if opts.MaxConcurrentRequests < 0 || opts.MaxWorkspacesPerTenant < 0 || opts.MaxWorkspacesPerSubject < 0 || opts.MaxMutationRecords < 0 ||
		opts.MaxTimers < 0 || opts.MaxTimersPerWorkspace < 0 {
		return nil, errors.New("server: control-plane quotas must not be negative")
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
	floor, err := validateSecurityMode(opts)
	if err != nil {
		return nil, err
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
	log := eventlog.New(sq)
	ctrl, err := control.New(control.Options{DB: sq.DB(), Log: log, Token: opts.Token,
		Bindings: opts.Bindings, LeaseSec: opts.LeaseSec, Logger: opts.Logger, Artifacts: store,
		Authenticator: opts.Authenticator, Authorizer: opts.Authorizer, ApprovedNodes: opts.ApprovedNodes,
		SecurityProfileFloor: floor, MaxWorkspacesPerTenant: opts.MaxWorkspacesPerTenant,
		MaxWorkspacesPerSubject: opts.MaxWorkspacesPerSubject, MaxMutationRecords: opts.MaxMutationRecords,
		MaxTimers: opts.MaxTimers, MaxTimersPerWorkspace: opts.MaxTimersPerWorkspace,
		MaxConcurrentRequests: opts.MaxConcurrentRequests, MaxEvents: opts.MaxEvents, PublicURL: opts.PublicURL})
	if err != nil {
		_ = log.Close()
		if opts.DataDir == "" {
			_ = os.RemoveAll(artDir)
		}
		return nil, err
	}
	r := relay.New(ctrl)
	ctrl.Attach(r)
	ctrl.Start()
	s := &Server{
		opts: opts, Control: ctrl, Relay: r, Store: store, Log: log, db: sq.DB(), logger: opts.Logger,
		ready: make(chan struct{}),
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
	s.gcWG.Add(1)
	go s.clientSweepLoop(s.lifetime)
	if opts.ArtifactGCInterval > 0 || opts.EventGCInterval > 0 || opts.RecordGCInterval > 0 {
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
	return s, nil
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
	err := s.Control.WithArtifactReferences(func(references []string) error {
		var err error
		result, err = s.Store.Collect(references, now.Add(-s.opts.ArtifactGracePeriod))
		return err
	})
	metrics.ArtifactGCRuns.Inc()
	return result, err
}

// PruneEvents removes every complete batch in the expired sequence prefix.
// It yields the event-log mutex between batches so current appends can make
// progress during a large first retention pass.
func (s *Server) PruneEvents(ctx context.Context, now time.Time) (int64, error) {
	const batch = 10_000
	var total int64
	// Age and count limits both delete only an oldest contiguous prefix. Run
	// age first so a quiet deployment still honors its audit-window policy.
	for {
		if err := ctx.Err(); err != nil {
			return total, err
		}
		n, err := s.Log.Prune(ctx, now.Add(-s.opts.EventRetention).UnixMilli(), batch)
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
		if opts.Token == "" || opts.Authenticator == nil || opts.Authorizer == nil || len(opts.ApprovedNodes) == 0 {
			return "", fmt.Errorf("server: %s requires a node token, authenticator, authorizer, and approved nodes", opts.Mode)
		}
		profile := proto.SecurityIsolated
		if opts.Mode == ModeProductionMultiTenant {
			profile = proto.SecurityMultiTenant
		}
		for nodeID, approval := range opts.ApprovedNodes {
			if len(approval.PubKey) != ed25519.PublicKeySize || len(approval.Info.BackendDescriptors) == 0 {
				return "", fmt.Errorf("server: approved node %s lacks a key or backend descriptors", nodeID)
			}
			for _, descriptor := range approval.Info.BackendDescriptors {
				if err := proto.ValidateBackendSecurity(proto.SecuritySpec{Profile: profile}, descriptor); err != nil {
					return "", fmt.Errorf("server: approved node %s backend %s cannot satisfy %s: %w", nodeID, descriptor.Name, profile, err)
				}
			}
		}
		return profile, nil
	default:
		return "", fmt.Errorf("server: unknown security mode %q", opts.Mode)
	}
}

// Handler returns the HTTP mux.
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
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
	w.Header().Set("Content-Type", "application/json")
	if !ready {
		w.WriteHeader(http.StatusServiceUnavailable)
	}
	body := map[string]any{"ok": ready, "peers": len(s.Relay.Peers()), "serving": serving, "security_mode": s.opts.Mode, "security_ready": ready}
	if dbErr != nil {
		body["database"] = dbErr.Error()
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
	if s.opts.Token == "" {
		return true
	}
	h := r.Header.Get("Authorization")
	tok := strings.TrimPrefix(h, "Bearer ")
	return subtle.ConstantTimeCompare([]byte(tok), []byte(s.opts.Token)) == 1
}

func (s *Server) handleArtifact(w http.ResponseWriter, r *http.Request) {
	if !s.authed(r) {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}
	id := strings.TrimPrefix(r.URL.Path, "/v1/artifacts/")
	if _, err := artifact.Digest(id); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	switch r.Method {
	case http.MethodPut:
		if r.ContentLength > s.opts.MaxArtifactBytes {
			http.Error(w, artifact.ErrTooLarge.Error(), http.StatusRequestEntityTooLarge)
			return
		}
		n, err := s.Store.PutExpected(id, r.Body, s.opts.MaxArtifactBytes)
		if err != nil {
			status := http.StatusInternalServerError
			switch {
			case errors.Is(err, artifact.ErrTooLarge):
				status = http.StatusRequestEntityTooLarge
			case errors.Is(err, artifact.ErrStoreFull):
				status = http.StatusInsufficientStorage
			case errors.Is(err, artifact.ErrDigestMismatch):
				status = http.StatusBadRequest
			}
			http.Error(w, err.Error(), status)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{"id": id, "bytes": n})
	case http.MethodGet, http.MethodHead:
		rc, size, err := s.Store.Open(id)
		if err != nil {
			http.Error(w, "not found", http.StatusNotFound)
			return
		}
		defer rc.Close()
		w.Header().Set("Content-Type", "application/gzip")
		w.Header().Set("Content-Length", strconv.FormatInt(size, 10))
		if r.Method == http.MethodHead {
			return
		}
		_, _ = io.Copy(w, rc)
	default:
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}
