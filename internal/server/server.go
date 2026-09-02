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
	Logger           *slog.Logger
	// Mode declares the deployment trust posture.
	Mode          string
	Authenticator control.Authenticator
	Authorizer    control.Authorizer
	ApprovedNodes map[string]control.NodeApproval
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
}

// New builds a server. Call Serve or Handler.
func New(opts Options) (*Server, error) {
	if opts.Logger == nil {
		opts.Logger = slog.Default()
	}
	if opts.MaxArtifactBytes <= 0 {
		opts.MaxArtifactBytes = 8 << 30
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
	store, err := artifact.NewStore(artDir)
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
		SecurityProfileFloor: floor})
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
	return s, nil
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
	return mux
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

// handleEvents accepts a JSON event and appends it (webhook wake).
func (s *Server) handleEvents(w http.ResponseWriter, r *http.Request) {
	if !s.authed(r) {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	var in struct {
		Type    string          `json:"type"`
		Stream  string          `json:"stream"`
		Payload json.RawMessage `json:"payload"`
	}
	if err := json.NewDecoder(io.LimitReader(r.Body, 1<<20)).Decode(&in); err != nil || in.Type == "" {
		http.Error(w, "bad event", http.StatusBadRequest)
		return
	}
	var payload any
	if len(in.Payload) > 0 {
		_ = json.Unmarshal(in.Payload, &payload)
	}
	e := proto.Event{Type: in.Type, Stream: in.Stream, Principal: "webhook"}
	if payload != nil {
		e.Payload = proto.MustMarshal(payload)
	}
	// Deliberately not r.Context(): the append, the timers it fires and the
	// offers that follow outlive this HTTP response.
	ctx, cancel := context.WithTimeout(context.WithoutCancel(r.Context()), 30*time.Second)
	defer cancel()
	if err := s.Control.PostEvents(ctx, "webhook", []proto.Event{e}); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusAccepted)
}
