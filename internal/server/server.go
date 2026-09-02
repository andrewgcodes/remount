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
	"io"
	"log/slog"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
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
	Logger   *slog.Logger
}

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
}

// New builds a server. Call Serve or Handler.
func New(opts Options) (*Server, error) {
	if opts.Logger == nil {
		opts.Logger = slog.Default()
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
			return nil, err
		}
		artDir = d
	}
	store, err := artifact.NewStore(artDir)
	if err != nil {
		return nil, err
	}
	log := eventlog.New(sq)
	ctrl, err := control.New(control.Options{DB: sq.DB(), Log: log, Token: opts.Token,
		Bindings: opts.Bindings, LeaseSec: opts.LeaseSec, Logger: opts.Logger, Artifacts: store})
	if err != nil {
		return nil, err
	}
	r := relay.New(ctrl)
	ctrl.Attach(r)
	ctrl.Start()
	return &Server{opts: opts, Control: ctrl, Relay: r, Store: store, Log: log, db: sq.DB(), logger: opts.Logger}, nil
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
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{"ok": true, "peers": len(s.Relay.Peers())})
	})
	return mux
}

// Serve listens on addr until ctx ends.
func (s *Server) Serve(ctx context.Context, addr string) error {
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}
	s.ln = ln
	s.http = &http.Server{Handler: s.Handler(), ReadHeaderTimeout: 30 * time.Second}
	go func() {
		<-ctx.Done()
		sctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = s.http.Shutdown(sctx)
	}()
	err = s.http.Serve(ln)
	if errors.Is(err, http.ErrServerClosed) {
		err = nil
	}
	return err
}

// Addr returns the bound address after Serve.
func (s *Server) Addr() string {
	if s.ln == nil {
		return ""
	}
	return s.ln.Addr().String()
}

// Close stops everything.
func (s *Server) Close() error {
	s.Relay.Close()
	s.Control.Stop()
	return s.Log.Close()
}

// AcceptConn serves an already-established transport (tests, embedded use).
func (s *Server) AcceptConn(ctx context.Context, conn transport.Conn) error {
	return s.Relay.Serve(ctx, conn)
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
		got, n, err := s.Store.Put(r.Body)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		if got != id {
			_ = s.Store.Delete(got)
			http.Error(w, "digest mismatch: body is "+got, http.StatusBadRequest)
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
