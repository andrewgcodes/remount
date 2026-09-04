// Package modelfake serves a bounded, deterministic OpenAI-compatible
// endpoint for tests: the Responses and Chat Completions shapes a real
// harness actually calls, and nothing else.
//
// It exists so the OpenCode end-to-end lane is keyless. Three properties make
// it evidence rather than a stub. It is deterministic: an answer is a pure
// function of the request, so a retry replays rather than advances. It is
// bounded: body size, request count and concurrency are capped, so a runaway
// harness fails the test instead of hanging it. And it demands a synthetic
// bearer of its own, so a lane that reached it proves the broker substituted
// the workspace's placeholder rather than proving the check was skipped.
package modelfake

import (
	"crypto/subtle"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
)

// Fixed clock and id stems: two runs of the same script must be byte-identical
// apart from what the harness itself sends.
const (
	createdAt  = 1735689600 // 2025-01-01T00:00:00Z
	systemFP   = "fp_remount_modelfake"
	defaultAux = "Deterministic Lane"
)

// Call is one scripted tool call. Arguments is marshalled with encoding/json,
// whose map ordering is sorted, so the bytes are stable.
type Call struct {
	// Name must match a tool the request declares. A script that names a tool
	// the harness no longer has is refused with a 409 rather than silently
	// producing a turn the harness drops, which is how a stale script against
	// a newer harness stays legible.
	Name string
	// Arguments is the tool input object.
	Arguments map[string]any
}

// Step is one scripted assistant turn: text, tool calls, or both.
type Step struct {
	Text  string
	Calls []Call
}

// Options configures a Server.
type Options struct {
	// Bearer is the synthetic upstream credential. Every request must present
	// it; a request without it is 401 and is still recorded, so a test can
	// prove the rejection path is live rather than assumed.
	Bearer string
	// Model is the scripted model. A request naming any other model gets Aux,
	// which is how OpenCode's separate title-generator model is answered
	// without letting it walk the script.
	Model string
	// Script is indexed by the number of tool results the request already
	// carries, so the answer depends only on the conversation in front of the
	// server. Running past the end is an error, not a silent empty turn.
	Script []Step
	// Aux answers every model that is not Model.
	Aux string
	// MaxBodyBytes caps one request body; 0 selects 8 MiB.
	MaxBodyBytes int64
	// MaxRequests caps the whole run; 0 selects 64. Exceeding it is 429 so a
	// looping harness ends the test quickly with a legible reason.
	MaxRequests int
	// MaxConcurrent caps in-flight requests; 0 selects 8.
	MaxConcurrent int
}

// Request is one request as it actually arrived.
type Request struct {
	Method string
	Path   string
	// Authorization is the header verbatim. Recording it is the point: a test
	// asserts the workspace's placeholder never arrived and the synthetic
	// upstream value did.
	Authorization string
	Model         string
	Body          string
	Status        int
	// Step is the script index the request resolved to, or -1.
	Step int
}

// Server is a running script. Its zero value is not usable; call New.
type Server struct {
	opts  Options
	slots chan struct{}

	mu       sync.Mutex
	requests []Request
}

// New validates the script and returns a server. A missing bearer or an empty
// script is a programming error: both would turn the lane green for the wrong
// reason.
func New(opts Options) (*Server, error) {
	if opts.Bearer == "" {
		return nil, errors.New("modelfake: a synthetic bearer is required; without it the broker substitution is not exercised")
	}
	if opts.Model == "" {
		return nil, errors.New("modelfake: model is required")
	}
	if len(opts.Script) == 0 {
		return nil, errors.New("modelfake: script is required")
	}
	for i, step := range opts.Script {
		if step.Text == "" && len(step.Calls) == 0 {
			return nil, fmt.Errorf("modelfake: script[%d] is empty", i)
		}
		for j, call := range step.Calls {
			if call.Name == "" {
				return nil, fmt.Errorf("modelfake: script[%d].calls[%d] has no name", i, j)
			}
		}
	}
	if opts.Aux == "" {
		opts.Aux = defaultAux
	}
	if opts.MaxBodyBytes <= 0 {
		opts.MaxBodyBytes = 8 << 20
	}
	if opts.MaxRequests <= 0 {
		opts.MaxRequests = 64
	}
	if opts.MaxConcurrent <= 0 {
		opts.MaxConcurrent = 8
	}
	return &Server{opts: opts, slots: make(chan struct{}, opts.MaxConcurrent)}, nil
}

// Requests returns every request the server saw, in arrival order.
func (s *Server) Requests() []Request {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]Request(nil), s.requests...)
}

// Count returns how many requests reached the server.
func (s *Server) Count() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.requests)
}

// Saw reports whether any recorded request body or Authorization header
// contains needle. Leak scans use it to prove the placeholder stopped at the
// broker.
func (s *Server) Saw(needle string) bool {
	if needle == "" {
		return false
	}
	for _, r := range s.Requests() {
		if strings.Contains(r.Body, needle) || strings.Contains(r.Authorization, needle) {
			return true
		}
	}
	return false
}

// Handler returns the HTTP surface. Wrap it in httptest.NewTLSServer: the
// broker's /d/ path always re-originates over TLS.
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/responses", s.responses)
	mux.HandleFunc("/v1/chat/completions", s.chat)
	mux.HandleFunc("/v1/models", s.models)
	// An unexpected path is bounded and recorded like any other request: a
	// harness calling something the lane does not implement must be visible,
	// not a silent 404 outside the record.
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		if !s.enter(w, r) {
			return
		}
		defer s.exit()
		s.record(r, "", nil, http.StatusNotFound, -1)
		writeError(w, http.StatusNotFound, "unsupported_path", "modelfake serves /v1/responses, /v1/chat/completions and /v1/models")
	})
	return mux
}

func (s *Server) models(w http.ResponseWriter, r *http.Request) {
	if !s.enter(w, r) {
		return
	}
	defer s.exit()
	if _, ok := s.authorize(w, r, nil); !ok {
		return
	}
	s.record(r, "", nil, http.StatusOK, -1)
	writeJSON(w, http.StatusOK, map[string]any{
		"object": "list",
		"data": []any{map[string]any{
			"id": s.opts.Model, "object": "model", "created": createdAt, "owned_by": "remount-modelfake",
		}},
	})
}

// enter admits a request against the concurrency and total-request bounds.
func (s *Server) enter(w http.ResponseWriter, r *http.Request) bool {
	select {
	case s.slots <- struct{}{}:
	default:
		s.record(r, "", nil, http.StatusTooManyRequests, -1)
		writeError(w, http.StatusTooManyRequests, "concurrency_exceeded", "modelfake concurrency bound reached")
		return false
	}
	s.mu.Lock()
	over := len(s.requests) >= s.opts.MaxRequests
	s.mu.Unlock()
	if over {
		s.record(r, "", nil, http.StatusTooManyRequests, -1)
		writeError(w, http.StatusTooManyRequests, "request_limit_exceeded", "modelfake request bound reached")
		<-s.slots
		return false
	}
	return true
}

func (s *Server) exit() { <-s.slots }

// authorize enforces the synthetic bearer. body is recorded with the failure
// so a 401 explains itself.
func (s *Server) authorize(w http.ResponseWriter, r *http.Request, body []byte) (string, bool) {
	auth := r.Header.Get("Authorization")
	token := strings.TrimSpace(strings.TrimPrefix(auth, "Bearer "))
	if !strings.HasPrefix(auth, "Bearer ") || subtle.ConstantTimeCompare([]byte(token), []byte(s.opts.Bearer)) != 1 {
		s.record(r, "", body, http.StatusUnauthorized, -1)
		writeError(w, http.StatusUnauthorized, "invalid_api_key", "modelfake requires the synthetic upstream bearer")
		return "", false
	}
	return token, true
}

func (s *Server) record(r *http.Request, model string, body []byte, status, step int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.requests = append(s.requests, Request{
		Method: r.Method, Path: r.URL.Path, Authorization: r.Header.Get("Authorization"),
		Model: model, Body: string(body), Status: status, Step: step,
	})
}

// readBody reads at most MaxBodyBytes+1 so an oversized body is refused rather
// than truncated into a request that looks valid.
func (s *Server) readBody(w http.ResponseWriter, r *http.Request) ([]byte, bool) {
	body, err := io.ReadAll(io.LimitReader(r.Body, s.opts.MaxBodyBytes+1))
	if err != nil {
		s.record(r, "", nil, http.StatusBadRequest, -1)
		writeError(w, http.StatusBadRequest, "read_failed", "modelfake could not read the request body")
		return nil, false
	}
	if int64(len(body)) > s.opts.MaxBodyBytes {
		s.record(r, "", nil, http.StatusRequestEntityTooLarge, -1)
		writeError(w, http.StatusRequestEntityTooLarge, "body_too_large", "modelfake body bound reached")
		return nil, false
	}
	return body, true
}

// checkTools refuses a step whose calls name a tool the request did not
// declare. A request that declares none (a plain completion) is not checked.
func checkTools(declared []string, step Step) error {
	if len(declared) == 0 {
		return nil
	}
	for _, call := range step.Calls {
		found := false
		for _, name := range declared {
			if name == call.Name {
				found = true
				break
			}
		}
		if !found {
			return fmt.Errorf("the script calls %q but the request declares only %s", call.Name, strings.Join(declared, ", "))
		}
	}
	return nil
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func writeError(w http.ResponseWriter, status int, code, message string) {
	writeJSON(w, status, map[string]any{"error": map[string]any{
		"message": message, "type": code, "code": code, "param": nil,
	}})
}
