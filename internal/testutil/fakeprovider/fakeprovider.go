// Package fakeprovider serves the three upstream shapes Remount's brokered
// credential examples talk to, so those examples can be executed in CI with no
// network access and no provider key.
//
// Each surface takes its credential in a different place, which is the point:
// together they exercise every substitution location a binding can declare
// except the form field, and each one answers 401 unless the exact expected
// credential arrived. An example that "passed" because the placeholder reached
// the upstream unsubstituted would fail here, which is what makes the examples
// evidence rather than decoration.
//
//	GET  /v1/models            OpenAI-compatible; Authorization: Bearer <key>
//	POST /v1/chat/completions  OpenAI-compatible; Authorization: Bearer <key>
//	GET  /search?q=…&key=…     a search API taking its key in the query string
//	POST /v1/records           a custom service taking its token at /auth/token
//
// The server speaks TLS with a self-signed certificate, because the broker
// refuses to substitute a credential into a plaintext request. Callers pass
// Pool() to whatever trusts the upstream (broker.Options.RootCAs, or
// node.Options.BrokerRootCAs).
package fakeprovider

import (
	"crypto/subtle"
	"crypto/x509"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
)

// MaxBodyBytes bounds what the fake reads from one request. A test upstream
// with no bound is a memory sink the moment a test sends the wrong thing.
const MaxBodyBytes = 1 << 20

// Config names the credential each surface requires. An empty value means the
// surface is disabled and answers 404, so a test can prove one shape in
// isolation.
type Config struct {
	// ModelKey is required in `Authorization: Bearer` on the model surface.
	ModelKey string
	// SearchKey is required as the `key` query parameter on /search.
	SearchKey string
	// ServiceToken is required as the JSON string at /auth/token on /v1/records.
	ServiceToken string
}

// Counts reports what the upstream saw. Authorized and Unauthorized are the
// two outcomes a test asserts on; Placeholders counts requests that carried a
// credential starting with "ref:", which is the shape of an unsubstituted
// Remount placeholder and therefore a broker defect rather than a user error.
type Counts struct {
	Authorized   int
	Unauthorized int
	Placeholders int
}

// Provider is a running fake upstream. Close it when the test ends.
type Provider struct {
	server *httptest.Server
	config Config

	mu     sync.Mutex
	counts Counts
	seen   []string
}

// New starts the fake upstream on a loopback TLS listener.
func New(config Config) *Provider {
	p := &Provider{config: config}
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/models", p.models)
	mux.HandleFunc("/v1/chat/completions", p.chatCompletions)
	mux.HandleFunc("/search", p.search)
	mux.HandleFunc("/v1/records", p.records)
	p.server = httptest.NewTLSServer(mux)
	return p
}

// Close stops the server.
func (p *Provider) Close() { p.server.Close() }

// Host is the `host:port` a binding names as its destination and a workspace
// puts in a `/d/<host>/…` broker URL.
func (p *Provider) Host() string { return strings.TrimPrefix(p.server.URL, "https://") }

// URL is the upstream's base URL.
func (p *Provider) URL() string { return p.server.URL }

// Certificate is the fake's self-signed leaf, so a caller running more than
// one fake can build a single pool that trusts all of them.
func (p *Provider) Certificate() *x509.Certificate { return p.server.Certificate() }

// Pool trusts the fake's self-signed certificate.
func (p *Provider) Pool() *x509.CertPool {
	pool := x509.NewCertPool()
	pool.AddCert(p.server.Certificate())
	return pool
}

// Counts returns what the upstream has seen so far.
func (p *Provider) Counts() Counts {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.counts
}

// Credentials returns every credential value the upstream was sent, so a test
// can assert the real one arrived and the placeholder never did.
func (p *Provider) Credentials() []string {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]string(nil), p.seen...)
}

// record accounts for one request and reports whether the credential matched.
func (p *Provider) record(presented, want string) bool {
	ok := want != "" && subtle.ConstantTimeCompare([]byte(presented), []byte(want)) == 1
	p.mu.Lock()
	defer p.mu.Unlock()
	p.seen = append(p.seen, presented)
	if ok {
		p.counts.Authorized++
	} else {
		p.counts.Unauthorized++
	}
	if strings.HasPrefix(presented, "ref:") {
		p.counts.Placeholders++
	}
	return ok
}

func writeJSON(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	encoder := json.NewEncoder(w)
	encoder.SetEscapeHTML(false)
	_ = encoder.Encode(body)
}

// unauthorized is the shape every real provider answers with, minus anything
// that could echo the presented credential back to the caller.
func unauthorized(w http.ResponseWriter, detail string) {
	writeJSON(w, http.StatusUnauthorized, map[string]any{
		"error": map[string]any{"type": "authentication_error", "message": detail},
	})
}

func (p *Provider) models(w http.ResponseWriter, r *http.Request) {
	if p.config.ModelKey == "" {
		http.NotFound(w, r)
		return
	}
	if !p.record(bearer(r), p.config.ModelKey) {
		unauthorized(w, "missing or invalid api key")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"object": "list",
		"data": []map[string]any{
			{"id": "fake-model-small", "object": "model", "owned_by": "remount-fakeprovider"},
			{"id": "fake-model-large", "object": "model", "owned_by": "remount-fakeprovider"},
		},
	})
}

func (p *Provider) chatCompletions(w http.ResponseWriter, r *http.Request) {
	if p.config.ModelKey == "" {
		http.NotFound(w, r)
		return
	}
	if !p.record(bearer(r), p.config.ModelKey) {
		unauthorized(w, "missing or invalid api key")
		return
	}
	var request struct {
		Model    string `json:"model"`
		Messages []struct {
			Role    string `json:"role"`
			Content string `json:"content"`
		} `json:"messages"`
	}
	if err := decodeBody(r, &request); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]any{
			"error": map[string]any{"type": "invalid_request_error", "message": "request body is not JSON"},
		})
		return
	}
	prompt := ""
	if n := len(request.Messages); n > 0 {
		prompt = request.Messages[n-1].Content
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"id": "chatcmpl-fake", "object": "chat.completion", "model": request.Model,
		"choices": []map[string]any{{
			"index":         0,
			"finish_reason": "stop",
			"message":       map[string]any{"role": "assistant", "content": "fake completion for: " + prompt},
		}},
	})
}

func (p *Provider) search(w http.ResponseWriter, r *http.Request) {
	if p.config.SearchKey == "" {
		http.NotFound(w, r)
		return
	}
	if !p.record(r.URL.Query().Get("key"), p.config.SearchKey) {
		unauthorized(w, "the key query parameter is missing or invalid")
		return
	}
	query := r.URL.Query().Get("q")
	writeJSON(w, http.StatusOK, map[string]any{
		"query": query,
		"results": []map[string]any{
			{"title": "fake result for " + query, "url": "https://example.invalid/1"},
		},
	})
}

func (p *Provider) records(w http.ResponseWriter, r *http.Request) {
	if p.config.ServiceToken == "" {
		http.NotFound(w, r)
		return
	}
	var request struct {
		Auth struct {
			Token string `json:"token"`
		} `json:"auth"`
		Record map[string]any `json:"record"`
	}
	if err := decodeBody(r, &request); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]any{
			"error": map[string]any{"type": "invalid_request_error", "message": "request body is not JSON"},
		})
		return
	}
	if !p.record(request.Auth.Token, p.config.ServiceToken) {
		unauthorized(w, "auth.token is missing or invalid")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"stored": true, "record": request.Record})
}

func bearer(r *http.Request) string {
	value := r.Header.Get("Authorization")
	if after, ok := strings.CutPrefix(value, "Bearer "); ok {
		return after
	}
	return value
}

func decodeBody(r *http.Request, into any) error {
	body, err := io.ReadAll(io.LimitReader(r.Body, MaxBodyBytes))
	if err != nil {
		return err
	}
	return json.Unmarshal(body, into)
}
