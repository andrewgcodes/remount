// Package e2bfake is a bounded, deterministic stand-in for the E2B sandbox
// REST API.
//
// It exists so the E2B driver's contract, retries, pagination, ambiguous-create
// recovery, tenant filtering and cleanup can be proven without an E2B account,
// and so the shapes it serves live in one checked-in place. When E2B changes a
// field name, `contract/` is the single file that moves; the fake and the
// driver's contract test fail together rather than the fake quietly agreeing
// with a driver that has drifted away from the real service.
//
// The fake is deliberately not a simulator of E2B's behavior at large. It
// implements exactly the four interactions the driver performs, and it refuses
// anything else loudly, because a permissive fake teaches a driver habits the
// real API will not honor.
package e2bfake

import (
	_ "embed"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

//go:embed contract/list_item.json
var listItemContract []byte

//go:embed contract/create_response.json
var createResponseContract []byte

//go:embed contract/create_request.json
var createRequestContract []byte

// ListItemContract returns the canonical GET /v2/sandboxes element, captured
// from the real service. Tests use it to assert the driver reads the fields it
// names and tolerates the ones it marks ignorable.
func ListItemContract() map[string]any { return decodeContract(listItemContract) }

// CreateResponseContract returns the canonical POST /sandboxes body. It is
// deliberately much sparser than a list item: the real service returns no
// metadata, startedAt or state there.
func CreateResponseContract() map[string]any { return decodeContract(createResponseContract) }

// CreateRequestContract returns the canonical create request shape.
func CreateRequestContract() map[string]any { return decodeContract(createRequestContract) }

func decodeContract(raw []byte) map[string]any {
	var out map[string]any
	if err := json.Unmarshal(raw, &out); err != nil {
		panic("e2bfake: embedded contract is not valid JSON: " + err.Error())
	}
	return out
}

// Options tunes the provider behaviors that a driver has to survive. Every one
// of them is a real thing a remote API does, and each has burned somebody:
// a create whose response is lost, an inventory that lags a delete, a sandbox
// that vanishes on its own.
type Options struct {
	// APIKey, when set, is required in X-API-Key. A request without it is 401.
	APIKey string
	// PageSize bounds one inventory page. Zero means every result in one page.
	PageSize int
	// LoseCreateResponses makes the next N creates take effect server-side and
	// then fail the response, which is the ambiguous case a client cannot
	// distinguish from "nothing happened".
	LoseCreateResponses int
	// DeletionLagLists keeps a destroyed sandbox visible for that many
	// subsequent list calls, because deletion is not instantly consistent.
	DeletionLagLists int
	// MaxLifetime removes a sandbox once it is older than this, modelling a
	// provider that reclaims capacity underneath the pool.
	MaxLifetime time.Duration
	// FailNextLists returns 503 for that many inventory calls.
	FailNextLists int
	// FailListsAfter lets that many inventory calls succeed and fails every
	// one after them. It models an outage that begins mid-operation, which is
	// the only way to reach a driver's recovery path with the pre-check
	// already done.
	FailListsAfter int
	// failAfterArmed is set once FailListsAfter has been consumed.
	failAfterArmed bool
	// Now supplies the clock. Defaults to time.Now.
	Now func() time.Time
}

type entry struct {
	id        string
	metadata  map[string]string
	createdAt time.Time
	destroyed bool
	// envdToken is the per-sandbox access token the create response carries
	// and the envd file route requires.
	envdToken string
	// files records what the driver delivered through the envd file route.
	files map[string]string
	// lag counts down the list calls a destroyed sandbox stays visible for.
	lag int
}

// Service is a running fake E2B API.
type Service struct {
	server *httptest.Server
	opts   Options

	mu       sync.Mutex
	next     int
	entries  map[string]*entry
	calls    []string
	creates  []map[string]any
	listSeen int
}

// New starts a fake service. Close it when the test ends.
func New(opts Options) *Service {
	if opts.Now == nil {
		opts.Now = time.Now
	}
	s := &Service{opts: opts, entries: map[string]*entry{}}
	s.server = httptest.NewServer(http.HandlerFunc(s.route))
	return s
}

// URL is the endpoint to hand the driver as Config.Endpoint.
func (s *Service) URL() string { return s.server.URL }

// Close stops the service.
func (s *Service) Close() { s.server.Close() }

// Calls returns the ordered method+path of every request received.
func (s *Service) Calls() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]string(nil), s.calls...)
}

// CreateBodies returns every decoded create request body, so a test can assert
// what the driver actually sent rather than what it believes it sent.
func (s *Service) CreateBodies() []map[string]any {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]map[string]any(nil), s.creates...)
}

// Live returns the ids of sandboxes that exist and are not destroyed. It is
// the ground truth a cleanup assertion compares against, independent of what
// the inventory endpoint is currently willing to admit.
func (s *Service) Live() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]string, 0, len(s.entries))
	for id, e := range s.entries {
		if !e.destroyed {
			out = append(out, id)
		}
	}
	sort.Strings(out)
	return out
}

func (s *Service) route(w http.ResponseWriter, r *http.Request) {
	s.mu.Lock()
	s.calls = append(s.calls, r.Method+" "+r.URL.Path)
	s.mu.Unlock()

	// The envd file route is the sandbox's own agent, authenticated by the
	// per-sandbox access token rather than the account API key.
	if r.URL.Path != "/files" && s.opts.APIKey != "" && r.Header.Get("X-API-Key") != s.opts.APIKey {
		http.Error(w, `{"message":"unauthorized"}`, http.StatusUnauthorized)
		return
	}
	switch {
	case r.Method == http.MethodPost && r.URL.Path == "/sandboxes":
		s.create(w, r)
	case r.Method == http.MethodGet && r.URL.Path == "/v2/sandboxes":
		s.list(w, r)
	case r.Method == http.MethodDelete && strings.HasPrefix(r.URL.Path, "/sandboxes/"):
		s.destroy(w, r)
	case r.Method == http.MethodPost && r.URL.Path == "/files":
		s.writeFile(w, r)
	default:
		// A permissive fake would teach the driver habits the real API does not
		// honor, so an unexpected route is a loud failure rather than a 200.
		http.Error(w, fmt.Sprintf(`{"message":"fake e2b: unexpected %s %s"}`, r.Method, r.URL.Path), http.StatusNotImplemented)
	}
}

func (s *Service) create(w http.ResponseWriter, r *http.Request) {
	var body map[string]any
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20)).Decode(&body); err != nil {
		http.Error(w, `{"message":"invalid body"}`, http.StatusBadRequest)
		return
	}
	metadata := map[string]string{}
	if raw, ok := body["metadata"].(map[string]any); ok {
		for key, value := range raw {
			text, _ := value.(string)
			metadata[key] = text
		}
	}

	s.mu.Lock()
	s.creates = append(s.creates, body)
	s.next++
	id := fmt.Sprintf("sbx_%04d", s.next)
	created := &entry{id: id, metadata: metadata, createdAt: s.opts.Now(), envdToken: "envd-token-" + id}
	s.entries[id] = created
	lose := s.opts.LoseCreateResponses > 0
	if lose {
		s.opts.LoseCreateResponses--
	}
	s.mu.Unlock()

	if lose {
		// The sandbox exists. The client will never learn its id from this
		// response, which is exactly the ambiguity recovery has to resolve.
		http.Error(w, `{"message":"upstream timeout"}`, http.StatusGatewayTimeout)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	_ = json.NewEncoder(w).Encode(createResponseJSON(created))
}

func (s *Service) destroy(w http.ResponseWriter, r *http.Request) {
	id := strings.TrimPrefix(r.URL.Path, "/sandboxes/")
	id, err := url.PathUnescape(id)
	if err != nil {
		http.Error(w, `{"message":"invalid id"}`, http.StatusBadRequest)
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	e, ok := s.entries[id]
	if !ok || e.destroyed {
		// A missing sandbox is already destroyed; the driver treats 404 as
		// success so a retried delete stays idempotent.
		w.WriteHeader(http.StatusNotFound)
		return
	}
	e.destroyed = true
	e.lag = s.opts.DeletionLagLists
	w.WriteHeader(http.StatusNoContent)
}

func (s *Service) list(w http.ResponseWriter, r *http.Request) {
	s.mu.Lock()
	if s.opts.FailNextLists > 0 {
		s.opts.FailNextLists--
		s.mu.Unlock()
		http.Error(w, `{"message":"service unavailable"}`, http.StatusServiceUnavailable)
		return
	}
	if s.opts.FailListsAfter > 0 {
		s.opts.FailListsAfter--
		if s.opts.FailListsAfter == 0 {
			s.opts.failAfterArmed = true
		}
	} else if s.opts.failAfterArmed {
		s.mu.Unlock()
		http.Error(w, `{"message":"service unavailable"}`, http.StatusServiceUnavailable)
		return
	}
	s.listSeen++
	now := s.opts.Now()
	filters, err := url.ParseQuery(r.URL.Query().Get("metadata"))
	if err != nil {
		s.mu.Unlock()
		http.Error(w, `{"message":"invalid metadata filter"}`, http.StatusBadRequest)
		return
	}

	ids := make([]string, 0, len(s.entries))
	for id := range s.entries {
		ids = append(ids, id)
	}
	sort.Strings(ids)

	visible := make([]*entry, 0, len(ids))
	for _, id := range ids {
		e := s.entries[id]
		if s.opts.MaxLifetime > 0 && !e.destroyed && now.Sub(e.createdAt) >= s.opts.MaxLifetime {
			// The provider reclaimed it. It is simply gone from inventory.
			e.destroyed, e.lag = true, 0
		}
		if e.destroyed {
			if e.lag <= 0 {
				continue
			}
			e.lag--
		}
		if !matches(e.metadata, filters) {
			continue
		}
		visible = append(visible, e)
	}
	pageSize := s.opts.PageSize
	if pageSize <= 0 || pageSize > len(visible) {
		pageSize = len(visible)
	}
	start := 0
	if token := r.URL.Query().Get("nextToken"); token != "" {
		parsed, convErr := strconv.Atoi(token)
		if convErr != nil || parsed < 0 || parsed > len(visible) {
			s.mu.Unlock()
			http.Error(w, `{"message":"invalid nextToken"}`, http.StatusBadRequest)
			return
		}
		start = parsed
	}
	end := start + pageSize
	if pageSize == 0 || end > len(visible) {
		end = len(visible)
	}
	page := make([]map[string]any, 0, end-start)
	for _, e := range visible[start:end] {
		page = append(page, listItemJSON(e))
	}
	more := end < len(visible)
	s.mu.Unlock()

	w.Header().Set("Content-Type", "application/json")
	if more {
		w.Header().Set("X-Next-Token", strconv.Itoa(end))
	}
	_ = json.NewEncoder(w).Encode(page)
}

// matches applies the metadata filter the driver encodes into the query. Every
// requested key must be present and equal, which is what lets a tenant filter
// be a real boundary rather than a hint.
func matches(metadata map[string]string, filters url.Values) bool {
	for key, values := range filters {
		if len(values) == 0 {
			continue
		}
		if metadata[key] != values[0] {
			return false
		}
	}
	return true
}

// createResponseJSON renders exactly what the real POST /sandboxes returns.
// It carries no metadata, startedAt or state, because the real service does
// not, and a fake that volunteered them would let the driver depend on fields
// production never sends.
func createResponseJSON(e *entry) map[string]any {
	out := map[string]any{}
	for key, value := range CreateResponseContract() {
		if strings.HasPrefix(key, "_") {
			continue
		}
		out[key] = value
	}
	out["sandboxID"] = e.id
	out["envdAccessToken"] = e.envdToken
	return out
}

// writeFile is the envd POST /files route as the sandbox's agent serves it:
// multipart body, path and username in the query, X-Access-Token required
// because the driver creates secure sandboxes. Deliveries are recorded so a
// test can assert the bootstrap reached the sandbox, and only the sandbox
// whose token was presented.
func (s *Service) writeFile(w http.ResponseWriter, r *http.Request) {
	token := r.Header.Get("X-Access-Token")
	s.mu.Lock()
	var target *entry
	for _, e := range s.entries {
		if token != "" && e.envdToken == token && !e.destroyed {
			target = e
		}
	}
	s.mu.Unlock()
	if target == nil {
		http.Error(w, `{"code":401,"message":"unauthorized access, please provide a valid access token"}`, http.StatusUnauthorized)
		return
	}
	if err := r.ParseMultipartForm(1 << 20); err != nil {
		http.Error(w, `{"message":"invalid multipart body"}`, http.StatusBadRequest)
		return
	}
	file, _, err := r.FormFile("file")
	if err != nil {
		http.Error(w, `{"message":"missing file part"}`, http.StatusBadRequest)
		return
	}
	defer file.Close()
	content, err := io.ReadAll(io.LimitReader(file, 1<<20))
	if err != nil {
		http.Error(w, `{"message":"read failed"}`, http.StatusBadRequest)
		return
	}
	filePath := r.URL.Query().Get("path")
	if filePath == "" || !strings.HasPrefix(filePath, "/") {
		http.Error(w, `{"message":"path must be absolute"}`, http.StatusBadRequest)
		return
	}
	s.mu.Lock()
	if target.files == nil {
		target.files = map[string]string{}
	}
	target.files[filePath] = string(content)
	s.mu.Unlock()
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode([]map[string]any{{"path": filePath, "name": filePath[strings.LastIndex(filePath, "/")+1:], "type": "file"}})
}

// Files returns what was written into a sandbox through the envd route, keyed
// by path. A missing sandbox or one with no deliveries yields an empty map.
func (s *Service) Files(id string) map[string]string {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := map[string]string{}
	if e, ok := s.entries[id]; ok {
		for k, v := range e.files {
			out[k] = v
		}
	}
	return out
}

// listItemJSON renders one entry in the list contract's shape, including the
// fields the contract marks ignorable, so a driver that accidentally depends on
// one of them is caught here rather than in production.
func listItemJSON(e *entry) map[string]any {
	metadata := make(map[string]any, len(e.metadata))
	for key, value := range e.metadata {
		metadata[key] = value
	}
	out := map[string]any{
		"sandboxID": e.id,
		"startedAt": e.createdAt.UTC().Format(time.RFC3339Nano),
		"state":     "running",
		"metadata":  metadata,
	}
	if ignorable, ok := ListItemContract()["_ignorable"].(map[string]any); ok {
		for key, value := range ignorable {
			if strings.HasPrefix(key, "_") {
				continue
			}
			out[key] = value
		}
	}
	return out
}
