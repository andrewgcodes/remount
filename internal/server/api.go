package server

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/coder/websocket"

	"remount.dev/remount/internal/client"
	"remount.dev/remount/internal/metrics"
	"remount.dev/remount/internal/proto"
)

// The agent HTTP API (docs/api.md). Every handler here is a thin translation
// of one SDK call: the request's bearer credential becomes an in-process
// client (clientPool), the body becomes the protocol request, the protocol
// error becomes a status. Nothing is authorized here; the control plane and
// the node do that exactly as they would for the CLI.

const (
	// apiBodyLimit bounds a JSON request body. The largest legitimate body
	// is an agent message (MaxAgentMessageBytes) or a recipe.
	apiBodyLimit = 1 << 20
	// apiFSWriteLimit bounds a PUT to the filesystem route.
	apiFSWriteLimit = 64 << 20
	// sessionCookie carries a bearer credential for browser-driven surfaces
	// (preview proxy, WebSockets) that cannot set an Authorization header.
	sessionCookie = "remount_session"
	// wsBearerProtocol is the WebSocket subprotocol prefix that carries a
	// base64url bearer credential, for browsers that cannot set headers on
	// a WebSocket.
	wsBearerProtocol = "remount.bearer."
	// idempotencyHeader is the header a client sets to make a retried
	// mutation return the original result.
	idempotencyHeader = "Idempotency-Key"
)

type apiError struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

type apiErrorBody struct {
	Error apiError `json:"error"`
}

// httpStatus maps a protocol error to a status. Unknown errors are 500 and
// their text is not forwarded.
func httpStatus(err error) (int, apiError) {
	var pe *proto.Error
	if !errors.As(err, &pe) {
		if errors.Is(err, context.DeadlineExceeded) {
			return http.StatusGatewayTimeout, apiError{Code: proto.CodeTimeout, Message: "request timed out"}
		}
		return http.StatusInternalServerError, apiError{Code: proto.CodeInternal, Message: "internal error"}
	}
	status := http.StatusInternalServerError
	switch pe.Code {
	case proto.CodeBadRequest:
		status = http.StatusBadRequest
	case proto.CodeNotFound:
		status = http.StatusNotFound
	case proto.CodeUnsupported:
		status = http.StatusNotImplemented
	case proto.CodeUnauthorized:
		status = http.StatusUnauthorized
	case proto.CodeDenied:
		status = http.StatusForbidden
	case proto.CodeConflict:
		status = http.StatusConflict
	case proto.CodeEvicted:
		status = http.StatusGone
	case proto.CodeUnreachable, proto.CodeClosed:
		status = http.StatusServiceUnavailable
	case proto.CodeTimeout:
		status = http.StatusGatewayTimeout
	case proto.CodeResourceExhausted:
		status = http.StatusTooManyRequests
	}
	return status, apiError{Code: pe.Code, Message: pe.Msg}
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	enc := json.NewEncoder(w)
	_ = enc.Encode(v)
}

func writeError(w http.ResponseWriter, err error) {
	status, body := httpStatus(err)
	if status == http.StatusUnauthorized {
		w.Header().Set("WWW-Authenticate", `Bearer realm="remount"`)
	}
	writeJSON(w, status, apiErrorBody{Error: body})
}

func badRequest(w http.ResponseWriter, format string, args ...any) {
	writeError(w, proto.Err(proto.CodeBadRequest, format, args...))
}

// credential extracts the caller's bearer credential. The Authorization
// header wins; a WebSocket may carry it as a subprotocol; a browser session
// cookie is accepted only where a browser navigation must be the caller: the
// preview proxy and the stable /a/{id} link. Everything else takes the header
// or subprotocol, because content served through the preview proxy is
// same-origin with this API and a cookie honoured on the terminal, file or
// diff routes would let one workspace's page act as the operator on every
// agent the operator can reach.
func credential(r *http.Request, allowCookie bool) (tok string, fromCookie, ok bool) {
	if h := r.Header.Get("Authorization"); h != "" {
		if tok, ok := strings.CutPrefix(h, "Bearer "); ok && tok != "" {
			return tok, false, true
		}
		return "", false, false
	}
	for _, p := range wsSubprotocols(r) {
		if enc, ok := strings.CutPrefix(p, wsBearerProtocol); ok {
			if raw, err := base64.RawURLEncoding.DecodeString(enc); err == nil && len(raw) > 0 {
				return string(raw), false, true
			}
			return "", false, false
		}
	}
	if allowCookie {
		if c, err := r.Cookie(sessionCookie); err == nil && c.Value != "" {
			return c.Value, true, true
		}
	}
	return "", false, false
}

// originTrusted reports whether a cookie-authenticated request came from
// this API's own origin or one the operator listed for CORS. SameSite=Strict
// already keeps the cookie off cross-site requests; this refuses the ones a
// browser sends anyway (WebSocket handshakes from an older browser, a
// misconfigured proxy) before they reach a workspace.
func (s *Server) originTrusted(r *http.Request) bool {
	origin := r.Header.Get("Origin")
	if origin == "" {
		return true
	}
	u, err := url.Parse(origin)
	if err != nil || u.Host == "" {
		return false
	}
	if strings.EqualFold(u.Host, r.Host) {
		return true
	}
	for _, o := range s.opts.CORSOrigins {
		if o != "*" && strings.EqualFold(o, origin) {
			return true
		}
	}
	return false
}

// plainID reports whether id is one URL-safe token.
func plainID(id string) bool {
	if id == "" || len(id) > 128 {
		return false
	}
	for _, c := range id {
		switch {
		case c >= 'a' && c <= 'z', c >= 'A' && c <= 'Z', c >= '0' && c <= '9', c == '_', c == '-':
		default:
			return false
		}
	}
	return true
}

// wsSubprotocols lists the Sec-WebSocket-Protocol tokens of a request.
func wsSubprotocols(r *http.Request) []string {
	var out []string
	for _, v := range r.Header.Values("Sec-WebSocket-Protocol") {
		for _, p := range strings.Split(v, ",") {
			if p = strings.TrimSpace(p); p != "" {
				out = append(out, p)
			}
		}
	}
	return out
}

func isUpgrade(r *http.Request) bool {
	return strings.EqualFold(r.Header.Get("Upgrade"), "websocket")
}

// apiClient authenticates the request and returns its SDK client. When it
// returns nil it has already written the response.
func (s *Server) apiClient(w http.ResponseWriter, r *http.Request, allowCookie bool) (*client.Client, func()) {
	tok, fromCookie, ok := credential(r, allowCookie)
	if !ok && (s.opts.Token != "" || s.opts.Authenticator != nil) {
		writeError(w, proto.Err(proto.CodeUnauthorized, "missing credential"))
		return nil, nil
	}
	if fromCookie && !s.originTrusted(r) {
		writeError(w, proto.Err(proto.CodeDenied, "cross-site request with a session cookie"))
		return nil, nil
	}
	cl, release, err := s.clients.acquire(r.Context(), tok)
	if err != nil {
		writeError(w, err)
		return nil, nil
	}
	metrics.HTTPRequests.Inc()
	return cl, release
}

func decodeBody(w http.ResponseWriter, r *http.Request, v any) bool {
	if r.Body == nil || r.ContentLength == 0 {
		return true
	}
	dec := json.NewDecoder(io.LimitReader(r.Body, apiBodyLimit))
	dec.DisallowUnknownFields()
	if err := dec.Decode(v); err != nil && !errors.Is(err, io.EOF) {
		badRequest(w, "invalid JSON body: %v", err)
		return false
	}
	return true
}

// idem returns the caller's idempotency key. Without one every request is a
// new mutation, as it is for the SDK.
func idem(r *http.Request) []client.OperationOption {
	if k := r.Header.Get(idempotencyHeader); k != "" {
		return []client.OperationOption{client.WithIdempotencyKey(k)}
	}
	return nil
}

// apiRoutes registers the agent API. Patterns use method and wildcard
// routing; a more specific literal segment wins over a wildcard, so
// /messages is not swallowed by /{action}.
func (s *Server) apiRoutes(mux *http.ServeMux) {
	mux.HandleFunc("POST /v1/session", s.handleSessionCreate)
	mux.HandleFunc("DELETE /v1/session", s.handleSessionDelete)
	mux.HandleFunc("GET /v1/usage", s.handleUsage)
	mux.HandleFunc("POST /v1/agents", s.handleAgentCreate)
	mux.HandleFunc("GET /v1/agents", s.handleAgentList)
	mux.HandleFunc("GET /v1/agents/{id}", s.handleAgentGet)
	mux.HandleFunc("POST /v1/agents/{id}/messages", s.handleAgentMessage)
	mux.HandleFunc("POST /v1/agents/{id}/fork", s.handleAgentFork)
	mux.HandleFunc("POST /v1/agents/{id}/{action}", s.handleAgentAction)
	mux.HandleFunc("GET /v1/agents/{id}/transcript", s.handleTranscript)
	mux.HandleFunc("GET /v1/agents/{id}/approvals", s.handleAgentApprovals)
	mux.HandleFunc("GET /v1/approvals/{id}", s.handleApprovalGet)
	mux.HandleFunc("POST /v1/approvals/{id}", s.handleApprovalDecide)
	mux.HandleFunc("GET /v1/agents/{id}/diff", s.handleDiff)
	mux.HandleFunc("GET /v1/agents/{id}/terminal", s.handleTerminal)
	mux.HandleFunc("/v1/agents/{id}/fs/{path...}", s.handleFS)
	mux.HandleFunc("/v1/agents/{id}/ports/{port}", s.handlePortRoot)
	mux.HandleFunc("/v1/agents/{id}/ports/{port}/{rest...}", s.handlePort)
	mux.HandleFunc("GET /a/{id}", s.handleAgentLink)
}

func (s *Server) handleUsage(w http.ResponseWriter, r *http.Request) {
	cl, release := s.apiClient(w, r, false)
	if cl == nil {
		return
	}
	defer release()
	query := r.URL.Query()
	usage, err := cl.Usage(r.Context(), proto.UsageReq{
		Tenant: query.Get("tenant"), WS: query.Get("ws"), Principal: query.Get("principal"),
		Binding: query.Get("binding"), Window: query.Get("window"),
	})
	if err != nil {
		writeError(w, err)
		return
	}
	if usage == nil {
		usage = []proto.Usage{}
	}
	writeJSON(w, http.StatusOK, proto.UsageRes{Usage: usage})
}

// cors wraps the API with the operator's origin allow list. A UI hosted on
// another origin needs it; a same-origin UI or a non-browser client never
// triggers it. Credentials are allowed only for a named origin, never "*".
func (s *Server) cors(next http.Handler) http.Handler {
	if len(s.opts.CORSOrigins) == 0 {
		return next
	}
	allowAll := false
	allowed := map[string]bool{}
	for _, o := range s.opts.CORSOrigins {
		if o == "*" {
			allowAll = true
		}
		allowed[strings.ToLower(o)] = true
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		origin := r.Header.Get("Origin")
		if origin != "" && (allowAll || allowed[strings.ToLower(origin)]) {
			h := w.Header()
			if allowAll {
				h.Set("Access-Control-Allow-Origin", "*")
			} else {
				h.Set("Access-Control-Allow-Origin", origin)
				h.Set("Access-Control-Allow-Credentials", "true")
				h.Add("Vary", "Origin")
			}
			h.Set("Access-Control-Expose-Headers", "Location, Idempotency-Key")
			if r.Method == http.MethodOptions && r.Header.Get("Access-Control-Request-Method") != "" {
				h.Set("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, OPTIONS")
				h.Set("Access-Control-Allow-Headers", "Authorization, Content-Type, Idempotency-Key, Last-Event-ID")
				h.Set("Access-Control-Max-Age", "600")
				w.WriteHeader(http.StatusNoContent)
				return
			}
		}
		next.ServeHTTP(w, r)
	})
}

// ---------------------------------------------------------------------------
// sessions (browser cookies)
// ---------------------------------------------------------------------------

// handleSessionCreate exchanges a header credential for a cookie so a
// browser can load a preview and its subresources. The cookie holds the same
// credential the header did; it is HttpOnly and SameSite=Strict so no other
// site can ride on it, and Secure whenever the request arrived over TLS.
func (s *Server) handleSessionCreate(w http.ResponseWriter, r *http.Request) {
	tok, _, ok := credential(r, false)
	if !ok {
		writeError(w, proto.Err(proto.CodeUnauthorized, "missing credential"))
		return
	}
	// The credential must authenticate before it is minted into a cookie.
	_, release, err := s.clients.acquire(r.Context(), tok)
	if err != nil {
		writeError(w, err)
		return
	}
	release()
	http.SetCookie(w, &http.Cookie{
		Name: sessionCookie, Value: tok, Path: "/", HttpOnly: true, SameSite: http.SameSiteStrictMode,
		Secure: r.TLS != nil || strings.EqualFold(r.Header.Get("X-Forwarded-Proto"), "https"),
		MaxAge: int((12 * time.Hour).Seconds()),
	})
	w.WriteHeader(http.StatusNoContent)
}

func (s *Server) handleSessionDelete(w http.ResponseWriter, _ *http.Request) {
	http.SetCookie(w, &http.Cookie{Name: sessionCookie, Value: "", Path: "/", HttpOnly: true, SameSite: http.SameSiteStrictMode, MaxAge: -1})
	w.WriteHeader(http.StatusNoContent)
}

// ---------------------------------------------------------------------------
// agents
// ---------------------------------------------------------------------------

func (s *Server) handleAgentCreate(w http.ResponseWriter, r *http.Request) {
	cl, release := s.apiClient(w, r, false)
	if cl == nil {
		return
	}
	defer release()
	var req proto.AgentCreateReq
	if !decodeBody(w, r, &req) {
		return
	}
	a, err := cl.CreateAgent(r.Context(), req, idem(r)...)
	if err != nil {
		writeError(w, err)
		return
	}
	w.Header().Set("Location", "/v1/agents/"+a.ID)
	writeJSON(w, http.StatusCreated, a)
}

func (s *Server) handleAgentList(w http.ResponseWriter, r *http.Request) {
	cl, release := s.apiClient(w, r, false)
	if cl == nil {
		return
	}
	defer release()
	q := r.URL.Query()
	agents, err := cl.ListAgents(r.Context(), proto.AgentListReq{Status: q.Get("status"), WS: q.Get("ws"), Parent: q.Get("parent")})
	if err != nil {
		writeError(w, err)
		return
	}
	if agents == nil {
		agents = []proto.Agent{}
	}
	writeJSON(w, http.StatusOK, proto.AgentListRes{Agents: agents})
}

func (s *Server) handleAgentGet(w http.ResponseWriter, r *http.Request) {
	cl, release := s.apiClient(w, r, false)
	if cl == nil {
		return
	}
	defer release()
	a, err := cl.GetAgent(r.Context(), r.PathValue("id"))
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, a)
}

func (s *Server) handleAgentMessage(w http.ResponseWriter, r *http.Request) {
	cl, release := s.apiClient(w, r, false)
	if cl == nil {
		return
	}
	defer release()
	var req proto.AgentMessageReq
	if !decodeBody(w, r, &req) {
		return
	}
	req.ID = r.PathValue("id")
	res, err := cl.MessageAgent(r.Context(), req, idem(r)...)
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusAccepted, res)
}

func (s *Server) handleAgentFork(w http.ResponseWriter, r *http.Request) {
	cl, release := s.apiClient(w, r, false)
	if cl == nil {
		return
	}
	defer release()
	var req proto.AgentForkReq
	if !decodeBody(w, r, &req) {
		return
	}
	req.ID = r.PathValue("id")
	a, err := cl.ForkAgent(r.Context(), req, idem(r)...)
	if err != nil {
		writeError(w, err)
		return
	}
	w.Header().Set("Location", "/v1/agents/"+a.ID)
	writeJSON(w, http.StatusCreated, a)
}

// handleAgentAction covers the bodiless lifecycle verbs.
func (s *Server) handleAgentAction(w http.ResponseWriter, r *http.Request) {
	cl, release := s.apiClient(w, r, false)
	if cl == nil {
		return
	}
	defer release()
	id := r.PathValue("id")
	var (
		a   *proto.Agent
		err error
	)
	switch r.PathValue("action") {
	case "cancel":
		a, err = cl.CancelAgent(r.Context(), id, idem(r)...)
	case "sleep":
		a, err = cl.SleepAgent(r.Context(), id, idem(r)...)
	case "wake":
		var req proto.AgentWakeReq
		if !decodeBody(w, r, &req) {
			return
		}
		if req.By == "" {
			req.By = r.URL.Query().Get("by")
		}
		a, err = cl.WakeAgent(r.Context(), id, req.By, idem(r)...)
	case "destroy":
		if err = cl.DestroyAgent(r.Context(), id, idem(r)...); err == nil {
			w.WriteHeader(http.StatusNoContent)
			return
		}
	default:
		writeError(w, proto.Err(proto.CodeNotFound, "no such agent action %q", r.PathValue("action")))
		return
	}
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, a)
}

// handleAgentLink is the stable per-agent URL. It is an API resource, not a
// page: with an operator UI configured it redirects there, otherwise it is
// the agent itself. There is no capability in the URL; the caller
// authenticates like everywhere else, and the JSON fallback takes the header
// only: the record holds the task and inbox text, which a preview page
// carrying the cookie must not read.
func (s *Server) handleAgentLink(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if !plainID(id) {
		writeError(w, proto.Err(proto.CodeNotFound, "agent %q", id))
		return
	}
	if base := s.opts.AgentURLBase; base != "" {
		// The redirect is unauthenticated: the UI it lands on authenticates.
		// The id is confined to one URL-safe token so it cannot rewrite the
		// target beyond its own slot.
		target := strings.ReplaceAll(base, "{id}", id)
		if target == base {
			target = strings.TrimRight(base, "/") + "/" + id
		}
		http.Redirect(w, r, target, http.StatusFound)
		return
	}
	cl, release := s.apiClient(w, r, false)
	if cl == nil {
		return
	}
	defer release()
	a, err := cl.GetAgent(r.Context(), id)
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, a)
}

// ---------------------------------------------------------------------------
// approvals
// ---------------------------------------------------------------------------

func (s *Server) handleAgentApprovals(w http.ResponseWriter, r *http.Request) {
	cl, release := s.apiClient(w, r, false)
	if cl == nil {
		return
	}
	defer release()
	aps, err := cl.ListApprovals(r.Context(), proto.ApprovalListReq{Agent: r.PathValue("id"), Status: r.URL.Query().Get("status")})
	if err != nil {
		writeError(w, err)
		return
	}
	if aps == nil {
		aps = []proto.Approval{}
	}
	writeJSON(w, http.StatusOK, proto.ApprovalListRes{Approvals: aps})
}

func (s *Server) handleApprovalGet(w http.ResponseWriter, r *http.Request) {
	cl, release := s.apiClient(w, r, false)
	if cl == nil {
		return
	}
	defer release()
	ap, err := cl.GetApproval(r.Context(), r.PathValue("id"))
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, ap)
}

func (s *Server) handleApprovalDecide(w http.ResponseWriter, r *http.Request) {
	cl, release := s.apiClient(w, r, false)
	if cl == nil {
		return
	}
	defer release()
	var req proto.ApprovalDecideReq
	if !decodeBody(w, r, &req) {
		return
	}
	req.ID = r.PathValue("id")
	ap, err := cl.DecideApproval(r.Context(), req, idem(r)...)
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, ap)
}

// ---------------------------------------------------------------------------
// transcript
// ---------------------------------------------------------------------------

// transcriptItem is a transcript record as the HTTP API renders it: the
// stream by name, and the ACP frame decoded so a UI reads JSON, not CBOR in
// base64. Data is kept for streams the API does not interpret.
type transcriptItem struct {
	Index  uint64                `json:"index"`
	Run    string                `json:"run"`
	Seq    uint64                `json:"seq"`
	Stream string                `json:"stream"`
	At     int64                 `json:"at"`
	Frame  *proto.ACPFrameRecord `json:"frame,omitempty"`
	Text   string                `json:"text,omitempty"`
	Data   []byte                `json:"data,omitempty"`
}

type transcriptPage struct {
	Records []transcriptItem     `json:"records"`
	Next    uint64               `json:"next"`
	Gap     *proto.TranscriptGap `json:"gap,omitempty"`
	Done    bool                 `json:"done,omitempty"`
}

func streamName(stream uint8) string {
	switch stream {
	case proto.StreamStdout:
		return "stdout"
	case proto.StreamStderr:
		return "stderr"
	case proto.StreamACPIn:
		return "acp_in"
	case proto.StreamACPOut:
		return "acp_out"
	case proto.StreamExit:
		return "exit"
	case proto.StreamInfo:
		return "info"
	case proto.StreamGap:
		return "gap"
	}
	return "stream_" + strconv.Itoa(int(stream))
}

func renderTranscript(rec proto.TranscriptRecord) transcriptItem {
	item := transcriptItem{Index: rec.Index, Run: rec.Run, Seq: rec.Seq, Stream: streamName(rec.Stream), At: rec.At}
	switch rec.Stream {
	case proto.StreamACPIn, proto.StreamACPOut:
		var fr proto.ACPFrameRecord
		if err := proto.Unmarshal(rec.Data, &fr); err == nil {
			item.Frame = &fr
			return item
		}
		item.Data = rec.Data
	case proto.StreamStdout, proto.StreamStderr:
		item.Text = string(rec.Data)
	default:
		item.Data = rec.Data
	}
	return item
}

func renderPage(res *proto.AgentTranscriptRes) transcriptPage {
	page := transcriptPage{Records: make([]transcriptItem, 0, len(res.Records)), Next: res.Next, Gap: res.Gap, Done: res.Done}
	for _, rec := range res.Records {
		page.Records = append(page.Records, renderTranscript(rec))
	}
	return page
}

func parseCursor(r *http.Request) (uint64, int, error) {
	q := r.URL.Query()
	var from uint64
	// A reconnecting EventSource resends the original URL (with its ?from)
	// plus Last-Event-ID for the last record it saw; the header is the
	// newer cursor, so it wins, otherwise every reconnect replays from the
	// first cursor.
	if v := r.Header.Get("Last-Event-ID"); v != "" {
		n, err := strconv.ParseUint(v, 10, 64)
		if err != nil {
			return 0, 0, proto.Err(proto.CodeBadRequest, "Last-Event-ID must be a non-negative integer")
		}
		from = n
	} else if v := q.Get("from"); v != "" {
		n, err := strconv.ParseUint(v, 10, 64)
		if err != nil {
			return 0, 0, proto.Err(proto.CodeBadRequest, "from must be a non-negative integer")
		}
		from = n
	}
	limit := 0
	if v := q.Get("limit"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n < 0 {
			return 0, 0, proto.Err(proto.CodeBadRequest, "limit must be a non-negative integer")
		}
		limit = n
	}
	return from, limit, nil
}

// handleTranscript serves the durable mirror three ways: a JSON page, a
// Server-Sent Events stream, or a WebSocket of JSON lines. All three are
// cursor-resumable (from, or Last-Event-ID for SSE reconnects) and none of
// them touches the workspace, so a sleeping agent's transcript reads
// without waking it.
func (s *Server) handleTranscript(w http.ResponseWriter, r *http.Request) {
	cl, release := s.apiClient(w, r, false)
	if cl == nil {
		return
	}
	defer release()
	id := r.PathValue("id")
	from, limit, err := parseCursor(r)
	if err != nil {
		writeError(w, err)
		return
	}
	switch {
	case isUpgrade(r):
		s.transcriptWS(w, r, cl, id, from)
	case strings.Contains(r.Header.Get("Accept"), "text/event-stream"):
		s.transcriptSSE(w, r, cl, id, from)
	default:
		res, err := cl.Transcript(r.Context(), id, from, limit)
		if err != nil {
			writeError(w, err)
			return
		}
		writeJSON(w, http.StatusOK, renderPage(res))
	}
}

// transcriptFollow drives a stream: it pages from the cursor, emits each
// page through emit, and when it catches up waits on the control plane's
// waiter for more, ending when the agent is terminal and fully read.
func (s *Server) transcriptFollow(ctx context.Context, cl *client.Client, id string, from uint64, emit func(transcriptPage) error, heartbeat func() error) error {
	cursor := from
	for {
		res, err := cl.Transcript(ctx, id, cursor, 0)
		if err != nil {
			return err
		}
		if len(res.Records) > 0 || res.Gap != nil || res.Done {
			if err := emit(renderPage(res)); err != nil {
				return err
			}
		}
		if res.Done {
			return nil
		}
		if res.Next > cursor {
			cursor = res.Next
			continue
		}
		wait := s.Control.TranscriptWait(id, cursor)
		select {
		case <-wait:
		case <-time.After(15 * time.Second):
			if heartbeat != nil {
				if err := heartbeat(); err != nil {
					return err
				}
			}
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

func (s *Server) transcriptSSE(w http.ResponseWriter, r *http.Request, cl *client.Client, id string, from uint64) {
	// Authorize and page once before committing to the stream so an error
	// is a status, not a broken event stream.
	first, err := cl.Transcript(r.Context(), id, from, 0)
	if err != nil {
		writeError(w, err)
		return
	}
	metrics.HTTPTranscriptStreams.Inc()
	h := w.Header()
	h.Set("Content-Type", "text/event-stream")
	h.Set("Cache-Control", "no-store")
	h.Set("X-Accel-Buffering", "no")
	w.WriteHeader(http.StatusOK)
	rc := http.NewResponseController(w)
	write := func(event string, id uint64, v any) error {
		b, err := json.Marshal(v)
		if err != nil {
			return err
		}
		if _, err := io.WriteString(w, "event: "+event+"\nid: "+strconv.FormatUint(id, 10)+"\ndata: "+string(b)+"\n\n"); err != nil {
			return err
		}
		return rc.Flush()
	}
	emit := func(page transcriptPage) error {
		if page.Gap != nil {
			if err := write("gap", page.Gap.To, page.Gap); err != nil {
				return err
			}
		}
		for _, rec := range page.Records {
			if err := write("record", rec.Index+1, rec); err != nil {
				return err
			}
		}
		if page.Done {
			return write("done", page.Next, map[string]uint64{"next": page.Next})
		}
		return nil
	}
	heartbeat := func() error {
		if _, err := io.WriteString(w, ": keepalive\n\n"); err != nil {
			return err
		}
		return rc.Flush()
	}
	if len(first.Records) > 0 || first.Gap != nil || first.Done {
		if emit(renderPage(first)) != nil {
			return
		}
	}
	if first.Done {
		return
	}
	cursor := from
	if first.Next > cursor {
		cursor = first.Next
	}
	_ = s.transcriptFollow(r.Context(), cl, id, cursor, emit, heartbeat)
}

// acceptWS upgrades with the bearer subprotocol echoed when the client used
// one, which the WebSocket handshake requires.
func acceptWS(w http.ResponseWriter, r *http.Request) (*websocket.Conn, error) {
	var protocols []string
	for _, p := range wsSubprotocols(r) {
		if strings.HasPrefix(p, wsBearerProtocol) {
			protocols = append(protocols, p)
		}
	}
	return websocket.Accept(w, r, &websocket.AcceptOptions{
		Subprotocols:       protocols,
		CompressionMode:    websocket.CompressionDisabled,
		InsecureSkipVerify: true, // the credential, not the Origin, is the authority
	})
}

func (s *Server) transcriptWS(w http.ResponseWriter, r *http.Request, cl *client.Client, id string, from uint64) {
	first, err := cl.Transcript(r.Context(), id, from, 0)
	if err != nil {
		writeError(w, err)
		return
	}
	c, err := acceptWS(w, r)
	if err != nil {
		return
	}
	metrics.HTTPTranscriptStreams.Inc()
	ctx, cancel := context.WithCancel(r.Context())
	defer cancel()
	// Reading drains control frames and notices the peer leaving.
	go func() {
		defer cancel()
		for {
			if _, _, err := c.Read(ctx); err != nil {
				return
			}
		}
	}()
	emit := func(page transcriptPage) error {
		if page.Gap != nil {
			if err := writeWSJSON(ctx, c, map[string]any{"type": "gap", "gap": page.Gap}); err != nil {
				return err
			}
		}
		for _, rec := range page.Records {
			if err := writeWSJSON(ctx, c, map[string]any{"type": "record", "record": rec}); err != nil {
				return err
			}
		}
		if page.Done {
			return writeWSJSON(ctx, c, map[string]any{"type": "done", "next": page.Next})
		}
		return nil
	}
	heartbeat := func() error { return c.Ping(ctx) }
	cursor := from
	if len(first.Records) > 0 || first.Gap != nil || first.Done {
		if emit(renderPage(first)) != nil {
			_ = c.CloseNow()
			return
		}
	}
	if first.Next > cursor {
		cursor = first.Next
	}
	if !first.Done {
		if err := s.transcriptFollow(ctx, cl, id, cursor, emit, heartbeat); err != nil && ctx.Err() == nil {
			_ = c.Close(websocket.StatusInternalError, truncateReason(err.Error()))
			return
		}
	}
	_ = c.Close(websocket.StatusNormalClosure, "done")
}

func writeWSJSON(ctx context.Context, c *websocket.Conn, v any) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	wctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	return c.Write(wctx, websocket.MessageText, b)
}

// truncateReason fits a close reason into the 123 bytes the protocol allows.
func truncateReason(s string) string {
	if len(s) > 120 {
		return s[:120]
	}
	return s
}

// ---------------------------------------------------------------------------
// diff
// ---------------------------------------------------------------------------

func (s *Server) handleDiff(w http.ResponseWriter, r *http.Request) {
	cl, release := s.apiClient(w, r, false)
	if cl == nil {
		return
	}
	defer release()
	res, err := cl.Diff(r.Context(), r.PathValue("id"), r.URL.Query().Get("wake") == "true")
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, res)
}

// ---------------------------------------------------------------------------
// filesystem
// ---------------------------------------------------------------------------

// handleFS exposes the workspace filesystem under the agent. GET reads a
// file, lists a directory (trailing slash or ?list=1) or stats (?stat=1);
// PUT writes the body (mode=0644, mkdirp implied); DELETE removes
// (?recursive=1). Paths are workspace-relative and jailed by the node.
func (s *Server) handleFS(w http.ResponseWriter, r *http.Request) {
	cl, release := s.apiClient(w, r, false)
	if cl == nil {
		return
	}
	defer release()
	id := r.PathValue("id")
	path := r.PathValue("path")
	if path == "" {
		path = "."
	}
	a, err := cl.AgentMaterialized(r.Context(), id, false, "")
	if err != nil {
		writeError(w, err)
		return
	}
	q := r.URL.Query()
	switch r.Method {
	case http.MethodGet, http.MethodHead:
		if q.Get("stat") == "1" {
			entry, err := cl.Stat(r.Context(), a.WS, path)
			if err != nil {
				writeError(w, err)
				return
			}
			writeJSON(w, http.StatusOK, entry)
			return
		}
		list := q.Get("list") == "1" || strings.HasSuffix(r.URL.Path, "/") || path == "."
		if !list {
			entry, err := cl.Stat(r.Context(), a.WS, path)
			if err != nil {
				writeError(w, err)
				return
			}
			list = entry.IsDir
		}
		if list {
			entries, err := cl.ListDir(r.Context(), a.WS, path)
			if err != nil {
				writeError(w, err)
				return
			}
			if entries == nil {
				entries = []proto.FSEntry{}
			}
			writeJSON(w, http.StatusOK, proto.FSListRes{Entries: entries})
			return
		}
		data, err := cl.ReadFile(r.Context(), a.WS, path)
		if err != nil {
			writeError(w, err)
			return
		}
		w.Header().Set("Content-Type", "application/octet-stream")
		w.Header().Set("Content-Length", strconv.Itoa(len(data)))
		w.Header().Set("X-Content-Type-Options", "nosniff")
		if r.Method == http.MethodHead {
			return
		}
		_, _ = w.Write(data)
	case http.MethodPut:
		if r.ContentLength > apiFSWriteLimit {
			writeError(w, proto.Err(proto.CodeResourceExhausted, "body exceeds %d bytes", apiFSWriteLimit))
			return
		}
		data, err := io.ReadAll(io.LimitReader(r.Body, apiFSWriteLimit+1))
		if err != nil {
			badRequest(w, "read body: %v", err)
			return
		}
		if len(data) > apiFSWriteLimit {
			writeError(w, proto.Err(proto.CodeResourceExhausted, "body exceeds %d bytes", apiFSWriteLimit))
			return
		}
		mode := uint32(0o644)
		if v := q.Get("mode"); v != "" {
			n, err := strconv.ParseUint(v, 8, 32)
			if err != nil {
				badRequest(w, "mode must be octal")
				return
			}
			mode = uint32(n)
		}
		if err := cl.WriteFile(r.Context(), a.WS, path, data, mode, idem(r)...); err != nil {
			writeError(w, err)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	case http.MethodDelete:
		if err := cl.Remove(r.Context(), a.WS, path, q.Get("recursive") == "1", idem(r)...); err != nil {
			writeError(w, err)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	default:
		w.Header().Set("Allow", "GET, HEAD, PUT, DELETE")
		writeError(w, proto.Err(proto.CodeBadRequest, "method %s not allowed", r.Method))
	}
}
