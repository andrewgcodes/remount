package mcp

import (
	"bufio"
	"bytes"
	"context"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"remount.dev/remount/internal/proto"
)

const (
	// ModernVersion is the current stateless MCP revision.
	ModernVersion = "2026-07-28"
	// LegacyVersion is the latest initialization-based MCP revision.
	LegacyVersion     = "2025-11-25"
	defaultMaxMessage = 1 << 20
	defaultMaxActive  = 16
)

// Options configure a Server.
type Options struct {
	Gateway         Gateway
	MaxMessageBytes int
	MaxActive       int
	CallTimeout     time.Duration
	Version         string
	BearerToken     string
	AllowedOrigins  []string
}

// Server is a bounded, concurrency-safe MCP JSON-RPC endpoint.
type Server struct {
	gateway        Gateway
	tools          []Tool
	byName         map[string]Operation
	maxMessage     int
	maxActive      int
	callTimeout    time.Duration
	version        string
	bearerToken    string
	allowedOrigins map[string]struct{}
}

// NewServer constructs an MCP server from the operation manifest.
func NewServer(opts Options) *Server {
	if opts.MaxMessageBytes <= 0 {
		opts.MaxMessageBytes = defaultMaxMessage
	}
	if opts.MaxActive <= 0 {
		opts.MaxActive = defaultMaxActive
	}
	if opts.CallTimeout <= 0 {
		opts.CallTimeout = 2 * time.Minute
	}
	if opts.Version == "" {
		opts.Version = "dev"
	}
	s := &Server{gateway: opts.Gateway, maxMessage: opts.MaxMessageBytes, maxActive: opts.MaxActive, callTimeout: opts.CallTimeout,
		version: opts.Version, bearerToken: opts.BearerToken, byName: make(map[string]Operation), allowedOrigins: make(map[string]struct{})}
	for _, origin := range opts.AllowedOrigins {
		s.allowedOrigins[origin] = struct{}{}
	}
	for _, operation := range Manifest() {
		s.tools = append(s.tools, operation.Tool)
		s.byName[operation.Tool.Name] = operation
	}
	return s
}

type request struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      json.RawMessage `json:"id,omitempty"`
	Method  string          `json:"method"`
	Params  json.RawMessage `json:"params,omitempty"`
}

type rpcError struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
	Data    any    `json:"data,omitempty"`
}

type response struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      json.RawMessage `json:"id"`
	Result  any             `json:"result,omitempty"`
	Error   *rpcError       `json:"error,omitempty"`
}

type callParams struct {
	Name      string          `json:"name"`
	Arguments json.RawMessage `json:"arguments"`
}

type requestMeta struct {
	ProtocolVersion    string          `json:"io.modelcontextprotocol/protocolVersion"`
	ClientCapabilities json.RawMessage `json:"io.modelcontextprotocol/clientCapabilities"`
}

type content struct{ Type, Text string }

func (c content) MarshalJSON() ([]byte, error) {
	return json.Marshal(struct {
		Type string `json:"type"`
		Text string `json:"text"`
	}{c.Type, c.Text})
}

type toolResult struct {
	ResultType        string    `json:"resultType,omitempty"`
	Content           []content `json:"content"`
	StructuredContent any       `json:"structuredContent,omitempty"`
	IsError           bool      `json:"isError,omitempty"`
	Meta              any       `json:"_meta,omitempty"`
}

func serverMeta(version string) map[string]any {
	return map[string]any{"io.modelcontextprotocol/serverInfo": map[string]any{"name": "remount", "version": version}}
}

// Handle processes one bounded JSON-RPC message. A nil response means the
// input was a notification.
func (s *Server) Handle(ctx context.Context, line []byte) *response {
	if len(line) > s.maxMessage {
		return errorResponse(nil, -32600, "request exceeds message limit", nil)
	}
	var req request
	if len(bytes.TrimSpace(line)) == 0 || bytes.HasPrefix(bytes.TrimSpace(line), []byte("[")) || json.Unmarshal(line, &req) != nil {
		return errorResponse(nil, -32700, "parse error", nil)
	}
	if req.JSONRPC != "2.0" || req.Method == "" || !validID(req.ID) {
		return errorResponse(normalizeID(req.ID), -32600, "invalid request", nil)
	}
	if len(req.ID) == 0 {
		return nil
	}
	if req.Method == "server/discover" || hasModernMetadata(req.Params) {
		if rpcErr := validateModernMetadata(req.Params); rpcErr != nil {
			return &response{JSONRPC: "2.0", ID: req.ID, Error: rpcErr}
		}
	}
	switch req.Method {
	case "initialize":
		var params struct {
			ProtocolVersion string `json:"protocolVersion"`
		}
		if err := json.Unmarshal(req.Params, &params); err != nil {
			return errorResponse(req.ID, -32602, "invalid initialize params", nil)
		}
		selected := params.ProtocolVersion
		if selected == "" || selected >= ModernVersion {
			selected = LegacyVersion
		}
		return resultResponse(req.ID, map[string]any{"protocolVersion": selected, "capabilities": map[string]any{"tools": map[string]any{}},
			"serverInfo":   map[string]any{"name": "remount", "version": s.version},
			"instructions": "Use Remount tools for durable workspaces and agents. Never request or print credentials or .remount/env."})
	case "server/discover":
		return resultResponse(req.ID, map[string]any{"resultType": "complete", "supportedVersions": []string{ModernVersion},
			"capabilities": map[string]any{"tools": map[string]any{}}, "instructions": "Durable Remount workspaces and agents; credentials stay at the broker edge.",
			"ttlMs": 300000, "cacheScope": "private", "_meta": serverMeta(s.version)})
	case "ping":
		return resultResponse(req.ID, map[string]any{"_meta": serverMeta(s.version)})
	case "tools/list":
		var params struct {
			Cursor string `json:"cursor"`
		}
		if len(req.Params) > 0 && json.Unmarshal(req.Params, &params) != nil {
			return errorResponse(req.ID, -32602, "invalid tools/list params", nil)
		}
		if params.Cursor != "" {
			return errorResponse(req.ID, -32602, "unknown cursor", nil)
		}
		return resultResponse(req.ID, map[string]any{"resultType": "complete", "tools": s.tools, "ttlMs": 300000, "cacheScope": "private", "_meta": serverMeta(s.version)})
	case "tools/call":
		var params callParams
		if err := json.Unmarshal(req.Params, &params); err != nil || params.Name == "" {
			return errorResponse(req.ID, -32602, "invalid tools/call params", nil)
		}
		if _, ok := s.byName[params.Name]; !ok {
			return errorResponse(req.ID, -32602, "unknown tool", map[string]any{"name": params.Name})
		}
		if len(params.Arguments) == 0 {
			params.Arguments = json.RawMessage("{}")
		}
		if len(params.Arguments) > s.maxMessage {
			return errorResponse(req.ID, -32602, "tool arguments exceed message limit", nil)
		}
		callCtx, cancel := context.WithTimeout(ctx, s.callTimeout)
		defer cancel()
		value, err := s.gateway.Invoke(callCtx, params.Name, params.Arguments)
		if err != nil {
			return resultResponse(req.ID, s.executionError(err))
		}
		value = sanitize(value)
		encoded, err := json.Marshal(value)
		if err != nil {
			return resultResponse(req.ID, s.executionError(errors.New("result could not be encoded")))
		}
		if len(encoded) > s.maxMessage {
			return resultResponse(req.ID, s.executionError(proto.Err(proto.CodeResourceExhausted, "tool result exceeds message limit")))
		}
		return resultResponse(req.ID, toolResult{ResultType: "complete", Content: []content{{Type: "text", Text: string(encoded)}}, StructuredContent: value, Meta: serverMeta(s.version)})
	default:
		return errorResponse(req.ID, -32601, "method not found", nil)
	}
}

func (s *Server) executionError(err error) toolResult {
	code := proto.CodeInternal
	message := "operation failed"
	var pe *proto.Error
	var unavailable *UnavailableError
	switch {
	case errors.As(err, &pe):
		code, message = pe.Code, pe.Msg
	case errors.As(err, &unavailable):
		code, message = "unavailable", unavailable.Error()
	case errors.Is(err, context.DeadlineExceeded):
		code, message = proto.CodeTimeout, "operation timed out"
	case errors.Is(err, context.Canceled):
		code, message = proto.CodeClosed, "operation cancelled"
	}
	structured := map[string]any{"ok": false, "code": code, "message": scrubString(message)}
	raw, _ := json.Marshal(structured)
	return toolResult{ResultType: "complete", Content: []content{{Type: "text", Text: string(raw)}}, StructuredContent: structured, IsError: true, Meta: serverMeta(s.version)}
}

func validID(id json.RawMessage) bool {
	if len(id) == 0 {
		return true
	}
	var value any
	if json.Unmarshal(id, &value) != nil {
		return false
	}
	switch value.(type) {
	case nil, string, float64:
		return true
	}
	return false
}

func normalizeID(id json.RawMessage) json.RawMessage {
	if len(id) == 0 {
		return json.RawMessage("null")
	}
	return id
}
func errorResponse(id json.RawMessage, code int, message string, data any) *response {
	return &response{JSONRPC: "2.0", ID: normalizeID(id), Error: &rpcError{Code: code, Message: message, Data: data}}
}
func resultResponse(id json.RawMessage, result any) *response {
	return &response{JSONRPC: "2.0", ID: normalizeID(id), Result: result}
}

type stdioConn struct {
	s      *Server
	out    io.Writer
	write  sync.Mutex
	mu     sync.Mutex
	cancel map[string]context.CancelFunc
	sem    chan struct{}
	wg     sync.WaitGroup
}

// ServeStdio serves newline-delimited MCP until input closes. Stdout contains
// protocol frames only, and every accepted request is joined before return.
func (s *Server) ServeStdio(ctx context.Context, in io.Reader, out io.Writer) error {
	c := &stdioConn{s: s, out: out, cancel: make(map[string]context.CancelFunc), sem: make(chan struct{}, s.maxActive)}
	scanner := bufio.NewScanner(in)
	scanner.Buffer(make([]byte, 64<<10), s.maxMessage+1)
	for scanner.Scan() {
		line := append([]byte(nil), scanner.Bytes()...)
		var probe request
		if json.Unmarshal(line, &probe) == nil && probe.Method == "notifications/cancelled" {
			c.cancelRequest(probe.Params)
			continue
		}
		select {
		case c.sem <- struct{}{}:
			requestCtx, cancel := context.WithCancel(ctx)
			key := string(probe.ID)
			if key != "" {
				c.mu.Lock()
				c.cancel[key] = cancel
				c.mu.Unlock()
			}
			c.wg.Add(1)
			go c.handle(requestCtx, cancel, key, line)
		default:
			if json.Unmarshal(line, &probe) == nil && len(probe.ID) > 0 {
				c.send(errorResponse(probe.ID, -32000, "too many active requests", nil))
			}
		}
	}
	c.mu.Lock()
	for _, cancel := range c.cancel {
		cancel()
	}
	c.mu.Unlock()
	c.wg.Wait()
	if err := scanner.Err(); err != nil {
		return fmt.Errorf("mcp stdio read: message exceeds limit or input failed: %w", err)
	}
	return nil
}

func (c *stdioConn) handle(ctx context.Context, cancel context.CancelFunc, key string, line []byte) {
	defer c.wg.Done()
	defer func() { <-c.sem }()
	defer func() {
		cancel()
		if key != "" {
			c.mu.Lock()
			delete(c.cancel, key)
			c.mu.Unlock()
		}
	}()
	if res := c.s.Handle(ctx, line); res != nil && ctx.Err() == nil {
		c.send(res)
	}
}

func (c *stdioConn) send(res *response) {
	c.write.Lock()
	defer c.write.Unlock()
	_ = json.NewEncoder(c.out).Encode(res)
}

func (c *stdioConn) cancelRequest(raw json.RawMessage) {
	var params struct {
		RequestID json.RawMessage `json:"requestId"`
	}
	if json.Unmarshal(raw, &params) != nil {
		return
	}
	c.mu.Lock()
	cancel := c.cancel[string(params.RequestID)]
	c.mu.Unlock()
	if cancel != nil {
		cancel()
	}
}

// ServeHTTP implements stateless MCP 2026-07-28 over Streamable HTTP. It
// intentionally does not claim legacy HTTP sessions.
func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/mcp" {
		http.NotFound(w, r)
		return
	}
	if r.Method != http.MethodPost {
		w.Header().Set("Allow", http.MethodPost)
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if !s.authorize(r) {
		w.Header().Set("WWW-Authenticate", "Bearer")
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}
	if !s.originAllowed(r) {
		http.Error(w, "forbidden origin", http.StatusForbidden)
		return
	}
	body, err := io.ReadAll(io.LimitReader(r.Body, int64(s.maxMessage)+1))
	if err != nil || len(body) > s.maxMessage {
		s.writeHTTPError(w, http.StatusRequestEntityTooLarge, -32600, "request exceeds message limit", nil)
		return
	}
	var req request
	parsed := json.Unmarshal(body, &req) == nil
	if parsed && len(req.ID) > 0 {
		if !acceptsModernHTTP(r.Header.Values("Accept")) {
			s.writeHTTPRPCError(w, http.StatusNotAcceptable, req.ID, -32600, "Accept must include application/json and text/event-stream", nil)
			return
		}
		if media, _, _ := strings.Cut(r.Header.Get("Content-Type"), ";"); !strings.EqualFold(strings.TrimSpace(media), "application/json") {
			s.writeHTTPRPCError(w, http.StatusUnsupportedMediaType, req.ID, -32600, "Content-Type must be application/json", nil)
			return
		}
		headerVersion := r.Header.Get("MCP-Protocol-Version")
		if headerVersion != ModernVersion {
			s.writeHTTPRPCError(w, http.StatusBadRequest, req.ID, -32022, "unsupported protocol version", map[string]any{"supported": []string{ModernVersion}, "requested": headerVersion})
			return
		}
		meta, metaErr := modernMetadata(req.Params)
		if metaErr != nil || meta.ProtocolVersion != headerVersion {
			s.writeHTTPRPCError(w, http.StatusBadRequest, req.ID, -32020, "MCP-Protocol-Version does not match request metadata", nil)
			return
		}
		if rpcErr := validateModernMetadata(req.Params); rpcErr != nil {
			s.writeHTTPRPCError(w, http.StatusBadRequest, req.ID, rpcErr.Code, rpcErr.Message, rpcErr.Data)
			return
		}
		if method := r.Header.Get("Mcp-Method"); method == "" || method != req.Method {
			s.writeHTTPRPCError(w, http.StatusBadRequest, req.ID, -32020, "Mcp-Method does not match request", nil)
			return
		}
		if req.Method == "tools/call" {
			var params callParams
			_ = json.Unmarshal(req.Params, &params)
			if name := r.Header.Get("Mcp-Name"); name == "" || name != params.Name {
				s.writeHTTPRPCError(w, http.StatusBadRequest, req.ID, -32020, "Mcp-Name does not match tool", nil)
				return
			}
		}
	}
	res := s.Handle(r.Context(), body)
	if res == nil {
		w.WriteHeader(http.StatusAccepted)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	if res.Error != nil && res.Error.Code == -32601 {
		w.WriteHeader(http.StatusNotFound)
	}
	_ = json.NewEncoder(w).Encode(res)
}

func hasModernMetadata(raw json.RawMessage) bool {
	meta, err := modernMetadata(raw)
	return err == nil && meta.ProtocolVersion != ""
}

func modernMetadata(raw json.RawMessage) (requestMeta, error) {
	var envelope struct {
		Meta *requestMeta `json:"_meta"`
	}
	if len(raw) == 0 || json.Unmarshal(raw, &envelope) != nil || envelope.Meta == nil {
		return requestMeta{}, errors.New("modern request metadata is absent")
	}
	return *envelope.Meta, nil
}

func validateModernMetadata(raw json.RawMessage) *rpcError {
	meta, err := modernMetadata(raw)
	if err != nil || meta.ProtocolVersion == "" {
		return &rpcError{Code: -32602, Message: "modern request metadata is invalid"}
	}
	if meta.ProtocolVersion != ModernVersion {
		return &rpcError{Code: -32022, Message: "unsupported protocol version", Data: map[string]any{"supported": []string{ModernVersion}, "requested": meta.ProtocolVersion}}
	}
	var capabilities map[string]any
	if len(meta.ClientCapabilities) == 0 || json.Unmarshal(meta.ClientCapabilities, &capabilities) != nil || capabilities == nil {
		return &rpcError{Code: -32602, Message: "clientCapabilities must be an object"}
	}
	return nil
}

func acceptsModernHTTP(values []string) bool {
	var jsonOK, eventOK bool
	for _, value := range values {
		for _, part := range strings.Split(value, ",") {
			media, _, _ := strings.Cut(strings.TrimSpace(part), ";")
			jsonOK = jsonOK || strings.EqualFold(media, "application/json") || media == "*/*"
			eventOK = eventOK || strings.EqualFold(media, "text/event-stream") || media == "*/*"
		}
	}
	return jsonOK && eventOK
}

func (s *Server) authorize(r *http.Request) bool {
	if s.bearerToken == "" {
		return true
	}
	want := "Bearer " + s.bearerToken
	got := r.Header.Get("Authorization")
	return len(got) == len(want) && subtle.ConstantTimeCompare([]byte(got), []byte(want)) == 1
}

func (s *Server) originAllowed(r *http.Request) bool {
	raw := r.Header.Get("Origin")
	if raw == "" {
		return true
	}
	if _, ok := s.allowedOrigins[raw]; ok {
		return true
	}
	u, err := url.Parse(raw)
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.User != nil || u.Host == "" || (u.Path != "" && u.Path != "/") || u.RawQuery != "" || u.Fragment != "" {
		return false
	}
	host, _, err := net.SplitHostPort(u.Host)
	if err != nil {
		host = u.Hostname()
	}
	ip := net.ParseIP(host)
	return strings.EqualFold(host, "localhost") || (ip != nil && ip.IsLoopback())
}

func (s *Server) writeHTTPError(w http.ResponseWriter, status, code int, message string, data any) {
	s.writeHTTPRPCError(w, status, nil, code, message, data)
}

func (s *Server) writeHTTPRPCError(w http.ResponseWriter, status int, id json.RawMessage, code int, message string, data any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(errorResponse(id, code, message, data))
}

func sanitize(value any) any {
	raw, err := json.Marshal(value)
	if err != nil {
		return map[string]any{"ok": false}
	}
	var decoded any
	if json.Unmarshal(raw, &decoded) != nil {
		return map[string]any{"ok": false}
	}
	return sanitizeJSON(decoded)
}

func sanitizeJSON(value any) any {
	switch v := value.(type) {
	case map[string]any:
		for key, child := range v {
			lower := strings.ToLower(key)
			if lower == "secret" || lower == "token" || lower == "grant" || lower == "node_token" {
				delete(v, key)
				continue
			}
			if lower == "root" || lower == "data_dir" {
				delete(v, key)
				continue
			}
			v[key] = sanitizeJSON(child)
		}
		return v
	case []any:
		for i := range v {
			v[i] = sanitizeJSON(v[i])
		}
		return v
	case string:
		return scrubString(v)
	default:
		return value
	}
}

func scrubString(value string) string {
	if !strings.Contains(value, "=") {
		return value
	}
	lines := strings.Split(value, "\n")
	for i, line := range lines {
		upper := strings.ToUpper(line)
		if strings.Contains(upper, "_API_KEY=") || strings.Contains(upper, "_TOKEN=") || strings.Contains(upper, "_SECRET=") || strings.HasPrefix(upper, "REMOUNT_") {
			lines[i] = "[credential-shaped environment line redacted]"
		}
	}
	return strings.Join(lines, "\n")
}
