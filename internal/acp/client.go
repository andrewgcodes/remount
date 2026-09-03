// Package acp is a client for the Agent Client Protocol: JSON-RPC 2.0 over a
// newline-delimited stdio pair to a coding-agent subprocess. Remount is only
// ever the client side (ADR 0042); the node supplies a Handler for the calls
// an agent makes back (permission requests, file access, terminals) and
// observes every frame for the transcript.
//
// Types and method names are generated from the vendored schema
// (spec/acp, cmd/acpgen). Unknown fields are ignored on decode and the raw
// frame is what the transcript keeps, so a newer agent never breaks this
// package.
package acp

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"
)

// ErrClosed is returned for calls after Close or after the agent's stream
// ended.
var ErrClosed = errors.New("acp: connection closed")

// JSON-RPC error codes ACP relies on, beyond those in ErrorCode.
const (
	codeParseError     = -32700
	codeInvalidRequest = -32600
	codeMethodNotFound = -32601
	codeInvalidParams  = -32602
	codeInternalError  = -32603
)

// RPCError is a JSON-RPC error returned by the agent, or produced locally
// when a request could not be answered.
type RPCError struct {
	Code    int64           `json:"code"`
	Message string          `json:"message"`
	Data    json.RawMessage `json:"data,omitempty"`
}

func (e *RPCError) Error() string { return fmt.Sprintf("acp: %s (%d)", e.Message, e.Code) }

// Direction says which way a Frame travelled.
type Direction string

// Directions of a Frame.
const (
	DirectionIn  Direction = "in"  // agent → client
	DirectionOut Direction = "out" // client → agent
)

// Frame is one JSON-RPC message as it crossed the pipe. Raw is the exact
// line without its newline.
type Frame struct {
	Direction Direction
	At        time.Time
	Raw       json.RawMessage
}

// Handler answers the requests and notifications an agent sends to the
// client. Every method corresponds to one client-side ACP method; return an
// *RPCError to control the code the agent sees, any other error becomes an
// internal error. Embed UnimplementedHandler to refuse what the node does not
// serve.
type Handler interface {
	RequestPermission(context.Context, RequestPermissionRequest) (RequestPermissionResponse, error)
	ReadTextFile(context.Context, ReadTextFileRequest) (ReadTextFileResponse, error)
	WriteTextFile(context.Context, WriteTextFileRequest) (WriteTextFileResponse, error)
	CreateTerminal(context.Context, CreateTerminalRequest) (CreateTerminalResponse, error)
	TerminalOutput(context.Context, TerminalOutputRequest) (TerminalOutputResponse, error)
	ReleaseTerminal(context.Context, ReleaseTerminalRequest) (ReleaseTerminalResponse, error)
	WaitForTerminalExit(context.Context, WaitForTerminalExitRequest) (WaitForTerminalExitResponse, error)
	KillTerminal(context.Context, KillTerminalRequest) (KillTerminalResponse, error)
	CreateElicitation(context.Context, CreateElicitationRequest) (CreateElicitationResponse, error)
	CompleteElicitation(context.Context, CompleteElicitationNotification) error
	// Ext receives requests and notifications outside the ACP method set
	// (extension methods start with an underscore). Return nil, nil for a
	// notification.
	Ext(ctx context.Context, method string, params json.RawMessage) (json.RawMessage, error)
}

// UnimplementedHandler refuses every agent request with "method not found".
// Embed it and override what the node can actually serve.
type UnimplementedHandler struct{}

var errNotImplemented = &RPCError{Code: codeMethodNotFound, Message: "method not supported by this client"}

// RequestPermission refuses.
func (UnimplementedHandler) RequestPermission(context.Context, RequestPermissionRequest) (RequestPermissionResponse, error) {
	return RequestPermissionResponse{}, errNotImplemented
}

// ReadTextFile refuses.
func (UnimplementedHandler) ReadTextFile(context.Context, ReadTextFileRequest) (ReadTextFileResponse, error) {
	return ReadTextFileResponse{}, errNotImplemented
}

// WriteTextFile refuses.
func (UnimplementedHandler) WriteTextFile(context.Context, WriteTextFileRequest) (WriteTextFileResponse, error) {
	return WriteTextFileResponse{}, errNotImplemented
}

// CreateTerminal refuses.
func (UnimplementedHandler) CreateTerminal(context.Context, CreateTerminalRequest) (CreateTerminalResponse, error) {
	return CreateTerminalResponse{}, errNotImplemented
}

// TerminalOutput refuses.
func (UnimplementedHandler) TerminalOutput(context.Context, TerminalOutputRequest) (TerminalOutputResponse, error) {
	return TerminalOutputResponse{}, errNotImplemented
}

// ReleaseTerminal refuses.
func (UnimplementedHandler) ReleaseTerminal(context.Context, ReleaseTerminalRequest) (ReleaseTerminalResponse, error) {
	return ReleaseTerminalResponse{}, errNotImplemented
}

// WaitForTerminalExit refuses.
func (UnimplementedHandler) WaitForTerminalExit(context.Context, WaitForTerminalExitRequest) (WaitForTerminalExitResponse, error) {
	return WaitForTerminalExitResponse{}, errNotImplemented
}

// KillTerminal refuses.
func (UnimplementedHandler) KillTerminal(context.Context, KillTerminalRequest) (KillTerminalResponse, error) {
	return KillTerminalResponse{}, errNotImplemented
}

// CreateElicitation refuses.
func (UnimplementedHandler) CreateElicitation(context.Context, CreateElicitationRequest) (CreateElicitationResponse, error) {
	return CreateElicitationResponse{}, errNotImplemented
}

// CompleteElicitation ignores the notification.
func (UnimplementedHandler) CompleteElicitation(context.Context, CompleteElicitationNotification) error {
	return nil
}

// Ext refuses requests and ignores notifications.
func (UnimplementedHandler) Ext(context.Context, string, json.RawMessage) (json.RawMessage, error) {
	return nil, errNotImplemented
}

// Options configure a Client.
type Options struct {
	// Handler answers the agent's calls. Required.
	Handler Handler
	// Frames, when set, sees every message in both directions before it is
	// acted on. It is called from the read and write paths and must not
	// block for long.
	Frames func(Frame)
	// MaxLine bounds one JSON-RPC message; larger lines end the connection.
	// Default 16 MiB.
	MaxLine int
	// UpdateBuffer is the capacity of the Updates channel. Default 1024. The
	// reader blocks when it is full, so a slow consumer backpressures the
	// agent instead of losing updates.
	UpdateBuffer int
	// CancelGrace is how long Prompt waits for the agent's cancelled
	// response after the context is done before giving up. Default 10s.
	CancelGrace time.Duration
	Logger      *slog.Logger
}

// Client speaks ACP to one agent process over r (its stdout) and w (its
// stdin). It is safe for concurrent use; concurrent Prompt calls on one
// session are the caller's problem, as in the protocol.
type Client struct {
	opts    Options
	w       io.Writer
	wmu     sync.Mutex
	nextID  atomic.Int64
	pending sync.Map // id -> chan *response
	updates chan SessionNotification
	done    chan struct{}
	closed  atomic.Bool
	errMu   sync.Mutex
	err     error
	inbound sync.Map // agent request id (string) -> context.CancelFunc
	logger  *slog.Logger

	capsMu      sync.Mutex
	caps        AgentCapabilities
	initialized bool
}

type response struct {
	Result json.RawMessage `json:"result"`
	Error  *RPCError       `json:"error"`
}

type message struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      json.RawMessage `json:"id,omitempty"`
	Method  string          `json:"method,omitempty"`
	Params  json.RawMessage `json:"params,omitempty"`
	Result  json.RawMessage `json:"result,omitempty"`
	Error   *RPCError       `json:"error,omitempty"`
}

// NewClient starts reading from r immediately. Close the agent's pipes to
// end the connection; Done is closed when the reader exits.
func NewClient(r io.Reader, w io.Writer, opts Options) *Client {
	if opts.Handler == nil {
		opts.Handler = UnimplementedHandler{}
	}
	if opts.MaxLine <= 0 {
		opts.MaxLine = 16 << 20
	}
	if opts.UpdateBuffer <= 0 {
		opts.UpdateBuffer = 1024
	}
	if opts.CancelGrace <= 0 {
		opts.CancelGrace = 10 * time.Second
	}
	if opts.Logger == nil {
		opts.Logger = slog.New(slog.DiscardHandler)
	}
	c := &Client{
		opts:    opts,
		w:       w,
		updates: make(chan SessionNotification, opts.UpdateBuffer),
		done:    make(chan struct{}),
		logger:  opts.Logger,
	}
	go c.readLoop(r)
	return c
}

// Updates delivers every session/update notification in arrival order. The
// channel is closed when the connection ends.
func (c *Client) Updates() <-chan SessionNotification { return c.updates }

// Done is closed when the agent's stream ends, for any reason.
func (c *Client) Done() <-chan struct{} { return c.done }

// Err reports why the connection ended: nil for a clean EOF after Close,
// io.EOF when the agent closed its side, or the read error.
func (c *Client) Err() error {
	c.errMu.Lock()
	defer c.errMu.Unlock()
	return c.err
}

// Close fails every pending call with ErrClosed and stops accepting new
// ones. It does not close the pipes; the owner of the process does that.
func (c *Client) Close() error {
	if c.closed.Swap(true) {
		return nil
	}
	c.failPending(ErrClosed)
	return nil
}

func (c *Client) failPending(err error) {
	c.pending.Range(func(k, v any) bool {
		ch := v.(chan *response)
		select {
		case ch <- &response{Error: &RPCError{Code: codeInternalError, Message: err.Error()}}:
		default:
		}
		c.pending.Delete(k)
		return true
	})
}

func (c *Client) finish(err error) {
	c.errMu.Lock()
	if c.err == nil {
		c.err = err
	}
	c.errMu.Unlock()
	c.closed.Store(true)
	c.failPending(ErrClosed)
	c.inbound.Range(func(k, v any) bool {
		v.(context.CancelFunc)()
		return true
	})
	close(c.updates)
	close(c.done)
}

func (c *Client) readLoop(r io.Reader) {
	sc := bufio.NewScanner(r)
	sc.Buffer(make([]byte, 0, min(64<<10, c.opts.MaxLine)), c.opts.MaxLine)
	for sc.Scan() {
		line := bytes.TrimSpace(sc.Bytes())
		if len(line) == 0 {
			continue
		}
		raw := append(json.RawMessage(nil), line...)
		if c.opts.Frames != nil {
			c.opts.Frames(Frame{Direction: DirectionIn, At: time.Now(), Raw: raw})
		}
		var m message
		if err := json.Unmarshal(raw, &m); err != nil || (m.Method == "" && len(m.ID) == 0) {
			c.logger.Warn("acp: malformed frame", "err", err, "bytes", len(raw))
			c.reply(nil, nil, &RPCError{Code: codeParseError, Message: "parse error"})
			continue
		}
		switch {
		case m.Method != "" && len(m.ID) > 0 && string(m.ID) != "null":
			go c.serveRequest(m)
		case m.Method != "":
			c.serveNotification(m)
		default:
			c.deliverResponse(m)
		}
	}
	err := sc.Err()
	if err == nil {
		err = io.EOF
	}
	if c.closed.Load() && errors.Is(err, io.EOF) {
		err = nil
	}
	c.finish(err)
}

func (c *Client) deliverResponse(m message) {
	var id int64
	if err := json.Unmarshal(m.ID, &id); err != nil {
		c.logger.Warn("acp: response with unknown id", "id", string(m.ID))
		return
	}
	v, ok := c.pending.LoadAndDelete(id)
	if !ok {
		c.logger.Warn("acp: response for no pending call", "id", id)
		return
	}
	v.(chan *response) <- &response{Result: m.Result, Error: m.Error}
}

func (c *Client) serveNotification(m message) {
	ctx := context.Background()
	switch m.Method {
	case MethodSessionUpdate:
		var n SessionNotification
		if err := json.Unmarshal(m.Params, &n); err != nil {
			c.logger.Warn("acp: bad session/update", "err", err)
			return
		}
		select {
		case c.updates <- n:
		case <-c.done:
		}
	case MethodCancelRequest:
		var p CancelRequestNotification
		if err := json.Unmarshal(m.Params, &p); err != nil {
			return
		}
		if cancel, ok := c.inbound.Load(string(p.RequestID)); ok {
			cancel.(context.CancelFunc)()
		}
	case MethodElicitationComplete:
		var p CompleteElicitationNotification
		if err := json.Unmarshal(m.Params, &p); err != nil {
			return
		}
		if err := c.opts.Handler.CompleteElicitation(ctx, p); err != nil {
			c.logger.Warn("acp: elicitation/complete handler", "err", err)
		}
	default:
		if _, err := c.opts.Handler.Ext(ctx, m.Method, m.Params); err != nil && !errors.Is(err, errNotImplemented) {
			c.logger.Warn("acp: extension notification handler", "method", m.Method, "err", err)
		}
	}
}

func (c *Client) serveRequest(m message) {
	ctx, cancel := context.WithCancel(context.Background())
	key := string(m.ID)
	c.inbound.Store(key, cancel)
	defer func() {
		c.inbound.Delete(key)
		cancel()
	}()
	result, err := c.dispatch(ctx, m.Method, m.Params)
	if err != nil {
		var rpcErr *RPCError
		if !errors.As(err, &rpcErr) {
			rpcErr = &RPCError{Code: codeInternalError, Message: err.Error()}
		}
		c.reply(m.ID, nil, rpcErr)
		return
	}
	c.reply(m.ID, result, nil)
}

func (c *Client) dispatch(ctx context.Context, method string, params json.RawMessage) (any, error) {
	h := c.opts.Handler
	decode := func(v any) error {
		if err := json.Unmarshal(params, v); err != nil {
			return &RPCError{Code: codeInvalidParams, Message: "invalid params: " + err.Error()}
		}
		return nil
	}
	switch method {
	case MethodSessionRequestPermission:
		var p RequestPermissionRequest
		if err := decode(&p); err != nil {
			return nil, err
		}
		return h.RequestPermission(ctx, p)
	case MethodFsReadTextFile:
		var p ReadTextFileRequest
		if err := decode(&p); err != nil {
			return nil, err
		}
		return h.ReadTextFile(ctx, p)
	case MethodFsWriteTextFile:
		var p WriteTextFileRequest
		if err := decode(&p); err != nil {
			return nil, err
		}
		return h.WriteTextFile(ctx, p)
	case MethodTerminalCreate:
		var p CreateTerminalRequest
		if err := decode(&p); err != nil {
			return nil, err
		}
		return h.CreateTerminal(ctx, p)
	case MethodTerminalOutput:
		var p TerminalOutputRequest
		if err := decode(&p); err != nil {
			return nil, err
		}
		return h.TerminalOutput(ctx, p)
	case MethodTerminalRelease:
		var p ReleaseTerminalRequest
		if err := decode(&p); err != nil {
			return nil, err
		}
		return h.ReleaseTerminal(ctx, p)
	case MethodTerminalWaitForExit:
		var p WaitForTerminalExitRequest
		if err := decode(&p); err != nil {
			return nil, err
		}
		return h.WaitForTerminalExit(ctx, p)
	case MethodTerminalKill:
		var p KillTerminalRequest
		if err := decode(&p); err != nil {
			return nil, err
		}
		return h.KillTerminal(ctx, p)
	case MethodElicitationCreate:
		var p CreateElicitationRequest
		if err := decode(&p); err != nil {
			return nil, err
		}
		return h.CreateElicitation(ctx, p)
	default:
		res, err := h.Ext(ctx, method, params)
		if err != nil {
			return nil, err
		}
		if res == nil {
			res = json.RawMessage("null")
		}
		return res, nil
	}
}

func (c *Client) reply(id json.RawMessage, result any, rpcErr *RPCError) {
	if len(id) == 0 {
		id = json.RawMessage("null")
	}
	out := map[string]any{"jsonrpc": "2.0", "id": id}
	if rpcErr != nil {
		out["error"] = rpcErr
	} else {
		if result == nil {
			result = json.RawMessage("null")
		}
		out["result"] = result
	}
	if err := c.write(out); err != nil {
		c.logger.Warn("acp: write reply", "err", err)
	}
}

func (c *Client) write(v any) error {
	raw, err := json.Marshal(v)
	if err != nil {
		return err
	}
	if bytes.IndexByte(raw, '\n') >= 0 {
		return errors.New("acp: message contains a newline")
	}
	if c.opts.Frames != nil {
		c.opts.Frames(Frame{Direction: DirectionOut, At: time.Now(), Raw: raw})
	}
	c.wmu.Lock()
	defer c.wmu.Unlock()
	_, err = c.w.Write(append(raw, '\n'))
	return err
}

// Call sends a request and decodes the result into result (which may be
// nil). It returns *RPCError for an agent-side error, ErrClosed if the
// connection ended first, or ctx.Err().
func (c *Client) Call(ctx context.Context, method string, params, result any) error {
	if c.closed.Load() {
		return ErrClosed
	}
	id := c.nextID.Add(1)
	ch := make(chan *response, 1)
	c.pending.Store(id, ch)
	req := message{JSONRPC: "2.0", ID: json.RawMessage(fmt.Sprint(id)), Method: method}
	if params != nil {
		raw, err := json.Marshal(params)
		if err != nil {
			c.pending.Delete(id)
			return err
		}
		req.Params = raw
	}
	if err := c.write(req); err != nil {
		c.pending.Delete(id)
		return err
	}
	select {
	case res := <-ch:
		return decodeResponse(res, result)
	case <-ctx.Done():
		c.pending.Delete(id)
		return ctx.Err()
	case <-c.done:
		return ErrClosed
	}
}

func decodeResponse(res *response, result any) error {
	if res.Error != nil {
		if res.Error.Code == codeInternalError && res.Error.Message == ErrClosed.Error() {
			return ErrClosed
		}
		return res.Error
	}
	if result == nil || len(res.Result) == 0 || string(res.Result) == "null" {
		return nil
	}
	return json.Unmarshal(res.Result, result)
}

// Notify sends a notification (no response expected).
func (c *Client) Notify(method string, params any) error {
	if c.closed.Load() {
		return ErrClosed
	}
	req := message{JSONRPC: "2.0", Method: method}
	if params != nil {
		raw, err := json.Marshal(params)
		if err != nil {
			return err
		}
		req.Params = raw
	}
	return c.write(req)
}

// ErrProtocolVersion is returned by Initialize when the agent answers with a
// protocol version this package does not speak.
var ErrProtocolVersion = errors.New("acp: agent protocol version not supported")

// ErrCannotReopen is returned by Reopen when the agent advertises neither
// session/resume nor session/load.
var ErrCannotReopen = errors.New("acp: agent cannot reopen sessions")

// Initialize negotiates the protocol version and capabilities. The request's
// ProtocolVersion defaults to SchemaProtocolVersion, and an agent that
// answers with any other version is rejected with ErrProtocolVersion. The
// agent's capabilities are remembered for the Can* helpers and Reopen.
func (c *Client) Initialize(ctx context.Context, req InitializeRequest) (InitializeResponse, error) {
	if req.ProtocolVersion == 0 {
		req.ProtocolVersion = SchemaProtocolVersion
	}
	var res InitializeResponse
	if err := c.Call(ctx, MethodInitialize, req, &res); err != nil {
		return res, err
	}
	if res.ProtocolVersion != req.ProtocolVersion {
		return res, fmt.Errorf("%w: agent offered %d, client speaks %d", ErrProtocolVersion, res.ProtocolVersion, req.ProtocolVersion)
	}
	var caps AgentCapabilities
	if res.AgentCapabilities != nil {
		caps = *res.AgentCapabilities
	}
	c.capsMu.Lock()
	c.caps = caps
	c.initialized = true
	c.capsMu.Unlock()
	return res, nil
}

// AgentCapabilities returns what the agent advertised in Initialize (zero
// before then).
func (c *Client) AgentCapabilities() AgentCapabilities {
	c.capsMu.Lock()
	defer c.capsMu.Unlock()
	return c.caps
}

// CanLoadSession reports whether the agent advertised session/load.
func (c *Client) CanLoadSession() bool { return c.AgentCapabilities().LoadSession }

// CanResumeSession reports whether the agent advertised session/resume.
func (c *Client) CanResumeSession() bool {
	sc := c.AgentCapabilities().SessionCapabilities
	return sc != nil && sc.Resume != nil
}

// CanCloseSession reports whether the agent advertised session/close.
func (c *Client) CanCloseSession() bool {
	sc := c.AgentCapabilities().SessionCapabilities
	return sc != nil && sc.Close != nil
}

// SupportsAdditionalDirectories reports whether the agent accepts
// additionalDirectories on session requests.
func (c *Client) SupportsAdditionalDirectories() bool {
	sc := c.AgentCapabilities().SessionCapabilities
	return sc != nil && sc.AdditionalDirectories != nil
}

// ReopenResult is what Reopen learned about the reopened session.
type ReopenResult struct {
	// Replayed is true when session/load was used, meaning the agent streamed
	// the session's history as updates before responding.
	Replayed bool
	Modes    *SessionModeState
	Config   []SessionConfigOption
}

// Reopen reattaches to an existing agent session, preferring session/resume
// (no history replay) and falling back to session/load when only that is
// advertised. Additional directories are dropped when the agent does not
// support them rather than sent to be rejected.
func (c *Client) Reopen(ctx context.Context, sessionID SessionId, cwd string, mcp []McpServer, additional []string) (ReopenResult, error) {
	if mcp == nil {
		mcp = []McpServer{}
	}
	if !c.SupportsAdditionalDirectories() {
		additional = nil
	}
	switch {
	case c.CanResumeSession():
		res, err := c.ResumeSession(ctx, ResumeSessionRequest{SessionID: sessionID, Cwd: cwd, MCPServers: mcp, AdditionalDirectories: additional})
		if err != nil {
			return ReopenResult{}, err
		}
		return ReopenResult{Modes: res.Modes, Config: res.ConfigOptions}, nil
	case c.CanLoadSession():
		res, err := c.LoadSession(ctx, LoadSessionRequest{SessionID: sessionID, Cwd: cwd, MCPServers: mcp, AdditionalDirectories: additional})
		if err != nil {
			return ReopenResult{}, err
		}
		return ReopenResult{Replayed: true, Modes: res.Modes, Config: res.ConfigOptions}, nil
	}
	return ReopenResult{}, ErrCannotReopen
}

// Authenticate runs one of the agent's advertised auth methods.
func (c *Client) Authenticate(ctx context.Context, req AuthenticateRequest) (AuthenticateResponse, error) {
	var res AuthenticateResponse
	err := c.Call(ctx, MethodAuthenticate, req, &res)
	return res, err
}

// NewSession creates a session; Cwd must be absolute. A nil MCPServers is
// sent as an empty list, which the schema requires.
func (c *Client) NewSession(ctx context.Context, req NewSessionRequest) (NewSessionResponse, error) {
	if req.MCPServers == nil {
		req.MCPServers = []McpServer{}
	}
	if !c.SupportsAdditionalDirectories() {
		req.AdditionalDirectories = nil
	}
	var res NewSessionResponse
	err := c.Call(ctx, MethodSessionNew, req, &res)
	return res, err
}

// LoadSession reopens a session by id; the agent replays its history as
// session/update notifications before responding.
func (c *Client) LoadSession(ctx context.Context, req LoadSessionRequest) (LoadSessionResponse, error) {
	if req.MCPServers == nil {
		req.MCPServers = []McpServer{}
	}
	var res LoadSessionResponse
	err := c.Call(ctx, MethodSessionLoad, req, &res)
	return res, err
}

// ResumeSession reopens a session without replaying history.
func (c *Client) ResumeSession(ctx context.Context, req ResumeSessionRequest) (ResumeSessionResponse, error) {
	if req.MCPServers == nil {
		req.MCPServers = []McpServer{}
	}
	var res ResumeSessionResponse
	err := c.Call(ctx, MethodSessionResume, req, &res)
	return res, err
}

// ListSessions asks the agent for the sessions it can load.
func (c *Client) ListSessions(ctx context.Context, req ListSessionsRequest) (ListSessionsResponse, error) {
	var res ListSessionsResponse
	err := c.Call(ctx, MethodSessionList, req, &res)
	return res, err
}

// CloseSession releases a session on the agent side.
func (c *Client) CloseSession(ctx context.Context, req CloseSessionRequest) (CloseSessionResponse, error) {
	var res CloseSessionResponse
	err := c.Call(ctx, MethodSessionClose, req, &res)
	return res, err
}

// SetMode switches the session's mode (for example plan vs build).
func (c *Client) SetMode(ctx context.Context, req SetSessionModeRequest) (SetSessionModeResponse, error) {
	var res SetSessionModeResponse
	err := c.Call(ctx, MethodSessionSetMode, req, &res)
	return res, err
}

// Cancel asks the agent to stop the running turn on sessionID. The agent
// answers the in-flight Prompt with StopReasonCancelled.
func (c *Client) Cancel(sessionID SessionId) error {
	return c.Notify(MethodSessionCancel, CancelNotification{SessionID: sessionID})
}

// Prompt sends one user turn and returns when the agent ends it. Updates
// stream on Updates meanwhile. If ctx ends first, Prompt sends
// session/cancel and waits up to CancelGrace for the agent's cancelled
// response so the turn is known to have stopped; the returned StopReason is
// then StopReasonCancelled when the agent confirmed, and the error is
// ctx.Err() if it did not.
func (c *Client) Prompt(ctx context.Context, req PromptRequest) (PromptResponse, error) {
	var res PromptResponse
	callCtx, cancelCall := context.WithCancel(context.Background())
	defer cancelCall()
	errc := make(chan error, 1)
	go func() { errc <- c.Call(callCtx, MethodSessionPrompt, req, &res) }()
	select {
	case err := <-errc:
		return res, err
	case <-ctx.Done():
	}
	if err := c.Cancel(req.SessionID); err != nil {
		cancelCall()
		<-errc
		return res, err
	}
	timer := time.NewTimer(c.opts.CancelGrace)
	defer timer.Stop()
	select {
	case err := <-errc:
		return res, err
	case <-timer.C:
		cancelCall()
		<-errc
		return res, ctx.Err()
	}
}
