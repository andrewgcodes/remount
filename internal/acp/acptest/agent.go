// Package acptest is an in-process fake ACP agent for tests. It speaks the
// same newline-delimited JSON-RPC as a real agent over any reader/writer
// pair, so it can sit behind an io.Pipe in a unit test or behind a real
// process's stdio (see Main) inside a workspace.
package acptest

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"sync"
	"sync/atomic"

	"remount.dev/remount/internal/acp"
)

// Turn is what the scripted agent does for one prompt. It streams through
// s and returns the stop reason. ctx is cancelled by session/cancel; a
// script that honours it should return acp.StopReasonCancelled.
type Turn func(ctx context.Context, s *Session, req acp.PromptRequest) (acp.StopReason, error)

// Config scripts the fake agent.
type Config struct {
	Info         acp.Implementation
	Capabilities acp.AgentCapabilities
	AuthMethods  []acp.AuthMethod
	// RequireAuth makes session/new fail with the auth-required code until
	// authenticate has been called.
	RequireAuth bool
	// Turn handles session/prompt. Default: echo the prompt text as one
	// agent_message_chunk and end the turn.
	Turn Turn
	// Modes, when set, is returned from session/new and session/load.
	Modes *acp.SessionModeState
	// OnNewSession, when set, can inspect or reject session creation.
	OnNewSession func(acp.NewSessionRequest) error
	// Rewind, when set, is called for session/load so the script can replay
	// history through s before the response is sent.
	Rewind func(ctx context.Context, s *Session, req acp.LoadSessionRequest) error
	// SessionIDs overrides the ids handed out by session/new.
	SessionIDs []acp.SessionId
}

// Session is the agent's view of one session and its channel back to the
// client.
type Session struct {
	ID    acp.SessionId
	Cwd   string
	agent *Agent
	mode  acp.SessionModeId
}

// Update sends a session/update notification.
func (s *Session) Update(u acp.SessionUpdate) error {
	return s.agent.notify(acp.MethodSessionUpdate, acp.SessionNotification{SessionID: s.ID, Update: u})
}

// Text streams one agent_message_chunk of text.
func (s *Session) Text(text string) error {
	block, err := acp.NewContentBlockText(acp.TextContent{Text: text})
	if err != nil {
		return err
	}
	u, err := acp.NewSessionUpdateAgentMessageChunk(acp.ContentChunk{Content: block})
	if err != nil {
		return err
	}
	return s.Update(u)
}

// Call makes a request to the client and decodes the result.
func (s *Session) Call(ctx context.Context, method string, params, result any) error {
	return s.agent.call(ctx, method, params, result)
}

// Notify sends a notification to the client.
func (s *Session) Notify(method string, params any) error {
	return s.agent.notify(method, params)
}

// Mode is the mode last set through session/set_mode.
func (s *Session) Mode() acp.SessionModeId { return s.mode }

// Agent is one fake agent connection.
type Agent struct {
	cfg      Config
	w        io.Writer
	wmu      sync.Mutex
	nextID   atomic.Int64
	pending  sync.Map // id -> chan rpcResponse
	sessions sync.Map // SessionId -> *Session
	turns    sync.Map // SessionId -> context.CancelFunc
	authed   atomic.Bool
	seq      atomic.Int64
	done     chan struct{}
	err      error

	// Received records every request method the agent has handled, for
	// assertions.
	Received func(method string, params json.RawMessage)
}

type rpcResponse struct {
	Result json.RawMessage
	Error  *acp.RPCError
}

type message struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      json.RawMessage `json:"id,omitempty"`
	Method  string          `json:"method,omitempty"`
	Params  json.RawMessage `json:"params,omitempty"`
	Result  json.RawMessage `json:"result,omitempty"`
	Error   *acp.RPCError   `json:"error,omitempty"`
}

// Serve runs the fake agent on r (client's writes) and w (client's reads)
// until r ends or ctx is cancelled. It returns the read error, if any.
func Serve(ctx context.Context, r io.Reader, w io.Writer, cfg Config) error {
	a := New(w, cfg)
	return a.Run(ctx, r)
}

// New builds an agent writing to w; call Run to read.
func New(w io.Writer, cfg Config) *Agent {
	if cfg.Turn == nil {
		cfg.Turn = Echo
	}
	if cfg.Info.Name == "" {
		cfg.Info = acp.Implementation{Name: "fake-acp-agent", Version: "0"}
	}
	return &Agent{cfg: cfg, w: w, done: make(chan struct{})}
}

// Echo is the default Turn: it repeats the prompt's text blocks and ends
// the turn.
func Echo(ctx context.Context, s *Session, req acp.PromptRequest) (acp.StopReason, error) {
	for _, b := range req.Prompt {
		if b.Kind == acp.ContentBlockKindText {
			t, err := b.AsText()
			if err != nil {
				return "", err
			}
			if err := s.Text(t.Text); err != nil {
				return "", err
			}
		}
	}
	return acp.StopReasonEndTurn, nil
}

// Run reads frames from r until it ends. Each request is served on its own
// goroutine so a long prompt does not block session/cancel.
func (a *Agent) Run(ctx context.Context, r io.Reader) error {
	defer close(a.done)
	sc := bufio.NewScanner(r)
	sc.Buffer(make([]byte, 0, 64<<10), 16<<20)
	var wg sync.WaitGroup
	defer wg.Wait()
	for sc.Scan() {
		line := bytes.TrimSpace(sc.Bytes())
		if len(line) == 0 {
			continue
		}
		var m message
		if err := json.Unmarshal(line, &m); err != nil {
			a.reply(json.RawMessage("null"), nil, &acp.RPCError{Code: -32700, Message: "parse error"})
			continue
		}
		switch {
		case m.Method != "" && len(m.ID) > 0:
			wg.Add(1)
			go func() {
				defer wg.Done()
				a.serve(ctx, m)
			}()
		case m.Method != "":
			a.notification(m)
		default:
			var id int64
			if json.Unmarshal(m.ID, &id) == nil {
				if ch, ok := a.pending.LoadAndDelete(id); ok {
					ch.(chan rpcResponse) <- rpcResponse{Result: m.Result, Error: m.Error}
				}
			}
		}
	}
	a.err = sc.Err()
	return a.err
}

// Done is closed when Run returns.
func (a *Agent) Done() <-chan struct{} { return a.done }

func (a *Agent) notification(m message) {
	switch m.Method {
	case acp.MethodSessionCancel:
		var p acp.CancelNotification
		if json.Unmarshal(m.Params, &p) != nil {
			return
		}
		if cancel, ok := a.turns.Load(p.SessionID); ok {
			cancel.(context.CancelFunc)()
		}
	}
}

func (a *Agent) serve(ctx context.Context, m message) {
	if a.Received != nil {
		a.Received(m.Method, m.Params)
	}
	res, err := a.dispatch(ctx, m.Method, m.Params)
	if err != nil {
		var rpcErr *acp.RPCError
		if !errors.As(err, &rpcErr) {
			rpcErr = &acp.RPCError{Code: -32603, Message: err.Error()}
		}
		a.reply(m.ID, nil, rpcErr)
		return
	}
	a.reply(m.ID, res, nil)
}

func (a *Agent) session(raw json.RawMessage) (*Session, error) {
	var p struct {
		SessionID acp.SessionId `json:"sessionId"`
	}
	if err := json.Unmarshal(raw, &p); err != nil {
		return nil, &acp.RPCError{Code: -32602, Message: err.Error()}
	}
	s, ok := a.sessions.Load(p.SessionID)
	if !ok {
		return nil, &acp.RPCError{Code: int64(acp.ErrorCodeResourceNotFound), Message: "unknown session"}
	}
	return s.(*Session), nil
}

func (a *Agent) dispatch(ctx context.Context, method string, params json.RawMessage) (any, error) {
	switch method {
	case acp.MethodInitialize:
		var p acp.InitializeRequest
		if err := json.Unmarshal(params, &p); err != nil {
			return nil, &acp.RPCError{Code: -32602, Message: err.Error()}
		}
		caps := a.cfg.Capabilities
		return acp.InitializeResponse{
			ProtocolVersion:   acp.SchemaProtocolVersion,
			AgentCapabilities: &caps,
			AgentInfo:         &a.cfg.Info,
			AuthMethods:       a.cfg.AuthMethods,
		}, nil
	case acp.MethodAuthenticate:
		var p acp.AuthenticateRequest
		if err := json.Unmarshal(params, &p); err != nil {
			return nil, &acp.RPCError{Code: -32602, Message: err.Error()}
		}
		for _, m := range a.cfg.AuthMethods {
			var id struct {
				ID acp.AuthMethodId `json:"id"`
			}
			if json.Unmarshal(m.Raw, &id) == nil && id.ID == p.MethodID {
				a.authed.Store(true)
				return acp.AuthenticateResponse{}, nil
			}
		}
		return nil, &acp.RPCError{Code: -32602, Message: "unknown auth method"}
	case acp.MethodSessionNew:
		var p acp.NewSessionRequest
		if err := json.Unmarshal(params, &p); err != nil {
			return nil, &acp.RPCError{Code: -32602, Message: err.Error()}
		}
		if a.cfg.RequireAuth && !a.authed.Load() {
			return nil, &acp.RPCError{Code: int64(acp.ErrorCodeAuthenticationRequired), Message: "authentication required"}
		}
		if a.cfg.OnNewSession != nil {
			if err := a.cfg.OnNewSession(p); err != nil {
				return nil, err
			}
		}
		n := a.seq.Add(1)
		id := acp.SessionId(fmt.Sprintf("fake-%d", n))
		if int(n) <= len(a.cfg.SessionIDs) {
			id = a.cfg.SessionIDs[n-1]
		}
		s := &Session{ID: id, Cwd: p.Cwd, agent: a}
		a.sessions.Store(id, s)
		return acp.NewSessionResponse{SessionID: id, Modes: a.cfg.Modes}, nil
	case acp.MethodSessionLoad:
		var p acp.LoadSessionRequest
		if err := json.Unmarshal(params, &p); err != nil {
			return nil, &acp.RPCError{Code: -32602, Message: err.Error()}
		}
		if !a.cfg.Capabilities.LoadSession {
			return nil, &acp.RPCError{Code: -32601, Message: "method not found"}
		}
		s := &Session{ID: p.SessionID, Cwd: p.Cwd, agent: a}
		a.sessions.Store(p.SessionID, s)
		if a.cfg.Rewind != nil {
			if err := a.cfg.Rewind(ctx, s, p); err != nil {
				return nil, err
			}
		}
		return acp.LoadSessionResponse{Modes: a.cfg.Modes}, nil
	case acp.MethodSessionResume:
		var p acp.ResumeSessionRequest
		if err := json.Unmarshal(params, &p); err != nil {
			return nil, &acp.RPCError{Code: -32602, Message: err.Error()}
		}
		if sc := a.cfg.Capabilities.SessionCapabilities; sc == nil || sc.Resume == nil {
			return nil, &acp.RPCError{Code: -32601, Message: "method not found"}
		}
		s := &Session{ID: p.SessionID, Cwd: p.Cwd, agent: a}
		a.sessions.Store(p.SessionID, s)
		return acp.ResumeSessionResponse{Modes: a.cfg.Modes}, nil
	case acp.MethodSessionSetMode:
		var p acp.SetSessionModeRequest
		if err := json.Unmarshal(params, &p); err != nil {
			return nil, &acp.RPCError{Code: -32602, Message: err.Error()}
		}
		s, err := a.session(params)
		if err != nil {
			return nil, err
		}
		s.mode = p.ModeID
		return acp.SetSessionModeResponse{}, nil
	case acp.MethodSessionClose:
		s, err := a.session(params)
		if err != nil {
			return nil, err
		}
		a.sessions.Delete(s.ID)
		return acp.CloseSessionResponse{}, nil
	case acp.MethodSessionPrompt:
		var p acp.PromptRequest
		if err := json.Unmarshal(params, &p); err != nil {
			return nil, &acp.RPCError{Code: -32602, Message: err.Error()}
		}
		s, err := a.session(params)
		if err != nil {
			return nil, err
		}
		turnCtx, cancel := context.WithCancel(ctx)
		defer cancel()
		a.turns.Store(s.ID, cancel)
		defer a.turns.Delete(s.ID)
		stop, err := a.cfg.Turn(turnCtx, s, p)
		if err != nil {
			return nil, err
		}
		return acp.PromptResponse{StopReason: stop}, nil
	default:
		return nil, &acp.RPCError{Code: -32601, Message: "method not found: " + method}
	}
}

func (a *Agent) reply(id json.RawMessage, result any, rpcErr *acp.RPCError) {
	out := map[string]any{"jsonrpc": "2.0", "id": id}
	if rpcErr != nil {
		out["error"] = rpcErr
	} else {
		out["result"] = result
	}
	_ = a.write(out)
}

func (a *Agent) write(v any) error {
	raw, err := json.Marshal(v)
	if err != nil {
		return err
	}
	a.wmu.Lock()
	defer a.wmu.Unlock()
	_, err = a.w.Write(append(raw, '\n'))
	return err
}

// WriteRaw sends bytes verbatim (plus newline), for malformed-frame tests.
func (a *Agent) WriteRaw(line string) error {
	a.wmu.Lock()
	defer a.wmu.Unlock()
	_, err := io.WriteString(a.w, line+"\n")
	return err
}

func (a *Agent) notify(method string, params any) error {
	raw, err := json.Marshal(params)
	if err != nil {
		return err
	}
	return a.write(message{JSONRPC: "2.0", Method: method, Params: raw})
}

func (a *Agent) call(ctx context.Context, method string, params, result any) error {
	id := a.nextID.Add(1)
	ch := make(chan rpcResponse, 1)
	a.pending.Store(id, ch)
	raw, err := json.Marshal(params)
	if err != nil {
		return err
	}
	if err := a.write(message{JSONRPC: "2.0", ID: json.RawMessage(fmt.Sprint(id)), Method: method, Params: raw}); err != nil {
		a.pending.Delete(id)
		return err
	}
	select {
	case res := <-ch:
		if res.Error != nil {
			return res.Error
		}
		if result == nil || len(res.Result) == 0 {
			return nil
		}
		return json.Unmarshal(res.Result, result)
	case <-ctx.Done():
		a.pending.Delete(id)
		_ = a.write(message{JSONRPC: "2.0", Method: acp.MethodCancelRequest, Params: json.RawMessage(fmt.Sprintf(`{"requestId":%d}`, id))})
		return ctx.Err()
	case <-a.done:
		return io.ErrClosedPipe
	}
}

// Main serves the fake agent on the process's stdio with the given config,
// for use from a test binary re-exec'd as a workspace command.
func Main(cfg Config) {
	if err := Serve(context.Background(), os.Stdin, os.Stdout, cfg); err != nil {
		fmt.Fprintln(os.Stderr, "fake acp agent:", err)
		os.Exit(1)
	}
}

// WriteRaw sends bytes verbatim on the agent's output, for malformed-frame
// tests.
func (s *Session) WriteRaw(line string) error { return s.agent.WriteRaw(line) }
