package acp_test

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"strings"
	"sync"
	"testing"
	"time"

	"remount.dev/remount/internal/acp"
	"remount.dev/remount/internal/acp/acptest"
)

// world is a client wired to an in-process fake agent through two pipes.
type world struct {
	t      *testing.T
	client *acp.Client
	agent  *acptest.Agent
	frames []acp.Frame
	fmu    sync.Mutex
	// closing these ends the agent's side of the pipes
	agentIn  *io.PipeWriter // client writes here
	agentOut *io.PipeWriter // agent writes here
}

type handler struct {
	acp.UnimplementedHandler
	permission func(acp.RequestPermissionRequest) (acp.RequestPermissionResponse, error)
	read       func(acp.ReadTextFileRequest) (acp.ReadTextFileResponse, error)
	write      func(acp.WriteTextFileRequest) (acp.WriteTextFileResponse, error)
	terminal   func(acp.CreateTerminalRequest) (acp.CreateTerminalResponse, error)
	elicit     func(context.Context, acp.CreateElicitationRequest) (acp.CreateElicitationResponse, error)
	ext        func(string, json.RawMessage) (json.RawMessage, error)
}

func (h handler) RequestPermission(_ context.Context, r acp.RequestPermissionRequest) (acp.RequestPermissionResponse, error) {
	if h.permission == nil {
		return h.UnimplementedHandler.RequestPermission(context.Background(), r)
	}
	return h.permission(r)
}

func (h handler) ReadTextFile(_ context.Context, r acp.ReadTextFileRequest) (acp.ReadTextFileResponse, error) {
	if h.read == nil {
		return h.UnimplementedHandler.ReadTextFile(context.Background(), r)
	}
	return h.read(r)
}

func (h handler) WriteTextFile(_ context.Context, r acp.WriteTextFileRequest) (acp.WriteTextFileResponse, error) {
	if h.write == nil {
		return h.UnimplementedHandler.WriteTextFile(context.Background(), r)
	}
	return h.write(r)
}

func (h handler) CreateTerminal(_ context.Context, r acp.CreateTerminalRequest) (acp.CreateTerminalResponse, error) {
	if h.terminal == nil {
		return h.UnimplementedHandler.CreateTerminal(context.Background(), r)
	}
	return h.terminal(r)
}

func (h handler) CreateElicitation(ctx context.Context, r acp.CreateElicitationRequest) (acp.CreateElicitationResponse, error) {
	if h.elicit == nil {
		return h.UnimplementedHandler.CreateElicitation(ctx, r)
	}
	return h.elicit(ctx, r)
}

func (h handler) Ext(_ context.Context, method string, params json.RawMessage) (json.RawMessage, error) {
	if h.ext == nil {
		return h.UnimplementedHandler.Ext(context.Background(), method, params)
	}
	return h.ext(method, params)
}

func newWorld(t *testing.T, cfg acptest.Config, h acp.Handler, opts ...func(*acp.Options)) *world {
	t.Helper()
	c2a, agentIn := io.Pipe()  // client → agent
	a2c, agentOut := io.Pipe() // agent → client
	w := &world{t: t, agentIn: agentIn, agentOut: agentOut}
	w.agent = acptest.New(agentOut, cfg)
	go func() {
		_ = w.agent.Run(context.Background(), c2a)
	}()
	o := acp.Options{
		Handler:     h,
		CancelGrace: 2 * time.Second,
		Frames: func(f acp.Frame) {
			w.fmu.Lock()
			w.frames = append(w.frames, f)
			w.fmu.Unlock()
		},
	}
	for _, fn := range opts {
		fn(&o)
	}
	w.client = acp.NewClient(a2c, agentIn, o)
	t.Cleanup(func() {
		_ = w.client.Close()
		_ = agentIn.Close()
		_ = agentOut.Close()
	})
	return w
}

func (w *world) init(ctx context.Context, caps acp.ClientCapabilities) acp.InitializeResponse {
	w.t.Helper()
	res, err := w.client.Initialize(ctx, acp.InitializeRequest{ClientCapabilities: &caps, ClientInfo: &acp.Implementation{Name: "remount", Version: "test"}})
	if err != nil {
		w.t.Fatalf("initialize: %v", err)
	}
	return res
}

func (w *world) newSession(ctx context.Context) acp.SessionId {
	w.t.Helper()
	res, err := w.client.NewSession(ctx, acp.NewSessionRequest{Cwd: "/work"})
	if err != nil {
		w.t.Fatalf("session/new: %v", err)
	}
	return res.SessionID
}

func (w *world) framesOf(dir acp.Direction) []string {
	w.fmu.Lock()
	defer w.fmu.Unlock()
	var out []string
	for _, f := range w.frames {
		if f.Direction == dir {
			out = append(out, string(f.Raw))
		}
	}
	return out
}

func textPrompt(text string) []acp.ContentBlock {
	b, err := acp.NewContentBlockText(acp.TextContent{Text: text})
	if err != nil {
		panic(err)
	}
	return []acp.ContentBlock{b}
}

func collectText(t *testing.T, updates <-chan acp.SessionNotification, want int) string {
	t.Helper()
	var sb strings.Builder
	deadline := time.After(10 * time.Second)
	for i := 0; i < want; i++ {
		select {
		case u, ok := <-updates:
			if !ok {
				t.Fatalf("updates closed after %d of %d", i, want)
			}
			if u.Update.Kind != acp.SessionUpdateKindAgentMessageChunk {
				i--
				continue
			}
			chunk, err := u.Update.AsAgentMessageChunk()
			if err != nil {
				t.Fatal(err)
			}
			text, err := chunk.Content.AsText()
			if err != nil {
				t.Fatal(err)
			}
			sb.WriteString(text.Text)
		case <-deadline:
			t.Fatalf("timed out waiting for update %d of %d", i, want)
		}
	}
	return sb.String()
}

func TestEveryMethodRoundTrips(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	title := "Fake"
	term, err := acp.NewAuthMethodTerminal(acp.AuthMethodTerminal{ID: "login", Name: "Login"})
	if err != nil {
		t.Fatal(err)
	}
	cfg := acptest.Config{
		Info: acp.Implementation{Name: "fake", Version: "1.2.3", Title: &title},
		Capabilities: acp.AgentCapabilities{
			LoadSession: true,
			SessionCapabilities: &acp.SessionCapabilities{
				Resume:                &acp.SessionResumeCapabilities{},
				Close:                 &acp.SessionCloseCapabilities{},
				AdditionalDirectories: &acp.SessionAdditionalDirectoriesCapabilities{},
			},
		},
		AuthMethods: []acp.AuthMethod{term},
		RequireAuth: true,
		Modes: &acp.SessionModeState{
			CurrentModeID:  "build",
			AvailableModes: []acp.SessionMode{{ID: "build", Name: "Build"}, {ID: "plan", Name: "Plan"}},
		},
		Rewind: func(_ context.Context, s *acptest.Session, req acp.LoadSessionRequest) error {
			return s.Text("history:" + string(req.SessionID))
		},
	}
	w := newWorld(t, cfg, handler{})

	res := w.init(ctx, acp.ClientCapabilities{})
	if res.AgentInfo == nil || res.AgentInfo.Name != "fake" || res.AgentInfo.Title == nil || *res.AgentInfo.Title != "Fake" {
		t.Fatalf("agent info: %+v", res.AgentInfo)
	}
	if !w.client.CanLoadSession() || !w.client.CanResumeSession() || !w.client.CanCloseSession() || !w.client.SupportsAdditionalDirectories() {
		t.Fatalf("capabilities not recorded: %+v", w.client.AgentCapabilities())
	}
	if len(res.AuthMethods) != 1 || res.AuthMethods[0].Kind != acp.AuthMethodKindTerminal {
		t.Fatalf("auth methods: %+v", res.AuthMethods)
	}
	got, err := res.AuthMethods[0].AsTerminal()
	if err != nil || got.ID != "login" {
		t.Fatalf("auth method decode: %+v %v", got, err)
	}

	// session/new is refused until authenticate.
	_, err = w.client.NewSession(ctx, acp.NewSessionRequest{Cwd: "/work"})
	var rpcErr *acp.RPCError
	if !errors.As(err, &rpcErr) || rpcErr.Code != int64(acp.ErrorCodeAuthenticationRequired) {
		t.Fatalf("expected auth required, got %v", err)
	}
	if _, err := w.client.Authenticate(ctx, acp.AuthenticateRequest{MethodID: "login"}); err != nil {
		t.Fatalf("authenticate: %v", err)
	}
	newRes, err := w.client.NewSession(ctx, acp.NewSessionRequest{Cwd: "/work", AdditionalDirectories: []string{"/extra"}})
	if err != nil {
		t.Fatalf("session/new: %v", err)
	}
	if newRes.Modes == nil || newRes.Modes.CurrentModeID != "build" {
		t.Fatalf("modes: %+v", newRes.Modes)
	}
	sid := newRes.SessionID

	// Every outbound session/new carries mcpServers even when nil in Go, and
	// the second one kept additionalDirectories because the agent supports it.
	var sessionNew []string
	for _, f := range w.framesOf(acp.DirectionOut) {
		if strings.Contains(f, `"session/new"`) {
			if !strings.Contains(f, `"mcpServers":[]`) {
				t.Fatalf("session/new without mcpServers list: %s", f)
			}
			sessionNew = append(sessionNew, f)
		}
	}
	if len(sessionNew) != 2 || !strings.Contains(sessionNew[1], `"additionalDirectories":["/extra"]`) {
		t.Fatalf("session/new frames: %v", sessionNew)
	}

	// prompt streams an update then ends the turn
	promptRes, err := w.client.Prompt(ctx, acp.PromptRequest{SessionID: sid, Prompt: textPrompt("hello")})
	if err != nil {
		t.Fatalf("prompt: %v", err)
	}
	if promptRes.StopReason != acp.StopReasonEndTurn {
		t.Fatalf("stop reason %q", promptRes.StopReason)
	}
	if got := collectText(t, w.client.Updates(), 1); got != "hello" {
		t.Fatalf("echo %q", got)
	}

	if _, err := w.client.SetMode(ctx, acp.SetSessionModeRequest{SessionID: sid, ModeID: "plan"}); err != nil {
		t.Fatalf("set_mode: %v", err)
	}

	// load replays history before the response
	loadRes, err := w.client.LoadSession(ctx, acp.LoadSessionRequest{SessionID: "old", Cwd: "/work"})
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if loadRes.Modes == nil {
		t.Fatal("load response lost modes")
	}
	if got := collectText(t, w.client.Updates(), 1); got != "history:old" {
		t.Fatalf("replay %q", got)
	}

	if _, err := w.client.ResumeSession(ctx, acp.ResumeSessionRequest{SessionID: "old2", Cwd: "/work"}); err != nil {
		t.Fatalf("resume: %v", err)
	}
	// Reopen prefers resume when advertised.
	if _, err := w.client.Reopen(ctx, "old3", "/work", nil, nil); err != nil {
		t.Fatalf("reopen: %v", err)
	}
	outs := w.framesOf(acp.DirectionOut)
	last := outs[len(outs)-1]
	if !strings.Contains(last, `"session/resume"`) {
		t.Fatalf("reopen did not use resume: %s", last)
	}
	if _, err := w.client.CloseSession(ctx, acp.CloseSessionRequest{SessionID: sid}); err != nil {
		t.Fatalf("close: %v", err)
	}
	// A closed session is gone on the agent.
	_, err = w.client.Prompt(ctx, acp.PromptRequest{SessionID: sid, Prompt: textPrompt("x")})
	if !errors.As(err, &rpcErr) || rpcErr.Code != int64(acp.ErrorCodeResourceNotFound) {
		t.Fatalf("prompt on closed session: %v", err)
	}
	// Unknown method is a method-not-found RPC error, not a hang.
	err = w.client.Call(ctx, "session/nonsense", nil, nil)
	if !errors.As(err, &rpcErr) || rpcErr.Code != int64(acp.ErrorCodeMethodNotFound) {
		t.Fatalf("unknown method: %v", err)
	}
	if err := w.client.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := w.client.NewSession(ctx, acp.NewSessionRequest{Cwd: "/work"}); !errors.Is(err, acp.ErrClosed) {
		t.Fatalf("call after close: %v", err)
	}
}

func TestReopenFallsBackToLoadAndRefusesWithoutEither(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	w := newWorld(t, acptest.Config{Capabilities: acp.AgentCapabilities{LoadSession: true}}, handler{})
	w.init(ctx, acp.ClientCapabilities{})
	res, err := w.client.Reopen(ctx, "s1", "/work", nil, []string{"/dropped"})
	if err != nil || !res.Replayed {
		t.Fatalf("reopen via load: %+v %v", res, err)
	}
	outs := w.framesOf(acp.DirectionOut)
	last := outs[len(outs)-1]
	if !strings.Contains(last, `"session/load"`) || strings.Contains(last, "additionalDirectories") {
		t.Fatalf("expected load without additionalDirectories: %s", last)
	}

	w2 := newWorld(t, acptest.Config{}, handler{})
	w2.init(ctx, acp.ClientCapabilities{})
	if _, err := w2.client.Reopen(ctx, "s1", "/work", nil, nil); !errors.Is(err, acp.ErrCannotReopen) {
		t.Fatalf("expected ErrCannotReopen, got %v", err)
	}
}

func TestInitializeRejectsForeignProtocolVersion(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	w := newWorld(t, acptest.Config{}, handler{})
	_, err := w.client.Initialize(ctx, acp.InitializeRequest{ProtocolVersion: 99})
	if !errors.Is(err, acp.ErrProtocolVersion) {
		t.Fatalf("expected ErrProtocolVersion, got %v", err)
	}
}

func TestCancelMidTurn(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	started := make(chan struct{})
	cfg := acptest.Config{Turn: func(turnCtx context.Context, s *acptest.Session, _ acp.PromptRequest) (acp.StopReason, error) {
		_ = s.Text("working")
		close(started)
		<-turnCtx.Done()
		_ = s.Text("stopped")
		return acp.StopReasonCancelled, nil
	}}
	w := newWorld(t, cfg, handler{})
	w.init(ctx, acp.ClientCapabilities{})
	sid := w.newSession(ctx)

	promptCtx, cancelPrompt := context.WithCancel(ctx)
	defer cancelPrompt()
	done := make(chan struct{})
	var res acp.PromptResponse
	var perr error
	go func() {
		res, perr = w.client.Prompt(promptCtx, acp.PromptRequest{SessionID: sid, Prompt: textPrompt("go")})
		close(done)
	}()
	<-started
	cancelPrompt()
	<-done
	if perr != nil {
		t.Fatalf("prompt after cancel: %v", perr)
	}
	if res.StopReason != acp.StopReasonCancelled {
		t.Fatalf("stop reason %q, want cancelled", res.StopReason)
	}
	if got := collectText(t, w.client.Updates(), 2); got != "workingstopped" {
		t.Fatalf("updates %q", got)
	}
	var sawCancel bool
	for _, f := range w.framesOf(acp.DirectionOut) {
		if strings.Contains(f, `"session/cancel"`) {
			sawCancel = true
		}
	}
	if !sawCancel {
		t.Fatal("no session/cancel notification sent")
	}
}

func TestCancelGraceExpiresWhenAgentIgnoresCancel(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	release := make(chan struct{})
	cfg := acptest.Config{Turn: func(_ context.Context, _ *acptest.Session, _ acp.PromptRequest) (acp.StopReason, error) {
		<-release
		return acp.StopReasonEndTurn, nil
	}}
	w := newWorld(t, cfg, handler{}, func(o *acp.Options) { o.CancelGrace = 200 * time.Millisecond })
	w.init(ctx, acp.ClientCapabilities{})
	sid := w.newSession(ctx)
	promptCtx, cancelPrompt := context.WithCancel(ctx)
	go func() {
		time.Sleep(50 * time.Millisecond)
		cancelPrompt()
	}()
	_, err := w.client.Prompt(promptCtx, acp.PromptRequest{SessionID: sid, Prompt: textPrompt("go")})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context.Canceled after grace, got %v", err)
	}
	close(release)
}

func TestPermissionAndFileRoundTrips(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	cfg := acptest.Config{Turn: func(turnCtx context.Context, s *acptest.Session, _ acp.PromptRequest) (acp.StopReason, error) {
		var perm acp.RequestPermissionResponse
		err := s.Call(turnCtx, acp.MethodSessionRequestPermission, acp.RequestPermissionRequest{
			SessionID: s.ID,
			ToolCall:  acp.ToolCallUpdate{ToolCallID: "tc1"},
			Options:   []acp.PermissionOption{{OptionID: "allow", Name: "Allow", Kind: acp.PermissionOptionKindAllowOnce}},
		}, &perm)
		if err != nil {
			return "", err
		}
		sel, err := perm.Outcome.AsSelected()
		if err != nil || sel.OptionID != "allow" {
			return "", errors.New("unexpected permission outcome")
		}
		var read acp.ReadTextFileResponse
		if err := s.Call(turnCtx, acp.MethodFsReadTextFile, acp.ReadTextFileRequest{SessionID: s.ID, Path: "/work/a.txt"}, &read); err != nil {
			return "", err
		}
		if err := s.Call(turnCtx, acp.MethodFsWriteTextFile, acp.WriteTextFileRequest{SessionID: s.ID, Path: "/work/b.txt", Content: read.Content + "!"}, nil); err != nil {
			return "", err
		}
		// terminal is not advertised: the client must refuse it.
		err = s.Call(turnCtx, acp.MethodTerminalCreate, acp.CreateTerminalRequest{SessionID: s.ID, Command: "ls"}, nil)
		var rpcErr *acp.RPCError
		if !errors.As(err, &rpcErr) || rpcErr.Code != int64(acp.ErrorCodeMethodNotFound) {
			return "", errors.New("terminal/create should be method-not-found")
		}
		return acp.StopReasonEndTurn, nil
	}}
	var writes []string
	var wmu sync.Mutex
	h := handler{
		permission: func(r acp.RequestPermissionRequest) (acp.RequestPermissionResponse, error) {
			out, err := acp.NewRequestPermissionOutcomeSelected(acp.SelectedPermissionOutcome{OptionID: r.Options[0].OptionID})
			if err != nil {
				return acp.RequestPermissionResponse{}, err
			}
			return acp.RequestPermissionResponse{Outcome: out}, nil
		},
		read: func(r acp.ReadTextFileRequest) (acp.ReadTextFileResponse, error) {
			return acp.ReadTextFileResponse{Content: "contents of " + r.Path}, nil
		},
		write: func(r acp.WriteTextFileRequest) (acp.WriteTextFileResponse, error) {
			wmu.Lock()
			writes = append(writes, r.Path+"="+r.Content)
			wmu.Unlock()
			return acp.WriteTextFileResponse{}, nil
		},
	}
	w := newWorld(t, cfg, h)
	w.init(ctx, acp.ClientCapabilities{Fs: &acp.FileSystemCapabilities{ReadTextFile: true, WriteTextFile: true}})
	sid := w.newSession(ctx)
	res, err := w.client.Prompt(ctx, acp.PromptRequest{SessionID: sid, Prompt: textPrompt("go")})
	if err != nil {
		t.Fatalf("prompt: %v", err)
	}
	if res.StopReason != acp.StopReasonEndTurn {
		t.Fatalf("stop %q", res.StopReason)
	}
	wmu.Lock()
	defer wmu.Unlock()
	if len(writes) != 1 || writes[0] != "/work/b.txt=contents of /work/a.txt!" {
		t.Fatalf("writes %v", writes)
	}
}

func TestAgentCancelsItsOwnRequest(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	handlerCancelled := make(chan struct{})
	cfg := acptest.Config{Turn: func(turnCtx context.Context, s *acptest.Session, _ acp.PromptRequest) (acp.StopReason, error) {
		reqCtx, cancelReq := context.WithTimeout(turnCtx, 100*time.Millisecond)
		defer cancelReq()
		err := s.Call(reqCtx, acp.MethodElicitationCreate, json.RawMessage(`{"sessionId":"`+string(s.ID)+`","mode":"form"}`), nil)
		if !errors.Is(err, context.DeadlineExceeded) {
			return "", errors.New("expected deadline")
		}
		select {
		case <-handlerCancelled:
		case <-time.After(5 * time.Second):
			return "", errors.New("client handler never saw $/cancel_request")
		}
		return acp.StopReasonEndTurn, nil
	}}
	h := handler{elicit: func(ctx context.Context, _ acp.CreateElicitationRequest) (acp.CreateElicitationResponse, error) {
		<-ctx.Done()
		close(handlerCancelled)
		return acp.CreateElicitationResponse{}, &acp.RPCError{Code: int64(acp.ErrorCodeRequestCancelled), Message: "cancelled"}
	}}
	w := newWorld(t, cfg, h)
	w.init(ctx, acp.ClientCapabilities{})
	sid := w.newSession(ctx)
	if _, err := w.client.Prompt(ctx, acp.PromptRequest{SessionID: sid, Prompt: textPrompt("go")}); err != nil {
		t.Fatalf("prompt: %v", err)
	}
}

func TestMalformedFramesDoNotKillTheConnection(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	cfg := acptest.Config{Turn: func(_ context.Context, s *acptest.Session, _ acp.PromptRequest) (acp.StopReason, error) {
		_ = s.WriteRaw("this is not json")
		_ = s.WriteRaw(`{"jsonrpc":"2.0"}`)
		_ = s.WriteRaw(`{"jsonrpc":"2.0","id":12345,"result":{}}`)
		_ = s.WriteRaw(`{"jsonrpc":"2.0","method":"session/update","params":"nope"}`)
		_ = s.WriteRaw(`{"jsonrpc":"2.0","method":"_ext/hello","params":{"x":1}}`)
		_ = s.Text("still alive")
		return acp.StopReasonEndTurn, nil
	}}
	w := newWorld(t, cfg, handler{})
	w.init(ctx, acp.ClientCapabilities{})
	sid := w.newSession(ctx)
	res, err := w.client.Prompt(ctx, acp.PromptRequest{SessionID: sid, Prompt: textPrompt("go")})
	if err != nil || res.StopReason != acp.StopReasonEndTurn {
		t.Fatalf("prompt after garbage: %+v %v", res, err)
	}
	if got := collectText(t, w.client.Updates(), 1); got != "still alive" {
		t.Fatalf("update %q", got)
	}
	// Parse errors were answered per JSON-RPC, with a null id.
	var parseErrors int
	for _, f := range w.framesOf(acp.DirectionOut) {
		if strings.Contains(f, `"code":-32700`) && strings.Contains(f, `"id":null`) {
			parseErrors++
		}
	}
	if parseErrors != 2 {
		t.Fatalf("expected 2 parse-error replies, got %d", parseErrors)
	}
	select {
	case <-w.client.Done():
		t.Fatal("connection ended")
	default:
	}
}

func TestOversizedFrameEndsConnection(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	cfg := acptest.Config{Turn: func(_ context.Context, s *acptest.Session, _ acp.PromptRequest) (acp.StopReason, error) {
		_ = s.WriteRaw(strings.Repeat("x", 5000))
		return acp.StopReasonEndTurn, nil
	}}
	w := newWorld(t, cfg, handler{}, func(o *acp.Options) { o.MaxLine = 4096 })
	w.init(ctx, acp.ClientCapabilities{})
	sid := w.newSession(ctx)
	_, err := w.client.Prompt(ctx, acp.PromptRequest{SessionID: sid, Prompt: textPrompt("go")})
	if !errors.Is(err, acp.ErrClosed) {
		t.Fatalf("expected ErrClosed, got %v", err)
	}
	<-w.client.Done()
	if w.client.Err() == nil {
		t.Fatal("expected a read error")
	}
}

func TestAgentExitMidPrompt(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	exit := make(chan struct{})
	cfg := acptest.Config{Turn: func(_ context.Context, s *acptest.Session, _ acp.PromptRequest) (acp.StopReason, error) {
		_ = s.Text("partial")
		close(exit)
		select {}
	}}
	w := newWorld(t, cfg, handler{})
	w.init(ctx, acp.ClientCapabilities{})
	sid := w.newSession(ctx)
	go func() {
		<-exit
		_ = w.agentOut.Close() // the agent process dies: its stdout closes
	}()
	_, err := w.client.Prompt(ctx, acp.PromptRequest{SessionID: sid, Prompt: textPrompt("go")})
	if !errors.Is(err, acp.ErrClosed) {
		t.Fatalf("expected ErrClosed, got %v", err)
	}
	select {
	case <-w.client.Done():
	case <-time.After(5 * time.Second):
		t.Fatal("Done not closed after agent exit")
	}
	if !errors.Is(w.client.Err(), io.EOF) {
		t.Fatalf("Err = %v, want EOF", w.client.Err())
	}
	if got := collectText(t, w.client.Updates(), 1); got != "partial" {
		t.Fatalf("partial update lost: %q", got)
	}
	if _, ok := <-w.client.Updates(); ok {
		t.Fatal("updates not closed")
	}
	if _, err := w.client.Initialize(ctx, acp.InitializeRequest{}); !errors.Is(err, acp.ErrClosed) {
		t.Fatalf("call after exit: %v", err)
	}
}

func TestUnknownFieldsRoundTripThroughUnions(t *testing.T) {
	raw := `{"sessionUpdate":"tool_call","toolCallId":"t1","title":"Read","status":"in_progress","futureField":{"a":1}}`
	var u acp.SessionUpdate
	if err := json.Unmarshal([]byte(raw), &u); err != nil {
		t.Fatal(err)
	}
	if u.Kind != acp.SessionUpdateKindToolCall {
		t.Fatalf("kind %q", u.Kind)
	}
	tc, err := u.AsToolCall()
	if err != nil || tc.Title != "Read" || tc.Status != acp.ToolCallStatusInProgress {
		t.Fatalf("decode %+v %v", tc, err)
	}
	out, err := json.Marshal(u)
	if err != nil || string(out) != raw {
		t.Fatalf("round trip lost data: %s %v", out, err)
	}
	// unknown kinds survive too
	if err := json.Unmarshal([]byte(`{"sessionUpdate":"from_the_future","x":1}`), &u); err != nil {
		t.Fatal(err)
	}
	if u.Kind != "from_the_future" {
		t.Fatalf("kind %q", u.Kind)
	}
	if _, err := u.AsPlan(); err == nil {
		t.Fatal("AsPlan on wrong kind should fail")
	}
	// constructors set the discriminator
	plan, err := acp.NewSessionUpdatePlan(acp.Plan{Entries: []acp.PlanEntry{{Content: "step", Priority: acp.PlanEntryPriorityHigh, Status: acp.PlanEntryStatusPending}}})
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(plan.Raw), `"sessionUpdate":"plan"`) {
		t.Fatalf("missing tag: %s", plan.Raw)
	}
	// an empty union refuses to marshal rather than emitting garbage
	if _, err := json.Marshal(acp.SessionUpdate{}); err == nil {
		t.Fatal("empty union marshalled")
	}
}
