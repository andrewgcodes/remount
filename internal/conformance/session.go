package conformance

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"runtime"
	"strings"
	"sync"
	"time"
)

// Unavailable is what a check returns when it could not run. It is not a
// failure and it is not a pass: Plan B §6.2 has exactly three outcomes and
// this is the third.
type Unavailable struct{ Reason string }

func (u *Unavailable) Error() string { return "unavailable: " + u.Reason }

// Unavailablef builds an Unavailable with a formatted reason.
func Unavailablef(format string, a ...any) error {
	return &Unavailable{Reason: fmt.Sprintf(format, a...)}
}

// IsUnavailable reports whether err says the check could not run.
func IsUnavailable(err error) bool {
	var u *Unavailable
	return errors.As(err, &u)
}

// Session is everything one conformance run may observe, and nothing else. A
// check receives a Session and no other handle on the world.
type Session struct {
	Target *Target
	// Control is a client connection to the implementation's control plane.
	Control *Conn
	// HTTP is the plain HTTP client for the artifact and event surfaces.
	HTTP *http.Client

	mu        sync.Mutex
	created   []string // workspace ids to destroy
	agents    []string // agent ids to destroy
	sharedWS  *fixture
	sharedErr error
	sharedOne sync.Once
	idem      int
}

// fixture is a claimed workspace plus the grant that reaches its node.
type fixture struct {
	WS    Workspace
	Grant Grant
	Node  string
}

// NewSession opens the surfaces a run needs.
//
// The handshake offers every identifier §3.1 defines so the report records
// what the target actually implements rather than what the runner happened to
// ask for. §3.1 makes that safe: the target echoes only what it implements,
// and a peer may not rely on anything that was not echoed.
func NewSession(ctx context.Context, t *Target) (*Session, error) {
	c, err := Dial(ctx, DialOptions{Endpoint: t.Endpoint, Token: t.Token, Role: "client", Caps: KnownCapabilities})
	if err != nil {
		return nil, err
	}
	return &Session{Target: t, Control: c, HTTP: &http.Client{Timeout: 60 * time.Second}}, nil
}

// Idem returns a fresh idempotency key that is unique within the run.
func (s *Session) Idem(prefix string) string {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.idem++
	return fmt.Sprintf("conf-%s-%d-%d", prefix, time.Now().UnixNano(), s.idem)
}

// Call sends a control-plane operation.
func (s *Session) Call(ctx context.Context, op string, body, out any) error {
	return s.Control.Request(ctx, PeerControl, op, body, out)
}

// CallNode sends a node operation to the node named by a grant.
func (s *Session) CallNode(ctx context.Context, node, op string, body, out any) error {
	return s.Control.Request(ctx, node, op, body, out)
}

// CreateWorkspace makes a workspace and registers it for cleanup.
func (s *Session) CreateWorkspace(ctx context.Context, spec WorkspaceSpec) (Workspace, error) {
	if spec.Requires.Backend == "" {
		spec.Requires.Backend = s.Target.Backend
	}
	var ws Workspace
	if err := s.Call(ctx, "ws.create", WSCreateReq{Spec: spec, Idem: s.Idem("ws")}, &ws); err != nil {
		return ws, err
	}
	s.mu.Lock()
	s.created = append(s.created, ws.ID)
	s.mu.Unlock()
	return ws, nil
}

// WaitClaimed polls until the workspace reports claimed with a node.
func (s *Session) WaitClaimed(ctx context.Context, id string) (Workspace, error) {
	deadline := time.Now().Add(90 * time.Second)
	var last Workspace
	for time.Now().Before(deadline) {
		var ws Workspace
		if err := s.Call(ctx, "ws.get", WSGetReq{ID: id}, &ws); err != nil {
			return last, err
		}
		last = ws
		if ws.State == WSClaimed && ws.Node != "" {
			return ws, nil
		}
		if ws.State == WSFailed {
			return ws, fmt.Errorf("conformance: workspace %s reached %s", id, ws.State)
		}
		select {
		case <-ctx.Done():
			return last, ctx.Err()
		case <-time.After(150 * time.Millisecond):
		}
	}
	return last, fmt.Errorf("conformance: workspace %s never became claimed (last state %q)", id, last.State)
}

// Grant fetches a grant for a workspace.
func (s *Session) Grant(ctx context.Context, ws string) (Grant, error) {
	var g Grant
	err := s.Call(ctx, "grant", GrantReq{WS: ws}, &g)
	return g, err
}

// Fixture returns a claimed workspace shared by every check that only needs
// "some workspace". Creating one per requirement would multiply the run's
// cost by fifty for no additional coverage.
// grantRefreshMargin is how much validity a shared grant must have left before
// a check is allowed to start with it. A grant observed against a real
// deployment had about twenty seconds of life, so the margin is most of that:
// refreshing costs one round trip, and not refreshing costs a false failure.
const grantRefreshMargin = 10 * time.Second

func (s *Session) Fixture(ctx context.Context) (*fixture, error) {
	s.sharedOne.Do(func() {
		f, err := s.NewFixture(ctx, WorkspaceSpec{Name: "conformance-shared"})
		s.sharedWS, s.sharedErr = f, err
	})
	if s.sharedErr != nil {
		return nil, s.sharedErr
	}
	// The shared fixture is created once and used by checks that run much
	// later, so on any run slower than the grant's lifetime the last checks
	// present an expired grant and fail with "unauthorized: grant expired" —
	// reporting the suite's own bookkeeping as a defect in the implementation
	// under test.
	//
	// That is not hypothetical and it is not a slow-machine edge case: judging
	// a deployment over the public internet is what this suite is for, and it
	// is exactly the case that takes long enough. Against a Modal deployment
	// the run takes about 95 s and CONF-SNAP-004 failed every time, while the
	// identical suite passed 53/53 against a local binary.
	s.mu.Lock()
	defer s.mu.Unlock()
	// ExpiresAt is milliseconds since the epoch, not seconds. Comparing it to
	// Unix() makes every grant look like it expires in fifty thousand years,
	// which is a bug that hides itself: the refresh simply never runs and the
	// symptom is unchanged.
	if expires := s.sharedWS.Grant.Claims.ExpiresAt; expires > 0 &&
		time.Now().Add(grantRefreshMargin).UnixMilli() >= expires {
		if err := s.Refresh(ctx, s.sharedWS); err != nil {
			return nil, fmt.Errorf("conformance: refreshing the shared grant: %w", err)
		}
	}
	return s.sharedWS, nil
}

// NewFixture creates, waits for and authorizes a fresh workspace.
func (s *Session) NewFixture(ctx context.Context, spec WorkspaceSpec) (*fixture, error) {
	ws, err := s.CreateWorkspace(ctx, spec)
	if err != nil {
		return nil, err
	}
	ready, err := s.WaitClaimed(ctx, ws.ID)
	if err != nil {
		return nil, err
	}
	g, err := s.Grant(ctx, ready.ID)
	if err != nil {
		return nil, err
	}
	node := g.Node
	if node == "" {
		node = ready.Node
	}
	return &fixture{WS: ready, Grant: g, Node: node}, nil
}

// Refresh re-reads a fixture's workspace and grant, which a move invalidates.
func (s *Session) Refresh(ctx context.Context, f *fixture) error {
	ready, err := s.WaitClaimed(ctx, f.WS.ID)
	if err != nil {
		return err
	}
	g, err := s.Grant(ctx, ready.ID)
	if err != nil {
		return err
	}
	f.WS, f.Grant = ready, g
	if g.Node != "" {
		f.Node = g.Node
	} else {
		f.Node = ready.Node
	}
	return nil
}

// RunResult is one completed exec session, as a cursor over its log.
type RunResult struct {
	Session string
	Chunks  []*Frame
	Stdout  []byte
	Stderr  []byte
	Exit    ExitInfo
	Gaps    []Gap
}

// Exec opens an exec session, drains its log to the exit chunk and returns
// what the cursor saw.
func (s *Session) Exec(ctx context.Context, f *fixture, program ...string) (*RunResult, error) {
	var res SOpenRes
	req := SOpenReq{WS: f.WS.ID, Kind: "exec", Program: program, Grant: &f.Grant, Idem: s.Idem("s")}
	if err := s.CallNode(ctx, f.Node, "s.open", req, &res); err != nil {
		return nil, err
	}
	frames, err := s.Control.CollectSession(ctx, res.S)
	if err != nil {
		return nil, fmt.Errorf("conformance: draining session %s: %w", res.S, err)
	}
	return decodeRun(res.S, frames)
}

func decodeRun(id string, frames []*Frame) (*RunResult, error) {
	out := &RunResult{Session: id, Chunks: frames}
	for _, f := range frames {
		var body ChunkBody
		if err := Unmarshal(f.Body, &body); err != nil {
			return nil, fmt.Errorf("conformance: chunk %d of %s is not a ChunkBody: %w", f.Seq, id, err)
		}
		switch body.St {
		case ChunkStdout:
			out.Stdout = append(out.Stdout, body.D...)
		case ChunkStderr:
			out.Stderr = append(out.Stderr, body.D...)
		case ChunkExit:
			var e ExitInfo
			if err := Unmarshal(body.D, &e); err != nil {
				return nil, fmt.Errorf("conformance: exit chunk of %s is not an ExitInfo: %w", id, err)
			}
			out.Exit = e
		case ChunkGap:
			var g Gap
			if err := Unmarshal(body.D, &g); err != nil {
				return nil, fmt.Errorf("conformance: gap chunk of %s is not a Gap: %w", id, err)
			}
			out.Gaps = append(out.Gaps, g)
		}
	}
	return out, nil
}

// Tail reads the canonical event log from a sequence, without following.
//
// §11 requires pagination to continue until the requested range is exhausted
// rather than stopping at an implementation page size, so this follows the
// cursor rather than trusting one response to be the whole answer.
func (s *Session) Tail(ctx context.Context, from uint64, ws string) ([]Event, error) {
	cursor := from
	var all []Event
	for {
		var page EventsRes
		if err := s.Call(ctx, "events.tail", EventsTailReq{
			From: cursor, Follow: false, WS: ws, Subscription: s.Idem("tail"),
		}, &page); err != nil {
			return all, err
		}
		if len(page.Events) == 0 {
			return all, nil
		}
		all = append(all, page.Events...)
		next := page.Events[len(page.Events)-1].Seq + 1
		if next <= cursor {
			return all, fmt.Errorf("conformance: event pagination did not advance past seq %d", cursor)
		}
		cursor = next
	}
}

// GetArtifact fetches a blob over HTTP and returns the status and body.
func (s *Session) GetArtifact(ctx context.Context, id string) (int, []byte, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, s.Target.Endpoint+"/v1/artifacts/"+id, nil)
	if err != nil {
		return 0, nil, err
	}
	s.authorize(req)
	res, err := s.HTTP.Do(req)
	if err != nil {
		return 0, nil, err
	}
	defer func() { _ = res.Body.Close() }()
	body, err := io.ReadAll(io.LimitReader(res.Body, 64<<20))
	return res.StatusCode, body, err
}

// PutArtifact stores a blob under an id and returns the status and body.
func (s *Session) PutArtifact(ctx context.Context, id string, blob []byte) (int, []byte, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodPut, s.Target.Endpoint+"/v1/artifacts/"+id, bytes.NewReader(blob))
	if err != nil {
		return 0, nil, err
	}
	s.authorize(req)
	res, err := s.HTTP.Do(req)
	if err != nil {
		return 0, nil, err
	}
	defer func() { _ = res.Body.Close() }()
	body, err := io.ReadAll(io.LimitReader(res.Body, 1<<20))
	return res.StatusCode, body, err
}

// PostEvent appends out of band over HTTP and returns the status and body.
func (s *Session) PostEvent(ctx context.Context, doc string) (int, []byte, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, s.Target.Endpoint+"/v1/events", strings.NewReader(doc))
	if err != nil {
		return 0, nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	s.authorize(req)
	res, err := s.HTTP.Do(req)
	if err != nil {
		return 0, nil, err
	}
	defer func() { _ = res.Body.Close() }()
	body, err := io.ReadAll(io.LimitReader(res.Body, 1<<20))
	return res.StatusCode, body, err
}

func (s *Session) authorize(req *http.Request) {
	if s.Target.Token != "" {
		req.Header.Set("Authorization", "Bearer "+s.Target.Token)
	}
}

// TrackAgent registers an agent for cleanup.
func (s *Session) TrackAgent(id string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.agents = append(s.agents, id)
}

// Cleanup destroys everything the run created and reports whether the
// absence was confirmed, which is what the evidence record's cleanup field
// means.
func (s *Session) Cleanup(ctx context.Context) error {
	s.mu.Lock()
	agents := append([]string(nil), s.agents...)
	workspaces := append([]string(nil), s.created...)
	s.mu.Unlock()

	var failures []string
	for _, id := range agents {
		if err := s.Call(ctx, "agent.destroy", AgentGetReq{ID: id, Idem: s.Idem("cleanup")}, nil); err != nil && !IsCode(err, "not_found") {
			failures = append(failures, fmt.Sprintf("agent %s: %v", id, err))
		}
	}
	for _, id := range workspaces {
		if err := s.Call(ctx, "ws.destroy", WSGetReq{ID: id, Idem: s.Idem("cleanup")}, nil); err != nil && !IsCode(err, "not_found") {
			failures = append(failures, fmt.Sprintf("workspace %s: %v", id, err))
		}
	}
	// Confirm the absence rather than trusting the delete.
	for _, id := range workspaces {
		var ws Workspace
		err := s.Call(ctx, "ws.get", WSGetReq{ID: id}, &ws)
		switch {
		case IsCode(err, "not_found"):
		case err != nil:
			failures = append(failures, fmt.Sprintf("workspace %s: confirming absence: %v", id, err))
		case ws.State != "destroyed":
			failures = append(failures, fmt.Sprintf("workspace %s still reports state %q", id, ws.State))
		}
	}
	if len(failures) > 0 {
		return errors.New("conformance: cleanup incomplete: " + strings.Join(failures, "; "))
	}
	return nil
}

// Close ends the run's connections.
func (s *Session) Close() { s.Control.Close() }

// Environment is the machine description Plan B §6 records.
type Environment struct {
	OS       string `json:"os"`
	Arch     string `json:"arch"`
	Backend  string `json:"backend,omitempty"`
	Provider string `json:"provider,omitempty"`
}

// HostEnvironment describes the machine this run is on.
func HostEnvironment() Environment {
	return Environment{OS: runtime.GOOS, Arch: runtime.GOARCH}
}

// track registers a workspace for cleanup without creating it.
func (s *Session) track(id string) {
	if id == "" {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, existing := range s.created {
		if existing == id {
			return
		}
	}
	s.created = append(s.created, id)
}

// headSequence returns the highest sequence the canonical log has assigned.
// It is the observable that makes "before any lifecycle mutation" checkable:
// a refused connection that advanced it mutated something.
func (s *Session) headSequence(ctx context.Context) (uint64, error) {
	events, err := s.Tail(ctx, 1, "")
	if err != nil {
		return 0, err
	}
	var head uint64
	for _, e := range events {
		if e.Seq > head {
			head = e.Seq
		}
	}
	return head, nil
}

// waitState polls until a workspace reaches the named state.
func (s *Session) waitState(ctx context.Context, id, want string) error {
	deadline := time.Now().Add(60 * time.Second)
	var last string
	for time.Now().Before(deadline) {
		var ws Workspace
		if err := s.Call(ctx, "ws.get", WSGetReq{ID: id}, &ws); err != nil {
			return err
		}
		last = ws.State
		if ws.State == want {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(150 * time.Millisecond):
		}
	}
	return fmt.Errorf("conformance: workspace %s never reached %q (last %q)", id, want, last)
}
