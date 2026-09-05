package session

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/creack/pty"

	"remount.dev/remount/internal/artifact"
	"remount.dev/remount/internal/ids"
	"remount.dev/remount/internal/metrics"
	"remount.dev/remount/internal/proto"
)

// Spec describes what to run.
type Spec struct {
	WS string
	// Generation fences durable log commits to the workspace assignment that
	// produced them. It is node/control metadata, never process input.
	Generation uint64
	Kind       string // proto.SessionExec | SessionPTY | SessionPort
	Program    []string
	Cwd        string
	Env        []string // full environment, KEY=VALUE
	Rows       uint16
	Cols       uint16
	Stdin      bool // exec: keep stdin open
	Timeout    time.Duration
	// Port sessions
	Host string
	Port int
	// Idempotency key: opening twice with the same key returns the same session.
	IdempotencyKey string
	// Principal for audit.
	Principal string
	// Tenant scopes Principal for quota accounting. Principal names need only be
	// unique inside one tenant, so omitting this would let an actor in one
	// tenant consume another tenant's per-principal session allowance.
	Tenant string
	// Run marks a harness launch; carried in the info chunk and to OnExit.
	Run *proto.RunInfo
	// Runner is backend-owned and process-local. It is deliberately excluded
	// from idempotency serialization.
	Runner Runner `cbor:"-" json:"-"`
}

// Runner starts a backend-managed process or byte stream. Manager remains the
// owner of admission, seq 0, logs, input deduplication and the terminal exit.
type Runner interface {
	Start(Spec) (Running, error)
}

// Running is one backend-managed session. Wait joins the producer and its
// output readers must reach EOF when Wait returns.
type Running interface {
	PID() int
	Stdin() io.WriteCloser
	Stdout() io.Reader
	Stderr() io.Reader
	Resize(rows, cols uint16) error
	Signal(name string) error
	CloseWrite() error
	Wait() proto.ExitInfo
}

// Session is a running or finished process with its output log.
type Session struct {
	ID        string
	WS        string
	Kind      string
	Info      proto.SessionInfo
	Log       *Log
	Principal string
	Tenant    string

	mu           sync.Mutex
	inputMu      sync.Mutex     // serializes writes without blocking lifecycle reads
	stdin        io.WriteCloser // exec: pipe; pty: the pty master; port: the conn
	ptmx         *os.File
	cmd          *exec.Cmd
	conn         net.Conn
	running      Running
	lastISeq     uint64
	inputPending *inputProgress // guarded by inputMu; one fixed-size retry record
	exit         *proto.ExitInfo
	exited       chan struct{}
	observed     chan struct{} // closed after durable completion callback and OnExit
	startDone    chan struct{} // closed once process/connection startup has resolved
	timeout      *time.Timer
	outputReady  chan struct{} // pumps wait until StreamInfo is committed at seq 0
	logErr       error
	onComplete   func(proto.ExitInfo, LogRecord) error // commits durability before exited is closed
	onFinish     func()                                // commits manager accounting before exited is closed
	closeReason  string                                // recorded in the exit chunk when the node ends the session
	// onSignal is how a record-only session (kind acp) hears a kill: there is
	// no process, so the owner that appends to it decides what stopping means.
	onSignal func(name string)
}

type inputProgress struct {
	sequence uint64
	digest   [sha256.Size]byte
	offset   int
	eof      bool
}

// Record appends one chunk to a record-only session. Kinds with a process
// own their streams and refuse it.
func (s *Session) Record(stream uint8, data []byte) (uint64, error) {
	if s.Kind != proto.SessionACP {
		return 0, proto.Err(proto.CodeBadRequest, "session %s is not record-only", s.ID)
	}
	<-s.startDone
	if s.Exited() {
		return 0, proto.Err(proto.CodeClosed, "session already exited")
	}
	return s.Log.Append(stream, data)
}

// End finishes a record-only session with info as its exit chunk.
func (s *Session) End(info proto.ExitInfo) {
	if s.Kind != proto.SessionACP {
		return
	}
	<-s.startDone
	s.finish(info)
}

// OnSignal registers what Signal does to a record-only session. Without a
// handler a signal ends the session immediately.
func (s *Session) OnSignal(fn func(name string)) {
	s.mu.Lock()
	s.onSignal = fn
	s.mu.Unlock()
}

// Exited reports whether the process has finished.
func (s *Session) Exited() bool {
	select {
	case <-s.exited:
		return true
	default:
		return false
	}
}

// ExitInfo returns the exit record, or nil while running.
func (s *Session) ExitInfo() *proto.ExitInfo {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.exit
}

// Wait blocks until exit or ctx.
func (s *Session) Wait(ctx context.Context) (*proto.ExitInfo, error) {
	select {
	case <-s.exited:
		return s.ExitInfo(), nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// alreadyClosed reports whether err is the close of something already closed.
// Delivering EOF twice is not a failure: the second close means the first took
// effect. The two runtimes disagree about which sentinel says so — a pipe
// reports os.ErrClosed, a socket net.ErrClosed — and errors.Is does not relate
// them, so a port session that had lost its peer used to fail an ordinary EOF.
func alreadyClosed(err error) bool {
	return err == nil || errors.Is(err, os.ErrClosed) || errors.Is(err, net.ErrClosed)
}

// Input writes to the process. Inputs carry a client-side sequence; an iseq
// at or below the last applied one is a retry and is dropped (idempotent).
func (s *Session) Input(iseq uint64, data []byte, eof bool) error {
	s.inputMu.Lock()
	defer s.inputMu.Unlock()
	s.mu.Lock()
	if iseq != 0 {
		if iseq <= s.lastISeq {
			s.mu.Unlock()
			metrics.InputsDropped.Inc()
			return nil // duplicate delivery
		}
	}
	stdin := s.stdin
	kind := s.Kind
	conn := s.conn
	running := s.running
	s.mu.Unlock()
	if stdin == nil {
		return proto.Err(proto.CodeUnsupported, "session has no stdin")
	}
	digest := sha256.Sum256(data)
	if p := s.inputPending; p != nil {
		if p.sequence != iseq || p.digest != digest || p.eof != eof {
			return proto.Err(proto.CodeConflict, "retry pending stdin input with the same sequence, bytes and EOF")
		}
	} else {
		s.inputPending = &inputProgress{sequence: iseq, digest: digest, eof: eof}
	}
	p := s.inputPending
	if p.offset < len(data) {
		n, err := writeFull(stdin, data[p.offset:])
		p.offset += n
		if err != nil {
			return proto.Err(proto.CodeClosed, "stdin: %v", err)
		}
	}
	if eof {
		if running != nil {
			if err := running.CloseWrite(); !alreadyClosed(err) {
				return proto.Err(proto.CodeClosed, "stdin close: %v", err)
			}
		} else if kind == proto.SessionExec {
			if err := stdin.Close(); !alreadyClosed(err) {
				return proto.Err(proto.CodeClosed, "stdin close: %v", err)
			}
		} else if kind == proto.SessionPort {
			if cw, ok := conn.(interface{ CloseWrite() error }); ok {
				if err := cw.CloseWrite(); !alreadyClosed(err) {
					return proto.Err(proto.CodeClosed, "stdin close: %v", err)
				}
			}
		}
	}
	if iseq != 0 {
		s.mu.Lock()
		s.lastISeq = iseq
		if eof && kind == proto.SessionExec {
			s.stdin = nil
		}
		s.mu.Unlock()
	}
	s.inputPending = nil
	return nil
}

func writeFull(w io.Writer, data []byte) (int, error) {
	written := 0
	for len(data) > 0 {
		n, err := w.Write(data)
		if n < 0 || n > len(data) {
			return written, io.ErrShortWrite
		}
		written += n
		if err != nil {
			return written, err
		}
		if n == 0 {
			return written, io.ErrShortWrite
		}
		data = data[n:]
	}
	return written, nil
}

// LastInputSeq is the last input sequence completely handed to the process.
// Pipe writes are not a durable transaction; progress belongs to this session.
func (s *Session) LastInputSeq() uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.lastISeq
}

// Resize changes the pty window.
func (s *Session) Resize(rows, cols uint16) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.running != nil {
		return s.running.Resize(rows, cols)
	}
	if s.ptmx == nil {
		return proto.Err(proto.CodeUnsupported, "not a pty session")
	}
	return pty.Setsize(s.ptmx, &pty.Winsize{Rows: rows, Cols: cols})
}

// Signal sends a signal by name (TERM, KILL, INT, HUP, QUIT, USR1, USR2).
func (s *Session) Signal(name string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.exit != nil {
		return proto.Err(proto.CodeClosed, "session already exited")
	}
	if s.running != nil {
		return s.running.Signal(name)
	}
	if s.Kind == proto.SessionPort {
		if s.conn != nil {
			return s.conn.Close()
		}
		return nil
	}
	if s.Kind == proto.SessionACP {
		if fn := s.onSignal; fn != nil {
			s.mu.Unlock()
			fn(name)
			s.mu.Lock()
			return nil
		}
		s.mu.Unlock()
		s.finish(proto.ExitInfo{Code: -1, Error: "signal " + name})
		s.mu.Lock()
		return nil
	}
	if s.cmd == nil || s.cmd.Process == nil {
		return proto.Err(proto.CodeNotFound, "no process")
	}
	return signalProcess(s.cmd, name)
}

// Kill terminates the process immediately.
func (s *Session) Kill() {
	_ = s.Signal("KILL")
}

// Terminate kills the process and records reason in the exit chunk, so a
// subscriber can tell a policy decision from the process ending on its own.
func (s *Session) Terminate(reason string) {
	s.mu.Lock()
	if s.exit == nil && s.closeReason == "" {
		s.closeReason = reason
	}
	s.mu.Unlock()
	s.Kill()
}

func (s *Session) finish(info proto.ExitInfo) {
	s.mu.Lock()
	if s.exit != nil {
		s.mu.Unlock()
		return
	}
	if info.Reason == "" {
		info.Reason = s.closeReason
	}
	s.exit = &info
	if s.logErr != nil && info.Error == "" {
		info.Error = s.logErr.Error()
		s.exit = &info
	}
	if s.timeout != nil {
		s.timeout.Stop()
	}
	onFinish := s.onFinish
	s.onFinish = nil
	s.mu.Unlock()
	metrics.SessionsExited.Inc()
	if _, err := s.Log.appendTerminal(proto.StreamExit, proto.MustMarshal(info)); err != nil {
		s.mu.Lock()
		if s.exit != nil && s.exit.Error == "" {
			s.exit.Error = "session log: " + err.Error()
		}
		s.mu.Unlock()
	}
	s.mu.Lock()
	finalInfo := *s.exit
	s.mu.Unlock()
	// A successful Wait must mean that the active-session admission slot is
	// reusable and any advertised durable replay record is complete. Commit
	// both before publishing session exit; doing either in an observer after
	// close(s.exited) leaves a node-loss window after the client observes EOF.
	var finalCommit func(LogRecord) error
	if s.onComplete != nil {
		finalCommit = func(record LogRecord) error {
			return s.onComplete(finalInfo, record)
		}
	}
	_ = s.Log.closeWithPublish(finalCommit, func(commitErr error) {
		if commitErr != nil {
			s.mu.Lock()
			failed := *s.exit
			message := "session log completion: " + commitErr.Error()
			if failed.Error == "" {
				failed.Error = message
			} else {
				failed.Error += "; " + message
			}
			s.exit = &failed
			s.mu.Unlock()
		}
		if onFinish != nil {
			onFinish()
		}
		close(s.exited)
	})
}

// ---------------------------------------------------------------------------
// Manager
// ---------------------------------------------------------------------------

// ManagerOptions configures per-session resources.
type ManagerOptions struct {
	SpillDir   string // where spill files live; "" disables spilling
	MemBytes   int
	SpillBytes int64
	MaxChunk   int
	MaxChunks  int
	// Retention keeps finished sessions (and their logs) for late attachers.
	Retention time.Duration
	// RetentionForTenant overrides Retention for one tenant. A non-positive
	// result uses Retention. The callback must not call back into Manager.
	RetentionForTenant func(tenant string) time.Duration
	// BlobStoreForTenant returns a tenant-scoped artifact view. When set,
	// CommitLogRecord is required and receives every complete replacement of
	// the session's remote segment references. Both callbacks run outside any
	// BlobStore operation but under the individual Log lock; they must not call
	// back into that Log.
	BlobStoreForTenant func(tenant string) (artifact.BlobStore, error)
	CommitLogRecord    func(sessionID, tenant string, record LogRecord) error
	// BlobStoreForSession permits workspace/generation authorization on every
	// blob operation. It takes precedence over BlobStoreForTenant.
	BlobStoreForSession func(tenant, workspace string) (artifact.BlobStore, error)
	// CommitSessionLogRecord receives the immutable session identity together
	// with each monotonic segment replacement. CompleteSessionLogRecord runs
	// after the terminal segment is sealed and before retention begins.
	CommitSessionLogRecord   func(sessionID string, spec Spec, record LogRecord) error
	CompleteSessionLogRecord func(sessionID string, spec Spec, info proto.SessionInfo, exit proto.ExitInfo, record LogRecord) error
	OnRecordError            func(sessionID string, err error)
	SegmentBytes             int64
	MaxLogSegments           int
	// MaxSessions includes retained exited sessions; MaxActive bounds processes
	// and port connections; per-workspace and per-principal limits prevent one
	// tenant actor from consuming the node-wide retained-session budget. Zero
	// selects conservative defaults of 1024, 256, 64, and 128.
	MaxSessions             int
	MaxActive               int
	MaxSessionsPerWorkspace int
	MaxSessionsPerPrincipal int
	// OnExit is called after a session's exit record is written.
	OnExit func(s *Session, info proto.ExitInfo)
}

// Manager owns all sessions on a node.
type Manager struct {
	opts ManagerOptions

	mu          sync.Mutex
	sessions    map[string]*Session
	byIdem      map[string]idemSession // idempotency key -> session id + request fingerprint
	byWS        map[string]int
	byPrincipal map[string]int
	active      int
	closed      bool
	observers   sync.WaitGroup
}

// ManagerStats is a point-in-time session-capacity snapshot.
type ManagerStats struct {
	Sessions                int
	Active                  int
	MaxSessions             int
	MaxActive               int
	MaxSessionsPerWorkspace int
	MaxSessionsPerPrincipal int
}

type idemSession struct {
	id          string
	fingerprint [32]byte
}

// NewManager creates a Manager.
func NewManager(opts ManagerOptions) *Manager {
	if opts.Retention == 0 {
		opts.Retention = 24 * time.Hour
	}
	if opts.SpillDir != "" {
		_ = os.MkdirAll(opts.SpillDir, 0o700)
	}
	if opts.MaxSessions <= 0 {
		opts.MaxSessions = 1024
	}
	if opts.MaxActive <= 0 {
		opts.MaxActive = 256
	}
	if opts.MaxSessionsPerWorkspace <= 0 {
		opts.MaxSessionsPerWorkspace = 64
	}
	if opts.MaxSessionsPerPrincipal <= 0 {
		opts.MaxSessionsPerPrincipal = 128
	}
	return &Manager{
		opts: opts, sessions: map[string]*Session{}, byIdem: map[string]idemSession{},
		byWS: map[string]int{}, byPrincipal: map[string]int{},
	}
}

// Stats reports current retained and active session usage.
func (m *Manager) Stats() ManagerStats {
	m.mu.Lock()
	defer m.mu.Unlock()
	return ManagerStats{
		Sessions: len(m.sessions), Active: m.active,
		MaxSessions: m.opts.MaxSessions, MaxActive: m.opts.MaxActive,
		MaxSessionsPerWorkspace: m.opts.MaxSessionsPerWorkspace,
		MaxSessionsPerPrincipal: m.opts.MaxSessionsPerPrincipal,
	}
}

// Get returns a session by id.
func (m *Manager) Get(id string) (*Session, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	s, ok := m.sessions[id]
	return s, ok
}

// List returns sessions, optionally filtered by workspace, sorted by id.
func (m *Manager) List(ws string) []*Session {
	m.mu.Lock()
	defer m.mu.Unlock()
	var out []*Session
	for _, s := range m.sessions {
		if ws == "" || s.WS == ws {
			out = append(out, s)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out
}

// Remove forgets a finished session and releases its log. Running sessions
// are killed first when kill is true; otherwise they are left alone and
// Remove returns false.
func (m *Manager) Remove(id string, kill bool) bool {
	m.mu.Lock()
	s, ok := m.sessions[id]
	if !ok {
		m.mu.Unlock()
		return false
	}
	if !s.Exited() {
		if !kill {
			m.mu.Unlock()
			return false
		}
		m.mu.Unlock()
		// Open publishes the Session before starting its process so idempotent
		// retries can find it. Wait for that short startup window before killing;
		// otherwise Close can miss a not-yet-installed process handle and leak it.
		<-s.startDone
		if !s.Exited() {
			s.Kill()
		}
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		_, waitErr := s.Wait(ctx)
		cancel()
		if waitErr != nil {
			return false
		}
		m.mu.Lock()
	}
	if current := m.sessions[id]; current != s {
		m.mu.Unlock()
		return false
	}
	// Capacity and the session record remain authoritative until remote
	// references are durably released. Forget runs while the manager lock keeps
	// Get/Open from observing a half-removed retained session.
	if err := s.Log.Forget(); err != nil {
		m.mu.Unlock()
		return false
	}
	delete(m.sessions, id)
	if m.byWS[s.WS] <= 1 {
		delete(m.byWS, s.WS)
	} else {
		m.byWS[s.WS]--
	}
	principalKey := sessionPrincipalKey(s.Tenant, s.Principal)
	if m.byPrincipal[principalKey] <= 1 {
		delete(m.byPrincipal, principalKey)
	} else {
		m.byPrincipal[principalKey]--
	}
	for k, v := range m.byIdem {
		if v.id == id {
			delete(m.byIdem, k)
		}
	}
	m.mu.Unlock()
	return true
}

// Terminate ends one live session with a reason and keeps its log so
// subscribers observe the exit chunk. It reports whether the session existed.
func (m *Manager) Terminate(id, reason string) bool {
	m.mu.Lock()
	s, ok := m.sessions[id]
	m.mu.Unlock()
	if !ok {
		return false
	}
	<-s.startDone
	if !s.Exited() {
		s.Terminate(reason)
	}
	return true
}

// KillWorkspace terminates every session of a workspace and forgets it. An
// error means at least one managed process could not be confirmed stopped, so
// callers must not describe a following snapshot as quiesced.
func (m *Manager) KillWorkspace(ws string) error {
	var failed []string
	for _, s := range m.List(ws) {
		if !m.Remove(s.ID, true) {
			failed = append(failed, s.ID)
		}
	}
	if len(failed) != 0 {
		return fmt.Errorf("sessions did not stop within the deadline: %s", strings.Join(failed, ", "))
	}
	return nil
}

// TerminateWorkspace joins every producer but retains completed logs for late
// attach and cross-node replay until the configured retention deadline.
func (m *Manager) TerminateWorkspace(ws, reason string) error {
	var failed []string
	for _, s := range m.List(ws) {
		<-s.startDone
		if !s.Exited() {
			s.Terminate(reason)
		}
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		_, err := s.Wait(ctx)
		if err == nil && s.observed != nil {
			select {
			case <-s.observed:
			case <-ctx.Done():
				err = ctx.Err()
			}
		}
		cancel()
		if err != nil {
			failed = append(failed, s.ID)
		}
	}
	if len(failed) != 0 {
		return fmt.Errorf("sessions did not stop within the deadline: %s", strings.Join(failed, ", "))
	}
	return nil
}

// Close kills everything.
func (m *Manager) Close() {
	m.mu.Lock()
	m.closed = true
	all := make([]*Session, 0, len(m.sessions))
	for _, s := range m.sessions {
		all = append(all, s)
	}
	m.mu.Unlock()
	for _, s := range all {
		m.Remove(s.ID, true)
	}
	m.mu.Lock()
	remaining := make([]*Session, 0, len(m.sessions))
	for _, s := range m.sessions {
		remaining = append(remaining, s)
	}
	m.mu.Unlock()
	for _, s := range remaining {
		_ = s.Log.closeLocal()
	}
	// Bounded for the same reason as Shutdown: a producer that will not stop
	// must not turn Close into a hang.
	_ = m.joinObservers()
}

// observerJoinTimeout bounds how long a shutdown waits for output producers
// to stop. The per-session Wait already has its own bound; this one covers the
// pumps, which block in a read on a live child's descriptor and therefore stop
// only when that child does. Without a bound here a process that ignores
// termination pins node shutdown forever, and the symptom is a node that never
// exits rather than one that reports what it could not join.
var observerJoinTimeout = 10 * time.Second

// observerJoinTimeoutForTest shortens the bound and returns a restore func, so
// a test can prove the bound is honoured without spending the production one.
func observerJoinTimeoutForTest(d time.Duration) func() {
	previous := observerJoinTimeout
	observerJoinTimeout = d
	return func() { observerJoinTimeout = previous }
}

// joinObservers waits for the producer group, bounded. It reports whether
// every producer stopped; false means shutdown proceeded while at least one
// was still running, which the caller must be able to say out loud.
func (m *Manager) joinObservers() bool {
	done := make(chan struct{})
	go func() {
		m.observers.Wait()
		close(done)
	}()
	timer := time.NewTimer(observerJoinTimeout)
	defer timer.Stop()
	select {
	case <-done:
		return true
	case <-timer.C:
		return false
	}
}

// Shutdown joins live producers while preserving durable log references. It
// is used for node process shutdown; retention remains control-plane owned.
//
// It returns false when a producer did not stop within the bound, so the
// caller reports an unclean shutdown instead of blocking on it.
func (m *Manager) Shutdown() bool {
	m.mu.Lock()
	m.closed = true
	all := make([]*Session, 0, len(m.sessions))
	for _, s := range m.sessions {
		all = append(all, s)
	}
	m.mu.Unlock()
	for _, s := range all {
		<-s.startDone
		if !s.Exited() {
			s.Terminate("node shutdown")
		}
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		_, _ = s.Wait(ctx)
		cancel()
	}
	clean := m.joinObservers()
	for _, s := range all {
		_ = s.Log.closeLocal()
	}
	return clean
}

// Open starts a session. If spec.IdempotencyKey names an existing session it
// is returned instead of starting a second process.
func (m *Manager) Open(spec Spec) (*Session, error) {
	s, _, err := m.OpenOrReplay(spec)
	return s, err
}

// OpenOrReplay is Open that also reports whether a new session was started
// (created=true) or an idempotent replay returned an existing one.
func (m *Manager) OpenOrReplay(spec Spec) (s *Session, created bool, err error) {
	fingerprint := sessionFingerprint(spec)
	m.mu.Lock()
	if m.closed {
		m.mu.Unlock()
		return nil, false, proto.Err(proto.CodeClosed, "manager closed")
	}
	if spec.IdempotencyKey != "" {
		if existing, ok := m.byIdem[spec.IdempotencyKey]; ok {
			if existing.fingerprint != fingerprint {
				m.mu.Unlock()
				return nil, false, proto.Err(proto.CodeConflict, "idempotency key was reused with different session arguments")
			}
			s := m.sessions[existing.id]
			m.mu.Unlock()
			return s, false, nil
		}
	}
	if len(m.sessions) >= m.opts.MaxSessions {
		m.mu.Unlock()
		metrics.SessionQuotaRejected.Inc()
		return nil, false, proto.Err(proto.CodeResourceExhausted, "node session limit %d reached", m.opts.MaxSessions)
	}
	if m.byWS[spec.WS] >= m.opts.MaxSessionsPerWorkspace {
		m.mu.Unlock()
		metrics.SessionQuotaRejected.Inc()
		return nil, false, proto.Err(proto.CodeResourceExhausted, "workspace session limit %d reached", m.opts.MaxSessionsPerWorkspace)
	}
	principalKey := sessionPrincipalKey(spec.Tenant, spec.Principal)
	if m.byPrincipal[principalKey] >= m.opts.MaxSessionsPerPrincipal {
		m.mu.Unlock()
		metrics.SessionQuotaRejected.Inc()
		return nil, false, proto.Err(proto.CodeResourceExhausted, "principal session limit %d reached", m.opts.MaxSessionsPerPrincipal)
	}
	if m.active >= m.opts.MaxActive {
		m.mu.Unlock()
		metrics.SessionQuotaRejected.Inc()
		return nil, false, proto.Err(proto.CodeResourceExhausted, "node active-session limit %d reached", m.opts.MaxActive)
	}
	id := ids.New("s")
	var spillPath string
	if m.opts.SpillDir != "" {
		spillPath = filepath.Join(m.opts.SpillDir, id+".log")
	}
	var blobStore artifact.BlobStore
	if m.opts.BlobStoreForSession != nil {
		blobStore, err = m.opts.BlobStoreForSession(spec.Tenant, spec.WS)
		if err != nil {
			if m.opts.OnRecordError != nil {
				m.opts.OnRecordError(id, fmt.Errorf("session artifact store: %w", err))
			}
			blobStore, err = nil, nil
		}
	} else if m.opts.BlobStoreForTenant != nil {
		blobStore, err = m.opts.BlobStoreForTenant(spec.Tenant)
	}
	if blobStore != nil || (m.opts.BlobStoreForTenant != nil && m.opts.BlobStoreForSession == nil) {
		if err != nil {
			m.mu.Unlock()
			return nil, false, fmt.Errorf("session artifact store for tenant: %w", err)
		}
		if m.opts.CommitSessionLogRecord == nil && m.opts.CommitLogRecord == nil {
			m.mu.Unlock()
			return nil, false, errors.New("session: CommitLogRecord required with BlobStoreForTenant")
		}
	}
	commitRecord := func(record LogRecord) error {
		if m.opts.CommitSessionLogRecord != nil {
			return m.opts.CommitSessionLogRecord(id, spec, record)
		}
		return m.opts.CommitLogRecord(id, spec.Tenant, record)
	}
	if blobStore == nil {
		commitRecord = nil
	}
	log, err := NewLog(LogOptions{
		MemBytes: m.opts.MemBytes, SpillBytes: m.opts.SpillBytes, SpillPath: spillPath,
		MaxChunk: m.opts.MaxChunk, MaxChunks: m.opts.MaxChunks, BlobStore: blobStore,
		SegmentBytes: m.opts.SegmentBytes, MaxSegments: m.opts.MaxLogSegments, CommitRecord: commitRecord,
	})
	if err != nil {
		m.mu.Unlock()
		return nil, false, err
	}
	s = &Session{
		ID: id, WS: spec.WS, Kind: spec.Kind, Log: log, exited: make(chan struct{}), observed: make(chan struct{}), startDone: make(chan struct{}), outputReady: make(chan struct{}), Principal: spec.Principal, Tenant: spec.Tenant,
		Info: proto.SessionInfo{ID: id, WS: spec.WS, Kind: spec.Kind, Program: spec.Program, OpenedAt: time.Now().UnixMilli(), Run: spec.Run},
	}
	s.onComplete = func(info proto.ExitInfo, record LogRecord) error {
		if m.opts.CompleteSessionLogRecord != nil {
			if err := m.opts.CompleteSessionLogRecord(s.ID, spec, s.Info, info, record); err != nil {
				if m.opts.OnRecordError != nil {
					m.opts.OnRecordError(s.ID, err)
				}
				return err
			}
		}
		return nil
	}
	s.onFinish = func() { m.markInactive(id, s) }
	m.sessions[id] = s
	m.byWS[spec.WS]++
	m.byPrincipal[principalKey]++
	m.active++
	metrics.SessionsOpened.Inc()
	if spec.IdempotencyKey != "" {
		m.byIdem[spec.IdempotencyKey] = idemSession{id: id, fingerprint: fingerprint}
	}
	m.mu.Unlock()
	m.observers.Add(1)
	go m.observe(s)
	defer close(s.startDone)

	var startErr error
	switch {
	case spec.Runner != nil:
		startErr = s.startRunner(spec)
	case spec.Kind == proto.SessionExec:
		startErr = s.startExec(spec)
	case spec.Kind == proto.SessionPTY:
		startErr = s.startPTY(spec)
	case spec.Kind == proto.SessionPort:
		startErr = s.startPort(spec)
	case spec.Kind == proto.SessionACP:
		// Record-only: the agent runner appends protocol frames and ends it.
	default:
		startErr = proto.Err(proto.CodeBadRequest, "unknown session kind %q", spec.Kind)
	}
	// The info chunk is always seq 0 so a replay from 0 reconstructs the
	// session header; it is written after start so PID is known.
	_, infoErr := s.Log.Append(proto.StreamInfo, proto.MustMarshal(s.Info))
	if infoErr != nil && startErr == nil {
		startErr = fmt.Errorf("session info log: %w", infoErr)
	}
	if startErr != nil {
		close(s.outputReady)
		// A process may already be running when committing the mandatory info
		// record fails. It must not outlive the terminal session record.
		if infoErr != nil {
			_ = s.Signal("KILL")
		}
		s.finish(proto.ExitInfo{Code: -1, Error: startErr.Error()})
		return s, true, nil // the session exists; its log says why it failed
	}
	if spec.Timeout > 0 {
		s.mu.Lock()
		s.timeout = time.AfterFunc(spec.Timeout, func() {
			_ = s.Signal("KILL")
		})
		s.mu.Unlock()
	}
	close(s.outputReady)
	return s, true, nil
}

func sessionPrincipalKey(tenant, principal string) string {
	// Treat authenticator output as opaque: a custom authenticator is allowed
	// to use any string, including delimiter characters. Deterministic CBOR
	// length-prefixes both values, and the digest keeps the accounting key
	// fixed-size without introducing concatenation aliases.
	sum := sha256.Sum256(proto.MustMarshal(struct {
		Tenant    string
		Principal string
	}{tenant, principal}))
	return string(sum[:])
}

func sessionFingerprint(spec Spec) [32]byte {
	copySpec := spec
	copySpec.IdempotencyKey = ""
	copySpec.Runner = nil
	return sha256.Sum256(proto.MustMarshal(copySpec))
}

func (m *Manager) observe(s *Session) {
	defer m.observers.Done()
	defer close(s.observed)
	<-s.exited
	info := *s.ExitInfo()
	if m.opts.OnExit != nil {
		m.opts.OnExit(s, info)
	}
	m.scheduleReap(s)
}

// RestoreArchived installs a complete remote-only session record for attach
// after a node restart or workspace handoff. It consumes retained-session
// capacity but never active-process capacity.
func (m *Manager) RestoreArchived(record proto.SessionLogRecord, store artifact.BlobStore) (*Session, error) {
	if !record.Complete || record.Session == "" || record.Workspace == "" || record.Principal == "" || record.Info.ID != record.Session || record.Info.WS != record.Workspace {
		return nil, errors.New("session: incomplete archived record")
	}
	segments := make([]SegmentRef, len(record.Segments))
	for i, segment := range record.Segments {
		segments[i] = SegmentRef{First: segment.First, Next: segment.Next, Artifact: segment.Artifact, Bytes: segment.Bytes}
	}
	log, err := OpenArchivedLog(store, LogRecord{Version: logRecordVersion, MaxChunk: record.MaxChunk, Segments: segments})
	if err != nil {
		return nil, err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if existing := m.sessions[record.Session]; existing != nil {
		return existing, nil
	}
	if m.closed || len(m.sessions) >= m.opts.MaxSessions || m.byWS[record.Workspace] >= m.opts.MaxSessionsPerWorkspace || m.byPrincipal[sessionPrincipalKey(record.Tenant, record.Principal)] >= m.opts.MaxSessionsPerPrincipal {
		return nil, proto.Err(proto.CodeResourceExhausted, "retained session capacity is exhausted")
	}
	exited := make(chan struct{})
	close(exited)
	started := make(chan struct{})
	close(started)
	outputReady := make(chan struct{})
	close(outputReady)
	exit := record.Exit
	s := &Session{
		ID: record.Session, WS: record.Workspace, Kind: record.Kind, Info: record.Info,
		Log: log, Principal: record.Principal, Tenant: record.Tenant, exit: &exit,
		exited: exited, observed: exited, startDone: started, outputReady: outputReady,
	}
	m.sessions[s.ID] = s
	m.byWS[s.WS]++
	m.byPrincipal[sessionPrincipalKey(s.Tenant, s.Principal)]++
	if record.ExpiresAt > 0 {
		m.scheduleReapAt(s, time.UnixMilli(record.ExpiresAt))
	} else {
		m.scheduleReap(s)
	}
	return s, nil
}

func (m *Manager) markInactive(id string, s *Session) {
	m.mu.Lock()
	if current := m.sessions[id]; current == s && m.active > 0 {
		m.active--
	}
	m.mu.Unlock()
}

func (m *Manager) scheduleReap(s *Session) {
	retention := m.opts.Retention
	if m.opts.RetentionForTenant != nil {
		if configured := m.opts.RetentionForTenant(s.Tenant); configured > 0 {
			retention = configured
		}
	}
	m.scheduleReapAt(s, time.Now().Add(retention))

}

func (m *Manager) scheduleReapAt(s *Session, at time.Time) {
	delay := time.Until(at)
	if delay < 0 {
		delay = 0
	}
	time.AfterFunc(delay, func() { m.reap(s.ID) })
}

func (m *Manager) reap(id string) {
	if m.Remove(id, false) {
		return
	}
	// A durable reference-release failure keeps the session and its quota
	// reservation. Retry at a bounded cadence rather than pretending retention
	// completed or spinning against an unavailable record store.
	m.mu.Lock()
	s, ok := m.sessions[id]
	m.mu.Unlock()
	if ok && s.Exited() {
		time.AfterFunc(time.Minute, func() { m.reap(id) })
	}
}

// ---- runners ----

func (s *Session) startRunner(spec Spec) error {
	if spec.Kind != proto.SessionExec && spec.Kind != proto.SessionPTY && spec.Kind != proto.SessionPort {
		return proto.Err(proto.CodeUnsupported, "backend runner does not support session kind %q", spec.Kind)
	}
	running, err := spec.Runner.Start(spec)
	if err != nil {
		return err
	}
	if running == nil {
		return errors.New("session: backend runner returned nil")
	}
	s.mu.Lock()
	s.running = running
	s.stdin = running.Stdin()
	s.Info.PID = running.PID()
	s.mu.Unlock()

	var pumps sync.WaitGroup
	pumpStream := func(stream uint8, reader io.Reader) {
		if reader == nil {
			return
		}
		pumps.Add(1)
		go func() {
			defer pumps.Done()
			<-s.outputReady
			s.recordLogError(pump(s.Log, stream, reader))
		}()
	}
	pumpStream(proto.StreamStdout, running.Stdout())
	pumpStream(proto.StreamStderr, running.Stderr())
	go func() {
		info := running.Wait()
		pumps.Wait()
		s.finish(info)
	}()
	return nil
}

func (s *Session) startExec(spec Spec) error {
	if len(spec.Program) == 0 {
		return proto.Err(proto.CodeBadRequest, "empty program")
	}
	cmd := exec.Command(spec.Program[0], spec.Program[1:]...)
	cmd.Dir = spec.Cwd
	cmd.Env = spec.Env
	configureProcessGroup(cmd)
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return err
	}
	stderr, err := cmd.StderrPipe()
	if err != nil {
		return err
	}
	if spec.Stdin {
		w, err := cmd.StdinPipe()
		if err != nil {
			return err
		}
		s.stdin = w
	}
	if err := cmd.Start(); err != nil {
		return err
	}
	if err := registerProcessGroup(cmd); err != nil {
		_ = cmd.Process.Kill()
		_ = cmd.Wait()
		return fmt.Errorf("contain process tree: %w", err)
	}
	s.mu.Lock()
	s.cmd = cmd
	s.Info.PID = cmd.Process.Pid
	s.mu.Unlock()
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		<-s.outputReady
		s.recordLogError(pump(s.Log, proto.StreamStdout, stdout))
	}()
	go func() {
		defer wg.Done()
		<-s.outputReady
		s.recordLogError(pump(s.Log, proto.StreamStderr, stderr))
	}()
	go func() {
		wg.Wait()
		err := cmd.Wait()
		releaseProcessGroup(cmd)
		s.finish(exitInfo(err, cmd))
	}()
	return nil
}

func (s *Session) startPTY(spec Spec) error {
	if len(spec.Program) == 0 {
		return proto.Err(proto.CodeBadRequest, "empty program")
	}
	cmd := exec.Command(spec.Program[0], spec.Program[1:]...)
	cmd.Dir = spec.Cwd
	cmd.Env = append(spec.Env, "TERM=xterm-256color")
	rows, cols := spec.Rows, spec.Cols
	if rows == 0 {
		rows = 24
	}
	if cols == 0 {
		cols = 80
	}
	ptmx, err := pty.StartWithSize(cmd, &pty.Winsize{Rows: rows, Cols: cols})
	if err != nil {
		return err
	}
	if err := registerProcessGroup(cmd); err != nil {
		_ = ptmx.Close()
		_ = cmd.Process.Kill()
		_ = cmd.Wait()
		return fmt.Errorf("contain process tree: %w", err)
	}
	s.mu.Lock()
	s.cmd = cmd
	s.ptmx = ptmx
	s.stdin = ptmx
	s.Info.PID = cmd.Process.Pid
	s.mu.Unlock()
	go func() {
		<-s.outputReady
		logErr := pump(s.Log, proto.StreamStdout, ptmx)
		// Linux PTY masters report EIO, rather than EOF, after the slave has
		// closed. Treat that terminal condition as a cleanly drained stream.
		if isPTYEOF(logErr) {
			logErr = nil
		}
		s.recordLogError(logErr)
		err := cmd.Wait()
		releaseProcessGroup(cmd)
		_ = ptmx.Close()
		s.finish(exitInfo(err, cmd))
	}()
	return nil
}

func (s *Session) startPort(spec Spec) error {
	if spec.Port < 1 || spec.Port > 65535 {
		return proto.Err(proto.CodeBadRequest, "port must be between 1 and 65535")
	}
	host := spec.Host
	if host == "" {
		host = "127.0.0.1"
	}
	d := net.Dialer{Timeout: 10 * time.Second}
	conn, err := d.Dial("tcp", net.JoinHostPort(host, fmt.Sprint(spec.Port)))
	if err != nil {
		return err
	}
	s.mu.Lock()
	s.conn = conn
	s.stdin = conn
	s.mu.Unlock()
	go func() {
		<-s.outputReady
		s.recordLogError(pump(s.Log, proto.StreamStdout, conn))
		_ = conn.Close()
		s.finish(proto.ExitInfo{Code: 0})
	}()
	return nil
}

// pump copies r into the log until EOF.
func pump(l *Log, stream uint8, r io.Reader) error {
	buf := make([]byte, 32<<10)
	for {
		n, err := r.Read(buf)
		if n > 0 {
			if _, aerr := l.Append(stream, buf[:n]); aerr != nil {
				return aerr
			}
		}
		if err != nil {
			if errors.Is(err, io.EOF) {
				return nil
			}
			return err
		}
	}
}

func (s *Session) recordLogError(err error) {
	if err == nil {
		return
	}
	s.mu.Lock()
	if s.logErr == nil {
		s.logErr = fmt.Errorf("session log: %w", err)
	}
	s.mu.Unlock()
	// Stop the producer: continuing after storage failure can block forever
	// once the kernel pipe fills and would hide the terminal error.
	_ = s.Signal("KILL")
}

func exitInfo(err error, cmd *exec.Cmd) proto.ExitInfo {
	if err == nil {
		return proto.ExitInfo{Code: 0}
	}
	var ee *exec.ExitError
	if errors.As(err, &ee) {
		info := proto.ExitInfo{Code: ee.ExitCode()}
		if signal, code, ok := platformExitSignal(ee); ok {
			info.Signal = signal
			info.Code = code
		}
		return info
	}
	return proto.ExitInfo{Code: -1, Error: err.Error()}
}
