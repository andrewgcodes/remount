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

	"remount.dev/remount/internal/ids"
	"remount.dev/remount/internal/metrics"
	"remount.dev/remount/internal/proto"
)

// Spec describes what to run.
type Spec struct {
	WS      string
	Kind    string // proto.SessionExec | SessionPTY | SessionPort
	Program []string
	Cwd     string
	Env     []string // full environment, KEY=VALUE
	Rows    uint16
	Cols    uint16
	Stdin   bool // exec: keep stdin open
	Timeout time.Duration
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

	mu          sync.Mutex
	inputMu     sync.Mutex     // serializes writes without blocking lifecycle reads
	stdin       io.WriteCloser // exec: pipe; pty: the pty master; port: the conn
	ptmx        *os.File
	cmd         *exec.Cmd
	conn        net.Conn
	lastISeq    uint64
	exit        *proto.ExitInfo
	exited      chan struct{}
	startDone   chan struct{} // closed once process/connection startup has resolved
	timeout     *time.Timer
	outputReady chan struct{} // pumps wait until StreamInfo is committed at seq 0
	logErr      error
	onFinish    func() // commits manager accounting before exited is closed
	closeReason string // recorded in the exit chunk when the node ends the session
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
	s.mu.Unlock()
	if stdin == nil {
		return proto.Err(proto.CodeUnsupported, "session has no stdin")
	}
	if len(data) > 0 {
		if err := writeFull(stdin, data); err != nil {
			return proto.Err(proto.CodeClosed, "stdin: %v", err)
		}
	}
	if eof {
		if kind == proto.SessionExec {
			if err := stdin.Close(); err != nil {
				return proto.Err(proto.CodeClosed, "stdin close: %v", err)
			}
		} else if kind == proto.SessionPort {
			if cw, ok := conn.(interface{ CloseWrite() error }); ok {
				if err := cw.CloseWrite(); err != nil {
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
	return nil
}

func writeFull(w io.Writer, data []byte) error {
	for len(data) > 0 {
		n, err := w.Write(data)
		if err != nil {
			return err
		}
		if n == 0 {
			return io.ErrShortWrite
		}
		data = data[n:]
	}
	return nil
}

// LastInputSeq is the last input sequence durably handed to the process.
func (s *Session) LastInputSeq() uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.lastISeq
}

// Resize changes the pty window.
func (s *Session) Resize(rows, cols uint16) error {
	s.mu.Lock()
	defer s.mu.Unlock()
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
	if s.Kind == proto.SessionPort {
		if s.conn != nil {
			return s.conn.Close()
		}
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
	if _, err := s.Log.Append(proto.StreamExit, proto.MustMarshal(info)); err != nil {
		s.mu.Lock()
		if s.exit != nil && s.exit.Error == "" {
			s.exit.Error = "session log: " + err.Error()
		}
		s.mu.Unlock()
	}
	_ = s.Log.Close()
	// A successful Wait must mean that the active-session admission slot is
	// reusable. Commit manager accounting before publishing session exit; doing
	// this in an observer after close(s.exited) left a scheduler-sized window in
	// which a completed process could still spuriously exhaust active capacity.
	if onFinish != nil {
		onFinish()
	}
	close(s.exited)
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
	s.Log.Release()
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
	log, err := NewLog(LogOptions{
		MemBytes: m.opts.MemBytes, SpillBytes: m.opts.SpillBytes, SpillPath: spillPath,
		MaxChunk: m.opts.MaxChunk, MaxChunks: m.opts.MaxChunks,
	})
	if err != nil {
		m.mu.Unlock()
		return nil, false, err
	}
	s = &Session{
		ID: id, WS: spec.WS, Kind: spec.Kind, Log: log, exited: make(chan struct{}), startDone: make(chan struct{}), outputReady: make(chan struct{}), Principal: spec.Principal, Tenant: spec.Tenant,
		Info: proto.SessionInfo{ID: id, WS: spec.WS, Kind: spec.Kind, Program: spec.Program, OpenedAt: time.Now().UnixMilli(), Run: spec.Run},
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
	go m.observe(s)
	defer close(s.startDone)

	var startErr error
	switch spec.Kind {
	case proto.SessionExec:
		startErr = s.startExec(spec)
	case proto.SessionPTY:
		startErr = s.startPTY(spec)
	case proto.SessionPort:
		startErr = s.startPort(spec)
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
	return sha256.Sum256(proto.MustMarshal(copySpec))
}

func (m *Manager) observe(s *Session) {
	<-s.exited
	if m.opts.OnExit != nil {
		m.opts.OnExit(s, *s.ExitInfo())
	}
	m.scheduleReap(s)
}

func (m *Manager) markInactive(id string, s *Session) {
	m.mu.Lock()
	if current := m.sessions[id]; current == s && m.active > 0 {
		m.active--
	}
	m.mu.Unlock()
}

func (m *Manager) scheduleReap(s *Session) {
	time.AfterFunc(m.opts.Retention, func() { m.Remove(s.ID, false) })
}

// ---- runners ----

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
	s.mu.Lock()
	s.cmd = cmd
	s.ptmx = ptmx
	s.stdin = ptmx
	s.Info.PID = cmd.Process.Pid
	s.mu.Unlock()
	go func() {
		<-s.outputReady
		s.recordLogError(pump(s.Log, proto.StreamStdout, ptmx))
		err := cmd.Wait()
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
