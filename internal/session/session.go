package session

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"sync"
	"syscall"
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
}

// Session is a running or finished process with its output log.
type Session struct {
	ID        string
	WS        string
	Kind      string
	Info      proto.SessionInfo
	Log       *Log
	Principal string

	mu       sync.Mutex
	stdin    io.WriteCloser // exec: pipe; pty: the pty master; port: the conn
	ptmx     *os.File
	cmd      *exec.Cmd
	conn     net.Conn
	lastISeq uint64
	exit     *proto.ExitInfo
	exited   chan struct{}
	timeout  *time.Timer
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
	s.mu.Lock()
	defer s.mu.Unlock()
	if iseq != 0 {
		if iseq <= s.lastISeq {
			metrics.InputsDropped.Inc()
			return nil // duplicate delivery
		}
		s.lastISeq = iseq
	}
	if s.stdin == nil {
		return proto.Err(proto.CodeUnsupported, "session has no stdin")
	}
	if len(data) > 0 {
		if _, err := s.stdin.Write(data); err != nil {
			return proto.Err(proto.CodeClosed, "stdin: %v", err)
		}
	}
	if eof {
		if s.Kind == proto.SessionExec {
			_ = s.stdin.Close()
		} else if s.Kind == proto.SessionPort {
			if cw, ok := s.conn.(interface{ CloseWrite() error }); ok {
				_ = cw.CloseWrite()
			}
		}
	}
	return nil
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
	if s.Kind == proto.SessionPort {
		if s.conn != nil {
			return s.conn.Close()
		}
		return nil
	}
	if s.cmd == nil || s.cmd.Process == nil {
		return proto.Err(proto.CodeNotFound, "no process")
	}
	sig, ok := signals[name]
	if !ok {
		return proto.Err(proto.CodeBadRequest, "unknown signal %q", name)
	}
	if s.cmd.SysProcAttr != nil && s.cmd.SysProcAttr.Setpgid {
		// Signal the whole process group so pipelines and children die too.
		return syscall.Kill(-s.cmd.Process.Pid, sig)
	}
	return s.cmd.Process.Signal(sig)
}

var signals = map[string]syscall.Signal{
	"TERM": syscall.SIGTERM, "KILL": syscall.SIGKILL, "INT": syscall.SIGINT,
	"HUP": syscall.SIGHUP, "QUIT": syscall.SIGQUIT, "USR1": syscall.SIGUSR1, "USR2": syscall.SIGUSR2,
}

// Kill terminates the process immediately.
func (s *Session) Kill() {
	_ = s.Signal("KILL")
}

func (s *Session) finish(info proto.ExitInfo) {
	s.mu.Lock()
	if s.exit != nil {
		s.mu.Unlock()
		return
	}
	s.exit = &info
	if s.timeout != nil {
		s.timeout.Stop()
	}
	s.mu.Unlock()
	metrics.SessionsExited.Inc()
	_, _ = s.Log.Append(proto.StreamExit, proto.MustMarshal(info))
	_ = s.Log.Close()
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
	// Retention keeps finished sessions (and their logs) for late attachers.
	Retention time.Duration
	// OnExit is called after a session's exit record is written.
	OnExit func(s *Session, info proto.ExitInfo)
}

// Manager owns all sessions on a node.
type Manager struct {
	opts ManagerOptions

	mu       sync.Mutex
	sessions map[string]*Session
	byIdem   map[string]string // idempotency key -> session id
	closed   bool
}

// NewManager creates a Manager.
func NewManager(opts ManagerOptions) *Manager {
	if opts.Retention == 0 {
		opts.Retention = 24 * time.Hour
	}
	if opts.SpillDir != "" {
		_ = os.MkdirAll(opts.SpillDir, 0o700)
	}
	return &Manager{opts: opts, sessions: map[string]*Session{}, byIdem: map[string]string{}}
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
		s.Kill()
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		_, _ = s.Wait(ctx)
		cancel()
		m.mu.Lock()
	}
	delete(m.sessions, id)
	for k, v := range m.byIdem {
		if v == id {
			delete(m.byIdem, k)
		}
	}
	m.mu.Unlock()
	s.Log.Release()
	return true
}

// KillWorkspace terminates every session of a workspace and forgets them.
func (m *Manager) KillWorkspace(ws string) {
	for _, s := range m.List(ws) {
		m.Remove(s.ID, true)
	}
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
	m.mu.Lock()
	if m.closed {
		m.mu.Unlock()
		return nil, proto.Err(proto.CodeClosed, "manager closed")
	}
	if spec.IdempotencyKey != "" {
		if id, ok := m.byIdem[spec.IdempotencyKey]; ok {
			s := m.sessions[id]
			m.mu.Unlock()
			return s, nil
		}
	}
	id := ids.New("s")
	var spillPath string
	if m.opts.SpillDir != "" {
		spillPath = filepath.Join(m.opts.SpillDir, id+".log")
	}
	log, err := NewLog(LogOptions{MemBytes: m.opts.MemBytes, SpillBytes: m.opts.SpillBytes, SpillPath: spillPath})
	if err != nil {
		m.mu.Unlock()
		return nil, err
	}
	s := &Session{
		ID: id, WS: spec.WS, Kind: spec.Kind, Log: log, exited: make(chan struct{}), Principal: spec.Principal,
		Info: proto.SessionInfo{ID: id, WS: spec.WS, Kind: spec.Kind, Program: spec.Program, OpenedAt: time.Now().UnixMilli()},
	}
	m.sessions[id] = s
	metrics.SessionsOpened.Inc()
	if spec.IdempotencyKey != "" {
		m.byIdem[spec.IdempotencyKey] = id
	}
	m.mu.Unlock()

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
	_, _ = s.Log.Append(proto.StreamInfo, proto.MustMarshal(s.Info))
	if startErr != nil {
		s.finish(proto.ExitInfo{Code: -1, Error: startErr.Error()})
		m.scheduleReap(s)
		return s, nil // the session exists; its log says why it failed
	}
	if spec.Timeout > 0 {
		s.mu.Lock()
		s.timeout = time.AfterFunc(spec.Timeout, func() {
			_ = s.Signal("KILL")
		})
		s.mu.Unlock()
	}
	go func() {
		<-s.exited
		if m.opts.OnExit != nil {
			m.opts.OnExit(s, *s.ExitInfo())
		}
		m.scheduleReap(s)
	}()
	return s, nil
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
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
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
	go func() { defer wg.Done(); pump(s.Log, proto.StreamStdout, stdout) }()
	go func() { defer wg.Done(); pump(s.Log, proto.StreamStderr, stderr) }()
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
		pump(s.Log, proto.StreamStdout, ptmx)
		err := cmd.Wait()
		_ = ptmx.Close()
		s.finish(exitInfo(err, cmd))
	}()
	return nil
}

func (s *Session) startPort(spec Spec) error {
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
		pump(s.Log, proto.StreamStdout, conn)
		_ = conn.Close()
		s.finish(proto.ExitInfo{Code: 0})
	}()
	return nil
}

// pump copies r into the log until EOF.
func pump(l *Log, stream uint8, r io.Reader) {
	buf := make([]byte, 32<<10)
	for {
		n, err := r.Read(buf)
		if n > 0 {
			if _, aerr := l.Append(stream, buf[:n]); aerr != nil {
				return
			}
		}
		if err != nil {
			return
		}
	}
}

func exitInfo(err error, cmd *exec.Cmd) proto.ExitInfo {
	if err == nil {
		return proto.ExitInfo{Code: 0}
	}
	var ee *exec.ExitError
	if errors.As(err, &ee) {
		info := proto.ExitInfo{Code: ee.ExitCode()}
		if ws, ok := ee.Sys().(syscall.WaitStatus); ok && ws.Signaled() {
			info.Signal = ws.Signal().String()
			info.Code = 128 + int(ws.Signal())
		}
		return info
	}
	return proto.ExitInfo{Code: -1, Error: err.Error()}
}
