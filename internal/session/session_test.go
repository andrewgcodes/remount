package session

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"remount.dev/remount/internal/artifact"
	"remount.dev/remount/internal/proto"
)

type failAfterWriter struct {
	n int
}

func (w *failAfterWriter) Write(p []byte) (int, error) {
	if w.n <= 0 {
		return 0, io.ErrClosedPipe
	}
	if len(p) > w.n {
		p = p[:w.n]
	}
	w.n -= len(p)
	return len(p), nil
}

func (w *failAfterWriter) Close() error { return nil }

func newMgr(t *testing.T) *Manager {
	t.Helper()
	// Most tests exercise session semantics rather than admission. Keep their
	// fixture comfortably above the ordering stress-test count; quota behavior
	// has dedicated tests below with intentionally small limits.
	m := NewManager(ManagerOptions{
		SpillDir: t.TempDir(), MemBytes: 1 << 20, SpillBytes: 1 << 20, Retention: time.Hour,
		MaxSessions: 256, MaxActive: 256, MaxSessionsPerWorkspace: 256, MaxSessionsPerPrincipal: 256,
	})
	t.Cleanup(m.Close)
	return m
}

// collect drains a session log to exit and returns stdout, stderr, exit.
func collect(t *testing.T, s *Session) ([]byte, []byte, proto.ExitInfo) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	var out, errb []byte
	var exit proto.ExitInfo
	c := s.Log.CursorAt(0)
	sawInfo := false
	for {
		chunks, err := c.Next(ctx, 0)
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		for _, ch := range chunks {
			switch ch.Stream {
			case proto.StreamInfo:
				sawInfo = true
				if ch.Seq != 0 {
					t.Fatalf("info chunk must be seq 0, got %d", ch.Seq)
				}
			case proto.StreamStdout:
				out = append(out, ch.Data...)
			case proto.StreamStderr:
				errb = append(errb, ch.Data...)
			case proto.StreamExit:
				_ = proto.Unmarshal(ch.Data, &exit)
			}
		}
	}
	if !sawInfo {
		t.Fatal("no info chunk")
	}
	return out, errb, exit
}

func TestExecEchoAndExitCode(t *testing.T) {
	m := newMgr(t)
	s, err := m.Open(Spec{WS: "ws_1", Kind: proto.SessionExec, Program: []string{"sh", "-c", "echo out; echo err >&2; exit 3"}})
	if err != nil {
		t.Fatal(err)
	}
	out, errb, exit := collect(t, s)
	if string(out) != "out\n" || string(errb) != "err\n" || exit.Code != 3 {
		t.Fatalf("out=%q err=%q exit=%+v", out, errb, exit)
	}
	if !s.Exited() || s.ExitInfo().Code != 3 {
		t.Fatal("exit record missing")
	}
	// Late attacher still sees everything (exit-before-attach).
	out2, _, exit2 := collect(t, s)
	if string(out2) != "out\n" || exit2.Code != 3 {
		t.Fatal("replay after exit failed")
	}
}

func TestExecStdinAndIdempotentInput(t *testing.T) {
	m := newMgr(t)
	s, _ := m.Open(Spec{WS: "ws_1", Kind: proto.SessionExec, Program: []string{"cat"}, Stdin: true})
	if err := s.Input(1, []byte("hello "), false); err != nil {
		t.Fatal(err)
	}
	// Duplicate delivery of iseq 1 must be ignored.
	if err := s.Input(1, []byte("hello "), false); err != nil {
		t.Fatal(err)
	}
	if err := s.Input(2, []byte("world\n"), true); err != nil {
		t.Fatal(err)
	}
	out, _, exit := collect(t, s)
	if string(out) != "hello world\n" || exit.Code != 0 {
		t.Fatalf("%q %+v", out, exit)
	}
}

func TestExecStartFailureIsRecordedInLog(t *testing.T) {
	m := newMgr(t)
	s, err := m.Open(Spec{WS: "ws_1", Kind: proto.SessionExec, Program: []string{"/definitely/not/a/binary"}})
	if err != nil {
		t.Fatal(err)
	}
	_, _, exit := collect(t, s)
	if exit.Code != -1 || exit.Error == "" {
		t.Fatalf("%+v", exit)
	}
}

func TestStartFailureAlwaysCallsOnExit(t *testing.T) {
	exited := make(chan proto.ExitInfo, 1)
	m := NewManager(ManagerOptions{
		SpillDir: t.TempDir(), MemBytes: 1 << 20, SpillBytes: 1 << 20, Retention: time.Hour,
		OnExit: func(_ *Session, info proto.ExitInfo) { exited <- info },
	})
	t.Cleanup(m.Close)
	if _, err := m.Open(Spec{WS: "ws_1", Kind: proto.SessionExec, Program: []string{"/definitely/not/a/binary"}}); err != nil {
		t.Fatal(err)
	}
	select {
	case info := <-exited:
		if info.Code != -1 || info.Error == "" {
			t.Fatalf("unexpected exit: %+v", info)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("OnExit was not called for a synchronous start failure")
	}
}

func TestWaitBlocksUntilDurableCompletionRecordCommits(t *testing.T) {
	store, err := artifact.NewStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	completionStarted := make(chan struct{})
	allowCompletion := make(chan struct{})
	var release sync.Once
	t.Cleanup(func() { release.Do(func() { close(allowCompletion) }) })
	m := NewManager(ManagerOptions{
		SpillDir: t.TempDir(), MemBytes: 1 << 20, SpillBytes: 1 << 20, Retention: time.Hour,
		BlobStoreForTenant: func(string) (artifact.BlobStore, error) { return store, nil },
		CommitSessionLogRecord: func(string, Spec, LogRecord) error {
			return nil
		},
		CompleteSessionLogRecord: func(string, Spec, proto.SessionInfo, proto.ExitInfo, LogRecord) error {
			close(completionStarted)
			<-allowCompletion
			return nil
		},
	})
	t.Cleanup(m.Close)
	s, err := m.Open(Spec{
		WS: "ws_durable", Tenant: "tenant", Principal: "principal",
		Kind: proto.SessionExec, Program: []string{"sh", "-c", "printf durable"},
	})
	if err != nil {
		t.Fatal(err)
	}
	select {
	case <-completionStarted:
	case <-time.After(5 * time.Second):
		t.Fatal("durable completion callback did not start")
	}
	terminalObserved := make(chan struct{})
	cursorDone := make(chan error, 1)
	go func() {
		cursor := s.Log.CursorAt(0)
		terminal := false
		for {
			chunks, err := cursor.Next(context.Background(), 0)
			if errors.Is(err, io.EOF) {
				cursorDone <- nil
				return
			}
			if err != nil {
				cursorDone <- err
				return
			}
			for _, chunk := range chunks {
				if chunk.Stream == proto.StreamExit && !terminal {
					terminal = true
					close(terminalObserved)
				}
			}
		}
	}()
	waited := make(chan error, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_, err := s.Wait(ctx)
		waited <- err
	}()
	select {
	case err := <-waited:
		t.Fatalf("Wait returned before durable completion committed: %v", err)
	case <-time.After(50 * time.Millisecond):
	}
	select {
	case <-terminalObserved:
		t.Fatal("terminal chunk became observable before durable completion committed")
	case err := <-cursorDone:
		t.Fatalf("cursor reached EOF before durable completion committed: %v", err)
	case <-time.After(50 * time.Millisecond):
	}
	release.Do(func() { close(allowCompletion) })
	if err := <-waited; err != nil {
		t.Fatal(err)
	}
	select {
	case <-terminalObserved:
	case <-time.After(5 * time.Second):
		t.Fatal("terminal chunk was not published after durable completion committed")
	}
	select {
	case err := <-cursorDone:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("cursor did not reach EOF after durable completion committed")
	}
}

func TestDurableCompletionFailureIsObservableAndReplayStaysIncomplete(t *testing.T) {
	store, err := artifact.NewStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	commitErr := errors.New("complete record rejected")
	recordErrors := make(chan error, 1)
	var partial LogRecord
	var partialMu sync.Mutex
	m := NewManager(ManagerOptions{
		SpillDir: t.TempDir(), MemBytes: 1, SpillBytes: 1 << 20, MaxChunk: 16, SegmentBytes: 64,
		Retention:          time.Hour,
		BlobStoreForTenant: func(string) (artifact.BlobStore, error) { return store, nil },
		CommitSessionLogRecord: func(_ string, _ Spec, record LogRecord) error {
			partialMu.Lock()
			partial = record
			partialMu.Unlock()
			return nil
		},
		CompleteSessionLogRecord: func(string, Spec, proto.SessionInfo, proto.ExitInfo, LogRecord) error {
			return commitErr
		},
		OnRecordError: func(_ string, err error) {
			recordErrors <- err
		},
	})
	t.Cleanup(m.Close)
	s, err := m.Open(Spec{
		WS: "ws_durable_failure", Tenant: "tenant", Principal: "principal",
		Kind: proto.SessionExec, Program: []string{"sh", "-c", "head -c 1024 /dev/zero | tr '\\0' x"},
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	exit, err := s.Wait(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if exit == nil || !strings.Contains(exit.Error, commitErr.Error()) {
		t.Fatalf("exit = %+v, want durable completion failure", exit)
	}
	select {
	case err := <-recordErrors:
		if !errors.Is(err, commitErr) {
			t.Fatalf("record error = %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("OnRecordError did not receive the completion failure")
	}
	cursor := s.Log.CursorAt(0)
	for {
		_, err := cursor.Next(ctx, 0)
		if err == nil {
			continue
		}
		var incomplete *ErrIncompleteLog
		if !errors.As(err, &incomplete) || !strings.Contains(err.Error(), commitErr.Error()) {
			t.Fatalf("cursor error = %v, want incomplete durable log", err)
		}
		break
	}
	partialMu.Lock()
	record := partial
	partialMu.Unlock()
	if len(record.Segments) == 0 {
		t.Fatal("lower-tier durable segments were not preserved")
	}
	archived := proto.SessionLogRecord{
		Session: s.ID, Workspace: s.WS, Tenant: s.Tenant, Principal: s.Principal,
		Kind: s.Kind, Info: s.Info, Exit: *exit, MaxChunk: record.MaxChunk,
	}
	for _, segment := range record.Segments {
		archived.Segments = append(archived.Segments, proto.SessionLogSegment{
			First: segment.First, Next: segment.Next, Artifact: segment.Artifact, Bytes: segment.Bytes,
		})
	}
	if _, err := m.RestoreArchived(archived, store); err == nil || !strings.Contains(err.Error(), "incomplete archived record") {
		t.Fatalf("incomplete replay restore = %v", err)
	}
}

func TestImmediateOutputStillFollowsInfo(t *testing.T) {
	m := newMgr(t)
	for i := 0; i < 100; i++ {
		s, err := m.Open(Spec{WS: "ws_1", Kind: proto.SessionExec, Program: []string{"sh", "-c", "printf x"}})
		if err != nil {
			t.Fatal(err)
		}
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		chunks, err := s.Log.CursorAt(0).Next(ctx, 1)
		cancel()
		if err != nil {
			t.Fatal(err)
		}
		if len(chunks) != 1 || chunks[0].Seq != 0 || chunks[0].Stream != proto.StreamInfo {
			t.Fatalf("iteration %d: first chunk = %+v", i, chunks)
		}
		if _, _, exit := collect(t, s); exit.Code != 0 {
			t.Fatalf("iteration %d: exit = %+v", i, exit)
		}
	}
}

func TestInputSequenceAdvancesOnlyAfterCompleteWrite(t *testing.T) {
	s := &Session{Kind: proto.SessionExec, stdin: &failAfterWriter{n: 2}}
	if err := s.Input(7, []byte("hello"), false); err == nil {
		t.Fatal("expected the partial write to fail")
	}
	if got := s.LastInputSeq(); got != 0 {
		t.Fatalf("failed input advanced sequence to %d", got)
	}
	var dst bytes.Buffer
	s.mu.Lock()
	s.stdin = nopWriteCloser{Writer: &dst}
	s.mu.Unlock()
	if err := s.Input(7, []byte("hello"), false); err != nil {
		t.Fatal(err)
	}
	if dst.String() != "hello" || s.LastInputSeq() != 7 {
		t.Fatalf("retry wrote %q at sequence %d", dst.String(), s.LastInputSeq())
	}
}

type nopWriteCloser struct{ io.Writer }

func (nopWriteCloser) Close() error { return nil }

func TestExecTimeoutKillsProcessGroup(t *testing.T) {
	m := newMgr(t)
	s, _ := m.Open(Spec{WS: "ws_1", Kind: proto.SessionExec, Program: []string{"sh", "-c", "sleep 30; echo done"}, Timeout: 100 * time.Millisecond})
	start := time.Now()
	_, _, exit := collect(t, s)
	if time.Since(start) > 5*time.Second {
		t.Fatal("timeout not enforced")
	}
	if exit.Signal == "" && exit.Code == 0 {
		t.Fatalf("expected signal exit, got %+v", exit)
	}
}

func TestExecSignalTERM(t *testing.T) {
	m := newMgr(t)
	program := []string{"sleep", "30"}
	if runtime.GOOS == "windows" {
		program = []string{"ping.exe", "-n", "30", "127.0.0.1"}
	}
	s, _ := m.Open(Spec{WS: "ws_1", Kind: proto.SessionExec, Program: program})
	time.Sleep(50 * time.Millisecond)
	if err := s.Signal("TERM"); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	info, err := s.Wait(ctx)
	if runtime.GOOS == "windows" {
		if err != nil || info.Signal != "" || info.Code != 1 {
			t.Fatalf("%v %+v", err, info)
		}
	} else if err != nil || !strings.Contains(info.Signal, "terminated") {
		t.Fatalf("%v %+v", err, info)
	}
	if err := s.Signal("BOGUS"); err == nil {
		t.Fatal("expected error for unknown signal")
	}
}

func TestKillWorkspaceConfirmsAllSessionsStopped(t *testing.T) {
	m := newMgr(t)
	first, err := m.Open(Spec{WS: "ws_kill", Kind: proto.SessionExec, Program: []string{"sh", "-c", "sleep 60"}})
	if err != nil {
		t.Fatal(err)
	}
	second, err := m.Open(Spec{WS: "ws_kill", Kind: proto.SessionExec, Program: []string{"sh", "-c", "sleep 60"}})
	if err != nil {
		t.Fatal(err)
	}
	if err := m.KillWorkspace("ws_kill"); err != nil {
		t.Fatal(err)
	}
	if _, ok := m.Get(first.ID); ok {
		t.Fatalf("first session %s remains", first.ID)
	}
	if _, ok := m.Get(second.ID); ok {
		t.Fatalf("second session %s remains", second.ID)
	}
}

func TestPTYEchoAndResize(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("the session PTY backend uses Unix PTYs; ConPTY is not implemented")
	}
	m := newMgr(t)
	s, err := m.Open(Spec{WS: "ws_1", Kind: proto.SessionPTY, Program: []string{"sh", "-c", "stty size; read x; stty size; echo got:$x"}, Rows: 30, Cols: 100})
	if err != nil {
		t.Fatal(err)
	}
	if s.Info.PID == 0 {
		t.Fatal("no pid")
	}
	deadline := time.Now().Add(10 * time.Second)
	for {
		chunks, readErr := s.Log.Read(0, 0)
		if readErr != nil {
			t.Fatal(readErr)
		}
		var observed []byte
		for _, chunk := range chunks {
			if chunk.Stream == proto.StreamStdout {
				observed = append(observed, chunk.Data...)
			}
		}
		if bytes.Contains(observed, []byte("30 100")) {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("initial PTY size was not observable: %q", observed)
		}
		time.Sleep(10 * time.Millisecond)
	}
	if err := s.Resize(40, 120); err != nil {
		t.Fatal(err)
	}
	if err := s.Input(1, []byte("abc\n"), false); err != nil {
		t.Fatal(err)
	}
	out, _, exit := collect(t, s)
	if !bytes.Contains(out, []byte("30 100")) || !bytes.Contains(out, []byte("40 120")) ||
		!bytes.Contains(out, []byte("got:abc")) || exit.Code != 0 || exit.Error != "" {
		t.Fatalf("%q %+v", out, exit)
	}
	// Resize on exec session is unsupported.
	e, _ := m.Open(Spec{WS: "ws_1", Kind: proto.SessionExec, Program: []string{"true"}})
	if err := e.Resize(1, 1); err == nil {
		t.Fatal("expected unsupported")
	}
}

func TestPortSessionForwardsBytes(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	go func() {
		c, err := ln.Accept()
		if err != nil {
			return
		}
		defer c.Close()
		b := make([]byte, 5)
		io.ReadFull(c, b)
		c.Write([]byte("echo:" + string(b)))
	}()
	m := newMgr(t)
	port := ln.Addr().(*net.TCPAddr).Port
	s, err := m.Open(Spec{WS: "ws_1", Kind: proto.SessionPort, Port: port})
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Input(1, []byte("hello"), false); err != nil {
		t.Fatal(err)
	}
	out, _, exit := collect(t, s)
	if string(out) != "echo:hello" || exit.Code != 0 {
		t.Fatalf("%q %+v", out, exit)
	}
}

func TestIdempotentOpenAndRemove(t *testing.T) {
	m := newMgr(t)
	a, _ := m.Open(Spec{WS: "ws_1", Kind: proto.SessionExec, Program: []string{"echo", "once"}, IdempotencyKey: "k1"})
	b, err := m.Open(Spec{WS: "ws_1", Kind: proto.SessionExec, Program: []string{"echo", "once"}, IdempotencyKey: "k1"})
	if err != nil {
		t.Fatal(err)
	}
	if a.ID != b.ID {
		t.Fatal("idempotency key did not dedupe")
	}
	if _, err := m.Open(Spec{WS: "ws_1", Kind: proto.SessionExec, Program: []string{"echo", "twice"}, IdempotencyKey: "k1"}); err == nil {
		t.Fatal("idempotency key reuse with different arguments was accepted")
	}
	if len(m.List("ws_1")) != 1 || len(m.List("ws_other")) != 0 {
		t.Fatal("list")
	}
	collect(t, a)
	if !m.Remove(a.ID, false) {
		t.Fatal("remove finished session")
	}
	if _, ok := m.Get(a.ID); ok {
		t.Fatal("still present")
	}
	// A new open with the same key starts fresh after removal.
	c, _ := m.Open(Spec{WS: "ws_1", Kind: proto.SessionExec, Program: []string{"echo", "again"}, IdempotencyKey: "k1"})
	if c.ID == a.ID {
		t.Fatal("reused id")
	}
	// Running session: Remove without kill refuses; with kill works.
	r, _ := m.Open(Spec{WS: "ws_1", Kind: proto.SessionExec, Program: []string{"sleep", "30"}})
	if m.Remove(r.ID, false) {
		t.Fatal("removed a running session")
	}
	if !m.Remove(r.ID, true) {
		t.Fatal("kill+remove failed")
	}
}

func TestExecEnvAndCwd(t *testing.T) {
	m := newMgr(t)
	dir := t.TempDir()
	program := []string{"sh", "-c", "pwd; echo $FOO"}
	if runtime.GOOS == "windows" {
		program = []string{"cmd.exe", "/d", "/s", "/c", "cd & echo %FOO%"}
	}
	s, _ := m.Open(Spec{WS: "ws_1", Kind: proto.SessionExec, Program: program, Cwd: dir, Env: []string{"PATH=" + os.Getenv("PATH"), "FOO=bar"}})
	out, _, _ := collect(t, s)
	got := strings.TrimSpace(string(out))
	lines := strings.Split(strings.ReplaceAll(got, "\r\n", "\n"), "\n")
	if len(lines) != 2 || lines[1] != "bar" {
		t.Fatalf("%q", out)
	}
	gotDir, err := filepath.EvalSymlinks(lines[0])
	if err != nil {
		t.Fatal(err)
	}
	wantDir, err := filepath.EvalSymlinks(dir)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.EqualFold(filepath.Clean(gotDir), filepath.Clean(wantDir)) {
		t.Fatalf("cwd = %q, want %q", gotDir, wantDir)
	}
}

func TestManagerEnforcesRetainedActiveAndWorkspaceQuotas(t *testing.T) {
	m := NewManager(ManagerOptions{
		Retention: time.Hour, MaxSessions: 2, MaxActive: 1, MaxSessionsPerWorkspace: 1,
	})
	t.Cleanup(m.Close)
	s, err := m.Open(Spec{
		WS: "ws_one", Kind: proto.SessionExec, Program: []string{"sh", "-c", "cat"}, Stdin: true,
		IdempotencyKey: "same",
	})
	if err != nil {
		t.Fatal(err)
	}
	replayed, err := m.Open(Spec{
		WS: "ws_one", Kind: proto.SessionExec, Program: []string{"sh", "-c", "cat"}, Stdin: true,
		IdempotencyKey: "same",
	})
	if err != nil || replayed != s {
		t.Fatalf("idempotent open at quota = (%p, %v), want %p", replayed, err, s)
	}
	if _, err := m.Open(Spec{WS: "ws_one", Kind: proto.SessionExec, Program: []string{"sh", "-c", "true"}}); !errors.Is(err, &proto.Error{Code: proto.CodeResourceExhausted}) {
		t.Fatalf("workspace quota error = %v", err)
	}
	if _, err := m.Open(Spec{WS: "ws_two", Kind: proto.SessionExec, Program: []string{"sh", "-c", "true"}}); !errors.Is(err, &proto.Error{Code: proto.CodeResourceExhausted}) {
		t.Fatalf("active quota error = %v", err)
	}
	s.Kill()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, err := s.Wait(ctx); err != nil {
		t.Fatal(err)
	}
	// Wait returning is the capacity handoff: callers must not have to poll an
	// asynchronous accounting goroutine before opening the replacement.
	second, err := m.Open(Spec{WS: "ws_two", Kind: proto.SessionExec, Program: []string{"sh", "-c", "true"}})
	if err != nil {
		t.Fatalf("active quota was not released: %v", err)
	}
	if _, err := second.Wait(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := m.Open(Spec{WS: "ws_three", Kind: proto.SessionExec, Program: []string{"sh", "-c", "true"}}); !errors.Is(err, &proto.Error{Code: proto.CodeResourceExhausted}) {
		t.Fatalf("retained-session quota error = %v", err)
	}
	if stats := m.Stats(); stats.Sessions != 2 || stats.Active != 0 {
		t.Fatalf("manager stats = %+v", stats)
	}
}

func TestManagerActiveQuotaCannotBeOvercommittedConcurrently(t *testing.T) {
	const limit = 3
	m := NewManager(ManagerOptions{
		Retention: time.Hour, MaxSessions: 32, MaxActive: limit, MaxSessionsPerWorkspace: 32,
	})
	t.Cleanup(m.Close)
	type result struct {
		s   *Session
		err error
	}
	start := make(chan struct{})
	results := make(chan result, 20)
	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			s, err := m.Open(Spec{WS: "ws", Kind: proto.SessionExec, Program: []string{"sh", "-c", "cat"}, Stdin: true})
			results <- result{s: s, err: err}
		}()
	}
	close(start)
	wg.Wait()
	close(results)
	var opened []*Session
	for result := range results {
		if result.err == nil {
			opened = append(opened, result.s)
			continue
		}
		if !errors.Is(result.err, &proto.Error{Code: proto.CodeResourceExhausted}) {
			t.Fatalf("unexpected concurrent open error: %v", result.err)
		}
	}
	if len(opened) != limit {
		t.Fatalf("concurrent opens = %d, want %d", len(opened), limit)
	}
	if active := m.Stats().Active; active != limit {
		t.Fatalf("active sessions = %d, want %d", active, limit)
	}
	for _, s := range opened {
		s.Kill()
	}
}

func TestManagerEnforcesPrincipalQuotaAcrossWorkspaces(t *testing.T) {
	m := NewManager(ManagerOptions{
		Retention: time.Hour, MaxSessions: 4, MaxActive: 4,
		MaxSessionsPerWorkspace: 4, MaxSessionsPerPrincipal: 1,
	})
	t.Cleanup(m.Close)
	first, err := m.Open(Spec{WS: "ws_one", Tenant: "tenant-a", Principal: "alice", Kind: proto.SessionExec, Program: []string{"sh", "-c", "true"}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := first.Wait(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, err := m.Open(Spec{WS: "ws_two", Tenant: "tenant-a", Principal: "alice", Kind: proto.SessionExec, Program: []string{"sh", "-c", "true"}}); !errors.Is(err, &proto.Error{Code: proto.CodeResourceExhausted}) {
		t.Fatalf("principal quota error = %v", err)
	}
	if _, err := m.Open(Spec{WS: "ws_two", Tenant: "tenant-a", Principal: "bob", Kind: proto.SessionExec, Program: []string{"sh", "-c", "true"}}); err != nil {
		t.Fatalf("unrelated principal was blocked: %v", err)
	}
	if _, err := m.Open(Spec{WS: "ws_three", Tenant: "tenant-b", Principal: "alice", Kind: proto.SessionExec, Program: []string{"sh", "-c", "true"}}); err != nil {
		t.Fatalf("same principal name in another tenant was blocked: %v", err)
	}
}

func TestSessionPrincipalKeyHasNoDelimiterAliases(t *testing.T) {
	if sessionPrincipalKey("tenant", "a\x00b") == sessionPrincipalKey("tenant\x00a", "b") {
		t.Fatal("distinct opaque tenant/principal pairs produced the same quota key")
	}
}

func TestManagerAppliesRetentionPerTenant(t *testing.T) {
	m := NewManager(ManagerOptions{
		SpillDir: t.TempDir(), MemBytes: 1 << 20, SpillBytes: 1 << 20,
		Retention: time.Hour,
		RetentionForTenant: func(tenant string) time.Duration {
			if tenant == "short" {
				return 20 * time.Millisecond
			}
			return time.Hour
		},
	})
	defer m.Close()
	short, err := m.Open(Spec{WS: "ws_short", Tenant: "short", Kind: proto.SessionACP})
	if err != nil {
		t.Fatal(err)
	}
	long, err := m.Open(Spec{WS: "ws_long", Tenant: "long", Kind: proto.SessionACP})
	if err != nil {
		t.Fatal(err)
	}
	short.End(proto.ExitInfo{Code: 0})
	long.End(proto.ExitInfo{Code: 0})
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if _, ok := m.Get(short.ID); !ok {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	if _, ok := m.Get(short.ID); ok {
		t.Fatal("short-retention tenant session was not reaped")
	}
	if _, ok := m.Get(long.ID); !ok {
		t.Fatal("long-retention tenant session was reaped")
	}
}

func TestManagerCommitsAndReleasesTenantLogRecord(t *testing.T) {
	store := newMemoryBlobStore()
	var mu sync.Mutex
	records := map[string]LogRecord{}
	m := NewManager(ManagerOptions{
		SpillDir: t.TempDir(), MemBytes: 64, SpillBytes: 1024, MaxChunk: 64, MaxChunks: 4,
		SegmentBytes: 231, Retention: 20 * time.Millisecond,
		BlobStoreForTenant: func(tenant string) (artifact.BlobStore, error) {
			if tenant != "tenant-a" {
				return nil, errors.New("unexpected tenant store request")
			}
			return store, nil
		},
		CommitLogRecord: func(id, tenant string, record LogRecord) error {
			if tenant != "tenant-a" {
				return errors.New("record escaped tenant")
			}
			mu.Lock()
			records[id] = LogRecord{Version: record.Version, MaxChunk: record.MaxChunk, Segments: append([]SegmentRef(nil), record.Segments...)}
			mu.Unlock()
			return nil
		},
	})
	defer m.Close()
	s, err := m.Open(Spec{WS: "ws", Tenant: "tenant-a", Kind: proto.SessionACP})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.Record(proto.StreamStdout, bytes.Repeat([]byte("x"), 512)); err != nil {
		t.Fatal(err)
	}
	s.End(proto.ExitInfo{Code: 0})
	if _, err := s.Wait(context.Background()); err != nil {
		t.Fatal(err)
	}
	mu.Lock()
	sealed := records[s.ID]
	mu.Unlock()
	if len(sealed.Segments) == 0 {
		t.Fatal("session record did not reference sealed segments")
	}
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if _, ok := m.Get(s.ID); !ok {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	if _, ok := m.Get(s.ID); ok {
		t.Fatal("retained session was not reaped")
	}
	mu.Lock()
	released := records[s.ID]
	mu.Unlock()
	if len(released.Segments) != 0 {
		t.Fatalf("retention left remote refs pinned: %+v", released)
	}
}

func TestManagerRetentionFailureKeepsSessionAndReferences(t *testing.T) {
	store := newMemoryBlobStore()
	var mu sync.Mutex
	releaseBlocked := true
	var latest LogRecord
	m := NewManager(ManagerOptions{
		SpillDir: t.TempDir(), MemBytes: 32, SpillBytes: 512, MaxChunk: 32, MaxChunks: 2,
		SegmentBytes: 90, Retention: 15 * time.Millisecond,
		BlobStoreForTenant: func(string) (artifact.BlobStore, error) { return store, nil },
		CommitLogRecord: func(_ string, _ string, record LogRecord) error {
			mu.Lock()
			defer mu.Unlock()
			if len(record.Segments) == 0 && releaseBlocked {
				return errors.New("record store unavailable")
			}
			latest = record
			return nil
		},
	})
	defer m.Close()
	s, err := m.Open(Spec{WS: "ws", Tenant: "tenant", Kind: proto.SessionACP})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.Record(proto.StreamStdout, bytes.Repeat([]byte("y"), 128)); err != nil {
		t.Fatal(err)
	}
	s.End(proto.ExitInfo{Code: 0})
	if _, err := s.Wait(context.Background()); err != nil {
		t.Fatal(err)
	}
	time.Sleep(50 * time.Millisecond)
	if _, ok := m.Get(s.ID); !ok {
		t.Fatal("failed durable dereference released session capacity")
	}
	if len(s.Log.SegmentRefs()) == 0 {
		t.Fatal("failed durable dereference dropped authoritative refs")
	}
	mu.Lock()
	releaseBlocked = false
	mu.Unlock()
	if !m.Remove(s.ID, false) {
		t.Fatal("manual retention retry failed")
	}
	mu.Lock()
	released := latest
	mu.Unlock()
	if len(released.Segments) != 0 {
		t.Fatalf("successful retry left refs: %+v", released)
	}
}

func TestTerminateRecordsReasonInExitChunk(t *testing.T) {
	m := newMgr(t)
	spec := Spec{WS: "ws_1", Kind: proto.SessionPTY, Program: []string{"sleep", "30"}, Rows: 10, Cols: 40}
	if runtime.GOOS == "windows" {
		spec = Spec{WS: "ws_1", Kind: proto.SessionExec, Program: []string{"ping.exe", "-n", "30", "127.0.0.1"}}
	}
	s, err := m.Open(spec)
	if err != nil {
		t.Fatal(err)
	}
	if !m.Terminate(s.ID, proto.ExitReasonRevoked) {
		t.Fatal("terminate should find the session")
	}
	_, _, exit := collect(t, s)
	if exit.Reason != proto.ExitReasonRevoked || (runtime.GOOS != "windows" && exit.Signal == "") {
		t.Fatalf("exit=%+v", exit)
	}
	if m.Terminate("s_missing", proto.ExitReasonRevoked) {
		t.Fatal("unknown session reported terminated")
	}
	// A normal exit carries no reason.
	plain, _ := m.Open(Spec{WS: "ws_1", Kind: proto.SessionExec, Program: []string{"true"}})
	if _, _, exit := collect(t, plain); exit.Reason != "" {
		t.Fatalf("plain exit=%+v", exit)
	}
}
