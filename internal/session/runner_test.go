package session

import (
	"bytes"
	"io"
	"os"
	"sync"
	"testing"

	"remount.dev/remount/internal/proto"
)

type testRunner struct{ running *testRunning }

func (r testRunner) Start(Spec) (Running, error) { return r.running, nil }

type testRunning struct {
	mu            sync.Mutex
	stdin         bytes.Buffer
	stdoutR       *io.PipeReader
	stdoutW       *io.PipeWriter
	done          chan proto.ExitInfo
	resize        [2]uint16
	signals       []string
	closeWriteErr error
}

func newTestRunning() *testRunning {
	r, w := io.Pipe()
	return &testRunning{stdoutR: r, stdoutW: w, done: make(chan proto.ExitInfo, 1)}
}

func (r *testRunning) PID() int              { return 42 }
func (r *testRunning) Stdin() io.WriteCloser { return runnerWriteCloser{Writer: &r.stdin} }
func (r *testRunning) Stdout() io.Reader     { return r.stdoutR }
func (r *testRunning) Stderr() io.Reader     { return nil }
func (r *testRunning) CloseWrite() error     { return r.closeWriteErr }
func (r *testRunning) Wait() proto.ExitInfo  { return <-r.done }
func (r *testRunning) Resize(rows, cols uint16) error {
	r.mu.Lock()
	r.resize = [2]uint16{rows, cols}
	r.mu.Unlock()
	return nil
}
func (r *testRunning) Signal(name string) error {
	r.mu.Lock()
	r.signals = append(r.signals, name)
	r.mu.Unlock()
	return nil
}

type runnerWriteCloser struct{ io.Writer }

func (runnerWriteCloser) Close() error { return nil }

func TestBackendRunnerPreservesManagerStreamAndInputContracts(t *testing.T) {
	running := newTestRunning()
	m := NewManager(ManagerOptions{})
	s, err := m.Open(Spec{WS: "ws_runner", Kind: proto.SessionPTY, Program: []string{"guest"}, Runner: testRunner{running}})
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Input(1, []byte("once"), false); err != nil {
		t.Fatal(err)
	}
	if err := s.Input(1, []byte("duplicate"), false); err != nil {
		t.Fatal(err)
	}
	if err := s.Resize(31, 97); err != nil {
		t.Fatal(err)
	}
	if _, err := running.stdoutW.Write([]byte("remote output")); err != nil {
		t.Fatal(err)
	}
	running.done <- proto.ExitInfo{Code: 7}
	_ = running.stdoutW.Close()
	if info, err := s.Wait(t.Context()); err != nil || info.Code != 7 {
		t.Fatalf("wait = %+v, %v", info, err)
	}
	if got := running.stdin.String(); got != "once" {
		t.Fatalf("deduplicated input = %q", got)
	}
	if running.resize != [2]uint16{31, 97} {
		t.Fatalf("resize = %v", running.resize)
	}
	chunks, err := s.Log.Read(0, 16)
	if err != nil {
		t.Fatal(err)
	}
	if len(chunks) != 3 || chunks[0].Stream != proto.StreamInfo || chunks[0].Seq != 0 || chunks[1].Stream != proto.StreamStdout || chunks[2].Stream != proto.StreamExit {
		t.Fatalf("unexpected remote stream sequence: %+v", chunks)
	}
}

func TestBackendRunnerInputEOFToleratesAlreadyClosedWriter(t *testing.T) {
	running := newTestRunning()
	running.closeWriteErr = os.ErrClosed
	m := NewManager(ManagerOptions{})
	s, err := m.Open(Spec{WS: "ws_runner", Kind: proto.SessionExec, Program: []string{"guest"}, Stdin: true, Runner: testRunner{running}})
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Input(7, []byte("stop\n"), true); err != nil {
		t.Fatal(err)
	}
	if got := s.LastInputSeq(); got != 7 {
		t.Fatalf("last input sequence = %d, want 7", got)
	}
	running.done <- proto.ExitInfo{Code: 0}
	_ = running.stdoutW.Close()
	if _, err := s.Wait(t.Context()); err != nil {
		t.Fatal(err)
	}
}
