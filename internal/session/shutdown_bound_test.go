package session

import (
	"testing"
	"time"

	"remount.dev/remount/internal/proto"
)

// TestJoinObserversIsBoundedWhenAProducerNeverStops pins MISTAKES #41 for node
// shutdown: every wait on a process needs a bound and an observable outcome
// when the bound is hit.
//
// The per-session Wait was already bounded; the producer group was not. The
// output pumps block in a read on a live child's descriptor, so a process that
// survives termination kept them running and Shutdown never returned — a node
// that hangs forever rather than one that reports what it could not join.
//
// This drives joinObservers directly with a producer that provably never
// finishes. Going through a real process would be a weaker test: whether the
// child actually outlives termination depends on the shell and the platform,
// so it can pass because the hang did not reproduce, which proves nothing.
func TestJoinObserversIsBoundedWhenAProducerNeverStops(t *testing.T) {
	m := NewManager(ManagerOptions{})
	release := make(chan struct{})
	m.observers.Add(1)
	go func() {
		<-release
		m.observers.Done()
	}()
	t.Cleanup(func() { close(release) })

	// Shorten the bound for the test rather than spending the production one.
	// The property is that a bound exists and is honoured, not its value.
	restore := observerJoinTimeoutForTest(50 * time.Millisecond)
	defer restore()

	start := time.Now()
	done := make(chan bool, 1)
	go func() { done <- m.joinObservers() }()
	select {
	case clean := <-done:
		if clean {
			t.Fatal("joinObservers reported a clean join while a producer was still running")
		}
		if elapsed := time.Since(start); elapsed > 5*time.Second {
			t.Fatalf("joinObservers took %s to give up; the bound is not being honoured", elapsed)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("joinObservers never returned; the producer join is unbounded again")
	}
}

// TestShutdownReportsCleanWhenProducersStop is the control. If Shutdown
// returned false unconditionally, the test above would pass while the return
// value carried no information.
func TestShutdownReportsCleanWhenProducersStop(t *testing.T) {
	m := NewManager(ManagerOptions{})
	if _, err := m.Open(Spec{Kind: proto.SessionExec, Program: []string{"/bin/sh", "-c", "echo done"}}); err != nil {
		t.Skipf("unavailable: cannot start a probe process: %v", err)
	}
	if clean := m.Shutdown(); !clean {
		t.Fatal("a session that exits on its own must produce a clean shutdown; the return value is not distinguishing anything")
	}
}
