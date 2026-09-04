package control

import (
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"remount.dev/remount/internal/artifact"
	"remount.dev/remount/internal/metrics"
	"remount.dev/remount/internal/proto"
)

// barrierArtifactStore holds every sessionLogCommit inside its verification
// window: the wrapped reader reports its artifact on verified after the
// underlying file is closed, then waits for release. The barrier sits at a
// point every implementation must still reach (a verified, closed segment),
// so a correct publication cannot avoid it by ordering alone.
type barrierArtifactStore struct {
	*artifact.Store
	verified chan string
	release  chan struct{}
}

type barrierArtifactReader struct {
	io.ReadCloser
	id    string
	store *barrierArtifactStore
}

func (r barrierArtifactReader) Close() error {
	err := r.ReadCloser.Close()
	r.store.verified <- r.id
	<-r.store.release
	return err
}

func (s *barrierArtifactStore) Open(id string) (io.ReadCloser, int64, error) {
	r, size, err := s.Store.Open(id)
	if err != nil {
		return nil, 0, err
	}
	return barrierArtifactReader{ReadCloser: r, id: id, store: s}, size, nil
}

func newBarrierArtifactStore(t *testing.T) (*barrierArtifactStore, string) {
	t.Helper()
	dir := t.TempDir()
	store, err := artifact.NewStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	return &barrierArtifactStore{Store: store, verified: make(chan string, 16), release: make(chan struct{})}, dir
}

func putAgedSegment(t *testing.T, store *artifact.Store, dir, body string, at time.Time) proto.SessionLogSegment {
	t.Helper()
	id, size, err := store.Put(strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	digest, err := artifact.Digest(id)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Chtimes(filepath.Join(dir, digest[:2], digest), at, at); err != nil {
		t.Fatal(err)
	}
	return proto.SessionLogSegment{First: 0, Next: 1, Artifact: id, Bytes: size}
}

func sessionLogRoots(c *Control) map[string]bool {
	roots := map[string]bool{}
	c.mu.Lock()
	c.sessionLogReferencesLocked(func(_, id string) { roots[id] = true })
	c.mu.Unlock()
	return roots
}

func countSessionLogs(c *Control, tenant string) int {
	c.mu.Lock()
	defer c.mu.Unlock()
	n := 0
	for _, record := range c.sessionLogs {
		if record.Tenant == tenant {
			n++
		}
	}
	return n
}

type commitResult struct {
	record *proto.SessionLogRecord
	err    error
}

// TestSessionLogCommitPinsVerifiedSegmentsUntilReferenceCommits pins RMR-006.
// Authority: control plane, on behalf of the current holder of ws-a at
// generation 7. Resource: the verified segment blob. Irreversible action: GC
// unlinking it. Fence: the segment is a GC root from admission until the
// session_logs row and its event commit. Postcondition: a successful commit
// names bytes that are still present, and the pin does not outlive the row.
func TestSessionLogCommitPinsVerifiedSegmentsUntilReferenceCommits(t *testing.T) {
	store, dir := newBarrierArtifactStore(t)
	now := time.Now()
	segment := putAgedSegment(t, store.Store, dir, "verified-but-not-yet-referenced", now.Add(-48*time.Hour))
	f := newControlFixture(t, "", func(options *Options) { options.Artifacts = store })
	putHeldSessionWorkspace(f.c, "ws-a", "tenant-a", "node-a", 7)
	req := proto.SessionLogCommitReq{
		Session: "ses-a", Workspace: "ws-a", Generation: 7, Principal: "alice",
		Kind: proto.SessionExec, MaxChunk: 32 << 10, Segments: []proto.SessionLogSegment{segment},
		Complete: true, Info: proto.SessionInfo{ID: "ses-a", WS: "ws-a", Kind: proto.SessionExec},
	}
	results := make(chan commitResult, 1)
	go func() {
		record, err := f.c.sessionLogCommit(context.Background(), "node-a", &req)
		results <- commitResult{record, err}
	}()
	select {
	case <-store.verified:
	case <-time.After(30 * time.Second):
		t.Fatal("commit never verified its segment")
	}

	// A reference-aware GC pass runs while the segment is verified but its
	// durable reference has not committed.
	var gc artifact.GCResult
	if err := f.c.WithArtifactReferences(func(references []string) error {
		var err error
		gc, err = store.Collect(references, now)
		return err
	}); err != nil {
		t.Fatal(err)
	}
	close(store.release)
	result := <-results
	if result.err != nil {
		t.Fatalf("commit after concurrent GC = %v, want success", result.err)
	}
	if !store.Has(segment.Artifact) {
		t.Fatalf("committed session log names an artifact GC unlinked during verification (gc=%+v)", gc)
	}
	if gc.Removed != 0 {
		t.Fatalf("GC removed %d objects during publication", gc.Removed)
	}
	if !sessionLogRoots(f.c)[segment.Artifact] {
		t.Fatal("committed segment is not a GC root")
	}

	// The publication pin must not outlive the row: once the record is
	// deleted, nothing else protects the blob.
	if err := f.c.sessionLogDelete("node-a", &proto.SessionLogDeleteReq{Session: "ses-a", Workspace: "ws-a", Generation: 7}); err != nil {
		t.Fatal(err)
	}
	if err := f.c.WithArtifactReferences(func(references []string) error {
		var err error
		gc, err = store.Collect(references, now)
		return err
	}); err != nil {
		t.Fatal(err)
	}
	if gc.Removed != 1 || store.Has(segment.Artifact) {
		t.Fatalf("publication pin leaked past the deleted record: gc=%+v present=%t", gc, store.Has(segment.Artifact))
	}
}

// TestSessionLogAdmissionIsAtomicUnderConcurrentNewSessions pins RMR-007.
// Resource: one tenant retained-record slot. Irreversible action: accepting a
// new session row. Fence: the slot is reserved under c.mu before verification
// and converted to the row in the same critical section that commits it.
// Postcondition: at most MaxSessionLogsPerTenant records exist, the loser
// gets an observable resource_exhausted, and capacity returns on deletion.
func TestSessionLogAdmissionIsAtomicUnderConcurrentNewSessions(t *testing.T) {
	store, dir := newBarrierArtifactStore(t)
	now := time.Now()
	segment := putAgedSegment(t, store.Store, dir, "contended-slot", now)
	f := newControlFixture(t, "", func(options *Options) {
		options.Artifacts = store
		options.MaxSessionLogsPerTenant = 1
	})
	putHeldSessionWorkspace(f.c, "ws-a", "tenant-a", "node-a", 7)
	request := func(session string) *proto.SessionLogCommitReq {
		return &proto.SessionLogCommitReq{
			Session: session, Workspace: "ws-a", Generation: 7, Principal: "alice",
			Kind: proto.SessionExec, MaxChunk: 32 << 10, Segments: []proto.SessionLogSegment{segment},
		}
	}
	rejectedBefore := metrics.SessionLogQuotaRejected.Value()
	results := make(chan commitResult, 2)
	var wg sync.WaitGroup
	for _, session := range []string{"ses-1", "ses-2"} {
		wg.Add(1)
		go func(session string) {
			defer wg.Done()
			record, err := f.c.sessionLogCommit(context.Background(), "node-a", request(session))
			results <- commitResult{record, err}
		}(session)
	}
	// Each contender either reaches verification or is refused before it. A
	// correct admission never lets both verify; the unsafe one always does.
	arrived, finished := 0, 0
	var collected []commitResult
	for arrived+finished < 2 {
		select {
		case <-store.verified:
			arrived++
		case result := <-results:
			finished++
			collected = append(collected, result)
		case <-time.After(30 * time.Second):
			t.Fatalf("contenders stalled: verified=%d finished=%d", arrived, finished)
		}
	}
	close(store.release)
	wg.Wait()
	for len(collected) < 2 {
		collected = append(collected, <-results)
	}
	accepted, exhausted := 0, 0
	for _, result := range collected {
		switch {
		case result.err == nil:
			accepted++
		case errors.Is(result.err, &proto.Error{Code: proto.CodeResourceExhausted}):
			exhausted++
		default:
			t.Fatalf("unexpected commit outcome: %v", result.err)
		}
	}
	if accepted != 1 || exhausted != 1 {
		t.Fatalf("accepted=%d exhausted=%d, want exactly one of each with limit 1", accepted, exhausted)
	}
	if got := countSessionLogs(f.c, "tenant-a"); got != 1 {
		t.Fatalf("tenant retains %d session logs, limit 1", got)
	}
	if delta := metrics.SessionLogQuotaRejected.Value() - rejectedBefore; delta != 1 {
		t.Fatalf("session log quota metric advanced by %d, want 1", delta)
	}
	events, err := f.log.Read(context.Background(), 0, "", 1000)
	if err != nil {
		t.Fatal(err)
	}
	quotaEvents := 0
	for _, event := range events {
		if event.Type != proto.EvQuotaExceeded {
			continue
		}
		var payload map[string]any
		if err := proto.Unmarshal(event.Payload, &payload); err != nil {
			t.Fatal(err)
		}
		if payload["resource"] == "session_logs" && event.Tenant == "tenant-a" && payload["limit"] != nil && payload["used"] != nil {
			quotaEvents++
		}
	}
	if quotaEvents != 1 {
		t.Fatalf("%d session_logs quota events, want 1", quotaEvents)
	}

	var winner string
	f.c.mu.Lock()
	for id := range f.c.sessionLogs {
		winner = id
	}
	f.c.mu.Unlock()
	// A continuation of the admitted session consumes no second slot, and an
	// exact replay of the completed record is idempotent at capacity.
	complete := request(winner)
	complete.Complete = true
	complete.Info = proto.SessionInfo{ID: winner, WS: "ws-a", Kind: proto.SessionExec}
	if _, err := f.c.sessionLogCommit(context.Background(), "node-a", complete); err != nil {
		t.Fatalf("continuation at capacity = %v", err)
	}
	if _, err := f.c.sessionLogCommit(context.Background(), "node-a", complete); err != nil {
		t.Fatalf("exact replay at capacity = %v", err)
	}
	if _, err := f.c.sessionLogCommit(context.Background(), "node-a", request("ses-3")); !errors.Is(err, &proto.Error{Code: proto.CodeResourceExhausted}) {
		t.Fatalf("third new session at capacity = %v, want resource_exhausted", err)
	}
	if err := f.c.sessionLogDelete("node-a", &proto.SessionLogDeleteReq{Session: winner, Workspace: "ws-a", Generation: 7}); err != nil {
		t.Fatal(err)
	}
	if _, err := f.c.sessionLogCommit(context.Background(), "node-a", request("ses-3")); err != nil {
		t.Fatalf("new session after capacity release = %v", err)
	}
}

// TestSessionLogCommitFailuresReleaseAdmissionAndPins injects the failures a
// publication can meet after admission: a missing segment, a cancelled
// caller, and a SQL failure at the durable commit. Each must leave no row,
// no committed event, no GC root, and no reserved slot, so a retry succeeds.
func TestSessionLogCommitFailuresReleaseAdmissionAndPins(t *testing.T) {
	store, dir := newBarrierArtifactStore(t)
	now := time.Now()
	first := putAgedSegment(t, store.Store, dir, "first-segment", now)
	second := putAgedSegment(t, store.Store, dir, "second-segment", now)
	second.First, second.Next = 1, 2
	elsewhere, err := artifact.NewStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	missingID, missingSize, err := elsewhere.Put(strings.NewReader("never-uploaded-here"))
	if err != nil {
		t.Fatal(err)
	}
	f := newControlFixture(t, "", func(options *Options) {
		options.Artifacts = store
		options.MaxSessionLogsPerTenant = 1
	})
	putHeldSessionWorkspace(f.c, "ws-a", "tenant-a", "node-a", 7)
	request := func(session string, segments ...proto.SessionLogSegment) *proto.SessionLogCommitReq {
		return &proto.SessionLogCommitReq{
			Session: session, Workspace: "ws-a", Generation: 7, Principal: "alice",
			Kind: proto.SessionExec, MaxChunk: 32 << 10, Segments: segments,
		}
	}
	assertClean := func(stage string) {
		t.Helper()
		if got := countSessionLogs(f.c, "tenant-a"); got != 0 {
			t.Fatalf("%s: %d records survived a failed commit", stage, got)
		}
		if roots := sessionLogRoots(f.c); len(roots) != 0 {
			t.Fatalf("%s: failed commit left %d pinned roots", stage, len(roots))
		}
		f.c.mu.Lock()
		admissions, pins := len(f.c.sessionLogAdmissions), len(f.c.sessionLogPins)
		f.c.mu.Unlock()
		if admissions != 0 || pins != 0 {
			t.Fatalf("%s: admissions=%d pins=%d after failure", stage, admissions, pins)
		}
	}
	drain := func() {
		for {
			select {
			case <-store.verified:
			default:
				return
			}
		}
	}

	// Missing segment: verification fails after admission, before any reader
	// is opened, so the barrier is never reached.
	if _, err := f.c.sessionLogCommit(context.Background(), "node-a", request("ses-missing", proto.SessionLogSegment{First: 0, Next: 1, Artifact: missingID, Bytes: missingSize})); !errors.Is(err, &proto.Error{Code: proto.CodeConflict}) {
		t.Fatalf("missing segment commit = %v, want conflict", err)
	}
	assertClean("missing segment")

	// Cancellation between two segments: the first has verified, the second
	// is never opened, and the caller's context error is returned.
	ctx, cancel := context.WithCancel(context.Background())
	results := make(chan commitResult, 1)
	go func() {
		record, err := f.c.sessionLogCommit(ctx, "node-a", request("ses-cancel", first, second))
		results <- commitResult{record, err}
	}()
	select {
	case <-store.verified:
	case <-time.After(30 * time.Second):
		t.Fatal("cancelled commit never verified its first segment")
	}
	cancel()
	close(store.release)
	if result := <-results; !errors.Is(result.err, context.Canceled) {
		t.Fatalf("cancelled commit = %v, want context.Canceled", result.err)
	}
	assertClean("cancellation")
	drain()

	// SQL failure at the durable commit; the barrier stays open from here on.
	if _, err := f.sq.DB().Exec(`CREATE TRIGGER session_logs_injected_failure BEFORE INSERT ON session_logs BEGIN SELECT RAISE(ABORT, 'injected session_logs failure'); END`); err != nil {
		t.Fatal(err)
	}
	if _, err := f.c.sessionLogCommit(context.Background(), "node-a", request("ses-sql", first)); err == nil || !strings.Contains(err.Error(), "injected session_logs failure") {
		t.Fatalf("commit with failing INSERT = %v, want injected failure", err)
	}
	assertClean("sql failure")
	drain()
	events, err := f.log.Read(context.Background(), 0, "", 1000)
	if err != nil {
		t.Fatal(err)
	}
	for _, event := range events {
		if event.Type == proto.EvSessionLogCommitted {
			t.Fatalf("a failed transaction published %s", event.Type)
		}
	}
	if _, err := f.sq.DB().Exec(`DROP TRIGGER session_logs_injected_failure`); err != nil {
		t.Fatal(err)
	}

	// Retry: every failed publication released its slot, so the tenant's
	// single slot is still free and the retried commit is the one that lands.
	record, err := f.c.sessionLogCommit(context.Background(), "node-a", request("ses-sql", first))
	if err != nil {
		t.Fatalf("retry after released failures = %v", err)
	}
	if got := countSessionLogs(f.c, "tenant-a"); got != 1 || record.Session != "ses-sql" {
		t.Fatalf("retry produced %d records (%+v)", got, record)
	}
	if !sessionLogRoots(f.c)[first.Artifact] {
		t.Fatal("retried commit is not a GC root")
	}
	f.c.mu.Lock()
	admissions, pins := len(f.c.sessionLogAdmissions), len(f.c.sessionLogPins)
	f.c.mu.Unlock()
	if admissions != 0 || pins != 0 {
		t.Fatalf("successful commit left admissions=%d pins=%d", admissions, pins)
	}
}

// TestSessionLogAdmissionReconstructsAfterRestart proves the durable side of
// the quota: committed rows count against the limit after a control-plane
// restart, and an in-flight reservation is process-local, so nothing is
// leaked into the restarted count.
func TestSessionLogAdmissionReconstructsAfterRestart(t *testing.T) {
	store, dir := newBarrierArtifactStore(t)
	segment := putAgedSegment(t, store.Store, dir, "durable-slot", time.Now())
	close(store.release)
	path := filepath.Join(t.TempDir(), "control.db")
	configure := func(options *Options) {
		options.Artifacts = store
		options.MaxSessionLogsPerTenant = 1
	}
	f := newControlFixture(t, path, configure)
	putHeldSessionWorkspace(f.c, "ws-a", "tenant-a", "node-a", 7)
	request := func(session string) *proto.SessionLogCommitReq {
		return &proto.SessionLogCommitReq{
			Session: session, Workspace: "ws-a", Generation: 7, Principal: "alice",
			Kind: proto.SessionExec, MaxChunk: 32 << 10, Segments: []proto.SessionLogSegment{segment},
		}
	}
	if _, err := f.c.sessionLogCommit(context.Background(), "node-a", request("ses-durable")); err != nil {
		t.Fatal(err)
	}
	f.c.Stop()
	if err := f.log.Close(); err != nil {
		t.Fatal(err)
	}

	restarted := newControlFixture(t, path, configure)
	putHeldSessionWorkspace(restarted.c, "ws-a", "tenant-a", "node-a", 7)
	if got := countSessionLogs(restarted.c, "tenant-a"); got != 1 {
		t.Fatalf("restart reconstructed %d session logs, want 1", got)
	}
	if !sessionLogRoots(restarted.c)[segment.Artifact] {
		t.Fatal("restart lost the committed segment as a GC root")
	}
	if _, err := restarted.c.sessionLogCommit(context.Background(), "node-a", request("ses-after-restart")); !errors.Is(err, &proto.Error{Code: proto.CodeResourceExhausted}) {
		t.Fatalf("new session against reconstructed quota = %v, want resource_exhausted", err)
	}
	if _, err := restarted.c.sessionLogCommit(context.Background(), "node-a", request("ses-durable")); err != nil {
		t.Fatalf("replay of the durable record after restart = %v", err)
	}
}
