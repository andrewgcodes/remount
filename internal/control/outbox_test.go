package control

import (
	"context"
	"database/sql"
	"errors"
	"path/filepath"
	"sync/atomic"
	"testing"

	"remount.dev/remount/internal/eventlog"
	"remount.dev/remount/internal/proto"
)

// crashingStore is the SQLite store with a fault at the boundary the outbox
// exists for: the resource transaction has committed, and the process dies
// before the event reaches the log. failNext arms that fault once.
type crashingStore struct {
	*eventlog.SQLite
	failNext atomic.Bool
	failures atomic.Int32
}

var errCrashed = errors.New("simulated crash between commit and append")

func (s *crashingStore) AppendTx(ctx context.Context, tx *sql.Tx, e *proto.Event) error {
	if s.failNext.CompareAndSwap(true, false) {
		s.failures.Add(1)
		return errCrashed
	}
	return s.SQLite.AppendTx(ctx, tx, e)
}

var _ eventlog.TxStore = (*crashingStore)(nil)

func countRows(t *testing.T, db *sql.DB, query string, args ...any) int {
	t.Helper()
	var n int
	if err := db.QueryRow(query, args...).Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
}

func eventsOfType(t *testing.T, log *eventlog.Log, typ string) []proto.Event {
	t.Helper()
	all, err := log.Read(context.Background(), 1, "", 10000)
	if err != nil {
		t.Fatal(err)
	}
	var out []proto.Event
	for _, e := range all {
		if e.Type == typ {
			out = append(out, e)
		}
	}
	return out
}

func TestOutboxCrashBetweenCommitAndAppendDeliversEventOnRestart(t *testing.T) {
	path := filepath.Join(t.TempDir(), "control.db")
	sq, err := eventlog.OpenSQLite(path)
	if err != nil {
		t.Fatal(err)
	}
	store := &crashingStore{SQLite: sq}
	log := eventlog.New(store)
	c, err := New(Options{DB: sq.DB(), Log: log, Token: "node-token", LeaseSec: 10})
	if err != nil {
		t.Fatal(err)
	}

	store.failNext.Store(true)
	ws := createWorkspace(t, c, localSubject(), proto.WorkspaceSpec{})
	if store.failures.Load() != 1 {
		t.Fatalf("fault was not exercised: %d failures", store.failures.Load())
	}
	// The caller saw success and the resource is durable, but the log has
	// not seen the event: it is parked in the outbox.
	if got := countRows(t, sq.DB(), `SELECT COUNT(*) FROM workspaces WHERE id=?`, ws.ID); got != 1 {
		t.Fatalf("workspace rows = %d, want 1", got)
	}
	if got := eventsOfType(t, log, proto.EvWSCreated); len(got) != 0 {
		t.Fatalf("ws.created reached the log despite the crash: %+v", got)
	}
	if got := countRows(t, sq.DB(), `SELECT COUNT(*) FROM event_outbox`); got != 1 {
		t.Fatalf("outbox rows = %d, want 1", got)
	}
	// A live control plane recovers on its own tick, without a restart.
	c.Tick(context.Background())
	created := eventsOfType(t, log, proto.EvWSCreated)
	if len(created) != 1 {
		t.Fatalf("ws.created after tick = %d, want 1", len(created))
	}
	if created[0].Workspace != ws.ID || created[0].Tenant != "tenant-a" || created[0].Principal != "alice" {
		t.Fatalf("event lost its attribution across the outbox: %+v", created[0])
	}
	if got := countRows(t, sq.DB(), `SELECT COUNT(*) FROM event_outbox`); got != 0 {
		t.Fatalf("outbox rows after drain = %d, want 0", got)
	}

	// Now the crash with no tick to save it: the process ends with the row
	// still parked, and the next process drains it during New.
	store.failNext.Store(true)
	if err := c.wsDestroy(context.Background(), "alice", ws.ID, "idem-destroy"); err != nil {
		t.Fatal(err)
	}
	if got := eventsOfType(t, log, proto.EvWSDestroyed); len(got) != 0 {
		t.Fatalf("ws.destroyed reached the log despite the crash: %+v", got)
	}
	c.Stop()
	if err := log.Close(); err != nil {
		t.Fatal(err)
	}

	sq2, err := eventlog.OpenSQLite(path)
	if err != nil {
		t.Fatal(err)
	}
	log2 := eventlog.New(sq2)
	c2, err := New(Options{DB: sq2.DB(), Log: log2, Token: "node-token", LeaseSec: 10})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		c2.Stop()
		_ = log2.Close()
	})
	destroyed := eventsOfType(t, log2, proto.EvWSDestroyed)
	if len(destroyed) != 1 {
		t.Fatalf("ws.destroyed after restart = %d, want exactly 1", len(destroyed))
	}
	if destroyed[0].Workspace != ws.ID {
		t.Fatalf("restart delivered an unattributed event: %+v", destroyed[0])
	}
	if got := eventsOfType(t, log2, proto.EvWSCreated); len(got) != 1 {
		t.Fatalf("ws.created duplicated across restart: %d", len(got))
	}
	if got := countRows(t, sq2.DB(), `SELECT COUNT(*) FROM event_outbox`); got != 0 {
		t.Fatalf("outbox rows after restart = %d, want 0", got)
	}
	// Ordering survived: created before destroyed.
	if got := eventsOfType(t, log2, proto.EvWSCreated)[0].Seq; got >= destroyed[0].Seq {
		t.Fatalf("ws.created seq %d not before ws.destroyed seq %d", got, destroyed[0].Seq)
	}
	if got := c2.snapshotWS(ws.ID); got == nil || got.State != proto.WSDestroyed {
		t.Fatalf("workspace after restart = %+v", got)
	}
}

func TestOutboxResourceWriteFailureEmitsNothing(t *testing.T) {
	f := newControlFixture(t, "", nil)
	// Take the resource table away so the workspace row cannot commit. The
	// event staged in the same transaction must vanish with it.
	if _, err := f.sq.DB().Exec(`ALTER TABLE workspaces RENAME TO workspaces_gone`); err != nil {
		t.Fatal(err)
	}
	_, err := f.c.wsCreate(context.Background(), localSubject(), &proto.WSCreateReq{Spec: proto.WorkspaceSpec{}})
	if err == nil {
		t.Fatal("wsCreate succeeded without a workspaces table")
	}
	if _, err := f.sq.DB().Exec(`ALTER TABLE workspaces_gone RENAME TO workspaces`); err != nil {
		t.Fatal(err)
	}
	if got := eventsOfType(t, f.log, proto.EvWSCreated); len(got) != 0 {
		t.Fatalf("event without its resource: %+v", got)
	}
	if got := countRows(t, f.sq.DB(), `SELECT COUNT(*) FROM event_outbox`); got != 0 {
		t.Fatalf("outbox rows after rollback = %d, want 0", got)
	}
	if got := countRows(t, f.sq.DB(), `SELECT COUNT(*) FROM workspaces`); got != 0 {
		t.Fatalf("workspace rows after rollback = %d, want 0", got)
	}
	f.c.mu.Lock()
	n := len(f.c.workspaces)
	f.c.mu.Unlock()
	if n != 0 {
		t.Fatalf("in-memory workspaces after failed commit = %d", n)
	}
	// The plane keeps working once the table is back.
	createWorkspace(t, f.c, localSubject(), proto.WorkspaceSpec{})
	if got := eventsOfType(t, f.log, proto.EvWSCreated); len(got) != 1 {
		t.Fatalf("ws.created after recovery = %d, want 1", len(got))
	}
}
