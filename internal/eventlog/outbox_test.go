package eventlog

import (
	"context"
	"errors"
	"path/filepath"
	"testing"

	"remount.dev/remount/internal/proto"
)

// flakyStore is a Store whose Append fails on demand, standing in for a
// process that dies between the resource commit and the log append.
type flakyStore struct {
	Store
	fail bool
}

func (f *flakyStore) Append(ctx context.Context, e *proto.Event) error {
	if f.fail {
		return errors.New("injected: store unavailable")
	}
	return f.Store.Append(ctx, e)
}

func openOutboxDB(t *testing.T) *SQLite {
	t.Helper()
	sq, err := OpenSQLite(filepath.Join(t.TempDir(), "ev.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { sq.Close() })
	if err := CreateOutbox(sq.DB()); err != nil {
		t.Fatal(err)
	}
	if _, err := sq.DB().Exec(`CREATE TABLE things (id TEXT PRIMARY KEY)`); err != nil {
		t.Fatal(err)
	}
	return sq
}

func countRows(t *testing.T, sq *SQLite, table string) int {
	t.Helper()
	var n int
	if err := sq.DB().QueryRow(`SELECT COUNT(*) FROM ` + table).Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
}

func TestTransactCommitsResourceAndEventTogether(t *testing.T) {
	ctx := context.Background()
	sq := openOutboxDB(t)
	l := New(sq)
	sub := l.Subscribe(1, "")
	defer sub.Close()

	err := l.Transact(ctx, sq.DB(), func(tx *Tx) error {
		if _, err := tx.Exec(`INSERT INTO things(id) VALUES('a')`); err != nil {
			return err
		}
		return tx.Emit(&proto.Event{Type: "thing.created", Stream: "a", EventID: "ev_a", Origin: "control"})
	})
	if err != nil {
		t.Fatal(err)
	}
	if countRows(t, sq, "things") != 1 || countRows(t, sq, "event_outbox") != 0 {
		t.Fatal("committed transaction must leave the row and an empty outbox")
	}
	evs, err := l.Read(ctx, 1, "", 10)
	if err != nil || len(evs) != 1 || evs[0].Type != "thing.created" || evs[0].Seq != 1 || evs[0].At == 0 {
		t.Fatalf("event not in log: %+v %v", evs, err)
	}
	live, err := sub.Next(ctx)
	if err != nil || len(live) != 1 || live[0].Seq != 1 {
		t.Fatalf("subscriber did not see the committed event: %v %v", live, err)
	}

	boom := errors.New("resource rejected")
	err = l.Transact(ctx, sq.DB(), func(tx *Tx) error {
		if err := tx.Emit(&proto.Event{Type: "thing.created", Stream: "b", EventID: "ev_b", Origin: "control"}); err != nil {
			return err
		}
		return boom
	})
	if !errors.Is(err, boom) {
		t.Fatalf("Transact must surface fn's error: %v", err)
	}
	if countRows(t, sq, "event_outbox") != 0 {
		t.Fatal("a rolled-back transaction must not leave a staged event")
	}
	if last, _ := l.Last(ctx); last != 1 {
		t.Fatalf("an event without its resource reached the log: last=%d", last)
	}
}

func TestOutboxSurvivesStoreFailureAndDrainsExactlyOnce(t *testing.T) {
	ctx := context.Background()
	sq := openOutboxDB(t)
	flaky := &flakyStore{Store: sq, fail: true}
	l := New(flaky)

	err := l.Transact(ctx, sq.DB(), func(tx *Tx) error {
		if _, err := tx.Exec(`INSERT INTO things(id) VALUES('a')`); err != nil {
			return err
		}
		return tx.Emit(&proto.Event{Type: "thing.created", Stream: "a", EventID: "ev_a", Origin: "control"})
	})
	if err == nil {
		t.Fatal("append failure must be reported")
	}
	if countRows(t, sq, "things") != 1 || countRows(t, sq, "event_outbox") != 1 {
		t.Fatal("the resource must be durable and its event must wait in the outbox")
	}
	if last, _ := l.Last(ctx); last != 0 {
		t.Fatal("nothing may reach the log while the store is down")
	}

	// "Restart": a fresh Log over the same database with a healthy store.
	l2 := New(sq)
	if err := l2.DrainOutbox(ctx, sq.DB()); err != nil {
		t.Fatal(err)
	}
	if err := l2.DrainOutbox(ctx, sq.DB()); err != nil {
		t.Fatal(err)
	}
	evs, err := l2.Read(ctx, 1, "", 10)
	if err != nil || len(evs) != 1 || evs[0].EventID != "ev_a" {
		t.Fatalf("outbox must deliver the event exactly once: %+v %v", evs, err)
	}
	if countRows(t, sq, "event_outbox") != 0 {
		t.Fatal("delivered rows must be removed")
	}
}

func TestOutboxDrainPreservesStagingOrderAcrossPages(t *testing.T) {
	ctx := context.Background()
	sq := openOutboxDB(t)
	l := New(sq)
	const n = outboxPage + 3
	err := l.Transact(ctx, sq.DB(), func(tx *Tx) error {
		for i := 0; i < n; i++ {
			if err := tx.Emit(&proto.Event{Type: "t", Stream: "s", Origin: "control", Cause: uint64(i + 1)}); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	var got []proto.Event
	if _, err := l.Export(ctx, 0, 0, func(e proto.Event) error { got = append(got, e); return nil }); err != nil {
		t.Fatal(err)
	}
	if len(got) != n {
		t.Fatalf("got %d events, want %d", len(got), n)
	}
	for i, e := range got {
		if e.Seq != uint64(i+1) || e.Cause != uint64(i+1) {
			t.Fatalf("event %d out of staging order: seq=%d cause=%d", i, e.Seq, e.Cause)
		}
	}
	if countRows(t, sq, "event_outbox") != 0 {
		t.Fatal("outbox not fully drained")
	}
}

func TestTransactRejectsUntypedEvent(t *testing.T) {
	sq := openOutboxDB(t)
	l := New(sq)
	err := l.Transact(context.Background(), sq.DB(), func(tx *Tx) error {
		return tx.Emit(&proto.Event{Stream: "s"})
	})
	if err == nil {
		t.Fatal("an event without a type must be refused before commit")
	}
}
