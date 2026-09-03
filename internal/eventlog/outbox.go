package eventlog

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"remount.dev/remount/internal/metrics"
	"remount.dev/remount/internal/proto"
)

// TxStore is a Store whose Append can join a transaction on the database the
// outbox lives in. When the log and the resource tables share one SQLite
// file, draining the outbox moves each event into the log and deletes its
// outbox row in a single transaction, so an event is delivered exactly once.
type TxStore interface {
	Store
	// AppendTx is Append inside tx. Seq is assigned when tx commits.
	AppendTx(ctx context.Context, tx *sql.Tx, e *proto.Event) error
}

// Tx is one resource transaction with an attached event outbox. Resource
// writes go through the embedded *sql.Tx; events go through Emit and are
// committed or rolled back together with the rows they describe.
type Tx struct {
	*sql.Tx
	staged int
}

// Emit stages an event in the outbox row of this transaction. It is not
// visible to subscribers until the transaction commits and the outbox drains.
func (t *Tx) Emit(e *proto.Event) error {
	if e == nil || e.Type == "" {
		return errors.New("eventlog: staged event needs a type")
	}
	if e.At == 0 {
		e.At = time.Now().UnixMilli()
	}
	if _, err := t.Tx.Exec(`INSERT INTO event_outbox(event) VALUES(?)`, proto.MustMarshal(e)); err != nil {
		return err
	}
	t.staged++
	return nil
}

// DrainError reports that a transaction committed but the outbox could not
// be drained into the log afterwards. The resource and its events are both
// durable; the events are delivered by the next Transact or DrainOutbox on
// the same database. Callers treat the commit as successful.
type DrainError struct {
	Err error
}

func (e *DrainError) Error() string { return "eventlog: outbox drain deferred: " + e.Err.Error() }

// Unwrap exposes the store error behind the deferral.
func (e *DrainError) Unwrap() error { return e.Err }

// CreateOutbox creates the outbox table used by Transact. Callers that own
// a database run it once alongside their own migrations.
func CreateOutbox(db *sql.DB) error {
	_, err := db.Exec(`CREATE TABLE IF NOT EXISTS event_outbox (id INTEGER PRIMARY KEY AUTOINCREMENT, event BLOB NOT NULL)`)
	return err
}

// Transact runs fn inside one transaction on db. Rows fn writes and events
// fn stages with Tx.Emit commit atomically; afterwards the outbox drains
// into the log in staging order. The log's mutex is held for the whole call
// so a concurrent Append cannot be fanned out ahead of a staged event that
// will receive a lower sequence, which is what would let a subscriber skip.
// fn must not call back into the Log.
//
// When drain fails after commit the resource is durable and the event is
// durable in the outbox; Transact returns a *DrainError and the next Transact
// or DrainOutbox on the same database delivers it. Neither the resource nor
// the event is ever lost.
func (l *Log) Transact(ctx context.Context, db *sql.DB, fn func(*Tx) error) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	t := &Tx{Tx: tx}
	if err := fn(t); err != nil {
		_ = tx.Rollback()
		return err
	}
	if err := tx.Commit(); err != nil {
		return err
	}
	if t.staged == 0 {
		return nil
	}
	if err := l.drainLocked(ctx, db); err != nil {
		return &DrainError{Err: err}
	}
	return nil
}

// DrainOutbox delivers every event left in the outbox by a transaction whose
// process ended between commit and drain. Callers run it at start-up.
func (l *Log) DrainOutbox(ctx context.Context, db *sql.DB) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.drainLocked(ctx, db)
}

// outboxPage bounds one drain read so a large backlog is delivered in order
// without holding every row in memory.
const outboxPage = 256

type outboxRow struct {
	id    int64
	event proto.Event
}

func (l *Log) drainLocked(ctx context.Context, db *sql.DB) error {
	for {
		rows, err := l.readOutbox(ctx, db)
		if err != nil {
			return err
		}
		if len(rows) == 0 {
			return nil
		}
		for i := range rows {
			if err := l.deliverLocked(ctx, db, &rows[i]); err != nil {
				return err
			}
		}
		if len(rows) < outboxPage {
			return nil
		}
	}
}

func (l *Log) readOutbox(ctx context.Context, db *sql.DB) ([]outboxRow, error) {
	rs, err := db.QueryContext(ctx, `SELECT id, event FROM event_outbox ORDER BY id LIMIT ?`, outboxPage)
	if err != nil {
		return nil, err
	}
	var out []outboxRow
	for rs.Next() {
		var r outboxRow
		var raw []byte
		if err := rs.Scan(&r.id, &raw); err != nil {
			rs.Close()
			return nil, err
		}
		if err := proto.Unmarshal(raw, &r.event); err != nil {
			rs.Close()
			return nil, fmt.Errorf("eventlog: outbox row %d is not an event: %w", r.id, err)
		}
		out = append(out, r)
	}
	if err := rs.Err(); err != nil {
		rs.Close()
		return nil, err
	}
	return out, rs.Close()
}

// deliverLocked appends one outbox row to the store and removes it. With a
// TxStore both happen in one transaction and delivery is exactly once. With
// any other store the append lands first, so a failure between the two
// leaves an at-least-once row; the Memory store lives no longer than the
// process, so no such replay survives a crash.
func (l *Log) deliverLocked(ctx context.Context, db *sql.DB, r *outboxRow) error {
	e := &r.event
	if ts, ok := l.store.(TxStore); ok {
		tx, err := db.BeginTx(ctx, nil)
		if err != nil {
			return err
		}
		if err := ts.AppendTx(ctx, tx, e); err != nil {
			_ = tx.Rollback()
			return err
		}
		if _, err := tx.Exec(`DELETE FROM event_outbox WHERE id=?`, r.id); err != nil {
			_ = tx.Rollback()
			return err
		}
		if err := tx.Commit(); err != nil {
			return err
		}
	} else {
		if err := l.store.Append(ctx, e); err != nil {
			return err
		}
		if _, err := db.ExecContext(ctx, `DELETE FROM event_outbox WHERE id=?`, r.id); err != nil {
			return err
		}
	}
	metrics.EventsAppended.Inc()
	l.fanOutLocked(e)
	return nil
}
