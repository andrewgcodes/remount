// Package eventlog is the canonical, append-only record of everything that
// happens in Remount. State elsewhere is a cache of this log
// (docs/adr/0004-log-is-truth.md).
//
// Two stores: Memory (nodes, tests, --standalone without persistence) and
// SQLite (control plane). Both expose the same Append/Read/Subscribe API.
package eventlog

import (
	"context"
	"database/sql"
	"errors"
	"sync"
	"time"

	_ "modernc.org/sqlite"

	"remount.dev/remount/internal/metrics"
	"remount.dev/remount/internal/proto"
)

// Store persists events.
type Store interface {
	// Append assigns Seq and At (if zero) and persists the event.
	Append(ctx context.Context, e *proto.Event) error
	// Read returns up to limit events with seq >= from, optionally filtered
	// by stream (workspace or node id).
	Read(ctx context.Context, from uint64, stream string, limit int) ([]proto.Event, error)
	// Last returns the highest assigned seq (0 if empty).
	Last(ctx context.Context) (uint64, error)
	Close() error
}

// Log wraps a Store with fan-out subscriptions.
type Log struct {
	store Store
	mu    sync.Mutex
	subs  map[*Subscription]struct{}
}

// New wraps a store.
func New(store Store) *Log {
	return &Log{store: store, subs: map[*Subscription]struct{}{}}
}

// Emit builds and appends an event. Payload is CBOR-encoded.
func (l *Log) Emit(ctx context.Context, typ, stream, principal, node string, payload any, cause uint64) (*proto.Event, error) {
	e := &proto.Event{Type: typ, Stream: stream, Principal: principal, Node: node, Cause: cause}
	if payload != nil {
		e.Payload = proto.MustMarshal(payload)
	}
	if err := l.Append(ctx, e); err != nil {
		return nil, err
	}
	return e, nil
}

// Append persists and fans out.
func (l *Log) Append(ctx context.Context, e *proto.Event) error {
	if e.At == 0 {
		e.At = time.Now().UnixMilli()
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if err := l.store.Append(ctx, e); err != nil {
		return err
	}
	metrics.EventsAppended.Inc()
	for s := range l.subs {
		if s.stream != "" && s.stream != e.Stream {
			continue
		}
		select {
		case s.ch <- *e:
		default:
			// Slow subscriber: mark lagged; it will catch up from the store.
			s.markLagged(e.Seq)
		}
	}
	return nil
}

// Read delegates to the store.
func (l *Log) Read(ctx context.Context, from uint64, stream string, limit int) ([]proto.Event, error) {
	return l.store.Read(ctx, from, stream, limit)
}

// Last delegates to the store.
func (l *Log) Last(ctx context.Context) (uint64, error) { return l.store.Last(ctx) }

// Close closes the store.
func (l *Log) Close() error { return l.store.Close() }

// Subscription receives live events. Use Next; it transparently backfills
// from the store when the live channel overflowed.
type Subscription struct {
	log    *Log
	stream string
	ch     chan proto.Event
	next   uint64 // next seq the caller expects
	mu     sync.Mutex
	lagged bool
	closed bool
}

// Subscribe returns a subscription delivering events with seq >= from
// (0 = from the beginning; use Last()+1 for "only new"). Historical events
// are read from the store, then live ones follow.
func (l *Log) Subscribe(from uint64, stream string) *Subscription {
	s := &Subscription{log: l, stream: stream, ch: make(chan proto.Event, 256), next: from}
	l.mu.Lock()
	l.subs[s] = struct{}{}
	l.mu.Unlock()
	return s
}

func (s *Subscription) markLagged(seq uint64) {
	s.mu.Lock()
	s.lagged = true
	s.mu.Unlock()
}

// Next returns the next batch of events (at least one) or blocks.
func (s *Subscription) Next(ctx context.Context) ([]proto.Event, error) {
	for {
		// Backfill from the store whenever we're behind.
		s.mu.Lock()
		lagged := s.lagged
		s.lagged = false
		closed := s.closed
		s.mu.Unlock()
		if closed {
			return nil, errors.New("eventlog: subscription closed")
		}
		last, err := s.log.Last(ctx)
		if err != nil {
			return nil, err
		}
		if lagged || s.next <= last {
			evs, err := s.log.Read(ctx, s.next, s.stream, 512)
			if err != nil {
				return nil, err
			}
			if len(evs) > 0 {
				s.next = evs[len(evs)-1].Seq + 1
				// Drop live copies of what we just read.
				s.drain()
				return evs, nil
			}
		}
		select {
		case e := <-s.ch:
			if e.Seq < s.next {
				continue // already delivered via backfill
			}
			batch := []proto.Event{e}
			s.next = e.Seq + 1
		more:
			for len(batch) < 256 {
				select {
				case e := <-s.ch:
					if e.Seq >= s.next {
						batch = append(batch, e)
						s.next = e.Seq + 1
					}
				default:
					break more
				}
			}
			return batch, nil
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
}

func (s *Subscription) drain() {
	for {
		select {
		case e := <-s.ch:
			if e.Seq >= s.next {
				// Newer than our backfill: put it back by re-marking lag so
				// the next Next() re-reads from the store.
				s.markLagged(e.Seq)
				return
			}
		default:
			return
		}
	}
}

// Close unsubscribes.
func (s *Subscription) Close() {
	s.log.mu.Lock()
	delete(s.log.subs, s)
	s.log.mu.Unlock()
	s.mu.Lock()
	s.closed = true
	s.mu.Unlock()
}

// ---------------------------------------------------------------------------
// Memory store
// ---------------------------------------------------------------------------

// Memory keeps events in a slice, optionally bounded.
type Memory struct {
	mu     sync.Mutex
	events []proto.Event
	first  uint64 // seq of events[0]
	next   uint64
	max    int
}

// NewMemory returns an in-memory store keeping at most max events (0 = all).
func NewMemory(max int) *Memory { return &Memory{next: 1, first: 1, max: max} }

func (m *Memory) Append(ctx context.Context, e *proto.Event) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	e.Seq = m.next
	m.next++
	m.events = append(m.events, *e)
	if m.max > 0 && len(m.events) > m.max {
		drop := len(m.events) - m.max
		m.events = m.events[drop:]
		m.first += uint64(drop)
	}
	return nil
}

func (m *Memory) Read(ctx context.Context, from uint64, stream string, limit int) ([]proto.Event, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if from < m.first {
		from = m.first
	}
	if from >= m.next {
		return nil, nil
	}
	var out []proto.Event
	for i := int(from - m.first); i < len(m.events); i++ {
		e := m.events[i]
		if stream != "" && e.Stream != stream {
			continue
		}
		out = append(out, e)
		if limit > 0 && len(out) >= limit {
			break
		}
	}
	return out, nil
}

func (m *Memory) Last(ctx context.Context) (uint64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.next - 1, nil
}

func (m *Memory) Close() error { return nil }

// ---------------------------------------------------------------------------
// SQLite store
// ---------------------------------------------------------------------------

// SQLite persists events in one table.
type SQLite struct {
	db *sql.DB
	mu sync.Mutex
}

// OpenSQLite opens or creates the database at path (":memory:" allowed).
func OpenSQLite(path string) (*SQLite, error) {
	dsn := path
	if path != ":memory:" {
		dsn = "file:" + path + "?_pragma=journal_mode(WAL)&_pragma=synchronous(NORMAL)&_pragma=busy_timeout(5000)"
	}
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(1) // serialize; sqlite is single-writer and this keeps seq strictly monotonic
	if _, err := db.Exec(`CREATE TABLE IF NOT EXISTS events (
		seq INTEGER PRIMARY KEY AUTOINCREMENT,
		at INTEGER NOT NULL,
		stream TEXT NOT NULL DEFAULT '',
		principal TEXT NOT NULL DEFAULT '',
		node TEXT NOT NULL DEFAULT '',
		type TEXT NOT NULL,
		payload BLOB,
		cause INTEGER NOT NULL DEFAULT 0
	); CREATE INDEX IF NOT EXISTS events_stream ON events(stream, seq);`); err != nil {
		db.Close()
		return nil, err
	}
	return &SQLite{db: db}, nil
}

// DB exposes the handle so the control plane can share the file for its own tables.
func (s *SQLite) DB() *sql.DB { return s.db }

func (s *SQLite) Append(ctx context.Context, e *proto.Event) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	res, err := s.db.ExecContext(ctx, `INSERT INTO events(at, stream, principal, node, type, payload, cause) VALUES(?,?,?,?,?,?,?)`,
		e.At, e.Stream, e.Principal, e.Node, e.Type, e.Payload, e.Cause)
	if err != nil {
		return err
	}
	id, err := res.LastInsertId()
	if err != nil {
		return err
	}
	e.Seq = uint64(id)
	return nil
}

func (s *SQLite) Read(ctx context.Context, from uint64, stream string, limit int) ([]proto.Event, error) {
	if limit <= 0 {
		limit = 1000
	}
	var rows *sql.Rows
	var err error
	if stream == "" {
		rows, err = s.db.QueryContext(ctx, `SELECT seq, at, stream, principal, node, type, payload, cause FROM events WHERE seq >= ? ORDER BY seq LIMIT ?`, from, limit)
	} else {
		rows, err = s.db.QueryContext(ctx, `SELECT seq, at, stream, principal, node, type, payload, cause FROM events WHERE seq >= ? AND stream = ? ORDER BY seq LIMIT ?`, from, stream, limit)
	}
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []proto.Event
	for rows.Next() {
		var e proto.Event
		if err := rows.Scan(&e.Seq, &e.At, &e.Stream, &e.Principal, &e.Node, &e.Type, &e.Payload, &e.Cause); err != nil {
			return nil, err
		}
		out = append(out, e)
	}
	return out, rows.Err()
}

func (s *SQLite) Last(ctx context.Context) (uint64, error) {
	var last sql.NullInt64
	if err := s.db.QueryRowContext(ctx, `SELECT MAX(seq) FROM events`).Scan(&last); err != nil {
		return 0, err
	}
	return uint64(last.Int64), nil
}

func (s *SQLite) Close() error { return s.db.Close() }
