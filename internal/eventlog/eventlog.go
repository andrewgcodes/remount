// Package eventlog is the canonical ordered audit and observation history.
// Transactional resource rows, rather than replay of this log, own lifecycle
// authority (docs/adr/0016-transactional-lifecycle-state.md).
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
	// First returns the oldest retained sequence. It is one past the last
	// assigned sequence when the store is empty after retention.
	First(ctx context.Context) (uint64, error)
	// Last returns the highest assigned seq (0 only if none was ever assigned).
	Last(ctx context.Context) (uint64, error)
	// Prune removes at most limit events older than beforeMillis.
	Prune(ctx context.Context, beforeMillis int64, limit int) (int64, error)
	// PruneSize removes an oldest contiguous prefix until at most max events
	// remain. At most limit rows are removed in one resumable transaction.
	PruneSize(ctx context.Context, max, limit int) (int64, error)
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
	first, err := l.store.First(ctx)
	if err != nil {
		return nil, err
	}
	if from == 0 {
		from = first
	} else if from < first {
		return nil, &proto.Error{Code: proto.CodeEvicted, Msg: "requested events are older than retention", Oldest: first}
	}
	return l.store.Read(ctx, from, stream, limit)
}

// Exporter copies a contiguous range of the log to a sink. Every audit export
// destination (a file, object storage, a SIEM) is built on this one contract
// so retention, redaction and resumption are decided in one place.
type Exporter interface {
	// Export sends events with from <= seq <= to to sink in order, stopping
	// at the first sink error, and returns the last sequence delivered. A
	// zero to means "through the newest event". A from below the oldest
	// retained sequence is CodeEvicted carrying Oldest so the caller can
	// record the hole instead of silently skipping it.
	Export(ctx context.Context, from, to uint64, sink func(proto.Event) error) (uint64, error)
}

var _ Exporter = (*Log)(nil)

// exportPage bounds one store read during Export so an export of a large log
// never pins the whole range in memory.
const exportPage = 512

// Export implements Exporter by paging through the store.
func (l *Log) Export(ctx context.Context, from, to uint64, sink func(proto.Event) error) (uint64, error) {
	first, err := l.store.First(ctx)
	if err != nil {
		return 0, err
	}
	if from == 0 {
		from = first
	} else if from < first {
		return 0, &proto.Error{Code: proto.CodeEvicted, Msg: "requested events are older than retention", Oldest: first}
	}
	if to == 0 {
		if to, err = l.store.Last(ctx); err != nil {
			return 0, err
		}
	}
	var last uint64
	for from <= to {
		if err := ctx.Err(); err != nil {
			return last, err
		}
		page, err := l.store.Read(ctx, from, "", exportPage)
		if err != nil {
			return last, err
		}
		if len(page) == 0 {
			return last, nil
		}
		for _, e := range page {
			if e.Seq > to {
				return last, nil
			}
			if err := sink(e); err != nil {
				return last, err
			}
			last = e.Seq
		}
		from = last + 1
	}
	return last, nil
}

// First returns the oldest retained event sequence.
func (l *Log) First(ctx context.Context) (uint64, error) { return l.store.First(ctx) }

// Last delegates to the store.
func (l *Log) Last(ctx context.Context) (uint64, error) { return l.store.Last(ctx) }

// Prune removes old events while serialized with appends and fan-out. A
// subscriber that subsequently asks below First receives CodeEvicted rather
// than a silently incomplete history.
func (l *Log) Prune(ctx context.Context, beforeMillis int64, limit int) (int64, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	n, err := l.store.Prune(ctx, beforeMillis, limit)
	if n > 0 {
		metrics.EventsPruned.Add(uint64(n))
	}
	return n, err
}

// PruneSize bounds event count while preserving the same contiguous-prefix
// and subscriber-eviction semantics as age-based retention.
func (l *Log) PruneSize(ctx context.Context, max, limit int) (int64, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	n, err := l.store.PruneSize(ctx, max, limit)
	if n > 0 {
		metrics.EventsPruned.Add(uint64(n))
	}
	return n, err
}

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
	done   chan struct{}
	once   sync.Once
}

// Subscribe returns a subscription delivering events with seq >= from
// (0 = from the beginning; use Last()+1 for "only new"). Historical events
// are read from the store, then live ones follow.
func (l *Log) Subscribe(from uint64, stream string) *Subscription {
	s := &Subscription{log: l, stream: stream, ch: make(chan proto.Event, 256), next: from, done: make(chan struct{})}
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
		case <-s.done:
			return nil, errors.New("eventlog: subscription closed")
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
	s.once.Do(func() {
		s.log.mu.Lock()
		delete(s.log.subs, s)
		s.log.mu.Unlock()
		s.mu.Lock()
		s.closed = true
		s.mu.Unlock()
		close(s.done)
	})
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

func (m *Memory) First(context.Context) (uint64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.first, nil
}

func (m *Memory) Prune(_ context.Context, beforeMillis int64, limit int) (int64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if limit <= 0 {
		limit = len(m.events)
	}
	remove := 0
	for remove < len(m.events) && remove < limit && m.events[remove].At < beforeMillis {
		remove++
	}
	if remove == 0 {
		return 0, nil
	}
	m.events = append([]proto.Event(nil), m.events[remove:]...)
	m.first += uint64(remove)
	return int64(remove), nil
}

func (m *Memory) PruneSize(_ context.Context, max, limit int) (int64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if max < 0 {
		return 0, errors.New("eventlog: maximum event count cannot be negative")
	}
	remove := len(m.events) - max
	if remove <= 0 {
		return 0, nil
	}
	if limit > 0 && remove > limit {
		remove = limit
	}
	m.events = append([]proto.Event(nil), m.events[remove:]...)
	m.first += uint64(remove)
	return int64(remove), nil
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
		cause INTEGER NOT NULL DEFAULT 0,
		event_id TEXT NOT NULL DEFAULT '',
		received_at INTEGER NOT NULL DEFAULT 0,
		observed_at INTEGER NOT NULL DEFAULT 0,
		origin TEXT NOT NULL DEFAULT '',
		actor TEXT NOT NULL DEFAULT '',
		tenant TEXT NOT NULL DEFAULT '',
		workspace TEXT NOT NULL DEFAULT '',
		generation INTEGER NOT NULL DEFAULT 0,
		operation_id TEXT NOT NULL DEFAULT '',
		producer_seq INTEGER NOT NULL DEFAULT 0,
		session TEXT NOT NULL DEFAULT ''
	); CREATE INDEX IF NOT EXISTS events_stream ON events(stream, seq);
	CREATE TABLE IF NOT EXISTS event_producers (
		node TEXT PRIMARY KEY,
		producer_seq INTEGER NOT NULL
	);`); err != nil {
		db.Close()
		return nil, err
	}
	// Additive migration for databases created before authoritative event
	// metadata. SQLite has no IF NOT EXISTS for ADD COLUMN, so inspect first.
	columns := []struct{ name, declaration string }{
		{"event_id", "TEXT NOT NULL DEFAULT ''"}, {"received_at", "INTEGER NOT NULL DEFAULT 0"},
		{"observed_at", "INTEGER NOT NULL DEFAULT 0"}, {"origin", "TEXT NOT NULL DEFAULT ''"},
		{"actor", "TEXT NOT NULL DEFAULT ''"}, {"tenant", "TEXT NOT NULL DEFAULT ''"},
		{"workspace", "TEXT NOT NULL DEFAULT ''"}, {"generation", "INTEGER NOT NULL DEFAULT 0"},
		{"operation_id", "TEXT NOT NULL DEFAULT ''"}, {"producer_seq", "INTEGER NOT NULL DEFAULT 0"},
		{"session", "TEXT NOT NULL DEFAULT ''"},
	}
	rows, err := db.Query(`PRAGMA table_info(events)`)
	if err != nil {
		db.Close()
		return nil, err
	}
	present := map[string]bool{}
	for rows.Next() {
		var cid int
		var name, typ string
		var notNull, pk int
		var defaultValue any
		if err := rows.Scan(&cid, &name, &typ, &notNull, &defaultValue, &pk); err != nil {
			rows.Close()
			db.Close()
			return nil, err
		}
		present[name] = true
	}
	if err := rows.Close(); err != nil {
		db.Close()
		return nil, err
	}
	for _, column := range columns {
		if !present[column.name] {
			if _, err := db.Exec(`ALTER TABLE events ADD COLUMN ` + column.name + ` ` + column.declaration); err != nil {
				db.Close()
				return nil, err
			}
		}
	}
	if _, err := db.Exec(`CREATE INDEX IF NOT EXISTS events_tenant ON events(tenant, seq);
		CREATE UNIQUE INDEX IF NOT EXISTS events_origin_id ON events(origin, event_id) WHERE event_id != '';`); err != nil {
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
	res, err := s.db.ExecContext(ctx, `INSERT INTO events(
		at, stream, principal, node, type, payload, cause, event_id, received_at,
		observed_at, origin, actor, tenant, workspace, generation, operation_id, producer_seq, session
	) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`,
		e.At, e.Stream, e.Principal, e.Node, e.Type, e.Payload, e.Cause, e.EventID, e.ReceivedAt,
		e.ObservedAt, e.Origin, e.Actor, e.Tenant, e.Workspace, e.Generation, e.OperationID, e.ProducerSeq, e.Session)
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
		rows, err = s.db.QueryContext(ctx, `SELECT seq, at, stream, principal, node, type, payload, cause,
			event_id, received_at, observed_at, origin, actor, tenant, workspace, generation, operation_id, producer_seq, session
			FROM events WHERE seq >= ? ORDER BY seq LIMIT ?`, from, limit)
	} else {
		rows, err = s.db.QueryContext(ctx, `SELECT seq, at, stream, principal, node, type, payload, cause,
			event_id, received_at, observed_at, origin, actor, tenant, workspace, generation, operation_id, producer_seq, session
			FROM events WHERE seq >= ? AND stream = ? ORDER BY seq LIMIT ?`, from, stream, limit)
	}
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []proto.Event
	for rows.Next() {
		var e proto.Event
		if err := rows.Scan(
			&e.Seq, &e.At, &e.Stream, &e.Principal, &e.Node, &e.Type, &e.Payload, &e.Cause,
			&e.EventID, &e.ReceivedAt, &e.ObservedAt, &e.Origin, &e.Actor, &e.Tenant,
			&e.Workspace, &e.Generation, &e.OperationID, &e.ProducerSeq, &e.Session,
		); err != nil {
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
	if last.Valid {
		return uint64(last.Int64), nil
	}
	var assigned sql.NullInt64
	if err := s.db.QueryRowContext(ctx, `SELECT seq FROM sqlite_sequence WHERE name='events'`).Scan(&assigned); errors.Is(err, sql.ErrNoRows) {
		return 0, nil
	} else if err != nil {
		return 0, err
	}
	return uint64(assigned.Int64), nil
}

func (s *SQLite) First(ctx context.Context) (uint64, error) {
	var first sql.NullInt64
	if err := s.db.QueryRowContext(ctx, `SELECT MIN(seq) FROM events`).Scan(&first); err != nil {
		return 0, err
	}
	if first.Valid {
		return uint64(first.Int64), nil
	}
	var assigned sql.NullInt64
	if err := s.db.QueryRowContext(ctx, `SELECT seq FROM sqlite_sequence WHERE name='events'`).Scan(&assigned); errors.Is(err, sql.ErrNoRows) {
		return 1, nil
	} else if err != nil {
		return 0, err
	}
	return uint64(assigned.Int64) + 1, nil
}

func (s *SQLite) Prune(ctx context.Context, beforeMillis int64, limit int) (int64, error) {
	if limit <= 0 {
		limit = 10_000
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, err
	}
	committed := false
	defer func() {
		if !committed {
			_ = tx.Rollback()
		}
	}()
	if _, err := tx.ExecContext(ctx, `INSERT INTO event_producers(node, producer_seq)
		SELECT node, MAX(producer_seq) FROM events
		WHERE origin='node' AND node != '' AND producer_seq > 0 GROUP BY node
		ON CONFLICT(node) DO UPDATE SET producer_seq=MAX(producer_seq, excluded.producer_seq)`); err != nil {
		return 0, err
	}
	// Remove only a contiguous prefix. Even if the wall clock moved backward,
	// retention must never punch silent holes into the middle of sequence space.
	res, err := tx.ExecContext(ctx, `DELETE FROM events WHERE seq IN (
		SELECT seq FROM events
		WHERE seq < COALESCE((SELECT MIN(seq) FROM events WHERE at >= ?), 9223372036854775807)
		ORDER BY seq LIMIT ?
	)`, beforeMillis, limit)
	if err != nil {
		return 0, err
	}
	n, err := res.RowsAffected()
	if err != nil {
		return 0, err
	}
	if err := tx.Commit(); err != nil {
		return 0, err
	}
	committed = true
	return n, nil
}

func (s *SQLite) PruneSize(ctx context.Context, max, limit int) (int64, error) {
	if max < 0 {
		return 0, errors.New("eventlog: maximum event count cannot be negative")
	}
	if limit <= 0 {
		limit = 10_000
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback()
	if _, err := tx.ExecContext(ctx, `INSERT INTO event_producers(node, producer_seq)
		SELECT node, MAX(producer_seq) FROM events
		WHERE origin='node' AND node != '' AND producer_seq > 0 GROUP BY node
		ON CONFLICT(node) DO UPDATE SET producer_seq=MAX(producer_seq, excluded.producer_seq)`); err != nil {
		return 0, err
	}
	var count int64
	if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM events`).Scan(&count); err != nil {
		return 0, err
	}
	remove := count - int64(max)
	if remove <= 0 {
		return 0, tx.Commit()
	}
	if remove > int64(limit) {
		remove = int64(limit)
	}
	result, err := tx.ExecContext(ctx, `DELETE FROM events WHERE seq IN (
		SELECT seq FROM events ORDER BY seq LIMIT ?
	)`, remove)
	if err != nil {
		return 0, err
	}
	removed, err := result.RowsAffected()
	if err != nil {
		return 0, err
	}
	if err := tx.Commit(); err != nil {
		return 0, err
	}
	return removed, nil
}

func (s *SQLite) Close() error { return s.db.Close() }
