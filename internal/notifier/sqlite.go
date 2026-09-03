package notifier

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"math"
	"strings"
)

const (
	maximumDeadLettersPerTenant = 1_000_000
	maximumDeadLetterPage       = 1000
	maximumDeadLetterTypes      = 64
)

var (
	// ErrDeadLetterCapacity means a tenant's retained dead-letter allowance is
	// full. A notifier must retain its export cursor until pruning makes room.
	ErrDeadLetterCapacity = errors.New("notifier: dead-letter capacity exhausted")
	// ErrInvalidDeadLetter means a caller supplied metadata outside the
	// intentionally narrow sanitized schema.
	ErrInvalidDeadLetter = errors.New("notifier: invalid dead-letter metadata")
)

// DeadLetterRecord is one durable dead-letter row and its monotonic local ID.
type DeadLetterRecord struct {
	ID int64
	DeadLetter
}

// SQLiteDeadLetterStore retains a bounded number of sanitized rows per tenant.
// The caller owns db and remains responsible for closing it.
type SQLiteDeadLetterStore struct {
	db           *sql.DB
	maxPerTenant int
}

// NewSQLiteDeadLetterStore initializes a dead-letter store on db. Schema
// creation is idempotent so reopening the same database preserves pending
// operator evidence across process restarts.
func NewSQLiteDeadLetterStore(db *sql.DB, maxPerTenant int) (*SQLiteDeadLetterStore, error) {
	if db == nil || maxPerTenant < 1 || maxPerTenant > maximumDeadLettersPerTenant {
		return nil, errors.New("notifier: database and bounded per-tenant capacity are required")
	}
	statements := []string{
		`CREATE TABLE IF NOT EXISTS notifier_dead_letters (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			subscription_id TEXT NOT NULL,
			tenant TEXT NOT NULL,
			first_seq INTEGER NOT NULL,
			last_seq INTEGER NOT NULL,
			event_types BLOB NOT NULL,
			attempts INTEGER NOT NULL,
			reason TEXT NOT NULL,
			failed_at INTEGER NOT NULL
		)`,
		`CREATE INDEX IF NOT EXISTS notifier_dead_letters_tenant_time
			ON notifier_dead_letters(tenant, failed_at, id)`,
	}
	for _, statement := range statements {
		if _, err := db.Exec(statement); err != nil {
			return nil, errors.New("notifier: dead-letter schema unavailable")
		}
	}
	return &SQLiteDeadLetterStore{db: db, maxPerTenant: maxPerTenant}, nil
}

// Store atomically admits a row only while the tenant remains below capacity.
func (s *SQLiteDeadLetterStore) Store(ctx context.Context, letter DeadLetter) error {
	if err := validateDeadLetter(letter); err != nil {
		return err
	}
	eventTypes, err := json.Marshal(letter.EventTypes)
	if err != nil {
		return ErrInvalidDeadLetter
	}
	result, err := s.db.ExecContext(ctx, `
		INSERT INTO notifier_dead_letters
			(subscription_id, tenant, first_seq, last_seq, event_types, attempts, reason, failed_at)
		SELECT ?, ?, ?, ?, ?, ?, ?, ?
		WHERE (SELECT COUNT(*) FROM notifier_dead_letters WHERE tenant = ?) < ?`,
		letter.SubscriptionID, letter.Tenant, int64(letter.FirstSeq), int64(letter.LastSeq), eventTypes,
		letter.Attempts, letter.Reason, letter.FailedAt, letter.Tenant, s.maxPerTenant,
	)
	if err != nil {
		return ErrDeadLetterUnavailable
	}
	inserted, err := result.RowsAffected()
	if err != nil {
		return ErrDeadLetterUnavailable
	}
	if inserted != 1 {
		return ErrDeadLetterCapacity
	}
	return nil
}

// List returns at most limit rows for tenant with ID greater than afterID.
func (s *SQLiteDeadLetterStore) List(ctx context.Context, tenant string, afterID int64, limit int) ([]DeadLetterRecord, error) {
	if !safeIdentifier(tenant) || afterID < 0 || limit < 1 || limit > maximumDeadLetterPage {
		return nil, ErrInvalidDeadLetter
	}
	rows, err := s.db.QueryContext(ctx, `
		SELECT id, subscription_id, tenant, first_seq, last_seq, event_types, attempts, reason, failed_at
		FROM notifier_dead_letters
		WHERE tenant = ? AND id > ?
		ORDER BY id
		LIMIT ?`, tenant, afterID, limit)
	if err != nil {
		return nil, ErrDeadLetterUnavailable
	}
	defer rows.Close()
	records := make([]DeadLetterRecord, 0, limit)
	for rows.Next() {
		var record DeadLetterRecord
		var first, last int64
		var eventTypes []byte
		if err := rows.Scan(
			&record.ID, &record.SubscriptionID, &record.Tenant, &first, &last,
			&eventTypes, &record.Attempts, &record.Reason, &record.FailedAt,
		); err != nil {
			return nil, ErrDeadLetterUnavailable
		}
		if first < 0 || last < 0 || json.Unmarshal(eventTypes, &record.EventTypes) != nil {
			return nil, ErrDeadLetterUnavailable
		}
		record.FirstSeq = uint64(first)
		record.LastSeq = uint64(last)
		if validateErr := validateDeadLetter(record.DeadLetter); validateErr != nil {
			return nil, ErrDeadLetterUnavailable
		}
		records = append(records, record)
	}
	if err := rows.Err(); err != nil {
		return nil, ErrDeadLetterUnavailable
	}
	return records, nil
}

// Prune removes at most limit rows for tenant older than beforeMillis.
func (s *SQLiteDeadLetterStore) Prune(ctx context.Context, tenant string, beforeMillis int64, limit int) (int64, error) {
	if !safeIdentifier(tenant) || beforeMillis < 0 || limit < 1 || limit > maximumDeadLetterPage {
		return 0, ErrInvalidDeadLetter
	}
	result, err := s.db.ExecContext(ctx, `
		DELETE FROM notifier_dead_letters
		WHERE id IN (
			SELECT id FROM notifier_dead_letters
			WHERE tenant = ? AND failed_at < ?
			ORDER BY id
			LIMIT ?
		)`, tenant, beforeMillis, limit)
	if err != nil {
		return 0, ErrDeadLetterUnavailable
	}
	removed, err := result.RowsAffected()
	if err != nil {
		return 0, ErrDeadLetterUnavailable
	}
	return removed, nil
}

func validateDeadLetter(letter DeadLetter) error {
	if !safeIdentifier(letter.SubscriptionID) || !safeIdentifier(letter.Tenant) ||
		letter.FirstSeq == 0 || letter.FirstSeq > math.MaxInt64 ||
		letter.LastSeq < letter.FirstSeq || letter.LastSeq > math.MaxInt64 ||
		letter.Attempts < 0 || letter.Attempts > maximumAttempts || letter.FailedAt < 0 ||
		len(letter.EventTypes) == 0 || len(letter.EventTypes) > maximumDeadLetterTypes || !safeReason(letter.Reason) {
		return ErrInvalidDeadLetter
	}
	for _, eventType := range letter.EventTypes {
		if !allowedDeliveredType(eventType) {
			return ErrInvalidDeadLetter
		}
	}
	return nil
}

func safeIdentifier(value string) bool {
	if value == "" || len(value) > 128 {
		return false
	}
	for _, character := range value {
		if (character < 'a' || character > 'z') && (character < 'A' || character > 'Z') &&
			(character < '0' || character > '9') && !strings.ContainsRune("._:/-", character) {
			return false
		}
	}
	return true
}

func safeReason(reason string) bool {
	switch reason {
	case "encode_failed", "cancelled", "delivery_failed", "destination_unavailable":
		return true
	default:
		return false
	}
}

func allowedDeliveredType(eventType string) bool {
	return allowedFilter(eventType) || strings.HasPrefix(eventType, "pool.")
}

var _ DeadLetterStore = (*SQLiteDeadLetterStore)(nil)
