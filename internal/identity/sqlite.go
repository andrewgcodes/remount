package identity

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/subtle"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"remount.dev/remount/internal/eventlog"
	"remount.dev/remount/internal/ids"
	"remount.dev/remount/internal/proto"
)

// SQLiteStore persists identity state in the control-plane database and uses
// the canonical event outbox for atomic resource/event commits.
type SQLiteStore struct {
	db  *sql.DB
	log *eventlog.Log
}

// NewSQLiteStore creates the identity tables in the shared control database.
func NewSQLiteStore(db *sql.DB, log *eventlog.Log) (*SQLiteStore, error) {
	if db == nil || log == nil {
		return nil, errors.New("identity: DB and event log are required")
	}
	if err := eventlog.CreateOutbox(db); err != nil {
		return nil, err
	}
	if _, err := db.Exec(`
CREATE TABLE IF NOT EXISTS identity_revocations (
  token_id TEXT PRIMARY KEY,
  expires_at INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS identity_revocations_expiry ON identity_revocations(expires_at);
CREATE TABLE IF NOT EXISTS identity_enrollments (
  digest BLOB PRIMARY KEY,
  id TEXT NOT NULL,
  pool TEXT NOT NULL,
  tenant TEXT NOT NULL,
  issued_at INTEGER NOT NULL,
  expires_at INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS identity_enrollments_expiry ON identity_enrollments(expires_at);
CREATE TABLE IF NOT EXISTS identity_nodes (
  node_id TEXT PRIMARY KEY,
  pubkey BLOB NOT NULL,
  pool TEXT NOT NULL,
  tenant TEXT NOT NULL,
  labels BLOB NOT NULL,
  enrolled_at INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS identity_keys (
  name TEXT PRIMARY KEY,
  private_key BLOB NOT NULL
);`); err != nil {
		return nil, err
	}
	return &SQLiteStore{db: db, log: log}, nil
}

// LoadOrCreateSigningKey returns the durable Ed25519 token-signing key.
func (s *SQLiteStore) LoadOrCreateSigningKey(ctx context.Context) (ed25519.PrivateKey, error) {
	var raw []byte
	err := s.db.QueryRowContext(ctx, `SELECT private_key FROM identity_keys WHERE name='tokens'`).Scan(&raw)
	if err == nil {
		if len(raw) != ed25519.PrivateKeySize {
			return nil, errors.New("identity: stored signing key has invalid size")
		}
		return ed25519.PrivateKey(append([]byte(nil), raw...)), nil
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return nil, err
	}
	_, key, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		return nil, err
	}
	result, err := s.db.ExecContext(ctx, `INSERT OR IGNORE INTO identity_keys(name, private_key) VALUES('tokens', ?)`, []byte(key))
	if err != nil {
		return nil, err
	}
	inserted, err := result.RowsAffected()
	if err != nil {
		return nil, err
	}
	if inserted == 1 {
		return key, nil
	}
	return s.LoadOrCreateSigningKey(ctx)
}

// Revoked reports a live revocation. Expired rows are removed together with
// an expiry event so the retained set is bounded by token lifetime.
func (s *SQLiteStore) Revoked(ctx context.Context, id string, now time.Time) (bool, error) {
	var expires int64
	err := s.db.QueryRowContext(ctx, `SELECT expires_at FROM identity_revocations WHERE token_id=?`, id).Scan(&expires)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	if time.Unix(expires, 0).After(now) {
		return true, nil
	}
	err = s.log.Transact(ctx, s.db, func(tx *eventlog.Tx) error {
		result, err := tx.ExecContext(ctx, `DELETE FROM identity_revocations WHERE token_id=? AND expires_at<=?`, id, now.Unix())
		if err != nil {
			return err
		}
		removed, err := result.RowsAffected()
		if err != nil || removed == 0 {
			return err
		}
		return tx.Emit(identityEvent(now, Event{Type: "identity.revocation_expired", ID: id}))
	})
	return false, normalizeDrain(err)
}

// Revoke commits a revocation and its event atomically.
func (s *SQLiteStore) Revoke(ctx context.Context, id string, expires time.Time, event Event) error {
	if id == "" || expires.IsZero() {
		return errors.New("identity: invalid revocation")
	}
	err := s.log.Transact(ctx, s.db, func(tx *eventlog.Tx) error {
		var existing int64
		err := tx.QueryRowContext(ctx, `SELECT expires_at FROM identity_revocations WHERE token_id=?`, id).Scan(&existing)
		if err == nil && existing >= expires.Unix() {
			return nil
		}
		if err != nil && !errors.Is(err, sql.ErrNoRows) {
			return err
		}
		if _, err := tx.ExecContext(ctx, `INSERT INTO identity_revocations(token_id, expires_at) VALUES(?, ?)
ON CONFLICT(token_id) DO UPDATE SET expires_at=max(expires_at, excluded.expires_at)`, id, expires.Unix()); err != nil {
			return err
		}
		return tx.Emit(identityEvent(time.Now(), event))
	})
	return normalizeDrain(err)
}

// PutEnrollment commits only the digest and non-secret authority plus event.
func (s *SQLiteStore) PutEnrollment(ctx context.Context, hash [32]byte, enrollment Enrollment, event Event) error {
	err := s.log.Transact(ctx, s.db, func(tx *eventlog.Tx) error {
		if _, err := tx.ExecContext(ctx, `INSERT INTO identity_enrollments(digest,id,pool,tenant,issued_at,expires_at) VALUES(?,?,?,?,?,?)`,
			hash[:], enrollment.ID, enrollment.Pool, enrollment.Tenant, enrollment.IssuedAt.Unix(), enrollment.ExpiresAt.Unix()); err != nil {
			return fmt.Errorf("identity: store enrollment: %w", err)
		}
		return tx.Emit(identityEvent(enrollment.IssuedAt, event))
	})
	return normalizeDrain(err)
}

// EnrollNode verifies an existing key binding or atomically consumes one live
// enrollment, creates the binding and emits node.enrolled.
func (s *SQLiteStore) EnrollNode(ctx context.Context, hash [32]byte, now time.Time, candidate NodeBinding, event Event) (binding NodeBinding, fresh, ok bool, err error) {
	err = s.log.Transact(ctx, s.db, func(tx *eventlog.Tx) error {
		var labels []byte
		var enrolledAt int64
		scanErr := tx.QueryRowContext(ctx, `SELECT pubkey,pool,tenant,labels,enrolled_at FROM identity_nodes WHERE node_id=?`, candidate.NodeID).
			Scan(&binding.PubKey, &binding.Pool, &binding.Tenant, &labels, &enrolledAt)
		if scanErr == nil {
			binding.NodeID, binding.EnrolledAt = candidate.NodeID, time.Unix(enrolledAt, 0)
			if err := json.Unmarshal(labels, &binding.Labels); err != nil {
				return errors.New("identity: corrupt node labels")
			}
			ok = subtle.ConstantTimeCompare(binding.PubKey, candidate.PubKey) == 1
			return nil
		}
		if !errors.Is(scanErr, sql.ErrNoRows) {
			return scanErr
		}
		if candidate.NodeID == "" || len(candidate.PubKey) != ed25519.PublicKeySize {
			return nil
		}
		var enrollment Enrollment
		var issuedAt, expiresAt int64
		scanErr = tx.QueryRowContext(ctx, `SELECT id,pool,tenant,issued_at,expires_at FROM identity_enrollments WHERE digest=?`, hash[:]).
			Scan(&enrollment.ID, &enrollment.Pool, &enrollment.Tenant, &issuedAt, &expiresAt)
		if errors.Is(scanErr, sql.ErrNoRows) {
			return nil
		}
		if scanErr != nil {
			return scanErr
		}
		enrollment.IssuedAt, enrollment.ExpiresAt = time.Unix(issuedAt, 0), time.Unix(expiresAt, 0)
		if _, err := tx.ExecContext(ctx, `DELETE FROM identity_enrollments WHERE digest=?`, hash[:]); err != nil {
			return err
		}
		if !enrollment.ExpiresAt.After(now) {
			return tx.Emit(identityEvent(now, Event{Type: "identity.enrollment_expired", ID: enrollment.ID, Pool: enrollment.Pool, Tenant: enrollment.Tenant}))
		}
		binding = cloneBinding(candidate)
		binding.Pool, binding.Tenant, binding.EnrolledAt = enrollment.Pool, enrollment.Tenant, now
		if binding.Labels == nil {
			binding.Labels = map[string]string{}
		}
		binding.Labels["pool"], binding.Labels["tenant"] = binding.Pool, binding.Tenant
		labels, err = json.Marshal(binding.Labels)
		if err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, `INSERT INTO identity_nodes(node_id,pubkey,pool,tenant,labels,enrolled_at) VALUES(?,?,?,?,?,?)`,
			binding.NodeID, binding.PubKey, binding.Pool, binding.Tenant, labels, binding.EnrolledAt.Unix()); err != nil {
			return err
		}
		event.ID, event.Pool, event.Tenant = binding.NodeID, binding.Pool, binding.Tenant
		if err := tx.Emit(identityEvent(now, event)); err != nil {
			return err
		}
		fresh, ok = true, true
		return nil
	})
	return cloneBinding(binding), fresh, ok, normalizeDrain(err)
}

func identityEvent(now time.Time, event Event) *proto.Event {
	return &proto.Event{
		EventID: ids.New("ev"), At: now.UnixMilli(), ReceivedAt: now.UnixMilli(),
		Origin: "control", Actor: event.Subject, Principal: event.Subject,
		Tenant: event.Tenant, Node: nodeForEvent(event), Stream: streamForEvent(event),
		Type: event.Type, Payload: proto.MustMarshal(map[string]string{"id": event.ID, "pool": event.Pool}),
	}
}

func nodeForEvent(event Event) string {
	if event.Type == proto.EvNodeEnrolled {
		return event.ID
	}
	return ""
}

func streamForEvent(event Event) string {
	if event.Type == proto.EvNodeEnrolled {
		return event.ID
	}
	return "identity"
}

func normalizeDrain(err error) error {
	var deferred *eventlog.DrainError
	if errors.As(err, &deferred) {
		return nil
	}
	return err
}

var _ Store = (*SQLiteStore)(nil)
