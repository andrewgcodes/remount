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
	"math"
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
CREATE TABLE IF NOT EXISTS identity_principal_revisions (
  tenant TEXT NOT NULL,
  subject TEXT NOT NULL,
  revision INTEGER NOT NULL,
  revoked_at INTEGER NOT NULL,
  PRIMARY KEY (tenant, subject)
);
CREATE TABLE IF NOT EXISTS identity_principals (
  tenant TEXT NOT NULL,
  subject TEXT NOT NULL,
  roles BLOB NOT NULL,
  created_at INTEGER NOT NULL,
  updated_at INTEGER NOT NULL,
  PRIMARY KEY (tenant, subject)
);
CREATE INDEX IF NOT EXISTS identity_principals_tenant ON identity_principals(tenant, subject);
CREATE TABLE IF NOT EXISTS identity_enrollments (
  digest BLOB PRIMARY KEY,
  id TEXT NOT NULL,
  pool TEXT NOT NULL,
  tenant TEXT NOT NULL,
  issued_at INTEGER NOT NULL,
  expires_at INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS identity_enrollments_expiry ON identity_enrollments(expires_at);
CREATE TABLE IF NOT EXISTS identity_enrollment_labels (
  digest BLOB PRIMARY KEY,
  labels BLOB NOT NULL
);
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

// PrincipalRevision returns the durable invalidation epoch for a principal.
func (s *SQLiteStore) PrincipalRevision(ctx context.Context, tenant, subject string) (uint64, error) {
	var revision int64
	err := s.db.QueryRowContext(ctx, `SELECT revision FROM identity_principal_revisions WHERE tenant=? AND subject=?`, tenant, subject).Scan(&revision)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}
	if revision < 0 {
		return 0, errors.New("identity: corrupt principal revision")
	}
	return uint64(revision), nil
}

// RevokePrincipal commits a revision advance and its event atomically.
func (s *SQLiteStore) RevokePrincipal(ctx context.Context, tenant, subject string, now time.Time, event Event) (revision uint64, err error) {
	return s.RevokePrincipalWith(ctx, tenant, subject, now, event, nil)
}

// RevokePrincipalWith atomically advances the identity revision and lets the
// root control plane commit every derived authorization fence before success.
func (s *SQLiteStore) RevokePrincipalWith(ctx context.Context, tenant, subject string, now time.Time, event Event, commit func(*eventlog.Tx, uint64) error) (revision uint64, err error) {
	err = s.log.Transact(ctx, s.db, func(tx *eventlog.Tx) error {
		var current int64
		scanErr := tx.QueryRowContext(ctx, `SELECT revision FROM identity_principal_revisions WHERE tenant=? AND subject=?`, tenant, subject).Scan(&current)
		if scanErr != nil && !errors.Is(scanErr, sql.ErrNoRows) {
			return scanErr
		}
		if current < 0 || current == math.MaxInt64 {
			return errors.New("identity: principal revision exhausted or corrupt")
		}
		current++
		if _, err := tx.ExecContext(ctx, `INSERT INTO identity_principal_revisions(tenant,subject,revision,revoked_at) VALUES(?,?,?,?)
ON CONFLICT(tenant,subject) DO UPDATE SET revision=excluded.revision, revoked_at=excluded.revoked_at`, tenant, subject, current, now.Unix()); err != nil {
			return err
		}
		revision, event.Revision = uint64(current), uint64(current)
		if commit != nil {
			if err := commit(tx, revision); err != nil {
				return err
			}
		}
		return tx.Emit(identityEvent(now, event))
	})
	return revision, normalizeDrain(err)
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

// ConsumeRefresh atomically makes one refresh credential single-use.
func (s *SQLiteStore) ConsumeRefresh(ctx context.Context, id string, expires time.Time, event Event) (consumed bool, err error) {
	if id == "" || expires.IsZero() {
		return false, errors.New("identity: invalid refresh consumption")
	}
	err = s.log.Transact(ctx, s.db, func(tx *eventlog.Tx) error {
		var existing int64
		scanErr := tx.QueryRowContext(ctx, `SELECT expires_at FROM identity_revocations WHERE token_id=?`, id).Scan(&existing)
		if scanErr == nil {
			return nil
		}
		if !errors.Is(scanErr, sql.ErrNoRows) {
			return scanErr
		}
		if _, err := tx.ExecContext(ctx, `INSERT INTO identity_revocations(token_id, expires_at) VALUES(?, ?)`, id, expires.Unix()); err != nil {
			return err
		}
		consumed = true
		return tx.Emit(identityEvent(time.Now(), event))
	})
	return consumed, normalizeDrain(err)
}

// PutEnrollment commits only the digest and non-secret authority plus event.
func (s *SQLiteStore) PutEnrollment(ctx context.Context, hash [32]byte, enrollment Enrollment, event Event) error {
	return s.PutEnrollmentBounded(ctx, hash, enrollment, math.MaxInt, event)
}

// PutEnrollmentBounded expires a bounded batch, checks live admission and
// commits the digest and event in one transaction.
func (s *SQLiteStore) PutEnrollmentBounded(ctx context.Context, hash [32]byte, enrollment Enrollment, max int, event Event) error {
	atCapacity := false
	err := s.log.Transact(ctx, s.db, func(tx *eventlog.Tx) error {
		rows, err := tx.QueryContext(ctx, `SELECT digest,id,pool,tenant FROM identity_enrollments WHERE expires_at<=? ORDER BY expires_at LIMIT 128`, enrollment.IssuedAt.Unix())
		if err != nil {
			return err
		}
		type expiredEnrollment struct {
			digest           []byte
			id, pool, tenant string
		}
		var expired []expiredEnrollment
		for rows.Next() {
			var item expiredEnrollment
			if err := rows.Scan(&item.digest, &item.id, &item.pool, &item.tenant); err != nil {
				rows.Close()
				return err
			}
			item.digest = append([]byte(nil), item.digest...)
			expired = append(expired, item)
		}
		if err := rows.Close(); err != nil {
			return err
		}
		for _, item := range expired {
			if _, err := tx.ExecContext(ctx, `DELETE FROM identity_enrollment_labels WHERE digest=?`, item.digest); err != nil {
				return err
			}
			if _, err := tx.ExecContext(ctx, `DELETE FROM identity_enrollments WHERE digest=?`, item.digest); err != nil {
				return err
			}
			if err := tx.Emit(identityEvent(enrollment.IssuedAt, Event{Type: "identity.enrollment_expired", ID: item.id, Pool: item.pool, Tenant: item.tenant})); err != nil {
				return err
			}
		}
		var live int
		if err := tx.QueryRowContext(ctx, `SELECT count(*) FROM identity_enrollments`).Scan(&live); err != nil {
			return err
		}
		if live >= max {
			atCapacity = true
			return nil
		}
		if _, err := tx.ExecContext(ctx, `INSERT INTO identity_enrollments(digest,id,pool,tenant,issued_at,expires_at) VALUES(?,?,?,?,?,?)`,
			hash[:], enrollment.ID, enrollment.Pool, enrollment.Tenant, enrollment.IssuedAt.Unix(), enrollment.ExpiresAt.Unix()); err != nil {
			return fmt.Errorf("identity: store enrollment: %w", err)
		}
		labels, err := json.Marshal(enrollment.Labels)
		if err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, `INSERT INTO identity_enrollment_labels(digest,labels) VALUES(?,?)`, hash[:], labels); err != nil {
			return err
		}
		return tx.Emit(identityEvent(enrollment.IssuedAt, event))
	})
	if err := normalizeDrain(err); err != nil {
		return err
	}
	if atCapacity {
		return ErrEnrollmentCapacity
	}
	return nil
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
		var enrollmentLabels []byte
		labelsErr := tx.QueryRowContext(ctx, `SELECT labels FROM identity_enrollment_labels WHERE digest=?`, hash[:]).Scan(&enrollmentLabels)
		if labelsErr != nil && !errors.Is(labelsErr, sql.ErrNoRows) {
			return labelsErr
		}
		if len(enrollmentLabels) > 0 && json.Unmarshal(enrollmentLabels, &enrollment.Labels) != nil {
			return errors.New("identity: corrupt enrollment labels")
		}
		if _, err := tx.ExecContext(ctx, `DELETE FROM identity_enrollments WHERE digest=?`, hash[:]); err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, `DELETE FROM identity_enrollment_labels WHERE digest=?`, hash[:]); err != nil {
			return err
		}
		if !enrollment.ExpiresAt.After(now) {
			return tx.Emit(identityEvent(now, Event{Type: "identity.enrollment_expired", ID: enrollment.ID, Pool: enrollment.Pool, Tenant: enrollment.Tenant}))
		}
		binding = cloneBinding(candidate)
		binding.Pool, binding.Tenant, binding.EnrolledAt = enrollment.Pool, enrollment.Tenant, now
		binding.Labels = cloneLabels(enrollment.Labels)
		if binding.Labels == nil {
			binding.Labels = map[string]string{}
		}
		binding.Labels["pool"], binding.Labels["tenant"] = binding.Pool, binding.Tenant
		binding.Labels["remount.pool"], binding.Labels["remount.node"] = binding.Pool, binding.NodeID
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
	actor := event.Actor
	if actor == "" {
		actor = event.Subject
	}
	return &proto.Event{
		EventID: ids.New("ev"), At: now.UnixMilli(), ReceivedAt: now.UnixMilli(),
		Origin: "control", Actor: actor, Principal: event.Subject,
		Tenant: event.Tenant, Node: nodeForEvent(event), Stream: streamForEvent(event),
		Type: event.Type, Payload: proto.MustMarshal(map[string]any{"id": event.ID, "pool": event.Pool, "revision": event.Revision, "roles": event.Roles}),
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

// CollectExpired durably removes a bounded batch of expired retained
// authority and emits one event per removal in the same transaction.
func (s *SQLiteStore) CollectExpired(ctx context.Context, now time.Time, limit int) (result MaintenanceResult, err error) {
	if limit < 1 || limit > 10_000 {
		return result, errors.New("identity: invalid maintenance limit")
	}
	err = s.log.Transact(ctx, s.db, func(tx *eventlog.Tx) error {
		rows, err := tx.QueryContext(ctx, `SELECT token_id FROM identity_revocations WHERE expires_at<=? ORDER BY expires_at LIMIT ?`, now.Unix(), limit)
		if err != nil {
			return err
		}
		var tokenIDs []string
		for rows.Next() {
			var id string
			if err := rows.Scan(&id); err != nil {
				rows.Close()
				return err
			}
			tokenIDs = append(tokenIDs, id)
		}
		if err := rows.Close(); err != nil {
			return err
		}
		for _, id := range tokenIDs {
			changed, err := tx.ExecContext(ctx, `DELETE FROM identity_revocations WHERE token_id=? AND expires_at<=?`, id, now.Unix())
			if err != nil {
				return err
			}
			count, err := changed.RowsAffected()
			if err != nil {
				return err
			}
			if count == 1 {
				result.Revocations++
				if err := tx.Emit(identityEvent(now, Event{Type: "identity.revocation_expired", ID: id})); err != nil {
					return err
				}
			}
		}
		remaining := limit - result.Revocations
		if remaining == 0 {
			return nil
		}
		rows, err = tx.QueryContext(ctx, `SELECT digest,id,pool,tenant FROM identity_enrollments WHERE expires_at<=? ORDER BY expires_at LIMIT ?`, now.Unix(), remaining)
		if err != nil {
			return err
		}
		type expired struct {
			digest           []byte
			id, pool, tenant string
		}
		var enrollments []expired
		for rows.Next() {
			var item expired
			if err := rows.Scan(&item.digest, &item.id, &item.pool, &item.tenant); err != nil {
				rows.Close()
				return err
			}
			item.digest = append([]byte(nil), item.digest...)
			enrollments = append(enrollments, item)
		}
		if err := rows.Close(); err != nil {
			return err
		}
		for _, item := range enrollments {
			if _, err := tx.ExecContext(ctx, `DELETE FROM identity_enrollment_labels WHERE digest=?`, item.digest); err != nil {
				return err
			}
			changed, err := tx.ExecContext(ctx, `DELETE FROM identity_enrollments WHERE digest=? AND expires_at<=?`, item.digest, now.Unix())
			if err != nil {
				return err
			}
			count, err := changed.RowsAffected()
			if err != nil {
				return err
			}
			if count == 1 {
				result.Enrollments++
				if err := tx.Emit(identityEvent(now, Event{Type: "identity.enrollment_expired", ID: item.id, Pool: item.pool, Tenant: item.tenant})); err != nil {
					return err
				}
			}
		}
		return nil
	})
	return result, normalizeDrain(err)
}

var _ Store = (*SQLiteStore)(nil)
var _ Maintainer = (*SQLiteStore)(nil)
