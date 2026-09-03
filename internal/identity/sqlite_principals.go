package identity

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"slices"
	"time"

	"remount.dev/remount/internal/eventlog"
	"remount.dev/remount/internal/proto"
)

// CreatePrincipal commits the role assignment and audit record together.
func (s *SQLiteStore) CreatePrincipal(ctx context.Context, principal proto.Principal, event Event) (result proto.Principal, err error) {
	roles, err := normalizeRoles(principal.Roles)
	if err != nil || !validPrincipal(principal.ID, principal.Tenant) {
		return result, errors.New("identity: invalid principal")
	}
	principal.Roles = roles
	now := time.UnixMilli(principal.CreatedAt)
	if principal.CreatedAt == 0 || principal.UpdatedAt == 0 {
		now = time.Now()
		principal.CreatedAt, principal.UpdatedAt = now.UnixMilli(), now.UnixMilli()
	}
	encoded, _ := json.Marshal(roles)
	err = s.log.Transact(ctx, s.db, func(tx *eventlog.Tx) error {
		insert, err := tx.ExecContext(ctx, `INSERT OR IGNORE INTO identity_principals(tenant,subject,roles,created_at,updated_at) VALUES(?,?,?,?,?)`,
			principal.Tenant, principal.ID, encoded, principal.CreatedAt, principal.UpdatedAt)
		if err != nil {
			return err
		}
		rows, err := insert.RowsAffected()
		if err != nil {
			return err
		}
		if rows == 0 {
			return ErrPrincipalExists
		}
		if _, err := tx.ExecContext(ctx, `INSERT OR IGNORE INTO identity_principal_revisions(tenant,subject,revision,revoked_at) VALUES(?,?,0,0)`, principal.Tenant, principal.ID); err != nil {
			return err
		}
		return tx.Emit(identityEvent(now, event))
	})
	if err = normalizeDrain(err); err != nil {
		return proto.Principal{}, err
	}
	principal.Revision = 0
	return clonePrincipal(principal), nil
}

// GetPrincipal reads one exact tenant-scoped role assignment.
func (s *SQLiteStore) GetPrincipal(ctx context.Context, tenant, subject string) (proto.Principal, error) {
	var principal proto.Principal
	var roles []byte
	var revision int64
	err := s.db.QueryRowContext(ctx, `SELECT p.subject,p.tenant,p.roles,COALESCE(r.revision,0),p.created_at,p.updated_at
FROM identity_principals p LEFT JOIN identity_principal_revisions r ON r.tenant=p.tenant AND r.subject=p.subject
WHERE p.tenant=? AND p.subject=?`, tenant, subject).Scan(&principal.ID, &principal.Tenant, &roles, &revision, &principal.CreatedAt, &principal.UpdatedAt)
	if errors.Is(err, sql.ErrNoRows) {
		return proto.Principal{}, ErrPrincipalNotFound
	}
	if err != nil || revision < 0 || json.Unmarshal(roles, &principal.Roles) != nil {
		if err != nil {
			return proto.Principal{}, err
		}
		return proto.Principal{}, errors.New("identity: corrupt principal record")
	}
	principal.Revision = uint64(revision)
	return clonePrincipal(principal), nil
}

// ListPrincipals returns principals in stable subject order.
func (s *SQLiteStore) ListPrincipals(ctx context.Context, tenant string) ([]proto.Principal, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT p.subject,p.tenant,p.roles,COALESCE(r.revision,0),p.created_at,p.updated_at
FROM identity_principals p LEFT JOIN identity_principal_revisions r ON r.tenant=p.tenant AND r.subject=p.subject
WHERE p.tenant=? ORDER BY p.subject LIMIT 10001`, tenant)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	result := make([]proto.Principal, 0)
	for rows.Next() {
		var principal proto.Principal
		var roles []byte
		var revision int64
		if err := rows.Scan(&principal.ID, &principal.Tenant, &roles, &revision, &principal.CreatedAt, &principal.UpdatedAt); err != nil {
			return nil, err
		}
		if revision < 0 || json.Unmarshal(roles, &principal.Roles) != nil {
			return nil, errors.New("identity: corrupt principal record")
		}
		principal.Revision = uint64(revision)
		result = append(result, clonePrincipal(principal))
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	if len(result) > 10_000 {
		return nil, errors.New("identity: principal directory exceeds bound")
	}
	return result, nil
}

// UpsertPrincipalRoles synchronizes federated roles and invalidates older
// credentials exactly when the authoritative role set changes.
func (s *SQLiteStore) UpsertPrincipalRoles(ctx context.Context, principal proto.Principal, event Event) (result proto.Principal, changed bool, err error) {
	roles, err := normalizeRoles(principal.Roles)
	if err != nil || !validPrincipal(principal.ID, principal.Tenant) {
		return result, false, errors.New("identity: invalid principal")
	}
	principal.Roles = roles
	now := time.UnixMilli(principal.UpdatedAt)
	if principal.UpdatedAt == 0 {
		now = time.Now()
		principal.UpdatedAt = now.UnixMilli()
	}
	if principal.CreatedAt == 0 {
		principal.CreatedAt = principal.UpdatedAt
	}
	err = s.log.Transact(ctx, s.db, func(tx *eventlog.Tx) error {
		var raw []byte
		var created int64
		scanErr := tx.QueryRowContext(ctx, `SELECT roles,created_at FROM identity_principals WHERE tenant=? AND subject=?`, principal.Tenant, principal.ID).Scan(&raw, &created)
		if scanErr != nil && !errors.Is(scanErr, sql.ErrNoRows) {
			return scanErr
		}
		var oldRoles []string
		if scanErr == nil && json.Unmarshal(raw, &oldRoles) != nil {
			return errors.New("identity: corrupt principal roles")
		}
		if scanErr == nil && slices.Equal(oldRoles, roles) {
			return nil
		}
		changed = true
		encoded, _ := json.Marshal(roles)
		if errors.Is(scanErr, sql.ErrNoRows) {
			if _, err := tx.ExecContext(ctx, `INSERT INTO identity_principals(tenant,subject,roles,created_at,updated_at) VALUES(?,?,?,?,?)`, principal.Tenant, principal.ID, encoded, principal.CreatedAt, principal.UpdatedAt); err != nil {
				return err
			}
			if _, err := tx.ExecContext(ctx, `INSERT OR IGNORE INTO identity_principal_revisions(tenant,subject,revision,revoked_at) VALUES(?,?,0,0)`, principal.Tenant, principal.ID); err != nil {
				return err
			}
			event.Type = proto.EvIdentityPrincipalCreated
		} else {
			principal.CreatedAt = created
			if _, err := tx.ExecContext(ctx, `UPDATE identity_principals SET roles=?,updated_at=? WHERE tenant=? AND subject=?`, encoded, principal.UpdatedAt, principal.Tenant, principal.ID); err != nil {
				return err
			}
			if _, err := tx.ExecContext(ctx, `INSERT INTO identity_principal_revisions(tenant,subject,revision,revoked_at) VALUES(?,?,1,?)
ON CONFLICT(tenant,subject) DO UPDATE SET revision=revision+1, revoked_at=excluded.revoked_at`, principal.Tenant, principal.ID, principal.UpdatedAt); err != nil {
				return err
			}
		}
		if err := tx.QueryRowContext(ctx, `SELECT revision FROM identity_principal_revisions WHERE tenant=? AND subject=?`, principal.Tenant, principal.ID).Scan(&principal.Revision); err != nil {
			return err
		}
		event.Revision, event.Roles = principal.Revision, roles
		return tx.Emit(identityEvent(now, event))
	})
	return clonePrincipal(principal), changed, normalizeDrain(err)
}

func clonePrincipal(principal proto.Principal) proto.Principal {
	principal.Roles = append([]string(nil), principal.Roles...)
	return principal
}
