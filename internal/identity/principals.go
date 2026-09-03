package identity

import (
	"context"
	"errors"
	"slices"
	"time"

	"remount.dev/remount/internal/ids"
	"remount.dev/remount/internal/proto"
)

var (
	// ErrPrincipalExists means the tenant already has this principal.
	ErrPrincipalExists = errors.New("identity: principal already exists")
	// ErrPrincipalNotFound means the exact tenant principal does not exist.
	ErrPrincipalNotFound = errors.New("identity: principal not found")
	// ErrRoleNotAssigned means token issuance requested authority the principal lacks.
	ErrRoleNotAssigned = errors.New("identity: role is not assigned to principal")
)

// PrincipalDirectoryStore owns durable tenant-scoped role assignments.
type PrincipalDirectoryStore interface {
	CreatePrincipal(context.Context, proto.Principal, Event) (proto.Principal, error)
	GetPrincipal(context.Context, string, string) (proto.Principal, error)
	ListPrincipals(context.Context, string) ([]proto.Principal, error)
	UpsertPrincipalRoles(context.Context, proto.Principal, Event) (proto.Principal, bool, error)
}

// CreatePrincipal commits one tenant-scoped role assignment and its audit event.
func (m *Manager) CreatePrincipal(ctx context.Context, tenant, subject string, roles []string, actor string) (proto.Principal, error) {
	roles, err := normalizeRoles(roles)
	if err != nil || !validPrincipal(subject, tenant) || !validText(actor, 256) || slices.Contains(roles, RoleNode) {
		return proto.Principal{}, errors.New("identity: invalid principal, actor, or roles")
	}
	store, ok := m.store.(PrincipalDirectoryStore)
	if !ok {
		return proto.Principal{}, errors.New("identity: principal directory is unavailable")
	}
	now := m.now()
	principal, err := store.CreatePrincipal(ctx, proto.Principal{ID: subject, Tenant: tenant, Roles: roles, CreatedAt: now.UnixMilli(), UpdatedAt: now.UnixMilli()}, Event{
		Type: proto.EvIdentityPrincipalCreated, Subject: subject, Tenant: tenant, Actor: actor, Roles: roles,
	})
	if errors.Is(err, ErrPrincipalExists) {
		return proto.Principal{}, proto.Err(proto.CodeConflict, "principal already exists")
	}
	return principal, err
}

// ListPrincipals returns a bounded, stable tenant directory without bearers.
func (m *Manager) ListPrincipals(ctx context.Context, tenant string) ([]proto.Principal, error) {
	if !validText(tenant, 253) {
		return nil, errors.New("identity: invalid tenant")
	}
	store, ok := m.store.(PrincipalDirectoryStore)
	if !ok {
		return nil, errors.New("identity: principal directory is unavailable")
	}
	return store.ListPrincipals(ctx, tenant)
}

// IssueAccessToken mints one access-only bearer for an assigned role. It does
// not return or persist a refresh bearer.
func (m *Manager) IssueAccessToken(ctx context.Context, tenant, subject, role string, ttl time.Duration) (string, time.Time, error) {
	roles, err := normalizeRoles([]string{role})
	if err != nil || slices.Contains(roles, RoleNode) || ttl < time.Second || ttl > 24*time.Hour {
		return "", time.Time{}, errors.New("identity: invalid access-token role or ttl")
	}
	store, ok := m.store.(PrincipalDirectoryStore)
	if !ok {
		return "", time.Time{}, errors.New("identity: principal directory is unavailable")
	}
	principal, err := store.GetPrincipal(ctx, tenant, subject)
	if err != nil {
		if errors.Is(err, ErrPrincipalNotFound) {
			return "", time.Time{}, proto.Err(proto.CodeNotFound, "principal does not exist")
		}
		return "", time.Time{}, err
	}
	if !slices.Contains(principal.Roles, roles[0]) {
		return "", time.Time{}, proto.Err(proto.CodeDenied, "role is not assigned to principal")
	}
	now := m.now()
	expires := now.Add(ttl)
	token, err := m.sign(Claims{ID: ids.New("tok"), Subject: subject, Tenant: tenant, Roles: roles,
		Kind: kindAccess, Audience: m.audience, Revision: principal.Revision, IssuedAt: now.Unix(), Expires: expires.Unix()})
	if err != nil {
		return "", time.Time{}, err
	}
	if err := m.emit(ctx, Event{Type: "identity.issued", Subject: subject, Tenant: tenant}); err != nil {
		return "", time.Time{}, err
	}
	return token, expires, nil
}

// SyncOIDCPrincipal creates or atomically changes a federated principal's
// assigned roles. A change advances its revision, invalidating older tokens.
func (m *Manager) SyncOIDCPrincipal(ctx context.Context, tenant, subject string, roles []string) (proto.Principal, error) {
	roles, err := normalizeRoles(roles)
	if err != nil || !validPrincipal(subject, tenant) || slices.Contains(roles, RoleNode) {
		return proto.Principal{}, errors.New("identity: invalid OIDC principal")
	}
	store, ok := m.store.(PrincipalDirectoryStore)
	if !ok {
		return proto.Principal{}, errors.New("identity: principal directory is unavailable")
	}
	now := m.now()
	principal, _, err := store.UpsertPrincipalRoles(ctx, proto.Principal{ID: subject, Tenant: tenant, Roles: roles, CreatedAt: now.UnixMilli(), UpdatedAt: now.UnixMilli()}, Event{
		Type: proto.EvIdentityRolesChanged, Subject: subject, Tenant: tenant, Actor: "oidc:" + subject, Roles: roles,
	})
	return principal, err
}

func (m *Manager) ensurePrincipalRoles(ctx context.Context, subject, tenant string, roles []string) error {
	store, ok := m.store.(PrincipalDirectoryStore)
	if !ok {
		return nil
	}
	principal, err := store.GetPrincipal(ctx, tenant, subject)
	if err == nil {
		if !slices.Equal(principal.Roles, roles) {
			return errors.New("identity: issued roles differ from durable assignment")
		}
		return nil
	}
	if !errors.Is(err, ErrPrincipalNotFound) {
		return err
	}
	now := m.now()
	_, err = store.CreatePrincipal(ctx, proto.Principal{ID: subject, Tenant: tenant, Roles: roles, CreatedAt: now.UnixMilli(), UpdatedAt: now.UnixMilli()}, Event{
		Type: proto.EvIdentityPrincipalCreated, Subject: subject, Tenant: tenant, Actor: subject, Roles: roles,
	})
	if errors.Is(err, ErrPrincipalExists) {
		principal, err = store.GetPrincipal(ctx, tenant, subject)
		if err == nil && slices.Equal(principal.Roles, roles) {
			return nil
		}
	}
	return err
}
