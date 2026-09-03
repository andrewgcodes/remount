package client

import (
	"context"
	"time"

	"remount.dev/remount/internal/proto"
)

// CreatePrincipal adds one tenant-scoped role assignment.
func (c *Client) CreatePrincipal(ctx context.Context, tenant, principal string, roles []string, idempotencyKey string) (*proto.Principal, error) {
	var result proto.Principal
	err := c.call(ctx, proto.PeerControl, proto.OpPrincipalCreate, proto.PrincipalCreateReq{Tenant: tenant, Principal: principal, Roles: roles, IdempotencyKey: idempotencyKey}, &result)
	return &result, err
}

// ListPrincipals lists role assignments for an exact authorized tenant.
func (c *Client) ListPrincipals(ctx context.Context, tenant string) ([]proto.Principal, error) {
	var result proto.PrincipalListRes
	err := c.call(ctx, proto.PeerControl, proto.OpPrincipalList, proto.PrincipalListReq{Tenant: tenant}, &result)
	return result.Principals, err
}

// IssuePrincipalToken returns one short-lived access-only bearer.
func (c *Client) IssuePrincipalToken(ctx context.Context, tenant, principal, role string, ttl time.Duration, idempotencyKey string) (*proto.PrincipalTokenIssueRes, error) {
	var result proto.PrincipalTokenIssueRes
	err := c.call(ctx, proto.PeerControl, proto.OpPrincipalTokenIssue, proto.PrincipalTokenIssueReq{Tenant: tenant, Principal: principal, Role: role, TTLMS: ttl.Milliseconds(), IdempotencyKey: idempotencyKey}, &result)
	return &result, err
}

// InvitePrincipal creates a tenant operator and returns a short-lived bearer.
func (c *Client) InvitePrincipal(ctx context.Context, tenant, principal string, ttl time.Duration, idempotencyKey string) (*proto.PrincipalTokenIssueRes, error) {
	var result proto.PrincipalTokenIssueRes
	err := c.call(ctx, proto.PeerControl, proto.OpPrincipalInvite, proto.PrincipalInviteReq{Tenant: tenant, Principal: principal, TTLMS: ttl.Milliseconds(), IdempotencyKey: idempotencyKey}, &result)
	return &result, err
}

// RevokePrincipal invalidates every credential and live workspace authority
// for principal in an exact tenant. Global operators must still name the
// target tenant; ordinary tenant operators may leave tenant empty.
func (c *Client) RevokePrincipal(ctx context.Context, tenant, principal, idempotencyKey string) (uint64, error) {
	var result proto.PrincipalRevokeRes
	err := c.call(ctx, proto.PeerControl, proto.OpPrincipalRevoke, proto.PrincipalRevokeReq{
		Tenant: tenant, Principal: principal, IdempotencyKey: idempotencyKey,
	}, &result)
	return result.Revision, err
}
