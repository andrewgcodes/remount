package client

import (
	"context"

	"remount.dev/remount/api"
	internalclient "remount.dev/remount/internal/client"
)

// CredentialFilter selects credential-use and egress-decision events.
type CredentialFilter = internalclient.CredentialFilter

// CredentialEvent is one recorded broker decision: who used which binding,
// for which workspace, against which host, with what outcome. It never
// carries a credential value.
type CredentialEvent = internalclient.CredentialEvent

// CreateBinding defines a tenant-scoped brokered credential. The workspace
// receives only the placeholder; the node substitutes the real value at the
// network edge for the destinations named here.
func (c *Client) CreateBinding(ctx context.Context, spec api.BindingSpec, options ...OperationOption) (*api.BindingSpec, error) {
	return c.inner.CreateBinding(ctx, spec, options...)
}

// ListBindings returns the bindings visible to the caller's tenant without
// secrets. Revoked rows are retained for audit and returned only on request.
func (c *Client) ListBindings(ctx context.Context, tenant string, includeRevoked bool) ([]api.BindingSpec, error) {
	return c.inner.ListBindings(ctx, tenant, includeRevoked)
}

// GetBinding returns one binding definition without its secret.
func (c *Client) GetBinding(ctx context.Context, tenant, id string) (*api.BindingSpec, error) {
	return c.inner.GetBinding(ctx, tenant, id)
}

// RotateBinding replaces the credential behind a binding and bumps its
// revision. A node holding a lease from the old revision re-leases within one
// renew interval, after which the previous secret is no longer substituted.
func (c *Client) RotateBinding(ctx context.Context, request api.BindingRotateRequest, options ...OperationOption) (*api.BindingSpec, error) {
	return c.inner.RotateBinding(ctx, request, options...)
}

// RevokeBinding permanently stops substitution for a binding. It is not
// provider-side revocation: rotate or delete the credential at the provider
// as well.
func (c *Client) RevokeBinding(ctx context.Context, tenant, id, reason string, options ...OperationOption) (*api.BindingSpec, error) {
	return c.inner.RevokeBinding(ctx, tenant, id, reason, options...)
}

// CreateSessionPrincipal mints an ephemeral principal and its workspace- and
// generation-bound capability in one call. The token is returned once, is
// refused after the workspace moves, and is invalidated by RevokePrincipal.
func (c *Client) CreateSessionPrincipal(ctx context.Context, request api.SessionPrincipalRequest, options ...OperationOption) (*api.SessionPrincipal, error) {
	return c.inner.CreateSessionPrincipal(ctx, request, options...)
}

// RevokePrincipal invalidates every credential and live workspace authority
// held by one principal in an exact tenant, and returns the durable identity
// revision the revocation committed at.
func (c *Client) RevokePrincipal(ctx context.Context, tenant, principal string, options ...OperationOption) (uint64, error) {
	return c.inner.RevokePrincipal(ctx, tenant, principal, internalclient.OperationKey(options))
}

// CredentialEvents answers the audit question in one call: who used which
// binding, for which workspace, to which host, with what status, under which
// tenant, at what time.
func (c *Client) CredentialEvents(ctx context.Context, filter CredentialFilter) ([]CredentialEvent, error) {
	return c.inner.CredentialEvents(ctx, filter)
}
