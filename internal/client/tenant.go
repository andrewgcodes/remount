package client

import (
	"context"

	"remount.dev/remount/internal/proto"
)

// CreateTenant creates a durable tenant authority.
func (c *Client) CreateTenant(ctx context.Context, id string, policy proto.TenantPolicy, idempotencyKey string) (*proto.Tenant, error) {
	var result proto.Tenant
	err := c.call(ctx, proto.PeerControl, proto.OpTenantCreate, proto.TenantCreateReq{ID: id, Policy: policy, IdempotencyKey: idempotencyKey}, &result)
	return &result, err
}

// GetTenant gets one exact tenant after authorization.
func (c *Client) GetTenant(ctx context.Context, id string) (*proto.Tenant, error) {
	var result proto.Tenant
	err := c.call(ctx, proto.PeerControl, proto.OpTenantGet, proto.TenantGetReq{ID: id}, &result)
	return &result, err
}

// ListTenants lists the bounded tenant directory for a global operator.
func (c *Client) ListTenants(ctx context.Context) ([]proto.Tenant, error) {
	var result proto.TenantListRes
	err := c.call(ctx, proto.PeerControl, proto.OpTenantList, struct{}{}, &result)
	return result.Tenants, err
}

// UpdateTenantPolicy applies an expected-revision tenant policy update.
func (c *Client) UpdateTenantPolicy(ctx context.Context, request proto.TenantUpdateReq) (*proto.Tenant, error) {
	var result proto.Tenant
	err := c.call(ctx, proto.PeerControl, proto.OpTenantUpdate, request, &result)
	return &result, err
}

// SetTenantState applies an expected-revision lifecycle transition.
func (c *Client) SetTenantState(ctx context.Context, request proto.TenantStateReq) (*proto.Tenant, error) {
	var result proto.Tenant
	err := c.call(ctx, proto.PeerControl, proto.OpTenantState, request, &result)
	return &result, err
}

// TenantUsage reports durable concurrent quota reservations.
func (c *Client) TenantUsage(ctx context.Context, tenant string) (proto.TenantUsage, error) {
	var result proto.TenantUsage
	err := c.call(ctx, proto.PeerControl, proto.OpTenantUsage, proto.TenantUsageReq{Tenant: tenant}, &result)
	return result, err
}
