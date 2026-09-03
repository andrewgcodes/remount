package client

import (
	"context"

	"remount.dev/remount/internal/proto"
)

// CreateBudget creates an immutable tenant-scoped budget definition.
func (c *Client) CreateBudget(ctx context.Context, value proto.Budget, options ...OperationOption) (*proto.Budget, error) {
	idem, _ := operationKey(options)
	var result proto.Budget
	err := c.call(ctx, proto.PeerControl, proto.OpBudgetCreate, proto.BudgetCreateReq{Budget: value, IdempotencyKey: idem}, &result)
	return &result, err
}

// ListBudgets returns the caller-visible budget definitions.
func (c *Client) ListBudgets(ctx context.Context) ([]proto.Budget, error) {
	var result proto.BudgetListRes
	err := c.call(ctx, proto.PeerControl, proto.OpBudgetList, proto.BudgetListReq{}, &result)
	return result.Budgets, err
}

// RemoveBudget stops new admission against a budget. Retained reservations
// remain settleable and auditable until their configured retention expires.
func (c *Client) RemoveBudget(ctx context.Context, id string, options ...OperationOption) error {
	idem, _ := operationKey(options)
	return c.call(ctx, proto.PeerControl, proto.OpBudgetRemove, proto.BudgetRemoveReq{ID: id, IdempotencyKey: idem}, nil)
}

// Usage returns authoritative rolling usage counters for visible budgets.
func (c *Client) Usage(ctx context.Context, query proto.UsageReq) ([]proto.Usage, error) {
	var result proto.UsageRes
	err := c.call(ctx, proto.PeerControl, proto.OpUsageGet, query, &result)
	return result.Usage, err
}
