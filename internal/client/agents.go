package client

import (
	"context"

	"remount.dev/remount/internal/ids"
	"remount.dev/remount/internal/proto"
)

// Agents (ADR 0043). Every mutating call takes an idempotency key through
// WithOperationKey or mints one; a retried call returns the first result.

func withKey(key *string, options []OperationOption) {
	if k, set := operationKey(options); set {
		*key = k
	} else if *key == "" {
		*key = ids.New("idem")
	}
}

// CreateAgent creates a durable agent and, unless it adopts a workspace,
// the workspace it runs in.
func (c *Client) CreateAgent(ctx context.Context, req proto.AgentCreateReq, options ...OperationOption) (*proto.Agent, error) {
	withKey(&req.IdempotencyKey, options)
	var a proto.Agent
	err := c.call(ctx, proto.PeerControl, proto.OpAgentCreate, req, &a)
	return &a, err
}

// GetAgent returns one agent.
func (c *Client) GetAgent(ctx context.Context, id string) (*proto.Agent, error) {
	var a proto.Agent
	err := c.call(ctx, proto.PeerControl, proto.OpAgentGet, proto.AgentGetReq{ID: id}, &a)
	return &a, err
}

// ListAgents returns the caller's agents, oldest first.
func (c *Client) ListAgents(ctx context.Context, req proto.AgentListReq) ([]proto.Agent, error) {
	var res proto.AgentListRes
	err := c.call(ctx, proto.PeerControl, proto.OpAgentList, req, &res)
	return res.Agents, err
}

// MessageAgent appends to the inbox; a sleeping agent wakes.
func (c *Client) MessageAgent(ctx context.Context, req proto.AgentMessageReq, options ...OperationOption) (*proto.AgentMessageRes, error) {
	withKey(&req.IdempotencyKey, options)
	var res proto.AgentMessageRes
	err := c.call(ctx, proto.PeerControl, proto.OpAgentMessage, req, &res)
	return &res, err
}

// CancelAgent interrupts the current turn and drops the inbox.
func (c *Client) CancelAgent(ctx context.Context, id string, options ...OperationOption) (*proto.Agent, error) {
	req := proto.AgentGetReq{ID: id}
	withKey(&req.IdempotencyKey, options)
	var a proto.Agent
	err := c.call(ctx, proto.PeerControl, proto.OpAgentCancel, req, &a)
	return &a, err
}

// SleepAgent snapshots the workspace and pauses the agent until a message.
func (c *Client) SleepAgent(ctx context.Context, id string, options ...OperationOption) (*proto.Agent, error) {
	req := proto.AgentGetReq{ID: id}
	withKey(&req.IdempotencyKey, options)
	var a proto.Agent
	err := c.call(ctx, proto.PeerControl, proto.OpAgentSleep, req, &a)
	return &a, err
}

// ForkAgent snapshots the workspace and starts a child on the copy.
func (c *Client) ForkAgent(ctx context.Context, req proto.AgentForkReq, options ...OperationOption) (*proto.Agent, error) {
	withKey(&req.IdempotencyKey, options)
	var a proto.Agent
	err := c.call(ctx, proto.PeerControl, proto.OpAgentFork, req, &a)
	return &a, err
}

// DestroyAgent ends the agent and destroys the workspace it owns.
func (c *Client) DestroyAgent(ctx context.Context, id string, options ...OperationOption) error {
	req := proto.AgentGetReq{ID: id}
	withKey(&req.IdempotencyKey, options)
	return c.call(ctx, proto.PeerControl, proto.OpAgentDestroy, req, nil)
}

// ListApprovals returns pending approvals, or those matching the filter.
func (c *Client) ListApprovals(ctx context.Context, req proto.ApprovalListReq) ([]proto.Approval, error) {
	var res proto.ApprovalListRes
	err := c.call(ctx, proto.PeerControl, proto.OpApprovalList, req, &res)
	return res.Approvals, err
}

// GetApproval returns one approval.
func (c *Client) GetApproval(ctx context.Context, id string) (*proto.Approval, error) {
	var ap proto.Approval
	err := c.call(ctx, proto.PeerControl, proto.OpApprovalGet, proto.ApprovalGetReq{ID: id}, &ap)
	return &ap, err
}

// DecideApproval answers an approval.
func (c *Client) DecideApproval(ctx context.Context, req proto.ApprovalDecideReq, options ...OperationOption) (*proto.Approval, error) {
	withKey(&req.IdempotencyKey, options)
	var ap proto.Approval
	err := c.call(ctx, proto.PeerControl, proto.OpApprovalDecide, req, &ap)
	return &ap, err
}
