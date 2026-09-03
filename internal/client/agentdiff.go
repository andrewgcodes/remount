package client

import (
	"context"
	"strconv"
	"strings"
	"time"

	"remount.dev/remount/internal/proto"
)

// AgentDiff is the working-tree state of an agent's workspace as git sees
// it: the porcelain status and the unified diff against HEAD (against the
// index when the repository has no commit yet).
type AgentDiff struct {
	Status    string `json:"status"`
	Diff      string `json:"diff"`
	Truncated bool   `json:"truncated,omitempty"`
}

// MaxAgentDiffBytes bounds the diff text an AgentDiff carries; a larger
// diff is cut and flagged Truncated rather than streamed.
const MaxAgentDiffBytes = 4 << 20

// AgentWakeTimeout bounds how long AgentMaterialized waits for a woken or
// creating workspace to be claimed by a node.
const AgentWakeTimeout = 90 * time.Second

// AgentMaterialized returns the agent once its workspace is claimed by a
// node. A sleeping agent is woken only when wake is set, with by recorded
// in agent.woken; otherwise the call fails with CodeConflict so a read never
// has a side effect the caller did not ask for.
func (c *Client) AgentMaterialized(ctx context.Context, id string, wake bool, by string) (*proto.Agent, error) {
	a, err := c.GetAgent(ctx, id)
	if err != nil {
		return nil, err
	}
	switch a.Status {
	case proto.AgentDestroyed:
		return nil, proto.Err(proto.CodeConflict, "agent %s is %s", a.ID, a.Status)
	case proto.AgentSleeping:
		if !wake {
			return nil, proto.Err(proto.CodeConflict, "agent %s is sleeping; wake=true wakes it", a.ID)
		}
		if _, err := c.WakeAgent(ctx, id, by); err != nil {
			return nil, err
		}
	}
	deadline := time.Now().Add(AgentWakeTimeout)
	for {
		ws, err := c.GetWorkspace(ctx, a.WS)
		if err != nil {
			return nil, err
		}
		switch ws.State {
		case proto.WSClaimed:
			return a, nil
		case proto.WSDestroyed:
			return nil, proto.Err(proto.CodeConflict, "workspace %s is destroyed", ws.ID)
		case proto.WSPaused:
			if !wake {
				return nil, proto.Err(proto.CodeConflict, "agent %s is sleeping; wake=true wakes it", a.ID)
			}
			// The wake above was a replay against an already-woken agent, or
			// the agent's status lagged its workspace; wake the tree itself.
			if _, err := c.WakeWorkspace(ctx, ws.ID); err != nil {
				return nil, err
			}
		}
		if time.Now().After(deadline) {
			return nil, proto.Err(proto.CodeUnreachable, "workspace %s is %s; no node has claimed it", ws.ID, ws.State)
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(200 * time.Millisecond):
		}
	}
}

// Diff runs git in an agent's workspace and returns what changed. It wakes
// a sleeping agent only when wake is set (recorded as woken by diff).
func (c *Client) Diff(ctx context.Context, id string, wake bool) (*AgentDiff, error) {
	a, err := c.AgentMaterialized(ctx, id, wake, proto.AgentWokenByDiff)
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithTimeout(ctx, 2*time.Minute)
	defer cancel()
	status, err := c.git(ctx, a.WS, "status", "--porcelain=v1", "--untracked-files=all")
	if err != nil {
		return nil, err
	}
	diff, err := c.git(ctx, a.WS, "diff", "--no-color", "--no-ext-diff", "HEAD")
	if err != nil {
		// A repository without a commit has no HEAD; the index diff is the
		// closest honest answer.
		if diff, err = c.git(ctx, a.WS, "diff", "--no-color", "--no-ext-diff"); err != nil {
			return nil, err
		}
	}
	res := &AgentDiff{Status: string(status), Diff: string(diff)}
	if len(res.Diff) > MaxAgentDiffBytes {
		res.Diff = res.Diff[:MaxAgentDiffBytes]
		res.Truncated = true
	}
	return res, nil
}

func (c *Client) git(ctx context.Context, ws string, args ...string) ([]byte, error) {
	out, stderr, exit, err := c.Run(ctx, ws, append([]string{"git"}, args...)...)
	if err != nil {
		return nil, err
	}
	if exit == nil || exit.Code != 0 || exit.Error != "" {
		detail := strings.TrimSpace(string(stderr))
		if detail == "" {
			detail = exitText(exit)
		}
		return nil, proto.Err(proto.CodeUnreachable, "git %s failed in the workspace: %s", args[0], detail)
	}
	return out, nil
}

func exitText(exit *proto.ExitInfo) string {
	if exit == nil {
		return "no exit status"
	}
	if exit.Error != "" {
		return exit.Error
	}
	if exit.Signal != "" {
		return "signal " + exit.Signal
	}
	return "exit status " + strconv.Itoa(exit.Code)
}
