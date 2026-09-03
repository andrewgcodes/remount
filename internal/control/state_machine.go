package control

import (
	"context"
	"math"

	"remount.dev/remount/internal/proto"
)

func nextWorkspaceGeneration(current uint64) (uint64, error) {
	// SQLite INTEGER is signed. Refuse the boundary rather than wrapping the
	// protocol uint64 or creating a generation persistence cannot represent.
	if current >= uint64(math.MaxInt64) {
		return 0, proto.Err(proto.CodeResourceExhausted, "workspace generation space exhausted")
	}
	return current + 1, nil
}

func canAdvanceWorkspaceAuthority(workspace *proto.Workspace) error {
	if workspace == nil {
		return proto.Err(proto.CodeNotFound, "workspace authority has no resource")
	}
	if _, err := nextWorkspaceGeneration(workspace.Generation); err != nil {
		return err
	}
	if workspace.AuthzRevision == math.MaxUint64 {
		return proto.Err(proto.CodeResourceExhausted, "workspace authorization revision space exhausted")
	}
	return nil
}

type lifecycleActor string

const (
	actorControl  lifecycleActor = "control"
	actorNode     lifecycleActor = "node"
	actorRecovery lifecycleActor = "recovery"

	transitionRecover            = "recover"
	transitionReconnect          = "node.disconnect"
	transitionReleaseAbort       = "release.abort"
	transitionDestroyBegin       = "destroy.begin"
	transitionDestroyCommit      = "destroy.commit"
	transitionReleaseBegin       = "release.begin"
	transitionReleaseCommit      = "release.commit"
	transitionMove               = "move.commit"
	transitionSleep              = "sleep.commit"
	transitionWake               = "wake"
	transitionClaim              = "claim"
	transitionReady              = "ready"
	transitionNodeReleased       = "node.released"
	transitionFleetBegin         = "fleet.begin"
	transitionFleetComplete      = "fleet.complete"
	transitionLeaseExpired       = "lease.expired"
	transitionAuthorityExhausted = "authority.exhausted"
)

type statePair struct {
	from string
	to   string
}

type lifecycleRule struct {
	actor lifecycleActor
	pairs map[statePair]struct{}
}

func pairs(values ...statePair) map[statePair]struct{} {
	out := make(map[statePair]struct{}, len(values))
	for _, value := range values {
		out[value] = struct{}{}
	}
	return out
}

var lifecycleRules = map[string]lifecycleRule{
	transitionRecover: {actor: actorRecovery, pairs: pairs(
		statePair{proto.WSClaimed, proto.WSClaiming},
		statePair{proto.WSClaiming, proto.WSClaiming},
		statePair{proto.WSQuiescing, proto.WSFailed},
		statePair{proto.WSCheckpointing, proto.WSFailed},
		statePair{proto.WSDestroying, proto.WSFailed},
		statePair{proto.WSReleased, proto.WSPending},
	)},
	transitionReconnect: {actor: actorControl, pairs: pairs(
		statePair{proto.WSClaimed, proto.WSClaiming},
	)},
	transitionReleaseAbort: {actor: actorControl, pairs: pairs(
		statePair{proto.WSQuiescing, proto.WSClaimed},
		statePair{proto.WSQuiescing, proto.WSFailed},
		statePair{proto.WSDestroying, proto.WSClaimed},
		statePair{proto.WSDestroying, proto.WSFailed},
	)},
	transitionDestroyBegin: {actor: actorControl, pairs: pairs(
		statePair{proto.WSClaimed, proto.WSDestroying},
		statePair{proto.WSClaiming, proto.WSDestroying},
	)},
	transitionDestroyCommit: {actor: actorControl, pairs: pairs(
		statePair{proto.WSDestroying, proto.WSDestroyed},
		statePair{proto.WSPending, proto.WSDestroyed},
		statePair{proto.WSReleased, proto.WSDestroyed},
		statePair{proto.WSPaused, proto.WSDestroyed},
	)},
	transitionReleaseBegin: {actor: actorControl, pairs: pairs(
		statePair{proto.WSClaimed, proto.WSQuiescing},
	)},
	transitionReleaseCommit: {actor: actorControl, pairs: pairs(
		statePair{proto.WSQuiescing, proto.WSReleased},
	)},
	transitionMove: {actor: actorControl, pairs: pairs(
		statePair{proto.WSPending, proto.WSPending},
		statePair{proto.WSReleased, proto.WSPending},
		statePair{proto.WSPaused, proto.WSPending},
	)},
	transitionSleep: {actor: actorControl, pairs: pairs(
		statePair{proto.WSPending, proto.WSPaused},
		statePair{proto.WSReleased, proto.WSPaused},
		statePair{proto.WSPaused, proto.WSPaused},
	)},
	transitionWake: {actor: actorControl, pairs: pairs(
		statePair{proto.WSPaused, proto.WSPending},
	)},
	transitionClaim: {actor: actorNode, pairs: pairs(
		statePair{proto.WSPending, proto.WSClaiming},
		statePair{proto.WSClaimed, proto.WSClaiming},
		statePair{proto.WSClaiming, proto.WSClaiming},
	)},
	transitionReady: {actor: actorNode, pairs: pairs(
		statePair{proto.WSClaiming, proto.WSClaimed},
		statePair{proto.WSClaimed, proto.WSClaimed},
	)},
	transitionNodeReleased: {actor: actorNode, pairs: pairs(
		statePair{proto.WSClaimed, proto.WSPending},
		statePair{proto.WSClaiming, proto.WSPending},
	)},
	transitionFleetBegin: {actor: actorControl, pairs: pairs(
		statePair{proto.WSClaimed, proto.WSQuiescing},
		statePair{proto.WSClaiming, proto.WSQuiescing},
		statePair{proto.WSFailed, proto.WSQuiescing},
	)},
	transitionFleetComplete: {actor: actorControl, pairs: fleetCompletionPairs()},
	transitionLeaseExpired: {actor: actorControl, pairs: pairs(
		statePair{proto.WSClaimed, proto.WSPending},
		statePair{proto.WSClaiming, proto.WSPending},
	)},
	transitionAuthorityExhausted: {actor: actorControl, pairs: pairs(
		statePair{proto.WSClaimed, proto.WSFailed},
		statePair{proto.WSClaiming, proto.WSFailed},
	)},
}

func fleetCompletionPairs() map[statePair]struct{} {
	states := []string{
		proto.WSPending, proto.WSClaiming, proto.WSClaimed, proto.WSQuiescing,
		proto.WSCheckpointing, proto.WSPaused, proto.WSReleased,
		proto.WSDestroying, proto.WSFailed,
	}
	out := make(map[statePair]struct{}, len(states)*2)
	for _, from := range states {
		out[statePair{from, proto.WSFailed}] = struct{}{}
		out[statePair{from, proto.WSDestroyed}] = struct{}{}
	}
	return out
}

type lifecycleTransition struct {
	operation string
	actor     lifecycleActor
	to        string

	expectGeneration bool
	generation       uint64
	expectNode       bool
	node             string
}

// transitionWorkspace is the only control-plane function allowed to change a
// workspace lifecycle state. The caller still owns persistence and publication
// under c.mu; this pure compare-and-transition step centralizes predecessor,
// actor, generation, and authoritative-node validation.
func transitionWorkspace(current *proto.Workspace, request lifecycleTransition) (proto.Workspace, error) {
	if current == nil {
		return proto.Workspace{}, proto.Err(proto.CodeNotFound, "workspace transition %s has no resource", request.operation)
	}
	rule, ok := lifecycleRules[request.operation]
	if !ok || request.operation == "" {
		return proto.Workspace{}, proto.Err(proto.CodeInternal, "unknown workspace transition operation %q", request.operation)
	}
	if request.actor != rule.actor {
		return proto.Workspace{}, proto.Err(proto.CodeDenied, "actor %q cannot perform workspace transition %s", request.actor, request.operation)
	}
	if request.expectGeneration && current.Generation != request.generation {
		return proto.Workspace{}, proto.Err(proto.CodeConflict,
			"workspace transition %s expected generation %d, have %d", request.operation, request.generation, current.Generation)
	}
	if request.expectNode && current.Node != request.node {
		return proto.Workspace{}, proto.Err(proto.CodeConflict,
			"workspace transition %s expected node %q, have %q", request.operation, request.node, current.Node)
	}
	if _, ok := rule.pairs[statePair{current.State, request.to}]; !ok {
		return proto.Workspace{}, proto.Err(proto.CodeConflict, "workspace transition %s cannot move %s to %s",
			request.operation, current.State, request.to)
	}
	next := *current
	next.State = request.to
	return next, nil
}

func (c *Control) emitWorkspaceTransition(ctx context.Context, before string, next proto.Workspace, request lifecycleTransition) {
	if before == next.State {
		return
	}
	node := next.Node
	if node == "" && request.expectNode {
		node = request.node
	}
	c.emit(context.WithoutCancel(ctx), proto.EvWSStateChanged, next.ID, next.Owner, node, map[string]any{
		"from": before, "to": next.State, "operation": request.operation,
		"actor": request.actor, "generation": next.Generation, "node": node,
	})
}
