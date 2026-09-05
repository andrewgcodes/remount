package control

import (
	"context"
	"fmt"
	"sort"
	"time"

	"remount.dev/remount/internal/eventlog"
	"remount.dev/remount/internal/metrics"
	"remount.dev/remount/internal/proto"
)

// ReconcileRecovery repairs the restored database from authenticated node
// state. It is the only path that clears a promoted controller's readiness
// hold, and it never advances a generation not observed on a node.
func (c *Control) ReconcileRecovery(ctx context.Context) error {
	_, recovery, pending := c.controllerRecoverySnapshot()
	if !pending || recovery == nil {
		return nil
	}
	c.mu.Lock()
	required := make(map[string]struct{})
	for _, workspace := range c.workspaces {
		if workspace.Node != "" && workspace.State != proto.WSDestroyed {
			required[workspace.Node] = struct{}{}
		}
	}
	for id, node := range c.nodes {
		if node.Status.Online {
			required[id] = struct{}{}
		}
	}
	c.mu.Unlock()
	nodes := make([]string, 0, len(required))
	for node := range required {
		nodes = append(nodes, node)
	}
	sort.Strings(nodes)
	reports := make([]proto.ControllerNodeState, 0, len(nodes))
	for _, node := range nodes {
		for c.send == nil || !c.send.Online(node) {
			select {
			case <-ctx.Done():
				return fmt.Errorf("controller recovery waiting for node %s: %w", node, ctx.Err())
			case <-time.After(25 * time.Millisecond):
			}
		}
		var report proto.ControllerNodeState
		if err := c.send.Request(ctx, node, proto.OpControllerState, struct{}{}, &report); err != nil {
			return fmt.Errorf("controller recovery query node %s: %w", node, err)
		}
		if report.Node != node || report.Epoch != c.controllerEpoch {
			return proto.Err(proto.CodeConflict, "node %s recovery report has epoch %d and identity %q", node, report.Epoch, report.Node)
		}
		reports = append(reports, report)
	}

	seen := make(map[string]string)
	for _, report := range reports {
		for _, observed := range report.Workspaces {
			if prior := seen[observed.Workspace.ID]; prior != "" && prior != report.Node {
				return proto.Err(proto.CodeConflict, "workspace %s is live on both %s and %s", observed.Workspace.ID, prior, report.Node)
			}
			seen[observed.Workspace.ID] = report.Node
			if err := c.reconcileObservedWorkspace(ctx, report.Node, observed.Workspace, nil, len(observed.Sessions), recovery); err != nil {
				return err
			}
		}
		for index := range report.Releases {
			release := report.Releases[index]
			if release.State == "committed" || release.State == "published" {
				continue
			}
			if prior := seen[release.Request.WS]; prior != "" && prior != report.Node {
				return proto.Err(proto.CodeConflict, "workspace %s authority is duplicated across nodes", release.Request.WS)
			}
			seen[release.Request.WS] = report.Node
			workspace := proto.Workspace{
				ID: release.Request.WS, Tenant: release.Request.Tenant, Generation: release.Request.Gen,
				Node: report.Node, State: proto.WSClaiming, Spec: release.Request.Spec,
				ReleaseEpoch: observedReleaseEpoch(release.Request), ReleaseOperation: release.OperationID,
			}
			workspace.Spec.Requires.Backend = release.Request.Backend
			if err := c.reconcileObservedWorkspace(ctx, report.Node, workspace, &release, 0, recovery); err != nil {
				return err
			}
		}
	}

	c.mu.Lock()
	for _, workspace := range c.workspaces {
		if workspace.Node != "" && workspace.State != proto.WSDestroyed && seen[workspace.ID] == "" {
			id := workspace.ID
			c.mu.Unlock()
			return proto.Err(proto.CodeConflict, "node authority for workspace %s was unavailable during recovery", id)
		}
	}
	event := c.newEvent(proto.EvControlRecovered, "", "", "", map[string]any{
		"epoch": c.controllerEpoch, "previous_epoch": recovery.PreviousEpoch,
		"last_replicated_at": recovery.LastReplicatedAt.UnixMilli(), "lost_window_ms": recovery.LostWindow.Milliseconds(),
		"restored_event_seq": recovery.RestoredEventSeq,
	})
	if err := c.transact(func(*eventlog.Tx) error { return nil }, []*proto.Event{event}); err != nil {
		c.mu.Unlock()
		return err
	}
	c.mu.Unlock()
	c.recoveryMu.Lock()
	c.recoveryPending = false
	c.recoveryMu.Unlock()
	metrics.ControllerReconciling.Set(0)
	c.offerPending(ctx)
	return nil
}

func (c *Control) reconcileObservedWorkspace(ctx context.Context, node string, observed proto.Workspace, release *proto.ControllerReleaseState, sessions int, recovery *RecoveryState) error {
	if observed.ID == "" || observed.Tenant == "" || observed.Generation == 0 || observed.Node != node {
		return proto.Err(proto.CodeConflict, "node %s reported invalid workspace authority", node)
	}
	c.mu.Lock()
	if enrolled := c.nodes[node]; enrolled != nil {
		if tenant := enrolled.Status.Labels["tenant"]; tenant != "" && tenant != observed.Tenant {
			c.mu.Unlock()
			return proto.Err(proto.CodeConflict, "node %s reported cross-tenant authority for workspace %s", node, observed.ID)
		}
	}
	current := c.workspaces[observed.ID]
	restoredGeneration := uint64(0)
	if current != nil {
		restoredGeneration = current.Generation
		if current.Tenant != observed.Tenant || (current.Node != "" && current.Node != node) || observed.Generation < current.Generation {
			c.mu.Unlock()
			return proto.Err(proto.CodeConflict, "node %s reported stale or cross-tenant authority for workspace %s", node, observed.ID)
		}
	}
	action := "confirmed"
	to := proto.WSClaimed
	if release != nil {
		action = "release_rolled_back"
		to = proto.WSClaiming
	}
	var next proto.Workspace
	if current == nil {
		next = observed
		next.State = to
		if next.Owner == "" {
			next.Owner = next.Spec.Principal
		}
		if next.AuthzRevision == 0 {
			next.AuthzRevision = 1
		}
		if next.CreatedAt == 0 {
			next.CreatedAt = c.now().UnixMilli()
		}
		action = "adopted_missing"
	} else {
		transition := lifecycleTransition{operation: transitionControllerReconcile, actor: actorRecovery, to: to}
		var err error
		next, err = transitionWorkspace(current, transition)
		if err != nil {
			c.mu.Unlock()
			return err
		}
		if observed.Generation > current.Generation {
			// The exact authenticated node view, not a locally incremented value,
			// becomes authority. Tenant and node were checked above.
			next = observed
			next.State = to
			action = "adopted_generation"
		}
	}
	next.Node = node
	next.LeaseUntil = c.now().Add(time.Duration(c.opts.LeaseSec) * time.Second).UnixMilli()
	if release != nil {
		releaseEpoch := observedReleaseEpoch(release.Request)
		if next.ReleaseEpoch < releaseEpoch {
			next.ReleaseEpoch = releaseEpoch
		}
		next.ReleaseOperation = release.OperationID
	}
	event := c.wsEvent(&next, proto.EvControlReconciled, "", node, map[string]any{
		"observed_generation": observed.Generation, "restored_generation": restoredGeneration,
		"from_epoch": recovery.PreviousEpoch, "action": action, "sessions": sessions,
	})
	if err := c.persistClaim(&next, event); err != nil {
		c.mu.Unlock()
		return err
	}
	if current == nil {
		c.workspaces[next.ID] = &next
	} else {
		*current = next
	}
	c.mu.Unlock()
	if release != nil {
		return c.finishReleaseAbort(ctx, next.ID, node, next.Generation, release.OperationID)
	}
	return nil
}

func observedReleaseEpoch(request proto.WSReleaseReq) uint64 {
	if request.ReleaseEpoch == 0 {
		return 1
	}
	return request.ReleaseEpoch
}
