package control

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"time"

	"remount.dev/remount/internal/eventlog"
	"remount.dev/remount/internal/ids"
	"remount.dev/remount/internal/metrics"
	"remount.dev/remount/internal/proto"
)

// Approvals (ADR 0044) are questions a harness asked that policy routed to a
// human. The node parks the ACP request and reports it; the control plane
// owns the durable record and the decision; the node answers the harness
// once the decision reaches it. An approval never outlives its run: ACP has
// no way to re-ask, so a run ending expires what it parked and the harness
// asks again on its next turn.

func approvalResource(ap *proto.Approval) Resource {
	return Resource{Kind: "approval", ID: ap.ID, Tenant: ap.Tenant, Owner: ap.Owner}
}

func copyApproval(ap *proto.Approval) *proto.Approval {
	cp := *ap
	cp.Locations = append([]string(nil), ap.Locations...)
	cp.Options = append([]proto.ApprovalOption(nil), ap.Options...)
	cp.Detail = append(json.RawMessage(nil), ap.Detail...)
	if ap.Decision != nil {
		d := *ap.Decision
		d.Content = append(json.RawMessage(nil), ap.Decision.Content...)
		cp.Decision = &d
	}
	return &cp
}

func (c *Control) approvalEvent(typ string, ap *proto.Approval, ws *proto.Workspace, principal, node string, payload map[string]any) *proto.Event {
	if payload == nil {
		payload = map[string]any{}
	}
	payload["approval"] = ap.ID
	payload["agent"] = ap.Agent
	payload["kind"] = ap.Kind
	e := c.newEvent(typ, ap.WS, principal, node, payload)
	if ws != nil {
		return stampWS(e, ws)
	}
	e.Tenant, e.Workspace = ap.Tenant, ap.WS
	return e
}

// loadAgents restores agents and approvals from SQLite at start.
func (c *Control) loadAgents() error {
	rows, err := c.db.Query(`SELECT data FROM agents`)
	if err != nil {
		return err
	}
	for rows.Next() {
		var b []byte
		if err := rows.Scan(&b); err != nil {
			rows.Close()
			return err
		}
		var a proto.Agent
		if err := proto.Unmarshal(b, &a); err != nil {
			rows.Close()
			return fmt.Errorf("decode agent: %w", err)
		}
		if a.Inbox == nil {
			a.Inbox = []proto.AgentMessage{}
		}
		if a.Runs == nil {
			a.Runs = []proto.AgentRun{}
		}
		c.agents[a.ID] = &a
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return err
	}
	if err := rows.Close(); err != nil {
		return err
	}
	rows, err = c.db.Query(`SELECT data FROM approvals`)
	if err != nil {
		return err
	}
	for rows.Next() {
		var b []byte
		if err := rows.Scan(&b); err != nil {
			rows.Close()
			return err
		}
		var ap proto.Approval
		if err := proto.Unmarshal(b, &ap); err != nil {
			rows.Close()
			return fmt.Errorf("decode approval: %w", err)
		}
		c.approvals[ap.ID] = &ap
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return err
	}
	return rows.Close()
}

// persistAgentRows is the single commit point for an agent and whatever
// approvals the same change touched (c.dirtyApprovals), with the events and
// optional mutation record, in one SQLite transaction. Caller holds c.mu.
func (c *Control) persistAgentRows(a *proto.Agent, scope, key, op string, request, result any, events ...*proto.Event) error {
	a.UpdatedAt = c.now().UnixMilli()
	trimAgentRuns(a)
	dirty := make([]*proto.Approval, 0, len(c.dirtyApprovals))
	for _, ap := range c.dirtyApprovals {
		dirty = append(dirty, ap)
	}
	sort.Slice(dirty, func(i, j int) bool { return dirty[i].ID < dirty[j].ID })
	err := c.transact(func(tx *eventlog.Tx) error {
		if _, err := tx.Exec(`INSERT OR REPLACE INTO agents(id, data) VALUES(?,?)`, a.ID, proto.MustMarshal(a)); err != nil {
			return err
		}
		for _, ap := range dirty {
			if _, err := tx.Exec(`INSERT OR REPLACE INTO approvals(id, data) VALUES(?,?)`, ap.ID, proto.MustMarshal(ap)); err != nil {
				return err
			}
		}
		if key == "" {
			return nil
		}
		return c.insertMutationTx(tx.Tx, scope, key, op, request, result)
	}, events)
	if err == nil {
		for id := range c.dirtyApprovals {
			delete(c.dirtyApprovals, id)
		}
		if agentTerminal(a.Status) {
			c.wakeTranscriptWaitersLocked(a.ID)
		}
	}
	return err
}

func (c *Control) markApprovalDirty(ap *proto.Approval) {
	ap.UpdatedAt = c.now().UnixMilli()
	c.dirtyApprovals[ap.ID] = ap
}

// parkApprovalLocked records a permission or elicitation request the node
// parked for a run. Caller holds c.mu.
func (c *Control) parkApprovalLocked(a *proto.Agent, run *proto.AgentRun, ws *proto.Workspace, node string, in *proto.Approval) ([]*proto.Event, error) {
	if in.ID == "" || !strings.HasPrefix(in.ID, "ap_") || len(in.ID) > 64 {
		return nil, proto.Err(proto.CodeBadRequest, "approval id %q is not an ap_ id", in.ID)
	}
	switch in.Kind {
	case proto.ApprovalToolCall, proto.ApprovalElicitation:
	default:
		return nil, proto.Err(proto.CodeBadRequest, "a node may park tool_call or elicitation approvals, not %q", in.Kind)
	}
	if len(in.Detail) > 64<<10 {
		return nil, proto.Err(proto.CodeBadRequest, "approval detail is larger than 64 KiB")
	}
	if len(in.Options) > 32 {
		return nil, proto.Err(proto.CodeBadRequest, "approval offers more than 32 options")
	}
	if existing := c.approvals[in.ID]; existing != nil {
		if existing.Agent != a.ID || existing.Run != run.ID {
			return nil, proto.Err(proto.CodeConflict, "approval %s belongs to another run", in.ID)
		}
		return nil, nil
	}
	pending := 0
	for _, other := range c.approvals {
		if other.Agent == a.ID && other.Status == proto.ApprovalPending {
			pending++
		}
	}
	if pending >= c.opts.MaxApprovalsPerAgent {
		metrics.ApprovalQuotaRejected.Inc()
		return nil, proto.Err(proto.CodeResourceExhausted, "agent %s has %d pending approvals", a.ID, pending)
	}
	now := c.now().UnixMilli()
	ap := &proto.Approval{
		ID: in.ID, Tenant: a.Tenant, Owner: a.Owner, Agent: a.ID, WS: a.WS, Run: run.ID, Kind: in.Kind,
		Title: truncate(in.Title, 512), ToolCall: in.ToolCall, ToolKind: in.ToolKind,
		Locations: append([]string(nil), in.Locations...), Options: append([]proto.ApprovalOption(nil), in.Options...),
		Detail: append(json.RawMessage(nil), in.Detail...), Status: proto.ApprovalPending, CreatedAt: now, UpdatedAt: now,
	}
	c.approvals[ap.ID] = ap
	c.markApprovalDirty(ap)
	a.PendingApprovals = pending + 1
	metrics.ApprovalsPending.Inc()
	return []*proto.Event{c.approvalEvent(proto.EvApprovalPending, ap, ws, "", node, map[string]any{
		"run": run.ID, "title": ap.Title, "tool_call": ap.ToolCall, "tool_kind": ap.ToolKind, "options": len(ap.Options),
	})}, nil
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n]
}

// expireApprovalsLocked ends every pending approval of the agent (or of one
// run) because the run that asked is over. Caller holds c.mu.
func (c *Control) expireApprovalsLocked(a *proto.Agent, run *proto.AgentRun, principal string) []*proto.Event {
	var events []*proto.Event
	ws := c.workspaces[a.WS]
	ids := make([]string, 0)
	for id, ap := range c.approvals {
		if ap.Agent == a.ID && ap.Status == proto.ApprovalPending && (run == nil || ap.Run == run.ID) {
			ids = append(ids, id)
		}
	}
	sort.Strings(ids)
	for _, id := range ids {
		ap := c.approvals[id]
		ap.Status = proto.ApprovalExpired
		c.markApprovalDirty(ap)
		metrics.ApprovalsExpired.Inc()
		events = append(events, c.approvalEvent(proto.EvApprovalExpired, ap, ws, principal, "", map[string]any{"run": ap.Run}))
	}
	a.PendingApprovals = c.pendingApprovalsLocked(a.ID)
	return events
}

func (c *Control) pendingApprovalsLocked(agent string) int {
	n := 0
	for _, ap := range c.approvals {
		if ap.Agent == agent && ap.Status == proto.ApprovalPending {
			n++
		}
	}
	return n
}

// expireStaleApprovals runs every tick: an approval whose run is done or
// whose agent is gone can never be answered to a harness. It also re-sends
// decisions the node has not acknowledged.
func (c *Control) expireStaleApprovals(ctx context.Context) {
	type resend struct {
		node string
		req  proto.AgentApprovalDecidedReq
	}
	var resends []resend
	c.mu.Lock()
	byAgent := map[string][]*proto.Approval{}
	var egressEvents []*proto.Event
	for _, ap := range c.approvals {
		if ap.Kind == proto.ApprovalEgress {
			if ap.Status == proto.ApprovalPending && ap.ExpiresAt > 0 && c.now().UnixMilli() >= ap.ExpiresAt {
				ap.Status = proto.ApprovalExpired
				ap.Decision = &proto.ApprovalDecision{Option: "deny", Denied: true, By: "control", At: c.now().UnixMilli(), Remember: proto.ApprovalRememberNone}
				c.markApprovalDirty(ap)
				metrics.ApprovalsExpired.Inc()
				egressEvents = append(egressEvents, c.approvalEvent(proto.EvEgressDenied, ap, c.workspaces[ap.WS], "control", "", map[string]any{
					"reason": "approval_timeout", "decision_id": ap.ID,
				}))
			}
			continue
		}
		if ap.Status == proto.ApprovalPending || (ap.Status == proto.ApprovalDecided && ap.DeliveredAt == 0) {
			byAgent[ap.Agent] = append(byAgent[ap.Agent], ap)
		}
	}
	if len(egressEvents) > 0 {
		if err := c.persistApprovalsOnly(egressEvents); err != nil {
			c.logger.Error("persist expired egress approvals", "err", err)
		}
	}
	agentIDs := make([]string, 0, len(byAgent))
	for id := range byAgent {
		agentIDs = append(agentIDs, id)
	}
	sort.Strings(agentIDs)
	for _, id := range agentIDs {
		a := c.agents[id]
		var events []*proto.Event
		for _, ap := range byAgent[id] {
			run := (*proto.AgentRun)(nil)
			if a != nil {
				run = findRun(a, ap.Run)
			}
			stale := a == nil || agentTerminal(a.Status) || run == nil || run.State == proto.AgentRunDone
			switch {
			case ap.Status == proto.ApprovalPending && stale:
				ap.Status = proto.ApprovalExpired
				c.markApprovalDirty(ap)
				metrics.ApprovalsExpired.Inc()
				events = append(events, c.approvalEvent(proto.EvApprovalExpired, ap, c.workspaces[ap.WS], "", "", map[string]any{"run": ap.Run, "reason": "run ended"}))
			case ap.Status == proto.ApprovalDecided && !stale && run.State == proto.AgentRunActive && ap.Decision != nil &&
				c.now().UnixMilli()-ap.UpdatedAt > agentRedeliverAfter.Milliseconds():
				ap.UpdatedAt = c.now().UnixMilli()
				resends = append(resends, resend{node: run.Node, req: proto.AgentApprovalDecidedReq{Agent: a.ID, Run: run.ID, Approval: ap.ID, Decision: *ap.Decision}})
			case ap.Status == proto.ApprovalDecided && stale:
				// Nobody to deliver to; stop retrying.
				ap.DeliveredAt = -1
				c.markApprovalDirty(ap)
			}
		}
		if a != nil {
			a.PendingApprovals = c.pendingApprovalsLocked(a.ID)
			more, err := c.refreshAgentStatusLocked(a, "")
			if err == nil {
				events = append(events, more...)
			}
			if len(events) > 0 || len(c.dirtyApprovals) > 0 {
				if err := c.persistAgentRows(a, "", "", "", nil, nil, events...); err != nil {
					c.logger.Error("persist approvals", "agent", a.ID, "err", err)
				}
			}
		} else if len(c.dirtyApprovals) > 0 {
			if err := c.persistApprovalsOnly(events); err != nil {
				c.logger.Error("persist approvals", "err", err)
			}
		}
	}
	c.mu.Unlock()
	for _, r := range resends {
		go c.sendDecision(context.WithoutCancel(ctx), r.node, r.req)
	}
}

// persistApprovalsOnly flushes dirty approvals for an agent that no longer
// exists. Caller holds c.mu.
func (c *Control) persistApprovalsOnly(events []*proto.Event) error {
	dirty := make([]*proto.Approval, 0, len(c.dirtyApprovals))
	for _, ap := range c.dirtyApprovals {
		dirty = append(dirty, ap)
	}
	err := c.transact(func(tx *eventlog.Tx) error {
		for _, ap := range dirty {
			if _, err := tx.Exec(`INSERT OR REPLACE INTO approvals(id, data) VALUES(?,?)`, ap.ID, proto.MustMarshal(ap)); err != nil {
				return err
			}
		}
		return nil
	}, events)
	if err == nil {
		for id := range c.dirtyApprovals {
			delete(c.dirtyApprovals, id)
		}
	}
	return err
}

// egressApproval creates or resolves the durable fence in front of an
// approve-mode rule. The current workspace holder is the only writer. The
// short wait is merely an optimization: expiry and decisions live in SQLite.
func (c *Control) egressApproval(ctx context.Context, node string, req *proto.EgressApprovalReq) (*proto.EgressApprovalRes, error) {
	if req.WS == "" || req.Gen == 0 || req.Rule == "" || req.Host == "" || req.Method == "" || req.Principal == "" {
		return nil, proto.Err(proto.CodeBadRequest, "egress approval requires ws, gen, principal, rule, host and method")
	}
	if !approvalDigest(req.PathHash) || !approvalDigest(req.BodyHash) || !approvalDigest(req.Fingerprint) {
		return nil, proto.Err(proto.CodeBadRequest, "egress approval hashes must be lowercase SHA-256")
	}
	if len(req.Host) > 512 || len(req.Method) > 32 {
		return nil, proto.Err(proto.CodeBadRequest, "egress approval host or method is too long")
	}
	wait := time.Duration(req.WaitMillis) * time.Millisecond
	if wait < 0 {
		return nil, proto.Err(proto.CodeBadRequest, "egress approval wait cannot be negative")
	}
	if wait > 30*time.Second {
		wait = 30 * time.Second
	}

	c.mu.Lock()
	ws := c.workspaces[req.WS]
	leaseable := ws != nil && (ws.State == proto.WSClaimed || ws.State == proto.WSClaiming)
	if !leaseable || ws.Node != node || ws.Generation != req.Gen {
		c.mu.Unlock()
		return nil, proto.Err(proto.CodeDenied, "workspace %s generation %d is not claimed by %s", req.WS, req.Gen, node)
	}
	ruleOK := false
	for _, rule := range ws.Spec.Security.Network.Rules {
		if rule.ID == req.Rule && rule.Mode == proto.EgressModeApprove {
			ruleOK = true
			break
		}
	}
	if !ruleOK {
		c.mu.Unlock()
		return nil, proto.Err(proto.CodeDenied, "workspace %s rule %s does not require approval", req.WS, req.Rule)
	}
	now := c.now().UnixMilli()
	var ap *proto.Approval
	for _, candidate := range c.approvals {
		if candidate.Kind != proto.ApprovalEgress || candidate.WS != req.WS || candidate.Principal != req.Principal || candidate.ExpiresAt <= now {
			continue
		}
		if candidate.Fingerprint == req.Fingerprint || (candidate.Status == proto.ApprovalDecided && candidate.Decision != nil && !candidate.Decision.Denied &&
			((candidate.Decision.Remember == proto.ApprovalRememberHost && candidate.Host == req.Host) ||
				(candidate.Decision.Remember == proto.ApprovalRememberRule && candidate.Rule == req.Rule))) {
			ap = candidate
			break
		}
	}
	if ap == nil {
		pending := 0
		for _, candidate := range c.approvals {
			if candidate.Tenant == ws.Tenant && candidate.Kind == proto.ApprovalEgress && candidate.Status == proto.ApprovalPending && candidate.ExpiresAt > now {
				pending++
			}
		}
		if pending >= c.opts.MaxPendingEgressApprovals {
			tenant := ws.Tenant
			c.mu.Unlock()
			metrics.ApprovalQuotaRejected.Inc()
			return nil, proto.Err(proto.CodeResourceExhausted, "tenant %s has %d pending egress approvals", tenant, pending)
		}
		ap = &proto.Approval{
			ID: ids.New("ap"), Tenant: ws.Tenant, Owner: ws.Owner, WS: ws.ID, Kind: proto.ApprovalEgress,
			Title: "Egress to " + req.Host, Principal: req.Principal, Rule: req.Rule, Host: req.Host,
			Method: strings.ToUpper(req.Method), PathHash: req.PathHash, BodyHash: req.BodyHash, Fingerprint: req.Fingerprint,
			Status: proto.ApprovalPending, CreatedAt: now, UpdatedAt: now,
			ExpiresAt: now + c.opts.ApprovalTimeout.Milliseconds(),
		}
		c.approvals[ap.ID] = ap
		c.markApprovalDirty(ap)
		event := c.approvalEvent(proto.EvEgressPending, ap, ws, req.Principal, node, map[string]any{
			"host": ap.Host, "method": ap.Method, "rule": ap.Rule, "path_hash": ap.PathHash, "body_hash": ap.BodyHash,
		})
		if err := c.persistApprovalsOnly([]*proto.Event{event}); err != nil {
			delete(c.approvals, ap.ID)
			delete(c.dirtyApprovals, ap.ID)
			c.mu.Unlock()
			return nil, err
		}
		metrics.ApprovalsPending.Inc()
	}
	id := ap.ID
	c.mu.Unlock()

	deadline := time.NewTimer(wait)
	defer deadline.Stop()
	ticker := time.NewTicker(25 * time.Millisecond)
	defer ticker.Stop()
	for {
		current, err := c.approvalCopy(id)
		if err != nil {
			return nil, err
		}
		result := &proto.EgressApprovalRes{ID: current.ID, Status: current.Status, ExpiresAt: current.ExpiresAt}
		if current.Status != proto.ApprovalPending {
			result.Allowed = current.Status == proto.ApprovalDecided && current.Decision != nil && !current.Decision.Denied
			return result, nil
		}
		if wait == 0 {
			return result, nil
		}
		select {
		case <-ctx.Done():
			return result, nil
		case <-deadline.C:
			return result, nil
		case <-ticker.C:
		}
	}
}

func approvalDigest(value string) bool {
	if len(value) != 64 || strings.ToLower(value) != value {
		return false
	}
	_, err := hex.DecodeString(value)
	return err == nil
}

// ---------------------------------------------------------------------------
// approval.list / approval.get / approval.decide
// ---------------------------------------------------------------------------

func (c *Control) approvalAuthorize(ctx context.Context, subject Subject, ap *proto.Approval, action string) error {
	if err := c.check(ctx, subject, action, approvalResource(ap)); err == nil {
		return nil
	}
	c.mu.Lock()
	a := c.agents[ap.Agent]
	var agentCopy *proto.Agent
	if a != nil {
		agentCopy = copyAgent(a)
	}
	c.mu.Unlock()
	if agentCopy == nil {
		return proto.Err(proto.CodeDenied, "subject %s may not %s approval %s", subject.ID, action, ap.ID)
	}
	return c.agentAuthorize(ctx, subject, agentCopy, action)
}

func (c *Control) approvalList(ctx context.Context, subject Subject, req *proto.ApprovalListReq) (*proto.ApprovalListRes, error) {
	c.mu.Lock()
	all := make([]*proto.Approval, 0, len(c.approvals))
	for _, ap := range c.approvals {
		if req.Agent != "" && ap.Agent != req.Agent {
			continue
		}
		if req.Kind != "" && ap.Kind != req.Kind {
			continue
		}
		if req.Status != "" && ap.Status != req.Status {
			continue
		}
		if req.Status == "" && ap.Status != proto.ApprovalPending {
			continue
		}
		all = append(all, copyApproval(ap))
	}
	c.mu.Unlock()
	sort.Slice(all, func(i, j int) bool {
		if all[i].CreatedAt != all[j].CreatedAt {
			return all[i].CreatedAt < all[j].CreatedAt
		}
		return all[i].ID < all[j].ID
	})
	out := &proto.ApprovalListRes{Approvals: []proto.Approval{}}
	for _, ap := range all {
		if c.approvalAuthorize(ctx, subject, ap, ActionRead) == nil {
			out.Approvals = append(out.Approvals, *ap)
		}
	}
	return out, nil
}

func (c *Control) approvalCopy(id string) (*proto.Approval, error) {
	c.mu.Lock()
	ap := c.approvals[id]
	var cp *proto.Approval
	if ap != nil {
		cp = copyApproval(ap)
	}
	c.mu.Unlock()
	if cp == nil {
		return nil, proto.Err(proto.CodeNotFound, "approval %s", id)
	}
	return cp, nil
}

func (c *Control) approvalGet(ctx context.Context, subject Subject, id string) (*proto.Approval, error) {
	if id == "" {
		return nil, proto.Err(proto.CodeBadRequest, "approval id is required")
	}
	ap, err := c.approvalCopy(id)
	if err != nil {
		return nil, err
	}
	if err := c.approvalAuthorize(ctx, subject, ap, ActionRead); err != nil {
		return nil, err
	}
	return ap, nil
}

// approvalDecide records the human's answer and hands it to the run. The
// decision commits first; delivery is retried by the tick until the node
// acknowledges or the run ends.
func (c *Control) approvalDecide(ctx context.Context, subject Subject, req *proto.ApprovalDecideReq) (*proto.Approval, error) {
	if req.ID == "" {
		return nil, proto.Err(proto.CodeBadRequest, "approval id is required")
	}
	if len(req.Content) > 64<<10 {
		return nil, proto.Err(proto.CodeBadRequest, "content is larger than 64 KiB")
	}
	if len(req.Content) > 0 && !json.Valid(req.Content) {
		return nil, proto.Err(proto.CodeBadRequest, "content is not valid JSON")
	}
	ap, err := c.approvalCopy(req.ID)
	if err != nil {
		return nil, err
	}
	if err := c.approvalAuthorize(ctx, subject, ap, ActionExecute); err != nil {
		return nil, err
	}
	scope := ap.Tenant + "|" + ap.ID + "|approval.decide"
	unlockMutation := c.lockMutation(scope, req.IdempotencyKey)
	defer unlockMutation()
	var prior struct{}
	if hit, err := c.mutationLookup(scope, req.IdempotencyKey, proto.OpApprovalDecide, req, &prior); err != nil {
		return nil, err
	} else if hit {
		return c.approvalCopy(ap.ID)
	}
	c.mu.Lock()
	live := c.approvals[ap.ID]
	if live == nil {
		c.mu.Unlock()
		return nil, proto.Err(proto.CodeNotFound, "approval %s", ap.ID)
	}
	if live.Status != proto.ApprovalPending {
		status := live.Status
		c.mu.Unlock()
		return nil, proto.Err(proto.CodeConflict, "approval %s is %s", ap.ID, status)
	}
	decision := proto.ApprovalDecision{Option: req.Option, Denied: req.Denied, By: subject.ID, At: c.now().UnixMilli()}
	if live.Kind != proto.ApprovalEgress && req.Remember != "" {
		c.mu.Unlock()
		return nil, proto.Err(proto.CodeBadRequest, "remember is only valid for egress approvals")
	}
	switch live.Kind {
	case proto.ApprovalToolCall:
		if !req.Denied {
			if req.Option == "" {
				if len(live.Options) == 0 {
					c.mu.Unlock()
					return nil, proto.Err(proto.CodeBadRequest, "approval %s offers no options; use denied", ap.ID)
				}
				decision.Option = firstAllowOption(live.Options)
				if decision.Option == "" {
					c.mu.Unlock()
					return nil, proto.Err(proto.CodeBadRequest, "approval %s has no allow option; pick one explicitly", ap.ID)
				}
			} else if !hasOption(live.Options, req.Option) {
				c.mu.Unlock()
				return nil, proto.Err(proto.CodeBadRequest, "option %q is not one the harness offered", req.Option)
			}
		} else if req.Option != "" && !hasOption(live.Options, req.Option) {
			c.mu.Unlock()
			return nil, proto.Err(proto.CodeBadRequest, "option %q is not one the harness offered", req.Option)
		}
	case proto.ApprovalElicitation:
		if req.Option != "" {
			c.mu.Unlock()
			return nil, proto.Err(proto.CodeBadRequest, "an elicitation takes content or denied, not an option")
		}
		if !req.Denied && len(req.Content) == 0 {
			c.mu.Unlock()
			return nil, proto.Err(proto.CodeBadRequest, "an elicitation needs content or denied")
		}
		if len(req.Content) > 0 && !isJSONObject(req.Content) {
			c.mu.Unlock()
			return nil, proto.Err(proto.CodeBadRequest, "elicitation content must be a JSON object of form fields")
		}
		if !req.Denied {
			decision.Content = append(json.RawMessage(nil), req.Content...)
		}
	case proto.ApprovalEgress:
		switch {
		case req.Option == "" || req.Option == "deny" || (req.Option == "allow" && !req.Denied):
		default:
			c.mu.Unlock()
			return nil, proto.Err(proto.CodeBadRequest, "an egress approval takes allow, deny or denied")
		}
		if req.Option == "deny" {
			decision.Denied = true
		}
		if decision.Denied {
			decision.Option = "deny"
		} else {
			decision.Option = "allow"
		}
		decision.Remember = req.Remember
		if decision.Remember == "" {
			decision.Remember = proto.ApprovalRememberNone
		}
		switch decision.Remember {
		case proto.ApprovalRememberNone, proto.ApprovalRememberHost, proto.ApprovalRememberRule:
		default:
			c.mu.Unlock()
			return nil, proto.Err(proto.CodeBadRequest, "unknown egress remember scope %q", decision.Remember)
		}
	default:
		kind := live.Kind
		c.mu.Unlock()
		return nil, proto.Err(proto.CodeInternal, "approval %s has unknown kind %q", ap.ID, kind)
	}
	live.Status = proto.ApprovalDecided
	live.Decision = &decision
	if live.Kind == proto.ApprovalEgress {
		live.ExpiresAt = decision.At + c.opts.ApprovalDecisionTTL.Milliseconds()
	}
	c.markApprovalDirty(live)
	a := c.agents[live.Agent]
	ws := c.workspaces[live.WS]
	eventType := proto.EvApprovalDecided
	if live.Kind == proto.ApprovalEgress {
		if decision.Denied {
			eventType = proto.EvEgressDenied
		} else {
			eventType = proto.EvEgressAllowed
		}
	}
	events := []*proto.Event{c.approvalEvent(eventType, live, ws, subject.ID, "", map[string]any{
		"option": decision.Option, "denied": decision.Denied, "by": subject.ID, "run": live.Run,
		"approved_by": subject.ID, "decision_id": live.ID, "remember": decision.Remember, "reason": "approval_decision",
	})}
	var send *proto.AgentApprovalDecidedReq
	node := ""
	if a != nil {
		a.PendingApprovals = c.pendingApprovalsLocked(a.ID)
		more, err := c.refreshAgentStatusLocked(a, subject.ID)
		if err != nil {
			c.mu.Unlock()
			return nil, err
		}
		events = append(events, more...)
		if run := findRun(a, live.Run); run != nil && run.State == proto.AgentRunActive && run.Node != "" {
			send = &proto.AgentApprovalDecidedReq{Agent: a.ID, Run: run.ID, Approval: live.ID, Decision: decision}
			node = run.Node
		}
		if err := c.persistAgentRows(a, scope, req.IdempotencyKey, proto.OpApprovalDecide, req, struct{}{}, events...); err != nil {
			c.mu.Unlock()
			return nil, err
		}
	} else {
		var nextWS *proto.Workspace
		if live.Kind == proto.ApprovalEgress && !decision.Denied && decision.Remember == proto.ApprovalRememberHost && ws != nil {
			next := *ws
			security, normalizeErr := proto.NormalizeSecurity(ws.Spec.Security)
			if normalizeErr != nil {
				c.mu.Unlock()
				return nil, normalizeErr
			}
			next.Spec = ws.Spec
			next.Spec.Security = security
			remembered := proto.EgressRule{
				ID: "approved-" + live.ID, Mode: proto.EgressModeAllow, Protocol: ruleProtocol(next.Spec.Security.Network.Rules, live.Rule), Hosts: []string{live.Host},
			}
			next.Spec.Security.Network.Rules = append([]proto.EgressRule{remembered}, next.Spec.Security.Network.Rules...)
			nextWS = &next
			events = append(events, c.approvalEvent(proto.EvPolicyUpdated, live, &next, subject.ID, "", map[string]any{
				"rule": live.Rule, "host": live.Host, "decision_id": live.ID,
			}))
		}
		if err := c.persistEgressDecision(nextWS, scope, req, events); err != nil {
			c.mu.Unlock()
			return nil, err
		}
		if nextWS != nil {
			c.workspaces[nextWS.ID] = nextWS
		}
	}
	cp := copyApproval(live)
	c.mu.Unlock()
	metrics.ApprovalsDecided.Inc()
	if send != nil && c.send != nil {
		go c.sendDecision(context.WithoutCancel(ctx), node, *send)
	}
	return cp, nil
}

func ruleProtocol(rules []proto.EgressRule, id string) string {
	for _, rule := range rules {
		if rule.ID == id {
			return rule.Protocol
		}
	}
	return proto.EgressProtocolHTTPS
}

// persistEgressDecision commits the decision, optional remembered policy,
// mutation record and their events together. Caller holds c.mu.
func (c *Control) persistEgressDecision(ws *proto.Workspace, scope string, req *proto.ApprovalDecideReq, events []*proto.Event) error {
	dirty := make([]*proto.Approval, 0, len(c.dirtyApprovals))
	for _, ap := range c.dirtyApprovals {
		dirty = append(dirty, ap)
	}
	err := c.transact(func(tx *eventlog.Tx) error {
		for _, ap := range dirty {
			if _, err := tx.Exec(`INSERT OR REPLACE INTO approvals(id, data) VALUES(?,?)`, ap.ID, proto.MustMarshal(ap)); err != nil {
				return err
			}
		}
		if ws != nil {
			ws.UpdatedAt = c.now().UnixMilli()
			if _, err := tx.Exec(`INSERT OR REPLACE INTO workspaces(id, data) VALUES(?,?)`, ws.ID, proto.MustMarshal(ws)); err != nil {
				return err
			}
		}
		return c.insertMutationTx(tx.Tx, scope, req.IdempotencyKey, proto.OpApprovalDecide, req, struct{}{})
	}, events)
	if err == nil {
		for id := range c.dirtyApprovals {
			delete(c.dirtyApprovals, id)
		}
	}
	return err
}

// isJSONObject reports whether valid JSON is an object, which is the only
// shape an elicitation's form content can take.
func isJSONObject(raw json.RawMessage) bool {
	trimmed := bytes.TrimSpace(raw)
	return len(trimmed) > 0 && trimmed[0] == '{'
}

func firstAllowOption(opts []proto.ApprovalOption) string {
	for _, kind := range []string{"allow_once", "allow_always"} {
		for _, o := range opts {
			if o.Kind == kind {
				return o.ID
			}
		}
	}
	return ""
}

func hasOption(opts []proto.ApprovalOption, id string) bool {
	for _, o := range opts {
		if o.ID == id {
			return true
		}
	}
	return false
}

// sendDecision hands a decision to the node and records the acknowledgment.
func (c *Control) sendDecision(ctx context.Context, node string, req proto.AgentApprovalDecidedReq) {
	dctx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	if err := c.send.Request(dctx, node, proto.OpAgentApprovalDecided, req, nil); err != nil {
		c.logger.Warn("approval deliver", "approval", req.Approval, "node", node, "err", err)
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	ap := c.approvals[req.Approval]
	if ap == nil || ap.DeliveredAt != 0 {
		return
	}
	ap.DeliveredAt = c.now().UnixMilli()
	c.markApprovalDirty(ap)
	if err := c.persistApprovalsOnly(nil); err != nil {
		c.logger.Error("persist approval delivery", "approval", ap.ID, "err", err)
	}
}
