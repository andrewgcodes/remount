package control

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"time"

	"remount.dev/remount/internal/ids"
	"remount.dev/remount/internal/metrics"
	"remount.dev/remount/internal/proto"
)

// Agents (ADR 0043) are the durable "who is working on what" resource: a
// harness process the control plane restarts, wakes and forks as needed.
// The control plane owns the state; the node holding the workspace runs the
// process and reports what it saw. Nothing here handles a credential or a
// prompt body beyond what the inbox carries into the node.

const (
	// agentLaunchTimeout bounds a run that the node acknowledged but never
	// reported as started (the node died between the ack and the spawn).
	agentLaunchTimeout = 90 * time.Second
	// agentRetryBackoff spaces launch attempts after a failure so a broken
	// recipe does not spin.
	agentRetryBackoff = 5 * time.Second
	// agentRedeliverAfter re-sends an inbox message to an idle run that has
	// not picked it up (the deliver frame was lost).
	agentRedeliverAfter = 10 * time.Second
	// agentMaxFailures is how many consecutive run failures an agent
	// survives: one retry with session/load, then failed.
	agentMaxFailures = 2
)

type mutationAgentResult struct {
	ID string `json:"id"`
}

func agentResource(a *proto.Agent) Resource {
	return Resource{Kind: "agent", ID: a.ID, Tenant: a.Tenant, Owner: a.Owner}
}

func copyAgent(a *proto.Agent) *proto.Agent {
	cp := *a
	cp.Inbox = append([]proto.AgentMessage(nil), a.Inbox...)
	cp.Runs = append([]proto.AgentRun(nil), a.Runs...)
	cp.Spec.Providers = append([]string(nil), a.Spec.Providers...)
	cp.Spec.ACPCommand = append([]string(nil), a.Spec.ACPCommand...)
	if a.Capabilities != nil {
		caps := *a.Capabilities
		cp.Capabilities = &caps
	}
	return &cp
}

func (c *Control) agentCopy(id string) (*proto.Agent, error) {
	c.mu.Lock()
	a := c.agents[id]
	var cp *proto.Agent
	if a != nil {
		cp = copyAgent(a)
	}
	c.mu.Unlock()
	if cp == nil {
		return nil, proto.Err(proto.CodeNotFound, "agent %s", id)
	}
	return cp, nil
}

// agentAuthorize accepts the agent's own ACL or, failing that, the
// workspace's: whoever may act on the tree may act on the agent driving it.
func (c *Control) agentAuthorize(ctx context.Context, subject Subject, a *proto.Agent, action string) error {
	if err := c.check(ctx, subject, action, agentResource(a)); err == nil {
		return nil
	}
	ws, wsErr := c.wsGet(a.WS)
	if wsErr != nil {
		return proto.Err(proto.CodeDenied, "subject %s may not %s agent %s", subject.ID, action, a.ID)
	}
	return c.check(ctx, subject, action, workspaceResource(ws))
}

func (c *Control) agentEvent(typ string, a *proto.Agent, ws *proto.Workspace, principal, node string, payload map[string]any) *proto.Event {
	if payload == nil {
		payload = map[string]any{}
	}
	payload["agent"] = a.ID
	e := c.newEvent(typ, a.WS, principal, node, payload)
	if ws != nil {
		return stampWS(e, ws)
	}
	e.Tenant, e.Workspace = a.Tenant, a.WS
	return e
}

// persistAgent writes the row, its touched approvals and its events in one
// transaction. The caller holds c.mu and has already applied the change.
func (c *Control) persistAgent(a *proto.Agent, events ...*proto.Event) error {
	return c.persistAgentRows(a, "", "", "", nil, nil, events...)
}

func (c *Control) persistAgentAndMutation(a *proto.Agent, scope, key, op string, request, result any, events ...*proto.Event) error {
	return c.persistAgentRows(a, scope, key, op, request, result, events...)
}

func trimAgentRuns(a *proto.Agent) {
	if len(a.Runs) > proto.MaxAgentRuns {
		a.Runs = append([]proto.AgentRun(nil), a.Runs[len(a.Runs)-proto.MaxAgentRuns:]...)
	}
}

// liveRun is the run that is pending or active, if any. There is at most one.
func liveRun(a *proto.Agent) *proto.AgentRun {
	for i := len(a.Runs) - 1; i >= 0; i-- {
		if a.Runs[i].State != proto.AgentRunDone {
			return &a.Runs[i]
		}
	}
	return nil
}

func findRun(a *proto.Agent, id string) *proto.AgentRun {
	for i := range a.Runs {
		if a.Runs[i].ID == id {
			return &a.Runs[i]
		}
	}
	return nil
}

func agentTerminal(status string) bool {
	switch status {
	case proto.AgentFailed, proto.AgentFinished, proto.AgentDestroyed:
		return true
	}
	return false
}

// deriveAgentStatus is the one place status comes from. It reads the agent's
// own durable state and the workspace's; the node never sets it directly.
func deriveAgentStatus(a *proto.Agent, ws *proto.Workspace) string {
	if agentTerminal(a.Status) {
		return a.Status
	}
	if ws == nil || ws.State == proto.WSDestroyed || ws.State == proto.WSDestroying {
		return proto.AgentFailed
	}
	if a.PendingApprovals > 0 {
		return proto.AgentWaitingApproval
	}
	switch ws.State {
	case proto.WSPaused, proto.WSQuiescing, proto.WSCheckpointing:
		return proto.AgentSleeping
	case proto.WSPending, proto.WSClaiming, proto.WSReleased:
		// Nothing can run until a node holds the tree. Before the first run
		// that is creation; afterwards it is a wake or a move in progress.
		if len(a.Runs) == 0 {
			return proto.AgentCreating
		}
		if len(a.Inbox) > 0 {
			return proto.AgentRunning
		}
		return proto.AgentIdle
	case proto.WSFailed:
		return proto.AgentFailed
	}
	run := liveRun(a)
	if run != nil && run.TurnMessage != "" {
		return proto.AgentRunning
	}
	if len(a.Inbox) > 0 {
		return proto.AgentRunning
	}
	if run != nil {
		return proto.AgentWaitingInput
	}
	if len(a.Runs) == 0 {
		return proto.AgentCreating
	}
	return proto.AgentIdle
}

// refreshAgentStatusLocked recomputes status and returns the events that
// the change implies. Caller holds c.mu.
func (c *Control) refreshAgentStatusLocked(a *proto.Agent, principal string) ([]*proto.Event, error) {
	ws := c.workspaces[a.WS]
	next := deriveAgentStatus(a, ws)
	if next == a.Status {
		return nil, nil
	}
	if err := transitionAgent(a.Status, next); err != nil {
		return nil, err
	}
	prev := a.Status
	a.Status = next
	if a.StatusReason == "" && next == proto.AgentFailed {
		switch {
		case ws == nil:
			a.StatusReason = "workspace is gone"
		case ws.State == proto.WSFailed:
			a.StatusReason = "workspace failed"
		default:
			a.StatusReason = "workspace destroyed"
		}
	}
	var events []*proto.Event
	switch next {
	case proto.AgentWaitingInput, proto.AgentIdle:
		if prev != proto.AgentWaitingInput && prev != proto.AgentIdle {
			a.IdleSince = c.now().UnixMilli()
		}
		events = append(events, c.agentEvent(proto.EvAgentWaiting, a, ws, principal, "", map[string]any{"kind": "input", "from": prev, "status": next}))
	case proto.AgentWaitingApproval:
		a.IdleSince = 0
		events = append(events, c.agentEvent(proto.EvAgentWaiting, a, ws, principal, "", map[string]any{"kind": "approval", "from": prev}))
	case proto.AgentFailed:
		a.IdleSince = 0
		metrics.AgentsFailed.Inc()
		events = append(events, c.agentEvent(proto.EvAgentFailed, a, ws, principal, "", map[string]any{"reason": a.StatusReason}))
	default:
		a.IdleSince = 0
	}
	return events, nil
}

// ---------------------------------------------------------------------------
// agent.create
// ---------------------------------------------------------------------------

func validateAgentPolicy(p proto.AgentPolicy) error {
	switch p.Approve {
	case "", proto.ApproveNever, proto.ApproveAuto, proto.ApproveOnRequest:
	default:
		return proto.Err(proto.CodeBadRequest, "policy.approve %q is not never, auto or on-request", p.Approve)
	}
	if p.SleepAfterSec < 0 {
		return proto.Err(proto.CodeBadRequest, "policy.sleep_after_sec must not be negative")
	}
	if p.MaxTurns < 0 {
		return proto.Err(proto.CodeBadRequest, "policy.max_turns must not be negative")
	}
	return nil
}

// approveAutoAllowed: auto-approval hands the harness every permission it
// asks for, which is only sane when the workspace has at least container
// isolation. A process-backend tree on a shared host keeps a human in the loop.
func approveAutoAllowed(spec proto.WorkspaceSpec) bool {
	if spec.Requires.Backend != "" && spec.Requires.Backend != "process" {
		return true
	}
	return spec.Security.Profile != "" && spec.Security.Profile != proto.SecurityLocal
}

// policyWithin reports whether child expands parent: a fork or child agent
// may only tighten what it inherited.
func policyWithin(child, parent proto.AgentPolicy) error {
	rank := map[string]int{proto.ApproveNever: 0, proto.ApproveOnRequest: 1, proto.ApproveAuto: 2}
	if rank[child.Approve] > rank[parent.Approve] {
		return proto.Err(proto.CodeDenied, "policy.approve %q is more permissive than the parent's %q", child.Approve, parent.Approve)
	}
	if parent.MaxTurns > 0 && (child.MaxTurns == 0 || child.MaxTurns > parent.MaxTurns) {
		return proto.Err(proto.CodeDenied, "policy.max_turns exceeds the parent's %d", parent.MaxTurns)
	}
	return nil
}

func normalizeAgentPolicy(p proto.AgentPolicy) proto.AgentPolicy {
	if p.Approve == "" {
		p.Approve = proto.ApproveOnRequest
	}
	return p
}

func validateAgentSpec(spec *proto.AgentSpec) error {
	if spec.Recipe == "" {
		return proto.Err(proto.CodeBadRequest, "spec.recipe is required")
	}
	if len(spec.RecipeYAML) > 64<<10 {
		return proto.Err(proto.CodeBadRequest, "spec.recipe_yaml is larger than 64 KiB")
	}
	if spec.Task != "" {
		if err := proto.ValidateAgentMessage(spec.Task); err != nil {
			return proto.Err(proto.CodeBadRequest, "spec.task: %v", err)
		}
	}
	for _, p := range spec.Providers {
		if strings.ContainsAny(p, " \t\n=") || p == "" {
			return proto.Err(proto.CodeBadRequest, "spec.providers has an invalid binding name %q", p)
		}
	}
	for _, arg := range spec.ACPCommand {
		if strings.ContainsRune(arg, 0) {
			return proto.Err(proto.CodeBadRequest, "spec.acp_command contains a NUL byte")
		}
	}
	switch spec.Auth {
	case "", proto.RunAuthAPIKey, proto.RunAuthWorkspaceResident:
	default:
		return proto.Err(proto.CodeBadRequest, "spec.auth %q is not api-key or workspace-resident", spec.Auth)
	}
	return nil
}

// agentMode is ACP unless the recipe has no ACP server, which the node
// decides when it resolves the recipe; the control plane records the intent.
func agentMode(spec proto.AgentSpec) string {
	if spec.Mode != "" {
		return spec.Mode
	}
	return proto.AgentModeACP
}

// agentCreate makes the agent and, unless one is adopted, its workspace.
// The workspace is created first through the ordinary path (its own row,
// events and idempotency); the agent row then commits with agent.created.
// A crash between the two leaves an ordinary workspace the retry adopts.
func (c *Control) agentCreate(ctx context.Context, subject Subject, req *proto.AgentCreateReq) (*proto.Agent, error) {
	if err := validateAgentSpec(&req.Spec); err != nil {
		return nil, err
	}
	if err := validateAgentPolicy(req.Policy); err != nil {
		return nil, err
	}
	if req.WS != "" && req.Workspace != nil {
		return nil, proto.Err(proto.CodeBadRequest, "ws and workspace are exclusive")
	}
	if len(req.Name) > 128 {
		return nil, proto.Err(proto.CodeBadRequest, "name is longer than 128 bytes")
	}
	if req.ACPSessionID != "" && len(req.ACPSessionID) > 256 {
		return nil, proto.Err(proto.CodeBadRequest, "acp_session_id is longer than 256 bytes")
	}
	policy := normalizeAgentPolicy(req.Policy)
	var parent *proto.Agent
	if req.Parent != "" {
		p, err := c.agentCopy(req.Parent)
		if err != nil {
			return nil, err
		}
		if err := c.agentAuthorize(ctx, subject, p, ActionWrite); err != nil {
			return nil, err
		}
		if agentTerminal(p.Status) {
			return nil, proto.Err(proto.CodeConflict, "parent agent %s is %s", p.ID, p.Status)
		}
		if err := policyWithin(policy, p.Policy); err != nil {
			return nil, err
		}
		parent = p
	}
	scope := subject.Tenant + "|" + subject.ID + "|agent.create"
	unlockMutation := c.lockMutation(scope, req.IdempotencyKey)
	defer unlockMutation()
	var prior mutationAgentResult
	if hit, err := c.mutationLookup(scope, req.IdempotencyKey, proto.OpAgentCreate, req, &prior); err != nil {
		return nil, err
	} else if hit {
		return c.agentCopy(prior.ID)
	}
	id := ids.New("ag")
	var ws *proto.Workspace
	ownsWS := false
	if req.WS != "" {
		current, err := c.wsGet(req.WS)
		if err != nil {
			return nil, err
		}
		if err := c.check(ctx, subject, ActionExecute, workspaceResource(current)); err != nil {
			return nil, err
		}
		if current.State == proto.WSDestroyed || current.State == proto.WSDestroying || current.State == proto.WSFailed {
			return nil, proto.Err(proto.CodeConflict, "workspace %s is %s", current.ID, current.State)
		}
		ws = current
	} else {
		spec := proto.WorkspaceSpec{}
		if req.Workspace != nil {
			spec = *req.Workspace
		}
		if spec.Name == "" {
			spec.Name = req.Name
		}
		if spec.Repo.URL != "" && spec.Repo.Branch == "" {
			spec.Repo.Branch = "agent/" + id
		}
		if spec.Labels == nil {
			spec.Labels = map[string]string{}
		}
		spec.Labels["remount.agent"] = id
		created, err := c.wsCreate(ctx, subject, &proto.WSCreateReq{Spec: spec, IdempotencyKey: derivedIdem(req.IdempotencyKey, "ws")})
		if err != nil {
			return nil, err
		}
		ws = created
		ownsWS = true
	}
	if policy.Approve == proto.ApproveAuto && !approveAutoAllowed(ws.Spec) {
		if ownsWS {
			_ = c.wsDestroy(ctx, subject.ID, ws.ID, derivedIdem(req.IdempotencyKey, "ws-undo"))
		}
		return nil, proto.Err(proto.CodeDenied, "policy.approve auto needs an isolated workspace (docker backend or a security profile above local)")
	}
	now := c.now().UnixMilli()
	a := &proto.Agent{
		ID: id, Tenant: ws.Tenant, Owner: subject.ID, Name: req.Name, WS: ws.ID, OwnsWS: ownsWS,
		Spec: req.Spec, Mode: agentMode(req.Spec), ACPSessionID: req.ACPSessionID,
		Status: proto.AgentCreating, Inbox: []proto.AgentMessage{}, Runs: []proto.AgentRun{},
		Parent: req.Parent, Policy: policy, CreatedAt: now, UpdatedAt: now,
		URL: c.agentURL(id),
	}
	if req.Spec.Task != "" {
		a.Inbox = append(a.Inbox, proto.AgentMessage{ID: ids.New("m"), Kind: proto.AgentMessageFollowUp, Text: req.Spec.Task, By: subject.ID, At: now})
	}
	wsID := ws.ID
	undo := func() {
		if ownsWS {
			_ = c.wsDestroy(ctx, subject.ID, wsID, derivedIdem(req.IdempotencyKey, "ws-undo"))
		}
	}
	c.mu.Lock()
	current := c.workspaces[wsID]
	if current == nil || current.State == proto.WSDestroyed || current.State == proto.WSDestroying {
		c.mu.Unlock()
		return nil, proto.Err(proto.CodeConflict, "workspace %s is gone", wsID)
	}
	tenantAgents := 0
	for _, other := range c.agents {
		if other.Tenant == a.Tenant && !agentTerminal(other.Status) {
			tenantAgents++
		}
		if other.WS == a.WS && !agentTerminal(other.Status) {
			otherID := other.ID
			c.mu.Unlock()
			undo()
			return nil, proto.Err(proto.CodeConflict, "workspace %s already has live agent %s", wsID, otherID)
		}
	}
	if c.opts.MaxAgentsPerTenant > 0 && tenantAgents >= c.opts.MaxAgentsPerTenant {
		limit := c.opts.MaxAgentsPerTenant
		c.mu.Unlock()
		undo()
		metrics.AgentQuotaRejected.Inc()
		return nil, proto.Err(proto.CodeResourceExhausted, "tenant agent limit %d reached", limit)
	}
	a.Status = deriveAgentStatus(a, current)
	wsCopy := *current
	payload := map[string]any{
		"ws": a.WS, "owns_ws": ownsWS, "recipe": a.Spec.Recipe, "mode": a.Mode,
		"task_hash": proto.AgentTaskHash(a.Spec.Task), "policy": a.Policy,
	}
	if parent != nil {
		payload["parent"] = parent.ID
	}
	if a.ACPSessionID != "" {
		payload["acp_session"] = true
	}
	err := c.persistAgentAndMutation(a, scope, req.IdempotencyKey, proto.OpAgentCreate, req, mutationAgentResult{ID: a.ID},
		c.agentEvent(proto.EvAgentCreated, a, &wsCopy, subject.ID, "", payload))
	if err != nil {
		c.mu.Unlock()
		return nil, err
	}
	c.agents[a.ID] = a
	cp := copyAgent(a)
	c.mu.Unlock()
	metrics.AgentsCreated.Inc()
	c.kickAgents()
	return cp, nil
}

// derivedIdem scopes a sub-operation's idempotency key under the parent's,
// so a replayed agent.create replays its workspace create too. No key means
// no key.
func derivedIdem(parent, suffix string) string {
	if parent == "" {
		return ""
	}
	return parent + "|" + suffix
}

func (c *Control) agentURL(id string) string {
	base := strings.TrimRight(c.opts.PublicURL, "/")
	if base == "" {
		return ""
	}
	return base + "/a/" + id
}

// ---------------------------------------------------------------------------
// agent.get / agent.list
// ---------------------------------------------------------------------------

func (c *Control) agentGet(ctx context.Context, subject Subject, id string) (*proto.Agent, error) {
	if id == "" {
		return nil, proto.Err(proto.CodeBadRequest, "agent id is required")
	}
	a, err := c.agentCopy(id)
	if err != nil {
		return nil, err
	}
	if err := c.agentAuthorize(ctx, subject, a, ActionRead); err != nil {
		return nil, err
	}
	return a, nil
}

func (c *Control) agentList(ctx context.Context, subject Subject, req *proto.AgentListReq) (*proto.AgentListRes, error) {
	c.mu.Lock()
	all := make([]*proto.Agent, 0, len(c.agents))
	for _, a := range c.agents {
		if req.WS != "" && a.WS != req.WS {
			continue
		}
		if req.Parent != "" && a.Parent != req.Parent {
			continue
		}
		if req.Status != "" && a.Status != req.Status {
			continue
		}
		if req.Status == "" && a.Status == proto.AgentDestroyed {
			continue
		}
		all = append(all, copyAgent(a))
	}
	c.mu.Unlock()
	sort.Slice(all, func(i, j int) bool {
		if all[i].CreatedAt != all[j].CreatedAt {
			return all[i].CreatedAt < all[j].CreatedAt
		}
		return all[i].ID < all[j].ID
	})
	out := &proto.AgentListRes{Agents: []proto.Agent{}}
	for _, a := range all {
		if c.agentAuthorize(ctx, subject, a, ActionRead) == nil {
			out.Agents = append(out.Agents, *a)
		}
	}
	return out, nil
}

// ---------------------------------------------------------------------------
// agent.message
// ---------------------------------------------------------------------------

// agentMessage appends to the inbox and commits agent.message. Delivery to
// a live run, or waking a sleeping workspace, happens after the commit: the
// durable inbox is the truth and reconcile re-drives either if the send is
// lost.
func (c *Control) agentMessage(ctx context.Context, subject Subject, req *proto.AgentMessageReq) (*proto.AgentMessageRes, error) {
	if req.ID == "" {
		return nil, proto.Err(proto.CodeBadRequest, "agent id is required")
	}
	if err := proto.ValidateAgentMessage(req.Text); err != nil {
		return nil, err
	}
	kind := req.Kind
	switch kind {
	case "":
		kind = proto.AgentMessageFollowUp
	case proto.AgentMessageFollowUp, proto.AgentMessageSteer:
	default:
		return nil, proto.Err(proto.CodeBadRequest, "kind %q is not follow_up or steer", req.Kind)
	}
	a, err := c.agentCopy(req.ID)
	if err != nil {
		return nil, err
	}
	if err := c.agentAuthorize(ctx, subject, a, ActionExecute); err != nil {
		return nil, err
	}
	scope := a.Tenant + "|" + a.ID + "|agent.message"
	unlockMutation := c.lockMutation(scope, req.IdempotencyKey)
	defer unlockMutation()
	var prior proto.AgentMessageRes
	if hit, err := c.mutationLookup(scope, req.IdempotencyKey, proto.OpAgentMessage, req, &prior); err != nil {
		return nil, err
	} else if hit {
		cur, err := c.agentCopy(req.ID)
		if err != nil {
			return nil, err
		}
		prior.Agent = *cur
		return &prior, nil
	}
	now := c.now().UnixMilli()
	msg := proto.AgentMessage{ID: ids.New("m"), Kind: kind, Text: req.Text, By: subject.ID, At: now}
	c.mu.Lock()
	live := c.agents[a.ID]
	if live == nil {
		c.mu.Unlock()
		return nil, proto.Err(proto.CodeNotFound, "agent %s", a.ID)
	}
	if agentTerminal(live.Status) {
		status := live.Status
		c.mu.Unlock()
		return nil, proto.Err(proto.CodeConflict, "agent %s is %s", a.ID, status)
	}
	if len(live.Inbox) >= proto.MaxAgentInbox {
		c.mu.Unlock()
		metrics.AgentInboxRejected.Inc()
		return nil, proto.Err(proto.CodeResourceExhausted, "agent %s inbox is full (%d)", a.ID, proto.MaxAgentInbox)
	}
	ws := c.workspaces[live.WS]
	run := liveRun(live)
	// ACP has no mid-turn input. A steer is honest about becoming the next
	// follow-up; the harness will see it after its current turn ends.
	degraded := kind == proto.AgentMessageSteer
	if degraded {
		msg.Kind = proto.AgentMessageFollowUp
	}
	live.Inbox = append(live.Inbox, msg)
	live.IdleSince = 0
	sleeping := ws != nil && ws.State == proto.WSPaused
	events := []*proto.Event{c.agentEvent(proto.EvAgentMessage, live, ws, subject.ID, "", map[string]any{
		"message": msg.ID, "kind": kind, "text_hash": proto.AgentTaskHash(msg.Text), "degraded": degraded, "inbox": len(live.Inbox),
	})}
	if !sleeping {
		more, err := c.refreshAgentStatusLocked(live, subject.ID)
		if err != nil {
			c.mu.Unlock()
			return nil, err
		}
		events = append(events, more...)
	}
	res := proto.AgentMessageRes{Message: msg, Degraded: degraded, Woken: sleeping}
	if err := c.persistAgentAndMutation(live, scope, req.IdempotencyKey, proto.OpAgentMessage, req, res, events...); err != nil {
		c.mu.Unlock()
		return nil, err
	}
	var deliver *proto.AgentDeliverReq
	node := ""
	if run != nil && run.State == proto.AgentRunActive && ws != nil && ws.State == proto.WSClaimed && ws.Node == run.Node {
		deliver = &proto.AgentDeliverReq{Agent: live.ID, Run: run.ID, Message: msg}
		node = run.Node
		c.agentDelivered[msg.ID] = c.now()
	}
	wakeTimer := live.WakeTimer
	wsID := live.WS
	res.Agent = *copyAgent(live)
	c.mu.Unlock()
	metrics.AgentMessages.Inc()
	if sleeping {
		if err := c.agentWake(ctx, subject.ID, a.ID, wsID, wakeTimer, "message", derivedIdem(req.IdempotencyKey, "wake")); err != nil {
			c.logger.Warn("agent wake on message", "agent", a.ID, "err", err)
		}
		if cur, err := c.agentCopy(a.ID); err == nil {
			res.Agent = *cur
		}
	} else if deliver != nil && c.send != nil {
		go c.deliverToRun(context.WithoutCancel(ctx), node, deliver)
	}
	c.kickAgents()
	return &res, nil
}

func (c *Control) deliverToRun(ctx context.Context, node string, req *proto.AgentDeliverReq) {
	dctx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	if err := c.send.Request(dctx, node, proto.OpAgentDeliver, req, nil); err != nil {
		c.logger.Warn("agent deliver", "agent", req.Agent, "run", req.Run, "node", node, "err", err)
	}
}

// agentWake resumes a paused workspace on the agent's behalf and commits
// agent.woken. The workspace's own wake is idempotent; a replay is a no-op.
func (c *Control) agentWake(ctx context.Context, principal, agentID, wsID, timer, by, idem string) error {
	if _, err := c.wsWake(ctx, principal, wsID, timer, idem); err != nil {
		return err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	a := c.agents[agentID]
	if a == nil || agentTerminal(a.Status) {
		return nil
	}
	ws := c.workspaces[wsID]
	a.WakeTimer = ""
	events := []*proto.Event{c.agentEvent(proto.EvAgentWoken, a, ws, principal, "", map[string]any{"ws": wsID, "by": by})}
	more, err := c.refreshAgentStatusLocked(a, principal)
	if err != nil {
		return err
	}
	events = append(events, more...)
	metrics.AgentsWoken.Inc()
	return c.persistAgent(a, events...)
}

// ---------------------------------------------------------------------------
// agent.cancel
// ---------------------------------------------------------------------------

// agentCancel drops the inbox and asks the node to stop the current run.
// The agent stays alive: the next message starts a fresh run that loads the
// harness session.
func (c *Control) agentCancel(ctx context.Context, subject Subject, req *proto.AgentGetReq) (*proto.Agent, error) {
	if req.ID == "" {
		return nil, proto.Err(proto.CodeBadRequest, "agent id is required")
	}
	a, err := c.agentCopy(req.ID)
	if err != nil {
		return nil, err
	}
	if err := c.agentAuthorize(ctx, subject, a, ActionExecute); err != nil {
		return nil, err
	}
	scope := a.Tenant + "|" + a.ID + "|agent.cancel"
	unlockMutation := c.lockMutation(scope, req.IdempotencyKey)
	defer unlockMutation()
	var prior mutationAgentResult
	if hit, err := c.mutationLookup(scope, req.IdempotencyKey, proto.OpAgentCancel, req, &prior); err != nil {
		return nil, err
	} else if hit {
		return c.agentCopy(a.ID)
	}
	c.mu.Lock()
	live := c.agents[a.ID]
	if live == nil {
		c.mu.Unlock()
		return nil, proto.Err(proto.CodeNotFound, "agent %s", a.ID)
	}
	if agentTerminal(live.Status) {
		status := live.Status
		c.mu.Unlock()
		return nil, proto.Err(proto.CodeConflict, "agent %s is %s", a.ID, status)
	}
	ws := c.workspaces[live.WS]
	dropped := len(live.Inbox)
	live.Inbox = live.Inbox[:0]
	run := liveRun(live)
	var cancelReq *proto.AgentRunCancelReq
	node := ""
	payload := map[string]any{"dropped": dropped, "by": subject.ID}
	if run != nil {
		payload["run"] = run.ID
		if run.Node != "" {
			cancelReq = &proto.AgentRunCancelReq{Agent: live.ID, Run: run.ID, Reason: "cancel"}
			node = run.Node
		}
		run.TurnMessage = ""
	}
	events := []*proto.Event{c.agentEvent(proto.EvAgentCancelled, live, ws, subject.ID, "", payload)}
	// Approvals the run parked are moot: the node answers them cancelled.
	events = append(events, c.expireApprovalsLocked(live, run, subject.ID)...)
	more, err := c.refreshAgentStatusLocked(live, subject.ID)
	if err != nil {
		c.mu.Unlock()
		return nil, err
	}
	events = append(events, more...)
	if err := c.persistAgentAndMutation(live, scope, req.IdempotencyKey, proto.OpAgentCancel, req, mutationAgentResult{ID: live.ID}, events...); err != nil {
		c.mu.Unlock()
		return nil, err
	}
	cp := copyAgent(live)
	c.mu.Unlock()
	if cancelReq != nil && c.send != nil {
		go c.cancelRun(context.WithoutCancel(ctx), node, cancelReq)
	}
	return cp, nil
}

func (c *Control) cancelRun(ctx context.Context, node string, req *proto.AgentRunCancelReq) {
	cctx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	if err := c.send.Request(cctx, node, proto.OpAgentRunCancel, req, nil); err != nil {
		c.logger.Warn("agent run cancel", "agent", req.Agent, "run", req.Run, "node", node, "err", err)
	}
}

// ---------------------------------------------------------------------------
// agent.sleep
// ---------------------------------------------------------------------------

// agentSleep checkpoints the workspace and pauses it. The run dies with the
// workspace's sessions; the harness's own state is in the tree, so the next
// run loads the session. A message or approval wakes the agent.
func (c *Control) agentSleep(ctx context.Context, subject Subject, req *proto.AgentGetReq) (*proto.Agent, error) {
	if req.ID == "" {
		return nil, proto.Err(proto.CodeBadRequest, "agent id is required")
	}
	a, err := c.agentCopy(req.ID)
	if err != nil {
		return nil, err
	}
	if err := c.agentAuthorize(ctx, subject, a, ActionExecute); err != nil {
		return nil, err
	}
	if agentTerminal(a.Status) {
		return nil, proto.Err(proto.CodeConflict, "agent %s is %s", a.ID, a.Status)
	}
	return c.sleepAgent(ctx, subject.ID, a.ID, "request", req.IdempotencyKey)
}

func (c *Control) sleepAgent(ctx context.Context, principal, id, by, idem string) (*proto.Agent, error) {
	a, err := c.agentCopy(id)
	if err != nil {
		return nil, err
	}
	scope := a.Tenant + "|" + a.ID + "|agent.sleep"
	unlockMutation := c.lockMutation(scope, idem)
	defer unlockMutation()
	request := proto.AgentGetReq{ID: id, IdempotencyKey: idem}
	var prior mutationAgentResult
	if hit, err := c.mutationLookup(scope, idem, proto.OpAgentSleep, request, &prior); err != nil {
		return nil, err
	} else if hit {
		return c.agentCopy(a.ID)
	}
	if run := liveRun(a); run != nil && run.Node != "" && c.send != nil {
		// Stop the harness before the checkpoint so the tree is quiet; the
		// release would kill it anyway, but a clean close lets it flush.
		c.cancelRun(ctx, run.Node, &proto.AgentRunCancelReq{Agent: a.ID, Run: run.ID, Reason: "sleep"})
	}
	timer, err := c.wsSleep(ctx, principal, &proto.WSSleepReq{ID: a.WS, OnEvent: proto.EvAgentMessage, IdempotencyKey: derivedIdem(idem, "ws")})
	if err != nil {
		return nil, err
	}
	c.mu.Lock()
	live := c.agents[a.ID]
	if live == nil {
		c.mu.Unlock()
		return nil, proto.Err(proto.CodeNotFound, "agent %s", a.ID)
	}
	ws := c.workspaces[live.WS]
	live.WakeTimer = timer.ID
	live.IdleSince = 0
	events := []*proto.Event{c.agentEvent(proto.EvAgentSlept, live, ws, principal, "", map[string]any{"ws": live.WS, "timer": timer.ID, "by": by})}
	if run := liveRun(live); run != nil {
		events = append(events, c.finishRunLocked(live, run, ws, principal, "sleep", "", true)...)
	}
	more, err := c.refreshAgentStatusLocked(live, principal)
	if err != nil {
		c.mu.Unlock()
		return nil, err
	}
	events = append(events, more...)
	if err := c.persistAgentAndMutation(live, scope, idem, proto.OpAgentSleep, request, mutationAgentResult{ID: live.ID}, events...); err != nil {
		c.mu.Unlock()
		return nil, err
	}
	cp := copyAgent(live)
	c.mu.Unlock()
	metrics.AgentsSlept.Inc()
	return cp, nil
}

// ---------------------------------------------------------------------------
// agent.destroy
// ---------------------------------------------------------------------------

// agentDestroy ends the agent. The workspace goes with it only when the
// agent created it; an adopted tree is left as the caller had it.
func (c *Control) agentDestroy(ctx context.Context, subject Subject, req *proto.AgentGetReq) error {
	if req.ID == "" {
		return proto.Err(proto.CodeBadRequest, "agent id is required")
	}
	a, err := c.agentCopy(req.ID)
	if err != nil {
		return err
	}
	if err := c.agentAuthorize(ctx, subject, a, ActionWrite); err != nil {
		return err
	}
	scope := a.Tenant + "|" + a.ID + "|agent.destroy"
	unlockMutation := c.lockMutation(scope, req.IdempotencyKey)
	defer unlockMutation()
	var prior struct{}
	if hit, err := c.mutationLookup(scope, req.IdempotencyKey, proto.OpAgentDestroy, req, &prior); err != nil {
		return err
	} else if hit {
		return nil
	}
	if a.Status == proto.AgentDestroyed {
		return nil
	}
	if run := liveRun(a); run != nil && run.Node != "" && c.send != nil {
		c.cancelRun(ctx, run.Node, &proto.AgentRunCancelReq{Agent: a.ID, Run: run.ID, Reason: "destroy"})
	}
	if a.OwnsWS {
		if err := c.wsDestroy(ctx, subject.ID, a.WS, derivedIdem(req.IdempotencyKey, "ws")); err != nil {
			var code string
			if pe, ok := err.(*proto.Error); ok {
				code = pe.Code
			}
			if code != proto.CodeNotFound {
				return err
			}
		}
	}
	c.mu.Lock()
	live := c.agents[a.ID]
	if live == nil {
		c.mu.Unlock()
		return nil
	}
	if live.Status == proto.AgentDestroyed {
		c.mu.Unlock()
		return nil
	}
	ws := c.workspaces[live.WS]
	var events []*proto.Event
	if run := liveRun(live); run != nil {
		events = append(events, c.finishRunLocked(live, run, ws, subject.ID, "destroy", "", true)...)
	}
	events = append(events, c.expireApprovalsLocked(live, nil, subject.ID)...)
	if err := transitionAgent(live.Status, proto.AgentDestroyed); err != nil {
		c.mu.Unlock()
		return err
	}
	live.Status = proto.AgentDestroyed
	live.StatusReason = "destroyed by " + subject.ID
	live.Inbox = live.Inbox[:0]
	live.IdleSince = 0
	live.WakeTimer = ""
	events = append(events, c.agentEvent(proto.EvAgentDestroyed, live, ws, subject.ID, "", map[string]any{"ws": live.WS, "owns_ws": live.OwnsWS}))
	if err := c.persistAgentAndMutation(live, scope, req.IdempotencyKey, proto.OpAgentDestroy, req, struct{}{}, events...); err != nil {
		c.mu.Unlock()
		return err
	}
	delete(c.agentRetry, live.ID)
	c.mu.Unlock()
	metrics.AgentsDestroyed.Inc()
	return nil
}

// ---------------------------------------------------------------------------
// agent.fork
// ---------------------------------------------------------------------------

// agentFork snapshots the source workspace, restores the copy into a new
// workspace, and starts a new agent there that loads the same harness
// session. Where the harness keeps its session state decides how much of
// the conversation the fork sees; the recipe documents that.
func (c *Control) agentFork(ctx context.Context, subject Subject, req *proto.AgentForkReq) (*proto.Agent, error) {
	if req.ID == "" {
		return nil, proto.Err(proto.CodeBadRequest, "agent id is required")
	}
	if req.Task != "" {
		if err := proto.ValidateAgentMessage(req.Task); err != nil {
			return nil, err
		}
	}
	src, err := c.agentCopy(req.ID)
	if err != nil {
		return nil, err
	}
	if err := c.agentAuthorize(ctx, subject, src, ActionExecute); err != nil {
		return nil, err
	}
	if src.Status == proto.AgentDestroyed {
		return nil, proto.Err(proto.CodeConflict, "agent %s is destroyed", src.ID)
	}
	policy := src.Policy
	if req.Policy != nil {
		if err := validateAgentPolicy(*req.Policy); err != nil {
			return nil, err
		}
		policy = normalizeAgentPolicy(*req.Policy)
		if err := policyWithin(policy, src.Policy); err != nil {
			return nil, err
		}
	}
	scope := src.Tenant + "|" + src.ID + "|agent.fork"
	unlockMutation := c.lockMutation(scope, req.IdempotencyKey)
	defer unlockMutation()
	var prior mutationAgentResult
	if hit, err := c.mutationLookup(scope, req.IdempotencyKey, proto.OpAgentFork, req, &prior); err != nil {
		return nil, err
	} else if hit {
		return c.agentCopy(prior.ID)
	}
	ws, err := c.wsGet(src.WS)
	if err != nil {
		return nil, err
	}
	snapshot, err := c.forkSnapshot(ctx, subject, ws, derivedIdem(req.IdempotencyKey, "snapshot"))
	if err != nil {
		return nil, err
	}
	spec := ws.Spec
	spec.Name = req.Name
	spec.RestoreFrom = snapshot
	spec.Base = ""
	spec.Repo = proto.RepoSpec{}
	spec.Run = ""
	spec.Labels = map[string]string{}
	for k, v := range ws.Spec.Labels {
		if k != "remount.agent" {
			spec.Labels[k] = v
		}
	}
	id := ids.New("ag")
	spec.Labels["remount.agent"] = id
	spec.Labels["remount.forked_from"] = src.ID
	copyWS, err := c.wsCreate(ctx, subject, &proto.WSCreateReq{Spec: spec, IdempotencyKey: derivedIdem(req.IdempotencyKey, "ws")})
	if err != nil {
		return nil, err
	}
	now := c.now().UnixMilli()
	a := &proto.Agent{
		ID: id, Tenant: copyWS.Tenant, Owner: subject.ID, Name: req.Name, WS: copyWS.ID, OwnsWS: true,
		Spec: src.Spec, Mode: src.Mode, ACPSessionID: src.ACPSessionID, Capabilities: nil,
		Status: proto.AgentCreating, Inbox: []proto.AgentMessage{}, Runs: []proto.AgentRun{},
		Parent: src.Parent, ForkedFrom: src.ID, Policy: policy, CreatedAt: now, UpdatedAt: now,
		URL: c.agentURL(id),
	}
	a.Spec.Task = req.Task
	if req.Task != "" {
		a.Inbox = append(a.Inbox, proto.AgentMessage{ID: ids.New("m"), Kind: proto.AgentMessageFollowUp, Text: req.Task, By: subject.ID, At: now})
	}
	c.mu.Lock()
	current := c.workspaces[copyWS.ID]
	if current == nil || current.State == proto.WSDestroyed || current.State == proto.WSDestroying {
		c.mu.Unlock()
		return nil, proto.Err(proto.CodeConflict, "workspace %s is gone", copyWS.ID)
	}
	tenantAgents := 0
	for _, other := range c.agents {
		if other.Tenant == a.Tenant && !agentTerminal(other.Status) {
			tenantAgents++
		}
	}
	if c.opts.MaxAgentsPerTenant > 0 && tenantAgents >= c.opts.MaxAgentsPerTenant {
		limit := c.opts.MaxAgentsPerTenant
		c.mu.Unlock()
		_ = c.wsDestroy(ctx, subject.ID, copyWS.ID, derivedIdem(req.IdempotencyKey, "ws-undo"))
		metrics.AgentQuotaRejected.Inc()
		return nil, proto.Err(proto.CodeResourceExhausted, "tenant agent limit %d reached", limit)
	}
	a.Status = deriveAgentStatus(a, current)
	wsCopy := *current
	events := []*proto.Event{
		c.agentEvent(proto.EvAgentForked, a, &wsCopy, subject.ID, "", map[string]any{"from": src.ID, "from_ws": src.WS, "snapshot": snapshot, "task_hash": proto.AgentTaskHash(req.Task)}),
		c.agentEvent(proto.EvAgentCreated, a, &wsCopy, subject.ID, "", map[string]any{
			"ws": a.WS, "owns_ws": true, "recipe": a.Spec.Recipe, "mode": a.Mode, "task_hash": proto.AgentTaskHash(req.Task),
			"policy": a.Policy, "forked_from": src.ID, "acp_session": a.ACPSessionID != "",
		}),
	}
	if err := c.persistAgentAndMutation(a, scope, req.IdempotencyKey, proto.OpAgentFork, req, mutationAgentResult{ID: a.ID}, events...); err != nil {
		c.mu.Unlock()
		return nil, err
	}
	c.agents[a.ID] = a
	cp := copyAgent(a)
	c.mu.Unlock()
	metrics.AgentsForked.Inc()
	c.kickAgents()
	return cp, nil
}

// forkSnapshot gets a durable artifact of the workspace as it is now: the
// node uploads one for a claimed tree; a paused tree already has one.
func (c *Control) forkSnapshot(ctx context.Context, subject Subject, ws *proto.Workspace, idem string) (string, error) {
	switch ws.State {
	case proto.WSClaimed:
		if c.send == nil || !c.send.Online(ws.Node) {
			return "", proto.Err(proto.CodeUnreachable, "node %s holding workspace %s is offline", ws.Node, ws.ID)
		}
		if err := c.check(ctx, subject, ActionExecute, workspaceResource(ws)); err != nil {
			return "", err
		}
		var res proto.WSSnapshotRes
		sctx, cancel := context.WithTimeout(ctx, 5*time.Minute)
		defer cancel()
		err := c.send.Request(sctx, ws.Node, proto.OpWSSnapshot, proto.WSSnapshotReq{WS: ws.ID, Gen: ws.Generation, Upload: true, IdempotencyKey: idem}, &res)
		if err != nil {
			return "", proto.Err(proto.CodeUnreachable, "snapshot for fork: %v", err)
		}
		if res.Artifact == "" {
			return "", proto.Err(proto.CodeInternal, "node returned an empty snapshot for fork")
		}
		return res.Artifact, nil
	case proto.WSPaused, proto.WSPending, proto.WSReleased:
		if ws.LastSnapshot == "" {
			return "", proto.Err(proto.CodeConflict, "workspace %s has no snapshot to fork from", ws.ID)
		}
		return ws.LastSnapshot, nil
	default:
		return "", proto.Err(proto.CodeConflict, "workspace %s is %s; fork needs claimed or paused", ws.ID, ws.State)
	}
}

// ---------------------------------------------------------------------------
// node reports
// ---------------------------------------------------------------------------

// agentReport applies one observation from the node running the harness.
// Reports are ordered per run by Seq and applied at most once; a report
// from a node that does not hold the run's workspace generation is refused.
func (c *Control) agentReport(ctx context.Context, node string, rep *proto.AgentReport) error {
	if rep.Agent == "" || rep.Run == "" {
		return proto.Err(proto.CodeBadRequest, "agent and run are required")
	}
	c.mu.Lock()
	a := c.agents[rep.Agent]
	if a == nil {
		c.mu.Unlock()
		return proto.Err(proto.CodeNotFound, "agent %s", rep.Agent)
	}
	run := findRun(a, rep.Run)
	if run == nil {
		c.mu.Unlock()
		return proto.Err(proto.CodeNotFound, "agent %s run %s", rep.Agent, rep.Run)
	}
	if run.Node != node {
		runNode := run.Node
		c.mu.Unlock()
		return proto.Err(proto.CodeUnauthorized, "run %s belongs to node %s, not %s", rep.Run, runNode, node)
	}
	if rep.WS != a.WS || rep.Gen != run.Generation {
		gen := run.Generation
		c.mu.Unlock()
		return proto.Err(proto.CodeConflict, "report for workspace %s generation %d does not match run generation %d", rep.WS, rep.Gen, gen)
	}
	if run.State == proto.AgentRunDone || rep.Seq <= run.LastReport {
		c.mu.Unlock()
		return nil
	}
	ws := c.workspaces[a.WS]
	run.LastReport = rep.Seq
	now := c.now().UnixMilli()
	var events []*proto.Event
	payload := map[string]any{"run": run.ID, "node": node}
	switch rep.Kind {
	case proto.AgentReportStarted:
		run.State = proto.AgentRunActive
		run.StartedAt = now
		if rep.Transcript != "" {
			run.Transcript = rep.Transcript
			a.TranscriptSession = rep.Transcript
			a.TranscriptNode = node
		}
		payload["transcript"] = run.Transcript
		payload["attempt"] = run.Attempt
		events = append(events, c.agentEvent(proto.EvAgentRunStarted, a, ws, "", node, payload))
	case proto.AgentReportSession:
		run.State = proto.AgentRunActive
		run.Loaded = rep.Loaded
		if rep.ACPSessionID != "" {
			a.ACPSessionID = rep.ACPSessionID
		}
		if rep.Capabilities != nil {
			caps := *rep.Capabilities
			a.Capabilities = &caps
		}
		payload["acp_session_id"] = a.ACPSessionID
		payload["loaded"] = rep.Loaded
		payload["capabilities"] = a.Capabilities
		events = append(events, c.agentEvent(proto.EvAgentSession, a, ws, "", node, payload))
	case proto.AgentReportTurnStarted:
		// The message stays in the inbox until the turn finishes so a harness
		// that dies mid-turn is re-prompted with it on the retry.
		run.State = proto.AgentRunActive
		run.TurnMessage = rep.Message
		a.IdleSince = 0
	case proto.AgentReportTurnFinished:
		run.Turns++
		a.Turns++
		a.Failures = 0
		done := run.TurnMessage
		if done == "" {
			done = rep.Message
		}
		for i := range a.Inbox {
			if a.Inbox[i].ID == done {
				a.Inbox = append(a.Inbox[:i], a.Inbox[i+1:]...)
				break
			}
		}
		delete(c.agentDelivered, done)
		payload["message"] = done
		payload["stop_reason"] = rep.StopReason
		payload["turn"] = a.Turns
		if rep.Usage != nil {
			payload["tokens"] = map[string]int64{"input": rep.Usage.Input, "output": rep.Usage.Output}
		}
		run.TurnMessage = ""
		events = append(events, c.agentEvent(proto.EvAgentTurn, a, ws, "", node, payload))
		if a.Policy.MaxTurns > 0 && a.Turns >= a.Policy.MaxTurns {
			if err := transitionAgent(a.Status, proto.AgentFinished); err == nil {
				a.Status = proto.AgentFinished
				a.StatusReason = fmt.Sprintf("max_turns %d reached", a.Policy.MaxTurns)
				a.Inbox = a.Inbox[:0]
				events = append(events, c.agentEvent(proto.EvAgentFinished, a, ws, "", node, map[string]any{"reason": a.StatusReason}))
				metrics.AgentsFinished.Inc()
			}
		}
	case proto.AgentReportToolCall:
		payload["tool_call"] = rep.ToolCall
		payload["kind"] = rep.ToolKind
		payload["title"] = rep.ToolTitle
		payload["status"] = rep.ToolStatus
		payload["locations"] = rep.Locations
		events = append(events, c.agentEvent(proto.EvAgentToolCall, a, ws, "", node, payload))
	case proto.AgentReportPermission, proto.AgentReportElicitation:
		if rep.Approval == nil {
			c.mu.Unlock()
			return proto.Err(proto.CodeBadRequest, "%s report needs an approval", rep.Kind)
		}
		more, err := c.parkApprovalLocked(a, run, ws, node, rep.Approval)
		if err != nil {
			c.mu.Unlock()
			return err
		}
		events = append(events, more...)
	case proto.AgentReportFinished:
		events = append(events, c.finishRunLocked(a, run, ws, "", rep.StopReason, rep.Error, rep.Cancelled)...)
		if rep.ExitCode != 0 && rep.Error == "" && !rep.Cancelled {
			run.Error = fmt.Sprintf("harness exited %d", rep.ExitCode)
		}
		if run.Error != "" && !rep.Cancelled {
			a.Failures++
			if a.Failures >= agentMaxFailures {
				if err := transitionAgent(a.Status, proto.AgentFailed); err == nil {
					a.Status = proto.AgentFailed
					a.StatusReason = run.Error
					a.Inbox = a.Inbox[:0]
					events = append(events, c.agentEvent(proto.EvAgentFailed, a, ws, "", node, map[string]any{"reason": run.Error, "run": run.ID, "attempt": run.Attempt}))
					metrics.AgentsFailed.Inc()
				}
			} else {
				c.agentRetry[a.ID] = c.now().Add(agentRetryBackoff)
			}
		}
	case proto.AgentReportTranscript:
		// Bulk data, not a state change: it is stored, not evented.
	default:
		c.mu.Unlock()
		return proto.Err(proto.CodeBadRequest, "unknown agent report kind %q", rep.Kind)
	}
	more, err := c.refreshAgentStatusLocked(a, "")
	if err != nil {
		c.mu.Unlock()
		return err
	}
	events = append(events, more...)
	if rep.Kind == proto.AgentReportTranscript {
		err = c.mirrorTranscriptLocked(a, run, rep.Chunks, events)
	} else {
		err = c.persistAgent(a, events...)
	}
	if err != nil {
		c.mu.Unlock()
		return err
	}
	needKick := run.State == proto.AgentRunDone && len(a.Inbox) > 0 && !agentTerminal(a.Status)
	c.mu.Unlock()
	if needKick {
		c.kickAgents()
	}
	return nil
}

// finishRunLocked closes a run and returns its events. It is the one place
// a run becomes done, whether the node said so, the control plane gave up
// on it, or a lifecycle operation killed it. Caller holds c.mu.
func (c *Control) finishRunLocked(a *proto.Agent, run *proto.AgentRun, ws *proto.Workspace, principal, stopReason, errText string, cancelled bool) []*proto.Event {
	if run.State == proto.AgentRunDone {
		return nil
	}
	run.State = proto.AgentRunDone
	run.FinishedAt = c.now().UnixMilli()
	run.StopReason = stopReason
	run.Error = errText
	run.TurnMessage = ""
	events := []*proto.Event{c.agentEvent(proto.EvAgentRunFinished, a, ws, principal, run.Node, map[string]any{
		"run": run.ID, "attempt": run.Attempt, "stop_reason": stopReason, "error": errText, "cancelled": cancelled, "turns": run.Turns,
	})}
	events = append(events, c.expireApprovalsLocked(a, run, principal)...)
	metrics.AgentRunsFinished.Inc()
	return events
}

// ---------------------------------------------------------------------------
// reconcile: the loop that turns durable intent into node work
// ---------------------------------------------------------------------------

// kickAgents asks the background loop to reconcile now rather than at the
// next tick. It never blocks.
func (c *Control) kickAgents() {
	select {
	case c.agentKick <- struct{}{}:
	default:
	}
}

type agentDispatch struct {
	node string
	req  proto.AgentRunReq
}

type agentRedeliver struct {
	node string
	req  proto.AgentDeliverReq
}

// agentReconcile is idempotent and runs every tick. For each live agent it
// decides at most one thing: start a run, wake the workspace, re-deliver a
// message, give up on a lost run, or put the agent to sleep. Decisions that
// change state commit before the network call they imply; the call failing
// is recorded and retried on a later tick.
func (c *Control) agentReconcile(ctx context.Context) {
	if c.send == nil {
		return
	}
	now := c.now()
	var dispatches []agentDispatch
	var redeliveries []agentRedeliver
	var wakes []struct{ agent, ws, timer string }
	var sleeps []string
	c.mu.Lock()
	keys := make([]string, 0, len(c.agents))
	for id := range c.agents {
		keys = append(keys, id)
	}
	sort.Strings(keys)
	for _, id := range keys {
		a := c.agents[id]
		if agentTerminal(a.Status) {
			continue
		}
		ws := c.workspaces[a.WS]
		var events []*proto.Event
		run := liveRun(a)
		if run != nil {
			lost := ""
			switch {
			case ws == nil || ws.Generation != run.Generation:
				lost = "workspace moved"
			case ws.State == proto.WSPaused || ws.State == proto.WSReleased || ws.State == proto.WSPending:
				lost = "workspace released"
			case ws.State == proto.WSDestroyed || ws.State == proto.WSDestroying || ws.State == proto.WSFailed:
				lost = "workspace " + ws.State
			case run.State == proto.AgentRunPending && now.UnixMilli()-run.StartedAt > agentLaunchTimeout.Milliseconds():
				lost = "launch timed out"
			case run.State == proto.AgentRunActive && !c.send.Online(run.Node) && c.nodeGone(run.Node):
				lost = "node gone"
			}
			if lost != "" {
				cancelled := lost == "workspace released"
				events = append(events, c.finishRunLocked(a, run, ws, "", lost, "", cancelled)...)
				if !cancelled {
					run.Error = lost
					c.agentRetry[a.ID] = now.Add(agentRetryBackoff)
				}
				run = nil
			}
		}
		if ws != nil && !agentTerminal(a.Status) {
			switch {
			case run == nil && len(a.Inbox) > 0 && ws.State == proto.WSClaimed && c.send.Online(ws.Node) && !c.agentRetry[a.ID].After(now):
				attempt := 1
				if n := len(a.Runs); n > 0 {
					attempt = a.Runs[n-1].Attempt + 1
				}
				newRun := proto.AgentRun{ID: ids.New("run"), Attempt: attempt, Node: ws.Node, Generation: ws.Generation, State: proto.AgentRunPending, StartedAt: now.UnixMilli()}
				a.Runs = append(a.Runs, newRun)
				events = append(events, c.agentEvent(proto.EvAgentRunStarted, a, ws, "", ws.Node, map[string]any{
					"run": newRun.ID, "attempt": attempt, "node": ws.Node, "pending": true, "inbox": len(a.Inbox),
				}))
				dispatches = append(dispatches, agentDispatch{node: ws.Node, req: proto.AgentRunReq{
					Agent: a.ID, Run: newRun.ID, Attempt: attempt, WS: a.WS, Gen: ws.Generation, Tenant: a.Tenant, Owner: a.Owner,
					Spec: a.Spec, Policy: a.Policy, Mode: a.Mode, ACPSessionID: a.ACPSessionID,
					Messages: append([]proto.AgentMessage(nil), a.Inbox...),
				}})
				for _, m := range a.Inbox {
					c.agentDelivered[m.ID] = now
				}
			case run == nil && len(a.Inbox) > 0 && ws.State == proto.WSPaused:
				wakes = append(wakes, struct{ agent, ws, timer string }{a.ID, a.WS, a.WakeTimer})
			case run != nil && run.State == proto.AgentRunActive && run.TurnMessage == "" && len(a.Inbox) > 0 && ws.State == proto.WSClaimed:
				m := a.Inbox[0]
				if last, ok := c.agentDelivered[m.ID]; !ok || now.Sub(last) > agentRedeliverAfter {
					c.agentDelivered[m.ID] = now
					redeliveries = append(redeliveries, agentRedeliver{node: run.Node, req: proto.AgentDeliverReq{Agent: a.ID, Run: run.ID, Message: m}})
				}
			case (a.Status == proto.AgentWaitingInput || a.Status == proto.AgentIdle) && a.Policy.SleepAfterSec > 0 && a.IdleSince > 0 && ws.State == proto.WSClaimed &&
				now.UnixMilli()-a.IdleSince >= a.Policy.SleepAfterSec*1000 && a.PendingApprovals == 0:
				sleeps = append(sleeps, a.ID)
			}
		}
		more, err := c.refreshAgentStatusLocked(a, "")
		if err != nil {
			c.logger.Error("agent status", "agent", a.ID, "err", err)
			continue
		}
		events = append(events, more...)
		if len(events) > 0 {
			if err := c.persistAgent(a, events...); err != nil {
				c.logger.Error("persist agent", "agent", a.ID, "err", err)
			}
		}
	}
	c.mu.Unlock()
	for _, d := range dispatches {
		c.launchRun(ctx, d)
	}
	for _, r := range redeliveries {
		go c.deliverToRun(ctx, r.node, &r.req)
	}
	for _, w := range wakes {
		if err := c.agentWake(ctx, "", w.agent, w.ws, w.timer, "message", ""); err != nil {
			c.logger.Warn("agent wake", "agent", w.agent, "err", err)
		}
	}
	for _, id := range sleeps {
		if _, err := c.sleepAgent(ctx, "", id, "policy", ""); err != nil {
			c.logger.Warn("agent policy sleep", "agent", id, "err", err)
		}
	}
}

// nodeGone reports whether a node has been offline past the lease: an
// active run there cannot be reporting anymore.
func (c *Control) nodeGone(node string) bool {
	n := c.nodes[node]
	if n == nil {
		return true
	}
	if n.Status.Online {
		return false
	}
	return n.Status.LastSeen > 0 && c.now().UnixMilli()-n.Status.LastSeen > c.opts.LeaseSec*1000
}

// launchRun sends the run to the node. The run row is already pending; a
// refused launch closes it with an error and lets the retry policy decide.
func (c *Control) launchRun(ctx context.Context, d agentDispatch) {
	rctx, cancel := context.WithTimeout(ctx, 60*time.Second)
	defer cancel()
	var res proto.AgentRunRes
	err := c.send.Request(rctx, d.node, proto.OpAgentRun, d.req, &res)
	c.mu.Lock()
	defer c.mu.Unlock()
	a := c.agents[d.req.Agent]
	if a == nil {
		return
	}
	run := findRun(a, d.req.Run)
	if run == nil || run.State == proto.AgentRunDone {
		return
	}
	ws := c.workspaces[a.WS]
	var events []*proto.Event
	if err != nil {
		text := "launch: " + err.Error()
		events = append(events, c.finishRunLocked(a, run, ws, "", "launch_failed", text, false)...)
		a.Failures++
		if a.Failures >= agentMaxFailures {
			if terr := transitionAgent(a.Status, proto.AgentFailed); terr == nil {
				a.Status = proto.AgentFailed
				a.StatusReason = text
				a.Inbox = a.Inbox[:0]
				events = append(events, c.agentEvent(proto.EvAgentFailed, a, ws, "", d.node, map[string]any{"reason": text, "run": run.ID, "attempt": run.Attempt}))
				metrics.AgentsFailed.Inc()
			}
		} else {
			c.agentRetry[a.ID] = c.now().Add(agentRetryBackoff)
		}
	} else {
		if res.Transcript != "" {
			run.Transcript = res.Transcript
			a.TranscriptSession = res.Transcript
			a.TranscriptNode = d.node
		}
		metrics.AgentRunsStarted.Inc()
	}
	more, terr := c.refreshAgentStatusLocked(a, "")
	if terr != nil {
		c.logger.Error("agent status", "agent", a.ID, "err", terr)
	}
	events = append(events, more...)
	if len(events) > 0 || err == nil {
		if perr := c.persistAgent(a, events...); perr != nil {
			c.logger.Error("persist agent", "agent", a.ID, "err", perr)
		}
	}
}
