package control

import (
	"sort"

	"remount.dev/remount/internal/metrics"
	"remount.dev/remount/internal/profile"
	"remount.dev/remount/internal/proto"
)

// nodeDescriptorsLocked returns the per-backend evidence to evaluate a node
// against. A v1 peer that sent no descriptors is credited only with a local,
// non-isolating backend, exactly as eligibleBackendLocked does: absence is
// evidence for weakness, never for strength.
func nodeDescriptorsLocked(info proto.NodeInfo) []proto.BackendDescriptor {
	if len(info.BackendDescriptors) > 0 {
		return info.BackendDescriptors
	}
	out := make([]proto.BackendDescriptor, 0, len(info.Backends))
	for _, name := range info.Backends {
		out = append(out, proto.BackendDescriptor{
			Name:     name,
			Security: proto.BackendSecurityCaps{Isolation: "none", EgressMode: "open", BrokerIdentity: "none"},
			Runtime:  proto.RuntimeCaps{Snapshots: info.Snapshots},
		})
	}
	return out
}

// evaluateNodeLocked runs one runtime profile against one node's evidence.
//
// The node's own RuntimeChecks are passed in, but profile.Evaluate uses them
// only to downgrade: a node reporting "pass" for a host check cannot make a
// backend descriptor satisfy a predicate it fails. Callers hold c.mu.
func evaluateNodeLocked(n *nodeState, p profile.Profile) profile.Report {
	if n == nil {
		return profile.Evaluate(p, nil, nil)
	}
	return profile.Evaluate(p, nodeDescriptorsLocked(n.Status.Info), n.Status.Info.RuntimeChecks)
}

// nodeSatisfiesProfileLocked is the scheduling predicate for
// Requires.Profile. An offline node satisfies nothing: its evidence is its
// last hello and it cannot be asked to re-prove anything. Callers hold c.mu.
func (c *Control) nodeSatisfiesProfileLocked(n *nodeState, want string) bool {
	p, err := profile.Parse(want)
	if err != nil {
		return false
	}
	if n == nil || !n.Status.Online {
		return false
	}
	return evaluateNodeLocked(n, p).OK()
}

// anyNodeSatisfiesProfileLocked reports whether some online node could take a
// workspace requiring want. Callers hold c.mu.
func (c *Control) anyNodeSatisfiesProfileLocked(want string) bool {
	for _, n := range c.nodes {
		if c.nodeSatisfiesProfileLocked(n, want) {
			return true
		}
	}
	return false
}

// profileHealthLocked recomputes a node's runtime-profile standing and
// returns the events its transitions require. The caller commits them in the
// same transaction as the node row, so a change of standing is never a state
// change without an event.
//
// The transition is defined on the node's *claimed* profile, because that is
// the promise an operator made about this machine. A node claiming dev never
// becomes unschedulable: dev makes no security claim, so there is nothing to
// lose.
func (c *Control) profileHealthLocked(id string, n *nodeState) []*proto.Event {
	if n == nil {
		return nil
	}
	claimed, err := profile.Parse(n.Status.Info.Profile)
	if err != nil {
		// An unparseable claim is not a reason to trust the node more.
		claimed = profile.Dev
	}
	report := evaluateNodeLocked(n, claimed)
	ok := report.OK() && n.Status.Online
	was, evaluated := n.profileVerified, n.profileEvaluated
	n.profileVerified, n.profileEvaluated = ok, true

	payload := proto.NodeProfileEvent{
		Node: id, Profile: string(claimed), Status: report.Status, Failed: report.Failed(),
	}
	switch {
	case !evaluated && ok:
		if !claimed.MakesSecurityClaim() {
			return nil
		}
		return []*proto.Event{c.newEvent(proto.EvNodeProfileVerified, id, "", id, payload)}
	case !evaluated:
		if !claimed.MakesSecurityClaim() {
			return nil
		}
		metrics.ProfileDriftDetected.Inc()
		return []*proto.Event{c.newEvent(proto.EvNodeProfileUnschedulable, id, "", id, payload)}
	case was && !ok:
		metrics.ProfileDriftDetected.Inc()
		return []*proto.Event{c.newEvent(proto.EvNodeProfileUnschedulable, id, "", id, payload)}
	case !was && ok:
		return []*proto.Event{c.newEvent(proto.EvNodeProfileRestored, id, "", id, payload)}
	default:
		return nil
	}
}

// refreshNodeGaugesLocked keeps the fleet gauges honest after any change to
// node membership or health. Callers hold c.mu.
func (c *Control) refreshNodeGaugesLocked() {
	var online, unschedulable int64
	for _, n := range c.nodes {
		if !n.Status.Online {
			continue
		}
		online++
		if n.profileEvaluated && !n.profileVerified {
			unschedulable++
		}
	}
	metrics.NodesOnline.Set(online)
	metrics.NodesProfileUnschedulable.Set(unschedulable)
}

// nodeProfileGet answers node.profile.get. Everything it returns is built
// from copies; no pointer into control-plane state escapes the lock.
func (c *Control) nodeProfileGet(req *proto.NodeProfileGetReq) (*proto.NodeProfileGetRes, error) {
	if req.Profile != "" {
		if _, err := profile.Parse(req.Profile); err != nil {
			return nil, proto.Err(proto.CodeBadRequest, "%s", err.Error())
		}
	}
	now := c.now().UnixMilli()
	c.mu.Lock()
	defer c.mu.Unlock()
	if req.Node != "" {
		n := c.nodes[req.Node]
		if n == nil {
			return nil, proto.Err(proto.CodeNotFound, "node %s is unknown", req.Node)
		}
		return &proto.NodeProfileGetRes{
			Profile: req.Profile,
			Nodes:   []proto.NodeProfileReport{nodeProfileReportLocked(req.Node, n, req.Profile, now)},
		}, nil
	}
	res := &proto.NodeProfileGetRes{Profile: req.Profile}
	for id, n := range c.nodes {
		res.Nodes = append(res.Nodes, nodeProfileReportLocked(id, n, req.Profile, now))
	}
	sort.Slice(res.Nodes, func(i, j int) bool { return res.Nodes[i].Node < res.Nodes[j].Node })
	return res, nil
}

// nodeProfileReportLocked evaluates want (or the node's own claim when want
// is empty) and copies the result out. Callers hold c.mu.
func nodeProfileReportLocked(id string, n *nodeState, want string, now int64) proto.NodeProfileReport {
	target := want
	if target == "" {
		target = n.Status.Info.Profile
	}
	p, err := profile.Parse(target)
	if err != nil {
		p = profile.Dev
	}
	report := evaluateNodeLocked(n, p)
	out := proto.NodeProfileReport{
		Node: id, Profile: string(p), Status: report.Status, EvaluatedAt: now,
		Configured: n.Status.Info.Profile, Online: n.Status.Online,
	}
	out.Checks = append(out.Checks, report.Checks...)
	if !n.Status.Online {
		// Evidence from a peer that is gone cannot be re-proved, so the
		// report says unavailable rather than repeating a stale pass.
		out.Status = proto.CheckUnavailable
		out.Checks = append(out.Checks, proto.Finding{
			Severity: "warn", Status: proto.CheckUnavailable, Check: "profile.node.online", Subject: id,
			Detail: "the node is offline; its evidence is its last hello and cannot be re-proved",
			Hint:   "an unavailable check is not a passing check",
		})
	}
	return out
}

// applyNodeRuntimeChecksLocked records a node's pushed host checks and
// returns the events its transitions require. Callers hold c.mu.
func (c *Control) applyNodeRuntimeChecksLocked(id string, n *nodeState, claimed string, checks []proto.Finding) []*proto.Event {
	if n == nil {
		return nil
	}
	if claimed != "" {
		n.Status.Info.Profile = claimed
	}
	n.Status.Info.RuntimeChecks = append([]proto.Finding(nil), checks...)
	return c.profileHealthLocked(id, n)
}
