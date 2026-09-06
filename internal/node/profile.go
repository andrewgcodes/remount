package node

import (
	"context"
	"sort"
	"time"

	"remount.dev/remount/internal/profile"
	"remount.dev/remount/internal/proto"
	"remount.dev/remount/internal/workspace"
)

// Profile reports the runtime profile this node was configured with. It is a
// claim, not evidence; the control plane decides what the node satisfies.
func (n *Node) Profile() profile.Profile {
	n.profileMu.Lock()
	defer n.profileMu.Unlock()
	return n.profile
}

// ProfileReport evaluates the node's configured profile against its own
// backend descriptors and its latest host checks. It is what the startup gate
// and `remount doctor --profile` see for this node, and it is the same
// function the control plane runs on the descriptors it received.
func (n *Node) ProfileReport() profile.Report {
	n.profileMu.Lock()
	p, checks := n.profile, append([]proto.Finding(nil), n.profileChecks...)
	reported := n.profileReported
	n.profileMu.Unlock()
	if !reported {
		checks = nil
	}
	return profile.Evaluate(p, n.opts.Backends.Descriptors(), checks)
}

// profileHealthLoop is drift detection. Every backend's Caps are fixed once
// its constructor returns, so without this a node keeps advertising a
// boundary that stopped existing the moment a kernel module was unloaded, a
// binary was replaced or a capability was dropped. It re-probes at
// Options.ProfileHealthInterval, records the result, and lets the next renewal
// carry it to the control plane.
func (n *Node) profileHealthLoop(ctx context.Context) {
	defer n.wg.Done()
	interval := n.opts.ProfileHealthInterval
	if interval <= 0 {
		interval = DefaultProfileHealthInterval
	}
	// Probe once immediately: a node that came up degraded must not serve a
	// profile-requiring workspace for a whole interval first.
	n.refreshProfileHealth(ctx)
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-n.stop:
			return
		case <-t.C:
			n.refreshProfileHealth(ctx)
		}
	}
}

// refreshProfileHealth re-probes every backend that can drift and records the
// verdict. The probe runs without n.mu and without n.profileMu: it performs
// host I/O, and neither lock may be held across it.
func (n *Node) refreshProfileHealth(ctx context.Context) {
	probeCtx, cancel := context.WithTimeout(ctx, profileProbeTimeout)
	checks := workspace.ReprobeRegistry(probeCtx, n.opts.Backends)
	cancel()
	sort.SliceStable(checks, func(i, j int) bool { return checks[i].Check < checks[j].Check })
	report := profile.Evaluate(n.Profile(), n.opts.Backends.Descriptors(), checks)
	degraded := !report.OK()

	n.profileMu.Lock()
	was, first := n.profileDegraded, !n.profileReported
	n.profileChecks = checks
	n.profileReported = true
	n.profileDegraded = degraded
	n.profileVersion++
	n.profileMu.Unlock()

	if degraded == was && !first {
		return
	}
	payload := proto.NodeProfileEvent{
		Node: n.id, Profile: string(n.Profile()), Status: report.Status, Failed: report.Failed(),
	}
	switch {
	case degraded:
		// The node records its own transition even while its uplink is down;
		// the control plane records its own view separately when the checks
		// reach it. Both are transitions of different state, so both emit.
		n.logger.Error("runtime profile is no longer satisfied", "profile", payload.Profile,
			"status", report.Status, "failed", payload.Failed)
		n.emit(proto.EvNodeProfileUnschedulable, n.id, "", payload)
	case !first:
		n.logger.Info("runtime profile restored", "profile", payload.Profile)
		n.emit(proto.EvNodeProfileRestored, n.id, "", payload)
	}
}

// profileProbeTimeout bounds one drift probe. Every implementation is a
// handful of stat and exec calls; a probe that cannot finish in this window
// is itself the signal.
const profileProbeTimeout = 20 * time.Second

// profileHealthForRenew returns the checks to attach to the next renewal.
// report is false when nothing changed since the control plane last accepted
// a set, so a steady node adds nothing to its renewals. The version is
// cleared only by profileHealthDelivered, so a renewal that fails leaves the
// checks pending instead of losing them.
func (n *Node) profileHealthForRenew() (checks []proto.Finding, version uint64, report bool) {
	n.profileMu.Lock()
	defer n.profileMu.Unlock()
	if !n.profileReported || n.profileVersion == n.profileDelivered {
		return nil, 0, false
	}
	return append([]proto.Finding(nil), n.profileChecks...), n.profileVersion, true
}

// profileHealthDelivered records that the control plane accepted version.
// A newer probe that landed while the renewal was in flight stays pending.
func (n *Node) profileHealthDelivered(version uint64) {
	n.profileMu.Lock()
	defer n.profileMu.Unlock()
	if version > n.profileDelivered {
		n.profileDelivered = version
	}
}

// profileHealthSnapshot is what the hello and diagnostics report.
func (n *Node) profileHealthSnapshot() []proto.Finding {
	n.profileMu.Lock()
	defer n.profileMu.Unlock()
	if !n.profileReported {
		return nil
	}
	return append([]proto.Finding(nil), n.profileChecks...)
}

// requireSchedulableProfile refuses to materialize a workspace whose
// Requires.Profile makes a security claim this node currently cannot honor.
// The control plane already filters on the same evidence; this is the node's
// own fail-closed re-check after the claim, in the same spirit as the
// ValidateBackendSecurity re-check in materialize.
func (n *Node) requireSchedulableProfile(spec proto.WorkspaceSpec) error {
	wanted, err := profile.Parse(spec.Requires.Profile)
	if err != nil {
		return proto.ErrReason(proto.CodeDenied, proto.ReasonProfileUnschedulable, "%s", err.Error())
	}
	if !wanted.MakesSecurityClaim() {
		return nil
	}
	checks := n.profileHealthSnapshot()
	report := profile.Evaluate(wanted, n.opts.Backends.Descriptors(), checks)
	if report.OK() {
		return nil
	}
	return proto.ErrReason(proto.CodeDenied, proto.ReasonProfileUnschedulable,
		"node %s does not satisfy runtime profile %s: %v", n.id, wanted, report.Failed())
}
