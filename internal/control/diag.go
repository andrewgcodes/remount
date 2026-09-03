package control

import (
	"context"
	"fmt"
	"time"

	"remount.dev/remount/internal/metrics"
	"remount.dev/remount/internal/proto"
)

// Diag reports the control plane's own view of its health, plus every problem
// it can see without asking a node. It is deliberately cheap enough to poll.
func (c *Control) Diag(ctx context.Context) *proto.ControlDiag {
	now := c.now()
	d := &proto.ControlDiag{
		Now:                     now.UnixMilli(),
		Uptime:                  int64(now.Sub(c.started).Seconds()),
		WorkspaceState:          map[string]int{},
		LeaseSec:                c.opts.LeaseSec,
		Bindings:                c.Bindings(),
		Metrics:                 metrics.Default.Snapshot(),
		TimersMax:               c.opts.MaxTimers,
		MutationRecordsMax:      c.opts.MaxMutationRecords,
		WorkspacesPerTenantMax:  c.opts.MaxWorkspacesPerTenant,
		WorkspacesPerSubjectMax: c.opts.MaxWorkspacesPerSubject,
		EventsMax:               c.opts.MaxEvents,
		RequestsActiveMax:       c.opts.MaxConcurrentRequests,
	}
	if seq, err := c.log.Last(ctx); err == nil {
		d.EventSeq = seq
	}
	if seq, err := c.log.First(ctx); err == nil {
		d.EventOldest = seq
	}
	_ = c.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM mutations`).Scan(&d.MutationRecords)
	if c.opts.Artifacts != nil {
		stats := c.opts.Artifacts.Stats()
		d.ArtifactCount = stats.Objects
		d.ArtifactBytes = stats.Bytes
		d.ArtifactReservedBytes = stats.ReservedBytes
		d.ArtifactReservedObjects = stats.ReservedObjects
		d.ArtifactMaxBytes = stats.MaxBytes
		d.ArtifactMaxObjects = stats.MaxObjects
	}

	c.mu.Lock()
	for _, ws := range c.workspaces {
		d.WorkspaceState[ws.State]++
	}
	d.NodesTotal = len(c.nodes)
	for _, n := range c.nodes {
		if n.Status.Online {
			d.NodesOnline++
		}
	}
	for _, t := range c.timers {
		if t.Fired {
			d.TimersFired++
		} else {
			d.TimersPending++
		}
	}
	// Collect findings while we hold the lock, emit them after.
	type pending struct{ id, state, node, pool string }
	var stuck []pending
	var expiring []pending
	for _, ws := range c.workspaces {
		switch {
		case ws.State == proto.WSPending:
			// Pending is only a problem if nothing could ever claim it.
			eligible := 0
			for _, n := range c.nodes {
				if c.eligibleLocked(ws, n) {
					eligible++
				}
			}
			if eligible == 0 {
				poolName := ""
				for _, candidate := range c.pools {
					if poolMatchesWorkspace(candidate, ws) {
						poolName = candidate.Spec.Name
						break
					}
				}
				stuck = append(stuck, pending{id: ws.ID, state: ws.State, pool: poolName})
			}
		case held(ws.State) && ws.LeaseUntil > 0:
			if remaining := ws.LeaseUntil - now.UnixMilli(); remaining < 0 {
				expiring = append(expiring, pending{id: ws.ID, state: ws.State, node: ws.Node})
			}
		}
		// A workspace claimed by a node that is not connected is held but
		// unreachable, which is worth saying out loud.
		if held(ws.State) && ws.Node != "" {
			if n, ok := c.nodes[ws.Node]; !ok || !n.Status.Online {
				d.Findings = append(d.Findings, proto.Finding{
					Severity: "warn", Check: "workspace.node_offline", Subject: ws.ID,
					Detail: fmt.Sprintf("held by %s, which is offline; the lease expires at %s",
						ws.Node, time.UnixMilli(ws.LeaseUntil).Format(time.RFC3339)),
					Hint: "it will return to pending and another eligible node will claim it",
				})
			}
		}
	}
	c.mu.Unlock()

	for _, p := range stuck {
		if p.pool != "" && c.opts.PoolReconciler != nil {
			d.Findings = append(d.Findings, proto.Finding{
				Severity: "warn", Check: "workspace.capacity_pending", Subject: p.id,
				Detail: "pending while node pool " + p.pool + " reconciles provider capacity",
				Hint:   "inspect pool.scaled and pool.provision_failed events",
			})
			continue
		}
		detail := "pending, and no online node satisfies its requires and placement"
		hint := "compare `remount ws get " + p.id + "` with `remount nodes`"
		if p.pool != "" {
			detail = "pending with matching pool " + p.pool + ", but no provisioner reconciler is configured"
			hint = "start the server with --provisioners and inspect its startup diagnostics"
		}
		d.Findings = append(d.Findings, proto.Finding{
			Severity: "error", Check: "workspace.unplaceable", Subject: p.id,
			Detail: detail,
			Hint:   hint,
		})
	}
	for _, p := range expiring {
		d.Findings = append(d.Findings, proto.Finding{
			Severity: "warn", Check: "workspace.lease_overdue", Subject: p.id,
			Detail: "lease deadline has passed but it has not been re-queued yet",
			Hint:   "expected within one tick; persistent means the control loop is stalled",
		})
	}
	if d.NodesOnline == 0 && d.NodesTotal > 0 {
		d.Findings = append(d.Findings, proto.Finding{
			Severity: "error", Check: "fleet.no_nodes_online",
			Detail: fmt.Sprintf("%d nodes known, none connected", d.NodesTotal),
			Hint:   "check node logs for token or URL errors",
		})
	}
	if v := d.Metrics["remount_egress_leak_blocked_total"]; v > 0 {
		d.Findings = append(d.Findings, proto.Finding{
			Severity: "warn", Check: "egress.leak_attempts",
			Detail: fmt.Sprintf("%g credential placeholders were sent to unbound hosts", v),
			Hint:   "`remount events | grep leak_blocked` shows which workspace and which host",
		})
	}
	if v := d.Metrics["remount_artifact_digest_mismatch_total"]; v > 0 {
		d.Findings = append(d.Findings, proto.Finding{
			Severity: "error", Check: "artifact.corruption",
			Detail: fmt.Sprintf("%g artifacts failed digest verification", v),
			Hint:   "a snapshot was damaged in transit or at rest; `remount doctor` re-verifies the store",
		})
	}
	if v := d.Metrics["remount_session_gaps_total"]; v > 0 {
		d.Findings = append(d.Findings, proto.Finding{
			Severity: "warn", Check: "session.replay_gaps",
			Detail: fmt.Sprintf("%g replay gaps were reported to clients", v),
			Hint:   "output was evicted before a client asked for it; raise the session log limits",
		})
	}
	return d
}

// verifyArtifacts re-hashes every blob and reports the damaged ones. This is
// the deepest integrity check available: it reads every byte rather than
// trusting a counter.
func (c *Control) verifyArtifacts() []proto.Finding {
	if c.opts.Artifacts == nil {
		return []proto.Finding{{
			Severity: "warn", Check: "artifact.verify_unavailable",
			Detail: "this control plane has no artifact store configured",
			Hint:   "snapshots were not verified; do not read this as a pass",
		}}
	}
	ids, err := c.opts.Artifacts.List()
	if err != nil {
		return []proto.Finding{{Severity: "error", Check: "artifact.list", Detail: err.Error()}}
	}
	var out []proto.Finding
	bad := 0
	for _, id := range ids {
		if err := c.opts.Artifacts.Verify(id); err != nil {
			bad++
			out = append(out, proto.Finding{
				Severity: "error", Check: "artifact.digest", Subject: id, Detail: err.Error(),
				Hint: "the blob no longer matches its content address; a restore from it would produce wrong files",
			})
		}
	}
	out = append(out, proto.Finding{
		Severity: "info", Check: "artifact.verified",
		Detail: fmt.Sprintf("re-hashed %d artifacts, %d damaged", len(ids), bad),
	})
	return out
}

// VerifyForTest exposes the artifact verifier to tests in other packages.
func (c *Control) VerifyForTest() []proto.Finding { return c.verifyArtifacts() }

// DBIntegrity runs SQLite's own integrity check.
func (c *Control) DBIntegrity(ctx context.Context) string {
	var res string
	if err := c.db.QueryRowContext(ctx, `PRAGMA integrity_check`).Scan(&res); err != nil {
		return "error: " + err.Error()
	}
	return res
}

// WorkspacePlacement returns, for one workspace, which nodes are eligible and
// why the others are not. This is what answers "why is this pending".
func (c *Control) WorkspacePlacement(id string) []proto.Finding {
	c.mu.Lock()
	defer c.mu.Unlock()
	ws := c.workspaces[id]
	if ws == nil {
		return []proto.Finding{{Severity: "error", Check: "workspace.missing", Subject: id, Detail: "no such workspace"}}
	}
	var out []proto.Finding
	for nid, n := range c.nodes {
		if c.eligibleLocked(ws, n) {
			out = append(out, proto.Finding{Severity: "info", Check: "placement.eligible", Subject: nid, Detail: "satisfies requires and placement"})
			continue
		}
		out = append(out, proto.Finding{
			Severity: "info", Check: "placement.ineligible", Subject: nid,
			Detail: c.ineligibleReason(ws, n),
		})
	}
	return out
}

// ineligibleReason explains, in one sentence, why a node cannot take a
// workspace. Caller holds c.mu.
func (c *Control) ineligibleReason(ws *proto.Workspace, n *nodeState) string {
	if !n.Status.Online {
		return "offline"
	}
	r := ws.Spec.Requires
	if r.Backend != "" && !contains(n.Status.Info.Backends, r.Backend) {
		return fmt.Sprintf("no %q backend; has %v", r.Backend, n.Status.Info.Backends)
	}
	if r.OS != "" && n.Status.Info.OS != r.OS {
		return fmt.Sprintf("os is %s, workspace requires %s", n.Status.Info.OS, r.OS)
	}
	if r.Arch != "" && n.Status.Info.Arch != r.Arch {
		return fmt.Sprintf("arch is %s, workspace requires %s", n.Status.Info.Arch, r.Arch)
	}
	if r.CPU > 0 && n.Status.Info.CPU < r.CPU {
		return fmt.Sprintf("has %d cpus, workspace requires %d", n.Status.Info.CPU, r.CPU)
	}
	if r.MemMiB > 0 && n.Status.Info.MemMiB < r.MemMiB {
		return fmt.Sprintf("has %d MiB, workspace requires %d MiB", n.Status.Info.MemMiB, r.MemMiB)
	}
	for _, cap := range r.Caps {
		if !contains(n.Status.Info.Caps, cap) {
			return fmt.Sprintf("lacks capability %q", cap)
		}
	}
	p := ws.Spec.Placement
	if p.Node != "" && p.Node != n.Status.ID {
		return "workspace is pinned to " + p.Node
	}
	for k, v := range p.Allow {
		if n.Status.Labels[k] != v {
			return fmt.Sprintf("label %s=%q does not match required %q", k, n.Status.Labels[k], v)
		}
	}
	if _, ok := c.eligibleBackendLocked(ws, n); !ok {
		return fmt.Sprintf("no backend satisfies security profile %q", ws.Spec.Security.Profile)
	}
	return "unknown"
}
