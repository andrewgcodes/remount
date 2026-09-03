package control

import (
	"context"
	"fmt"
	"sort"
	"time"

	"remount.dev/remount/internal/metrics"
	"remount.dev/remount/internal/proto"
	"remount.dev/remount/internal/tenant"
)

// residencySubject is one held workspace and the placement labels the control
// plane believes its node advertises, collected under the workspace lock and
// evaluated after it is released.
type residencySubject struct {
	workspace string
	tenant    string
	node      string
	labels    map[string]string
	// known is false when the node is not in the fleet map at all, so its
	// labels are absent rather than empty. An absent label set can never be
	// read as conforming.
	known bool
}

// tenantPolicyFindings reports retention and residency problems the control
// plane can see without asking a node.
//
// Both checks obey the same rule as every other diagnostic here: verified good
// is healthy, verified bad is unhealthy, and anything the check could not read
// is unavailable. A residency check that cannot resolve tenant policy or node
// labels must never render as a pass, because an unearned "healthy" on a data
// residency control is worse than no check at all.
func (c *Control) tenantPolicyFindings(ctx context.Context) []proto.Finding {
	subjects := c.residencySubjectsLocked()
	findings := c.residencyFindings(ctx, subjects)
	return append(findings, c.retentionFindings(ctx)...)
}

func (c *Control) residencySubjectsLocked() []residencySubject {
	c.mu.Lock()
	defer c.mu.Unlock()
	subjects := make([]residencySubject, 0, len(c.workspaces))
	for id, ws := range c.workspaces {
		if !held(ws.State) || ws.Node == "" || ws.Tenant == "" {
			continue
		}
		subject := residencySubject{workspace: id, tenant: ws.Tenant, node: ws.Node}
		if n, ok := c.nodes[ws.Node]; ok {
			subject.known = true
			subject.labels = make(map[string]string, len(n.Status.Labels))
			for key, value := range n.Status.Labels {
				subject.labels[key] = value
			}
		}
		subjects = append(subjects, subject)
	}
	sort.Slice(subjects, func(i, j int) bool { return subjects[i].workspace < subjects[j].workspace })
	return subjects
}

func (c *Control) residencyFindings(ctx context.Context, subjects []residencySubject) []proto.Finding {
	if len(subjects) == 0 {
		return nil
	}
	if c.opts.Tenants == nil {
		return []proto.Finding{{
			Severity: "warn", Check: "tenant.residency_unavailable",
			Detail: fmt.Sprintf("%d held workspaces carry a tenant, but this control plane has no tenant authority to read a residency policy from", len(subjects)),
			Hint:   "residency was not checked; do not read this as a pass",
		}}
	}
	var findings []proto.Finding
	policies := make(map[string]tenant.Residency, len(subjects))
	unreadable := make(map[string]string)
	for _, subject := range subjects {
		if _, resolved := policies[subject.tenant]; !resolved {
			if _, failed := unreadable[subject.tenant]; !failed {
				current, err := c.opts.Tenants.Get(ctx, subject.tenant)
				if err != nil {
					unreadable[subject.tenant] = codeOfError(mapTenantError(err))
				} else {
					policies[subject.tenant] = current.Policy.Residency
				}
			}
		}
		if reason, failed := unreadable[subject.tenant]; failed {
			findings = append(findings, proto.Finding{
				Severity: "warn", Check: "tenant.residency_unavailable", Subject: subject.workspace,
				Detail: "tenant " + subject.tenant + " policy could not be read (" + reason + ")",
				Hint:   "residency was not checked for this workspace; do not read this as a pass",
			})
			continue
		}
		policy := policies[subject.tenant]
		if len(policy.AllowedRegions) == 0 && len(policy.RequiredLabels) == 0 {
			continue
		}
		if !subject.known {
			findings = append(findings, proto.Finding{
				Severity: "warn", Check: "tenant.residency_unavailable", Subject: subject.workspace,
				Detail: "held by " + subject.node + ", whose placement labels this control plane cannot read",
				Hint:   "residency was not checked for this workspace; do not read this as a pass",
			})
			continue
		}
		if tenant.MatchResidency(policy, subject.labels) {
			continue
		}
		metrics.TenantResidencyDenied.Inc()
		findings = append(findings, proto.Finding{
			Severity: "error", Check: "tenant.residency_violation", Subject: subject.workspace,
			Detail: fmt.Sprintf("held by %s in region %q, which tenant %s residency policy does not allow",
				subject.node, subject.labels["region"], subject.tenant),
			Hint: "move the workspace to a conforming node; placement already refuses new assignments here",
		})
	}
	return findings
}

// retentionFindings reports what the two-mechanism retention design cannot
// hide: a tenant whose event envelopes outlive its own policy because a
// longer-lived tenant holds the contiguous prefix, and an artifact store that
// cannot apply a per-tenant cutoff at all.
func (c *Control) retentionFindings(ctx context.Context) []proto.Finding {
	if c.opts.Tenants == nil {
		return nil
	}
	tenants, err := c.opts.Tenants.List(ctx)
	if err != nil {
		metrics.TenantRetentionUnenforced.Set(0)
		return []proto.Finding{{
			Severity: "warn", Check: "tenant.retention_unavailable",
			Detail: "the tenant directory could not be listed (" + codeOfError(mapTenantError(err)) + ")",
			Hint:   "retention policy was not checked; do not read this as a pass",
		}}
	}
	var longest time.Duration
	var longestTenant string
	artifactPolicies := 0
	for _, current := range tenants {
		if current.State == tenant.StateDeleted {
			continue
		}
		if current.Policy.Retention.Artifacts > 0 {
			artifactPolicies++
		}
		if current.Policy.Retention.Events > longest {
			longest, longestTenant = current.Policy.Retention.Events, current.ID
		}
	}
	var findings []proto.Finding
	unenforced := 0
	for _, current := range tenants {
		if current.State == tenant.StateDeleted {
			continue
		}
		policy := current.Policy.Retention.Events
		if policy <= 0 || policy >= longest {
			continue
		}
		unenforced++
		findings = append(findings, proto.Finding{
			Severity: "warn", Check: "tenant.retention_violation", Subject: current.ID,
			Detail: fmt.Sprintf("event content is removed at %s as the policy requires, but the row envelopes survive until %s because tenant %s holds the contiguous prefix",
				policy, longest, longestTenant),
			Hint: "shorten the longest tenant retention, or accept that sequence, time, type and tenant outlive the payload (ADR 0080)",
		})
	}
	if artifactPolicies > 0 && !c.TenantArtifactRetentionEnforceable() {
		unenforced += artifactPolicies
		findings = append(findings, proto.Finding{
			Severity: "warn", Check: "tenant.retention_unavailable",
			Detail: fmt.Sprintf("%d tenants set an artifact retention, but the configured artifact store cannot apply a per-tenant cutoff", artifactPolicies),
			Hint:   "artifact retention was not enforced; one tenant's schedule must never sweep a shared namespace",
		})
	}
	metrics.TenantRetentionUnenforced.Set(int64(unenforced))
	return findings
}
