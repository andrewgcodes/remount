package control

import (
	"context"
	"sort"
	"time"

	"remount.dev/remount/internal/artifact"
	"remount.dev/remount/internal/eventlog"
	"remount.dev/remount/internal/metrics"
	"remount.dev/remount/internal/proto"
	"remount.dev/remount/internal/tenant"
)

// Per-tenant retention, and why it is shaped like this.
//
// The canonical event log is one sequence and retention deletes only a
// contiguous oldest prefix, because a hole punched into the middle of the
// sequence would make an export that skipped it look complete. That rule is
// what `internal/compliance` relies on to report a gap instead of a short
// bundle, so per-tenant retention may not break it.
//
// The consequence is two mechanisms rather than one:
//
//  1. The prefix is deleted at the OLDEST cutoff any tenant still requires.
//     A tenant that keeps events for ninety days therefore holds rows that a
//     seven-day tenant would rather have gone. No tenant's rows are ever
//     removed before that tenant's own policy allows, which is the property
//     that makes one tenant's policy unable to destroy another's evidence.
//  2. Between a tenant's own cutoff and that global floor, the tenant's event
//     CONTENT is removed in place. The envelope — sequence, time, type, tenant
//     — survives, so the sequence stays contiguous and a reader still learns
//     that something happened; the payload becomes the constant redaction
//     marker. This is what makes Retention.Events actually delete on the
//     tenant's own schedule rather than on the slowest tenant's schedule.
//
// Artifacts are the mirror image. A tenant's Retention.Artifacts is a maximum
// age, so it may only accelerate collection relative to the shared grace
// window, never delay it. It can never select a referenced object at any age:
// a live workspace, base, fleet or session-log closure outranks a retention
// policy, and an artifact still held by one is reported as a violation rather
// than deleted.

// tenantRetentionBatch bounds one redaction statement so a large first pass
// yields the event-log mutex to current appends between batches.
const tenantRetentionBatch = 10_000

// tenantRetentionPasses bounds how many batches one tenant may take in a
// single pass, so a very large backlog cannot starve the other tenants or the
// prune that follows. The remainder is redacted on the next pass.
const tenantRetentionPasses = 16

// TenantCutoff is one tenant's own retention boundary.
type TenantCutoff struct {
	Tenant string
	Before time.Time
}

// RetentionPlan is one resolved retention pass across every tenant.
type RetentionPlan struct {
	// PruneBefore is how far the contiguous event prefix may be deleted. It is
	// the oldest cutoff any tenant still requires, never a tenant's own.
	PruneBefore time.Time
	// Redact names each tenant whose own event retention has already elapsed
	// for rows that PruneBefore still protects.
	Redact []TenantCutoff
	// ArtifactCutoffs is the per-tenant artifact collection cutoff, present
	// only for a tenant whose policy is stricter than the shared grace window.
	ArtifactCutoffs map[string]time.Time
	// UntenantedExtendedBy is how much longer than the server's own retention
	// events belonging to no tenant are now held, because the floor is set by
	// the longest-lived tenant. Zero when nothing extended it.
	UntenantedExtendedBy time.Duration
	// Tenants is every tenant the plan considered, for diagnostics.
	Tenants []TenantRetention
}

// TenantRetention is one tenant's resolved policy.
type TenantRetention struct {
	Tenant    string
	State     string
	Events    time.Duration
	Artifacts time.Duration
	// EventsRedactedFrom is the tenant's own event cutoff when it is earlier
	// than the global prune floor, and the zero time when the prefix delete
	// already satisfies the policy.
	EventsRedactedFrom time.Time
}

// TenantRetentionPlan resolves every tenant's retention policy into one pass.
//
// fallback is the server's own event retention, applied to events that belong
// to no tenant and to a tenant that set no policy of its own. grace is the
// shared artifact collection window; a tenant artifact policy longer than it
// changes nothing, because retention is a maximum age and the shared window
// already collects sooner.
func (c *Control) TenantRetentionPlan(ctx context.Context, now time.Time, fallback, grace time.Duration) (RetentionPlan, error) {
	plan := RetentionPlan{PruneBefore: now.Add(-fallback), ArtifactCutoffs: map[string]time.Time{}}
	if c.opts.Tenants == nil {
		return plan, nil
	}
	tenants, err := c.opts.Tenants.List(ctx)
	if err != nil {
		return RetentionPlan{}, err
	}
	cutoffs := make(map[string]time.Time, len(tenants))
	for _, current := range tenants {
		if current.State == tenant.StateDeleted {
			// A deleted tenant keeps no claim on the log; its rows leave with
			// the prefix the remaining tenants allow.
			continue
		}
		events := current.Policy.Retention.Events
		if events <= 0 {
			events = fallback
		}
		cutoff := now.Add(-events)
		cutoffs[current.ID] = cutoff
		if cutoff.Before(plan.PruneBefore) {
			plan.PruneBefore = cutoff
		}
		plan.Tenants = append(plan.Tenants, TenantRetention{
			Tenant: current.ID, State: string(current.State),
			Events: events, Artifacts: current.Policy.Retention.Artifacts,
		})
		if artifacts := current.Policy.Retention.Artifacts; artifacts > 0 && artifacts < grace {
			plan.ArtifactCutoffs[current.ID] = now.Add(-artifacts)
		}
	}
	// The floor is known only after every tenant has been seen, so the
	// redaction list is built in a second pass over the resolved cutoffs.
	for i := range plan.Tenants {
		cutoff := cutoffs[plan.Tenants[i].Tenant]
		if !cutoff.After(plan.PruneBefore) {
			continue
		}
		plan.Tenants[i].EventsRedactedFrom = cutoff
		plan.Redact = append(plan.Redact, TenantCutoff{Tenant: plan.Tenants[i].Tenant, Before: cutoff})
	}
	sort.Slice(plan.Redact, func(i, j int) bool { return plan.Redact[i].Tenant < plan.Redact[j].Tenant })
	if extended := now.Add(-fallback).Sub(plan.PruneBefore); extended > 0 {
		plan.UntenantedExtendedBy = extended
	}
	return plan, nil
}

// EnforceTenantEventRetention removes the content of every tenant's events
// that its own policy has already expired but the contiguous prefix still
// holds. It returns how many events were redacted.
//
// Every outcome is both an event and a metric. A pass that quietly failed
// would leave a tenant believing its data was destroyed, which is the one
// mistake a retention system may not make.
func (c *Control) EnforceTenantEventRetention(ctx context.Context, plan RetentionPlan) (int64, error) {
	if c.opts.Log == nil {
		return 0, nil
	}
	var total int64
	for _, target := range plan.Redact {
		redacted, err := c.redactTenantEvents(ctx, target)
		total += redacted
		if err != nil {
			metrics.TenantRetentionFailed.Inc()
			c.emitRetentionEvent(proto.EvRetentionViolation, target.Tenant, map[string]any{
				"scope": "events", "before": target.Before.UnixMilli(), "redacted": redacted, "reason": codeOfError(err),
			})
			return total, err
		}
		if redacted == 0 {
			continue
		}
		metrics.TenantRetentionEnforced.Inc()
		c.emitRetentionEvent(proto.EvRetentionEnforced, target.Tenant, map[string]any{
			"scope": "events", "before": target.Before.UnixMilli(), "redacted": redacted,
		})
	}
	return total, nil
}

func (c *Control) redactTenantEvents(ctx context.Context, target TenantCutoff) (int64, error) {
	var redacted int64
	for pass := 0; pass < tenantRetentionPasses; pass++ {
		if err := ctx.Err(); err != nil {
			return redacted, err
		}
		n, err := c.opts.Log.RedactTenant(ctx, target.Tenant, target.Before.UnixMilli(), tenantRetentionBatch)
		redacted += n
		if err != nil {
			return redacted, err
		}
		if n < tenantRetentionBatch {
			return redacted, nil
		}
	}
	return redacted, nil
}

// emitRetentionEvent records one retention outcome. The event is best effort
// in the sense that a failure to store it must not abort the pass, but it is
// never silent: the failure is logged and the paired counter has already
// moved.
func (c *Control) emitRetentionEvent(typ, tenantID string, payload map[string]any) {
	event := c.newEvent(typ, "tenant:"+tenantID, "control", "", payload)
	event.Tenant = tenantID
	if err := c.transact(func(*eventlog.Tx) error { return nil }, []*proto.Event{event}); err != nil {
		c.logger.Warn("retention outcome was not recorded", "err", err, "tenant", tenantID, "type", typ)
	}
}

// TenantArtifactRetentionEnforceable reports whether the configured artifact
// store can apply a per-tenant cutoff at all. A store that cannot separate
// tenants must never be swept on one tenant's schedule, so the honest answer
// is that the policy is unenforced and a diagnostic says so rather than a
// global sweep silently deleting another tenant's bytes.
func (c *Control) TenantArtifactRetentionEnforceable() bool {
	_, ok := c.opts.TenantArtifacts.(artifact.TenantRetentionCollector)
	return ok
}

// EnforceTenantResidency records every workspace that is currently held
// outside its tenant's residency policy and returns how many it found.
//
// Placement already refuses a non-conforming node, so this catches drift: a
// policy tightened, or a label changed, under a workspace that is already
// running. That is a durable compliance fact, so it is an event as well as a
// counter. A condition that is still true on the next pass is recorded again,
// because an unresolved residency violation is a continuing state and an
// operator watching the log should see that it did not go away.
func (c *Control) EnforceTenantResidency(ctx context.Context) int {
	if c.opts.Tenants == nil {
		return 0
	}
	subjects := c.residencySubjectsLocked()
	denied := 0
	for _, subject := range subjects {
		current, err := c.opts.Tenants.Get(ctx, subject.tenant)
		if err != nil {
			// Unreadable policy is unavailable, never conforming. doctor
			// reports it; enforcement declines to guess.
			continue
		}
		policy := current.Policy.Residency
		if len(policy.AllowedRegions) == 0 && len(policy.RequiredLabels) == 0 {
			continue
		}
		if subject.known && tenant.MatchResidency(policy, subject.labels) {
			continue
		}
		denied++
		metrics.TenantResidencyDenied.Inc()
		c.emitResidencyDenial(subject, policy)
	}
	return denied
}

func (c *Control) emitResidencyDenial(subject residencySubject, policy tenant.Residency) {
	reason := "labels_unavailable"
	if subject.known {
		reason = "policy_mismatch"
	}
	event := c.newEvent(proto.EvResidencyDenied, subject.workspace, "control", subject.node, map[string]any{
		"reason": reason, "region": subject.labels["region"], "allowed_regions": policy.AllowedRegions,
	})
	event.Tenant, event.Workspace = subject.tenant, subject.workspace
	if err := c.transact(func(*eventlog.Tx) error { return nil }, []*proto.Event{event}); err != nil {
		c.logger.Warn("residency denial was not recorded", "err", err, "ws", subject.workspace)
	}
}
