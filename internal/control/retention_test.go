package control

import (
	"context"
	"testing"
	"time"

	"remount.dev/remount/internal/eventlog"
	"remount.dev/remount/internal/metrics"
	"remount.dev/remount/internal/proto"
)

// retentionPolicy is a tenant policy carrying only the retention this test
// cares about, in the milliseconds the wire uses.
func retentionPolicy(events, artifacts time.Duration) proto.TenantPolicy {
	return proto.TenantPolicy{Retention: proto.TenantRetention{
		EventsMS: events.Milliseconds(), ArtifactsMS: artifacts.Milliseconds(),
	}}
}

func appendTenantEvent(t *testing.T, f *controlFixture, tenantID, typ string, at time.Time, payload map[string]any) proto.Event {
	t.Helper()
	event := &proto.Event{
		EventID: typ + "-" + tenantID + "-" + at.Format(time.RFC3339Nano), Origin: "control", Type: typ,
		Tenant: tenantID, Stream: "tenant:" + tenantID, At: at.UnixMilli(), ReceivedAt: at.UnixMilli(),
		Payload: proto.MustMarshal(payload),
	}
	if err := f.log.Append(context.Background(), event); err != nil {
		t.Fatal(err)
	}
	return *event
}

func createRetentionTenants(t *testing.T, f *controlFixture, policies map[string]proto.TenantPolicy) {
	t.Helper()
	global := Subject{ID: "root", Tenant: "*", Roles: []string{"admin"}}
	for id, policy := range policies {
		if _, err := f.c.tenantCreate(context.Background(), global, &proto.TenantCreateReq{
			ID: id, Policy: policy, IdempotencyKey: "create-" + id,
		}); err != nil {
			t.Fatal(err)
		}
	}
}

// The floor is the oldest instant any tenant still requires, so a short-lived
// tenant can never shorten a long-lived tenant's prefix, and a long-lived
// tenant's floor is what a short-lived tenant's rows are redacted against.
func TestRetentionPlanFloorIsTheOldestTenantRequirement(t *testing.T) {
	f := tenantControlFixture(t)
	ctx := context.Background()
	createRetentionTenants(t, f, map[string]proto.TenantPolicy{
		"tenant-short": retentionPolicy(2*time.Hour, 0),
		"tenant-long":  retentionPolicy(100*time.Hour, 0),
	})
	now := time.Now()
	plan, err := f.c.TenantRetentionPlan(ctx, now, 24*time.Hour, 24*time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	if want := now.Add(-100 * time.Hour); !plan.PruneBefore.Equal(want) {
		t.Fatalf("prune floor=%s want %s", plan.PruneBefore, want)
	}
	if plan.UntenantedExtendedBy != 76*time.Hour {
		t.Fatalf("untenanted events extended by %s", plan.UntenantedExtendedBy)
	}
	// Only the short-lived tenant needs redaction. The long-lived tenant sets
	// the floor, so the prefix delete already satisfies its policy exactly.
	if len(plan.Redact) != 1 || plan.Redact[0].Tenant != "tenant-short" {
		t.Fatalf("redaction targets=%+v", plan.Redact)
	}
	if want := now.Add(-2 * time.Hour); !plan.Redact[0].Before.Equal(want) {
		t.Fatalf("redaction cutoff=%s want %s", plan.Redact[0].Before, want)
	}
}

// Artifact retention is a maximum age: shorter than the shared grace window it
// collects sooner, longer than it changes nothing, because the shared window
// already collects first.
func TestRetentionPlanArtifactCutoffOnlyAccelerates(t *testing.T) {
	f := tenantControlFixture(t)
	createRetentionTenants(t, f, map[string]proto.TenantPolicy{
		"tenant-strict": retentionPolicy(0, time.Hour),
		"tenant-loose":  retentionPolicy(0, 72*time.Hour),
	})
	now := time.Now()
	plan, err := f.c.TenantRetentionPlan(context.Background(), now, 24*time.Hour, 24*time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	if len(plan.ArtifactCutoffs) != 1 {
		t.Fatalf("artifact cutoffs=%+v", plan.ArtifactCutoffs)
	}
	if want := now.Add(-time.Hour); !plan.ArtifactCutoffs["tenant-strict"].Equal(want) {
		t.Fatalf("strict cutoff=%s want %s", plan.ArtifactCutoffs["tenant-strict"], want)
	}
	if _, present := plan.ArtifactCutoffs["tenant-loose"]; present {
		t.Fatal("a retention longer than the grace window must not delay collection")
	}
}

// The property the whole design exists to guarantee: enforcing tenant A's
// retention removes tenant A's content and leaves tenant B byte-identical,
// including B's rows that are older than A's cutoff.
func TestTenantRetentionNeverTouchesAnotherTenant(t *testing.T) {
	f := tenantControlFixture(t)
	ctx := context.Background()
	createRetentionTenants(t, f, map[string]proto.TenantPolicy{
		"tenant-a": retentionPolicy(time.Hour, 0),
		"tenant-b": retentionPolicy(100*time.Hour, 0),
	})
	now := time.Now()
	old := now.Add(-50 * time.Hour)
	aOld := appendTenantEvent(t, f, "tenant-a", "ws.created", old, map[string]any{"secret": "a-private"})
	bOld := appendTenantEvent(t, f, "tenant-b", "ws.created", old, map[string]any{"secret": "b-private"})
	aNew := appendTenantEvent(t, f, "tenant-a", "ws.created", now, map[string]any{"secret": "a-current"})

	plan, err := f.c.TenantRetentionPlan(ctx, now, 24*time.Hour, 24*time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	redacted, err := f.c.EnforceTenantEventRetention(ctx, plan)
	if err != nil || redacted != 1 {
		t.Fatalf("redacted=%d err=%v", redacted, err)
	}
	events, err := f.log.Read(ctx, 0, "", 1000)
	if err != nil {
		t.Fatal(err)
	}
	marker := string(eventlog.RedactedPayload())
	for _, event := range events {
		switch event.Seq {
		case aOld.Seq:
			if string(event.Payload) != marker {
				t.Fatalf("tenant-a expired payload survived: %q", event.Payload)
			}
			if event.Type != aOld.Type || event.Tenant != "tenant-a" || event.At != aOld.At {
				t.Fatalf("redaction changed the envelope: %+v", event)
			}
		case bOld.Seq:
			if string(event.Payload) != string(bOld.Payload) {
				t.Fatalf("tenant-a retention altered tenant-b: %q", event.Payload)
			}
		case aNew.Seq:
			if string(event.Payload) != string(aNew.Payload) {
				t.Fatalf("a live tenant-a event was redacted: %q", event.Payload)
			}
		}
	}
	// Contiguity is what the compliance exporter relies on to report a gap
	// rather than a short bundle, so redaction must leave it untouched.
	for i := 1; i < len(events); i++ {
		if events[i].Seq != events[i-1].Seq+1 {
			t.Fatalf("redaction punched a hole between %d and %d", events[i-1].Seq, events[i].Seq)
		}
	}
}

// A retention pass that quietly succeeded or quietly failed would leave a
// tenant believing something about its data that is not true, so every pass
// leaves both an attributable event and a counter.
func TestTenantRetentionEmitsAnEventAndAMetric(t *testing.T) {
	f := tenantControlFixture(t)
	ctx := context.Background()
	createRetentionTenants(t, f, map[string]proto.TenantPolicy{
		"tenant-a": retentionPolicy(time.Hour, 0),
		"tenant-b": retentionPolicy(100*time.Hour, 0),
	})
	now := time.Now()
	appendTenantEvent(t, f, "tenant-a", "ws.created", now.Add(-50*time.Hour), map[string]any{"secret": "a-private"})
	before := metrics.TenantRetentionEnforced.Value()

	plan, err := f.c.TenantRetentionPlan(ctx, now, 24*time.Hour, 24*time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.c.EnforceTenantEventRetention(ctx, plan); err != nil {
		t.Fatal(err)
	}
	if delta := metrics.TenantRetentionEnforced.Value() - before; delta != 1 {
		t.Fatalf("retention counter delta=%d", delta)
	}
	events, err := f.log.Read(ctx, 0, "", 1000)
	if err != nil {
		t.Fatal(err)
	}
	enforced := false
	for _, event := range events {
		if event.Type == proto.EvRetentionEnforced && event.Tenant == "tenant-a" {
			enforced = true
		}
	}
	if !enforced {
		t.Fatal("retention moved a counter without recording an event")
	}
}

// A residency policy tightened under a running workspace is drift the
// scheduler cannot catch, so it is both recorded and counted.
func TestResidencyDriftEmitsAnEventAndAMetric(t *testing.T) {
	f := tenantControlFixture(t)
	ctx := context.Background()
	createRetentionTenants(t, f, map[string]proto.TenantPolicy{"tenant-a": {}})
	connectNode(t, f.c, "n_drift", processNodeInfo(4096))
	f.c.mu.Lock()
	f.c.nodes["n_drift"].Status.Labels = map[string]string{"region": "us-west-2"}
	f.c.mu.Unlock()
	alice := Subject{ID: "alice", Tenant: "tenant-a", Roles: []string{"admin"}}
	workspace, err := f.c.wsCreate(ctx, alice, &proto.WSCreateReq{IdempotencyKey: "ws-drift"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.c.wsClaim(ctx, "n_drift", workspace.ID); err != nil {
		t.Fatal(err)
	}
	if denied := f.c.EnforceTenantResidency(ctx); denied != 0 {
		t.Fatalf("a conforming placement was reported as a violation: %d", denied)
	}
	// Tighten the policy under the running workspace.
	global := Subject{ID: "root", Tenant: "*", Roles: []string{"admin"}}
	current, err := f.c.tenantGet(ctx, global, "tenant-a")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.c.tenantUpdate(ctx, global, &proto.TenantUpdateReq{
		ID: "tenant-a", Policy: proto.TenantPolicy{Residency: proto.TenantResidency{AllowedRegions: []string{"us-east-1"}}},
		ExpectedRevision: current.Revision, IdempotencyKey: "tighten-a",
	}); err != nil {
		t.Fatal(err)
	}
	before := metrics.TenantResidencyDenied.Value()
	if denied := f.c.EnforceTenantResidency(ctx); denied != 1 {
		t.Fatalf("residency drift denied=%d", denied)
	}
	if delta := metrics.TenantResidencyDenied.Value() - before; delta != 1 {
		t.Fatalf("residency counter delta=%d", delta)
	}
	events, err := f.log.Read(ctx, 0, "", 1000)
	if err != nil {
		t.Fatal(err)
	}
	recorded := false
	for _, event := range events {
		if event.Type == proto.EvResidencyDenied && event.Workspace == workspace.ID {
			recorded = true
		}
	}
	if !recorded {
		t.Fatal("residency drift moved a counter without recording an event")
	}
}

// A check that cannot read tenant policy or node labels must say so. An
// unearned "healthy" on a residency control is the most dangerous output a
// diagnostic can produce.
func TestResidencyCheckThatCannotRunIsUnavailable(t *testing.T) {
	ctx := context.Background()
	t.Run("no tenant authority", func(t *testing.T) {
		f := newControlFixture(t, "", nil)
		findings := f.c.residencyFindings(ctx, []residencySubject{{workspace: "ws_1", tenant: "tenant-a", node: "n1", known: true}})
		if len(findings) != 1 || findings[0].Check != "tenant.residency_unavailable" || findings[0].Severity != "warn" {
			t.Fatalf("findings=%+v", findings)
		}
	})
	t.Run("node labels unreadable", func(t *testing.T) {
		f := tenantControlFixture(t)
		createRetentionTenants(t, f, map[string]proto.TenantPolicy{
			"tenant-a": {Residency: proto.TenantResidency{AllowedRegions: []string{"us-east-1"}}},
		})
		findings := f.c.residencyFindings(ctx, []residencySubject{{workspace: "ws_1", tenant: "tenant-a", node: "n_gone"}})
		if len(findings) != 1 || findings[0].Check != "tenant.residency_unavailable" || findings[0].Subject != "ws_1" {
			t.Fatalf("findings=%+v", findings)
		}
	})
	t.Run("policy unreadable", func(t *testing.T) {
		f := tenantControlFixture(t)
		findings := f.c.residencyFindings(ctx, []residencySubject{{workspace: "ws_1", tenant: "tenant-missing", node: "n1", known: true}})
		if len(findings) != 1 || findings[0].Check != "tenant.residency_unavailable" {
			t.Fatalf("findings=%+v", findings)
		}
	})
	t.Run("verified bad is a violation, not unavailable", func(t *testing.T) {
		f := tenantControlFixture(t)
		createRetentionTenants(t, f, map[string]proto.TenantPolicy{
			"tenant-a": {Residency: proto.TenantResidency{AllowedRegions: []string{"us-east-1"}}},
		})
		findings := f.c.residencyFindings(ctx, []residencySubject{
			{workspace: "ws_1", tenant: "tenant-a", node: "n1", known: true, labels: map[string]string{"region": "us-west-2"}},
		})
		if len(findings) != 1 || findings[0].Check != "tenant.residency_violation" || findings[0].Severity != "error" {
			t.Fatalf("findings=%+v", findings)
		}
	})
	t.Run("verified good is silent", func(t *testing.T) {
		f := tenantControlFixture(t)
		createRetentionTenants(t, f, map[string]proto.TenantPolicy{
			"tenant-a": {Residency: proto.TenantResidency{AllowedRegions: []string{"us-east-1"}}},
		})
		findings := f.c.residencyFindings(ctx, []residencySubject{
			{workspace: "ws_1", tenant: "tenant-a", node: "n1", known: true, labels: map[string]string{"region": "us-east-1"}},
		})
		if len(findings) != 0 {
			t.Fatalf("findings=%+v", findings)
		}
	})
}

// A tenant whose event envelopes outlive its own policy because a longer-lived
// tenant holds the prefix is a real, reportable limit of the design, not
// something to leave silent.
func TestRetentionFindingsNameTheEnvelopeLimit(t *testing.T) {
	f := tenantControlFixture(t)
	createRetentionTenants(t, f, map[string]proto.TenantPolicy{
		"tenant-a": retentionPolicy(2*time.Hour, 0),
		"tenant-b": retentionPolicy(100*time.Hour, 0),
	})
	findings := f.c.retentionFindings(context.Background())
	var violation *proto.Finding
	for i := range findings {
		if findings[i].Check == "tenant.retention_violation" {
			violation = &findings[i]
		}
	}
	if violation == nil || violation.Subject != "tenant-a" {
		t.Fatalf("findings=%+v", findings)
	}
}

// Session-log retention is per record, and each record carries the tenant
// whose policy set its expiry. A pass that expired tenant A's records must
// leave tenant B's attachable even though both were swept together.
func TestSessionLogRetentionNeverExpiresAnotherTenant(t *testing.T) {
	f := tenantControlFixture(t)
	ctx := context.Background()
	createRetentionTenants(t, f, map[string]proto.TenantPolicy{
		"tenant-a": retentionPolicy(time.Hour, 0),
		"tenant-b": retentionPolicy(100*time.Hour, 0),
	})
	now := time.Now()
	records := []*proto.SessionLogRecord{
		{Session: "s_a", Workspace: "ws_a", Tenant: "tenant-a", Principal: "alice", Kind: proto.SessionExec,
			MaxChunk: 1024, Complete: true, ExpiresAt: now.Add(-time.Minute).UnixMilli()},
		{Session: "s_b", Workspace: "ws_b", Tenant: "tenant-b", Principal: "bob", Kind: proto.SessionExec,
			MaxChunk: 1024, Complete: true, ExpiresAt: now.Add(99 * time.Hour).UnixMilli()},
	}
	for _, record := range records {
		if _, err := f.c.db.ExecContext(ctx, `INSERT INTO session_logs(id, tenant, expires_at, data) VALUES(?,?,?,?)`,
			record.Session, record.Tenant, record.ExpiresAt, proto.MustMarshal(record)); err != nil {
			t.Fatal(err)
		}
		f.c.mu.Lock()
		f.c.sessionLogs[record.Session] = record
		f.c.mu.Unlock()
	}
	result, err := f.c.PruneRecords(ctx, now, 100)
	if err != nil || result.SessionLogs != 1 {
		t.Fatalf("session log prune=%+v err=%v", result, err)
	}
	f.c.mu.Lock()
	_, expired := f.c.sessionLogs["s_a"]
	survivor, kept := f.c.sessionLogs["s_b"]
	f.c.mu.Unlock()
	if expired {
		t.Fatal("tenant-a's expired session log survived its own retention")
	}
	if !kept || survivor.Tenant != "tenant-b" {
		t.Fatal("tenant-a's retention expired tenant-b's session log")
	}
	var remaining int
	if err := f.c.db.QueryRowContext(ctx, `SELECT count(*) FROM session_logs WHERE tenant='tenant-b'`).Scan(&remaining); err != nil {
		t.Fatal(err)
	}
	if remaining != 1 {
		t.Fatalf("tenant-b durable session log rows=%d", remaining)
	}
}
