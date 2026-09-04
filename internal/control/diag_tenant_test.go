package control

import (
	"context"
	"testing"
	"time"

	"remount.dev/remount/internal/metrics"
	"remount.dev/remount/internal/proto"
)

// A retention check that could not read a single tenant policy must not
// publish a machine-readable "nothing is unenforced".
//
// The finding already says the check did not run, but a gauge is what an
// operator alerts on, and resetting it to zero makes the alert clear at the
// exact moment the check broke. That is the unearned pass this package refuses
// everywhere else: verified good is healthy, and anything unread is
// unavailable, never healthy.
func TestRetentionGaugeIsNotClearedByACheckThatCouldNotRun(t *testing.T) {
	f := tenantControlFixture(t)
	createRetentionTenants(t, f, map[string]proto.TenantPolicy{
		"tenant-a": retentionPolicy(2*time.Hour, 0),
		"tenant-b": retentionPolicy(100*time.Hour, 0),
	})
	if findings := f.c.retentionFindings(context.Background()); len(findings) != 1 ||
		findings[0].Check != "tenant.retention_violation" {
		t.Fatalf("findings=%+v", findings)
	}
	if got := metrics.TenantRetentionUnenforced.Value(); got != 1 {
		t.Fatalf("unenforced gauge after a good read = %d, want 1", got)
	}

	dead, cancel := context.WithCancel(context.Background())
	cancel()
	failures := metrics.TenantRetentionFailed.Value()
	findings := f.c.retentionFindings(dead)
	if len(findings) != 1 || findings[0].Check != "tenant.retention_unavailable" || findings[0].Severity != "warn" {
		t.Fatalf("findings=%+v", findings)
	}
	if got := metrics.TenantRetentionUnenforced.Value(); got == 0 {
		t.Fatal("a check that could not read the tenant directory published zero unenforced retention policies")
	}
	if delta := metrics.TenantRetentionFailed.Value() - failures; delta != 1 {
		t.Fatalf("retention failure counter delta=%d, want 1: an unreadable directory must move a counter of its own", delta)
	}
}
