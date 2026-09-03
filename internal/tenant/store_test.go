package tenant

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"remount.dev/remount/internal/eventlog"
)

type testStore struct {
	store *Store
	log   *eventlog.Log
	path  string
	now   *time.Time
}

func newTestStore(t *testing.T, options Options) *testStore {
	t.Helper()
	path := filepath.Join(t.TempDir(), "control.db")
	now := time.Unix(1_800_000_000, 0).UTC()
	options.Now = func() time.Time { return now }
	sqlite, err := eventlog.OpenSQLite(path)
	if err != nil {
		t.Fatal(err)
	}
	log := eventlog.New(sqlite)
	store, err := NewStore(sqlite.DB(), log, options)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = log.Close() })
	return &testStore{store: store, log: log, path: path, now: &now}
}

func policyWithWorkspaceLimit(limit int64) Policy {
	return Policy{Quotas: Quotas{MaxWorkspaces: limit}, Retention: Retention{MeterEvents: 30 * 24 * time.Hour},
		Residency: Residency{AllowedRegions: []string{"us-west-2", "us-east-1", "us-west-2"}, RequiredLabels: map[string]string{"tier": "trusted"}},
		Billing:   Billing{StripeCustomerID: "cus_test"}}
}

func createTenant(t *testing.T, store *Store, id string, policy Policy) Tenant {
	t.Helper()
	tenant, err := store.Create(context.Background(), id, policy, Mutation{OperationID: "op_create_" + id, Actor: "operator"})
	if err != nil {
		t.Fatal(err)
	}
	return tenant
}

func TestLifecycleIdempotencyIsolationAndEvents(t *testing.T) {
	ts := newTestStore(t, Options{})
	ctx := context.Background()
	created := createTenant(t, ts.store, "tenant-a", policyWithWorkspaceLimit(2))
	if created.Revision != 1 || len(created.Policy.Residency.AllowedRegions) != 2 || created.Policy.Residency.AllowedRegions[0] != "us-east-1" {
		t.Fatalf("canonical tenant = %+v", created)
	}
	replayed, err := ts.store.Create(ctx, "tenant-a", policyWithWorkspaceLimit(2), Mutation{OperationID: "op_create_tenant-a", Actor: "operator"})
	if err != nil || replayed.Revision != created.Revision {
		t.Fatalf("create replay = %+v, %v", replayed, err)
	}
	_, err = ts.store.Create(ctx, "tenant-a", Policy{}, Mutation{OperationID: "op_create_tenant-a", Actor: "operator"})
	if !IsCode(err, CodeConflict) {
		t.Fatalf("changed replay error = %v", err)
	}
	_, err = ts.store.Create(ctx, "tenant-a", policyWithWorkspaceLimit(2), Mutation{OperationID: "op_create_tenant-a", Actor: "different-operator"})
	if !IsCode(err, CodeConflict) {
		t.Fatalf("cross-actor replay error = %v", err)
	}
	createTenant(t, ts.store, "tenant-b", Policy{})
	if _, err := ts.store.Get(ctx, "*"); !IsCode(err, CodeBadRequest) {
		t.Fatalf("wildcard tenant lookup = %v", err)
	}
	if _, err := ts.store.CurrentUsage(ctx, ""); !IsCode(err, CodeBadRequest) {
		t.Fatalf("empty tenant usage = %v", err)
	}
	updatedPolicy := policyWithWorkspaceLimit(3)
	updated, err := ts.store.UpdatePolicy(ctx, "tenant-a", updatedPolicy, Mutation{OperationID: "op_update", Actor: "operator", ExpectedRevision: 1})
	if err != nil || updated.Revision != 2 || updated.Policy.Quotas.MaxWorkspaces != 3 {
		t.Fatalf("update = %+v, %v", updated, err)
	}
	if _, err := ts.store.SetState(ctx, "tenant-a", StateSuspended, Mutation{OperationID: "op_suspend", Actor: "operator", ExpectedRevision: 1}); !IsCode(err, CodeConflict) {
		t.Fatalf("stale revision = %v", err)
	}
	events, err := ts.log.Read(ctx, 0, "tenant:tenant-a", 20)
	if err != nil {
		t.Fatal(err)
	}
	if len(events) != 2 || events[0].Type != "tenant.created" || events[1].Type != "tenant.updated" {
		t.Fatalf("events = %+v", events)
	}
	for _, event := range events {
		if event.Tenant != "tenant-a" || event.Actor != "operator" {
			t.Fatalf("unscoped event = %+v", event)
		}
	}
}

func TestConcurrentQuotaNoOvercommitAndDenialReplay(t *testing.T) {
	ts := newTestStore(t, Options{})
	createTenant(t, ts.store, "tenant-a", policyWithWorkspaceLimit(5))
	createTenant(t, ts.store, "tenant-b", policyWithWorkspaceLimit(1))
	ctx := context.Background()
	var admitted atomic.Int64
	var exhausted atomic.Int64
	deniedIDs := make(chan int, 1)
	var wg sync.WaitGroup
	for i := 0; i < 32; i++ {
		wg.Add(1)
		go func(index int) {
			defer wg.Done()
			fresh, err := ts.store.Reserve(ctx, Admission{Tenant: "tenant-a", OperationID: fmt.Sprintf("op_reserve_%d", index), Actor: "scheduler",
				Resource: ResourceWorkspace, ResourceID: fmt.Sprintf("ws_%d", index), Amount: 1})
			if err == nil && fresh {
				admitted.Add(1)
			} else if IsCode(err, CodeResourceExhausted) {
				exhausted.Add(1)
				select {
				case deniedIDs <- index:
				default:
				}
			} else {
				t.Errorf("reserve %d = %v, fresh=%v", index, err, fresh)
			}
		}(i)
	}
	wg.Wait()
	if admitted.Load() != 5 || exhausted.Load() != 27 {
		t.Fatalf("admitted=%d exhausted=%d", admitted.Load(), exhausted.Load())
	}
	usage, err := ts.store.CurrentUsage(ctx, "tenant-a")
	if err != nil || usage.Workspaces != 5 {
		t.Fatalf("usage = %+v, %v", usage, err)
	}
	// Exact replay is checked before quota and emits no second denial.
	denied := <-deniedIDs
	_, err = ts.store.Reserve(ctx, Admission{Tenant: "tenant-a", OperationID: fmt.Sprintf("op_reserve_%d", denied), Actor: "scheduler", Resource: ResourceWorkspace, ResourceID: fmt.Sprintf("ws_%d", denied), Amount: 1})
	if !IsCode(err, CodeResourceExhausted) {
		t.Fatalf("denial replay = %v", err)
	}
	var quotaErr *Error
	if !errors.As(err, &quotaErr) || quotaErr.Resource != ResourceWorkspace || quotaErr.Limit != 5 || quotaErr.Used != 5 {
		t.Fatalf("denial replay lost details = %#v", err)
	}
	events, _ := ts.log.Read(ctx, 0, "tenant:tenant-a", 100)
	denials := 0
	for _, event := range events {
		if event.Type == "quota.exceeded" {
			denials++
		}
	}
	if denials != 27 {
		t.Fatalf("quota denial events = %d", denials)
	}
	if fresh, err := ts.store.Reserve(ctx, Admission{Tenant: "tenant-b", OperationID: "op_reserve_0", Actor: "scheduler", Resource: ResourceWorkspace, ResourceID: "ws_0", Amount: 1}); err != nil || !fresh {
		t.Fatalf("tenant-scoped IDs collided = %v, %v", fresh, err)
	}
	if _, err := ts.store.Reserve(ctx, Admission{Tenant: "tenant-c", OperationID: "op_cross", Actor: "scheduler", Resource: ResourceWorkspace, ResourceID: "ws_0", Amount: 1}); !IsCode(err, CodeNotFound) {
		t.Fatalf("negative tenant admission = %v", err)
	}
}

func TestSuspensionAndOperationRetention(t *testing.T) {
	ts := newTestStore(t, Options{MaxOperations: 2, OperationRetention: time.Hour})
	created := createTenant(t, ts.store, "tenant-a", policyWithWorkspaceLimit(10))
	suspended, err := ts.store.SetState(context.Background(), "tenant-a", StateSuspended,
		Mutation{OperationID: "op_suspend", Actor: "operator", ExpectedRevision: created.Revision})
	if err != nil || suspended.State != StateSuspended {
		t.Fatalf("suspend = %+v, %v", suspended, err)
	}
	admission := Admission{Tenant: "tenant-a", OperationID: "op_while_suspended", Actor: "scheduler", Resource: ResourceWorkspace, ResourceID: "ws_blocked", Amount: 1}
	if _, err := ts.store.Reserve(context.Background(), admission); !IsCode(err, CodeResourceExhausted) {
		// The per-tenant operation journal is full before it can record another
		// result. This is deliberate visible backpressure, not silent eviction.
		t.Fatalf("bounded journal = %v", err)
	}
	*ts.now = ts.now.Add(2 * time.Hour)
	if _, err := ts.store.Reserve(context.Background(), admission); !IsCode(err, CodeConflict) {
		t.Fatalf("suspended admission after retention = %v", err)
	}
	events, _ := ts.log.Read(context.Background(), 0, "", 20)
	foundPrune := false
	for _, event := range events {
		foundPrune = foundPrune || event.Type == "tenant.operations_pruned"
	}
	if !foundPrune {
		t.Fatal("operation retention was not observable")
	}
}

func TestSystemSafetyBoundsUnlimitedPolicy(t *testing.T) {
	ts := newTestStore(t, Options{MaxReservationsPerTenant: 1, MaxExportersPerTenant: 1})
	createTenant(t, ts.store, "tenant-a", Policy{})
	first := Admission{Tenant: "tenant-a", OperationID: "op_first", Actor: "scheduler", Resource: ResourceWorkspace, ResourceID: "ws_1", Amount: 1}
	if fresh, err := ts.store.Reserve(context.Background(), first); err != nil || !fresh {
		t.Fatalf("first unlimited reservation = %v, %v", fresh, err)
	}
	second := first
	second.OperationID, second.ResourceID = "op_second", "ws_2"
	if _, err := ts.store.Reserve(context.Background(), second); !IsCode(err, CodeResourceExhausted) {
		t.Fatalf("reservation safety bound = %v", err)
	}
	if _, err := ts.store.EnsureExporter(context.Background(), "tenant-a", "stripe", "billing"); err != nil {
		t.Fatal(err)
	}
	if _, err := ts.store.EnsureExporter(context.Background(), "tenant-a", "backup", "billing"); !IsCode(err, CodeResourceExhausted) {
		t.Fatalf("exporter safety bound = %v", err)
	}
}

func TestTenantCapacityPreservesConflictSemantics(t *testing.T) {
	ts := newTestStore(t, Options{MaxTenants: 1})
	createTenant(t, ts.store, "tenant-a", Policy{})
	if _, err := ts.store.Create(context.Background(), "tenant-a", Policy{}, Mutation{OperationID: "op_duplicate", Actor: "operator"}); !IsCode(err, CodeConflict) {
		t.Fatalf("duplicate at capacity = %v", err)
	}
	if _, err := ts.store.Create(context.Background(), "tenant-b", Policy{}, Mutation{OperationID: "op_create_b", Actor: "operator"}); !IsCode(err, CodeResourceExhausted) {
		t.Fatalf("tenant capacity = %v", err)
	}
}

func TestReleaseDeleteAndRestartDurability(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "control.db")
	now := time.Unix(1_800_000_000, 0).UTC()
	open := func() (*Store, *eventlog.Log) {
		sqlite, err := eventlog.OpenSQLite(path)
		if err != nil {
			t.Fatal(err)
		}
		log := eventlog.New(sqlite)
		store, err := NewStore(sqlite.DB(), log, Options{Now: func() time.Time { return now }})
		if err != nil {
			t.Fatal(err)
		}
		return store, log
	}
	store, log := open()
	tenant := createTenant(t, store, "tenant-a", policyWithWorkspaceLimit(1))
	admission := Admission{Tenant: "tenant-a", OperationID: "op_reserve", Actor: "scheduler", Resource: ResourceWorkspace, ResourceID: "ws_1", Amount: 1}
	if fresh, err := store.Reserve(context.Background(), admission); err != nil || !fresh {
		t.Fatalf("reserve = %v, %v", fresh, err)
	}
	if _, err := store.SetState(context.Background(), "tenant-a", StateDeleted, Mutation{OperationID: "op_delete_busy", Actor: "operator", ExpectedRevision: tenant.Revision}); !IsCode(err, CodeConflict) {
		t.Fatalf("delete with live resource = %v", err)
	}
	_ = log.Close()
	store, log = open()
	defer log.Close()
	restartedTenant, err := store.Get(context.Background(), "tenant-a")
	if err != nil || restartedTenant.Policy.Billing.StripeCustomerID != "cus_test" ||
		len(restartedTenant.Policy.Residency.AllowedRegions) != 2 || restartedTenant.Policy.Retention.MeterEvents != 30*24*time.Hour {
		t.Fatalf("restart policy = %+v, %v", restartedTenant.Policy, err)
	}
	usage, err := store.CurrentUsage(context.Background(), "tenant-a")
	if err != nil || usage.Workspaces != 1 {
		t.Fatalf("restart usage = %+v, %v", usage, err)
	}
	admission.OperationID = "op_release"
	if fresh, err := store.Release(context.Background(), admission); err != nil || !fresh {
		t.Fatalf("release = %v, %v", fresh, err)
	}
	deleted, err := store.SetState(context.Background(), "tenant-a", StateDeleted, Mutation{OperationID: "op_delete", Actor: "operator", ExpectedRevision: tenant.Revision})
	if err != nil || deleted.State != StateDeleted || deleted.Revision != 2 {
		t.Fatalf("delete = %+v, %v", deleted, err)
	}
	if _, err := store.RecordMeter(context.Background(), MeterEvent{ID: "meter_after_delete", Tenant: "tenant-a", Kind: MeterBytesIn, Value: 1, At: now}); !IsCode(err, CodeConflict) {
		t.Fatalf("meter after delete = %v", err)
	}
}

func TestAdmissionCallbackSharesCommitAndRollback(t *testing.T) {
	ts := newTestStore(t, Options{})
	createTenant(t, ts.store, "tenant-a", policyWithWorkspaceLimit(1))
	admission := Admission{Tenant: "tenant-a", OperationID: "op_admit", Actor: "scheduler", Resource: ResourceWorkspace, ResourceID: "ws_atomic", Amount: 1}
	if _, err := ts.store.Admit(context.Background(), admission, func(*eventlog.Tx) error { return fmt.Errorf("resource write failed") }); err == nil {
		t.Fatal("failed resource callback committed quota")
	}
	usage, err := ts.store.CurrentUsage(context.Background(), "tenant-a")
	if err != nil || usage.Workspaces != 0 {
		t.Fatalf("rolled-back admission usage = %+v, %v", usage, err)
	}
	fresh, err := ts.store.Admit(context.Background(), admission, func(tx *eventlog.Tx) error {
		return tx.Emit(ts.store.event(*ts.now, "workspace.created", "tenant-a", "scheduler", "op_admit", map[string]any{"workspace": "ws_atomic"}))
	})
	if err != nil || !fresh {
		t.Fatalf("atomic admission retry = %v, %v", fresh, err)
	}
	release := admission
	release.OperationID = "op_retire"
	if _, err := ts.store.Retire(context.Background(), release, func(*eventlog.Tx) error { return fmt.Errorf("resource delete failed") }); err == nil {
		t.Fatal("failed resource delete released quota")
	}
	usage, _ = ts.store.CurrentUsage(context.Background(), "tenant-a")
	if usage.Workspaces != 1 {
		t.Fatalf("failed retire changed usage = %+v", usage)
	}
	if fresh, err := ts.store.Retire(context.Background(), release, nil); err != nil || !fresh {
		t.Fatalf("retire retry = %v, %v", fresh, err)
	}
}

func TestMeterIdempotencyRetentionCursorAndSummary(t *testing.T) {
	ts := newTestStore(t, Options{MaxMeterEvents: 3})
	createTenant(t, ts.store, "tenant-a", Policy{})
	createTenant(t, ts.store, "tenant-b", Policy{})
	ctx := context.Background()
	for i := 1; i <= 3; i++ {
		_, err := ts.store.RecordMeter(ctx, MeterEvent{ID: fmt.Sprintf("meter_%d", i), Tenant: "tenant-a", Kind: MeterBytesOut, Value: int64(i), At: ts.now.Add(-time.Duration(10-i) * time.Hour), Workspace: "ws_meter"})
		if err != nil {
			t.Fatal(err)
		}
	}
	replayed, err := ts.store.RecordMeter(ctx, MeterEvent{ID: "meter_1", Tenant: "tenant-a", Kind: MeterBytesOut, Value: 1, At: ts.now.Add(-9 * time.Hour), Workspace: "ws_meter"})
	if err != nil || replayed.Seq == 0 {
		t.Fatalf("meter replay = %+v, %v", replayed, err)
	}
	if _, err := ts.store.RecordMeter(ctx, MeterEvent{ID: "meter_1", Tenant: "tenant-a", Kind: MeterBytesOut, Value: 2, At: ts.now.Add(-9 * time.Hour), Workspace: "ws_meter"}); !IsCode(err, CodeConflict) {
		t.Fatalf("changed meter replay = %v", err)
	}
	if _, err := ts.store.RecordMeter(ctx, MeterEvent{ID: "meter_4", Tenant: "tenant-a", Kind: MeterBytesOut, Value: 1, At: *ts.now}); !IsCode(err, CodeResourceExhausted) {
		t.Fatalf("meter capacity = %v", err)
	}
	// Creating a cursor fences unexported usage from retention.
	if _, err := ts.store.ExportOnce(ctx, &captureExporter{}, ExportOptions{Tenant: "tenant-a", Name: "billing", PageSize: 1, Actor: "billing"}); err != nil {
		t.Fatal(err)
	}
	removed, err := ts.store.PruneMeter(ctx, "tenant-a", ts.now.Add(time.Hour), 10, "gc")
	if err != nil || removed != 1 {
		t.Fatalf("prune exported prefix = %d, %v", removed, err)
	}
	summary, err := ts.store.UsageSummary(ctx, "tenant-a", time.Time{}, time.Time{})
	if err != nil || summary[MeterBytesOut] != 5 {
		t.Fatalf("summary = %+v, %v", summary, err)
	}
	other, err := ts.store.UsageSummary(ctx, "tenant-b", time.Time{}, time.Time{})
	if err != nil || len(other) != 0 {
		t.Fatalf("cross-tenant summary = %+v, %v", other, err)
	}
	events, _ := ts.log.Read(ctx, 0, "tenant:tenant-a", 20)
	foundWorkspace := false
	for _, event := range events {
		foundWorkspace = foundWorkspace || event.Type == string(MeterBytesOut) && event.Workspace == "ws_meter"
	}
	if !foundWorkspace {
		t.Fatal("meter event lost workspace attribution")
	}
}

type captureExporter struct {
	mu     sync.Mutex
	events []MeterEvent
	err    error
}

func (e *captureExporter) Export(_ context.Context, events []MeterEvent) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.events = append(e.events, events...)
	return e.err
}

func TestExportCursorAtLeastOnceAndFailure(t *testing.T) {
	ts := newTestStore(t, Options{})
	createTenant(t, ts.store, "tenant-a", Policy{})
	for i := 0; i < 3; i++ {
		_, err := ts.store.RecordMeter(context.Background(), MeterEvent{ID: fmt.Sprintf("m_%d", i), Tenant: "tenant-a", Kind: MeterBrokeredRequest, Value: 1, At: *ts.now})
		if err != nil {
			t.Fatal(err)
		}
	}
	failing := &captureExporter{err: fmt.Errorf("sink down")}
	if n, err := ts.store.ExportOnce(context.Background(), failing, ExportOptions{Tenant: "tenant-a", Name: "sink", PageSize: 2, Actor: "billing"}); err == nil || n != 0 {
		t.Fatalf("failed export = %d, %v", n, err)
	}
	if next, err := ts.store.Cursor(context.Background(), "tenant-a", "sink"); err != nil || next != 1 {
		t.Fatalf("cursor advanced on failure = %d, %v", next, err)
	}
	good := &captureExporter{}
	if n, err := ts.store.ExportOnce(context.Background(), good, ExportOptions{Tenant: "tenant-a", Name: "sink", PageSize: 2, Actor: "billing"}); err != nil || n != 2 {
		t.Fatalf("export = %d, %v", n, err)
	}
	if n, err := ts.store.ExportOnce(context.Background(), good, ExportOptions{Tenant: "tenant-a", Name: "sink", PageSize: 2, Actor: "billing"}); err != nil || n != 1 {
		t.Fatalf("export tail = %d, %v", n, err)
	}
	if len(good.events) != 3 || good.events[0].ID != "m_0" || good.events[2].ID != "m_2" {
		t.Fatalf("ordered export = %+v", good.events)
	}
}

func TestMeterAndCursorSurviveRestart(t *testing.T) {
	path := filepath.Join(t.TempDir(), "control.db")
	now := time.Unix(1_800_000_000, 0).UTC()
	open := func() (*Store, *eventlog.Log) {
		sqlite, err := eventlog.OpenSQLite(path)
		if err != nil {
			t.Fatal(err)
		}
		log := eventlog.New(sqlite)
		store, err := NewStore(sqlite.DB(), log, Options{Now: func() time.Time { return now }})
		if err != nil {
			t.Fatal(err)
		}
		return store, log
	}
	store, log := open()
	createTenant(t, store, "tenant-a", Policy{})
	for _, id := range []string{"meter_a", "meter_b"} {
		if _, err := store.RecordMeter(context.Background(), MeterEvent{ID: id, Tenant: "tenant-a", Kind: MeterBytesIn, Value: 3, At: now}); err != nil {
			t.Fatal(err)
		}
	}
	first := &captureExporter{}
	if n, err := store.ExportOnce(context.Background(), first, ExportOptions{Tenant: "tenant-a", Name: "stripe", PageSize: 1, Actor: "billing"}); err != nil || n != 1 {
		t.Fatalf("first export = %d, %v", n, err)
	}
	if err := log.Close(); err != nil {
		t.Fatal(err)
	}
	store, log = open()
	defer log.Close()
	next, err := store.Cursor(context.Background(), "tenant-a", "stripe")
	if err != nil || next != 2 {
		t.Fatalf("restart cursor = %d, %v", next, err)
	}
	remaining := &captureExporter{}
	if n, err := store.ExportOnce(context.Background(), remaining, ExportOptions{Tenant: "tenant-a", Name: "stripe", PageSize: 10, Actor: "billing"}); err != nil || n != 1 || remaining.events[0].ID != "meter_b" {
		t.Fatalf("restart export = %d, %+v, %v", n, remaining.events, err)
	}
	summary, err := store.UsageSummary(context.Background(), "tenant-a", time.Time{}, time.Time{})
	if err != nil || summary[MeterBytesIn] != 6 {
		t.Fatalf("restart summary = %+v, %v", summary, err)
	}
}

func TestResidency(t *testing.T) {
	policy := Residency{AllowedRegions: []string{"us-east-1"}, RequiredLabels: map[string]string{"gpu": "a100"}}
	if !MatchResidency(policy, map[string]string{"region": "us-east-1", "gpu": "a100"}) ||
		MatchResidency(policy, map[string]string{"region": "us-west-2", "gpu": "a100"}) ||
		MatchResidency(policy, map[string]string{"region": "us-east-1"}) {
		t.Fatal("residency predicate mismatch")
	}
}
