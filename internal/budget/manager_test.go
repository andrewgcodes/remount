package budget

import (
	"context"
	"errors"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

var testSubject = Subject{Tenant: "tenant-a", Workspace: "ws-a", Principal: "agent-a", Binding: "binding-a"}

func TestReserveSettleAcrossAllAttachments(t *testing.T) {
	now := time.Date(2026, 9, 3, 12, 0, 0, 0, time.UTC)
	manager := newTestManager(t, Config{
		Now: func() time.Time { return now },
		Budgets: []Budget{
			{ID: "b-tenant", Tenant: "tenant-a", AttachTo: AttachTenant, AttachID: "tenant-a", Window: WindowHour, MaxRequests: 2},
			{ID: "b-workspace", Tenant: "tenant-a", AttachTo: AttachWorkspace, AttachID: "ws-a", Window: WindowDay, MaxTokens: 10},
			{ID: "b-principal", Tenant: "tenant-a", AttachTo: AttachPrincipal, AttachID: "agent-a", Window: Window30Days, MaxEstimatedCostMicros: 10},
			{ID: "b-other", Tenant: "tenant-a", AttachTo: AttachBinding, AttachID: "other", Window: WindowHour, MaxRequests: 1},
		},
	})
	request := ReserveRequest{
		Key: "request-1", Node: "node-a", Subject: testSubject,
		Provider: "openai", Model: "gpt-4o-mini", InputTokens: 2, MaxOutputTokens: 4, Metered: true,
	}
	reservation, err := manager.Reserve(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	if !reservation.Tracked || reservation.State != StateReserved || reservation.ReservedTokens != 6 || reservation.ReservedEstimatedCostMicros != 4 || len(reservation.BudgetIDs) != 3 {
		t.Fatalf("reservation = %+v", reservation)
	}
	usage, err := manager.Usage(context.Background(), UsageQuery{Tenant: "tenant-a"})
	if err != nil || len(usage) != 4 {
		t.Fatalf("Usage = %+v, %v", usage, err)
	}
	workspace := usageByID(usage, "b-workspace")
	if workspace.Requests != 1 || workspace.Tokens != 6 || workspace.ActiveReservations != 1 {
		t.Fatalf("reserved usage = %+v", workspace)
	}

	settled, err := manager.Settle(context.Background(), SettleRequest{ReservationID: reservation.ID, Mode: SettlementMetered, InputTokens: 2, OutputTokens: 2})
	if err != nil {
		t.Fatal(err)
	}
	if settled.State != StateSettled || settled.Tokens != 4 || settled.EstimatedCostMicros != 3 || settled.UsageExceededReservation {
		t.Fatalf("settlement = %+v", settled)
	}
	usage, _ = manager.Usage(context.Background(), UsageQuery{Workspace: "ws-a"})
	workspace = usageByID(usage, "b-workspace")
	if workspace.Tokens != 4 || workspace.ActiveReservations != 0 {
		t.Fatalf("settled usage = %+v", workspace)
	}

	second := request
	second.Key = "request-2"
	second.InputTokens = 2
	second.MaxOutputTokens = 3
	if _, err := manager.Reserve(context.Background(), second); err != nil {
		t.Fatal(err)
	}
	third := request
	third.Key = "request-3"
	if _, err := manager.Reserve(context.Background(), third); !errors.Is(err, ErrBudgetExceeded) {
		t.Fatalf("third Reserve error = %v", err)
	} else {
		var denied *DeniedError
		if !errors.As(err, &denied) || denied.Limit != LimitRequests || len(denied.BudgetIDs) != 1 || denied.BudgetIDs[0] != "b-tenant" {
			t.Fatalf("DeniedError = %#v", denied)
		}
	}
}

func TestConcurrentOvercommitAndIdempotencyAtCapacity(t *testing.T) {
	manager := newTestManager(t, Config{
		MaxReservations: 1,
		Budgets:         []Budget{{ID: "only", Tenant: "tenant-a", AttachTo: AttachTenant, AttachID: "tenant-a", Window: WindowHour, MaxRequests: 1}},
	})
	const workers = 64
	start := make(chan struct{})
	var admitted atomic.Int64
	var denied atomic.Int64
	var unexpected atomic.Int64
	var winnerMu sync.Mutex
	var winner ReserveRequest
	var winnerReservation Reservation
	var wait sync.WaitGroup
	for i := range workers {
		wait.Add(1)
		go func() {
			defer wait.Done()
			request := ReserveRequest{Key: "request-" + strconv.Itoa(i), Node: "node-a", Subject: testSubject, Provider: "openai", Model: "gpt-4o-mini", MaxOutputTokens: 1, Metered: true}
			<-start
			reservation, err := manager.Reserve(context.Background(), request)
			switch {
			case err == nil:
				admitted.Add(1)
				winnerMu.Lock()
				winner, winnerReservation = request, reservation
				winnerMu.Unlock()
			case errors.Is(err, ErrBudgetExceeded), errors.Is(err, ErrCapacity):
				denied.Add(1)
			default:
				unexpected.Add(1)
			}
		}()
	}
	close(start)
	wait.Wait()
	if admitted.Load() != 1 || denied.Load() != workers-1 || unexpected.Load() != 0 {
		t.Fatalf("admitted=%d denied=%d unexpected=%d", admitted.Load(), denied.Load(), unexpected.Load())
	}
	replayed, err := manager.Reserve(context.Background(), winner)
	if err != nil || replayed.ID != winnerReservation.ID {
		t.Fatalf("idempotent replay = %+v, %v", replayed, err)
	}
	changed := winner
	changed.MaxOutputTokens++
	if _, err := manager.Reserve(context.Background(), changed); !errors.Is(err, ErrConflict) {
		t.Fatalf("changed replay error = %v", err)
	}
}

func TestUnknownIncompleteExpiryAndNodeDeath(t *testing.T) {
	now := time.Date(2026, 9, 3, 12, 0, 0, 0, time.UTC)
	manager := newTestManager(t, Config{
		Now: func() time.Time { return now }, ReservationTTL: time.Minute,
		Budgets: []Budget{{ID: "tokens", Tenant: "tenant-a", AttachTo: AttachTenant, AttachID: "tenant-a", Window: WindowHour, MaxRequests: 10, MaxTokens: 100}},
	})
	unknown, err := manager.Reserve(context.Background(), ReserveRequest{Key: "unknown", Node: "node-a", Subject: testSubject, Provider: "other", Model: "model", MaxOutputTokens: 99, Metered: false})
	if err != nil || unknown.ReservedTokens != 0 || unknown.UnmeteredReason != "provider_unknown" {
		t.Fatalf("unknown reservation = %+v, %v", unknown, err)
	}
	unknownSettlement, err := manager.Settle(context.Background(), SettleRequest{ReservationID: unknown.ID, Mode: SettlementRequestOnly})
	if err != nil || !unknownSettlement.EstimatedCostUnavailable {
		t.Fatalf("unknown settlement = %+v, %v", unknownSettlement, err)
	}
	unknownWithNoModel, err := manager.Reserve(context.Background(), ReserveRequest{Key: "unknown-no-model", Node: "node-a", Subject: testSubject, Provider: "other", Metered: false})
	if err != nil {
		t.Fatalf("request-only provider without model: %v", err)
	}
	if _, err := manager.Settle(context.Background(), SettleRequest{ReservationID: unknownWithNoModel.ID, Mode: SettlementMetered, OutputTokens: 1}); err == nil {
		t.Fatal("request-only provider accepted a metered settlement")
	}
	if _, err := manager.Settle(context.Background(), SettleRequest{ReservationID: unknownWithNoModel.ID, Mode: SettlementRequestOnly}); err != nil {
		t.Fatalf("request-only provider recovery settlement: %v", err)
	}

	incomplete, err := manager.Reserve(context.Background(), ReserveRequest{Key: "incomplete", Node: "node-b", Subject: testSubject, Provider: "openai", Model: "gpt-4o-mini", InputTokens: 10, MaxOutputTokens: 20, Metered: true})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := manager.Settle(context.Background(), SettleRequest{ReservationID: incomplete.ID, Mode: SettlementRequestOnly}); err == nil {
		t.Fatal("supported provider released its token reservation as request-only")
	}
	settled, err := manager.Settle(context.Background(), SettleRequest{ReservationID: incomplete.ID, Mode: SettlementIncomplete})
	if err != nil || settled.Tokens != 30 || settled.Mode != SettlementIncomplete {
		t.Fatalf("incomplete settlement = %+v, %v", settled, err)
	}

	expiring, err := manager.Reserve(context.Background(), ReserveRequest{Key: "expiring", Node: "node-c", Subject: testSubject, Provider: "openai", Model: "gpt-4o-mini", MaxOutputTokens: 12, Metered: true})
	if err != nil {
		t.Fatal(err)
	}
	now = now.Add(2 * time.Minute)
	result, err := manager.Sweep(context.Background(), now)
	if err != nil || len(result.ReservationIDs) != 1 || result.ReservationIDs[0] != expiring.ID {
		t.Fatalf("Sweep = %+v, %v", result, err)
	}
	if _, err := manager.Settle(context.Background(), SettleRequest{ReservationID: expiring.ID, Mode: SettlementMetered, OutputTokens: 1}); !errors.Is(err, ErrExpired) {
		t.Fatalf("late settlement error = %v", err)
	}

	nodeOwned, err := manager.Reserve(context.Background(), ReserveRequest{Key: "node-death", Node: "node-d", Subject: testSubject, Provider: "openai", Model: "gpt-4o-mini", MaxOutputTokens: 8, Metered: true})
	if err != nil {
		t.Fatal(err)
	}
	result, err = manager.ExpireNode(context.Background(), "node-d", now)
	if err != nil || len(result.ReservationIDs) != 1 || result.ReservationIDs[0] != nodeOwned.ID {
		t.Fatalf("ExpireNode = %+v, %v", result, err)
	}
	result, err = manager.ExpireNode(context.Background(), "node-d", now)
	if err != nil || len(result.ReservationIDs) != 0 {
		t.Fatalf("idempotent ExpireNode = %+v, %v", result, err)
	}
	futureOwned, err := manager.Reserve(context.Background(), ReserveRequest{Key: "future-node-request", Node: "node-e", Subject: testSubject, Provider: "openai", Model: "gpt-4o-mini", MaxOutputTokens: 1, Metered: true})
	if err != nil {
		t.Fatal(err)
	}
	result, err = manager.ExpireNode(context.Background(), "node-e", now.Add(-time.Second))
	if err != nil || len(result.ReservationIDs) != 0 {
		t.Fatalf("stale node-death fence expired a later reservation: %+v, %v", result, err)
	}
	if replay, err := manager.Reserve(context.Background(), ReserveRequest{Key: "future-node-request", Node: "node-e", Subject: testSubject, Provider: "openai", Model: "gpt-4o-mini", MaxOutputTokens: 1, Metered: true}); err != nil || replay.State != StateReserved || replay.ID != futureOwned.ID {
		t.Fatalf("future reservation after stale death = %+v, %v", replay, err)
	}
	stats := manager.Stats()
	if stats.Unmetered != 2 || stats.Incomplete != 3 || stats.Expired != 2 {
		t.Fatalf("Stats = %+v", stats)
	}
}

func TestSettlementIdempotencyOverrunAndRollingWindow(t *testing.T) {
	now := time.Date(2026, 9, 3, 12, 0, 0, 0, time.UTC)
	manager := newTestManager(t, Config{
		Now:     func() time.Time { return now },
		Budgets: []Budget{{ID: "hour", Tenant: "tenant-a", AttachTo: AttachWorkspace, AttachID: "ws-a", Window: WindowHour, MaxRequests: 1, MaxTokens: 100}},
	})
	reservation, err := manager.Reserve(context.Background(), ReserveRequest{Key: "overrun", Node: "node-a", Subject: testSubject, Provider: "openai", Model: "gpt-4o-mini", MaxOutputTokens: 2, Metered: true})
	if err != nil {
		t.Fatal(err)
	}
	request := SettleRequest{ReservationID: reservation.ID, Mode: SettlementMetered, InputTokens: 2, OutputTokens: 3}
	settled, err := manager.Settle(context.Background(), request)
	if err != nil || !settled.UsageExceededReservation || settled.Tokens != 5 {
		t.Fatalf("overrun settlement = %+v, %v", settled, err)
	}
	replay, err := manager.Settle(context.Background(), request)
	if err != nil || replay.Tokens != settled.Tokens {
		t.Fatalf("settlement replay = %+v, %v", replay, err)
	}
	request.OutputTokens++
	if _, err := manager.Settle(context.Background(), request); !errors.Is(err, ErrConflict) {
		t.Fatalf("changed settlement error = %v", err)
	}
	now = now.Add(time.Hour + time.Nanosecond)
	if _, err := manager.Reserve(context.Background(), ReserveRequest{Key: "next-window", Node: "node-a", Subject: testSubject, Provider: "openai", Model: "gpt-4o-mini", MaxOutputTokens: 1, Metered: true}); err != nil {
		t.Fatalf("rolling window did not release request: %v", err)
	}
}

func TestPricingDefaultsAndTenantOverride(t *testing.T) {
	catalog, err := DefaultCatalog()
	if err != nil {
		t.Fatal(err)
	}
	if cost, ok := catalog.Estimate("tenant-a", "openai", "gpt-4o-mini", 1_000_000, 1_000_000); !ok || cost != 750_000 {
		t.Fatalf("default Estimate = %d, %v", cost, ok)
	}
	override := Price{Provider: "openai", Model: "gpt-4o-mini", InputMicrosPerMillionTokens: 1, OutputMicrosPerMillionTokens: 2, Approximate: true}
	if err := catalog.ReplaceTenant("tenant-a", []Price{override}); err != nil {
		t.Fatal(err)
	}
	if cost, ok := catalog.Estimate("tenant-a", "OPENAI", "GPT-4O-MINI", 1_000_000, 1_000_000); !ok || cost != 3 {
		t.Fatalf("override Estimate = %d, %v", cost, ok)
	}
	if cost, _ := catalog.Estimate("tenant-b", "openai", "gpt-4o-mini", 1_000_000, 1_000_000); cost != 750_000 {
		t.Fatalf("override escaped tenant: %d", cost)
	}
	if _, err := ParseCatalog([]byte(`{"version":1,"currency":"USD","prices":[]} trailing`)); err == nil {
		t.Fatal("invalid catalogue was accepted")
	}
}

func TestEstimatedCostDenialAndPriceUnavailable(t *testing.T) {
	manager := newTestManager(t, Config{Budgets: []Budget{{
		ID: "cost", Tenant: "tenant-a", AttachTo: AttachTenant, AttachID: "tenant-a",
		Window: WindowHour, MaxEstimatedCostMicros: 1,
	}}})
	priced := ReserveRequest{Key: "priced", Node: "node-a", Subject: testSubject, Provider: "openai", Model: "gpt-4o-mini", MaxOutputTokens: 2, Metered: true}
	if _, err := manager.Reserve(context.Background(), priced); !errors.Is(err, ErrBudgetExceeded) {
		t.Fatalf("priced Reserve error = %v", err)
	}
	zeroBound := priced
	zeroBound.Key = "unbounded"
	zeroBound.MaxOutputTokens = 0
	if _, err := manager.Reserve(context.Background(), zeroBound); !errors.Is(err, ErrBudgetExceeded) {
		t.Fatalf("zero-bound governed Reserve error = %v", err)
	}
	unpriced := priced
	unpriced.Key = "unpriced"
	unpriced.Model = "new-model-without-price"
	if _, err := manager.Reserve(context.Background(), unpriced); !errors.Is(err, ErrBudgetExceeded) {
		t.Fatalf("unpriced cost-governed Reserve error = %v", err)
	} else {
		var denied *DeniedError
		if !errors.As(err, &denied) || denied.UnmeteredReason != "price_unavailable" || denied.Limit != LimitCost {
			t.Fatalf("unpriced DeniedError = %#v", denied)
		}
	}
	manager = newTestManager(t, Config{Budgets: []Budget{{
		ID: "tokens", Tenant: "tenant-a", AttachTo: AttachTenant, AttachID: "tenant-a",
		Window: WindowHour, MaxTokens: 10,
	}}})
	reservation, err := manager.Reserve(context.Background(), unpriced)
	if err != nil || !reservation.Tracked || reservation.CostKnown || reservation.UnmeteredReason != "price_unavailable" {
		t.Fatalf("unpriced token-only reservation = %+v, %v", reservation, err)
	}
	settled, err := manager.Settle(context.Background(), SettleRequest{ReservationID: reservation.ID, Mode: SettlementMetered, OutputTokens: 1})
	if err != nil || !settled.EstimatedCostUnavailable || settled.Tokens != 1 {
		t.Fatalf("unpriced settlement = %+v, %v", settled, err)
	}
}

func TestRetentionCollectionIsExplicitAndBounded(t *testing.T) {
	now := time.Date(2026, 9, 3, 12, 0, 0, 0, time.UTC)
	manager := newTestManager(t, Config{
		Now: func() time.Time { return now }, MaxReservations: 1, Retention: 30 * 24 * time.Hour,
		Budgets: []Budget{{ID: "requests", Tenant: "tenant-a", AttachTo: AttachTenant, AttachID: "tenant-a", Window: WindowHour, MaxRequests: 2}},
	})
	request := ReserveRequest{Key: "old", Node: "node-a", Subject: testSubject, Provider: "other", Model: "model", Metered: false}
	reservation, err := manager.Reserve(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := manager.Settle(context.Background(), SettleRequest{ReservationID: reservation.ID, Mode: SettlementRequestOnly}); err != nil {
		t.Fatal(err)
	}
	now = now.Add(30*24*time.Hour + time.Nanosecond)
	request.Key = "new"
	if _, err := manager.Reserve(context.Background(), request); !errors.Is(err, ErrCapacity) {
		t.Fatalf("Reserve before explicit collection = %v", err)
	}
	if _, err := manager.Sweep(context.Background(), now); err != nil {
		t.Fatal(err)
	}
	if _, err := manager.Reserve(context.Background(), request); err != nil {
		t.Fatalf("Reserve after collection = %v", err)
	}
}

func TestNoMatchingBudgetIsExplicitlyUntracked(t *testing.T) {
	manager := newTestManager(t, Config{Budgets: []Budget{{ID: "other", Tenant: "tenant-b", AttachTo: AttachTenant, AttachID: "tenant-b", Window: WindowHour, MaxRequests: 1}}})
	reservation, err := manager.Reserve(context.Background(), ReserveRequest{Key: "untracked", Node: "node-a", Subject: testSubject, Provider: "openai", Model: "gpt-4o-mini", Metered: true})
	if err != nil || reservation.Tracked || reservation.ID != "" || len(reservation.BudgetIDs) != 0 {
		t.Fatalf("untracked reservation = %+v, %v", reservation, err)
	}
	if stats := manager.Stats(); stats.Reservations != 0 {
		t.Fatalf("untracked request retained: %+v", stats)
	}
}

func TestBudgetAdministrationIsIdempotentAndReferenceSafe(t *testing.T) {
	manager := newTestManager(t, Config{MaxBudgets: 1})
	value := Budget{ID: "dynamic", Tenant: "tenant-a", AttachTo: AttachTenant, AttachID: "tenant-a", Window: WindowHour, MaxRequests: 2}
	if err := manager.PutBudget(context.Background(), value); err != nil {
		t.Fatal(err)
	}
	if err := manager.PutBudget(context.Background(), value); err != nil {
		t.Fatalf("exact PutBudget replay: %v", err)
	}
	changed := value
	changed.MaxRequests = 3
	if err := manager.PutBudget(context.Background(), changed); !errors.Is(err, ErrConflict) {
		t.Fatalf("changed PutBudget error = %v", err)
	}
	reservation, err := manager.Reserve(context.Background(), ReserveRequest{Key: "dynamic-request", Node: "node-a", Subject: testSubject, Provider: "other", Metered: false})
	if err != nil || !reservation.Tracked {
		t.Fatalf("dynamic Reserve = %+v, %v", reservation, err)
	}
	if err := manager.DeleteBudget(context.Background(), value.Tenant, value.ID); err != nil {
		t.Fatal(err)
	}
	if err := manager.PutBudget(context.Background(), value); !errors.Is(err, ErrConflict) {
		t.Fatalf("reused referenced budget id error = %v", err)
	}
	values, err := manager.Budgets(context.Background())
	if err != nil || len(values) != 0 {
		t.Fatalf("Budgets after delete = %+v, %v", values, err)
	}
}

func TestBudgetIDsAreTenantScoped(t *testing.T) {
	manager := newTestManager(t, Config{MaxBudgets: 2})
	for _, tenant := range []string{"tenant-a", "tenant-b"} {
		value := Budget{ID: "daily", Tenant: tenant, AttachTo: AttachTenant, AttachID: tenant, Window: WindowDay, MaxRequests: 1}
		if err := manager.PutBudget(context.Background(), value); err != nil {
			t.Fatalf("put %s: %v", tenant, err)
		}
		if got, ok := manager.Budget(tenant, value.ID); !ok || got != value {
			t.Fatalf("budget %s = %+v, %v", tenant, got, ok)
		}
	}
	if err := manager.DeleteBudget(context.Background(), "tenant-a", "daily"); err != nil {
		t.Fatal(err)
	}
	if _, ok := manager.Budget("tenant-a", "daily"); ok {
		t.Fatal("tenant-a budget remained")
	}
	if _, ok := manager.Budget("tenant-b", "daily"); !ok {
		t.Fatal("tenant-a removal deleted tenant-b budget")
	}
}

func TestPerRequestAttachmentWorkIsBounded(t *testing.T) {
	budgets := make([]Budget, maximumBudgetsPerRequest+1)
	for i := range budgets {
		budgets[i] = Budget{ID: "budget-" + strconv.Itoa(i), Tenant: "tenant-a", AttachTo: AttachTenant, AttachID: "tenant-a", Window: WindowHour, MaxRequests: 100}
	}
	manager := newTestManager(t, Config{Budgets: budgets, MaxBudgets: len(budgets)})
	_, err := manager.Reserve(context.Background(), ReserveRequest{Key: "bounded", Node: "node-a", Subject: testSubject, Provider: "other", Metered: false})
	if !errors.Is(err, ErrCapacity) {
		t.Fatalf("Reserve with too many matching budgets = %v", err)
	}
	if stats := manager.Stats(); stats.Reservations != 0 {
		t.Fatalf("bounded rejection retained a reservation: %+v", stats)
	}
}

func TestDurableRecordRoundTripPreservesAdmissionAndSettlement(t *testing.T) {
	policy := Budget{ID: "durable", Tenant: "tenant-a", AttachTo: AttachTenant, AttachID: "tenant-a", Window: WindowDay, MaxRequests: 1, MaxTokens: 100}
	first := newTestManager(t, Config{Budgets: []Budget{policy}})
	request := ReserveRequest{Key: "durable-request", Node: "node-a", Subject: testSubject, Provider: "openai", Model: "gpt-4o-mini", InputTokens: 10, MaxOutputTokens: 20, Metered: true}
	reservation, err := first.Reserve(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	record, ok := first.Record(reservation.ID)
	if !ok {
		t.Fatal("reserved record is not exportable")
	}
	restored := newTestManager(t, Config{Budgets: []Budget{policy}})
	if err := restored.RestoreRecord(reservation.ID, &record); err != nil {
		t.Fatal(err)
	}
	if _, err := restored.Reserve(context.Background(), ReserveRequest{Key: "second", Node: "node-a", Subject: testSubject, Provider: "other", Metered: false}); !errors.Is(err, ErrBudgetExceeded) {
		t.Fatalf("restored active reservation did not consume request capacity: %v", err)
	}
	settlement, err := first.Settle(context.Background(), SettleRequest{ReservationID: reservation.ID, Mode: SettlementMetered, InputTokens: 9, OutputTokens: 11})
	if err != nil {
		t.Fatal(err)
	}
	record, ok = first.Record(reservation.ID)
	if !ok || record.Settlement == nil || record.Settlement.Tokens != settlement.Tokens {
		t.Fatalf("settled record = %+v, ok=%v", record, ok)
	}
	if err := restored.RestoreRecord(reservation.ID, &record); err != nil {
		t.Fatal(err)
	}
	usage, err := restored.Usage(context.Background(), UsageQuery{Tenant: "tenant-a"})
	if err != nil || len(usage) != 1 || usage[0].Tokens != 20 || usage[0].ActiveReservations != 0 {
		t.Fatalf("restored usage = %+v, %v", usage, err)
	}
}

func newTestManager(t *testing.T, cfg Config) *Manager {
	t.Helper()
	manager, err := NewManager(cfg)
	if err != nil {
		t.Fatal(err)
	}
	return manager
}

func usageByID(values []Usage, id string) Usage {
	for _, value := range values {
		if value.BudgetID == id {
			return value
		}
	}
	return Usage{}
}
