package control

import (
	"context"
	"testing"
	"time"

	"remount.dev/remount/internal/budget"
	"remount.dev/remount/internal/proto"
)

func TestBudgetReservationIsAuthoritativeDurableAndObservable(t *testing.T) {
	f := newControlFixture(t, "", nil)
	ws := claimedApprovalWorkspace(t, f)
	created, err := f.c.budgetCreate(context.Background(), localSubject(), &proto.BudgetCreateReq{
		Budget:         proto.Budget{ID: "daily-requests", AttachTo: string(budget.AttachTenant), AttachID: "tenant-a", Window: string(budget.WindowDay), MaxRequests: 1},
		IdempotencyKey: "create-budget",
	})
	if err != nil || created.Tenant != "tenant-a" {
		t.Fatalf("create budget = %+v, %v", created, err)
	}
	request := &proto.BudgetReserveReq{
		Key: "request-one", WS: ws.ID, Gen: ws.Generation, Principal: "alice", Provider: "unknown",
	}
	reserved, err := f.c.budgetReserve(context.Background(), "n_approve", request)
	if err != nil || reserved.Denied || !reserved.Tracked || reserved.ID == "" {
		t.Fatalf("reserve = %+v, %v", reserved, err)
	}
	if _, err := f.c.budgetSettle(context.Background(), "n_approve", &proto.BudgetSettleReq{
		Reservation: reserved.ID, Mode: string(budget.SettlementRequestOnly),
	}); err != nil {
		t.Fatal(err)
	}
	request.Key = "request-two"
	denied, err := f.c.budgetReserve(context.Background(), "n_approve", request)
	if err != nil || !denied.Denied || len(denied.BudgetIDs) != 1 || denied.BudgetIDs[0] != created.ID {
		t.Fatalf("denied reserve = %+v, %v", denied, err)
	}
	usage, err := f.c.usageGet(context.Background(), localSubject(), &proto.UsageReq{Window: string(budget.WindowDay)})
	if err != nil || len(usage.Usage) != 1 || usage.Usage[0].Requests != 1 || usage.Usage[0].UnmeteredRequests != 1 {
		t.Fatalf("usage = %+v, %v", usage, err)
	}
	if len(eventsOfType(t, f.log, proto.EvBudgetReserved)) != 1 || len(eventsOfType(t, f.log, proto.EvBudgetSettled)) != 1 || len(eventsOfType(t, f.log, proto.EvEgressDenied)) != 1 {
		t.Fatal("budget transitions were not each observable")
	}

	f.c.Stop()
	again, err := New(Options{DB: f.sq.DB(), Log: f.log, Token: "node-token", LeaseSec: 10})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(again.Stop)
	restored, err := again.usageGet(context.Background(), localSubject(), &proto.UsageReq{Window: string(budget.WindowDay)})
	if err != nil || len(restored.Usage) != 1 || restored.Usage[0].Requests != 1 {
		t.Fatalf("restored usage = %+v, %v", restored, err)
	}
}

func TestBudgetReserveRejectsStaleWorkspaceGeneration(t *testing.T) {
	f := newControlFixture(t, "", nil)
	ws := claimedApprovalWorkspace(t, f)
	_, err := f.c.budgetReserve(context.Background(), "n_approve", &proto.BudgetReserveReq{
		Key: "stale", WS: ws.ID, Gen: ws.Generation + 1, Principal: "alice", Provider: "unknown",
	})
	if codeOf(err) != proto.CodeDenied {
		t.Fatalf("stale reserve = %v", err)
	}
}

func TestBudgetCreateExactDefinitionDoesNotDuplicateOrDelete(t *testing.T) {
	f := newControlFixture(t, "", nil)
	req := proto.BudgetCreateReq{
		Budget:         proto.Budget{ID: "same-budget", AttachTo: string(budget.AttachTenant), AttachID: "tenant-a", Window: string(budget.WindowHour), MaxRequests: 2},
		IdempotencyKey: "first-key",
	}
	first, err := f.c.budgetCreate(context.Background(), localSubject(), &req)
	if err != nil {
		t.Fatal(err)
	}
	req.IdempotencyKey = "second-key"
	second, err := f.c.budgetCreate(context.Background(), localSubject(), &req)
	if err != nil || *second != *first {
		t.Fatalf("exact definition replay = %+v, %v; want %+v", second, err, first)
	}
	listed, err := f.c.budgetList(context.Background(), localSubject())
	if err != nil || len(listed.Budgets) != 1 || listed.Budgets[0].CreatedAt == 0 {
		t.Fatalf("listed = %+v, %v", listed, err)
	}
	if got := len(eventsOfType(t, f.log, proto.EvBudgetCreated)); got != 1 {
		t.Fatalf("budget.created events = %d, want 1", got)
	}
}

func TestBudgetReserveRejectsBindingNotAttachedToWorkspace(t *testing.T) {
	f := newControlFixture(t, "", nil)
	ws := claimedApprovalWorkspace(t, f)
	_, err := f.c.budgetReserve(context.Background(), "n_approve", &proto.BudgetReserveReq{
		Key: "forged-binding", WS: ws.ID, Gen: ws.Generation, Principal: "alice", Provider: "unknown", Bindings: []string{"other-binding"},
	})
	if codeOf(err) != proto.CodeDenied {
		t.Fatalf("unattached binding reserve = %v", err)
	}
}

func TestBudgetReservationExpiryIsDurableAndObservable(t *testing.T) {
	now := time.Now().UTC()
	f := newControlFixture(t, "", func(opts *Options) {
		opts.Now = func() time.Time { return now }
		opts.BudgetConfig.ReservationTTL = time.Second
	})
	ws := claimedApprovalWorkspace(t, f)
	_, err := f.c.budgetCreate(context.Background(), localSubject(), &proto.BudgetCreateReq{
		Budget: proto.Budget{ID: "expiry-budget", AttachTo: string(budget.AttachTenant), AttachID: "tenant-a", Window: string(budget.WindowHour), MaxRequests: 2}, IdempotencyKey: "create-expiry",
	})
	if err != nil {
		t.Fatal(err)
	}
	reserved, err := f.c.budgetReserve(context.Background(), "n_approve", &proto.BudgetReserveReq{
		Key: "will-expire", WS: ws.ID, Gen: ws.Generation, Principal: "alice", Provider: "unknown",
	})
	if err != nil || !reserved.Tracked {
		t.Fatalf("reserve = %+v, %v", reserved, err)
	}
	now = now.Add(2 * time.Second)
	f.c.Tick(context.Background())
	usage, err := f.c.usageGet(context.Background(), localSubject(), &proto.UsageReq{})
	if err != nil || len(usage.Usage) != 1 || usage.Usage[0].ActiveReservations != 0 || usage.Usage[0].IncompleteRequests != 1 {
		t.Fatalf("expired usage = %+v, %v", usage, err)
	}
	if got := len(eventsOfType(t, f.log, proto.EvBudgetExpired)); got != 1 {
		t.Fatalf("budget.expired events = %d, want 1", got)
	}
	f.c.Stop()
	again, err := New(Options{DB: f.sq.DB(), Log: f.log, Token: "node-token", LeaseSec: 10, Now: func() time.Time { return now }, BudgetConfig: budget.Config{ReservationTTL: time.Second}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(again.Stop)
	restored, err := again.usageGet(context.Background(), localSubject(), &proto.UsageReq{})
	if err != nil || len(restored.Usage) != 1 || restored.Usage[0].IncompleteRequests != 1 {
		t.Fatalf("restored expiry = %+v, %v", restored, err)
	}
}
