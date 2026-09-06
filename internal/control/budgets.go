package control

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"sort"
	"time"

	"remount.dev/remount/internal/budget"
	"remount.dev/remount/internal/eventlog"
	"remount.dev/remount/internal/metrics"
	"remount.dev/remount/internal/proto"
)

func protoBudget(value budget.Budget, createdAt, updatedAt int64) proto.Budget {
	return proto.Budget{
		ID: value.ID, Tenant: value.Tenant, AttachTo: string(value.AttachTo), AttachID: value.AttachID, Window: string(value.Window),
		MaxRequests: value.MaxRequests, MaxTokens: value.MaxTokens, MaxEstimatedCostMicros: value.MaxEstimatedCostMicros,
		CreatedAt: createdAt, UpdatedAt: updatedAt,
	}
}

func budgetValue(value proto.Budget, tenant string) budget.Budget {
	return budget.Budget{
		ID: value.ID, Tenant: tenant, AttachTo: budget.AttachmentKind(value.AttachTo), AttachID: value.AttachID,
		Window: budget.Window(value.Window), MaxRequests: value.MaxRequests, MaxTokens: value.MaxTokens,
		MaxEstimatedCostMicros: value.MaxEstimatedCostMicros,
	}
}

func (c *Control) loadBudgets() error {
	rows, err := c.db.Query(`SELECT data FROM budgets ORDER BY id`)
	if err != nil {
		return err
	}
	for rows.Next() {
		var data []byte
		if err := rows.Scan(&data); err != nil {
			rows.Close()
			return err
		}
		var value proto.Budget
		if err := proto.Unmarshal(data, &value); err != nil {
			rows.Close()
			return fmt.Errorf("decode budget: %w", err)
		}
		if err := c.budgets.PutBudget(context.Background(), budgetValue(value, value.Tenant)); err != nil {
			rows.Close()
			return fmt.Errorf("restore budget %s: %w", value.ID, err)
		}
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return err
	}
	if err := rows.Close(); err != nil {
		return err
	}
	rows, err = c.db.Query(`SELECT id, data FROM budget_reservations ORDER BY id`)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		var id string
		var data []byte
		if err := rows.Scan(&id, &data); err != nil {
			return err
		}
		var record budget.Record
		if err := proto.Unmarshal(data, &record); err != nil {
			return fmt.Errorf("decode budget reservation %s: %w", id, err)
		}
		if err := c.budgets.RestoreRecord(id, &record); err != nil {
			return fmt.Errorf("restore budget reservation %s: %w", id, err)
		}
	}
	return rows.Err()
}

func (c *Control) budgetCreate(ctx context.Context, subject Subject, req *proto.BudgetCreateReq) (*proto.Budget, error) {
	value := budgetValue(req.Budget, subject.Tenant)
	if err := budget.ValidateBudget(value); err != nil {
		return nil, proto.Err(proto.CodeBadRequest, "%v", err)
	}
	if err := c.check(ctx, subject, ActionAdmin, Resource{Kind: "budget", ID: value.ID, Tenant: subject.Tenant, Owner: subject.ID}); err != nil {
		return nil, err
	}
	scope := subject.Tenant + "|" + subject.ID + "|budget.create"
	unlock := c.lockMutation(scope, req.IdempotencyKey)
	defer unlock()
	var prior proto.Budget
	if hit, err := c.mutationLookup(scope, req.IdempotencyKey, proto.OpBudgetCreate, req, &prior); err != nil {
		return nil, err
	} else if hit {
		return &prior, nil
	}
	now := c.now().UnixMilli()
	result := protoBudget(value, now, now)
	c.budgetMu.Lock()
	defer c.budgetMu.Unlock()
	if current, ok := c.budgets.Budget(value.Tenant, value.ID); ok {
		if current != value {
			return nil, proto.Err(proto.CodeConflict, "budget %s already exists with another definition", value.ID)
		}
		var stored []byte
		if err := c.db.QueryRowContext(ctx, `SELECT data FROM budgets WHERE id=? AND tenant=?`, value.ID, value.Tenant).Scan(&stored); err != nil {
			if !errors.Is(err, sql.ErrNoRows) {
				return nil, err
			}
			return nil, proto.Err(proto.CodeInternal, "budget %s exists only in volatile authority state", value.ID)
		}
		var existing proto.Budget
		if err := proto.Unmarshal(stored, &existing); err != nil {
			return nil, proto.Err(proto.CodeInternal, "decode budget %s: %v", value.ID, err)
		}
		if budgetValue(existing, existing.Tenant) != value {
			return nil, proto.Err(proto.CodeConflict, "budget %s durable definition differs", value.ID)
		}
		if err := c.transact(func(tx *eventlog.Tx) error {
			return c.insertMutationTx(tx.Tx, scope, req.IdempotencyKey, proto.OpBudgetCreate, req, existing)
		}, nil); err != nil {
			return nil, err
		}
		return &existing, nil
	}
	if err := c.budgets.PutBudget(ctx, value); err != nil {
		if errors.Is(err, budget.ErrConflict) {
			return nil, proto.Err(proto.CodeConflict, "budget %s already exists with another definition", value.ID)
		}
		if errors.Is(err, budget.ErrCapacity) {
			return nil, proto.ErrReason(proto.CodeResourceExhausted, proto.ReasonQuotaExceeded, "budget capacity exhausted")
		}
		return nil, proto.Err(proto.CodeBadRequest, "%v", err)
	}
	event := c.newEvent(proto.EvBudgetCreated, value.ID, subject.ID, "", map[string]any{"budget": value.ID, "attach_to": value.AttachTo, "attach_id": value.AttachID, "window": value.Window})
	event.Tenant = subject.Tenant
	err := c.transact(func(tx *eventlog.Tx) error {
		if _, err := tx.Exec(`INSERT INTO budgets(id, tenant, data) VALUES(?,?,?)`, result.ID, result.Tenant, proto.MustMarshal(result)); err != nil {
			return err
		}
		return c.insertMutationTx(tx.Tx, scope, req.IdempotencyKey, proto.OpBudgetCreate, req, result)
	}, []*proto.Event{event})
	if err != nil {
		_ = c.budgets.RestoreBudget(value.Tenant, value.ID, nil)
		return nil, err
	}
	return &result, nil
}

func (c *Control) budgetList(ctx context.Context, subject Subject) (*proto.BudgetListRes, error) {
	values, err := func() ([]proto.Budget, error) {
		c.budgetMu.Lock()
		defer c.budgetMu.Unlock()
		rows, err := c.db.QueryContext(ctx, `SELECT data FROM budgets ORDER BY id`)
		if err != nil {
			return nil, err
		}
		defer rows.Close()
		var values []proto.Budget
		for rows.Next() {
			var data []byte
			if err := rows.Scan(&data); err != nil {
				return nil, err
			}
			var value proto.Budget
			if err := proto.Unmarshal(data, &value); err != nil {
				return nil, proto.Err(proto.CodeInternal, "decode budget: %v", err)
			}
			values = append(values, value)
		}
		return values, rows.Err()
	}()
	if err != nil {
		return nil, err
	}
	result := &proto.BudgetListRes{Budgets: []proto.Budget{}}
	for _, value := range values {
		if value.Tenant != subject.Tenant {
			continue
		}
		if err := c.check(ctx, subject, ActionRead, Resource{Kind: "budget", ID: value.ID, Tenant: value.Tenant}); err == nil {
			result.Budgets = append(result.Budgets, value)
		}
	}
	sort.Slice(result.Budgets, func(i, j int) bool { return result.Budgets[i].ID < result.Budgets[j].ID })
	return result, nil
}

func (c *Control) expireBudgets(ctx context.Context, at time.Time, lostNodes []string) {
	c.budgetMu.Lock()
	defer c.budgetMu.Unlock()
	before := c.budgets.Records()
	expiredSet := map[string]struct{}{}
	collectedSet := map[string]struct{}{}
	for _, node := range lostNodes {
		result, err := c.budgets.ExpireNode(ctx, node, at)
		if err != nil {
			c.logger.Error("expire node budget reservations", "node", node, "err", err)
			c.restoreBudgetRecords(before)
			return
		}
		for _, id := range result.ReservationIDs {
			expiredSet[id] = struct{}{}
		}
		for _, id := range result.CollectedIDs {
			collectedSet[id] = struct{}{}
		}
	}
	result, err := c.budgets.Sweep(ctx, at)
	if err != nil {
		c.logger.Error("sweep budget reservations", "err", err)
		c.restoreBudgetRecords(before)
		return
	}
	for _, id := range result.ReservationIDs {
		expiredSet[id] = struct{}{}
	}
	for _, id := range result.CollectedIDs {
		collectedSet[id] = struct{}{}
	}
	if len(expiredSet) == 0 && len(collectedSet) == 0 {
		return
	}
	expiredIDs := sortedSet(expiredSet)
	collectedIDs := sortedSet(collectedSet)
	events := make([]*proto.Event, 0, len(expiredIDs))
	for _, id := range expiredIDs {
		record, ok := c.budgets.Record(id)
		if !ok {
			continue
		}
		event := c.newEvent(proto.EvBudgetExpired, record.Reservation.Subject.Workspace, record.Reservation.Subject.Principal, record.Reservation.Node, map[string]any{
			"reservation": id, "budget_id": record.Reservation.BudgetIDs,
		})
		event.Tenant = record.Reservation.Subject.Tenant
		event.Generation = record.Reservation.Subject.Generation
		events = append(events, event)
	}
	if err := c.transact(func(tx *eventlog.Tx) error {
		for _, id := range expiredIDs {
			record, ok := c.budgets.Record(id)
			if !ok {
				continue
			}
			if _, err := tx.Exec(`UPDATE budget_reservations SET data=? WHERE id=?`, proto.MustMarshal(record), id); err != nil {
				return err
			}
		}
		for _, id := range collectedIDs {
			if _, err := tx.Exec(`DELETE FROM budget_reservations WHERE id=?`, id); err != nil {
				return err
			}
		}
		return nil
	}, events); err != nil {
		c.logger.Error("persist expired budget reservations", "err", err)
		c.restoreBudgetRecords(before)
		return
	}
	metrics.BudgetExpired.Add(uint64(len(expiredIDs)))
}

func (c *Control) restoreBudgetRecords(before map[string]budget.Record) {
	current := c.budgets.Records()
	for id := range current {
		if _, ok := before[id]; !ok {
			_ = c.budgets.RestoreRecord(id, nil)
		}
	}
	for id, record := range before {
		copy := record
		_ = c.budgets.RestoreRecord(id, &copy)
	}
}

func sortedSet(values map[string]struct{}) []string {
	result := make([]string, 0, len(values))
	for value := range values {
		result = append(result, value)
	}
	sort.Strings(result)
	return result
}

func (c *Control) budgetRemove(ctx context.Context, subject Subject, req *proto.BudgetRemoveReq) error {
	if req.ID == "" {
		return proto.Err(proto.CodeBadRequest, "budget id is required")
	}
	scope := subject.Tenant + "|" + subject.ID + "|budget.remove"
	unlock := c.lockMutation(scope, req.IdempotencyKey)
	defer unlock()
	if hit, err := c.mutationLookup(scope, req.IdempotencyKey, proto.OpBudgetRemove, req, nil); err != nil || hit {
		return err
	}
	value, found := c.budgets.Budget(subject.Tenant, req.ID)
	if !found || value.Tenant != subject.Tenant {
		return proto.Err(proto.CodeNotFound, "budget %s", req.ID)
	}
	if err := c.check(ctx, subject, ActionAdmin, Resource{Kind: "budget", ID: value.ID, Tenant: value.Tenant}); err != nil {
		return err
	}
	c.budgetMu.Lock()
	defer c.budgetMu.Unlock()
	current, found := c.budgets.Budget(subject.Tenant, req.ID)
	if !found || current != value {
		return proto.Err(proto.CodeConflict, "budget %s changed during removal", req.ID)
	}
	if err := c.budgets.DeleteBudget(ctx, value.Tenant, value.ID); err != nil {
		return err
	}
	event := c.newEvent(proto.EvBudgetRemoved, value.ID, subject.ID, "", map[string]any{"budget": value.ID})
	event.Tenant = value.Tenant
	if err := c.transact(func(tx *eventlog.Tx) error {
		if _, err := tx.Exec(`DELETE FROM budgets WHERE id=? AND tenant=?`, value.ID, value.Tenant); err != nil {
			return err
		}
		return c.insertMutationTx(tx.Tx, scope, req.IdempotencyKey, proto.OpBudgetRemove, req, struct{}{})
	}, []*proto.Event{event}); err != nil {
		_ = c.budgets.RestoreBudget(value.Tenant, value.ID, &value)
		return err
	}
	return nil
}

func (c *Control) budgetReserve(ctx context.Context, node string, req *proto.BudgetReserveReq) (*proto.BudgetReservation, error) {
	c.mu.Lock()
	ws := c.workspaces[req.WS]
	if ws == nil || ws.Node != node || ws.Generation != req.Gen || (ws.State != proto.WSClaimed && ws.State != proto.WSClaiming) {
		c.mu.Unlock()
		return nil, proto.Err(proto.CodeDenied, "workspace %s generation is not held by %s", req.WS, node)
	}
	wsCopy := *ws
	c.mu.Unlock()
	allowedBindings := make(map[string]struct{}, len(wsCopy.Spec.Bindings))
	for _, id := range wsCopy.Spec.Bindings {
		allowedBindings[id] = struct{}{}
	}
	for _, id := range req.Bindings {
		if _, ok := allowedBindings[id]; !ok {
			return nil, proto.Err(proto.CodeDenied, "binding %s is not attached to workspace %s", id, req.WS)
		}
	}
	request := budget.ReserveRequest{
		Key: req.Key, Node: node, Subject: budget.Subject{Tenant: wsCopy.Tenant, Workspace: wsCopy.ID, Generation: req.Gen, Principal: req.Principal, Bindings: append([]string(nil), req.Bindings...)},
		Provider: req.Provider, Model: req.Model, InputTokens: req.InputTokens, MaxOutputTokens: req.MaxOutputTokens, Metered: req.Metered,
	}
	c.budgetMu.Lock()
	defer c.budgetMu.Unlock()
	reservationID := budget.ReservationID(wsCopy.Tenant, req.Key)
	_, recordExisted := c.budgets.Record(reservationID)
	reservation, err := c.budgets.Reserve(ctx, request)
	if err != nil {
		var denied *budget.DeniedError
		if errors.As(err, &denied) {
			event := stampWS(c.newEvent(proto.EvEgressDenied, wsCopy.ID, req.Principal, node, map[string]any{
				"generation": req.Gen, "reason": "budget_exceeded", "budget_id": denied.BudgetIDs, "limit": denied.Limit,
			}), &wsCopy)
			if txErr := c.transact(func(*eventlog.Tx) error { return nil }, []*proto.Event{event}); txErr != nil {
				return nil, txErr
			}
			metrics.BudgetDenied.Inc()
			return &proto.BudgetReservation{Denied: true, BudgetIDs: append([]string(nil), denied.BudgetIDs...), Limit: string(denied.Limit), UnmeteredReason: denied.UnmeteredReason}, nil
		}
		if errors.Is(err, budget.ErrCapacity) {
			return nil, proto.ErrReason(proto.CodeResourceExhausted, proto.ReasonQuotaExceeded, "budget authority capacity exhausted")
		}
		return nil, proto.Err(proto.CodeBadRequest, "%v", err)
	}
	result := &proto.BudgetReservation{
		ID: reservation.ID, Tracked: reservation.Tracked, BudgetIDs: append([]string(nil), reservation.BudgetIDs...),
		UnmeteredReason: reservation.UnmeteredReason,
	}
	if !reservation.Tracked {
		return result, nil
	}
	result.ExpiresAt = reservation.ExpiresAt.UnixMilli()
	if _, existed := c.budgets.Record(reservation.ID); !existed {
		return nil, proto.Err(proto.CodeInternal, "budget reservation disappeared before commit")
	}
	// An exact retry is already durable and must not duplicate its event.
	var count int
	if err := c.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM budget_reservations WHERE id=?`, reservation.ID).Scan(&count); err != nil {
		if !recordExisted {
			_ = c.budgets.RestoreRecord(reservation.ID, nil)
		}
		return nil, err
	}
	if count > 0 {
		return result, nil
	}
	record, _ := c.budgets.Record(reservation.ID)
	events := []*proto.Event{stampWS(c.newEvent(proto.EvBudgetReserved, wsCopy.ID, req.Principal, node, map[string]any{
		"reservation": reservation.ID, "budget_id": reservation.BudgetIDs, "provider": reservation.Provider,
	}), &wsCopy)}
	if reservation.UnmeteredReason != "" {
		events = append(events, stampWS(c.newEvent(proto.EvBudgetUnmetered, wsCopy.ID, req.Principal, node, map[string]any{
			"reservation": reservation.ID, "reason": reservation.UnmeteredReason,
		}), &wsCopy))
	}
	if err := c.transact(func(tx *eventlog.Tx) error {
		_, err := tx.Exec(`INSERT INTO budget_reservations(id, data) VALUES(?,?)`, reservation.ID, proto.MustMarshal(record))
		return err
	}, events); err != nil {
		_ = c.budgets.RestoreRecord(reservation.ID, nil)
		return nil, err
	}
	metrics.BudgetReservations.Inc()
	if reservation.UnmeteredReason != "" {
		metrics.BudgetUnmetered.Inc()
	}
	return result, nil
}

func (c *Control) budgetSettle(ctx context.Context, node string, req *proto.BudgetSettleReq) (*proto.BudgetSettlement, error) {
	c.budgetMu.Lock()
	defer c.budgetMu.Unlock()
	prior, ok := c.budgets.Record(req.Reservation)
	if !ok || prior.Request.Node != node {
		return nil, proto.Err(proto.CodeDenied, "reservation %s does not belong to %s", req.Reservation, node)
	}
	settlement, err := c.budgets.Settle(ctx, budget.SettleRequest{
		ReservationID: req.Reservation, Mode: budget.SettlementMode(req.Mode), InputTokens: req.InputTokens, OutputTokens: req.OutputTokens,
	})
	if err != nil {
		return nil, proto.Err(proto.CodeConflict, "%v", err)
	}
	result := &proto.BudgetSettlement{
		Reservation: settlement.ID, Mode: string(settlement.Mode), Tokens: settlement.Tokens,
		EstimatedCostMicros: settlement.EstimatedCostMicros, EstimatedCostUnavailable: settlement.EstimatedCostUnavailable,
		UsageExceededReservation: settlement.UsageExceededReservation,
	}
	if prior.Settlement != nil {
		return result, nil
	}
	record, _ := c.budgets.Record(req.Reservation)
	event := c.newEvent(proto.EvBudgetSettled, record.Reservation.Subject.Workspace, record.Reservation.Subject.Principal, node, map[string]any{
		"reservation": settlement.ID, "mode": settlement.Mode, "tokens": settlement.Tokens, "estimated_cost_micros": settlement.EstimatedCostMicros,
	})
	event.Tenant = record.Reservation.Subject.Tenant
	event.Generation = record.Reservation.Subject.Generation
	if err := c.transact(func(tx *eventlog.Tx) error {
		_, err := tx.Exec(`UPDATE budget_reservations SET data=? WHERE id=?`, proto.MustMarshal(record), req.Reservation)
		return err
	}, []*proto.Event{event}); err != nil {
		_ = c.budgets.RestoreRecord(req.Reservation, &prior)
		return nil, err
	}
	metrics.BudgetSettlements.Inc()
	return result, nil
}

func (c *Control) usageGet(ctx context.Context, subject Subject, req *proto.UsageReq) (*proto.UsageRes, error) {
	tenant := req.Tenant
	if tenant == "" {
		tenant = subject.Tenant
	}
	if tenant != subject.Tenant && subject.Tenant != "*" {
		return nil, proto.Err(proto.CodeDenied, "subject %s cannot read tenant %s usage", subject.ID, tenant)
	}
	if err := c.check(ctx, subject, ActionRead, Resource{Kind: "usage", Tenant: tenant}); err != nil {
		return nil, err
	}
	c.budgetMu.Lock()
	values, err := c.budgets.Usage(ctx, budget.UsageQuery{
		Tenant: tenant, Workspace: req.WS, Principal: req.Principal, Binding: req.Binding, Window: budget.Window(req.Window), At: c.now(),
	})
	c.budgetMu.Unlock()
	if err != nil {
		return nil, proto.Err(proto.CodeBadRequest, "%v", err)
	}
	result := &proto.UsageRes{Usage: make([]proto.Usage, 0, len(values))}
	for _, value := range values {
		result.Usage = append(result.Usage, proto.Usage{
			BudgetID: value.BudgetID, Window: string(value.Window), Since: value.Since.UnixMilli(), Requests: value.Requests,
			Tokens: value.Tokens, EstimatedCostMicros: value.EstimatedCostMicros, ActiveReservations: value.ActiveReservations,
			UnmeteredRequests: value.UnmeteredRequests, IncompleteRequests: value.IncompleteRequests,
		})
	}
	return result, nil
}
