package tenant

import (
	"bufio"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"io"
	"strings"
	"sync"
	"time"

	"remount.dev/remount/internal/eventlog"
)

// ExportOptions identify one independently-resumable tenant exporter.
type ExportOptions struct {
	Tenant   string
	Name     string
	PageSize int
	Actor    string
}

// ExportOnce sends at most PageSize events in sequence order and advances the
// durable cursor afterwards. A crash after send and before cursor commit may
// replay the page; a successful cursor commit guarantees no event was skipped.
func (s *Store) ExportOnce(ctx context.Context, exporter BillingExporter, options ExportOptions) (int, error) {
	if exporter == nil || validateTenantID(options.Tenant) != nil || !safeIdentifier(options.Name, 128) ||
		options.PageSize < 1 || options.PageSize > maxPage {
		return 0, &Error{Code: CodeBadRequest}
	}
	next, err := s.EnsureExporter(ctx, options.Tenant, options.Name, options.Actor)
	if err != nil {
		return 0, err
	}
	events, err := s.readMeterPage(ctx, options.Tenant, next, options.PageSize)
	if err != nil || len(events) == 0 {
		return 0, err
	}
	if err := exporter.Export(ctx, events); err != nil {
		return 0, err
	}
	advanced := events[len(events)-1].Seq + 1
	now := s.now().UTC()
	err = s.log.Transact(ctx, s.db, func(tx *eventlog.Tx) error {
		result, err := tx.ExecContext(ctx, `UPDATE tenant_meter_cursors SET next_seq=?,updated_at=? WHERE tenant=? AND exporter=? AND next_seq=?`,
			advanced, millis(now), options.Tenant, options.Name, next)
		if err != nil {
			return err
		}
		changed, err := result.RowsAffected()
		if err != nil {
			return err
		}
		if changed != 1 {
			return &Error{Code: CodeConflict}
		}
		return tx.Emit(s.event(now, "usage.exported", options.Tenant, options.Actor, options.Name,
			map[string]any{"exporter": options.Name, "count": len(events), "through_seq": events[len(events)-1].Seq}))
	})
	if err = normalizeDrain(err); err != nil {
		return 0, err
	}
	return len(events), nil
}

// Cursor returns the next global meter sequence for a named tenant exporter.
func (s *Store) Cursor(ctx context.Context, tenant, name string) (uint64, error) {
	if validateTenantID(tenant) != nil || !safeIdentifier(name, 128) {
		return 0, &Error{Code: CodeBadRequest}
	}
	var next uint64
	err := s.db.QueryRowContext(ctx, `SELECT next_seq FROM tenant_meter_cursors WHERE tenant=? AND exporter=?`, tenant, name).Scan(&next)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, &Error{Code: CodeNotFound}
	}
	return next, err
}

// EnsureExporter durably registers an exporter cursor before retention can
// prune unexported meter rows. Repeated registration is a no-op.
func (s *Store) EnsureExporter(ctx context.Context, tenant, name, actor string) (uint64, error) {
	if validateTenantID(tenant) != nil || !safeIdentifier(name, 128) || strings.TrimSpace(actor) == "" || len(actor) > 256 {
		return 0, &Error{Code: CodeBadRequest}
	}
	var next uint64
	err := s.db.QueryRowContext(ctx, `SELECT next_seq FROM tenant_meter_cursors WHERE tenant=? AND exporter=?`, tenant, name).Scan(&next)
	if err == nil {
		return next, nil
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return 0, err
	}
	// Avoid depending on driver error text: an existence check and INSERT OR
	// IGNORE safely converge concurrent initializers.
	var exists int
	if tenantErr := s.db.QueryRowContext(ctx, `SELECT count(*) FROM tenants WHERE id=? AND state<>?`, tenant, StateDeleted).Scan(&exists); tenantErr != nil {
		return 0, tenantErr
	}
	if exists != 1 {
		return 0, &Error{Code: CodeNotFound}
	}
	now := s.now().UTC()
	err = s.log.Transact(ctx, s.db, func(tx *eventlog.Tx) error {
		var count int
		if countErr := tx.QueryRowContext(ctx, `SELECT count(*) FROM tenant_meter_cursors WHERE tenant=?`, tenant).Scan(&count); countErr != nil {
			return countErr
		}
		if count >= s.maxExporters {
			return &Error{Code: CodeResourceExhausted}
		}
		result, insertErr := tx.ExecContext(ctx, `INSERT OR IGNORE INTO tenant_meter_cursors(tenant,exporter,next_seq,updated_at) VALUES(?,?,1,?)`, tenant, name, millis(now))
		if insertErr != nil {
			return insertErr
		}
		inserted, insertErr := result.RowsAffected()
		if insertErr != nil || inserted == 0 {
			return insertErr
		}
		return tx.Emit(s.event(now, "usage.exporter_registered", tenant, actor, name, map[string]any{"exporter": name}))
	})
	if err = normalizeDrain(err); err != nil {
		return 0, err
	}
	if err := s.db.QueryRowContext(ctx, `SELECT next_seq FROM tenant_meter_cursors WHERE tenant=? AND exporter=?`, tenant, name).Scan(&next); err != nil {
		return 0, err
	}
	return next, nil
}

func (s *Store) readMeterPage(ctx context.Context, tenant string, next uint64, limit int) ([]MeterEvent, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT seq,event_id,tenant,kind,value,at,workspace,principal,binding FROM tenant_meter_events WHERE tenant=? AND seq>=? ORDER BY seq LIMIT ?`,
		tenant, next, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	events := make([]MeterEvent, 0, limit)
	for rows.Next() {
		var event MeterEvent
		var at int64
		if err := rows.Scan(&event.Seq, &event.ID, &event.Tenant, &event.Kind, &event.Value, &at,
			&event.Workspace, &event.Principal, &event.Binding); err != nil {
			return nil, err
		}
		event.At = time.UnixMilli(at).UTC()
		events = append(events, event)
	}
	return events, rows.Err()
}

// JSONLExporter is the generic local/export-pipeline adapter. Each call
// writes complete newline-delimited JSON records under one mutex.
type JSONLExporter struct {
	Writer io.Writer
	mu     sync.Mutex
}

// Export implements BillingExporter.
func (e *JSONLExporter) Export(ctx context.Context, events []MeterEvent) error {
	if e == nil || e.Writer == nil {
		return &Error{Code: CodeBadRequest}
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	writer := bufio.NewWriter(e.Writer)
	encoder := json.NewEncoder(writer)
	for _, event := range events {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := encoder.Encode(event); err != nil {
			return err
		}
	}
	return writer.Flush()
}

var _ BillingExporter = (*JSONLExporter)(nil)
