package tenant

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"net/url"
	"sort"
	"strings"
	"time"

	"remount.dev/remount/internal/eventlog"
	"remount.dev/remount/internal/ids"
	"remount.dev/remount/internal/metrics"
	"remount.dev/remount/internal/proto"
)

const (
	defaultMaxTenants      = 10_000
	defaultMaxOperations   = 100_000
	defaultMaxMeterEvents  = 1_000_000
	defaultMaxReservations = 1_000_000
	defaultMaxExporters    = 16
	maxPage                = 1_000
)

// Options bound every durable collection owned by Store.
type Options struct {
	Now                      func() time.Time
	MaxTenants               int
	MaxOperations            int
	MaxOperationsPerTenant   int
	MaxMeterEvents           int
	MaxReservationsPerTenant int
	MaxExportersPerTenant    int
	OperationRetention       time.Duration
}

// Store persists tenant policy, quota reservations, meter events and export
// cursors in the control-plane SQLite database.
type Store struct {
	db                     *sql.DB
	log                    *eventlog.Log
	now                    func() time.Time
	maxTenants             int
	maxOperations          int
	maxOperationsPerTenant int
	maxMeterEvents         int
	maxReservations        int
	maxExporters           int
	operationRetention     time.Duration
}

// NewStore creates or migrates tenant tables. db and log must refer to the
// same control-plane SQLite database so rows and canonical events commit in
// one transaction.
func NewStore(db *sql.DB, log *eventlog.Log, options Options) (*Store, error) {
	if db == nil || log == nil {
		return nil, errors.New("tenant: DB and event log are required")
	}
	if options.Now == nil {
		options.Now = time.Now
	}
	if options.MaxTenants == 0 {
		options.MaxTenants = defaultMaxTenants
	}
	if options.MaxOperations == 0 {
		options.MaxOperations = defaultMaxOperations
	}
	if options.MaxOperationsPerTenant == 0 {
		options.MaxOperationsPerTenant = min(options.MaxOperations, 10_000)
	}
	if options.MaxMeterEvents == 0 {
		options.MaxMeterEvents = defaultMaxMeterEvents
	}
	if options.MaxReservationsPerTenant == 0 {
		options.MaxReservationsPerTenant = defaultMaxReservations
	}
	if options.MaxExportersPerTenant == 0 {
		options.MaxExportersPerTenant = defaultMaxExporters
	}
	if options.OperationRetention == 0 {
		options.OperationRetention = 30 * 24 * time.Hour
	}
	if options.MaxTenants < 1 || options.MaxOperations < 1 || options.MaxOperationsPerTenant < 1 ||
		options.MaxOperationsPerTenant > options.MaxOperations || options.MaxMeterEvents < 1 || options.MaxReservationsPerTenant < 1 ||
		options.MaxExportersPerTenant < 1 || options.OperationRetention < time.Hour {
		return nil, errors.New("tenant: invalid store bounds")
	}
	if err := eventlog.CreateOutbox(db); err != nil {
		return nil, err
	}
	if _, err := db.Exec(`
CREATE TABLE IF NOT EXISTS tenants (
  id TEXT PRIMARY KEY,
  state TEXT NOT NULL,
  revision INTEGER NOT NULL,
  policy BLOB NOT NULL,
  created_at INTEGER NOT NULL,
  updated_at INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS tenant_operations (
  tenant TEXT NOT NULL,
  operation_id TEXT NOT NULL,
  fingerprint TEXT NOT NULL,
  outcome TEXT NOT NULL,
  result BLOB,
  created_at INTEGER NOT NULL,
  PRIMARY KEY(tenant, operation_id)
);
CREATE INDEX IF NOT EXISTS tenant_operations_age ON tenant_operations(created_at);
CREATE TABLE IF NOT EXISTS tenant_reservations (
  tenant TEXT NOT NULL,
  resource TEXT NOT NULL,
  resource_id TEXT NOT NULL,
  amount INTEGER NOT NULL,
  created_at INTEGER NOT NULL,
  PRIMARY KEY(tenant, resource, resource_id),
  FOREIGN KEY(tenant) REFERENCES tenants(id)
);
CREATE INDEX IF NOT EXISTS tenant_reservations_usage ON tenant_reservations(tenant, resource);
CREATE TABLE IF NOT EXISTS tenant_meter_events (
  seq INTEGER PRIMARY KEY AUTOINCREMENT,
  tenant TEXT NOT NULL,
  event_id TEXT NOT NULL,
  fingerprint TEXT NOT NULL,
  kind TEXT NOT NULL,
  value INTEGER NOT NULL,
  at INTEGER NOT NULL,
  workspace TEXT NOT NULL,
  principal TEXT NOT NULL,
  binding TEXT NOT NULL,
  UNIQUE(tenant, event_id),
  FOREIGN KEY(tenant) REFERENCES tenants(id)
);
CREATE INDEX IF NOT EXISTS tenant_meter_order ON tenant_meter_events(tenant, seq);
CREATE INDEX IF NOT EXISTS tenant_meter_age ON tenant_meter_events(tenant, at);
CREATE TABLE IF NOT EXISTS tenant_meter_cursors (
  tenant TEXT NOT NULL,
  exporter TEXT NOT NULL,
  next_seq INTEGER NOT NULL,
  updated_at INTEGER NOT NULL,
  PRIMARY KEY(tenant, exporter)
);`); err != nil {
		return nil, err
	}
	return &Store{db: db, log: log, now: options.Now, maxTenants: options.MaxTenants,
		maxOperations: options.MaxOperations, maxOperationsPerTenant: options.MaxOperationsPerTenant, maxMeterEvents: options.MaxMeterEvents,
		maxReservations: options.MaxReservationsPerTenant, maxExporters: options.MaxExportersPerTenant,
		operationRetention: options.OperationRetention}, nil
}

// Create atomically creates a tenant and tenant.created event.
func (s *Store) Create(ctx context.Context, id string, policy Policy, mutation Mutation) (Tenant, error) {
	policy = canonicalPolicy(policy)
	if err := validateTenantID(id); err != nil || validatePolicy(policy) != nil || validateMutation(mutation) != nil {
		return Tenant{}, &Error{Code: CodeBadRequest}
	}
	fingerprint := digest("create", id, policy, mutation.Actor)
	now := s.now().UTC()
	var result Tenant
	var outcomeErr error
	err := s.log.Transact(ctx, s.db, func(tx *eventlog.Tx) error {
		if replayed, replayErr := s.replayTenantOperation(ctx, tx, id, mutation.OperationID, fingerprint, &result); replayed || replayErr != nil {
			outcomeErr = replayErr
			return nil
		}
		if err := s.ensureOperationCapacity(ctx, tx, id, now); err != nil {
			return err
		}
		var existing int
		if err := tx.QueryRowContext(ctx, `SELECT count(*) FROM tenants WHERE id=?`, id).Scan(&existing); err != nil {
			return err
		}
		if existing != 0 {
			outcomeErr = &Error{Code: CodeConflict}
			return s.putOperation(ctx, tx, id, mutation.OperationID, fingerprint, "conflict", nil, now)
		}
		var count int
		if err := tx.QueryRowContext(ctx, `SELECT count(*) FROM tenants`).Scan(&count); err != nil {
			return err
		}
		if count >= s.maxTenants {
			outcomeErr = &Error{Code: CodeResourceExhausted}
			return s.putOperation(ctx, tx, id, mutation.OperationID, fingerprint, "resource_exhausted", nil, now)
		}
		encoded, _ := json.Marshal(policy)
		result = Tenant{ID: id, State: StateActive, Revision: 1, Policy: clonePolicy(policy), CreatedAt: now, UpdatedAt: now}
		if _, err := tx.ExecContext(ctx, `INSERT INTO tenants(id,state,revision,policy,created_at,updated_at) VALUES(?,?,?,?,?,?)`,
			id, result.State, result.Revision, encoded, millis(now), millis(now)); err != nil {
			if isUnique(err) {
				outcomeErr = &Error{Code: CodeConflict}
				return s.putOperation(ctx, tx, id, mutation.OperationID, fingerprint, "conflict", nil, now)
			}
			return err
		}
		if err := s.putOperation(ctx, tx, id, mutation.OperationID, fingerprint, "ok", result, now); err != nil {
			return err
		}
		return tx.Emit(s.event(now, "tenant.created", id, mutation.Actor, mutation.OperationID, map[string]any{"revision": 1}))
	})
	if err = normalizeDrain(err); err != nil {
		return Tenant{}, err
	}
	return result, outcomeErr
}

// UpdatePolicy replaces policy under optimistic revision control.
func (s *Store) UpdatePolicy(ctx context.Context, id string, policy Policy, mutation Mutation) (Tenant, error) {
	policy = canonicalPolicy(policy)
	if validateTenantID(id) != nil || validatePolicy(policy) != nil || validateMutation(mutation) != nil || mutation.ExpectedRevision == 0 {
		return Tenant{}, &Error{Code: CodeBadRequest}
	}
	return s.mutateTenant(ctx, id, mutation, "policy", policy, func(current Tenant, now time.Time) (Tenant, string, error) {
		if current.State == StateDeleted || current.Revision != mutation.ExpectedRevision {
			return Tenant{}, "", &Error{Code: CodeConflict}
		}
		current.Policy, current.Revision, current.UpdatedAt = clonePolicy(policy), current.Revision+1, now
		return current, "tenant.updated", nil
	})
}

// SetState changes administrative state under optimistic revision control.
// Deleted is terminal and requires all quota reservations to have been released.
func (s *Store) SetState(ctx context.Context, id string, state State, mutation Mutation) (Tenant, error) {
	if validateTenantID(id) != nil || validateMutation(mutation) != nil || mutation.ExpectedRevision == 0 ||
		(state != StateActive && state != StateSuspended && state != StateDeleted) {
		return Tenant{}, &Error{Code: CodeBadRequest}
	}
	return s.mutateTenant(ctx, id, mutation, "state", state, func(current Tenant, now time.Time) (Tenant, string, error) {
		if current.State == StateDeleted || current.Revision != mutation.ExpectedRevision {
			return Tenant{}, "", &Error{Code: CodeConflict}
		}
		if current.State == state {
			return current, "", nil
		}
		current.State, current.Revision, current.UpdatedAt = state, current.Revision+1, now
		switch state {
		case StateActive:
			return current, "tenant.activated", nil
		case StateSuspended:
			return current, "tenant.suspended", nil
		default:
			return current, "tenant.deleted", nil
		}
	})
}

func (s *Store) mutateTenant(ctx context.Context, id string, mutation Mutation, action string, argument any,
	apply func(Tenant, time.Time) (Tenant, string, error)) (Tenant, error) {
	fingerprint := digest(action, id, argument, mutation.ExpectedRevision, mutation.Actor)
	now := s.now().UTC()
	var result Tenant
	var outcomeErr error
	err := s.log.Transact(ctx, s.db, func(tx *eventlog.Tx) error {
		if replayed, replayErr := s.replayTenantOperation(ctx, tx, id, mutation.OperationID, fingerprint, &result); replayed || replayErr != nil {
			outcomeErr = replayErr
			return nil
		}
		if err := s.ensureOperationCapacity(ctx, tx, id, now); err != nil {
			return err
		}
		current, err := getTenantTx(ctx, tx, id)
		if errors.Is(err, sql.ErrNoRows) {
			outcomeErr = &Error{Code: CodeNotFound}
			return s.putOperation(ctx, tx, id, mutation.OperationID, fingerprint, "not_found", nil, now)
		}
		if err != nil {
			return err
		}
		if action == "state" && argument == StateDeleted {
			var active int
			if err := tx.QueryRowContext(ctx, `SELECT count(*) FROM tenant_reservations WHERE tenant=?`, id).Scan(&active); err != nil {
				return err
			}
			if active != 0 {
				outcomeErr = &Error{Code: CodeConflict}
				return s.putOperation(ctx, tx, id, mutation.OperationID, fingerprint, "conflict", nil, now)
			}
		}
		var eventType string
		var applyErr error
		result, eventType, applyErr = apply(current, now)
		if applyErr != nil {
			outcomeErr = applyErr
			return s.putOperation(ctx, tx, id, mutation.OperationID, fingerprint, codeOf(applyErr), nil, now)
		}
		if eventType != "" {
			encoded, _ := json.Marshal(result.Policy)
			if _, err := tx.ExecContext(ctx, `UPDATE tenants SET state=?, revision=?, policy=?, updated_at=? WHERE id=?`,
				result.State, result.Revision, encoded, millis(now), id); err != nil {
				return err
			}
			if err := tx.Emit(s.event(now, eventType, id, mutation.Actor, mutation.OperationID, map[string]any{"revision": result.Revision})); err != nil {
				return err
			}
		}
		return s.putOperation(ctx, tx, id, mutation.OperationID, fingerprint, "ok", result, now)
	})
	if err = normalizeDrain(err); err != nil {
		return Tenant{}, err
	}
	return result, outcomeErr
}

// Get returns one tenant. Deleted tombstones remain addressable to operators.
func (s *Store) Get(ctx context.Context, id string) (Tenant, error) {
	if validateTenantID(id) != nil {
		return Tenant{}, &Error{Code: CodeBadRequest}
	}
	tenant, err := getTenantRow(s.db.QueryRowContext(ctx, `SELECT id,state,revision,policy,created_at,updated_at FROM tenants WHERE id=?`, id))
	if errors.Is(err, sql.ErrNoRows) {
		return Tenant{}, &Error{Code: CodeNotFound}
	}
	return tenant, err
}

// List returns the bounded tenant directory for an explicitly authorized
// global operator. Tenant-scoped callers must use Get with their exact key.
func (s *Store) List(ctx context.Context) ([]Tenant, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT id,state,revision,policy,created_at,updated_at FROM tenants ORDER BY id LIMIT ?`, s.maxTenants+1)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	result := make([]Tenant, 0)
	for rows.Next() {
		if len(result) == s.maxTenants {
			return nil, &Error{Code: CodeResourceExhausted}
		}
		var item Tenant
		var policy []byte
		var created, updated int64
		if err := rows.Scan(&item.ID, &item.State, &item.Revision, &policy, &created, &updated); err != nil {
			return nil, err
		}
		if err := json.Unmarshal(policy, &item.Policy); err != nil {
			return nil, errors.New("tenant: corrupt tenant policy")
		}
		item.Policy = canonicalPolicy(item.Policy)
		item.CreatedAt, item.UpdatedAt = time.UnixMilli(created).UTC(), time.UnixMilli(updated).UTC()
		result = append(result, item)
	}
	return result, rows.Err()
}

// Reserve atomically checks and consumes one quota dimension. It returns true
// only when this call created the durable reservation.
func (s *Store) Reserve(ctx context.Context, admission Admission) (bool, error) {
	return s.Admit(ctx, admission, nil)
}

// Admit reserves quota and runs commit inside the same SQLite/event-outbox
// transaction. Root resource stores use this form so quota, resource state,
// and both events have one durable commit point. commit is not called on an
// idempotent replay or when an equivalent reservation already exists.
func (s *Store) Admit(ctx context.Context, admission Admission, commit func(*eventlog.Tx) error) (bool, error) {
	if validateAdmission(admission) != nil {
		return false, &Error{Code: CodeBadRequest}
	}
	fingerprint := digest("reserve", admission.Resource, admission.ResourceID, admission.Amount, admission.Actor)
	now := s.now().UTC()
	var fresh bool
	var outcomeErr error
	// quotaExceeded pairs the counter with the event exactly once: an
	// idempotent replay of an already-refused admission emits no new event and
	// must not move the counter either.
	var quotaExceeded bool
	err := s.log.Transact(ctx, s.db, func(tx *eventlog.Tx) error {
		var storedFingerprint, outcome string
		var raw []byte
		err := tx.QueryRowContext(ctx, `SELECT fingerprint,outcome,result FROM tenant_operations WHERE tenant=? AND operation_id=?`, admission.Tenant, admission.OperationID).Scan(&storedFingerprint, &outcome, &raw)
		if err == nil {
			if storedFingerprint != fingerprint {
				outcomeErr = &Error{Code: CodeConflict}
			} else if outcome != "ok" {
				var replayed Error
				if len(raw) > 0 && json.Unmarshal(raw, &replayed) == nil {
					outcomeErr = &replayed
				} else {
					outcomeErr = outcomeError(outcome)
				}
			}
			return nil
		}
		if !errors.Is(err, sql.ErrNoRows) {
			return err
		}
		if err := s.ensureOperationCapacity(ctx, tx, admission.Tenant, now); err != nil {
			return err
		}
		current, err := getTenantTx(ctx, tx, admission.Tenant)
		if errors.Is(err, sql.ErrNoRows) {
			outcomeErr = &Error{Code: CodeNotFound}
			return s.putOperation(ctx, tx, admission.Tenant, admission.OperationID, fingerprint, "not_found", nil, now)
		}
		if err != nil {
			return err
		}
		if current.State != StateActive {
			outcomeErr = &Error{Code: CodeConflict}
			return s.putOperation(ctx, tx, admission.Tenant, admission.OperationID, fingerprint, "conflict", nil, now)
		}
		var existing int64
		err = tx.QueryRowContext(ctx, `SELECT amount FROM tenant_reservations WHERE tenant=? AND resource=? AND resource_id=?`,
			admission.Tenant, admission.Resource, admission.ResourceID).Scan(&existing)
		if err == nil {
			if existing != admission.Amount {
				outcomeErr = &Error{Code: CodeConflict}
				return s.putOperation(ctx, tx, admission.Tenant, admission.OperationID, fingerprint, "conflict", nil, now)
			}
			return s.putOperation(ctx, tx, admission.Tenant, admission.OperationID, fingerprint, "ok", nil, now)
		}
		if !errors.Is(err, sql.ErrNoRows) {
			return err
		}
		var reservationCount int
		if err := tx.QueryRowContext(ctx, `SELECT count(*) FROM tenant_reservations WHERE tenant=?`, admission.Tenant).Scan(&reservationCount); err != nil {
			return err
		}
		if reservationCount >= s.maxReservations {
			outcomeErr = &Error{Code: CodeResourceExhausted}
			quotaExceeded = true
			if err := s.putOperation(ctx, tx, admission.Tenant, admission.OperationID, fingerprint, "resource_exhausted", outcomeErr, now); err != nil {
				return err
			}
			return tx.Emit(s.event(now, "quota.exceeded", admission.Tenant, admission.Actor, admission.OperationID,
				map[string]any{"resource": admission.Resource, "reason": "reservation_capacity"}))
		}
		var used int64
		if err := tx.QueryRowContext(ctx, `SELECT coalesce(sum(amount),0) FROM tenant_reservations WHERE tenant=? AND resource=?`,
			admission.Tenant, admission.Resource).Scan(&used); err != nil {
			return err
		}
		limit := current.Policy.Quotas.Limit(admission.Resource)
		if limit > 0 && (admission.Amount > limit || used > limit-admission.Amount) {
			outcomeErr = &Error{Code: CodeResourceExhausted, Resource: admission.Resource, Limit: limit, Used: used}
			quotaExceeded = true
			if err := s.putOperation(ctx, tx, admission.Tenant, admission.OperationID, fingerprint, "resource_exhausted", outcomeErr, now); err != nil {
				return err
			}
			return tx.Emit(s.event(now, "quota.exceeded", admission.Tenant, admission.Actor, admission.OperationID,
				map[string]any{"resource": admission.Resource, "used": used, "requested": admission.Amount, "limit": limit}))
		}
		if _, err := tx.ExecContext(ctx, `INSERT INTO tenant_reservations(tenant,resource,resource_id,amount,created_at) VALUES(?,?,?,?,?)`,
			admission.Tenant, admission.Resource, admission.ResourceID, admission.Amount, millis(now)); err != nil {
			return err
		}
		if commit != nil {
			if err := commit(tx); err != nil {
				return err
			}
		}
		if err := s.putOperation(ctx, tx, admission.Tenant, admission.OperationID, fingerprint, "ok", nil, now); err != nil {
			return err
		}
		fresh = true
		return tx.Emit(s.event(now, "quota.reserved", admission.Tenant, admission.Actor, admission.OperationID,
			map[string]any{"resource": admission.Resource, "resource_id": admission.ResourceID, "amount": admission.Amount}))
	})
	if err = normalizeDrain(err); err != nil {
		return false, err
	}
	// The counter moves only after the refusal is durable, so a metric can
	// never claim a rejection the event log does not also record.
	if quotaExceeded {
		metrics.TenantQuotaExceeded.Inc()
	}
	return fresh, outcomeErr
}

// Release removes one durable quota reservation. Missing reservations are a
// successful no-op, making cleanup naturally retryable.
func (s *Store) Release(ctx context.Context, admission Admission) (bool, error) {
	return s.Retire(ctx, admission, nil)
}

// Retire releases quota and runs commit in the same SQLite/event-outbox
// transaction. commit is called only when a reservation is actually removed.
func (s *Store) Retire(ctx context.Context, admission Admission, commit func(*eventlog.Tx) error) (bool, error) {
	if validateAdmission(admission) != nil {
		return false, &Error{Code: CodeBadRequest}
	}
	fingerprint := digest("release", admission.Resource, admission.ResourceID, admission.Amount, admission.Actor)
	now := s.now().UTC()
	var fresh bool
	var outcomeErr error
	err := s.log.Transact(ctx, s.db, func(tx *eventlog.Tx) error {
		var stored, outcome string
		err := tx.QueryRowContext(ctx, `SELECT fingerprint,outcome FROM tenant_operations WHERE tenant=? AND operation_id=?`, admission.Tenant, admission.OperationID).Scan(&stored, &outcome)
		if err == nil {
			if stored != fingerprint {
				outcomeErr = &Error{Code: CodeConflict}
			} else if outcome != "ok" {
				outcomeErr = outcomeError(outcome)
			}
			return nil
		}
		if !errors.Is(err, sql.ErrNoRows) {
			return err
		}
		if err := s.ensureOperationCapacity(ctx, tx, admission.Tenant, now); err != nil {
			return err
		}
		var state State
		if err := tx.QueryRowContext(ctx, `SELECT state FROM tenants WHERE id=?`, admission.Tenant).Scan(&state); errors.Is(err, sql.ErrNoRows) {
			outcomeErr = &Error{Code: CodeNotFound}
			return s.putOperation(ctx, tx, admission.Tenant, admission.OperationID, fingerprint, "not_found", outcomeErr, now)
		} else if err != nil {
			return err
		} else if state == StateDeleted {
			outcomeErr = &Error{Code: CodeConflict}
			return s.putOperation(ctx, tx, admission.Tenant, admission.OperationID, fingerprint, "conflict", outcomeErr, now)
		}
		result, err := tx.ExecContext(ctx, `DELETE FROM tenant_reservations WHERE tenant=? AND resource=? AND resource_id=? AND amount=?`,
			admission.Tenant, admission.Resource, admission.ResourceID, admission.Amount)
		if err != nil {
			return err
		}
		removed, err := result.RowsAffected()
		if err != nil {
			return err
		}
		if err := s.putOperation(ctx, tx, admission.Tenant, admission.OperationID, fingerprint, "ok", nil, now); err != nil {
			return err
		}
		fresh = removed != 0
		if !fresh {
			return nil
		}
		if commit != nil {
			if err := commit(tx); err != nil {
				return err
			}
		}
		return tx.Emit(s.event(now, "quota.released", admission.Tenant, admission.Actor, admission.OperationID,
			map[string]any{"resource": admission.Resource, "resource_id": admission.ResourceID, "amount": admission.Amount}))
	})
	if err = normalizeDrain(err); err != nil {
		return false, err
	}
	return fresh, outcomeErr
}

// CurrentUsage returns one tenant's quota usage; callers cannot use an empty
// or wildcard tenant to turn this into a cross-tenant query.
func (s *Store) CurrentUsage(ctx context.Context, tenant string) (Usage, error) {
	if validateTenantID(tenant) != nil {
		return Usage{}, &Error{Code: CodeBadRequest}
	}
	var exists int
	if err := s.db.QueryRowContext(ctx, `SELECT count(*) FROM tenants WHERE id=?`, tenant).Scan(&exists); err != nil {
		return Usage{}, err
	}
	if exists == 0 {
		return Usage{}, &Error{Code: CodeNotFound}
	}
	rows, err := s.db.QueryContext(ctx, `SELECT resource,coalesce(sum(amount),0) FROM tenant_reservations WHERE tenant=? GROUP BY resource`, tenant)
	if err != nil {
		return Usage{}, err
	}
	defer rows.Close()
	var usage Usage
	for rows.Next() {
		var resource Resource
		var value int64
		if err := rows.Scan(&resource, &value); err != nil {
			return Usage{}, err
		}
		switch resource {
		case ResourceWorkspace:
			usage.Workspaces = value
		case ResourceNode:
			usage.Nodes = value
		case ResourceArtifactBytes:
			usage.ArtifactBytes = value
		case ResourceActiveSession:
			usage.ActiveSessions = value
		}
	}
	return usage, rows.Err()
}

// RecordMeter commits one immutable meter row and canonical usage event.
// An exact (tenant,event-id) replay returns the original event; changed
// arguments are rejected.
func (s *Store) RecordMeter(ctx context.Context, event MeterEvent) (MeterEvent, error) {
	if event.At.IsZero() {
		event.At = s.now().UTC()
	}
	event.At = event.At.UTC()
	if validateMeterEvent(event) != nil {
		return MeterEvent{}, &Error{Code: CodeBadRequest}
	}
	fingerprint := digest(event.Kind, event.Value, millis(event.At), event.Workspace, event.Principal, event.Binding)
	var result MeterEvent
	err := s.log.Transact(ctx, s.db, func(tx *eventlog.Tx) error {
		stored, lookupErr := getMeterTx(ctx, tx, event.Tenant, event.ID)
		if lookupErr == nil {
			var storedFingerprint string
			if err := tx.QueryRowContext(ctx, `SELECT fingerprint FROM tenant_meter_events WHERE tenant=? AND event_id=?`, event.Tenant, event.ID).Scan(&storedFingerprint); err != nil {
				return err
			}
			if storedFingerprint != fingerprint {
				return &Error{Code: CodeConflict}
			}
			result = stored
			return nil
		}
		if !errors.Is(lookupErr, sql.ErrNoRows) {
			return lookupErr
		}
		var state State
		if err := tx.QueryRowContext(ctx, `SELECT state FROM tenants WHERE id=?`, event.Tenant).Scan(&state); errors.Is(err, sql.ErrNoRows) {
			return &Error{Code: CodeNotFound}
		} else if err != nil {
			return err
		}
		if state == StateDeleted {
			return &Error{Code: CodeConflict}
		}
		var count int
		if err := tx.QueryRowContext(ctx, `SELECT count(*) FROM tenant_meter_events WHERE tenant=?`, event.Tenant).Scan(&count); err != nil {
			return err
		}
		if count >= s.maxMeterEvents {
			return &Error{Code: CodeResourceExhausted}
		}
		res, err := tx.ExecContext(ctx, `INSERT INTO tenant_meter_events(tenant,event_id,fingerprint,kind,value,at,workspace,principal,binding) VALUES(?,?,?,?,?,?,?,?,?)`,
			event.Tenant, event.ID, fingerprint, event.Kind, event.Value, millis(event.At), event.Workspace, event.Principal, event.Binding)
		if err != nil {
			return err
		}
		seq, err := res.LastInsertId()
		if err != nil {
			return err
		}
		event.Seq, result = uint64(seq), event
		canonical := s.event(event.At, string(event.Kind), event.Tenant, event.Principal, event.ID,
			map[string]any{"meter_event_id": event.ID, "value": event.Value, "binding": event.Binding})
		canonical.Workspace = event.Workspace
		return tx.Emit(canonical)
	})
	return result, normalizeDrain(err)
}

// UsageSummary aggregates canonical meter values for exactly one tenant.
func (s *Store) UsageSummary(ctx context.Context, tenant string, from, to time.Time) (map[MeterKind]int64, error) {
	if validateTenantID(tenant) != nil || (!to.IsZero() && !from.IsZero() && to.Before(from)) {
		return nil, &Error{Code: CodeBadRequest}
	}
	fromMillis := -int64(^uint64(0)>>1) - 1
	if !from.IsZero() {
		fromMillis = millis(from)
	}
	toMillis := int64(^uint64(0) >> 1)
	if !to.IsZero() {
		toMillis = millis(to)
	}
	var exists int
	if err := s.db.QueryRowContext(ctx, `SELECT count(*) FROM tenants WHERE id=?`, tenant).Scan(&exists); err != nil {
		return nil, err
	}
	if exists == 0 {
		return nil, &Error{Code: CodeNotFound}
	}
	rows, err := s.db.QueryContext(ctx, `SELECT kind,coalesce(sum(value),0) FROM tenant_meter_events WHERE tenant=? AND at>=? AND at<? GROUP BY kind`,
		tenant, fromMillis, toMillis)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := make(map[MeterKind]int64)
	for rows.Next() {
		var kind MeterKind
		var value int64
		if err := rows.Scan(&kind, &value); err != nil {
			return nil, err
		}
		out[kind] = value
	}
	return out, rows.Err()
}

// PruneMeter removes a bounded page older than before only when every durable
// exporter cursor has advanced beyond it. It never creates a billing gap.
func (s *Store) PruneMeter(ctx context.Context, tenant string, before time.Time, limit int, actor string) (int64, error) {
	if validateTenantID(tenant) != nil || before.IsZero() || limit < 1 || limit > maxPage || strings.TrimSpace(actor) == "" || len(actor) > 256 {
		return 0, &Error{Code: CodeBadRequest}
	}
	var removed int64
	now := s.now().UTC()
	err := s.log.Transact(ctx, s.db, func(tx *eventlog.Tx) error {
		result, err := tx.ExecContext(ctx, `DELETE FROM tenant_meter_events WHERE seq IN (
SELECT seq FROM tenant_meter_events e WHERE tenant=? AND at<? AND
NOT EXISTS (SELECT 1 FROM tenant_meter_cursors c WHERE c.tenant=e.tenant AND c.next_seq<=e.seq)
ORDER BY seq LIMIT ?)`, tenant, millis(before), limit)
		if err != nil {
			return err
		}
		removed, err = result.RowsAffected()
		if err != nil || removed == 0 {
			return err
		}
		return tx.Emit(s.event(now, "usage.pruned", tenant, actor, ids.New("op"), map[string]any{"removed": removed, "before": millis(before)}))
	})
	return removed, normalizeDrain(err)
}

func (s *Store) event(at time.Time, typ, tenant, actor, operation string, payload any) *proto.Event {
	return &proto.Event{EventID: ids.New("ev"), At: millis(at), ReceivedAt: millis(at), Origin: "control", Actor: actor,
		Principal: actor, Tenant: tenant, Stream: "tenant:" + tenant, OperationID: operation, Type: typ, Payload: proto.MustMarshal(payload)}
}

func (s *Store) ensureOperationCapacity(ctx context.Context, tx *eventlog.Tx, tenant string, now time.Time) error {
	result, err := tx.ExecContext(ctx, `DELETE FROM tenant_operations WHERE rowid IN (SELECT rowid FROM tenant_operations WHERE created_at<? ORDER BY created_at LIMIT 256)`,
		millis(now.Add(-s.operationRetention)))
	if err != nil {
		return err
	}
	removed, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if removed > 0 {
		if err := tx.Emit(s.event(now, "tenant.operations_pruned", "*", "retention", ids.New("op"), map[string]any{"removed": removed})); err != nil {
			return err
		}
	}
	var total int
	if err := tx.QueryRowContext(ctx, `SELECT count(*) FROM tenant_operations`).Scan(&total); err != nil {
		return err
	}
	if total >= s.maxOperations {
		return &Error{Code: CodeResourceExhausted}
	}
	var tenantCount int
	if err := tx.QueryRowContext(ctx, `SELECT count(*) FROM tenant_operations WHERE tenant=?`, tenant).Scan(&tenantCount); err != nil {
		return err
	}
	if tenantCount >= s.maxOperationsPerTenant {
		return &Error{Code: CodeResourceExhausted}
	}
	return nil
}

func (s *Store) replayTenantOperation(ctx context.Context, tx *eventlog.Tx, tenant, operation, fingerprint string, result *Tenant) (bool, error) {
	var storedFingerprint, outcome string
	var raw []byte
	err := tx.QueryRowContext(ctx, `SELECT fingerprint,outcome,result FROM tenant_operations WHERE tenant=? AND operation_id=?`, tenant, operation).
		Scan(&storedFingerprint, &outcome, &raw)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	if storedFingerprint != fingerprint {
		return true, &Error{Code: CodeConflict}
	}
	if outcome != "ok" {
		return true, outcomeError(outcome)
	}
	if len(raw) > 0 && json.Unmarshal(raw, result) != nil {
		return true, errors.New("tenant: corrupt operation result")
	}
	return true, nil
}

func (s *Store) putOperation(ctx context.Context, tx *eventlog.Tx, tenant, operation, fingerprint, outcome string, result any, now time.Time) error {
	var raw []byte
	var err error
	if result != nil {
		raw, err = json.Marshal(result)
		if err != nil {
			return err
		}
	}
	_, err = tx.ExecContext(ctx, `INSERT INTO tenant_operations(tenant,operation_id,fingerprint,outcome,result,created_at) VALUES(?,?,?,?,?,?)`,
		tenant, operation, fingerprint, outcome, raw, millis(now))
	return err
}

func getTenantTx(ctx context.Context, tx *eventlog.Tx, id string) (Tenant, error) {
	return getTenantRow(tx.QueryRowContext(ctx, `SELECT id,state,revision,policy,created_at,updated_at FROM tenants WHERE id=?`, id))
}

type scanner interface{ Scan(...any) error }

func getTenantRow(row scanner) (Tenant, error) {
	var tenant Tenant
	var raw []byte
	var created, updated int64
	if err := row.Scan(&tenant.ID, &tenant.State, &tenant.Revision, &raw, &created, &updated); err != nil {
		return Tenant{}, err
	}
	if err := json.Unmarshal(raw, &tenant.Policy); err != nil {
		return Tenant{}, errors.New("tenant: corrupt policy")
	}
	tenant.Policy = canonicalPolicy(tenant.Policy)
	if tenant.Revision == 0 || (tenant.State != StateActive && tenant.State != StateSuspended && tenant.State != StateDeleted) || validatePolicy(tenant.Policy) != nil {
		return Tenant{}, errors.New("tenant: corrupt tenant row")
	}
	tenant.CreatedAt, tenant.UpdatedAt = time.UnixMilli(created).UTC(), time.UnixMilli(updated).UTC()
	return tenant, nil
}

func getMeterTx(ctx context.Context, tx *eventlog.Tx, tenant, id string) (MeterEvent, error) {
	var event MeterEvent
	var at int64
	err := tx.QueryRowContext(ctx, `SELECT seq,event_id,tenant,kind,value,at,workspace,principal,binding FROM tenant_meter_events WHERE tenant=? AND event_id=?`, tenant, id).
		Scan(&event.Seq, &event.ID, &event.Tenant, &event.Kind, &event.Value, &at, &event.Workspace, &event.Principal, &event.Binding)
	event.At = time.UnixMilli(at).UTC()
	return event, err
}

func canonicalPolicy(policy Policy) Policy {
	out := policy
	out.Residency.AllowedRegions = append([]string(nil), policy.Residency.AllowedRegions...)
	sort.Strings(out.Residency.AllowedRegions)
	if len(out.Residency.AllowedRegions) > 0 {
		unique := out.Residency.AllowedRegions[:0]
		for _, value := range out.Residency.AllowedRegions {
			if len(unique) == 0 || unique[len(unique)-1] != value {
				unique = append(unique, value)
			}
		}
		out.Residency.AllowedRegions = unique
	}
	out.Residency.RequiredLabels = make(map[string]string, len(policy.Residency.RequiredLabels))
	for key, value := range policy.Residency.RequiredLabels {
		out.Residency.RequiredLabels[key] = value
	}
	out.OIDC.Scopes = append([]string(nil), policy.OIDC.Scopes...)
	sort.Strings(out.OIDC.Scopes)
	out.OIDC.GroupRoles = make(map[string][]string, len(policy.OIDC.GroupRoles))
	for group, roles := range policy.OIDC.GroupRoles {
		copied := append([]string(nil), roles...)
		sort.Strings(copied)
		out.OIDC.GroupRoles[group] = copied
	}
	return out
}

func clonePolicy(policy Policy) Policy { return canonicalPolicy(policy) }

func digest(parts ...any) string {
	raw, _ := json.Marshal(parts)
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:])
}

func codeOf(err error) string {
	var typed *Error
	if errors.As(err, &typed) {
		return string(typed.Code)
	}
	return "unavailable"
}

func outcomeError(outcome string) error {
	switch Code(outcome) {
	case CodeNotFound, CodeConflict, CodeResourceExhausted, CodeUnavailable:
		return &Error{Code: Code(outcome)}
	default:
		return &Error{Code: CodeUnavailable}
	}
}

func normalizeDrain(err error) error {
	var deferred *eventlog.DrainError
	if errors.As(err, &deferred) {
		return nil
	}
	return err
}

func isUnique(err error) bool {
	return strings.Contains(strings.ToLower(err.Error()), "unique constraint")
}

func millis(t time.Time) int64 { return t.UTC().UnixMilli() }

func validateTenantID(value string) error {
	if !safeIdentifier(value, 128) || value == "*" {
		return errors.New("invalid tenant")
	}
	return nil
}

func validateMutation(m Mutation) error {
	if !safeIdentifier(m.OperationID, 128) || strings.TrimSpace(m.Actor) == "" || len(m.Actor) > 256 {
		return errors.New("invalid mutation")
	}
	return nil
}

func validateAdmission(a Admission) error {
	if validateTenantID(a.Tenant) != nil || validateMutation(Mutation{OperationID: a.OperationID, Actor: a.Actor}) != nil ||
		!validResource(a.Resource) || !safeIdentifier(a.ResourceID, 256) || a.Amount <= 0 {
		return errors.New("invalid admission")
	}
	return nil
}

func validatePolicy(policy Policy) error {
	q := policy.Quotas
	if q.MaxWorkspaces < 0 || q.MaxNodes < 0 || q.MaxArtifactBytes < 0 || q.MaxActiveSessions < 0 {
		return errors.New("negative quota")
	}
	retentions := []time.Duration{policy.Retention.Events, policy.Retention.SessionLogs, policy.Retention.Artifacts, policy.Retention.MeterEvents}
	for _, retention := range retentions {
		if retention < 0 || retention > 10*365*24*time.Hour || (retention > 0 && retention < time.Hour) {
			return errors.New("invalid retention")
		}
	}
	if len(policy.Residency.AllowedRegions) > 64 || len(policy.Residency.RequiredLabels) > 64 {
		return errors.New("residency policy too large")
	}
	for _, region := range policy.Residency.AllowedRegions {
		if !safeIdentifier(region, 128) {
			return errors.New("invalid region")
		}
	}
	for key, value := range policy.Residency.RequiredLabels {
		if !safeIdentifier(key, 128) || !safeIdentifier(value, 256) {
			return errors.New("invalid label")
		}
	}
	if policy.Billing.StripeCustomerID != "" && !safeIdentifier(policy.Billing.StripeCustomerID, 128) {
		return errors.New("invalid billing customer")
	}
	oidc := policy.OIDC
	if oidc.Issuer == "" && oidc.ClientID == "" && len(oidc.GroupRoles) == 0 {
		return nil
	}
	issuer, err := url.Parse(oidc.Issuer)
	if err != nil || issuer.Scheme != "https" || issuer.Host == "" || issuer.User != nil || issuer.RawQuery != "" || issuer.Fragment != "" ||
		!safeIdentifier(oidc.ClientID, 256) || len(oidc.Scopes) > 16 || len(oidc.GroupRoles) == 0 || len(oidc.GroupRoles) > 64 {
		return errors.New("invalid OIDC configuration")
	}
	for _, scope := range oidc.Scopes {
		if !safeIdentifier(scope, 128) {
			return errors.New("invalid OIDC scope")
		}
	}
	for group, roles := range oidc.GroupRoles {
		if strings.TrimSpace(group) != group || group == "" || len(group) > 256 || len(roles) == 0 || len(roles) > 8 {
			return errors.New("invalid OIDC group mapping")
		}
		for _, role := range roles {
			switch role {
			case "operator", "agent", "viewer", "service":
			default:
				return errors.New("invalid OIDC role")
			}
		}
	}
	return nil
}

func validateMeterEvent(event MeterEvent) error {
	if validateTenantID(event.Tenant) != nil || !safeIdentifier(event.ID, 100) || !validMeterKind(event.Kind) || event.Value <= 0 ||
		len(event.Workspace) > 256 || len(event.Principal) > 256 || len(event.Binding) > 256 {
		return errors.New("invalid meter event")
	}
	return nil
}

func safeIdentifier(value string, max int) bool {
	if value == "" || len(value) > max {
		return false
	}
	for _, char := range value {
		if (char >= 'a' && char <= 'z') || (char >= 'A' && char <= 'Z') || (char >= '0' && char <= '9') || strings.ContainsRune("._:@-", char) {
			continue
		}
		return false
	}
	return true
}

func validResource(resource Resource) bool {
	return resource == ResourceWorkspace || resource == ResourceNode || resource == ResourceArtifactBytes || resource == ResourceActiveSession
}

func validMeterKind(kind MeterKind) bool {
	return kind == MeterWorkspaceSeconds || kind == MeterBrokeredRequest || kind == MeterBytesIn || kind == MeterBytesOut || kind == MeterStorageBytes
}

// MatchResidency reports whether scheduler labels satisfy the tenant's
// residency policy. An empty region allow-list permits any region.
func MatchResidency(policy Residency, labels map[string]string) bool {
	if len(policy.AllowedRegions) > 0 {
		region := labels["region"]
		matched := false
		for _, allowed := range policy.AllowedRegions {
			matched = matched || region == allowed
		}
		if !matched {
			return false
		}
	}
	for key, value := range policy.RequiredLabels {
		if labels[key] != value {
			return false
		}
	}
	return true
}
