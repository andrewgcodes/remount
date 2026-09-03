package budget

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"sort"
	"strings"
	"sync"
	"time"
	"unicode"
	"unicode/utf8"
)

const (
	defaultReservationTTL    = 5 * time.Minute
	maximumReservationTTL    = time.Hour
	defaultRetention         = 31 * 24 * time.Hour
	maximumRetention         = 365 * 24 * time.Hour
	defaultMaxBudgets        = 4096
	maximumBudgets           = 64 << 10
	defaultMaxReservations   = 64 << 10
	maximumReservations      = 1 << 20
	maximumBudgetsPerRequest = 64
	maximumIDBytes           = 256
	maxInt64                 = int64(^uint64(0) >> 1)
)

// Config bounds an in-memory Manager. Production may implement Store with the
// same semantics inside the control plane's transactional database.
type Config struct {
	Budgets         []Budget
	Pricing         *Catalog
	Now             func() time.Time
	ReservationTTL  time.Duration
	Retention       time.Duration
	MaxBudgets      int
	MaxReservations int
}

// Manager is an in-memory authoritative Store intended for standalone mode,
// tests, and as the executable contract for a durable implementation.
type Manager struct {
	mu sync.Mutex

	cfg          Config
	budgets      map[string]Budget
	reservations map[string]*reservationRecord
	denied       uint64
	unmetered    uint64
	incomplete   uint64
	expired      uint64
}

var _ Store = (*Manager)(nil)
var _ AdminStore = (*Manager)(nil)

type reservationRecord struct {
	reservation Reservation
	request     ReserveRequest
	settlement  *Settlement
	settleInput SettleRequest
	terminalAt  time.Time
}

type charge struct {
	requests   int64
	tokens     int64
	cost       int64
	active     bool
	unmetered  bool
	incomplete bool
}

// NewManager returns a bounded in-memory budget authority.
func NewManager(cfg Config) (*Manager, error) {
	if cfg.Pricing == nil {
		catalog, err := DefaultCatalog()
		if err != nil {
			return nil, err
		}
		cfg.Pricing = catalog
	}
	if cfg.Now == nil {
		cfg.Now = time.Now
	}
	if cfg.ReservationTTL == 0 {
		cfg.ReservationTTL = defaultReservationTTL
	}
	if cfg.Retention == 0 {
		cfg.Retention = defaultRetention
	}
	if cfg.MaxBudgets == 0 {
		cfg.MaxBudgets = defaultMaxBudgets
	}
	if cfg.MaxReservations == 0 {
		cfg.MaxReservations = defaultMaxReservations
	}
	if cfg.ReservationTTL < time.Second || cfg.ReservationTTL > maximumReservationTTL || cfg.Retention < 30*24*time.Hour || cfg.Retention > maximumRetention || cfg.MaxBudgets < 1 || cfg.MaxBudgets > maximumBudgets || cfg.MaxReservations < 1 || cfg.MaxReservations > maximumReservations {
		return nil, errors.New("budget: invalid authority bounds")
	}
	if len(cfg.Budgets) > cfg.MaxBudgets {
		return nil, ErrCapacity
	}
	budgets := make(map[string]Budget, len(cfg.Budgets))
	for _, value := range cfg.Budgets {
		if err := validateBudget(value); err != nil {
			return nil, err
		}
		key := budgetKey(value.Tenant, value.ID)
		if _, exists := budgets[key]; exists {
			return nil, errors.New("budget: duplicate budget id")
		}
		budgets[key] = value
	}
	cfg.Budgets = nil
	return &Manager{cfg: cfg, budgets: budgets, reservations: make(map[string]*reservationRecord)}, nil
}

// PutBudget admits a new immutable budget definition. Exact replay is a no-op.
func (m *Manager) PutBudget(ctx context.Context, value Budget) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := validateBudget(value); err != nil {
		return err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	key := budgetKey(value.Tenant, value.ID)
	if current, ok := m.budgets[key]; ok {
		if current == value {
			return nil
		}
		return ErrConflict
	}
	for _, record := range m.reservations {
		if record.reservation.Subject.Tenant == value.Tenant && contains(record.reservation.BudgetIDs, value.ID) {
			return ErrConflict
		}
	}
	if len(m.budgets) >= m.cfg.MaxBudgets {
		return ErrCapacity
	}
	m.budgets[key] = value
	return nil
}

// DeleteBudget stops new admissions against a budget. Retained reservations
// remain settleable, and its ID cannot be reused until they are collected.
func (m *Manager) DeleteBudget(ctx context.Context, tenant, id string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if !safeID(tenant) || !safeID(id) {
		return errors.New("budget: invalid budget id")
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.budgets, budgetKey(tenant, id))
	return nil
}

// Budgets returns stable ID-ordered value copies.
func (m *Manager) Budgets(ctx context.Context) ([]Budget, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	values := make([]Budget, 0, len(m.budgets))
	for _, value := range m.budgets {
		values = append(values, value)
	}
	sort.Slice(values, func(i, j int) bool { return values[i].ID < values[j].ID })
	return values, nil
}

// Budget returns one immutable budget definition as a value copy.
func (m *Manager) Budget(tenant, id string) (Budget, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	value, ok := m.budgets[budgetKey(tenant, id)]
	return value, ok
}

// RestoreBudget restores or removes one definition after a failed durable
// commit. Unlike PutBudget it may restore an ID referenced by retained
// reservations; callers must serialize it with other Store mutations.
func (m *Manager) RestoreBudget(tenant, id string, value *Budget) error {
	if !safeID(tenant) || !safeID(id) {
		return errors.New("budget: invalid durable budget id")
	}
	key := budgetKey(tenant, id)
	m.mu.Lock()
	defer m.mu.Unlock()
	if value == nil {
		delete(m.budgets, key)
		return nil
	}
	if value.ID != id || value.Tenant != tenant {
		return errors.New("budget: inconsistent durable budget id")
	}
	if err := validateBudget(*value); err != nil {
		return err
	}
	if _, exists := m.budgets[key]; !exists && len(m.budgets) >= m.cfg.MaxBudgets {
		return ErrCapacity
	}
	m.budgets[key] = *value
	return nil
}

// Reserve atomically admits one request against all matching attachments.
// Exact retries return the original reservation even when capacity is full.
func (m *Manager) Reserve(ctx context.Context, request ReserveRequest) (Reservation, error) {
	if err := ctx.Err(); err != nil {
		return Reservation{}, err
	}
	if err := validateReserveRequest(request); err != nil {
		return Reservation{}, err
	}
	now := m.cfg.Now().UTC()
	id := reservationID(request.Subject.Tenant, request.Key)

	m.mu.Lock()
	defer m.mu.Unlock()
	if record := m.reservations[id]; record != nil {
		if !sameReserveRequest(record.request, request) {
			return Reservation{}, ErrConflict
		}
		return cloneReservation(record.reservation), nil
	}

	matched := m.matchingBudgetsLocked(request.Subject)
	if len(matched) > maximumBudgetsPerRequest {
		return Reservation{}, ErrCapacity
	}
	reservedTokens := request.InputTokens + request.MaxOutputTokens
	if !request.Metered {
		reservedTokens = 0
	}
	reservedCost, costKnown := int64(0), false
	if request.Metered {
		reservedCost, costKnown = m.cfg.Pricing.Estimate(request.Subject.Tenant, request.Provider, request.Model, request.InputTokens, request.MaxOutputTokens)
	}
	unmeteredReason := ""
	if !request.Metered {
		unmeteredReason = "provider_unknown"
	} else if !costKnown {
		unmeteredReason = "price_unavailable"
	}
	if len(matched) == 0 {
		untrackedSubject := request.Subject
		untrackedSubject.Bindings = append([]string(nil), request.Subject.Bindings...)
		return Reservation{
			Key: request.Key, Node: request.Node, Subject: untrackedSubject,
			Provider: request.Provider, Model: request.Model, ReservedTokens: reservedTokens,
			ReservedEstimatedCostMicros: reservedCost, CostKnown: costKnown,
			UnmeteredReason: unmeteredReason, CreatedAt: now,
			ExpiresAt: now.Add(m.cfg.ReservationTTL), State: StateReserved,
		}, nil
	}
	if request.Metered && reservedTokens == 0 {
		var tokenBudgets, costBudgets []string
		for _, budget := range matched {
			if budget.MaxTokens > 0 {
				tokenBudgets = append(tokenBudgets, budget.ID)
			}
			if budget.MaxEstimatedCostMicros > 0 {
				costBudgets = append(costBudgets, budget.ID)
			}
		}
		if len(tokenBudgets) > 0 {
			m.denied++
			return Reservation{}, &DeniedError{BudgetIDs: tokenBudgets, Limit: LimitTokens}
		}
		if len(costBudgets) > 0 {
			m.denied++
			return Reservation{}, &DeniedError{BudgetIDs: costBudgets, Limit: LimitCost}
		}
	}
	if request.Metered && !costKnown {
		var costBudgets []string
		for _, budget := range matched {
			if budget.MaxEstimatedCostMicros > 0 {
				costBudgets = append(costBudgets, budget.ID)
			}
		}
		if len(costBudgets) > 0 {
			m.denied++
			m.unmetered++
			return Reservation{}, &DeniedError{BudgetIDs: costBudgets, Limit: LimitCost, UnmeteredReason: "price_unavailable"}
		}
	}
	if len(m.reservations) >= m.cfg.MaxReservations {
		return Reservation{}, ErrCapacity
	}

	var deniedRequests, deniedTokens, deniedCost []string
	currentByBudget := m.usageForBudgetsLocked(matched, now, UsageQuery{})
	for _, budget := range matched {
		current := currentByBudget[budget.ID]
		if budget.MaxRequests > 0 && exceeds(current.Requests, 1, budget.MaxRequests) {
			deniedRequests = append(deniedRequests, budget.ID)
		}
		if budget.MaxTokens > 0 && exceeds(current.Tokens, reservedTokens, budget.MaxTokens) {
			deniedTokens = append(deniedTokens, budget.ID)
		}
		if budget.MaxEstimatedCostMicros > 0 && costKnown && exceeds(current.EstimatedCostMicros, reservedCost, budget.MaxEstimatedCostMicros) {
			deniedCost = append(deniedCost, budget.ID)
		}
	}
	if len(deniedRequests) > 0 {
		m.denied++
		return Reservation{}, &DeniedError{BudgetIDs: deniedRequests, Limit: LimitRequests}
	}
	if len(deniedTokens) > 0 {
		m.denied++
		return Reservation{}, &DeniedError{BudgetIDs: deniedTokens, Limit: LimitTokens}
	}
	if len(deniedCost) > 0 {
		m.denied++
		return Reservation{}, &DeniedError{BudgetIDs: deniedCost, Limit: LimitCost}
	}

	budgetIDs := make([]string, len(matched))
	for i := range matched {
		budgetIDs[i] = matched[i].ID
	}
	reservationSubject := request.Subject
	reservationSubject.Bindings = append([]string(nil), request.Subject.Bindings...)
	reservation := Reservation{
		ID: id, Tracked: true, Key: request.Key, Node: request.Node, Subject: reservationSubject,
		Provider: request.Provider, Model: request.Model, BudgetIDs: budgetIDs,
		ReservedTokens: reservedTokens, ReservedEstimatedCostMicros: reservedCost,
		CostKnown: costKnown, UnmeteredReason: unmeteredReason, CreatedAt: now,
		ExpiresAt: now.Add(m.cfg.ReservationTTL), State: StateReserved,
	}
	m.reservations[id] = &reservationRecord{reservation: reservation, request: cloneReserveRequest(request)}
	return cloneReservation(reservation), nil
}

// Settle commits complete usage or an explicit conservative/request-only
// result. Exact retries return the original settlement.
func (m *Manager) Settle(ctx context.Context, request SettleRequest) (Settlement, error) {
	if err := ctx.Err(); err != nil {
		return Settlement{}, err
	}
	if err := validateSettleRequest(request); err != nil {
		return Settlement{}, err
	}
	now := m.cfg.Now().UTC()
	m.mu.Lock()
	defer m.mu.Unlock()
	record := m.reservations[request.ReservationID]
	if record == nil {
		return Settlement{}, ErrNotFound
	}
	if record.reservation.State == StateExpired {
		return Settlement{}, ErrExpired
	}
	if record.settlement != nil {
		if record.settleInput != request {
			return Settlement{}, ErrConflict
		}
		return cloneSettlement(*record.settlement), nil
	}
	if !now.Before(record.reservation.ExpiresAt) {
		m.expireRecordLocked(record, now)
		return Settlement{}, ErrExpired
	}
	if (request.Mode == SettlementRequestOnly && record.request.Metered) || (request.Mode == SettlementMetered && !record.request.Metered) {
		return Settlement{}, errors.New("budget: settlement mode does not match provider metering")
	}

	settlement := Settlement{Reservation: cloneReservation(record.reservation), Mode: request.Mode}
	settlement.State = StateSettled
	switch request.Mode {
	case SettlementMetered:
		settlement.InputTokens = request.InputTokens
		settlement.OutputTokens = request.OutputTokens
		settlement.Tokens = request.InputTokens + request.OutputTokens
		settlement.EstimatedCostMicros, settlement.CostKnown = m.cfg.Pricing.Estimate(record.request.Subject.Tenant, record.request.Provider, record.request.Model, request.InputTokens, request.OutputTokens)
		settlement.EstimatedCostUnavailable = !settlement.CostKnown
		settlement.UsageExceededReservation = settlement.Tokens > record.reservation.ReservedTokens || (settlement.CostKnown && record.reservation.CostKnown && settlement.EstimatedCostMicros > record.reservation.ReservedEstimatedCostMicros)
		if settlement.EstimatedCostUnavailable {
			m.unmetered++
		}
	case SettlementRequestOnly:
		settlement.EstimatedCostUnavailable = true
		m.unmetered++
	case SettlementIncomplete:
		settlement.Tokens = record.reservation.ReservedTokens
		settlement.EstimatedCostMicros = record.reservation.ReservedEstimatedCostMicros
		settlement.EstimatedCostUnavailable = !record.reservation.CostKnown
		m.incomplete++
	}
	record.reservation.State = StateSettled
	settlement.Reservation.State = StateSettled
	record.settleInput = request
	record.settlement = &settlement
	record.terminalAt = now
	return cloneSettlement(settlement), nil
}

// ExpireNode conservatively settles every active reservation owned by a dead
// node at its reserved maximum. Repeated calls are idempotent.
func (m *Manager) ExpireNode(ctx context.Context, node string, at time.Time) (ExpiryResult, error) {
	if err := ctx.Err(); err != nil {
		return ExpiryResult{}, err
	}
	if !safeID(node) {
		return ExpiryResult{}, errors.New("budget: invalid node id")
	}
	if at.IsZero() {
		at = m.cfg.Now()
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	ids := m.expireLocked(at.UTC(), node)
	collected := m.collectLocked(at.UTC())
	return ExpiryResult{ReservationIDs: ids, CollectedIDs: collected}, nil
}

// Sweep conservatively settles expired reservations and collects terminal
// records outside the retention window.
func (m *Manager) Sweep(ctx context.Context, at time.Time) (ExpiryResult, error) {
	if err := ctx.Err(); err != nil {
		return ExpiryResult{}, err
	}
	if at.IsZero() {
		at = m.cfg.Now()
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	ids := m.expireLocked(at.UTC(), "")
	collected := m.collectLocked(at.UTC())
	return ExpiryResult{ReservationIDs: ids, CollectedIDs: collected}, nil
}

// Usage returns stable budget-id ordered rolling counters.
func (m *Manager) Usage(ctx context.Context, query UsageQuery) ([]Usage, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if query.Window != "" {
		if _, ok := windowDuration(query.Window); !ok {
			return nil, errors.New("budget: invalid usage window")
		}
	}
	if query.At.IsZero() {
		query.At = m.cfg.Now()
	}
	now := query.At.UTC()
	m.mu.Lock()
	defer m.mu.Unlock()
	selected := make([]Budget, 0, len(m.budgets))
	for _, value := range m.budgets {
		if !queryMatchesBudget(query, value) {
			continue
		}
		selected = append(selected, value)
	}
	byBudget := m.usageForBudgetsLocked(selected, now, query)
	results := make([]Usage, 0, len(selected))
	for _, value := range selected {
		results = append(results, byBudget[value.ID])
	}
	sort.Slice(results, func(i, j int) bool { return results[i].BudgetID < results[j].BudgetID })
	return results, nil
}

// Stats returns bounded-state occupancy and monotonic exceptional outcomes.
func (m *Manager) Stats() Stats {
	m.mu.Lock()
	defer m.mu.Unlock()
	active := 0
	for _, record := range m.reservations {
		if record.reservation.State == StateReserved {
			active++
		}
	}
	return Stats{
		Budgets: len(m.budgets), MaxBudgets: m.cfg.MaxBudgets,
		Reservations: len(m.reservations), MaxReservations: m.cfg.MaxReservations,
		Active: active, Denied: m.denied, Unmetered: m.unmetered,
		Incomplete: m.incomplete, Expired: m.expired,
	}
}

// Record returns a deep value copy of one retained reservation record.
func (m *Manager) Record(id string) (Record, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	record := m.reservations[id]
	if record == nil {
		return Record{}, false
	}
	return exportRecord(record), true
}

// Records returns deep copies of every retained record keyed by durable ID.
func (m *Manager) Records() map[string]Record {
	m.mu.Lock()
	defer m.mu.Unlock()
	result := make(map[string]Record, len(m.reservations))
	for id, record := range m.reservations {
		result[id] = exportRecord(record)
	}
	return result
}

// RestoreRecord replaces one retained record from durable control-plane
// state. A nil record removes id and is used to roll back a failed first
// insert. Callers serialize this with Store mutations.
func (m *Manager) RestoreRecord(id string, value *Record) error {
	if !safeID(id) {
		return errors.New("budget: invalid durable reservation id")
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if value == nil {
		delete(m.reservations, id)
		return nil
	}
	record, err := importRecord(*value)
	if err != nil || record.reservation.ID != id {
		return errors.New("budget: invalid durable reservation record")
	}
	if m.reservations[id] == nil && len(m.reservations) >= m.cfg.MaxReservations {
		return ErrCapacity
	}
	m.reservations[id] = record
	return nil
}

// ReservationID returns the deterministic durable ID for one tenant-scoped
// idempotency key.
func ReservationID(tenant, key string) string { return reservationID(tenant, key) }

func exportRecord(record *reservationRecord) Record {
	result := Record{
		Request: cloneReserveRequest(record.request), Reservation: cloneReservation(record.reservation),
		SettleInput: record.settleInput, TerminalAt: record.terminalAt,
	}
	if record.settlement != nil {
		settlement := cloneSettlement(*record.settlement)
		result.Settlement = &settlement
	}
	return result
}

func importRecord(value Record) (*reservationRecord, error) {
	if err := validateReserveRequest(value.Request); err != nil {
		return nil, err
	}
	if value.Reservation.ID == "" || reservationID(value.Request.Subject.Tenant, value.Request.Key) != value.Reservation.ID ||
		value.Reservation.Key != value.Request.Key || value.Reservation.Node != value.Request.Node || !sameSubject(value.Reservation.Subject, value.Request.Subject) ||
		value.Reservation.Provider != value.Request.Provider || value.Reservation.Model != value.Request.Model || !value.Reservation.Tracked ||
		value.Reservation.CreatedAt.IsZero() || value.Reservation.ExpiresAt.Before(value.Reservation.CreatedAt) {
		return nil, errors.New("budget: inconsistent durable reservation")
	}
	record := &reservationRecord{request: cloneReserveRequest(value.Request), reservation: cloneReservation(value.Reservation), settleInput: value.SettleInput, terminalAt: value.TerminalAt}
	if value.Settlement == nil {
		if value.Reservation.State != StateReserved || !value.TerminalAt.IsZero() {
			return nil, errors.New("budget: inconsistent active reservation")
		}
		return record, nil
	}
	if err := validateSettleRequest(value.SettleInput); err != nil || value.SettleInput.ReservationID != value.Reservation.ID ||
		(value.Reservation.State != StateSettled && value.Reservation.State != StateExpired) || value.TerminalAt.IsZero() {
		return nil, errors.New("budget: inconsistent durable settlement")
	}
	settlement := cloneSettlement(*value.Settlement)
	if settlement.ID != value.Reservation.ID || settlement.State != value.Reservation.State || settlement.Mode != value.SettleInput.Mode {
		return nil, errors.New("budget: inconsistent durable settlement")
	}
	record.settlement = &settlement
	return record, nil
}

func (m *Manager) matchingBudgetsLocked(subject Subject) []Budget {
	matched := make([]Budget, 0, 4)
	for _, value := range m.budgets {
		if budgetMatches(value, subject) {
			matched = append(matched, value)
		}
	}
	sort.Slice(matched, func(i, j int) bool { return matched[i].ID < matched[j].ID })
	return matched
}

func (m *Manager) usageForBudgetsLocked(values []Budget, now time.Time, query UsageQuery) map[string]Usage {
	results := make(map[string]Usage, len(values))
	for _, value := range values {
		duration, _ := windowDuration(value.Window)
		results[value.ID] = Usage{BudgetID: value.ID, Window: value.Window, Since: now.Add(-duration)}
	}
	for _, record := range m.reservations {
		if !queryMatchesSubject(query, record.reservation.Subject) {
			continue
		}
		entry := recordCharge(record)
		for _, id := range record.reservation.BudgetIDs {
			result, ok := results[id]
			if !ok || record.reservation.CreatedAt.Before(result.Since) {
				continue
			}
			result.Requests = saturatingAdd(result.Requests, entry.requests)
			result.Tokens = saturatingAdd(result.Tokens, entry.tokens)
			result.EstimatedCostMicros = saturatingAdd(result.EstimatedCostMicros, entry.cost)
			if entry.active {
				result.ActiveReservations++
			}
			if entry.unmetered {
				result.UnmeteredRequests++
			}
			if entry.incomplete {
				result.IncompleteRequests++
			}
			results[id] = result
		}
	}
	return results
}

func recordCharge(record *reservationRecord) charge {
	if record.settlement == nil {
		return charge{requests: 1, tokens: record.reservation.ReservedTokens, cost: record.reservation.ReservedEstimatedCostMicros, active: record.reservation.State == StateReserved, incomplete: record.reservation.State == StateExpired}
	}
	settlement := record.settlement
	return charge{
		requests: 1, tokens: settlement.Tokens, cost: settlement.EstimatedCostMicros,
		unmetered:  settlement.Mode == SettlementRequestOnly || settlement.EstimatedCostUnavailable,
		incomplete: settlement.Mode == SettlementIncomplete,
	}
}

func (m *Manager) expireLocked(now time.Time, node string) []string {
	var ids []string
	for _, record := range m.reservations {
		if record.reservation.State != StateReserved {
			continue
		}
		if node != "" && record.reservation.Node != node {
			continue
		}
		if node != "" && record.reservation.CreatedAt.After(now) {
			continue
		}
		if node == "" && now.Before(record.reservation.ExpiresAt) {
			continue
		}
		m.expireRecordLocked(record, now)
		ids = append(ids, record.reservation.ID)
	}
	sort.Strings(ids)
	return ids
}

func (m *Manager) expireRecordLocked(record *reservationRecord, now time.Time) {
	if record.reservation.State != StateReserved {
		return
	}
	settlement := Settlement{
		Reservation: cloneReservation(record.reservation), Mode: SettlementIncomplete,
		Tokens:                   record.reservation.ReservedTokens,
		EstimatedCostMicros:      record.reservation.ReservedEstimatedCostMicros,
		EstimatedCostUnavailable: !record.reservation.CostKnown,
	}
	settlement.State = StateExpired
	record.reservation.State = StateExpired
	record.settlement = &settlement
	record.settleInput = SettleRequest{ReservationID: record.reservation.ID, Mode: SettlementIncomplete}
	record.terminalAt = now
	m.expired++
	m.incomplete++
}

func (m *Manager) collectLocked(now time.Time) []string {
	cutoff := now.Add(-m.cfg.Retention)
	var ids []string
	for id, record := range m.reservations {
		if record.reservation.State != StateReserved && record.terminalAt.Before(cutoff) {
			delete(m.reservations, id)
			ids = append(ids, id)
		}
	}
	sort.Strings(ids)
	return ids
}

func validateBudget(value Budget) error {
	if !safeID(value.ID) || !safeID(value.Tenant) || !safeID(value.AttachID) {
		return errors.New("budget: invalid budget identity")
	}
	if _, ok := windowDuration(value.Window); !ok {
		return errors.New("budget: invalid budget window")
	}
	switch value.AttachTo {
	case AttachTenant:
		if value.AttachID != value.Tenant {
			return errors.New("budget: tenant attachment must target its tenant")
		}
	case AttachWorkspace, AttachPrincipal, AttachBinding:
	default:
		return errors.New("budget: invalid attachment kind")
	}
	if value.MaxRequests < 0 || value.MaxTokens < 0 || value.MaxTokens > MaxTokenCount || value.MaxEstimatedCostMicros < 0 || value.MaxRequests == 0 && value.MaxTokens == 0 && value.MaxEstimatedCostMicros == 0 {
		return errors.New("budget: invalid limits")
	}
	return nil
}

func validateReserveRequest(request ReserveRequest) error {
	if !safeID(request.Key) || !safeID(request.Node) || !safeID(request.Subject.Tenant) || !optionalID(request.Subject.Workspace) || !optionalID(request.Subject.Principal) || !optionalID(request.Subject.Binding) || !safeID(request.Provider) || request.Metered && !safeID(request.Model) || !request.Metered && !optionalID(request.Model) {
		return errors.New("budget: invalid reservation identity")
	}
	if request.InputTokens < 0 || request.MaxOutputTokens < 0 || request.InputTokens > MaxTokenCount || request.MaxOutputTokens > MaxTokenCount || request.InputTokens > MaxTokenCount-request.MaxOutputTokens {
		return errors.New("budget: invalid reservation token bound")
	}
	if len(request.Subject.Bindings) > 32 {
		return errors.New("budget: too many reservation bindings")
	}
	for _, binding := range request.Subject.Bindings {
		if !safeID(binding) {
			return errors.New("budget: invalid reservation binding")
		}
	}
	return nil
}

func validateSettleRequest(request SettleRequest) error {
	if !safeID(request.ReservationID) {
		return errors.New("budget: invalid reservation id")
	}
	switch request.Mode {
	case SettlementMetered:
		if request.InputTokens < 0 || request.OutputTokens < 0 || request.InputTokens > MaxTokenCount || request.OutputTokens > MaxTokenCount || request.InputTokens > MaxTokenCount-request.OutputTokens {
			return errors.New("budget: invalid settled token count")
		}
	case SettlementRequestOnly, SettlementIncomplete:
		if request.InputTokens != 0 || request.OutputTokens != 0 {
			return errors.New("budget: non-metered settlement included tokens")
		}
	default:
		return errors.New("budget: invalid settlement mode")
	}
	return nil
}

func budgetMatches(value Budget, subject Subject) bool {
	if value.Tenant != subject.Tenant {
		return false
	}
	switch value.AttachTo {
	case AttachTenant:
		return value.AttachID == subject.Tenant
	case AttachWorkspace:
		return value.AttachID == subject.Workspace
	case AttachPrincipal:
		return value.AttachID == subject.Principal
	case AttachBinding:
		if value.AttachID == subject.Binding {
			return true
		}
		return contains(subject.Bindings, value.AttachID)
	default:
		return false
	}
}

func queryMatchesBudget(query UsageQuery, value Budget) bool {
	if query.BudgetID != "" && query.BudgetID != value.ID || query.Tenant != "" && query.Tenant != value.Tenant || query.Window != "" && query.Window != value.Window {
		return false
	}
	switch value.AttachTo {
	case AttachWorkspace:
		return query.Workspace == "" || query.Workspace == value.AttachID
	case AttachPrincipal:
		return query.Principal == "" || query.Principal == value.AttachID
	case AttachBinding:
		return query.Binding == "" || query.Binding == value.AttachID
	case AttachTenant:
		return true
	default:
		return false
	}
}

func queryMatchesSubject(query UsageQuery, subject Subject) bool {
	return (query.Tenant == "" || query.Tenant == subject.Tenant) &&
		(query.Workspace == "" || query.Workspace == subject.Workspace) &&
		(query.Principal == "" || query.Principal == subject.Principal) &&
		(query.Binding == "" || query.Binding == subject.Binding || contains(subject.Bindings, query.Binding))
}

func reservationID(tenant, key string) string {
	digest := sha256.Sum256([]byte(tenant + "\x00" + key))
	return "bres_" + hex.EncodeToString(digest[:])
}

func budgetKey(tenant, id string) string { return tenant + "\x00" + id }

func cloneReservation(value Reservation) Reservation {
	value.BudgetIDs = append([]string(nil), value.BudgetIDs...)
	value.Subject.Bindings = append([]string(nil), value.Subject.Bindings...)
	return value
}

func cloneReserveRequest(value ReserveRequest) ReserveRequest {
	value.Subject.Bindings = append([]string(nil), value.Subject.Bindings...)
	return value
}

func sameReserveRequest(a, b ReserveRequest) bool {
	if a.Key != b.Key || a.Node != b.Node || a.Provider != b.Provider || a.Model != b.Model || a.InputTokens != b.InputTokens || a.MaxOutputTokens != b.MaxOutputTokens || a.Metered != b.Metered ||
		a.Subject.Tenant != b.Subject.Tenant || a.Subject.Workspace != b.Subject.Workspace || a.Subject.Generation != b.Subject.Generation || a.Subject.Principal != b.Subject.Principal || a.Subject.Binding != b.Subject.Binding || len(a.Subject.Bindings) != len(b.Subject.Bindings) {
		return false
	}
	for i := range a.Subject.Bindings {
		if a.Subject.Bindings[i] != b.Subject.Bindings[i] {
			return false
		}
	}
	return true
}

func sameSubject(a, b Subject) bool {
	if a.Tenant != b.Tenant || a.Workspace != b.Workspace || a.Generation != b.Generation || a.Principal != b.Principal || a.Binding != b.Binding || len(a.Bindings) != len(b.Bindings) {
		return false
	}
	for i := range a.Bindings {
		if a.Bindings[i] != b.Bindings[i] {
			return false
		}
	}
	return true
}

func cloneSettlement(value Settlement) Settlement {
	value.Reservation = cloneReservation(value.Reservation)
	return value
}

func safeID(value string) bool {
	if value == "" || len(value) > maximumIDBytes || !utf8.ValidString(value) || strings.TrimSpace(value) != value {
		return false
	}
	for _, char := range value {
		if unicode.IsControl(char) {
			return false
		}
	}
	return true
}

func optionalID(value string) bool { return value == "" || safeID(value) }

func contains(values []string, target string) bool {
	for _, value := range values {
		if value == target {
			return true
		}
	}
	return false
}

func exceeds(current, delta, limit int64) bool {
	return delta > limit || current > limit-delta
}

func saturatingAdd(left, right int64) int64 {
	if right > 0 && left > maxInt64-right {
		return maxInt64
	}
	return left + right
}
