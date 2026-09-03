// Package budget provides atomic request, token, and estimated-cost admission
// for brokered model calls.
package budget

import (
	"context"
	"errors"
	"fmt"
	"time"
)

// MaxTokenCount is the closed upper bound accepted by the authority. Brokers
// use it when a provider request omits a finite output bound or references
// external media whose token cost cannot be derived before release.
const MaxTokenCount int64 = 1_000_000_000_000

// Window is a rolling usage interval supported by budget policy.
type Window string

const (
	// WindowHour is a rolling one-hour interval.
	WindowHour Window = "1h"
	// WindowDay is a rolling one-day interval.
	WindowDay Window = "1d"
	// Window30Days is a rolling thirty-day interval.
	Window30Days Window = "30d"
)

// AttachmentKind identifies the subject dimension a budget constrains.
type AttachmentKind string

const (
	// AttachTenant applies a budget to its whole tenant.
	AttachTenant AttachmentKind = "tenant"
	// AttachWorkspace applies a budget to one workspace.
	AttachWorkspace AttachmentKind = "workspace"
	// AttachPrincipal applies a budget to one principal.
	AttachPrincipal AttachmentKind = "principal"
	// AttachBinding applies a budget to one credential binding.
	AttachBinding AttachmentKind = "binding"
)

// SettlementMode describes how trustworthy the terminal token count is.
type SettlementMode string

const (
	// SettlementMetered carries a complete provider-reported usage result.
	SettlementMetered SettlementMode = "metered"
	// SettlementRequestOnly is used for providers with no supported usage
	// format. Only the request reservation remains charged.
	SettlementRequestOnly SettlementMode = "request_only"
	// SettlementIncomplete means a supported response could not be observed to
	// completion. Reserved tokens and cost remain charged conservatively.
	SettlementIncomplete SettlementMode = "incomplete"
)

// ReservationState is the terminal state of a reservation.
type ReservationState string

const (
	// StateReserved means upstream work may consume the reserved allowance.
	StateReserved ReservationState = "reserved"
	// StateSettled means terminal accounting replaced the reservation.
	StateSettled ReservationState = "settled"
	// StateExpired means ambiguity conservatively consumed the reservation.
	StateExpired ReservationState = "expired"
)

// LimitKind identifies the budget dimension that refused admission.
type LimitKind string

const (
	// LimitRequests identifies a request-count refusal.
	LimitRequests LimitKind = "requests"
	// LimitTokens identifies a token-count refusal.
	LimitTokens LimitKind = "tokens"
	// LimitCost identifies an estimated-cost refusal.
	LimitCost LimitKind = "estimated_cost_micros"
)

var (
	// ErrBudgetExceeded matches a typed DeniedError.
	ErrBudgetExceeded = errors.New("budget exceeded")
	// ErrConflict means an idempotency key or settlement was reused with
	// different arguments.
	ErrConflict = errors.New("budget idempotency conflict")
	// ErrCapacity means bounded authority state has no room for new work.
	ErrCapacity = errors.New("budget authority capacity exhausted")
	// ErrNotFound means a reservation is unknown or no longer retained.
	ErrNotFound = errors.New("budget reservation not found")
	// ErrExpired means settlement arrived after conservative expiry committed.
	ErrExpired = errors.New("budget reservation expired")
)

// Subject identifies every supported budget attachment for one request.
type Subject struct {
	Tenant     string
	Workspace  string
	Generation uint64
	Principal  string
	Binding    string
	Bindings   []string
}

// Budget is an attached rolling-window policy. A zero limit disables that
// dimension. Cost is explicitly estimated rather than a billing guarantee.
type Budget struct {
	ID                     string
	Tenant                 string
	AttachTo               AttachmentKind
	AttachID               string
	Window                 Window
	MaxRequests            int64
	MaxTokens              int64
	MaxEstimatedCostMicros int64
}

// ReserveRequest describes the worst-case usage reserved before credential
// substitution and upstream release. Key must be stable across request retries.
type ReserveRequest struct {
	Key             string
	Node            string
	Subject         Subject
	Provider        string
	Model           string
	InputTokens     int64
	MaxOutputTokens int64
	Metered         bool
}

// Reservation is the value-only admission result safe to pass to a node.
type Reservation struct {
	ID                          string
	Tracked                     bool
	Key                         string
	Node                        string
	Subject                     Subject
	Provider                    string
	Model                       string
	BudgetIDs                   []string
	ReservedTokens              int64
	ReservedEstimatedCostMicros int64
	CostKnown                   bool
	UnmeteredReason             string
	CreatedAt                   time.Time
	ExpiresAt                   time.Time
	State                       ReservationState
}

// SettleRequest replaces a reservation with the provider-reported usage, or
// with an explicit request-only/incomplete outcome.
type SettleRequest struct {
	ReservationID string
	Mode          SettlementMode
	InputTokens   int64
	OutputTokens  int64
}

// Settlement is the committed terminal accounting result.
type Settlement struct {
	Reservation
	Mode                     SettlementMode
	InputTokens              int64
	OutputTokens             int64
	Tokens                   int64
	EstimatedCostMicros      int64
	UsageExceededReservation bool
	EstimatedCostUnavailable bool
}

// Record is the durable representation of one reservation and its optional
// terminal settlement. It is exported so an authoritative control-plane
// adapter can commit one bounded row without serializing the whole ledger.
type Record struct {
	Request     ReserveRequest
	Reservation Reservation
	Settlement  *Settlement
	SettleInput SettleRequest
	TerminalAt  time.Time
}

// ExpiryResult reports conservative reservations committed by expiry or node
// death. IDs are returned so the caller can emit one event per transition.
type ExpiryResult struct {
	ReservationIDs []string
	CollectedIDs   []string
}

// UsageQuery selects current rolling counters. Empty fields are wildcards;
// Window filters budgets by their configured window.
type UsageQuery struct {
	BudgetID  string
	Tenant    string
	Workspace string
	Principal string
	Binding   string
	Window    Window
	At        time.Time
}

// Usage is the current reserved-or-settled charge for one budget window.
type Usage struct {
	BudgetID            string
	Window              Window
	Since               time.Time
	Requests            int64
	Tokens              int64
	EstimatedCostMicros int64
	ActiveReservations  int64
	UnmeteredRequests   int64
	IncompleteRequests  int64
}

// Stats exposes bounded authority state and observable exceptional outcomes.
type Stats struct {
	Budgets         int
	MaxBudgets      int
	Reservations    int
	MaxReservations int
	Active          int
	Denied          uint64
	Unmetered       uint64
	Incomplete      uint64
	Expired         uint64
}

// DeniedError identifies all budgets that atomically refused one dimension.
type DeniedError struct {
	BudgetIDs       []string
	Limit           LimitKind
	UnmeteredReason string
}

func (e *DeniedError) Error() string {
	return fmt.Sprintf("budget admission denied by %s limit", e.Limit)
}

// Is makes DeniedError match ErrBudgetExceeded without message inspection.
func (e *DeniedError) Is(target error) bool { return target == ErrBudgetExceeded }

// Store is the narrow authority seam injected into the broker/control plane.
// Implementations must make Reserve atomic across all matching budgets.
type Store interface {
	Reserve(context.Context, ReserveRequest) (Reservation, error)
	Settle(context.Context, SettleRequest) (Settlement, error)
	ExpireNode(context.Context, string, time.Time) (ExpiryResult, error)
	Sweep(context.Context, time.Time) (ExpiryResult, error)
	Usage(context.Context, UsageQuery) ([]Usage, error)
	Stats() Stats
}

// AdminStore adds bounded budget configuration used by the control plane.
// Budget IDs are immutable while any retained reservation references them.
type AdminStore interface {
	Store
	PutBudget(context.Context, Budget) error
	DeleteBudget(context.Context, string, string) error
	Budgets(context.Context) ([]Budget, error)
}

// ValidateBudget validates a budget before durable admission.
func ValidateBudget(value Budget) error { return validateBudget(value) }

// Duration returns the rolling duration represented by the window.
func (w Window) Duration() (time.Duration, bool) { return windowDuration(w) }

func windowDuration(window Window) (time.Duration, bool) {
	switch window {
	case WindowHour:
		return time.Hour, true
	case WindowDay:
		return 24 * time.Hour, true
	case Window30Days:
		return 30 * 24 * time.Hour, true
	default:
		return 0, false
	}
}
