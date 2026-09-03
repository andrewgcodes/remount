package proto

// Budget is one rolling governance limit attached to a tenant, workspace,
// principal, or binding. Cost is explicitly approximate.
type Budget struct {
	ID                     string `cbor:"id" json:"id"`
	Tenant                 string `cbor:"tenant,omitempty" json:"tenant,omitempty"`
	AttachTo               string `cbor:"attach_to" json:"attach_to"`
	AttachID               string `cbor:"attach_id" json:"attach_id"`
	Window                 string `cbor:"window" json:"window"`
	MaxRequests            int64  `cbor:"max_requests,omitempty" json:"max_requests,omitempty"`
	MaxTokens              int64  `cbor:"max_tokens,omitempty" json:"max_tokens,omitempty"`
	MaxEstimatedCostMicros int64  `cbor:"max_estimated_cost_micros,omitempty" json:"max_estimated_cost_micros,omitempty"`
	CreatedAt              int64  `cbor:"created_at,omitempty" json:"created_at,omitempty"`
	UpdatedAt              int64  `cbor:"updated_at,omitempty" json:"updated_at,omitempty"`
}

const (
	OpBudgetCreate  = "budget.create"  // BudgetCreateReq -> Budget
	OpBudgetList    = "budget.list"    // BudgetListReq -> BudgetListRes
	OpBudgetRemove  = "budget.remove"  // BudgetRemoveReq -> {}
	OpBudgetReserve = "budget.reserve" // node: BudgetReserveReq -> BudgetReservation
	OpBudgetSettle  = "budget.settle"  // node: BudgetSettleReq -> BudgetSettlement
	OpUsageGet      = "usage.get"      // UsageReq -> UsageRes
)

type BudgetCreateReq struct {
	Budget         Budget `cbor:"budget" json:"budget"`
	IdempotencyKey string `cbor:"idem,omitempty" json:"idem,omitempty"`
}

type BudgetListReq struct{}

type BudgetListRes struct {
	Budgets []Budget `cbor:"budgets" json:"budgets"`
}

type BudgetRemoveReq struct {
	ID             string `cbor:"id" json:"id"`
	IdempotencyKey string `cbor:"idem,omitempty" json:"idem,omitempty"`
}

// BudgetReserveReq carries the conservative admission bound computed before
// a request is released by the node broker.
type BudgetReserveReq struct {
	Key             string   `cbor:"key" json:"key"`
	WS              string   `cbor:"ws" json:"ws"`
	Gen             uint64   `cbor:"gen" json:"gen"`
	Principal       string   `cbor:"principal,omitempty" json:"principal,omitempty"`
	Bindings        []string `cbor:"bindings,omitempty" json:"bindings,omitempty"`
	Provider        string   `cbor:"provider" json:"provider"`
	Model           string   `cbor:"model,omitempty" json:"model,omitempty"`
	InputTokens     int64    `cbor:"input_tokens,omitempty" json:"input_tokens,omitempty"`
	MaxOutputTokens int64    `cbor:"max_output_tokens,omitempty" json:"max_output_tokens,omitempty"`
	Metered         bool     `cbor:"metered,omitempty" json:"metered,omitempty"`
}

type BudgetReservation struct {
	ID              string   `cbor:"id,omitempty" json:"id,omitempty"`
	Tracked         bool     `cbor:"tracked,omitempty" json:"tracked,omitempty"`
	BudgetIDs       []string `cbor:"budget_ids,omitempty" json:"budget_ids,omitempty"`
	UnmeteredReason string   `cbor:"unmetered_reason,omitempty" json:"unmetered_reason,omitempty"`
	ExpiresAt       int64    `cbor:"expires_at,omitempty" json:"expires_at,omitempty"`
	Denied          bool     `cbor:"denied,omitempty" json:"denied,omitempty"`
	Limit           string   `cbor:"limit,omitempty" json:"limit,omitempty"`
}

type BudgetSettleReq struct {
	Reservation  string `cbor:"reservation" json:"reservation"`
	Mode         string `cbor:"mode" json:"mode"`
	InputTokens  int64  `cbor:"input_tokens,omitempty" json:"input_tokens,omitempty"`
	OutputTokens int64  `cbor:"output_tokens,omitempty" json:"output_tokens,omitempty"`
}

type BudgetSettlement struct {
	Reservation              string `cbor:"reservation" json:"reservation"`
	Mode                     string `cbor:"mode" json:"mode"`
	Tokens                   int64  `cbor:"tokens,omitempty" json:"tokens,omitempty"`
	EstimatedCostMicros      int64  `cbor:"estimated_cost_micros,omitempty" json:"estimated_cost_micros,omitempty"`
	EstimatedCostUnavailable bool   `cbor:"estimated_cost_unavailable,omitempty" json:"estimated_cost_unavailable,omitempty"`
	UsageExceededReservation bool   `cbor:"usage_exceeded_reservation,omitempty" json:"usage_exceeded_reservation,omitempty"`
}

type UsageReq struct {
	Tenant    string `cbor:"tenant,omitempty" json:"tenant,omitempty"`
	WS        string `cbor:"ws,omitempty" json:"ws,omitempty"`
	Principal string `cbor:"principal,omitempty" json:"principal,omitempty"`
	Binding   string `cbor:"binding,omitempty" json:"binding,omitempty"`
	Window    string `cbor:"window,omitempty" json:"window,omitempty"`
}

type Usage struct {
	BudgetID            string `cbor:"budget_id" json:"budget_id"`
	Window              string `cbor:"window" json:"window"`
	Since               int64  `cbor:"since" json:"since"`
	Requests            int64  `cbor:"requests" json:"requests"`
	Tokens              int64  `cbor:"tokens" json:"tokens"`
	EstimatedCostMicros int64  `cbor:"estimated_cost_micros" json:"estimated_cost_micros"`
	ActiveReservations  int64  `cbor:"active_reservations" json:"active_reservations"`
	UnmeteredRequests   int64  `cbor:"unmetered_requests" json:"unmetered_requests"`
	IncompleteRequests  int64  `cbor:"incomplete_requests" json:"incomplete_requests"`
}

type UsageRes struct {
	Usage []Usage `cbor:"usage" json:"usage"`
}

const (
	EvBudgetCreated   = "budget.created"
	EvBudgetRemoved   = "budget.removed"
	EvBudgetReserved  = "budget.reserved"
	EvBudgetSettled   = "budget.settled"
	EvBudgetExpired   = "budget.expired"
	EvBudgetUnmetered = "budget.unmetered"
)
