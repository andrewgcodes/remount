// Package tenant owns durable tenant policy, quota admission and canonical
// usage metering. Authentication remains in identity and enforcement remains
// at the resource boundary; this package is the shared policy authority.
package tenant

import (
	"context"
	"errors"
	"fmt"
	"time"
)

// State is the administrative tenant lifecycle state.
type State string

const (
	StateActive    State = "active"
	StateSuspended State = "suspended"
	StateDeleted   State = "deleted"
)

// Resource names the independently-accounted quota dimensions.
type Resource string

const (
	ResourceWorkspace     Resource = "workspace"
	ResourceNode          Resource = "node"
	ResourceArtifactBytes Resource = "artifact_bytes"
	ResourceActiveSession Resource = "active_session"
)

// MeterKind is a canonical billable observation. Values are additive whole
// numbers so exporters do not need floating-point arithmetic.
type MeterKind string

const (
	MeterWorkspaceSeconds MeterKind = "usage.workspace_seconds"
	MeterBrokeredRequest  MeterKind = "usage.brokered_request"
	MeterBytesIn          MeterKind = "usage.bytes_in"
	MeterBytesOut         MeterKind = "usage.bytes_out"
	MeterStorageBytes     MeterKind = "usage.storage_bytes"
)

// Quotas are hard concurrent limits. Zero means unlimited.
type Quotas struct {
	MaxWorkspaces     int64 `json:"max_workspaces"`
	MaxNodes          int64 `json:"max_nodes"`
	MaxArtifactBytes  int64 `json:"max_artifact_bytes"`
	MaxActiveSessions int64 `json:"max_active_sessions"`
}

// Limit returns the configured hard limit for resource.
func (q Quotas) Limit(resource Resource) int64 {
	switch resource {
	case ResourceWorkspace:
		return q.MaxWorkspaces
	case ResourceNode:
		return q.MaxNodes
	case ResourceArtifactBytes:
		return q.MaxArtifactBytes
	case ResourceActiveSession:
		return q.MaxActiveSessions
	default:
		return -1
	}
}

// Retention is the tenant-specific data lifecycle policy. Zero delegates to
// the deployment default; non-zero values are explicit maximum ages.
type Retention struct {
	Events      time.Duration `json:"events"`
	SessionLogs time.Duration `json:"session_logs"`
	Artifacts   time.Duration `json:"artifacts"`
	MeterEvents time.Duration `json:"meter_events"`
}

// Residency restricts placement. AllowedRegions is an allow-list and
// RequiredLabels is an AND predicate applied by the scheduler.
type Residency struct {
	AllowedRegions []string          `json:"allowed_regions,omitempty"`
	RequiredLabels map[string]string `json:"required_labels,omitempty"`
}

// Billing is non-secret exporter routing metadata.
type Billing struct {
	StripeCustomerID string `json:"stripe_customer_id,omitempty"`
}

// OIDC configures one tenant's public-client federation and exact group map.
type OIDC struct {
	Issuer      string              `json:"issuer,omitempty"`
	ClientID    string              `json:"client_id,omitempty"`
	Audience    string              `json:"audience,omitempty"`
	Scopes      []string            `json:"scopes,omitempty"`
	GroupRoles  map[string][]string `json:"group_roles,omitempty"`
	TenantClaim string              `json:"tenant_claim,omitempty"`
	GroupsClaim string              `json:"groups_claim,omitempty"`
}

// Policy is the mutable administrative policy of a tenant.
type Policy struct {
	Quotas    Quotas    `json:"quotas"`
	Retention Retention `json:"retention"`
	Residency Residency `json:"residency"`
	Billing   Billing   `json:"billing"`
	OIDC      OIDC      `json:"oidc"`
}

// Tenant is a durable tenant record.
type Tenant struct {
	ID        string    `json:"id"`
	State     State     `json:"state"`
	Revision  uint64    `json:"revision"`
	Policy    Policy    `json:"policy"`
	CreatedAt time.Time `json:"created_at"`
	UpdatedAt time.Time `json:"updated_at"`
}

// Mutation identifies one replay-safe administrative operation.
type Mutation struct {
	OperationID      string
	Actor            string
	ExpectedRevision uint64
}

// Admission is a replay-safe quota reservation. ResourceID is the stable
// identity of the resource being counted and Amount must be positive.
type Admission struct {
	Tenant      string
	OperationID string
	Actor       string
	Resource    Resource
	ResourceID  string
	Amount      int64
}

// Usage is the current durable reservation total in every quota dimension.
type Usage struct {
	Workspaces     int64 `json:"workspaces"`
	Nodes          int64 `json:"nodes"`
	ArtifactBytes  int64 `json:"artifact_bytes"`
	ActiveSessions int64 `json:"active_sessions"`
}

// MeterEvent is one immutable canonical usage record.
type MeterEvent struct {
	Seq       uint64    `json:"seq"`
	ID        string    `json:"id"`
	Tenant    string    `json:"tenant"`
	Kind      MeterKind `json:"kind"`
	Value     int64     `json:"value"`
	At        time.Time `json:"at"`
	Workspace string    `json:"workspace,omitempty"`
	Principal string    `json:"principal,omitempty"`
	Binding   string    `json:"binding,omitempty"`
}

// Code is a stable error category for root protocol adapters.
type Code string

const (
	CodeBadRequest        Code = "bad_request"
	CodeNotFound          Code = "not_found"
	CodeConflict          Code = "conflict"
	CodeResourceExhausted Code = "resource_exhausted"
	CodeUnavailable       Code = "unavailable"
)

// Error is safe to expose; it never includes credentials or response bodies.
type Error struct {
	Code       Code
	Resource   Resource
	Limit      int64
	Used       int64
	HTTPStatus int
	Retryable  bool
}

func (e *Error) Error() string {
	if e.Code == CodeResourceExhausted {
		if e.Resource == "" {
			return "tenant: resource exhausted"
		}
		return fmt.Sprintf("tenant: %s quota exhausted (used=%d limit=%d)", e.Resource, e.Used, e.Limit)
	}
	if e.HTTPStatus != 0 {
		return fmt.Sprintf("tenant: %s (HTTP %d)", e.Code, e.HTTPStatus)
	}
	return "tenant: " + string(e.Code)
}

// IsCode reports whether err carries code.
func IsCode(err error, code Code) bool {
	var target *Error
	return errors.As(err, &target) && target.Code == code
}

// BillingExporter sends one bounded ordered page. It must be replay-safe:
// ExportOnce advances its durable cursor only after this call succeeds.
type BillingExporter interface {
	Export(context.Context, []MeterEvent) error
}
