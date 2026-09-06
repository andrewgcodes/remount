package proto

// Identity and tenant administration operations. Identity-mutating calls are
// client operations; session capability calls are node-only internal RPCs.
const (
	OpPrincipalRevoke          = "principal.revoke"
	OpPrincipalCreate          = "principal.create"
	OpPrincipalList            = "principal.list"
	OpPrincipalTokenIssue      = "principal.token.issue"
	OpPrincipalInvite          = "principal.invite"
	OpSessionCapabilityIssue   = "session.cap.issue"
	OpSessionCapabilityRenew   = "session.cap.renew"
	OpSessionCapabilityCheck   = "session.cap.check"
	OpTenantCreate             = "tenant.create"
	OpTenantGet                = "tenant.get"
	OpTenantList               = "tenant.list"
	OpTenantUpdate             = "tenant.update"
	OpTenantState              = "tenant.state"
	OpTenantUsage              = "tenant.usage"
	EvIdentityWorkspaceRevoked = "identity.workspace_revoked"
	EvIdentityPrincipalCreated = "identity.principal_created"
	EvIdentityRolesChanged     = "identity.roles_changed"
)

// Node enrollment is the operator half of §3's one-time enrollment: an
// operator mints a single-use credential, hands it to exactly one machine,
// and that machine's first hello consumes it. The credential is returned once
// and never enters durable control state or any canonical event payload.
const (
	OpNodeEnroll                   = "node.enroll"
	EvIdentityNodeEnrollmentIssued = "identity.node_enrollment_issued"
)

// NodeEnrollReq asks for one one-time node enrollment credential. Name is the
// pool or machine name the enrollment is recorded under; Labels are trusted
// placement labels a node can neither expand nor replace in its hello. TTLMS
// is bounded by the issuer at ten minutes.
type NodeEnrollReq struct {
	Tenant         string            `cbor:"tenant,omitempty" json:"tenant,omitempty"`
	Name           string            `cbor:"name" json:"name"`
	Labels         map[string]string `cbor:"labels,omitempty" json:"labels,omitempty"`
	TTLMS          int64             `cbor:"ttl_ms" json:"ttl_ms"`
	IdempotencyKey string            `cbor:"idem" json:"idem"`
}

// NodeEnrollRes carries the one-time bearer exactly once. It must never be
// written to durable state, a log line, or an event payload.
type NodeEnrollRes struct {
	EnrollmentToken string `cbor:"enrollment_token" json:"enrollment_token"`
	Tenant          string `cbor:"tenant" json:"tenant"`
	Name            string `cbor:"name" json:"name"`
	ExpiresAt       int64  `cbor:"expires_at" json:"expires_at"`
}

const (
	TenantActive    = "active"
	TenantSuspended = "suspended"
	TenantDeleted   = "deleted"
)

// TenantQuotas are hard concurrent limits; zero means unlimited.
type TenantQuotas struct {
	MaxWorkspaces     int64 `cbor:"max_workspaces" json:"max_workspaces"`
	MaxNodes          int64 `cbor:"max_nodes" json:"max_nodes"`
	MaxArtifactBytes  int64 `cbor:"max_artifact_bytes" json:"max_artifact_bytes"`
	MaxActiveSessions int64 `cbor:"max_active_sessions" json:"max_active_sessions"`
}

// TenantRetention is expressed in milliseconds on the wire.
type TenantRetention struct {
	EventsMS      int64 `cbor:"events_ms" json:"events_ms"`
	SessionLogsMS int64 `cbor:"session_logs_ms" json:"session_logs_ms"`
	ArtifactsMS   int64 `cbor:"artifacts_ms" json:"artifacts_ms"`
	MeterEventsMS int64 `cbor:"meter_events_ms" json:"meter_events_ms"`
}

// TenantResidency is an allowed region set plus required node-label AND.
type TenantResidency struct {
	AllowedRegions []string          `cbor:"allowed_regions,omitempty" json:"allowed_regions,omitempty"`
	RequiredLabels map[string]string `cbor:"required_labels,omitempty" json:"required_labels,omitempty"`
}

// TenantOIDC is public-client federation configuration. It contains no
// provider secret; exact group names map to bounded Remount roles.
type TenantOIDC struct {
	Issuer      string              `cbor:"issuer,omitempty" json:"issuer,omitempty"`
	ClientID    string              `cbor:"client_id,omitempty" json:"client_id,omitempty"`
	Audience    string              `cbor:"audience,omitempty" json:"audience,omitempty"`
	Scopes      []string            `cbor:"scopes,omitempty" json:"scopes,omitempty"`
	GroupRoles  map[string][]string `cbor:"group_roles,omitempty" json:"group_roles,omitempty"`
	TenantClaim string              `cbor:"tenant_claim,omitempty" json:"tenant_claim,omitempty"`
	GroupsClaim string              `cbor:"groups_claim,omitempty" json:"groups_claim,omitempty"`
}

// TenantPolicy is mutable only through expected-revision operations.
type TenantPolicy struct {
	Quotas           TenantQuotas    `cbor:"quotas" json:"quotas"`
	Retention        TenantRetention `cbor:"retention" json:"retention"`
	Residency        TenantResidency `cbor:"residency" json:"residency"`
	OIDC             TenantOIDC      `cbor:"oidc" json:"oidc"`
	StripeCustomerID string          `cbor:"stripe_customer_id,omitempty" json:"stripe_customer_id,omitempty"`
}

// Tenant is one durable administrative and quota authority.
type Tenant struct {
	ID        string       `cbor:"id" json:"id"`
	State     string       `cbor:"state" json:"state"`
	Revision  uint64       `cbor:"revision" json:"revision"`
	Policy    TenantPolicy `cbor:"policy" json:"policy"`
	CreatedAt int64        `cbor:"created_at" json:"created_at"`
	UpdatedAt int64        `cbor:"updated_at" json:"updated_at"`
}

type TenantCreateReq struct {
	ID             string       `cbor:"id" json:"id"`
	Policy         TenantPolicy `cbor:"policy" json:"policy"`
	IdempotencyKey string       `cbor:"idem" json:"idem"`
}

type TenantGetReq struct {
	ID string `cbor:"id" json:"id"`
}

type TenantListRes struct {
	Tenants []Tenant `cbor:"tenants" json:"tenants"`
}

type TenantUpdateReq struct {
	ID               string       `cbor:"id" json:"id"`
	Policy           TenantPolicy `cbor:"policy" json:"policy"`
	ExpectedRevision uint64       `cbor:"expected_revision" json:"expected_revision"`
	IdempotencyKey   string       `cbor:"idem" json:"idem"`
}

type TenantStateReq struct {
	ID               string `cbor:"id" json:"id"`
	State            string `cbor:"state" json:"state"`
	ExpectedRevision uint64 `cbor:"expected_revision" json:"expected_revision"`
	IdempotencyKey   string `cbor:"idem" json:"idem"`
}

type TenantUsageReq struct {
	Tenant string `cbor:"tenant,omitempty" json:"tenant,omitempty"`
}

type TenantUsage struct {
	Workspaces     int64 `cbor:"workspaces" json:"workspaces"`
	Nodes          int64 `cbor:"nodes" json:"nodes"`
	ArtifactBytes  int64 `cbor:"artifact_bytes" json:"artifact_bytes"`
	ActiveSessions int64 `cbor:"active_sessions" json:"active_sessions"`
}

// PrincipalRevokeReq invalidates every credential for one tenant principal.
type PrincipalRevokeReq struct {
	Tenant         string `cbor:"tenant,omitempty" json:"tenant,omitempty"`
	Principal      string `cbor:"principal" json:"principal"`
	IdempotencyKey string `cbor:"idem" json:"idem"`
}

// PrincipalRevokeRes reports the durable principal invalidation revision.
type PrincipalRevokeRes struct {
	Revision uint64 `cbor:"revision" json:"revision"`
}

// Principal is durable tenant-scoped login authority. Bearers are never
// retained with this record.
type Principal struct {
	ID        string   `cbor:"id" json:"id"`
	Tenant    string   `cbor:"tenant" json:"tenant"`
	Roles     []string `cbor:"roles" json:"roles"`
	Revision  uint64   `cbor:"revision" json:"revision"`
	CreatedAt int64    `cbor:"created_at" json:"created_at"`
	UpdatedAt int64    `cbor:"updated_at" json:"updated_at"`
}

type PrincipalCreateReq struct {
	Tenant         string   `cbor:"tenant,omitempty" json:"tenant,omitempty"`
	Principal      string   `cbor:"principal" json:"principal"`
	Roles          []string `cbor:"roles" json:"roles"`
	IdempotencyKey string   `cbor:"idem" json:"idem"`
}

type PrincipalListReq struct {
	Tenant string `cbor:"tenant,omitempty" json:"tenant,omitempty"`
}

type PrincipalListRes struct {
	Principals []Principal `cbor:"principals" json:"principals"`
}

// PrincipalTokenIssueReq mints one short-lived access bearer for exactly one
// role already assigned to the principal. TTLMS is bounded by the issuer.
type PrincipalTokenIssueReq struct {
	Tenant         string `cbor:"tenant,omitempty" json:"tenant,omitempty"`
	Principal      string `cbor:"principal" json:"principal"`
	Role           string `cbor:"role" json:"role"`
	TTLMS          int64  `cbor:"ttl_ms" json:"ttl_ms"`
	IdempotencyKey string `cbor:"idem" json:"idem"`
}

type PrincipalTokenIssueRes struct {
	AccessToken string `cbor:"access_token" json:"access_token"`
	ExpiresAt   int64  `cbor:"expires_at" json:"expires_at"`
}

// PrincipalInviteReq creates a tenant operator and returns a short-lived
// access bearer once. No bearer is written to durable state or audit events.
type PrincipalInviteReq struct {
	Tenant         string `cbor:"tenant" json:"tenant"`
	Principal      string `cbor:"principal" json:"principal"`
	TTLMS          int64  `cbor:"ttl_ms" json:"ttl_ms"`
	IdempotencyKey string `cbor:"idem" json:"idem"`
}

// SessionCapabilityIssueReq asks control to mint a capability for the signed
// identity carried by a grant. The receiving control plane derives the node
// from the authenticated peer and revalidates every field against live state.
type SessionCapabilityIssueReq struct {
	Client        string   `cbor:"client" json:"client"`
	Workspace     string   `cbor:"ws" json:"ws"`
	Generation    uint64   `cbor:"gen" json:"gen"`
	AuthzRevision uint64   `cbor:"authz_revision" json:"authz_revision"`
	Principal     string   `cbor:"principal" json:"principal"`
	Tenant        string   `cbor:"tenant" json:"tenant"`
	Roles         []string `cbor:"roles,omitempty" json:"roles,omitempty"`
}

// SessionCapabilityIssueRes carries a short-lived bearer only over the
// authenticated node/control connection. It must never enter durable state.
type SessionCapabilityIssueRes struct {
	Capability string `cbor:"capability" json:"capability"`
	ExpiresAt  int64  `cbor:"expires_at" json:"expires_at"`
}

// SessionCapabilityRenewReq rotates an unexpired signed capability without
// requiring the originating client connection to remain open. The caller is
// the authenticated node currently holding the workspace generation.
type SessionCapabilityRenewReq struct {
	Workspace  string `cbor:"ws" json:"ws"`
	Generation uint64 `cbor:"gen" json:"gen"`
	Capability string `cbor:"capability" json:"capability"`
}

// SessionCapabilityCheckReq is evaluated for every broker request.
type SessionCapabilityCheckReq struct {
	Workspace  string `cbor:"ws" json:"ws"`
	Generation uint64 `cbor:"gen" json:"gen"`
	Capability string `cbor:"capability" json:"capability"`
}

// SessionCapabilityCheckRes returns only non-secret attribution.
type SessionCapabilityCheckRes struct {
	Principal string `cbor:"principal" json:"principal"`
	Tenant    string `cbor:"tenant" json:"tenant"`
}
