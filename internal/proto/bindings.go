package proto

// Binding lifecycle operations. A binding names a real credential that never
// enters a workspace: the workspace holds a placeholder and the node's broker
// substitutes the secret at the network edge. These operations make that
// binding a durable, mutable, tenant-scoped resource rather than a static
// entry in a file the control plane read once at start-up.
const (
	OpBindingCreate = "binding.create" // BindingCreateReq -> BindingSpec (secret never returned)
	OpBindingList   = "binding.list"   // BindingListReq -> BindingListRes
	OpBindingGet    = "binding.get"    // BindingGetReq -> BindingSpec
	OpBindingRotate = "binding.rotate" // BindingRotateReq -> BindingSpec (bumps Revision)
	OpBindingRevoke = "binding.revoke" // BindingRevokeReq -> BindingSpec (RevokedAt set)

	// OpPrincipalSessionCreate is the one-call session-scoped principal: an
	// ephemeral principal plus its workspace- and generation-bound capability.
	OpPrincipalSessionCreate = "principal.session.create" // PrincipalSessionCreateReq -> PrincipalSessionCreateRes

	EvBindingCreated          = "binding.created"
	EvBindingRotated          = "binding.rotated"
	EvBindingRevoked          = "binding.revoked"
	EvPrincipalSessionCreated = "principal.session.created"
)

// Binding kinds. The kind is descriptive policy metadata: it tells an
// operator and an SDK what the substituted value is, and it keeps browser
// session cookies a separate class from API keys so the two are never mixed
// under one destination scope.
const (
	BindingKindAPIKey = "api_key" // provider API key, usually an Authorization header
	BindingKindBearer = "bearer"  // generic HTTP bearer token
	BindingKindCookie = "cookie"  // browser login/session cookie; never an API key
	BindingKindHeader = "header"  // generic header value substitution
)

// BindingRetention records provider data-retention requirements as metadata.
// Remount does not enforce a provider's retention policy; it records what the
// operator asserted so an audit can answer the question.
type BindingRetention struct {
	// NoLog asks every Remount component to keep request and response
	// content out of durable records for this binding's traffic.
	NoLog bool   `cbor:"no_log,omitempty" json:"no_log,omitempty"`
	Note  string `cbor:"note,omitempty" json:"note,omitempty"`
}

// BindingSpec is the public shape of a binding. Secret is write-only: it is
// accepted on create and rotate and is never present in a response, an event,
// or a diagnostic. Source names an external secret instead, resolved at lease
// time by the control plane's SecretResolver.
type BindingSpec struct {
	ID     string `cbor:"id" json:"id"`
	Tenant string `cbor:"tenant,omitempty" json:"tenant,omitempty"`
	// Kind is one of the BindingKind* constants.
	Kind string `cbor:"kind,omitempty" json:"kind,omitempty"`
	// Secret is write-only. A response always carries an empty Secret.
	Secret       string   `cbor:"secret,omitempty" json:"secret,omitempty"`
	Source       string   `cbor:"source,omitempty" json:"source,omitempty"`
	Destinations []string `cbor:"destinations,omitempty" json:"destinations,omitempty"`
	Principals   []string `cbor:"principals,omitempty" json:"principals,omitempty"`
	Workspaces   []string `cbor:"workspaces,omitempty" json:"workspaces,omitempty"`
	Placeholder  string   `cbor:"placeholder,omitempty" json:"placeholder,omitempty"`
	TTLSec       int64    `cbor:"ttl_sec,omitempty" json:"ttl_sec,omitempty"`
	// Methods and PathPrefixes narrow the binding beyond its destination
	// hosts. Empty means every method or every path.
	Methods      []string         `cbor:"methods,omitempty" json:"methods,omitempty"`
	PathPrefixes []string         `cbor:"path_prefixes,omitempty" json:"path_prefixes,omitempty"`
	Retention    BindingRetention `cbor:"retention,omitempty" json:"retention,omitempty"`
	// Revision increments on every rotation. A lease carries the revision it
	// was minted from, so a node can tell a stale lease from a live one.
	Revision  uint64 `cbor:"revision,omitempty" json:"revision,omitempty"`
	CreatedAt int64  `cbor:"created_at,omitempty" json:"created_at,omitempty"`
	RotatedAt int64  `cbor:"rotated_at,omitempty" json:"rotated_at,omitempty"`
	// RevokedAt is non-zero once the binding is revoked. Revocation is
	// permanent: the id is never reusable.
	RevokedAt     int64  `cbor:"revoked_at,omitempty" json:"revoked_at,omitempty"`
	RevokedReason string `cbor:"revoked_reason,omitempty" json:"revoked_reason,omitempty"`
}

// BindingCreateReq defines a new tenant-scoped binding. Exactly one of
// Binding.Secret and Binding.Source must be set.
type BindingCreateReq struct {
	Binding        BindingSpec `cbor:"binding" json:"binding"`
	IdempotencyKey string      `cbor:"idem" json:"idem"`
}

type BindingListReq struct {
	Tenant string `cbor:"tenant,omitempty" json:"tenant,omitempty"`
	// IncludeRevoked returns revoked rows too. They are retained so an audit
	// can still resolve a binding id seen in an older event.
	IncludeRevoked bool `cbor:"include_revoked,omitempty" json:"include_revoked,omitempty"`
}

type BindingListRes struct {
	Bindings []BindingSpec `cbor:"bindings" json:"bindings"`
}

type BindingGetReq struct {
	ID     string `cbor:"id" json:"id"`
	Tenant string `cbor:"tenant,omitempty" json:"tenant,omitempty"`
}

// BindingRotateReq replaces the credential behind an existing binding and
// bumps its revision. Leases minted from the previous revision stop being
// honored at the node within one renew interval.
type BindingRotateReq struct {
	ID             string `cbor:"id" json:"id"`
	Tenant         string `cbor:"tenant,omitempty" json:"tenant,omitempty"`
	Secret         string `cbor:"secret,omitempty" json:"secret,omitempty"`
	Source         string `cbor:"source,omitempty" json:"source,omitempty"`
	IdempotencyKey string `cbor:"idem" json:"idem"`
}

// BindingRevokeReq permanently stops substitution for a binding. It does not
// revoke the credential at the provider; see the binding lifecycle ADR.
type BindingRevokeReq struct {
	ID             string `cbor:"id" json:"id"`
	Tenant         string `cbor:"tenant,omitempty" json:"tenant,omitempty"`
	Reason         string `cbor:"reason,omitempty" json:"reason,omitempty"`
	IdempotencyKey string `cbor:"idem" json:"idem"`
}

// PrincipalSessionCreateReq mints an ephemeral principal and, in the same
// call, the workspace- and generation-bound capability it acts with. Subject
// may be empty, in which case the control plane generates one.
type PrincipalSessionCreateReq struct {
	Tenant         string   `cbor:"tenant,omitempty" json:"tenant,omitempty"`
	Subject        string   `cbor:"subject,omitempty" json:"subject,omitempty"`
	Roles          []string `cbor:"roles,omitempty" json:"roles,omitempty"`
	Workspace      string   `cbor:"ws" json:"ws"`
	TTLSec         int64    `cbor:"ttl_sec,omitempty" json:"ttl_sec,omitempty"`
	IdempotencyKey string   `cbor:"idem" json:"idem"`
}

// PrincipalSessionCreateRes carries the capability bearer exactly once. It is
// never written to durable control state or to an event.
type PrincipalSessionCreateRes struct {
	Principal  Principal `cbor:"principal" json:"principal"`
	Token      string    `cbor:"token" json:"token"`
	ExpiresAt  int64     `cbor:"expires_at" json:"expires_at"`
	Workspace  string    `cbor:"ws" json:"ws"`
	Generation uint64    `cbor:"gen" json:"gen"`
}
