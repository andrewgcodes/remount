package proto

import (
	"path"
	"strings"
	"time"
)

// ---------------------------------------------------------------------------
// hello
// ---------------------------------------------------------------------------

// Role of a peer on the relay.
const (
	RoleNode   = "node"
	RoleClient = "client"
)

// Hello is the first frame body on every connection.
type Hello struct {
	Peer   string            `cbor:"peer" json:"peer"`                         // requested peer id (n_…, c_…); empty = assign
	Role   string            `cbor:"role" json:"role"`                         // node | client
	Token  string            `cbor:"token,omitempty" json:"token,omitempty"`   // bearer credential for the control plane
	Caps   []string          `cbor:"caps,omitempty" json:"caps,omitempty"`     // protocol capabilities this peer speaks
	PubKey []byte            `cbor:"pubkey,omitempty" json:"pubkey,omitempty"` // ed25519 identity, nodes only
	Labels map[string]string `cbor:"labels,omitempty" json:"labels,omitempty"`
	// Principal a client acts as (audit); empty = the peer id.
	Principal string `cbor:"principal,omitempty" json:"principal,omitempty"`
	// Node-only: what workspaces this node can run.
	Node *NodeInfo `cbor:"node,omitempty" json:"node,omitempty"`
	// Nodes sign the canonical hello (with Proof omitted). IssuedAt and Nonce
	// make the proof fresh and single-use, so a captured enrollment cannot be
	// replayed as proof of private-key possession.
	IssuedAt int64  `cbor:"issued_at,omitempty" json:"issued_at,omitempty"`
	Nonce    []byte `cbor:"nonce,omitempty" json:"nonce,omitempty"`
	Proof    []byte `cbor:"proof,omitempty" json:"proof,omitempty"`
}

// HelloOK is the response body.
type HelloOK struct {
	Peer     string   `cbor:"peer" json:"peer"`
	Caps     []string `cbor:"caps" json:"caps"`
	Server   string   `cbor:"server,omitempty" json:"server,omitempty"`
	Now      int64    `cbor:"now" json:"now"`                                 // server unix millis; clients may use for skew
	PubKey   []byte   `cbor:"pubkey,omitempty" json:"pubkey,omitempty"`       // control plane's ed25519 key; nodes verify grants with it
	LeaseSec int64    `cbor:"lease_sec,omitempty" json:"lease_sec,omitempty"` // how often nodes must renew claims
	Subject  string   `cbor:"subject,omitempty" json:"subject,omitempty"`
	Tenant   string   `cbor:"tenant,omitempty" json:"tenant,omitempty"`
	// NodeToken is a short-lived node-principal credential for authenticated
	// HTTP operations. It is returned only to a successfully enrolled node.
	NodeToken string `cbor:"node_token,omitempty" json:"node_token,omitempty"`
}

// NodeInfo describes a node's capabilities for placement.
type NodeInfo struct {
	Backends           []string            `cbor:"backends" json:"backends"` // legacy discovery names
	BackendDescriptors []BackendDescriptor `cbor:"backend_descriptors,omitempty" json:"backend_descriptors,omitempty"`
	Connectors         []string            `cbor:"connectors,omitempty" json:"connectors,omitempty"` // managed connector names implemented by this node
	OS                 string              `cbor:"os" json:"os"`
	Arch               string              `cbor:"arch" json:"arch"`
	CPU                int                 `cbor:"cpu" json:"cpu"`
	MemMiB             int                 `cbor:"mem_mib" json:"mem_mib"`
	Caps               []string            `cbor:"caps,omitempty" json:"caps,omitempty"` // display, browser, gpu …; never security evidence
	Snapshots          string              `cbor:"snapshots,omitempty" json:"snapshots,omitempty"`
	Version            string              `cbor:"version,omitempty" json:"version,omitempty"`
}

// BackendDescriptor reports evidence-bearing capabilities for one backend.
// Security properties are kept per backend so a weak backend cannot inherit
// a stronger backend's node-wide capability union.
type BackendDescriptor struct {
	Name     string              `cbor:"name" json:"name"`
	Security BackendSecurityCaps `cbor:"security" json:"security"`
	Runtime  RuntimeCaps         `cbor:"runtime" json:"runtime"`
}

type BackendSecurityCaps struct {
	Isolation          string `cbor:"isolation" json:"isolation"` // none | process_sandbox | container | microvm
	MultiTenant        bool   `cbor:"multi_tenant" json:"multi_tenant"`
	SiblingIsolation   bool   `cbor:"sibling_isolation" json:"sibling_isolation"`
	EgressMode         string `cbor:"egress_mode" json:"egress_mode"`         // open | cooperative_proxy | enforced_gateway
	BrokerIdentity     string `cbor:"broker_identity" json:"broker_identity"` // none | token | unix_socket | workload_identity
	FilesystemBoundary string `cbor:"filesystem_boundary" json:"filesystem_boundary"`
	NetworkNamespace   bool   `cbor:"network_namespace" json:"network_namespace"`
	DeviceIsolation    bool   `cbor:"device_isolation" json:"device_isolation"`
}

type RuntimeCaps struct {
	Snapshots string `cbor:"snapshots" json:"snapshots"` // fs | fs+mem
	Display   bool   `cbor:"display,omitempty" json:"display,omitempty"`
	// MountPath: the backend can materialize a workspace at an arbitrary
	// WorkspaceSpec.MountPath inside the workspace's mount namespace.
	MountPath bool `cbor:"mount_path,omitempty" json:"mount_path,omitempty"`
}

// ---------------------------------------------------------------------------
// Workspace (control-plane resource)
// ---------------------------------------------------------------------------

// Workspace states.
const (
	WSPending       = "pending"       // waiting for a node to claim it
	WSClaiming      = "claiming"      // a node owns it but is not serving yet (materializing, or offline)
	WSClaimed       = "claimed"       // running on Node and ready to serve requests
	WSQuiescing     = "quiescing"     // rejecting new work while sessions stop
	WSCheckpointing = "checkpointing" // producing and verifying a durable snapshot
	WSPaused        = "paused"        // snapshotted, no node; wakes on timer/event/attach
	WSReleased      = "released"      // node gave it up; will return to pending
	WSDestroying    = "destroying"
	WSDestroyed     = "destroyed"
	WSFailed        = "failed" // operator action or retry is required
)

// Requires constrains placement.
type Requires struct {
	CPU     int      `cbor:"cpu,omitempty" json:"cpu,omitempty"`
	MemMiB  int      `cbor:"mem_mib,omitempty" json:"mem_mib,omitempty"`
	Backend string   `cbor:"backend,omitempty" json:"backend,omitempty"` // process | docker | firecracker …
	Caps    []string `cbor:"caps,omitempty" json:"caps,omitempty"`       // required node caps
	OS      string   `cbor:"os,omitempty" json:"os,omitempty"`
	Arch    string   `cbor:"arch,omitempty" json:"arch,omitempty"`
}

// Placement restricts which nodes may claim.
type Placement struct {
	Allow  map[string]string `cbor:"allow,omitempty" json:"allow,omitempty"`   // node labels that must match
	Prefer map[string]string `cbor:"prefer,omitempty" json:"prefer,omitempty"` // soft preference (unused in v0 ordering)
	Node   string            `cbor:"node,omitempty" json:"node,omitempty"`     // pin to a specific node id
}

// WorkspaceSpec is the movable unit's declaration.
type WorkspaceSpec struct {
	Name        string            `cbor:"name,omitempty" json:"name,omitempty"`
	Run         string            `cbor:"run,omitempty" json:"run,omitempty"`
	Model       string            `cbor:"model,omitempty" json:"model,omitempty"`
	Labels      map[string]string `cbor:"labels,omitempty" json:"labels,omitempty"`
	Image       string            `cbor:"image,omitempty" json:"image,omitempty"`               // backend-specific (docker image); ignored by process
	RestoreFrom string            `cbor:"restore_from,omitempty" json:"restore_from,omitempty"` // artifact id to restore the filesystem from
	Base        string            `cbor:"base,omitempty" json:"base,omitempty"`                 // tenant base name; control resolves it into RestoreFrom at create
	Requires    Requires          `cbor:"requires" json:"requires"`
	Placement   Placement         `cbor:"placement" json:"placement"`
	Bindings    []string          `cbor:"bindings,omitempty" json:"bindings,omitempty"`   // secret binding ids this workspace may use
	Env         map[string]string `cbor:"env,omitempty" json:"env,omitempty"`             // default env for sessions; values may be ref:…
	Principal   string            `cbor:"principal,omitempty" json:"principal,omitempty"` // who acts through this workspace (a_…)
	Idle        Idle              `cbor:"idle" json:"idle"`
	Exclude     []string          `cbor:"exclude,omitempty" json:"exclude,omitempty"` // snapshot path globs to skip (node_modules, .venv …)
	Security    SecuritySpec      `cbor:"security,omitempty" json:"security,omitempty"`
	ACL         WorkspaceACL      `cbor:"acl,omitempty" json:"acl,omitempty"`
	// MountPath is where the workspace filesystem appears inside the
	// workspace's own mount namespace; "" means DefaultMountPath. Harnesses
	// key state on the working directory, so a handoff sets this to the
	// local checkout's path and their --continue/--resume find that state
	// unchanged. Only backends that advertise RuntimeCaps.MountPath may claim
	// a workspace that sets it.
	MountPath string `cbor:"mount_path,omitempty" json:"mount_path,omitempty"`
	// Repo is cloned into the tree by the node, through its own broker,
	// before ws.ready. It is exclusive with RestoreFrom and Base: a wake or
	// move restores the snapshot instead and never clones again.
	Repo RepoSpec `cbor:"repo,omitempty" json:"repo,omitempty"`
}

// DefaultMountPath is where a workspace is materialized when the spec does
// not say otherwise.
const DefaultMountPath = "/work"

// ValidateMountPath rejects a MountPath a node could not honor safely: it
// must be absolute, clean, not the filesystem root and not under the node's
// own reserved paths.
func ValidateMountPath(p string) error {
	if p == "" {
		return nil
	}
	if len(p) > 1024 {
		return Err(CodeBadRequest, "mount_path is longer than 1024 bytes")
	}
	if !strings.HasPrefix(p, "/") {
		return Err(CodeBadRequest, "mount_path %q must be absolute", p)
	}
	if path.Clean(p) != p || strings.Contains(p, "\x00") {
		return Err(CodeBadRequest, "mount_path %q must be a clean path", p)
	}
	for _, reserved := range []string{"/", "/proc", "/sys", "/dev", "/etc", "/bin", "/sbin", "/lib", "/usr", "/var", "/run", "/boot"} {
		if p == reserved || strings.HasPrefix(p, reserved+"/") {
			return Err(CodeBadRequest, "mount_path %q is a system path", p)
		}
	}
	return nil
}

const (
	SecurityLocal       = "local"
	SecurityIsolated    = "isolated"
	SecurityMultiTenant = "multi_tenant"

	NetworkDefaultDeny  = "deny"
	NetworkDefaultAllow = "allow"
	EgressModeAllow     = "allow"
	EgressModeDeny      = "deny"
	EgressModeApprove   = "approve"

	EgressProtocolHTTP    = "http"
	EgressProtocolHTTPS   = "https"
	EgressProtocolConnect = "connect"

	// EgressConnectorPackage identifies the managed, read-only package
	// retrieval surface. A connector-scoped rule is never usable through the
	// generic HTTP proxy routes.
	EgressConnectorPackage = "package"

	SharedStateNone          = "none"
	SharedStateImmutableRead = "immutable_read"
	SharedStateScopedWrite   = "scoped_write"
	SharedStateGlobalWrite   = "global_write"
)

// SecuritySpec is an enforceable placement contract, not a documentation
// hint. Empty Profile is normalized to local only in standalone mode.
type SecuritySpec struct {
	Profile                 string        `cbor:"profile,omitempty" json:"profile,omitempty"`
	MinIsolation            string        `cbor:"min_isolation,omitempty" json:"min_isolation,omitempty"`
	RequireSiblingIsolation bool          `cbor:"require_sibling_isolation,omitempty" json:"require_sibling_isolation,omitempty"`
	RequireEnforcedEgress   bool          `cbor:"require_enforced_egress,omitempty" json:"require_enforced_egress,omitempty"`
	SecretMode              string        `cbor:"secret_mode,omitempty" json:"secret_mode,omitempty"` // none | brokered
	Network                 NetworkPolicy `cbor:"network,omitempty" json:"network,omitempty"`
	Audit                   AuditPolicy   `cbor:"audit,omitempty" json:"audit,omitempty"`
}

// NetworkPolicy is a first-match list of typed egress capabilities. When at
// least one rule exists, an omitted default is normalized to deny.
type NetworkPolicy struct {
	Default string       `cbor:"default,omitempty" json:"default,omitempty"` // deny | allow (local only)
	Rules   []EgressRule `cbor:"rules,omitempty" json:"rules,omitempty"`
}

// EgressRule constrains one outbound HTTP, HTTPS, or CONNECT capability.
type EgressRule struct {
	ID               string   `cbor:"id" json:"id"`
	Mode             string   `cbor:"mode,omitempty" json:"mode,omitempty"` // allow | deny | approve; empty normalizes to allow
	Connector        string   `cbor:"connector,omitempty" json:"connector,omitempty"`
	Protocol         string   `cbor:"protocol" json:"protocol"`
	Hosts            []string `cbor:"hosts,omitempty" json:"hosts,omitempty"`
	Ports            []uint16 `cbor:"ports,omitempty" json:"ports,omitempty"`
	Methods          []string `cbor:"methods,omitempty" json:"methods,omitempty"`
	PathPrefixes     []string `cbor:"path_prefixes,omitempty" json:"path_prefixes,omitempty"`
	MaxRequests      int64    `cbor:"max_requests,omitempty" json:"max_requests,omitempty"`
	MaxRequestBytes  int64    `cbor:"max_request_bytes,omitempty" json:"max_request_bytes,omitempty"`
	MaxResponseBytes int64    `cbor:"max_response_bytes,omitempty" json:"max_response_bytes,omitempty"`
	Redact           []string `cbor:"redact,omitempty" json:"redact,omitempty"` // RE2 expressions replaced before the response crosses into the workspace
	SharedState      string   `cbor:"shared_state,omitempty" json:"shared_state,omitempty"`
	// Repos names the repositories a git connector rule covers as
	// "owner/name" or "owner/*"; Push additionally permits git-receive-pack.
	// Both are refused on every other connector.
	Repos []string `cbor:"repos,omitempty" json:"repos,omitempty"`
	Push  bool     `cbor:"push,omitempty" json:"push,omitempty"`
}

type AuditPolicy struct {
	Required bool `cbor:"required,omitempty" json:"required,omitempty"`
}

// WorkspaceACL names additional subjects that may read or mutate a
// workspace. The authenticated creator is always its owner.
type WorkspaceACL struct {
	Readers []string `cbor:"readers,omitempty" json:"readers,omitempty"`
	Writers []string `cbor:"writers,omitempty" json:"writers,omitempty"`
}

// AuthzRevocation is one principal losing access to a workspace at one
// authorization revision.
type AuthzRevocation struct {
	Revision  uint64 `cbor:"rev" json:"rev"`
	Principal string `cbor:"principal" json:"principal"`
}

// MaxRetainedRevocations bounds Workspace.Revocations. A node whose known
// revision is older than the oldest retained entry is told to reset.
const MaxRetainedRevocations = 64

// Idle policies (durations in seconds; 0 = disabled).
type Idle struct {
	SnapshotEverySec int64 `cbor:"snapshot_every_sec,omitempty" json:"snapshot_every_sec,omitempty"`
	DestroyAfterSec  int64 `cbor:"destroy_after_sec,omitempty" json:"destroy_after_sec,omitempty"`
}

// Workspace is the control plane's view.
type Workspace struct {
	ID            string        `cbor:"id" json:"id"`
	Spec          WorkspaceSpec `cbor:"spec" json:"spec"`
	State         string        `cbor:"state" json:"state"`
	Node          string        `cbor:"node,omitempty" json:"node,omitempty"`
	LeaseUntil    int64         `cbor:"lease_until,omitempty" json:"lease_until,omitempty"`     // unix millis
	LastSnapshot  string        `cbor:"last_snapshot,omitempty" json:"last_snapshot,omitempty"` // artifact id
	CreatedAt     int64         `cbor:"created_at" json:"created_at"`
	UpdatedAt     int64         `cbor:"updated_at" json:"updated_at"`
	Generation    uint64        `cbor:"gen" json:"gen"` // bumps on every claim; a node with a stale gen must not act
	Tenant        string        `cbor:"tenant,omitempty" json:"tenant,omitempty"`
	Owner         string        `cbor:"owner,omitempty" json:"owner,omitempty"`
	AuthzRevision uint64        `cbor:"authz_revision,omitempty" json:"authz_revision,omitempty"`
	// Revocations records which principals lost access at which authorization
	// revision, newest last, so a node renewing from an older revision learns
	// exactly whose sessions to close. Entries below RevocationFloor have been
	// pruned; a node behind the floor must fail closed for the whole workspace.
	Revocations     []AuthzRevocation `cbor:"revocations,omitempty" json:"revocations,omitempty"`
	RevocationFloor uint64            `cbor:"revocation_floor,omitempty" json:"revocation_floor,omitempty"`
	// QuarantineOperation identifies the durable fleet operation that fenced
	// this workspace. It prevents restart reconciliation from treating an
	// incident response as an ordinary transient failure.
	QuarantineOperation string `cbor:"quarantine_operation,omitempty" json:"quarantine_operation,omitempty"`
	QuarantinedAt       int64  `cbor:"quarantined_at,omitempty" json:"quarantined_at,omitempty"`
}

// ---------------------------------------------------------------------------
// Control-plane operations (client -> control, node -> control)
// ---------------------------------------------------------------------------

const (
	OpWSCreate         = "ws.create"          // WSCreateReq -> Workspace
	OpWSGet            = "ws.get"             // WSGetReq -> Workspace
	OpWSList           = "ws.list"            // -> WSListRes
	OpWSDestroy        = "ws.destroy"         // WSGetReq -> {}
	OpWSMove           = "ws.move"            // WSMoveReq -> Workspace (re-queued)
	OpWSSleep          = "ws.sleep"           // WSSleepReq -> Timer
	OpWSWake           = "ws.wake"            // WSGetReq -> Workspace
	OpWSACL            = "ws.acl"             // WSACLReq -> Workspace (bumps authz_revision)
	OpWSClaim          = "ws.claim"           // node: WSClaimReq -> WSClaimRes
	OpWSRenew          = "ws.renew"           // node: WSRenewReq -> WSRenewRes
	OpWSReleased       = "ws.released"        // node: WSReleasedReq -> {}
	OpWSReady          = "ws.ready"           // node: WSReadyReq -> {} (materialized, now serving)
	OpWSReleaseCommit  = "ws.release.commit"  // control -> node: source checkpoint is committed; destroy source
	OpWSReleaseAbort   = "ws.release.abort"   // control -> node: prepare failed/ambiguous; resume retained source
	OpWSSnapshotCommit = "ws.snapshot.commit" // node -> control: make an uploaded snapshot authoritative
	OpNodeList         = "node.list"          // -> NodeListRes
	OpEventsTail       = "events.tail"        // EventsTailReq -> streams ev frames, then res
	OpEventsStop       = "events.stop"        // EventsStopReq -> {}; stops one event subscription
	OpEventsPost       = "events.post"        // EventPost -> {} (node -> control; also webhook wake)
	OpBindingLease     = "binding.lease"      // node: BindingLeaseReq -> BindingLeaseRes
	OpEgressApproval   = "egress.approval"    // node: EgressApprovalReq -> EgressApprovalRes
	OpGrant            = "grant"              // client: GrantReq -> Grant (permission to talk to a node about a ws)
	OpTimerList        = "timer.list"         // -> TimerListRes
	OpDiag             = "diag"               // -> ControlDiag (control-plane health and integrity)
	OpFleetQuarantine  = "fleet.quarantine"   // FleetQuarantineReq -> FleetOperation
	OpFleetGet         = "fleet.get"          // FleetGetReq -> FleetOperation
	OpFleetList        = "fleet.list"         // -> FleetListRes
	OpBaseCreate       = "base.create"        // BaseCreateReq -> Base (pins a snapshot under a tenant-scoped name)
	OpBaseList         = "base.list"          // -> BaseListRes
	OpBaseRemove       = "base.remove"        // BaseRemoveReq -> {}
	OpQueueCreate      = "queue.create"       // QueueCreateReq -> Queue (durable task list for one workspace)
	OpQueueGet         = "queue.get"          // QueueGetReq -> Queue
	OpQueueList        = "queue.list"         // QueueListReq -> QueueListRes
	OpQueueAdvance     = "queue.advance"      // QueueAdvanceReq -> Queue (records one task's outcome, moves the cursor)
	OpPoolCreate       = "pool.create"        // PoolCreateReq -> Pool
	OpPoolGet          = "pool.get"           // PoolGetReq -> Pool
	OpPoolList         = "pool.list"          // -> PoolListRes
	OpPoolRemove       = "pool.remove"        // PoolRemoveReq -> {}
)

type WSCreateReq struct {
	Spec           WorkspaceSpec `cbor:"spec" json:"spec"`
	IdempotencyKey string        `cbor:"idem,omitempty" json:"idem,omitempty"`
}

type WSGetReq struct {
	ID             string `cbor:"id" json:"id"`
	IdempotencyKey string `cbor:"idem,omitempty" json:"idem,omitempty"`
	Grant          *Grant `cbor:"grant,omitempty" json:"grant,omitempty"`
}

type WSListRes struct {
	Workspaces []Workspace `cbor:"workspaces" json:"workspaces"`
}

type WSMoveReq struct {
	ID             string     `cbor:"id" json:"id"`
	Requires       *Requires  `cbor:"requires,omitempty" json:"requires,omitempty"`
	Placement      *Placement `cbor:"placement,omitempty" json:"placement,omitempty"`
	IdempotencyKey string     `cbor:"idem,omitempty" json:"idem,omitempty"`
}

type WSSleepReq struct {
	ID             string            `cbor:"id" json:"id"`
	AfterSec       int64             `cbor:"after_sec,omitempty" json:"after_sec,omitempty"` // wake after N seconds
	AtMillis       int64             `cbor:"at,omitempty" json:"at,omitempty"`               // or at unix millis
	OnEvent        string            `cbor:"on,omitempty" json:"on,omitempty"`               // or when an event of this type is posted
	Match          map[string]string `cbor:"match,omitempty" json:"match,omitempty"`         // payload fields that must all match
	IdempotencyKey string            `cbor:"idem,omitempty" json:"idem,omitempty"`
}

// WSACLReq replaces a workspace's ACL. Principals present before and absent
// after are revoked: their grants stop verifying and their live sessions are
// closed within one renew interval.
type WSACLReq struct {
	ID             string       `cbor:"id" json:"id"`
	ACL            WorkspaceACL `cbor:"acl" json:"acl"`
	IdempotencyKey string       `cbor:"idem,omitempty" json:"idem,omitempty"`
}

type WSClaimReq struct {
	ID string `cbor:"id" json:"id"`
}

type WSClaimRes struct {
	Workspace Workspace `cbor:"workspace" json:"workspace"`
	LeaseSec  int64     `cbor:"lease_sec" json:"lease_sec"`
}

type WSReadyReq struct {
	ID  string `cbor:"id" json:"id"`
	Gen uint64 `cbor:"gen" json:"gen"`
}

type WSRenewReq struct {
	IDs []string          `cbor:"ids" json:"ids"`
	Gen map[string]uint64 `cbor:"gen,omitempty" json:"gen,omitempty"`
	// Authz is the authorization revision the node currently enforces for
	// each workspace (authz-push). Control answers with what changed since.
	Authz map[string]uint64 `cbor:"authz,omitempty" json:"authz,omitempty"`
}

// WSRenewResult is the control plane's affirmative ownership decision for
// one workspace named in WSRenewReq. Missing and rejected results require the
// node to fence that workspace.
type WSRenewResult struct {
	ID               string `cbor:"id" json:"id"`
	Generation       uint64 `cbor:"gen" json:"gen"`
	Accepted         bool   `cbor:"accepted" json:"accepted"`
	AuthoritativeGen uint64 `cbor:"authoritative_gen,omitempty" json:"authoritative_gen,omitempty"`
	LeaseUntil       int64  `cbor:"lease_until,omitempty" json:"lease_until,omitempty"`
	Action           string `cbor:"action" json:"action"` // continue | fence | destroy | reconcile
	// AuthzRevision is the authoritative authorization revision (authz-push).
	// Revoked lists principals that lost access after the revision the node
	// reported in WSRenewReq.Authz. AuthzReset means the node's revision is
	// older than the retained history: every session of the workspace must be
	// closed because control cannot name the affected principals.
	AuthzRevision uint64   `cbor:"authz_revision,omitempty" json:"authz_revision,omitempty"`
	Revoked       []string `cbor:"revoked,omitempty" json:"revoked,omitempty"`
	AuthzReset    bool     `cbor:"authz_reset,omitempty" json:"authz_reset,omitempty"`
}

type WSRenewRes struct {
	Results []WSRenewResult `cbor:"results" json:"results"`
}

type WSReleasedReq struct {
	ID       string `cbor:"id" json:"id"`
	Gen      uint64 `cbor:"gen" json:"gen"`
	Snapshot string `cbor:"snapshot,omitempty" json:"snapshot,omitempty"` // artifact id, if one was taken
	Reason   string `cbor:"reason,omitempty" json:"reason,omitempty"`
	// Failed marks a release caused by a materialization that could not
	// complete (restore, clone, policy). Control holds the workspace out of
	// placement with a growing delay instead of re-offering it immediately.
	Failed bool `cbor:"failed,omitempty" json:"failed,omitempty"`
	// Preparing is returned by a duplicate ws.release while the original
	// checkpoint is still running. It lets control poll without accumulating
	// blocked request handlers after a response timeout or reconnect.
	Preparing bool `cbor:"preparing,omitempty" json:"preparing,omitempty"`
}

type WSReleaseCommitReq struct {
	ID       string `cbor:"id" json:"id"`
	Gen      uint64 `cbor:"gen" json:"gen"`
	Snapshot string `cbor:"snapshot,omitempty" json:"snapshot,omitempty"`
}

type WSSnapshotCommitReq struct {
	ID       string `cbor:"id" json:"id"`
	Gen      uint64 `cbor:"gen" json:"gen"`
	Snapshot string `cbor:"snapshot" json:"snapshot"`
}

type NodeListRes struct {
	Nodes []NodeStatus `cbor:"nodes" json:"nodes"`
}

// WorkspaceSelector identifies incident-containment targets. Empty selectors
// are rejected unless All is explicit.
type WorkspaceSelector struct {
	All           bool              `cbor:"all,omitempty" json:"all,omitempty"`
	Tenant        string            `cbor:"tenant,omitempty" json:"tenant,omitempty"`
	Principal     string            `cbor:"principal,omitempty" json:"principal,omitempty"`
	Run           string            `cbor:"run,omitempty" json:"run,omitempty"`
	Node          string            `cbor:"node,omitempty" json:"node,omitempty"`
	Model         string            `cbor:"model,omitempty" json:"model,omitempty"`
	Backend       string            `cbor:"backend,omitempty" json:"backend,omitempty"`
	Labels        map[string]string `cbor:"labels,omitempty" json:"labels,omitempty"`
	CreatedAfter  int64             `cbor:"created_after,omitempty" json:"created_after,omitempty"`
	CreatedBefore int64             `cbor:"created_before,omitempty" json:"created_before,omitempty"`
}

const (
	FleetActionFreeze       = "freeze"
	FleetActionRevokeEgress = "revoke_egress"
	FleetActionCheckpoint   = "checkpoint"
	FleetActionStop         = "stop"
	FleetActionDestroy      = "destroy"

	FleetStatePending   = "pending"
	FleetStateRunning   = "running"
	FleetStateCompleted = "completed"
	FleetStatePartial   = "partial"

	FleetTargetPending      = "pending"
	FleetTargetAcknowledged = "acknowledged"
	FleetTargetFailed       = "failed"
)

// FleetOperation is one durable, selector-frozen containment request.
type FleetOperation struct {
	ID          string                 `cbor:"id" json:"id"`
	Selector    WorkspaceSelector      `cbor:"selector" json:"selector"`
	Action      string                 `cbor:"action" json:"action"`
	RequestedBy string                 `cbor:"requested_by" json:"requested_by"`
	Tenant      string                 `cbor:"tenant" json:"tenant"`
	State       string                 `cbor:"state" json:"state"`
	CreatedAt   int64                  `cbor:"created_at" json:"created_at"`
	Deadline    int64                  `cbor:"deadline" json:"deadline"`
	UpdatedAt   int64                  `cbor:"updated_at" json:"updated_at"`
	Results     []FleetOperationResult `cbor:"results" json:"results"`
}

// FleetOperationResult records containment progress for one frozen target.
type FleetOperationResult struct {
	Workspace    string `cbor:"workspace" json:"workspace"`
	Node         string `cbor:"node,omitempty" json:"node,omitempty"`
	Backend      string `cbor:"backend,omitempty" json:"backend,omitempty"`
	Generation   uint64 `cbor:"generation,omitempty" json:"generation,omitempty"`
	State        string `cbor:"state" json:"state"`
	Fenced       bool   `cbor:"fenced,omitempty" json:"fenced,omitempty"`
	Acknowledged bool   `cbor:"acknowledged" json:"acknowledged"`
	Snapshot     string `cbor:"snapshot,omitempty" json:"snapshot,omitempty"`
	Error        string `cbor:"error,omitempty" json:"error,omitempty"`
	UpdatedAt    int64  `cbor:"updated_at" json:"updated_at"`
}

// FleetQuarantineReq creates an idempotent fleet containment operation.
type FleetQuarantineReq struct {
	Selector       WorkspaceSelector `cbor:"selector" json:"selector"`
	Action         string            `cbor:"action" json:"action"`
	DeadlineMillis int64             `cbor:"deadline,omitempty" json:"deadline,omitempty"`
	TimeoutMillis  int64             `cbor:"timeout_ms,omitempty" json:"timeout_ms,omitempty"`
	IdempotencyKey string            `cbor:"idem,omitempty" json:"idem,omitempty"`
}

// FleetGetReq identifies a durable fleet operation.
type FleetGetReq struct {
	ID string `cbor:"id" json:"id"`
}

// FleetListRes contains fleet operations visible to the caller.
type FleetListRes struct {
	Operations []FleetOperation `cbor:"operations" json:"operations"`
}

// Base is a named, tenant-scoped snapshot that new workspaces can start from.
// The artifact stays pinned against garbage collection until the base is
// removed, which is what distinguishes it from a workspace's last snapshot.
type Base struct {
	Name      string `cbor:"name" json:"name"`
	Tenant    string `cbor:"tenant" json:"tenant"`
	Owner     string `cbor:"owner" json:"owner"`
	Artifact  string `cbor:"artifact" json:"artifact"`
	Workspace string `cbor:"workspace,omitempty" json:"workspace,omitempty"` // the workspace it was snapshotted from, when known
	Bytes     int64  `cbor:"bytes,omitempty" json:"bytes,omitempty"`
	CreatedAt int64  `cbor:"created_at" json:"created_at"`
}

// BaseCreateReq pins an uploaded artifact under a base name in the caller's
// tenant. Names are unique per tenant; a second create with the same name is a
// conflict unless it replays the same idempotency key.
type BaseCreateReq struct {
	Name           string `cbor:"name" json:"name"`
	Artifact       string `cbor:"artifact" json:"artifact"`
	Workspace      string `cbor:"workspace,omitempty" json:"workspace,omitempty"`
	IdempotencyKey string `cbor:"idem,omitempty" json:"idem,omitempty"`
}

// BaseListRes lists the bases visible to the caller, sorted by name.
type BaseListRes struct {
	Bases []Base `cbor:"bases" json:"bases"`
}

// Queue states.
const (
	QueueRunning = "running" // a task is running or is due to run next
	QueueDone    = "done"    // every task finished with exit 0
	QueueFailed  = "failed"  // Items[Cursor] exited non-zero; the cursor stays on it
)

// QueueItem is one task of a Queue with its outcome once it has run. Task text
// is control-plane data, never written into the workspace; events about the
// item carry only its index and exit.
type QueueItem struct {
	Task       string `cbor:"task" json:"task"`
	Session    string `cbor:"session,omitempty" json:"session,omitempty"`
	Exit       int    `cbor:"exit,omitempty" json:"exit,omitempty"`
	Signal     string `cbor:"signal,omitempty" json:"signal,omitempty"`
	Attempts   int    `cbor:"attempts,omitempty" json:"attempts,omitempty"`
	FinishedAt int64  `cbor:"finished_at,omitempty" json:"finished_at,omitempty"`
}

// Queue is a durable, ordered list of harness tasks for one workspace
// (ADR 0041). It survives moves of the workspace and restarts of the control
// plane because it is a control-plane resource; a driver reads it back and
// continues from Cursor.
type Queue struct {
	ID     string      `cbor:"id" json:"id"`
	WS     string      `cbor:"ws" json:"ws"`
	Tenant string      `cbor:"tenant" json:"tenant"`
	Owner  string      `cbor:"owner" json:"owner"`
	Recipe string      `cbor:"recipe,omitempty" json:"recipe,omitempty"`
	Items  []QueueItem `cbor:"items" json:"items"`
	Cursor int         `cbor:"cursor" json:"cursor"` // index of the next task to run; len(Items) when done
	Status string      `cbor:"status" json:"status"`
	// SleepAfterSec and SleepUntil record the pause between tasks the driver
	// asked for, so a resumed driver honors the same rhythm.
	SleepAfterSec int64  `cbor:"sleep_after_sec,omitempty" json:"sleep_after_sec,omitempty"`
	SleepUntil    string `cbor:"sleep_until,omitempty" json:"sleep_until,omitempty"` // HH:MM, local to the driver
	CreatedAt     int64  `cbor:"created_at" json:"created_at"`
	UpdatedAt     int64  `cbor:"updated_at" json:"updated_at"`
}

// Queue bounds. A queue is control-plane state written by a driver that may
// be a script, so the task list is sized like a request, not like a file.
const (
	MaxQueueItems    = 256
	MaxQueueTaskSize = 16 << 10
)

// ValidateQueueTasks is the admission check for a queue's task list, shared
// by the control plane and the CLI so a bad file is refused before anything
// is created.
func ValidateQueueTasks(tasks []string) error {
	if len(tasks) == 0 {
		return Err(CodeBadRequest, "queue needs at least one task")
	}
	if len(tasks) > MaxQueueItems {
		return Err(CodeBadRequest, "queue has %d tasks; the limit is %d", len(tasks), MaxQueueItems)
	}
	for i, t := range tasks {
		if strings.TrimSpace(t) == "" {
			return Err(CodeBadRequest, "queue task %d is empty", i)
		}
		if len(t) > MaxQueueTaskSize {
			return Err(CodeBadRequest, "queue task %d is %d bytes; the limit is %d", i, len(t), MaxQueueTaskSize)
		}
	}
	return nil
}

// QueueCreateReq creates a queue for a workspace the caller may write. One
// unfinished queue per workspace: a second create is a conflict.
type QueueCreateReq struct {
	WS             string   `cbor:"ws" json:"ws"`
	Recipe         string   `cbor:"recipe,omitempty" json:"recipe,omitempty"`
	Tasks          []string `cbor:"tasks" json:"tasks"`
	SleepAfterSec  int64    `cbor:"sleep_after_sec,omitempty" json:"sleep_after_sec,omitempty"`
	SleepUntil     string   `cbor:"sleep_until,omitempty" json:"sleep_until,omitempty"`
	IdempotencyKey string   `cbor:"idem,omitempty" json:"idem,omitempty"`
}

// QueueGetReq fetches one queue.
type QueueGetReq struct {
	ID string `cbor:"id" json:"id"`
}

// QueueListReq lists the caller's queues, optionally for one workspace.
type QueueListReq struct {
	WS string `cbor:"ws,omitempty" json:"ws,omitempty"`
}

// QueueListRes is the queues visible to the caller, oldest first.
type QueueListRes struct {
	Queues []Queue `cbor:"queues" json:"queues"`
}

// QueueAdvanceReq records the outcome of Items[Index], which must be the
// cursor. Exit 0 moves the cursor on; anything else marks the queue failed
// and leaves the cursor so a later driver can retry.
type QueueAdvanceReq struct {
	ID             string `cbor:"id" json:"id"`
	Index          int    `cbor:"index" json:"index"`
	Session        string `cbor:"session,omitempty" json:"session,omitempty"`
	Exit           int    `cbor:"exit" json:"exit"`
	Signal         string `cbor:"signal,omitempty" json:"signal,omitempty"`
	IdempotencyKey string `cbor:"idem,omitempty" json:"idem,omitempty"`
}

// BaseRemoveReq unpins a base. Workspaces already created from it keep their
// own RestoreFrom reference, so their artifact is unaffected.
type BaseRemoveReq struct {
	Name           string `cbor:"name" json:"name"`
	IdempotencyKey string `cbor:"idem,omitempty" json:"idem,omitempty"`
}

// PoolSpec declares provider-backed node capacity for exactly one tenant.
// Vendor credentials are server configuration and never cross this boundary.
type PoolSpec struct {
	Name               string            `cbor:"name" json:"name"`
	Vendor             string            `cbor:"vendor" json:"vendor"`
	Min                int               `cbor:"min" json:"min"`
	Max                int               `cbor:"max" json:"max"`
	Labels             map[string]string `cbor:"labels,omitempty" json:"labels,omitempty"`
	Backend            string            `cbor:"backend" json:"backend"`
	IdleScaleDownMilli int64             `cbor:"idle_scale_down_ms,omitempty" json:"idle_scale_down_ms,omitempty"`
	Region             string            `cbor:"region,omitempty" json:"region,omitempty"`
	Size               string            `cbor:"size,omitempty" json:"size,omitempty"`
}

// ValidatePoolSpec applies provider-independent admission checks. Provider
// credentials and bootstrap URLs are server configuration, not protocol data.
func ValidatePoolSpec(spec PoolSpec) error {
	name := strings.TrimSpace(spec.Name)
	if name == "" || len(name) > 64 || strings.HasPrefix(name, ".") || strings.ContainsAny(name, "/\\=\x00\n\r") {
		return Err(CodeBadRequest, "pool name must be 1-64 path-safe characters and may not start with '.'")
	}
	if strings.TrimSpace(spec.Vendor) == "" || strings.TrimSpace(spec.Backend) == "" {
		return Err(CodeBadRequest, "pool vendor and backend are required")
	}
	if spec.Min < 0 || spec.Max <= 0 || spec.Min > spec.Max {
		return Err(CodeBadRequest, "pool capacity must satisfy 0 <= min <= max, max > 0")
	}
	if spec.IdleScaleDownMilli < 0 {
		return Err(CodeBadRequest, "pool idle scale-down must not be negative")
	}
	for key, value := range spec.Labels {
		if strings.TrimSpace(key) == "" || strings.ContainsAny(key, "=\x00\n\r") || strings.ContainsAny(value, "\x00\n\r") {
			return Err(CodeBadRequest, "pool label %q is invalid", key)
		}
		if key == "remount.pool" || key == "remount.node" {
			return Err(CodeBadRequest, "pool label %q is reserved", key)
		}
	}
	return nil
}

// Pool is a durable, tenant-scoped desired-capacity resource. Current is the
// last observed provider inventory, not a scheduler grant.
type Pool struct {
	Spec      PoolSpec `cbor:"spec" json:"spec"`
	Tenant    string   `cbor:"tenant" json:"tenant"`
	Owner     string   `cbor:"owner" json:"owner"`
	Current   int      `cbor:"current" json:"current"`
	CreatedAt int64    `cbor:"created_at" json:"created_at"`
	UpdatedAt int64    `cbor:"updated_at" json:"updated_at"`
}

// PoolCreateReq creates one pool in the caller's tenant.
type PoolCreateReq struct {
	Spec           PoolSpec `cbor:"spec" json:"spec"`
	IdempotencyKey string   `cbor:"idem,omitempty" json:"idem,omitempty"`
}

// PoolGetReq identifies a pool by its tenant-unique name.
type PoolGetReq struct {
	Name string `cbor:"name" json:"name"`
}

// PoolListRes contains the pools visible to the caller.
type PoolListRes struct {
	Pools []Pool `cbor:"pools" json:"pools"`
}

// PoolRemoveReq removes a pool only after its provider inventory reaches
// zero; force-deleting machines is deliberately not a protocol side effect.
type PoolRemoveReq struct {
	Name           string `cbor:"name" json:"name"`
	IdempotencyKey string `cbor:"idem,omitempty" json:"idem,omitempty"`
}

type NodeStatus struct {
	ID         string            `cbor:"id" json:"id"`
	Labels     map[string]string `cbor:"labels,omitempty" json:"labels,omitempty"`
	Info       NodeInfo          `cbor:"info" json:"info"`
	Online     bool              `cbor:"online" json:"online"`
	LastSeen   int64             `cbor:"last_seen" json:"last_seen"`
	Workspaces []string          `cbor:"workspaces,omitempty" json:"workspaces,omitempty"`
	Protocol   []string          `cbor:"protocol,omitempty" json:"protocol,omitempty"` // capabilities negotiated at the node's last hello
}

type EventsTailReq struct {
	From         uint64 `cbor:"from" json:"from"`                 // first seq to deliver (0 = from beginning)
	Follow       bool   `cbor:"follow" json:"follow"`             // keep streaming
	WS           string `cbor:"ws,omitempty" json:"ws,omitempty"` // filter by workspace
	Subscription string `cbor:"sub,omitempty" json:"sub,omitempty"`
}

type EventsStopReq struct {
	Subscription string `cbor:"sub,omitempty" json:"sub,omitempty"`
}

// Event is one entry in the canonical log. Payload is CBOR.
type Event struct {
	Seq         uint64 `cbor:"seq" json:"seq"`
	At          int64  `cbor:"at" json:"at"`                             // unix millis
	Stream      string `cbor:"stream,omitempty" json:"stream,omitempty"` // ws id, node id, or "" for global
	Principal   string `cbor:"principal,omitempty" json:"principal,omitempty"`
	Node        string `cbor:"node,omitempty" json:"node,omitempty"`
	EventID     string `cbor:"event_id,omitempty" json:"event_id,omitempty"`
	ReceivedAt  int64  `cbor:"received_at,omitempty" json:"received_at,omitempty"`
	ObservedAt  int64  `cbor:"observed_at,omitempty" json:"observed_at,omitempty"`
	Origin      string `cbor:"origin,omitempty" json:"origin,omitempty"`
	Actor       string `cbor:"actor,omitempty" json:"actor,omitempty"`
	Tenant      string `cbor:"tenant,omitempty" json:"tenant,omitempty"`
	Workspace   string `cbor:"workspace,omitempty" json:"workspace,omitempty"`
	Generation  uint64 `cbor:"generation,omitempty" json:"generation,omitempty"`
	Session     string `cbor:"session,omitempty" json:"session,omitempty"` // session id for s.* events
	OperationID string `cbor:"operation_id,omitempty" json:"operation_id,omitempty"`
	ProducerSeq uint64 `cbor:"producer_seq,omitempty" json:"producer_seq,omitempty"`
	Type        string `cbor:"type" json:"type"`
	Payload     []byte `cbor:"payload,omitempty" json:"payload,omitempty"`
	Cause       uint64 `cbor:"cause,omitempty" json:"cause,omitempty"` // seq of the event that caused this one
}

// Time returns At as time.Time.
func (e Event) Time() time.Time { return time.UnixMilli(e.At) }

type EventPost struct {
	Events       []Event `cbor:"events" json:"events"`
	Subscription string  `cbor:"sub,omitempty" json:"sub,omitempty"`
}

type BindingLeaseReq struct {
	WS  string `cbor:"ws" json:"ws"`
	Gen uint64 `cbor:"gen,omitempty" json:"gen,omitempty"`
}

// BindingLease is what a node receives: the real secret, scoped and time-limited.
type BindingLease struct {
	ID           string   `cbor:"id" json:"id"`
	Secret       string   `cbor:"secret" json:"secret"`
	Destinations []string `cbor:"destinations" json:"destinations"`       // host[:port] patterns; "*.example.com" allowed
	Shape        string   `cbor:"shape,omitempty" json:"shape,omitempty"` // informational: http_header | basic_password | env
	Principals   []string `cbor:"principals,omitempty" json:"principals,omitempty"`
	ExpiresAt    int64    `cbor:"expires_at" json:"expires_at"` // unix millis; node must fail closed after this
	// Placeholder is what the workspace holds instead of the secret. Empty
	// means "ref:<id>". A shape-preserving value (same prefix/length as the
	// real secret) keeps client-side format validation happy.
	Placeholder string `cbor:"placeholder,omitempty" json:"placeholder,omitempty"`
}

type BindingLeaseRes struct {
	Leases []BindingLease `cbor:"leases" json:"leases"`
}

type GrantReq struct {
	WS string `cbor:"ws" json:"ws"`
}

// Grant authorizes a client peer to operate a workspace on a node. The node
// verifies the control plane's signature over the canonical encoding of
// GrantClaims before honoring any request.
type Grant struct {
	Claims    GrantClaims `cbor:"claims" json:"claims"`
	Signature []byte      `cbor:"sig" json:"sig"`
	Node      string      `cbor:"node" json:"node"` // where the workspace currently is
}

type GrantClaims struct {
	Client        string `cbor:"client" json:"client"`
	WS            string `cbor:"ws" json:"ws"`
	Node          string `cbor:"node" json:"node"`
	Principal     string `cbor:"principal,omitempty" json:"principal,omitempty"`
	Tenant        string `cbor:"tenant,omitempty" json:"tenant,omitempty"`
	AuthzRevision uint64 `cbor:"authz_revision,omitempty" json:"authz_revision,omitempty"`
	ExpiresAt     int64  `cbor:"exp" json:"exp"`
	Gen           uint64 `cbor:"gen" json:"gen"`
}

// Timer is a durable wake.
type Timer struct {
	ID        string            `cbor:"id" json:"id"`
	WS        string            `cbor:"ws" json:"ws"`
	At        int64             `cbor:"at,omitempty" json:"at,omitempty"` // unix millis
	OnEvent   string            `cbor:"on,omitempty" json:"on,omitempty"` // event type
	Match     map[string]string `cbor:"match,omitempty" json:"match,omitempty"`
	Action    string            `cbor:"action" json:"action"` // resume
	Fired     bool              `cbor:"fired" json:"fired"`
	FiredAt   int64             `cbor:"fired_at,omitempty" json:"fired_at,omitempty"`
	CreatedAt int64             `cbor:"created_at" json:"created_at"`
}

type TimerListRes struct {
	Timers []Timer `cbor:"timers" json:"timers"`
}

// ---------------------------------------------------------------------------
// Node operations (client -> node via relay)
// ---------------------------------------------------------------------------

const (
	OpSOpen   = "s.open"   // SOpenReq -> SOpenRes; chunks follow
	OpSAttach = "s.attach" // SAttachReq -> SOpenRes; chunks follow from From
	OpSInput  = "s.input"  // SInputReq -> {}
	OpSResize = "s.resize" // SResizeReq -> {}
	OpSSignal = "s.signal" // SSignalReq -> {}
	OpSAck    = "s.ack"    // SAckReq -> {} (flow control / liveness)
	OpSClose  = "s.close"  // SCloseReq -> {} (detach this client; process keeps running unless Kill)
	OpSList   = "s.list"   // SListReq -> SListRes
	OpSWait   = "s.wait"   // SWaitReq -> SWaitRes (blocks until exit or timeout)

	OpFSRead   = "fs.read"
	OpFSWrite  = "fs.write"
	OpFSList   = "fs.list"
	OpFSStat   = "fs.stat"
	OpFSMkdir  = "fs.mkdir"
	OpFSRemove = "fs.remove"
	OpFSRename = "fs.rename"
	OpFSSearch = "fs.search"
	OpFSEdit   = "fs.edit"
	// OpFSApplyTar overlays an uploaded artifact onto the workspace tree:
	// FSApplyTarReq -> FSApplyTarRes. Every file lands atomically by rename;
	// nothing the archive does not name is removed.
	OpFSApplyTar = "fs.apply_tar"

	OpWSSnapshot         = "ws.snapshot"          // WSSnapshotReq -> WSSnapshotRes
	OpWSRelease          = "ws.release"           // control -> node: WSReleaseReq -> WSReleasedReq
	OpWSInfo             = "ws.info"              // WSGetReq -> WSInfoRes
	OpNodeStatus         = "node.status"          // -> NodeStatus
	OpNodeDiag           = "node.diag"            // -> NodeDiag (deep: sessions, logs, disk, metrics)
	OpWSQuarantine       = "ws.quarantine"        // control -> node: WSQuarantineReq -> WSQuarantineRes
	OpWSQuarantineCommit = "ws.quarantine.commit" // control -> node: destroy a durably fenced source

	OpPortOpen = "port.open" // PortOpenReq -> SOpenRes (a session of kind port)
)

// Session kinds.
const (
	SessionExec = "exec" // pipes: stdout/stderr separate
	SessionPTY  = "pty"  // pseudo-terminal: single stream
	SessionPort = "port" // TCP forward: bytes both ways
	// SessionACP is a record-only transcript: the node's agent runner appends
	// every ACP frame it exchanges with the harness (StreamACPIn from the
	// harness, StreamACPOut to it) plus the harness's stderr. Clients cannot
	// open one; they attach to the one an agent run created.
	SessionACP = "acp"
)

// Chunk streams.
const (
	StreamStdout = 1
	StreamStderr = 2
	StreamExit   = 3 // Data = CBOR ExitInfo
	StreamInfo   = 4 // Data = CBOR SessionInfo (emitted once at open, replayable)
	StreamGap    = 5 // Data = CBOR Gap: chunks before this were evicted
	StreamACPIn  = 6 // Data = ACPFrameRecord: a JSON-RPC line the harness sent
	StreamACPOut = 7 // Data = ACPFrameRecord: a JSON-RPC line Remount sent to the harness
)

// ChunkBody is the payload of a chunk frame.
type ChunkBody struct {
	Stream uint8  `cbor:"st" json:"st"`
	Data   []byte `cbor:"d,omitempty" json:"d,omitempty"`
}

type ExitInfo struct {
	Code   int    `cbor:"code" json:"code"`
	Signal string `cbor:"signal,omitempty" json:"signal,omitempty"`
	Error  string `cbor:"error,omitempty" json:"error,omitempty"` // failed to start, etc.
	// Reason names why the node ended the session when the process did not
	// end on its own; empty for an ordinary exit.
	Reason string `cbor:"reason,omitempty" json:"reason,omitempty"`
}

// ExitReasonRevoked marks a session the node closed because its principal
// lost access to the workspace.
const ExitReasonRevoked = "revoked"

type SessionInfo struct {
	ID       string   `cbor:"id" json:"id"`
	WS       string   `cbor:"ws" json:"ws"`
	Kind     string   `cbor:"kind" json:"kind"`
	Program  []string `cbor:"program,omitempty" json:"program,omitempty"`
	PID      int      `cbor:"pid,omitempty" json:"pid,omitempty"`
	OpenedAt int64    `cbor:"opened_at" json:"opened_at"`
	Run      *RunInfo `cbor:"run,omitempty" json:"run,omitempty"`
}

// RunInfo marks a session as a harness launch (`remount run`). The node
// records it so run.started and run.finished are emitted by the peer that
// observes the process, not by a client that may have detached.
type RunInfo struct {
	Recipe string `cbor:"recipe" json:"recipe"`
	// TaskHash is a short digest of the task text; the text itself is never
	// put in the event log.
	TaskHash string `cbor:"task_hash,omitempty" json:"task_hash,omitempty"`
	Sandbox  string `cbor:"sandbox,omitempty" json:"sandbox,omitempty"`
	// Auth is RunAuthAPIKey or RunAuthWorkspaceResident.
	Auth string `cbor:"auth,omitempty" json:"auth,omitempty"`
}

// RunInfo.Auth values.
const (
	// RunAuthAPIKey: the harness reads a brokered provider key placeholder
	// from the environment.
	RunAuthAPIKey = "api_key"
	// RunAuthWorkspaceResident: the harness keeps its own login token in the
	// workspace, outside the broker's view.
	RunAuthWorkspaceResident = "workspace_resident"
)

// Validate rejects a RunInfo the node should not record.
func (r *RunInfo) Validate() error {
	if r == nil {
		return nil
	}
	if r.Recipe == "" || len(r.Recipe) > 64 {
		return Err(CodeBadRequest, "run.recipe is required and at most 64 bytes")
	}
	if len(r.TaskHash) > 64 || len(r.Sandbox) > 32 {
		return Err(CodeBadRequest, "run.task_hash or run.sandbox too long")
	}
	switch r.Auth {
	case "", RunAuthAPIKey, RunAuthWorkspaceResident:
		return nil
	}
	return Err(CodeBadRequest, "run.auth must be %s or %s", RunAuthAPIKey, RunAuthWorkspaceResident)
}

type Gap struct {
	From uint64 `cbor:"from" json:"from"` // first evicted seq
	To   uint64 `cbor:"to" json:"to"`     // last evicted seq
}

type SOpenReq struct {
	WS             string            `cbor:"ws" json:"ws"`
	Kind           string            `cbor:"kind" json:"kind"` // exec | pty
	Program        []string          `cbor:"program" json:"program"`
	Cwd            string            `cbor:"cwd,omitempty" json:"cwd,omitempty"`
	Env            map[string]string `cbor:"env,omitempty" json:"env,omitempty"`
	Rows           uint16            `cbor:"rows,omitempty" json:"rows,omitempty"`
	Cols           uint16            `cbor:"cols,omitempty" json:"cols,omitempty"`
	Stdin          bool              `cbor:"stdin" json:"stdin"` // exec: keep stdin open for input
	TimeoutSec     int64             `cbor:"timeout_sec,omitempty" json:"timeout_sec,omitempty"`
	IdempotencyKey string            `cbor:"idem,omitempty" json:"idem,omitempty"`
	// Grant authorizes this client for WS. Required on the first request per
	// (client, ws) on a connection; the node caches it.
	Grant *Grant `cbor:"grant,omitempty" json:"grant,omitempty"`
	// Subscribe: if true (default) chunks are streamed to this client from seq 0.
	NoSubscribe bool `cbor:"no_sub,omitempty" json:"no_sub,omitempty"`
	// Run marks a harness launch; see RunInfo.
	Run *RunInfo `cbor:"run,omitempty" json:"run,omitempty"`
}

type SOpenRes struct {
	S            string `cbor:"s" json:"s"`
	Next         uint64 `cbor:"next" json:"next"` // next seq the node will emit (for attach: where live begins)
	LastInputSeq uint64 `cbor:"last_iseq,omitempty" json:"last_iseq,omitempty"`
}

type SAttachReq struct {
	S     string `cbor:"s" json:"s"`
	From  uint64 `cbor:"from" json:"from"` // replay from this seq
	Grant *Grant `cbor:"grant,omitempty" json:"grant,omitempty"`
}

type SInputReq struct {
	S     string `cbor:"s" json:"s"`
	ISeq  uint64 `cbor:"iseq" json:"iseq"` // client-side input sequence; duplicates are dropped
	Data  []byte `cbor:"d,omitempty" json:"d,omitempty"`
	EOF   bool   `cbor:"eof,omitempty" json:"eof,omitempty"` // close stdin (exec only)
	Grant *Grant `cbor:"grant,omitempty" json:"grant,omitempty"`
}

type SResizeReq struct {
	S     string `cbor:"s" json:"s"`
	Rows  uint16 `cbor:"rows" json:"rows"`
	Cols  uint16 `cbor:"cols" json:"cols"`
	Grant *Grant `cbor:"grant,omitempty" json:"grant,omitempty"`
}

type SSignalReq struct {
	S      string `cbor:"s" json:"s"`
	Signal string `cbor:"signal" json:"signal"` // TERM, KILL, INT, HUP
	Grant  *Grant `cbor:"grant,omitempty" json:"grant,omitempty"`
}

type SAckReq struct {
	S     string `cbor:"s" json:"s"`
	Seq   uint64 `cbor:"seq" json:"seq"` // highest contiguous seq received
	Grant *Grant `cbor:"grant,omitempty" json:"grant,omitempty"`
}

type SCloseReq struct {
	S     string `cbor:"s" json:"s"`
	Kill  bool   `cbor:"kill,omitempty" json:"kill,omitempty"` // also terminate the process
	Grant *Grant `cbor:"grant,omitempty" json:"grant,omitempty"`
}

type SListReq struct {
	WS    string `cbor:"ws,omitempty" json:"ws,omitempty"`
	Grant *Grant `cbor:"grant,omitempty" json:"grant,omitempty"`
}

type SListRes struct {
	Sessions []SessionStatus `cbor:"sessions" json:"sessions"`
}

type SessionStatus struct {
	Info   SessionInfo `cbor:"info" json:"info"`
	Exited bool        `cbor:"exited" json:"exited"`
	Exit   *ExitInfo   `cbor:"exit,omitempty" json:"exit,omitempty"`
	Next   uint64      `cbor:"next" json:"next"`     // next seq
	Oldest uint64      `cbor:"oldest" json:"oldest"` // oldest replayable seq
}

type SWaitReq struct {
	S          string `cbor:"s" json:"s"`
	TimeoutSec int64  `cbor:"timeout_sec,omitempty" json:"timeout_sec,omitempty"`
	Grant      *Grant `cbor:"grant,omitempty" json:"grant,omitempty"`
}

type SWaitRes struct {
	Exited bool      `cbor:"exited" json:"exited"`
	Exit   *ExitInfo `cbor:"exit,omitempty" json:"exit,omitempty"`
}

type PortOpenReq struct {
	WS             string `cbor:"ws" json:"ws"`
	Port           int    `cbor:"port" json:"port"`
	Host           string `cbor:"host,omitempty" json:"host,omitempty"` // default 127.0.0.1 inside the workspace
	IdempotencyKey string `cbor:"idem,omitempty" json:"idem,omitempty"`
	Grant          *Grant `cbor:"grant,omitempty" json:"grant,omitempty"`
}

// ---- filesystem ----

type FSReadReq struct {
	WS     string `cbor:"ws" json:"ws"`
	Path   string `cbor:"path" json:"path"`
	Offset int64  `cbor:"offset,omitempty" json:"offset,omitempty"`
	Limit  int64  `cbor:"limit,omitempty" json:"limit,omitempty"` // bytes; 0 = default cap
	Grant  *Grant `cbor:"grant,omitempty" json:"grant,omitempty"`
}

type FSReadRes struct {
	Data []byte `cbor:"d" json:"d"`
	Size int64  `cbor:"size" json:"size"` // total file size
	EOF  bool   `cbor:"eof" json:"eof"`
}

type FSWriteReq struct {
	WS             string `cbor:"ws" json:"ws"`
	Path           string `cbor:"path" json:"path"`
	Data           []byte `cbor:"d" json:"d"`
	Mode           uint32 `cbor:"mode,omitempty" json:"mode,omitempty"`
	Append         bool   `cbor:"append,omitempty" json:"append,omitempty"`
	MkdirP         bool   `cbor:"mkdirp,omitempty" json:"mkdirp,omitempty"`
	IdempotencyKey string `cbor:"idem,omitempty" json:"idem,omitempty"`
	Grant          *Grant `cbor:"grant,omitempty" json:"grant,omitempty"`
}

type FSListReq struct {
	WS    string `cbor:"ws" json:"ws"`
	Path  string `cbor:"path" json:"path"`
	Grant *Grant `cbor:"grant,omitempty" json:"grant,omitempty"`
}

type FSEntry struct {
	Name    string `cbor:"name" json:"name"`
	Size    int64  `cbor:"size" json:"size"`
	Mode    uint32 `cbor:"mode" json:"mode"`
	IsDir   bool   `cbor:"dir" json:"dir"`
	ModTime int64  `cbor:"mtime" json:"mtime"`
	IsLink  bool   `cbor:"link,omitempty" json:"link,omitempty"`
}

type FSListRes struct {
	Entries []FSEntry `cbor:"entries" json:"entries"`
}

type FSStatReq = FSListReq
type FSStatRes struct {
	Entry FSEntry `cbor:"entry" json:"entry"`
}

type FSMkdirReq struct {
	WS             string `cbor:"ws" json:"ws"`
	Path           string `cbor:"path" json:"path"`
	IdempotencyKey string `cbor:"idem,omitempty" json:"idem,omitempty"`
	Grant          *Grant `cbor:"grant,omitempty" json:"grant,omitempty"`
}

type FSRemoveReq struct {
	WS             string `cbor:"ws" json:"ws"`
	Path           string `cbor:"path" json:"path"`
	Recursive      bool   `cbor:"recursive,omitempty" json:"recursive,omitempty"`
	IdempotencyKey string `cbor:"idem,omitempty" json:"idem,omitempty"`
	Grant          *Grant `cbor:"grant,omitempty" json:"grant,omitempty"`
}

type FSRenameReq struct {
	WS             string `cbor:"ws" json:"ws"`
	From           string `cbor:"from" json:"from"`
	To             string `cbor:"to" json:"to"`
	IdempotencyKey string `cbor:"idem,omitempty" json:"idem,omitempty"`
	Grant          *Grant `cbor:"grant,omitempty" json:"grant,omitempty"`
}

type FSSearchReq struct {
	WS         string `cbor:"ws" json:"ws"`
	Path       string `cbor:"path" json:"path"`                     // root to search
	Pattern    string `cbor:"pattern" json:"pattern"`               // RE2 regexp
	Glob       string `cbor:"glob,omitempty" json:"glob,omitempty"` // filename glob filter
	MaxResults int    `cbor:"max,omitempty" json:"max,omitempty"`
	Grant      *Grant `cbor:"grant,omitempty" json:"grant,omitempty"`
}

type FSMatch struct {
	Path string `cbor:"path" json:"path"`
	Line int    `cbor:"line" json:"line"`
	Text string `cbor:"text" json:"text"`
}

type FSSearchRes struct {
	Matches   []FSMatch `cbor:"matches" json:"matches"`
	Truncated bool      `cbor:"truncated" json:"truncated"`
}

// FSEdit is one find/replace. All edits in a request are applied atomically:
// either every Old is found exactly once (or Count times) and replaced, or
// the file is untouched.
type FSEdit struct {
	Old string `cbor:"old" json:"old"`
	New string `cbor:"new" json:"new"`
	All bool   `cbor:"all,omitempty" json:"all,omitempty"` // replace all occurrences
}

type FSEditReq struct {
	WS             string   `cbor:"ws" json:"ws"`
	Path           string   `cbor:"path" json:"path"`
	Edits          []FSEdit `cbor:"edits" json:"edits"`
	IdempotencyKey string   `cbor:"idem,omitempty" json:"idem,omitempty"`
	Grant          *Grant   `cbor:"grant,omitempty" json:"grant,omitempty"`
}

type FSEditRes struct {
	Replacements int `cbor:"replacements" json:"replacements"`
}

// FSApplyTarReq names an artifact already present in the control plane's
// store (uploaded with PUT /v1/artifacts/{id}) to overlay onto the workspace.
type FSApplyTarReq struct {
	WS             string `cbor:"ws" json:"ws"`
	Artifact       string `cbor:"artifact" json:"artifact"`
	IdempotencyKey string `cbor:"idem,omitempty" json:"idem,omitempty"`
	Grant          *Grant `cbor:"grant,omitempty" json:"grant,omitempty"`
}

// FSApplyTarRes reports what the overlay wrote. Files counts regular files
// and symlinks that were created or replaced.
type FSApplyTarRes struct {
	Files int   `cbor:"files" json:"files"`
	Dirs  int   `cbor:"dirs" json:"dirs"`
	Bytes int64 `cbor:"bytes" json:"bytes"`
}

// ---- workspace on node ----

type WSSnapshotReq struct {
	WS     string `cbor:"ws" json:"ws"`
	Upload bool   `cbor:"upload" json:"upload"` // push to the control plane's artifact store
	// Authoritative requests a quiesced checkpoint. It requires Upload and
	// terminates process-backend sessions; a live snapshot is never committed
	// as failover state.
	Authoritative  bool   `cbor:"authoritative,omitempty" json:"authoritative,omitempty"`
	IdempotencyKey string `cbor:"idem,omitempty" json:"idem,omitempty"`
	Grant          *Grant `cbor:"grant,omitempty" json:"grant,omitempty"`
	// Gen fences a control-plane-issued snapshot (agent fork) to the workspace
	// generation the control plane believes the node holds. Clients leave it
	// zero; their grant carries the generation.
	Gen uint64 `cbor:"gen,omitempty" json:"gen,omitempty"`
}

type WSSnapshotRes struct {
	Artifact      string `cbor:"artifact" json:"artifact"` // art_sha256:<hex>
	Bytes         int64  `cbor:"bytes" json:"bytes"`
	Consistency   string `cbor:"consistency" json:"consistency"`
	Authoritative bool   `cbor:"authoritative" json:"authoritative"`
}

const (
	SnapshotConsistencyLive     = "live"
	SnapshotConsistencyQuiesced = "quiesced"
)

type WSReleaseReq struct {
	WS       string `cbor:"ws" json:"ws"`
	Gen      uint64 `cbor:"gen" json:"gen"`
	Snapshot bool   `cbor:"snapshot" json:"snapshot"` // take + upload a snapshot before releasing
	Reason   string `cbor:"reason,omitempty" json:"reason,omitempty"`
}

// WSQuarantineReq asks the recorded holder to fence and optionally checkpoint
// a workspace as phase one of a fleet operation.
type WSQuarantineReq struct {
	OperationID string       `cbor:"operation" json:"operation"`
	WS          string       `cbor:"ws" json:"ws"`
	Gen         uint64       `cbor:"gen" json:"gen"`
	Action      string       `cbor:"action" json:"action"`
	Backend     string       `cbor:"backend,omitempty" json:"backend,omitempty"`
	Exclude     []string     `cbor:"exclude,omitempty" json:"exclude,omitempty"`
	Security    SecuritySpec `cbor:"security,omitempty" json:"security,omitempty"`
}

// WSQuarantineRes is the node's durable phase-one containment proof.
type WSQuarantineRes struct {
	Fenced     bool   `cbor:"fenced" json:"fenced"`
	Generation uint64 `cbor:"gen" json:"gen"`
	Action     string `cbor:"action" json:"action"`
	Backend    string `cbor:"backend,omitempty" json:"backend,omitempty"`
	Snapshot   string `cbor:"snapshot,omitempty" json:"snapshot,omitempty"`
	Warning    string `cbor:"warning,omitempty" json:"warning,omitempty"`
}

// WSQuarantineCommitReq authorizes physical deletion after control has
// durably committed a matching phase-one proof and generation fence.
type WSQuarantineCommitReq struct {
	OperationID string `cbor:"operation" json:"operation"`
	WS          string `cbor:"ws" json:"ws"`
	Gen         uint64 `cbor:"gen" json:"gen"`
	Backend     string `cbor:"backend" json:"backend"`
	Snapshot    string `cbor:"snapshot" json:"snapshot"`
}

type WSInfoRes struct {
	WS       string   `cbor:"ws" json:"ws"`
	Backend  string   `cbor:"backend" json:"backend"`
	Root     string   `cbor:"root,omitempty" json:"root,omitempty"`
	Sessions []string `cbor:"sessions,omitempty" json:"sessions,omitempty"`
	Broker   string   `cbor:"broker,omitempty" json:"broker,omitempty"` // base URL of the egress broker as seen from inside
}

// ---------------------------------------------------------------------------
// Diagnostics
//
// These exist so a human or an agent can ask the system what it thinks is
// true, at three depths: the fleet (ControlDiag), one machine (NodeDiag), and
// one session's log (SessionStatus, already defined above).
// ---------------------------------------------------------------------------

// ControlDiag is the control plane's own view of its health.
type ControlDiag struct {
	Now                     int64              `cbor:"now" json:"now"`
	Uptime                  int64              `cbor:"uptime_sec" json:"uptime_sec"`
	Version                 string             `cbor:"version,omitempty" json:"version,omitempty"`
	WorkspaceState          map[string]int     `cbor:"ws_state" json:"ws_state"` // state -> count
	NodesTotal              int                `cbor:"nodes_total" json:"nodes_total"`
	NodesOnline             int                `cbor:"nodes_online" json:"nodes_online"`
	TimersPending           int                `cbor:"timers_pending" json:"timers_pending"`
	TimersFired             int                `cbor:"timers_fired" json:"timers_fired"`
	TimersMax               int                `cbor:"timers_max,omitempty" json:"timers_max,omitempty"`
	MutationRecords         int                `cbor:"mutation_records,omitempty" json:"mutation_records,omitempty"`
	MutationRecordsMax      int                `cbor:"mutation_records_max,omitempty" json:"mutation_records_max,omitempty"`
	EventSeq                uint64             `cbor:"event_seq" json:"event_seq"`
	EventOldest             uint64             `cbor:"event_oldest,omitempty" json:"event_oldest,omitempty"`
	EventsMax               int                `cbor:"events_max,omitempty" json:"events_max,omitempty"`
	RequestsActiveMax       int                `cbor:"requests_active_max,omitempty" json:"requests_active_max,omitempty"`
	Bindings                []string           `cbor:"bindings,omitempty" json:"bindings,omitempty"`
	ArtifactCount           int                `cbor:"artifact_count" json:"artifact_count"`
	ArtifactBytes           int64              `cbor:"artifact_bytes" json:"artifact_bytes"`
	ArtifactReservedBytes   int64              `cbor:"artifact_reserved_bytes,omitempty" json:"artifact_reserved_bytes,omitempty"`
	ArtifactReservedObjects int                `cbor:"artifact_reserved_objects,omitempty" json:"artifact_reserved_objects,omitempty"`
	ArtifactMaxBytes        int64              `cbor:"artifact_max_bytes,omitempty" json:"artifact_max_bytes,omitempty"`
	ArtifactMaxObjects      int                `cbor:"artifact_max_objects,omitempty" json:"artifact_max_objects,omitempty"`
	WorkspacesPerTenantMax  int                `cbor:"workspaces_per_tenant_max,omitempty" json:"workspaces_per_tenant_max,omitempty"`
	WorkspacesPerSubjectMax int                `cbor:"workspaces_per_subject_max,omitempty" json:"workspaces_per_subject_max,omitempty"`
	DBIntegrity             string             `cbor:"db_integrity,omitempty" json:"db_integrity,omitempty"`
	LeaseSec                int64              `cbor:"lease_sec" json:"lease_sec"`
	Metrics                 map[string]float64 `cbor:"metrics,omitempty" json:"metrics,omitempty"`
	// Findings are problems the control plane can see by itself.
	Findings []Finding `cbor:"findings,omitempty" json:"findings,omitempty"`
}

// Finding is one diagnostic problem. Severity is "info", "warn" or "error".
type Finding struct {
	Severity string `cbor:"severity" json:"severity"`
	Check    string `cbor:"check" json:"check"`
	Subject  string `cbor:"subject,omitempty" json:"subject,omitempty"`
	Detail   string `cbor:"detail" json:"detail"`
	Hint     string `cbor:"hint,omitempty" json:"hint,omitempty"`
}

// NodeDiagReq asks a node for its deep state. Diagnostics expose workspace
// roots and session programs, so the caller must prove it is entitled to at
// least one workspace on that node.
type NodeDiagReq struct {
	WS    string `cbor:"ws,omitempty" json:"ws,omitempty"`
	Grant *Grant `cbor:"grant,omitempty" json:"grant,omitempty"`
	// Verify re-hashes every cached artifact. It reads every byte, so it is
	// the honest integrity check rather than the fast one.
	Verify bool `cbor:"verify,omitempty" json:"verify,omitempty"`
}

// DiagReq asks the control plane for its health.
type DiagReq struct {
	Verify bool `cbor:"verify,omitempty" json:"verify,omitempty"`
}

// NodeDiag is one machine's deep state.
type NodeDiag struct {
	Node                     string             `cbor:"node" json:"node"`
	Info                     NodeInfo           `cbor:"info" json:"info"`
	Now                      int64              `cbor:"now" json:"now"`
	Uptime                   int64              `cbor:"uptime_sec" json:"uptime_sec"`
	DataDir                  string             `cbor:"data_dir" json:"data_dir"`
	DiskFree                 int64              `cbor:"disk_free_bytes" json:"disk_free_bytes"`
	DiskTotal                int64              `cbor:"disk_total_bytes" json:"disk_total_bytes"`
	Goroutines               int                `cbor:"goroutines" json:"goroutines"`
	HeapBytes                uint64             `cbor:"heap_bytes" json:"heap_bytes"`
	Workspaces               []WSDiag           `cbor:"workspaces,omitempty" json:"workspaces,omitempty"`
	Artifacts                int                `cbor:"artifacts" json:"artifacts"`
	ArtifactBytes            int64              `cbor:"artifact_bytes,omitempty" json:"artifact_bytes,omitempty"`
	ArtifactReservedBytes    int64              `cbor:"artifact_reserved_bytes,omitempty" json:"artifact_reserved_bytes,omitempty"`
	ArtifactReservedObjects  int                `cbor:"artifact_reserved_objects,omitempty" json:"artifact_reserved_objects,omitempty"`
	ArtifactMaxBytes         int64              `cbor:"artifact_max_bytes,omitempty" json:"artifact_max_bytes,omitempty"`
	ArtifactMaxObjects       int                `cbor:"artifact_max_objects,omitempty" json:"artifact_max_objects,omitempty"`
	SessionsRetained         int                `cbor:"sessions_retained,omitempty" json:"sessions_retained,omitempty"`
	SessionsActive           int                `cbor:"sessions_active,omitempty" json:"sessions_active,omitempty"`
	SessionsMax              int                `cbor:"sessions_max,omitempty" json:"sessions_max,omitempty"`
	SessionsActiveMax        int                `cbor:"sessions_active_max,omitempty" json:"sessions_active_max,omitempty"`
	SessionsPerWorkspaceMax  int                `cbor:"sessions_per_workspace_max,omitempty" json:"sessions_per_workspace_max,omitempty"`
	SessionsPerPrincipalMax  int                `cbor:"sessions_per_principal_max,omitempty" json:"sessions_per_principal_max,omitempty"`
	SessionMemoryBytes       int                `cbor:"session_memory_bytes,omitempty" json:"session_memory_bytes,omitempty"`
	SessionSpillBytes        int64              `cbor:"session_spill_bytes,omitempty" json:"session_spill_bytes,omitempty"`
	SessionMemoryChunks      int                `cbor:"session_memory_chunks,omitempty" json:"session_memory_chunks,omitempty"`
	SessionChunkBytes        int                `cbor:"session_chunk_bytes,omitempty" json:"session_chunk_bytes,omitempty"`
	RequestsActiveMax        int                `cbor:"requests_active_max,omitempty" json:"requests_active_max,omitempty"`
	ConnectorMaxBytes        int64              `cbor:"connector_max_bytes,omitempty" json:"connector_max_bytes,omitempty"`
	ConnectorScopeMaxBytes   int64              `cbor:"connector_scope_max_bytes,omitempty" json:"connector_scope_max_bytes,omitempty"`
	ConnectorObjectMaxBytes  int64              `cbor:"connector_object_max_bytes,omitempty" json:"connector_object_max_bytes,omitempty"`
	ConnectorMaxObjects      int64              `cbor:"connector_max_objects,omitempty" json:"connector_max_objects,omitempty"`
	ConnectorScopeMaxObjects int64              `cbor:"connector_scope_max_objects,omitempty" json:"connector_scope_max_objects,omitempty"`
	MutationRecords          int                `cbor:"mutation_records,omitempty" json:"mutation_records,omitempty"`
	MutationRecordsMax       int                `cbor:"mutation_records_max,omitempty" json:"mutation_records_max,omitempty"`
	SnapshotsActive          int                `cbor:"snapshots_active,omitempty" json:"snapshots_active,omitempty"`
	SnapshotsActiveMax       int                `cbor:"snapshots_active_max,omitempty" json:"snapshots_active_max,omitempty"`
	Metrics                  map[string]float64 `cbor:"metrics,omitempty" json:"metrics,omitempty"`
	Findings                 []Finding          `cbor:"findings,omitempty" json:"findings,omitempty"`
}

// WSDiag is one workspace as the node holding it sees it.
type WSDiag struct {
	ID        string          `cbor:"id" json:"id"`
	Gen       uint64          `cbor:"gen" json:"gen"`
	Backend   string          `cbor:"backend" json:"backend"`
	Root      string          `cbor:"root" json:"root"`
	Bytes     int64           `cbor:"bytes" json:"bytes"`
	Files     int             `cbor:"files" json:"files"`
	Broker    string          `cbor:"broker,omitempty" json:"broker,omitempty"`
	Bindings  []string        `cbor:"bindings,omitempty" json:"bindings,omitempty"`
	LeaseEnds int64           `cbor:"lease_ends,omitempty" json:"lease_ends,omitempty"`
	Sessions  []SessionStatus `cbor:"sessions,omitempty" json:"sessions,omitempty"`
}

// ---------------------------------------------------------------------------
// Event types (canonical names)
// ---------------------------------------------------------------------------

const (
	EvNodeEnrolled      = "node.enrolled"
	EvNodeOnline        = "node.online"
	EvNodeOffline       = "node.offline"
	EvWSCreated         = "ws.created"
	EvWSOffered         = "ws.offered"
	EvWSClaiming        = "ws.claiming"
	EvWSClaimed         = "ws.claimed"
	EvWSReleased        = "ws.released"
	EvWSMoved           = "ws.moved"
	EvWSPaused          = "ws.paused"
	EvWSResumed         = "ws.resumed"
	EvWSSnapshot        = "ws.snapshot"
	EvWSRestored        = "ws.restored"
	EvWSDestroyed       = "ws.destroyed"
	EvWSLeaseExpired    = "ws.lease_expired"
	EvWSFenced          = "ws.fenced"
	EvWSStateChanged    = "ws.state_changed"
	EvWSACL             = "ws.acl"        // ACL replaced; payload names revoked principals and the new revision
	EvAuthzRevoked      = "authz.revoked" // node closed a revoked principal's sessions
	EvSOpened           = "s.opened"
	EvSExited           = "s.exited"
	EvSInput            = "s.input"
	EvFSWrite           = "fs.write"
	EvFSApplyTar        = "fs.apply_tar" // one overlay applied; payload carries artifact and counts, fs.write follows per path
	EvFSMkdir           = "fs.mkdir"
	EvFSEdit            = "fs.edit"
	EvFSRemove          = "fs.remove"
	EvFSRename          = "fs.rename"
	EvCredUsed          = "cred.used"
	EvEgressPending     = "egress.pending"
	EvEgressAllowed     = "egress.allowed"
	EvEgressDenied      = "egress.denied"
	EvEgressRedacted    = "egress.redacted"
	EvPolicyUpdated     = "policy.updated"
	EvTimerSet          = "timer.set"
	EvTimerFired        = "timer.fired"
	EvPeerGone          = "peer.gone"
	EvEventGap          = "event.producer_gap"
	EvFleetRequested    = "fleet.quarantine.requested"
	EvFleetTarget       = "fleet.quarantine.target"
	EvFleetCompleted    = "fleet.quarantine.completed"
	EvBaseCreated       = "base.created"
	EvBaseRemoved       = "base.removed"
	EvQueueCreated      = "queue.created"                // payload {queue, ws, items}
	EvQueueAdvanced     = "queue.advanced"               // one queued task finished; payload {queue, index, exit, status}
	EvPoolCreated       = "pool.created"                 // a durable pool specification was admitted
	EvPoolRemoved       = "pool.removed"                 // an empty pool specification was removed
	EvPoolScaled        = "pool.scaled"                  // payload {pool, from, to, reason}
	EvPoolFailed        = "pool.provision_failed"        // payload {pool, reason, retry_at}; no credentials
	EvExportAdvanced    = "export.cursor.advanced"       // a durable destination cursor advanced after accepting a batch
	EvNotifyUnavailable = "notifier.unavailable"         // delivery or its durable fallback is unavailable; payload is sanitized
	EvNotifyDeadLetter  = "notifier.dead_lettered"       // failed delivery metadata was durably retained
	EvNotifyDLQPruned   = "notifier.dead_letters_pruned" // bounded retention removed old dead-letter metadata
	EvRunStarted        = "run.started"                  // a harness launch opened its session; payload {s, recipe, task_hash, sandbox, auth}
	EvRunFinished       = "run.finished"                 // that session exited; payload {s, recipe, exit, signal}
	EvAuthWSResident    = "auth.workspace_resident"      // a launch relies on a login the harness keeps inside the workspace; payload {s, recipe}
	EvWSOffer           = "ws.offer"                     // control -> node (not logged; a hint to claim)
)
