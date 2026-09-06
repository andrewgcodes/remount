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
	// ControllerEpoch is the durable writer epoch acquired before this hello.
	ControllerEpoch uint64 `cbor:"controller_epoch,omitempty" json:"controller_epoch,omitempty"`
	Subject         string `cbor:"subject,omitempty" json:"subject,omitempty"`
	Tenant          string `cbor:"tenant,omitempty" json:"tenant,omitempty"`
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
	// Profile is the runtime profile this node was configured with. Like
	// Caps it is a self-claim and is never security evidence: the control
	// plane decides which profiles a node satisfies from BackendDescriptors.
	// It is reported so an operator can see a node whose configuration and
	// evidence disagree.
	Profile string `cbor:"profile,omitempty" json:"profile,omitempty"`
	// RuntimeChecks are host prerequisites only the node can observe (KVM,
	// runsc, netlink capability, cgroup parent). The control plane trusts
	// them only to DOWNGRADE a profile result: a failing or unavailable
	// check makes the node unschedulable for the profiles that need it, and
	// a passing check never satisfies a descriptor predicate by itself.
	RuntimeChecks []Finding `cbor:"runtime_checks,omitempty" json:"runtime_checks,omitempty"`
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
	// Profile is the runtime profile a node must currently satisfy to run
	// this workspace ("dev", "trusted-single-tenant",
	// "multi-tenant-isolated", "microvm"). Unlike Caps it is not matched
	// against anything the node claims: the control plane evaluates the
	// node's backend descriptors and its reported host checks. Empty means
	// no runtime-profile constraint.
	Profile string `cbor:"profile,omitempty" json:"profile,omitempty"`
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
	// RestoreFormat distinguishes the legacy deterministic tar.gz body from a
	// canonical chunk manifest. Empty means tar for wire compatibility.
	RestoreFormat string `cbor:"restore_format,omitempty" json:"restore_format,omitempty"`
	// RestoreObjects is the control-verified, tenant-local transitive closure
	// of RestoreFrom. It is durable GC metadata, never caller authority.
	RestoreObjects []string          `cbor:"restore_objects,omitempty" json:"restore_objects,omitempty"`
	Base           string            `cbor:"base,omitempty" json:"base,omitempty"` // tenant base name; control resolves it into RestoreFrom at create
	Requires       Requires          `cbor:"requires" json:"requires"`
	Placement      Placement         `cbor:"placement" json:"placement"`
	Bindings       []string          `cbor:"bindings,omitempty" json:"bindings,omitempty"`   // secret binding ids this workspace may use
	Env            map[string]string `cbor:"env,omitempty" json:"env,omitempty"`             // default env for sessions; values may be ref:…
	Principal      string            `cbor:"principal,omitempty" json:"principal,omitempty"` // who acts through this workspace (a_…)
	Idle           Idle              `cbor:"idle" json:"idle"`
	Exclude        []string          `cbor:"exclude,omitempty" json:"exclude,omitempty"` // snapshot path globs to skip (node_modules, .venv …)
	Security       SecuritySpec      `cbor:"security,omitempty" json:"security,omitempty"`
	ACL            WorkspaceACL      `cbor:"acl,omitempty" json:"acl,omitempty"`
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
	// Volumes are immutable artifact versions mounted read-only into the
	// workspace. The control plane resolves ID-only declarations to a pinned
	// Version and Artifact before a node can claim the workspace.
	Volumes []VolumeMount `cbor:"volumes,omitempty" json:"volumes,omitempty"`
}

// VolumeMount pins one immutable shared-data volume version at a clean,
// absolute workspace path. Version and Artifact are control-plane resolved;
// callers normally supply only ID and Path.
type VolumeMount struct {
	ID       string `cbor:"id" json:"id"`
	Path     string `cbor:"path" json:"path"`
	Version  uint64 `cbor:"version,omitempty" json:"version,omitempty"`
	Artifact string `cbor:"artifact,omitempty" json:"artifact,omitempty"`
}

const (
	// MaxWorkspaceVolumes bounds declarations copied into every offer and
	// persisted with every workspace transition.
	MaxWorkspaceVolumes = 64
	// MaxVolumeVersions bounds durable artifact references per volume.
	MaxVolumeVersions = 128
	// CapabilityReadOnlyVolumes is advertised only after the node proves it
	// can establish the required read-only bind mount.
	CapabilityReadOnlyVolumes = "readonly-volumes"
)

// ValidateVolumeMount applies the path and identity rules shared by control,
// clients and nodes. Shared data may not shadow the workspace root or system
// paths and is always read-only by protocol definition.
func ValidateVolumeMount(m VolumeMount) error {
	if err := ValidateVolumeID(m.ID); err != nil {
		return err
	}
	if len(m.Path) > 1024 || !strings.HasPrefix(m.Path, "/") || path.Clean(m.Path) != m.Path || strings.Contains(m.Path, "\x00") {
		return Err(CodeBadRequest, "volume path %q must be a clean absolute path", m.Path)
	}
	for _, reserved := range []string{"/", "/proc", "/sys", "/dev", "/etc", "/bin", "/sbin", "/lib", "/usr", "/var", "/run", "/boot", "/.remount"} {
		if m.Path == reserved || strings.HasPrefix(m.Path, reserved+"/") {
			return Err(CodeBadRequest, "volume path %q is reserved", m.Path)
		}
	}
	return nil
}

// ValidateVolumeID enforces the one identifier grammar shared by the wire,
// control plane, CLI delimiter and node-local catalog.
func ValidateVolumeID(id string) error {
	if id == "" || len(id) > 64 || strings.HasPrefix(id, ".") {
		return Err(CodeBadRequest, "volume id must be 1-64 path-safe characters and may not start with '.'")
	}
	for _, char := range id {
		if (char >= 'a' && char <= 'z') || (char >= 'A' && char <= 'Z') || (char >= '0' && char <= '9') || strings.ContainsRune("._-", char) {
			continue
		}
		return Err(CodeBadRequest, "volume id must contain only ASCII letters, digits, '.', '_' or '-'")
	}
	return nil
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
	ID                  string        `cbor:"id" json:"id"`
	Spec                WorkspaceSpec `cbor:"spec" json:"spec"`
	State               string        `cbor:"state" json:"state"`
	Node                string        `cbor:"node,omitempty" json:"node,omitempty"`
	LeaseUntil          int64         `cbor:"lease_until,omitempty" json:"lease_until,omitempty"`     // unix millis
	LastSnapshot        string        `cbor:"last_snapshot,omitempty" json:"last_snapshot,omitempty"` // artifact id
	LastSnapshotFormat  string        `cbor:"last_snapshot_format,omitempty" json:"last_snapshot_format,omitempty"`
	LastSnapshotObjects []string      `cbor:"last_snapshot_objects,omitempty" json:"last_snapshot_objects,omitempty"`
	CreatedAt           int64         `cbor:"created_at" json:"created_at"`
	UpdatedAt           int64         `cbor:"updated_at" json:"updated_at"`
	Generation          uint64        `cbor:"gen" json:"gen"` // bumps on every claim; a node with a stale gen must not act
	Tenant              string        `cbor:"tenant,omitempty" json:"tenant,omitempty"`
	Owner               string        `cbor:"owner,omitempty" json:"owner,omitempty"`
	AuthzRevision       uint64        `cbor:"authz_revision,omitempty" json:"authz_revision,omitempty"`
	// Revocations records which principals lost access at which authorization
	// revision, newest last, so a node renewing from an older revision learns
	// exactly whose sessions to close. Entries below RevocationFloor have been
	// pruned; a node behind the floor must fail closed for the whole workspace.
	Revocations     []AuthzRevocation `cbor:"revocations,omitempty" json:"revocations,omitempty"`
	RevocationFloor uint64            `cbor:"revocation_floor,omitempty" json:"revocation_floor,omitempty"`
	// ReleaseEpoch orders control-authorized release cycles for one workspace.
	// It survives abort publication so a node can reject delayed older cycles.
	ReleaseEpoch uint64 `cbor:"release_epoch,omitempty" json:"release_epoch,omitempty"`
	// ReleaseOperation identifies the release or release-abort handshake
	// currently controlling this workspace generation.
	ReleaseOperation string `cbor:"release_operation,omitempty" json:"release_operation,omitempty"`
	// QuarantineOperation identifies the durable fleet operation that fenced
	// this workspace. It prevents restart reconciliation from treating an
	// incident response as an ordinary transient failure.
	QuarantineOperation string `cbor:"quarantine_operation,omitempty" json:"quarantine_operation,omitempty"`
	QuarantinedAt       int64  `cbor:"quarantined_at,omitempty" json:"quarantined_at,omitempty"`
	// PendingReason explains why a pending workspace has not been placed. It
	// is derived at read time from the current fleet, never persisted and
	// never an input to a decision, so it emits no event of its own. It
	// carries a Reason* constant, currently only ReasonProfileUnschedulable.
	PendingReason string `cbor:"pending_reason,omitempty" json:"pending_reason,omitempty"`
	// Lease is the durable hold that keeps this workspace awake (ADR 0090).
	// IdlePolicy, IdleSince and LastActivityAt are the no-work cleanup rule
	// and its clock. LifecycleDeadline is the derived view of what the
	// control plane will do next, so a client never has to read timers.
	//
	// All four are replaced wholesale, never mutated through the pointer, so
	// a copy taken from under the control-plane mutex stays valid.
	Lease             *WorkspaceLease    `cbor:"lease,omitempty" json:"lease,omitempty"`
	IdlePolicy        *IdlePolicy        `cbor:"idle_policy,omitempty" json:"idle_policy,omitempty"`
	IdleSince         int64              `cbor:"idle_since,omitempty" json:"idle_since,omitempty"`
	LastActivityAt    int64              `cbor:"last_activity_at,omitempty" json:"last_activity_at,omitempty"`
	LifecycleDeadline *LifecycleDeadline `cbor:"lifecycle_deadline,omitempty" json:"lifecycle_deadline,omitempty"`
}

// CloneLifecycle returns a copy of the lifecycle pointers, so a caller holding
// a shallow Workspace copy can hand it out without sharing rows the control
// plane still owns.
func (w *Workspace) CloneLifecycle() (*WorkspaceLease, *IdlePolicy, *LifecycleDeadline) {
	return w.Lease.Clone(), w.IdlePolicy.Clone(), w.LifecycleDeadline.Clone()
}

// ---------------------------------------------------------------------------
// Control-plane operations (client -> control, node -> control)
// ---------------------------------------------------------------------------

const (
	OpWSCreate             = "ws.create"               // WSCreateReq -> Workspace
	OpWSGet                = "ws.get"                  // WSGetReq -> Workspace
	OpWSList               = "ws.list"                 // -> WSListRes
	OpWSDestroy            = "ws.destroy"              // WSGetReq -> {}
	OpWSMove               = "ws.move"                 // WSMoveReq -> Workspace (re-queued)
	OpWSSleep              = "ws.sleep"                // WSSleepReq -> Timer
	OpWSWake               = "ws.wake"                 // WSGetReq -> Workspace
	OpWSLaunchRecord       = "ws.launch.record"        // WSLaunchRecordReq -> Workspace
	OpWSACL                = "ws.acl"                  // WSACLReq -> Workspace (bumps authz_revision)
	OpWSClaim              = "ws.claim"                // node: WSClaimReq -> WSClaimRes
	OpWSRenew              = "ws.renew"                // node: WSRenewReq -> WSRenewRes
	OpWSReleased           = "ws.released"             // node: WSReleasedReq -> {}
	OpWSReady              = "ws.ready"                // node: WSReadyReq -> {} (materialized, now serving)
	OpWSReleaseCommit      = "ws.release.commit"       // control -> node: source checkpoint is committed; destroy source
	OpWSReleaseAbort       = "ws.release.abort"        // control -> node: restore retained source but keep it fenced
	OpWSReleaseAbortCommit = "ws.release.abort.commit" // control -> node: control durably permits restored source publication
	OpControllerState      = "controller.state"        // control -> node: authoritative promotion reconciliation state
	OpWSSnapshotCommit     = "ws.snapshot.commit"      // node -> control: make an uploaded snapshot authoritative
	OpNodeList             = "node.list"               // -> NodeListRes
	OpNodeProfileGet       = "node.profile.get"        // NodeProfileGetReq -> NodeProfileGetRes
	OpEventsTail           = "events.tail"             // EventsTailReq -> streams ev frames, then res
	OpEventsStop           = "events.stop"             // EventsStopReq -> {}; stops one event subscription
	OpEventsPost           = "events.post"             // EventPost -> {} (node -> control; also webhook wake)
	OpBindingLease         = "binding.lease"           // node: BindingLeaseReq -> BindingLeaseRes
	OpEgressApproval       = "egress.approval"         // node: EgressApprovalReq -> EgressApprovalRes
	OpGrant                = "grant"                   // client: GrantReq -> Grant (permission to talk to a node about a ws)
	OpTimerList            = "timer.list"              // -> TimerListRes
	OpDiag                 = "diag"                    // -> ControlDiag (control-plane health and integrity)
	OpFleetQuarantine      = "fleet.quarantine"        // FleetQuarantineReq -> FleetOperation
	OpFleetGet             = "fleet.get"               // FleetGetReq -> FleetOperation
	OpFleetList            = "fleet.list"              // -> FleetListRes
	OpBaseCreate           = "base.create"             // BaseCreateReq -> Base (pins a snapshot under a tenant-scoped name)
	OpBaseList             = "base.list"               // -> BaseListRes
	OpBaseRemove           = "base.remove"             // BaseRemoveReq -> {}
	OpVolumeCreate         = "volume.create"           // VolumeCreateReq -> Volume
	OpVolumeGet            = "volume.get"              // VolumeGetReq -> Volume
	OpVolumeList           = "volume.list"             // -> VolumeListRes
	OpVolumeRemove         = "volume.remove"           // VolumeRemoveReq -> {}
	OpVolumePublish        = "volume.publish"          // client -> node: VolumePublishPathReq -> Volume
	OpVolumePublishCommit  = "volume.publish.commit"   // node -> control: VolumePublishReq -> Volume
	OpVolumeAttach         = "volume.attach"           // VolumeAttachReq -> Workspace
	OpVolumeDetach         = "volume.detach"           // VolumeDetachReq -> Workspace
	OpVolumeArchive        = "volume.archive"          // node: VolumeArchiveReq -> WSSnapshotRes
	OpQueueCreate          = "queue.create"            // QueueCreateReq -> Queue (durable task list for one workspace)
	OpQueueGet             = "queue.get"               // QueueGetReq -> Queue
	OpQueueList            = "queue.list"              // QueueListReq -> QueueListRes
	OpQueueAdvance         = "queue.advance"           // QueueAdvanceReq -> Queue (records one task's outcome, moves the cursor)
	OpPoolCreate           = "pool.create"             // PoolCreateReq -> Pool
	OpPoolGet              = "pool.get"                // PoolGetReq -> Pool
	OpPoolList             = "pool.list"               // -> PoolListRes
	OpPoolRemove           = "pool.remove"             // PoolRemoveReq -> {}

	// Durable workspace lifecycle (ADR 0090). The deadline lives in the
	// control plane, so it survives the caller's process dying.
	OpWSLease       = "ws.lease"        // WSLeaseReq -> WorkspaceLease
	OpWSLeaseRenew  = "ws.lease.renew"  // WSLeaseRenewReq -> WorkspaceLease
	OpWSLeaseCancel = "ws.lease.cancel" // WSLeaseCancelReq -> Workspace
	OpWSLeaseGet    = "ws.lease.get"    // WSLeaseGetReq -> WSLeaseRes
	OpWSIdlePolicy  = "ws.idle.policy"  // WSIdlePolicyReq -> Workspace
	OpWSIdleMark    = "ws.idle.mark"    // WSIdleMarkReq -> Workspace
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

// WSLaunchRecordReq replaces the non-secret launch metadata that remount run needs
// to reconstruct the latest harness invocation on resume.
type WSLaunchRecordReq struct {
	ID             string            `cbor:"id" json:"id"`
	Labels         map[string]string `cbor:"labels" json:"labels"`
	IdempotencyKey string            `cbor:"idem,omitempty" json:"idem,omitempty"`
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

// ---------------------------------------------------------------------------
// Durable workspace leases and idle policy (ADR 0090)
// ---------------------------------------------------------------------------

// Lease expiry actions. OnExpiry names what the control plane does when a
// hold's hard deadline passes with nobody renewing it.
const (
	LeaseExpirySleep   = "sleep"   // release with a filesystem checkpoint, then paused
	LeaseExpiryDestroy = "destroy" // release and destroy the workspace
)

// Lifecycle deadline sources.
const (
	LifecycleSourceLease = "lease" // the hard deadline of an explicit hold
	LifecycleSourceIdle  = "idle"  // the workspace's idle policy
)

// Lease end reasons recorded in WorkspaceLease.EndedReason and in the
// ws.lease.expired event.
const (
	LeaseEndDeadline   = "deadline"   // the hard deadline fired
	LeaseEndCancelled  = "cancelled"  // ws.lease.cancel removed the hold
	LeaseEndMoved      = "moved"      // ws.move re-queued the workspace
	LeaseEndDestroyed  = "destroyed"  // ws.destroy ended the workspace
	LeaseEndSuperseded = "superseded" // a later ws.lease replaced the hold
)

// WorkspaceLease is a durable hold that keeps a claimed workspace awake until
// a control-plane deadline, whether or not the caller that asked for it is
// still alive. It is not the claim lease: Workspace.LeaseUntil is the node's
// ownership renewal, and this is the client's statement that work is running.
//
// A published lease value is immutable. Every mutation replaces the whole
// pointer on Workspace, so a shallow copy handed out from under the
// control-plane mutex shares nothing that can change underneath it.
type WorkspaceLease struct {
	ID string `cbor:"id" json:"id"`
	WS string `cbor:"ws" json:"ws"`
	// Generation is the workspace generation the hold was granted against.
	// A move invalidates the hold; renewing it then reports
	// ReasonGenerationMismatch rather than silently holding a new placement.
	Generation uint64 `cbor:"gen" json:"gen"`
	// MinAliveUntil is the earliest the idle policy may put this workspace to
	// sleep. MaxAliveUntil is the hard deadline. Both are unix millis.
	MinAliveUntil int64 `cbor:"min_alive_until,omitempty" json:"min_alive_until,omitempty"`
	MaxAliveUntil int64 `cbor:"max_alive_until" json:"max_alive_until"`
	// OnExpiry is LeaseExpirySleep or LeaseExpiryDestroy.
	OnExpiry  string `cbor:"on_expiry" json:"on_expiry"`
	Reason    string `cbor:"reason,omitempty" json:"reason,omitempty"`
	CreatedAt int64  `cbor:"created_at" json:"created_at"`
	RenewedAt int64  `cbor:"renewed_at,omitempty" json:"renewed_at,omitempty"`
	Renewals  uint64 `cbor:"renewals,omitempty" json:"renewals,omitempty"`
	// EndedAt and EndedReason record that the hold no longer keeps the
	// workspace awake. An ended lease is retained so a late renew learns why
	// it was refused instead of getting a bare not_found.
	EndedAt     int64  `cbor:"ended_at,omitempty" json:"ended_at,omitempty"`
	EndedReason string `cbor:"ended_reason,omitempty" json:"ended_reason,omitempty"`
}

// Live reports whether the hold still keeps its workspace awake.
func (l *WorkspaceLease) Live() bool { return l != nil && l.EndedAt == 0 }

// Clone copies a lease so a value returned from under a lock shares nothing.
func (l *WorkspaceLease) Clone() *WorkspaceLease {
	if l == nil {
		return nil
	}
	cp := *l
	return &cp
}

// IdlePolicy is the durable no-work cleanup rule for one workspace. It is the
// generic form of the built-in Agent resource's AgentPolicy.SleepAfterSec, and
// unlike WorkspaceSpec.Idle it is enforced by the control plane.
type IdlePolicy struct {
	// SleepAfterSec puts a claimed, idle workspace to sleep. DestroyAfterSec
	// destroys one that has stayed idle that long, whether claimed or already
	// asleep. Zero disables that half of the policy.
	SleepAfterSec   int64 `cbor:"sleep_after_sec,omitempty" json:"sleep_after_sec,omitempty"`
	DestroyAfterSec int64 `cbor:"destroy_after_sec,omitempty" json:"destroy_after_sec,omitempty"`
}

// Set reports whether the policy asks for anything.
func (p *IdlePolicy) Set() bool {
	return p != nil && (p.SleepAfterSec > 0 || p.DestroyAfterSec > 0)
}

// Clone copies a policy so a value returned from under a lock shares nothing.
func (p *IdlePolicy) Clone() *IdlePolicy {
	if p == nil {
		return nil
	}
	cp := *p
	return &cp
}

// LifecycleDeadline is the derived view of what the control plane will do to
// this workspace next and when. A client reads it from ws.get without knowing
// anything about timers. Exactly one is pending per workspace: the earliest of
// the lease's hard deadline and the idle policy's next action.
type LifecycleDeadline struct {
	At      int64  `cbor:"at" json:"at"`         // unix millis
	Action  string `cbor:"action" json:"action"` // LeaseExpirySleep | LeaseExpiryDestroy
	Source  string `cbor:"source" json:"source"` // LifecycleSourceLease | LifecycleSourceIdle
	TimerID string `cbor:"timer,omitempty" json:"timer,omitempty"`
	// Fired records that the deadline passed and the control plane has taken
	// ownership of executing it. Failed records that execution exhausted its
	// retries; the workspace is then degraded and operator-actionable.
	Fired    bool   `cbor:"fired,omitempty" json:"fired,omitempty"`
	FiredAt  int64  `cbor:"fired_at,omitempty" json:"fired_at,omitempty"`
	Failed   bool   `cbor:"failed,omitempty" json:"failed,omitempty"`
	Attempts int    `cbor:"attempts,omitempty" json:"attempts,omitempty"`
	Error    string `cbor:"error,omitempty" json:"error,omitempty"`
}

// Pending reports whether the deadline is still waiting to fire.
func (d *LifecycleDeadline) Pending() bool { return d != nil && !d.Fired }

// Clone copies a deadline so a value returned from under a lock shares nothing.
func (d *LifecycleDeadline) Clone() *LifecycleDeadline {
	if d == nil {
		return nil
	}
	cp := *d
	return &cp
}

// WSLeaseReq takes a durable hold on a claimed workspace. MaxAliveSec is the
// hard deadline; MinAliveSec, when set, is how long the idle policy is
// forbidden from acting. A workspace has at most one hold: a request with a
// new idempotency key replaces the previous one.
type WSLeaseReq struct {
	ID             string `cbor:"id" json:"id"`
	MinAliveSec    int64  `cbor:"min_alive_sec,omitempty" json:"min_alive_sec,omitempty"`
	MaxAliveSec    int64  `cbor:"max_alive_sec" json:"max_alive_sec"`
	OnExpiry       string `cbor:"on_expiry,omitempty" json:"on_expiry,omitempty"` // default sleep
	Reason         string `cbor:"reason,omitempty" json:"reason,omitempty"`
	IdempotencyKey string `cbor:"idem,omitempty" json:"idem,omitempty"`
}

// WSLeaseRenewReq extends an existing hold by ExtendSec from now, bounded by
// the control plane's maximum hold. It is refused with
// ReasonLifecycleDeadlineExpired once the deadline already fired, and with
// ReasonGenerationMismatch once the workspace moved.
type WSLeaseRenewReq struct {
	ID             string `cbor:"id" json:"id"`
	LeaseID        string `cbor:"lease" json:"lease"`
	ExtendSec      int64  `cbor:"extend_sec" json:"extend_sec"`
	MinAliveSec    int64  `cbor:"min_alive_sec,omitempty" json:"min_alive_sec,omitempty"`
	IdempotencyKey string `cbor:"idem,omitempty" json:"idem,omitempty"`
}

// WSLeaseCancelReq removes a hold. The workspace stays claimed and falls back
// to its idle policy, which is what it would have done had the hold never
// existed.
type WSLeaseCancelReq struct {
	ID             string `cbor:"id" json:"id"`
	LeaseID        string `cbor:"lease" json:"lease"`
	IdempotencyKey string `cbor:"idem,omitempty" json:"idem,omitempty"`
}

// WSLeaseGetReq reads a workspace's hold and derived deadline.
type WSLeaseGetReq struct {
	ID string `cbor:"id" json:"id"`
}

// WSLeaseRes is the hold plus the derived deadline the control plane will act
// on next. Deadline may name the idle policy even when Lease is nil.
type WSLeaseRes struct {
	Lease    *WorkspaceLease    `cbor:"lease,omitempty" json:"lease,omitempty"`
	Deadline *LifecycleDeadline `cbor:"deadline,omitempty" json:"deadline,omitempty"`
}

// WSIdlePolicyReq installs or replaces a workspace's idle policy. Both zero
// removes it.
type WSIdlePolicyReq struct {
	ID              string `cbor:"id" json:"id"`
	SleepAfterSec   int64  `cbor:"sleep_after_sec,omitempty" json:"sleep_after_sec,omitempty"`
	DestroyAfterSec int64  `cbor:"destroy_after_sec,omitempty" json:"destroy_after_sec,omitempty"`
	IdempotencyKey  string `cbor:"idem,omitempty" json:"idem,omitempty"`
}

// WSIdleMarkReq starts or stops the idle clock. Idle=false is the activity
// signal: session traffic does not extend a deadline, because that would make
// every session open a durable control-plane write.
type WSIdleMarkReq struct {
	ID             string `cbor:"id" json:"id"`
	Idle           bool   `cbor:"idle,omitempty" json:"idle,omitempty"`
	Reason         string `cbor:"reason,omitempty" json:"reason,omitempty"`
	IdempotencyKey string `cbor:"idem,omitempty" json:"idem,omitempty"`
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
	// RestoreProcesses settles process continuity only after destination restore.
	RestoreProcesses string `cbor:"restore_processes,omitempty" json:"restore_processes,omitempty"`
}

const (
	RestoreProcessesRestarted = "restarted"
	RestoreProcessesPreserved = "preserved"
)

type WSRenewReq struct {
	IDs             []string          `cbor:"ids" json:"ids"`
	Gen             map[string]uint64 `cbor:"gen,omitempty" json:"gen,omitempty"`
	ControllerEpoch uint64            `cbor:"controller_epoch,omitempty" json:"controller_epoch,omitempty"`
	// Authz is the authorization revision the node currently enforces for
	// each workspace (authz-push). Control answers with what changed since.
	Authz map[string]uint64 `cbor:"authz,omitempty" json:"authz,omitempty"`
	// Profile and RuntimeChecks carry the node's latest runtime-profile
	// health (ADR 0089). Renewal is the existing periodic node→control
	// channel, so drift rides on it rather than opening a second one. A node
	// with no workspaces still renews when its checks change, so IDs may be
	// empty when RuntimeChecks is set. Control trusts these only to
	// downgrade.
	Profile       string    `cbor:"profile,omitempty" json:"profile,omitempty"`
	RuntimeChecks []Finding `cbor:"runtime_checks,omitempty" json:"runtime_checks,omitempty"`
	// ReportChecks distinguishes "no checks to report" from "this node has
	// no reprobing backend", so an empty RuntimeChecks can still clear a
	// previously failing set.
	ReportChecks bool `cbor:"report_checks,omitempty" json:"report_checks,omitempty"`
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
	ControllerEpoch  uint64 `cbor:"controller_epoch,omitempty" json:"controller_epoch,omitempty"`
	// AuthzRevision is the authoritative authorization revision (authz-push).
	// Revoked lists principals that lost access after the revision the node
	// reported in WSRenewReq.Authz. AuthzReset means the node's revision is
	// older than the retained history: every session of the workspace must be
	// closed because control cannot name the affected principals.
	AuthzRevision uint64   `cbor:"authz_revision,omitempty" json:"authz_revision,omitempty"`
	Revoked       []string `cbor:"revoked,omitempty" json:"revoked,omitempty"`
	AuthzReset    bool     `cbor:"authz_reset,omitempty" json:"authz_reset,omitempty"`
	// BindingRevision fingerprints the workspace's current binding set. It
	// differs from the value the node received with its last lease whenever a
	// binding was rotated or revoked, which is how a credential change reaches
	// a running workspace within one renew rather than at lease expiry. Zero
	// means the workspace declares no bindings.
	BindingRevision uint64 `cbor:"binding_revision,omitempty" json:"binding_revision,omitempty"`
}

type WSRenewRes struct {
	Results         []WSRenewResult `cbor:"results" json:"results"`
	ControllerEpoch uint64          `cbor:"controller_epoch,omitempty" json:"controller_epoch,omitempty"`
}

type WSReleasedReq struct {
	ID             string `cbor:"id" json:"id"`
	Gen            uint64 `cbor:"gen" json:"gen"`
	OperationID    string `cbor:"operation,omitempty" json:"operation,omitempty"`
	Snapshot       string `cbor:"snapshot,omitempty" json:"snapshot,omitempty"` // artifact id, if one was taken
	SnapshotFormat string `cbor:"snapshot_format,omitempty" json:"snapshot_format,omitempty"`
	Reason         string `cbor:"reason,omitempty" json:"reason,omitempty"`
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
	ID             string `cbor:"id" json:"id"`
	Gen            uint64 `cbor:"gen" json:"gen"`
	OperationID    string `cbor:"operation,omitempty" json:"operation,omitempty"`
	Snapshot       string `cbor:"snapshot,omitempty" json:"snapshot,omitempty"`
	SnapshotFormat string `cbor:"snapshot_format,omitempty" json:"snapshot_format,omitempty"`
}

// ControllerNodeState is the live and retained authority a node reports to a
// newly promoted controller. It contains no credentials or broker capability.
type ControllerNodeState struct {
	Node       string                   `cbor:"node" json:"node"`
	Epoch      uint64                   `cbor:"epoch" json:"epoch"`
	Workspaces []ControllerWorkspace    `cbor:"workspaces,omitempty" json:"workspaces,omitempty"`
	Releases   []ControllerReleaseState `cbor:"releases,omitempty" json:"releases,omitempty"`
}

// ControllerWorkspace is one currently serviceable node assignment.
type ControllerWorkspace struct {
	Workspace Workspace `cbor:"workspace" json:"workspace"`
	Sessions  []string  `cbor:"sessions,omitempty" json:"sessions,omitempty"`
}

// ControllerReleaseState is a retained release journal entry.
type ControllerReleaseState struct {
	Request     WSReleaseReq  `cbor:"request" json:"request"`
	Response    WSReleasedReq `cbor:"response" json:"response"`
	OperationID string        `cbor:"operation_id,omitempty" json:"operation_id,omitempty"`
	State       string        `cbor:"state" json:"state"`
}

type WSSnapshotCommitReq struct {
	ID       string `cbor:"id" json:"id"`
	Gen      uint64 `cbor:"gen" json:"gen"`
	Snapshot string `cbor:"snapshot" json:"snapshot"`
	Format   string `cbor:"format,omitempty" json:"format,omitempty"`
}

type NodeListRes struct {
	Nodes []NodeStatus `cbor:"nodes" json:"nodes"`
}

// NodeProfileGetReq asks the control plane to evaluate one runtime profile
// against the evidence it holds. Node empty means every known node; Profile
// empty means each node's own configured profile.
type NodeProfileGetReq struct {
	Node    string `cbor:"node,omitempty" json:"node,omitempty"`
	Profile string `cbor:"profile,omitempty" json:"profile,omitempty"`
}

// NodeProfileReport is one node's evaluation of one runtime profile. Status
// is CheckPass, CheckFail or CheckUnavailable, and it is CheckPass only when
// every entry in Checks passed.
type NodeProfileReport struct {
	Node        string    `cbor:"node" json:"node"`
	Profile     string    `cbor:"profile" json:"profile"`
	Status      string    `cbor:"status" json:"status"`
	Checks      []Finding `cbor:"checks,omitempty" json:"checks,omitempty"`
	EvaluatedAt int64     `cbor:"evaluated_at" json:"evaluated_at"`
	// Configured is the profile the node says it was started with. It is a
	// self-claim reported for comparison, never evidence.
	Configured string `cbor:"configured,omitempty" json:"configured,omitempty"`
	// Online reports whether the node was connected at evaluation time. An
	// offline node's evidence is its last hello, which is stale by
	// definition, so it is reported rather than silently trusted.
	Online bool `cbor:"online,omitempty" json:"online,omitempty"`
}

// NodeProfileGetRes answers node.profile.get, one report per node, sorted by
// node id.
type NodeProfileGetRes struct {
	Profile string              `cbor:"profile,omitempty" json:"profile,omitempty"`
	Nodes   []NodeProfileReport `cbor:"nodes,omitempty" json:"nodes,omitempty"`
}

// NodeProfileEvent is the payload of node.profile.verified,
// node.profile.unschedulable and node.profile.restored. Failed names the
// checks that did not pass, so a reader never has to parse Detail text.
type NodeProfileEvent struct {
	Node    string   `cbor:"node" json:"node"`
	Profile string   `cbor:"profile" json:"profile"`
	Status  string   `cbor:"status" json:"status"`
	Failed  []string `cbor:"failed,omitempty" json:"failed,omitempty"`
}

// WorkspaceSelector identifies incident-containment targets. Empty selectors
// are rejected unless All is explicit.
type WorkspaceSelector struct {
	All           bool              `cbor:"all,omitempty" json:"all,omitempty"`
	Workspace     string            `cbor:"workspace,omitempty" json:"workspace,omitempty"`
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
	Workspace       string   `cbor:"workspace" json:"workspace"`
	Node            string   `cbor:"node,omitempty" json:"node,omitempty"`
	Backend         string   `cbor:"backend,omitempty" json:"backend,omitempty"`
	Generation      uint64   `cbor:"generation,omitempty" json:"generation,omitempty"`
	State           string   `cbor:"state" json:"state"`
	Fenced          bool     `cbor:"fenced,omitempty" json:"fenced,omitempty"`
	Acknowledged    bool     `cbor:"acknowledged" json:"acknowledged"`
	Snapshot        string   `cbor:"snapshot,omitempty" json:"snapshot,omitempty"`
	SnapshotFormat  string   `cbor:"snapshot_format,omitempty" json:"snapshot_format,omitempty"`
	SnapshotObjects []string `cbor:"snapshot_objects,omitempty" json:"snapshot_objects,omitempty"`
	Error           string   `cbor:"error,omitempty" json:"error,omitempty"`
	UpdatedAt       int64    `cbor:"updated_at" json:"updated_at"`
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
	Name      string   `cbor:"name" json:"name"`
	Tenant    string   `cbor:"tenant" json:"tenant"`
	Owner     string   `cbor:"owner" json:"owner"`
	Artifact  string   `cbor:"artifact" json:"artifact"`
	Format    string   `cbor:"format,omitempty" json:"format,omitempty"`
	Objects   []string `cbor:"objects,omitempty" json:"objects,omitempty"`
	Workspace string   `cbor:"workspace,omitempty" json:"workspace,omitempty"` // the workspace it was snapshotted from, when known
	Bytes     int64    `cbor:"bytes,omitempty" json:"bytes,omitempty"`
	CreatedAt int64    `cbor:"created_at" json:"created_at"`
}

// BaseCreateReq pins an uploaded artifact under a base name in the caller's
// tenant. Names are unique per tenant; a second create with the same name is a
// conflict unless it replays the same idempotency key.
type BaseCreateReq struct {
	Name           string `cbor:"name" json:"name"`
	Artifact       string `cbor:"artifact" json:"artifact"`
	Format         string `cbor:"format,omitempty" json:"format,omitempty"`
	Workspace      string `cbor:"workspace,omitempty" json:"workspace,omitempty"`
	IdempotencyKey string `cbor:"idem,omitempty" json:"idem,omitempty"`
}

// BaseListRes lists the bases visible to the caller, sorted by name.
type BaseListRes struct {
	Bases []Base `cbor:"bases" json:"bases"`
}

// Volume is a tenant-scoped pointer to the latest immutable artifact. Its
// retained Versions keep artifacts pinned while an attachment references an
// older version.
type Volume struct {
	ID        string          `cbor:"id" json:"id"`
	Tenant    string          `cbor:"tenant" json:"tenant"`
	Owner     string          `cbor:"owner" json:"owner"`
	Artifact  string          `cbor:"artifact" json:"artifact"`
	Version   uint64          `cbor:"version" json:"version"`
	Versions  []VolumeVersion `cbor:"versions" json:"versions"`
	CreatedAt int64           `cbor:"created_at" json:"created_at"`
	UpdatedAt int64           `cbor:"updated_at" json:"updated_at"`
}

// VolumeVersion records one immutable value published from a fenced
// workspace generation.
type VolumeVersion struct {
	Number      uint64 `cbor:"number" json:"number"`
	Artifact    string `cbor:"artifact" json:"artifact"`
	PublishedBy string `cbor:"published_by,omitempty" json:"published_by,omitempty"`
	Generation  uint64 `cbor:"generation,omitempty" json:"generation,omitempty"`
	PublishedAt int64  `cbor:"published_at" json:"published_at"`
}

// VolumeCreateReq creates the first version from an already uploaded and
// verified artifact.
type VolumeCreateReq struct {
	ID             string `cbor:"id" json:"id"`
	Artifact       string `cbor:"artifact" json:"artifact"`
	IdempotencyKey string `cbor:"idem,omitempty" json:"idem,omitempty"`
}

// VolumeGetReq identifies a tenant-scoped volume.
type VolumeGetReq struct {
	ID string `cbor:"id" json:"id"`
}

// VolumeListRes lists every volume visible to the caller.
type VolumeListRes struct {
	Volumes []Volume `cbor:"volumes" json:"volumes"`
}

// VolumeRemoveReq removes an unattached volume and unpins its versions.
type VolumeRemoveReq struct {
	ID             string `cbor:"id" json:"id"`
	IdempotencyKey string `cbor:"idem,omitempty" json:"idem,omitempty"`
}

// VolumePublishReq advances a volume with both version and workspace-
// generation compare-and-swap fences.
type VolumePublishReq struct {
	ID              string `cbor:"id" json:"id"`
	Workspace       string `cbor:"ws" json:"ws"`
	Generation      uint64 `cbor:"generation" json:"generation"`
	Artifact        string `cbor:"artifact" json:"artifact"`
	ExpectedVersion uint64 `cbor:"expected_version" json:"expected_version"`
	IdempotencyKey  string `cbor:"idem,omitempty" json:"idem,omitempty"`
	Grant           *Grant `cbor:"grant,omitempty" json:"grant,omitempty"`
}

// VolumeAttachReq pins the current volume version in one workspace spec.
type VolumeAttachReq struct {
	ID             string `cbor:"id" json:"id"`
	Workspace      string `cbor:"ws" json:"ws"`
	Generation     uint64 `cbor:"generation" json:"generation"`
	Path           string `cbor:"path" json:"path"`
	IdempotencyKey string `cbor:"idem,omitempty" json:"idem,omitempty"`
}

// VolumeDetachReq removes a pinned read-only mount declaration.
type VolumeDetachReq struct {
	Workspace      string `cbor:"ws" json:"ws"`
	Generation     uint64 `cbor:"generation" json:"generation"`
	Path           string `cbor:"path" json:"path"`
	IdempotencyKey string `cbor:"idem,omitempty" json:"idem,omitempty"`
}

// VolumeArchiveReq snapshots one jailed workspace directory as a standalone
// immutable artifact suitable for VolumePublishReq.
type VolumeArchiveReq struct {
	WS             string `cbor:"ws" json:"ws"`
	Path           string `cbor:"path" json:"path"`
	Upload         bool   `cbor:"upload" json:"upload"`
	IdempotencyKey string `cbor:"idem,omitempty" json:"idem,omitempty"`
	Grant          *Grant `cbor:"grant,omitempty" json:"grant,omitempty"`
}

// VolumePublishPathReq is the client-to-node half of volume.publish. The node
// archives Path and keeps the workspace tree boundary until control commits
// the version and generation CAS.
type VolumePublishPathReq struct {
	WS              string `cbor:"ws" json:"ws"`
	Path            string `cbor:"path" json:"path"`
	Volume          string `cbor:"volume" json:"volume"`
	ExpectedVersion uint64 `cbor:"expected_version" json:"expected_version"`
	IdempotencyKey  string `cbor:"idem,omitempty" json:"idem,omitempty"`
	Grant           *Grant `cbor:"grant,omitempty" json:"grant,omitempty"`
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
	// Binding, Host and Types narrow the stream server-side so an audit
	// question ("what did this binding do against this host?") is one call
	// rather than a full history the caller filters itself. Binding and Host
	// match the payload fields the broker records; Types are event-type
	// prefixes, so "egress" selects every egress.* type. All are additive:
	// an omitted filter matches everything, and a peer that predates them
	// still receives the unfiltered stream it asked for.
	Binding string   `cbor:"binding,omitempty" json:"binding,omitempty"`
	Host    string   `cbor:"host,omitempty" json:"host,omitempty"`
	Types   []string `cbor:"types,omitempty" json:"types,omitempty"`
}

type EventsStopReq struct {
	Subscription string `cbor:"sub,omitempty" json:"sub,omitempty"`
}

// Event is one entry in the canonical log. Payload is CBOR.
type Event struct {
	Seq             uint64 `cbor:"seq" json:"seq"`
	At              int64  `cbor:"at" json:"at"`                             // unix millis
	Stream          string `cbor:"stream,omitempty" json:"stream,omitempty"` // ws id, node id, or "" for global
	Principal       string `cbor:"principal,omitempty" json:"principal,omitempty"`
	Node            string `cbor:"node,omitempty" json:"node,omitempty"`
	EventID         string `cbor:"event_id,omitempty" json:"event_id,omitempty"`
	ReceivedAt      int64  `cbor:"received_at,omitempty" json:"received_at,omitempty"`
	ObservedAt      int64  `cbor:"observed_at,omitempty" json:"observed_at,omitempty"`
	Origin          string `cbor:"origin,omitempty" json:"origin,omitempty"`
	Actor           string `cbor:"actor,omitempty" json:"actor,omitempty"`
	Tenant          string `cbor:"tenant,omitempty" json:"tenant,omitempty"`
	Workspace       string `cbor:"workspace,omitempty" json:"workspace,omitempty"`
	Generation      uint64 `cbor:"generation,omitempty" json:"generation,omitempty"`
	ControllerEpoch uint64 `cbor:"controller_epoch,omitempty" json:"controller_epoch,omitempty"`
	Session         string `cbor:"session,omitempty" json:"session,omitempty"` // session id for s.* events
	OperationID     string `cbor:"operation_id,omitempty" json:"operation_id,omitempty"`
	ProducerSeq     uint64 `cbor:"producer_seq,omitempty" json:"producer_seq,omitempty"`
	Type            string `cbor:"type" json:"type"`
	Payload         []byte `cbor:"payload,omitempty" json:"payload,omitempty"`
	Cause           uint64 `cbor:"cause,omitempty" json:"cause,omitempty"` // seq of the event that caused this one
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
	// Kind is one of the BindingKind* constants: what the substituted value
	// is, so a browser cookie is never treated as an API key.
	Kind string `cbor:"kind,omitempty" json:"kind,omitempty"`
	// Methods and PathPrefixes narrow the binding beyond its destinations.
	// Empty means every method or every path.
	Methods      []string `cbor:"methods,omitempty" json:"methods,omitempty"`
	PathPrefixes []string `cbor:"path_prefixes,omitempty" json:"path_prefixes,omitempty"`
	// Revision is the binding revision this lease was minted from. A rotation
	// bumps it, so a node can tell a stale lease from a live one.
	Revision uint64 `cbor:"revision,omitempty" json:"revision,omitempty"`
	// Generation is the workspace generation the lease was issued for. A lease
	// carried across a move is not valid for the new generation.
	Generation uint64 `cbor:"gen,omitempty" json:"gen,omitempty"`
	// Substitution names where in a request the broker replaces the
	// placeholder. Nil means SubstitutionHeader, which is what every binding
	// did before this field existed.
	Substitution *BindingSubstitution `cbor:"substitution,omitempty" json:"substitution,omitempty"`
}

// Substitution locations. A binding declares exactly one; the broker refuses
// a request whose placeholder is anywhere else, because a placeholder that
// silently travels unsubstituted is an opaque upstream failure.
const (
	SubstitutionHeader   = "header"    // an HTTP header value (the default)
	SubstitutionQuery    = "query"     // a named query parameter
	SubstitutionBodyForm = "body_form" // a named application/x-www-form-urlencoded field
	SubstitutionBodyJSON = "body_json" // the JSON string at an RFC 6901 pointer
)

// BindingSubstitution names where a binding's placeholder is replaced.
// Name applies to SubstitutionQuery and SubstitutionBodyForm; JSONPointer to
// SubstitutionBodyJSON. Body locations require the broker to buffer the
// request, so they are bounded and fail closed on any ambiguity (§9).
type BindingSubstitution struct {
	Location    string `cbor:"location" json:"location"`
	Name        string `cbor:"name,omitempty" json:"name,omitempty"`
	JSONPointer string `cbor:"json_pointer,omitempty" json:"json_pointer,omitempty"`
}

// SubstitutionLocation returns the effective location of a lease: the
// declared one, or SubstitutionHeader when the binding declares none.
func SubstitutionLocation(l BindingLease) string {
	if l.Substitution == nil || l.Substitution.Location == "" {
		return SubstitutionHeader
	}
	return l.Substitution.Location
}

// ValidSubstitution reports whether a declared substitution is well formed.
// An unrecognized location, or a location missing the field it needs, is a
// configuration error the control plane refuses rather than a request the
// broker fails one at a time.
func ValidSubstitution(s *BindingSubstitution) bool {
	if s == nil {
		return true
	}
	switch s.Location {
	case "", SubstitutionHeader:
		return s.Name == "" && s.JSONPointer == ""
	case SubstitutionQuery, SubstitutionBodyForm:
		return s.Name != "" && s.JSONPointer == ""
	case SubstitutionBodyJSON:
		return s.Name == "" && strings.HasPrefix(s.JSONPointer, "/")
	default:
		return false
	}
}

type BindingLeaseRes struct {
	Leases []BindingLease `cbor:"leases" json:"leases"`
	// Revision fingerprints the binding set this response was computed from.
	// The node compares it with WSRenewResult.BindingRevision and re-leases
	// when they differ, so a rotation or revocation reaches a live workspace
	// within one renew interval instead of at lease TTL.
	Revision uint64 `cbor:"revision,omitempty" json:"revision,omitempty"`
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
	Client          string   `cbor:"client" json:"client"`
	WS              string   `cbor:"ws" json:"ws"`
	Node            string   `cbor:"node" json:"node"`
	Principal       string   `cbor:"principal,omitempty" json:"principal,omitempty"`
	Tenant          string   `cbor:"tenant,omitempty" json:"tenant,omitempty"`
	Roles           []string `cbor:"roles,omitempty" json:"roles,omitempty"`
	AuthzRevision   uint64   `cbor:"authz_revision,omitempty" json:"authz_revision,omitempty"`
	ExpiresAt       int64    `cbor:"exp" json:"exp"`
	Gen             uint64   `cbor:"gen" json:"gen"`
	ControllerEpoch uint64   `cbor:"controller_epoch,omitempty" json:"controller_epoch,omitempty"`
}

// Timer is a durable wake.
type Timer struct {
	ID        string            `cbor:"id" json:"id"`
	WS        string            `cbor:"ws" json:"ws"`
	At        int64             `cbor:"at,omitempty" json:"at,omitempty"` // unix millis
	OnEvent   string            `cbor:"on,omitempty" json:"on,omitempty"` // event type
	Match     map[string]string `cbor:"match,omitempty" json:"match,omitempty"`
	Action    string            `cbor:"action" json:"action"` // resume | sleep | destroy
	Fired     bool              `cbor:"fired" json:"fired"`
	FiredAt   int64             `cbor:"fired_at,omitempty" json:"fired_at,omitempty"`
	CreatedAt int64             `cbor:"created_at" json:"created_at"`
	// Kind distinguishes the wake timers ws.sleep has always created from the
	// lifecycle deadlines of ADR 0090. Empty is TimerKindResume, so rows
	// written before this field decode unchanged.
	Kind string `cbor:"kind,omitempty" json:"kind,omitempty"`
	// Generation is the workspace generation a lifecycle timer was armed
	// against. A move invalidates the timer rather than acting on a placement
	// nobody asked to hold.
	Generation uint64 `cbor:"gen,omitempty" json:"gen,omitempty"`
	// Superseded marks a timer that was retired without acting. It is set
	// alongside Fired so retention collects it like any other spent row.
	Superseded bool   `cbor:"superseded,omitempty" json:"superseded,omitempty"`
	Reason     string `cbor:"reason,omitempty" json:"reason,omitempty"`
}

// Timer kinds. TimerKindResume is the historical ws.sleep wake and is the
// zero value, so timers written by earlier releases keep their meaning.
const (
	TimerKindResume      = ""
	TimerKindLeaseExpiry = "lease_expiry"
	TimerKindIdleSleep   = "idle_sleep"
	TimerKindIdleDestroy = "idle_destroy"
)

// Lifecycle reports whether the timer is a control-plane lifecycle deadline
// rather than a wake.
func (t *Timer) Lifecycle() bool {
	switch t.Kind {
	case TimerKindLeaseExpiry, TimerKindIdleSleep, TimerKindIdleDestroy:
		return true
	default:
		return false
	}
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
	ID            string         `cbor:"id" json:"id"`
	WS            string         `cbor:"ws" json:"ws"`
	Kind          string         `cbor:"kind" json:"kind"`
	Program       []string       `cbor:"program,omitempty" json:"program,omitempty"`
	PID           int            `cbor:"pid,omitempty" json:"pid,omitempty"`
	OpenedAt      int64          `cbor:"opened_at" json:"opened_at"`
	Run           *RunInfo       `cbor:"run,omitempty" json:"run,omitempty"`
	Sensitive     bool           `cbor:"sensitive,omitempty" json:"sensitive,omitempty"`
	AuthOperation *AuthOperation `cbor:"auth_operation,omitempty" json:"auth_operation,omitempty"`
}

// AuthOperation marks a provider-native authentication command. Its output is
// delivered only to the opening client and is never retained for replay.
type AuthOperation struct {
	Recipe string `cbor:"recipe" json:"recipe"`
	Action string `cbor:"action" json:"action"`
}

const (
	AuthActionLogin  = "login"
	AuthActionStatus = "status"
	AuthActionLogout = "logout"
)

func (a *AuthOperation) Validate() error {
	if a == nil {
		return nil
	}
	if a.Recipe == "" || len(a.Recipe) > 64 {
		return Err(CodeBadRequest, "auth operation recipe is required and at most 64 bytes")
	}
	switch a.Action {
	case AuthActionLogin, AuthActionStatus, AuthActionLogout:
		return nil
	default:
		return Err(CodeBadRequest, "auth operation action must be %s, %s or %s", AuthActionLogin, AuthActionStatus, AuthActionLogout)
	}
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
	// Auth is RunAuthAPIKey, RunAuthSubscription or the legacy
	// RunAuthWorkspaceResident.
	Auth string `cbor:"auth,omitempty" json:"auth,omitempty"`
}

// RunInfo.Auth values.
const (
	// RunAuthAPIKey: the harness reads a brokered provider key placeholder
	// from the environment.
	RunAuthAPIKey = "api_key"
	// RunAuthSubscription: the harness uses a provider-native subscription
	// login kept inside the trusted local workspace.
	RunAuthSubscription = "subscription"
	// RunAuthWorkspaceResident: the harness keeps its own login token in the
	// workspace, outside the broker's view. It is retained for older recipes
	// and recorded workspaces.
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
	case "", RunAuthAPIKey, RunAuthSubscription, RunAuthWorkspaceResident:
		return nil
	}
	return Err(CodeBadRequest, "run.auth must be %s, %s or %s", RunAuthAPIKey, RunAuthSubscription, RunAuthWorkspaceResident)
}

type Gap struct {
	From uint64 `cbor:"from" json:"from"` // first evicted seq
	To   uint64 `cbor:"to" json:"to"`     // last evicted seq
	Tier string `cbor:"tier,omitempty" json:"tier,omitempty"`
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
	// Sensitive makes this a memory-only provider auth session. It must carry
	// AuthOperation, cannot be attached later, and is never spilled,
	// published, or committed as a durable session log.
	Sensitive     bool           `cbor:"sensitive,omitempty" json:"sensitive,omitempty"`
	AuthOperation *AuthOperation `cbor:"auth_operation,omitempty" json:"auth_operation,omitempty"`
}

type SOpenRes struct {
	S            string `cbor:"s" json:"s"`
	Next         uint64 `cbor:"next" json:"next"` // next seq the node will emit (for attach: where live begins)
	LastInputSeq uint64 `cbor:"last_iseq,omitempty" json:"last_iseq,omitempty"`
	Kind         string `cbor:"kind,omitempty" json:"kind,omitempty"`
}

type SAttachReq struct {
	S            string `cbor:"s" json:"s"`
	From         uint64 `cbor:"from" json:"from"` // replay from this seq
	Subscription string `cbor:"subscription,omitempty" json:"subscription,omitempty"`
	Grant        *Grant `cbor:"grant,omitempty" json:"grant,omitempty"`
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
	S            string `cbor:"s" json:"s"`
	Kill         bool   `cbor:"kill,omitempty" json:"kill,omitempty"` // also terminate the process
	Subscription string `cbor:"subscription,omitempty" json:"subscription,omitempty"`
	Grant        *Grant `cbor:"grant,omitempty" json:"grant,omitempty"`
}

type SListReq struct {
	WS    string `cbor:"ws,omitempty" json:"ws,omitempty"`
	Grant *Grant `cbor:"grant,omitempty" json:"grant,omitempty"`
}

type SListRes struct {
	Sessions []SessionStatus `cbor:"sessions" json:"sessions"`
}

type SessionStatus struct {
	Info            SessionInfo `cbor:"info" json:"info"`
	Exited          bool        `cbor:"exited" json:"exited"`
	Exit            *ExitInfo   `cbor:"exit,omitempty" json:"exit,omitempty"`
	Next            uint64      `cbor:"next" json:"next"`     // next seq
	Oldest          uint64      `cbor:"oldest" json:"oldest"` // oldest replayable seq
	BlobBytes       int64       `cbor:"blob_bytes,omitempty" json:"blob_bytes,omitempty"`
	BlobSegments    int         `cbor:"blob_segments,omitempty" json:"blob_segments,omitempty"`
	UnavailableTier string      `cbor:"unavailable_tier,omitempty" json:"unavailable_tier,omitempty"`
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
	WS       string `cbor:"ws" json:"ws"`
	Artifact string `cbor:"artifact" json:"artifact"`
	// Path is the existing workspace directory that receives the overlay.
	// Empty means the workspace root.
	Path string `cbor:"path,omitempty" json:"path,omitempty"`
	// Format names the representation Artifact is stored in. Empty means the
	// legacy deterministic tar.gz, so a client built before chunked pushes
	// existed keeps working unchanged; a node that cannot restore the named
	// representation refuses the request rather than misparsing the body.
	Format         string `cbor:"format,omitempty" json:"format,omitempty"`
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
	Artifact       string `cbor:"artifact" json:"artifact"` // art_sha256:<hex>
	Bytes          int64  `cbor:"bytes" json:"bytes"`
	Consistency    string `cbor:"consistency" json:"consistency"`
	Authoritative  bool   `cbor:"authoritative" json:"authoritative"`
	Format         string `cbor:"format,omitempty" json:"format,omitempty"`
	UploadedBytes  int64  `cbor:"uploaded_bytes,omitempty" json:"uploaded_bytes,omitempty"`
	Chunks         int    `cbor:"chunks,omitempty" json:"chunks,omitempty"`
	UploadedChunks int    `cbor:"uploaded_chunks,omitempty" json:"uploaded_chunks,omitempty"`
}

const (
	SnapshotConsistencyLive         = "live"
	SnapshotConsistencyQuiesced     = "quiesced"
	ArtifactFormatTar               = "tar"
	ArtifactFormatChunkedV1         = "chunked-v1"
	ArtifactFormatFirecrackerFullV1 = "firecracker-full-v1"
)

type WSReleaseReq struct {
	WS           string `cbor:"ws" json:"ws"`
	Gen          uint64 `cbor:"gen" json:"gen"`
	ReleaseEpoch uint64 `cbor:"release_epoch,omitempty" json:"release_epoch,omitempty"`
	OperationID  string `cbor:"operation,omitempty" json:"operation,omitempty"`
	Snapshot     bool   `cbor:"snapshot" json:"snapshot"` // take + upload a snapshot before releasing
	Reason       string `cbor:"reason,omitempty" json:"reason,omitempty"`
	// Tenant, Backend and Spec are control-derived recovery declarations. They
	// let a restarted node reconcile the exact retained tree and volume pins;
	// they never grant authority to release a different generation.
	Tenant  string        `cbor:"tenant,omitempty" json:"tenant,omitempty"`
	Backend string        `cbor:"backend,omitempty" json:"backend,omitempty"`
	Spec    WorkspaceSpec `cbor:"spec,omitempty" json:"spec,omitempty"`
}

// WSQuarantineReq asks the recorded holder to fence and optionally checkpoint
// a workspace as phase one of a fleet operation.
type WSQuarantineReq struct {
	OperationID string        `cbor:"operation" json:"operation"`
	WS          string        `cbor:"ws" json:"ws"`
	Gen         uint64        `cbor:"gen" json:"gen"`
	Action      string        `cbor:"action" json:"action"`
	Backend     string        `cbor:"backend,omitempty" json:"backend,omitempty"`
	Exclude     []string      `cbor:"exclude,omitempty" json:"exclude,omitempty"`
	Security    SecuritySpec  `cbor:"security,omitempty" json:"security,omitempty"`
	Tenant      string        `cbor:"tenant,omitempty" json:"tenant,omitempty"`
	Volumes     []VolumeMount `cbor:"volumes,omitempty" json:"volumes,omitempty"`
}

// WSQuarantineRes is the node's durable phase-one containment proof.
type WSQuarantineRes struct {
	Fenced         bool   `cbor:"fenced" json:"fenced"`
	Generation     uint64 `cbor:"gen" json:"gen"`
	Action         string `cbor:"action" json:"action"`
	Backend        string `cbor:"backend,omitempty" json:"backend,omitempty"`
	Snapshot       string `cbor:"snapshot,omitempty" json:"snapshot,omitempty"`
	SnapshotFormat string `cbor:"snapshot_format,omitempty" json:"snapshot_format,omitempty"`
	Warning        string `cbor:"warning,omitempty" json:"warning,omitempty"`
}

// WSQuarantineCommitReq authorizes physical deletion after control has
// durably committed a matching phase-one proof and generation fence.
type WSQuarantineCommitReq struct {
	OperationID    string `cbor:"operation" json:"operation"`
	WS             string `cbor:"ws" json:"ws"`
	Gen            uint64 `cbor:"gen" json:"gen"`
	Backend        string `cbor:"backend" json:"backend"`
	Snapshot       string `cbor:"snapshot" json:"snapshot"`
	SnapshotFormat string `cbor:"snapshot_format,omitempty" json:"snapshot_format,omitempty"`
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
	ControllerRole          string             `cbor:"controller_role,omitempty" json:"controller_role,omitempty"`
	ControllerEpoch         uint64             `cbor:"controller_epoch,omitempty" json:"controller_epoch,omitempty"`
	ControllerLeaseAgeMS    int64              `cbor:"controller_lease_age_ms,omitempty" json:"controller_lease_age_ms,omitempty"`
	LastReplicatedAt        int64              `cbor:"last_replicated_at,omitempty" json:"last_replicated_at,omitempty"`
	RestoredEventSeq        uint64             `cbor:"restored_event_seq,omitempty" json:"restored_event_seq,omitempty"`
	RecoveryLostWindowMS    int64              `cbor:"recovery_lost_window_ms,omitempty" json:"recovery_lost_window_ms,omitempty"`
	ControllerReconciling   bool               `cbor:"controller_reconciling,omitempty" json:"controller_reconciling,omitempty"`
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
	// Status is the check's verdict when the finding is one result in a
	// conformance-style report: CheckPass, CheckFail or CheckUnavailable.
	// It is empty on a finding that merely describes a problem, and an empty
	// status is never read as a pass.
	Status string `cbor:"status,omitempty" json:"status,omitempty"`
}

// Check verdicts. An unavailable check is never a passing check; a report
// that contains one is not healthy (AGENTS.md, `doctor`).
const (
	CheckPass        = "pass"
	CheckFail        = "fail"
	CheckUnavailable = "unavailable"
)

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
	ControllerEpoch          uint64             `cbor:"controller_epoch,omitempty" json:"controller_epoch,omitempty"`
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
	Volumes                  int                `cbor:"volumes,omitempty" json:"volumes,omitempty"`
	VolumeAttachments        int                `cbor:"volume_attachments,omitempty" json:"volume_attachments,omitempty"`
	VolumeSourceBytes        int64              `cbor:"volume_source_bytes,omitempty" json:"volume_source_bytes,omitempty"`
	VolumeSourceMaxBytes     int64              `cbor:"volume_source_max_bytes,omitempty" json:"volume_source_max_bytes,omitempty"`
	VolumeSourceEntries      int                `cbor:"volume_source_entries,omitempty" json:"volume_source_entries,omitempty"`
	VolumeSourceMaxEntries   int                `cbor:"volume_source_max_entries,omitempty" json:"volume_source_max_entries,omitempty"`
	VolumeQuotaRejections    uint64             `cbor:"volume_quota_rejections,omitempty" json:"volume_quota_rejections,omitempty"`
	Metrics                  map[string]float64 `cbor:"metrics,omitempty" json:"metrics,omitempty"`
	Findings                 []Finding          `cbor:"findings,omitempty" json:"findings,omitempty"`
}

// WSDiag is one workspace as the node holding it sees it.
type WSDiag struct {
	ID        string          `cbor:"id" json:"id"`
	Gen       uint64          `cbor:"gen" json:"gen"`
	Backend   string          `cbor:"backend" json:"backend"`
	Root      string          `cbor:"root" json:"root"`
	Volumes   []VolumeMount   `cbor:"volumes,omitempty" json:"volumes,omitempty"`
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
	EvNodeEnrolled = "node.enrolled"
	EvNodeOnline   = "node.online"
	EvNodeOffline  = "node.offline"
	// Runtime-profile transitions (ADR 0089). Payload is NodeProfileEvent.
	// The control plane emits them when its own view of a node's satisfied
	// profiles changes; a node also emits the unschedulable/restored pair
	// for its local degraded state so the transition is recorded even while
	// its uplink is down.
	EvNodeProfileVerified      = "node.profile.verified"
	EvNodeProfileUnschedulable = "node.profile.unschedulable"
	EvNodeProfileRestored      = "node.profile.restored"
	EvWSCreated                = "ws.created"
	EvWSOffered                = "ws.offered"
	EvWSClaiming               = "ws.claiming"
	EvWSClaimed                = "ws.claimed"
	EvWSReleased               = "ws.released"
	EvWSMoved                  = "ws.moved"
	EvWSPaused                 = "ws.paused"
	EvWSResumed                = "ws.resumed"
	EvWSSnapshot               = "ws.snapshot"
	EvWSRestored               = "ws.restored"
	EvWSDestroyed              = "ws.destroyed"
	EvWSLaunchRecorded         = "ws.launch_recorded"
	EvWSLeaseExpired           = "ws.lease_expired"
	EvWSFenced                 = "ws.fenced"
	EvWSStateChanged           = "ws.state_changed"
	EvControlReconciled        = "control.reconciled"
	// EvQuotaExceeded records an admission refusal. It is emitted by both the
	// tenant-authority admission transaction and the legacy in-process limits,
	// so a rejection is never only a counter.
	EvQuotaExceeded         = "quota.exceeded"
	EvControlRecovered      = "control.recovered"
	EvWSACL                 = "ws.acl"        // ACL replaced; payload names revoked principals and the new revision
	EvAuthzRevoked          = "authz.revoked" // node closed a revoked principal's sessions
	EvSOpened               = "s.opened"
	EvSExited               = "s.exited"
	EvSInput                = "s.input"
	EvFSWrite               = "fs.write"
	EvFSApplyTar            = "fs.apply_tar" // one overlay applied; payload carries artifact and counts, fs.write follows per path
	EvFSMkdir               = "fs.mkdir"
	EvFSEdit                = "fs.edit"
	EvFSRemove              = "fs.remove"
	EvFSRename              = "fs.rename"
	EvCredUsed              = "cred.used"
	EvEgressPending         = "egress.pending"
	EvEgressAllowed         = "egress.allowed"
	EvEgressDenied          = "egress.denied"
	EvEgressRedacted        = "egress.redacted"
	EvPolicyUpdated         = "policy.updated"
	EvTimerSet              = "timer.set"
	EvTimerFired            = "timer.fired"
	EvPeerGone              = "peer.gone"
	EvEventGap              = "event.producer_gap"
	EvFleetRequested        = "fleet.quarantine.requested"
	EvFleetTarget           = "fleet.quarantine.target"
	EvFleetCompleted        = "fleet.quarantine.completed"
	EvBaseCreated           = "base.created"
	EvBaseRemoved           = "base.removed"
	EvVolumeCreated         = "volume.created"
	EvVolumePublished       = "volume.published"
	EvVolumeAttached        = "volume.attached"
	EvVolumeDetached        = "volume.detached"
	EvVolumeRemoved         = "volume.removed"
	EvQueueCreated          = "queue.created"                // payload {queue, ws, items}
	EvQueueAdvanced         = "queue.advanced"               // one queued task finished; payload {queue, index, exit, status}
	EvPoolCreated           = "pool.created"                 // a durable pool specification was admitted
	EvPoolRemoved           = "pool.removed"                 // an empty pool specification was removed
	EvPoolScaled            = "pool.scaled"                  // payload {pool, from, to, reason}
	EvPoolFailed            = "pool.provision_failed"        // payload {pool, reason, retry_at}; no credentials
	EvPoolRetiring          = "pool.retiring"                // payload {pool, machine, node}; the node is fenced from new claims before its provider destroy
	EvPoolRetireAborted     = "pool.retire_aborted"          // payload {pool, machine, node}; a definite provider failure lifted the fence
	EvPoolRetired           = "pool.retired"                 // payload {pool, machine, node}; inventory no longer lists the machine and the fence is lifted
	EvExportAdvanced        = "export.cursor.advanced"       // a durable destination cursor advanced after accepting a batch
	EvNotifyUnavailable     = "notifier.unavailable"         // delivery or its durable fallback is unavailable; payload is sanitized
	EvNotifyDeadLetter      = "notifier.dead_lettered"       // failed delivery metadata was durably retained
	EvNotifyDLQPruned       = "notifier.dead_letters_pruned" // bounded retention removed old dead-letter metadata
	EvRunStarted            = "run.started"                  // a harness launch opened its session; payload {s, recipe, task_hash, sandbox, auth}
	EvRunFinished           = "run.finished"                 // that session exited; payload {s, recipe, exit, signal}
	EvAuthWSResident        = "auth.workspace_resident"      // a launch relies on a login the harness keeps inside the workspace; payload {s, recipe}
	EvAuthOperationStarted  = "auth.operation.started"       // confidential provider auth began; payload {s, recipe, action}
	EvAuthOperationFinished = "auth.operation.finished"      // confidential provider auth exited; payload {s, recipe, action, exit, signal}
	EvWSOffer               = "ws.offer"                     // control -> node (not logged; a hint to claim)

	// Durable workspace leases and idle policy (ADR 0090). EvWSLeaseExpired
	// above is the *claim* lease returning a workspace to pending; these are
	// the client-visible hold and its deadline.
	EvWSLeaseGranted          = "ws.lease.granted"           // payload {lease, max_alive_until, min_alive_until, on_expiry, reason}
	EvWSLeaseRenewed          = "ws.lease.renewed"           // payload {lease, max_alive_until, renewals}
	EvWSLeaseCancelled        = "ws.lease.cancelled"         // payload {lease}
	EvWSLeaseHoldExpired      = "ws.lease.expired"           // payload {lease, reason: deadline|moved|destroyed|superseded}
	EvWSIdlePolicySet         = "ws.idle.policy_set"         // payload {sleep_after_sec, destroy_after_sec}
	EvWSIdleMarked            = "ws.idle.marked"             // payload {idle, reason, idle_since}
	EvWSLifecycleExpired      = "ws.lifecycle.expired"       // payload {action, source, at, timer}
	EvWSLifecycleExpiryFailed = "ws.lifecycle.expiry_failed" // payload {action, source, attempts, error}
	EvWSHoldMaxReached        = "ws.hold.max_reached"        // payload {scope, used, limit}
)

// ---------------------------------------------------------------------------
// Computer sessions (browser/computer use over the port substrate)
//
// A computer is a Chrome DevTools Protocol conversation the node holds with a
// browser running inside the workspace. The bytes travel the same resolved TCP
// path a `port.open` session uses, so every backend that can forward a
// workspace port can host one. See ADR 0088.
// ---------------------------------------------------------------------------

const (
	OpComputerCreate     = "computer.create"     // ComputerCreateReq -> ComputerCreateRes
	OpComputerScreenshot = "computer.screenshot" // ComputerScreenshotReq -> ComputerScreenshotRes
	OpComputerInput      = "computer.input"      // ComputerInputReq -> ComputerInputRes
	OpComputerNavigate   = "computer.navigate"   // ComputerNavigateReq -> ComputerNavigateRes
	OpComputerEval       = "computer.eval"       // ComputerEvalReq -> ComputerEvalRes
	OpComputerDownloads  = "computer.downloads"  // ComputerDownloadsReq -> ComputerDownloadsRes
	OpComputerClose      = "computer.close"      // ComputerCloseReq -> {}
	OpComputerGet        = "computer.get"        // ComputerGetReq -> ComputerGetRes
)

// Computer lifecycle events. Payloads name the computer, never page content.
const (
	// EvComputerCreated payload {computer, s, port, attach, profile, w, h, cdp}
	EvComputerCreated = "computer.created"
	// EvComputerClosed payload {computer, reason}; reason is a ComputerClosedReason*.
	EvComputerClosed = "computer.closed"
	// EvComputerDownload payload {computer, artifact, filename, bytes,
	// artifact_bytes, s}; bytes is the file, artifact_bytes the archive.
	EvComputerDownload = "computer.download"
	// EvComputerDegraded payload {computer, reason}
	EvComputerDegraded = "computer.degraded"
)

// Computer states reported by computer.get.
const (
	ComputerStateReady    = "ready"
	ComputerStateDegraded = "degraded"
	ComputerStateClosed   = "closed"
)

// computer.closed reasons. A crash reuses the shared error vocabulary so one
// name covers the event payload and the Error.Reason a later op returns.
const (
	ComputerClosedReasonClosed            = "closed"
	ComputerClosedReasonCrashed           = ReasonBrowserCrashed
	ComputerClosedReasonWorkspaceReleased = "workspace_released"
)

// Action kinds accepted by computer.input.
const (
	ComputerActionClick  = "click"
	ComputerActionType   = "type"
	ComputerActionKey    = "key"
	ComputerActionScroll = "scroll"
	ComputerActionDrag   = "drag"
	ComputerActionMove   = "move"
)

// Download states reported by computer.downloads.
const (
	ComputerDownloadInProgress = "in_progress"
	ComputerDownloadCompleted  = "completed"
	ComputerDownloadCanceled   = "canceled"
	// ComputerDownloadBlocked: the file finished but publication failed.
	ComputerDownloadBlocked = "blocked"
)

// ComputerMaxScreenshotBytes caps one computer.screenshot response. A capture
// larger than this is refused with CodeResourceExhausted rather than streamed:
// a screenshot is a single response body, not a session log.
const ComputerMaxScreenshotBytes = 8 << 20

// ComputerViewport is the CSS-pixel rectangle the browser renders and the
// coordinate system every action uses. The origin is its top-left corner.
type ComputerViewport struct {
	Width  int `cbor:"w,omitempty" json:"w,omitempty"`
	Height int `cbor:"h,omitempty" json:"h,omitempty"`
}

// ComputerLaunch describes how the node obtains a CDP endpoint. Attach means
// the browser is already listening; otherwise the node spawns Program as a
// managed exec session with the workspace's ordinary brokered environment.
type ComputerLaunch struct {
	Program []string `cbor:"program,omitempty" json:"program,omitempty"`
	Port    int      `cbor:"port,omitempty" json:"port,omitempty"`
	Attach  bool     `cbor:"attach,omitempty" json:"attach,omitempty"`
}

type ComputerCreateReq struct {
	WS             string            `cbor:"ws" json:"ws"`
	IdempotencyKey string            `cbor:"idem,omitempty" json:"idem,omitempty"`
	Grant          *Grant            `cbor:"grant,omitempty" json:"grant,omitempty"`
	Launch         *ComputerLaunch   `cbor:"launch,omitempty" json:"launch,omitempty"`
	Viewport       ComputerViewport  `cbor:"viewport,omitempty" json:"viewport,omitempty"`
	Profile        string            `cbor:"profile,omitempty" json:"profile,omitempty"`
	Env            map[string]string `cbor:"env,omitempty" json:"env,omitempty"`
}

type ComputerCreateRes struct {
	Computer   string           `cbor:"computer" json:"computer"`
	Session    string           `cbor:"s,omitempty" json:"s,omitempty"`
	CDPVersion string           `cbor:"cdp,omitempty" json:"cdp,omitempty"`
	Viewport   ComputerViewport `cbor:"viewport,omitempty" json:"viewport,omitempty"`
}

type ComputerScreenshotReq struct {
	WS       string `cbor:"ws" json:"ws"`
	Computer string `cbor:"computer" json:"computer"`
	Grant    *Grant `cbor:"grant,omitempty" json:"grant,omitempty"`
}

type ComputerScreenshotRes struct {
	PNG    []byte `cbor:"png" json:"png"`
	Width  int    `cbor:"w" json:"w"`
	Height int    `cbor:"h" json:"h"`
}

// ComputerAction is one input event in CSS pixels. Unused fields stay zero.
type ComputerAction struct {
	Kind      string `cbor:"kind" json:"kind"`
	X         int    `cbor:"x,omitempty" json:"x,omitempty"`
	Y         int    `cbor:"y,omitempty" json:"y,omitempty"`
	DX        int    `cbor:"dx,omitempty" json:"dx,omitempty"`
	DY        int    `cbor:"dy,omitempty" json:"dy,omitempty"`
	ToX       int    `cbor:"tox,omitempty" json:"tox,omitempty"`
	ToY       int    `cbor:"toy,omitempty" json:"toy,omitempty"`
	Text      string `cbor:"text,omitempty" json:"text,omitempty"`
	Key       string `cbor:"key,omitempty" json:"key,omitempty"`
	Button    string `cbor:"button,omitempty" json:"button,omitempty"`
	Modifiers int    `cbor:"mod,omitempty" json:"mod,omitempty"`
}

// Modifier bits for ComputerAction.Modifiers; the values are CDP's own.
const (
	ComputerModifierAlt   = 1
	ComputerModifierCtrl  = 2
	ComputerModifierMeta  = 4
	ComputerModifierShift = 8
)

type ComputerInputReq struct {
	WS       string           `cbor:"ws" json:"ws"`
	Computer string           `cbor:"computer" json:"computer"`
	Grant    *Grant           `cbor:"grant,omitempty" json:"grant,omitempty"`
	ISeq     uint64           `cbor:"iseq" json:"iseq"`
	Actions  []ComputerAction `cbor:"actions,omitempty" json:"actions,omitempty"`
}

// ComputerInputRes reports whether this request was applied or recognised as a
// duplicate, and the highest input sequence the computer has applied.
type ComputerInputRes struct {
	Applied      bool   `cbor:"applied" json:"applied"`
	LastInputSeq uint64 `cbor:"last_iseq" json:"last_iseq"`
}

type ComputerNavigateReq struct {
	WS             string `cbor:"ws" json:"ws"`
	Computer       string `cbor:"computer" json:"computer"`
	Grant          *Grant `cbor:"grant,omitempty" json:"grant,omitempty"`
	URL            string `cbor:"url" json:"url"`
	IdempotencyKey string `cbor:"idem,omitempty" json:"idem,omitempty"`
}

type ComputerNavigateRes struct {
	URL    string `cbor:"url,omitempty" json:"url,omitempty"`
	Title  string `cbor:"title,omitempty" json:"title,omitempty"`
	Status string `cbor:"status,omitempty" json:"status,omitempty"`
}

// Navigation outcomes reported by ComputerNavigateRes.Status.
const (
	ComputerNavigateLoaded  = "loaded"
	ComputerNavigateTimeout = "timeout"
)

type ComputerEvalReq struct {
	WS         string `cbor:"ws" json:"ws"`
	Computer   string `cbor:"computer" json:"computer"`
	Grant      *Grant `cbor:"grant,omitempty" json:"grant,omitempty"`
	Expression string `cbor:"expr" json:"expr"`
}

// ComputerEvalRes carries the JSON encoding of Runtime.evaluate's returned
// value. It is page-controlled data: decode it, never execute it.
type ComputerEvalRes struct {
	Value []byte `cbor:"value,omitempty" json:"value,omitempty"`
}

type ComputerDownloadsReq struct {
	WS       string `cbor:"ws" json:"ws"`
	Computer string `cbor:"computer" json:"computer"`
	Grant    *Grant `cbor:"grant,omitempty" json:"grant,omitempty"`
}

// ComputerDownload is one file the browser downloaded. Artifact is set once
// the node has published the file into the tenant's artifact store.
type ComputerDownload struct {
	Artifact string `cbor:"artifact,omitempty" json:"artifact,omitempty"`
	Filename string `cbor:"filename,omitempty" json:"filename,omitempty"`
	URL      string `cbor:"url,omitempty" json:"url,omitempty"`
	Bytes    int64  `cbor:"bytes,omitempty" json:"bytes,omitempty"`
	State    string `cbor:"state" json:"state"`
	Reason   string `cbor:"reason,omitempty" json:"reason,omitempty"`
}

type ComputerDownloadsRes struct {
	Downloads []ComputerDownload `cbor:"downloads,omitempty" json:"downloads,omitempty"`
}

type ComputerCloseReq struct {
	WS             string `cbor:"ws" json:"ws"`
	Computer       string `cbor:"computer" json:"computer"`
	Grant          *Grant `cbor:"grant,omitempty" json:"grant,omitempty"`
	IdempotencyKey string `cbor:"idem,omitempty" json:"idem,omitempty"`
}

type ComputerGetReq struct {
	WS       string `cbor:"ws" json:"ws"`
	Computer string `cbor:"computer" json:"computer"`
	Grant    *Grant `cbor:"grant,omitempty" json:"grant,omitempty"`
}

type ComputerGetRes struct {
	Computer string           `cbor:"computer" json:"computer"`
	State    string           `cbor:"state" json:"state"`
	Reason   string           `cbor:"reason,omitempty" json:"reason,omitempty"`
	Viewport ComputerViewport `cbor:"viewport,omitempty" json:"viewport,omitempty"`
	Session  string           `cbor:"s,omitempty" json:"s,omitempty"`
	// LastInputSeq is the highest input sequence the node has applied. A
	// caller that did not create this computer needs it before its first
	// action: a fresh handle counting from one would send a sequence the node
	// has already applied, and the batch would be dropped as a duplicate.
	LastInputSeq uint64 `cbor:"last_iseq,omitempty" json:"last_iseq,omitempty"`
}
