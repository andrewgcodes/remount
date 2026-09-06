package conformance

// The bodies a conformance check needs, transcribed from spec/PROTOCOL.md
// rather than imported. Only the fields an assertion reads are declared:
// §2 rule 2 says unknown fields are ignored within a negotiated version, so a
// partial transcription is the correct client, and a field this file omits is
// a field no requirement depends on.

// WorkspaceSpec is the subset of §5's spec a conformance run sets.
type WorkspaceSpec struct {
	Name        string            `cbor:"name,omitempty"`
	Image       string            `cbor:"image,omitempty"`
	RestoreFrom string            `cbor:"restore_from,omitempty"`
	Requires    Requires          `cbor:"requires"`
	Bindings    []string          `cbor:"bindings,omitempty"`
	Env         map[string]string `cbor:"env,omitempty"`
	MountPath   string            `cbor:"mount_path,omitempty"`
	Security    SecuritySpec      `cbor:"security,omitempty"`
}

// Requires is the placement constraint of §5.
type Requires struct {
	Backend string   `cbor:"backend,omitempty"`
	Caps    []string `cbor:"caps,omitempty"`
	// Profile is §5's node-level runtime-profile constraint. A workspace
	// naming one is placed only on a node whose backend evidence satisfies
	// it, and stays pending with pending_reason otherwise.
	Profile string `cbor:"profile,omitempty"`
}

// SecuritySpec is §5's enforceable security contract, reduced to the profile
// a conformance run selects.
type SecuritySpec struct {
	Profile string `cbor:"profile,omitempty"`
}

// Workspace is §5's resource.
type Workspace struct {
	ID            string        `cbor:"id"`
	Spec          WorkspaceSpec `cbor:"spec"`
	State         string        `cbor:"state"`
	Node          string        `cbor:"node,omitempty"`
	LastSnapshot  string        `cbor:"last_snapshot,omitempty"`
	CreatedAt     int64         `cbor:"created_at"`
	UpdatedAt     int64         `cbor:"updated_at"`
	Generation    uint64        `cbor:"gen"`
	Tenant        string        `cbor:"tenant,omitempty"`
	Owner         string        `cbor:"owner,omitempty"`
	AuthzRevision uint64        `cbor:"authz_revision,omitempty"`
	// PendingReason is §5's explanation for a workspace that has not been
	// placed. It is empty unless the workspace is pending.
	PendingReason string `cbor:"pending_reason,omitempty"`
}

// Workspace states of §5's transition table.
const (
	WSPending  = "pending"
	WSClaiming = "claiming"
	WSClaimed  = "claimed"
	WSPaused   = "paused"
	WSReleased = "released"
	WSFailed   = "failed"
)

// WorkspaceStates is the closed set a conforming implementation may report.
var WorkspaceStates = []string{
	WSPending, WSClaiming, WSClaimed, WSPaused, WSReleased,
	"quiescing", "checkpointing", "destroying", "destroyed", WSFailed,
}

// WSCreateReq is `ws.create`.
type WSCreateReq struct {
	Spec WorkspaceSpec `cbor:"spec"`
	Idem string        `cbor:"idem,omitempty"`
}

// WSGetReq is `ws.get`, `ws.destroy` and `ws.wake` at the control plane, and
// `ws.info` at a node, which is why it carries a grant.
type WSGetReq struct {
	ID    string `cbor:"id"`
	Idem  string `cbor:"idem,omitempty"`
	Grant *Grant `cbor:"grant,omitempty"`
}

// WSListRes is `ws.list`.
type WSListRes struct {
	Workspaces []Workspace `cbor:"workspaces"`
}

// WSMoveReq is `ws.move`.
type WSMoveReq struct {
	ID   string `cbor:"id"`
	Idem string `cbor:"idem,omitempty"`
}

// WSSleepReq is `ws.sleep`.
type WSSleepReq struct {
	ID       string `cbor:"id"`
	AfterSec int64  `cbor:"after_sec,omitempty"`
	OnEvent  string `cbor:"on,omitempty"`
	Idem     string `cbor:"idem,omitempty"`
}

// Timer is the §6 response of `ws.sleep`.
type Timer struct {
	ID string `cbor:"id"`
	WS string `cbor:"ws,omitempty"`
}

// GrantClaims is §4's signed claim set.
type GrantClaims struct {
	Client          string   `cbor:"client"`
	WS              string   `cbor:"ws"`
	Node            string   `cbor:"node"`
	Principal       string   `cbor:"principal,omitempty"`
	Tenant          string   `cbor:"tenant,omitempty"`
	Roles           []string `cbor:"roles,omitempty"`
	AuthzRevision   uint64   `cbor:"authz_revision,omitempty"`
	ExpiresAt       int64    `cbor:"exp"`
	Gen             uint64   `cbor:"gen"`
	ControllerEpoch uint64   `cbor:"controller_epoch,omitempty"`
}

// Grant is §4's offline authorization token.
type Grant struct {
	Claims    GrantClaims `cbor:"claims"`
	Signature []byte      `cbor:"sig"`
	Node      string      `cbor:"node"`
}

// GrantReq is `grant`.
type GrantReq struct {
	WS string `cbor:"ws"`
}

// SOpenReq is `s.open` (§7).
type SOpenReq struct {
	WS         string            `cbor:"ws"`
	Kind       string            `cbor:"kind"`
	Program    []string          `cbor:"program"`
	Cwd        string            `cbor:"cwd,omitempty"`
	Env        map[string]string `cbor:"env,omitempty"`
	Stdin      bool              `cbor:"stdin"`
	TimeoutSec int64             `cbor:"timeout_sec,omitempty"`
	Idem       string            `cbor:"idem,omitempty"`
	Grant      *Grant            `cbor:"grant,omitempty"`
}

// SOpenRes answers `s.open` and `s.attach`.
type SOpenRes struct {
	S            string `cbor:"s"`
	Next         uint64 `cbor:"next"`
	LastInputSeq uint64 `cbor:"last_iseq,omitempty"`
}

// SAttachReq is `s.attach`.
type SAttachReq struct {
	S     string `cbor:"s"`
	From  uint64 `cbor:"from"`
	Grant *Grant `cbor:"grant,omitempty"`
}

// SInputReq is `s.input`.
type SInputReq struct {
	S     string `cbor:"s"`
	ISeq  uint64 `cbor:"iseq"`
	Data  []byte `cbor:"d,omitempty"`
	EOF   bool   `cbor:"eof,omitempty"`
	Grant *Grant `cbor:"grant,omitempty"`
}

// SWaitReq is `s.wait`.
type SWaitReq struct {
	S          string `cbor:"s"`
	TimeoutSec int64  `cbor:"timeout_sec,omitempty"`
	Grant      *Grant `cbor:"grant,omitempty"`
}

// SWaitRes answers `s.wait`.
type SWaitRes struct {
	Exited bool      `cbor:"exited"`
	Exit   *ExitInfo `cbor:"exit,omitempty"`
}

// SessionInfo is the payload of the seq 0 info chunk (§8).
type SessionInfo struct {
	ID       string   `cbor:"id"`
	WS       string   `cbor:"ws"`
	Kind     string   `cbor:"kind"`
	Program  []string `cbor:"program,omitempty"`
	OpenedAt int64    `cbor:"opened_at"`
}

// FSReadReq is `fs.read` (§7).
type FSReadReq struct {
	WS    string `cbor:"ws"`
	Path  string `cbor:"path"`
	Limit int64  `cbor:"limit,omitempty"`
	Grant *Grant `cbor:"grant,omitempty"`
}

// FSReadRes answers `fs.read`.
type FSReadRes struct {
	Data []byte `cbor:"d"`
	Size int64  `cbor:"size"`
	EOF  bool   `cbor:"eof"`
}

// FSWriteReq is `fs.write`.
type FSWriteReq struct {
	WS     string `cbor:"ws"`
	Path   string `cbor:"path"`
	Data   []byte `cbor:"d"`
	MkdirP bool   `cbor:"mkdirp,omitempty"`
	Idem   string `cbor:"idem,omitempty"`
	Grant  *Grant `cbor:"grant,omitempty"`
}

// WSInfoRes answers `ws.info`; Broker is the loopback base URL of §9.
type WSInfoRes struct {
	WS       string   `cbor:"ws"`
	Backend  string   `cbor:"backend"`
	Root     string   `cbor:"root,omitempty"`
	Sessions []string `cbor:"sessions,omitempty"`
	Broker   string   `cbor:"broker,omitempty"`
}

// WSSnapshotReq is `ws.snapshot` (§7).
type WSSnapshotReq struct {
	WS            string `cbor:"ws"`
	Upload        bool   `cbor:"upload"`
	Authoritative bool   `cbor:"authoritative,omitempty"`
	Idem          string `cbor:"idem,omitempty"`
	Grant         *Grant `cbor:"grant,omitempty"`
}

// WSSnapshotRes answers `ws.snapshot`.
type WSSnapshotRes struct {
	Artifact      string `cbor:"artifact"`
	Bytes         int64  `cbor:"bytes"`
	Consistency   string `cbor:"consistency"`
	Authoritative bool   `cbor:"authoritative"`
	Format        string `cbor:"format,omitempty"`
}

// Event is §11's canonical record.
type Event struct {
	Seq        uint64 `cbor:"seq"`
	At         int64  `cbor:"at"`
	Stream     string `cbor:"stream,omitempty"`
	Principal  string `cbor:"principal,omitempty"`
	Node       string `cbor:"node,omitempty"`
	EventID    string `cbor:"event_id,omitempty"`
	Origin     string `cbor:"origin,omitempty"`
	Actor      string `cbor:"actor,omitempty"`
	Tenant     string `cbor:"tenant,omitempty"`
	Workspace  string `cbor:"workspace,omitempty"`
	Generation uint64 `cbor:"generation,omitempty"`
	// ControllerEpoch is §3.2's fence, persisted on every event by a peer
	// that negotiated controller-epoch.
	ControllerEpoch uint64 `cbor:"controller_epoch,omitempty"`
	Session         string `cbor:"session,omitempty"`
	Type            string `cbor:"type"`
	Payload         []byte `cbor:"payload,omitempty"`
	Cause           uint64 `cbor:"cause,omitempty"`
}

// EventsTailReq is `events.tail`.
type EventsTailReq struct {
	From         uint64 `cbor:"from"`
	Follow       bool   `cbor:"follow"`
	WS           string `cbor:"ws,omitempty"`
	Subscription string `cbor:"sub,omitempty"`
}

// EventsRes is what a non-following `events.tail` returns.
type EventsRes struct {
	Events []Event `cbor:"events"`
}

// NodeStatus is one row of `node.list`.
type NodeStatus struct {
	ID       string   `cbor:"id"`
	Online   bool     `cbor:"online"`
	LastSeen int64    `cbor:"last_seen"`
	Protocol []string `cbor:"protocol,omitempty"`
}

// NodeListRes answers `node.list`.
type NodeListRes struct {
	Nodes []NodeStatus `cbor:"nodes"`
}

// AgentSpec is §6.1's harness description.
type AgentSpec struct {
	Recipe     string   `cbor:"recipe"`
	Task       string   `cbor:"task"`
	Mode       string   `cbor:"mode,omitempty"`
	ACPCommand []string `cbor:"acp_command,omitempty"`
}

// AgentPolicy is §6.1's policy.
type AgentPolicy struct {
	SleepAfterSec int64  `cbor:"sleep_after_sec,omitempty"`
	Approve       string `cbor:"approve,omitempty"`
	MaxTurns      int    `cbor:"max_turns,omitempty"`
	StartAt       int64  `cbor:"start_at,omitempty"`
}

// AgentCreateReq is `agent.create`.
type AgentCreateReq struct {
	Name      string         `cbor:"name,omitempty"`
	WS        string         `cbor:"ws,omitempty"`
	Workspace *WorkspaceSpec `cbor:"workspace,omitempty"`
	Spec      AgentSpec      `cbor:"spec"`
	Policy    AgentPolicy    `cbor:"policy"`
	Parent    string         `cbor:"parent,omitempty"`
	Idem      string         `cbor:"idem,omitempty"`
}

// AgentMessage is one inbox entry.
type AgentMessage struct {
	ID   string `cbor:"id"`
	Kind string `cbor:"kind"`
	Text string `cbor:"text"`
	By   string `cbor:"by,omitempty"`
	At   int64  `cbor:"at"`
}

// Agent is §6.1's durable resource.
type Agent struct {
	ID              string         `cbor:"id"`
	Tenant          string         `cbor:"tenant"`
	Owner           string         `cbor:"owner"`
	Name            string         `cbor:"name,omitempty"`
	WS              string         `cbor:"ws"`
	OwnsWS          bool           `cbor:"owns_ws,omitempty"`
	Spec            AgentSpec      `cbor:"spec"`
	Mode            string         `cbor:"mode"`
	Status          string         `cbor:"status"`
	StatusReason    string         `cbor:"status_reason,omitempty"`
	Inbox           []AgentMessage `cbor:"inbox"`
	Turns           int            `cbor:"turns"`
	Policy          AgentPolicy    `cbor:"policy"`
	TranscriptNext  uint64         `cbor:"transcript_next,omitempty"`
	TranscriptFirst uint64         `cbor:"transcript_first,omitempty"`
	CreatedAt       int64          `cbor:"created_at"`
	UpdatedAt       int64          `cbor:"updated_at"`
}

// AgentGetReq is `agent.get`, `agent.cancel`, `agent.sleep` and
// `agent.destroy`.
type AgentGetReq struct {
	ID   string `cbor:"id"`
	Idem string `cbor:"idem,omitempty"`
}

// AgentWakeReq is `agent.wake`.
type AgentWakeReq struct {
	ID   string `cbor:"id"`
	By   string `cbor:"by,omitempty"`
	Idem string `cbor:"idem,omitempty"`
}

// AgentListReq is `agent.list`.
type AgentListReq struct {
	Status string `cbor:"status,omitempty"`
	WS     string `cbor:"ws,omitempty"`
	Parent string `cbor:"parent,omitempty"`
}

// AgentListRes answers `agent.list`.
type AgentListRes struct {
	Agents []Agent `cbor:"agents"`
}

// AgentMessageReq is `agent.message`.
type AgentMessageReq struct {
	ID   string `cbor:"id"`
	Text string `cbor:"text"`
	Kind string `cbor:"kind,omitempty"`
	Idem string `cbor:"idem,omitempty"`
}

// AgentMessageRes answers `agent.message`.
type AgentMessageRes struct {
	Agent    Agent        `cbor:"agent"`
	Message  AgentMessage `cbor:"message"`
	Degraded bool         `cbor:"degraded,omitempty"`
	Woken    bool         `cbor:"woken,omitempty"`
}

// AgentTranscriptReq is `agent.transcript`.
type AgentTranscriptReq struct {
	ID    string `cbor:"id"`
	From  uint64 `cbor:"from,omitempty"`
	Limit int    `cbor:"limit,omitempty"`
}

// TranscriptRecord is one mirrored record (§6.2).
type TranscriptRecord struct {
	Index  uint64 `cbor:"index"`
	Run    string `cbor:"run"`
	Seq    uint64 `cbor:"seq"`
	Stream uint8  `cbor:"stream"`
	At     int64  `cbor:"at"`
	Data   []byte `cbor:"data"`
}

// TranscriptGap names an evicted index range.
type TranscriptGap struct {
	From uint64 `cbor:"from"`
	To   uint64 `cbor:"to"`
}

// AgentTranscriptRes answers `agent.transcript`.
type AgentTranscriptRes struct {
	Records []TranscriptRecord `cbor:"records"`
	Next    uint64             `cbor:"next"`
	Gap     *TranscriptGap     `cbor:"gap,omitempty"`
	Done    bool               `cbor:"done,omitempty"`
}

// ApprovalListReq is `approval.list`.
type ApprovalListReq struct {
	Agent  string `cbor:"agent,omitempty"`
	Kind   string `cbor:"kind,omitempty"`
	Status string `cbor:"status,omitempty"`
}

// Approval is §6.1's durable human decision point.
type Approval struct {
	ID          string `cbor:"id"`
	Agent       string `cbor:"agent,omitempty"`
	WS          string `cbor:"ws,omitempty"`
	Rule        string `cbor:"rule,omitempty"`
	Host        string `cbor:"host,omitempty"`
	Method      string `cbor:"method,omitempty"`
	Fingerprint string `cbor:"fingerprint,omitempty"`
	Kind        string `cbor:"kind"`
	Status      string `cbor:"status"`
	CreatedAt   int64  `cbor:"created_at"`
}

// ApprovalListRes answers `approval.list`.
type ApprovalListRes struct {
	Approvals []Approval `cbor:"approvals"`
}

// ApprovalGetReq is `approval.get`.
type ApprovalGetReq struct {
	ID string `cbor:"id"`
}

// ApprovalDecideReq is `approval.decide`.
type ApprovalDecideReq struct {
	ID       string `cbor:"id"`
	Option   string `cbor:"option,omitempty"`
	Denied   bool   `cbor:"denied,omitempty"`
	Remember string `cbor:"remember,omitempty"`
	Idem     string `cbor:"idem,omitempty"`
}

// CanonicalEventTypes is §11's list of canonical types, transcribed. A type
// outside it is an extension, which §11 permits; a lifecycle event that is
// absent is not.
var CanonicalEventTypes = []string{
	"node.enrolled", "node.online", "node.offline",
	"ws.created", "ws.claiming", "ws.claimed", "ws.released", "ws.moved",
	"ws.paused", "ws.resumed", "ws.snapshot", "ws.restored", "ws.destroyed",
	"ws.lease_expired", "ws.acl", "authz.revoked", "ws.fenced",
	"ws.state_changed",
	"s.opened", "s.exited",
	"fs.write", "fs.edit", "fs.remove", "fs.apply_tar",
	"cred.used", "egress.allowed", "egress.denied", "egress.redacted",
	"egress.pending", "policy.updated",
	"timer.set", "timer.fired", "peer.gone", "event.producer_gap",
	"fleet.quarantine.requested", "fleet.quarantine.target",
	"fleet.quarantine.completed",
	"base.created", "base.removed",
	"volume.created", "volume.published", "volume.attached",
	"volume.detached", "volume.removed",
	"run.started", "run.finished", "auth.workspace_resident",
	"queue.created", "queue.advanced",
	"pool.created", "pool.removed", "pool.scaled", "pool.provision_failed",
	"pool.retiring", "pool.retire_aborted", "pool.retired",
	"repo.cloned",
	"agent.created", "agent.message", "agent.run.started",
	"agent.run.finished", "agent.session", "agent.turn", "agent.tool_call",
	"agent.waiting", "agent.cancelled", "agent.slept", "agent.woken",
	"agent.forked", "agent.failed", "agent.finished", "agent.destroyed",
	"agent.child.finished",
	"approval.pending", "approval.decided", "approval.expired",
	"budget.created", "budget.removed", "budget.reserved", "budget.settled",
	"budget.expired", "budget.unmetered",
	"notifier.unavailable", "notifier.dead_lettered",
	"notifier.dead_letters_pruned",
	"identity.principal_created", "identity.roles_changed",
	"identity.principal_revoked",
	"session.log.committed", "session.log.deleted", "session.log.unavailable",
	"audit.exported", "audit.export_denied", "retention.enforced",
	"retention.violation", "residency.denied", "export.cursor.advanced",
}
