package proto

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"strings"
	"unicode/utf8"
)

// An Agent is a durable conversation with a harness that owns exactly one
// workspace (ADR 0043). The control plane holds the resource; a node runs the
// harness as an ACP server (or on a pty when the recipe has no ACP command)
// and reports what happened. Status is derived from those reports and the
// workspace state, never set by the harness.
type Agent struct {
	ID     string `cbor:"id" json:"id"`
	Tenant string `cbor:"tenant" json:"tenant"`
	Owner  string `cbor:"owner" json:"owner"`
	Name   string `cbor:"name,omitempty" json:"name,omitempty"`
	WS     string `cbor:"ws" json:"ws"`
	// OwnsWS records that agent.create made the workspace; destroy takes it
	// along. An adopted workspace (--ws) outlives its agent.
	OwnsWS bool      `cbor:"owns_ws,omitempty" json:"owns_ws,omitempty"`
	Spec   AgentSpec `cbor:"spec" json:"spec"`
	Mode   string    `cbor:"mode" json:"mode"` // AgentModeACP | AgentModePTY
	// ACPSessionID is the harness's own session id, "" until the first run
	// reports one. It is what session/load continues.
	ACPSessionID string           `cbor:"acp_session_id,omitempty" json:"acp_session_id,omitempty"`
	Capabilities *ACPCapabilities `cbor:"capabilities,omitempty" json:"capabilities,omitempty"`
	Status       string           `cbor:"status" json:"status"`
	// StatusReason explains failed and finished.
	StatusReason string `cbor:"status_reason,omitempty" json:"status_reason,omitempty"`
	// Inbox is the FIFO of messages not yet consumed by a turn. A message
	// leaves the inbox when the node reports the turn that carries it.
	Inbox  []AgentMessage `cbor:"inbox" json:"inbox"`
	Runs   []AgentRun     `cbor:"runs" json:"runs"`
	Turns  int            `cbor:"turns" json:"turns"`
	Parent string         `cbor:"parent,omitempty" json:"parent,omitempty"`
	// ForkedFrom names the agent whose snapshot this one started from.
	ForkedFrom string      `cbor:"forked_from,omitempty" json:"forked_from,omitempty"`
	Policy     AgentPolicy `cbor:"policy" json:"policy"`
	// TranscriptSession is the node session (kind acp or pty) holding the
	// transcript of the current or last run.
	TranscriptSession string `cbor:"transcript_session,omitempty" json:"transcript_session,omitempty"`
	TranscriptNode    string `cbor:"transcript_node,omitempty" json:"transcript_node,omitempty"`
	// TranscriptNext is the index the next mirrored transcript record gets;
	// TranscriptFirst is the oldest index still retained (records below it
	// were evicted to stay within MaxTranscriptBytesPerAgent, accounted in
	// TranscriptBytes). A reader at TranscriptNext has everything.
	TranscriptNext  uint64 `cbor:"transcript_next,omitempty" json:"transcript_next,omitempty"`
	TranscriptFirst uint64 `cbor:"transcript_first,omitempty" json:"transcript_first,omitempty"`
	TranscriptBytes int64  `cbor:"transcript_bytes,omitempty" json:"transcript_bytes,omitempty"`
	// PendingApprovals counts approvals waiting on a human; it is what makes
	// status waiting_approval.
	PendingApprovals int    `cbor:"pending_approvals,omitempty" json:"pending_approvals,omitempty"`
	URL              string `cbor:"url,omitempty" json:"url,omitempty"`
	CreatedAt        int64  `cbor:"created_at" json:"created_at"`
	UpdatedAt        int64  `cbor:"updated_at" json:"updated_at"`
	// WakeTimer is the timer set when the agent's workspace was put to sleep
	// by policy; a message cancels it by waking.
	WakeTimer string `cbor:"wake_timer,omitempty" json:"wake_timer,omitempty"`
	// IdleSince is when the agent last became waiting_input; Policy.SleepAfterSec
	// counts from here.
	IdleSince int64 `cbor:"idle_since,omitempty" json:"idle_since,omitempty"`
	// Failures counts consecutive runs that ended in error. One is retried
	// (with session/load); the second fails the agent.
	Failures int `cbor:"failures,omitempty" json:"failures,omitempty"`
	// ParentNotified is set once a finished child has been reported to its
	// parent (agent.child.finished plus an inbox message).
	ParentNotified bool `cbor:"parent_notified,omitempty" json:"parent_notified,omitempty"`
}

// Agent.Mode values.
const (
	AgentModeACP = "acp"
	AgentModePTY = "pty"
)

// AgentSpec.Sandbox values.
const (
	AgentSandboxReadOnly       = "read-only"
	AgentSandboxWorkspaceWrite = "workspace-write"
	AgentSandboxFull           = "full"
)

// Agent.Status values. The order here is the order a healthy agent moves
// through them; sleeping, failed, finished and destroyed are the exits.
const (
	AgentCreating        = "creating"         // workspace not yet claimed
	AgentScheduled       = "scheduled"        // Policy.StartAt is in the future; nothing runs before it
	AgentRunning         = "running"          // a prompt is in flight or queued
	AgentWaitingInput    = "waiting_input"    // end_turn with an empty inbox; the harness is alive and waiting
	AgentIdle            = "idle"             // no harness process and nothing queued; a message starts a new run
	AgentWaitingApproval = "waiting_approval" // a permission request is parked as an Approval
	AgentSleeping        = "sleeping"         // workspace paused; a message wakes it
	AgentFailed          = "failed"           // harness exited non-zero or broke protocol twice
	AgentFinished        = "finished"         // policy max_turns reached
	AgentDestroyed       = "destroyed"
)

// AgentSpec is what the node needs to launch the harness. Nothing in it is a
// secret: providers are binding names the node resolves through its broker.
type AgentSpec struct {
	Recipe string `cbor:"recipe" json:"recipe"`
	// RecipeYAML carries a user-supplied recipe (remount run -f); empty for a
	// built-in one.
	RecipeYAML string `cbor:"recipe_yaml,omitempty" json:"recipe_yaml,omitempty"`
	// Task is the first message. It is kept here so a fork or a restart can
	// show what the agent was asked, and hashed into events, never copied.
	Task      string   `cbor:"task" json:"task"`
	Providers []string `cbor:"providers,omitempty" json:"providers,omitempty"`
	Primary   string   `cbor:"primary,omitempty" json:"primary,omitempty"`
	// BindingSpecs are the non-secret ID:preset declarations the client used.
	// They let a recipe recover provider environment names when it adopts an
	// existing workspace whose bindings were attached without launch labels.
	BindingSpecs []string `cbor:"binding_specs,omitempty" json:"binding_specs,omitempty"`
	Model        string   `cbor:"model,omitempty" json:"model,omitempty"`
	Sandbox      string   `cbor:"sandbox,omitempty" json:"sandbox,omitempty"`
	// ACPCommand overrides the recipe's acp.command (remount agent create
	// custom --acp-cmd). It runs in the workspace like everything else.
	ACPCommand []string `cbor:"acp_command,omitempty" json:"acp_command,omitempty"`
	// Auth is RunAuthAPIKey, RunAuthSubscription or the legacy
	// RunAuthWorkspaceResident (ADR 0039).
	Auth string `cbor:"auth,omitempty" json:"auth,omitempty"`
	// Mode is AgentModeACP or AgentModePTY; empty means ACP. The client that
	// resolved the recipe knows whether it has an ACP server.
	Mode string `cbor:"mode,omitempty" json:"mode,omitempty"`
}

// AgentPolicy is what the control plane enforces around the harness.
type AgentPolicy struct {
	// SleepAfterSec puts the workspace to sleep that long after the agent
	// starts waiting for input. Zero never sleeps by policy.
	SleepAfterSec int64 `cbor:"sleep_after_sec,omitempty" json:"sleep_after_sec,omitempty"`
	// Approve is ApproveNever, ApproveAuto or ApproveOnRequest.
	Approve string `cbor:"approve,omitempty" json:"approve,omitempty"`
	// MaxTurns finishes the agent after that many prompt turns; zero is
	// unlimited.
	MaxTurns int `cbor:"max_turns,omitempty" json:"max_turns,omitempty"`
	// StartAt (Unix ms) holds every run until then: the agent is created,
	// its workspace provisioned, and its inbox kept, but the harness is not
	// launched before StartAt. Zero starts immediately.
	StartAt int64 `cbor:"start_at,omitempty" json:"start_at,omitempty"`
}

// AgentPolicy.Approve values.
const (
	// ApproveNever denies every permission request; the harness runs
	// read-only where it honors that.
	ApproveNever = "never"
	// ApproveAuto allows every permission request. It is only sane because
	// the workspace is isolated and credential-blind.
	ApproveAuto = "auto"
	// ApproveOnRequest parks each request as an Approval for a human.
	ApproveOnRequest = "on-request"
)

// AgentMessage is one user message to the agent.
type AgentMessage struct {
	ID   string `cbor:"id" json:"id"`
	Kind string `cbor:"kind" json:"kind"` // AgentMessageFollowUp | AgentMessageSteer
	Text string `cbor:"text" json:"text"`
	By   string `cbor:"by,omitempty" json:"by,omitempty"`
	At   int64  `cbor:"at" json:"at"`
}

// AgentMessage.Kind values.
const (
	// AgentMessageFollowUp is delivered as the next prompt turn.
	AgentMessageFollowUp = "follow_up"
	// AgentMessageSteer asks to interrupt the current turn. ACP has no
	// mid-turn steering, so it is queued as a follow-up and the response says
	// degraded.
	AgentMessageSteer = "steer"
	// AgentMessageChild is the control plane's summary of a finished child
	// agent, delivered to the parent as a prompt turn. By is "agent:<id>".
	AgentMessageChild = "child"
)

// ChildSummary is the JSON text of an AgentMessageChild message.
type ChildSummary struct {
	Child  string `json:"child"`
	Name   string `json:"name,omitempty"`
	Status string `json:"status"`
	Reason string `json:"reason,omitempty"`
	Turns  int    `json:"turns"`
	WS     string `json:"ws"`
	URL    string `json:"url,omitempty"`
}

// AgentRun is one launch of the harness process. A run spans many turns; it
// ends when the process exits, is cancelled, or is lost with its node.
type AgentRun struct {
	ID         string `cbor:"id" json:"id"`
	Attempt    int    `cbor:"attempt" json:"attempt"`
	Node       string `cbor:"node,omitempty" json:"node,omitempty"`
	Generation uint64 `cbor:"gen,omitempty" json:"gen,omitempty"`
	State      string `cbor:"state" json:"state"` // AgentRunPending | AgentRunActive | AgentRunDone
	Transcript string `cbor:"transcript,omitempty" json:"transcript,omitempty"`
	Loaded     bool   `cbor:"loaded,omitempty" json:"loaded,omitempty"` // session/load replayed history
	StartedAt  int64  `cbor:"started_at,omitempty" json:"started_at,omitempty"`
	FinishedAt int64  `cbor:"finished_at,omitempty" json:"finished_at,omitempty"`
	StopReason string `cbor:"stop_reason,omitempty" json:"stop_reason,omitempty"`
	Error      string `cbor:"error,omitempty" json:"error,omitempty"`
	Turns      int    `cbor:"turns" json:"turns"`
	// LastReport is the highest report sequence applied, so a redelivered
	// report is a no-op.
	LastReport uint64 `cbor:"last_report,omitempty" json:"last_report,omitempty"`
	// TurnMessage is the inbox message the current turn carries.
	TurnMessage string `cbor:"turn_message,omitempty" json:"turn_message,omitempty"`
}

// AgentRun.State values.
const (
	AgentRunPending = "pending" // sent to the node, no report yet
	AgentRunActive  = "active"
	AgentRunDone    = "done"
)

// ACPCapabilities records what the harness advertised in initialize.
type ACPCapabilities struct {
	LoadSession           bool `cbor:"load_session,omitempty" json:"load_session,omitempty"`
	ResumeSession         bool `cbor:"resume_session,omitempty" json:"resume_session,omitempty"`
	CloseSession          bool `cbor:"close_session,omitempty" json:"close_session,omitempty"`
	AdditionalDirectories bool `cbor:"additional_directories,omitempty" json:"additional_directories,omitempty"`
}

// ACPFrameRecord is the data of one StreamACPIn/StreamACPOut chunk: the
// JSON-RPC line as exchanged, after redaction, with the metadata a replayer
// needs to de-duplicate history that session/load re-sent.
type ACPFrameRecord struct {
	At    int64           `cbor:"at" json:"at"`
	Frame json.RawMessage `cbor:"frame" json:"frame"`
	// Replayed marks session/update notifications delivered while a
	// session/load was in flight: they restate earlier turns and a transcript
	// reader that already has them skips them.
	Replayed bool `cbor:"replayed,omitempty" json:"replayed,omitempty"`
	// Redacted marks that a secret-shaped value in the frame was replaced.
	Redacted bool `cbor:"redacted,omitempty" json:"redacted,omitempty"`
	// Truncated marks a frame cut at MaxACPTranscriptFrame.
	Truncated bool `cbor:"truncated,omitempty" json:"truncated,omitempty"`
}

// MaxACPTranscriptFrame bounds one recorded frame; a bigger one is cut and
// marked, so a harness that dumps a file into a tool result cannot fill the
// node's session log with it.
const MaxACPTranscriptFrame = 256 << 10

// ACPRecordAssembler rebuilds ACPFrameRecords from transcript chunks. The
// session log splits one Record call into several chunks above its chunk
// size, so a record can span consecutive chunks of the same stream; the
// assembler concatenates until the bytes decode as one record. A definite
// CBOR map never decodes from a strict prefix of itself, so the boundary is
// unambiguous without a length header.
type ACPRecordAssembler struct {
	stream uint8
	buf    []byte
}

// Push adds one chunk. It returns the completed record and true when data
// finished one, false when more chunks are needed. A chunk on a different
// stream than the partial record discards the partial bytes and reports
// them as an error, since the transcript is then already inconsistent.
func (a *ACPRecordAssembler) Push(stream uint8, data []byte) (ACPFrameRecord, bool, error) {
	if stream != StreamACPIn && stream != StreamACPOut {
		return ACPFrameRecord{}, false, Err(CodeBadRequest, "stream %d is not an ACP transcript stream", stream)
	}
	if len(a.buf) > 0 && stream != a.stream {
		a.buf = a.buf[:0]
		return ACPFrameRecord{}, false, Err(CodeInternal, "acp transcript: stream %d interleaved a partial record on stream %d", stream, a.stream)
	}
	a.stream = stream
	a.buf = append(a.buf, data...)
	var rec ACPFrameRecord
	if err := Unmarshal(a.buf, &rec); err != nil {
		if len(a.buf) > MaxACPTranscriptFrame+4096 {
			a.buf = a.buf[:0]
			return ACPFrameRecord{}, false, Err(CodeInternal, "acp transcript: record exceeds %d bytes without decoding: %v", MaxACPTranscriptFrame, err)
		}
		return ACPFrameRecord{}, false, nil
	}
	a.buf = a.buf[:0]
	return rec, true, nil
}

// Pending reports whether a partial record is buffered.
func (a *ACPRecordAssembler) Pending() bool { return len(a.buf) > 0 }

// Agent bounds.
const (
	MaxAgentMessageSize = 256 << 10
	MaxAgentInbox       = 64
	MaxAgentRuns        = 64 // older runs are trimmed from the resource; the log keeps them
)

// AgentTaskHash is the digest events carry instead of the text.
func AgentTaskHash(text string) string {
	sum := sha256.Sum256([]byte(text))
	return hex.EncodeToString(sum[:8])
}

// ValidateAgentMessage is the admission check for one message body.
func ValidateAgentMessage(text string) error {
	if strings.TrimSpace(text) == "" {
		return Err(CodeBadRequest, "message is empty")
	}
	if len(text) > MaxAgentMessageSize {
		return Err(CodeBadRequest, "message is %d bytes; the limit is %d", len(text), MaxAgentMessageSize)
	}
	if !utf8.ValidString(text) {
		return Err(CodeBadRequest, "message is not valid UTF-8")
	}
	return nil
}

// ---------------------------------------------------------------------------
// Approvals (ADR 0044)
// ---------------------------------------------------------------------------

// An Approval is one question parked for a human: a harness permission
// request, an ACP elicitation, or an egress decision. One list, one CLI, one
// wake path.
type Approval struct {
	ID          string `cbor:"id" json:"id"`
	Tenant      string `cbor:"tenant" json:"tenant"`
	Owner       string `cbor:"owner" json:"owner"`
	Agent       string `cbor:"agent,omitempty" json:"agent,omitempty"`
	WS          string `cbor:"ws,omitempty" json:"ws,omitempty"`
	Run         string `cbor:"run,omitempty" json:"run,omitempty"`
	Principal   string `cbor:"principal,omitempty" json:"principal,omitempty"`
	Rule        string `cbor:"rule,omitempty" json:"rule,omitempty"`
	Host        string `cbor:"host,omitempty" json:"host,omitempty"`
	Method      string `cbor:"method,omitempty" json:"method,omitempty"`
	PathHash    string `cbor:"path_hash,omitempty" json:"path_hash,omitempty"`
	BodyHash    string `cbor:"body_hash,omitempty" json:"body_hash,omitempty"`
	Fingerprint string `cbor:"fingerprint,omitempty" json:"fingerprint,omitempty"`
	Kind        string `cbor:"kind" json:"kind"` // ApprovalToolCall | ApprovalElicitation | ApprovalEgress
	Title       string `cbor:"title,omitempty" json:"title,omitempty"`
	// ToolCall is the harness's tool call id the request belongs to.
	ToolCall  string           `cbor:"tool_call,omitempty" json:"tool_call,omitempty"`
	ToolKind  string           `cbor:"tool_kind,omitempty" json:"tool_kind,omitempty"`
	Locations []string         `cbor:"locations,omitempty" json:"locations,omitempty"`
	Options   []ApprovalOption `cbor:"options,omitempty" json:"options,omitempty"`
	// Detail is the harness's request verbatim (an ACP RequestPermissionRequest
	// or CreateElicitationRequest), so a UI can render what the schema offers
	// without Remount modelling every field.
	Detail   json.RawMessage   `cbor:"detail,omitempty" json:"detail,omitempty"`
	Status   string            `cbor:"status" json:"status"` // ApprovalPending | ApprovalDecided | ApprovalExpired
	Decision *ApprovalDecision `cbor:"decision,omitempty" json:"decision,omitempty"`
	// DeliveredAt is when the node acknowledged the decision; zero means the
	// control plane is still trying to hand it to the run, -1 that the run
	// ended before it could (the harness will ask again).
	DeliveredAt int64 `cbor:"delivered_at,omitempty" json:"delivered_at,omitempty"`
	CreatedAt   int64 `cbor:"created_at" json:"created_at"`
	UpdatedAt   int64 `cbor:"updated_at" json:"updated_at"`
	ExpiresAt   int64 `cbor:"expires_at,omitempty" json:"expires_at,omitempty"`
}

// ApprovalOption is one answer the harness offered.
type ApprovalOption struct {
	ID   string `cbor:"id" json:"id"`
	Name string `cbor:"name,omitempty" json:"name,omitempty"`
	Kind string `cbor:"kind,omitempty" json:"kind,omitempty"` // allow_once | allow_always | reject_once | reject_always
}

// ApprovalDecision is what the human answered.
type ApprovalDecision struct {
	// Option is the chosen ApprovalOption id. Empty with Denied means the
	// request was rejected without picking a specific reject option.
	Option string `cbor:"option,omitempty" json:"option,omitempty"`
	Denied bool   `cbor:"denied,omitempty" json:"denied,omitempty"`
	// Content answers an elicitation: the form fields as the schema asked.
	Content  json.RawMessage `cbor:"content,omitempty" json:"content,omitempty"`
	By       string          `cbor:"by" json:"by"`
	At       int64           `cbor:"at" json:"at"`
	Remember string          `cbor:"remember,omitempty" json:"remember,omitempty"` // none | host | rule (egress only)
}

// Approval.Kind values.
const (
	ApprovalToolCall    = "tool_call"
	ApprovalElicitation = "elicitation"
	ApprovalEgress      = "egress"
)

// Approval.Status values.
const (
	ApprovalPending = "pending"
	ApprovalDecided = "decided"
	// ApprovalExpired: the run that asked ended before a decision. ACP has no
	// replay for pending permissions; the harness asks again next turn.
	ApprovalExpired = "expired"
)

// Egress approval remember scopes.
const (
	ApprovalRememberNone = "none"
	ApprovalRememberHost = "host"
	ApprovalRememberRule = "rule"
)

// ---------------------------------------------------------------------------
// ops: client -> control
// ---------------------------------------------------------------------------

const (
	OpAgentCreate  = "agent.create"  // AgentCreateReq -> Agent
	OpAgentGet     = "agent.get"     // AgentGetReq -> Agent
	OpAgentList    = "agent.list"    // AgentListReq -> AgentListRes
	OpAgentMessage = "agent.message" // AgentMessageReq -> AgentMessageRes
	OpAgentCancel  = "agent.cancel"  // AgentGetReq -> Agent
	OpAgentSleep   = "agent.sleep"   // AgentGetReq -> Agent
	OpAgentFork    = "agent.fork"    // AgentForkReq -> Agent
	OpAgentDestroy = "agent.destroy" // AgentGetReq -> {}
	// OpAgentTranscript reads the control plane's durable copy of the
	// transcript. It never touches the node, so it never wakes a workspace.
	OpAgentTranscript = "agent.transcript" // AgentTranscriptReq -> AgentTranscriptRes
	// OpAgentWake resumes a sleeping agent's workspace without sending it a
	// message: a preview or a diff wants the tree, not a turn.
	OpAgentWake = "agent.wake" // AgentWakeReq -> Agent

	OpApprovalList   = "approval.list"   // ApprovalListReq -> ApprovalListRes
	OpApprovalGet    = "approval.get"    // ApprovalGetReq -> Approval
	OpApprovalDecide = "approval.decide" // ApprovalDecideReq -> Approval

	// control -> node
	OpAgentRun             = "agent.run"              // AgentRunReq -> AgentRunRes: start the harness for a run
	OpAgentDeliver         = "agent.deliver"          // AgentDeliverReq -> {}: hand a message to a live run
	OpAgentRunCancel       = "agent.run.cancel"       // AgentRunCancelReq -> {}
	OpAgentApprovalDecided = "agent.approval.decided" // AgentApprovalDecidedReq -> {}

	// node -> control
	OpAgentReport = "agent.report" // AgentReport -> {}
)

// AgentCreateReq makes an Agent and, unless WS names an existing workspace,
// its workspace.
type AgentCreateReq struct {
	Name string `cbor:"name,omitempty" json:"name,omitempty"`
	// WS adopts an existing workspace; Workspace describes a new one.
	WS        string         `cbor:"ws,omitempty" json:"ws,omitempty"`
	Workspace *WorkspaceSpec `cbor:"workspace,omitempty" json:"workspace,omitempty"`
	Spec      AgentSpec      `cbor:"spec" json:"spec"`
	Policy    AgentPolicy    `cbor:"policy" json:"policy"`
	Parent    string         `cbor:"parent,omitempty" json:"parent,omitempty"`
	// ACPSessionID continues a conversation the harness already has state
	// for (handoff from a laptop).
	ACPSessionID   string `cbor:"acp_session_id,omitempty" json:"acp_session_id,omitempty"`
	IdempotencyKey string `cbor:"idem,omitempty" json:"idem,omitempty"`
}

// AgentGetReq names one agent.
type AgentGetReq struct {
	ID             string `cbor:"id" json:"id"`
	IdempotencyKey string `cbor:"idem,omitempty" json:"idem,omitempty"`
}

// AgentWakeReq resumes a sleeping agent's workspace. By names the surface
// that asked (AgentWokenByPreview, AgentWokenByDiff or AgentWokenByRequest)
// and lands in the agent.woken event.
type AgentWakeReq struct {
	ID             string `cbor:"id" json:"id"`
	By             string `cbor:"by,omitempty" json:"by,omitempty"`
	IdempotencyKey string `cbor:"idem,omitempty" json:"idem,omitempty"`
}

// agent.woken payload "by" values a client may supply. The control plane
// adds message, approval and timer on its own wakes.
const (
	AgentWokenByRequest = "request"
	AgentWokenByPreview = "preview"
	AgentWokenByDiff    = "diff"
)

// AgentListReq filters the agents the subject may read.
type AgentListReq struct {
	Status string `cbor:"status,omitempty" json:"status,omitempty"`
	WS     string `cbor:"ws,omitempty" json:"ws,omitempty"`
	Parent string `cbor:"parent,omitempty" json:"parent,omitempty"`
}

// AgentListRes lists agents.
type AgentListRes struct {
	Agents []Agent `cbor:"agents" json:"agents"`
}

// AgentMessageReq appends to the inbox. A sleeping agent wakes.
type AgentMessageReq struct {
	ID             string `cbor:"id" json:"id"`
	Text           string `cbor:"text" json:"text"`
	Kind           string `cbor:"kind,omitempty" json:"kind,omitempty"`
	IdempotencyKey string `cbor:"idem,omitempty" json:"idem,omitempty"`
}

// AgentMessageRes is the accepted message and whether it was downgraded.
type AgentMessageRes struct {
	Agent   Agent        `cbor:"agent" json:"agent"`
	Message AgentMessage `cbor:"message" json:"message"`
	// Degraded: a steer was queued as a follow-up because the harness cannot
	// take input mid-turn.
	Degraded bool `cbor:"degraded,omitempty" json:"degraded,omitempty"`
	// Woken: the workspace was asleep and is being materialized.
	Woken bool `cbor:"woken,omitempty" json:"woken,omitempty"`
}

// AgentForkReq snapshots the agent's workspace and starts a new agent from
// the copy with the same harness session.
type AgentForkReq struct {
	ID             string       `cbor:"id" json:"id"`
	Name           string       `cbor:"name,omitempty" json:"name,omitempty"`
	Task           string       `cbor:"task,omitempty" json:"task,omitempty"`
	Policy         *AgentPolicy `cbor:"policy,omitempty" json:"policy,omitempty"`
	IdempotencyKey string       `cbor:"idem,omitempty" json:"idem,omitempty"`
}

// AgentTranscriptReq reads transcript records from index From (inclusive).
// Limit bounds records per page; zero selects the server default.
type AgentTranscriptReq struct {
	ID    string `cbor:"id" json:"id"`
	From  uint64 `cbor:"from,omitempty" json:"from,omitempty"`
	Limit int    `cbor:"limit,omitempty" json:"limit,omitempty"`
}

// AgentTranscriptRes is one page. Next is the index to ask for next; Gap is
// set when From was below the oldest retained record, and names the first
// index that is retained, so a reader learns about the loss rather than
// silently skipping it. Done is true once the agent is terminal and every
// record it will ever have is below Next.
type AgentTranscriptRes struct {
	Records []TranscriptRecord `cbor:"records" json:"records"`
	Next    uint64             `cbor:"next" json:"next"`
	Gap     *TranscriptGap     `cbor:"gap,omitempty" json:"gap,omitempty"`
	Done    bool               `cbor:"done,omitempty" json:"done,omitempty"`
}

// TranscriptGap says records [From, To) are gone.
type TranscriptGap struct {
	From uint64 `cbor:"from" json:"from"`
	To   uint64 `cbor:"to" json:"to"`
}

// TranscriptRecord is one transcript chunk as the control plane keeps it.
// Index is the agent-wide position (monotonic across runs); Seq is the
// position in the run's node session log; Stream is the session stream
// (StreamACPIn/StreamACPOut for ACP frames, StreamStderr for install and
// harness noise). Data is the redacted chunk: an ACPFrameRecord for the ACP
// streams.
type TranscriptRecord struct {
	Index  uint64 `cbor:"index" json:"index"`
	Run    string `cbor:"run" json:"run"`
	Seq    uint64 `cbor:"seq" json:"seq"`
	Stream uint8  `cbor:"stream" json:"stream"`
	At     int64  `cbor:"at" json:"at"`
	Data   []byte `cbor:"data" json:"data"`
}

// TranscriptChunk is what a node ships in an AgentReportTranscript report:
// one record of the run's transcript session, already redacted and bounded.
type TranscriptChunk struct {
	Seq    uint64 `cbor:"seq" json:"seq"`
	Stream uint8  `cbor:"stream" json:"stream"`
	At     int64  `cbor:"at" json:"at"`
	Data   []byte `cbor:"data" json:"data"`
}

// Transcript mirror bounds. A report carries at most MaxTranscriptReportBytes
// of chunk data; the control plane retains at most MaxTranscriptBytesPerAgent
// per agent and evicts oldest-first, reporting the loss as a gap.
const (
	MaxTranscriptReportBytes   = 256 << 10
	MaxTranscriptBytesPerAgent = 64 << 20
	MaxTranscriptPage          = 1000
)

// ApprovalListReq filters approvals.
type ApprovalListReq struct {
	Agent  string `cbor:"agent,omitempty" json:"agent,omitempty"`
	Kind   string `cbor:"kind,omitempty" json:"kind,omitempty"`
	Status string `cbor:"status,omitempty" json:"status,omitempty"`
}

// ApprovalListRes lists approvals.
type ApprovalListRes struct {
	Approvals []Approval `cbor:"approvals" json:"approvals"`
}

// ApprovalGetReq names one approval.
type ApprovalGetReq struct {
	ID string `cbor:"id" json:"id"`
}

// ApprovalDecideReq answers one approval.
type ApprovalDecideReq struct {
	ID             string          `cbor:"id" json:"id"`
	Option         string          `cbor:"option,omitempty" json:"option,omitempty"`
	Denied         bool            `cbor:"denied,omitempty" json:"denied,omitempty"`
	Content        json.RawMessage `cbor:"content,omitempty" json:"content,omitempty"`
	Remember       string          `cbor:"remember,omitempty" json:"remember,omitempty"`
	IdempotencyKey string          `cbor:"idem,omitempty" json:"idem,omitempty"`
}

// EgressApprovalReq asks the control plane to create or resolve a durable
// approval for one exact outbound request. Only the workspace's current node
// may issue it.
type EgressApprovalReq struct {
	WS          string `cbor:"ws" json:"ws"`
	Gen         uint64 `cbor:"gen" json:"gen"`
	Principal   string `cbor:"principal" json:"principal"`
	Rule        string `cbor:"rule" json:"rule"`
	Host        string `cbor:"host" json:"host"`
	Method      string `cbor:"method" json:"method"`
	PathHash    string `cbor:"path_hash" json:"path_hash"`
	BodyHash    string `cbor:"body_hash" json:"body_hash"`
	Fingerprint string `cbor:"fingerprint" json:"fingerprint"`
	WaitMillis  int64  `cbor:"wait_ms,omitempty" json:"wait_ms,omitempty"`
}

// EgressApprovalRes reports whether the request may cross the broker edge.
type EgressApprovalRes struct {
	ID        string `cbor:"id" json:"id"`
	Status    string `cbor:"status" json:"status"`
	Allowed   bool   `cbor:"allowed,omitempty" json:"allowed,omitempty"`
	ExpiresAt int64  `cbor:"expires_at,omitempty" json:"expires_at,omitempty"`
}

// ---------------------------------------------------------------------------
// ops: control <-> node
// ---------------------------------------------------------------------------

// AgentRunReq tells the node holding the workspace to launch the harness.
// The node answers as soon as the process is spawned; everything after
// arrives as AgentReport.
type AgentRunReq struct {
	Agent        string         `cbor:"agent" json:"agent"`
	Run          string         `cbor:"run" json:"run"`
	Attempt      int            `cbor:"attempt" json:"attempt"`
	WS           string         `cbor:"ws" json:"ws"`
	Gen          uint64         `cbor:"gen" json:"gen"`
	Tenant       string         `cbor:"tenant" json:"tenant"`
	Owner        string         `cbor:"owner" json:"owner"`
	Spec         AgentSpec      `cbor:"spec" json:"spec"`
	Policy       AgentPolicy    `cbor:"policy" json:"policy"`
	Mode         string         `cbor:"mode" json:"mode"`
	ACPSessionID string         `cbor:"acp_session_id,omitempty" json:"acp_session_id,omitempty"`
	Messages     []AgentMessage `cbor:"messages" json:"messages"`
}

// AgentRunRes acknowledges a launch.
type AgentRunRes struct {
	Transcript string `cbor:"transcript" json:"transcript"`
}

// AgentDeliverReq hands one inbox message to a run that is already going.
type AgentDeliverReq struct {
	Agent   string       `cbor:"agent" json:"agent"`
	Run     string       `cbor:"run" json:"run"`
	Message AgentMessage `cbor:"message" json:"message"`
}

// AgentRunCancelReq stops a run: the current turn is cancelled, queued
// messages are dropped and the harness is closed.
type AgentRunCancelReq struct {
	Agent  string `cbor:"agent" json:"agent"`
	Run    string `cbor:"run" json:"run"`
	Reason string `cbor:"reason,omitempty" json:"reason,omitempty"`
}

// AgentApprovalDecidedReq answers a permission request the node parked.
type AgentApprovalDecidedReq struct {
	Agent    string           `cbor:"agent" json:"agent"`
	Run      string           `cbor:"run" json:"run"`
	Approval string           `cbor:"approval" json:"approval"`
	Decision ApprovalDecision `cbor:"decision" json:"decision"`
}

// AgentReport is one thing the node observed about a run. Seq orders reports
// within a run and makes redelivery idempotent.
type AgentReport struct {
	Agent string `cbor:"agent" json:"agent"`
	Run   string `cbor:"run" json:"run"`
	WS    string `cbor:"ws" json:"ws"`
	Gen   uint64 `cbor:"gen" json:"gen"`
	Seq   uint64 `cbor:"seq" json:"seq"`
	Kind  string `cbor:"kind" json:"kind"`
	At    int64  `cbor:"at" json:"at"`

	Transcript   string           `cbor:"transcript,omitempty" json:"transcript,omitempty"`
	ACPSessionID string           `cbor:"acp_session_id,omitempty" json:"acp_session_id,omitempty"`
	Capabilities *ACPCapabilities `cbor:"capabilities,omitempty" json:"capabilities,omitempty"`
	Loaded       bool             `cbor:"loaded,omitempty" json:"loaded,omitempty"`
	Message      string           `cbor:"message,omitempty" json:"message,omitempty"` // inbox message id
	StopReason   string           `cbor:"stop_reason,omitempty" json:"stop_reason,omitempty"`
	Error        string           `cbor:"error,omitempty" json:"error,omitempty"`
	ExitCode     int              `cbor:"exit_code,omitempty" json:"exit_code,omitempty"`
	Cancelled    bool             `cbor:"cancelled,omitempty" json:"cancelled,omitempty"`
	// ToolCall fields (AgentReportToolCall).
	ToolCall   string   `cbor:"tool_call,omitempty" json:"tool_call,omitempty"`
	ToolKind   string   `cbor:"tool_kind,omitempty" json:"tool_kind,omitempty"`
	ToolTitle  string   `cbor:"tool_title,omitempty" json:"tool_title,omitempty"`
	ToolStatus string   `cbor:"tool_status,omitempty" json:"tool_status,omitempty"`
	Locations  []string `cbor:"locations,omitempty" json:"locations,omitempty"`
	// Approval fields (AgentReportPermission, AgentReportElicitation).
	Approval *Approval   `cbor:"approval,omitempty" json:"approval,omitempty"`
	Usage    *AgentUsage `cbor:"usage,omitempty" json:"usage,omitempty"`
	// Chunks (AgentReportTranscript) mirror the run's transcript session in
	// seq order so the control plane can serve it without the node.
	Chunks []TranscriptChunk `cbor:"chunks,omitempty" json:"chunks,omitempty"`
}

// AgentUsage is token accounting when the harness reports it.
type AgentUsage struct {
	Input  int64 `cbor:"input,omitempty" json:"input,omitempty"`
	Output int64 `cbor:"output,omitempty" json:"output,omitempty"`
}

// AgentReport.Kind values.
const (
	AgentReportStarted      = "started"       // process spawned; Transcript set
	AgentReportSession      = "session"       // ACPSessionID, Capabilities, Loaded known
	AgentReportTurnStarted  = "turn_started"  // Message is the inbox id being prompted
	AgentReportTurnFinished = "turn_finished" // StopReason
	AgentReportToolCall     = "tool_call"
	AgentReportPermission   = "permission"  // Approval carries the parked request
	AgentReportElicitation  = "elicitation" // Approval carries the parked question
	AgentReportFinished     = "finished"    // process gone; Error/ExitCode/Cancelled
	AgentReportTranscript   = "transcript"  // Chunks: a batch of transcript records
)

// ---------------------------------------------------------------------------
// events
// ---------------------------------------------------------------------------

const (
	EvAgentCreated     = "agent.created"        // payload {agent, ws, recipe, mode, task_hash, parent}
	EvAgentRunStarted  = "agent.run.started"    // payload {agent, run, attempt, node, transcript}
	EvAgentRunFinished = "agent.run.finished"   // payload {agent, run, stop_reason, error, cancelled, turns}
	EvAgentTurn        = "agent.turn"           // payload {agent, run, message, stop_reason, tokens}
	EvAgentWaiting     = "agent.waiting"        // payload {agent, kind: input|approval}
	EvAgentSlept       = "agent.slept"          // payload {agent, ws, timer}
	EvAgentWoken       = "agent.woken"          // payload {agent, ws, by: message|approval|timer|request|preview|diff}
	EvAgentForked      = "agent.forked"         // payload {agent, from, snapshot}
	EvAgentFailed      = "agent.failed"         // payload {agent, reason}
	EvAgentFinished    = "agent.finished"       // payload {agent, reason}
	EvAgentDestroyed   = "agent.destroyed"      // payload {agent, ws}
	EvAgentCancelled   = "agent.cancelled"      // payload {agent, run, dropped, by}
	EvAgentToolCall    = "agent.tool_call"      // payload {agent, run, tool_call, kind, title, status, locations}
	EvAgentSession     = "agent.session"        // payload {agent, run, acp_session_id, loaded, capabilities}
	EvAgentMessage     = "agent.message"        // payload {agent, message, kind, text_hash, degraded}
	EvAgentChildDone   = "agent.child.finished" // payload {agent, child, status, reason, turns} on the parent's stream
	EvApprovalPending  = "approval.pending"     // payload {approval, agent, kind, title}
	EvApprovalDecided  = "approval.decided"     // payload {approval, agent, option, denied, by}
	EvApprovalExpired  = "approval.expired"     // payload {approval, agent, run}
)
