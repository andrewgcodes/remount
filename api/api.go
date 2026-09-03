// Package api contains the stable, versioned data model used by the public
// Remount Go client. It deliberately excludes relay frames, grants, and other
// wire-internal structures; callers work with resources and typed errors.
package api

import (
	"errors"

	"remount.dev/remount/internal/proto"
)

// Error is a machine-readable server failure. Code is stable across patch
// releases; Msg is intended for humans and may change.
type Error = proto.Error

const (
	CodeBadRequest        = proto.CodeBadRequest
	CodeNotFound          = proto.CodeNotFound
	CodeUnsupported       = proto.CodeUnsupported
	CodeUnauthorized      = proto.CodeUnauthorized
	CodeConflict          = proto.CodeConflict
	CodeEvicted           = proto.CodeEvicted
	CodeUnreachable       = proto.CodeUnreachable
	CodeInternal          = proto.CodeInternal
	CodeTimeout           = proto.CodeTimeout
	CodeClosed            = proto.CodeClosed
	CodeDenied            = proto.CodeDenied
	CodeResourceExhausted = proto.CodeResourceExhausted
)

// ErrorCode extracts a stable Remount error code, or an empty string when err
// is not a protocol error (for example context cancellation or a dial error).
func ErrorCode(err error) string {
	var protocolError *proto.Error
	if errors.As(err, &protocolError) {
		return protocolError.Code
	}
	return ""
}

// IsErrorCode reports whether err wraps a Remount error with code.
func IsErrorCode(err error, code string) bool {
	return errors.Is(err, &proto.Error{Code: code})
}

// DecodeEventPayload decodes an event's deterministic CBOR payload into out.
func DecodeEventPayload(event Event, out any) error {
	return proto.Unmarshal(event.Payload, out)
}

type (
	Requires          = proto.Requires
	Placement         = proto.Placement
	WorkspaceSpec     = proto.WorkspaceSpec
	SecuritySpec      = proto.SecuritySpec
	NetworkPolicy     = proto.NetworkPolicy
	EgressRule        = proto.EgressRule
	AuditPolicy       = proto.AuditPolicy
	WorkspaceACL      = proto.WorkspaceACL
	Idle              = proto.Idle
	Workspace         = proto.Workspace
	WorkspaceSelector = proto.WorkspaceSelector

	FleetOperation         = proto.FleetOperation
	FleetOperationResult   = proto.FleetOperationResult
	FleetQuarantineRequest = proto.FleetQuarantineReq

	NodeInfo             = proto.NodeInfo
	NodeStatus           = proto.NodeStatus
	BackendDescriptor    = proto.BackendDescriptor
	BackendSecurityCaps  = proto.BackendSecurityCaps
	RuntimeCaps          = proto.RuntimeCaps
	Event                = proto.Event
	Timer                = proto.Timer
	SleepRequest         = proto.WSSleepReq
	SessionStatus        = proto.SessionStatus
	SessionInfo          = proto.SessionInfo
	ExitInfo             = proto.ExitInfo
	Gap                  = proto.Gap
	FileEntry            = proto.FSEntry
	FileEdit             = proto.FSEdit
	FileMatch            = proto.FSMatch
	FileSearchResult     = proto.FSSearchRes
	SnapshotResult       = proto.WSSnapshotRes
	WorkspaceInfo        = proto.WSInfoRes
	ControlDiagnostics   = proto.ControlDiag
	NodeDiagnostics      = proto.NodeDiag
	WorkspaceDiagnostics = proto.WSDiag
	Finding              = proto.Finding
)

// SessionOpenRequest starts an exec or PTY session. It is intentionally a
// public resource request rather than an alias of the wire request, whose
// signed grant field is an implementation detail.
type SessionOpenRequest struct {
	WS             string
	Kind           string
	Program        []string
	Cwd            string
	Env            map[string]string
	Rows           uint16
	Cols           uint16
	Stdin          bool
	TimeoutSec     int64
	IdempotencyKey string
	NoSubscribe    bool
}

const (
	WorkspacePending       = proto.WSPending
	WorkspaceClaiming      = proto.WSClaiming
	WorkspaceClaimed       = proto.WSClaimed
	WorkspaceQuiescing     = proto.WSQuiescing
	WorkspaceCheckpointing = proto.WSCheckpointing
	WorkspacePaused        = proto.WSPaused
	WorkspaceReleased      = proto.WSReleased
	WorkspaceDestroying    = proto.WSDestroying
	WorkspaceDestroyed     = proto.WSDestroyed
	WorkspaceFailed        = proto.WSFailed

	SecurityLocal       = proto.SecurityLocal
	SecurityIsolated    = proto.SecurityIsolated
	SecurityMultiTenant = proto.SecurityMultiTenant

	NetworkDefaultDeny  = proto.NetworkDefaultDeny
	NetworkDefaultAllow = proto.NetworkDefaultAllow

	EgressProtocolHTTP     = proto.EgressProtocolHTTP
	EgressProtocolHTTPS    = proto.EgressProtocolHTTPS
	EgressProtocolConnect  = proto.EgressProtocolConnect
	EgressConnectorPackage = proto.EgressConnectorPackage

	SharedStateNone          = proto.SharedStateNone
	SharedStateImmutableRead = proto.SharedStateImmutableRead
	SharedStateScopedWrite   = proto.SharedStateScopedWrite
	SharedStateGlobalWrite   = proto.SharedStateGlobalWrite

	FleetActionFreeze       = proto.FleetActionFreeze
	FleetActionRevokeEgress = proto.FleetActionRevokeEgress
	FleetActionCheckpoint   = proto.FleetActionCheckpoint
	FleetActionStop         = proto.FleetActionStop
	FleetActionDestroy      = proto.FleetActionDestroy

	FleetStatePending   = proto.FleetStatePending
	FleetStateRunning   = proto.FleetStateRunning
	FleetStateCompleted = proto.FleetStateCompleted
	FleetStatePartial   = proto.FleetStatePartial

	FleetTargetPending      = proto.FleetTargetPending
	FleetTargetAcknowledged = proto.FleetTargetAcknowledged
	FleetTargetFailed       = proto.FleetTargetFailed

	SessionExec = proto.SessionExec
	SessionPTY  = proto.SessionPTY
	SessionPort = proto.SessionPort

	StreamStdout = proto.StreamStdout
	StreamStderr = proto.StreamStderr
	StreamExit   = proto.StreamExit
	StreamInfo   = proto.StreamInfo
	StreamGap    = proto.StreamGap

	SnapshotConsistencyLive     = proto.SnapshotConsistencyLive
	SnapshotConsistencyQuiesced = proto.SnapshotConsistencyQuiesced
)
