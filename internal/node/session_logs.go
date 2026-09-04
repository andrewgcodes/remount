package node

import (
	"context"
	"fmt"
	"time"

	"remount.dev/remount/internal/artifact"
	"remount.dev/remount/internal/proto"
	"remount.dev/remount/internal/session"
)

func (n *Node) tieredSessionLogsEnabled() bool {
	n.mu.Lock()
	enabled := n.opts.ArtifactURL != "" && proto.HasCapability(n.protocol, proto.CapabilityTieredSessionLogs)
	n.mu.Unlock()
	return enabled
}

func (n *Node) sessionLogBlobStore(tenant, workspaceID string) (artifact.BlobStore, error) {
	if !n.tieredSessionLogsEnabled() {
		return nil, proto.Err(proto.CodeUnsupported, "tiered session log storage is unavailable")
	}
	n.mu.Lock()
	w := n.workspaces[workspaceID]
	var authority proto.Workspace
	if w != nil {
		authority = w.Workspace
	}
	n.mu.Unlock()
	if w == nil || authority.Tenant != tenant {
		return nil, proto.Err(proto.CodeDenied, "workspace is not held for session log storage")
	}
	// The assignment copy is immutable for the lifetime of the session. Every
	// individual HTTP operation still obtains a fresh proof and control checks
	// that this exact generation is live.
	return &workspaceBlobStore{n: n, ctx: context.Background(), w: &authority}, nil
}

func protoSessionLogSegments(record session.LogRecord) []proto.SessionLogSegment {
	out := make([]proto.SessionLogSegment, len(record.Segments))
	for i, segment := range record.Segments {
		out[i] = proto.SessionLogSegment{First: segment.First, Next: segment.Next, Artifact: segment.Artifact, Bytes: segment.Bytes}
	}
	return out
}

func (n *Node) callSessionLogControl(op string, request, response any) error {
	n.mu.Lock()
	peer := n.peer
	n.mu.Unlock()
	if peer == nil {
		return proto.Err(proto.CodeUnreachable, "control is offline")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	return peer.Call(ctx, proto.PeerControl, op, request, response)
}

func (n *Node) commitSessionLogRecord(id string, spec session.Spec, record session.LogRecord) error {
	if !n.tieredSessionLogsEnabled() {
		return proto.Err(proto.CodeUnsupported, "tiered session log storage is unavailable")
	}
	if len(record.Segments) == 0 {
		return n.callSessionLogControl(proto.OpSessionLogDelete, proto.SessionLogDeleteReq{Session: id, Workspace: spec.WS, Generation: spec.Generation}, nil)
	}
	request := proto.SessionLogCommitReq{
		Session: id, Workspace: spec.WS, Generation: spec.Generation,
		Principal: spec.Principal, Kind: spec.Kind, MaxChunk: record.MaxChunk,
		Segments: protoSessionLogSegments(record),
	}
	return n.callSessionLogControl(proto.OpSessionLogCommit, request, nil)
}

func (n *Node) completeSessionLogRecord(id string, spec session.Spec, info proto.SessionInfo, exit proto.ExitInfo, record session.LogRecord) error {
	if len(record.Segments) == 0 {
		return fmt.Errorf("session %s closed without a durable segment", id)
	}
	request := proto.SessionLogCommitReq{
		Session: id, Workspace: spec.WS, Generation: spec.Generation,
		Principal: spec.Principal, Kind: spec.Kind, Info: info, Exit: exit,
		MaxChunk: record.MaxChunk, Segments: protoSessionLogSegments(record), Complete: true,
	}
	return n.callSessionLogControl(proto.OpSessionLogCommit, request, nil)
}

func sessionLogAuthority(w *ws) proto.Workspace {
	return proto.Workspace{ID: w.ID, Generation: w.Generation, Tenant: w.Tenant}
}

func (n *Node) restoreSessionLog(ctx context.Context, w *ws, id string) (*session.Session, error) {
	authority := sessionLogAuthority(w)
	if !n.tieredSessionLogsEnabled() {
		return nil, proto.Err(proto.CodeNotFound, "session %s", id)
	}
	n.mu.Lock()
	peer := n.peer
	n.mu.Unlock()
	if peer == nil {
		return nil, proto.Err(proto.CodeUnreachable, "control is offline")
	}
	var record proto.SessionLogRecord
	if err := peer.Call(ctx, proto.PeerControl, proto.OpSessionLogGet, proto.SessionLogGetReq{
		Session: id, Workspace: authority.ID, Generation: authority.Generation,
	}, &record); err != nil {
		return nil, err
	}
	if record.Workspace != authority.ID || record.Tenant != authority.Tenant || record.Session != id {
		return nil, proto.Err(proto.CodeConflict, "control returned a mismatched session log record")
	}
	store := &workspaceBlobStore{n: n, ctx: context.Background(), w: &authority}
	return n.sessions.RestoreArchived(record, store)
}
