package node

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"

	"remount.dev/remount/internal/metrics"
	"remount.dev/remount/internal/proto"
	"remount.dev/remount/internal/workspace"
)

const (
	releasePreparing  = "preparing"
	releaseQuiesced   = "quiesced"
	releaseCheckpoint = "checkpointed"
	releasePrepared   = "prepared"
	releaseAborting   = "aborting"
	releaseRestored   = "abort-restored"
	releaseAuthorized = "abort-authorized"
	releasePublished  = "abort-published"
	releaseCommitted  = "committed"
)

// durableRelease is the typed node-side proof for destructive workspace
// release. Unlike a generic mutation intent, it contains everything needed to
// inspect and finish volume detachment after a process restart.
type durableRelease struct {
	Request     proto.WSReleaseReq  `cbor:"request"`
	Response    proto.WSReleasedReq `cbor:"response"`
	OperationID string              `cbor:"operation_id,omitempty"`
	State       string              `cbor:"state"`
	CompletedAt int64               `cbor:"completed_at,omitempty"`
}

func loadReleases(path string) (map[string]durableRelease, error) {
	out := map[string]durableRelease{}
	b, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return out, nil
	}
	if err != nil {
		return nil, err
	}
	if err := proto.Unmarshal(b, &out); err != nil {
		return nil, fmt.Errorf("node: corrupt release journal: %w", err)
	}
	for id, record := range out {
		if id == "" || record.Request.WS != id || record.Request.Gen == 0 {
			return nil, fmt.Errorf("node: corrupt release record for %q", id)
		}
		switch record.State {
		case releasePreparing, releaseQuiesced, releaseCheckpoint, releasePrepared,
			releaseAborting, releaseRestored, releaseAuthorized, releasePublished, releaseCommitted:
		default:
			return nil, fmt.Errorf("node: corrupt release state %q for %q", record.State, id)
		}
	}
	return out, nil
}

func (n *Node) persistReleasesLocked() error {
	b, err := proto.Marshal(n.releases)
	if err != nil {
		return err
	}
	tmp, err := os.CreateTemp(filepath.Dir(n.releasePath), ".releases-*")
	if err != nil {
		return err
	}
	name := tmp.Name()
	defer os.Remove(name)
	if err = tmp.Chmod(0o600); err == nil {
		var written int
		written, err = tmp.Write(b)
		if err == nil && written != len(b) {
			err = io.ErrShortWrite
		}
	}
	if err == nil {
		err = tmp.Sync()
	}
	if closeErr := tmp.Close(); err == nil {
		err = closeErr
	}
	if err == nil {
		err = os.Rename(name, n.releasePath)
	}
	if err == nil {
		err = syncParentDir(filepath.Dir(n.releasePath))
	}
	return err
}

func (n *Node) pruneCommittedReleasesLocked(now time.Time) {
	retention := n.opts.MutationRetention
	if retention <= 0 {
		retention = 30 * 24 * time.Hour
	}
	cutoff := now.Add(-retention).UnixMilli()
	for id, record := range n.releases {
		// abort-published is still the only proof that a restarted node may
		// reconstruct the runtime while control can remain durably WSClaiming.
		// Without a final control acknowledgement it is not a terminal tombstone.
		if record.State == releaseCommitted && record.CompletedAt > 0 && record.CompletedAt < cutoff {
			delete(n.releases, id)
		}
	}
}

func sameReleaseRequest(a, b proto.WSReleaseReq) bool {
	if a.WS != b.WS || a.Gen != b.Gen || a.OperationID != b.OperationID || a.Snapshot != b.Snapshot || a.Reason != b.Reason {
		return false
	}
	if b.Tenant != "" && a.Tenant != b.Tenant {
		return false
	}
	if b.Backend != "" && a.Backend != b.Backend {
		return false
	}
	emptySpec := proto.MustMarshal(proto.WorkspaceSpec{})
	return string(proto.MustMarshal(b.Spec)) == string(emptySpec) ||
		string(proto.MustMarshal(a.Spec)) == string(proto.MustMarshal(b.Spec))
}

func startsNewReleaseCycle(existing durableRelease, req proto.WSReleaseReq) bool {
	return (existing.State == releaseCommitted && req.Gen > existing.Request.Gen) ||
		(existing.State == releasePublished && req.OperationID != "" && req.OperationID != existing.Request.OperationID)
}

func (n *Node) beginRelease(req proto.WSReleaseReq) (durableRelease, bool, error) {
	n.releaseMu.Lock()
	defer n.releaseMu.Unlock()
	var replaced *durableRelease
	if existing, ok := n.releases[req.WS]; ok {
		if startsNewReleaseCycle(existing, req) {
			replaced = &existing
			delete(n.releases, req.WS)
		} else if !sameReleaseRequest(existing.Request, req) {
			return durableRelease{}, true, proto.Err(proto.CodeConflict,
				"release retry does not match durable operation")
		} else {
			return existing, true, nil
		}
	}
	n.pruneCommittedReleasesLocked(time.Now())
	limit := n.opts.MaxMutationRecords
	if limit <= 0 {
		limit = 10_000
	}
	if len(n.releases) >= limit {
		metrics.ReleaseJournalQuotaRejected.Inc()
		return durableRelease{}, false, proto.Err(proto.CodeResourceExhausted,
			"node release journal contains %d records", limit)
	}
	record := durableRelease{Request: req, OperationID: req.OperationID, State: releasePreparing}
	n.releases[req.WS] = record
	if err := n.persistReleasesLocked(); err != nil {
		delete(n.releases, req.WS)
		if replaced != nil {
			n.releases[req.WS] = *replaced
		}
		return durableRelease{}, false, proto.Err(proto.CodeInternal,
			"persist release intent before fencing: %v", err)
	}
	return record, false, nil
}

func releaseOperationScope(record durableRelease) string {
	if record.OperationID != "" {
		return record.OperationID
	}
	// Compatibility for release journals created before operation ids. The
	// scope remains stable across retries but new release cycles always receive
	// a fresh random id in beginRelease.
	return fmt.Sprintf("legacy-%x", proto.MustMarshal(record.Request))
}

func releaseMatchesCommit(record durableRelease, req *proto.WSReleaseCommitReq) bool {
	return req != nil && record.Request.WS == req.ID && record.Request.Gen == req.Gen &&
		(req.OperationID == "" || record.OperationID == req.OperationID)
}

func sameArtifactFormat(a, b string) bool {
	a, errA := proto.NormalizeArtifactFormat(a)
	b, errB := proto.NormalizeArtifactFormat(b)
	return errA == nil && errB == nil && a == b
}

func (n *Node) updateRelease(id, from, to string, response proto.WSReleasedReq) error {
	n.releaseMu.Lock()
	defer n.releaseMu.Unlock()
	record, ok := n.releases[id]
	if !ok || record.State != from {
		return proto.Err(proto.CodeConflict, "release %s changed from state %s", id, from)
	}
	previous := record
	record.State = to
	record.Response = response
	if to == releaseCommitted {
		record.CompletedAt = time.Now().UnixMilli()
	}
	n.releases[id] = record
	if err := n.persistReleasesLocked(); err != nil {
		n.releases[id] = previous
		return proto.Err(proto.CodeInternal, "persist release state %s: %v", to, err)
	}
	return nil
}

// beginReleaseAbort durably changes a retained release from destructive work
// to restoration work. Once this succeeds, no release retry may interpret the
// journal as authority to checkpoint, detach, or destroy the tree.
func (n *Node) beginReleaseAbort(id string, generation uint64, operationID string) (durableRelease, error) {
	n.releaseMu.Lock()
	defer n.releaseMu.Unlock()
	record, ok := n.releases[id]
	if !ok || record.Request.Gen != generation || (operationID != "" && record.OperationID != operationID) {
		return durableRelease{}, proto.Err(proto.CodeConflict, "workspace %s has no matching durable release", id)
	}
	switch record.State {
	case releaseAborting, releaseRestored, releaseAuthorized, releasePublished:
		return record, nil
	case releasePreparing, releaseQuiesced, releaseCheckpoint, releasePrepared:
		previous := record
		record.State = releaseAborting
		n.releases[id] = record
		if err := n.persistReleasesLocked(); err != nil {
			n.releases[id] = previous
			return durableRelease{}, proto.Err(proto.CodeInternal, "persist release abort intent: %v", err)
		}
		return record, nil
	default:
		return durableRelease{}, proto.Err(proto.CodeConflict, "workspace %s release cannot be aborted from %s", id, record.State)
	}
}

func (n *Node) advanceReleaseAbort(id string, generation uint64, operationID, from, to string) error {
	n.releaseMu.Lock()
	defer n.releaseMu.Unlock()
	record, ok := n.releases[id]
	if !ok || record.Request.Gen != generation || (operationID != "" && record.OperationID != operationID) {
		return proto.Err(proto.CodeConflict, "workspace %s has no matching abort intent", id)
	}
	if record.State == to {
		return nil
	}
	if record.State != from {
		return proto.Err(proto.CodeConflict, "workspace %s release abort changed from %s", id, from)
	}
	previous := record
	record.State = to
	if to == releasePublished {
		record.CompletedAt = time.Now().UnixMilli()
	}
	n.releases[id] = record
	if err := n.persistReleasesLocked(); err != nil {
		n.releases[id] = previous
		return proto.Err(proto.CodeInternal, "persist release abort state %s: %v", to, err)
	}
	return nil
}

func (n *Node) markReleaseRestored(id string, generation uint64) error {
	n.releaseMu.Lock()
	defer n.releaseMu.Unlock()
	record, ok := n.releases[id]
	if !ok || record.Request.Gen != generation {
		return proto.Err(proto.CodeConflict, "workspace %s has no matching abort intent", id)
	}
	if record.State == releaseRestored {
		return nil
	}
	if record.State != releaseAborting {
		return proto.Err(proto.CodeConflict, "workspace %s release is not aborting", id)
	}
	previous := record
	record.State = releaseRestored
	n.releases[id] = record
	if err := n.persistReleasesLocked(); err != nil {
		n.releases[id] = previous
		return proto.Err(proto.CodeInternal, "persist restored release source: %v", err)
	}
	return nil
}

func (n *Node) releaseRecord(id string) (durableRelease, bool) {
	n.releaseMu.Lock()
	defer n.releaseMu.Unlock()
	record, ok := n.releases[id]
	return record, ok
}

func (n *Node) reconcileReleaseLocked(ctx context.Context, record durableRelease) (proto.WSReleasedReq, error) {
	latest, ok := n.releaseRecord(record.Request.WS)
	if !ok || !sameReleaseRequest(latest.Request, record.Request) {
		return proto.WSReleasedReq{}, proto.Err(proto.CodeConflict, "durable release changed during reconciliation")
	}
	record = latest
	if record.State == releasePrepared || record.State == releaseCommitted {
		return record.Response, nil
	}
	if record.State == releasePreparing {
		// The process died before it durably proved session producers were
		// joined. A process backend cannot reconstruct that proof, so control
		// must abort and restore the retained source rather than risk archiving
		// a writer that survived the node.
		return proto.WSReleasedReq{}, proto.Err(proto.CodeConflict,
			"release interrupted before quiescence; abort is required")
	}
	if record.State == releaseAborting || record.State == releaseRestored {
		return proto.WSReleasedReq{}, proto.Err(proto.CodeConflict,
			"release abort restoration is in progress")
	}
	backend, err := n.opts.Backends.Get(record.Request.Backend)
	if err != nil {
		return proto.WSReleasedReq{}, err
	}
	handle, err := backend.Adopt(ctx, record.Request.WS)
	if err != nil {
		return proto.WSReleasedReq{}, fmt.Errorf("reconcile retained release source: %w", err)
	}
	w := &ws{Workspace: proto.Workspace{
		ID: record.Request.WS, Tenant: record.Request.Tenant, Generation: record.Request.Gen,
		State: proto.WSClaimed, Spec: record.Request.Spec,
	}, handle: handle}
	if record.State == releaseQuiesced && workspace.KindOf(handle) == workspace.CheckpointFSMem {
		_ = handle.FS().Close()
		return proto.WSReleasedReq{}, proto.Err(proto.CodeConflict,
			"memory checkpoint quiescence was interrupted; abort is required")
	}
	n.mu.Lock()
	n.quarantined[w.ID] = struct{}{}
	n.mu.Unlock()
	response := record.Response
	if record.State == releaseQuiesced {
		response = proto.WSReleasedReq{ID: w.ID, Gen: w.Generation, Reason: record.Request.Reason}
		if record.Request.Snapshot {
			result, snapshotErr := n.snapshotResultFenced(ctx, w, true)
			if snapshotErr != nil {
				_ = handle.FS().Close()
				return proto.WSReleasedReq{}, snapshotErr
			}
			response.Snapshot = result.Artifact
			response.SnapshotFormat = result.Format
		}
		if err := n.updateRelease(w.ID, releaseQuiesced, releaseCheckpoint, response); err != nil {
			_ = handle.FS().Close()
			return proto.WSReleasedReq{}, err
		}
	}
	if err := n.detachWorkspaceVolumesScoped(context.WithoutCancel(ctx), w, w.Spec.Volumes, releaseOperationScope(record)+":prepare"); err != nil {
		_ = handle.FS().Close()
		return proto.WSReleasedReq{}, fmt.Errorf("reconcile retained release volumes: %w", err)
	}
	if err := n.updateRelease(w.ID, releaseCheckpoint, releasePrepared, response); err != nil {
		_ = handle.FS().Close()
		return proto.WSReleasedReq{}, err
	}
	prepared := &preparedRelease{workspace: w, request: record.Request, response: response, done: make(chan struct{})}
	close(prepared.done)
	n.mu.Lock()
	n.prepared[w.ID] = prepared
	n.mu.Unlock()
	return response, nil
}
