package control

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"sort"
	"time"

	"remount.dev/remount/internal/artifact"
	"remount.dev/remount/internal/eventlog"
	"remount.dev/remount/internal/metrics"
	"remount.dev/remount/internal/proto"
)

const (
	defaultMaxSessionLogsPerTenant = 4096
	defaultSessionLogRetention     = 24 * time.Hour
	maxSessionLogSegments          = 65536
	maxSessionLogSegmentBytes      = 64 << 20
	maxSessionLogChunkBytes        = 1 << 20
)

func cloneSessionLogRecord(record *proto.SessionLogRecord) *proto.SessionLogRecord {
	if record == nil {
		return nil
	}
	cp := *record
	cp.Segments = append([]proto.SessionLogSegment(nil), record.Segments...)
	return &cp
}

func (c *Control) loadSessionLogs() error {
	rows, err := c.db.Query(`SELECT data FROM session_logs`)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		var body []byte
		if err := rows.Scan(&body); err != nil {
			return err
		}
		var record proto.SessionLogRecord
		if err := proto.Unmarshal(body, &record); err != nil {
			return fmt.Errorf("decode session log record: %w", err)
		}
		if err := validateSessionLogRecord(&record); err != nil {
			return fmt.Errorf("invalid session log record %s: %w", record.Session, err)
		}
		c.sessionLogs[record.Session] = &record
	}
	return rows.Err()
}

func validateSessionLogRecord(record *proto.SessionLogRecord) error {
	if record == nil || record.Session == "" || record.Workspace == "" || record.Tenant == "" || record.MaxChunk <= 0 || record.MaxChunk > maxSessionLogChunkBytes {
		return errors.New("required session log metadata is absent")
	}
	if len(record.Segments) > maxSessionLogSegments {
		return errors.New("segment reference limit exceeded")
	}
	var next uint64
	for i, segment := range record.Segments {
		if segment.First >= segment.Next || segment.Bytes <= 0 || segment.Bytes > maxSessionLogSegmentBytes {
			return errors.New("invalid segment range or size")
		}
		if _, err := artifact.Digest(segment.Artifact); err != nil {
			return err
		}
		if i > 0 && segment.First != next {
			return errors.New("non-contiguous segment references")
		}
		next = segment.Next
	}
	return nil
}

func sessionLogPrefix(existing, replacement []proto.SessionLogSegment) bool {
	if len(replacement) < len(existing) {
		return false
	}
	for i := range existing {
		if existing[i] != replacement[i] {
			return false
		}
	}
	return true
}

func sameSessionLogRecord(existing, replacement *proto.SessionLogRecord) bool {
	if existing == nil || replacement == nil {
		return false
	}
	a, b := *existing, *replacement
	a.UpdatedAt, a.ExpiresAt = 0, 0
	b.UpdatedAt, b.ExpiresAt = 0, 0
	return bytes.Equal(proto.MustMarshal(a), proto.MustMarshal(b))
}

func (c *Control) sessionLogCommit(ctx context.Context, node string, req *proto.SessionLogCommitReq) (*proto.SessionLogRecord, error) {
	if req.Session == "" || req.Workspace == "" || req.Generation == 0 || req.Principal == "" || req.Kind == "" {
		return nil, proto.Err(proto.CodeBadRequest, "session log commit omits identity metadata")
	}
	c.mu.Lock()
	workspace := c.workspaces[req.Workspace]
	var authority proto.Workspace
	if workspace != nil {
		authority = *workspace
	}
	existing := cloneSessionLogRecord(c.sessionLogs[req.Session])
	used := 0
	if existing == nil {
		for _, candidate := range c.sessionLogs {
			if candidate.Tenant == authority.Tenant {
				used++
			}
		}
	}
	c.mu.Unlock()
	if workspace == nil {
		return nil, proto.Err(proto.CodeNotFound, "workspace %s", req.Workspace)
	}
	if authority.Node != node || authority.Generation != req.Generation || !held(authority.State) {
		return nil, proto.Err(proto.CodeConflict, "stale session log producer for %s", req.Workspace)
	}
	record := &proto.SessionLogRecord{
		Session: req.Session, Workspace: req.Workspace, Tenant: authority.Tenant,
		Principal: req.Principal, Kind: req.Kind, Info: req.Info, Exit: req.Exit, MaxChunk: req.MaxChunk,
		Segments: append([]proto.SessionLogSegment(nil), req.Segments...), Complete: req.Complete,
		UpdatedAt: c.now().UnixMilli(),
	}
	if record.Complete {
		if record.Info.ID != record.Session || record.Info.WS != record.Workspace {
			return nil, proto.Err(proto.CodeBadRequest, "complete session log omits matching session info")
		}
		retention := c.opts.SessionLogRetention
		if c.opts.Tenants != nil {
			if tenantValue, err := c.opts.Tenants.Get(ctx, authority.Tenant); err == nil && tenantValue.Policy.Retention.SessionLogs > 0 {
				retention = tenantValue.Policy.Retention.SessionLogs
			}
		}
		record.ExpiresAt = c.now().Add(retention).UnixMilli()
	}
	if err := validateSessionLogRecord(record); err != nil {
		return nil, proto.Err(proto.CodeBadRequest, "invalid session log record: %v", err)
	}
	if existing != nil && existing.Complete && sameSessionLogRecord(existing, record) {
		return cloneSessionLogRecord(existing), nil
	}
	if existing == nil {
		if used >= c.opts.MaxSessionLogsPerTenant {
			return nil, proto.Err(proto.CodeResourceExhausted, "tenant session log record limit %d reached", c.opts.MaxSessionLogsPerTenant)
		}
	} else if existing.Workspace != record.Workspace || existing.Tenant != record.Tenant || existing.Principal != record.Principal || existing.Kind != record.Kind || existing.MaxChunk != record.MaxChunk || existing.Complete || !sessionLogPrefix(existing.Segments, record.Segments) {
		return nil, proto.Err(proto.CodeConflict, "session log replacement is not a monotonic continuation")
	}
	store, err := c.artifactStoreForTenant(authority.Tenant)
	if err != nil {
		return nil, proto.Err(proto.CodeUnsupported, "session log storage is unavailable")
	}
	// Verify outside c.mu; the generation is revalidated below before commit.
	verifiedFrom := 0
	if existing != nil {
		verifiedFrom = len(existing.Segments)
	}
	for _, segment := range record.Segments[verifiedFrom:] {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if err := store.Verify(segment.Artifact); err != nil {
			return nil, proto.Err(proto.CodeConflict, "session log segment is unavailable: %v", err)
		}
		r, size, err := store.Open(segment.Artifact)
		if err != nil {
			return nil, proto.Err(proto.CodeConflict, "session log segment is unavailable: %v", err)
		}
		closeErr := r.Close()
		if closeErr != nil || size != segment.Bytes {
			return nil, proto.Err(proto.CodeConflict, "session log segment size mismatch")
		}
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	workspace = c.workspaces[req.Workspace]
	if workspace == nil || workspace.Node != node || workspace.Generation != req.Generation || !held(workspace.State) || workspace.Tenant != authority.Tenant {
		return nil, proto.Err(proto.CodeConflict, "session log authority changed during verification")
	}
	current := c.sessionLogs[req.Session]
	if (existing == nil) != (current == nil) || (existing != nil && !sameSessionLogRecord(existing, current)) {
		return nil, proto.Err(proto.CodeConflict, "session log record changed during verification")
	}
	event := c.newEvent(proto.EvSessionLogCommitted, req.Session, req.Principal, node, map[string]any{
		"workspace": req.Workspace, "segments": len(record.Segments), "complete": record.Complete,
	})
	event.Tenant = authority.Tenant
	if err := c.transact(func(tx *eventlog.Tx) error {
		_, err := tx.Exec(`INSERT OR REPLACE INTO session_logs(id, tenant, expires_at, data) VALUES(?,?,?,?)`, record.Session, record.Tenant, record.ExpiresAt, proto.MustMarshal(record))
		return err
	}, []*proto.Event{event}); err != nil {
		return nil, err
	}
	c.sessionLogs[record.Session] = record
	return cloneSessionLogRecord(record), nil
}

func (c *Control) sessionLogGet(node string, req *proto.SessionLogGetReq) (*proto.SessionLogRecord, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	workspace := c.workspaces[req.Workspace]
	if workspace == nil || workspace.Node != node || workspace.Generation != req.Generation || !held(workspace.State) {
		return nil, proto.Err(proto.CodeDenied, "session log requires the current workspace holder")
	}
	record := c.sessionLogs[req.Session]
	if record == nil || record.Workspace != req.Workspace || record.Tenant != workspace.Tenant {
		return nil, proto.Err(proto.CodeNotFound, "session %s", req.Session)
	}
	return cloneSessionLogRecord(record), nil
}

func (c *Control) sessionLogDelete(node string, req *proto.SessionLogDeleteReq) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	workspace := c.workspaces[req.Workspace]
	record := c.sessionLogs[req.Session]
	if workspace == nil || workspace.Node != node || workspace.Generation != req.Generation || !held(workspace.State) || record == nil || record.Workspace != req.Workspace || record.Tenant != workspace.Tenant {
		return proto.Err(proto.CodeDenied, "session log delete requires the current workspace holder")
	}
	if !record.Complete {
		return proto.Err(proto.CodeConflict, "live session log cannot be deleted")
	}
	event := c.newEvent(proto.EvSessionLogDeleted, req.Session, record.Principal, node, map[string]any{
		"workspace": req.Workspace, "segments": len(record.Segments),
	})
	event.Tenant = record.Tenant
	if err := c.transact(func(tx *eventlog.Tx) error {
		_, err := tx.Exec(`DELETE FROM session_logs WHERE id=?`, req.Session)
		return err
	}, []*proto.Event{event}); err != nil {
		return err
	}
	delete(c.sessionLogs, req.Session)
	return nil
}

// pruneSessionLogsLocked deletes expired session-log records and emits one
// session.log.deleted event per removed row in the same transaction. It
// returns how many rows were actually removed.
//
// Authority is the control plane's own retention pass, not a node, so the
// event records `reason: retention` and carries no node. The irreversible
// action is releasing the record's segment artifacts from the GC root set;
// the durable commit is the DELETE plus its events. The postcondition is that
// every deleted row has exactly one durable deletion event, and that a record
// left behind by a failed transaction is still present in memory for the next
// pass.
//
// The caller holds c.mu and has already committed the surrounding prune
// transaction, so this opens a fresh one rather than nesting.
func (c *Control) pruneSessionLogsLocked(ids []string) int {
	if len(ids) == 0 {
		return 0
	}
	records := make([]*proto.SessionLogRecord, 0, len(ids))
	events := make([]*proto.Event, 0, len(ids))
	for _, id := range ids {
		record := c.sessionLogs[id]
		if record == nil {
			continue
		}
		event := c.newEvent(proto.EvSessionLogDeleted, record.Session, record.Principal, "", map[string]any{
			"workspace": record.Workspace, "segments": len(record.Segments), "reason": "retention",
		})
		event.Tenant = record.Tenant
		records = append(records, record)
		events = append(events, event)
	}
	if len(records) == 0 {
		return 0
	}
	if err := c.transact(func(tx *eventlog.Tx) error {
		for _, record := range records {
			if _, err := tx.Exec(`DELETE FROM session_logs WHERE id=?`, record.Session); err != nil {
				return err
			}
		}
		return nil
	}, events); err != nil {
		c.logger.Error("session log retention prune deferred", "err", err, "records", len(records))
		return 0
	}
	for _, record := range records {
		delete(c.sessionLogs, record.Session)
	}
	metrics.SessionLogsPruned.Add(uint64(len(records)))
	return len(records)
}

func (c *Control) sessionLogReferencesLocked(add func(string, string)) {
	ids := make([]string, 0, len(c.sessionLogs))
	for id := range c.sessionLogs {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	for _, id := range ids {
		record := c.sessionLogs[id]
		for _, segment := range record.Segments {
			add(record.Tenant, segment.Artifact)
		}
	}
}
