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

// sessionLogAdmission is one reserved tenant slot for a new session whose
// first commit is in flight. refs counts concurrent commits for the same
// session sharing the slot; the slot is released when the last one ends.
type sessionLogAdmission struct {
	tenant string
	refs   int
}

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

// sessionLogPublication is the in-flight state one sessionLogCommit holds
// between admission and its durable commit: the tenant slot it reserved for a
// new session, and the segment artifacts it pinned as GC roots while they are
// verified outside c.mu. Both are process-local; a restart has no in-flight
// commits, and committed rows are reconstructed by loadSessionLogs.
type sessionLogPublication struct {
	tenant    string
	session   string
	admitted  bool
	artifacts []string
}

// sessionLogAdmitLocked reserves what a commit needs before verification runs
// outside the lock. For a new session it takes one tenant slot, counting both
// committed rows and other reserved slots, so two concurrent new sessions can
// never both pass the check. It pins every not-yet-referenced segment so a
// GC pass between verification and commit sees the artifact as a root. It
// returns used and false when the tenant limit is reached.
func (c *Control) sessionLogAdmitLocked(tenant, session string, isNew bool, artifacts []string) (*sessionLogPublication, int, bool) {
	publication := &sessionLogPublication{tenant: tenant, session: session}
	if isNew {
		admission := c.sessionLogAdmissions[session]
		if admission == nil {
			used := 0
			for _, record := range c.sessionLogs {
				if record.Tenant == tenant {
					used++
				}
			}
			for otherSession, other := range c.sessionLogAdmissions {
				if other.tenant == tenant && c.sessionLogs[otherSession] == nil {
					used++
				}
			}
			if used >= c.opts.MaxSessionLogsPerTenant {
				return nil, used, false
			}
			admission = &sessionLogAdmission{tenant: tenant}
			c.sessionLogAdmissions[session] = admission
		}
		admission.refs++
		publication.admitted = true
	}
	pins := c.sessionLogPins[tenant]
	if pins == nil && len(artifacts) > 0 {
		pins = map[string]int{}
		c.sessionLogPins[tenant] = pins
	}
	for _, id := range artifacts {
		pins[id]++
	}
	publication.artifacts = append([]string(nil), artifacts...)
	return publication, 0, true
}

// sessionLogReleaseLocked ends a publication on every terminal path. On
// success the caller has already installed the record, so its segments stay
// roots through c.sessionLogs and its slot is now a committed row; on failure
// the slot and pins simply return. Both happen in the same critical section
// as the outcome, so there is no interval where a segment is unrooted or a
// slot is double-counted.
func (c *Control) sessionLogReleaseLocked(publication *sessionLogPublication) {
	if publication == nil {
		return
	}
	if publication.admitted {
		if admission := c.sessionLogAdmissions[publication.session]; admission != nil {
			admission.refs--
			if admission.refs <= 0 {
				delete(c.sessionLogAdmissions, publication.session)
			}
		}
	}
	pins := c.sessionLogPins[publication.tenant]
	for _, id := range publication.artifacts {
		if pins[id] <= 1 {
			delete(pins, id)
		} else {
			pins[id]--
		}
	}
	if len(pins) == 0 {
		delete(c.sessionLogPins, publication.tenant)
	}
	publication.admitted, publication.artifacts = false, nil
}

func (c *Control) sessionLogRelease(publication *sessionLogPublication) {
	c.mu.Lock()
	c.sessionLogReleaseLocked(publication)
	c.mu.Unlock()
}

// sessionLogCommit publishes a session's segment references on behalf of the
// workspace's current holder.
//
// Authority: the control plane, acting for the node that holds req.Workspace
// at req.Generation. Resource: the verified segment blobs and one tenant
// retained-record slot. Irreversible action: GC unlinking a blob the record
// will name, or accepting a row past MaxSessionLogsPerTenant. Fence: the
// slot is reserved and the new segments pinned as GC roots under c.mu before
// verification leaves the lock; the session_logs row and its event commit in
// one transaction while the reservation is still held, and the reservation
// is released in that same critical section. Postcondition: a committed
// record's bytes are present, the tenant never holds more than its limit,
// and any failure leaves no row, no event, no pin and no reserved slot.
func (c *Control) sessionLogCommit(ctx context.Context, node string, req *proto.SessionLogCommitReq) (*proto.SessionLogRecord, error) {
	if req.Session == "" || req.Workspace == "" || req.Generation == 0 || req.Principal == "" || req.Kind == "" {
		return nil, proto.Err(proto.CodeBadRequest, "session log commit omits identity metadata")
	}
	c.mu.Lock()
	workspace := c.workspaces[req.Workspace]
	if workspace == nil {
		c.mu.Unlock()
		return nil, proto.Err(proto.CodeNotFound, "workspace %s", req.Workspace)
	}
	authority := *workspace
	if authority.Node != node || authority.Generation != req.Generation || !held(authority.State) {
		c.mu.Unlock()
		return nil, proto.Err(proto.CodeConflict, "stale session log producer for %s", req.Workspace)
	}
	existing := cloneSessionLogRecord(c.sessionLogs[req.Session])
	record := &proto.SessionLogRecord{
		Session: req.Session, Workspace: req.Workspace, Tenant: authority.Tenant,
		Principal: req.Principal, Kind: req.Kind, Info: req.Info, Exit: req.Exit, MaxChunk: req.MaxChunk,
		Segments: append([]proto.SessionLogSegment(nil), req.Segments...), Complete: req.Complete,
		UpdatedAt: c.now().UnixMilli(),
	}
	if record.Complete && (record.Info.ID != record.Session || record.Info.WS != record.Workspace) {
		c.mu.Unlock()
		return nil, proto.Err(proto.CodeBadRequest, "complete session log omits matching session info")
	}
	if err := validateSessionLogRecord(record); err != nil {
		c.mu.Unlock()
		return nil, proto.Err(proto.CodeBadRequest, "invalid session log record: %v", err)
	}
	if existing != nil && existing.Complete && sameSessionLogRecord(existing, record) {
		c.mu.Unlock()
		return cloneSessionLogRecord(existing), nil
	}
	if existing != nil && (existing.Workspace != record.Workspace || existing.Tenant != record.Tenant || existing.Principal != record.Principal || existing.Kind != record.Kind || existing.MaxChunk != record.MaxChunk || existing.Complete || !sessionLogPrefix(existing.Segments, record.Segments)) {
		c.mu.Unlock()
		return nil, proto.Err(proto.CodeConflict, "session log replacement is not a monotonic continuation")
	}
	verifiedFrom := 0
	if existing != nil {
		verifiedFrom = len(existing.Segments)
	}
	pending := record.Segments[verifiedFrom:]
	artifacts := make([]string, 0, len(pending))
	for _, segment := range pending {
		artifacts = append(artifacts, segment.Artifact)
	}
	publication, used, admitted := c.sessionLogAdmitLocked(authority.Tenant, req.Session, existing == nil, artifacts)
	if !admitted {
		limit := c.opts.MaxSessionLogsPerTenant
		event := c.newEvent(proto.EvQuotaExceeded, req.Session, req.Principal, node, map[string]any{
			"resource": "session_logs", "scope": "tenant", "workspace": req.Workspace, "used": used, "limit": limit,
		})
		event.Tenant = authority.Tenant
		c.mu.Unlock()
		metrics.SessionLogQuotaRejected.Inc()
		c.appendAudit(ctx, event)
		return nil, proto.Err(proto.CodeResourceExhausted, "tenant session log record limit %d reached", limit)
	}
	c.mu.Unlock()

	if record.Complete {
		retention := c.opts.SessionLogRetention
		if c.opts.Tenants != nil {
			if tenantValue, err := c.opts.Tenants.Get(ctx, authority.Tenant); err == nil && tenantValue.Policy.Retention.SessionLogs > 0 {
				retention = tenantValue.Policy.Retention.SessionLogs
			}
		}
		record.ExpiresAt = c.now().Add(retention).UnixMilli()
	}
	store, err := c.artifactStoreForTenant(authority.Tenant)
	if err != nil {
		c.sessionLogRelease(publication)
		return nil, proto.Err(proto.CodeUnsupported, "session log storage is unavailable")
	}
	// Verify outside c.mu; the pins taken above keep these segments as GC
	// roots meanwhile, and the generation is revalidated below before commit.
	for _, segment := range pending {
		if err := ctx.Err(); err != nil {
			c.sessionLogRelease(publication)
			return nil, err
		}
		if err := store.Verify(segment.Artifact); err != nil {
			c.sessionLogRelease(publication)
			return nil, proto.Err(proto.CodeConflict, "session log segment is unavailable: %v", err)
		}
		r, size, err := store.Open(segment.Artifact)
		if err != nil {
			c.sessionLogRelease(publication)
			return nil, proto.Err(proto.CodeConflict, "session log segment is unavailable: %v", err)
		}
		closeErr := r.Close()
		if closeErr != nil || size != segment.Bytes {
			c.sessionLogRelease(publication)
			return nil, proto.Err(proto.CodeConflict, "session log segment size mismatch")
		}
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	defer c.sessionLogReleaseLocked(publication)
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

// sessionLogReferencesLocked reports every segment a committed record names
// plus every segment an in-flight publication has verified or is verifying,
// so reference-aware GC never unlinks bytes a commit is about to reference.
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
	tenants := make([]string, 0, len(c.sessionLogPins))
	for tenant := range c.sessionLogPins {
		tenants = append(tenants, tenant)
	}
	sort.Strings(tenants)
	for _, tenant := range tenants {
		pinned := make([]string, 0, len(c.sessionLogPins[tenant]))
		for id := range c.sessionLogPins[tenant] {
			pinned = append(pinned, id)
		}
		sort.Strings(pinned)
		for _, id := range pinned {
			add(tenant, id)
		}
	}
}
