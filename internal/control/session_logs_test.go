package control

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"remount.dev/remount/internal/artifact"
	"remount.dev/remount/internal/metrics"
	"remount.dev/remount/internal/proto"
)

func putHeldSessionWorkspace(c *Control, id, tenant, node string, generation uint64) {
	c.mu.Lock()
	c.workspaces[id] = &proto.Workspace{
		ID: id, Tenant: tenant, Owner: "owner-" + tenant, Node: node,
		Generation: generation, State: proto.WSClaimed,
	}
	c.mu.Unlock()
}

func TestSessionLogAuthorityRetentionQuotaAndDeletionEvent(t *testing.T) {
	now := time.Unix(1_800_000_000, 0)
	storeA, err := artifact.NewStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	storeB, err := artifact.NewStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	id, size, err := storeA.Put(strings.NewReader("tenant-a-session-segment"))
	if err != nil {
		t.Fatal(err)
	}
	resolver := &recordingTenantResolver{stores: map[string]*artifact.Store{"tenant-a": storeA, "tenant-b": storeB}}
	f := newControlFixture(t, "", func(options *Options) {
		options.Now = func() time.Time { return now }
		options.TenantArtifacts = resolver
		options.MaxSessionLogsPerTenant = 1
		options.SessionLogRetention = 2 * time.Hour
	})
	putHeldSessionWorkspace(f.c, "ws-a", "tenant-a", "node-a", 7)
	putHeldSessionWorkspace(f.c, "ws-b", "tenant-b", "node-b", 3)

	segment := proto.SessionLogSegment{First: 0, Next: 2, Artifact: id, Bytes: size}
	base := proto.SessionLogCommitReq{
		Session: "ses-a", Workspace: "ws-a", Generation: 7,
		Principal: "alice", Kind: proto.SessionExec, MaxChunk: 32 << 10,
		Segments: []proto.SessionLogSegment{segment},
	}
	record, err := f.c.sessionLogCommit(context.Background(), "node-a", &base)
	if err != nil {
		t.Fatal(err)
	}
	if record.Tenant != "tenant-a" || record.Complete {
		t.Fatalf("committed record = %#v", record)
	}
	if _, err := f.c.sessionLogGet("node-b", &proto.SessionLogGetReq{Session: base.Session, Workspace: base.Workspace, Generation: 7}); !errors.Is(err, &proto.Error{Code: proto.CodeDenied}) {
		t.Fatalf("non-holder get = %v, want denied", err)
	}

	crossTenant := base
	crossTenant.Session, crossTenant.Workspace, crossTenant.Generation = "ses-b", "ws-b", 3
	if _, err := f.c.sessionLogCommit(context.Background(), "node-b", &crossTenant); !errors.Is(err, &proto.Error{Code: proto.CodeConflict}) {
		t.Fatalf("cross-tenant segment commit = %v, want conflict", err)
	}
	second := base
	second.Session = "ses-a-2"
	if _, err := f.c.sessionLogCommit(context.Background(), "node-a", &second); !errors.Is(err, &proto.Error{Code: proto.CodeResourceExhausted}) {
		t.Fatalf("tenant record quota = %v, want resource_exhausted", err)
	}

	complete := base
	complete.Complete = true
	complete.Info = proto.SessionInfo{ID: base.Session, WS: base.Workspace, Kind: base.Kind}
	complete.Exit = proto.ExitInfo{Code: 0}
	record, err = f.c.sessionLogCommit(context.Background(), "node-a", &complete)
	if err != nil {
		t.Fatal(err)
	}
	if record.ExpiresAt != now.Add(2*time.Hour).UnixMilli() {
		t.Fatalf("expires_at = %d, want %d", record.ExpiresAt, now.Add(2*time.Hour).UnixMilli())
	}
	if err := f.c.sessionLogDelete("node-b", &proto.SessionLogDeleteReq{Session: base.Session, Workspace: base.Workspace, Generation: 7}); !errors.Is(err, &proto.Error{Code: proto.CodeDenied}) {
		t.Fatalf("non-holder delete = %v, want denied", err)
	}
	if err := f.c.sessionLogDelete("node-a", &proto.SessionLogDeleteReq{Session: base.Session, Workspace: base.Workspace, Generation: 7}); err != nil {
		t.Fatal(err)
	}
	events, err := f.log.Read(context.Background(), 0, "", 100)
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, event := range events {
		if event.Type == proto.EvSessionLogDeleted && event.Stream == base.Session && event.Tenant == "tenant-a" {
			found = true
		}
	}
	if !found {
		t.Fatal("session log deletion committed without its tenant-scoped event")
	}
}

func TestSessionLogRejectsStaleProducerAndUnboundedChunk(t *testing.T) {
	store, err := artifact.NewStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	id, size, err := store.Put(strings.NewReader("segment"))
	if err != nil {
		t.Fatal(err)
	}
	resolver := &recordingTenantResolver{stores: map[string]*artifact.Store{"tenant-a": store}}
	f := newControlFixture(t, "", func(options *Options) { options.TenantArtifacts = resolver })
	putHeldSessionWorkspace(f.c, "ws-a", "tenant-a", "node-a", 9)
	req := proto.SessionLogCommitReq{
		Session: "ses-a", Workspace: "ws-a", Generation: 8, Principal: "alice",
		Kind: proto.SessionExec, MaxChunk: 32 << 10,
		Segments: []proto.SessionLogSegment{{First: 0, Next: 1, Artifact: id, Bytes: size}},
	}
	if _, err := f.c.sessionLogCommit(context.Background(), "node-a", &req); !errors.Is(err, &proto.Error{Code: proto.CodeConflict}) {
		t.Fatalf("stale generation commit = %v, want conflict", err)
	}
	req.Generation = 9
	req.MaxChunk = maxSessionLogChunkBytes + 1
	if _, err := f.c.sessionLogCommit(context.Background(), "node-a", &req); !errors.Is(err, &proto.Error{Code: proto.CodeBadRequest}) {
		t.Fatalf("unbounded chunk record = %v, want bad_request", err)
	}
}

func TestRetentionPruneEmitsDeletionEventPerRecord(t *testing.T) {
	now := time.Unix(1_800_000_000, 0)
	storeA, err := artifact.NewStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	storeB, err := artifact.NewStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	segmentA, sizeA, err := storeA.Put(strings.NewReader("tenant-a-retained-output"))
	if err != nil {
		t.Fatal(err)
	}
	segmentB, sizeB, err := storeB.Put(strings.NewReader("tenant-b-retained-output"))
	if err != nil {
		t.Fatal(err)
	}
	resolver := &recordingTenantResolver{stores: map[string]*artifact.Store{"tenant-a": storeA, "tenant-b": storeB}}
	f := newControlFixture(t, "", func(options *Options) {
		options.Now = func() time.Time { return now }
		options.TenantArtifacts = resolver
		options.SessionLogRetention = time.Hour
	})
	putHeldSessionWorkspace(f.c, "ws-a", "tenant-a", "node-a", 4)
	putHeldSessionWorkspace(f.c, "ws-b", "tenant-b", "node-b", 9)

	commit := func(session, workspace, node, tenant string, generation uint64, segment proto.SessionLogSegment) {
		t.Helper()
		req := proto.SessionLogCommitReq{
			Session: session, Workspace: workspace, Generation: generation,
			Principal: "alice-" + tenant, Kind: proto.SessionExec, MaxChunk: 32 << 10,
			Segments: []proto.SessionLogSegment{segment}, Complete: true,
			Info: proto.SessionInfo{ID: session, WS: workspace, Kind: proto.SessionExec},
			Exit: proto.ExitInfo{Code: 0},
		}
		if _, err := f.c.sessionLogCommit(context.Background(), node, &req); err != nil {
			t.Fatal(err)
		}
	}
	commit("ses-a1", "ws-a", "node-a", "tenant-a", 4, proto.SessionLogSegment{First: 0, Next: 2, Artifact: segmentA, Bytes: sizeA})
	commit("ses-a2", "ws-a", "node-a", "tenant-a", 4, proto.SessionLogSegment{First: 0, Next: 3, Artifact: segmentA, Bytes: sizeA})
	commit("ses-b1", "ws-b", "node-b", "tenant-b", 9, proto.SessionLogSegment{First: 0, Next: 2, Artifact: segmentB, Bytes: sizeB})

	// Nothing has expired yet, so retention must remove nothing.
	before, err := f.c.PruneRecords(context.Background(), now, 100)
	if err != nil {
		t.Fatal(err)
	}
	if before.SessionLogs != 0 {
		t.Fatalf("pruned %d unexpired session logs", before.SessionLogs)
	}

	// Move past tenant-a's and tenant-b's shared retention window.
	now = now.Add(2 * time.Hour)
	pruned := metrics.SessionLogsPruned.Value()
	result, err := f.c.PruneRecords(context.Background(), now, 100)
	if err != nil {
		t.Fatal(err)
	}
	if result.SessionLogs != 3 {
		t.Fatalf("pruned %d session logs, want 3", result.SessionLogs)
	}
	if delta := metrics.SessionLogsPruned.Value() - pruned; delta != 3 {
		t.Fatalf("session log prune metric advanced by %d, want 3", delta)
	}

	events, err := f.log.Read(context.Background(), 0, "", 1000)
	if err != nil {
		t.Fatal(err)
	}
	deleted := map[string]*proto.Event{}
	for _, event := range events {
		if event.Type != proto.EvSessionLogDeleted {
			continue
		}
		if prior := deleted[event.Stream]; prior != nil {
			t.Fatalf("session %s deleted twice", event.Stream)
		}
		copied := event
		deleted[event.Stream] = &copied
	}
	for session, tenant := range map[string]string{"ses-a1": "tenant-a", "ses-a2": "tenant-a", "ses-b1": "tenant-b"} {
		event := deleted[session]
		if event == nil {
			t.Fatalf("session %s was pruned without a deletion event", session)
		}
		if event.Tenant != tenant {
			t.Fatalf("deletion event for %s has tenant %q, want %q", session, event.Tenant, tenant)
		}
		var payload map[string]any
		if err := proto.Unmarshal(event.Payload, &payload); err != nil {
			t.Fatalf("decode payload for %s: %v", session, err)
		}
		if payload["reason"] != "retention" {
			t.Fatalf("deletion event for %s reason = %v, want retention", session, payload["reason"])
		}
	}

	// The rows and their in-memory records are gone, so the segments they
	// pinned are no longer GC roots.
	f.c.mu.Lock()
	remaining := len(f.c.sessionLogs)
	f.c.mu.Unlock()
	if remaining != 0 {
		t.Fatalf("%d session log records survived retention", remaining)
	}
	roots := map[string]bool{}
	f.c.mu.Lock()
	f.c.sessionLogReferencesLocked(func(tenant, id string) { roots[tenant+"\x00"+id] = true })
	f.c.mu.Unlock()
	if len(roots) != 0 {
		t.Fatalf("pruned session logs still pin %d artifact roots", len(roots))
	}
}
