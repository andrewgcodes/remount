package control

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"remount.dev/remount/internal/artifact"
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
