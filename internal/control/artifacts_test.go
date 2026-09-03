package control

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"remount.dev/remount/internal/artifact"
	"remount.dev/remount/internal/proto"
)

const artifactProofTestID = "art_sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"

func TestArtifactProofBindsRequestAndIsSingleUse(t *testing.T) {
	now := time.Unix(1_800_000_000, 0)
	f := newControlFixture(t, "", func(opts *Options) { opts.Now = func() time.Time { return now } })
	putArtifactProofWorkspace(f.c, now, proto.WSClaimed)
	res, err := f.c.IssueArtifactProof(context.Background(), "n_one", proto.ArtifactProofReq{
		Workspace: "ws_one", Generation: 7, Method: http.MethodGet, Artifact: artifactProofTestID,
	})
	if err != nil {
		t.Fatal(err)
	}

	for name, args := range map[string][]any{
		"node":       {"n_two", "tenant-a", "ws_one", uint64(7), http.MethodGet, artifactProofTestID},
		"tenant":     {"n_one", "tenant-b", "ws_one", uint64(7), http.MethodGet, artifactProofTestID},
		"workspace":  {"n_one", "tenant-a", "ws_two", uint64(7), http.MethodGet, artifactProofTestID},
		"generation": {"n_one", "tenant-a", "ws_one", uint64(8), http.MethodGet, artifactProofTestID},
		"method":     {"n_one", "tenant-a", "ws_one", uint64(7), http.MethodHead, artifactProofTestID},
		"artifact":   {"n_one", "tenant-a", "ws_one", uint64(7), http.MethodGet, "art_sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"},
	} {
		t.Run(name, func(t *testing.T) {
			err := f.c.AuthorizeNodeArtifact(context.Background(), args[0].(string), args[1].(string), args[2].(string), args[3].(uint64), args[4].(string), args[5].(string), res.Proof)
			if !errors.Is(err, &proto.Error{Code: proto.CodeUnauthorized}) {
				t.Fatalf("AuthorizeNodeArtifact mismatch = %v, want unauthorized", err)
			}
		})
	}

	if err := f.c.AuthorizeNodeArtifact(context.Background(), "n_one", "tenant-a", "ws_one", 7, http.MethodGet, artifactProofTestID, res.Proof); err != nil {
		t.Fatalf("AuthorizeNodeArtifact valid request: %v", err)
	}
	if err := f.c.AuthorizeNodeArtifact(context.Background(), "n_one", "tenant-a", "ws_one", 7, http.MethodGet, artifactProofTestID, res.Proof); !errors.Is(err, &proto.Error{Code: proto.CodeDenied}) {
		t.Fatalf("AuthorizeNodeArtifact replay = %v, want denied", err)
	}
}

func TestArtifactProofExpiresAndRejectsTampering(t *testing.T) {
	now := time.Unix(1_800_000_000, 0)
	f := newControlFixture(t, "", func(opts *Options) { opts.Now = func() time.Time { return now } })
	putArtifactProofWorkspace(f.c, now, proto.WSClaiming)
	res, err := f.c.IssueArtifactProof(context.Background(), "n_one", proto.ArtifactProofReq{
		Workspace: "ws_one", Generation: 7, Method: http.MethodPut, Artifact: artifactProofTestID,
	})
	if err != nil {
		t.Fatal(err)
	}
	raw, err := base64.RawURLEncoding.DecodeString(res.Proof)
	if err != nil {
		t.Fatal(err)
	}
	var envelope proto.ArtifactProofEnvelope
	if err := proto.Unmarshal(raw, &envelope); err != nil {
		t.Fatal(err)
	}
	envelope.Claims.Tenant = "tenant-b"
	tampered := base64.RawURLEncoding.EncodeToString(proto.MustMarshal(envelope))
	if err := f.c.AuthorizeNodeArtifact(context.Background(), "n_one", "tenant-b", "ws_one", 7, http.MethodPut, artifactProofTestID, tampered); !errors.Is(err, &proto.Error{Code: proto.CodeUnauthorized}) {
		t.Fatalf("tampered proof = %v, want unauthorized", err)
	}

	now = now.Add(31 * time.Second)
	if err := f.c.AuthorizeNodeArtifact(context.Background(), "n_one", "tenant-a", "ws_one", 7, http.MethodPut, artifactProofTestID, res.Proof); !errors.Is(err, &proto.Error{Code: proto.CodeUnauthorized}) {
		t.Fatalf("expired proof = %v, want unauthorized", err)
	}
}

func TestArtifactProofRechecksLiveAssignment(t *testing.T) {
	now := time.Unix(1_800_000_000, 0)
	f := newControlFixture(t, "", func(opts *Options) { opts.Now = func() time.Time { return now } })
	putArtifactProofWorkspace(f.c, now, proto.WSQuiescing)
	res, err := f.c.IssueArtifactProof(context.Background(), "n_one", proto.ArtifactProofReq{
		Workspace: "ws_one", Generation: 7, Method: http.MethodPut, Artifact: artifactProofTestID,
	})
	if err != nil {
		t.Fatal(err)
	}
	f.c.mu.Lock()
	ws := f.c.workspaces["ws_one"]
	ws.State = proto.WSPending
	ws.Node = ""
	ws.Generation++
	f.c.mu.Unlock()
	if err := f.c.AuthorizeNodeArtifact(context.Background(), "n_one", "tenant-a", "ws_one", 7, http.MethodPut, artifactProofTestID, res.Proof); !errors.Is(err, &proto.Error{Code: proto.CodeDenied}) {
		t.Fatalf("released generation proof = %v, want denied", err)
	}
}

func TestArtifactProofIssuanceRequiresCurrentHolderAndTransferState(t *testing.T) {
	now := time.Unix(1_800_000_000, 0)
	f := newControlFixture(t, "", func(opts *Options) { opts.Now = func() time.Time { return now } })
	putArtifactProofWorkspace(f.c, now, proto.WSPending)
	req := proto.ArtifactProofReq{Workspace: "ws_one", Generation: 7, Method: http.MethodGet, Artifact: artifactProofTestID}
	if _, err := f.c.IssueArtifactProof(context.Background(), "n_one", req); !errors.Is(err, &proto.Error{Code: proto.CodeDenied}) {
		t.Fatalf("pending workspace issuance = %v, want denied", err)
	}
	f.c.mu.Lock()
	f.c.workspaces["ws_one"].State = proto.WSClaimed
	f.c.mu.Unlock()
	if _, err := f.c.IssueArtifactProof(context.Background(), "n_two", req); !errors.Is(err, &proto.Error{Code: proto.CodeDenied}) {
		t.Fatalf("non-holder issuance = %v, want denied", err)
	}
}

func putArtifactProofWorkspace(c *Control, now time.Time, state string) {
	c.mu.Lock()
	c.workspaces["ws_one"] = &proto.Workspace{
		ID: "ws_one", Tenant: "tenant-a", Node: "n_one", Generation: 7, State: state,
		LeaseUntil: now.Add(time.Minute).UnixMilli(),
	}
	c.mu.Unlock()
}

func TestGlobalFleetCheckpointVerifiesTargetTenant(t *testing.T) {
	resolver := &recordingTenantResolver{stores: make(map[string]*artifact.Store)}
	tenantStore, err := artifact.NewStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	snapshot, _, err := tenantStore.Put(strings.NewReader("tenant-b checkpoint"))
	if err != nil {
		t.Fatal(err)
	}
	resolver.stores["tenant-b"] = tenantStore
	f := newControlFixture(t, "", func(opts *Options) { opts.TenantArtifacts = resolver })
	now := time.Now()
	f.c.mu.Lock()
	f.c.workspaces["ws_b"] = &proto.Workspace{
		ID: "ws_b", Tenant: "tenant-b", Node: "n_one", Generation: 3, State: proto.WSClaimed,
		LeaseUntil: now.Add(time.Minute).UnixMilli(), Spec: proto.WorkspaceSpec{Requires: proto.Requires{Backend: "process"}},
	}
	f.c.fleetOps["fleet_global"] = &proto.FleetOperation{
		ID: "fleet_global", Tenant: "", Action: proto.FleetActionCheckpoint, State: proto.FleetStateRunning,
		Deadline: now.Add(time.Minute).UnixMilli(), Results: []proto.FleetOperationResult{{
			Workspace: "ws_b", Node: "n_one", Backend: "process", Generation: 3, State: proto.FleetTargetPending,
		}},
	}
	f.c.mu.Unlock()
	f.c.send = &fakeSender{online: map[string]bool{"n_one": true}, request: func(_ context.Context, _ string, op string, body, out any) error {
		if op != proto.OpWSQuarantine {
			return proto.Err(proto.CodeBadRequest, "unexpected operation")
		}
		req := body.(proto.WSQuarantineReq)
		if req.Tenant != "tenant-b" {
			t.Errorf("quarantine request tenant = %q", req.Tenant)
		}
		*(out.(*proto.WSQuarantineRes)) = proto.WSQuarantineRes{
			Fenced: true, Generation: 3, Action: proto.FleetActionCheckpoint, Backend: "process", Snapshot: snapshot,
		}
		return nil
	}}
	f.c.processFleetTarget(context.Background(), "fleet_global", 0)
	if got := resolver.lastTenant(); got != "tenant-b" {
		t.Fatalf("checkpoint verified tenant = %q, want target tenant", got)
	}
	f.c.mu.Lock()
	target := f.c.fleetOps["fleet_global"].Results[0]
	f.c.mu.Unlock()
	if target.State != proto.FleetTargetAcknowledged || strings.Contains(target.Error, "verification") {
		t.Fatalf("global fleet checkpoint target = %+v", target)
	}
}

func TestTenantArtifactReferencesRetainPendingDestroyedCleanupAndGlobalFleetTarget(t *testing.T) {
	f := newControlFixture(t, "", nil)
	idA := artifactProofTestID
	idB := "art_sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	idC := "art_sha256:cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc"
	f.c.mu.Lock()
	f.c.workspaces["ws_pending_cleanup"] = &proto.Workspace{
		ID: "ws_pending_cleanup", Tenant: "tenant-a", State: proto.WSDestroyed, Node: "n_one", LastSnapshot: idA,
	}
	f.c.workspaces["ws_global_target"] = &proto.Workspace{ID: "ws_global_target", Tenant: "tenant-b", State: proto.WSDestroyed, Node: "n_two"}
	f.c.workspaces["ws_clean"] = &proto.Workspace{ID: "ws_clean", Tenant: "tenant-c", State: proto.WSDestroyed, LastSnapshot: idC}
	f.c.fleetOps["fleet_global"] = &proto.FleetOperation{ID: "fleet_global", Results: []proto.FleetOperationResult{{
		Workspace: "ws_global_target", Snapshot: idB, State: proto.FleetTargetPending,
	}}}
	f.c.mu.Unlock()
	var references []artifact.TenantReference
	if err := f.c.WithTenantArtifactReferences(func(got []artifact.TenantReference) error {
		references = append(references, got...)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	want := map[artifact.TenantReference]bool{{Tenant: "tenant-a", ID: idA}: true, {Tenant: "tenant-b", ID: idB}: true}
	for _, reference := range references {
		delete(want, reference)
		if reference.Tenant == "tenant-c" && reference.ID == idC {
			t.Fatal("completed destroyed cleanup remained a GC root")
		}
	}
	if len(want) != 0 {
		t.Fatalf("missing tenant GC roots: %+v (got %+v)", want, references)
	}
}

type recordingTenantResolver struct {
	mu     sync.Mutex
	stores map[string]*artifact.Store
	seen   []string
}

func (r *recordingTenantResolver) ResolveTenant(tenant string) (artifact.TenantBlobStore, error) {
	r.mu.Lock()
	r.seen = append(r.seen, tenant)
	store := r.stores[tenant]
	r.mu.Unlock()
	if store == nil {
		return nil, fmt.Errorf("tenant store %q not found", tenant)
	}
	return tenantStoreAdapter{Store: store}, nil
}

func (*recordingTenantResolver) EncryptedAtRest() bool       { return true }
func (*recordingTenantResolver) Ready(context.Context) error { return nil }

func (r *recordingTenantResolver) lastTenant() string {
	r.mu.Lock()
	defer r.mu.Unlock()
	if len(r.seen) == 0 {
		return ""
	}
	return r.seen[len(r.seen)-1]
}

type tenantStoreAdapter struct{ *artifact.Store }

func (s tenantStoreAdapter) PutExpected(id string, body io.Reader, maxBytes int64) (int64, error) {
	return s.Store.PutExpected(id, body, maxBytes)
}
