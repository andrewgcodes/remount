package control

import (
	"context"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"remount.dev/remount/internal/artifact"
	"remount.dev/remount/internal/proto"
)

type blockingVolumeAuthorizer struct {
	mu      sync.Mutex
	match   func(Subject, string, Resource) bool
	entered chan struct{}
	release chan struct{}
	blocked bool
}

func (a *blockingVolumeAuthorizer) Check(ctx context.Context, subject Subject, action string, resource Resource) error {
	a.mu.Lock()
	if !a.blocked && a.match != nil && a.match(subject, action, resource) {
		a.blocked = true
		entered, release := a.entered, a.release
		a.mu.Unlock()
		close(entered)
		select {
		case <-release:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	a.mu.Unlock()
	return nil
}

func (a *blockingVolumeAuthorizer) arm(match func(Subject, string, Resource) bool) (<-chan struct{}, chan<- struct{}) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.match = match
	a.entered = make(chan struct{})
	a.release = make(chan struct{})
	a.blocked = false
	return a.entered, a.release
}

func volumeTestCode(err error) string {
	var protocolError *proto.Error
	if errors.As(err, &protocolError) {
		return protocolError.Code
	}
	return ""
}

func TestVolumeIdentityPathAndBackendAdmission(t *testing.T) {
	for _, tc := range []struct {
		id    string
		valid bool
	}{
		{id: "dataset-1.prod_cache", valid: true},
		{id: "", valid: false},
		{id: ".hidden", valid: false},
		{id: "has/slash", valid: false},
		{id: "has\\slash", valid: false},
		{id: "café", valid: false},
		{id: strings.Repeat("a", 65), valid: false},
	} {
		err := proto.ValidateVolumeID(tc.id)
		if (err == nil) != tc.valid {
			t.Errorf("ValidateVolumeID(%q) = %v, valid=%v", tc.id, err, tc.valid)
		}
	}

	store, err := artifact.NewStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	art := volumeArtifact(t, store, "dataset")
	f := newControlFixture(t, "", func(opts *Options) { opts.Artifacts = store })
	owner := Subject{ID: "alice", Tenant: "tenant-a"}
	if _, err := f.c.volumeCreate(context.Background(), owner, &proto.VolumeCreateReq{ID: "dataset", Artifact: art, IdempotencyKey: "create-dataset"}); err != nil {
		t.Fatal(err)
	}

	for _, volumes := range [][]proto.VolumeMount{
		{{ID: "dataset", Path: "/data"}, {ID: "dataset", Path: "/data/nested"}},
		{{ID: "dataset", Path: "/data/nested"}, {ID: "dataset", Path: "/data"}},
		{{ID: "dataset", Path: "/data"}, {ID: "dataset", Path: "/data"}},
	} {
		if _, err := f.c.wsCreate(context.Background(), owner, &proto.WSCreateReq{Spec: proto.WorkspaceSpec{Volumes: volumes}}); volumeTestCode(err) != proto.CodeBadRequest {
			t.Fatalf("overlapping volume paths %+v = %v, want bad_request", volumes, err)
		}
	}
	if _, err := f.c.wsCreate(context.Background(), owner, &proto.WSCreateReq{Spec: proto.WorkspaceSpec{
		Requires: proto.Requires{Backend: "docker"}, Volumes: []proto.VolumeMount{{ID: "dataset", Path: "/data"}},
	}}); volumeTestCode(err) != proto.CodeUnsupported {
		t.Fatalf("docker workspace with a volume = %v, want unsupported", err)
	}

	ws, err := f.c.wsCreate(context.Background(), owner, &proto.WSCreateReq{})
	if err != nil {
		t.Fatal(err)
	}
	attached, err := f.c.volumeAttach(context.Background(), owner, &proto.VolumeAttachReq{
		ID: "dataset", Workspace: ws.ID, Path: "/data", IdempotencyKey: "attach",
	})
	if err != nil {
		t.Fatal(err)
	}
	if attached.Spec.Requires.Backend != "process" || !contains(attached.Spec.Requires.Caps, proto.CapabilityReadOnlyVolumes) {
		t.Fatalf("dynamic attach requirements = %+v", attached.Spec.Requires)
	}
	f.c.mu.Lock()
	f.c.workspaces[ws.ID].State = proto.WSFailed
	f.c.mu.Unlock()
	if _, err := f.c.volumeDetach(context.Background(), owner, &proto.VolumeDetachReq{
		Workspace: ws.ID, Path: "/data", IdempotencyKey: "detach-failed",
	}); volumeTestCode(err) != proto.CodeConflict {
		t.Fatalf("detach from failed workspace = %v, want conflict", err)
	}
}

func TestVolumeAttachmentRevalidatesACLAfterAuthorizerWait(t *testing.T) {
	store, err := artifact.NewStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	authorizer := &blockingVolumeAuthorizer{}
	f := newControlFixture(t, "", func(opts *Options) {
		opts.Artifacts = store
		opts.Authorizer = authorizer
	})
	ctx := context.Background()
	owner := Subject{ID: "alice", Tenant: "tenant-a"}
	writer := Subject{ID: "bob", Tenant: "tenant-a"}
	art := volumeArtifact(t, store, "dataset")
	if _, err := f.c.volumeCreate(ctx, owner, &proto.VolumeCreateReq{ID: "dataset", Artifact: art, IdempotencyKey: "create-dataset"}); err != nil {
		t.Fatal(err)
	}
	ws, err := f.c.wsCreate(ctx, owner, &proto.WSCreateReq{Spec: proto.WorkspaceSpec{
		ACL: proto.WorkspaceACL{Writers: []string{writer.ID}},
	}})
	if err != nil {
		t.Fatal(err)
	}
	entered, release := authorizer.arm(func(subject Subject, action string, resource Resource) bool {
		return subject.ID == writer.ID && action == ActionWrite && resource.Kind == "workspace" && resource.ID == ws.ID
	})
	result := make(chan error, 1)
	go func() {
		_, err := f.c.volumeAttach(ctx, writer, &proto.VolumeAttachReq{
			ID: "dataset", Workspace: ws.ID, Path: "/data", IdempotencyKey: "stale-attach",
		})
		result <- err
	}()
	<-entered
	if _, err := f.c.wsACL(ctx, owner.ID, &proto.WSACLReq{
		ID: ws.ID, ACL: proto.WorkspaceACL{}, IdempotencyKey: "revoke-bob",
	}); err != nil {
		t.Fatal(err)
	}
	close(release)
	if err := <-result; volumeTestCode(err) != proto.CodeConflict {
		t.Fatalf("attach after ACL revision = %v, want conflict", err)
	}
	if got := f.c.snapshotWS(ws.ID); len(got.Spec.Volumes) != 0 {
		t.Fatalf("stale attach committed after revocation: %+v", got.Spec.Volumes)
	}
}

func TestVolumeMutationsRejectRemoveRecreateABA(t *testing.T) {
	for _, operation := range []string{"publish", "attach", "remove"} {
		t.Run(operation, func(t *testing.T) {
			store, err := artifact.NewStore(t.TempDir())
			if err != nil {
				t.Fatal(err)
			}
			authorizer := &blockingVolumeAuthorizer{}
			f := newControlFixture(t, "", func(opts *Options) {
				opts.Artifacts = store
				opts.Authorizer = authorizer
			})
			ctx := context.Background()
			alice := Subject{ID: "alice", Tenant: "tenant-a"}
			bob := Subject{ID: "bob", Tenant: "tenant-a"}
			first := volumeArtifact(t, store, "first")
			replacement := volumeArtifact(t, store, "replacement")
			published := volumeArtifact(t, store, "published")
			if _, err := f.c.volumeCreate(ctx, alice, &proto.VolumeCreateReq{ID: "dataset", Artifact: first, IdempotencyKey: "create-first"}); err != nil {
				t.Fatal(err)
			}
			var ws *proto.Workspace
			if operation != "remove" {
				ws, err = f.c.wsCreate(ctx, alice, &proto.WSCreateReq{IdempotencyKey: "workspace"})
				if err != nil {
					t.Fatal(err)
				}
			}
			entered, release := authorizer.arm(func(subject Subject, action string, resource Resource) bool {
				if subject.ID != alice.ID {
					return false
				}
				switch operation {
				case "attach":
					return action == ActionRead && resource.Kind == "volume" && resource.ID == "dataset"
				default:
					return action == ActionWrite && resource.Kind == "volume" && resource.ID == "dataset"
				}
			})
			result := make(chan error, 1)
			go func() {
				switch operation {
				case "publish":
					f.c.mu.Lock()
					f.c.workspaces[ws.ID].Generation = 7
					f.c.workspaces[ws.ID].State = proto.WSClaimed
					if err := f.c.persistWS(f.c.workspaces[ws.ID]); err != nil {
						f.c.mu.Unlock()
						result <- err
						return
					}
					f.c.mu.Unlock()
					_, err := f.c.volumePublish(ctx, alice, &proto.VolumePublishReq{
						ID: "dataset", Workspace: ws.ID, Generation: 7, ExpectedVersion: 1,
						Artifact: published, IdempotencyKey: "publish-stale",
					}, "")
					result <- err
				case "attach":
					_, err := f.c.volumeAttach(ctx, alice, &proto.VolumeAttachReq{
						ID: "dataset", Workspace: ws.ID, Path: "/data", IdempotencyKey: "attach-stale",
					})
					result <- err
				case "remove":
					result <- f.c.volumeRemove(ctx, alice, &proto.VolumeRemoveReq{ID: "dataset", IdempotencyKey: "remove-stale"})
				}
			}()
			<-entered
			if err := f.c.volumeRemove(ctx, bob, &proto.VolumeRemoveReq{ID: "dataset", IdempotencyKey: "remove-original"}); err != nil {
				t.Fatal(err)
			}
			if _, err := f.c.volumeCreate(ctx, bob, &proto.VolumeCreateReq{ID: "dataset", Artifact: replacement, IdempotencyKey: "create-replacement"}); err != nil {
				t.Fatal(err)
			}
			close(release)
			wantCode := map[string]string{
				"publish": proto.CodeConflict,
				"attach":  proto.CodeNotFound,
				"remove":  proto.CodeConflict,
			}[operation]
			if err := <-result; volumeTestCode(err) != wantCode {
				t.Fatalf("stale %s after volume ABA = %v, want %s", operation, err, wantCode)
			}
			got, err := f.c.volumeGet(ctx, bob, "dataset")
			if err != nil {
				t.Fatal(err)
			}
			if got.Owner != bob.ID || got.Artifact != replacement || got.Version != 1 {
				t.Fatalf("stale %s changed replacement volume: %+v", operation, got)
			}
			if ws != nil && len(f.c.snapshotWS(ws.ID).Spec.Volumes) != 0 {
				t.Fatalf("stale %s changed workspace mounts: %+v", operation, f.c.snapshotWS(ws.ID).Spec.Volumes)
			}
		})
	}
}

func TestVolumePublishCommitRequiresHoldingNodeAndCurrentForwardedGrant(t *testing.T) {
	store, err := artifact.NewStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	f := newControlFixture(t, "", func(opts *Options) { opts.Artifacts = store })
	ctx := context.Background()
	owner := Subject{ID: "alice", Tenant: "tenant-a"}
	f.c.mu.Lock()
	f.c.subjects["c_owner"] = owner
	f.c.mu.Unlock()
	connectNode(t, f.c, "n_one", processNodeInfo(4096))
	first := volumeArtifact(t, store, "first")
	second := volumeArtifact(t, store, "second")
	if _, err := f.c.volumeCreate(ctx, owner, &proto.VolumeCreateReq{ID: "dataset", Artifact: first, IdempotencyKey: "create-dataset"}); err != nil {
		t.Fatal(err)
	}
	ws, err := f.c.wsCreate(ctx, owner, &proto.WSCreateReq{})
	if err != nil {
		t.Fatal(err)
	}
	claim, err := f.c.wsClaim(ctx, "n_one", ws.ID)
	if err != nil {
		t.Fatal(err)
	}
	if err := f.c.wsReady(ctx, "n_one", &proto.WSReadyReq{ID: ws.ID, Gen: claim.Workspace.Generation}); err != nil {
		t.Fatal(err)
	}
	grant, err := f.c.grant(ctx, "c_owner", owner, ws.ID)
	if err != nil {
		t.Fatal(err)
	}
	req := proto.VolumePublishReq{
		ID: "dataset", Workspace: ws.ID, Generation: claim.Workspace.Generation,
		ExpectedVersion: 1, Artifact: second, IdempotencyKey: "publish", Grant: grant,
	}
	frame := func(from string, request proto.VolumePublishReq) *proto.Frame {
		return &proto.Frame{T: proto.KindReq, From: from, Op: proto.OpVolumePublishCommit,
			ControllerEpoch: f.c.ControllerEpoch(), Body: proto.MustMarshal(request)}
	}
	if _, err := f.c.dispatch(ctx, frame("c_owner", req)); volumeTestCode(err) != proto.CodeUnauthorized {
		t.Fatalf("client-side publish commit = %v, want unauthorized", err)
	}
	tampered := *grant
	tampered.Signature = append([]byte(nil), grant.Signature...)
	tampered.Signature[0] ^= 0xff
	req.Grant = &tampered
	if _, err := f.c.dispatch(ctx, frame("n_one", req)); volumeTestCode(err) != proto.CodeUnauthorized {
		t.Fatalf("tampered forwarded grant = %v, want unauthorized", err)
	}

	if _, err := f.c.wsACL(ctx, owner.ID, &proto.WSACLReq{ID: ws.ID, ACL: proto.WorkspaceACL{}, IdempotencyKey: "advance-authz"}); err != nil {
		t.Fatal(err)
	}
	req.Grant = grant
	if _, err := f.c.dispatch(ctx, frame("n_one", req)); volumeTestCode(err) != proto.CodeConflict {
		t.Fatalf("stale forwarded grant = %v, want conflict", err)
	}
	currentGrant, err := f.c.grant(ctx, "c_owner", owner, ws.ID)
	if err != nil {
		t.Fatal(err)
	}
	req.Grant = currentGrant
	result, err := f.c.dispatch(ctx, frame("n_one", req))
	if err != nil {
		t.Fatal(err)
	}
	if got := result.(*proto.Volume); got.Version != 2 || got.Artifact != second {
		t.Fatalf("valid forwarded publish = %+v", got)
	}
}

func TestVolumePublishRevalidatesLiveWorkspaceAndPrincipalAfterAuthorization(t *testing.T) {
	for _, mutation := range []string{"workspace-state", "principal"} {
		t.Run(mutation, func(t *testing.T) {
			store, err := artifact.NewStore(t.TempDir())
			if err != nil {
				t.Fatal(err)
			}
			authorizer := &blockingVolumeAuthorizer{}
			f := newControlFixture(t, "", func(opts *Options) {
				opts.Artifacts = store
				opts.Authorizer = authorizer
			})
			ctx := context.Background()
			owner := Subject{ID: "alice", Tenant: "tenant-a", Roles: []string{"writer"}}
			f.c.mu.Lock()
			f.c.subjects["c_owner"] = owner
			f.c.mu.Unlock()
			connectNode(t, f.c, "n_one", processNodeInfo(4096))
			first := volumeArtifact(t, store, "first")
			second := volumeArtifact(t, store, "second")
			if _, err := f.c.volumeCreate(ctx, owner, &proto.VolumeCreateReq{ID: "dataset", Artifact: first, IdempotencyKey: "create-dataset"}); err != nil {
				t.Fatal(err)
			}
			ws, err := f.c.wsCreate(ctx, owner, &proto.WSCreateReq{})
			if err != nil {
				t.Fatal(err)
			}
			claim, err := f.c.wsClaim(ctx, "n_one", ws.ID)
			if err != nil {
				t.Fatal(err)
			}
			if err := f.c.wsReady(ctx, "n_one", &proto.WSReadyReq{ID: ws.ID, Gen: claim.Workspace.Generation}); err != nil {
				t.Fatal(err)
			}
			grant, err := f.c.grant(ctx, "c_owner", owner, ws.ID)
			if err != nil {
				t.Fatal(err)
			}
			entered, release := authorizer.arm(func(subject Subject, action string, resource Resource) bool {
				return subject.ID == owner.ID && action == ActionWrite && resource.Kind == "volume"
			})
			result := make(chan error, 1)
			go func() {
				_, err := f.c.volumePublish(ctx, owner, &proto.VolumePublishReq{
					ID: "dataset", Workspace: ws.ID, Generation: claim.Workspace.Generation,
					ExpectedVersion: 1, Artifact: second, IdempotencyKey: "blocked-publish", Grant: grant,
				}, "n_one")
				result <- err
			}()
			<-entered
			f.c.mu.Lock()
			switch mutation {
			case "workspace-state":
				f.c.workspaces[ws.ID].State = proto.WSDestroying
			case "principal":
				delete(f.c.subjects, "c_owner")
			}
			f.c.mu.Unlock()
			close(release)
			if err := <-result; volumeTestCode(err) != proto.CodeConflict && volumeTestCode(err) != proto.CodeUnauthorized {
				t.Fatalf("publish after %s changed = %v", mutation, err)
			}
			volume, err := f.c.volumeGet(ctx, owner, "dataset")
			if err != nil {
				t.Fatal(err)
			}
			if volume.Version != 1 || volume.Artifact != first {
				t.Fatalf("publish committed after %s changed: %+v", mutation, volume)
			}
		})
	}
}

func TestVolumeEventsCarryWorkspaceAndPinnedDetachVersion(t *testing.T) {
	store, err := artifact.NewStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	f := newControlFixture(t, "", func(opts *Options) { opts.Artifacts = store })
	ctx := context.Background()
	owner := Subject{ID: "alice", Tenant: "tenant-a"}
	art := volumeArtifact(t, store, "dataset")
	if _, err := f.c.volumeCreate(ctx, owner, &proto.VolumeCreateReq{ID: "dataset", Artifact: art, IdempotencyKey: "create-dataset"}); err != nil {
		t.Fatal(err)
	}
	ws, err := f.c.wsCreate(ctx, owner, &proto.WSCreateReq{})
	if err != nil {
		t.Fatal(err)
	}
	f.c.mu.Lock()
	f.c.workspaces[ws.ID].Generation = 9
	f.c.workspaces[ws.ID].Node = "n_one"
	if err := f.c.persistWS(f.c.workspaces[ws.ID]); err != nil {
		f.c.mu.Unlock()
		t.Fatal(err)
	}
	f.c.mu.Unlock()
	if _, err := f.c.volumeAttach(ctx, owner, &proto.VolumeAttachReq{
		ID: "dataset", Workspace: ws.ID, Generation: 9, Path: "/data", IdempotencyKey: "attach-event",
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := f.c.volumeDetach(ctx, owner, &proto.VolumeDetachReq{
		Workspace: ws.ID, Generation: 9, Path: "/data", IdempotencyKey: "detach-event",
	}); err != nil {
		t.Fatal(err)
	}
	events, err := f.log.Read(ctx, 0, ws.ID, 100)
	if err != nil {
		t.Fatal(err)
	}
	seen := map[string]bool{}
	for _, event := range events {
		if event.Type != proto.EvVolumeAttached && event.Type != proto.EvVolumeDetached {
			continue
		}
		seen[event.Type] = true
		if event.Stream != ws.ID || event.Workspace != ws.ID || event.Generation != 9 || event.Node != "n_one" || event.OperationID == "" {
			t.Errorf("incomplete workspace volume event: %+v", event)
		}
		if event.Type == proto.EvVolumeDetached {
			var payload struct {
				Version  uint64 `cbor:"version"`
				Artifact string `cbor:"artifact"`
				Path     string `cbor:"path"`
			}
			if err := proto.Unmarshal(event.Payload, &payload); err != nil {
				t.Fatal(err)
			}
			if payload.Version != 1 || payload.Artifact != art || payload.Path != "/data" {
				t.Errorf("detach payload lost pinned mount: %+v", payload)
			}
		}
	}
	for _, typ := range []string{proto.EvVolumeAttached, proto.EvVolumeDetached} {
		if !seen[typ] {
			t.Fatalf("workspace-filtered events missing %s: %+v", typ, events)
		}
	}
}

func TestVolumePublishRefusesVersionOverflow(t *testing.T) {
	store, err := artifact.NewStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	f := newControlFixture(t, "", func(opts *Options) { opts.Artifacts = store })
	ctx := context.Background()
	owner := Subject{ID: "alice", Tenant: "tenant-a"}
	first := volumeArtifact(t, store, "first")
	second := volumeArtifact(t, store, "second")
	if _, err := f.c.volumeCreate(ctx, owner, &proto.VolumeCreateReq{ID: "dataset", Artifact: first, IdempotencyKey: "create"}); err != nil {
		t.Fatal(err)
	}
	workspace, err := f.c.wsCreate(ctx, owner, &proto.WSCreateReq{IdempotencyKey: "workspace"})
	if err != nil {
		t.Fatal(err)
	}
	f.c.mu.Lock()
	volume := f.c.volumes[volumeKey(owner.Tenant, "dataset")]
	volume.Version = ^uint64(0)
	volume.Versions[len(volume.Versions)-1].Number = ^uint64(0)
	f.c.workspaces[workspace.ID].State = proto.WSClaimed
	f.c.workspaces[workspace.ID].Generation = 1
	f.c.mu.Unlock()
	_, err = f.c.volumePublish(ctx, owner, &proto.VolumePublishReq{
		ID: "dataset", Workspace: workspace.ID, Generation: 1, ExpectedVersion: ^uint64(0),
		Artifact: second, IdempotencyKey: "overflow",
	}, "")
	if volumeTestCode(err) != proto.CodeResourceExhausted {
		t.Fatalf("overflow publish = %v, want resource_exhausted", err)
	}
	got, err := f.c.volumeGet(ctx, owner, "dataset")
	if err != nil || got.Version != ^uint64(0) || got.Artifact != first {
		t.Fatalf("overflow mutated volume: %+v err=%v", got, err)
	}
}

func TestAgentForkPreservesExactVolumePin(t *testing.T) {
	af := newAgentFixture(t, "", nil)
	owner := localSubject()
	first := af.put("dataset-v1")
	second := af.put("dataset-v2")
	if _, err := af.c.volumeCreate(context.Background(), owner, &proto.VolumeCreateReq{ID: "dataset", Artifact: first, IdempotencyKey: "create-dataset"}); err != nil {
		t.Fatal(err)
	}
	a := af.create(t, owner, proto.AgentCreateReq{
		Spec: agentSpec("task"), Policy: proto.AgentPolicy{MaxTurns: 10},
		Workspace: &proto.WorkspaceSpec{Volumes: []proto.VolumeMount{{ID: "dataset", Path: "/data"}}},
	})
	publisher, err := af.c.wsCreate(context.Background(), owner, &proto.WSCreateReq{IdempotencyKey: "publisher"})
	if err != nil {
		t.Fatal(err)
	}
	// Keep the Agent workspace pending and pinned to v1 while an independent
	// claimed publisher advances the catalog to v2.
	af.c.mu.Lock()
	af.c.workspaces[a.WS].LastSnapshot = af.put("workspace-snapshot")
	af.c.workspaces[publisher.ID].Generation = 7
	af.c.workspaces[publisher.ID].State = proto.WSClaimed
	if err := af.c.persistWS(af.c.workspaces[a.WS]); err != nil {
		af.c.mu.Unlock()
		t.Fatal(err)
	}
	if err := af.c.persistWS(af.c.workspaces[publisher.ID]); err != nil {
		af.c.mu.Unlock()
		t.Fatal(err)
	}
	af.c.mu.Unlock()
	if _, err := af.c.volumePublish(context.Background(), owner, &proto.VolumePublishReq{
		ID: "dataset", Workspace: publisher.ID, Generation: 7, ExpectedVersion: 1,
		Artifact: second, IdempotencyKey: "publish-v2",
	}, ""); err != nil {
		t.Fatal(err)
	}
	child, err := af.c.agentFork(context.Background(), owner, &proto.AgentForkReq{ID: a.ID, Name: "child", IdempotencyKey: "fork"})
	if err != nil {
		t.Fatal(err)
	}
	childWS := af.c.snapshotWS(child.WS)
	if len(childWS.Spec.Volumes) != 1 {
		t.Fatalf("forked workspace mounts = %+v", childWS.Spec.Volumes)
	}
	if got := childWS.Spec.Volumes[0]; got.Version != 1 || got.Artifact != first {
		t.Fatalf("fork drifted to current volume version: %+v", got)
	}
}

func TestDestroyedWorkspaceRetainsVolumePinsUntilSourceCleanupAck(t *testing.T) {
	store, err := artifact.NewStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	f := newControlFixture(t, "", func(opts *Options) { opts.Artifacts = store })
	ctx := context.Background()
	owner := localSubject()
	art := volumeArtifact(t, store, "dataset")
	if _, err := f.c.volumeCreate(ctx, owner, &proto.VolumeCreateReq{ID: "dataset", Artifact: art, IdempotencyKey: "create-dataset"}); err != nil {
		t.Fatal(err)
	}
	info := processNodeInfo(4096)
	info.Caps = append(info.Caps, proto.CapabilityReadOnlyVolumes)
	connectNode(t, f.c, "n_one", info)
	ws, err := f.c.wsCreate(ctx, owner, &proto.WSCreateReq{Spec: proto.WorkspaceSpec{
		Volumes: []proto.VolumeMount{{ID: "dataset", Path: "/data"}},
	}})
	if err != nil {
		t.Fatal(err)
	}
	claim, err := f.c.wsClaim(ctx, "n_one", ws.ID)
	if err != nil {
		t.Fatal(err)
	}
	if err := f.c.wsReady(ctx, "n_one", &proto.WSReadyReq{ID: ws.ID, Gen: claim.Workspace.Generation}); err != nil {
		t.Fatal(err)
	}

	var rejectCommit atomic.Bool
	rejectCommit.Store(true)
	sender := &fakeSender{online: map[string]bool{"n_one": true}}
	sender.request = func(_ context.Context, to, op string, body, out any) error {
		if to != "n_one" {
			return proto.Err(proto.CodeUnreachable, "unexpected node %s", to)
		}
		switch op {
		case proto.OpWSRelease:
			req := body.(proto.WSReleaseReq)
			res := out.(*proto.WSReleasedReq)
			res.ID, res.Gen, res.OperationID = req.WS, req.Gen, req.OperationID
			return nil
		case proto.OpWSReleaseAbort, proto.OpWSReleaseAbortCommit:
			return proto.Err(proto.CodeConflict, "abort is invalid after an exact prepared destroy")
		case proto.OpWSReleaseCommit:
			if rejectCommit.Load() {
				return proto.Err(proto.CodeUnreachable, "commit acknowledgement lost")
			}
			return nil
		default:
			return proto.Err(proto.CodeUnsupported, "unexpected operation %s", op)
		}
	}
	f.c.Attach(sender)
	sender.online["n_one"] = false
	if err := f.c.wsDestroy(ctx, owner.ID, ws.ID, "destroy-while-offline"); volumeTestCode(err) != proto.CodeUnreachable {
		t.Fatalf("offline destroy = %v, want unreachable", err)
	}
	if stillClaimed := f.c.snapshotWS(ws.ID); stillClaimed.State != proto.WSClaimed || stillClaimed.Node != "n_one" {
		t.Fatalf("offline destroy crossed source boundary: %+v", stillClaimed)
	}
	sender.online["n_one"] = true

	const destroyKey = "destroy-with-delayed-cleanup"
	if err := f.c.wsDestroy(ctx, owner.ID, ws.ID, destroyKey); volumeTestCode(err) != proto.CodeUnreachable {
		t.Fatalf("destroy with failed source cleanup = %v, want unreachable", err)
	}
	retained := f.c.snapshotWS(ws.ID)
	if retained.State != proto.WSDestroyed || retained.Node != "n_one" || len(retained.Spec.Volumes) != 1 {
		t.Fatalf("destroy lost pending source authority or volume pin: %+v", retained)
	}
	if err := f.c.volumeRemove(ctx, owner, &proto.VolumeRemoveReq{ID: "dataset", IdempotencyKey: "remove-before-cleanup"}); volumeTestCode(err) != proto.CodeConflict {
		t.Fatalf("volume remove before source cleanup = %v, want conflict", err)
	}
	assertReferenced := func(want bool) {
		t.Helper()
		found := false
		if err := f.c.WithArtifactReferences(func(ids []string) error {
			for _, id := range ids {
				found = found || id == art
			}
			return nil
		}); err != nil {
			t.Fatal(err)
		}
		if found != want {
			t.Fatalf("artifact reference present=%v, want %v", found, want)
		}
	}
	assertReferenced(true)
	f.c.mu.Lock()
	f.c.workspaces[ws.ID].UpdatedAt = 1
	f.c.mu.Unlock()
	if _, err := f.c.PruneRecords(ctx, time.Now().Add(time.Hour), 100); err != nil {
		t.Fatal(err)
	}
	if retainedAfterPrune := f.c.snapshotWS(ws.ID); retainedAfterPrune == nil || retainedAfterPrune.Node != "n_one" || len(retainedAfterPrune.Spec.Volumes) != 1 {
		t.Fatalf("record GC removed pending source cleanup proof: %+v", retainedAfterPrune)
	}

	rejectCommit.Store(false)
	if err := f.c.wsDestroy(ctx, owner.ID, ws.ID, "destroy-cleanup-retry"); err != nil {
		t.Fatalf("destroy cleanup replay: %v", err)
	}
	cleaned := f.c.snapshotWS(ws.ID)
	if cleaned.State != proto.WSDestroyed || cleaned.Node != "" || len(cleaned.Spec.Volumes) != 0 {
		t.Fatalf("cleanup replay did not clear source pins: %+v", cleaned)
	}
	if err := f.c.volumeRemove(ctx, owner, &proto.VolumeRemoveReq{ID: "dataset", IdempotencyKey: "remove-after-cleanup"}); err != nil {
		t.Fatalf("volume remove after source cleanup: %v", err)
	}
	assertReferenced(false)
}
