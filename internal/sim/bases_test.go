package sim

import (
	"errors"
	"testing"
	"time"

	"remount.dev/remount/internal/client"
	"remount.dev/remount/internal/control"
	"remount.dev/remount/internal/proto"
	"remount.dev/remount/internal/server"
)

// TestBaseLifecyclePinsSnapshotAcrossTenant is the P1.2 proof: a snapshot
// pinned as a base survives artifact GC after its source workspace is gone,
// any member of the tenant can start from it, another tenant cannot see or
// use it, and `base rm` is what makes the artifact collectable again.
func TestBaseLifecyclePinsSnapshotAcrossTenant(t *testing.T) {
	w := newWorldWith(t, func(o *server.Options) {
		o.Authenticator = control.StaticAuthenticator{
			"owner-tok": {ID: "owner", Tenant: "team"},
			"peer-tok":  {ID: "peer", Tenant: "team"},
			"other-tok": {ID: "other", Tenant: "rivals"},
		}
	})
	w.node("n1", nil)
	owner := w.clientWithToken("owner", "owner-tok")
	peer := w.clientWithToken("peer", "peer-tok")
	other := w.clientWithToken("other", "other-tok")
	ctx := ctxT(t, 90*time.Second)

	src := mustWS(t, owner, proto.WorkspaceSpec{Name: "golden"})
	if err := owner.WriteFile(ctx, src.ID, "tools/setup.sh", []byte("#!/bin/sh\necho ready\n"), 0); err != nil {
		t.Fatal(err)
	}
	snap, err := owner.Snapshot(ctx, src.ID, true, client.WithIdempotencyKey("golden-snap"))
	if err != nil {
		t.Fatal(err)
	}

	// Name validation is fail-closed before any state changes.
	var pe *proto.Error
	if _, err := owner.CreateBase(ctx, proto.BaseCreateReq{Name: "../etc", Artifact: snap.Artifact}); !errors.As(err, &pe) || pe.Code != proto.CodeBadRequest {
		t.Fatalf("hostile base name: %v, want bad_request", err)
	}
	if _, err := owner.CreateBase(ctx, proto.BaseCreateReq{Name: "golden", Artifact: "art_sha256:" + zeros(64)}); !errors.As(err, &pe) || pe.Code != proto.CodeNotFound {
		t.Fatalf("missing artifact: %v, want not_found", err)
	}

	base, err := owner.CreateBase(ctx, proto.BaseCreateReq{Name: "golden", Artifact: snap.Artifact, Workspace: src.ID}, client.WithIdempotencyKey("pin-golden"))
	if err != nil {
		t.Fatal(err)
	}
	if base.Tenant != "team" || base.Owner != "owner" || base.Artifact != snap.Artifact || base.Bytes <= 0 || base.Workspace != src.ID {
		t.Fatalf("base = %+v", base)
	}
	// Idempotent replay returns the same base; a different payload under the
	// same key is a conflict; a fresh key for the same name is a conflict too.
	again, err := owner.CreateBase(ctx, proto.BaseCreateReq{Name: "golden", Artifact: snap.Artifact, Workspace: src.ID}, client.WithIdempotencyKey("pin-golden"))
	if err != nil || again.CreatedAt != base.CreatedAt {
		t.Fatalf("replay = %+v, %v", again, err)
	}
	if _, err := owner.CreateBase(ctx, proto.BaseCreateReq{Name: "golden", Artifact: snap.Artifact}, client.WithIdempotencyKey("pin-golden")); !errors.As(err, &pe) || pe.Code != proto.CodeConflict {
		t.Fatalf("reused key with different args: %v, want conflict", err)
	}
	if _, err := peer.CreateBase(ctx, proto.BaseCreateReq{Name: "golden", Artifact: snap.Artifact}); !errors.As(err, &pe) || pe.Code != proto.CodeConflict {
		t.Fatalf("duplicate name in tenant: %v, want conflict", err)
	}

	// Tenant scoping: the peer sees it, the other tenant does not, and may
	// even reuse the name for its own base.
	if bases, err := peer.ListBases(ctx); err != nil || len(bases) != 1 || bases[0].Name != "golden" {
		t.Fatalf("peer ls = %+v, %v", bases, err)
	}
	if bases, err := other.ListBases(ctx); err != nil || len(bases) != 0 {
		t.Fatalf("other tenant ls = %+v, %v", bases, err)
	}
	if _, err := other.CreateWorkspace(ctx, proto.WorkspaceSpec{Base: "golden"}); !errors.As(err, &pe) || pe.Code != proto.CodeNotFound {
		t.Fatalf("other tenant ws create --base: %v, want not_found", err)
	}
	if _, err := owner.CreateWorkspace(ctx, proto.WorkspaceSpec{Base: "golden", RestoreFrom: snap.Artifact}); !errors.As(err, &pe) || pe.Code != proto.CodeBadRequest {
		t.Fatalf("base + restore_from: %v, want bad_request", err)
	}

	// The source workspace goes away; the pin alone keeps the artifact alive.
	if err := owner.DestroyWorkspace(ctx, src.ID, client.WithIdempotencyKey("destroy-src")); err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	ageArtifact(t, w.artifactDir, snap.Artifact, now.Add(-48*time.Hour))
	if _, err := w.srv.CollectArtifacts(now); err != nil {
		t.Fatal(err)
	}
	if !w.srv.Store.Has(snap.Artifact) {
		t.Fatal("GC unlinked a pinned base artifact")
	}

	// A tenant peer starts from the base and sees the snapshot's content.
	fromBase := mustWS(t, peer, proto.WorkspaceSpec{Base: "golden"})
	if fromBase.Spec.RestoreFrom != snap.Artifact || fromBase.Spec.Base != "golden" {
		t.Fatalf("resolved spec = %+v", fromBase.Spec)
	}
	got, err := peer.ReadFile(ctx, fromBase.ID, "tools/setup.sh")
	if err != nil || string(got) != "#!/bin/sh\necho ready\n" {
		t.Fatalf("restored content = %q, %v", got, err)
	}

	// Only the owner (or an admin) may remove; the tenant peer may not.
	if err := peer.RemoveBase(ctx, "golden"); !errors.As(err, &pe) || pe.Code != proto.CodeDenied {
		t.Fatalf("peer rm: %v, want denied", err)
	}
	if err := other.RemoveBase(ctx, "golden"); !errors.As(err, &pe) || pe.Code != proto.CodeNotFound {
		t.Fatalf("other tenant rm: %v, want not_found", err)
	}
	if err := owner.RemoveBase(ctx, "golden", client.WithIdempotencyKey("rm-golden")); err != nil {
		t.Fatal(err)
	}
	if err := owner.RemoveBase(ctx, "golden", client.WithIdempotencyKey("rm-golden")); err != nil {
		t.Fatalf("rm replay: %v", err)
	}
	if err := owner.RemoveBase(ctx, "golden"); !errors.As(err, &pe) || pe.Code != proto.CodeNotFound {
		t.Fatalf("rm after rm: %v, want not_found", err)
	}
	if _, err := peer.CreateWorkspace(ctx, proto.WorkspaceSpec{Base: "golden"}); !errors.As(err, &pe) || pe.Code != proto.CodeNotFound {
		t.Fatalf("ws create from removed base: %v, want not_found", err)
	}

	// The workspace created from the base still references the artifact, so
	// GC keeps it; destroying that workspace releases the last reference.
	if _, err := w.srv.CollectArtifacts(now); err != nil {
		t.Fatal(err)
	}
	if !w.srv.Store.Has(snap.Artifact) {
		t.Fatal("GC unlinked an artifact still referenced by a workspace's RestoreFrom")
	}
	if err := peer.DestroyWorkspace(ctx, fromBase.ID, client.WithIdempotencyKey("destroy-from-base")); err != nil {
		t.Fatal(err)
	}
	result, err := w.srv.CollectArtifacts(now)
	if err != nil {
		t.Fatal(err)
	}
	if w.srv.Store.Has(snap.Artifact) || result.Removed != 1 {
		t.Fatalf("post-unpin GC = %+v, has=%t", result, w.srv.Store.Has(snap.Artifact))
	}

	// Events: exactly one base.created and one base.removed, tenant-tagged.
	evs, err := owner.ReadEvents(ctx, 1, "")
	if err != nil {
		t.Fatal(err)
	}
	created, removed := 0, 0
	for _, e := range evs {
		if e.Tenant != "team" {
			continue
		}
		var payload struct {
			Name     string `cbor:"name"`
			Artifact string `cbor:"artifact"`
		}
		switch e.Type {
		case proto.EvBaseCreated:
			if err := proto.Unmarshal(e.Payload, &payload); err != nil || payload.Name != "golden" || payload.Artifact != snap.Artifact || e.Principal != "owner" {
				t.Fatalf("base.created payload = %+v (%v), principal=%q", payload, err, e.Principal)
			}
			created++
		case proto.EvBaseRemoved:
			removed++
		}
	}
	if created != 1 || removed != 1 {
		t.Fatalf("events: %d base.created, %d base.removed, want 1 and 1", created, removed)
	}
}

func zeros(n int) string {
	b := make([]byte, n)
	for i := range b {
		b[i] = '0'
	}
	return string(b)
}
