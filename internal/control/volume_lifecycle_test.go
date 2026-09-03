package control

import (
	"context"
	"sync/atomic"
	"testing"

	"remount.dev/remount/internal/artifact"
	"remount.dev/remount/internal/proto"
	"remount.dev/remount/internal/transport"
)

func TestFleetDestroyRetainsPinsUntilDurableNodeAck(t *testing.T) {
	store, err := artifact.NewStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	f := newControlFixture(t, "", func(opts *Options) { opts.Artifacts = store })
	ctx := context.Background()
	owner := localSubject()
	dataset := volumeArtifact(t, store, "dataset")
	checkpoint := volumeArtifact(t, store, "forensic-checkpoint")
	if _, err := f.c.volumeCreate(ctx, owner, &proto.VolumeCreateReq{ID: "dataset", Artifact: dataset, IdempotencyKey: "create-dataset"}); err != nil {
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

	var commits atomic.Int32
	sender := &fakeSender{online: map[string]bool{"n_one": true}}
	sender.request = func(_ context.Context, to, op string, body, out any) error {
		if to != "n_one" {
			t.Fatalf("request node=%q", to)
		}
		switch op {
		case proto.OpWSQuarantine:
			req := body.(proto.WSQuarantineReq)
			if req.Tenant != owner.Tenant || len(req.Volumes) != 1 || req.Volumes[0].Artifact != dataset ||
				req.Volumes[0].Version != 1 || req.Volumes[0].Path != "/data" {
				t.Fatalf("quarantine lost exact recovery declarations: %+v", req)
			}
			*out.(*proto.WSQuarantineRes) = proto.WSQuarantineRes{
				Fenced: true, Generation: req.Gen, Action: req.Action, Backend: "process", Snapshot: checkpoint,
			}
		case proto.OpWSQuarantineCommit:
			if commits.Add(1) == 1 {
				return transport.ErrClosed
			}
		default:
			t.Fatalf("unexpected operation %s", op)
		}
		return nil
	}
	f.c.Attach(sender)
	op, err := f.c.fleetQuarantine(ctx, owner, &proto.FleetQuarantineReq{
		Selector: proto.WorkspaceSelector{Workspace: ws.ID}, Action: proto.FleetActionDestroy,
		IdempotencyKey: "destroy-with-lost-ack",
	})
	if err != nil {
		t.Fatal(err)
	}
	f.c.runFleetOperation(ctx, op.ID)
	retained := f.c.snapshotWS(ws.ID)
	if retained.State != proto.WSDestroyed || retained.Node != "n_one" || len(retained.Spec.Volumes) != 1 {
		t.Fatalf("lost acknowledgement released source pins: %+v", retained)
	}
	events, err := f.log.Read(ctx, 0, ws.ID, 100)
	if err != nil {
		t.Fatal(err)
	}
	for _, event := range events {
		if event.Type == proto.EvVolumeDetached {
			t.Fatalf("detach event emitted before durable node acknowledgement: %+v", event)
		}
	}

	f.c.runFleetOperation(ctx, op.ID)
	cleaned := f.c.snapshotWS(ws.ID)
	if cleaned.State != proto.WSDestroyed || cleaned.Node != "" || len(cleaned.Spec.Volumes) != 0 || commits.Load() != 2 {
		t.Fatalf("durable acknowledgement did not clear pins: workspace=%+v commits=%d", cleaned, commits.Load())
	}
	events, err = f.log.Read(ctx, 0, ws.ID, 200)
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, event := range events {
		if event.Type != proto.EvVolumeDetached {
			continue
		}
		found = true
		if event.Workspace != ws.ID || event.Stream != ws.ID || event.Generation != claim.Workspace.Generation ||
			event.Node != "n_one" || event.OperationID != op.ID {
			t.Fatalf("detach event is not indexed to physical pin: %+v", event)
		}
		var payload struct {
			Artifact string `cbor:"artifact"`
			Version  uint64 `cbor:"version"`
			Path     string `cbor:"path"`
			Reason   string `cbor:"reason"`
		}
		if err := proto.Unmarshal(event.Payload, &payload); err != nil {
			t.Fatal(err)
		}
		if payload.Artifact != dataset || payload.Version != 1 || payload.Path != "/data" || payload.Reason != "fleet.quarantine" {
			t.Fatalf("detach event lost pinned version: %+v", payload)
		}
	}
	if !found {
		t.Fatal("durable cleanup emitted no volume.detached event")
	}
}

func TestMoveDoesNotAdvancePastLostReleaseCommitAck(t *testing.T) {
	f := newControlFixture(t, "", nil)
	var commits atomic.Int32
	snapshot := "art_sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	sender := &fakeSender{online: map[string]bool{"n_one": true}}
	sender.request = func(_ context.Context, _ string, op string, body, out any) error {
		switch op {
		case proto.OpWSRelease:
			req := body.(proto.WSReleaseReq)
			*out.(*proto.WSReleasedReq) = proto.WSReleasedReq{ID: req.WS, Gen: req.Gen, OperationID: req.OperationID, Snapshot: snapshot, Reason: req.Reason}
		case proto.OpWSReleaseCommit:
			if commits.Add(1) == 1 {
				return transport.ErrClosed
			}
		default:
			t.Fatalf("unexpected operation %s", op)
		}
		return nil
	}
	f.c.Attach(sender)
	connectNode(t, f.c, "n_one", processNodeInfo(4096))
	ws := createWorkspace(t, f.c, localSubject(), proto.WorkspaceSpec{})
	claim, err := f.c.wsClaim(context.Background(), "n_one", ws.ID)
	if err != nil {
		t.Fatal(err)
	}
	if err := f.c.wsReady(context.Background(), "n_one", &proto.WSReadyReq{ID: ws.ID, Gen: claim.Workspace.Generation}); err != nil {
		t.Fatal(err)
	}
	moved, err := f.c.wsMove(context.Background(), localSubject().ID, &proto.WSMoveReq{ID: ws.ID, IdempotencyKey: "move-lost-commit-ack"})
	if err != nil {
		t.Fatal(err)
	}
	if commits.Load() != 2 || moved.State != proto.WSPending || moved.Node != "" || moved.Spec.RestoreFrom != snapshot {
		t.Fatalf("move crossed unacknowledged cleanup: workspace=%+v commits=%d", moved, commits.Load())
	}
}
