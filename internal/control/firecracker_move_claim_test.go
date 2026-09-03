package control

import (
	"context"
	"testing"

	"remount.dev/remount/internal/proto"
)

// movedProcessesClaim returns the processes field of the single ws.moved event
// for ws, or "" when no such event was recorded.
func movedProcessesClaim(t *testing.T, f *controlFixture, ws string) string {
	t.Helper()
	events, err := f.log.Read(context.Background(), 0, "", 1000)
	if err != nil {
		t.Fatal(err)
	}
	claim := ""
	seen := 0
	for _, event := range events {
		if event.Type != proto.EvWSMoved || event.Workspace != ws {
			continue
		}
		seen++
		var payload map[string]any
		if err := proto.Unmarshal(event.Payload, &payload); err != nil {
			t.Fatalf("decode ws.moved payload: %v", err)
		}
		value, _ := payload["processes"].(string)
		claim = value
	}
	if seen != 1 {
		t.Fatalf("ws.moved events for %s = %d, want 1", ws, seen)
	}
	return claim
}

// moveWithSnapshotFormat drives one workspace through claim, ready and move,
// with the node reporting the given checkpoint format on release.
func moveWithSnapshotFormat(t *testing.T, format string) (*controlFixture, string) {
	t.Helper()
	f := newControlFixture(t, "", nil)
	sender := &fakeSender{online: map[string]bool{"n_one": true}}
	snapshot := "art_sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	sender.request = func(_ context.Context, _ string, op string, body, out any) error {
		if op == proto.OpWSRelease {
			req := body.(proto.WSReleaseReq)
			*out.(*proto.WSReleasedReq) = proto.WSReleasedReq{
				ID: req.WS, Gen: req.Gen, OperationID: req.OperationID,
				Snapshot: snapshot, SnapshotFormat: format, Reason: req.Reason,
			}
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
	moved, err := f.c.wsMove(context.Background(), "alice", &proto.WSMoveReq{ID: ws.ID, IdempotencyKey: "move-claim"})
	if err != nil {
		t.Fatal(err)
	}
	if moved.State != proto.WSPending || moved.Spec.RestoreFrom != snapshot {
		t.Fatalf("move did not reach pending with a checkpoint: %#v", moved)
	}
	return f, ws.ID
}

// TestMoveNeverClaimsPreservedProcessesBeforeRestore pins ADR 0063: ws.moved is
// emitted when the move enters pending, before a destination is chosen and long
// before any guest is restored, so it may not assert process continuity from the
// artifact format alone. A full-VM checkpoint reports restore_pending; the
// positive claim belongs to a destination that actually restored.
func TestMoveNeverClaimsPreservedProcessesBeforeRestore(t *testing.T) {
	f, ws := moveWithSnapshotFormat(t, proto.ArtifactFormatFirecrackerFullV1)
	claim := movedProcessesClaim(t, f, ws)
	if claim == "preserved" {
		t.Fatal("ws.moved claimed processes=preserved from the artifact format alone; no destination had been chosen and no guest had been restored (ADR 0063)")
	}
	if claim != "restore_pending" {
		t.Fatalf("full-VM move recorded processes=%q, want restore_pending", claim)
	}
}

// TestMoveReportsRestartedForFilesystemOnlyCheckpoints keeps the settled answer
// settled: a snapshot that carries no process memory restarts processes no
// matter which destination claims it, so that claim is final at move time.
func TestMoveReportsRestartedForFilesystemOnlyCheckpoints(t *testing.T) {
	for _, format := range []string{proto.ArtifactFormatTar, proto.ArtifactFormatChunkedV1} {
		t.Run(format, func(t *testing.T) {
			f, ws := moveWithSnapshotFormat(t, format)
			if claim := movedProcessesClaim(t, f, ws); claim != "restarted" {
				t.Fatalf("%s move recorded processes=%q, want restarted", format, claim)
			}
		})
	}
}
