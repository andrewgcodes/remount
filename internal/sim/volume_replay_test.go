package sim

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"remount.dev/remount/internal/artifact"
	"remount.dev/remount/internal/client"
	"remount.dev/remount/internal/node"
	"remount.dev/remount/internal/proto"
)

func TestVolumePublishRetryAfterLostCommitResponseDoesNotRepeatSnapshot(t *testing.T) {
	w := newWorld(t)
	const nodeName = "volume-replay-node"
	var hellos atomic.Int32
	var dropped atomic.Bool
	droppedResponse := make(chan struct{})
	w.mu.Lock()
	w.peerHooks[nodeName] = func(frame *proto.Frame) bool {
		if frame.T == proto.KindHello {
			hellos.Add(1)
		}
		return true
	}
	w.serverHooks[nodeName] = func(frame *proto.Frame) bool {
		if frame.T == proto.KindRes && frame.Op == proto.OpVolumePublishCommit && frame.Err == nil && dropped.CompareAndSwap(false, true) {
			close(droppedResponse)
			go w.cut(nodeName)
			return false
		}
		return true
	}
	w.mu.Unlock()
	var mounts *recordingVolumes
	n := w.nodeWith(nodeName, func(options *node.Options) {
		mounts = newRecordingVolumes(filepath.Join(options.DataDir, "volumes", "sources"))
		options.Volumes, options.VolumeCapability = mounts, true
		// A repeated archive would be rejected by admission. A successful
		// exact retry therefore also proves admission was not consumed twice.
		options.SnapshotMinInterval = time.Hour
	})
	c := w.client("volume-replay-client")

	seed := t.TempDir()
	if err := os.WriteFile(filepath.Join(seed, "dataset.txt"), []byte("version one"), 0o644); err != nil {
		t.Fatal(err)
	}
	var archive bytes.Buffer
	if err := artifact.Snapshot(seed, nil, &archive); err != nil {
		t.Fatal(err)
	}
	ctx := ctxT(t, 30*time.Second)
	artifactID, _, err := c.UploadArtifact(ctx, bytes.NewReader(archive.Bytes()))
	if err != nil {
		t.Fatal(err)
	}
	created, err := c.CreateVolume(ctx, proto.VolumeCreateReq{ID: "dataset", Artifact: artifactID})
	if err != nil {
		t.Fatal(err)
	}
	ws := mustWS(t, c, proto.WorkspaceSpec{
		Placement: proto.Placement{Node: n.ID()},
		Volumes:   []proto.VolumeMount{{ID: "dataset", Path: "/shared"}},
	})
	if err := c.WriteFile(ctx, ws.ID, "publish/value.txt", []byte("version two"), 0o644); err != nil {
		t.Fatal(err)
	}

	firstCtx, cancelFirst := context.WithTimeout(context.Background(), 5*time.Second)
	_, firstErr := c.PublishVolumePath(firstCtx, ws.ID, "/publish", "dataset", created.Version,
		client.WithIdempotencyKey("publish-after-lost-ack"))
	cancelFirst()
	select {
	case <-droppedResponse:
	case <-time.After(5 * time.Second):
		t.Fatal("test did not drop the committed publish response")
	}
	if firstErr == nil {
		// The client can reconnect quickly enough to complete its built-in
		// retry. Replaying below is still required to be side-effect free.
	} else if !errors.Is(firstErr, context.DeadlineExceeded) {
		var protocolError *proto.Error
		if !errors.As(firstErr, &protocolError) && !strings.Contains(firstErr.Error(), "closed") {
			t.Fatalf("first ambiguous publish = %v", firstErr)
		}
	}
	deadline := time.Now().Add(10 * time.Second)
	for hellos.Load() < 2 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if hellos.Load() < 2 {
		t.Fatal("node did not reconnect after the lost commit response")
	}

	// A fresh session started after the ambiguous first call must survive the
	// retry. KillWorkspace is part of archive production, not commit replay.
	session, err := c.Exec(ctx, proto.SOpenReq{
		WS: ws.ID, Kind: proto.SessionExec, Program: []string{"sh", "-c", "sleep 30"},
	})
	if err != nil {
		t.Fatal(err)
	}
	published, err := c.PublishVolumePath(ctx, ws.ID, "/publish", "dataset", created.Version,
		client.WithIdempotencyKey("publish-after-lost-ack"))
	if err != nil {
		t.Fatal(err)
	}
	if published.Version != 2 {
		t.Fatalf("publish replay version = %d, want 2", published.Version)
	}
	select {
	case <-session.Done():
		t.Fatalf("publish replay terminated a fresh session: exit=%+v err=%v", session.Exit(), session.Err())
	default:
	}
	if err := session.Close(ctx, true); err != nil {
		t.Fatal(err)
	}

	events, err := c.ReadEvents(ctx, 0, ws.ID)
	if err != nil {
		t.Fatal(err)
	}
	snapshots := 0
	for _, event := range events {
		if event.Type == proto.EvWSSnapshot {
			snapshots++
		}
	}
	if snapshots != 1 {
		t.Fatalf("volume publish archive events = %d, want one; events=%+v", snapshots, events)
	}
}
