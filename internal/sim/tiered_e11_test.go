package sim

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"remount.dev/remount/internal/artifact"
	"remount.dev/remount/internal/client"
	"remount.dev/remount/internal/node"
	"remount.dev/remount/internal/proto"
)

func collectSessionChunks(t *testing.T, ctx context.Context, s *client.Session) []client.Chunk {
	t.Helper()
	var chunks []client.Chunk
	for {
		select {
		case chunk, ok := <-s.Chunks():
			if !ok {
				return chunks
			}
			chunks = append(chunks, chunk)
		case <-ctx.Done():
			t.Fatalf("collect session %s: %v", s.ID, ctx.Err())
		}
	}
}

func sameReplay(got, want []client.Chunk) bool {
	if len(got) != len(want) {
		return false
	}
	for i := range got {
		if got[i].Seq != want[i].Seq || got[i].Stream != want[i].Stream || !bytes.Equal(got[i].Data, want[i].Data) {
			return false
		}
	}
	return true
}

func waitSessionLogComplete(t *testing.T, ctx context.Context, c *client.Client, session string) {
	t.Helper()
	for deadline := time.Now().Add(10 * time.Second); ; {
		events, err := c.ReadEvents(ctx, 0, "")
		if err != nil {
			t.Fatal(err)
		}
		for _, event := range events {
			if event.Type != proto.EvSessionLogCommitted || event.Stream != session {
				continue
			}
			var payload struct {
				Complete bool `cbor:"complete"`
			}
			if err := proto.Unmarshal(event.Payload, &payload); err != nil {
				t.Fatalf("decode %s at seq %d: %v", event.Type, event.Seq, err)
			}
			if payload.Complete {
				return
			}
		}
		if time.Now().After(deadline) {
			t.Fatalf("session log %s was not durably completed", session)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// TestE11TieredSessionReplayCrossesNodeAndNamesUnavailableBlob composes the
// process producer, node/session manager, authorized artifact HTTP transfers,
// control-owned durable record, workspace handoff, and public client cursors.
// Small segment thresholds compactly exercise the same segment lifecycle as
// the six-hour/2GiB acceptance workload without allocating two gigabytes.
func TestE11TieredSessionReplayCrossesNodeAndNamesUnavailableBlob(t *testing.T) {
	w := newWorld(t)
	n1Dir, n2Dir := t.TempDir(), t.TempDir()
	configure := func(dir string) func(*node.Options) {
		return func(options *node.Options) {
			options.DataDir = dir
			options.Labels = map[string]string{"e11": "yes"}
			options.SessionMemoryBytes = 64 << 10
			options.SessionSpillBytes = 1 << 20
			options.SessionSegmentBytes = 128 << 10
			options.SessionMaxLogSegments = 1024
		}
	}
	n1 := w.nodeWith("e11-n1", configure(n1Dir))
	n2 := w.nodeWith("e11-n2", configure(n2Dir))
	c := w.client("e11-client")
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	ws := mustWS(t, c, proto.WorkspaceSpec{Placement: proto.Placement{Node: n1.ID()}})

	s, err := c.Exec(ctx, proto.SOpenReq{WS: ws.ID, Kind: proto.SessionExec, Program: []string{"sh", "-c", "head -c 8388608 /dev/zero"}})
	if err != nil {
		t.Fatal(err)
	}
	original := collectSessionChunks(t, ctx, s)
	if len(original) < 128 || original[0].Stream != proto.StreamInfo || original[len(original)-1].Stream != proto.StreamExit {
		t.Fatalf("producer traversed too few chunks or broke framing: %d", len(original))
	}

	moved, err := c.MoveWorkspace(ctx, ws.ID, nil, &proto.Placement{Node: n2.ID()})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := c.WaitClaimed(ctx, moved.ID); err != nil {
		t.Fatal(err)
	}
	for _, from := range []int{0, len(original) / 2, len(original) - 2} {
		replayed, err := c.Attach(ctx, ws.ID, s.ID, uint64(from))
		if err != nil {
			t.Fatalf("attach from %d: %v", from, err)
		}
		got := collectSessionChunks(t, ctx, replayed)
		if !sameReplay(got, original[from:]) {
			t.Fatalf("replay from %d differs: got=%d want=%d", from, len(got), len(original)-from)
		}
	}

	// A live GC root would prevent this in production; forced deletion models
	// an unavailable/corrupt blob service. The node must reject its plaintext
	// cache without a fresh authorization HEAD and emit a tier-named gap.
	ids, err := w.srv.Store.List()
	if err != nil {
		t.Fatal(err)
	}
	if len(ids) == 0 {
		t.Fatal("tiered session produced no remote artifacts")
	}
	for _, id := range ids {
		if err := w.srv.Store.Delete(id); err != nil {
			t.Fatalf("delete remote artifact %s: %v", id, err)
		}
		digest, err := artifact.Digest(id)
		if err != nil {
			t.Fatal(err)
		}
		// Standalone mode intentionally permits already-authorized local cache
		// reads. Removing the destination cache as well models the blob tier
		// being unavailable after a cold restart, which is the E11 failure
		// boundary this assertion exercises.
		if err := os.Remove(filepath.Join(n2Dir, "artifacts", digest[:2], digest)); err != nil && !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("remove cached artifact %s: %v", id, err)
		}
	}
	unavailable, err := c.Attach(ctx, ws.ID, s.ID, 0)
	if err != nil {
		t.Fatal(err)
	}
	var first client.Chunk
	select {
	case chunk, ok := <-unavailable.Chunks():
		if !ok {
			t.Fatal("unavailable blob replay closed without a gap")
		}
		first = chunk
	case <-ctx.Done():
		t.Fatalf("wait for unavailable blob gap: %v", ctx.Err())
	}
	if first.Stream != proto.StreamGap {
		t.Fatalf("unavailable blob replay began with stream %d, want gap", first.Stream)
	}
	var gap proto.Gap
	if err := proto.Unmarshal(first.Data, &gap); err != nil || gap.Tier != "blob" {
		t.Fatalf("unavailable gap = %+v, %v; want tier blob", gap, err)
	}
}

func TestE11TieredSessionRecordSurvivesNodeRestart(t *testing.T) {
	w := newWorldExpiring(t)
	dataDir := t.TempDir()
	configure := func(options *node.Options) {
		options.DataDir = dataDir
		options.Labels = map[string]string{"e11-restart": "yes"}
		options.SessionMemoryBytes = 32 << 10
		options.SessionSpillBytes = 256 << 10
		options.SessionSegmentBytes = 64 << 10
		options.SessionMaxLogSegments = 256
	}
	n := w.nodeWith("e11-restart", configure)
	c := w.client("e11-restart-client")
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	ws := mustWS(t, c, proto.WorkspaceSpec{Placement: proto.Placement{Node: n.ID()}})
	s, err := c.Exec(ctx, proto.SOpenReq{WS: ws.ID, Kind: proto.SessionExec, Program: []string{"sh", "-c", "head -c 1048576 /dev/zero"}})
	if err != nil {
		t.Fatal(err)
	}
	original := collectSessionChunks(t, ctx, s)
	if len(original) < 16 {
		t.Fatalf("restart fixture traversed too few chunks: %d", len(original))
	}
	waitSessionLogComplete(t, ctx, c, s.ID)

	w.stopNode("e11-restart")
	restarted := w.nodeWith("e11-restart", configure)
	if restarted.ID() != n.ID() {
		t.Fatalf("durable node identity changed across restart: %s != %s", restarted.ID(), n.ID())
	}
	if _, err := c.WaitClaimed(ctx, ws.ID); err != nil {
		t.Fatal(err)
	}
	replayed, err := c.Attach(ctx, ws.ID, s.ID, 0)
	if err != nil {
		t.Fatal(err)
	}
	if got := collectSessionChunks(t, ctx, replayed); !sameReplay(got, original) {
		t.Fatalf("replay after node restart differs: got=%d want=%d", len(got), len(original))
	}
}

func TestE11ArchivedPTYRetainsKindAfterMove(t *testing.T) {
	w := newWorld(t)
	n1 := w.nodeWith("e11-pty-n1", func(options *node.Options) {
		options.Labels = map[string]string{"e11-pty": "yes"}
		options.SessionSegmentBytes = 64 << 10
	})
	n2 := w.nodeWith("e11-pty-n2", func(options *node.Options) {
		options.Labels = map[string]string{"e11-pty": "yes"}
		options.SessionSegmentBytes = 64 << 10
	})
	c := w.client("e11-pty-client")
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	ws := mustWS(t, c, proto.WorkspaceSpec{Placement: proto.Placement{Node: n1.ID()}})

	s, err := c.Exec(ctx, proto.SOpenReq{
		WS: ws.ID, Kind: proto.SessionPTY, Program: []string{"sh", "-c", "printf 'one\\ntwo\\n'"},
		Rows: 20, Cols: 90,
	})
	if err != nil {
		t.Fatal(err)
	}
	original := collectSessionChunks(t, ctx, s)
	waitSessionLogComplete(t, ctx, c, s.ID)

	moved, err := c.MoveWorkspace(ctx, ws.ID, nil, &proto.Placement{Node: n2.ID()})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := c.WaitClaimed(ctx, moved.ID); err != nil {
		t.Fatal(err)
	}
	replayed, err := c.Attach(ctx, ws.ID, s.ID, 0)
	if err != nil {
		t.Fatal(err)
	}
	if replayed.Kind != proto.SessionPTY {
		t.Fatalf("archived session kind = %q, want %q", replayed.Kind, proto.SessionPTY)
	}
	if got := collectSessionChunks(t, ctx, replayed); !sameReplay(got, original) {
		t.Fatalf("PTY replay after move differs: got=%d want=%d", len(got), len(original))
	}
}
