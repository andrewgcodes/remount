package sim

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"testing"
	"time"

	"remount.dev/remount/internal/client"
	"remount.dev/remount/internal/node"
	"remount.dev/remount/internal/proto"
)

// perfPayloadBytes is the fixture size for the move throughput harness.
// REMOUNT_PERF_MB overrides it so the same test can reproduce the reported
// 200 MB measurement without making the default run expensive.
func perfPayloadBytes(t *testing.T) int64 {
	t.Helper()
	mb := int64(64)
	if raw := os.Getenv("REMOUNT_PERF_MB"); raw != "" {
		parsed, err := strconv.ParseInt(raw, 10, 64)
		if err != nil || parsed <= 0 {
			t.Fatalf("REMOUNT_PERF_MB=%q is not a positive integer", raw)
		}
		mb = parsed
	}
	return mb << 20
}

func perfIncompressible(t *testing.T, n int64) []byte {
	t.Helper()
	buf := make([]byte, n)
	if _, err := rand.Read(buf); err != nil {
		t.Fatal(err)
	}
	return buf
}

func perfCompressible(n int64) []byte {
	buf := make([]byte, n)
	line := []byte("the quick brown fox jumps over the lazy dog 0123456789\n")
	for i := 0; i < len(buf); i += len(line) {
		copy(buf[i:], line)
	}
	return buf
}

// perfMove runs one whole two-node move of a workspace holding payload and
// reports the wall clock. It fails if the destination tree does not hold the
// exact bytes, so a throughput number is never reported for a move that lost
// or corrupted content.
func perfMove(t *testing.T, payload []byte, label string) time.Duration {
	t.Helper()
	w := newWorld(t)
	n1Dir, n2Dir := t.TempDir(), t.TempDir()
	n1 := w.nodeWith("perf-n1", func(o *node.Options) {
		o.DataDir = n1Dir
		o.SnapshotMinInterval = time.Nanosecond
	})
	n2 := w.nodeWith("perf-n2", func(o *node.Options) { o.DataDir = n2Dir })
	c := w.client("perf-client")
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Minute)
	defer cancel()
	ws := mustWS(t, c, proto.WorkspaceSpec{Placement: proto.Placement{Node: n1.ID()}})

	// Writing the fixture straight into the source tree keeps the measurement
	// on the move rather than on however the bytes arrived.
	if err := os.WriteFile(filepath.Join(n1Dir, "ws", ws.ID, "payload.bin"), payload, 0o600); err != nil {
		t.Fatal(err)
	}
	want := sha256.Sum256(payload)

	started := time.Now()
	moved, err := c.MoveWorkspace(ctx, ws.ID, nil, &proto.Placement{Node: n2.ID()})
	if err != nil {
		t.Fatal(err)
	}
	moved, err = c.WaitClaimed(ctx, moved.ID)
	if err != nil {
		t.Fatal(err)
	}
	elapsed := time.Since(started)
	perfReportPhases(t, ctx, c, ws.ID, label, started)
	if moved.Node != n2.ID() {
		t.Fatalf("workspace landed on %s, want %s", moved.Node, n2.ID())
	}
	got, err := os.ReadFile(filepath.Join(n2Dir, "ws", ws.ID, "payload.bin"))
	if err != nil {
		t.Fatal(err)
	}
	if sha256.Sum256(got) != want {
		t.Fatalf("%s: destination payload differs (%d bytes)", label, len(got))
	}
	mb := float64(len(payload)) / (1 << 20)
	t.Logf("%s: %d MiB moved in %s (%.1f MB/s), format=%s",
		label, len(payload)>>20, elapsed.Round(time.Millisecond), mb/elapsed.Seconds(), moved.LastSnapshotFormat)
	return elapsed
}

// TestPlanbPerfMoveIncompressible is the defect reproduction. A workspace
// whose content is entirely new used to be stored as one content-addressed,
// separately authorized and separately fsynced object per 64 KiB chunk, which
// ran at a few MB/s regardless of disk or link speed. The postcondition is
// stated as a floor on throughput rather than a ceiling on seconds, and the
// floor is set an order of magnitude below the measured rate so it fails on
// the defect and not on a loaded machine.
func TestPlanbPerfMoveIncompressible(t *testing.T) {
	n := perfPayloadBytes(t)
	elapsed := perfMove(t, perfIncompressible(t, n), "incompressible")
	if rate := float64(n) / (1 << 20) / elapsed.Seconds(); rate < 20 {
		t.Fatalf("incompressible move ran at %.1f MB/s, want at least 20 MB/s", rate)
	}
}

func TestPlanbPerfMoveCompressible(t *testing.T) {
	n := perfPayloadBytes(t)
	perfMove(t, perfCompressible(n), "compressible")
}

// TestPlanbPerfMoveRatio states the defect the way it was reported: moving
// incompressible bytes cost roughly thirty times what moving the same volume
// of compressible bytes cost. A ratio is machine independent in a way that a
// second count is not.
func TestPlanbPerfMoveRatio(t *testing.T) {
	n := perfPayloadBytes(t)
	incompressible := perfMove(t, perfIncompressible(t, n), "incompressible")
	compressible := perfMove(t, perfCompressible(n), "compressible")
	ratio := float64(incompressible) / float64(compressible)
	t.Logf("incompressible/compressible = %.1fx", ratio)
	if ratio > 5 {
		t.Fatalf("moving incompressible bytes cost %.1fx moving compressible bytes, want at most 5x", ratio)
	}
}

// TestPlanbPerfIncrementalCheckpointStillDeduplicates is the other half of the
// postcondition. The fix does not remove chunk-level deduplication, it stops
// choosing it for trees it would not pay for; a deployment that wants chunking
// for a large tree raises MaxChunkedUnsharedBytes, and this proves that knob
// still buys real deduplication: a second checkpoint of a 48 MiB tree with one
// small edit stays chunked and uploads a small fraction of the tree.
func TestPlanbPerfIncrementalCheckpointStillDeduplicates(t *testing.T) {
	w := newWorld(t)
	n1Dir := t.TempDir()
	n1 := w.nodeWith("perf-dedup-n1", func(o *node.Options) {
		o.DataDir = n1Dir
		o.SnapshotMinInterval = time.Nanosecond
		o.MaxChunkedUnsharedBytes = 1 << 30
	})
	c := w.client("perf-dedup-client")
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	ws := mustWS(t, c, proto.WorkspaceSpec{Placement: proto.Placement{Node: n1.ID()}})

	payload := perfIncompressible(t, 48<<20)
	path := filepath.Join(n1Dir, "ws", ws.ID, "payload.bin")
	if err := os.WriteFile(path, payload, 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Checkpoint(ctx, ws.ID); err != nil {
		t.Fatal(err)
	}
	// One small edit in the middle: content-defined chunking should carry
	// almost every chunk over from the first checkpoint.
	copy(payload[len(payload)/2:], perfIncompressible(t, 4096))
	if err := os.WriteFile(path, payload, 0o600); err != nil {
		t.Fatal(err)
	}
	second, err := c.Checkpoint(ctx, ws.ID)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("incremental checkpoint: format=%s chunks=%d uploaded_chunks=%d uploaded_bytes=%d",
		second.Format, second.Chunks, second.UploadedChunks, second.UploadedBytes)
	if second.Format != proto.ArtifactFormatChunkedV1 {
		t.Fatalf("incremental checkpoint used %q, want %q", second.Format, proto.ArtifactFormatChunkedV1)
	}
	if second.UploadedBytes >= 4<<20 {
		t.Fatalf("incremental checkpoint uploaded %d bytes of a %d byte tree, want deduplication",
			second.UploadedBytes, len(payload))
	}
}

// perfReportPhases attributes the move's wall clock to its checkpoint and
// restore halves using the canonical event log, the same boundaries an
// operator reads from `remount events`.
func perfReportPhases(t *testing.T, ctx context.Context, c *client.Client, ws, label string, since time.Time) {
	t.Helper()
	events, err := c.ReadEvents(ctx, 0, ws)
	if err != nil {
		t.Fatal(err)
	}
	sort.Slice(events, func(i, j int) bool { return events[i].Seq < events[j].Seq })
	// The move is the only lifecycle activity after since, so the first
	// matching event at or after it is the one this move produced.
	at := func(kind string) (time.Time, bool) {
		for _, e := range events {
			when := time.UnixMilli(e.At)
			if e.Type == kind && !when.Before(since.Truncate(time.Millisecond)) {
				return when, true
			}
		}
		return time.Time{}, false
	}
	var chunks, uploaded, uploadedBytes int64
	for _, e := range events {
		if e.Type != proto.EvWSSnapshot || time.UnixMilli(e.At).Before(since.Truncate(time.Millisecond)) {
			continue
		}
		var payload struct {
			Chunks         int64 `cbor:"chunks"`
			UploadedChunks int64 `cbor:"uploaded_chunks"`
			UploadedBytes  int64 `cbor:"uploaded_bytes"`
		}
		if proto.Unmarshal(e.Payload, &payload) != nil {
			continue
		}
		chunks, uploaded, uploadedBytes = payload.Chunks, payload.UploadedChunks, payload.UploadedBytes
	}
	begin, okBegin := at(proto.EvWSStateChanged)
	snapshot, okSnapshot := at(proto.EvWSSnapshot)
	claimed, okClaimed := at(proto.EvWSClaimed)
	if okBegin && okSnapshot {
		t.Logf("%s: checkpoint phase %s", label, snapshot.Sub(begin).Round(time.Millisecond))
	}
	if okSnapshot && okClaimed {
		t.Logf("%s: restore phase %s", label, claimed.Sub(snapshot).Round(time.Millisecond))
	}
	t.Logf("%s: chunks=%d uploaded_chunks=%d uploaded_bytes=%d", label, chunks, uploaded, uploadedBytes)
}
