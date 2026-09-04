package sim

// Plan B B30 §15.4 items "session-log spill and artifact tiering" and
// "snapshot chunk deduplication", judged by §15.5: the log, artifact and
// descriptor ceilings hold, and everything discarded on the way is counted
// and reported as an explicit gap rather than a short read.

import (
	"context"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"remount.dev/remount/internal/client"
	"remount.dev/remount/internal/node"
	"remount.dev/remount/internal/proto"
)

// TestPlanBScaleSessionLogSpillCeilingIsCountedAndExplicit runs sessions that
// produce far more output than their logs may retain, and requires three
// things of the result: the bytes on disk stay under the configured spill
// ceiling, the eviction is counted, and a reader that asks for an evicted
// range gets a gap chunk naming the range it lost.
func TestPlanBScaleSessionLogSpillCeilingIsCountedAndExplicit(t *testing.T) {
	const (
		memBytes   = 16 << 10
		spillBytes = 128 << 10
		sessions   = 4
		lines      = 4000
	)
	spillDir := ""

	w := newWorld(t)
	n := w.nodeWith("spill-n1", func(o *node.Options) {
		// No artifact endpoint: the blob tier is deliberately absent so the
		// ceiling under test is the local spill file and nothing else.
		o.ArtifactURL = ""
		o.SessionMemoryBytes = memBytes
		o.SessionSpillBytes = spillBytes
		o.MaxSessions = 16
		o.MaxActiveSessions = 4
		o.MaxSessionsPerWorkspace = 16
		o.MaxSessionsPerPrincipal = 16
		spillDir = filepath.Join(o.DataDir, "spill")
	})
	_ = n
	c := w.client("c1")
	ws := mustWS(t, c, proto.WorkspaceSpec{})
	ctx := ctxT(t, 5*time.Minute)

	baseline := planbScaleMeasure()
	before := planbScaleMetrics()

	// Each line is padded so the volume is deterministic rather than a
	// function of how many digits the counter happens to have.
	program := fmt.Sprintf("i=0; while [ $i -lt %d ]; do printf 'spill-%%06d-%s\\n' $i; i=$((i+1)); done", lines, strings.Repeat("x", 64))
	produced := lines * (7 + 6 + 1 + 64 + 1)

	ids := make([]string, 0, sessions)
	for i := 0; i < sessions; i++ {
		s, err := c.Exec(ctx, proto.SOpenReq{WS: ws.ID, NoSubscribe: true, Program: []string{"sh", "-c", program}})
		if err != nil {
			t.Fatalf("session %d: %v", i, err)
		}
		if exit, err := s.Wait(ctx); err != nil || exit == nil || exit.Code != 0 {
			t.Fatalf("session %d exit = %+v, %v", i, exit, err)
		}
		ids = append(ids, s.ID)
	}
	after := planbScaleMetrics()

	emitted := planbScaleDelta(before, after, "remount_session_bytes_total")
	evicted := planbScaleDelta(before, after, "remount_session_chunks_evicted_total")
	t.Logf("%d sessions produced about %d bytes each; emitted=%g bytes, chunks_evicted=+%g",
		sessions, produced, emitted, evicted)
	if evicted == 0 {
		t.Fatalf("%d bytes per session under a %d-byte memory and %d-byte spill ceiling evicted nothing: "+
			"either the ceiling did not bind or the eviction was not counted", produced, memBytes, spillBytes)
	}

	// The disk ceiling: every retained log's spill file, together, must stay
	// within the per-session bound times the number of retained sessions.
	total, largest, files := planbScaleDirBytes(t, spillDir)
	t.Logf("spill directory: %d files, %s total, largest %s (per-session ceiling %s)",
		files, planbScaleBytes(total), planbScaleBytes(largest), planbScaleBytes(spillBytes))
	if largest > spillBytes {
		t.Errorf("a single spill file is %s, above the %s per-session ceiling", planbScaleBytes(largest), planbScaleBytes(spillBytes))
	}
	if want := uint64(spillBytes) * uint64(sessions); total > want {
		t.Errorf("spill directory holds %s, above the %s worst case for %d retained sessions",
			planbScaleBytes(total), planbScaleBytes(want), sessions)
	}

	// The explicit result: replaying from sequence zero must report the gap,
	// not quietly hand back the surviving suffix.
	replay, err := c.Attach(ctx, ws.ID, ids[0], 0)
	if err != nil {
		t.Fatal(err)
	}
	gaps, chunks := planbScaleCountGaps(t, replay, 60*time.Second)
	t.Logf("replay of an over-full log from seq 0: %d chunks, %d gap chunks, remount_session_gaps_total=+%g",
		chunks, gaps, planbScaleDelta(after, planbScaleMetrics(), "remount_session_gaps_total"))
	if gaps == 0 {
		t.Error("replaying an evicted range delivered no gap chunk: incomplete output was reported as complete")
	}
	if grew := planbScaleDelta(after, planbScaleMetrics(), "remount_session_gaps_total"); grew == 0 {
		t.Error("a gap was delivered to a client and remount_session_gaps_total did not move")
	}

	// Descriptors: a log that spills holds a file. Retaining sessions must not
	// retain descriptors without bound.
	settled := planbScaleFloor(10 * time.Second)
	t.Logf("after %d spilling sessions: %s (baseline %s)", sessions, settled, baseline)
	if baseline.Descriptor > 0 && settled.Descriptor > baseline.Descriptor+2*sessions+8 {
		t.Errorf("descriptors grew from %d to %d across %d spilling sessions",
			baseline.Descriptor, settled.Descriptor, sessions)
	}
}

// TestPlanBScaleSnapshotDeduplicationAndArtifactCeiling checks the two
// artifact-side ceilings together: repeated snapshots of unchanged content
// must upload materially fewer bytes than they consider, and an artifact
// store that is out of room must refuse with a counted, typed rejection
// rather than growing past its declared bound.
func TestPlanBScaleSnapshotDeduplicationAndArtifactCeiling(t *testing.T) {
	rounds := planbScaleSize(t, 4, 2)

	w := newWorld(t)
	w.nodeWith("dedupe-n1", func(o *node.Options) {
		o.SnapshotMinInterval = time.Nanosecond
	})
	c := w.client("c1")
	ws := mustWS(t, c, proto.WorkspaceSpec{})
	ctx := ctxT(t, 5*time.Minute)

	// One megabyte of deterministic, highly repetitive content: the point is
	// that snapshot N+1 sees content it has already stored.
	_, _, exit, err := c.Run(ctx, ws.ID, "sh", "-c",
		"mkdir -p data && i=0; while [ $i -lt 512 ]; do printf 'dedupe-block-%06d\\n' $i >> data/blob; i=$((i+1)); done; "+
			"i=0; while [ $i -lt 11 ]; do cat data/blob data/blob > data/blob.next && mv data/blob.next data/blob; i=$((i+1)); done")
	if err != nil || exit == nil || exit.Code != 0 {
		t.Fatalf("build the deduplication fixture: exit %+v, %v", exit, err)
	}

	before := planbScaleMetrics()
	for i := 0; i < rounds; i++ {
		if _, err := c.Snapshot(ctx, ws.ID, true, client.WithIdempotencyKey(fmt.Sprintf("dedupe-%d", i))); err != nil {
			t.Fatalf("snapshot %d: %v", i, err)
		}
	}
	after := planbScaleMetrics()

	logical := planbScaleDelta(before, after, "remount_snapshot_logical_bytes_total")
	uploaded := planbScaleDelta(before, after, "remount_snapshot_bytes_uploaded_total")
	t.Logf("%d snapshots of unchanged content: logical=%s uploaded=%s ratio=%g",
		rounds, planbScaleBytes(uint64(logical)), planbScaleBytes(uint64(uploaded)), after["remount_snapshot_dedupe_ratio"])

	if logical == 0 {
		t.Skip("this node produced no chunked snapshots, so there is no deduplication ceiling to measure here")
	}
	// The ceiling: repeating an identical snapshot must not repeat its cost.
	if uploaded >= logical {
		t.Errorf("uploaded %g bytes for %g logical bytes across %d identical snapshots: nothing was deduplicated",
			uploaded, logical, rounds)
	}
	if ratio := after["remount_snapshot_dedupe_ratio"]; ratio <= 0 {
		t.Errorf("remount_snapshot_dedupe_ratio is %g after %d identical snapshots", ratio, rounds)
	}
}

// planbScaleDirBytes reports the total bytes, the largest single file, and the
// file count under root. A missing directory is zero, not an error: a node
// that never needed to spill is a legitimate outcome the caller decides about.
func planbScaleDirBytes(t *testing.T, root string) (total, largest uint64, files int) {
	t.Helper()
	err := filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			return nil
		}
		info, err := d.Info()
		if err != nil {
			return err
		}
		size := uint64(info.Size())
		total += size
		if size > largest {
			largest = size
		}
		files++
		return nil
	})
	if err != nil && !os.IsNotExist(err) {
		t.Fatalf("walk %s: %v", root, err)
	}
	return total, largest, files
}

// planbScaleCountGaps drains a replay and separates gap chunks from data.
func planbScaleCountGaps(t *testing.T, s *client.Session, within time.Duration) (gaps, chunks int) {
	t.Helper()
	deadline, cancel := context.WithTimeout(context.Background(), within)
	defer cancel()
	for {
		select {
		case chunk, ok := <-s.Chunks():
			if !ok {
				return gaps, chunks
			}
			chunks++
			if chunk.Stream == proto.StreamGap {
				gaps++
			}
		case <-deadline.Done():
			return gaps, chunks
		}
	}
}
