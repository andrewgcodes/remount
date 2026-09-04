package sim

// Plan B B14: cut the client mid-command; the reattach is byte-identical or
// carries an explicit gap.
//
// TestReconnectMidStreamIsLossless already proves one fixed cut schedule is
// lossless. This lane adds the two things §11 asks for that it does not have:
// bounded repeated runs over deterministic seeds (§11.4), so the losslessness
// claim is not a claim about one arbitrary cut pattern; and the other half of
// the B14 disjunction, where the cut lasts long enough for the node's log to
// rotate past the client's cursor and the reattach must say so instead of
// handing back a shorter stream as if it were complete.

import (
	"bytes"
	"context"
	"fmt"
	"math/rand/v2"
	"strings"
	"testing"
	"time"

	"remount.dev/remount/internal/client"
	"remount.dev/remount/internal/node"
	"remount.dev/remount/internal/proto"
)

const (
	// planBCutSeeds bounds the repeated chaos run. Each seed is a distinct
	// cut schedule replayed exactly by rerunning with the printed seed.
	planBCutSeeds = 4
	// planBCutLines is one seeded producer's output. Small enough that six
	// runs stay fast under -race, long enough that a cut lands mid-stream.
	planBCutLines = 1500
)

func TestPlanBContinuityClientCutReattachIsLosslessOrExplicitlyGapped(t *testing.T) {
	t.Run("seeded-cuts-stay-byte-identical", planBContinuitySeededCutsAreLossless)
	t.Run("stale-cursor-is-an-explicit-gap", planBContinuityStaleCursorIsAGap)
}

// planBContinuitySeededCutsAreLossless replays several deterministic cut
// schedules against one node. The invariant under test is the whole of B14's
// first branch: the bytes the client ends up with are exactly the bytes the
// process produced, the sequence never goes backwards, and no gap was needed.
func planBContinuitySeededCutsAreLossless(t *testing.T) {
	w := newWorld(t)
	w.node("b14-n1", nil)
	c := w.client("b14-c1")
	ctx := ctxT(t, 5*time.Minute)
	ws := mustWS(t, c, proto.WorkspaceSpec{Name: "b14-lossless"})

	var want bytes.Buffer
	for i := 0; i < planBCutLines; i++ {
		fmt.Fprintf(&want, "line-%06d\n", i)
	}

	for seed := uint64(1); seed <= planBCutSeeds; seed++ {
		// The seed is the whole reproduction recipe, so it is named in every
		// failure below: rerun with only that seed to replay the schedule.
		schedule := planBCutSchedule(seed, want.Len())
		s, err := c.Exec(ctx, proto.SOpenReq{
			WS: ws.ID, Kind: proto.SessionExec,
			Program: []string{"sh", "-c", fmt.Sprintf(
				"i=0; while [ $i -lt %d ]; do printf 'line-%%06d\\n' $i; i=$((i+1)); if [ $((i %% 150)) -eq 0 ]; then sleep 0.02; fi; done", planBCutLines)},
		})
		if err != nil {
			t.Fatalf("seed=%d exec: %v", seed, err)
		}
		var got bytes.Buffer
		var seqs []uint64
		next := 0
		for ch := range s.Chunks() {
			switch ch.Stream {
			case proto.StreamStdout:
				got.Write(ch.Data)
				seqs = append(seqs, ch.Seq)
			case proto.StreamGap:
				t.Fatalf("seed=%d: a live reconnect reported a gap at seq %d; the node log is far larger than this stream", seed, ch.Seq)
			}
			// One cut per chunk. Firing two at once would sever a connection
			// the client has not had a chance to rebuild, which tests the
			// harness rather than the reconnect.
			if next < len(schedule) && got.Len() >= schedule[next] {
				planBCut(t, w, "b14-c1", seed, next, schedule[next])
				next++
			}
		}
		if next == 0 {
			t.Fatalf("seed=%d: the stream finished before any cut landed (schedule %v)", seed, schedule)
		}
		if got.String() != want.String() {
			t.Fatalf("seed=%d: output differs after %d cuts (schedule %v): got %d bytes, want %d",
				seed, next, schedule, got.Len(), want.Len())
		}
		for i := 1; i < len(seqs); i++ {
			if seqs[i] <= seqs[i-1] {
				t.Fatalf("seed=%d: session cursor went backwards at %d: seq %d after %d", seed, i, seqs[i], seqs[i-1])
			}
		}
		if exit := s.Exit(); exit == nil || exit.Code != 0 {
			t.Fatalf("seed=%d: exit = %+v", seed, exit)
		}
		if last := seqs[len(seqs)-1]; s.Next() <= last {
			t.Fatalf("seed=%d: cursor %d does not follow the last delivered seq %d", seed, s.Next(), last)
		}
	}
}

// planBCutSchedule turns one seed into an ascending list of byte offsets at
// which the client's connection is severed. Cuts stay in the first three
// quarters so every one of them lands while the process is still producing,
// and each is drawn from its own band so a seed can vary where a cut lands
// without ever asking for two the client has no chance to reconnect between.
func planBCutSchedule(seed uint64, total int) []int {
	rng := rand.New(rand.NewPCG(seed, 0x9E3779B97F4A7C15))
	cuts := 1 + rng.IntN(3)
	band := total * 3 / 4 / cuts
	out := make([]int, cuts)
	for i := range out {
		out[i] = i*band + 1 + rng.IntN(band)
	}
	return out
}

// planBCut severs the client's connection and insists that it severed
// something. The client rebuilds its connection on its own after an earlier
// cut, so this waits for one to exist rather than reporting a fault it never
// injected.
func planBCut(t *testing.T, w *world, who string, seed uint64, index, at int) {
	t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	for {
		if w.cut(who) > 0 {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("seed=%d: cut %d at %d bytes never found a connection to sever", seed, index, at)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// planBContinuityStaleCursorIsAGap proves B14's second branch. The node runs
// with no artifact endpoint, so its session log has no blob tier and a full
// spill file is dropped rather than sealed: the exact production shape in
// which replay from an old cursor is genuinely impossible. The reattach must
// then be an explicit, bounded gap — never a shorter stream presented as the
// whole one.
func planBContinuityStaleCursorIsAGap(t *testing.T) {
	w := newWorld(t)
	w.nodeWith("b14-gap-n1", func(o *node.Options) {
		// Without an artifact endpoint tiered session logs are unavailable,
		// which is what makes eviction observable rather than transparent.
		o.ArtifactURL = ""
		o.SessionMemoryBytes = 8 << 10
		o.SessionSpillBytes = 32 << 10
		o.SessionMaxChunkBytes = 4 << 10
	})
	c := w.client("b14-gap-c1")
	ctx := ctxT(t, 3*time.Minute)
	ws := mustWS(t, c, proto.WorkspaceSpec{Name: "b14-gap"})

	const lines = 8000
	var flood bytes.Buffer
	for i := 0; i < lines; i++ {
		fmt.Fprintf(&flood, "line-%d\n", i)
	}
	// The producer waits for a file rather than for wall-clock time, so the
	// cut is guaranteed to precede every byte the test cares about.
	s, err := c.Exec(ctx, proto.SOpenReq{
		WS: ws.ID, Kind: proto.SessionExec,
		Program: []string{"sh", "-c", fmt.Sprintf(
			"echo ready; while [ ! -f go.txt ]; do sleep 0.05; done; i=0; while [ $i -lt %d ]; do echo \"line-$i\"; i=$((i+1)); done", lines)},
	})
	if err != nil {
		t.Fatal(err)
	}
	var prefix bytes.Buffer
	ready := false
	for ch := range s.Chunks() {
		if ch.Stream == proto.StreamStdout {
			prefix.Write(ch.Data)
		}
		if strings.Contains(prefix.String(), "ready\n") {
			ready = true
			break
		}
	}
	if !ready {
		t.Fatalf("the producer never parked before the flood: %q err=%v", prefix.String(), s.Err())
	}
	cursor := s.Next()

	// The fault: the client's connection is severed mid-command and it stops
	// following the stream. The process keeps running on the node.
	if severed := w.cut("b14-gap-c1"); severed == 0 {
		t.Fatal("no client connection to cut")
	}
	if err := s.Close(ctx, false); err != nil {
		t.Fatalf("detach after the cut: %v", err)
	}

	// The process runs to completion with nobody following it, so the cursor
	// the client kept is overrun by the node's bounded log.
	if err := c.WriteFile(ctx, ws.ID, "go.txt", []byte("go"), 0); err != nil {
		t.Fatal(err)
	}
	var status proto.SessionStatus
	for deadline := time.Now().Add(90 * time.Second); ; {
		status = planBSessionStatus(t, ctx, c, ws.ID, s.ID)
		if status.Exited {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("session never exited: next=%d oldest=%d", status.Next, status.Oldest)
		}
		time.Sleep(100 * time.Millisecond)
	}
	// Prove the precondition before asserting the consequence: a log that
	// never rotated cannot demonstrate anything about rotation.
	if status.Oldest <= cursor {
		t.Fatalf("session log never rotated past cursor %d: oldest=%d next=%d", cursor, status.Oldest, status.Next)
	}

	stale, err := c.Attach(ctx, ws.ID, s.ID, cursor)
	if err != nil {
		t.Fatalf("reattach at the stale cursor %d: %v", cursor, err)
	}
	var tail bytes.Buffer
	var gap proto.Gap
	gapped := false
	for ch := range stale.Chunks() {
		switch ch.Stream {
		case proto.StreamGap:
			if gapped {
				t.Fatalf("a single rotation produced more than one gap: %+v", gap)
			}
			if err := proto.Unmarshal(ch.Data, &gap); err != nil {
				t.Fatalf("decode gap: %v", err)
			}
			if ch.Seq != cursor {
				t.Fatalf("gap chunk carries seq %d, want the requested cursor %d", ch.Seq, cursor)
			}
			gapped = true
		case proto.StreamStdout:
			if !gapped {
				t.Fatalf("stale reattach delivered output at seq %d before naming the gap", ch.Seq)
			}
			tail.Write(ch.Data)
		}
	}
	if !gapped {
		t.Fatalf("stale reattach reported no gap: oldest=%d cursor=%d", status.Oldest, cursor)
	}
	if gap.From != cursor || gap.To != status.Oldest-1 {
		t.Fatalf("gap %+v does not name the elided range [%d,%d]", gap, cursor, status.Oldest-1)
	}
	// The replay is honestly incomplete: what arrives is a suffix of the true
	// output, and it is shorter than the whole of it.
	if tail.Len() >= flood.Len() {
		t.Fatalf("stale replay returned %d bytes but only %d were still retained", tail.Len(), flood.Len())
	}
	if !strings.HasSuffix(flood.String(), tail.String()) {
		t.Fatal("stale replay is not a suffix of the produced output")
	}

	// The public helper renders the elision rather than concatenating around
	// it, so a caller cannot mistake the suffix for the whole stream.
	marked, err := c.Attach(ctx, ws.ID, s.ID, cursor)
	if err != nil {
		t.Fatal(err)
	}
	var out, errBuf bytes.Buffer
	client.Copy(marked, &out, &errBuf)
	want := fmt.Sprintf("[remount: output seq %d-%d elided]", gap.From, gap.To)
	if !strings.Contains(errBuf.String(), want) {
		t.Fatalf("Copy did not render the gap: stderr=%q want %q", errBuf.String(), want)
	}
}

// planBSessionStatus returns the node's own record of one session's cursor
// bounds: the authoritative answer to "which bytes still exist".
func planBSessionStatus(t *testing.T, ctx context.Context, c *client.Client, ws, session string) proto.SessionStatus {
	t.Helper()
	statuses, err := c.ListSessions(ctx, ws)
	if err != nil {
		t.Fatal(err)
	}
	for _, status := range statuses {
		if status.Info.ID == session {
			return status
		}
	}
	t.Fatalf("session %s is not listed on %s", session, ws)
	return proto.SessionStatus{}
}
