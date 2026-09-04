package sim

// The 2026-09 regression sweep (docs/engineering/performance-regressions-2026-09.md)
// left one row open — exec round trip, 0.645 s baseline against 3.1 s at
// a3b5235 — and named the lane that would have caught it: "a single-session
// exec round trip with an artifact URL configured, which is what makes the
// session-log upload path live". This is that lane.
//
// It exists to attribute cost, not to assert a wall-clock budget. A timing
// threshold on a shared laptop is a flaky test; a *ratio* between two
// configurations measured back to back on the same host, in the same process,
// is stable enough to mean something. So the measurement is:
//
//	exec round trip with the artifact tier live  ÷  the same exec without it
//
// Everything else is held equal. What that ratio isolates is the per-session
// cost of sealing a completed log into an immutable artifact, which spec
// §8.2 requires of any node advertising tiered-session-logs so that an exited
// session replays byte-exactly after node loss.

import (
	"sort"
	"testing"
	"time"

	"remount.dev/remount/internal/node"
	"remount.dev/remount/internal/proto"
)

// execSealSamples is deliberately small. Each sample forks a shell, and this
// host serializes fork/exec process-wide, so raising it measures the fork lock
// rather than the seal.
const execSealSamples = 24

// measureExecRoundTrip runs execSealSamples sequential trivial execs against a
// fresh world and returns their median. Sequential on purpose: concurrency
// here would measure syscall.ForkLock, which Go takes process-wide around
// fork/exec. That lock is why the 200-way lane cannot go below ~440 ms on this
// host, and it is a property of running 200 nodes inside one test process
// rather than a property of the product.
func measureExecRoundTrip(t *testing.T, artifactTier bool) time.Duration {
	t.Helper()
	w := newWorld(t)
	name := "seal-off"
	if artifactTier {
		name = "seal-on"
	}
	w.nodeWith(name, func(o *node.Options) {
		if !artifactTier {
			// The node keeps its ring and spill; it simply stops advertising a
			// durable tier, which is the one variable under test.
			o.ArtifactURL = ""
		}
	})
	c := w.client("c-" + name)
	ws := mustWS(t, c, proto.WorkspaceSpec{})
	ctx := ctxT(t, 5*time.Minute)

	// One warm exec so image pull, node claim and process backend start-up are
	// not attributed to the first sample.
	if _, _, exit, err := c.Run(ctx, ws.ID, "sh", "-c", "printf warm"); err != nil || exit == nil || exit.Code != 0 {
		t.Fatalf("warm exec: exit=%+v err=%v", exit, err)
	}

	lat := make([]time.Duration, execSealSamples)
	for i := range lat {
		started := time.Now()
		stdout, _, exit, err := c.Run(ctx, ws.ID, "sh", "-c", "printf 'scale-exec'")
		lat[i] = time.Since(started)
		if err != nil || exit == nil || exit.Code != 0 || string(stdout) != "scale-exec" {
			t.Fatalf("exec %d: stdout=%q exit=%+v err=%v", i, stdout, exit, err)
		}
	}
	sort.Slice(lat, func(a, b int) bool { return lat[a] < lat[b] })
	return lat[len(lat)/2]
}

// TestExecRoundTripCostOfTheDurableSessionTier reports what the durable
// session-log tier costs a single exec, and fails if that cost returns to the
// level that made the 200-way lane 4.8x slower at a3b5235.
//
// The bound is a ratio, because the absolute number is a property of the host.
// It is deliberately loose. Sealing a ~195-byte segment is legitimately not
// free: it writes the segment to the node's own artifact store (two fsyncs),
// uploads it to the control plane's store (two more), and commits a record —
// about 15 ms on this host, against an 11 ms exec. So the honest measured
// ratio is near 2.7x and a 3x gate would flake on a loaded machine. 5x is set
// where it still catches the regression it exists for while leaving room for
// a noisy host. The ratio is logged unconditionally, so drift stays visible
// in the run output long before it trips the bound; docs/benchmarks.md
// records the value observed when this lane was written.
func TestExecRoundTripCostOfTheDurableSessionTier(t *testing.T) {
	if testing.Short() {
		t.Skip("unavailable: this lane runs two full sim worlds and many execs; skipped under -short")
	}
	// Each configuration gets its own fresh world, so neither inherits the
	// other's caches. Measuring the cheap one first biases against the finding
	// rather than towards it, since a machine that gets busier during the run
	// inflates the second number.
	off := measureExecRoundTrip(t, false)
	on := measureExecRoundTrip(t, true)

	ratio := float64(on) / float64(off)
	t.Logf("exec round trip p50: artifact tier off %v, on %v, ratio %.2fx (n=%d each)",
		off.Round(time.Millisecond), on.Round(time.Millisecond), ratio, execSealSamples)

	// The lower bound is what keeps this lane honest. If a change ever stopped
	// the node from sealing — a dropped capability, an ArtifactURL that no
	// longer reaches the control plane, a silently disabled tier — then "on"
	// would behave exactly like "off", the ratio would fall to about 1.0, and
	// an upper bound alone would report that as a fine result. A lane that
	// passes because the work it measures never ran is the failure this
	// repository calls skipped-success, so it is asserted against directly.
	if ratio < 1.2 {
		t.Errorf("the artifact tier cost nothing (%.2fx: %v -> %v): the node is almost certainly "+
			"not sealing session logs at all, so this lane is measuring two identical "+
			"configurations and cannot detect the regression it exists for", ratio, off, on)
	}
	if ratio > 5.0 {
		t.Errorf("the durable session-log tier makes a trivial exec %.2fx slower (%v -> %v): "+
			"every session close writes a segment to the node's artifact store, uploads it to the "+
			"control plane's store and commits a record, all on the caller's critical path, so exec "+
			"latency is bounded by fsyncs and control round trips rather than by the work the "+
			"session did", ratio, off, on)
	}
}
