package sim

// Plan B B30 (§15.4/§15.5) shared measurement support.
//
// B30 is not a throughput benchmark. Its acceptance property is that every
// ceiling the system claims is real: memory, goroutines, descriptors,
// artifacts, logs, events and retained control state stay bounded while a
// deterministic workload is driven to steady state twice, and every unit that
// is dropped or rejected on the way moves a counter in internal/metrics and
// produces an explicit result for the caller.
//
// The measurement discipline these helpers encode:
//
//   - A leak is monotonic growth across identical cycles, not a large
//     absolute number. Every test here drives the workload at least twice and
//     compares cycle N+1 against cycle N, so the one-time cost of building a
//     world, opening a SQLite file or warming a pool is charged to cycle 1 and
//     never mistaken for a leak.
//   - Goroutines and heap wind down asynchronously. Reading NumGoroutine once
//     after a cycle measures the scheduler, not the program, so growth
//     assertions poll to a settled floor before they conclude anything.
//   - metrics.Default is process-global and shared by every test in this
//     binary, so counter assertions are always deltas taken around the work
//     under test and the tests never run in parallel with each other.

import (
	"fmt"
	"math/rand"
	"os"
	"runtime"
	"strconv"
	"testing"
	"time"

	"remount.dev/remount/internal/metrics"
)

// planbScaleSample is one resource-ceiling observation.
type planbScaleSample struct {
	Goroutines int
	HeapBytes  uint64
	HeapInUse  uint64
	Descriptor int // -1 when this platform exposes no descriptor directory
}

func (s planbScaleSample) String() string {
	return fmt.Sprintf("goroutines=%d heap_alloc=%s heap_inuse=%s fds=%d",
		s.Goroutines, planbScaleBytes(s.HeapBytes), planbScaleBytes(s.HeapInUse), s.Descriptor)
}

func planbScaleBytes(n uint64) string {
	switch {
	case n >= 1<<20:
		return fmt.Sprintf("%.1fMiB", float64(n)/(1<<20))
	case n >= 1<<10:
		return fmt.Sprintf("%.1fKiB", float64(n)/(1<<10))
	default:
		return fmt.Sprintf("%dB", n)
	}
}

// planbScaleMeasure forces two collections before reading the heap. One GC
// can leave the just-freed cycle's finalizable objects alive, which reads as
// growth that a second pass does not show.
func planbScaleMeasure() planbScaleSample {
	runtime.GC()
	runtime.GC()
	var stats runtime.MemStats
	runtime.ReadMemStats(&stats)
	return planbScaleSample{
		Goroutines: runtime.NumGoroutine(),
		HeapBytes:  stats.HeapAlloc,
		HeapInUse:  stats.HeapInuse,
		Descriptor: planbScaleOpenDescriptors(),
	}
}

// planbScaleOpenDescriptors counts this process's open descriptors, or -1
// where the platform exposes no descriptor directory. The count includes the
// directory handle the walk itself holds, which is a constant, so it is only
// ever compared against another reading taken the same way.
func planbScaleOpenDescriptors() int {
	for _, dir := range []string{"/proc/self/fd", "/dev/fd"} {
		entries, err := os.ReadDir(dir)
		if err != nil {
			continue
		}
		return len(entries)
	}
	return -1
}

// planbScaleFloor waits until the goroutine count stops falling and returns
// that reading. Used when there is no known limit to wait for, only the
// question of where this cycle came to rest.
func planbScaleFloor(within time.Duration) planbScaleSample {
	deadline := time.Now().Add(within)
	best := planbScaleMeasure()
	stable := 0
	for time.Now().Before(deadline) && stable < 5 {
		time.Sleep(20 * time.Millisecond)
		next := planbScaleMeasure()
		if next.Goroutines < best.Goroutines {
			stable = 0
		} else {
			stable++
		}
		if next.Goroutines < best.Goroutines {
			best = next
		} else {
			best.HeapBytes, best.HeapInUse, best.Descriptor = next.HeapBytes, next.HeapInUse, next.Descriptor
			if next.Goroutines < best.Goroutines {
				best.Goroutines = next.Goroutines
			}
		}
	}
	return best
}

// planbScaleMetrics snapshots the process-global registry. Counters are
// shared by every test in the binary, so B30 only ever asserts on deltas
// taken around the work under test.
func planbScaleMetrics() map[string]float64 { return metrics.Default.Snapshot() }

// planbScaleDelta reports how much a named counter moved between two
// snapshots. A missing name is zero: an unregistered counter and an
// unmoved one are the same evidence, namely that nothing counted the unit.
func planbScaleDelta(before, after map[string]float64, name string) float64 {
	return after[name] - before[name]
}

// planbScaleCountedRejections fails unless the named counter moved by exactly
// want. This is the §15.5 property in one call: a rejection nobody counted is
// indistinguishable from a silent drop.
func planbScaleCountedRejections(t *testing.T, before, after map[string]float64, name string, want int) {
	t.Helper()
	got := planbScaleDelta(before, after, name)
	if got != float64(want) {
		t.Errorf("%s moved by %g, want %d: every rejected unit needs a metric", name, got, want)
	}
}

// planbScaleSeed returns the deterministic seed for a randomized workload.
// REMOUNT_SCALE_SEED overrides it so a failure can be replayed exactly; the
// value in use is always logged, failure or not.
func planbScaleSeed(t *testing.T) int64 {
	t.Helper()
	seed := int64(0x5ca1e)
	if raw := os.Getenv("REMOUNT_SCALE_SEED"); raw != "" {
		parsed, err := strconv.ParseInt(raw, 10, 64)
		if err != nil {
			t.Fatalf("REMOUNT_SCALE_SEED=%q: %v", raw, err)
		}
		seed = parsed
	}
	t.Logf("scale seed %d (set REMOUNT_SCALE_SEED=%d to replay)", seed, seed)
	return seed
}

// planbScaleRand is the only source of randomness in these tests.
func planbScaleRand(t *testing.T) *rand.Rand { return rand.New(rand.NewSource(planbScaleSeed(t))) }

// planbScaleSize picks a workload width. REMOUNT_SCALE_N overrides both, so
// the laptop-sized default that runs in CI and the larger run used to check
// that the ceiling is width-independent are the same code path.
func planbScaleSize(t *testing.T, full, short int) int {
	t.Helper()
	if raw := os.Getenv("REMOUNT_SCALE_N"); raw != "" {
		parsed, err := strconv.Atoi(raw)
		if err != nil || parsed <= 0 {
			t.Fatalf("REMOUNT_SCALE_N=%q: want a positive integer", raw)
		}
		return parsed
	}
	if testing.Short() {
		return short
	}
	return full
}

func planbScaleSizeFromEnv(t *testing.T, name string, full, short int) int {
	t.Helper()
	if raw := os.Getenv(name); raw != "" {
		parsed, err := strconv.Atoi(raw)
		if err != nil || parsed <= 0 {
			t.Fatalf("%s=%q: want a positive integer", name, raw)
		}
		return parsed
	}
	return planbScaleSize(t, full, short)
}

// planbScaleGrowth reports the fractional growth of b over a, and 0 when a is
// zero and b is not positive.
func planbScaleGrowth(a, b uint64) float64 {
	if a == 0 {
		if b == 0 {
			return 0
		}
		return 1
	}
	return float64(b)/float64(a) - 1
}
