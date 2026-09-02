package metrics

import (
	"strings"
	"sync"
	"testing"
)

func TestCounterAndGauge(t *testing.T) {
	r := New()
	c := r.Counter("thing_total", "things")
	c.Inc()
	c.Add(4)
	if c.Value() != 5 {
		t.Fatal(c.Value())
	}
	// The same name returns the same counter, so call sites need not share a var.
	if r.Counter("thing_total", "") != c {
		t.Fatal("counter not deduplicated by name")
	}
	g := r.Gauge("level", "a level")
	g.Set(10)
	g.Add(-3)
	if g.Value() != 7 {
		t.Fatal(g.Value())
	}
	r.GaugeFunc("computed", "computed on scrape", func() float64 { return 42 })
	snap := r.Snapshot()
	if snap["thing_total"] != 5 || snap["level"] != 7 || snap["computed"] != 42 {
		t.Fatalf("%v", snap)
	}
}

func TestPrometheusRendering(t *testing.T) {
	r := New()
	r.Counter("b_total", "second").Add(2)
	r.Counter("a_total", "first").Add(1)
	r.Gauge("z_gauge", "third").Set(-5)
	var sb strings.Builder
	r.Write(&sb)
	out := sb.String()
	// Sorted, so a diff between two scrapes is readable.
	if strings.Index(out, "a_total") > strings.Index(out, "b_total") {
		t.Fatalf("not sorted:\n%s", out)
	}
	for _, want := range []string{
		"# HELP a_total first",
		"# TYPE a_total counter",
		"a_total 1",
		"# TYPE z_gauge gauge",
		"z_gauge -5",
	} {
		if !strings.Contains(out, want) {
			t.Fatalf("missing %q in:\n%s", want, out)
		}
	}
}

func TestConcurrentUse(t *testing.T) {
	r := New()
	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				r.Counter("hot_total", "contended").Inc()
				r.Gauge("hot_gauge", "contended").Add(1)
			}
		}()
	}
	wg.Wait()
	if got := r.Counter("hot_total", "").Value(); got != 5000 {
		t.Fatalf("lost increments: %d", got)
	}
	if got := r.Gauge("hot_gauge", "").Value(); got != 5000 {
		t.Fatalf("lost adds: %d", got)
	}
}

// The named metrics are the ones the docs and the doctor refer to, so a rename
// should break a test rather than silently produce an empty dashboard.
func TestDocumentedMetricNamesExist(t *testing.T) {
	snap := Default.Snapshot()
	for _, name := range []string{
		"remount_egress_leak_blocked_total",
		"remount_credentials_substituted_total",
		"remount_session_gaps_total",
		"remount_artifact_digest_mismatch_total",
		"remount_workspace_lease_expired_total",
		"remount_session_inputs_deduped_total",
		"remount_frames_dropped_total",
		"remount_sessions_opened_total",
		"remount_snapshots_total",
		"remount_restores_total",
	} {
		if _, ok := snap[name]; !ok {
			t.Errorf("metric %s is referenced in docs and tooling but not registered", name)
		}
	}
}
