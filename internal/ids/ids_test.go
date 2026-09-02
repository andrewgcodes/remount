package ids

import (
	"sort"
	"testing"
	"time"
)

func TestNewSortsByTime(t *testing.T) {
	a := New("ws")
	time.Sleep(2 * time.Millisecond)
	b := New("ws")
	if !(a < b) {
		t.Fatalf("ids not time-ordered: %s %s", a, b)
	}
	if Prefix(a) != "ws" {
		t.Fatalf("prefix: %s", Prefix(a))
	}
	seen := map[string]bool{}
	var all []string
	for i := 0; i < 1000; i++ {
		id := New("s")
		if seen[id] {
			t.Fatal("duplicate id")
		}
		seen[id] = true
		all = append(all, id)
	}
	if !sort.StringsAreSorted(all) {
		// same-millisecond ids are random-ordered; only assert no dupes
		t.Log("ids within one ms are not sorted (expected)")
	}
}
