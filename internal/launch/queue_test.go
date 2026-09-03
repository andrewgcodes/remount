package launch

import (
	"strings"
	"testing"
	"time"
)

func TestParseQueueFileDropsCommentsAndJoinsContinuations(t *testing.T) {
	tasks, err := ParseQueueFile(strings.NewReader("# a plan\n\n  first task  \nsecond \\\nline two \\\nline three\n# trailing\nlast \\"))
	if err != nil {
		t.Fatal(err)
	}
	want := []string{"first task", "second \nline two \nline three", "last"}
	if len(tasks) != len(want) {
		t.Fatalf("tasks = %q", tasks)
	}
	for i := range want {
		if tasks[i] != want[i] {
			t.Fatalf("task %d = %q, want %q", i, tasks[i], want[i])
		}
	}
	if _, err := ParseQueueFile(strings.NewReader("# only comments\n\n")); err == nil {
		t.Fatal("empty queue accepted")
	}
	if _, err := ParseQueueFile(strings.NewReader(strings.Repeat("x", 17<<10))); err == nil {
		t.Fatal("oversized task accepted")
	}
	if _, err := ParseQueueFile(strings.NewReader(strings.Repeat("t\n", 257))); err == nil {
		t.Fatal("257 tasks accepted")
	}
}

func TestNextWallClockIsStrictlyInTheFuture(t *testing.T) {
	loc := time.FixedZone("x", 3600)
	now := time.Date(2026, 3, 1, 9, 30, 15, 0, loc)
	at, err := NextWallClock(now, "09:45")
	if err != nil || !at.Equal(time.Date(2026, 3, 1, 9, 45, 0, 0, loc)) {
		t.Fatalf("%v %v", at, err)
	}
	// Same minute already begun: tomorrow, not 15 seconds ago.
	at, err = NextWallClock(now, "09:30")
	if err != nil || !at.Equal(time.Date(2026, 3, 2, 9, 30, 0, 0, loc)) {
		t.Fatalf("%v %v", at, err)
	}
	at, err = NextWallClock(now, "00:00")
	if err != nil || !at.Equal(time.Date(2026, 3, 2, 0, 0, 0, 0, loc)) {
		t.Fatalf("%v %v", at, err)
	}
	for _, bad := range []string{"", "9:00am", "24:00", "09:60", "0900", "09:00:00"} {
		if _, err := NextWallClock(now, bad); err == nil {
			t.Fatalf("%q accepted", bad)
		}
	}
}
