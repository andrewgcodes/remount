package conformance

import (
	"context"
	"testing"
)

func TestWaitForEventsObservesDelayedCanonicalAudit(t *testing.T) {
	var calls int
	events, err := waitForEvents(context.Background(), func(context.Context) ([]Event, error) {
		calls++
		if calls < 3 {
			return nil, nil
		}
		return []Event{{Type: "egress.denied"}}, nil
	}, func(events []Event) bool {
		return len(events) == 1 && events[0].Type == "egress.denied"
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(events) != 1 || calls != 3 {
		t.Fatalf("events = %+v after %d calls", events, calls)
	}
}
