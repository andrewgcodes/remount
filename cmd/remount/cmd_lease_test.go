package main

import (
	"bytes"
	"context"
	"strings"
	"testing"
	"time"

	"remount.dev/remount/internal/proto"
)

// TestLeaseCommandsValidateBeforeDialing keeps the argument mistakes on this
// side of the network. Every case below would otherwise become a control-plane
// round trip that fails with a message about a protocol field the user never
// typed, and `--max 500ms` would be worse than that: whole-second truncation
// turns it into "no deadline asked for".
func TestLeaseCommandsValidateBeforeDialing(t *testing.T) {
	for _, args := range [][]string{
		{"lease"},                                  // no workspace
		{"lease", "ws_a"},                          // no --max
		{"lease", "ws_a", "--max", "500ms"},        // truncates to zero seconds
		{"lease", "ws_a", "--max", "-1m"},          // negative
		{"lease", "ws_a", "--max", "20m", "extra"}, // extra positional
		{"lease", "ws_a", "--max", "20m", "--min", "900ms"},
		{"lease", "renew", "ws_a"},         // no lease id
		{"lease", "renew", "ws_a", "wl_a"}, // no --extend
		{"lease", "renew", "ws_a", "wl_a", "--extend", "0s"},
		{"lease", "cancel", "ws_a"},       // no lease id
		{"lease", "get"},                  // no workspace
		{"lease", "get", "ws_a", "extra"}, // extra positional
		{"idle-policy"},                   // no workspace
		{"idle-policy", "ws_a", "--sleep-after", "300ms"},
		{"idle-policy", "ws_a", "--destroy-after", "-1h"},
		{"mark-idle"},                    // no workspace
		{"mark-active"},                  // no workspace
		{"mark-active", "ws_a", "extra"}, // extra positional
	} {
		if err := cmdWSLifecycle(context.Background(), args[0], args[1:]); err == nil {
			t.Fatalf("ws %v reached dial", args)
		}
	}
	if err := cmdWSLifecycle(context.Background(), "lease", []string{"--help"}); err == nil ||
		!strings.Contains(err.Error(), "ws lease WS --max") {
		t.Fatalf("help error=%v", err)
	}
	if err := cmdWSLifecycle(context.Background(), "wat", nil); err == nil ||
		!strings.Contains(err.Error(), "unknown ws subcommand") {
		t.Fatalf("error=%v", err)
	}
}

// TestLeaseSecondsRejectsSubSecondDurations pins the conversion the CLI does
// to reach the protocol's whole-second fields.
func TestLeaseSecondsRejectsSubSecondDurations(t *testing.T) {
	if _, err := leaseSeconds("--max", 0, true); err == nil {
		t.Fatal("a required duration of zero was accepted")
	}
	if got, err := leaseSeconds("--min", 0, false); err != nil || got != 0 {
		t.Fatalf("optional zero: got=%d err=%v", got, err)
	}
	if _, err := leaseSeconds("--max", 999*time.Millisecond, true); err == nil {
		t.Fatal("999ms was accepted and would truncate to zero seconds")
	}
	if _, err := leaseSeconds("--max", -time.Minute, true); err == nil {
		t.Fatal("a negative duration was accepted")
	}
	// Truncation toward zero, not rounding: 90s is a minute and a half, and
	// 20m is exactly 1200 seconds.
	for _, tc := range []struct {
		in   time.Duration
		want int64
	}{{time.Second, 1}, {90 * time.Second, 90}, {20 * time.Minute, 1200}, {90500 * time.Millisecond, 90}} {
		got, err := leaseSeconds("--max", tc.in, true)
		if err != nil || got != tc.want {
			t.Fatalf("leaseSeconds(%s)=%d,%v want %d", tc.in, got, err, tc.want)
		}
	}
}

// TestPrintLifecycleDeadlineSaysWhatHappensNext covers the renderer `ws get`
// and `ws lease get` add. A raw millisecond field in the JSON does not tell a
// reader that the control plane is about to put this workspace to sleep, and a
// failed deadline has to say so out loud rather than look like a pending one.
func TestPrintLifecycleDeadlineSaysWhatHappensNext(t *testing.T) {
	var out bytes.Buffer
	printLifecycleDeadline(&out, nil)
	if out.Len() != 0 {
		t.Fatalf("no deadline printed %q", out.String())
	}

	at := time.Now().Add(90 * time.Second).UnixMilli()
	printLifecycleDeadline(&out, &proto.LifecycleDeadline{
		At: at, Action: proto.LeaseExpirySleep, Source: proto.LifecycleSourceLease,
	})
	pending := out.String()
	for _, want := range []string{"lifecycle deadline: sleep at", "source=lease", "in 1m3"} {
		if !strings.Contains(pending, want) {
			t.Fatalf("pending deadline %q is missing %q", pending, want)
		}
	}

	out.Reset()
	printLifecycleDeadline(&out, &proto.LifecycleDeadline{
		At: at, Action: proto.LeaseExpiryDestroy, Source: proto.LifecycleSourceIdle,
		Fired: true, FiredAt: at, Failed: true, Attempts: 5, Error: "node unreachable",
	})
	failed := out.String()
	for _, want := range []string{"FAILED", "attempts=5", "node unreachable", "degraded"} {
		if !strings.Contains(failed, want) {
			t.Fatalf("failed deadline %q is missing %q", failed, want)
		}
	}
	if strings.Contains(failed, "lifecycle deadline: destroy at") {
		t.Fatalf("a failed deadline was rendered as pending: %q", failed)
	}
}
