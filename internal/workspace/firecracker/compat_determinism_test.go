//go:build linux

package firecracker

import (
	"context"
	"os"
	"os/exec"
	"testing"
	"time"
)

// TestDetectCompatibilityIsDeterministic is the regression for a fence that
// could never be satisfied.
//
// Compatibility is compared for equality to decide whether a snapshot may be
// taken or restored, and two components detect it independently — the jailer
// factory and the CoW volume provider. So the value has to be a function of the
// host and nothing else. It was not: one field held the whole captured output
// of `firecracker --snapshot-version`, which prints the version and then its own
// timestamped shutdown line, so the value differed on every invocation and every
// checkpoint failed with "Firecracker snapshot compatibility changed while
// checkpointing".
//
// The existing bundle test did not catch this because it constructs
// Compatibility values itself. Only calling the real binary twice does, which is
// what this does.
func TestDetectCompatibilityIsDeterministic(t *testing.T) {
	binary := os.Getenv("REMOUNT_FIRECRACKER_BINARY")
	if binary == "" {
		binary = "firecracker"
	}
	if _, err := exec.LookPath(binary); err != nil {
		t.Skipf("unavailable: %s is not installed: %v", binary, err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	first, err := DetectCompatibility(ctx, binary)
	if err != nil {
		t.Skipf("unavailable: DetectCompatibility: %v", err)
	}
	// A deliberate gap, so that anything carrying a clock differs across the
	// two calls rather than happening to agree.
	time.Sleep(1100 * time.Millisecond)
	second, err := DetectCompatibility(ctx, binary)
	if err != nil {
		t.Fatalf("second DetectCompatibility: %v", err)
	}

	if first != second {
		t.Fatalf("compatibility is not a function of the host, so the fence can never be satisfied and no checkpoint can ever be taken:\n first=%+v\nsecond=%+v", first, second)
	}
	// Each field is checked individually too: an equal struct built from two
	// empty strings would also pass the comparison above, and an empty fence is
	// its own defect.
	for name, value := range map[string]string{
		"Architecture":   first.Architecture,
		"CPUFingerprint": first.CPUFingerprint,
		"HostKernel":     first.HostKernel,
		"Firecracker":    first.Firecracker,
		"Snapshot":       first.Snapshot,
	} {
		if value == "" {
			t.Errorf("compatibility field %s is empty, so the fence does not constrain it", name)
		}
	}
	if first.GuestProtocol == 0 {
		t.Error("compatibility guest protocol is zero")
	}
}
