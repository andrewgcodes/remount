package main

import (
	"bytes"
	"testing"

	"remount.dev/remount/internal/proto"
)

func TestGenerateReflectsEnforcedBackendCaps(t *testing.T) {
	body, err := generate()
	if err != nil {
		t.Fatal(err)
	}
	for _, expected := range [][]byte{
		[]byte("| `process` | yes | no | no |"),
		[]byte("| `docker` | yes | no | no |"),
		[]byte("| `gvisor` | yes | yes | no |"),
		[]byte("| `firecracker` | yes | no | no |"),
		[]byte("`enforced_gateway`"),
		[]byte("Firecracker descriptor in the enforcement table above is the fail-closed,"),
	} {
		if !bytes.Contains(body, expected) {
			t.Fatalf("generated profile matrix lacks %q", expected)
		}
	}
}

// TestSupportMatrixIsDerivedFromCaps pins the rows an adopter reads before
// choosing a backend. Each is derived from that backend's own advertised
// capabilities, so a Caps change that quietly widened a claim breaks here.
func TestSupportMatrixIsDerivedFromCaps(t *testing.T) {
	body, err := generate()
	if err != nil {
		t.Fatal(err)
	}
	for _, expected := range [][]byte{
		// Operating systems: the two isolation backends are Linux only.
		[]byte("| Linux | yes | yes | `docker`, `firecracker`, `gvisor`, `process` |"),
		[]byte("| macOS | yes | yes | `docker`, `process` |"),
		[]byte("| Windows | yes | yes, reduced | `docker`, `process` |"),
		// Only a verified Firecracker advertises a memory snapshot, and even
		// then the microVM profile is unavailable until a host check runs.
		[]byte("| `firecracker` (verified) | yes | yes | yes | yes | yes | yes | yes | `pass` | `pass` | `pass` | `unavailable` |"),
		// The unverified zero value claims nothing it has not proven.
		[]byte("| `firecracker` (unverified) | yes | yes | no | no | no | no | yes | `pass` | `fail` | `fail` | `fail` |"),
		// A cooperative proxy is not enforced egress, whatever else it does.
		[]byte("| `docker` | yes | yes | no | yes | yes | no | yes | `pass` | `pass` | `fail` | `fail` |"),
		[]byte("| `gvisor` | yes | yes | no | yes | yes | yes | yes | `pass` | `pass` | `pass` | `fail` |"),
	} {
		if !bytes.Contains(body, expected) {
			t.Fatalf("generated support matrix lacks %q", expected)
		}
	}
}

// TestUnrecordedBackendFailsGeneration proves the operating-system table
// cannot silently omit a backend: a new one must state where it runs before
// the documentation can be regenerated.
func TestUnrecordedBackendFailsGeneration(t *testing.T) {
	if err := checkOSCoverage([]proto.BackendDescriptor{{Name: "invented"}}); err == nil {
		t.Fatal("a backend with no recorded operating systems was accepted")
	}
	if err := checkOSCoverage(nil); err == nil {
		t.Fatal("a recorded backend that is not registered was accepted")
	}
}

func TestRegisteredBackendsAreCovered(t *testing.T) {
	names, err := registeredBackendNames()
	if err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"process", "docker", "gvisor", "firecracker"} {
		if !names[name] {
			t.Fatalf("buildNode backend %q was not discovered", name)
		}
	}
}
