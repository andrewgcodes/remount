package main

import (
	"bytes"
	"testing"
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
		[]byte("`enforced_gateway`"),
		[]byte("Firecracker package is host-gated but is not registered"),
	} {
		if !bytes.Contains(body, expected) {
			t.Fatalf("generated profile matrix lacks %q", expected)
		}
	}
}

func TestRegisteredBackendsAreCovered(t *testing.T) {
	names, err := registeredBackendNames()
	if err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"process", "docker", "gvisor"} {
		if !names[name] {
			t.Fatalf("buildNode backend %q was not discovered", name)
		}
	}
}
