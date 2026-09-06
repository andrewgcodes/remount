package main

import (
	"context"
	"strings"
	"testing"
)

// `ws create --requires-profile` states the runtime profile a node must
// satisfy (ADR 0089). An unknown name has to fail locally and by name: the
// alternative is a workspace that parks forever with
// pending_reason profile_unschedulable because nobody can satisfy a profile
// that does not exist.
func TestWorkspaceCreateRequiresProfileIsValidatedBeforeDial(t *testing.T) {
	err := cmdWS(context.Background(), []string{"create", "--requires-profile", "production", "--wait=false"})
	if err == nil {
		t.Fatal("unknown runtime profile accepted")
	}
	for _, want := range []string{"unknown runtime profile", "multi-tenant-isolated", "microvm"} {
		if !strings.Contains(err.Error(), want) {
			t.Fatalf("error %q does not name %q", err, want)
		}
	}
}

// The value that reaches Requires.Profile: every known profile passes through
// unchanged, surrounding whitespace is trimmed, and an unset flag stays empty
// rather than defaulting to "dev". No requirement matches every node, while
// "dev" is a stated constraint the control plane still evaluates.
func TestRequiresProfileNormalizesKnownProfilesAndKeepsUnsetEmpty(t *testing.T) {
	for _, tc := range []struct{ in, want string }{
		{"", ""},
		{"   ", ""},
		{"dev", "dev"},
		{"trusted-single-tenant", "trusted-single-tenant"},
		{" multi-tenant-isolated ", "multi-tenant-isolated"},
		{"microvm", "microvm"},
	} {
		got, err := requiresProfile(tc.in)
		if err != nil {
			t.Fatalf("requiresProfile(%q): %v", tc.in, err)
		}
		if got != tc.want {
			t.Fatalf("requiresProfile(%q)=%q, want %q", tc.in, got, tc.want)
		}
	}
	if _, err := requiresProfile("MULTI-TENANT-ISOLATED"); err == nil {
		t.Fatal("a case-variant profile was accepted; the wire value is exact")
	}
}
