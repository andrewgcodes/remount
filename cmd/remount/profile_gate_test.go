package main

import (
	"path/filepath"
	"strings"
	"testing"
)

// The startup gate must fail closed. A node that cannot prove the profile it
// was configured with never reaches node.New, so it never registers, never
// advertises capabilities and never receives an offer.
func TestBuildNodeRefusesUnprovableProfile(t *testing.T) {
	cases := []struct {
		name     string
		profile  string
		backends string
		want     string
	}{
		{
			name:     "multi-tenant refuses the process backend by name",
			profile:  "multi-tenant-isolated",
			backends: "process",
			want:     "shares the host kernel",
		},
		{
			name:     "multi-tenant refuses docker by name",
			profile:  "multi-tenant-isolated",
			backends: "docker",
			want:     "shares the host kernel",
		},
		{
			name:     "trusted-single-tenant refuses an unisolated backend",
			profile:  "trusted-single-tenant",
			backends: "process",
			want:     "is not satisfied",
		},
		{
			name:     "an unknown profile is refused rather than ignored",
			profile:  "production",
			backends: "process",
			want:     "unknown runtime profile",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := buildNode(filepath.Join(t.TempDir(), "node"), "", common{}, nil, tc.backends, "",
				nil, nil, nodeResourceOptions{profile: tc.profile})
			if err == nil {
				t.Fatalf("profile %q with backend %q started anyway", tc.profile, tc.backends)
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("error %q does not contain %q", err, tc.want)
			}
		})
	}
}

// The default profile claims nothing, so the development path is unchanged.
func TestBuildNodeDefaultProfileAllowsProcess(t *testing.T) {
	n, err := buildNode(filepath.Join(t.TempDir(), "node"), "", common{}, nil, "process", "", nil, nil, nodeResourceOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if got := string(n.Profile()); got != "dev" {
		t.Fatalf("default profile %q, want dev", got)
	}
}
