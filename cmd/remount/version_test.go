package main

import (
	"runtime/debug"
	"testing"
)

func TestResolveVersionUsesModuleVersionOnlyForUnlinkedInstall(t *testing.T) {
	tests := []struct {
		name, linked, module, want string
	}{
		{name: "release linker flag wins", linked: "v2.0.0", module: "v1.0.0", want: "v2.0.0"},
		{name: "versioned go install", linked: "dev", module: "v1.2.3", want: "v1.2.3"},
		{name: "local build", linked: "dev", module: "(devel)", want: "dev"},
		{name: "missing module", linked: "dev", want: "dev"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			info := &debug.BuildInfo{Main: debug.Module{Version: test.module}}
			if got := resolveVersion(test.linked, info); got != test.want {
				t.Fatalf("resolveVersion(%q, %q) = %q, want %q", test.linked, test.module, got, test.want)
			}
		})
	}
	if got := resolveVersion("dev", nil); got != "dev" {
		t.Fatalf("resolveVersion with nil build info = %q, want dev", got)
	}
}
