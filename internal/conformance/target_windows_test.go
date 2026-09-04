//go:build windows

package conformance

import "testing"

func TestLocalExecutableNameUsesWindowsSuffix(t *testing.T) {
	if got := localExecutableName("remount"); got != "remount.exe" {
		t.Fatalf("local executable name = %q", got)
	}
}
