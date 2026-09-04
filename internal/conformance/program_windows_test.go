//go:build windows

package conformance

import (
	"os/exec"
	"testing"
)

func TestWindowsExitProgramReturnsRequestedCode(t *testing.T) {
	err := exec.Command(exitProgram(7)[0], exitProgram(7)[1:]...).Run()
	exit, ok := err.(*exec.ExitError)
	if !ok || exit.ExitCode() != 7 {
		t.Fatalf("exit program error = %v", err)
	}
}
