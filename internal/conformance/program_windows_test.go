//go:build windows

package conformance

import (
	"os/exec"
	"strings"
	"testing"
)

func TestWindowsEchoProgramPreservesLiteral(t *testing.T) {
	const want = "conf-sess-009: stdout must arrive verbatim."
	program := echoProgram(want)
	out, err := exec.Command(program[0], program[1:]...).Output()
	if err != nil {
		t.Fatal(err)
	}
	if got := strings.TrimRight(string(out), "\r\n"); got != want {
		t.Fatalf("stdout = %q, want %q", got, want)
	}
}

func TestWindowsExitProgramReturnsRequestedCode(t *testing.T) {
	err := exec.Command(exitProgram(7)[0], exitProgram(7)[1:]...).Run()
	exit, ok := err.(*exec.ExitError)
	if !ok || exit.ExitCode() != 7 {
		t.Fatalf("exit program error = %v", err)
	}
}
