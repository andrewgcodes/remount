//go:build windows

package conformance

import (
	"os/exec"
	"strings"
	"testing"
)

func TestWindowsEchoProgramPreservesLiteral(t *testing.T) {
	const want = "conf-sess-009: stdout must arrive verbatim."
	program := echoProgram("windows", want)
	out, err := exec.Command(program[0], program[1:]...).Output()
	if err != nil {
		t.Fatal(err)
	}
	if got := strings.TrimRight(string(out), "\r\n"); got != want {
		t.Fatalf("stdout = %q, want %q", got, want)
	}
}

func TestWindowsExitProgramReturnsRequestedCode(t *testing.T) {
	program := exitProgram("windows", 7)
	err := exec.Command(program[0], program[1:]...).Run()
	exit, ok := err.(*exec.ExitError)
	if !ok || exit.ExitCode() != 7 {
		t.Fatalf("exit program error = %v", err)
	}
}
