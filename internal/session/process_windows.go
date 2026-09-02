//go:build windows

package session

import (
	"os"
	"os/exec"

	"remount.dev/remount/internal/proto"
)

func configureProcessGroup(cmd *exec.Cmd) {}

func signalProcess(cmd *exec.Cmd, name string) error {
	switch name {
	case "KILL", "TERM":
		return cmd.Process.Kill()
	case "INT":
		return cmd.Process.Signal(os.Interrupt)
	case "HUP", "QUIT", "USR1", "USR2":
		return proto.Err(proto.CodeUnsupported, "signal %q is not supported on Windows", name)
	default:
		return proto.Err(proto.CodeBadRequest, "unknown signal %q", name)
	}
}

func platformExitSignal(err *exec.ExitError) (string, int, bool) {
	return "", 0, false
}
