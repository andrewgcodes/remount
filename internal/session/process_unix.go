//go:build !windows

package session

import (
	"errors"
	"os/exec"
	"syscall"

	"remount.dev/remount/internal/proto"
)

func configureProcessGroup(cmd *exec.Cmd) {
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
}

func isPTYEOF(err error) bool {
	return errors.Is(err, syscall.EIO)
}

func signalProcess(cmd *exec.Cmd, name string) error {
	signals := map[string]syscall.Signal{
		"TERM": syscall.SIGTERM, "KILL": syscall.SIGKILL, "INT": syscall.SIGINT,
		"HUP": syscall.SIGHUP, "QUIT": syscall.SIGQUIT, "USR1": syscall.SIGUSR1, "USR2": syscall.SIGUSR2,
	}
	sig, ok := signals[name]
	if !ok {
		return proto.Err(proto.CodeBadRequest, "unknown signal %q", name)
	}
	if cmd.SysProcAttr != nil && cmd.SysProcAttr.Setpgid {
		// Signal the whole process group so pipelines and children die too.
		return syscall.Kill(-cmd.Process.Pid, sig)
	}
	return cmd.Process.Signal(sig)
}

func platformExitSignal(err *exec.ExitError) (string, int, bool) {
	status, ok := err.Sys().(syscall.WaitStatus)
	if !ok || !status.Signaled() {
		return "", 0, false
	}
	return status.Signal().String(), 128 + int(status.Signal()), true
}

func isPTYEOF(err error) bool {
	return errors.Is(err, syscall.EIO)
}
