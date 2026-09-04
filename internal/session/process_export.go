package session

import "os/exec"

// ConfigureProcessGroup puts cmd in its own process group where the platform
// has one, so SignalProcess reaches the children a harness spawns and not
// only the harness itself.
func ConfigureProcessGroup(cmd *exec.Cmd) { configureProcessGroup(cmd) }

// RegisterProcessGroup attaches a started process to its platform containment
// object. ConfigureProcessGroup must be called before cmd.Start.
func RegisterProcessGroup(cmd *exec.Cmd) error { return registerProcessGroup(cmd) }

// ReleaseProcessGroup releases the platform containment object after Wait.
func ReleaseProcessGroup(cmd *exec.Cmd) { releaseProcessGroup(cmd) }

// SignalProcess delivers the named signal (TERM, KILL, INT, ...) to cmd's
// process group with the same platform handling managed sessions use.
func SignalProcess(cmd *exec.Cmd, name string) error { return signalProcess(cmd, name) }

// ExitStatus reports how a finished exec.Cmd ended: the exit code and, when
// a signal ended it, the signal's name.
func ExitStatus(err error) (code int, signal string) {
	if err == nil {
		return 0, ""
	}
	if ee, ok := err.(*exec.ExitError); ok {
		if sig, c, ok := platformExitSignal(ee); ok {
			return c, sig
		}
		return ee.ExitCode(), ""
	}
	return -1, ""
}
