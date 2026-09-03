//go:build windows

package main

import (
	"os"
	"syscall"
)

// detachedProcAttr starts the standalone outside the console's process
// group so closing the terminal does not take it down.
func detachedProcAttr() *syscall.SysProcAttr {
	return &syscall.SysProcAttr{CreationFlags: syscall.CREATE_NEW_PROCESS_GROUP | 0x00000008} // DETACHED_PROCESS
}

// signalProcess has no signal 0 on Windows; FindProcess succeeding is the
// liveness check there.
func signalProcess(p *os.Process) error { return nil }
