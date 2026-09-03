//go:build unix

package main

import (
	"os"
	"syscall"
)

// detachedProcAttr puts the standalone in its own session so the terminal's
// SIGINT/SIGHUP reach only the CLI that started it.
func detachedProcAttr() *syscall.SysProcAttr {
	return &syscall.SysProcAttr{Setsid: true}
}

func signalProcess(p *os.Process) error { return p.Signal(syscall.Signal(0)) }
