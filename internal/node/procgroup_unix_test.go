//go:build unix

package node

import "syscall"

// processGroupAlive reports whether any process remains in pid's group.
// Signal 0 performs the permission and existence checks without delivering
// anything, which is the portable POSIX way to ask.
func processGroupAlive(pid int) bool { return syscall.Kill(-pid, 0) == nil }

// posixProcessGroups is true where a process group is a real kernel object a
// test can interrogate.
const posixProcessGroups = true
