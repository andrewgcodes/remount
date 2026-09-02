//go:build !windows

package fsops

import (
	"os"

	"golang.org/x/sys/unix"
)

func openRead(root *os.Root, name string) (*os.File, error) {
	return root.OpenFile(name, os.O_RDONLY|unix.O_NONBLOCK, 0)
}

func openAppend(root *os.Root, name string, mode os.FileMode) (*os.File, error) {
	return root.OpenFile(name, os.O_CREATE|os.O_WRONLY|os.O_APPEND|unix.O_NONBLOCK, mode)
}
