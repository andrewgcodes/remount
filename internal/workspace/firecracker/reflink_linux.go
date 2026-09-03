//go:build linux

package firecracker

import (
	"fmt"
	"os"

	"golang.org/x/sys/unix"
)

func hostReflink(source, destination string) error {
	srcFD, err := unix.Open(source, unix.O_RDONLY|unix.O_CLOEXEC|unix.O_NOFOLLOW, 0)
	if err != nil {
		return err
	}
	defer unix.Close(srcFD)
	dstFD, err := unix.Open(destination, unix.O_WRONLY|unix.O_CLOEXEC|unix.O_NOFOLLOW, 0)
	if err != nil {
		return err
	}
	defer unix.Close(dstFD)
	if err := unix.IoctlFileClone(dstFD, srcFD); err != nil {
		return fmt.Errorf("clone %q to %q: %w", source, destination, err)
	}
	if err := unix.Fsync(dstFD); err != nil {
		return err
	}
	st, err := os.Stat(destination)
	if err != nil || !st.Mode().IsRegular() {
		return fmt.Errorf("reflink destination is not a regular file")
	}
	return nil
}
