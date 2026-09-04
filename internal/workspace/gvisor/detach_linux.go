//go:build linux

package gvisor

import "golang.org/x/sys/unix"

// detachMount lazily unmounts path, ignoring the case where nothing is mounted
// there. MNT_DETACH so a mount still referenced by an exiting sandbox is
// released when its last user goes away rather than failing here.
func detachMount(path string) error {
	err := unix.Unmount(path, unix.MNT_DETACH)
	if err == unix.EINVAL || err == unix.ENOENT {
		return nil // nothing mounted there, which is the normal case
	}
	return err
}
