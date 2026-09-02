//go:build !windows

package node

import "golang.org/x/sys/unix"

func diskSpace(path string) (free, total int64) {
	var st unix.Statfs_t
	if err := unix.Statfs(path, &st); err != nil {
		return 0, 0
	}
	return int64(st.Bavail) * int64(st.Bsize), int64(st.Blocks) * int64(st.Bsize)
}
