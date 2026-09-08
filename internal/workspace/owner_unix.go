//go:build !windows

package workspace

import (
	"io/fs"
	"syscall"
)

func fileOwnedBy(info fs.FileInfo, uid int) bool {
	st, ok := info.Sys().(*syscall.Stat_t)
	return ok && int(st.Uid) == uid
}
