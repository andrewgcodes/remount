//go:build windows

package node

import "golang.org/x/sys/windows"

func diskSpace(path string) (free, total int64) {
	name, err := windows.UTF16PtrFromString(path)
	if err != nil {
		return 0, 0
	}
	var available, capacity uint64
	if err := windows.GetDiskFreeSpaceEx(name, &available, &capacity, nil); err != nil {
		return 0, 0
	}
	return int64(available), int64(capacity)
}
