//go:build windows

package volume

import (
	"errors"
	"os"
	"path/filepath"
	"strings"

	"golang.org/x/sys/windows"
)

func openDirectoryNoSymlink(rootPath, rel string, create bool) (*os.File, error) {
	root, err := os.OpenRoot(rootPath)
	if err != nil {
		return nil, err
	}
	defer root.Close()
	prefix := ""
	for _, component := range strings.Split(rel, "/") {
		prefix = filepath.Join(prefix, component)
		info, statErr := root.Lstat(prefix)
		if errors.Is(statErr, os.ErrNotExist) && create {
			if err := root.Mkdir(prefix, 0o755); err != nil {
				return nil, errors.Join(ErrUnsafePath, err)
			}
			info, statErr = root.Lstat(prefix)
		}
		if statErr != nil {
			return nil, statErr
		}
		if info.Mode()&os.ModeSymlink != 0 || !info.IsDir() {
			return nil, ErrUnsafePath
		}
	}
	return root.Open(rel)
}

func directoryIdentity(file *os.File) (uint64, uint64, error) {
	info, err := file.Stat()
	if err != nil {
		return 0, 0, err
	}
	if !info.IsDir() {
		return 0, 0, ErrUnsafePath
	}
	var identity windows.ByHandleFileInformation
	if err := windows.GetFileInformationByHandle(windows.Handle(file.Fd()), &identity); err != nil {
		return 0, 0, err
	}
	index := uint64(identity.FileIndexHigh)<<32 | uint64(identity.FileIndexLow)
	return uint64(identity.VolumeSerialNumber), index, nil
}
