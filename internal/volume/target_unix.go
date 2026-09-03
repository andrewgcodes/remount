//go:build linux || darwin

package volume

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"syscall"

	"golang.org/x/sys/unix"
)

func openDirectoryNoSymlink(rootPath, rel string, create bool) (*os.File, error) {
	fd, err := unix.Open(rootPath, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_CLOEXEC|unix.O_NOFOLLOW, 0)
	if err != nil {
		return nil, unsafePathError(err)
	}
	for _, component := range strings.Split(rel, "/") {
		if component == "" || component == "." || component == ".." {
			_ = unix.Close(fd)
			return nil, ErrUnsafePath
		}
		if create {
			if err := unix.Mkdirat(fd, component, 0o755); err != nil && !errors.Is(err, syscall.EEXIST) {
				_ = unix.Close(fd)
				return nil, unsafePathError(err)
			}
		}
		next, err := unix.Openat(fd, component, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_CLOEXEC|unix.O_NOFOLLOW, 0)
		_ = unix.Close(fd)
		if err != nil {
			return nil, unsafePathError(err)
		}
		fd = next
	}
	return os.NewFile(uintptr(fd), filepath.Join(rootPath, filepath.FromSlash(rel))), nil
}

func unsafePathError(err error) error {
	if errors.Is(err, syscall.ELOOP) || errors.Is(err, syscall.ENOTDIR) {
		return errors.Join(ErrUnsafePath, err)
	}
	return err
}

func directoryIdentity(file *os.File) (uint64, uint64, error) {
	info, err := file.Stat()
	if err != nil {
		return 0, 0, err
	}
	if !info.IsDir() {
		return 0, 0, ErrUnsafePath
	}
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok {
		return 0, 0, ErrUnsupported
	}
	return uint64(stat.Dev), uint64(stat.Ino), nil
}
