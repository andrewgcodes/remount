//go:build linux

package volume

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"

	"golang.org/x/sys/unix"
)

// BindMountEngine creates private, read-only bind mounts. The source may be a
// local directory or a directory on an NFS filesystem already mounted by the
// trusted host.
type BindMountEngine struct{}

// MountReadOnly installs a bind mount and then applies per-mount read-only,
// nodev and nosuid flags. Failure to apply any fence rolls the bind back.
func (BindMountEngine) MountReadOnly(ctx context.Context, source, target *os.File) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	flags := uint(unix.OPEN_TREE_CLONE | unix.OPEN_TREE_CLOEXEC | unix.AT_EMPTY_PATH)
	mountFD, err := unix.OpenTree(int(source.Fd()), "", flags)
	if err != nil {
		if errors.Is(err, unix.ENOSYS) {
			return errors.Join(ErrUnsupported, fmt.Errorf("volume: clone mount tree: %w", err))
		}
		return fmt.Errorf("volume: clone mount tree: %w", err)
	}
	defer unix.Close(mountFD)
	attr := &unix.MountAttr{
		Attr_set:    unix.MOUNT_ATTR_RDONLY | unix.MOUNT_ATTR_NODEV | unix.MOUNT_ATTR_NOSUID,
		Propagation: unix.MS_PRIVATE,
	}
	if err := unix.MountSetattr(mountFD, "", unix.AT_EMPTY_PATH, attr); err != nil {
		if errors.Is(err, unix.ENOSYS) {
			return errors.Join(ErrUnsupported, fmt.Errorf("volume: fence detached mount: %w", err))
		}
		return fmt.Errorf("volume: fence detached mount: %w", err)
	}
	moveFlags := unix.MOVE_MOUNT_F_EMPTY_PATH | unix.MOVE_MOUNT_T_EMPTY_PATH
	if err := unix.MoveMount(mountFD, "", int(target.Fd()), "", moveFlags); err != nil {
		if errors.Is(err, unix.ENOSYS) {
			return errors.Join(ErrUnsupported, fmt.Errorf("volume: attach detached mount: %w", err))
		}
		return fmt.Errorf("volume: attach detached mount: %w", err)
	}
	return nil
}

// Unmount synchronously removes a bind mount.
func (BindMountEngine) Unmount(ctx context.Context, target *os.File) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := unix.Unmount(procFD(target), 0); err != nil {
		return fmt.Errorf("volume: unmount: %w", err)
	}
	return nil
}

// Inspect reports whether the opened target is an exact mount point and
// whether its per-mount flags include read-only.
func (BindMountEngine) Inspect(ctx context.Context, target *os.File) (MountStatus, error) {
	if err := ctx.Err(); err != nil {
		return MountStatus{}, err
	}
	targetPath, err := os.Readlink(procFD(target))
	if err != nil {
		return MountStatus{}, fmt.Errorf("volume: resolve mount target: %w", err)
	}
	f, err := os.Open("/proc/self/mountinfo")
	if err != nil {
		return MountStatus{}, fmt.Errorf("volume: inspect mounts: %w", err)
	}
	defer f.Close()
	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		fields := strings.Fields(scanner.Text())
		if len(fields) >= 6 && unescapeMountInfo(fields[4]) == targetPath {
			source, err := os.Stat(targetPath)
			if err != nil {
				return MountStatus{}, fmt.Errorf("volume: inspect mounted source: %w", err)
			}
			return MountStatus{Mounted: true, ReadOnly: mountOptionsReadOnly(fields[5]), Source: source}, nil
		}
	}
	if err := scanner.Err(); err != nil {
		return MountStatus{}, fmt.Errorf("volume: inspect mounts: %w", err)
	}
	return MountStatus{}, nil
}

func procFD(f *os.File) string { return "/proc/self/fd/" + strconv.FormatUint(uint64(f.Fd()), 10) }

func unescapeMountInfo(value string) string {
	replacer := strings.NewReplacer(`\040`, " ", `\011`, "\t", `\012`, "\n", `\134`, `\`)
	return replacer.Replace(value)
}

func mountOptionsReadOnly(value string) bool {
	for _, option := range strings.Split(value, ",") {
		if option == "ro" {
			return true
		}
	}
	return false
}
