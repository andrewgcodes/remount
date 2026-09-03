//go:build linux

package firecracker

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"

	"golang.org/x/sys/unix"
)

func acquireHostLock(path string) (*os.File, error) {
	file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return nil, err
	}
	if err := unix.Flock(int(file.Fd()), unix.LOCK_EX|unix.LOCK_NB); err != nil {
		_ = file.Close()
		return nil, err
	}
	return file, nil
}

func releaseHostLock(file *os.File) error {
	if file == nil {
		return nil
	}
	err := unix.Flock(int(file.Fd()), unix.LOCK_UN)
	return errors.Join(err, file.Close())
}

func recoverOwnedJails(ctx context.Context, base, executable string, maxScan int) error {
	parent := filepath.Join(base, executable)
	entries, err := os.ReadDir(parent)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	owned := 0
	for _, entry := range entries {
		if !strings.HasPrefix(entry.Name(), "rm-") {
			continue
		}
		owned++
		if owned > maxScan {
			return fmt.Errorf("more than %d retained Remount jails require operator inspection", maxScan)
		}
		root := filepath.Join(parent, entry.Name(), "root")
		if err := recoverJail(ctx, root, executable); err != nil {
			return err
		}
	}
	return syncDirectory(parent)
}

func recoverJail(ctx context.Context, root, executable string) error {
	st, err := os.Lstat(root)
	if errors.Is(err, os.ErrNotExist) {
		return removeJailRoot(root, filepath.Dir(filepath.Dir(filepath.Dir(root))))
	}
	if err != nil || !st.IsDir() || st.Mode()&os.ModeSymlink != 0 {
		return fmt.Errorf("retained jail root %q is not a real directory", root)
	}
	pidData, err := os.ReadFile(filepath.Join(root, executable+".pid"))
	if err == nil {
		pid, parseErr := strconv.Atoi(strings.TrimSpace(string(pidData)))
		if parseErr != nil || pid <= 1 {
			return fmt.Errorf("invalid retained Firecracker pid in %q", root)
		}
		procRoot := filepath.Join("/proc", strconv.Itoa(pid), "root")
		rootInfo, rootErr := os.Stat(root)
		procInfo, procErr := os.Stat(procRoot)
		switch {
		case errors.Is(procErr, os.ErrNotExist):
			// Dead orphan: only its exact, generated jail is removed below.
		case rootErr != nil || procErr != nil:
			return errors.Join(rootErr, procErr)
		case os.SameFile(rootInfo, procInfo):
			process, findErr := os.FindProcess(pid)
			if findErr != nil {
				return findErr
			}
			if err := process.Signal(syscall.SIGKILL); err != nil && !errors.Is(err, os.ErrProcessDone) {
				return fmt.Errorf("fence retained Firecracker pid %d: %w", pid, err)
			}
			deadline := time.NewTimer(10 * time.Second)
			ticker := time.NewTicker(10 * time.Millisecond)
			defer deadline.Stop()
			defer ticker.Stop()
			for {
				if _, err := os.Stat(procRoot); errors.Is(err, os.ErrNotExist) {
					break
				} else if err != nil {
					return err
				}
				select {
				case <-ctx.Done():
					return ctx.Err()
				case <-deadline.C:
					return errors.New("timed out joining retained Firecracker process")
				case <-ticker.C:
				}
			}
		default:
			// PID reuse is not authority to signal the unrelated process.
		}
	} else if !errors.Is(err, os.ErrNotExist) {
		return err
	}
	return removeJailRoot(root, filepath.Dir(filepath.Dir(filepath.Dir(root))))
}
