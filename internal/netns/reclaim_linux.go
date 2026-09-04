//go:build linux

package netns

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"

	"golang.org/x/sys/unix"
)

// ReclaimOrphans removes namespaces that no process inhabits, together with
// their veths, and reports how many it reclaimed.
//
// Every namespace this package creates is a bind mount under
// /run/remount/netns, and it survives the process that made it. That is
// deliberate — a workspace outlives a node restart — but nothing reclaimed the
// ones whose owner died. Worse, the names are not incidental: they are formatted
// from an allocator slot that lives in memory, so a fresh process starts
// allocating from the beginning and asks the kernel for the very slot whose
// leftovers are still there. A node killed once therefore collides on its next
// start, deterministically, and reports "create veth: file exists" — an error
// that names a file rather than the cause.
//
// A namespace is orphaned when no process is in it. That is decidable rather
// than guessed: the bind mount's inode is the namespace's inode, and every
// /proc/<pid>/ns/net links to net:[inode] for the namespace that process is in.
// A namespace whose inode appears in none of them has no processes, so nothing
// can be using it and reclaiming it cannot disturb a live workspace.
func ReclaimOrphans(ctx context.Context) (int, error) {
	kernel := newSystemKernel()
	entries, err := os.ReadDir(namespaceDir)
	if errors.Is(err, os.ErrNotExist) {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}
	live, err := inhabitedNamespaces()
	if err != nil {
		return 0, err
	}
	reclaimed := 0
	var errs []error
	for _, entry := range entries {
		name := entry.Name()
		if !strings.HasPrefix(name, "rm-") {
			continue
		}
		path := filepath.Join(namespaceDir, name)
		info, err := os.Stat(path)
		if err != nil {
			errs = append(errs, err)
			continue
		}
		// os.Stat reports *syscall.Stat_t here, not unix.Stat_t; asserting the
		// wrong one compiles, always fails, and silently reclaims nothing.
		stat, ok := info.Sys().(*syscall.Stat_t)
		if !ok {
			continue
		}
		if _, inhabited := live[stat.Ino]; inhabited {
			continue
		}
		if err := reclaimNamespace(ctx, kernel, path, name); err != nil {
			errs = append(errs, err)
			continue
		}
		reclaimed++
	}
	return reclaimed, errors.Join(errs...)
}

// reclaimNamespace unmounts and removes one namespace and deletes the veth
// named for the same slot. Both are best effort in the sense that an already
// absent resource is success; anything else is reported.
func reclaimNamespace(ctx context.Context, kernel Kernel, path, name string) error {
	if err := unix.Unmount(path, unix.MNT_DETACH); err != nil && err != unix.EINVAL && err != unix.ENOENT {
		return fmt.Errorf("unmount %s: %w", path, err)
	}
	if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("remove %s: %w", path, err)
	}
	// rm-<slot>-<generation> names the veth pair rmh<slot>/rmg<slot>. The guest
	// end lives inside the namespace and goes away with it; deleting the host
	// end deletes both.
	fields := strings.Split(name, "-")
	if len(fields) != 3 {
		return nil
	}
	slot, err := strconv.ParseUint(fields[1], 16, 16)
	if err != nil {
		return nil
	}
	host := fmt.Sprintf("rmh%04x", slot)
	if _, err := net.InterfaceByName(host); err != nil {
		return nil // already gone
	}
	if err := kernel.DeleteVeth(ctx, host); err != nil {
		return fmt.Errorf("delete %s: %w", host, err)
	}
	return nil
}

// inhabitedNamespaces is the set of network-namespace inodes some process is
// currently in.
func inhabitedNamespaces() (map[uint64]struct{}, error) {
	procs, err := os.ReadDir("/proc")
	if err != nil {
		return nil, err
	}
	live := make(map[uint64]struct{}, len(procs))
	for _, proc := range procs {
		if _, err := strconv.Atoi(proc.Name()); err != nil {
			continue
		}
		target, err := os.Readlink(filepath.Join("/proc", proc.Name(), "ns", "net"))
		if err != nil {
			continue // the process exited, or is not ours to inspect
		}
		inode, ok := strings.CutPrefix(target, "net:[")
		if !ok {
			continue
		}
		inode, ok = strings.CutSuffix(inode, "]")
		if !ok {
			continue
		}
		n, err := strconv.ParseUint(inode, 10, 64)
		if err != nil {
			continue
		}
		live[n] = struct{}{}
	}
	return live, nil
}
