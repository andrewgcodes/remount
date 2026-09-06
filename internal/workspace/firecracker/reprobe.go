package firecracker

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"

	"remount.dev/remount/internal/profile"
	"remount.dev/remount/internal/proto"
	"remount.dev/remount/internal/workspace"
)

// Reprobe check names. They are stable identifiers a report matches on.
const (
	CheckVerified = "firecracker.verified"
	CheckMachine  = "firecracker.machine"
	CheckNetwork  = "firecracker.network"
	CheckVolume   = "firecracker.volume"
	CheckGuest    = "firecracker.guest"
)

// MachineReprober is the optional read-only re-verification a MachineFactory
// offers. Probe is one-shot by design — it acquires the exclusive host lock
// and recovers retained jails, neither of which may run again while VMs are
// alive — so drift detection needs a separate, side-effect-free entry point.
type MachineReprober interface {
	Reprobe(context.Context) error
}

// Reprobe re-verifies the host prerequisites this backend's Caps depend on:
// KVM, the jailer and Firecracker executables, the cgroup parent, the
// node-owned network, the CoW volume provider and the guest transport. It
// also reports profile.CheckHostMicroVMCompat, the host compatibility result
// the microvm profile requires and that no control plane can observe itself.
//
// Every call is read-only. Nothing here acquires the host ownership lock,
// recovers retained jails or reclaims cgroups: those steps are safe in Probe
// only because no machine this process owns exists yet.
func (b *Backend) Reprobe(ctx context.Context) []proto.Finding {
	if !b.verified {
		// An unverified backend already advertises a fail-closed zero
		// descriptor; say so as a check rather than letting the absence of a
		// finding read as health.
		return []proto.Finding{{
			Severity: "error", Status: proto.CheckFail, Check: CheckVerified,
			Detail: "this firecracker backend never passed construction verification",
			Hint:   "restart the node; an unverified backend advertises no isolation",
		}, {
			Severity: "error", Status: proto.CheckFail, Check: profile.CheckHostMicroVMCompat,
			Detail: "host compatibility cannot be established from an unverified backend",
			Hint:   "repair KVM, jailer and cgroup prerequisites and restart the node",
		}}
	}
	out := []proto.Finding{
		workspace.CheckFinding(CheckMachine, "", "KVM, jailer and cgroup prerequisites still hold",
			"repair /dev/kvm access, the jailer/firecracker binaries or the cgroup parent",
			b.reprobeMachines(ctx)),
		workspace.CheckFinding(CheckNetwork, "", "the node-owned microVM network is available",
			"restore CAP_NET_ADMIN and the TAP/namespace prerequisites",
			b.opts.Networks.Probe(ctx)),
		workspace.CheckFinding(CheckVolume, "", "the CoW volume provider is available",
			"restore the reflink-capable image store",
			b.opts.Volumes.Probe(ctx, b.opts.BaseRootFS)),
		workspace.CheckFinding(CheckGuest, "", "the guest executor transport is available",
			"restore the versioned guest-agent manifest",
			b.opts.Guest.Probe(ctx)),
	}
	out = append(out, b.hostCompatibility(out))
	return out
}

// reprobeMachines prefers the factory's read-only re-verification and falls
// back to Probe, which memoizes success. A memoized Probe cannot see drift,
// so the fallback path reports unavailable rather than a pass it did not earn.
func (b *Backend) reprobeMachines(ctx context.Context) error {
	if rp, ok := b.opts.Machines.(MachineReprober); ok {
		return rp.Reprobe(ctx)
	}
	return errNoMachineReprobe
}

var errNoMachineReprobe = errors.New("machine factory offers no read-only re-verification")

// hostCompatibility folds the machine, network and guest results into the one
// check the microvm profile names. It is a fold rather than a fresh probe
// because the compatibility fence itself (DetectCompatibility) is owned by the
// jailer factory, which reports it through Reprobe.
func (b *Backend) hostCompatibility(checks []proto.Finding) proto.Finding {
	for _, c := range checks {
		if c.Check != CheckMachine {
			continue
		}
		switch c.Status {
		case proto.CheckPass:
			return proto.Finding{
				Severity: "info", Status: proto.CheckPass, Check: profile.CheckHostMicroVMCompat,
				Detail: "the host still satisfies the Firecracker microVM prerequisites",
			}
		case proto.CheckFail:
			return proto.Finding{
				Severity: "error", Status: proto.CheckFail, Check: profile.CheckHostMicroVMCompat,
				Detail: "host microVM prerequisites are not satisfied: " + c.Detail,
				Hint:   c.Hint,
			}
		}
	}
	return workspace.UnavailableFinding(profile.CheckHostMicroVMCompat, "",
		"host microVM compatibility could not be established",
		"an unavailable check is not a passing one; the node stays unschedulable for the microvm profile")
}

// hostPrerequisites re-runs only the side-effect-free half of Probe: the
// host is Linux, /dev/kvm opens read-write, both executables are still
// trusted regular files that answer --version, the jailer chroot base is
// still a trusted directory, and the cgroup parent still exists. It never
// creates a directory, takes a lock, or recovers anything.
func (f *JailerFactory) hostPrerequisites(ctx context.Context, firecrackerPath, jailerPath string) error {
	if runtime.GOOS != "linux" {
		return fmt.Errorf("firecracker requires Linux KVM, current OS is %s", runtime.GOOS)
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	kvm, err := os.OpenFile(f.opts.KVM, os.O_RDWR, 0)
	if err != nil {
		return fmt.Errorf("open %s read-write: %w", f.opts.KVM, err)
	}
	if err := kvm.Close(); err != nil {
		return fmt.Errorf("close %s probe: %w", f.opts.KVM, err)
	}
	for name, path := range map[string]string{"firecracker": firecrackerPath, "jailer": jailerPath} {
		if err := trustedExecutable(path); err != nil {
			return fmt.Errorf("%s executable: %w", name, err)
		}
		if _, err := boundedCommand(ctx, path, "--version"); err != nil {
			return fmt.Errorf("%s version probe: %w", name, err)
		}
	}
	if err := trustedDirectory(f.opts.ChrootBase); err != nil {
		return fmt.Errorf("jailer base: %w", err)
	}
	cgroupRoot := filepath.Join("/sys/fs/cgroup", f.opts.CgroupParent)
	if st, err := os.Stat(cgroupRoot); err != nil || !st.IsDir() {
		return fmt.Errorf("cgroup parent %q is unavailable", cgroupRoot)
	}
	return nil
}

// Reprobe re-verifies the jailer factory's host inputs without touching the
// exclusive host lock, retained jails or retained cgroups. Probe does those
// once, before any machine exists; repeating them against a live node would
// kill running VMMs.
func (f *JailerFactory) Reprobe(ctx context.Context) error {
	f.probeMu.Lock()
	probed := f.probed
	firecrackerPath, jailerPath := f.opts.Firecracker, f.opts.Jailer
	expected := f.compat
	f.probeMu.Unlock()
	if !probed {
		return errors.New("jailer factory has not passed Probe")
	}
	if err := f.hostPrerequisites(ctx, firecrackerPath, jailerPath); err != nil {
		return err
	}
	compat, err := DetectCompatibility(ctx, firecrackerPath)
	if err != nil {
		return err
	}
	if compat != expected {
		return fmt.Errorf("firecracker host compatibility changed since start-up (was %+v, now %+v)", expected, compat)
	}
	return nil
}
