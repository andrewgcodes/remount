package gvisor

import (
	"context"
	"errors"
	"os"

	"remount.dev/remount/internal/proto"
	"remount.dev/remount/internal/workspace"
)

// Reprobe names of the gVisor host checks. They are stable identifiers a
// report matches on.
const (
	CheckRunsc   = "gvisor.runsc"
	CheckRootFS  = "gvisor.rootfs"
	CheckNetwork = "gvisor.network"
)

// Reprobe re-verifies the host prerequisites this backend's Caps depend on.
//
// It deliberately re-runs only the read-only subset of New: the runsc binary
// still answers, the immutable rootfs is still a directory, and the kernel
// still grants the namespace and netlink privileges the enforced-gateway
// network needs. It never calls reapStateRoot or netns.ReclaimOrphans. Those
// two are safe in the constructor precisely because no sandbox this process
// owns can exist yet; running them against a serving node would destroy live
// workspaces and reclaim the namespaces they are running in.
func (b *Backend) Reprobe(ctx context.Context) []proto.Finding {
	out := []proto.Finding{
		workspace.CheckFinding(CheckRunsc, b.runtime.binaryPath(),
			"runsc answers --version",
			"reinstall runsc or fix PATH; the node is advertising a sandbox it can no longer start",
			b.runtime.probe(ctx)),
		workspace.CheckFinding(CheckRootFS, b.rootfs,
			"the immutable rootfs is present",
			"restore REMOUNT_GVISOR_ROOTFS; without it no workspace can be materialized",
			rootfsPresent(b.rootfs)),
	}
	if b.network == nil {
		out = append(out, workspace.UnavailableFinding(CheckNetwork, "",
			"no network manager is configured for this backend",
			"an unavailable network check is not a passing one; the node stays unschedulable for isolated profiles"))
		return out
	}
	out = append(out, workspace.CheckFinding(CheckNetwork, "",
		"the host still grants deny-first network namespace setup",
		"restore CAP_NET_ADMIN/CAP_SYS_ADMIN and netlink access; enforced egress cannot be set up without them",
		b.network.Probe(ctx)))
	return out
}

func rootfsPresent(rootfs string) error {
	st, err := os.Stat(rootfs)
	if err != nil {
		return err
	}
	if !st.IsDir() {
		return errors.New("rootfs is not a directory")
	}
	return nil
}
