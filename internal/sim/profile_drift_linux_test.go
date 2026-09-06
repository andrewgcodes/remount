//go:build linux

package sim

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"remount.dev/remount/internal/node"
	"remount.dev/remount/internal/profile"
	"remount.dev/remount/internal/proto"
	"remount.dev/remount/internal/workspace"
	"remount.dev/remount/internal/workspace/gvisor"
)

// TestE26ProfileDriftMakesNodeUnschedulable is the exact-host half of the
// drift model (evidence gate E26). TestProfileDriftMakesNodeUnschedulable
// proves the control loop against a backend whose Reprobe answers a script;
// this proves the same loop against a real gVisor backend whose real host
// prerequisite is removed out of band, which is the only version of the claim
// an operator can act on.
//
// # Why these levers
//
// gvisor.Backend.Reprobe re-verifies three things: runsc still answers
// --version, the immutable rootfs is still a directory, and the kernel still
// grants the namespace and netlink privileges deny-first networking needs.
// The third is the one the gap brief names, and it is the one a test cannot
// take away and give back: netns.systemKernel.Probe reads this process's
// effective CAP_NET_ADMIN and CAP_SYS_ADMIN, and a process that drops a
// capability cannot restore it, so a test using that lever could prove drift
// and never prove recovery.
//
// So the test takes away the other two, through symlinks it owns rather than
// the shared host paths: the node is constructed against
// $TMPDIR/runsc -> $(command -v runsc) and $TMPDIR/rootfs -> $REMOUNT_GVISOR_ROOTFS,
// and removing one of those links is exactly the host mutation Reprobe is
// there to notice — the binary the sandbox is started with, or the image it
// is started from, stopped existing while the node was serving. Restoring the
// link restores the prerequisite, so both transitions are observable.
//
// What it asserts, per lever: node.profile.unschedulable is emitted naming
// the failing check, node.profile.get reports fail for
// multi-tenant-isolated, a workspace requiring that profile stays pending
// with pending_reason profile_unschedulable, and after the prerequisite comes
// back node.profile.restored is emitted and the parked workspace is placed.
func TestE26ProfileDriftMakesNodeUnschedulable(t *testing.T) {
	if testing.Short() || os.Getenv("REMOUNT_GVISOR_INTEGRATION") != "1" {
		t.Skip("unavailable: set REMOUNT_GVISOR_INTEGRATION=1 on a privileged Linux host with runsc")
	}
	if os.Geteuid() != 0 {
		t.Skip("unavailable: gVisor drift detection requires root for runsc, netns and nftables")
	}
	rootfs := os.Getenv("REMOUNT_GVISOR_ROOTFS")
	if rootfs == "" {
		t.Skip("unavailable: REMOUNT_GVISOR_ROOTFS is not set")
	}
	runsc, err := exec.LookPath("runsc")
	if err != nil {
		t.Skip("unavailable: runsc is not on PATH")
	}

	// The links this test owns. Breaking a link never touches the shared
	// runsc install or the shared rootfs, so a failed run cannot leave the
	// host less able to run the next one.
	links := t.TempDir()
	runscLink := filepath.Join(links, "runsc")
	rootfsLink := filepath.Join(links, "rootfs")
	for _, l := range []struct{ from, to string }{{runsc, runscLink}, {rootfs, rootfsLink}} {
		if err := os.Symlink(l.from, l.to); err != nil {
			t.Fatalf("linking %s: %v", l.to, err)
		}
	}

	ctx := ctxT(t, 5*time.Minute)
	w := newWorld(t)
	backendCtx, cancelBackend := context.WithTimeout(ctx, 60*time.Second)
	defer cancelBackend()
	backend, err := gvisor.New(backendCtx, gvisor.Options{
		Dir: t.TempDir(), RootFS: rootfsLink, Runsc: runscLink,
	})
	if err != nil {
		t.Fatalf("gvisor backend unavailable in required lane: %v", err)
	}
	n := w.nodeWith("gv-drift", func(o *node.Options) {
		o.Backends = workspace.NewRegistry(backend)
		o.Profile = string(profile.MultiTenantIsolated)
		o.ProfileHealthInterval = 500 * time.Millisecond
	})
	c := w.client("gv-drift-client")

	// A gVisor-only node claiming multi-tenant-isolated must start out
	// satisfying it; if it does not, the rest of the test would be proving
	// drift from a state that was already broken.
	waitProfileStatus(t, ctx, c, n.ID(), proto.CheckPass)
	waitNodeEvent(t, ctx, c, n.ID(), proto.EvNodeProfileVerified)

	for _, lever := range []struct {
		name  string
		link  string
		to    string
		check string
	}{
		{"the sandbox runtime is removed", runscLink, runsc, gvisor.CheckRunsc},
		{"the immutable rootfs is removed", rootfsLink, rootfs, gvisor.CheckRootFS},
	} {
		t.Run(lever.name, func(t *testing.T) {
			if err := os.Remove(lever.link); err != nil {
				t.Fatalf("breaking %s out of band: %v", lever.link, err)
			}
			restored := false
			defer func() {
				if !restored {
					_ = os.Symlink(lever.to, lever.link)
				}
			}()

			report := waitProfileStatus(t, ctx, c, n.ID(), proto.CheckFail)
			if !reportNamesFailingCheck(report, lever.check) {
				t.Fatalf("node.profile.get reports fail but names no failing check %q: %+v", lever.check, report.Checks)
			}
			ev := waitNodeEvent(t, ctx, c, n.ID(), proto.EvNodeProfileUnschedulable)
			var payload proto.NodeProfileEvent
			if err := proto.Unmarshal(ev.Payload, &payload); err != nil {
				t.Fatal(err)
			}
			if payload.Profile != string(profile.MultiTenantIsolated) || len(payload.Failed) == 0 {
				t.Fatalf("unschedulable payload %+v names no failing check", payload)
			}

			// The point of the whole loop: the node stops receiving work that
			// depends on the boundary it just lost.
			parked, err := c.CreateWorkspace(ctx, proto.WorkspaceSpec{
				Requires: proto.Requires{Profile: string(profile.MultiTenantIsolated)},
			})
			if err != nil {
				t.Fatal(err)
			}
			deadline := time.Now().Add(15 * time.Second)
			var got *proto.Workspace
			for time.Now().Before(deadline) {
				got, err = c.GetWorkspace(ctx, parked.ID)
				if err != nil {
					t.Fatal(err)
				}
				if got.PendingReason == proto.ReasonProfileUnschedulable {
					break
				}
				time.Sleep(100 * time.Millisecond)
			}
			if got.State != proto.WSPending || got.PendingReason != proto.ReasonProfileUnschedulable {
				t.Fatalf("a drifted node took profile-constrained work: state=%s node=%s pending_reason=%q",
					got.State, got.Node, got.PendingReason)
			}

			if err := os.Symlink(lever.to, lever.link); err != nil {
				t.Fatalf("restoring %s: %v", lever.link, err)
			}
			restored = true
			waitProfileStatus(t, ctx, c, n.ID(), proto.CheckPass)
			waitNodeEvent(t, ctx, c, n.ID(), proto.EvNodeProfileRestored)
			if _, err := c.WaitClaimed(ctx, parked.ID); err != nil {
				t.Fatalf("the parked workspace was not placed after the prerequisite came back: %v", err)
			}
			if err := c.DestroyWorkspace(ctx, parked.ID); err != nil {
				t.Fatalf("cleaning up the parked workspace: %v", err)
			}
		})
	}
}

func reportNamesFailingCheck(report proto.NodeProfileReport, check string) bool {
	for _, c := range report.Checks {
		if c.Status != proto.CheckFail {
			continue
		}
		// The runtime-health obligation folds the backend's own check names
		// into its subject, which is what makes a fleet report readable
		// without a second round trip.
		if c.Check == check || strings.Contains(c.Subject, check) || strings.Contains(c.Detail, check) {
			return true
		}
	}
	return false
}
