//go:build linux

package netns

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"
)

// TestReclaimOrphansRemovesOnlyUninhabitedNamespaces is the whole contract, and
// both halves matter equally.
//
// Reclaiming too little is the bug this exists for: a namespace left by a
// killed node holds the slot a restarted node will ask for, so the restart
// collides with its own leftovers. Reclaiming too much would be far worse — it
// would tear the network out from under a workspace that is still running — so
// the test pins that a namespace with a process in it is left alone.
func TestReclaimOrphansRemovesOnlyUninhabitedNamespaces(t *testing.T) {
	if os.Geteuid() != 0 {
		t.Skip("unavailable: creating a network namespace requires root")
	}
	if _, err := exec.LookPath("ip"); err != nil {
		t.Skip("unavailable: iproute2 is not installed")
	}
	if err := os.MkdirAll(namespaceDir, 0o700); err != nil {
		t.Fatal(err)
	}

	// An orphan: a bind-mounted namespace with nothing running in it, which is
	// exactly what a killed process leaves.
	orphan := filepath.Join(namespaceDir, "rm-7ffe-1")
	makeNamespace(t, orphan)

	// An inhabited one, held open by a sleeping process in it.
	live := filepath.Join(namespaceDir, "rm-7ffd-1")
	makeNamespace(t, live)
	hold := exec.Command("nsenter", "--net="+live, "sleep", "60")
	if err := hold.Start(); err != nil {
		t.Skipf("unavailable: cannot hold a namespace open: %v", err)
	}
	defer func() {
		_ = hold.Process.Kill()
		_, _ = hold.Process.Wait()
		cleanupNamespace(live)
	}()
	waitForProcessNamespace(t, hold.Process.Pid, live)

	// The count is deliberately not asserted. Anything else on the host may
	// have left its own orphan, and a test that fails because an unrelated one
	// existed would be reporting the state of the machine rather than the
	// behaviour of this function. What matters is what happened to these two.
	if _, err := ReclaimOrphans(context.Background()); err != nil {
		t.Fatalf("ReclaimOrphans: %v", err)
	}
	if _, statErr := os.Stat(orphan); !os.IsNotExist(statErr) {
		t.Errorf("the uninhabited namespace survived, so a restarted node still collides with it (stat: %v)", statErr)
	}
	if _, statErr := os.Stat(live); statErr != nil {
		t.Errorf("a namespace with a live process in it was reclaimed, which would cut the network out from under a running workspace: %v", statErr)
	}
}

func waitForProcessNamespace(t *testing.T, pid int, namespace string) {
	t.Helper()
	want, err := os.Stat(namespace)
	if err != nil {
		t.Fatal(err)
	}
	procNamespace := filepath.Join("/proc", fmt.Sprint(pid), "ns", "net")
	for deadline := time.Now().Add(5 * time.Second); time.Now().Before(deadline); {
		got, statErr := os.Stat(procNamespace)
		if statErr == nil && os.SameFile(want, got) {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("process %d did not enter %s", pid, namespace)
}

func makeNamespace(t *testing.T, path string) {
	t.Helper()
	name := filepath.Base(path)
	if out, err := exec.Command("ip", "netns", "add", name).CombinedOutput(); err != nil {
		t.Skipf("unavailable: ip netns add: %s: %v", out, err)
	}
	// ip netns puts it under /run/netns; bind it where this package looks, the
	// same shape CreateNamespace produces.
	if err := os.WriteFile(path, nil, 0o600); err != nil && !os.IsExist(err) {
		t.Fatal(err)
	}
	if out, err := exec.Command("mount", "--bind", "/run/netns/"+name, path).CombinedOutput(); err != nil {
		t.Skipf("unavailable: bind namespace: %s: %v", out, err)
	}
	t.Cleanup(func() {
		cleanupNamespace(path)
		_ = exec.Command("ip", "netns", "del", name).Run()
	})
}

func cleanupNamespace(path string) {
	_ = exec.Command("umount", "-l", path).Run()
	_ = os.Remove(path)
}
