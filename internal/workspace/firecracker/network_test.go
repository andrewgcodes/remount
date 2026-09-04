//go:build !windows

package firecracker

import (
	"context"
	"net/netip"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"remount.dev/remount/internal/netns"
)

type testNetKernel struct {
	mu    sync.Mutex
	calls []string
}

func (k *testNetKernel) add(call string) { k.mu.Lock(); k.calls = append(k.calls, call); k.mu.Unlock() }
func (k *testNetKernel) joined() string {
	k.mu.Lock()
	defer k.mu.Unlock()
	return strings.Join(k.calls, ",")
}
func (*testNetKernel) Probe(context.Context) error { return nil }
func (k *testNetKernel) Validate(context.Context, string, netns.Link) error {
	k.add("validate")
	return nil
}
func (k *testNetKernel) CreateNamespace(context.Context, string) (string, error) {
	k.add("namespace")
	return "/run/remount/netns/test", nil
}
func (k *testNetKernel) CreateVeth(context.Context, string, string, string) error {
	k.add("veth")
	return nil
}
func (k *testNetKernel) Configure(context.Context, string, netns.Link) error {
	k.add("configure")
	return nil
}
func (k *testNetKernel) InstallDenyAll(context.Context, string) error { k.add("deny"); return nil }
func (k *testNetKernel) PermitBroker(context.Context, string, netip.AddrPort) error {
	k.add("permit")
	return nil
}
func (k *testNetKernel) BringUp(context.Context, string, string) error { k.add("up"); return nil }
func (k *testNetKernel) DeleteVeth(context.Context, string) error      { k.add("delete-veth"); return nil }
func (k *testNetKernel) CloseNamespace(string) error                   { k.add("close-ns"); return nil }
func (k *testNetKernel) CreateTap(context.Context, string, string, int, int, netip.Prefix) error {
	k.add("tap")
	return nil
}
func (k *testNetKernel) DeleteTap(context.Context, string, string) error {
	k.add("delete-tap")
	return nil
}

func TestSystemNetworkProviderDenyFirstAndSynchronousRevoke(t *testing.T) {
	kernel := &testNetKernel{}
	provider, err := NewSystemNetworkProvider(t.Context(), NetworkOptions{Dir: filepath.Join(t.TempDir(), "net"), Manager: netns.NewManager(kernel), UID: 1000, GID: 1000})
	if err != nil {
		t.Fatal(err)
	}
	raw, err := provider.Reserve(t.Context(), "ws_network")
	if err != nil {
		t.Fatal(err)
	}
	lease := raw.(*systemNetworkLease)
	if _, err := lease.Prepare(t.Context(), 9); err != nil {
		t.Fatal(err)
	}
	broker := netip.AddrPortFrom(lease.HostAddress(), 7443)
	if err := lease.Activate(t.Context(), broker); err != nil {
		t.Fatal(err)
	}
	if err := lease.Revoke(t.Context()); err != nil {
		t.Fatal(err)
	}
	if got, want := kernel.joined(), "namespace,veth,configure,deny,tap,permit,up,delete-tap,delete-veth,close-ns"; got != want {
		t.Fatalf("network ordering = %s, want %s", got, want)
	}
	entries, err := filepath.Glob(filepath.Join(provider.opts.Dir, "*.json"))
	if err != nil || len(entries) != 0 {
		t.Fatalf("network journal retained after revoke: %v, %v", entries, err)
	}
}

func TestSystemNetworkProviderRecoversCrashLeftJournal(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "net")
	kernel := &testNetKernel{}
	first, err := NewSystemNetworkProvider(t.Context(), NetworkOptions{Dir: dir, Manager: netns.NewManager(kernel), UID: 1000, GID: 1000})
	if err != nil {
		t.Fatal(err)
	}
	raw, err := first.Reserve(t.Context(), "ws_crash")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := raw.Prepare(t.Context(), 4); err != nil {
		t.Fatal(err)
	}
	if _, err := NewSystemNetworkProvider(t.Context(), NetworkOptions{Dir: dir, Manager: netns.NewManager(kernel), UID: 1000, GID: 1000}); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(kernel.joined(), "validate,delete-tap,delete-veth,close-ns") {
		t.Fatalf("stale boundary was not synchronously recovered: %s", kernel.joined())
	}
}
