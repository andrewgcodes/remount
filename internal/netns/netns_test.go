package netns

import (
	"context"
	"errors"
	"net/netip"
	"reflect"
	"sync"
	"testing"
)

type fakeKernel struct {
	mu     sync.Mutex
	calls  []string
	failAt string
}

func (f *fakeKernel) call(name string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls = append(f.calls, name)
	if f.failAt == name {
		return errors.New("injected " + name)
	}
	return nil
}
func (f *fakeKernel) Probe(context.Context) error                  { return f.call("probe") }
func (f *fakeKernel) Validate(context.Context, string, Link) error { return f.call("validate") }
func (f *fakeKernel) CreateNamespace(context.Context, string) (string, error) {
	return "/proc/fake/ns", f.call("namespace")
}
func (f *fakeKernel) CreateVeth(context.Context, string, string, string) error { return f.call("veth") }
func (f *fakeKernel) Configure(context.Context, string, Link) error            { return f.call("configure") }
func (f *fakeKernel) InstallDenyAll(context.Context, string) error             { return f.call("deny") }
func (f *fakeKernel) PermitBroker(context.Context, string, netip.AddrPort) error {
	return f.call("permit")
}
func (f *fakeKernel) BringUp(context.Context, string, string) error { return f.call("up") }
func (f *fakeKernel) DeleteVeth(context.Context, string) error      { return f.call("delete") }
func (f *fakeKernel) CloseNamespace(string) error                   { return f.call("close") }

func TestOpenDeniesBeforeActivation(t *testing.T) {
	f := &fakeKernel{}
	n, err := NewManager(f).Open(context.Background(), "ws_one", 7)
	if err != nil {
		t.Fatal(err)
	}
	wantPrefix := []string{"namespace", "veth", "configure", "deny"}
	if !reflect.DeepEqual(f.calls, wantPrefix) {
		t.Fatalf("calls = %v, want %v", f.calls, wantPrefix)
	}
	broker := netip.AddrPortFrom(n.HostAddress(), 7443)
	if err := n.Apply(context.Background(), broker); err != nil {
		t.Fatal(err)
	}
	want := append(wantPrefix, "permit", "up")
	if !reflect.DeepEqual(f.calls, want) {
		t.Fatalf("calls = %v, want %v", f.calls, want)
	}
}

func TestApplyFailureDeletesVethBeforeReturning(t *testing.T) {
	f := &fakeKernel{failAt: "permit"}
	n, err := NewManager(f).Open(context.Background(), "ws_one", 7)
	if err != nil {
		t.Fatal(err)
	}
	err = n.Apply(context.Background(), netip.AddrPortFrom(n.HostAddress(), 7443))
	if err == nil {
		t.Fatal("Apply succeeded after rule-programming failure")
	}
	want := []string{"namespace", "veth", "configure", "deny", "permit", "delete", "close"}
	if !reflect.DeepEqual(f.calls, want) {
		t.Fatalf("calls = %v, want %v", f.calls, want)
	}
	if err := n.Apply(context.Background(), netip.AddrPortFrom(n.HostAddress(), 7443)); err == nil {
		t.Fatal("revoked network accepted a later policy")
	}
}

func TestOpenFailureCleansPartialBoundary(t *testing.T) {
	f := &fakeKernel{failAt: "deny"}
	_, err := NewManager(f).Open(context.Background(), "ws_one", 7)
	if err == nil {
		t.Fatal("Open succeeded after deny-all failure")
	}
	want := []string{"namespace", "veth", "configure", "deny", "delete", "close"}
	if !reflect.DeepEqual(f.calls, want) {
		t.Fatalf("calls = %v, want %v", f.calls, want)
	}
}

func TestRevokeRetriesDeletionAndDoesNotReuseLiveSlot(t *testing.T) {
	f := &fakeKernel{failAt: "delete"}
	m := NewManager(f)
	n, err := m.Open(context.Background(), "ws_one", 7)
	if err != nil {
		t.Fatal(err)
	}
	if err := n.Revoke(context.Background()); err == nil {
		t.Fatal("Revoke succeeded after delete failure")
	}
	f.failAt = ""
	if err := n.Revoke(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := n.Revoke(context.Background()); err != nil {
		t.Fatalf("idempotent Revoke: %v", err)
	}
}

func TestAdoptRejectsLinkNamesThatCouldDeleteAnotherInterface(t *testing.T) {
	f := &fakeKernel{}
	state := State{Slot: 4, Namespace: "/proc/fake/ns", Link: linkForSlot(4)}
	state.Link.HostName = "eth0"
	if _, err := NewManager(f).Adopt(context.Background(), state); err == nil {
		t.Fatal("Adopt accepted retained metadata naming an unrelated host interface")
	}
	if len(f.calls) != 0 {
		t.Fatalf("kernel touched for invalid retained state: %v", f.calls)
	}
}
