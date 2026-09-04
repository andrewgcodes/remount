package gvisor

import (
	"context"
	"encoding/json"
	"errors"
	"net/netip"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"remount.dev/remount/internal/netns"
	"remount.dev/remount/internal/proto"
	"remount.dev/remount/internal/session"
	"remount.dev/remount/internal/workspace"
)

var (
	_ workspace.Backend           = (*Backend)(nil)
	_ workspace.NetworkController = (*handle)(nil)
)

type callLog struct {
	mu    sync.Mutex
	calls []string
}

func (l *callLog) add(call string) { l.mu.Lock(); l.calls = append(l.calls, call); l.mu.Unlock() }
func (l *callLog) contains(call string) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	for _, got := range l.calls {
		if got == call {
			return true
		}
	}
	return false
}

type fakeKernel struct {
	log  *callLog
	fail string
}

func (f *fakeKernel) call(name string) error {
	f.log.add(name)
	if f.fail == name {
		return errors.New("injected " + name)
	}
	return nil
}
func (f *fakeKernel) Probe(context.Context) error                        { return f.call("probe") }
func (f *fakeKernel) Validate(context.Context, string, netns.Link) error { return f.call("validate") }
func (f *fakeKernel) CreateNamespace(context.Context, string) (string, error) {
	return "/run/remount/netns/fake", f.call("namespace")
}
func (f *fakeKernel) CreateVeth(context.Context, string, string, string) error { return f.call("veth") }
func (f *fakeKernel) Configure(context.Context, string, netns.Link) error      { return f.call("configure") }
func (f *fakeKernel) InstallDenyAll(context.Context, string, netns.Link) error {
	return f.call("deny")
}
func (f *fakeKernel) PermitBroker(context.Context, string, netns.Link, netip.AddrPort) error {
	return f.call("permit")
}
func (f *fakeKernel) BringUp(context.Context, string, string) error { return f.call("up") }
func (f *fakeKernel) DeleteVeth(context.Context, string) error      { return f.call("delete") }
func (f *fakeKernel) CloseNamespace(string) error                   { return f.call("close") }

type fakeRuntime struct {
	log          *callLog
	failCreate   bool
	root, binary string
}

func (r *fakeRuntime) probe(context.Context) error { r.log.add("runsc-probe"); return nil }

// reapStateRoot records the call so ordering is observable: the real one must
// run before any workspace exists, since that is what makes "everything here is
// a leftover" true.
func (r *fakeRuntime) reapStateRoot(context.Context) (int, error) {
	r.log.add("runsc-reap")
	return 0, nil
}
func (r *fakeRuntime) createStart(context.Context, string, string) error {
	r.log.add("runsc-create")
	if r.failCreate {
		return errors.New("injected runsc create")
	}
	return nil
}
func (r *fakeRuntime) pause(context.Context, string) error   { r.log.add("pause"); return nil }
func (r *fakeRuntime) resume(context.Context, string) error  { r.log.add("resume"); return nil }
func (r *fakeRuntime) destroy(context.Context, string) error { r.log.add("destroy"); return nil }
func (r *fakeRuntime) binaryPath() string                    { return r.binary }
func (r *fakeRuntime) stateRoot() string                     { return r.root }

func testBackend(t *testing.T, kernel *fakeKernel, runtime *fakeRuntime) *Backend {
	t.Helper()
	dir := t.TempDir()
	rootfs := filepath.Join(dir, "rootfs")
	if err := os.Mkdir(rootfs, 0o755); err != nil {
		t.Fatal(err)
	}
	return &Backend{dir: filepath.Join(dir, "workspaces"), rootfs: rootfs, network: netns.NewManager(kernel), runtime: runtime}
}

func TestCapabilitiesAreTheEnforcedBoundary(t *testing.T) {
	b := &Backend{}
	caps := b.Caps()
	if caps.Isolation != "container" || !caps.SiblingIsolation || !caps.NetworkNamespace || !caps.DeviceIsolation {
		t.Fatalf("incomplete isolation caps: %+v", caps)
	}
	if caps.EgressMode != "enforced_gateway" || !caps.EgressEnforced || caps.BrokerIdentity != "per_session_capability" {
		t.Fatalf("incomplete gateway caps: %+v", caps)
	}
}

func TestApplyStartsSandboxDeniedThenPermitsOnlyBroker(t *testing.T) {
	log := &callLog{}
	kernel := &fakeKernel{log: log}
	runtime := &fakeRuntime{log: log, root: "/state", binary: "/usr/bin/runsc"}
	b := testBackend(t, kernel, runtime)
	if err := os.MkdirAll(b.dir, 0o700); err != nil {
		t.Fatal(err)
	}
	raw, err := b.Create(context.Background(), "ws_one", proto.WorkspaceSpec{}, nil)
	if err != nil {
		t.Fatal(err)
	}
	h := raw.(*handle)
	host := h.BrokerAdvertiseHost()
	endpoint := workspace.NetworkEndpoint{Workspace: "ws_one", Generation: 9, ReverseProxyURL: "http://" + host + ":7443/reverse", ForwardProxyURL: "http://" + host + ":7443"}
	if err := h.ApplyNetworkPolicy(context.Background(), proto.NetworkPolicy{Default: proto.NetworkDefaultDeny}, endpoint); err != nil {
		t.Fatal(err)
	}
	want := []string{"namespace", "veth", "configure", "deny", "runsc-create", "permit", "up"}
	if strings.Join(log.calls, ",") != strings.Join(want, ",") {
		t.Fatalf("calls = %v, want %v", log.calls, want)
	}
	data, err := os.ReadFile(filepath.Join(h.bundle, "config.json"))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(data), `"type": "network"`) || !strings.Contains(string(data), `"readonly": true`) {
		t.Fatalf("OCI config does not carry the isolated namespace and immutable rootfs:\n%s", data)
	}
	var spec session.Spec
	spec.Kind = proto.SessionExec
	spec.Program = []string{"echo", "ok"}
	if err := h.Prepare(&spec); err != nil {
		t.Fatal(err)
	}
	if len(spec.Program) < 4 || spec.Program[0] != "/usr/bin/runsc" || !strings.Contains(strings.Join(spec.Program, " "), "remount-ws-one echo ok") {
		t.Fatalf("prepared program = %v", spec.Program)
	}
}

func TestApplyFailureRevokesBeforeReturning(t *testing.T) {
	log := &callLog{}
	kernel := &fakeKernel{log: log, fail: "permit"}
	runtime := &fakeRuntime{log: log, root: "/state", binary: "/usr/bin/runsc"}
	b := testBackend(t, kernel, runtime)
	if err := os.MkdirAll(b.dir, 0o700); err != nil {
		t.Fatal(err)
	}
	raw, err := b.Create(context.Background(), "ws_one", proto.WorkspaceSpec{}, nil)
	if err != nil {
		t.Fatal(err)
	}
	h := raw.(*handle)
	host := h.BrokerAdvertiseHost()
	err = h.ApplyNetworkPolicy(context.Background(), proto.NetworkPolicy{}, workspace.NetworkEndpoint{
		Workspace: "ws_one", Generation: 9, ReverseProxyURL: "http://" + host + ":7443", ForwardProxyURL: "http://" + host + ":7443",
	})
	if err == nil {
		t.Fatal("ApplyNetworkPolicy succeeded after nft failure")
	}
	if !log.contains("delete") || !log.contains("close") || !log.contains("destroy") {
		t.Fatalf("failure did not synchronously close boundary: %v", log.calls)
	}
	if err := h.Prepare(&session.Spec{Kind: proto.SessionExec}); err == nil {
		t.Fatal("failed network left sandbox serviceable")
	}
}

func TestWrongGenerationCannotReplaceActiveBoundary(t *testing.T) {
	log := &callLog{}
	kernel := &fakeKernel{log: log}
	runtime := &fakeRuntime{log: log, root: "/state", binary: "/usr/bin/runsc"}
	b := testBackend(t, kernel, runtime)
	if err := os.MkdirAll(b.dir, 0o700); err != nil {
		t.Fatal(err)
	}
	raw, err := b.Create(context.Background(), "ws_one", proto.WorkspaceSpec{}, nil)
	if err != nil {
		t.Fatal(err)
	}
	h := raw.(*handle)
	host := h.BrokerAdvertiseHost()
	first := workspace.NetworkEndpoint{Workspace: "ws_one", Generation: 9, ReverseProxyURL: "http://" + host + ":7443", ForwardProxyURL: "http://" + host + ":7443"}
	if err := h.ApplyNetworkPolicy(context.Background(), proto.NetworkPolicy{}, first); err != nil {
		t.Fatal(err)
	}
	first.Generation++
	if err := h.ApplyNetworkPolicy(context.Background(), proto.NetworkPolicy{}, first); err == nil {
		t.Fatal("stale handle accepted a new generation")
	}
}

func TestAdoptRebuildsNetworkAfterStartupReap(t *testing.T) {
	log := &callLog{}
	kernel := &fakeKernel{log: log}
	runtime := &fakeRuntime{log: log, root: "/state", binary: "/usr/bin/runsc"}
	b := testBackend(t, kernel, runtime)
	bundle := filepath.Join(b.dir, "ws_one")
	if err := os.MkdirAll(filepath.Join(bundle, "work"), 0o755); err != nil {
		t.Fatal(err)
	}
	data, err := json.Marshal(metadata{
		Generation: 9,
		Mount:      "/workspace",
		Network: netns.State{
			Slot:      7,
			Namespace: "/run/remount/netns/rm-7-9",
			Link:      netns.Link{HostName: "rmh0007", GuestName: "rmg0007"},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(bundle, metadataName), data, 0o600); err != nil {
		t.Fatal(err)
	}

	if _, err := b.Adopt(context.Background(), "ws_one"); err != nil {
		t.Fatal(err)
	}
	want := []string{"destroy", "namespace", "veth", "configure", "deny"}
	if strings.Join(log.calls, ",") != strings.Join(want, ",") {
		t.Fatalf("calls = %v, want %v", log.calls, want)
	}
}
