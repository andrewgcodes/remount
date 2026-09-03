package firecracker

import (
	"bytes"
	"context"
	"io"
	"net/netip"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"remount.dev/remount/internal/fsops"
	"remount.dev/remount/internal/proto"
	"remount.dev/remount/internal/session"
	"remount.dev/remount/internal/workspace"
)

var _ workspace.Backend = (*Backend)(nil)
var _ workspace.NetworkController = (*handle)(nil)
var _ workspace.MemoryCheckpointer = (*handle)(nil)

type logbook struct {
	sync.Mutex
	calls []string
}

func (l *logbook) add(v string)   { l.Lock(); l.calls = append(l.calls, v); l.Unlock() }
func (l *logbook) joined() string { l.Lock(); defer l.Unlock(); return strings.Join(l.calls, ",") }

type fakeMachine struct{ log *logbook }

func (m *fakeMachine) Configure(context.Context, MachineConfig) error {
	m.log.add("configure")
	return nil
}
func (m *fakeMachine) Start(context.Context) error { m.log.add("start"); return nil }
func (m *fakeMachine) Pause(context.Context) error { m.log.add("pause"); return nil }
func (m *fakeMachine) CreateSnapshot(context.Context, SnapshotFiles) error {
	m.log.add("snapshot")
	return nil
}
func (m *fakeMachine) LoadSnapshot(context.Context, SnapshotFiles) error {
	m.log.add("load")
	return nil
}
func (m *fakeMachine) Resume(context.Context) error { m.log.add("resume"); return nil }
func (m *fakeMachine) Kill(context.Context) error   { m.log.add("kill"); return nil }

type fakeMachines struct {
	log     *logbook
	machine *fakeMachine
}

func (f *fakeMachines) Probe(context.Context) error { f.log.add("probe-machine"); return nil }
func (*fakeMachines) Jailed() bool                  { return true }
func (f *fakeMachines) New(context.Context, string) (Machine, error) {
	f.log.add("new-machine")
	return f.machine, nil
}

type fakeNetwork struct {
	log         *logbook
	host, guest netip.Addr
}

func (n *fakeNetwork) HostAddress() netip.Addr  { return n.host }
func (n *fakeNetwork) GuestAddress() netip.Addr { return n.guest }
func (n *fakeNetwork) Prepare(context.Context, uint64) (string, error) {
	n.log.add("net-prepare")
	return "tap0", nil
}
func (n *fakeNetwork) Activate(context.Context, netip.AddrPort) error {
	n.log.add("net-activate")
	return nil
}
func (n *fakeNetwork) Revoke(context.Context) error { n.log.add("net-revoke"); return nil }

type fakeNetworks struct {
	log   *logbook
	lease *fakeNetwork
}

func (n *fakeNetworks) Probe(context.Context) error { n.log.add("probe-network"); return nil }
func (n *fakeNetworks) Reserve(context.Context, string) (NetworkLease, error) {
	n.log.add("reserve-network")
	return n.lease, nil
}

type fakeVolume struct {
	log              *logbook
	fs               *fsops.FS
	memory           bool
	entered, release chan struct{}
}

func (v *fakeVolume) FS() *fsops.FS              { return v.fs }
func (*fakeVolume) ImagePath() string            { return "/jail/rootfs.ext4" }
func (*fakeVolume) SnapshotFiles() SnapshotFiles { return SnapshotFiles{"vm.state", "vm.mem"} }
func (v *fakeVolume) HasMemorySnapshot() bool    { return v.memory }
func (v *fakeVolume) Snapshot(context.Context, []string, io.Writer) error {
	v.log.add("fs-snapshot")
	return nil
}
func (v *fakeVolume) Checkpoint(_ context.Context, _ []string, _ SnapshotFiles, _ io.Writer) error {
	v.log.add("archive")
	if v.entered != nil {
		close(v.entered)
		<-v.release
	}
	return nil
}
func (v *fakeVolume) Destroy(context.Context) error { v.log.add("volume-destroy"); return nil }

type fakeVolumes struct {
	log    *logbook
	volume *fakeVolume
}

func (v *fakeVolumes) Probe(context.Context, string) error { v.log.add("probe-volume"); return nil }
func (v *fakeVolumes) Create(context.Context, string, string, io.Reader) (Volume, error) {
	v.log.add("volume-create")
	return v.volume, nil
}
func (v *fakeVolumes) Adopt(context.Context, string) (Volume, error) {
	v.log.add("volume-adopt")
	return v.volume, nil
}

type fakeGuest struct{ log *logbook }

func (g *fakeGuest) Probe(context.Context) error { g.log.add("probe-guest"); return nil }
func (g *fakeGuest) Prepare(s *session.Spec, _ GuestEndpoint) error {
	g.log.add("guest-prepare")
	s.Program = []string{"guest-agent"}
	return nil
}

func fixture(t *testing.T, memory bool) (*Backend, *logbook) {
	t.Helper()
	dir := t.TempDir()
	kernel := filepath.Join(dir, "vmlinux")
	base := filepath.Join(dir, "base.ext4")
	if err := os.WriteFile(kernel, []byte("k"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(base, []byte("d"), 0600); err != nil {
		t.Fatal(err)
	}
	log := &logbook{}
	tree := filepath.Join(dir, "tree")
	if err := os.Mkdir(tree, 0o700); err != nil {
		t.Fatal(err)
	}
	fs, err := fsops.New(tree)
	if err != nil {
		t.Fatal(err)
	}
	machine := &fakeMachine{log}
	network := &fakeNetwork{log: log, host: netip.MustParseAddr("169.254.9.1"), guest: netip.MustParseAddr("169.254.9.2")}
	opts := Options{Dir: filepath.Join(dir, "data"), KernelImage: kernel, BaseRootFS: base, Machines: &fakeMachines{log, machine}, Networks: &fakeNetworks{log, network}, Volumes: &fakeVolumes{log, &fakeVolume{log: log, fs: fs, memory: memory}}, Guest: &fakeGuest{log}}
	b, err := New(context.Background(), opts)
	if err != nil {
		t.Fatal(err)
	}
	return b, log
}

func endpoint() workspace.NetworkEndpoint {
	return workspace.NetworkEndpoint{Workspace: "ws_one", Generation: 7, ReverseProxyURL: "http://169.254.9.1:7443/r", ForwardProxyURL: "http://169.254.9.1:7443"}
}

func TestFreshLifecycleOrdering(t *testing.T) {
	b, log := fixture(t, false)
	raw, err := b.Create(context.Background(), "ws_one", proto.WorkspaceSpec{}, nil)
	if err != nil {
		t.Fatal(err)
	}
	h := raw.(*handle)
	if err := h.ApplyNetworkPolicy(context.Background(), proto.NetworkPolicy{}, endpoint()); err != nil {
		t.Fatal(err)
	}
	var out bytes.Buffer
	if err := h.Checkpoint(context.Background(), nil, &out); err != nil {
		t.Fatal(err)
	}
	if err := h.Destroy(context.Background()); err != nil {
		t.Fatal(err)
	}
	want := "probe-machine,probe-network,probe-volume,probe-guest,volume-create,reserve-network,net-prepare,new-machine,configure,start,net-activate,pause,snapshot,archive,resume,net-revoke,kill,volume-destroy"
	if got := log.joined(); got != want {
		t.Fatalf("calls=%s\nwant=%s", got, want)
	}
}

func TestRestoreLoadsBeforeResumeAndNeverStartsFresh(t *testing.T) {
	b, log := fixture(t, true)
	raw, err := b.Adopt(context.Background(), "ws_one")
	if err != nil {
		t.Fatal(err)
	}
	if err := raw.(*handle).ApplyNetworkPolicy(context.Background(), proto.NetworkPolicy{}, endpoint()); err != nil {
		t.Fatal(err)
	}
	got := log.joined()
	if !strings.Contains(got, "net-prepare,new-machine,load,resume,net-activate") {
		t.Fatalf("restore order: %s", got)
	}
	if strings.Contains(got, "configure") || strings.Contains(got, "start") {
		t.Fatalf("restore configured fresh VM: %s", got)
	}
}

func TestCheckpointJoinsArchiveBeforeResume(t *testing.T) {
	b, _ := fixture(t, false)
	raw, err := b.Create(context.Background(), "ws_one", proto.WorkspaceSpec{}, nil)
	if err != nil {
		t.Fatal(err)
	}
	h := raw.(*handle)
	if err := h.ApplyNetworkPolicy(context.Background(), proto.NetworkPolicy{}, endpoint()); err != nil {
		t.Fatal(err)
	}
	entered := make(chan struct{})
	release := make(chan struct{})
	h.volume.(*fakeVolume).entered = entered
	h.volume.(*fakeVolume).release = release
	done := make(chan error, 1)
	go func() { done <- h.Checkpoint(context.Background(), nil, io.Discard) }()
	<-entered
	if strings.HasSuffix(h.volume.(*fakeVolume).log.joined(), "resume") {
		t.Fatal("resume became observable before archive producer joined")
	}
	close(release)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if !strings.HasSuffix(h.volume.(*fakeVolume).log.joined(), "archive,resume") {
		t.Fatalf("producer was not joined: %s", h.volume.(*fakeVolume).log.joined())
	}
}

func TestCapsOnlyExistAfterAllProbes(t *testing.T) {
	b, _ := fixture(t, false)
	caps := b.Caps()
	if caps.Isolation != "microvm" || caps.Snapshots != "fs+mem" || caps.EgressMode != "enforced_gateway" || !caps.MultiTenant {
		t.Fatalf("caps=%+v", caps)
	}
	unverified := (&Backend{}).Caps()
	if unverified.Isolation != "none" || unverified.EgressMode != "open" || unverified.Snapshots != "fs" {
		t.Fatalf("unverified backend overclaimed caps: %+v", unverified)
	}
}
