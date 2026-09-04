//go:build !windows

package firecracker

import (
	"context"
	"encoding/json"
	"errors"
	"net/netip"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"
)

type apiCall struct {
	method string
	path   string
	body   map[string]any
}

type recordingAPI struct {
	mu    sync.Mutex
	calls []apiCall
	root  string
	fail  string
}

func (a *recordingAPI) Put(_ context.Context, path string, body any) error {
	if err := a.record("PUT", path, body); err != nil {
		return err
	}
	if path == "/snapshot/create" {
		dir := filepath.Join(a.root, "snapshots")
		if err := os.WriteFile(filepath.Join(dir, "vm.state"), []byte("state"), 0o600); err != nil {
			return err
		}
		if err := os.WriteFile(filepath.Join(dir, "vm.mem"), []byte("memory"), 0o600); err != nil {
			return err
		}
	}
	return nil
}

func (a *recordingAPI) Patch(_ context.Context, path string, body any) error {
	return a.record("PATCH", path, body)
}

func (a *recordingAPI) Get(_ context.Context, path string, body any) error {
	return a.record("GET", path, body)
}

func (a *recordingAPI) record(method, path string, body any) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.fail == path {
		return errors.New("injected API failure")
	}
	data, _ := json.Marshal(body)
	var decoded map[string]any
	_ = json.Unmarshal(data, &decoded)
	a.calls = append(a.calls, apiCall{method: method, path: path, body: decoded})
	return nil
}

func (a *recordingAPI) call(path string) (apiCall, bool) {
	a.mu.Lock()
	defer a.mu.Unlock()
	for _, call := range a.calls {
		if call.path == path {
			return call, true
		}
	}
	return apiCall{}, false
}

func TestJailerMachineUsesFullSnapshotAndPrebootLoadSchemas(t *testing.T) {
	dir := t.TempDir()
	root := filepath.Join(dir, "root")
	if err := os.MkdirAll(root, 0o700); err != nil {
		t.Fatal(err)
	}
	kernel := filepath.Join(dir, "vmlinux")
	drive := filepath.Join(dir, "rootfs.ext4")
	state := filepath.Join(dir, "restore.state")
	memory := filepath.Join(dir, "restore.mem")
	for path, value := range map[string]string{kernel: "kernel", drive: "drive", state: "state", memory: "memory"} {
		if err := os.WriteFile(path, []byte(value), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	api := &recordingAPI{root: root}
	machine := &jailerMachine{workspace: "ws_one", root: root, api: api, maxVCPU: 4, maxMemMiB: 1024, uid: os.Getuid(), gid: os.Getgid(), fileSizeLimit: 1 << 20}
	if err := machine.Configure(context.Background(), MachineConfig{KernelImage: kernel, RootDrive: drive, TapName: "tap0", GuestIP: netip.MustParseAddr("172.31.0.2"), GatewayIP: netip.MustParseAddr("172.31.0.1"), VCPU: 2, MemMiB: 256, TrackDirty: true}); err != nil {
		t.Fatal(err)
	}
	machineConfig, ok := api.call("/machine-config")
	if !ok || machineConfig.body["track_dirty_pages"] != true || machineConfig.body["vcpu_count"] != float64(2) {
		t.Fatalf("machine config=%#v", machineConfig)
	}
	if err := machine.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := machine.Pause(context.Background()); err != nil {
		t.Fatal(err)
	}
	files, err := machine.CreateSnapshot(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	snapshot, ok := api.call("/snapshot/create")
	if !ok || snapshot.body["snapshot_type"] != "Full" || files.State == "" || files.Memory == "" {
		t.Fatalf("snapshot=%#v files=%+v", snapshot, files)
	}

	restoredRoot := filepath.Join(dir, "restored-root")
	if err := os.MkdirAll(restoredRoot, 0o700); err != nil {
		t.Fatal(err)
	}
	restoreAPI := &recordingAPI{root: restoredRoot}
	restored := &jailerMachine{workspace: "ws_one", root: restoredRoot, api: restoreAPI, uid: os.Getuid(), gid: os.Getgid(), fileSizeLimit: 1 << 20}
	if err := restored.LoadSnapshot(context.Background(), SnapshotFiles{State: state, Memory: memory}, drive, "tap-restored"); err != nil {
		t.Fatal(err)
	}
	load, ok := restoreAPI.call("/snapshot/load")
	backend, _ := load.body["mem_backend"].(map[string]any)
	_, diffRequested := load.body["enable_diff_snapshots"]
	vsock, _ := load.body["vsock_override"].(map[string]any)
	if !ok || load.body["resume_vm"] != false || diffRequested || backend["backend_type"] != "File" || vsock["uds_path"] != "/run/guest.vsock" {
		t.Fatalf("load=%#v", load)
	}
	if _, configured := restoreAPI.call("/machine-config"); configured {
		t.Fatal("restore configured a fresh machine before snapshot load")
	}
}

func TestJailerMachineRejectsUnboundedOrDirtyTrackingDisabled(t *testing.T) {
	machine := &jailerMachine{maxVCPU: 2, maxMemMiB: 512}
	for _, cfg := range []MachineConfig{
		{VCPU: 3, MemMiB: 256, TrackDirty: true},
		{VCPU: 1, MemMiB: 1024, TrackDirty: true},
		{VCPU: 1, MemMiB: 256, TrackDirty: false},
	} {
		if err := machine.Configure(context.Background(), cfg); err == nil {
			t.Fatalf("unbounded config accepted: %+v", cfg)
		}
	}
}

func TestJailerFactoryArgumentsAreJailedBoundedAndNamespaced(t *testing.T) {
	factory, err := NewJailerFactory(JailerOptions{
		Firecracker: "/usr/bin/firecracker", Jailer: "/usr/bin/jailer", ChrootBase: "/srv/remount-jailer",
		UID: 123, GID: 456, CgroupParent: "remount", Cgroups: []string{"memory.max=536870912", "cpu.max=100000 100000"},
		NoFileLimit: 1024, FileSizeLimit: 1 << 30,
	})
	if err != nil {
		t.Fatal(err)
	}
	joined := strings.Join(factory.jailerArgs("rm-test", "/var/run/netns/ws"), " ")
	for _, required := range []string{"--exec-file /usr/bin/firecracker", "--netns /var/run/netns/ws", "--new-pid-ns", "--resource-limit no-file=1024", "--resource-limit fsize=1073741824", "--cgroup memory.max=536870912"} {
		if !strings.Contains(joined, required) {
			t.Fatalf("missing %q in %s", required, joined)
		}
	}
	if strings.Contains(joined, "--daemonize") {
		t.Fatalf("daemonized VMM cannot be joined through cmd.Wait: %s", joined)
	}
}

func TestJailerFactoryProbeIsUnavailableOffLinux(t *testing.T) {
	if runtime.GOOS == "linux" {
		t.Skip("non-Linux assertion")
	}
	factory, err := NewJailerFactory(JailerOptions{UID: 123, GID: 456, CgroupParent: "remount", Cgroups: []string{"memory.max=1"}})
	if err != nil {
		t.Fatal(err)
	}
	if err := factory.Probe(context.Background()); err == nil || !strings.Contains(err.Error(), "requires Linux KVM") {
		t.Fatalf("probe err=%v", err)
	}
}

func TestJailerMachineKillJoinsBeforeCleanupAndCapacityRelease(t *testing.T) {
	base := t.TempDir()
	root := filepath.Join(base, "firecracker", "rm-test", "root")
	if err := os.MkdirAll(root, 0o700); err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command("sh", "-c", "while :; do sleep 1; done")
	configureProcessGroup(cmd)
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	factory := &JailerFactory{opts: JailerOptions{ChrootBase: base}, active: make(map[string]*jailerMachine)}
	machine := &jailerMachine{factory: factory, workspace: "ws_one", root: root, cmd: cmd, done: make(chan struct{}), waitErr: make(chan error, 1)}
	factory.active["ws_one"] = machine
	go func() {
		machine.waitErr <- cmd.Wait()
		close(machine.done)
	}()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := machine.Kill(ctx); err != nil {
		t.Fatal(err)
	}
	select {
	case <-machine.done:
	default:
		t.Fatal("Kill returned before cmd.Wait joined")
	}
	if _, err := os.Stat(filepath.Dir(root)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("jail survived joined cleanup: %v", err)
	}
	factory.mu.Lock()
	_, retained := factory.active["ws_one"]
	factory.mu.Unlock()
	if retained {
		t.Fatal("machine admission remained after joined cleanup")
	}
	if err := machine.Kill(context.Background()); err != nil {
		t.Fatalf("idempotent kill: %v", err)
	}
}

func FuzzCgroupValidation(f *testing.F) {
	for _, seed := range []string{"memory.max=536870912", "cpu.max=100000 100000", "", "../x=1"} {
		f.Add(seed)
	}
	f.Fuzz(func(t *testing.T, input string) {
		err := validateCgroup(input)
		if err == nil {
			name, value, ok := strings.Cut(input, "=")
			if !ok || name == "" || value == "" || strings.ContainsAny(name, "/\\") {
				t.Fatalf("accepted invalid cgroup %q", input)
			}
		}
	})
}
