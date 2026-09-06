//go:build !windows

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

// TestAdoptRecoversATreeWhoseSandboxNeverStarted is the unit form of the
// wedge the live Colima gVisor drift lane hit on 2026-09-05.
//
// A materialization that fails inside ApplyNetworkPolicy — the sandbox would
// not start — has already created the bundle and its work tree, and the
// deferred cleanup revokes the boundary before the metadata file is ever
// written. The node then quarantines the workspace, deliberately retaining
// the local filesystem, and every retry arrives with adopt=true. Before this
// test, Create refused with CodeConflict because the bundle existed and Adopt
// refused with CodeNotFound because network.json did not, so the workspace
// could never be materialized again on that node: the retry loop reported
// "has no retained network metadata" forever, which is the wrong problem and
// one no operator can act on.
//
// A retained tree with no retained boundary is not a missing workspace. It is
// a workspace that was never started, so the honest recovery is a fresh
// boundary over the bytes that are already there.
func TestAdoptRecoversATreeWhoseSandboxNeverStarted(t *testing.T) {
	log := &callLog{}
	kernel := &fakeKernel{log: log}
	runtime := &fakeRuntime{log: log, root: "/state", binary: "/usr/bin/runsc", failCreate: true}
	b := testBackend(t, kernel, runtime)
	if err := os.MkdirAll(b.dir, 0o700); err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()

	raw, err := b.Create(ctx, "ws_one", proto.WorkspaceSpec{}, nil)
	if err != nil {
		t.Fatal(err)
	}
	h := raw.(*handle)
	if err := os.WriteFile(filepath.Join(h.bundle, "work", "keep.txt"), []byte("bytes only here"), 0o600); err != nil {
		t.Fatal(err)
	}
	host := h.BrokerAdvertiseHost()
	endpoint := workspace.NetworkEndpoint{Workspace: "ws_one", Generation: 3,
		ReverseProxyURL: "http://" + host + ":7443/reverse", ForwardProxyURL: "http://" + host + ":7443"}
	if err := h.ApplyNetworkPolicy(ctx, proto.NetworkPolicy{Default: proto.NetworkDefaultDeny}, endpoint); err == nil {
		t.Fatal("ApplyNetworkPolicy succeeded although the sandbox could not start")
	}

	// This is exactly the sequence internal/node.materializeWithReadyHook
	// runs on the retry: Adopt, then Create, then Adopt again on conflict.
	var pe *proto.Error
	if _, err := b.Create(ctx, "ws_one", proto.WorkspaceSpec{}, nil); err == nil {
		t.Fatal("Create accepted a workspace whose tree is still on disk")
	} else if !errors.As(err, &pe) || pe.Code != proto.CodeConflict {
		t.Fatalf("Create after a failed start = %v, want conflict", err)
	}

	runtime.failCreate = false
	adopted, err := b.Adopt(ctx, "ws_one")
	if err != nil {
		t.Fatalf("Adopt refused a retained tree whose sandbox never started: %v", err)
	}
	if got := adopted.(*handle).MountPath(); got != proto.DefaultMountPath {
		t.Fatalf("adopted mount path = %q, want %q", got, proto.DefaultMountPath)
	}
	body, err := os.ReadFile(filepath.Join(b.dir, "ws_one", "work", "keep.txt"))
	if err != nil || string(body) != "bytes only here" {
		t.Fatalf("adoption did not retain the workspace bytes: %q %v", body, err)
	}
	// The recovered handle must be serviceable rather than merely
	// constructed: the whole point of adopting is that the next
	// materialization succeeds.
	ah := adopted.(*handle)
	next := workspace.NetworkEndpoint{Workspace: "ws_one", Generation: 4,
		ReverseProxyURL: "http://" + ah.BrokerAdvertiseHost() + ":7443/reverse",
		ForwardProxyURL: "http://" + ah.BrokerAdvertiseHost() + ":7443"}
	if err := ah.ApplyNetworkPolicy(ctx, proto.NetworkPolicy{Default: proto.NetworkDefaultDeny}, next); err != nil {
		t.Fatalf("the adopted workspace could not be started: %v", err)
	}
}

// TestAdoptHonoursTheMountPathOfATreeThatNeverStarted proves the recovery
// above does not silently move the workspace. A non-default mount path chosen
// at Create time has to survive a failed start, because serving the same
// bytes at a different path is a wrong answer rather than an error.
func TestAdoptHonoursTheMountPathOfATreeThatNeverStarted(t *testing.T) {
	log := &callLog{}
	kernel := &fakeKernel{log: log}
	runtime := &fakeRuntime{log: log, root: "/state", binary: "/usr/bin/runsc", failCreate: true}
	b := testBackend(t, kernel, runtime)
	if err := os.MkdirAll(b.dir, 0o700); err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	raw, err := b.Create(ctx, "ws_two", proto.WorkspaceSpec{MountPath: "/srv/app"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	h := raw.(*handle)
	host := h.BrokerAdvertiseHost()
	if err := h.ApplyNetworkPolicy(ctx, proto.NetworkPolicy{}, workspace.NetworkEndpoint{
		Workspace: "ws_two", Generation: 3,
		ReverseProxyURL: "http://" + host + ":7443", ForwardProxyURL: "http://" + host + ":7443",
	}); err == nil {
		t.Fatal("ApplyNetworkPolicy succeeded although the sandbox could not start")
	}
	runtime.failCreate = false
	adopted, err := b.Adopt(ctx, "ws_two")
	if err != nil {
		t.Fatalf("Adopt refused a retained tree whose sandbox never started: %v", err)
	}
	if got := adopted.(*handle).MountPath(); got != "/srv/app" {
		t.Fatalf("adopted mount path = %q, want /srv/app", got)
	}
}

// TestRootFSGivenAsASymlinkIsResolvedForTheSandbox is the second defect the
// live Colima lane found on 2026-09-05, and the one with teeth.
//
// Measured on runsc release-20260831.0, aarch64: an OCI bundle whose
// root.path is a symlink to a directory fails to start with
//
//	creating container: cannot create sandbox: cannot read client sync file:
//	waiting for sandbox to start: EOF
//
// while the identical bundle naming the resolved directory starts. The same
// four-way experiment showed the runsc binary may be reached through a
// symlink; only the rootfs matters.
//
// Nothing refused the configuration. os.Stat follows symlinks, so New
// verified the rootfs, the node registered gvisor, advertised
// enforced_gateway and satisfied multi-tenant-isolated, and then failed every
// single materialization with a runsc message that names neither the rootfs
// nor the symlink. Pointing REMOUNT_GVISOR_ROOTFS at a symlink is the normal
// way to swap an immutable image atomically, so this is a configuration an
// operator will reach for.
//
// Resolving it is the fix rather than refusing it, and the split matters: the
// sandbox is started from the resolved directory, while the drift probe keeps
// watching the path the operator configured, so removing that symlink is
// still observed as the rootfs going away.
func TestRootFSGivenAsASymlinkIsResolvedForTheSandbox(t *testing.T) {
	dir := t.TempDir()
	image := filepath.Join(dir, "rootfs-2026-09-05")
	if err := os.Mkdir(image, 0o755); err != nil {
		t.Fatal(err)
	}
	link := filepath.Join(dir, "rootfs")
	if err := os.Symlink(image, link); err != nil {
		t.Fatal(err)
	}

	// The comparison target is the image directory with its own symlinks
	// resolved: on darwin t.TempDir() itself sits under /var -> /private/var,
	// and the property under test is "no symlink survives into the bundle",
	// not "exactly one link was followed".
	image, err := filepath.EvalSymlinks(image)
	if err != nil {
		t.Fatal(err)
	}
	configured, resolved, err := resolveRootFS(link)
	if err != nil {
		t.Fatalf("resolveRootFS(%q): %v", link, err)
	}
	if configured != link {
		t.Fatalf("configured rootfs = %q, want the path as given %q", configured, link)
	}
	if resolved != image {
		t.Fatalf("resolved rootfs = %q, want the real directory %q", resolved, image)
	}

	log := &callLog{}
	b := &Backend{dir: filepath.Join(dir, "workspaces"), rootfs: resolved, rootfsPath: configured,
		network: netns.NewManager(&fakeKernel{log: log}),
		runtime: &fakeRuntime{log: log, root: "/state", binary: "/usr/bin/runsc"}}
	if err := os.MkdirAll(b.dir, 0o700); err != nil {
		t.Fatal(err)
	}
	raw, err := b.Create(context.Background(), "ws_one", proto.WorkspaceSpec{}, nil)
	if err != nil {
		t.Fatal(err)
	}
	h := raw.(*handle)
	config, err := h.ociConfig("/run/remount/netns/fake")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(config), `"path": "`+image+`"`) {
		t.Fatalf("the OCI bundle does not name the resolved rootfs %q:\n%s", image, config)
	}
	if strings.Contains(string(config), `"path": "`+link+`"`) {
		t.Fatalf("the OCI bundle still names the symlink %q, which runsc cannot serve", link)
	}

	// Drift is still measured against the operator's path: swapping or
	// deleting the symlink is the rootfs going away, whatever survives at the
	// far end of it.
	checks := b.Reprobe(context.Background())
	if !findingStatus(checks, CheckRootFS, proto.CheckPass) {
		t.Fatalf("the rootfs check did not pass while the symlink was intact: %+v", checks)
	}
	if err := os.Remove(link); err != nil {
		t.Fatal(err)
	}
	checks = b.Reprobe(context.Background())
	if !findingStatus(checks, CheckRootFS, proto.CheckFail) {
		t.Fatalf("removing the configured rootfs path was not observed as drift: %+v", checks)
	}
}

func findingStatus(checks []proto.Finding, name, status string) bool {
	for _, c := range checks {
		if c.Check == name {
			return c.Status == status
		}
	}
	return false
}
