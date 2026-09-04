// Package gvisor implements a runsc workspace backend whose only network path
// is a generation-specific broker reachable over a private veth.
package gvisor

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/netip"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"remount.dev/remount/internal/artifact"
	"remount.dev/remount/internal/fsops"
	"remount.dev/remount/internal/netns"
	"remount.dev/remount/internal/proto"
	"remount.dev/remount/internal/session"
	"remount.dev/remount/internal/workspace"
)

const metadataName = "network.json"

// Options configures the gVisor backend. RootFS is an unpacked OCI rootfs;
// workspace data is mounted separately and is the only snapshotted content.
type Options struct {
	Dir     string
	RootFS  string
	Runsc   string
	Network *netns.Manager
}

// Backend runs OCI bundles with runsc.
type Backend struct {
	dir, rootfs string
	network     *netns.Manager
	runtime     runtimeClient
}

// New verifies runsc, the immutable rootfs, and the host's namespace
// privileges before returning a backend whose capabilities may be advertised.
func New(ctx context.Context, opts Options) (*Backend, error) {
	if opts.Dir == "" || opts.RootFS == "" {
		return nil, errors.New("gvisor: workspace directory and rootfs are required")
	}
	if opts.Runsc == "" {
		opts.Runsc = "runsc"
	}
	path, err := exec.LookPath(opts.Runsc)
	if err != nil {
		return nil, fmt.Errorf("gvisor: runsc unavailable: %w", err)
	}
	dir, err := filepath.Abs(opts.Dir)
	if err != nil {
		return nil, err
	}
	rootfs, err := filepath.Abs(opts.RootFS)
	if err != nil {
		return nil, err
	}
	if st, err := os.Stat(rootfs); err != nil || !st.IsDir() {
		return nil, fmt.Errorf("gvisor: rootfs %q is unavailable", rootfs)
	}
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return nil, err
	}
	state := filepath.Join(dir, ".runsc")
	if err := os.MkdirAll(state, 0o700); err != nil {
		return nil, err
	}
	network := opts.Network
	if network == nil {
		network = netns.NewSystemManager()
	}
	runtime := execRuntime{binary: path, root: state}
	if err := runtime.probe(ctx); err != nil {
		return nil, err
	}
	// Reclaim what a previous process left, before allocating anything.
	//
	// The slot allocator lives in memory, so a fresh backend asks the kernel for
	// the same slots the last one used — which are exactly the slots whose
	// leftovers are still there. A node that was killed therefore collides with
	// itself on its very next start and reports "create veth: file exists", an
	// error naming a file rather than the cause.
	//
	// Order matters: a leftover sandbox still inhabits the namespace it was
	// started in, so the namespaces only look uninhabited once the sandboxes
	// are gone. Nothing this process created can exist yet, so everything found
	// here is a leftover by construction.
	if _, err := runtime.reapStateRoot(ctx); err != nil {
		return nil, fmt.Errorf("gvisor: sandboxes from a previous run could not be reaped: %w", err)
	}
	if _, err := netns.ReclaimOrphans(ctx); err != nil {
		return nil, fmt.Errorf("gvisor: orphaned network state from a previous run could not be reclaimed: %w", err)
	}
	if err := network.Probe(ctx); err != nil {
		return nil, fmt.Errorf("gvisor: enforced network unavailable: %w", err)
	}
	return &Backend{dir: dir, rootfs: rootfs, network: network, runtime: runtime}, nil
}

func (b *Backend) Name() string { return "gvisor" }

// Caps reports only the boundary this backend enforces. Registration must be
// gated on New succeeding and the E4 host lane passing for the release.
func (b *Backend) Caps() workspace.Caps {
	return workspace.Caps{
		Isolation: "container", Snapshots: "fs", EgressEnforced: true,
		SiblingIsolation: true, EgressMode: "enforced_gateway",
		BrokerIdentity: "per_session_capability", FilesystemBoundary: "bind_mount",
		NetworkNamespace: true, DeviceIsolation: true, MountPath: true,
	}
}

func (b *Backend) Create(ctx context.Context, id string, spec proto.WorkspaceSpec, restore io.Reader) (workspace.Handle, error) {
	if err := proto.ValidateMountPath(spec.MountPath); err != nil {
		return nil, err
	}
	mount := spec.MountPath
	if mount == "" {
		mount = proto.DefaultMountPath
	}
	bundle := filepath.Join(b.dir, id)
	if _, err := os.Stat(bundle); err == nil {
		return nil, proto.Err(proto.CodeConflict, "workspace %s already exists on this node", id)
	}
	work := filepath.Join(bundle, "work")
	if err := os.MkdirAll(work, 0o755); err != nil {
		return nil, err
	}
	if restore != nil {
		if err := artifact.Restore(work, restore); err != nil {
			_ = os.RemoveAll(bundle)
			return nil, fmt.Errorf("restore: %w", err)
		}
	}
	handle, err := b.handle(ctx, id, bundle, work, mount, 1)
	if err != nil {
		_ = os.RemoveAll(bundle)
		return nil, err
	}
	return handle, nil
}

func (b *Backend) Adopt(ctx context.Context, id string) (workspace.Handle, error) {
	bundle := filepath.Join(b.dir, id)
	work := filepath.Join(bundle, "work")
	if st, err := os.Stat(work); err != nil || !st.IsDir() {
		return nil, proto.Err(proto.CodeNotFound, "no workspace %s here", id)
	}
	var retained metadata
	data, err := os.ReadFile(filepath.Join(bundle, metadataName))
	if err != nil {
		return nil, proto.Err(proto.CodeNotFound, "workspace %s has no retained network metadata", id)
	}
	if err := json.Unmarshal(data, &retained); err != nil {
		return nil, fmt.Errorf("decode retained gvisor metadata: %w", err)
	}
	if retained.Generation == 0 {
		return nil, errors.New("retained gvisor metadata has no generation")
	}
	if err := proto.ValidateMountPath(retained.Mount); err != nil {
		return nil, fmt.Errorf("retained gvisor mount: %w", err)
	}
	network, err := b.network.Adopt(ctx, retained.Network)
	if err != nil {
		return nil, err
	}
	// Broker ports are materialization-local. Stop the old sandbox and remove
	// its veth before the node starts a new broker; ApplyNetworkPolicy rebuilds
	// both with the authoritative generation before ws.ready.
	if err := b.runtime.destroy(ctx, containerName(id)); err != nil {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = network.Revoke(cleanupCtx)
		return nil, fmt.Errorf("stop retained runsc sandbox: %w", err)
	}
	if err := network.Revoke(ctx); err != nil {
		return nil, err
	}
	return b.handle(ctx, id, bundle, work, retained.Mount, retained.Generation)
}

func (b *Backend) handle(ctx context.Context, id, bundle, work, mount string, generation uint64) (*handle, error) {
	fs, err := fsops.New(work)
	if err != nil {
		return nil, err
	}
	network, err := b.network.Open(ctx, id, generation)
	if err != nil {
		_ = fs.Close()
		return nil, err
	}
	return &handle{id: id, bundle: bundle, rootfs: b.rootfs, mount: mount, fs: fs, runtime: b.runtime, network: network}, nil
}

type handle struct {
	id, bundle, rootfs, mount string
	fs                        *fsops.FS
	runtime                   runtimeClient

	mu         sync.Mutex
	network    *netns.Network
	generation uint64
	started    bool
}

func (h *handle) ID() string               { return h.id }
func (h *handle) Backend() string          { return "gvisor" }
func (h *handle) FS() workspace.FileSystem { return h.fs }
func (h *handle) MountPath() string        { return h.mount }

// BrokerAdvertiseHost is consumed by the node integration hook so the broker
// binds the host-veth address rather than loopback or all interfaces.
func (h *handle) BrokerAdvertiseHost() string {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.network == nil {
		return ""
	}
	return h.network.HostAddress().String()
}

func (h *handle) Prepare(spec *session.Spec) error {
	h.mu.Lock()
	started := h.started
	guest := netip.Addr{}
	if h.network != nil {
		guest = h.network.GuestAddress()
	}
	h.mu.Unlock()
	if !started {
		return proto.Err(proto.CodeClosed, "gvisor sandbox %s is not network-ready", h.id)
	}
	if spec.Kind == proto.SessionPort {
		spec.Host = guest.String()
		return nil
	}
	cwd := h.mount
	if spec.Cwd != "" {
		resolved, err := h.fs.Resolve(spec.Cwd)
		if err != nil {
			return err
		}
		rel, _ := filepath.Rel(h.fs.Root(), resolved)
		cwd = filepath.ToSlash(filepath.Join(h.mount, rel))
	}
	args := []string{"--root=" + h.runtime.stateRoot(), "exec", "--cwd=" + cwd}
	for _, value := range workspace.MergeEnv([]string{"HOME=" + h.mount, "USER=root", "LANG=C.UTF-8", "REMOUNT_WORKSPACE=" + h.id}, spec.Env) {
		args = append(args, "--env="+value)
	}
	args = append(args, containerName(h.id))
	args = append(args, spec.Program...)
	spec.Program = append([]string{h.runtime.binaryPath()}, args...)
	spec.Cwd = ""
	spec.Env = workspace.MergeEnv(os.Environ())
	return nil
}

func (h *handle) ApplyNetworkPolicy(ctx context.Context, _ proto.NetworkPolicy, endpoint workspace.NetworkEndpoint) (err error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if endpoint.Workspace != h.id || endpoint.Generation == 0 {
		return proto.Err(proto.CodeDenied, "network endpoint does not identify this workspace generation")
	}
	if h.started {
		if endpoint.Generation == h.generation {
			return nil
		}
		return proto.Err(proto.CodeDenied, "stale network generation %d for active generation %d", endpoint.Generation, h.generation)
	}
	network := h.network
	if network == nil {
		return proto.Err(proto.CodeClosed, "gvisor network boundary is unavailable")
	}
	defer func() {
		if err == nil {
			return
		}
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		err = errors.Join(err, network.Revoke(cleanupCtx))
		err = errors.Join(err, h.runtime.destroy(cleanupCtx, containerName(h.id)))
		h.network = nil
	}()
	broker, err := endpointAddress(endpoint)
	if err != nil {
		return err
	}
	if broker.Addr() != network.HostAddress() {
		return fmt.Errorf("broker must listen on %s, got %s", network.HostAddress(), broker.Addr())
	}
	config, err := h.ociConfig(network.NamespacePath())
	if err != nil {
		return err
	}
	if err := os.WriteFile(filepath.Join(h.bundle, "config.json"), config, 0o600); err != nil {
		return err
	}
	if err := h.runtime.createStart(ctx, h.bundle, containerName(h.id)); err != nil {
		return err
	}
	if err := network.Apply(ctx, broker); err != nil {
		return err
	}
	meta, err := json.Marshal(metadata{Generation: endpoint.Generation, Mount: h.mount, Network: network.State()})
	if err != nil {
		return err
	}
	if err := os.WriteFile(filepath.Join(h.bundle, metadataName), meta, 0o600); err != nil {
		return err
	}
	h.generation = endpoint.Generation
	h.started = true
	return nil
}

func (h *handle) RevokeNetwork(ctx context.Context) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.network == nil {
		return nil
	}
	if err := h.network.Revoke(ctx); err != nil {
		return err
	}
	h.network = nil
	h.started = false
	return nil
}

func (h *handle) Snapshot(ctx context.Context, excludes []string, w io.Writer) error {
	return artifact.Snapshot(h.fs.Root(), excludes, w)
}

func (h *handle) Checkpoint(ctx context.Context, excludes []string, w io.Writer) (err error) {
	h.mu.Lock()
	started := h.started
	h.mu.Unlock()
	if !started {
		return proto.Err(proto.CodeClosed, "gvisor sandbox %s is not running", h.id)
	}
	if err := h.runtime.pause(ctx, containerName(h.id)); err != nil {
		return err
	}
	defer func() {
		resumeCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		err = errors.Join(err, h.runtime.resume(resumeCtx, containerName(h.id)))
	}()
	return artifact.Snapshot(h.fs.Root(), excludes, w)
}

func (h *handle) Destroy(ctx context.Context) error {
	if err := h.RevokeNetwork(ctx); err != nil {
		return err
	}
	if err := h.runtime.destroy(ctx, containerName(h.id)); err != nil {
		return err
	}
	_ = h.fs.Close()
	return os.RemoveAll(h.bundle)
}

func endpointAddress(endpoint workspace.NetworkEndpoint) (netip.AddrPort, error) {
	var result netip.AddrPort
	for _, raw := range []string{endpoint.ReverseProxyURL, endpoint.ForwardProxyURL} {
		u, err := url.Parse(raw)
		if err != nil || u.Scheme != "http" || u.Host == "" {
			return netip.AddrPort{}, fmt.Errorf("invalid broker URL %q", raw)
		}
		candidate, err := netip.ParseAddrPort(u.Host)
		if err != nil {
			return netip.AddrPort{}, fmt.Errorf("broker URL must use a literal IPv4 address: %w", err)
		}
		if !result.IsValid() {
			result = candidate
		} else if result != candidate {
			return netip.AddrPort{}, errors.New("reverse and forward proxy URLs name different brokers")
		}
	}
	return result, nil
}

func containerName(id string) string { return "remount-" + strings.ReplaceAll(id, "_", "-") }

type metadata struct {
	Generation uint64      `json:"generation"`
	Mount      string      `json:"mount"`
	Network    netns.State `json:"network"`
}

type ociSpec struct {
	OCIVersion string     `json:"ociVersion"`
	Process    ociProcess `json:"process"`
	Root       ociRoot    `json:"root"`
	Hostname   string     `json:"hostname"`
	Mounts     []ociMount `json:"mounts"`
	Linux      ociLinux   `json:"linux"`
}
type ociProcess struct {
	Terminal bool `json:"terminal"`
	User     struct {
		UID uint32 `json:"uid"`
		GID uint32 `json:"gid"`
	} `json:"user"`
	Args            []string `json:"args"`
	Env             []string `json:"env"`
	Cwd             string   `json:"cwd"`
	NoNewPrivileges bool     `json:"noNewPrivileges"`
}
type ociRoot struct {
	Path     string `json:"path"`
	Readonly bool   `json:"readonly"`
}
type ociMount struct {
	Destination string   `json:"destination"`
	Type        string   `json:"type"`
	Source      string   `json:"source"`
	Options     []string `json:"options,omitempty"`
}
type ociNamespace struct {
	Type string `json:"type"`
	Path string `json:"path,omitempty"`
}
type ociLinux struct {
	Namespaces []ociNamespace `json:"namespaces"`
}

func (h *handle) ociConfig(namespace string) ([]byte, error) {
	spec := ociSpec{
		OCIVersion: "1.0.2", Root: ociRoot{Path: h.rootfs, Readonly: true}, Hostname: containerName(h.id),
		Process: ociProcess{Args: []string{"sleep", "infinity"}, Env: []string{"PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin", "HOME=" + h.mount}, Cwd: h.mount, NoNewPrivileges: true},
		Mounts: []ociMount{
			{Destination: "/proc", Type: "proc", Source: "proc"},
			{Destination: "/dev", Type: "tmpfs", Source: "tmpfs", Options: []string{"nosuid", "strictatime", "mode=755", "size=65536k"}},
			{Destination: h.mount, Type: "bind", Source: h.fs.Root(), Options: []string{"rbind", "rw"}},
		},
		Linux: ociLinux{Namespaces: []ociNamespace{{Type: "pid"}, {Type: "ipc"}, {Type: "uts"}, {Type: "mount"}, {Type: "network", Path: namespace}}},
	}
	return json.MarshalIndent(spec, "", "  ")
}
