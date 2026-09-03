// Package firecracker implements the lifecycle composition for a jailed
// Firecracker workspace. Host-specific disk, guest-agent, and netns seams are
// explicit because claiming them without a coherent implementation would
// overstate the backend's isolation and checkpoint guarantees.
package firecracker

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/netip"
	"net/url"
	"os"
	"path/filepath"
	"sync"
	"time"

	"remount.dev/remount/internal/fsops"
	"remount.dev/remount/internal/proto"
	"remount.dev/remount/internal/session"
	"remount.dev/remount/internal/workspace"
)

// SnapshotFiles are the VMM state and guest memory files created while a VM
// is paused. Paths are interpreted inside the jail by Machine.
type SnapshotFiles struct{ State, Memory string }

// MachineConfig is the complete pre-boot Firecracker configuration.
type MachineConfig struct {
	KernelImage string
	RootDrive   string
	TapName     string
	VCPU        int
	MemMiB      int
	TrackDirty  bool
}

// Machine is one fresh jailed Firecracker API process.
type Machine interface {
	Configure(context.Context, MachineConfig) error
	Start(context.Context) error
	Pause(context.Context) error
	CreateSnapshot(context.Context, SnapshotFiles) error
	LoadSnapshot(context.Context, SnapshotFiles) error
	Resume(context.Context) error
	Kill(context.Context) error
}

// MachineFactory creates jailer-owned Firecracker processes. Jailed must be
// true; an unjailed implementation is rejected even if its probe succeeds.
type MachineFactory interface {
	Probe(context.Context) error
	Jailed() bool
	New(context.Context, string) (Machine, error)
}

// NetworkProvider reserves a host address before the broker binds and returns
// a node-owned network lease. The provider is expected to reuse the netns
// firewall from ADR 0061 rather than install a second policy engine.
type NetworkProvider interface {
	Probe(context.Context) error
	Reserve(context.Context, string) (NetworkLease, error)
}

// NetworkLease is a deny-first namespace reservation. Prepare attaches a TAP
// for one generation but leaves egress disabled; Activate permits only broker.
type NetworkLease interface {
	HostAddress() netip.Addr
	GuestAddress() netip.Addr
	Prepare(context.Context, uint64) (tapName string, err error)
	Activate(context.Context, netip.AddrPort) error
	Revoke(context.Context) error
}

// VolumeProvider owns the CoW block image and its coherent host-side FS view.
// Its Checkpoint method must join archive production before returning.
type VolumeProvider interface {
	Probe(context.Context, string) error
	Create(context.Context, string, string, io.Reader) (Volume, error)
	Adopt(context.Context, string) (Volume, error)
}

// Volume is a CoW workspace disk plus the node's jailed filesystem view.
type Volume interface {
	FS() *fsops.FS
	ImagePath() string
	SnapshotFiles() SnapshotFiles
	HasMemorySnapshot() bool
	Snapshot(context.Context, []string, io.Writer) error
	Checkpoint(context.Context, []string, SnapshotFiles, io.Writer) error
	Destroy(context.Context) error
}

// GuestExecutor rewrites a portable session into the guest-agent transport.
type GuestExecutor interface {
	Probe(context.Context) error
	Prepare(*session.Spec, GuestEndpoint) error
}

// GuestEndpoint identifies a running VM without carrying a credential into
// the workspace disk.
type GuestEndpoint struct {
	Workspace string
	Address   netip.Addr
}

// Options are deliberately explicit: a kernel, base disk, jailed VMM,
// coherent CoW volume, guest executor and node-owned network are all required.
type Options struct {
	Dir, KernelImage, BaseRootFS string
	Machines                     MachineFactory
	Networks                     NetworkProvider
	Volumes                      VolumeProvider
	Guest                        GuestExecutor
}

// Backend is a production-capable Firecracker backend only after New has
// verified every enforcement seam.
type Backend struct {
	opts     Options
	verified bool
}

// New verifies all capability-bearing prerequisites. An unavailable check is
// returned as an error; callers must not register the backend in that case.
func New(ctx context.Context, opts Options) (*Backend, error) {
	if opts.Dir == "" || opts.KernelImage == "" || opts.BaseRootFS == "" {
		return nil, errors.New("firecracker: data directory, kernel image and base rootfs are required")
	}
	for name, path := range map[string]string{"kernel": opts.KernelImage, "base rootfs": opts.BaseRootFS} {
		if st, err := os.Stat(path); err != nil || !st.Mode().IsRegular() {
			return nil, fmt.Errorf("firecracker: %s artifact %q is unavailable", name, path)
		}
	}
	if opts.Machines == nil || opts.Networks == nil || opts.Volumes == nil || opts.Guest == nil {
		return nil, errors.New("firecracker: jailed VMM, network, CoW volume and guest executor are required")
	}
	if !opts.Machines.Jailed() {
		return nil, errors.New("firecracker: unjailed machine factory refused")
	}
	checks := []struct {
		name string
		run  func() error
	}{
		{"KVM/jailer/snapshot", func() error { return opts.Machines.Probe(ctx) }},
		{"node-owned network", func() error { return opts.Networks.Probe(ctx) }},
		{"CoW volume", func() error { return opts.Volumes.Probe(ctx, opts.BaseRootFS) }},
		{"guest executor", func() error { return opts.Guest.Probe(ctx) }},
	}
	for _, check := range checks {
		if err := check.run(); err != nil {
			return nil, fmt.Errorf("firecracker: %s unavailable: %w", check.name, err)
		}
	}
	abs, err := filepath.Abs(opts.Dir)
	if err != nil {
		return nil, err
	}
	if err := os.MkdirAll(abs, 0o700); err != nil {
		return nil, err
	}
	opts.Dir = abs
	return &Backend{opts: opts, verified: true}, nil
}

func (b *Backend) Name() string { return "firecracker" }
func (b *Backend) Caps() workspace.Caps {
	if !b.verified {
		return workspace.Caps{Isolation: "none", Snapshots: "fs", EgressMode: "open", BrokerIdentity: "none"}
	}
	return workspace.Caps{Isolation: "microvm", Snapshots: "fs+mem", EgressEnforced: true, MultiTenant: true, SiblingIsolation: true, EgressMode: "enforced_gateway", BrokerIdentity: "per_session_capability", FilesystemBoundary: "block_device", NetworkNamespace: true, DeviceIsolation: true, MountPath: true}
}

func (b *Backend) Create(ctx context.Context, id string, spec proto.WorkspaceSpec, restore io.Reader) (workspace.Handle, error) {
	if err := proto.ValidateMountPath(spec.MountPath); err != nil {
		return nil, err
	}
	volume, err := b.opts.Volumes.Create(ctx, id, b.opts.BaseRootFS, restore)
	if err != nil {
		return nil, err
	}
	return b.newHandle(ctx, id, spec, volume)
}

func (b *Backend) Adopt(ctx context.Context, id string) (workspace.Handle, error) {
	volume, err := b.opts.Volumes.Adopt(ctx, id)
	if err != nil {
		return nil, err
	}
	return b.newHandle(ctx, id, proto.WorkspaceSpec{}, volume)
}

func (b *Backend) newHandle(ctx context.Context, id string, spec proto.WorkspaceSpec, volume Volume) (*handle, error) {
	network, err := b.opts.Networks.Reserve(ctx, id)
	if err != nil {
		cleanupCtx, cancel := cleanupContext()
		defer cancel()
		_ = volume.Destroy(cleanupCtx)
		return nil, err
	}
	return &handle{id: id, spec: spec, kernel: b.opts.KernelImage, volume: volume, network: network, machines: b.opts.Machines, guest: b.opts.Guest, restored: volume.HasMemorySnapshot()}, nil
}

type handle struct {
	id, kernel        string
	spec              proto.WorkspaceSpec
	volume            Volume
	network           NetworkLease
	machines          MachineFactory
	guest             GuestExecutor
	mu                sync.Mutex
	machine           Machine
	generation        uint64
	started, restored bool
}

func (h *handle) ID() string      { return h.id }
func (h *handle) Backend() string { return "firecracker" }
func (h *handle) FS() *fsops.FS   { return h.volume.FS() }
func (h *handle) MountPath() string {
	if h.spec.MountPath != "" {
		return h.spec.MountPath
	}
	return proto.DefaultMountPath
}
func (h *handle) CheckpointKind() workspace.CheckpointKind { return workspace.CheckpointFSMem }
func (h *handle) BrokerAdvertiseHost() string              { return h.network.HostAddress().String() }

func (h *handle) Prepare(spec *session.Spec) error {
	h.mu.Lock()
	started := h.started
	endpoint := GuestEndpoint{Workspace: h.id, Address: h.network.GuestAddress()}
	h.mu.Unlock()
	if !started {
		return proto.Err(proto.CodeClosed, "firecracker VM %s is not ready", h.id)
	}
	return h.guest.Prepare(spec, endpoint)
}

func (h *handle) ApplyNetworkPolicy(ctx context.Context, _ proto.NetworkPolicy, endpoint workspace.NetworkEndpoint) (err error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if endpoint.Workspace != h.id || endpoint.Generation == 0 {
		return proto.Err(proto.CodeDenied, "network endpoint does not identify this workspace generation")
	}
	if h.started {
		if h.generation == endpoint.Generation {
			return nil
		}
		return proto.Err(proto.CodeDenied, "active Firecracker generation is %d, not %d", h.generation, endpoint.Generation)
	}
	broker, err := brokerAddress(endpoint)
	if err != nil {
		return err
	}
	if broker.Addr() != h.network.HostAddress() {
		return fmt.Errorf("broker must bind %s, got %s", h.network.HostAddress(), broker.Addr())
	}
	tap, err := h.network.Prepare(ctx, endpoint.Generation)
	if err != nil {
		return err
	}
	machine, err := h.machines.New(ctx, h.id)
	if err != nil {
		cleanupCtx, cancel := cleanupContext()
		defer cancel()
		_ = h.network.Revoke(cleanupCtx)
		return err
	}
	h.machine = machine
	defer func() {
		if err != nil {
			cleanupCtx, cancel := cleanupContext()
			defer cancel()
			err = errors.Join(err, machine.Kill(cleanupCtx), h.network.Revoke(cleanupCtx))
			h.machine = nil
		}
	}()
	if h.restored {
		if err = machine.LoadSnapshot(ctx, h.volume.SnapshotFiles()); err != nil {
			return err
		}
		if err = machine.Resume(ctx); err != nil {
			return err
		}
	} else {
		cfg := MachineConfig{KernelImage: h.kernel, RootDrive: h.volume.ImagePath(), TapName: tap, VCPU: max(1, h.spec.Requires.CPU), MemMiB: max(128, h.spec.Requires.MemMiB), TrackDirty: true}
		if err = machine.Configure(ctx, cfg); err != nil {
			return err
		}
		if err = machine.Start(ctx); err != nil {
			return err
		}
	}
	if err = h.network.Activate(ctx, broker); err != nil {
		return err
	}
	h.generation, h.started = endpoint.Generation, true
	return nil
}

func (h *handle) RevokeNetwork(ctx context.Context) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	if err := h.network.Revoke(ctx); err != nil {
		return err
	}
	h.started = false
	return nil
}
func (h *handle) Snapshot(ctx context.Context, excludes []string, w io.Writer) error {
	return h.volume.Snapshot(ctx, excludes, w)
}
func (h *handle) Checkpoint(ctx context.Context, excludes []string, w io.Writer) (err error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if !h.started || h.machine == nil {
		return proto.Err(proto.CodeClosed, "firecracker VM %s is not running", h.id)
	}
	if err = h.machine.Pause(ctx); err != nil {
		return err
	}
	defer func() {
		resumeCtx, cancel := cleanupContext()
		defer cancel()
		err = errors.Join(err, h.machine.Resume(resumeCtx))
	}()
	files := h.volume.SnapshotFiles()
	if err = h.machine.CreateSnapshot(ctx, files); err != nil {
		return err
	}
	return h.volume.Checkpoint(ctx, excludes, files, w)
}
func (h *handle) Destroy(ctx context.Context) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	if err := h.network.Revoke(ctx); err != nil {
		return err
	}
	if h.machine != nil {
		if err := h.machine.Kill(ctx); err != nil {
			return err
		}
		h.machine = nil
	}
	return h.volume.Destroy(ctx)
}

func brokerAddress(endpoint workspace.NetworkEndpoint) (netip.AddrPort, error) {
	var out netip.AddrPort
	for _, raw := range []string{endpoint.ReverseProxyURL, endpoint.ForwardProxyURL} {
		u, err := url.Parse(raw)
		if err != nil || u.Scheme != "http" {
			return out, fmt.Errorf("invalid broker URL %q", raw)
		}
		candidate, err := netip.ParseAddrPort(u.Host)
		if err != nil {
			return out, err
		}
		if out.IsValid() && out != candidate {
			return out, errors.New("broker URLs disagree")
		}
		out = candidate
	}
	return out, nil
}

func cleanupContext() (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.Background(), 10*time.Second)
}
