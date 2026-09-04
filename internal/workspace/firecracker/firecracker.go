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

	"remount.dev/remount/internal/proto"
	"remount.dev/remount/internal/session"
	"remount.dev/remount/internal/workspace"
)

// SnapshotFiles are the VMM state and guest memory files created while a VM
// is paused. Paths are interpreted inside the jail by Machine.
type SnapshotFiles struct {
	State, Memory string
	Compatibility Compatibility
}

// MachineConfig is the complete pre-boot Firecracker configuration.
type MachineConfig struct {
	KernelImage string
	RootDrive   string
	TapName     string
	GuestIP     netip.Addr
	GatewayIP   netip.Addr
	VCPU        int
	MemMiB      int
	TrackDirty  bool
}

// Machine is one fresh jailed Firecracker API process.
type Machine interface {
	Configure(context.Context, MachineConfig) error
	Start(context.Context) error
	Pause(context.Context) error
	CreateSnapshot(context.Context) (SnapshotFiles, error)
	LoadSnapshot(context.Context, SnapshotFiles, string, string) error
	Resume(context.Context) error
	// GuestSocket is the exact host UDS backing this VM's virtio-vsock device.
	GuestSocket() string
	// Kill terminates and joins the jailer and VMM. Returning nil means no
	// process from this machine remains alive.
	Kill(context.Context) error
}

// MachineLaunch contains host boundaries which must be selected before the
// jailer starts. The namespace is node-owned; the factory must only join it.
type MachineLaunch struct {
	NetworkNamespace string
}

// MachineFactory creates jailer-owned Firecracker processes. Jailed must be
// true; an unjailed implementation is rejected even if its probe succeeds.
type MachineFactory interface {
	Probe(context.Context) error
	Jailed() bool
	New(context.Context, string, MachineLaunch) (Machine, error)
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
	NamespacePath() string
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
	ImagePath() string
	MemorySnapshot() (files SnapshotFiles, sourceGeneration uint64, ok bool)
	Checkpoint(context.Context, []string, SnapshotFiles, uint64, io.Writer) error
	Destroy(context.Context) error
}

// GuestExecutor rewrites a portable session into the guest-agent transport.
type GuestExecutor interface {
	Probe(context.Context) error
	FileSystem(func() (GuestEndpoint, error)) workspace.FileSystem
	Prepare(*session.Spec, GuestEndpoint) error
}

// GuestEndpoint identifies a running VM without carrying a credential into
// the workspace disk.
type GuestEndpoint struct {
	Workspace  string
	Generation uint64
	Address    netip.Addr
	Socket     string
}

// Options are deliberately explicit: a kernel, base disk, jailed VMM,
// coherent CoW volume, guest executor and node-owned network are all required.
type Options struct {
	Dir, KernelImage, BaseRootFS string
	Machines                     MachineFactory
	Networks                     NetworkProvider
	Volumes                      VolumeProvider
	Guest                        GuestExecutor
	// MaxWorkspaces bounds live handles. Providers must independently bound
	// their durable disks, namespaces and process state.
	MaxWorkspaces int
}

// Backend is a production-capable Firecracker backend only after New has
// verified every enforcement seam.
type Backend struct {
	opts     Options
	verified bool
	mu       sync.Mutex
	handles  map[string]*handle
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
	if opts.MaxWorkspaces <= 0 {
		opts.MaxWorkspaces = 128
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
	return &Backend{opts: opts, verified: true, handles: make(map[string]*handle)}, nil
}

func (b *Backend) Name() string { return "firecracker" }
func (b *Backend) Caps() workspace.Caps {
	if !b.verified {
		return workspace.Caps{Isolation: "none", Snapshots: "fs", EgressMode: "open", BrokerIdentity: "none"}
	}
	return workspace.Caps{Isolation: "microvm", Snapshots: "fs+mem", EgressEnforced: true, MultiTenant: true, SiblingIsolation: true, EgressMode: "enforced_gateway", BrokerIdentity: "per_session_capability", FilesystemBoundary: "block_device", NetworkNamespace: true, DeviceIsolation: true, MountPath: true}
}

func (b *Backend) Create(ctx context.Context, id string, spec proto.WorkspaceSpec, restore io.Reader) (workspace.Handle, error) {
	if err := validateWorkspaceID(id); err != nil {
		return nil, err
	}
	if err := proto.ValidateMountPath(spec.MountPath); err != nil {
		return nil, err
	}
	if err := b.reserve(id); err != nil {
		return nil, err
	}
	reserved := true
	defer func() {
		if reserved {
			b.release(id, nil)
		}
	}()
	volume, err := b.opts.Volumes.Create(ctx, id, b.opts.BaseRootFS, restore)
	if err != nil {
		return nil, err
	}
	h, err := b.newHandle(ctx, id, spec, volume, true)
	if err != nil {
		return nil, err
	}
	b.install(id, h)
	reserved = false
	return h, nil
}

func (b *Backend) Adopt(ctx context.Context, id string) (workspace.Handle, error) {
	if err := validateWorkspaceID(id); err != nil {
		return nil, err
	}
	if err := b.reserve(id); err != nil {
		return nil, err
	}
	reserved := true
	defer func() {
		if reserved {
			b.release(id, nil)
		}
	}()
	volume, err := b.opts.Volumes.Adopt(ctx, id)
	if err != nil {
		return nil, err
	}
	h, err := b.newHandle(ctx, id, proto.WorkspaceSpec{}, volume, false)
	if err != nil {
		return nil, err
	}
	b.install(id, h)
	reserved = false
	return h, nil
}

func (b *Backend) newHandle(ctx context.Context, id string, spec proto.WorkspaceSpec, volume Volume, destroyOnFailure bool) (*handle, error) {
	network, err := b.opts.Networks.Reserve(ctx, id)
	if err != nil {
		if destroyOnFailure {
			cleanupCtx, cancel := cleanupContext()
			defer cancel()
			err = errors.Join(err, volume.Destroy(cleanupCtx))
		}
		return nil, err
	}
	files, sourceGeneration, restored := volume.MemorySnapshot()
	if restored && (sourceGeneration == 0 || files.State == "" || files.Memory == "") {
		cleanupCtx, cancel := cleanupContext()
		defer cancel()
		cleanupErr := network.Revoke(cleanupCtx)
		if destroyOnFailure {
			cleanupErr = errors.Join(cleanupErr, volume.Destroy(cleanupCtx))
		}
		return nil, errors.Join(errors.New("firecracker: incomplete retained memory snapshot"), cleanupErr)
	}
	h := &handle{id: id, spec: spec, kernel: b.opts.KernelImage, volume: volume, network: network, machines: b.opts.Machines, guest: b.opts.Guest, restored: restored, snapshotFiles: files, sourceGeneration: sourceGeneration, release: b.release}
	h.filesystem = b.opts.Guest.FileSystem(h.guestEndpoint)
	if h.filesystem == nil {
		return nil, errors.New("firecracker: guest executor returned no filesystem")
	}
	return h, nil
}

func (b *Backend) reserve(id string) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if _, exists := b.handles[id]; exists {
		return proto.Err(proto.CodeConflict, "firecracker workspace %s is already active", id)
	}
	if len(b.handles) >= b.opts.MaxWorkspaces {
		return proto.Err(proto.CodeResourceExhausted, "firecracker workspace capacity %d exhausted", b.opts.MaxWorkspaces)
	}
	b.handles[id] = nil
	return nil
}

func (b *Backend) install(id string, h *handle) {
	b.mu.Lock()
	b.handles[id] = h
	b.mu.Unlock()
}

func (b *Backend) release(id string, expected *handle) {
	b.mu.Lock()
	if current, exists := b.handles[id]; exists && (expected == nil || current == expected) {
		delete(b.handles, id)
	}
	b.mu.Unlock()
}

type handle struct {
	id, kernel         string
	spec               proto.WorkspaceSpec
	volume             Volume
	network            NetworkLease
	machines           MachineFactory
	guest              GuestExecutor
	filesystem         workspace.FileSystem
	mu                 sync.Mutex
	machine            Machine
	generation         uint64
	broker             netip.AddrPort
	sourceGeneration   uint64
	snapshotFiles      SnapshotFiles
	started, restored  bool
	snapshotFenced     bool
	fencedEnv          []byte
	fencedEnvPending   bool
	revoked, destroyed bool
	release            func(string, *handle)
}

// Detach drops this workspace from the backend's live registry without
// touching the volume, the snapshot or anything else on disk. It exists for the
// node's quarantine path; see workspace.Detacher.
func (h *handle) Detach() {
	if h.release != nil {
		h.release(h.id, h)
	}
}

func (h *handle) ID() string               { return h.id }
func (h *handle) Backend() string          { return "firecracker" }
func (h *handle) FS() workspace.FileSystem { return h.filesystem }
func (h *handle) MountPath() string {
	if h.spec.MountPath != "" {
		return h.spec.MountPath
	}
	return proto.DefaultMountPath
}
func (h *handle) CheckpointKind() workspace.CheckpointKind { return workspace.CheckpointFSMem }
func (h *handle) BrokerAdvertiseHost() string              { return h.network.HostAddress().String() }
func (h *handle) RestoreProcesses() string {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.restored && h.started {
		return proto.RestoreProcessesPreserved
	}
	return proto.RestoreProcessesRestarted
}

func (h *handle) Prepare(spec *session.Spec) error {
	endpoint, err := h.guestEndpoint()
	if err != nil {
		return err
	}
	return h.guest.Prepare(spec, endpoint)
}

func (h *handle) guestEndpoint() (GuestEndpoint, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if !h.started || h.machine == nil {
		return GuestEndpoint{}, proto.Err(proto.CodeClosed, "firecracker VM %s is not ready", h.id)
	}
	return GuestEndpoint{Workspace: h.id, Generation: h.generation, Address: h.network.GuestAddress(), Socket: h.machine.GuestSocket()}, nil
}

func (h *handle) ApplyNetworkPolicy(ctx context.Context, _ proto.NetworkPolicy, endpoint workspace.NetworkEndpoint) (err error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if endpoint.Workspace != h.id || endpoint.Generation == 0 {
		return proto.Err(proto.CodeDenied, "network endpoint does not identify this workspace generation")
	}
	if h.destroyed || h.revoked {
		return proto.Err(proto.CodeClosed, "firecracker workspace %s is fenced", h.id)
	}
	broker, err := brokerAddress(endpoint)
	if err != nil {
		return err
	}
	if h.started {
		if h.generation == endpoint.Generation && h.broker == broker {
			return nil
		}
		return proto.Err(proto.CodeDenied, "active Firecracker network identity differs from replay")
	}
	if broker.Addr() != h.network.HostAddress() {
		return fmt.Errorf("broker must bind %s, got %s", h.network.HostAddress(), broker.Addr())
	}
	if h.restored && endpoint.Generation <= h.sourceGeneration {
		return proto.Err(proto.CodeDenied, "restore generation %d does not advance checkpoint generation %d", endpoint.Generation, h.sourceGeneration)
	}
	tap, err := h.network.Prepare(ctx, endpoint.Generation)
	if err != nil {
		return err
	}
	machine, err := h.machines.New(ctx, h.id, MachineLaunch{NetworkNamespace: h.network.NamespacePath()})
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
		if err = machine.LoadSnapshot(ctx, h.snapshotFiles, h.volume.ImagePath(), tap); err != nil {
			return err
		}
		if err = machine.Resume(ctx); err != nil {
			return err
		}
	} else {
		guestIP := h.network.GuestAddress()
		cfg := MachineConfig{KernelImage: h.kernel, RootDrive: h.volume.ImagePath(), TapName: tap, GuestIP: guestIP, GatewayIP: guestIP.Prev(), VCPU: max(1, h.spec.Requires.CPU), MemMiB: max(128, h.spec.Requires.MemMiB), TrackDirty: true}
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
	h.generation, h.broker, h.started = endpoint.Generation, broker, true
	return nil
}

func (h *handle) RevokeNetwork(ctx context.Context) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	var killErr error
	if h.machine != nil {
		killErr = h.machine.Kill(ctx)
		if killErr == nil {
			h.machine = nil
		}
	}
	revokeErr := h.network.Revoke(ctx)
	if killErr != nil || revokeErr != nil {
		return errors.Join(killErr, revokeErr)
	}
	h.revoked = true
	h.started = false
	return nil
}
func (h *handle) Snapshot(ctx context.Context, excludes []string, w io.Writer) error {
	return h.checkpoint(ctx, excludes, w, false)
}
func (h *handle) Checkpoint(ctx context.Context, excludes []string, w io.Writer) (err error) {
	return h.checkpoint(ctx, excludes, w, false)
}

// CheckpointFenced snapshots the full VM and deliberately leaves it paused.
// This is the source-side prepare boundary for a destructive lifecycle.
func (h *handle) CheckpointFenced(ctx context.Context, excludes []string, w io.Writer) (err error) {
	return h.checkpoint(ctx, excludes, w, true)
}

// ResumeFenced resumes an in-memory source only after abort authorization is
// durable. A replay is a no-op, so the resume boundary is exactly once.
func (h *handle) ResumeFenced(ctx context.Context) error {
	h.mu.Lock()
	if h.snapshotFenced && (h.destroyed || h.machine == nil) {
		h.mu.Unlock()
		return proto.Err(proto.CodeClosed, "firecracker VM %s cannot resume", h.id)
	}
	if h.snapshotFenced {
		if err := h.machine.Resume(ctx); err != nil {
			h.mu.Unlock()
			return err
		}
		h.snapshotFenced = false
	}
	env := append([]byte(nil), h.fencedEnv...)
	envPending := h.fencedEnvPending
	h.mu.Unlock()
	if envPending {
		if err := h.filesystem.Write(".remount/env", env, 0o644, false, true); err != nil {
			return err
		}
		h.mu.Lock()
		h.fencedEnv = nil
		h.fencedEnvPending = false
		h.mu.Unlock()
	}
	return nil
}

func (h *handle) checkpoint(ctx context.Context, excludes []string, w io.Writer, retainFence bool) (err error) {
	// The broker URL is a node-local capability. Remove it while the guest is
	// live, then restore it only on the retained source after the disk bytes
	// have been captured. A destination rewrites its own value at materialize.
	var env []byte
	var envPresent bool
	if read, readErr := h.filesystem.Read(".remount/env", 0, 1<<20); readErr == nil {
		envPresent = true
		env = append([]byte(nil), read.Data...)
		if err := h.filesystem.Remove(".remount/env", false); err != nil {
			return err
		}
	} else {
		var protocol *proto.Error
		if !errors.As(readErr, &protocol) || protocol.Code != proto.CodeNotFound {
			return readErr
		}
	}
	defer func() {
		if envPresent && (!retainFence || err != nil) {
			restoreErr := h.filesystem.Write(".remount/env", env, 0o644, false, true)
			err = errors.Join(err, restoreErr)
		}
	}()
	h.mu.Lock()
	defer h.mu.Unlock()
	if !h.started || h.machine == nil {
		return proto.Err(proto.CodeClosed, "firecracker VM %s is not running", h.id)
	}
	if h.snapshotFenced {
		return proto.Err(proto.CodeConflict, "firecracker VM %s is already checkpoint-fenced", h.id)
	}
	if err = h.machine.Pause(ctx); err != nil {
		return err
	}
	defer func() {
		if !retainFence || err != nil {
			resumeCtx, cancel := cleanupContext()
			defer cancel()
			err = errors.Join(err, h.machine.Resume(resumeCtx))
		}
	}()
	files, err := h.machine.CreateSnapshot(ctx)
	if err != nil {
		return err
	}
	if err = h.volume.Checkpoint(ctx, excludes, files, h.generation, w); err != nil {
		return err
	}
	h.snapshotFiles = files
	h.sourceGeneration = h.generation
	h.restored = true
	h.snapshotFenced = retainFence
	if retainFence {
		h.fencedEnv = env
		h.fencedEnvPending = envPresent
	}
	return nil
}
func (h *handle) Destroy(ctx context.Context) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.destroyed {
		return nil
	}
	// Join the VMM before reclaiming its network, and destroy the volume only
	// after both.
	//
	// The network teardown deletes the workspace's devices, and the VMM holds
	// the TAP open for as long as it lives, so reclaiming first fails with
	// "delete TAP: device or resource busy" and every teardown stops there.
	// That is what made `remount ws move` fail with "released workspace source
	// cleanup is pending" and what left a quarantined workspace holding a lease
	// no retry could reclaim.
	//
	// This order was chosen rather than fallen into. The property that matters
	// is that nothing is dismantled while the workspace can still carry
	// traffic, and killing the VMM satisfies it more completely than revoking
	// ever could: a process that does not exist sends nothing, whereas a
	// revoked network still has a live guest behind it. The data property is
	// unchanged in every branch — the volume is destroyed only after both the
	// join and the revoke succeed, so a teardown that fails part way always
	// leaves a recoverable workspace rather than a half-erased one.
	if h.machine != nil {
		if err := h.machine.Kill(ctx); err != nil {
			return err
		}
		h.machine = nil
	}
	if err := h.network.Revoke(ctx); err != nil {
		return err
	}
	if err := h.volume.Destroy(ctx); err != nil {
		return err
	}
	h.destroyed = true
	h.started = false
	if h.release != nil {
		h.release(h.id, h)
	}
	return nil
}

func validateWorkspaceID(id string) error {
	if id == "" || len(id) > 128 || id[0] == '.' {
		return proto.Err(proto.CodeBadRequest, "invalid Firecracker workspace id")
	}
	for _, c := range id {
		if (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') || c == '_' || c == '-' || c == '.' {
			continue
		}
		return proto.Err(proto.CodeBadRequest, "invalid Firecracker workspace id")
	}
	return nil
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
