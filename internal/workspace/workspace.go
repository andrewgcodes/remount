// Package workspace defines the Backend interface a node uses to give a
// workspace a filesystem and processes, and implements the "process"
// backend (a directory on the host, isolation: none) and the "docker"
// backend (a container with the workspace directory bind-mounted).
//
// A Backend is compiled in, never a plugin (docs/adr/0005-no-plugin-abi.md).
// The node treats every backend the same way: it asks for a Handle, runs
// sessions through Prepare, does filesystem operations through FS, and
// snapshots through Snapshot. What a backend cannot do it says in Caps.
package workspace

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"sync"
	"time"

	"remount.dev/remount/internal/artifact"
	"remount.dev/remount/internal/fsops"
	"remount.dev/remount/internal/proto"
	"remount.dev/remount/internal/session"
)

// Caps describes what a backend can do; advertised honestly to the control
// plane so policy can refuse to place workloads that need more.
type Caps struct {
	Isolation          string // none | process_sandbox | container | microvm
	Snapshots          string // fs | fs+mem
	EgressEnforced     bool   // can the backend force all egress through the broker?
	Display            bool
	MultiTenant        bool
	SiblingIsolation   bool
	EgressMode         string // open | cooperative_proxy | enforced_gateway
	BrokerIdentity     string // none | token | unix_socket | workload_identity
	FilesystemBoundary string
	NetworkNamespace   bool
	DeviceIsolation    bool
	// MountPath: the backend honors WorkspaceSpec.MountPath (the workspace
	// has its own mount namespace). A backend without one must refuse a
	// spec that sets it rather than symlink host paths into its jail.
	MountPath bool
}

// Describer reports a backend's identity and evidence-bearing capabilities.
type Describer interface {
	Name() string
	Caps() Caps
}

// Provisioner creates a fresh materialization.
type Provisioner interface {
	// Create materializes a workspace. If restore is non-nil it is a tar.gz
	// snapshot to extract into the fresh filesystem.
	Create(ctx context.Context, id string, spec proto.WorkspaceSpec, restore io.Reader) (Handle, error)
}

// Adopter reattaches to a materialization retained across a node restart.
type Adopter interface {
	// Adopt reattaches to a workspace that already exists on disk (node
	// restart). Returns proto.CodeNotFound if there is nothing to adopt.
	Adopt(ctx context.Context, id string) (Handle, error)
}

// Backend is the explicit set of responsibilities every registered backend
// must implement. Splitting the contracts lets implementations and conformance
// tests reason about each security-sensitive responsibility independently.
type Backend interface {
	Describer
	Provisioner
	Adopter
}

// Identity identifies one materialization.
type Identity interface {
	ID() string
	Backend() string
}

// Mounter reports where the tree appears to processes running inside the
// workspace. Harness protocols that speak in absolute paths (ACP) need it to
// translate between the harness's view and the node's jail.
type Mounter interface {
	// MountPath is the tree's root as the workspace's processes see it.
	MountPath() string
}

// MountPathOf returns the in-workspace root of h: the backend's answer when
// it has a mount namespace, otherwise the host-side jail root, which is the
// same directory the processes see.
func MountPathOf(h Handle) string {
	if m, ok := h.(Mounter); ok {
		return m.MountPath()
	}
	if host, ok := h.FS().(HostFileSystem); ok {
		return host.Root()
	}
	return ""
}

// FileSystem is the path-jailed filesystem surface used by the node. A
// backend may implement it over a descriptor-rooted host directory or over a
// generation-bound guest transport. Host paths are deliberately a separate
// capability: callers must never treat an in-guest tree as a local directory.
type FileSystem interface {
	Close() error
	Read(string, int64, int64) (*proto.FSReadRes, error)
	Write(string, []byte, uint32, bool, bool) error
	List(string) ([]proto.FSEntry, error)
	Stat(string) (*proto.FSEntry, error)
	Mkdir(string) error
	Remove(string, bool) error
	Rename(string, string) error
	Search(string, string, string, int) (*proto.FSSearchRes, error)
	Edit(string, []proto.FSEdit) (int, error)
}

// HostFileSystem is implemented only when the filesystem is safely available
// as a local host directory. Block devices attached to a running guest must
// not implement this interface.
type HostFileSystem interface {
	FileSystem
	Root() string
	Resolve(string) (string, error)
}

// HostFileSystemOf returns h's local-path capability, when one exists.
func HostFileSystemOf(h Handle) (HostFileSystem, bool) {
	host, ok := h.FS().(HostFileSystem)
	return host, ok
}

// Filesystem exposes the backend's path-jailed filesystem adapter.
type Filesystem interface {
	// FS returns the filesystem view. Callers needing a local path must also
	// require HostFileSystem rather than guessing from Root or MountPath.
	FS() FileSystem
}

// FilesystemAccessPreparer makes a backend-owned tree accessible to host-side
// filesystem operations. Container processes may create files under a uid the
// node does not run as, so the node invokes this while holding the tree lock.
type FilesystemAccessPreparer interface {
	PrepareFilesystemAccess(context.Context) error
}

// RestoreProcessReporter reports process continuity after a restored backend
// has completed its serviceability checks.
type RestoreProcessReporter interface {
	RestoreProcesses() string
}

// SessionPreparer turns a portable session request into a backend-specific
// process specification.
type SessionPreparer interface {
	// Prepare rewrites a session spec so it runs inside the workspace:
	// cwd is resolved, env is merged with the workspace baseline, and for
	// container backends the program is wrapped in an exec.
	Prepare(spec *session.Spec) error
}

// CheckpointKind names what a checkpoint archive captures. It is the value a
// backend's Caps.Snapshots advertises and the value stored on a snapshot so a
// restore knows whether processes resume or restart.
type CheckpointKind string

const (
	// CheckpointFS is the filesystem alone; processes restart on restore.
	CheckpointFS CheckpointKind = "fs"
	// CheckpointFSMem is filesystem plus process memory; processes resume.
	CheckpointFSMem CheckpointKind = "fs+mem"
)

// Checkpointer owns snapshot consistency. Snapshot is explicitly live and may
// observe concurrent workspace writes. Checkpoint must freeze backend-managed
// execution until the archive is complete; the node additionally excludes its
// direct filesystem mutation surface while calling it.
type Checkpointer interface {
	// Snapshot streams a tar.gz of the filesystem.
	Snapshot(ctx context.Context, excludes []string, w io.Writer) error
	// Checkpoint streams a quiesced tar.gz suitable for authoritative failover.
	Checkpoint(ctx context.Context, excludes []string, w io.Writer) error
}

// MemoryCheckpointer is implemented by handles whose Checkpoint captures more
// than the filesystem. A handle that does not implement it is CheckpointFS;
// callers use KindOf and never type-assert on a concrete backend.
type MemoryCheckpointer interface {
	Checkpointer
	// CheckpointKind reports what Checkpoint will produce for this handle.
	CheckpointKind() CheckpointKind
}

// FencedCheckpointer is an optional destructive-lifecycle contract. A
// successful CheckpointFenced leaves all backend-managed execution stopped;
// the caller either destroys the retained source after its durable commit or
// calls ResumeFenced after durable abort authorization.
type FencedCheckpointer interface {
	CheckpointFenced(context.Context, []string, io.Writer) error
	ResumeFenced(context.Context) error
}

// KindOf reports the checkpoint kind a handle produces. Only a kind the
// handle itself claims is trusted; an unknown claim degrades to CheckpointFS
// so a caller never records a memory snapshot that is not one.
func KindOf(h Checkpointer) CheckpointKind {
	if m, ok := h.(MemoryCheckpointer); ok && m.CheckpointKind() == CheckpointFSMem {
		return CheckpointFSMem
	}
	return CheckpointFS
}

// Destroyer permanently removes one materialization.
type Destroyer interface {
	// Destroy removes everything.
	Destroy(ctx context.Context) error
}

// Handle is one live workspace composed from explicit responsibilities.
type Handle interface {
	Identity
	Filesystem
	SessionPreparer
	Checkpointer
	Destroyer
}

// Detacher is implemented by a backend handle whose backend keeps a registry of
// live workspaces, so that a caller giving up a handle without destroying it
// can say so.
//
// A node that quarantines a failed materialization deliberately keeps the
// filesystem: the tree may hold bytes nothing else has. But it does let go of
// the handle, and a backend that goes on believing the workspace is active will
// refuse the next Create *and* the next Adopt for it — so the node retries
// forever against a conflict it caused itself, and the workspace is stuck on
// that node with an error naming the wrong problem. Detach is how the node says
// "I am no longer holding this" without saying "destroy it".
type Detacher interface {
	Detach()
}

// NetworkEndpoint identifies the generation-specific broker that an enforced
// workspace network must use as its only egress path.
type NetworkEndpoint struct {
	Workspace       string
	Generation      uint64
	ReverseProxyURL string
	ForwardProxyURL string
}

// NetworkController is required when a backend advertises
// EgressMode=enforced_gateway. Applying policy must fail closed, and revoke
// must synchronously remove the workspace's network capability.
type NetworkController interface {
	ApplyNetworkPolicy(context.Context, proto.NetworkPolicy, NetworkEndpoint) error
	RevokeNetwork(context.Context) error
}

// BrokerAdvertiser exposes the exact node-owned address a workspace boundary
// can reach before its broker is started. Enforced gateways use a private host
// veth address; returning a wildcard or loopback would fail policy activation.
type BrokerAdvertiser interface {
	BrokerAdvertiseHost() string
}

// BaseEnv is the environment every session starts from, before workspace
// and session env. HOME is the workspace root so dotfiles live with it.
func BaseEnv(root string) []string {
	path := os.Getenv("PATH")
	if path == "" {
		path = "/usr/local/bin:/usr/bin:/bin"
	}
	return []string{
		"PATH=" + path,
		"HOME=" + root,
		"USER=remount",
		"LANG=C.UTF-8",
		"REMOUNT_WORKSPACE=" + filepath.Base(root),
	}
}

// MergeEnv overlays layers of KEY=VALUE, later winning, sorted for determinism.
func MergeEnv(layers ...[]string) []string {
	m := map[string]string{}
	for _, layer := range layers {
		for _, kv := range layer {
			k, v, ok := strings.Cut(kv, "=")
			if !ok {
				continue
			}
			m[k] = v
		}
	}
	out := make([]string, 0, len(m))
	for k, v := range m {
		out = append(out, k+"="+v)
	}
	sort.Strings(out)
	return out
}

// MapEnv converts a map to KEY=VALUE slice.
func MapEnv(m map[string]string) []string {
	out := make([]string, 0, len(m))
	for k, v := range m {
		out = append(out, k+"="+v)
	}
	sort.Strings(out)
	return out
}

// ---------------------------------------------------------------------------
// process backend
// ---------------------------------------------------------------------------

// Process runs sessions as ordinary host processes inside a directory.
type Process struct {
	Dir string // parent directory for workspace roots
}

// NewProcess creates the backend rooted at dir.
func NewProcess(dir string) (*Process, error) {
	dir, err := backendRoot(dir)
	if err != nil {
		return nil, err
	}
	return &Process{Dir: dir}, nil
}

// backendRoot creates dir and returns it as an absolute path. Backends hand
// workspace roots to other programs (docker bind mounts) and outlive any
// working directory the node started in.
func backendRoot(dir string) (string, error) {
	abs, err := filepath.Abs(dir)
	if err != nil {
		return "", fmt.Errorf("backend root %q: %w", dir, err)
	}
	if err := os.MkdirAll(abs, 0o700); err != nil {
		return "", err
	}
	return abs, nil
}

func (p *Process) Name() string { return "process" }

func (p *Process) Caps() Caps {
	return Caps{
		Isolation: "none", Snapshots: "fs", EgressEnforced: false,
		EgressMode: "cooperative_proxy", BrokerIdentity: "token", FilesystemBoundary: "root_handle",
	}
}

func (p *Process) root(id string) string { return filepath.Join(p.Dir, id) }

func (p *Process) Create(ctx context.Context, id string, spec proto.WorkspaceSpec, restore io.Reader) (Handle, error) {
	if spec.MountPath != "" && spec.MountPath != proto.DefaultMountPath {
		return nil, proto.Err(proto.CodeUnsupported, "process backend cannot materialize at %s: it has no mount namespace", spec.MountPath)
	}
	root := p.root(id)
	if _, err := os.Stat(root); err == nil {
		return nil, proto.Err(proto.CodeConflict, "workspace %s already exists on this node", id)
	}
	if err := os.MkdirAll(root, 0o755); err != nil {
		return nil, err
	}
	if restore != nil {
		if err := artifact.Restore(root, restore); err != nil {
			_ = os.RemoveAll(root)
			return nil, fmt.Errorf("restore: %w", err)
		}
	}
	return newProcessHandle(id, root)
}

func (p *Process) Adopt(ctx context.Context, id string) (Handle, error) {
	root := p.root(id)
	if st, err := os.Stat(root); err != nil || !st.IsDir() {
		return nil, proto.Err(proto.CodeNotFound, "no workspace %s here", id)
	}
	return newProcessHandle(id, root)
}

type processHandle struct {
	id   string
	root string
	fs   *fsops.FS
}

func newProcessHandle(id, root string) (*processHandle, error) {
	f, err := fsops.New(root)
	if err != nil {
		return nil, err
	}
	return &processHandle{id: id, root: f.Root(), fs: f}, nil
}

func (h *processHandle) ID() string      { return h.id }
func (h *processHandle) Backend() string { return "process" }
func (h *processHandle) FS() FileSystem  { return h.fs }

func (h *processHandle) Prepare(spec *session.Spec) error {
	cwd, err := h.fs.Resolve(spec.Cwd)
	if err != nil {
		return err
	}
	if st, err := os.Stat(cwd); err != nil || !st.IsDir() {
		return proto.Err(proto.CodeNotFound, "cwd %q does not exist", spec.Cwd)
	}
	spec.Cwd = cwd
	spec.Env = MergeEnv(BaseEnv(h.root), spec.Env)
	return nil
}

func (h *processHandle) Snapshot(ctx context.Context, excludes []string, w io.Writer) error {
	return artifact.Snapshot(h.root, excludes, w)
}

func (h *processHandle) Checkpoint(ctx context.Context, excludes []string, w io.Writer) error {
	// The process backend has no independent runtime. Its only managed writers
	// are sessions, which the node fences and joins before entering this call.
	// Host processes are outside the local/unisolated backend's trust boundary.
	return artifact.Snapshot(h.root, excludes, w)
}

func (h *processHandle) Destroy(ctx context.Context) error {
	_ = h.fs.Close()
	return os.RemoveAll(h.root)
}

// ---------------------------------------------------------------------------
// docker backend
// ---------------------------------------------------------------------------

// Docker runs each workspace as a long-lived container with the workspace
// directory bind-mounted at /work. Filesystem operations and snapshots use
// the host directory; processes run via docker exec.
type Docker struct {
	Dir     string
	Image   string // default image when the spec has none
	Binary  string // "docker" or "podman"
	mu      sync.Mutex
	checked bool
	err     error
}

// DefaultImageRepository is the registry path of the image CI builds from
// images/workspace/Dockerfile. docs/images.md is the contract.
const DefaultImageRepository = "ghcr.io/andrewgcodes/remount-workspace"

// DefaultImage returns the docker image a workspace gets when its spec names
// none. A release binary pins the tag built alongside it; a development build
// (`dev`, or any version that is not a release tag) tracks `latest`.
func DefaultImage(version string) string {
	if strings.HasPrefix(version, "v") && !strings.Contains(version, "-dirty") {
		return DefaultImageRepository + ":" + version
	}
	return DefaultImageRepository + ":latest"
}

// NewDocker creates the backend; the daemon is checked lazily on first use.
// An empty defaultImage selects DefaultImage("dev"); pass an explicit image
// such as ubuntu:24.04 to keep a plain distro workspace.
func NewDocker(dir, defaultImage string) (*Docker, error) {
	dir, err := backendRoot(dir)
	if err != nil {
		return nil, err
	}
	if defaultImage == "" {
		defaultImage = DefaultImage("dev")
	}
	return &Docker{Dir: dir, Image: defaultImage, Binary: "docker"}, nil
}

func (d *Docker) Name() string { return "docker" }

func (d *Docker) Caps() Caps {
	return Caps{
		Isolation: "container", Snapshots: "fs", EgressEnforced: false,
		EgressMode: "cooperative_proxy", BrokerIdentity: "token", FilesystemBoundary: "bind_mount",
		NetworkNamespace: true, DeviceIsolation: true, MountPath: true,
	}
}

// Available reports whether the docker daemon answers.
func (d *Docker) Available(ctx context.Context) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.checked {
		return d.err
	}
	d.checked = true
	if _, err := exec.LookPath(d.Binary); err != nil {
		d.err = err
		return err
	}
	if out, err := exec.CommandContext(ctx, d.Binary, "info", "--format", "{{.ServerVersion}}").CombinedOutput(); err != nil {
		d.err = fmt.Errorf("docker daemon unavailable: %s", strings.TrimSpace(string(out)))
		return d.err
	}
	if err := d.probeSharedFilesystem(ctx); err != nil {
		d.err = err
		return d.err
	}
	return nil
}

// probeSharedFilesystem proves the daemon can see this node's data directory.
//
// The backend bind-mounts a per-workspace directory into the container, which
// silently assumes the daemon shares a filesystem with the node. When it does
// not — a daemon in a VM, a forwarded socket, a CI runner with the daemon in a
// sidecar — nothing fails. The workspace is created, writes are accepted, the
// session starts and reports the right mount path, and the container sees an
// empty directory. Observed exactly that way by pointing this backend at a
// daemon inside a Linux VM: Create succeeded, FS().Write succeeded, and the
// container could not read the file the node had just written.
//
// So the assumption is checked once, by demonstration, in the same spirit as
// the read-only volume mount probe and the gVisor denial probe: a byte is
// written on this side and read back from inside a throwaway container. A
// backend that cannot show it works is refused rather than offered.
func (d *Docker) probeSharedFilesystem(ctx context.Context) error {
	dir, err := os.MkdirTemp(d.Dir, ".probe-")
	if err != nil {
		return fmt.Errorf("docker filesystem probe: %w", err)
	}
	defer os.RemoveAll(dir)
	// A nonce, so a stale file or a coincidentally present path cannot pass.
	var nonce [16]byte
	if _, err := rand.Read(nonce[:]); err != nil {
		return fmt.Errorf("docker filesystem probe: %w", err)
	}
	want := hex.EncodeToString(nonce[:])
	if err := os.WriteFile(filepath.Join(dir, "probe"), []byte(want), 0o644); err != nil {
		return fmt.Errorf("docker filesystem probe: %w", err)
	}
	probeCtx, cancel := context.WithTimeout(ctx, dockerProbeTimeout)
	defer cancel()
	out, err := exec.CommandContext(probeCtx, d.Binary, "run", "--rm",
		"-v", dir+":/probe:ro", d.Image, "cat", "/probe/probe").CombinedOutput()
	if err != nil {
		return fmt.Errorf("docker filesystem probe: the daemon could not read this node's data directory %s: %s: %w",
			d.Dir, strings.TrimSpace(string(out)), err)
	}
	if strings.TrimSpace(string(out)) != want {
		return fmt.Errorf("docker filesystem probe: the daemon bind-mounted %s and saw different content, "+
			"so it does not share a filesystem with this node; workspaces would start and silently contain none of their data", d.Dir)
	}
	return nil
}

// dockerProbeTimeout bounds the probe, including any image pull it triggers.
const dockerProbeTimeout = 2 * time.Minute

func (d *Docker) container(id string) string { return "remount-" + strings.ReplaceAll(id, "_", "-") }

func (d *Docker) Create(ctx context.Context, id string, spec proto.WorkspaceSpec, restore io.Reader) (Handle, error) {
	if err := d.Available(ctx); err != nil {
		return nil, proto.Err(proto.CodeUnsupported, "%v", err)
	}
	if err := proto.ValidateMountPath(spec.MountPath); err != nil {
		return nil, err
	}
	mount := spec.MountPath
	if mount == "" {
		mount = proto.DefaultMountPath
	}
	root := filepath.Join(d.Dir, id)
	if _, err := os.Stat(root); err == nil {
		return nil, proto.Err(proto.CodeConflict, "workspace %s already exists on this node", id)
	}
	if err := os.MkdirAll(root, 0o755); err != nil {
		return nil, err
	}
	if restore != nil {
		if err := artifact.Restore(root, restore); err != nil {
			_ = os.RemoveAll(root)
			return nil, fmt.Errorf("restore: %w", err)
		}
	}
	image := spec.Image
	if image == "" {
		image = d.Image
	}
	name := d.container(id)
	args := []string{"run", "-d", "--name", name, "--init",
		"-v", root + ":" + mount, "-w", mount,
		"--add-host", "host.docker.internal:host-gateway",
		"--label", "remount.workspace=" + id,
	}
	if spec.Requires.CPU > 0 {
		args = append(args, "--cpus", fmt.Sprint(spec.Requires.CPU))
	}
	if spec.Requires.MemMiB > 0 {
		args = append(args, "--memory", fmt.Sprintf("%dm", spec.Requires.MemMiB))
	}
	args = append(args, image, "sleep", "infinity")
	if out, err := exec.CommandContext(ctx, d.Binary, args...).CombinedOutput(); err != nil {
		cleanupErr := d.cleanupFailedContainer(ctx, name, root)
		if cleanupErr == nil {
			if removeErr := os.RemoveAll(root); removeErr != nil {
				cleanupErr = fmt.Errorf("remove workspace root: %w", removeErr)
			}
		}
		if cleanupErr != nil {
			return nil, proto.Err(proto.CodeInternal, "docker run: %s; cleanup: %v", strings.TrimSpace(string(out)), cleanupErr)
		}
		return nil, proto.Err(proto.CodeInternal, "docker run: %s", strings.TrimSpace(string(out)))
	}
	return d.handle(id, root, name, mount)
}

func (d *Docker) cleanupFailedContainer(ctx context.Context, name, root string) error {
	cleanupCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 30*time.Second)
	defer cancel()
	out, err := exec.CommandContext(cleanupCtx, d.Binary, "inspect", "--format", "{{range .Mounts}}{{println .Source}}{{end}}", name).CombinedOutput()
	if err != nil {
		message := strings.TrimSpace(string(out))
		if strings.Contains(strings.ToLower(message), "no such") {
			return nil
		}
		return fmt.Errorf("inspect %s: %s", name, message)
	}
	ownsContainer := false
	for _, source := range strings.Split(strings.TrimSpace(string(out)), "\n") {
		if source == root {
			ownsContainer = true
			break
		}
	}
	if !ownsContainer {
		return fmt.Errorf("container %s does not mount workspace root %s", name, root)
	}
	out, err = exec.CommandContext(cleanupCtx, d.Binary, "rm", "-f", name).CombinedOutput()
	if err != nil {
		return fmt.Errorf("%s", strings.TrimSpace(string(out)))
	}
	return nil
}

func (d *Docker) Adopt(ctx context.Context, id string) (Handle, error) {
	root := filepath.Join(d.Dir, id)
	if st, err := os.Stat(root); err != nil || !st.IsDir() {
		return nil, proto.Err(proto.CodeNotFound, "no workspace %s here", id)
	}
	name := d.container(id)
	out, err := exec.CommandContext(ctx, d.Binary, "inspect", "--format", "{{.State.Running}}", name).CombinedOutput()
	if err != nil {
		return nil, proto.Err(proto.CodeNotFound, "container %s missing: %s", name, strings.TrimSpace(string(out)))
	}
	if strings.TrimSpace(string(out)) != "true" {
		if out, err := exec.CommandContext(ctx, d.Binary, "start", name).CombinedOutput(); err != nil {
			return nil, proto.Err(proto.CodeInternal, "docker start: %s", strings.TrimSpace(string(out)))
		}
	}
	// The container is the durable record of where the tree is mounted.
	out, err = exec.CommandContext(ctx, d.Binary, "inspect", "--format", "{{range .Mounts}}{{.Source}}\t{{.Destination}}\n{{end}}", name).CombinedOutput()
	if err != nil {
		return nil, proto.Err(proto.CodeInternal, "inspect mounts of %s: %s", name, strings.TrimSpace(string(out)))
	}
	mount := mountDestination(string(out), root)
	return d.handle(id, root, name, mount)
}

// mountDestination picks the container path of the bind mount whose host
// source is root from `docker inspect` output (one "source\tdestination" per
// line). A lone mount is taken as-is so a symlinked data dir still resolves.
func mountDestination(inspect, root string) string {
	var lines []string
	for _, line := range strings.Split(strings.TrimSpace(inspect), "\n") {
		if line = strings.TrimSpace(line); line != "" {
			lines = append(lines, line)
		}
	}
	for _, line := range lines {
		src, dst, ok := strings.Cut(line, "\t")
		if ok && src == root && dst != "" {
			return dst
		}
	}
	if len(lines) == 1 {
		if _, dst, ok := strings.Cut(lines[0], "\t"); ok && dst != "" {
			return dst
		}
	}
	return proto.DefaultMountPath
}

func (d *Docker) handle(id, root, name, mount string) (Handle, error) {
	f, err := fsops.New(root)
	if err != nil {
		return nil, err
	}
	return &dockerHandle{id: id, root: f.Root(), fs: f, name: name, bin: d.Binary, mount: mount}, nil
}

type dockerHandle struct {
	id, root, name, bin string
	// mount is the tree's path inside the container (WorkspaceSpec.MountPath).
	mount string
	fs    *fsops.FS
}

func (h *dockerHandle) ID() string        { return h.id }
func (h *dockerHandle) Backend() string   { return "docker" }
func (h *dockerHandle) FS() FileSystem    { return h.fs }
func (h *dockerHandle) MountPath() string { return h.mount }

func (h *dockerHandle) Prepare(spec *session.Spec) error {
	if spec.Kind == proto.SessionPort {
		// Ports inside the container are reachable via docker's network; v0
		// resolves them through the container IP.
		out, err := exec.Command(h.bin, "inspect", "--format", "{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}", h.name).Output()
		if err != nil {
			return proto.Err(proto.CodeInternal, "inspect: %v", err)
		}
		spec.Host = strings.TrimSpace(string(out))
		return nil
	}
	cwd := h.mount
	if spec.Cwd != "" {
		c, err := h.fs.Resolve(spec.Cwd)
		if err != nil {
			return err
		}
		rel, _ := filepath.Rel(h.root, c)
		cwd = filepath.ToSlash(filepath.Join(h.mount, rel))
	}
	args := []string{"exec", "-i", "-w", cwd}
	if spec.Kind == proto.SessionPTY {
		args = append(args, "-t")
	}
	env := MergeEnv([]string{"HOME=" + h.mount, "USER=root", "LANG=C.UTF-8", "REMOUNT_WORKSPACE=" + h.id}, spec.Env)
	for _, kv := range env {
		args = append(args, "-e", kv)
	}
	args = append(args, h.name)
	args = append(args, spec.Program...)
	spec.Program = append([]string{h.bin}, args...)
	spec.Cwd = ""
	spec.Env = MergeEnv(os.Environ()) // the docker CLI itself needs the host env
	return nil
}

// reown hands the bind mount back to the node's uid. The container runs as
// root, so everything a harness writes lands on the host owned by root; a
// non-root node could then neither snapshot a 0600 file nor delete the tree.
// Runs inside the container so it works even when the node cannot chown.
func (h *dockerHandle) reown(ctx context.Context) error {
	if os.Getuid() == 0 {
		return nil
	}
	owner := fmt.Sprintf("%d:%d", os.Getuid(), os.Getgid())
	out, err := exec.CommandContext(ctx, h.bin, "exec", h.name, "chown", "-R", owner, h.mount).CombinedOutput()
	if err != nil {
		return proto.Err(proto.CodeInternal, "docker exec chown: %s", strings.TrimSpace(string(out)))
	}
	return nil
}

func (h *dockerHandle) PrepareFilesystemAccess(ctx context.Context) error {
	return h.reown(ctx)
}

func (h *dockerHandle) Snapshot(ctx context.Context, excludes []string, w io.Writer) error {
	if err := h.reown(ctx); err != nil {
		return err
	}
	return artifact.Snapshot(h.root, excludes, w)
}

func (h *dockerHandle) Checkpoint(ctx context.Context, excludes []string, w io.Writer) (err error) {
	if err := h.reown(ctx); err != nil { // a paused container cannot exec
		return err
	}
	out, err := exec.CommandContext(ctx, h.bin, "pause", h.name).CombinedOutput()
	if err != nil {
		return proto.Err(proto.CodeInternal, "docker pause: %s", strings.TrimSpace(string(out)))
	}
	defer func() {
		resumeCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		out, resumeErr := exec.CommandContext(resumeCtx, h.bin, "unpause", h.name).CombinedOutput()
		if resumeErr != nil {
			resumeErr = proto.Err(proto.CodeInternal, "docker unpause: %s", strings.TrimSpace(string(out)))
			err = errors.Join(err, resumeErr)
		}
	}()
	return artifact.Snapshot(h.root, excludes, w)
}

func (h *dockerHandle) Destroy(ctx context.Context) error {
	_ = h.fs.Close()
	// Best effort: the container may already be gone or stopped, and a failed
	// chown surfaces as the RemoveAll error below if it matters.
	_ = h.reown(ctx)
	out, err := exec.CommandContext(ctx, h.bin, "rm", "-f", h.name).CombinedOutput()
	if err != nil && !strings.Contains(string(out), "No such container") {
		return proto.Err(proto.CodeInternal, "docker rm: %s", strings.TrimSpace(string(out)))
	}
	return os.RemoveAll(h.root)
}

// ---------------------------------------------------------------------------
// registry
// ---------------------------------------------------------------------------

// Registry maps backend names to instances on a node.
type Registry struct {
	backends map[string]Backend
	order    []string
}

// NewRegistry builds a registry.
func NewRegistry(backends ...Backend) *Registry {
	r := &Registry{backends: map[string]Backend{}}
	for _, b := range backends {
		r.backends[b.Name()] = b
		r.order = append(r.order, b.Name())
	}
	return r
}

// Get returns a backend by name, or the first registered when name is "".
func (r *Registry) Get(name string) (Backend, error) {
	if name == "" {
		if len(r.order) == 0 {
			return nil, errors.New("workspace: no backends registered")
		}
		return r.backends[r.order[0]], nil
	}
	b, ok := r.backends[name]
	if !ok {
		return nil, proto.Err(proto.CodeUnsupported, "backend %q not available on this node", name)
	}
	return b, nil
}

// Names lists registered backends in order.
func (r *Registry) Names() []string { return append([]string(nil), r.order...) }

// Descriptors returns immutable, per-backend capability evidence.
func (r *Registry) Descriptors() []proto.BackendDescriptor {
	out := make([]proto.BackendDescriptor, 0, len(r.order))
	for _, name := range r.order {
		caps := r.backends[name].Caps()
		egress := caps.EgressMode
		if egress == "" {
			egress = "open"
		}
		if caps.EgressEnforced {
			egress = "enforced_gateway"
		}
		out = append(out, proto.BackendDescriptor{
			Name: name,
			Security: proto.BackendSecurityCaps{
				Isolation: caps.Isolation, MultiTenant: caps.MultiTenant,
				SiblingIsolation: caps.SiblingIsolation, EgressMode: egress,
				BrokerIdentity: caps.BrokerIdentity, FilesystemBoundary: caps.FilesystemBoundary,
				NetworkNamespace: caps.NetworkNamespace, DeviceIsolation: caps.DeviceIsolation,
			},
			Runtime: proto.RuntimeCaps{Snapshots: caps.Snapshots, Display: caps.Display, MountPath: caps.MountPath},
		})
	}
	return out
}

// Descriptor reports one registered backend's capabilities.
func (r *Registry) Descriptor(name string) (proto.BackendDescriptor, error) {
	for _, d := range r.Descriptors() {
		if d.Name == name {
			return d, nil
		}
	}
	return proto.BackendDescriptor{}, proto.Err(proto.CodeUnsupported, "backend %q not available on this node", name)
}

// HostInfo fills the static parts of NodeInfo.
func HostInfo(backends []string) proto.NodeInfo {
	return proto.NodeInfo{
		Backends:  backends,
		OS:        runtime.GOOS,
		Arch:      runtime.GOARCH,
		CPU:       runtime.NumCPU(),
		MemMiB:    totalMemMiB(),
		Snapshots: "fs",
	}
}

// HostInfoForRegistry fills NodeInfo with backend-specific descriptors.
func HostInfoForRegistry(r *Registry) proto.NodeInfo {
	info := HostInfo(r.Names())
	info.BackendDescriptors = r.Descriptors()
	return info
}
