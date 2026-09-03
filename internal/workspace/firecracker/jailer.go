package firecracker

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"remount.dev/remount/internal/proto"
)

const (
	defaultMachineLimit = 128
	defaultVCPULimit    = 32
	defaultMemMiBLimit  = 131072
	defaultNoFileLimit  = 2048
	defaultFileSize     = int64(256 << 30)
	defaultAPITimeout   = 10 * time.Second
)

// JailerOptions configures the production Firecracker process adapter. The
// caller supplies cgroup controls appropriate for its host; omitting them is
// refused because guest sizing alone does not bound host process overhead.
type JailerOptions struct {
	Firecracker   string
	Jailer        string
	KVM           string
	ChrootBase    string
	UID           int
	GID           int
	CgroupVersion int
	CgroupParent  string
	Cgroups       []string
	MaxMachines   int
	MaxVCPU       int
	MaxMemMiB     int
	NoFileLimit   int
	FileSizeLimit int64
	StartTimeout  time.Duration
	APITimeout    time.Duration
}

// JailerFactory starts one non-daemonized jailer process per machine and owns
// its admission slot until Kill has joined the complete process group.
type JailerFactory struct {
	opts JailerOptions

	probeMu  sync.Mutex
	probed   bool
	hostLock *os.File
	mu       sync.Mutex
	active   map[string]*jailerMachine
	serial   atomic.Uint64
	compat   Compatibility
}

// NewJailerFactory validates static configuration. Probe performs the Linux,
// KVM, executable, path-ownership and cgroup checks which carry capability.
func NewJailerFactory(opts JailerOptions) (*JailerFactory, error) {
	if opts.Firecracker == "" {
		opts.Firecracker = "firecracker"
	}
	if opts.Jailer == "" {
		opts.Jailer = "jailer"
	}
	if opts.KVM == "" {
		opts.KVM = "/dev/kvm"
	}
	if opts.ChrootBase == "" {
		opts.ChrootBase = "/srv/jailer"
	}
	if opts.UID <= 0 || opts.GID <= 0 {
		return nil, errors.New("firecracker: dedicated non-root jailer UID and GID are required")
	}
	if opts.CgroupVersion == 0 {
		opts.CgroupVersion = 2
	}
	if opts.CgroupVersion != 1 && opts.CgroupVersion != 2 {
		return nil, errors.New("firecracker: cgroup version must be 1 or 2")
	}
	if opts.CgroupParent == "" || len(opts.Cgroups) == 0 {
		return nil, errors.New("firecracker: an existing cgroup parent and explicit resource controls are required")
	}
	if err := validateRelativePath(opts.CgroupParent); err != nil {
		return nil, fmt.Errorf("firecracker: cgroup parent: %w", err)
	}
	for _, limit := range opts.Cgroups {
		if err := validateCgroup(limit); err != nil {
			return nil, err
		}
	}
	if opts.MaxMachines <= 0 {
		opts.MaxMachines = defaultMachineLimit
	}
	if opts.MaxVCPU <= 0 {
		opts.MaxVCPU = defaultVCPULimit
	}
	if opts.MaxMemMiB <= 0 {
		opts.MaxMemMiB = defaultMemMiBLimit
	}
	if opts.NoFileLimit <= 0 {
		opts.NoFileLimit = defaultNoFileLimit
	}
	if opts.FileSizeLimit <= 0 {
		opts.FileSizeLimit = defaultFileSize
	}
	if opts.StartTimeout <= 0 {
		opts.StartTimeout = defaultAPITimeout
	}
	if opts.APITimeout <= 0 {
		opts.APITimeout = defaultAPITimeout
	}
	for field, value := range map[string]int{"max machines": opts.MaxMachines, "max vCPU": opts.MaxVCPU, "max memory MiB": opts.MaxMemMiB, "no-file limit": opts.NoFileLimit} {
		if value <= 0 {
			return nil, fmt.Errorf("firecracker: %s must be positive", field)
		}
	}
	abs, err := filepath.Abs(opts.ChrootBase)
	if err != nil {
		return nil, err
	}
	opts.ChrootBase = abs
	return &JailerFactory{opts: opts, active: make(map[string]*jailerMachine)}, nil
}

// Jailed reports that this factory can only start Firecracker through jailer.
func (*JailerFactory) Jailed() bool { return true }

// Probe fails closed unless the host can open KVM read-write and all trusted
// jailer inputs and cgroup controls are available.
func (f *JailerFactory) Probe(ctx context.Context) error {
	f.probeMu.Lock()
	defer f.probeMu.Unlock()
	if f.probed {
		return nil
	}
	if runtime.GOOS != "linux" {
		return fmt.Errorf("firecracker requires Linux KVM, current OS is %s", runtime.GOOS)
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	kvm, err := os.OpenFile(f.opts.KVM, os.O_RDWR, 0)
	if err != nil {
		return fmt.Errorf("open %s read-write: %w", f.opts.KVM, err)
	}
	if err := kvm.Close(); err != nil {
		return fmt.Errorf("close %s probe: %w", f.opts.KVM, err)
	}
	for name, raw := range map[string]string{"firecracker": f.opts.Firecracker, "jailer": f.opts.Jailer} {
		path, err := exec.LookPath(raw)
		if err != nil {
			return fmt.Errorf("%s executable unavailable: %w", name, err)
		}
		path, err = filepath.Abs(path)
		if err != nil {
			return err
		}
		if err := trustedExecutable(path); err != nil {
			return fmt.Errorf("%s executable: %w", name, err)
		}
		if _, err := boundedCommand(ctx, path, "--version"); err != nil {
			return fmt.Errorf("%s version probe: %w", name, err)
		}
		if name == "firecracker" {
			f.opts.Firecracker = path
		} else {
			f.opts.Jailer = path
		}
	}
	compat, err := DetectCompatibility(ctx, f.opts.Firecracker)
	if err != nil {
		return err
	}
	if err := os.MkdirAll(f.opts.ChrootBase, 0o700); err != nil {
		return fmt.Errorf("create jailer base: %w", err)
	}
	if err := trustedDirectory(f.opts.ChrootBase); err != nil {
		return fmt.Errorf("jailer base: %w", err)
	}
	hostLock, err := acquireHostLock(filepath.Join(f.opts.ChrootBase, ".remount-firecracker.lock"))
	if err != nil {
		return fmt.Errorf("acquire exclusive Firecracker host ownership: %w", err)
	}
	locked := true
	defer func() {
		if locked {
			_ = releaseHostLock(hostLock)
		}
	}()
	if err := recoverOwnedJails(ctx, f.opts.ChrootBase, filepath.Base(f.opts.Firecracker), f.opts.MaxMachines*4); err != nil {
		return fmt.Errorf("recover retained Firecracker jails: %w", err)
	}
	if err := validateRelativePath(f.opts.CgroupParent); err != nil {
		return fmt.Errorf("cgroup parent: %w", err)
	}
	cgroupRoot := filepath.Join("/sys/fs/cgroup", f.opts.CgroupParent)
	if st, err := os.Stat(cgroupRoot); err != nil || !st.IsDir() {
		return fmt.Errorf("cgroup parent %q is unavailable", cgroupRoot)
	}
	f.hostLock = hostLock
	f.compat = compat
	f.probed = true
	locked = false
	return nil
}

// Close releases the host ownership lock only when every VMM has been joined.
func (f *JailerFactory) Close() error {
	f.probeMu.Lock()
	defer f.probeMu.Unlock()
	f.mu.Lock()
	active := len(f.active)
	f.mu.Unlock()
	if active != 0 {
		return fmt.Errorf("firecracker: cannot close factory with %d active machines", active)
	}
	if f.hostLock == nil {
		return nil
	}
	err := releaseHostLock(f.hostLock)
	if err == nil {
		f.hostLock = nil
		f.probed = false
	}
	return err
}

// New starts a fresh jailed API process inside the supplied network namespace.
func (f *JailerFactory) New(ctx context.Context, workspace string, launch MachineLaunch) (Machine, error) {
	f.probeMu.Lock()
	probed := f.probed
	f.probeMu.Unlock()
	if !probed {
		return nil, errors.New("firecracker: jailer factory has not passed Probe")
	}
	if err := validateWorkspaceID(workspace); err != nil {
		return nil, err
	}
	if launch.NetworkNamespace == "" || !filepath.IsAbs(launch.NetworkNamespace) {
		return nil, errors.New("firecracker: an absolute network namespace handle is required")
	}
	resolvedNamespace, err := filepath.EvalSymlinks(launch.NetworkNamespace)
	if err != nil {
		return nil, fmt.Errorf("firecracker: resolve network namespace: %w", err)
	}
	if st, err := os.Lstat(resolvedNamespace); err != nil || !st.Mode().IsRegular() {
		return nil, fmt.Errorf("firecracker: network namespace %q is unavailable", launch.NetworkNamespace)
	}
	if err := trustedDirectory(filepath.Dir(resolvedNamespace)); err != nil {
		return nil, fmt.Errorf("firecracker: network namespace parent: %w", err)
	}
	launch.NetworkNamespace = resolvedNamespace

	f.mu.Lock()
	if len(f.active) >= f.opts.MaxMachines {
		f.mu.Unlock()
		return nil, proto.Err(proto.CodeResourceExhausted, "Firecracker machine capacity %d exhausted", f.opts.MaxMachines)
	}
	if _, exists := f.active[workspace]; exists {
		f.mu.Unlock()
		return nil, fmt.Errorf("firecracker: machine for %s is already active", workspace)
	}
	f.active[workspace] = nil
	f.mu.Unlock()
	reserved := true
	defer func() {
		if reserved {
			f.release(workspace, nil)
		}
	}()

	id := jailID(workspace, f.serial.Add(1), time.Now().UnixNano())
	root := filepath.Join(f.opts.ChrootBase, filepath.Base(f.opts.Firecracker), id, "root")
	if _, err := os.Lstat(filepath.Dir(root)); err == nil {
		return nil, fmt.Errorf("firecracker: jail %s already exists; refusing to reuse ambiguous state", id)
	} else if !errors.Is(err, os.ErrNotExist) {
		return nil, err
	}
	args := f.jailerArgs(id, launch.NetworkNamespace)
	cmd := exec.Command(f.opts.Jailer, args...)
	configureProcessGroup(cmd)
	cmd.Stdin, cmd.Stdout, cmd.Stderr = nil, io.Discard, io.Discard
	if err := cmd.Start(); err != nil {
		return nil, fmt.Errorf("start jailer: %w", err)
	}
	m := &jailerMachine{
		factory: f, workspace: workspace, id: id, root: root, cmd: cmd,
		done: make(chan struct{}), waitErr: make(chan error, 1),
		maxVCPU: f.opts.MaxVCPU, maxMemMiB: f.opts.MaxMemMiB,
		uid: f.opts.UID, gid: f.opts.GID, fileSizeLimit: f.opts.FileSizeLimit,
		compatibility: f.compat,
	}
	go func() {
		m.waitErr <- cmd.Wait()
		close(m.done)
	}()
	socket := filepath.Join(root, "run", "firecracker.socket")
	m.api = newUnixAPI(socket, f.opts.APITimeout)
	readyCtx, cancel := context.WithTimeout(ctx, f.opts.StartTimeout)
	defer cancel()
	if err := m.waitReady(readyCtx, socket); err != nil {
		cleanupCtx, cleanupCancel := cleanupContext()
		defer cleanupCancel()
		return nil, errors.Join(err, m.Kill(cleanupCtx))
	}
	f.mu.Lock()
	f.active[workspace] = m
	f.mu.Unlock()
	reserved = false
	return m, nil
}

func (f *JailerFactory) jailerArgs(id, networkNamespace string) []string {
	args := []string{
		"--id", id,
		"--exec-file", f.opts.Firecracker,
		"--uid", strconv.Itoa(f.opts.UID),
		"--gid", strconv.Itoa(f.opts.GID),
		"--chroot-base-dir", f.opts.ChrootBase,
		"--netns", networkNamespace,
		"--cgroup-version", strconv.Itoa(f.opts.CgroupVersion),
		"--parent-cgroup", f.opts.CgroupParent,
		"--resource-limit", "no-file=" + strconv.Itoa(f.opts.NoFileLimit),
		"--resource-limit", "fsize=" + strconv.FormatInt(f.opts.FileSizeLimit, 10),
		"--new-pid-ns",
	}
	for _, limit := range f.opts.Cgroups {
		args = append(args, "--cgroup", limit)
	}
	return append(args, "--", "--api-sock", "/run/firecracker.socket")
}

func (f *JailerFactory) release(workspace string, expected *jailerMachine) {
	f.mu.Lock()
	if current, exists := f.active[workspace]; exists && (expected == nil || current == expected) {
		delete(f.active, workspace)
	}
	f.mu.Unlock()
}

type jailerMachine struct {
	factory   *JailerFactory
	workspace string
	id        string
	root      string
	cmd       *exec.Cmd
	api       API
	done      chan struct{}
	waitErr   chan error

	maxVCPU, maxMemMiB int
	uid, gid           int
	fileSizeLimit      int64
	compatibility      Compatibility

	killMu     sync.Mutex
	killSent   bool
	cleaned    bool
	waited     bool
	processErr error
}

func (m *jailerMachine) waitReady(ctx context.Context, socket string) error {
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		if st, err := os.Lstat(socket); err == nil && st.Mode()&os.ModeSocket != 0 {
			var version struct {
				Version string `json:"firecracker_version"`
			}
			if err := m.api.Get(ctx, "/version", &version); err == nil && version.Version != "" {
				return nil
			}
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("wait for jailed Firecracker API: %w", ctx.Err())
		case <-m.done:
			return fmt.Errorf("jailer exited before API readiness: %w", m.consumeWait())
		case <-ticker.C:
		}
	}
}

func (m *jailerMachine) Configure(ctx context.Context, cfg MachineConfig) error {
	if cfg.VCPU <= 0 || cfg.VCPU > m.maxVCPU || cfg.MemMiB < 128 || cfg.MemMiB > m.maxMemMiB {
		return fmt.Errorf("firecracker: machine request %d vCPU/%d MiB exceeds configured bounds %d/%d", cfg.VCPU, cfg.MemMiB, m.maxVCPU, m.maxMemMiB)
	}
	if !cfg.TrackDirty {
		return errors.New("firecracker: dirty page tracking is required for memory checkpoints")
	}
	if !cfg.GuestIP.Is4() || !cfg.GatewayIP.Is4() || cfg.GuestIP.Prev() != cfg.GatewayIP {
		return errors.New("firecracker: valid adjacent guest and gateway IPv4 addresses are required")
	}
	kernel, err := m.stageCopy("kernel", cfg.KernelImage, 0o400)
	if err != nil {
		return err
	}
	root, err := m.stageLink("rootfs.ext4", cfg.RootDrive, 0o600)
	if err != nil {
		return err
	}
	requests := []struct {
		path string
		body any
	}{
		{"/boot-source", map[string]any{"kernel_image_path": kernel, "boot_args": fmt.Sprintf("console=ttyS0 reboot=k panic=1 pci=off ip=%s::%s:255.255.255.252::eth0:off", cfg.GuestIP, cfg.GatewayIP)}},
		{"/drives/rootfs", map[string]any{"drive_id": "rootfs", "path_on_host": root, "is_root_device": true, "is_read_only": false}},
		{"/network-interfaces/eth0", map[string]any{"iface_id": "eth0", "host_dev_name": cfg.TapName, "guest_mac": guestMAC(m.workspace)}},
		{"/vsock", map[string]any{"guest_cid": 3, "uds_path": "/run/guest.vsock"}},
		{"/machine-config", map[string]any{"vcpu_count": cfg.VCPU, "mem_size_mib": cfg.MemMiB, "track_dirty_pages": true}},
	}
	for _, request := range requests {
		if err := m.api.Put(ctx, request.path, request.body); err != nil {
			return err
		}
	}
	return nil
}

func (m *jailerMachine) Start(ctx context.Context) error {
	return m.api.Put(ctx, "/actions", map[string]string{"action_type": "InstanceStart"})
}

func (m *jailerMachine) Pause(ctx context.Context) error {
	return m.api.Patch(ctx, "/vm", map[string]string{"state": "Paused"})
}

func (m *jailerMachine) Resume(ctx context.Context) error {
	return m.api.Patch(ctx, "/vm", map[string]string{"state": "Resumed"})
}

func (m *jailerMachine) GuestSocket() string { return filepath.Join(m.root, "run", "guest.vsock") }

func (m *jailerMachine) CreateSnapshot(ctx context.Context) (SnapshotFiles, error) {
	dir := filepath.Join(m.root, "snapshots")
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return SnapshotFiles{}, err
	}
	if err := os.Chown(dir, m.uid, m.gid); err != nil {
		return SnapshotFiles{}, err
	}
	files := SnapshotFiles{State: filepath.Join(dir, "vm.state"), Memory: filepath.Join(dir, "vm.mem"), Compatibility: m.compatibility}
	for _, path := range []string{files.State, files.Memory} {
		if err := removeOwnedRegular(path, dir); err != nil {
			return SnapshotFiles{}, err
		}
	}
	if err := m.api.Put(ctx, "/snapshot/create", map[string]any{
		"snapshot_type": "Full", "snapshot_path": "/snapshots/vm.state", "mem_file_path": "/snapshots/vm.mem",
	}); err != nil {
		return SnapshotFiles{}, err
	}
	for _, path := range []string{files.State, files.Memory} {
		st, err := os.Lstat(path)
		if err != nil || !st.Mode().IsRegular() || st.Size() < 0 || st.Size() > m.fileSizeLimit {
			return SnapshotFiles{}, fmt.Errorf("firecracker: invalid snapshot output %q", path)
		}
	}
	return files, nil
}

func (m *jailerMachine) LoadSnapshot(ctx context.Context, files SnapshotFiles, rootDrive string) error {
	if _, err := m.stageLink("rootfs.ext4", rootDrive, 0o600); err != nil {
		return err
	}
	state, err := m.stageLink("restore/vm.state", files.State, 0o400)
	if err != nil {
		return err
	}
	memory, err := m.stageLink("restore/vm.mem", files.Memory, 0o600)
	if err != nil {
		return err
	}
	return m.api.Put(ctx, "/snapshot/load", map[string]any{
		"snapshot_path": state,
		"mem_backend":   map[string]string{"backend_type": "File", "backend_path": memory},
		"vsock_override": map[string]string{
			"uds_path": "/run/guest.vsock",
		},
		"resume_vm": false,
	})
}

func (m *jailerMachine) Kill(ctx context.Context) error {
	m.killMu.Lock()
	defer m.killMu.Unlock()
	if m.cleaned {
		return nil
	}

	var stopErr error
	if !m.killSent {
		select {
		case <-m.done:
		default:
			if err := killProcessGroup(m.cmd.Process); err != nil && !errors.Is(err, os.ErrProcessDone) {
				stopErr = err
			} else {
				m.killSent = true
			}
		}
	}
	select {
	case <-m.done:
		waitErr := m.consumeWait()
		if waitErr != nil && stopErr == nil && !expectedKill(waitErr) {
			stopErr = waitErr
		}
	case <-ctx.Done():
		stopErr = errors.Join(stopErr, fmt.Errorf("join jailer: %w", ctx.Err()))
	}
	if stopErr == nil {
		if err := removeJailRoot(m.root, m.factory.opts.ChrootBase); err != nil {
			stopErr = err
		} else {
			m.factory.release(m.workspace, m)
			m.cleaned = true
		}
	}
	return stopErr
}

func (m *jailerMachine) consumeWait() error {
	if m.waited {
		return m.processErr
	}
	m.waited = true
	select {
	case err := <-m.waitErr:
		m.processErr = err
		return m.processErr
	default:
		return nil
	}
}

func (m *jailerMachine) stageCopy(name, source string, mode os.FileMode) (string, error) {
	destination, guest, err := m.stagePaths(name, source)
	if err != nil {
		return "", err
	}
	in, err := os.Open(source)
	if err != nil {
		return "", err
	}
	defer in.Close()
	out, err := os.OpenFile(destination, os.O_WRONLY|os.O_CREATE|os.O_EXCL, mode)
	if err != nil {
		return "", err
	}
	n, copyErr := io.Copy(out, io.LimitReader(in, m.fileSizeLimit+1))
	closeErr := out.Close()
	if copyErr != nil || closeErr != nil || n > m.fileSizeLimit {
		_ = os.Remove(destination)
		if n > m.fileSizeLimit {
			copyErr = fmt.Errorf("resource exceeds %d-byte jail limit", m.fileSizeLimit)
		}
		return "", errors.Join(copyErr, closeErr)
	}
	if err := os.Chown(destination, m.uid, m.gid); err != nil {
		_ = os.Remove(destination)
		return "", err
	}
	return guest, nil
}

func (m *jailerMachine) stageLink(name, source string, mode os.FileMode) (string, error) {
	destination, guest, err := m.stagePaths(name, source)
	if err != nil {
		return "", err
	}
	if err := os.Link(source, destination); err != nil {
		return "", fmt.Errorf("hard-link %q into jail (volume and jail must share a filesystem): %w", source, err)
	}
	if err := os.Chmod(destination, mode); err != nil {
		_ = os.Remove(destination)
		return "", err
	}
	if err := os.Chown(destination, m.uid, m.gid); err != nil {
		_ = os.Remove(destination)
		return "", err
	}
	return guest, nil
}

func (m *jailerMachine) stagePaths(name, source string) (destination, guest string, err error) {
	st, err := os.Lstat(source)
	if err != nil || !st.Mode().IsRegular() {
		return "", "", fmt.Errorf("firecracker: resource %q is not a regular file", source)
	}
	clean := filepath.Clean(name)
	if clean == "." || filepath.IsAbs(clean) || clean == ".." || strings.HasPrefix(clean, ".."+string(filepath.Separator)) {
		return "", "", fmt.Errorf("firecracker: invalid jail resource %q", name)
	}
	destination = filepath.Join(m.root, clean)
	if err := os.MkdirAll(filepath.Dir(destination), 0o700); err != nil {
		return "", "", err
	}
	if err := os.Chown(filepath.Dir(destination), m.uid, m.gid); err != nil {
		return "", "", err
	}
	if err := removeOwnedRegular(destination, m.root); err != nil {
		return "", "", err
	}
	return destination, "/" + filepath.ToSlash(clean), nil
}

func validateCgroup(value string) error {
	name, limit, ok := strings.Cut(value, "=")
	if !ok || name == "" || limit == "" {
		return fmt.Errorf("firecracker: invalid cgroup control %q", value)
	}
	for _, c := range name {
		if (c >= 'a' && c <= 'z') || (c >= '0' && c <= '9') || c == '.' || c == '_' || c == '-' {
			continue
		}
		return fmt.Errorf("firecracker: invalid cgroup control %q", value)
	}
	return nil
}

func validateRelativePath(path string) error {
	clean := filepath.Clean(path)
	if clean == "." || filepath.IsAbs(clean) || clean == ".." || strings.HasPrefix(clean, ".."+string(filepath.Separator)) {
		return fmt.Errorf("%q must be a non-traversing relative path", path)
	}
	return nil
}

func trustedExecutable(path string) error {
	st, err := os.Lstat(path)
	if err != nil {
		return err
	}
	if !st.Mode().IsRegular() || st.Mode()&0o111 == 0 {
		return fmt.Errorf("%q is not an executable regular file", path)
	}
	return trustedDirectory(filepath.Dir(path))
}

func trustedDirectory(path string) error {
	path = filepath.Clean(path)
	for {
		st, err := os.Lstat(path)
		if err != nil {
			return err
		}
		if !st.IsDir() || st.Mode()&os.ModeSymlink != 0 {
			return fmt.Errorf("%q is not a real directory", path)
		}
		if st.Mode().Perm()&0o022 != 0 {
			return fmt.Errorf("%q is writable by group or other", path)
		}
		parent := filepath.Dir(path)
		if parent == path {
			return nil
		}
		path = parent
	}
}

func boundedCommand(ctx context.Context, binary string, args ...string) (string, error) {
	cmd := exec.CommandContext(ctx, binary, args...)
	var output cappedBuffer
	cmd.Stdout, cmd.Stderr = &output, &output
	err := cmd.Run()
	if output.overflow {
		return "", errors.New("command output exceeded 64 KiB")
	}
	if err != nil {
		return "", fmt.Errorf("%w: %s", err, strings.TrimSpace(output.String()))
	}
	return output.String(), nil
}

type cappedBuffer struct {
	data     []byte
	overflow bool
}

func (b *cappedBuffer) Write(p []byte) (int, error) {
	const limit = 64 << 10
	written := len(p)
	remaining := limit - len(b.data)
	if remaining > 0 {
		if remaining > len(p) {
			remaining = len(p)
		}
		b.data = append(b.data, p[:remaining]...)
	}
	if remaining < len(p) {
		b.overflow = true
	}
	return written, nil
}

func (b *cappedBuffer) String() string { return string(b.data) }

func jailID(workspace string, serial uint64, now int64) string {
	sum := sha256.Sum256([]byte(fmt.Sprintf("%s\x00%d\x00%d", workspace, serial, now)))
	return fmt.Sprintf("rm-%x", sum[:16])
}

func guestMAC(workspace string) string {
	sum := sha256.Sum256([]byte(workspace))
	return fmt.Sprintf("06:%02x:%02x:%02x:%02x:%02x", sum[0], sum[1], sum[2], sum[3], sum[4])
}

func removeOwnedRegular(path, root string) error {
	cleanRoot := filepath.Clean(root) + string(filepath.Separator)
	cleanPath := filepath.Clean(path)
	if !strings.HasPrefix(cleanPath, cleanRoot) {
		return fmt.Errorf("firecracker: cleanup path %q escapes %q", path, root)
	}
	st, err := os.Lstat(cleanPath)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	if !st.Mode().IsRegular() {
		return fmt.Errorf("firecracker: refusing to replace non-regular %q", cleanPath)
	}
	return os.Remove(cleanPath)
}

func removeJailRoot(root, base string) error {
	root = filepath.Clean(root)
	base = filepath.Clean(base) + string(filepath.Separator)
	if !strings.HasPrefix(root, base) || filepath.Base(filepath.Dir(root)) == "" || filepath.Base(root) != "root" {
		return fmt.Errorf("firecracker: refusing unsafe jail cleanup %q", root)
	}
	return os.RemoveAll(filepath.Dir(root))
}

func expectedKill(err error) bool {
	var exitErr *exec.ExitError
	return errors.As(err, &exitErr)
}
