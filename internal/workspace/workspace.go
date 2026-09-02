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

	"remount.dev/remount/internal/artifact"
	"remount.dev/remount/internal/fsops"
	"remount.dev/remount/internal/proto"
	"remount.dev/remount/internal/session"
)

// Caps describes what a backend can do; advertised honestly to the control
// plane so policy can refuse to place workloads that need more.
type Caps struct {
	Isolation      string // none | container | microvm
	Snapshots      string // fs | fs+mem
	EgressEnforced bool   // can the backend force all egress through the broker?
	Display        bool
}

// Backend creates workspaces.
type Backend interface {
	Name() string
	Caps() Caps
	// Create materializes a workspace. If restore is non-nil it is a tar.gz
	// snapshot to extract into the fresh filesystem.
	Create(ctx context.Context, id string, spec proto.WorkspaceSpec, restore io.Reader) (Handle, error)
	// Adopt reattaches to a workspace that already exists on disk (node
	// restart). Returns proto.CodeNotFound if there is nothing to adopt.
	Adopt(ctx context.Context, id string) (Handle, error)
}

// Handle is one live workspace.
type Handle interface {
	ID() string
	Backend() string
	// FS returns the jailed filesystem view.
	FS() *fsops.FS
	// Prepare rewrites a session spec so it runs inside the workspace:
	// cwd is resolved, env is merged with the workspace baseline, and for
	// container backends the program is wrapped in an exec.
	Prepare(spec *session.Spec) error
	// Snapshot streams a tar.gz of the filesystem.
	Snapshot(ctx context.Context, excludes []string, w io.Writer) error
	// Destroy removes everything.
	Destroy(ctx context.Context) error
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
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return nil, err
	}
	return &Process{Dir: dir}, nil
}

func (p *Process) Name() string { return "process" }

func (p *Process) Caps() Caps {
	return Caps{Isolation: "none", Snapshots: "fs", EgressEnforced: false}
}

func (p *Process) root(id string) string { return filepath.Join(p.Dir, id) }

func (p *Process) Create(ctx context.Context, id string, spec proto.WorkspaceSpec, restore io.Reader) (Handle, error) {
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
func (h *processHandle) FS() *fsops.FS   { return h.fs }

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

func (h *processHandle) Destroy(ctx context.Context) error {
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

// NewDocker creates the backend; the daemon is checked lazily on first use.
func NewDocker(dir, defaultImage string) (*Docker, error) {
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return nil, err
	}
	if defaultImage == "" {
		defaultImage = "ubuntu:24.04"
	}
	return &Docker{Dir: dir, Image: defaultImage, Binary: "docker"}, nil
}

func (d *Docker) Name() string { return "docker" }

func (d *Docker) Caps() Caps {
	return Caps{Isolation: "container", Snapshots: "fs", EgressEnforced: false}
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
	return nil
}

func (d *Docker) container(id string) string { return "remount-" + strings.ReplaceAll(id, "_", "-") }

func (d *Docker) Create(ctx context.Context, id string, spec proto.WorkspaceSpec, restore io.Reader) (Handle, error) {
	if err := d.Available(ctx); err != nil {
		return nil, proto.Err(proto.CodeUnsupported, "%v", err)
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
		"-v", root + ":/work", "-w", "/work",
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
		_ = os.RemoveAll(root)
		return nil, proto.Err(proto.CodeInternal, "docker run: %s", strings.TrimSpace(string(out)))
	}
	return d.handle(id, root, name)
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
	return d.handle(id, root, name)
}

func (d *Docker) handle(id, root, name string) (Handle, error) {
	f, err := fsops.New(root)
	if err != nil {
		return nil, err
	}
	return &dockerHandle{id: id, root: f.Root(), fs: f, name: name, bin: d.Binary}, nil
}

type dockerHandle struct {
	id, root, name, bin string
	fs                  *fsops.FS
}

func (h *dockerHandle) ID() string      { return h.id }
func (h *dockerHandle) Backend() string { return "docker" }
func (h *dockerHandle) FS() *fsops.FS   { return h.fs }

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
	cwd := "/work"
	if spec.Cwd != "" {
		c, err := h.fs.Resolve(spec.Cwd)
		if err != nil {
			return err
		}
		rel, _ := filepath.Rel(h.root, c)
		cwd = filepath.ToSlash(filepath.Join("/work", rel))
	}
	args := []string{"exec", "-i", "-w", cwd}
	if spec.Kind == proto.SessionPTY {
		args = append(args, "-t")
	}
	env := MergeEnv([]string{"HOME=/work", "USER=root", "LANG=C.UTF-8", "REMOUNT_WORKSPACE=" + h.id}, spec.Env)
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

func (h *dockerHandle) Snapshot(ctx context.Context, excludes []string, w io.Writer) error {
	return artifact.Snapshot(h.root, excludes, w)
}

func (h *dockerHandle) Destroy(ctx context.Context) error {
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
