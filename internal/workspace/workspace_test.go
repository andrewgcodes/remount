package workspace

import (
	"bytes"
	"context"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"remount.dev/remount/internal/proto"
	"remount.dev/remount/internal/session"
)

func TestBackendRootsAreAbsolute(t *testing.T) {
	wd, _ := os.Getwd()
	if err := os.Chdir(t.TempDir()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chdir(wd) })
	p, err := NewProcess("rel/ws")
	if err != nil {
		t.Fatal(err)
	}
	d, err := NewDocker("rel/docker", "")
	if err != nil {
		t.Fatal(err)
	}
	if !filepath.IsAbs(p.Dir) || !filepath.IsAbs(d.Dir) || d.Image != DefaultImage("dev") {
		t.Fatalf("process=%q docker=%q image=%q", p.Dir, d.Dir, d.Image)
	}
}

func TestProcessBackendLifecycle(t *testing.T) {
	ctx := context.Background()
	be, err := NewProcess(filepath.Join(t.TempDir(), "ws"))
	if err != nil {
		t.Fatal(err)
	}
	if be.Name() != "process" || be.Caps().Isolation != "none" {
		t.Fatal("caps")
	}
	h, err := be.Create(ctx, "ws_1", proto.WorkspaceSpec{}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := be.Create(ctx, "ws_1", proto.WorkspaceSpec{}, nil); err == nil {
		t.Fatal("duplicate create allowed")
	}
	if err := h.FS().Write("hello.txt", []byte("hi"), 0, false, false); err != nil {
		t.Fatal(err)
	}
	if err := h.FS().Mkdir("sub"); err != nil {
		t.Fatal(err)
	}

	// Prepare resolves cwd inside the root and merges env.
	spec := session.Spec{Kind: proto.SessionExec, Program: []string{"sh", "-c", "pwd; echo $HOME; echo $FOO"}, Cwd: "sub", Env: []string{"FOO=bar"}}
	if err := h.Prepare(&spec); err != nil {
		t.Fatal(err)
	}
	if !strings.HasSuffix(spec.Cwd, "/ws_1/sub") {
		t.Fatal(spec.Cwd)
	}
	m := session.NewManager(session.ManagerOptions{})
	defer m.Close()
	s, _ := m.Open(spec)
	out := drain(t, s)
	lines := strings.Split(strings.TrimSpace(out), "\n")
	if len(lines) != 3 || !strings.HasSuffix(lines[0], "/ws_1/sub") || !strings.HasSuffix(lines[1], "/ws_1") || lines[2] != "bar" {
		t.Fatalf("%q", out)
	}
	bad := session.Spec{Cwd: "../../etc"}
	if err := h.Prepare(&bad); err == nil {
		t.Fatal("escape not caught")
	}
	missing := session.Spec{Cwd: "nope"}
	if err := h.Prepare(&missing); err == nil {
		t.Fatal("missing cwd not caught")
	}

	// Snapshot -> restore into a new workspace, possibly on another backend instance.
	var buf bytes.Buffer
	if err := h.Snapshot(ctx, []string{"sub"}, &buf); err != nil {
		t.Fatal(err)
	}
	be2, _ := NewProcess(filepath.Join(t.TempDir(), "ws2"))
	h2, err := be2.Create(ctx, "ws_1", proto.WorkspaceSpec{}, bytes.NewReader(buf.Bytes()))
	if err != nil {
		t.Fatal(err)
	}
	r, err := h2.FS().Read("hello.txt", 0, 0)
	if err != nil || string(r.Data) != "hi" {
		t.Fatal(err)
	}
	if _, err := h2.FS().Stat("sub"); err == nil {
		t.Fatal("excluded dir restored")
	}

	// Adopt after "restart".
	h3, err := be2.Adopt(ctx, "ws_1")
	if err != nil || h3.ID() != "ws_1" {
		t.Fatal(err)
	}
	if _, err := be2.Adopt(ctx, "ws_missing"); err == nil {
		t.Fatal("adopted nothing")
	}
	if err := h2.Destroy(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(be2.Dir, "ws_1")); !os.IsNotExist(err) {
		t.Fatal("not destroyed")
	}
}

func TestMergeEnvAndRegistry(t *testing.T) {
	env := MergeEnv([]string{"A=1", "B=2", "junk"}, []string{"B=3", "C=4"})
	if strings.Join(env, ",") != "A=1,B=3,C=4" {
		t.Fatal(env)
	}
	if got := MapEnv(map[string]string{"z": "1", "a": "2"}); got[0] != "a=2" {
		t.Fatal(got)
	}
	p, _ := NewProcess(t.TempDir())
	r := NewRegistry(p)
	if b, err := r.Get(""); err != nil || b.Name() != "process" {
		t.Fatal(err)
	}
	if _, err := r.Get("firecracker"); err == nil {
		t.Fatal("unknown backend returned")
	}
	info := HostInfo(r.Names())
	if info.CPU == 0 || info.OS == "" || len(info.Backends) != 1 {
		t.Fatalf("%+v", info)
	}
	empty := NewRegistry()
	if _, err := empty.Get(""); err == nil {
		t.Fatal("empty registry")
	}
}

func TestDockerBackendUnavailableIsClean(t *testing.T) {
	d, _ := NewDocker(t.TempDir(), "")
	d.Binary = "definitely-not-docker-" + t.Name()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, err := d.Create(ctx, "ws_x", proto.WorkspaceSpec{}, nil); err == nil {
		t.Fatal("expected unsupported")
	}
	if d.Caps().Isolation != "container" {
		t.Fatal("caps")
	}
}

func TestDockerBackendFailedRunRemovesPartialContainer(t *testing.T) {
	dir := t.TempDir()
	binary := filepath.Join(dir, "docker")
	marker := filepath.Join(dir, "container")
	commands := filepath.Join(dir, "commands")
	root := filepath.Join(dir, "data", "ws_fail")
	script := `#!/bin/sh
case "$1" in
	info) echo 27.4.1 ;;
	run) touch "$REMOUNT_DOCKER_MARKER"; exit 1 ;;
	inspect) echo "$REMOUNT_DOCKER_ROOT" ;;
	rm) printf '%s\n' "$*" >> "$REMOUNT_DOCKER_COMMANDS"; rm -f "$REMOUNT_DOCKER_MARKER" ;;
esac
`
	if err := os.WriteFile(binary, []byte(script), 0o755); err != nil {
		t.Fatal(err)
	}
	t.Setenv("REMOUNT_DOCKER_MARKER", marker)
	t.Setenv("REMOUNT_DOCKER_COMMANDS", commands)
	t.Setenv("REMOUNT_DOCKER_ROOT", root)

	d, err := NewDocker(filepath.Join(dir, "data"), "alpine:3.20")
	if err != nil {
		t.Fatal(err)
	}
	d.Binary = binary
	if _, err := d.Create(context.Background(), "ws_fail", proto.WorkspaceSpec{}, nil); err == nil {
		t.Fatal("failed docker run succeeded")
	}
	if _, err := os.Stat(marker); !os.IsNotExist(err) {
		t.Fatalf("partial container marker remains: %v", err)
	}
	body, err := os.ReadFile(commands)
	if err != nil {
		t.Fatal(err)
	}
	if got := strings.TrimSpace(string(body)); got != "rm -f remount-ws-fail" {
		t.Fatalf("cleanup command = %q", got)
	}
	if _, err := os.Stat(root); !os.IsNotExist(err) {
		t.Fatalf("failed workspace root remains: %v", err)
	}
}

func TestDockerBackendFailedRunRetainsRootWhenContainerCleanupIsUncertain(t *testing.T) {
	dir := t.TempDir()
	binary := filepath.Join(dir, "docker")
	marker := filepath.Join(dir, "container")
	root := filepath.Join(dir, "data", "ws_fail")
	script := `#!/bin/sh
case "$1" in
	info) echo 27.4.1 ;;
	run) touch "$REMOUNT_DOCKER_MARKER"; exit 1 ;;
	inspect) echo "daemon unavailable" >&2; exit 1 ;;
esac
`
	if err := os.WriteFile(binary, []byte(script), 0o755); err != nil {
		t.Fatal(err)
	}
	t.Setenv("REMOUNT_DOCKER_MARKER", marker)

	d, err := NewDocker(filepath.Join(dir, "data"), "alpine:3.20")
	if err != nil {
		t.Fatal(err)
	}
	d.Binary = binary
	if _, err := d.Create(context.Background(), "ws_fail", proto.WorkspaceSpec{}, nil); err == nil || !strings.Contains(err.Error(), "cleanup: inspect") {
		t.Fatalf("failed docker run error = %v", err)
	}
	if _, err := os.Stat(marker); err != nil {
		t.Fatalf("partial container marker was lost: %v", err)
	}
	if _, err := os.Stat(root); err != nil {
		t.Fatalf("workspace root was removed before container cleanup was proven: %v", err)
	}
}

// TestDockerBackendReal runs only when a docker daemon is reachable.
func TestDockerBackendReal(t *testing.T) {
	d, _ := NewDocker(filepath.Join(t.TempDir(), "d"), "alpine:3.20")
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	if err := d.Available(ctx); err != nil {
		t.Skipf("docker not available: %v", err)
	}
	h, err := d.Create(ctx, "ws_dk", proto.WorkspaceSpec{}, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer h.Destroy(ctx)
	h.FS().Write("f.txt", []byte("from host"), 0, false, false)
	spec := session.Spec{Kind: proto.SessionExec, Program: []string{"sh", "-c", "cat f.txt; echo $REMOUNT_WORKSPACE; pwd; mkdir -p .config/opencode; chmod 700 .config/opencode"}}
	if err := h.Prepare(&spec); err != nil {
		t.Fatal(err)
	}
	m := session.NewManager(session.ManagerOptions{})
	defer m.Close()
	s, _ := m.Open(spec)
	out := drain(t, s)
	if !strings.Contains(out, "from host") || !strings.Contains(out, "ws_dk") || !strings.Contains(out, "/work") {
		t.Fatalf("%q", out)
	}
	if err := h.(FilesystemAccessPreparer).PrepareFilesystemAccess(ctx); err != nil {
		t.Fatal(err)
	}
	if err := h.FS().Write(".config/opencode/config.json", []byte("{}"), 0, false, false); err != nil {
		t.Fatal(err)
	}
	if _, err := d.Adopt(ctx, "ws_dk"); err != nil {
		t.Fatal(err)
	}
}

func drain(t *testing.T, s *session.Session) string {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	var out []byte
	c := s.Log.CursorAt(0)
	for {
		chunks, err := c.Next(ctx, 0)
		if err == io.EOF {
			return string(out)
		}
		if err != nil {
			t.Fatal(err)
		}
		for _, ch := range chunks {
			if ch.Stream == proto.StreamStdout || ch.Stream == proto.StreamStderr {
				out = append(out, ch.Data...)
			}
		}
	}
}

func TestDefaultImageTracksRelease(t *testing.T) {
	if got := DefaultImage("v1.4.0"); got != DefaultImageRepository+":v1.4.0" {
		t.Fatalf("release binary must pin its own image tag: %q", got)
	}
	for _, v := range []string{"dev", "", "v1.4.0-dirty", "abc123"} {
		if got := DefaultImage(v); got != DefaultImageRepository+":latest" {
			t.Fatalf("DefaultImage(%q) = %q", v, got)
		}
	}
}

type fsOnlyCheckpointer struct{ Checkpointer }

type memClaimer struct {
	Checkpointer
	kind CheckpointKind
}

func (m memClaimer) CheckpointKind() CheckpointKind { return m.kind }

func TestKindOfTrustsOnlyKnownClaims(t *testing.T) {
	if got := KindOf(fsOnlyCheckpointer{}); got != CheckpointFS {
		t.Fatalf("plain handle = %q", got)
	}
	if got := KindOf(memClaimer{kind: CheckpointFSMem}); got != CheckpointFSMem {
		t.Fatalf("memory claim = %q", got)
	}
	if got := KindOf(memClaimer{kind: "gpu+mem"}); got != CheckpointFS {
		t.Fatalf("unknown claim must degrade to fs, got %q", got)
	}
	p, _ := NewProcess(t.TempDir())
	h, err := p.Create(context.Background(), "ws_kind", proto.WorkspaceSpec{}, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer h.Destroy(context.Background())
	if got := KindOf(h); string(got) != p.Caps().Snapshots {
		t.Fatalf("process handle kind %q disagrees with advertised Caps.Snapshots %q", got, p.Caps().Snapshots)
	}
}

func TestProcessBackendRefusesForeignMountPathAndDockerParsesMounts(t *testing.T) {
	p := &Process{Dir: t.TempDir()}
	if p.Caps().MountPath {
		t.Fatal("process backend must not claim a mount namespace")
	}
	if _, err := p.Create(context.Background(), "ws_mp", proto.WorkspaceSpec{MountPath: "/home/me/proj"}, nil); err == nil {
		t.Fatal("process backend accepted a mount path it would have to symlink")
	}
	if _, err := os.Stat(filepath.Join(p.Dir, "ws_mp")); err == nil {
		t.Fatal("refused workspace left a directory behind")
	}
	h, err := p.Create(context.Background(), "ws_default", proto.WorkspaceSpec{MountPath: proto.DefaultMountPath}, nil)
	if err != nil {
		t.Fatal(err)
	}
	_ = h.Destroy(context.Background())

	if !(&Docker{}).Caps().MountPath {
		t.Fatal("docker backend owns a mount namespace and must say so")
	}
	root := "/data/ws/ws_1"
	for _, tc := range []struct{ in, want string }{
		{"/data/ws/ws_1\t/home/me/proj\n", "/home/me/proj"},
		{"/other\t/x\n/data/ws/ws_1\t/home/me/proj\n", "/home/me/proj"},
		{"/resolved/elsewhere\t/lone\n", "/lone"},
		{"/a\t/x\n/b\t/y\n", proto.DefaultMountPath},
		{"", proto.DefaultMountPath},
	} {
		if got := mountDestination(tc.in, root); got != tc.want {
			t.Fatalf("mountDestination(%q) = %q, want %q", tc.in, got, tc.want)
		}
	}
}
