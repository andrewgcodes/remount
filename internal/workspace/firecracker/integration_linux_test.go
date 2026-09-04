//go:build linux

package firecracker

import (
	"bytes"
	"context"
	"errors"
	"net"
	"net/http"
	"net/netip"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"remount.dev/remount/internal/netns"
	"remount.dev/remount/internal/proto"
	"remount.dev/remount/internal/session"
	"remount.dev/remount/internal/workspace"
)

func TestB29FirecrackerHostSmoke(t *testing.T) {
	if os.Getenv("REMOUNT_FIRECRACKER_INTEGRATION") != "1" {
		t.Skip("set REMOUNT_FIRECRACKER_INTEGRATION=1 on a Linux KVM host")
	}
	if os.Geteuid() != 0 {
		t.Fatal("Firecracker integration requires root for KVM, jailer, cgroups, TAP and netns")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	backend := newFirecrackerIntegrationBackend(t, ctx, "s")
	h, err := backend.Create(ctx, "ws_b29_smoke", proto.WorkspaceSpec{MountPath: "/workspace"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer cleanupCancel()
		if err := h.Destroy(cleanupCtx); err != nil {
			t.Errorf("destroy Firecracker workspace: %v", err)
		}
	})

	controller := h.(workspace.NetworkController)
	brokerHost := h.(workspace.BrokerAdvertiser).BrokerAdvertiseHost()
	listener, err := net.Listen("tcp4", net.JoinHostPort(brokerHost, "0"))
	if err != nil {
		t.Fatal(err)
	}
	var streamed atomic.Int64
	server := &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/stream" {
			flusher, ok := w.(http.Flusher)
			if !ok {
				http.Error(w, "streaming unavailable", http.StatusInternalServerError)
				return
			}
			chunk := bytes.Repeat([]byte("x"), 4096)
			for {
				if _, err := w.Write(chunk); err != nil {
					return
				}
				streamed.Add(int64(len(chunk)))
				flusher.Flush()
				select {
				case <-r.Context().Done():
					return
				case <-time.After(10 * time.Millisecond):
				}
			}
		}
		_, _ = w.Write([]byte("broker-ok"))
	})}
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(func() {
		_ = server.Close()
		_ = listener.Close()
	})
	broker := listener.Addr().(*net.TCPAddr)
	brokerAddr := netip.AddrPortFrom(netip.MustParseAddr(broker.IP.String()), uint16(broker.Port))
	endpoint := workspace.NetworkEndpoint{
		Workspace:       h.ID(),
		Generation:      1,
		ReverseProxyURL: "http://" + brokerAddr.String(),
		ForwardProxyURL: "http://" + brokerAddr.String(),
	}
	if err := controller.ApplyNetworkPolicy(ctx, proto.NetworkPolicy{}, endpoint); err != nil {
		t.Fatal(err)
	}
	waitForGuest(t, ctx, h)

	if err := h.FS().Write("proof/file.txt", []byte("firecracker-file-ok"), 0o644, false, true); err != nil {
		t.Fatal(err)
	}
	read, err := h.FS().Read("proof/file.txt", 0, 1024)
	if err != nil || string(read.Data) != "firecracker-file-ok" {
		t.Fatalf("file round trip = %q, %v", read.Data, err)
	}

	manager := session.NewManager(session.ManagerOptions{MaxSessions: 16, MaxActive: 16, MaxSessionsPerWorkspace: 16, MaxSessionsPerPrincipal: 16})
	t.Cleanup(manager.Close)
	out, exit := runGuestSession(t, ctx, manager, h, session.Spec{
		WS: h.ID(), Kind: proto.SessionExec, Program: []string{"sh", "-c", "printf exec-ok"},
	})
	if string(out) != "exec-ok" || exit.Code != 0 || exit.Error != "" {
		t.Fatalf("exec output = %q, exit = %+v", out, exit)
	}

	pty := openGuestSession(t, manager, h, session.Spec{
		WS: h.ID(), Kind: proto.SessionPTY,
		Program: []string{"sh", "-c", "stty size; read line; stty size; printf 'pty:%s\\n' \"$line\""},
		Rows:    30, Cols: 100,
	})
	waitForSessionOutput(t, ctx, pty, "30 100")
	if err := pty.Resize(40, 120); err != nil {
		t.Fatal(err)
	}
	if err := pty.Input(1, []byte("hello\n"), false); err != nil {
		t.Fatal(err)
	}
	ptyOut, ptyExit := collectGuestSession(t, ctx, pty)
	if !bytes.Contains(ptyOut, []byte("40 120")) || !bytes.Contains(ptyOut, []byte("pty:hello")) || ptyExit.Code != 0 {
		t.Fatalf("PTY output = %q, exit = %+v", ptyOut, ptyExit)
	}

	portServer := openGuestSession(t, manager, h, session.Spec{
		WS: h.ID(), Kind: proto.SessionExec,
		Program: []string{"socat", "TCP4-LISTEN:18080,bind=127.0.0.1,reuseaddr,fork", "EXEC:/bin/cat"},
	})
	t.Cleanup(func() { _ = portServer.Signal("KILL") })
	port := waitForGuestPort(t, ctx, manager, h, 18080)
	if err := port.Input(1, []byte("port-ok"), true); err != nil {
		t.Fatal(err)
	}
	portOut, portExit := collectGuestSession(t, ctx, port)
	if string(portOut) != "port-ok" || portExit.Code != 0 {
		t.Fatalf("port output = %q, exit = %+v", portOut, portExit)
	}

	brokerOut, brokerExit := runGuestSession(t, ctx, manager, h, session.Spec{
		WS: h.ID(), Kind: proto.SessionExec,
		Program: []string{"curl", "-fsS", "--max-time", "3", "http://" + brokerAddr.String() + "/proof"},
	})
	if string(brokerOut) != "broker-ok" || brokerExit.Code != 0 {
		t.Fatalf("broker output = %q, exit = %+v", brokerOut, brokerExit)
	}
	_, deniedExit := runGuestSession(t, ctx, manager, h, session.Spec{
		WS: h.ID(), Kind: proto.SessionExec,
		Program: []string{"curl", "-fsS", "--max-time", "2", "http://1.1.1.1/"},
	})
	if deniedExit.Code == 0 {
		t.Fatal("direct Firecracker guest egress unexpectedly succeeded")
	}

	stream := openGuestSession(t, manager, h, session.Spec{
		WS: h.ID(), Kind: proto.SessionExec,
		Program: []string{"curl", "-fsS", "http://" + brokerAddr.String() + "/stream", "-o", "/workspace/proof/stream"},
	})
	waitForGuestFileSize(t, ctx, h, "proof/stream", 4096)
	if err := controller.RevokeNetwork(ctx); err != nil {
		t.Fatal(err)
	}
	_, streamExit := collectGuestSession(t, ctx, stream)
	if streamExit.Code == 0 {
		t.Fatal("in-flight broker transfer survived network revoke")
	}
	sizeAfterRevoke := streamed.Load()
	time.Sleep(200 * time.Millisecond)
	if size := streamed.Load(); size != sizeAfterRevoke {
		t.Fatalf("stream grew after revoke returned: %d -> %d", sizeAfterRevoke, size)
	}
}

func TestB29FirecrackerCheckpointMoveRestore(t *testing.T) {
	if os.Getenv("REMOUNT_FIRECRACKER_INTEGRATION") != "1" {
		t.Skip("set REMOUNT_FIRECRACKER_INTEGRATION=1 on a Linux KVM host")
	}
	if os.Geteuid() != 0 {
		t.Fatal("Firecracker integration requires root for KVM, jailer, cgroups, TAP and netns")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
	defer cancel()

	backend := newFirecrackerIntegrationBackend(t, ctx, "m")
	source, err := backend.Create(ctx, "ws_b29_move", proto.WorkspaceSpec{MountPath: "/workspace"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	destroy := func(h workspace.Handle) {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer cleanupCancel()
		if err := h.Destroy(cleanupCtx); err != nil {
			t.Errorf("destroy Firecracker workspace: %v", err)
		}
	}
	t.Cleanup(func() { destroy(source) })

	brokerHost := source.(workspace.BrokerAdvertiser).BrokerAdvertiseHost()
	listener, err := net.Listen("tcp4", net.JoinHostPort(brokerHost, "0"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = listener.Close() })
	broker := listener.Addr().(*net.TCPAddr)
	brokerAddr := netip.AddrPortFrom(netip.MustParseAddr(broker.IP.String()), uint16(broker.Port))
	apply := func(h workspace.Handle, generation uint64) {
		t.Helper()
		endpoint := workspace.NetworkEndpoint{
			Workspace: h.ID(), Generation: generation,
			ReverseProxyURL: "http://" + brokerAddr.String(), ForwardProxyURL: "http://" + brokerAddr.String(),
		}
		if err := h.(workspace.NetworkController).ApplyNetworkPolicy(ctx, proto.NetworkPolicy{}, endpoint); err != nil {
			t.Fatal(err)
		}
		waitForGuest(t, ctx, h)
	}
	apply(source, 1)

	manager := session.NewManager(session.ManagerOptions{MaxSessions: 16, MaxActive: 16, MaxSessionsPerWorkspace: 16, MaxSessionsPerPrincipal: 16})
	t.Cleanup(manager.Close)
	pidOut, exit := runGuestSession(t, ctx, manager, source, session.Spec{
		WS: source.ID(), Kind: proto.SessionExec,
		Program: []string{"sh", "-c", "mkdir -p /workspace/proof; sh -c 'i=0; while :; do i=$((i+1)); echo \"$i\" >> /workspace/proof/counter; sleep 0.1; done' </dev/null >/dev/null 2>&1 & echo $! > /workspace/proof/pid; cat /workspace/proof/pid"},
	})
	if exit.Code != 0 {
		t.Fatalf("start continuation process: %+v", exit)
	}
	pid := strings.TrimSpace(string(pidOut))
	waitForCounter(t, ctx, source, 3)

	abortBundle, err := os.CreateTemp(os.Getenv("REMOUNT_FIRECRACKER_DATA_ROOT"), "b29-abort-*.tgz")
	if err != nil {
		t.Fatal(err)
	}
	abortPath := abortBundle.Name()
	t.Cleanup(func() { _ = os.Remove(abortPath) })
	fenced := source.(workspace.FencedCheckpointer)
	beforeAbort := readCounter(t, source)
	if err := fenced.CheckpointFenced(ctx, nil, abortBundle); err != nil {
		t.Fatal(err)
	}
	if err := abortBundle.Close(); err != nil {
		t.Fatal(err)
	}
	if err := fenced.ResumeFenced(ctx); err != nil {
		t.Fatal(err)
	}
	waitForCounter(t, ctx, source, len(beforeAbort)+2)

	commitBundle, err := os.CreateTemp(os.Getenv("REMOUNT_FIRECRACKER_DATA_ROOT"), "b29-commit-*.tgz")
	if err != nil {
		t.Fatal(err)
	}
	commitPath := commitBundle.Name()
	t.Cleanup(func() { _ = os.Remove(commitPath) })
	if err := fenced.CheckpointFenced(ctx, nil, commitBundle); err != nil {
		t.Fatal(err)
	}
	if err := commitBundle.Close(); err != nil {
		t.Fatal(err)
	}
	if err := source.Destroy(ctx); err != nil {
		t.Fatal(err)
	}

	restore, err := os.Open(commitPath)
	if err != nil {
		t.Fatal(err)
	}
	destination, err := backend.Create(ctx, "ws_b29_move", proto.WorkspaceSpec{MountPath: "/workspace"}, restore)
	closeErr := restore.Close()
	if err != nil || closeErr != nil {
		t.Fatal(errors.Join(err, closeErr))
	}
	t.Cleanup(func() { destroy(destination) })
	checkpointer, ok := destination.(workspace.Checkpointer)
	if !ok || workspace.KindOf(checkpointer) != workspace.CheckpointFSMem {
		t.Fatal("restored destination did not earn process-preserving readiness")
	}
	staleEndpoint := workspace.NetworkEndpoint{
		Workspace: destination.ID(), Generation: 1,
		ReverseProxyURL: "http://" + brokerAddr.String(), ForwardProxyURL: "http://" + brokerAddr.String(),
	}
	if err := destination.(workspace.NetworkController).ApplyNetworkPolicy(ctx, proto.NetworkPolicy{}, staleEndpoint); !errors.Is(err, proto.Err(proto.CodeDenied, "")) {
		t.Fatalf("stale restore generation error = %v", err)
	}
	apply(destination, 2)

	restoredPID, restoredExit := runGuestSession(t, ctx, manager, destination, session.Spec{
		WS: destination.ID(), Kind: proto.SessionExec, Program: []string{"cat", "/workspace/proof/pid"},
	})
	if restoredExit.Code != 0 || strings.TrimSpace(string(restoredPID)) != pid {
		t.Fatalf("restored PID = %q, want %q, exit = %+v", strings.TrimSpace(string(restoredPID)), pid, restoredExit)
	}
	before := len(readCounter(t, destination))
	waitForCounter(t, ctx, destination, before+3)
	counter := readCounter(t, destination)
	for i, value := range counter {
		if value != i+1 {
			t.Fatalf("continuation sequence[%d] = %d, want %d", i, value, i+1)
		}
	}
}

func readCounter(t *testing.T, h workspace.Handle) []int {
	t.Helper()
	read, err := h.FS().Read("proof/counter", 0, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	lines := strings.Fields(string(read.Data))
	values := make([]int, len(lines))
	for i, line := range lines {
		values[i], err = strconv.Atoi(line)
		if err != nil {
			t.Fatalf("counter line %q: %v", line, err)
		}
	}
	return values
}

func waitForCounter(t *testing.T, ctx context.Context, h workspace.Handle, count int) {
	t.Helper()
	for ctx.Err() == nil {
		read, err := h.FS().Read("proof/counter", 0, 1<<20)
		if err == nil && len(strings.Fields(string(read.Data))) >= count {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("counter did not reach %d", count)
}

func waitForGuestFileSize(t *testing.T, ctx context.Context, h workspace.Handle, path string, minimum int64) {
	t.Helper()
	for {
		entry, err := h.FS().Stat(path)
		if err == nil && entry.Size >= minimum {
			return
		}
		select {
		case <-ctx.Done():
			t.Fatalf("wait for guest file %s: %v", path, ctx.Err())
		case <-time.After(25 * time.Millisecond):
		}
	}
}

func newFirecrackerIntegrationBackend(t *testing.T, ctx context.Context, suffix string) *Backend {
	t.Helper()
	required := func(name string) string {
		t.Helper()
		value := os.Getenv(name)
		if value == "" {
			t.Fatalf("%s is required", name)
		}
		return value
	}
	base := required("REMOUNT_FIRECRACKER_DATA_ROOT")
	root := filepath.Join(base, suffix)
	if err := os.Mkdir(root, 0o700); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := os.RemoveAll(root); err != nil {
			t.Errorf("remove Firecracker integration root: %v", err)
		}
	})
	const uid = 65534
	const gid = 65534
	factory, err := NewJailerFactory(JailerOptions{
		Firecracker:   required("REMOUNT_FIRECRACKER_BINARY"),
		Jailer:        required("REMOUNT_FIRECRACKER_JAILER"),
		KVM:           "/dev/kvm",
		ChrootBase:    filepath.Join(root, "j"),
		UID:           uid,
		GID:           gid,
		CgroupVersion: 2,
		CgroupParent:  required("REMOUNT_FIRECRACKER_CGROUP_PARENT"),
		Cgroups:       []string{"memory.max=536870912", "pids.max=256"},
		MaxMachines:   4,
		MaxVCPU:       2,
		MaxMemMiB:     512,
		StartTimeout:  60 * time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := factory.Close(); err != nil {
			t.Errorf("close Firecracker factory: %v", err)
		}
	})
	images, err := NewReflinkStore(ReflinkStoreOptions{
		Dir: filepath.Join(root, "i"), MaxImages: 4, MaxLogicalBytes: 8 << 30,
		OwnerUID: uid, OwnerGID: gid,
	})
	if err != nil {
		t.Fatal(err)
	}
	networks, err := NewSystemNetworkProvider(ctx, NetworkOptions{
		Dir: filepath.Join(root, "n"), Manager: netns.NewSystemManager(), UID: uid, GID: gid,
	})
	if err != nil {
		t.Fatal(err)
	}
	guest, err := NewGuestBridge(GuestOptions{Manifest: required("REMOUNT_FIRECRACKER_GUEST_MANIFEST"), Timeout: 10 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	volumes, err := NewCoWVolumeProvider(CoWVolumeOptions{
		Dir: filepath.Join(root, "p"), Images: images,
		Firecracker: required("REMOUNT_FIRECRACKER_BINARY"), MaxBundleBytes: 8 << 30,
	})
	if err != nil {
		t.Fatal(err)
	}
	backend, err := New(ctx, Options{
		Dir: root, KernelImage: required("REMOUNT_FIRECRACKER_KERNEL"),
		BaseRootFS: required("REMOUNT_FIRECRACKER_ROOTFS"),
		Machines:   factory, Networks: networks, Volumes: volumes, Guest: guest, MaxWorkspaces: 4,
	})
	if err != nil {
		t.Fatal(err)
	}
	descriptor, err := workspace.NewRegistry(backend).Descriptor("firecracker")
	if err != nil {
		t.Fatal(err)
	}
	if descriptor.Security.Isolation != "microvm" ||
		descriptor.Security.EgressMode != "enforced_gateway" ||
		descriptor.Runtime.Snapshots != string(workspace.CheckpointFSMem) {
		t.Fatalf("verified candidate descriptor = %+v", descriptor)
	}
	return backend
}

func waitForGuest(t *testing.T, ctx context.Context, h workspace.Handle) {
	t.Helper()
	var last error
	for ctx.Err() == nil {
		_, last = h.FS().Stat(".")
		if last == nil {
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("guest agent did not become ready: %v", last)
}

func openGuestSession(t *testing.T, manager *session.Manager, h workspace.Handle, spec session.Spec) *session.Session {
	t.Helper()
	if err := h.Prepare(&spec); err != nil {
		t.Fatal(err)
	}
	s, err := manager.Open(spec)
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func runGuestSession(t *testing.T, ctx context.Context, manager *session.Manager, h workspace.Handle, spec session.Spec) ([]byte, proto.ExitInfo) {
	t.Helper()
	return collectGuestSession(t, ctx, openGuestSession(t, manager, h, spec))
}

func collectGuestSession(t *testing.T, ctx context.Context, s *session.Session) ([]byte, proto.ExitInfo) {
	t.Helper()
	exit, err := s.Wait(ctx)
	if err != nil {
		t.Fatal(err)
	}
	chunks, err := s.Log.Read(0, 0)
	if err != nil {
		t.Fatal(err)
	}
	var out []byte
	for _, chunk := range chunks {
		if chunk.Stream == proto.StreamStdout {
			out = append(out, chunk.Data...)
		}
	}
	return out, *exit
}

func waitForSessionOutput(t *testing.T, ctx context.Context, s *session.Session, expected string) {
	t.Helper()
	for ctx.Err() == nil {
		chunks, err := s.Log.Read(0, 0)
		if err != nil {
			t.Fatal(err)
		}
		var out strings.Builder
		for _, chunk := range chunks {
			if chunk.Stream == proto.StreamStdout {
				out.Write(chunk.Data)
			}
		}
		if strings.Contains(out.String(), expected) {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("session output never contained %q", expected)
}

func waitForGuestPort(t *testing.T, ctx context.Context, manager *session.Manager, h workspace.Handle, port int) *session.Session {
	t.Helper()
	for ctx.Err() == nil {
		s := openGuestSession(t, manager, h, session.Spec{WS: h.ID(), Kind: proto.SessionPort, Port: port})
		probeCtx, cancel := context.WithTimeout(ctx, 100*time.Millisecond)
		_, err := s.Wait(probeCtx)
		cancel()
		if errors.Is(err, context.DeadlineExceeded) {
			return s
		}
	}
	t.Fatalf("guest port %d did not become ready", port)
	return nil
}
