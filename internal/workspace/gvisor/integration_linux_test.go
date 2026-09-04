//go:build linux

package gvisor

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/netip"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"

	"golang.org/x/sys/unix"

	"remount.dev/remount/internal/netns"
	"remount.dev/remount/internal/proto"
	"remount.dev/remount/internal/session"
	"remount.dev/remount/internal/workspace"
)

func TestE4DenialConformance(t *testing.T) {
	if os.Getenv("REMOUNT_GVISOR_INTEGRATION") != "1" {
		t.Skip("unavailable: set REMOUNT_GVISOR_INTEGRATION=1 on a privileged Linux host with runsc")
	}
	rootfs := os.Getenv("REMOUNT_GVISOR_ROOTFS")
	if rootfs == "" {
		t.Skip("unavailable: REMOUNT_GVISOR_ROOTFS is not set")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	dir := t.TempDir()
	b, err := New(ctx, Options{Dir: dir, RootFS: rootfs})
	if err != nil {
		t.Fatalf("gvisor backend unavailable in required lane: %v", err)
	}
	defer func() {
		err := unix.Unmount(filepath.Join(dir, ".runsc", "null-netns"), 0)
		if err != nil && !errors.Is(err, unix.EINVAL) && !errors.Is(err, unix.ENOENT) {
			t.Errorf("unmount runsc network namespace: %v", err)
		}
	}()
	raw, err := b.Create(ctx, "ws_e4", proto.WorkspaceSpec{}, nil)
	if err != nil {
		t.Fatal(err)
	}
	h := raw.(*handle)
	networkState := h.network.State()
	defer func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cleanupCancel()
		if err := h.Destroy(cleanupCtx); err != nil {
			t.Errorf("cleanup: %v", err)
		}
	}()

	listener, err := net.Listen("tcp4", net.JoinHostPort(h.BrokerAdvertiseHost(), "0"))
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			go func() {
				defer conn.Close()
				payload := make([]byte, 1024)
				ticker := time.NewTicker(25 * time.Millisecond)
				defer ticker.Stop()
				for range ticker.C {
					if _, err := conn.Write(payload); err != nil {
						return
					}
				}
			}()
		}
	}()
	endpoint := "http://" + listener.Addr().String()
	udpListener, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.ParseIP(h.BrokerAdvertiseHost())})
	if err != nil {
		t.Fatal(err)
	}
	defer udpListener.Close()
	go func() {
		buffer := make([]byte, 128)
		for {
			n, peer, err := udpListener.ReadFromUDP(buffer)
			if err != nil {
				return
			}
			_, _ = udpListener.WriteToUDP(buffer[:n], peer)
		}
	}()
	if err := h.ApplyNetworkPolicy(ctx, proto.NetworkPolicy{Default: proto.NetworkDefaultDeny}, workspace.NetworkEndpoint{
		Workspace: "ws_e4", Generation: 1, ReverseProxyURL: endpoint, ForwardProxyURL: endpoint,
	}); err != nil {
		t.Fatal(err)
	}

	run := func(command string) error {
		spec := session.Spec{Kind: proto.SessionExec, Program: []string{"/bin/sh", "-c", command}}
		if err := h.Prepare(&spec); err != nil {
			return err
		}
		out, err := exec.CommandContext(ctx, spec.Program[0], spec.Program[1:]...).CombinedOutput()
		if err != nil {
			return fmt.Errorf("%w: %s", err, strings.TrimSpace(string(out)))
		}
		return nil
	}
	host, port, _ := net.SplitHostPort(listener.Addr().String())
	transferCtx, stopTransfer := context.WithCancel(ctx)
	transfer := exec.CommandContext(transferCtx, h.runtime.binaryPath(), "--root="+h.runtime.stateRoot(), "exec", containerName(h.id),
		"/bin/sh", "-c", "nc "+host+" "+port+" > /work/inflight")
	if err := transfer.Start(); err != nil {
		t.Fatal(err)
	}
	defer func() {
		stopTransfer()
		_ = transfer.Wait()
	}()
	transferPath := filepath.Join(h.bundle, "work", "inflight")
	var transferred int64
	for deadline := time.Now().Add(5 * time.Second); time.Now().Before(deadline); {
		info, err := os.Stat(transferPath)
		if err == nil && info.Size() > 0 {
			transferred = info.Size()
			break
		}
		time.Sleep(25 * time.Millisecond)
	}
	if transferred == 0 {
		t.Fatal("broker transfer did not start")
	}
	udpHost, udpPort, _ := net.SplitHostPort(udpListener.LocalAddr().String())
	if out, err := exec.CommandContext(ctx, filepath.Join(rootfs, "udpprobe"), udpHost, udpPort).CombinedOutput(); err != nil {
		t.Fatalf("UDP probe positive control failed: %v: %s", err, strings.TrimSpace(string(out)))
	}
	denied := map[string]string{
		"direct IPv4 TCP":         "nc -z -w 2 1.1.1.1 443",
		"IPv6":                    "nc -z -w 2 2606:4700:4700::1111 443",
		"DNS":                     "nslookup example.com 8.8.8.8",
		"ICMP":                    "ping -c 1 -W 2 8.8.8.8",
		"raw socket":              "/rawprobe",
		"CONNECT unlisted target": "printf 'CONNECT unlisted.example:443 HTTP/1.1\\r\\n\\r\\n' | nc -w 2 203.0.113.1 443",
	}
	// Watch the host side of the veth for the duration of the denial checks.
	// Each check below can only observe whether a reply came back; this
	// observes whether anything went out, which is the property the policy
	// actually claims.
	brokerAddr, _ := netip.ParseAddr(h.BrokerAdvertiseHost())
	watcher := newEscapeWatcher(t, brokerAddr)
	_ = watcher.escaped() // discard anything from setup, including the broker reachability probe

	for name, command := range denied {
		t.Run(name, func(t *testing.T) {
			if err := run(command); err == nil {
				t.Fatalf("forbidden lane succeeded: %s", command)
			}
		})
	}

	// UDP is generated but not asserted on by exit status, and the reason is
	// worth stating because it is easy to mistake for a weakened test.
	//
	// A connectionless send has nothing to fail against: `nc -u -z` reports
	// success as soon as the local sendto is accepted. Inside a sandbox that
	// runs its own network stack, the sendto is accepted by that stack no
	// matter what the host does with the frame afterwards, so no in-sandbox
	// command can observe the denial. Requiring this command to fail would be
	// requiring the wrong thing, and for a long time it was the only check
	// reporting the truth precisely because it could not be satisfied by the
	// absence of a reply.
	//
	// The traffic is still produced, and the watcher below is what judges it.
	_ = run("nc -u -z -w 2 8.8.8.8 53")

	// The load-bearing assertion. A denial check that passes because no reply
	// arrived proves nothing about egress, so require that no packet reached
	// the host side of the veth for any destination other than the broker.
	if escaped := watcher.escaped(); len(escaped) > 0 {
		t.Errorf("packets reached %s for %v: the deny-first policy did not stop egress, it only stopped the replies. "+
			"Every check above that passed did so because nothing came back, which is not the same property, and "+
			"one-way egress is sufficient for exfiltration. See docs/engineering/gvisor-egress-finding-2026-09-04.md",
			watcher.iface, escaped)
	}
	if err := h.RevokeNetwork(ctx); err != nil {
		t.Fatal(err)
	}
	assertNetworkGone(t, networkState)
	info, err := os.Stat(transferPath)
	if err != nil {
		t.Fatal(err)
	}
	revokedAt := info.Size()
	time.Sleep(500 * time.Millisecond)
	info, err = os.Stat(transferPath)
	if err != nil {
		t.Fatal(err)
	}
	if info.Size() != revokedAt {
		t.Fatalf("in-flight transfer advanced after synchronous revoke: %d to %d bytes", revokedAt, info.Size())
	}
	revoked := exec.CommandContext(ctx, h.runtime.binaryPath(), "--root="+h.runtime.stateRoot(), "exec", containerName(h.id),
		"/bin/sh", "-c", "nc -z -w 2 "+host+" "+port)
	if out, err := revoked.CombinedOutput(); err == nil {
		t.Fatal("broker remained reachable after synchronous revoke")
	} else {
		t.Logf("post-revoke connection denied: %s", strings.TrimSpace(string(out)))
	}
}

// escapeWatcher reports IPv4 packets that leave the workspace namespace.
//
// It exists because the denial checks above cannot, on their own, tell denial
// from silence. Each of them asserts that a command fails, and a command that
// sends a packet into a black hole fails exactly like one whose packet was
// refused: the difference is whether a reply comes back, and no reply comes
// back either way. Six of the seven checks were observed passing on a backend
// where every packet was in fact crossing the boundary, because the return path
// happened to be blocked one hop further out. One-way egress is all that
// exfiltration needs, so "nothing came back" is not the property to assert.
//
// This asserts the property directly: nothing may appear on the host side of
// the veth. An AF_PACKET socket sees frames however the peer produced them,
// including the link-layer injection gVisor's sandbox netstack uses, which is
// precisely what slips past an nftables output hook.
type escapeWatcher struct {
	fd     int
	iface  string
	broker netip.Addr
}

// newEscapeWatcher binds to the host end of this workspace's veth. The backend
// names it rmh<pid> and the test runs one workspace, so a single rmh interface
// is unambiguous.
func newEscapeWatcher(t *testing.T, broker netip.Addr) *escapeWatcher {
	t.Helper()
	ifaces, err := net.Interfaces()
	if err != nil {
		t.Fatal(err)
	}
	var found *net.Interface
	for i := range ifaces {
		if strings.HasPrefix(ifaces[i].Name, "rmh") {
			if found != nil {
				t.Fatalf("more than one rmh interface (%s and %s); this watcher assumes one workspace", found.Name, ifaces[i].Name)
			}
			found = &ifaces[i]
		}
	}
	if found == nil {
		t.Fatal("no rmh interface: the workspace veth was not created, so this test cannot observe egress")
	}
	// htons(ETH_P_ALL): every frame, not just IPv4, so a policy that leaks by
	// some other ethertype is still visible.
	fd, err := unix.Socket(unix.AF_PACKET, unix.SOCK_RAW, int(htons(unix.ETH_P_ALL)))
	if err != nil {
		t.Skipf("unavailable: AF_PACKET requires CAP_NET_RAW: %v", err)
	}
	if err := unix.Bind(fd, &unix.SockaddrLinklayer{Protocol: htons(unix.ETH_P_ALL), Ifindex: found.Index}); err != nil {
		unix.Close(fd)
		t.Fatalf("bind %s: %v", found.Name, err)
	}
	// Non-blocking reads with a short timeout: drain() must return even when
	// the boundary holds and there is nothing at all to read.
	tv := unix.Timeval{Usec: 200000}
	_ = unix.SetsockoptTimeval(fd, unix.SOL_SOCKET, unix.SO_RCVTIMEO, &tv)
	w := &escapeWatcher{fd: fd, iface: found.Name, broker: broker}
	t.Cleanup(func() { unix.Close(fd) })
	return w
}

func htons(v uint16) uint16 { return v<<8 | v>>8 }

// escaped drains whatever has arrived and returns the distinct destinations of
// IPv4 packets that are not the broker. Traffic to the broker is permitted and
// is the one thing that should be there.
func (w *escapeWatcher) escaped() []string {
	seen := map[string]struct{}{}
	buf := make([]byte, 2048)
	for {
		n, err := unix.Read(w.fd, buf)
		if err != nil || n <= 0 {
			break
		}
		// Ethernet header is 14 bytes; IPv4 needs 20 more.
		if n < 34 || buf[12] != 0x08 || buf[13] != 0x00 {
			continue
		}
		ip := buf[14:]
		if ip[0]>>4 != 4 {
			continue
		}
		dst, ok := netip.AddrFromSlice(ip[16:20])
		if !ok || dst == w.broker || dst.IsLoopback() {
			continue
		}
		seen[dst.String()] = struct{}{}
	}
	out := make([]string, 0, len(seen))
	for d := range seen {
		out = append(out, d)
	}
	sort.Strings(out)
	return out
}

// TestE5SiblingTenantsCannotReachEachOther is E5: two mutually untrusting
// tenants on one node.
//
// The backend claims `SiblingIsolation: true`. That claim is exactly the shape
// of the `enforced_gateway` claim which, when it was finally probed, turned out
// to be unearned — so this probes it rather than trusting it. Two workspaces are
// created on one backend and each is asked to reach the other's private
// network: its broker address, and the guest address inside its namespace.
// Neither may succeed, and — the part a command's exit status cannot tell you —
// no frame from one may reach the other's veth at all.
func TestE5SiblingTenantsCannotReachEachOther(t *testing.T) {
	if os.Getenv("REMOUNT_GVISOR_INTEGRATION") != "1" {
		t.Skip("unavailable: set REMOUNT_GVISOR_INTEGRATION=1 on a privileged Linux host with runsc")
	}
	rootfs := os.Getenv("REMOUNT_GVISOR_ROOTFS")
	if rootfs == "" {
		t.Skip("unavailable: REMOUNT_GVISOR_ROOTFS is not set")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
	defer cancel()

	// One backend, which is what "share one node" means.
	backend, err := New(ctx, Options{Dir: t.TempDir(), RootFS: rootfs})
	if err != nil {
		t.Fatalf("gvisor backend unavailable in required lane: %v", err)
	}

	type tenant struct {
		id       string
		handle   *handle
		listener net.Listener
	}
	tenants := make([]*tenant, 0, 2)
	for _, id := range []string{"ws_tenant_a", "ws_tenant_b"} {
		raw, err := backend.Create(ctx, id, proto.WorkspaceSpec{}, nil)
		if err != nil {
			t.Fatalf("create %s: %v", id, err)
		}
		h := raw.(*handle)
		t.Cleanup(func() {
			cleanupCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			if err := h.Destroy(cleanupCtx); err != nil {
				t.Errorf("cleanup %s: %v", id, err)
			}
		})
		listener, err := net.Listen("tcp4", net.JoinHostPort(h.BrokerAdvertiseHost(), "0"))
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { listener.Close() })
		go func() {
			for {
				conn, err := listener.Accept()
				if err != nil {
					return
				}
				_ = conn.Close()
			}
		}()
		endpoint := "http://" + listener.Addr().String()
		if err := h.ApplyNetworkPolicy(ctx, proto.NetworkPolicy{Default: proto.NetworkDefaultDeny}, workspace.NetworkEndpoint{
			Workspace: id, Generation: 1, ReverseProxyURL: endpoint, ForwardProxyURL: endpoint,
		}); err != nil {
			t.Fatalf("apply policy for %s: %v", id, err)
		}
		tenants = append(tenants, &tenant{id: id, handle: h, listener: listener})
	}
	a, b := tenants[0], tenants[1]

	// Each tenant's own broker must work, or the negative results below would
	// be meaningless — a workspace with no network at all trivially cannot
	// reach its neighbour.
	for _, ten := range tenants {
		host, port, _ := net.SplitHostPort(ten.listener.Addr().String())
		if err := runIn(ctx, ten.handle, "nc -z -w 2 "+host+" "+port); err != nil {
			t.Fatalf("%s could not reach its own broker, so this test proves nothing: %v", ten.id, err)
		}
	}

	// The neighbour's addresses: its broker, and the address inside its
	// namespace. Neither is a destination this tenant has any claim to.
	bHost, bPort, _ := net.SplitHostPort(b.listener.Addr().String())
	for name, command := range map[string]string{
		"sibling broker":     "nc -z -w 2 " + bHost + " " + bPort,
		"sibling guest":      "nc -z -w 2 " + b.handle.network.GuestAddress().String() + " 22",
		"sibling broker UDP": "nslookup example.com " + bHost,
	} {
		t.Run(name, func(t *testing.T) {
			if err := runIn(ctx, a.handle, command); err == nil {
				t.Errorf("tenant A reached tenant B's %s (%s): the node does not isolate siblings, "+
					"and SiblingIsolation is claimed unconditionally", name, command)
			}
		})
	}
}

// runIn executes a shell command inside a workspace and reports its failure.
func runIn(ctx context.Context, h *handle, command string) error {
	spec := session.Spec{Kind: proto.SessionExec, Program: []string{"/bin/sh", "-c", command}}
	if err := h.Prepare(&spec); err != nil {
		return err
	}
	out, err := exec.CommandContext(ctx, spec.Program[0], spec.Program[1:]...).CombinedOutput()
	if err != nil {
		return fmt.Errorf("%w: %s", err, strings.TrimSpace(string(out)))
	}
	return nil
}

func TestE4FailedSetupCleanupConformance(t *testing.T) {
	if os.Getenv("REMOUNT_GVISOR_INTEGRATION") != "1" {
		t.Skip("unavailable: set REMOUNT_GVISOR_INTEGRATION=1 on a privileged Linux host with runsc")
	}
	rootfs := os.Getenv("REMOUNT_GVISOR_ROOTFS")
	if rootfs == "" {
		t.Skip("unavailable: REMOUNT_GVISOR_ROOTFS is not set")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	dir := t.TempDir()
	b, err := New(ctx, Options{Dir: dir, RootFS: rootfs})
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		err := unix.Unmount(filepath.Join(dir, ".runsc", "null-netns"), 0)
		if err != nil && !errors.Is(err, unix.EINVAL) && !errors.Is(err, unix.ENOENT) {
			t.Errorf("unmount runsc network namespace: %v", err)
		}
	}()
	raw, err := b.Create(ctx, "ws_e4_failed_setup", proto.WorkspaceSpec{}, nil)
	if err != nil {
		t.Fatal(err)
	}
	h := raw.(*handle)
	state := h.network.State()
	err = h.ApplyNetworkPolicy(ctx, proto.NetworkPolicy{Default: proto.NetworkDefaultDeny}, workspace.NetworkEndpoint{
		Workspace: h.id, Generation: 1,
		ReverseProxyURL: "http://127.0.0.1:17443", ForwardProxyURL: "http://127.0.0.1:17443",
	})
	if err == nil {
		t.Fatal("setup with a broker outside the generation link succeeded")
	}
	if h.network != nil {
		t.Fatal("failed setup retained the network handle")
	}
	assertNetworkGone(t, state)
	if err := h.Destroy(ctx); err != nil {
		t.Fatal(err)
	}
}

func assertNetworkGone(t *testing.T, state netns.State) {
	t.Helper()
	if _, err := os.Stat(state.Namespace); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("network namespace survived cleanup: %v", err)
	}
	if out, err := exec.Command("ip", "link", "show", "dev", state.Link.HostName).CombinedOutput(); err == nil {
		t.Fatalf("host veth survived cleanup: %s", strings.TrimSpace(string(out)))
	}
	table := "remount_" + state.Link.HostName
	if out, err := exec.Command("nft", "list", "table", "netdev", table).CombinedOutput(); err == nil {
		t.Fatalf("host ingress policy survived cleanup: %s", strings.TrimSpace(string(out)))
	}
}
