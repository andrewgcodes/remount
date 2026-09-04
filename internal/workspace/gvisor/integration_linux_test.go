//go:build linux

package gvisor

import (
	"context"
	"fmt"
	"golang.org/x/sys/unix"
	"net"
	"net/netip"
	"os"
	"os/exec"
	"sort"
	"strings"
	"testing"
	"time"

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
	b, err := New(ctx, Options{Dir: t.TempDir(), RootFS: rootfs})
	if err != nil {
		t.Fatalf("gvisor backend unavailable in required lane: %v", err)
	}
	raw, err := b.Create(ctx, "ws_e4", proto.WorkspaceSpec{}, nil)
	if err != nil {
		t.Fatal(err)
	}
	h := raw.(*handle)
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
			_ = conn.Close()
		}
	}()
	endpoint := "http://" + listener.Addr().String()
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
	if err := run("nc -z -w 2 " + host + " " + port); err != nil {
		t.Fatalf("broker was not reachable: %v", err)
	}
	denied := map[string]string{
		"direct IPv4 TCP":         "nc -z -w 2 1.1.1.1 443",
		"IPv6":                    "nc -z -w 2 2606:4700:4700::1111 443",
		"UDP":                     "nc -u -z -w 2 8.8.8.8 53",
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
