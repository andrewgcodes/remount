//go:build linux

package gvisor

import (
	"context"
	"fmt"
	"net"
	"os"
	"os/exec"
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
	for name, command := range denied {
		t.Run(name, func(t *testing.T) {
			if err := run(command); err == nil {
				t.Fatalf("forbidden lane succeeded: %s", command)
			}
		})
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
