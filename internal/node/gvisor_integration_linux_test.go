//go:build linux

package node

import (
	"context"
	"encoding/base64"
	"fmt"
	"net"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"golang.org/x/sys/unix"

	"remount.dev/remount/internal/proto"
	"remount.dev/remount/internal/session"
	"remount.dev/remount/internal/workspace"
	"remount.dev/remount/internal/workspace/gvisor"
)

func TestE5TenantIsolationConformance(t *testing.T) {
	if testing.Short() || os.Getenv("REMOUNT_GVISOR_INTEGRATION") != "1" {
		t.Skip("unavailable: set REMOUNT_GVISOR_INTEGRATION=1 on a privileged Linux host with runsc")
	}
	rootfs := os.Getenv("REMOUNT_GVISOR_ROOTFS")
	if rootfs == "" {
		t.Skip("unavailable: REMOUNT_GVISOR_ROOTFS is not set")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	backendDir := t.TempDir()
	backend, err := gvisor.New(ctx, gvisor.Options{Dir: backendDir, RootFS: rootfs})
	if err != nil {
		t.Fatalf("gvisor backend unavailable in required lane: %v", err)
	}
	n := newTestNode(t, func(opts *Options) {
		opts.Backends = workspace.NewRegistry(backend)
	})
	n.protocol = proto.PeerCapabilities()
	workspaces := []proto.Workspace{
		{
			ID: "ws_e5_a", Tenant: "tenant-a", Generation: 1, State: proto.WSClaiming,
			Spec: proto.WorkspaceSpec{
				Principal: "alice", Requires: proto.Requires{Backend: "gvisor"},
				Security: proto.SecuritySpec{Profile: proto.SecurityIsolated, Network: proto.NetworkPolicy{Default: proto.NetworkDefaultDeny}},
			},
		},
		{
			ID: "ws_e5_b", Tenant: "tenant-b", Generation: 1, State: proto.WSClaiming,
			Spec: proto.WorkspaceSpec{
				Principal: "bob", Requires: proto.Requires{Backend: "gvisor"},
				Security: proto.SecuritySpec{Profile: proto.SecurityIsolated, Network: proto.NetworkPolicy{Default: proto.NetworkDefaultDeny}},
			},
		},
	}
	for _, w := range workspaces {
		n.materializing[w.ID] = &materialization{generation: w.Generation, deadline: time.Now().Add(time.Minute), cancel: func() {}}
		n.eventClaims[w.ID] = eventClaim{tenant: w.Tenant, generation: w.Generation}
		if err := n.materializeWithReady(ctx, w, false, false); err != nil {
			t.Fatal(err)
		}
	}
	defer func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer cleanupCancel()
		for _, w := range workspaces {
			entry := n.workspaces[w.ID]
			if entry == nil {
				continue
			}
			_ = entry.broker.Close()
			if err := entry.handle.Destroy(cleanupCtx); err != nil {
				t.Errorf("destroy %s: %v", w.ID, err)
			}
		}
		if err := unix.Unmount(filepath.Join(backendDir, ".runsc", "null-netns"), 0); err != nil && err != unix.EINVAL && err != unix.ENOENT {
			t.Errorf("unmount runsc network namespace: %v", err)
		}
	}()

	a := n.workspaces["ws_e5_a"]
	b := n.workspaces["ws_e5_b"]
	if _, err := runGVisorCommand(ctx, a, "printf tenant-a > /work/private"); err != nil {
		t.Fatal(err)
	}
	if _, err := runGVisorCommand(ctx, b, "test ! -e /work/private"); err != nil {
		t.Fatal("tenant-b observed tenant-a workspace data")
	}
	for source, target := range map[*ws]*ws{a: b, b: a} {
		if err := crossTenantBrokerDenial(ctx, source, target); err != nil {
			t.Fatal(err)
		}
	}
	events, err := n.Events().Read(ctx, 1, "", 100)
	if err != nil {
		t.Fatal(err)
	}
	denials := map[string]string{}
	for _, event := range events {
		if event.Type == proto.EvEgressDenied {
			denials[event.Workspace] = event.Tenant
		}
	}
	if denials["ws_e5_a"] != "tenant-a" || denials["ws_e5_b"] != "tenant-b" || len(denials) != 2 {
		t.Fatalf("cross-tenant denials were not isolated events: %#v", denials)
	}
}

func crossTenantBrokerDenial(ctx context.Context, source, target *ws) error {
	proxy, err := url.Parse(source.broker.ProxyURL())
	if err != nil {
		return err
	}
	targetProxy, err := url.Parse(target.broker.ProxyURL())
	if err != nil {
		return err
	}
	proxyHost, proxyPort, err := net.SplitHostPort(proxy.Host)
	if err != nil {
		return err
	}
	targetHost, targetPort, err := net.SplitHostPort(targetProxy.Host)
	if err != nil {
		return err
	}
	auth := base64.StdEncoding.EncodeToString([]byte(proxy.User.Username() + ":"))
	targetURL := "http://" + net.JoinHostPort(targetHost, targetPort) + "/"
	request := fmt.Sprintf(
		"printf 'GET %s HTTP/1.1\\r\\nHost: %s\\r\\nProxy-Authorization: Basic %s\\r\\nConnection: close\\r\\n\\r\\n' | nc -w 3 %s %s",
		targetURL, net.JoinHostPort(targetHost, targetPort), auth, proxyHost, proxyPort,
	)
	out, err := runGVisorCommand(ctx, source, request)
	if err != nil {
		return err
	}
	if !strings.Contains(string(out), " 403 ") {
		return fmt.Errorf("cross-tenant broker request was not denied")
	}
	return nil
}

func runGVisorCommand(ctx context.Context, entry *ws, command string) ([]byte, error) {
	spec := session.Spec{Kind: proto.SessionExec, Program: []string{"/bin/sh", "-c", command}}
	if err := entry.handle.Prepare(&spec); err != nil {
		return nil, err
	}
	return exec.CommandContext(ctx, spec.Program[0], spec.Program[1:]...).CombinedOutput()
}
