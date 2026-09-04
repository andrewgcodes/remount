//go:build !windows

package firecracker

import (
	"context"
	"encoding/json"
	"io"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"remount.dev/remount/internal/fsops"
	"remount.dev/remount/internal/proto"
	"remount.dev/remount/internal/session"
)

func testGuestBridge(t *testing.T, root string) (*GuestBridge, GuestEndpoint, func()) {
	t.Helper()
	dir, err := os.MkdirTemp("/tmp", "remount-guest-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(dir) })
	manifestPath := filepath.Join(dir, "guest.json")
	manifest, _ := json.Marshal(GuestManifest{Protocol: GuestProtocolVersion, VsockPort: GuestVsockPort, WorkspaceDir: "/workspace", BinarySHA256: strings.Repeat("0", 64)})
	if err := os.WriteFile(manifestPath, manifest, 0o600); err != nil {
		t.Fatal(err)
	}
	bridge, err := NewGuestBridge(GuestOptions{Manifest: manifestPath, Timeout: 2 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	fs, err := fsops.New(root)
	if err != nil {
		t.Fatal(err)
	}
	socket := filepath.Join(dir, "vsock")
	listener, err := net.Listen("unix", socket)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			go func() {
				line, err := readBoundedLine(conn, 64)
				if err != nil || line != "CONNECT 10789" {
					conn.Close()
					return
				}
				_, _ = io.WriteString(conn, "OK 1\n")
				_ = serveGuestConn(ctx, conn, fs)
				_ = conn.Close()
			}()
		}
	}()
	cleanup := func() { cancel(); _ = listener.Close(); _ = fs.Close() }
	return bridge, GuestEndpoint{Workspace: "ws_guest", Generation: 3, Socket: socket}, cleanup
}

func TestGuestBridgeFilesystemOperatesOnGuestRoot(t *testing.T) {
	root := t.TempDir()
	bridge, endpoint, cleanup := testGuestBridge(t, root)
	defer cleanup()
	remote := bridge.FileSystem(func() (GuestEndpoint, error) { return endpoint, nil })
	if _, ok := remote.(interface{ Root() string }); ok {
		t.Fatal("guest filesystem exposed a host root capability")
	}
	if err := remote.Write("dir/file", []byte("guest bytes"), 0o600, false, true); err != nil {
		t.Fatal(err)
	}
	if got, err := os.ReadFile(filepath.Join(root, "dir", "file")); err != nil || string(got) != "guest bytes" {
		t.Fatalf("guest disk bytes = %q, %v", got, err)
	}
	read, err := remote.Read("dir/file", 0, 0)
	if err != nil || string(read.Data) != "guest bytes" {
		t.Fatalf("remote read = %+v, %v", read, err)
	}
}

func TestGuestRunnerPreservesOutputAndExit(t *testing.T) {
	bridge, endpoint, cleanup := testGuestBridge(t, t.TempDir())
	defer cleanup()
	spec := session.Spec{WS: endpoint.Workspace, Kind: proto.SessionExec, Program: []string{"/bin/sh", "-c", "printf guest-output; exit 9"}}
	if err := bridge.Prepare(&spec, endpoint); err != nil {
		t.Fatal(err)
	}
	manager := session.NewManager(session.ManagerOptions{})
	s, err := manager.Open(spec)
	if err != nil {
		t.Fatal(err)
	}
	info, err := s.Wait(t.Context())
	if err != nil || info.Code != 9 {
		t.Fatalf("exit = %+v, %v", info, err)
	}
	chunks, err := s.Log.Read(0, 16)
	if err != nil {
		t.Fatal(err)
	}
	if len(chunks) != 3 || chunks[0].Stream != proto.StreamInfo || chunks[1].Stream != proto.StreamStdout || string(chunks[1].Data) != "guest-output" || chunks[2].Stream != proto.StreamExit {
		t.Fatalf("chunks = %+v", chunks)
	}
}
