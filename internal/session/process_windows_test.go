//go:build windows

package session

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"golang.org/x/sys/windows"
	"remount.dev/remount/internal/proto"
)

func TestKillWorkspaceTerminatesWindowsDescendants(t *testing.T) {
	dir := t.TempDir()
	pidFile := filepath.Join(dir, "child.pid")
	quotedPIDFile := strings.ReplaceAll(pidFile, "'", "''")
	parent := `$p = Start-Process ping -ArgumentList @('-n','60','127.0.0.1') -PassThru; ` +
		`[IO.File]::WriteAllText('` + quotedPIDFile + `', [string]$p.Id); Wait-Process -Id $p.Id`
	m := newMgr(t)
	s, err := m.Open(Spec{
		WS:      "ws_windows_job",
		Kind:    proto.SessionExec,
		Program: []string{"powershell", "-NoProfile", "-Command", parent},
		Cwd:     dir,
	})
	if err != nil {
		t.Fatal(err)
	}
	var pid int
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		body, readErr := os.ReadFile(pidFile)
		if readErr == nil {
			pid, err = strconv.Atoi(strings.TrimSpace(string(body)))
			if err != nil {
				t.Fatal(err)
			}
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if pid == 0 {
		t.Fatal("child pid was not reported")
	}
	if err := m.KillWorkspace("ws_windows_job"); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, err := s.Wait(ctx); err != nil {
		t.Fatal(err)
	}
	process, err := windows.OpenProcess(windows.SYNCHRONIZE, false, uint32(pid))
	if errors.Is(err, windows.ERROR_INVALID_PARAMETER) {
		return
	}
	if err != nil {
		t.Fatal(err)
	}
	defer windows.CloseHandle(process)
	if status, err := windows.WaitForSingleObject(process, 0); err != nil || status != windows.WAIT_OBJECT_0 {
		t.Fatalf("descendant %d remains alive: status=%d err=%v", pid, status, err)
	}
}
