//go:build windows

package session

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"golang.org/x/sys/windows"
	"remount.dev/remount/internal/proto"
)

func TestWindowsProcessIsContainedBeforeItCanSpawn(t *testing.T) {
	dir := t.TempDir()
	pidFile := filepath.Join(dir, "child.pid")
	quotedPIDFile := strings.ReplaceAll(pidFile, "'", "''")
	parent := `$p = Start-Process ping -ArgumentList @('-n','60','127.0.0.1') -PassThru; ` +
		`[IO.File]::WriteAllText('` + quotedPIDFile + `', [string]$p.Id); Wait-Process -Id $p.Id`
	cmd := exec.Command("powershell", "-NoProfile", "-Command", parent)
	configureProcessGroup(cmd)
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = signalProcess(cmd, "KILL")
		_ = cmd.Wait()
		releaseProcessGroup(cmd)
	})

	time.Sleep(200 * time.Millisecond)
	if _, err := os.Stat(pidFile); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("process executed before Job Object assignment: %v", err)
	}
	if err := registerProcessGroup(cmd); err != nil {
		t.Fatal(err)
	}
	pid := waitForPID(t, pidFile)
	if err := signalProcess(cmd, "KILL"); err != nil {
		t.Fatal(err)
	}
	if err := cmd.Wait(); err == nil {
		t.Fatal("terminated process exited successfully")
	}
	releaseProcessGroup(cmd)
	assertProcessExited(t, pid)
}

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
	pid := waitForPID(t, pidFile)
	if err := m.KillWorkspace("ws_windows_job"); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, err := s.Wait(ctx); err != nil {
		t.Fatal(err)
	}
	assertProcessExited(t, pid)
}

func waitForPID(t *testing.T, path string) int {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		body, err := os.ReadFile(path)
		if err == nil && len(strings.TrimSpace(string(body))) > 0 {
			pid, err := strconv.Atoi(strings.TrimSpace(string(body)))
			if err == nil {
				return pid
			}
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("child pid was not reported")
	return 0
}

func assertProcessExited(t *testing.T, pid int) {
	t.Helper()
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
