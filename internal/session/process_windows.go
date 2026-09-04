//go:build windows

package session

import (
	"fmt"
	"os"
	"os/exec"
	"sync"
	"syscall"
	"unsafe"

	"golang.org/x/sys/windows"
	"remount.dev/remount/internal/proto"
)

var processJobs = struct {
	sync.Mutex
	handles map[int]windows.Handle
}{handles: make(map[int]windows.Handle)}

func configureProcessGroup(cmd *exec.Cmd) {
	cmd.SysProcAttr = &syscall.SysProcAttr{CreationFlags: windows.CREATE_NEW_PROCESS_GROUP}
}

func registerProcessGroup(cmd *exec.Cmd) error {
	if cmd.Process == nil {
		return fmt.Errorf("process has not started")
	}
	job, err := windows.CreateJobObject(nil, nil)
	if err != nil {
		return err
	}
	info := windows.JOBOBJECT_EXTENDED_LIMIT_INFORMATION{}
	info.BasicLimitInformation.LimitFlags = windows.JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE
	if _, err := windows.SetInformationJobObject(
		job,
		windows.JobObjectExtendedLimitInformation,
		uintptr(unsafe.Pointer(&info)),
		uint32(unsafe.Sizeof(info)),
	); err != nil {
		windows.CloseHandle(job)
		return err
	}
	process, err := windows.OpenProcess(
		windows.PROCESS_SET_QUOTA|windows.PROCESS_TERMINATE,
		false,
		uint32(cmd.Process.Pid),
	)
	if err != nil {
		windows.CloseHandle(job)
		return err
	}
	err = windows.AssignProcessToJobObject(job, process)
	windows.CloseHandle(process)
	if err != nil {
		windows.CloseHandle(job)
		return err
	}
	processJobs.Lock()
	processJobs.handles[cmd.Process.Pid] = job
	processJobs.Unlock()
	return nil
}

func releaseProcessGroup(cmd *exec.Cmd) {
	if cmd.Process == nil {
		return
	}
	processJobs.Lock()
	job := processJobs.handles[cmd.Process.Pid]
	delete(processJobs.handles, cmd.Process.Pid)
	processJobs.Unlock()
	if job != 0 {
		windows.CloseHandle(job)
	}
}

func signalProcess(cmd *exec.Cmd, name string) error {
	switch name {
	case "KILL", "TERM":
		processJobs.Lock()
		job := processJobs.handles[cmd.Process.Pid]
		processJobs.Unlock()
		if job != 0 {
			return windows.TerminateJobObject(job, 1)
		}
		return cmd.Process.Kill()
	case "INT":
		return cmd.Process.Signal(os.Interrupt)
	case "HUP", "QUIT", "USR1", "USR2":
		return proto.Err(proto.CodeUnsupported, "signal %q is not supported on Windows", name)
	default:
		return proto.Err(proto.CodeBadRequest, "unknown signal %q", name)
	}
}

func platformExitSignal(err *exec.ExitError) (string, int, bool) {
	return "", 0, false
}

func isPTYEOF(error) bool {
	return false
}
