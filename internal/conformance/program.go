package conformance

import (
	"runtime"
	"strconv"
	"strings"
)

func (t *Target) workspaceOS() string {
	switch strings.ToLower(t.Backend) {
	case "docker", "firecracker", "gvisor":
		return "linux"
	}
	if t.Environment.OS != "" {
		return t.Environment.OS
	}
	return runtime.GOOS
}

func echoProgram(osName, text string) []string {
	if osName == "windows" {
		return []string{"cmd.exe", "/d", "/s", "/c", "echo " + text}
	}
	return []string{"/bin/echo", text}
}

func catProgram(osName string) []string {
	if osName == "windows" {
		return []string{"powershell.exe", "-NoProfile", "-Command", "$input | ForEach-Object { $_ }"}
	}
	return []string{"/bin/cat"}
}

func exitProgram(osName string, code int) []string {
	if osName == "windows" {
		return []string{"cmd.exe", "/d", "/s", "/c", "exit", strconv.Itoa(code)}
	}
	return []string{"/bin/sh", "-c", "exit " + strconv.Itoa(code)}
}
