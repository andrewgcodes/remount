//go:build windows

package node

import "remount.dev/remount/internal/proto"

func explicitACPProgram(backend string, command []string) []string {
	if backend == "process" {
		return command
	}
	return append([]string{"/bin/sh", "-c", acpLauncherEnvScript, "remount-acp"}, command...)
}

func recipeACPProgram(backend, path string) ([]string, error) {
	if backend == "process" {
		return nil, proto.Err(proto.CodeUnsupported, "recipe ACP launchers require a POSIX shell and are unavailable in Windows process workspaces")
	}
	return []string{"/bin/sh", path}, nil
}
