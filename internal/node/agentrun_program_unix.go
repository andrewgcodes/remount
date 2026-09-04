//go:build !windows

package node

func explicitACPProgram(_ string, command []string) []string {
	return append([]string{"/bin/sh", "-c", acpLauncherEnvScript, "remount-acp"}, command...)
}

func recipeACPProgram(_ string, path string) ([]string, error) {
	return []string{"/bin/sh", path}, nil
}
