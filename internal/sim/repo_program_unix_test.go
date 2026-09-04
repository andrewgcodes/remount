//go:build !windows

package sim

func repoShellProgram(script string) []string {
	return []string{"sh", "-c", script}
}

func repoEnvironmentProgram() []string {
	return []string{"env"}
}
