//go:build windows

package sim

func repoShellProgram(script string) []string {
	return []string{"cmd.exe", "/d", "/s", "/c", script}
}

func repoEnvironmentProgram() []string {
	return []string{"cmd.exe", "/d", "/s", "/c", "set"}
}
