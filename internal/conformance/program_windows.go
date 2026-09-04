//go:build windows

package conformance

import "strconv"

func echoProgram(text string) []string {
	return []string{"cmd.exe", "/d", "/s", "/c", "echo", text}
}

func catProgram() []string {
	return []string{"powershell.exe", "-NoProfile", "-Command", "$input | ForEach-Object { $_ }"}
}

func exitProgram(code int) []string {
	return []string{"cmd.exe", "/d", "/s", "/c", "exit", strconv.Itoa(code)}
}
