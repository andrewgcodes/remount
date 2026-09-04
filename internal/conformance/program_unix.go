//go:build !windows

package conformance

import "strconv"

func echoProgram(text string) []string { return []string{"/bin/echo", text} }

func catProgram() []string { return []string{"/bin/cat"} }

func exitProgram(code int) []string {
	return []string{"/bin/sh", "-c", "exit " + strconv.Itoa(code)}
}
