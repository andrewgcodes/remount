//go:build !unix

package node

// Windows has job objects rather than POSIX process groups, and no equivalent
// of signal 0, so a test cannot ask this question the same way. The tests that
// need it skip rather than assert a property the platform does not have; this
// stub exists so the package still compiles and every other test in it runs.
func processGroupAlive(int) bool { return false }

const posixProcessGroups = false
