//go:build windows

package node

// Windows does not permit opening a directory for os.File.Sync. The file is
// flushed before the atomic rename; there is no portable parent-directory
// flush primitive in the Go standard library on this platform.
func syncParentDir(string) error { return nil }
