//go:build windows

package fsops

import "os"

func openRead(root *os.Root, name string) (*os.File, error) {
	return root.Open(name)
}

func openAppend(root *os.Root, name string, mode os.FileMode) (*os.File, error) {
	return root.OpenFile(name, os.O_CREATE|os.O_WRONLY|os.O_APPEND, mode)
}
