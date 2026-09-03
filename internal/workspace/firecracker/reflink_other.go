//go:build !linux

package firecracker

import "errors"

func hostReflink(string, string) error {
	return errors.New("reflink (FICLONE) requires Linux")
}
