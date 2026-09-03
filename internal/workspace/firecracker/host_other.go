//go:build !linux

package firecracker

import (
	"context"
	"errors"
	"os"
)

func acquireHostLock(string) (*os.File, error) {
	return nil, errors.New("firecracker host ownership requires Linux")
}

func releaseHostLock(file *os.File) error {
	if file == nil {
		return nil
	}
	return file.Close()
}

func recoverOwnedJails(context.Context, string, string, int) error {
	return errors.New("firecracker jail recovery requires Linux")
}
