//go:build !linux

package volume

import (
	"context"
	"os"
)

// BindMountEngine is unavailable on non-Linux hosts because this package
// cannot honestly enforce Linux per-mount read-only flags there.
type BindMountEngine struct{}

// MountReadOnly returns ErrUnsupported on non-Linux hosts.
func (BindMountEngine) MountReadOnly(context.Context, *os.File, *os.File) error {
	return ErrUnsupported
}

// Unmount returns ErrUnsupported on non-Linux hosts.
func (BindMountEngine) Unmount(context.Context, *os.File) error { return ErrUnsupported }

// Inspect returns ErrUnsupported on non-Linux hosts.
func (BindMountEngine) Inspect(context.Context, *os.File) (MountStatus, error) {
	return MountStatus{}, ErrUnsupported
}
