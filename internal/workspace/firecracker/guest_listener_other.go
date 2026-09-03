//go:build !linux

package firecracker

import (
	"context"
	"errors"
	"io"
)

func serveGuestVsock(context.Context, func(io.ReadWriteCloser)) error {
	return errors.New("firecracker guest agent requires Linux AF_VSOCK")
}
