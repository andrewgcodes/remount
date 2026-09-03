//go:build linux

package firecracker

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"

	"golang.org/x/sys/unix"
)

func serveGuestVsock(ctx context.Context, serve func(io.ReadWriteCloser)) error {
	fd, err := unix.Socket(unix.AF_VSOCK, unix.SOCK_STREAM|unix.SOCK_CLOEXEC, 0)
	if err != nil {
		return fmt.Errorf("guest vsock socket: %w", err)
	}
	defer unix.Close(fd)
	if err := unix.Bind(fd, &unix.SockaddrVM{CID: unix.VMADDR_CID_ANY, Port: guestVsockPort}); err != nil {
		return fmt.Errorf("guest vsock bind: %w", err)
	}
	if err := unix.Listen(fd, 64); err != nil {
		return fmt.Errorf("guest vsock listen: %w", err)
	}
	go func() {
		<-ctx.Done()
		_ = unix.Shutdown(fd, unix.SHUT_RDWR)
	}()
	for {
		connFD, peer, err := unix.Accept4(fd, unix.SOCK_CLOEXEC)
		if err != nil {
			if ctx.Err() != nil || errors.Is(err, unix.EINVAL) || errors.Is(err, unix.EBADF) {
				return ctx.Err()
			}
			return err
		}
		vm, ok := peer.(*unix.SockaddrVM)
		if !ok || vm.CID != unix.VMADDR_CID_HOST {
			_ = unix.Close(connFD)
			continue
		}
		file := os.NewFile(uintptr(connFD), "remount-guest-vsock")
		go serve(file)
	}
}
