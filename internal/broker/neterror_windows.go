//go:build windows

package broker

import (
	"errors"

	"golang.org/x/sys/windows"
)

func isPlatformConnectionRefused(err error) bool {
	return errors.Is(err, windows.WSAECONNREFUSED)
}

func isPlatformConnectionReset(err error) bool {
	return errors.Is(err, windows.WSAECONNRESET)
}
