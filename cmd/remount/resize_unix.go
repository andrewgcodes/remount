//go:build !windows

package main

import (
	"context"
	"os"
	"os/signal"
	"syscall"

	"golang.org/x/term"
)

// watchResize forwards terminal size changes to a pty session.
func watchResize(ctx context.Context, s liveSession) {
	ch := make(chan os.Signal, 1)
	signal.Notify(ch, syscall.SIGWINCH)
	defer signal.Stop(ch)
	for {
		select {
		case <-ctx.Done():
			return
		case <-ch:
			if w, h, err := term.GetSize(int(os.Stdout.Fd())); err == nil {
				_ = s.Resize(ctx, uint16(h), uint16(w))
			}
		}
	}
}
