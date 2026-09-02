//go:build windows

package main

import (
	"context"

	"remount.dev/remount/internal/client"
)

func watchResize(ctx context.Context, s *client.Session) {}
