// Command minimal-shell is the smallest complete Remount program: one
// workspace, one streamed command, one file written and read back, then the
// workspace destroyed. It imports only the supported public packages
// (remount.dev/remount/api and remount.dev/remount/client), needs no
// credentials, and reaches no network but the Remount server itself.
//
// Run a whole system on a laptop and point this at it:
//
//	go run ./cmd/remount standalone --listen 127.0.0.1:7443 --data ./remount-data
//	REMOUNT_SERVER=http://127.0.0.1:7443 go run ./examples/minimal-shell
//
// REMOUNT_TOKEN is read when set, so the same program works against a server
// that requires one. Standalone mode has no token.
package main

import (
	"context"
	"fmt"
	"io"
	"os"
	"time"

	"remount.dev/remount/api"
	"remount.dev/remount/client"
)

func main() {
	if err := run(context.Background(), os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, "minimal-shell:", err)
		os.Exit(1)
	}
}

func run(ctx context.Context, out io.Writer) error {
	ctx, cancel := context.WithTimeout(ctx, 3*time.Minute)
	defer cancel()

	// One reconnecting connection. The client redials and resumes streams on
	// its own; nothing below has to know a connection was lost.
	c, err := client.New(client.Options{
		Server:    envOr("REMOUNT_SERVER", "http://127.0.0.1:7443"),
		Token:     os.Getenv("REMOUNT_TOKEN"),
		Principal: "a_minimal_shell",
	})
	if err != nil {
		return err
	}
	defer c.Close()

	// Create is a mutation, so it carries an idempotency key. The SDK mints
	// one per call; pass client.WithIdempotencyKey to own a key that survives
	// a restart of this process and makes a retry a no-op instead of a
	// second workspace.
	ws, err := c.CreateWorkspace(ctx, api.WorkspaceSpec{Name: "minimal-shell"})
	if err != nil {
		return err
	}
	// A workspace is pending until a node has claimed and restored it. Talking
	// to it before ws.ready is what WaitClaimed exists to prevent.
	if ws, err = c.WaitClaimed(ctx, ws.ID); err != nil {
		return err
	}
	fmt.Fprintf(out, "workspace %s claimed by node %s\n", ws.ID, ws.Node)
	defer func() {
		// Destroy on the way out even when the body failed; the deferred
		// context is the caller's, which may already be cancelled.
		stop, cancelStop := context.WithTimeout(context.WithoutCancel(ctx), 30*time.Second)
		defer cancelStop()
		if err := c.DestroyWorkspace(stop, ws.ID); err != nil {
			fmt.Fprintf(out, "destroy %s: %v\n", ws.ID, err)
			return
		}
		fmt.Fprintf(out, "workspace %s destroyed\n", ws.ID)
	}()

	// A session is a replayable log, not a socket: Copy streams it and
	// reports an elided range as an error rather than as complete output.
	session, err := c.Exec(ctx, api.SessionOpenRequest{
		WS: ws.ID, Kind: api.SessionExec, TimeoutSec: 60,
		Program: []string{"sh", "-c", "echo hello from $(pwd)"},
	})
	if err != nil {
		return err
	}
	exit, err := client.Copy(ctx, session, out, out)
	if err != nil {
		return err
	}
	if exit == nil {
		return fmt.Errorf("session %s ended without an exit chunk", session.ID)
	}
	fmt.Fprintf(out, "exit %d\n", exit.Code)

	if err := c.WriteFile(ctx, ws.ID, "hello.txt", []byte("written by the Remount SDK\n"), 0o644); err != nil {
		return err
	}
	data, err := c.ReadFile(ctx, ws.ID, "hello.txt")
	if err != nil {
		return err
	}
	fmt.Fprintf(out, "read back %d bytes: %s", len(data), data)
	return nil
}

func envOr(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}
