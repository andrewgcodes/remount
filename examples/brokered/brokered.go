// Package brokered holds the few helpers the three brokered-credential
// examples share, so each example's own file is only the part that differs:
// where its provider takes the credential.
//
// It imports nothing but the supported public packages
// (remount.dev/remount/api and remount.dev/remount/client) and the standard
// library, so copying any of these examples into another repository means
// copying this file with it and nothing else.
package brokered

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	"remount.dev/remount/client"
)

// EnvOr reads an environment variable with a fallback.
func EnvOr(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}

// Suffix is a short random tail for a binding id, so running an example twice
// does not collide with the binding the first run revoked. Revocation is
// permanent and an id is never reusable.
func Suffix() string {
	var raw [4]byte
	if _, err := rand.Read(raw[:]); err != nil {
		return fmt.Sprint(time.Now().UnixNano() % 1e8)
	}
	return hex.EncodeToString(raw[:])
}

// Probe runs one curl invocation inside the workspace and returns the HTTP
// status, the broker's X-Remount-Reason (empty when the request was not
// refused by the broker) and the response body.
//
// It is curl rather than a Go client because the point is what an ordinary
// program inside the workspace sees: it holds a placeholder, it talks to a
// URL, and it never learns that a credential was involved.
func Probe(ctx context.Context, c *client.Client, workspace, script string) (status, reason, body string, err error) {
	// `-o body -D head` write into the workspace's own working directory, so
	// the probe needs no temporary directory and leaves nothing on the host.
	program := script + `; printf ' '; grep -i '^x-remount-reason:' head | tr -d '\r' | awk '{print $2}'; printf '|'; cat body`
	stdout, stderr, exit, err := c.Run(ctx, workspace, "sh", "-c", program)
	if err != nil {
		return "", "", "", err
	}
	if exit == nil {
		return "", "", "", fmt.Errorf("probe ended without an exit chunk: %s", stderr)
	}
	head, rest, _ := strings.Cut(string(stdout), "|")
	fields := strings.Fields(head)
	if len(fields) == 0 {
		return "", "", rest, fmt.Errorf("probe printed no status (exit %d): %s", exit.Code, stderr)
	}
	status = fields[0]
	if len(fields) > 1 {
		reason = fields[1]
	}
	return status, reason, rest, nil
}

// CountCredentialInEnv reports how many times the real credential appears in
// the workspace's session environment.
//
// Counting the value this program already holds is a stronger check than
// matching a provider's key pattern: it cannot be fooled by a key shape the
// pattern does not know, and it needs no pattern to be kept up to date. The
// value is passed as a positional argument to a shell that only does a
// fixed-string count, so it is never echoed and never written to a file.
func CountCredentialInEnv(ctx context.Context, c *client.Client, workspace, secret string) (int, error) {
	stdout, stderr, exit, err := c.Run(ctx, workspace, "sh", "-c",
		`env | grep -c -F -- "$1" || true`, "count-credential", secret)
	if err != nil {
		return 0, err
	}
	if exit == nil {
		return 0, fmt.Errorf("credential scan ended without an exit chunk: %s", stderr)
	}
	count := 0
	if _, err := fmt.Sscan(strings.TrimSpace(string(stdout)), &count); err != nil {
		return 0, fmt.Errorf("credential scan printed %q", strings.TrimSpace(string(stdout)))
	}
	return count, nil
}

// Cleanup destroys the workspace and revokes the binding on the way out, even
// when the body failed. The deferred context is the caller's, which may
// already be cancelled, so the teardown gets one of its own.
func Cleanup(ctx context.Context, c *client.Client, out io.Writer, workspace, binding string) {
	stop, cancel := context.WithTimeout(context.WithoutCancel(ctx), 60*time.Second)
	defer cancel()
	if workspace != "" {
		if err := c.DestroyWorkspace(stop, workspace); err != nil {
			fmt.Fprintf(out, "destroy %s: %v\n", workspace, err)
		} else {
			fmt.Fprintf(out, "workspace %s destroyed\n", workspace)
		}
	}
	if binding == "" {
		return
	}
	// Revocation stops Remount substituting the credential within one renew
	// interval. It is not provider-side revocation: the key stays valid at the
	// provider until it is rotated or deleted there.
	if _, err := c.RevokeBinding(stop, "", binding, "example finished"); err != nil {
		fmt.Fprintf(out, "revoke %s: %v\n", binding, err)
		return
	}
	fmt.Fprintf(out, "binding %s revoked\n", binding)
}
