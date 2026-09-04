package gvisor

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"
)

type runtimeClient interface {
	probe(context.Context) error
	reapStateRoot(context.Context) (int, error)
	createStart(context.Context, string, string) error
	pause(context.Context, string) error
	resume(context.Context, string) error
	destroy(context.Context, string) error
	binaryPath() string
	stateRoot() string
}

type execRuntime struct{ binary, root string }

func (r execRuntime) binaryPath() string { return r.binary }
func (r execRuntime) stateRoot() string  { return r.root }

func (r execRuntime) probe(ctx context.Context) error {
	out, err := exec.CommandContext(ctx, r.binary, "--version").CombinedOutput()
	if err != nil {
		return fmt.Errorf("gvisor: runsc --version: %s: %w", strings.TrimSpace(string(out)), err)
	}
	return nil
}

// reapStateRoot destroys every container still recorded in this backend's own
// runsc state root, and reports how many.
//
// It runs before any workspace is created, and that is what makes it safe:
// nothing this process started can be in there yet, so anything present was
// left by a previous process that did not get to clean up. Those sandboxes are
// not merely idle — each one holds the network namespace it was started in, so
// until they are gone the namespace reaper cannot reclaim a single slot, and
// the backend collides with its own leftovers on every start.
//
// This mirrors what the Firecracker backend already does with retained jails.
func (r execRuntime) reapStateRoot(ctx context.Context) (int, error) {
	out, err := exec.CommandContext(ctx, r.binary, "--root="+r.root, "list", "--format=json").Output()
	if err != nil {
		// A state root that has never been used has nothing to list, which is
		// the ordinary first-start case rather than a problem.
		return 0, nil
	}
	var containers []struct {
		ID string `json:"id"`
	}
	if err := json.Unmarshal(out, &containers); err != nil {
		return 0, fmt.Errorf("runsc list: %w", err)
	}
	reaped := 0
	var errs []error
	for _, container := range containers {
		if container.ID == "" {
			continue
		}
		if err := r.destroy(ctx, container.ID); err != nil {
			errs = append(errs, fmt.Errorf("reap %s: %w", container.ID, err))
			continue
		}
		reaped++
	}
	return reaped, errors.Join(errs...)
}

func (r execRuntime) createStart(ctx context.Context, bundle, id string) error {
	if err := r.run(ctx, "--network=sandbox", "--net-raw=false", "--allow-packet-socket-write=false", "create", "--bundle="+bundle, id); err != nil {
		return err
	}
	if err := r.run(ctx, "start", id); err != nil {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = r.destroy(cleanupCtx, id)
		return err
	}
	return nil
}
func (r execRuntime) pause(ctx context.Context, id string) error  { return r.run(ctx, "pause", id) }
func (r execRuntime) resume(ctx context.Context, id string) error { return r.run(ctx, "resume", id) }
func (r execRuntime) destroy(ctx context.Context, id string) error {
	err := r.run(ctx, "delete", "--force", id)
	if err != nil && strings.Contains(err.Error(), "does not exist") {
		err = nil
	}
	// `runsc delete` does not unmount the nsfs bind it leaves at
	// <root>/null-netns, so a node that creates and destroys workspaces
	// accumulates one mount per workspace, permanently, on the path where
	// everything succeeded. Measured: a completed run took the host from 9 such
	// mounts to 10. The state root belongs to this backend, so releasing it
	// belongs here.
	if detachErr := detachMount(filepath.Join(r.root, "null-netns")); detachErr != nil {
		err = errors.Join(err, fmt.Errorf("release %s/null-netns: %w", r.root, detachErr))
	}
	return err
}

// run invokes runsc and returns its output only on failure.
//
// It deliberately does not use CombinedOutput. `runsc create` and `runsc start`
// leave a sandbox process and a gofer process running after they themselves
// exit, and those daemons inherit whatever stdout and stderr they were given.
// CombinedOutput supplies an os.Pipe and then waits for every writer to close
// it, so the wait does not end when runsc exits — it ends when the sandbox
// does, which is to say never. The call hangs forever holding the workspace's
// network policy half applied.
//
// Handing os/exec a real *os.File avoids it entirely: os/exec passes a file
// descriptor straight to the child and starts no copying goroutine, so nothing
// is left waiting on a descriptor the daemon still holds. This was observed as
// ApplyNetworkPolicy hanging indefinitely on a Linux host with runsc
// registered; see docs/engineering/verification-2026-09.md.
func (r execRuntime) run(ctx context.Context, args ...string) error {
	full := append([]string{"--root=" + r.root}, args...)
	log, err := os.CreateTemp("", "runsc-output-*")
	if err != nil {
		return fmt.Errorf("runsc %s: capture output: %w", args[0], err)
	}
	defer func() {
		_ = log.Close()
		_ = os.Remove(log.Name())
	}()
	cmd := exec.CommandContext(ctx, r.binary, full...)
	cmd.Stdout, cmd.Stderr = log, log
	runErr := cmd.Run()
	if runErr == nil {
		return nil
	}
	// Only read the output on the failure path, and bound it: a runsc failure
	// can be verbose and the message ends up in an error a caller may log.
	var out []byte
	if _, seekErr := log.Seek(0, io.SeekStart); seekErr == nil {
		out, _ = io.ReadAll(io.LimitReader(log, 8<<10))
	}
	return fmt.Errorf("runsc %s: %s: %w", args[0], strings.TrimSpace(string(out)), runErr)
}
