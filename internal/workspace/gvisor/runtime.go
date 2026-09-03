package gvisor

import (
	"context"
	"fmt"
	"os/exec"
	"strings"
	"time"
)

type runtimeClient interface {
	probe(context.Context) error
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
		return nil
	}
	return err
}
func (r execRuntime) run(ctx context.Context, args ...string) error {
	full := append([]string{"--root=" + r.root}, args...)
	out, err := exec.CommandContext(ctx, r.binary, full...).CombinedOutput()
	if err != nil {
		return fmt.Errorf("runsc %s: %s: %w", args[0], strings.TrimSpace(string(out)), err)
	}
	return nil
}
