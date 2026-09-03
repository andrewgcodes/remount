package providerutil

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"strings"
)

// Command describes a secret-safe helper invocation. Request-specific secret
// material belongs in Stdin or Env, never Args.
type Command struct {
	Executable string
	Args       []string
	Stdin      []byte
	Env        map[string]string
	MaxOutput  int
}

// Runner executes an external provider or remote-host helper.
type Runner interface {
	Run(context.Context, Command) ([]byte, error)
}

// OSRunner runs helpers without a shell and returns bounded stdout. Stderr and
// command output are never incorporated into errors because they may echo a
// bootstrap credential.
type OSRunner struct{}

// Run implements Runner.
func (OSRunner) Run(ctx context.Context, command Command) ([]byte, error) {
	if command.Executable == "" {
		return nil, errors.New("provision: helper executable is required")
	}
	limit := command.MaxOutput
	if limit == 0 {
		limit = defaultBodyLimit
	}
	if limit < 1 || limit > maximumBodyLimit {
		return nil, errors.New("provision: helper output limit is invalid")
	}
	cmd := exec.CommandContext(ctx, command.Executable, command.Args...)
	cmd.Stdin = bytes.NewReader(command.Stdin)
	provided := make(map[string]struct{}, len(command.Env))
	for key, value := range command.Env {
		if key == "" || strings.ContainsAny(key, "=\x00\r\n") || strings.ContainsAny(value, "\x00") {
			return nil, errors.New("provision: helper environment is invalid")
		}
		provided[key] = struct{}{}
	}
	for _, item := range os.Environ() {
		key, _, _ := strings.Cut(item, "=")
		if _, replaced := provided[key]; !replaced {
			cmd.Env = append(cmd.Env, item)
		}
	}
	for key, value := range command.Env {
		cmd.Env = append(cmd.Env, key+"="+value)
	}
	stdout := &boundedBuffer{remaining: limit}
	cmd.Stdout = stdout
	cmd.Stderr = io.Discard
	if err := cmd.Run(); err != nil {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return nil, ctxErr
		}
		if stdout.overflow {
			return nil, errors.New("provision: helper output exceeded limit")
		}
		return nil, fmt.Errorf("provision: helper exited unsuccessfully")
	}
	if stdout.overflow {
		return nil, errors.New("provision: helper output exceeded limit")
	}
	return stdout.Bytes(), nil
}

type boundedBuffer struct {
	buffer    bytes.Buffer
	remaining int
	overflow  bool
}

func (b *boundedBuffer) Write(payload []byte) (int, error) {
	original := len(payload)
	if len(payload) > b.remaining {
		payload = payload[:b.remaining]
		b.overflow = true
	}
	_, _ = b.buffer.Write(payload)
	b.remaining -= len(payload)
	return original, nil
}

func (b *boundedBuffer) Bytes() []byte { return b.buffer.Bytes() }
