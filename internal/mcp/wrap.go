package mcp

import (
	"context"
	"fmt"
	"io"
	"net/url"
	"os"
	"os/exec"
	"sort"
	"strings"
)

// WrapOptions configure a third-party MCP subprocess. SecretEnv maps an
// environment variable to a Remount binding id; the real inherited value is
// replaced by ref:<binding> before the child starts.
type WrapOptions struct {
	Command   []string
	Broker    string
	SecretEnv map[string]string
	Env       []string
	Stdin     io.Reader
	Stdout    io.Writer
	Stderr    io.Writer
}

// Wrap runs a third-party MCP server without a shell. On cancellation the
// child is terminated and joined before Wrap returns.
func Wrap(ctx context.Context, opts WrapOptions) error {
	if len(opts.Command) == 0 {
		return fmt.Errorf("wrapped MCP command is required")
	}
	if strings.TrimSpace(opts.Broker) == "" {
		return fmt.Errorf("broker URL is required explicitly via --broker or REMOUNT_BROKER")
	}
	broker, err := url.Parse(opts.Broker)
	if err != nil || (broker.Scheme != "http" && broker.Scheme != "https") || broker.Host == "" || broker.User != nil || broker.RawQuery != "" || broker.Fragment != "" {
		return fmt.Errorf("broker URL is invalid")
	}
	if len(opts.Env) == 0 {
		opts.Env = os.Environ()
	}
	env, err := rewriteEnv(opts.Env, strings.TrimSuffix(opts.Broker, "/"), opts.SecretEnv)
	if err != nil {
		return err
	}
	cmd := exec.CommandContext(ctx, opts.Command[0], opts.Command[1:]...)
	cmd.Env, cmd.Stdin, cmd.Stdout, cmd.Stderr = env, opts.Stdin, opts.Stdout, opts.Stderr
	if err := cmd.Run(); err != nil {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		return fmt.Errorf("wrapped MCP server exited: %w", err)
	}
	return nil
}

func rewriteEnv(environ []string, broker string, secrets map[string]string) ([]string, error) {
	values := make(map[string]string, len(environ)+len(secrets))
	for _, item := range environ {
		key, value, ok := strings.Cut(item, "=")
		if ok {
			values[key] = value
		}
	}
	for key, binding := range secrets {
		if !validEnvName(key) || strings.TrimSpace(binding) == "" || strings.ContainsAny(binding, "\r\n") {
			return nil, fmt.Errorf("invalid --secret-env mapping for %q", key)
		}
		values[key] = "ref:" + binding
	}
	for key, value := range values {
		if !strings.HasSuffix(key, "_BASE_URL") || value == "" {
			continue
		}
		if strings.HasPrefix(value, broker+"/d/") {
			continue
		}
		u, err := url.Parse(value)
		if err != nil || u.Scheme != "https" || u.Host == "" || u.User != nil || u.RawQuery != "" || u.Fragment != "" {
			return nil, fmt.Errorf("%s must be an HTTPS URL to route through the broker", key)
		}
		values[key] = broker + "/d/" + u.Host + strings.TrimSuffix(u.EscapedPath(), "/")
	}
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	out := make([]string, 0, len(keys))
	for _, key := range keys {
		out = append(out, key+"="+values[key])
	}
	return out, nil
}

func validEnvName(value string) bool {
	if value == "" {
		return false
	}
	for i, r := range value {
		if (r >= 'A' && r <= 'Z') || r == '_' || (i > 0 && r >= '0' && r <= '9') {
			continue
		}
		return false
	}
	return true
}
