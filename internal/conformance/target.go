package conformance

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"
)

// TargetKind is one of the three things Plan B §12.2 says the runner must be
// able to point at.
type TargetKind string

const (
	// KindBuilt is a binary this run built and launched itself.
	KindBuilt TargetKind = "built-binary"
	// KindEndpoint is an already-running Remount endpoint.
	KindEndpoint TargetKind = "running-endpoint"
	// KindExternal is another implementation declaring the same protocol
	// version. It differs from KindEndpoint only in what the report claims:
	// the suite reaches it through the same public surfaces either way, which
	// is the point.
	KindExternal TargetKind = "external-implementation"
)

// BindingFixture describes an egress binding the target was configured with,
// so the binding checks know what to look for without reading the target's
// configuration.
type BindingFixture struct {
	ID           string
	Placeholder  string
	Secret       string
	Destinations []string
	// UnboundHost is a host the binding does NOT cover; sending the
	// placeholder there must be blocked.
	UnboundHost string
}

// Target is an implementation under test, reduced to the surfaces a
// conformance run may touch.
type Target struct {
	Name     string
	Kind     TargetKind
	Endpoint string
	Token    string
	// CLI is a command-line client for this implementation, or empty.
	CLI string
	// DeclaredProtocol is the frame version the target says it speaks. A
	// target declaring anything but ProtocolVersion is not judged by this
	// manifest at all.
	DeclaredProtocol string
	// Binding is the fixture backing the binding checks.
	Binding BindingFixture
	// Backend is the workspace backend the run should ask for.
	Backend string
	// Prereqs maps a prerequisite to the reason it is unavailable. A key that
	// is absent is available; a key present with a reason is not. Storing the
	// reason rather than a boolean is deliberate: §6.2 forbids an unexplained
	// unavailable row.
	Prereqs map[Prerequisite]string
	// Environment describes the host, for the evidence record.
	Environment Environment
	// stop tears down anything this process started.
	stop func() error
}

// Missing returns the reason a prerequisite is unavailable, or "" when it is
// satisfied.
func (t *Target) Missing(p Prerequisite) string {
	if t.Prereqs == nil {
		return "the target declares no prerequisites"
	}
	return t.Prereqs[p]
}

// Close releases whatever the target owns. It is safe to call more than once.
func (t *Target) Close() error {
	if t.stop == nil {
		return nil
	}
	stop := t.stop
	t.stop = nil
	return stop()
}

// defaultUnavailable is the honest starting position: everything a run cannot
// arrange by itself is unavailable and says why.
func defaultUnavailable() map[Prerequisite]string {
	return map[Prerequisite]string{
		PrereqBindings:           "no egress binding was configured for this target",
		PrereqCLI:                "no command-line client was named for this target",
		PrereqSessionEviction:    "the target's session retention bound was not reduced, so a run cannot overrun it inside its time budget",
		PrereqEventEviction:      "the target's event retention watermark was not reduced, so a run cannot drive the log past it",
		PrereqTranscriptEviction: "no harness produced transcript records, so the mirror bound cannot be overrun",
		PrereqNodeFault:          "the runner may not stop this target's nodes",
		PrereqEgressApproval:     "no approve-mode egress rule was configured for this target",
		PrereqRestart:            "the runner may not restart this target's control plane",
		PrereqQuiescedSnapshot:   "the target's backend was not confirmed able to quiesce a workspace",
		PrereqNotifier:           "no outbound notification destination was configured for this target",
	}
}

// AttachOptions points the runner at something already running.
type AttachOptions struct {
	Name             string
	Endpoint         string
	Token            string
	CLI              string
	Kind             TargetKind
	DeclaredProtocol string
	Backend          string
	Binding          BindingFixture
	// Satisfied lists prerequisites the operator asserts this target
	// provides. The runner takes the assertion at face value and the check
	// fails loudly if it was wrong, which is better than silently skipping.
	Satisfied []Prerequisite
	// Timeout bounds how long the target is given to answer health. Zero
	// waits 30 seconds, which suits an endpoint that is still starting.
	Timeout time.Duration
}

// Attach builds a Target for an endpoint this process did not start.
func Attach(ctx context.Context, opts AttachOptions) (*Target, error) {
	if opts.Endpoint == "" {
		return nil, errors.New("conformance: attach needs an endpoint")
	}
	kind := opts.Kind
	if kind == "" {
		kind = KindEndpoint
	}
	declared := opts.DeclaredProtocol
	if declared == "" {
		declared = ProtocolVersion
	}
	t := &Target{
		Name:             orDefault(opts.Name, opts.Endpoint),
		Kind:             kind,
		Endpoint:         strings.TrimSuffix(opts.Endpoint, "/"),
		Token:            opts.Token,
		CLI:              opts.CLI,
		DeclaredProtocol: declared,
		Backend:          orDefault(opts.Backend, "process"),
		Binding:          opts.Binding,
		Prereqs:          defaultUnavailable(),
		Environment:      HostEnvironment(),
	}
	if opts.CLI != "" {
		delete(t.Prereqs, PrereqCLI)
	}
	if opts.Binding.ID != "" {
		delete(t.Prereqs, PrereqBindings)
	}
	for _, p := range opts.Satisfied {
		delete(t.Prereqs, p)
	}
	within := opts.Timeout
	if within == 0 {
		within = 30 * time.Second
	}
	if err := waitHealthy(ctx, t.Endpoint, t.Token, within); err != nil {
		return nil, err
	}
	return t, nil
}

// LaunchOptions builds and starts a local binary, which is the artifact
// evidence layer of B20: the thing under test is a binary produced from this
// commit, not a library linked into the test process.
type LaunchOptions struct {
	// ModuleDir is the Go module to build from. Empty uses BinaryPath as-is.
	ModuleDir string
	// Package is the main package to build; defaults to ./cmd/remount.
	Package string
	// BinaryPath is where to put (or find) the binary.
	BinaryPath string
	// DataDir is the target's data directory; a temporary one is made when
	// empty.
	DataDir string
	// Addr is the listen address; a free loopback port is chosen when empty.
	Addr string
	// Backend is the workspace backend to serve.
	Backend string
	// Stdout receives the target's log, for a failure report.
	Stdout io.Writer
}

// conformanceBinding is the fixture every launched target is given. The
// secret is a synthetic value that exists only inside this run; it is a
// canary, and a check that finds it in a workspace or an event has found a
// real leak. It is deliberately not a plausible provider key.
var conformanceBinding = BindingFixture{
	ID:           "b_conformance",
	Placeholder:  "remount-placeholder-b_conformance",
	Secret:       "conformance-canary-2b7f4a1c9e5d0000",
	Destinations: []string{"bound.conformance.invalid"},
	UnboundHost:  "unbound.conformance.invalid",
}

// Launch builds the binary if asked, starts it in standalone mode with a
// conformance binding, and waits for it to serve.
func Launch(ctx context.Context, opts LaunchOptions) (*Target, error) {
	bin := opts.BinaryPath
	if opts.ModuleDir != "" {
		pkg := orDefault(opts.Package, "./cmd/remount")
		if bin == "" {
			dir, err := os.MkdirTemp("", "conformance-bin-")
			if err != nil {
				return nil, err
			}
			bin = filepath.Join(dir, "remount")
		}
		build := exec.CommandContext(ctx, "go", "build", "-o", bin, pkg)
		build.Dir = opts.ModuleDir
		build.Env = append(os.Environ(), "CGO_ENABLED=0")
		if out, err := build.CombinedOutput(); err != nil {
			return nil, fmt.Errorf("conformance: build %s: %v: %s", pkg, err, out)
		}
	}
	if bin == "" {
		return nil, errors.New("conformance: launch needs a binary path or a module to build")
	}
	data := opts.DataDir
	if data == "" {
		dir, err := os.MkdirTemp("", "conformance-data-")
		if err != nil {
			return nil, err
		}
		data = dir
	}
	addr := opts.Addr
	if addr == "" {
		var err error
		if addr, err = freeLoopbackAddr(); err != nil {
			return nil, err
		}
	}
	bindings := filepath.Join(data, "conformance-bindings.json")
	body, err := json.MarshalIndent([]map[string]any{{
		"id":           conformanceBinding.ID,
		"secret":       conformanceBinding.Secret,
		"destinations": conformanceBinding.Destinations,
		"placeholder":  conformanceBinding.Placeholder,
	}}, "", "  ")
	if err != nil {
		return nil, err
	}
	if err := os.WriteFile(bindings, body, 0o600); err != nil {
		return nil, err
	}

	backend := orDefault(opts.Backend, "process")
	cmd := exec.Command(bin, "standalone",
		"--listen", addr, "--data", data,
		"--bindings", bindings, "--backend", backend)
	if opts.Stdout != nil {
		cmd.Stdout = opts.Stdout
		cmd.Stderr = opts.Stdout
	}
	if err := cmd.Start(); err != nil {
		return nil, fmt.Errorf("conformance: start %s: %w", bin, err)
	}
	endpoint := "http://" + addr
	stop := func() error {
		if cmd.Process == nil {
			return nil
		}
		_ = cmd.Process.Kill()
		_, _ = cmd.Process.Wait()
		return nil
	}
	if err := waitHealthy(ctx, endpoint, "", 60*time.Second); err != nil {
		_ = stop()
		return nil, err
	}
	t := &Target{
		Name:             "remount standalone (" + filepath.Base(bin) + ")",
		Kind:             KindBuilt,
		Endpoint:         endpoint,
		CLI:              bin,
		DeclaredProtocol: ProtocolVersion,
		Backend:          backend,
		Binding:          conformanceBinding,
		Prereqs:          defaultUnavailable(),
		Environment:      HostEnvironment(),
		stop:             stop,
	}
	t.Environment.Backend = backend
	delete(t.Prereqs, PrereqCLI)
	delete(t.Prereqs, PrereqBindings)
	// A launched standalone is a local, unisolated deployment whose backend
	// drains node-mediated mutations and joins its own sessions, so an
	// authoritative checkpoint is reachable. §7 is explicit that this makes
	// the checkpoint quiesced, not that it makes the backend production-
	// qualified, and the manifest asserts only the former.
	delete(t.Prereqs, PrereqQuiescedSnapshot)
	return t, nil
}

// Health is the /healthz document every implementation publishes.
type Health struct {
	OK            bool   `json:"ok"`
	Peers         int    `json:"peers"`
	Serving       bool   `json:"serving"`
	SecurityMode  string `json:"security_mode"`
	SecurityReady bool   `json:"security_ready"`
	Recovery      string `json:"recovery"`
}

func waitHealthy(ctx context.Context, endpoint, token string, within time.Duration) error {
	deadline := time.Now().Add(within)
	var last error
	for time.Now().Before(deadline) {
		h, err := fetchHealth(ctx, endpoint, token)
		switch {
		case err != nil:
			last = err
		case !h.OK || !h.Serving:
			last = fmt.Errorf("conformance: %s answered health ok=%v serving=%v", endpoint, h.OK, h.Serving)
		default:
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(200 * time.Millisecond):
		}
	}
	return fmt.Errorf("conformance: %s never became healthy: %w", endpoint, last)
}

func fetchHealth(ctx context.Context, endpoint, token string) (Health, error) {
	var h Health
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint+"/healthz", nil)
	if err != nil {
		return h, err
	}
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		return h, err
	}
	defer func() { _ = res.Body.Close() }()
	body, err := io.ReadAll(io.LimitReader(res.Body, 1<<20))
	if err != nil {
		return h, err
	}
	if err := json.Unmarshal(body, &h); err != nil {
		return h, fmt.Errorf("conformance: /healthz is not JSON: %w", err)
	}
	return h, nil
}

func freeLoopbackAddr() (string, error) {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return "", err
	}
	addr := l.Addr().String()
	if err := l.Close(); err != nil {
		return "", err
	}
	return addr, nil
}

func orDefault(v, def string) string {
	if v == "" {
		return def
	}
	return v
}
