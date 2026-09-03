// Command remount is the single binary: server, node, and client CLI.
//
//	remount server   --listen :7443 --data ./data --token T [--bindings bindings.json]
//	remount up       --server https://host:7443 --token T [--label k=v] [--backend process|docker]
//	remount ws       create|ls|get|destroy|move|sleep|wake|snapshot
//	remount exec     WS -- cmd args...
//	remount sh       WS            (interactive pty)
//	remount attach   WS SESSION [--from SEQ]
//	remount fs       read|write|ls|stat|rm|mv|search WS ...
//	remount port     WS PORT [--local ADDR]
//	remount fleet    quarantine|ls|get
//	remount base     ls|rm
//	remount nodes / remount events [--follow] / remount timers
//	remount standalone [--data DIR]   (server + node in one process, no token)
package main

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"

	"golang.org/x/term"

	"remount.dev/remount/internal/client"
	"remount.dev/remount/internal/control"
	"remount.dev/remount/internal/localfs"
	"remount.dev/remount/internal/node"
	"remount.dev/remount/internal/proto"
	"remount.dev/remount/internal/server"
	"remount.dev/remount/internal/transport"
	"remount.dev/remount/internal/workspace"
)

var version = "dev"

func main() {
	if len(os.Args) < 2 {
		usage()
		os.Exit(2)
	}
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	err := run(ctx, os.Args[1:])
	if err != nil {
		var ee exitError
		if errors.As(err, &ee) {
			os.Exit(int(ee))
		}
		fmt.Fprintln(os.Stderr, "remount:", err)
		os.Exit(1)
	}
}

// run dispatches one invocation. Global flags (--server, --token, --json) may
// precede the command as well as follow it; they are moved after the
// (sub)command name so every FlagSet sees them.
func run(ctx context.Context, argv []string) error {
	globals, argv := splitGlobalFlags(argv)
	if len(argv) == 0 {
		usage()
		return exitError(2)
	}
	cmd, args := argv[0], argv[1:]
	switch cmd {
	case "ws", "fs", "fleet":
		if len(args) > 0 && !isFlag(args[0]) {
			args = append(append([]string{args[0]}, globals...), args[1:]...)
		} else {
			args = append(globals, args...)
		}
	case "server", "standalone", "version", "help", "-h", "--help":
		if len(globals) > 0 {
			return fmt.Errorf("%s does not accept %s before the command", cmd, globals[0])
		}
	default:
		args = append(globals, args...)
	}
	switch cmd {
	case "server":
		return cmdServer(ctx, args)
	case "up":
		return cmdUp(ctx, args)
	case "standalone":
		return cmdStandalone(ctx, args)
	case "ws":
		return cmdWS(ctx, args)
	case "exec":
		return cmdExec(ctx, args, false)
	case "sh":
		return cmdExec(ctx, args, true)
	case "attach":
		return cmdAttach(ctx, args)
	case "fs":
		return cmdFS(ctx, args)
	case "push":
		return cmdPush(ctx, args)
	case "pull":
		return cmdPull(ctx, args)
	case "port":
		return cmdPort(ctx, args)
	case "fleet":
		return cmdFleet(ctx, args)
	case "base":
		return cmdBase(ctx, args)
	case "status":
		return cmdStatus(ctx, args)
	case "inspect":
		return cmdInspect(ctx, args)
	case "doctor":
		return cmdDoctor(ctx, args)
	case "metrics":
		return cmdMetrics(ctx, args)
	case "nodes":
		return cmdNodes(ctx, args)
	case "events":
		return cmdEvents(ctx, args)
	case "timers":
		return cmdTimers(ctx, args)
	case "version":
		fmt.Println("remount", version)
		return nil
	case "help", "-h", "--help":
		usage()
		return nil
	}
	usage()
	return exitError(2)
}

// globalFlags are accepted before the command name.
var globalFlags = map[string]bool{"server": true, "token": true, "json": false}

// splitGlobalFlags peels leading global flags off argv. The bool records
// whether the flag takes a value.
func splitGlobalFlags(argv []string) (globals, rest []string) {
	for len(argv) > 0 && isFlag(argv[0]) {
		name, _, hasValue := strings.Cut(strings.TrimLeft(argv[0], "-"), "=")
		takesValue, known := globalFlags[name]
		if !known {
			break
		}
		globals = append(globals, argv[0])
		argv = argv[1:]
		if takesValue && !hasValue && len(argv) > 0 {
			globals = append(globals, argv[0])
			argv = argv[1:]
		}
	}
	return globals, argv
}

func isFlag(a string) bool { return len(a) > 1 && a[0] == '-' }

func isHelp(a string) bool { return a == "help" || a == "-h" || a == "--help" }

type exitError int

func (e exitError) Error() string { return "exit " + strconv.Itoa(int(e)) }

func usage() {
	fmt.Fprint(os.Stderr, `remount — any agent, any machine, never holding the keys, never losing its place

  remount server      run the control plane + relay + artifact store
  remount up          enroll this machine as a node (outbound only)
  remount standalone  server + node in one process (try it on a laptop)

  remount ws create [--name N] [--dir PATH | --base NAME] [--backend B] [--image IMG] [--security PROFILE] [--egress-rule JSON] [--binding ID]
  remount ws ls | get WS | destroy WS | move WS [--node ID] [--cpu N] | sleep WS (--after 1h | --on EVENT) | wake WS | snapshot WS [--authoritative] [--as-base NAME] | acl WS [--reader P]... [--writer P]...
  remount exec WS -- cmd args...      run a command (stdout/stderr/exit streamed)
  remount sh WS [cmd]                 interactive shell (pty)
  remount attach WS SESSION [--from N]
  remount fs read|write|ls|stat|rm|mv|search|edit WS ...
  remount push WS [--dir .] [--include-git=false] [--exclude GLOB]...   upload a local directory over the workspace
  remount pull WS [--dir .] [--force]                                   snapshot the workspace and write what differs locally
  remount port WS PORT [--local 127.0.0.1:PORT]
  remount fleet quarantine --action freeze (--all | SELECTORS...) | ls | get OPERATION
  remount base ls | rm NAME                                              named snapshots for ws create --base (pinned until rm)
  remount nodes | events [--follow] [--ws WS] | timers

Inspection, at three depths. All take --json.
  remount status [--watch 5s]         the fleet in one screen
  remount inspect WS                  one workspace, down to session log positions
  remount doctor                      every check for damage, loss or disagreement
  remount metrics [--node ID]         raw counters

Common flags (or env), before or after the command: --server REMOUNT_SERVER (default http://127.0.0.1:7443) --token REMOUNT_TOKEN --json
`)
}

// ---------------------------------------------------------------------------
// common
// ---------------------------------------------------------------------------

type common struct {
	server string
	token  string
	json   bool
}

func (c *common) flags(fs *flag.FlagSet) {
	fs.StringVar(&c.server, "server", envOr("REMOUNT_SERVER", "http://127.0.0.1:7443"), "server URL")
	fs.StringVar(&c.token, "token", os.Getenv("REMOUNT_TOKEN"), "bearer token")
	fs.BoolVar(&c.json, "json", false, "JSON output")
}

// parse reorders args so flags may appear after positionals, the way people
// actually type them ("ws move WS --node N"). Go's flag package stops at the
// first non-flag argument, which silently drops those flags.
func parse(fs *flag.FlagSet, args []string) {
	var flags, positional []string
	for i := 0; i < len(args); i++ {
		a := args[i]
		if a == "--" {
			// Everything from here is positional; keep the marker so the
			// flag package stops too.
			positional = append(positional, args[i:]...)
			break
		}
		if len(a) > 1 && a[0] == '-' {
			flags = append(flags, a)
			name := strings.TrimLeft(a, "-")
			if strings.Contains(name, "=") {
				continue
			}
			f := fs.Lookup(name)
			if f == nil {
				continue
			}
			if b, ok := f.Value.(interface{ IsBoolFlag() bool }); ok && b.IsBoolFlag() {
				continue
			}
			if i+1 < len(args) {
				i++
				flags = append(flags, args[i])
			}
			continue
		}
		positional = append(positional, a)
	}
	_ = fs.Parse(append(flags, positional...))
}

// arity enforces the positional count of a subcommand after parse. Extra
// positionals are an error rather than silently ignored: `ws create
// python:3.12` was accepted and created a workspace with the default image.
func arity(fs *flag.FlagSet, min, max int, usage string) error {
	args := fs.Args()
	if len(args) < min {
		return fmt.Errorf("missing argument: %s", usage)
	}
	if max >= 0 && len(args) > max {
		extra := args[max]
		if looksLikeImage(extra) {
			return fmt.Errorf("unexpected argument %q: did you mean --image %s? usage: %s", extra, extra, usage)
		}
		return fmt.Errorf("unexpected argument %q: %s", extra, usage)
	}
	return nil
}

// looksLikeImage recognises the shapes people type for container images
// (name:tag, registry/name, name@sha256:...).
func looksLikeImage(s string) bool {
	if s == "" || strings.ContainsAny(s, " \t") || strings.HasPrefix(s, "ws_") {
		return false
	}
	return strings.Contains(s, ":") || strings.Contains(s, "/") || strings.Contains(s, "@sha256")
}

// mutationResult is the --json shape of a mutating command that has no
// richer object to print.
type mutationResult struct {
	OK        bool   `json:"ok"`
	Operation string `json:"operation"`
	Workspace string `json:"workspace,omitempty"`
	Path      string `json:"path,omitempty"`
	To        string `json:"to,omitempty"`
	Count     *int   `json:"count,omitempty"`
}

// absFlagPath resolves a directory flag against the current working directory.
// Nodes hand the path to backends (Docker bind mounts need absolute paths) and
// long-running processes may change directory, so relative data roots are a
// latent bug rather than a convenience.
func absFlagPath(flagName, value string) (string, error) {
	if strings.TrimSpace(value) == "" {
		return "", fmt.Errorf("--%s must not be empty", flagName)
	}
	abs, err := filepath.Abs(value)
	if err != nil {
		return "", fmt.Errorf("--%s %q: %w", flagName, value, err)
	}
	return abs, nil
}

func envOr(k, d string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return d
}

func wsURL(server string) string {
	u := strings.TrimSuffix(server, "/")
	u = strings.Replace(u, "http://", "ws://", 1)
	u = strings.Replace(u, "https://", "wss://", 1)
	return u + "/v1/link"
}

func (c *common) dialer() transport.Dialer {
	url := wsURL(c.server)
	return transport.DialFunc(func(ctx context.Context) (transport.Conn, error) {
		return transport.DialWS(ctx, url, nil)
	})
}

func (c *common) client() *client.Client {
	principal := envOr("REMOUNT_PRINCIPAL", envOr("USER", "cli"))
	return client.New(client.Options{
		Dialer: c.dialer(), Token: c.token, Principal: principal,
		ArtifactURL: strings.TrimSuffix(c.server, "/") + "/v1/artifacts",
	})
}

func printJSON(v any) {
	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	_ = enc.Encode(v)
}

type kvFlag map[string]string

func (k kvFlag) String() string { return fmt.Sprint(map[string]string(k)) }
func (k kvFlag) Set(s string) error {
	key, val, ok := strings.Cut(s, "=")
	if !ok {
		return fmt.Errorf("expected key=value, got %q", s)
	}
	k[key] = val
	return nil
}

type listFlag []string

func (l *listFlag) String() string     { return strings.Join(*l, ",") }
func (l *listFlag) Set(s string) error { *l = append(*l, s); return nil }

type egressRuleFlag []proto.EgressRule

func (rules *egressRuleFlag) String() string {
	raw, _ := json.Marshal([]proto.EgressRule(*rules))
	return string(raw)
}

// Set accepts either one JSON rule object or @path to a file containing one.
// Files are useful for rules with several methods/path prefixes while keeping
// shell quoting out of production runbooks.
func (rules *egressRuleFlag) Set(value string) error {
	if strings.HasPrefix(value, "@") {
		path := strings.TrimPrefix(value, "@")
		if path == "" {
			return errors.New("--egress-rule @path is empty")
		}
		raw, err := os.ReadFile(path)
		if err != nil {
			return fmt.Errorf("read egress rule %s: %w", path, err)
		}
		value = string(raw)
	}
	decoder := json.NewDecoder(strings.NewReader(value))
	decoder.DisallowUnknownFields()
	var rule proto.EgressRule
	if err := decoder.Decode(&rule); err != nil {
		return fmt.Errorf("decode egress rule: %w", err)
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		if err == nil {
			return errors.New("decode egress rule: multiple JSON values")
		}
		return fmt.Errorf("decode egress rule: %w", err)
	}
	*rules = append(*rules, rule)
	return nil
}

// ---------------------------------------------------------------------------
// server / up / standalone
// ---------------------------------------------------------------------------

func loadBindings(path string) ([]control.Binding, error) {
	if path == "" {
		return nil, nil
	}
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var out []control.Binding
	if err := json.Unmarshal(b, &out); err != nil {
		return nil, fmt.Errorf("%s: %w", path, err)
	}
	// Secrets may be given as $ENV references so the file itself stays clean.
	for i := range out {
		if strings.HasPrefix(out[i].Secret, "$") {
			out[i].Secret = os.Getenv(strings.TrimPrefix(out[i].Secret, "$"))
		}
	}
	return out, nil
}

func cmdServer(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("server", flag.ExitOnError)
	listen := fs.String("listen", envOr("REMOUNT_LISTEN", "127.0.0.1:7443"), "listen address")
	data := fs.String("data", envOr("REMOUNT_DATA", "./remount-data"), "data directory")
	token := fs.String("token", os.Getenv("REMOUNT_TOKEN"), "shared bearer token (required unless --insecure)")
	insecure := fs.Bool("insecure", false, "allow an empty token")
	bindings := fs.String("bindings", "", "bindings JSON file")
	lease := fs.Int64("lease", 30, "claim lease seconds")
	artifactBytes := fs.Int64("artifact-object-bytes", 8<<30, "maximum compressed bytes per artifact")
	artifactStoreBytes := fs.Int64("artifact-store-bytes", 64<<30, "maximum retained and staging artifact bytes")
	artifactObjects := fs.Int("artifact-store-objects", 100_000, "maximum retained and staging artifact objects")
	artifactGCInterval := fs.Duration("artifact-gc-interval", 10*time.Minute, "reference-aware artifact GC interval")
	artifactGrace := fs.Duration("artifact-grace", 24*time.Hour, "minimum age before unreferenced artifact collection")
	eventRetention := fs.Duration("event-retention", 30*24*time.Hour, "retained canonical event history")
	eventGCInterval := fs.Duration("event-gc-interval", 10*time.Minute, "event-retention pruning interval")
	maxEvents := fs.Int("max-events", 1_000_000, "maximum retained canonical event rows")
	recordRetention := fs.Duration("control-record-retention", 30*24*time.Hour, "idempotency result and fired-timer retention")
	recordGCInterval := fs.Duration("control-record-gc-interval", 10*time.Minute, "control-record pruning interval")
	maxTenantWorkspaces := fs.Int("max-tenant-workspaces", 1000, "maximum non-destroyed workspaces per tenant")
	maxSubjectWorkspaces := fs.Int("max-subject-workspaces", 100, "maximum non-destroyed workspaces per subject")
	maxMutationRecords := fs.Int("max-mutation-records", 100_000, "maximum retained control-plane idempotency results")
	maxTimers := fs.Int("max-timers", 100_000, "maximum retained durable timers")
	maxWorkspaceTimers := fs.Int("max-workspace-timers", 128, "maximum retained durable timers per workspace")
	maxConcurrentRequests := fs.Int("max-concurrent-requests", 128, "maximum concurrent control-plane requests")
	mode := fs.String("mode", envOr("REMOUNT_SECURITY_MODE", server.ModeStandalone), "security mode: standalone, production-single-tenant, production-multi-tenant")
	parse(fs, args)
	if *token == "" && !*insecure {
		return errors.New("--token is required (or --insecure for local experiments)")
	}
	dataDir, err := absFlagPath("data", *data)
	if err != nil {
		return err
	}
	*data = dataDir
	if err := os.MkdirAll(*data, 0o700); err != nil {
		return fmt.Errorf("--data %s: %w", *data, err)
	}
	b, err := loadBindings(*bindings)
	if err != nil {
		return err
	}
	srv, err := server.New(server.Options{
		DataDir: *data, Token: *token, Bindings: b, LeaseSec: *lease, Logger: slog.Default(), Mode: *mode,
		MaxArtifactBytes: *artifactBytes, MaxArtifactStoreBytes: *artifactStoreBytes, MaxArtifactObjects: *artifactObjects,
		ArtifactGCInterval: *artifactGCInterval, ArtifactGracePeriod: *artifactGrace,
		EventRetention: *eventRetention, EventGCInterval: *eventGCInterval, MaxEvents: *maxEvents,
		RecordRetention: *recordRetention, RecordGCInterval: *recordGCInterval,
		MaxWorkspacesPerTenant: *maxTenantWorkspaces, MaxWorkspacesPerSubject: *maxSubjectWorkspaces,
		MaxMutationRecords: *maxMutationRecords, MaxTimers: *maxTimers, MaxTimersPerWorkspace: *maxWorkspaceTimers,
		MaxConcurrentRequests: *maxConcurrentRequests,
	})
	if err != nil {
		return err
	}
	defer srv.Close()
	slog.Info("remount server listening", "addr", *listen, "data", *data, "bindings", len(b), "security_mode", *mode)
	return srv.Serve(ctx, *listen)
}

func cmdUp(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("up", flag.ExitOnError)
	var c common
	c.flags(fs)
	data := fs.String("data", envOr("REMOUNT_NODE_DATA", defaultNodeData()), "node data directory")
	labels := kvFlag{}
	fs.Var(labels, "label", "node label k=v (repeatable)")
	backends := fs.String("backend", "process", "comma-separated backends: process,docker")
	image := fs.String("image", workspace.DefaultImage(version), "default docker image (ubuntu:24.04 for a plain distro)")
	artifactBytes := fs.Int64("artifact-object-bytes", 8<<30, "maximum compressed bytes per cached artifact")
	artifactStoreBytes := fs.Int64("artifact-store-bytes", 32<<30, "maximum node artifact-cache bytes")
	artifactObjects := fs.Int("artifact-store-objects", 50_000, "maximum node artifact-cache objects")
	artifactRetention := fs.Duration("artifact-retention", 24*time.Hour, "minimum age of unreferenced node cache artifacts")
	artifactGCInterval := fs.Duration("artifact-gc-interval", 10*time.Minute, "node artifact-cache collection interval")
	connectorCacheBytes := fs.Int64("connector-cache-bytes", 16<<30, "maximum managed-connector cache bytes")
	connectorWorkspaceBytes := fs.Int64("connector-workspace-bytes", 2<<30, "maximum connector bytes pinned per workspace")
	connectorObjectBytes := fs.Int64("connector-object-bytes", 512<<20, "maximum bytes in one connector response")
	connectorObjects := fs.Int64("connector-cache-objects", 100_000, "maximum connector cache objects")
	connectorWorkspaceObjects := fs.Int64("connector-workspace-objects", 4_096, "maximum connector objects pinned per workspace")
	maxSessions := fs.Int("max-sessions", 1024, "maximum retained sessions on this node")
	maxActiveSessions := fs.Int("max-active-sessions", 256, "maximum live sessions on this node")
	maxWorkspaceSessions := fs.Int("max-workspace-sessions", 64, "maximum retained sessions per workspace")
	maxPrincipalSessions := fs.Int("max-principal-sessions", 128, "maximum retained sessions per principal")
	sessionMemoryBytes := fs.Int("session-memory-bytes", 2<<20, "maximum in-memory log bytes per session")
	sessionSpillBytes := fs.Int64("session-spill-bytes", 128<<20, "maximum spill bytes per session")
	sessionMaxChunkBytes := fs.Int("session-chunk-bytes", 32<<10, "maximum bytes in one session chunk")
	sessionMemoryChunks := fs.Int("session-memory-chunks", 16_384, "maximum in-memory chunks per session")
	maxConcurrentRequests := fs.Int("max-concurrent-requests", 128, "maximum concurrent node requests")
	mutationRetention := fs.Duration("mutation-retention", 30*24*time.Hour, "node idempotency-result replay window")
	maxMutationRecords := fs.Int("max-mutation-records", 10_000, "maximum retained node idempotency records")
	maxConcurrentSnapshots := fs.Int("max-concurrent-snapshots", 4, "maximum concurrent node snapshots")
	snapshotMinInterval := fs.Duration("snapshot-min-interval", time.Second, "minimum interval between explicit snapshots of one workspace")
	var allow, allowPrivate listFlag
	fs.Var(&allow, "allow", "host pattern workspaces may reach without a credential (repeatable)")
	fs.Var(&allowPrivate, "allow-private", "host pattern allowed to resolve to a private address (repeatable)")
	parse(fs, args)
	dataDir, err := absFlagPath("data", *data)
	if err != nil {
		return err
	}
	*data = dataDir
	n, err := buildNode(*data, c, labels, *backends, *image, allow, allowPrivate, nodeResourceOptions{
		artifactBytes: *artifactBytes, artifactStoreBytes: *artifactStoreBytes, artifactObjects: *artifactObjects,
		artifactRetention: *artifactRetention, artifactGCInterval: *artifactGCInterval,
		connectorCacheBytes: *connectorCacheBytes, connectorWorkspaceBytes: *connectorWorkspaceBytes,
		connectorObjectBytes: *connectorObjectBytes, connectorObjects: *connectorObjects,
		connectorWorkspaceObjects: *connectorWorkspaceObjects,
		maxSessions:               *maxSessions, maxActiveSessions: *maxActiveSessions, maxWorkspaceSessions: *maxWorkspaceSessions,
		maxPrincipalSessions: *maxPrincipalSessions, sessionMemoryBytes: *sessionMemoryBytes,
		sessionSpillBytes: *sessionSpillBytes, sessionMaxChunkBytes: *sessionMaxChunkBytes,
		sessionMemoryChunks: *sessionMemoryChunks, maxConcurrentRequests: *maxConcurrentRequests,
		mutationRetention: *mutationRetention, maxMutationRecords: *maxMutationRecords,
		maxConcurrentSnapshots: *maxConcurrentSnapshots, snapshotMinInterval: *snapshotMinInterval,
	})
	if err != nil {
		return err
	}
	slog.Info("remount node starting", "id", n.ID(), "server", c.server, "data", *data)
	return n.Run(ctx)
}

func defaultNodeData() string {
	if h, err := os.UserHomeDir(); err == nil {
		return filepath.Join(h, ".remount", "node")
	}
	return "./remount-node"
}

type nodeResourceOptions struct {
	artifactBytes             int64
	artifactStoreBytes        int64
	artifactObjects           int
	artifactRetention         time.Duration
	artifactGCInterval        time.Duration
	connectorCacheBytes       int64
	connectorWorkspaceBytes   int64
	connectorObjectBytes      int64
	connectorObjects          int64
	connectorWorkspaceObjects int64
	maxSessions               int
	maxActiveSessions         int
	maxWorkspaceSessions      int
	maxPrincipalSessions      int
	sessionMemoryBytes        int
	sessionSpillBytes         int64
	sessionMaxChunkBytes      int
	sessionMemoryChunks       int
	maxConcurrentRequests     int
	mutationRetention         time.Duration
	maxMutationRecords        int
	maxConcurrentSnapshots    int
	snapshotMinInterval       time.Duration
}

func buildNode(data string, c common, labels map[string]string, backends, image string, allow, allowPrivate []string, resources nodeResourceOptions) (*node.Node, error) {
	if !filepath.IsAbs(data) {
		return nil, fmt.Errorf("node data directory %q must be absolute", data)
	}
	var list []workspace.Backend
	for _, b := range strings.Split(backends, ",") {
		switch strings.TrimSpace(b) {
		case "process":
			pb, err := workspace.NewProcess(filepath.Join(data, "ws"))
			if err != nil {
				return nil, err
			}
			list = append(list, pb)
		case "docker":
			d, err := workspace.NewDocker(filepath.Join(data, "ws"), image)
			if err != nil {
				return nil, err
			}
			list = append(list, d)
		case "":
		default:
			return nil, fmt.Errorf("unknown backend %q", b)
		}
	}
	if len(list) == 0 {
		return nil, errors.New("no backends")
	}
	return node.New(node.Options{
		DataDir: data, Dialer: c.dialer(), Token: c.token, Labels: labels, Backends: workspace.NewRegistry(list...),
		ArtifactURL: strings.TrimSuffix(c.server, "/") + "/v1/artifacts", Logger: slog.Default(),
		MaxArtifactBytes: resources.artifactBytes, MaxArtifactStoreBytes: resources.artifactStoreBytes,
		MaxArtifactObjects: resources.artifactObjects,
		ArtifactRetention:  resources.artifactRetention, ArtifactGCInterval: resources.artifactGCInterval,
		MaxConnectorCacheBytes:       resources.connectorCacheBytes,
		MaxConnectorWorkspaceBytes:   resources.connectorWorkspaceBytes,
		MaxConnectorObjectBytes:      resources.connectorObjectBytes,
		MaxConnectorObjects:          resources.connectorObjects,
		MaxConnectorWorkspaceObjects: resources.connectorWorkspaceObjects,
		MaxSessions:                  resources.maxSessions, MaxActiveSessions: resources.maxActiveSessions,
		MaxSessionsPerWorkspace: resources.maxWorkspaceSessions,
		MaxSessionsPerPrincipal: resources.maxPrincipalSessions,
		SessionMemoryBytes:      resources.sessionMemoryBytes, SessionSpillBytes: resources.sessionSpillBytes,
		SessionMaxChunkBytes: resources.sessionMaxChunkBytes, SessionMaxMemoryChunks: resources.sessionMemoryChunks,
		MaxConcurrentRequests: resources.maxConcurrentRequests,
		MutationRetention:     resources.mutationRetention, MaxMutationRecords: resources.maxMutationRecords,
		MaxConcurrentSnapshots: resources.maxConcurrentSnapshots, SnapshotMinInterval: resources.snapshotMinInterval,
		Allow: allow, AllowPrivate: allowPrivate, Version: version,
	})
}

func cmdStandalone(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("standalone", flag.ExitOnError)
	listen := fs.String("listen", "127.0.0.1:7443", "listen address")
	data := fs.String("data", envOr("REMOUNT_DATA", "./remount-data"), "data directory")
	bindings := fs.String("bindings", "", "bindings JSON file")
	backends := fs.String("backend", "process", "backends")
	var allow listFlag
	fs.Var(&allow, "allow", "host pattern reachable without a credential")
	parse(fs, args)
	b, err := loadBindings(*bindings)
	if err != nil {
		return err
	}
	dataDir, err := absFlagPath("data", *data)
	if err != nil {
		return err
	}
	*data = dataDir
	if err := os.MkdirAll(*data, 0o700); err != nil {
		return fmt.Errorf("--data %s: %w", *data, err)
	}
	srv, err := server.New(server.Options{DataDir: filepath.Join(*data, "server"), Bindings: b, Logger: slog.Default(), Mode: server.ModeStandalone})
	if err != nil {
		return err
	}
	defer srv.Close()
	serveErr := make(chan error, 1)
	go func() { serveErr <- srv.Serve(ctx, *listen) }()
	readyCtx, readyCancel := context.WithTimeout(ctx, 10*time.Second)
	addr, err := srv.WaitReady(readyCtx)
	readyCancel()
	if err != nil {
		select {
		case serveErrValue := <-serveErr:
			if serveErrValue != nil {
				return serveErrValue
			}
		default:
		}
		return err
	}
	c := common{server: "http://" + addr}
	n, err := buildNode(filepath.Join(*data, "node"), c, map[string]string{"standalone": "true"}, *backends, workspace.DefaultImage(version), allow, []string{"127.0.0.1", "localhost"}, nodeResourceOptions{})
	if err != nil {
		return err
	}
	fmt.Fprintf(os.Stderr, "remount standalone [LOCAL/UNISOLATED]: server http://%s (no token), node %s\n  export REMOUNT_SERVER=http://%s\n", addr, n.ID(), addr)
	return n.Run(ctx)
}

// ---------------------------------------------------------------------------
// workspaces
// ---------------------------------------------------------------------------

func cmdWS(ctx context.Context, args []string) error {
	if len(args) == 0 || isHelp(args[0]) {
		return errors.New("ws: create|ls|get|destroy|move|sleep|wake|snapshot|acl")
	}
	sub, rest := args[0], args[1:]
	fs := flag.NewFlagSet("ws "+sub, flag.ExitOnError)
	var c common
	c.flags(fs)
	switch sub {
	case "create":
		name := fs.String("name", "", "name")
		backend := fs.String("backend", "", "required backend")
		image := fs.String("image", "", "image (docker)")
		cpu := fs.Int("cpu", 0, "required cpus")
		mem := fs.Int("mem", 0, "required memory MiB")
		nodeID := fs.String("node", "", "pin to node id")
		principal := fs.String("principal", "", "deprecated; identity is server-authoritative")
		run := fs.String("run", "", "run identifier used by fleet selectors")
		model := fs.String("model", "", "model identifier used by fleet selectors")
		labels, workspaceLabels, env := kvFlag{}, kvFlag{}, kvFlag{}
		fs.Var(labels, "label", "placement label k=v (repeatable)")
		fs.Var(workspaceLabels, "workspace-label", "workspace label k=v (repeatable)")
		fs.Var(env, "env", "env K=V; values may use ref:<binding>, ${REMOUNT_BROKER}, or ${REMOUNT_PACKAGE_CONNECTOR} (repeatable)")
		var bindings, exclude listFlag
		var egressRules egressRuleFlag
		fs.Var(&bindings, "binding", "binding id (repeatable)")
		fs.Var(&exclude, "exclude", "snapshot exclude glob (repeatable)")
		dir := fs.String("dir", "", "seed the workspace from this local directory (honors .gitignore and .remountignore)")
		includeGit := fs.Bool("include-git", true, "with --dir: include .git so the agent can commit")
		restoreFrom := fs.String("restore-from", "", "seed the workspace from an existing artifact id")
		base := fs.String("base", "", "seed the workspace from a named base (see remount base ls)")
		securityProfile := fs.String("security", "", "security profile: local, isolated, multi_tenant")
		minIsolation := fs.String("min-isolation", "", "minimum backend isolation: none, process_sandbox, container, microvm")
		requireSiblingIsolation := fs.Bool("require-sibling-isolation", false, "require a backend with sibling isolation")
		requireEnforcedEgress := fs.Bool("require-enforced-egress", false, "require a backend-controlled egress boundary")
		secretMode := fs.String("secret-mode", "", "secret mode: none or brokered")
		networkDefault := fs.String("network-default", "", "typed network default: deny or allow (allow is local-only)")
		auditRequired := fs.Bool("audit-required", false, "require security audit events")
		fs.Var(&egressRules, "egress-rule", "typed egress rule as JSON or @path (repeatable)")
		wait := fs.Bool("wait", true, "wait until claimed")
		parse(fs, rest)
		if err := arity(fs, 0, 0, "ws create [--name N] [--image IMG] [flags]"); err != nil {
			return err
		}
		if *principal != "" {
			return errors.New("--principal is not supported; authenticated caller identity is authoritative")
		}
		seeds := 0
		for _, set := range []bool{*dir != "", *restoreFrom != "", *base != ""} {
			if set {
				seeds++
			}
		}
		if seeds > 1 {
			return errors.New("--dir, --restore-from and --base are mutually exclusive")
		}
		cl := c.client()
		defer cl.Close()
		if *dir != "" {
			id, _, err := uploadDir(ctx, cl, *dir, localfs.PackOptions{ExcludeGit: !*includeGit, Excludes: exclude})
			if err != nil {
				return err
			}
			*restoreFrom = id
		}
		spec := proto.WorkspaceSpec{
			Name: *name, Run: *run, Model: *model, Labels: workspaceLabels, Image: *image, RestoreFrom: *restoreFrom, Base: *base,
			Requires:  proto.Requires{Backend: *backend, CPU: *cpu, MemMiB: *mem},
			Placement: proto.Placement{Allow: labels, Node: *nodeID},
			Bindings:  bindings, Env: env, Exclude: exclude,
			Security: proto.SecuritySpec{
				Profile: *securityProfile, MinIsolation: *minIsolation,
				RequireSiblingIsolation: *requireSiblingIsolation,
				RequireEnforcedEgress:   *requireEnforcedEgress, SecretMode: *secretMode,
				Network: proto.NetworkPolicy{Default: *networkDefault, Rules: egressRules},
				Audit:   proto.AuditPolicy{Required: *auditRequired},
			},
		}
		normalized, err := proto.NormalizeSecurity(spec.Security)
		if err != nil {
			return fmt.Errorf("security policy: %w", err)
		}
		spec.Security = normalized
		ws, err := cl.CreateWorkspace(ctx, spec)
		if err != nil {
			return err
		}
		if *wait {
			wctx, cancel := context.WithTimeout(ctx, 60*time.Second)
			defer cancel()
			if ws, err = cl.WaitClaimed(wctx, ws.ID); err != nil {
				return fmt.Errorf("%s created but not claimed: %w (is a matching node online?)", ws.ID, err)
			}
		}
		if c.json {
			printJSON(ws)
		} else {
			fmt.Println(ws.ID)
			fmt.Fprintf(os.Stderr, "state=%s node=%s gen=%d\n", ws.State, ws.Node, ws.Generation)
		}
	case "ls":
		parse(fs, rest)
		if err := arity(fs, 0, 0, "ws ls"); err != nil {
			return err
		}
		cl := c.client()
		defer cl.Close()
		list, err := cl.ListWorkspaces(ctx)
		if err != nil {
			return err
		}
		if c.json {
			printJSON(list)
			return nil
		}
		tw := tabWriter()
		fmt.Fprintln(tw, "ID\tNAME\tSTATE\tNODE\tGEN\tBACKEND\tSNAPSHOT")
		for _, ws := range list {
			fmt.Fprintf(tw, "%s\t%s\t%s\t%s\t%d\t%s\t%s\n", ws.ID, ws.Spec.Name, ws.State, ws.Node, ws.Generation, ws.Spec.Requires.Backend, short(ws.LastSnapshot))
		}
		tw.Flush()
	case "get":
		parse(fs, rest)
		if err := arity(fs, 1, 1, "ws get WS"); err != nil {
			return err
		}
		cl := c.client()
		defer cl.Close()
		ws, err := cl.GetWorkspace(ctx, fs.Arg(0))
		if err != nil {
			return err
		}
		printJSON(ws)
	case "destroy":
		parse(fs, rest)
		if err := arity(fs, 1, 1, "ws destroy WS"); err != nil {
			return err
		}
		cl := c.client()
		defer cl.Close()
		if err := cl.DestroyWorkspace(ctx, fs.Arg(0)); err != nil {
			return err
		}
		if c.json {
			printJSON(mutationResult{OK: true, Operation: "ws.destroy", Workspace: fs.Arg(0)})
		}
	case "move":
		nodeID := fs.String("node", "", "pin to node id")
		cpu := fs.Int("cpu", 0, "required cpus")
		mem := fs.Int("mem", 0, "required memory MiB")
		backend := fs.String("backend", "", "required backend")
		labels := kvFlag{}
		fs.Var(labels, "label", "placement label k=v")
		parse(fs, rest)
		if err := arity(fs, 1, 1, "ws move WS [--node ID] [--cpu N] [--label k=v]"); err != nil {
			return err
		}
		cl := c.client()
		defer cl.Close()
		cur, err := cl.GetWorkspace(ctx, fs.Arg(0))
		if err != nil {
			return err
		}
		req := cur.Spec.Requires
		if *cpu > 0 {
			req.CPU = *cpu
		}
		if *mem > 0 {
			req.MemMiB = *mem
		}
		if *backend != "" {
			req.Backend = *backend
		}
		pl := cur.Spec.Placement
		if *nodeID != "" {
			pl.Node = *nodeID
			// Naming a node means that node. Any label constraint the
			// workspace was created with would otherwise have to be
			// satisfied as well, and a contradiction leaves it pending
			// forever with nothing to explain why.
			if len(labels) == 0 {
				pl.Allow = nil
			}
		}
		if len(labels) > 0 {
			pl.Allow = labels
			if *nodeID == "" {
				pl.Node = ""
			}
		}
		ws, err := cl.MoveWorkspace(ctx, fs.Arg(0), &req, &pl)
		if err != nil {
			return err
		}
		wctx, cancel := context.WithTimeout(ctx, 120*time.Second)
		defer cancel()
		ws, err = cl.WaitClaimed(wctx, ws.ID)
		if err != nil {
			cur, gerr := cl.GetWorkspace(ctx, fs.Arg(0))
			if gerr == nil && cur.State == proto.WSPending {
				return fmt.Errorf("%s is snapshotted and queued but no node has claimed it: %w\n"+
					"  requires:  %+v\n  placement: %+v\n  check `remount nodes` for a node that matches", cur.ID, err, cur.Spec.Requires, cur.Spec.Placement)
			}
			return err
		}
		if c.json {
			printJSON(ws)
			return nil
		}
		fmt.Fprintf(os.Stderr, "moved: node=%s gen=%d restored_from=%s\n", ws.Node, ws.Generation, short(ws.LastSnapshot))
	case "acl":
		var readers, writers listFlag
		fs.Var(&readers, "reader", "principal that may read (repeatable; replaces the current list)")
		fs.Var(&writers, "writer", "principal that may read and mutate (repeatable; replaces the current list)")
		parse(fs, rest)
		if err := arity(fs, 1, 1, "ws acl WS [--reader P]... [--writer P]..."); err != nil {
			return err
		}
		cl := c.client()
		defer cl.Close()
		ws, err := cl.SetWorkspaceACL(ctx, fs.Arg(0), proto.WorkspaceACL{Readers: readers, Writers: writers})
		if err != nil {
			return err
		}
		if c.json {
			printJSON(ws)
			return nil
		}
		fmt.Fprintf(os.Stderr, "acl set: readers=%s writers=%s authz_revision=%d\n", strings.Join(ws.Spec.ACL.Readers, ","), strings.Join(ws.Spec.ACL.Writers, ","), ws.AuthzRevision)
	case "sleep":
		after := fs.Duration("after", 0, "wake after duration")
		on := fs.String("on", "", "wake on event type")
		parse(fs, rest)
		if err := arity(fs, 1, 1, "ws sleep WS --after 1h | --on github.pr.merged"); err != nil {
			return err
		}
		if *after == 0 && *on == "" {
			return errors.New("ws sleep WS --after 1h | --on github.pr.merged")
		}
		cl := c.client()
		defer cl.Close()
		t, err := cl.SleepWorkspace(ctx, proto.WSSleepReq{ID: fs.Arg(0), AfterSec: int64(after.Seconds()), OnEvent: *on})
		if err != nil {
			return err
		}
		printJSON(t)
	case "wake":
		parse(fs, rest)
		if err := arity(fs, 1, 1, "ws wake WS"); err != nil {
			return err
		}
		cl := c.client()
		defer cl.Close()
		ws, err := cl.WakeWorkspace(ctx, fs.Arg(0))
		if err != nil {
			return err
		}
		if ws, err = cl.WaitClaimed(ctx, ws.ID); err != nil {
			return err
		}
		if c.json {
			printJSON(ws)
			return nil
		}
		fmt.Fprintf(os.Stderr, "awake: node=%s gen=%d\n", ws.Node, ws.Generation)
	case "snapshot":
		authoritative := fs.Bool("authoritative", false, "quiesce managed execution and commit as failover state")
		upload := fs.Bool("upload", true, "upload the snapshot to the control-plane artifact store")
		asBase := fs.String("as-base", "", "pin the uploaded snapshot as a named base for ws create --base")
		parse(fs, rest)
		if err := arity(fs, 1, 1, "ws snapshot WS [--authoritative] [--upload=true] [--as-base NAME]"); err != nil {
			return err
		}
		if *asBase != "" && !*upload {
			return errors.New("--as-base requires --upload=true")
		}
		cl := c.client()
		defer cl.Close()
		var res *proto.WSSnapshotRes
		var err error
		if *authoritative {
			if !*upload {
				return errors.New("--authoritative requires --upload=true")
			}
			res, err = cl.Checkpoint(ctx, fs.Arg(0))
		} else {
			res, err = cl.Snapshot(ctx, fs.Arg(0), *upload)
		}
		if err != nil {
			return err
		}
		var base *proto.Base
		if *asBase != "" {
			base, err = cl.CreateBase(ctx, proto.BaseCreateReq{Name: *asBase, Artifact: res.Artifact, Workspace: fs.Arg(0)})
			if err != nil {
				return fmt.Errorf("snapshot %s uploaded but not pinned as base %q: %w", res.Artifact, *asBase, err)
			}
		}
		if c.json {
			if base != nil {
				printJSON(struct {
					*proto.WSSnapshotRes
					Base *proto.Base `json:"base"`
				}{res, base})
				return nil
			}
			printJSON(res)
			return nil
		}
		fmt.Println(res.Artifact)
		fmt.Fprintf(os.Stderr, "%d bytes, consistency=%s, authoritative=%t\n", res.Bytes, res.Consistency, res.Authoritative)
		if base != nil {
			fmt.Fprintf(os.Stderr, "pinned as base %s\n", base.Name)
		}
	default:
		return fmt.Errorf("unknown ws subcommand %q", sub)
	}
	return nil
}

// ---------------------------------------------------------------------------
// fleet incident response
// ---------------------------------------------------------------------------

func parseRFC3339Millis(value string) (int64, error) {
	if value == "" {
		return 0, nil
	}
	parsed, err := time.Parse(time.RFC3339, value)
	if err != nil {
		return 0, fmt.Errorf("parse %q as RFC3339: %w", value, err)
	}
	return parsed.UnixMilli(), nil
}

func cmdFleet(ctx context.Context, args []string) error {
	if len(args) == 0 || isHelp(args[0]) {
		return errors.New("fleet: quarantine|ls|get")
	}
	sub, rest := args[0], args[1:]
	fs := flag.NewFlagSet("fleet "+sub, flag.ExitOnError)
	var commonFlags common
	commonFlags.flags(fs)
	switch sub {
	case "quarantine":
		action := fs.String("action", proto.FleetActionFreeze, "freeze|revoke_egress|checkpoint|stop|destroy")
		all := fs.Bool("all", false, "explicitly select every visible workspace")
		tenant := fs.String("tenant", "", "tenant selector")
		principal := fs.String("principal", "", "principal selector")
		run := fs.String("run", "", "run selector")
		nodeID := fs.String("node", "", "node selector")
		model := fs.String("model", "", "model selector")
		backend := fs.String("backend", "", "backend selector")
		createdAfter := fs.String("created-after", "", "RFC3339 lower creation-time bound")
		createdBefore := fs.String("created-before", "", "RFC3339 upper creation-time bound")
		deadline := fs.Duration("deadline", 5*time.Minute, "containment acknowledgement deadline")
		idem := fs.String("idem", "", "stable idempotency key for retry after an ambiguous result")
		wait := fs.Bool("wait", true, "wait for completed/partial state")
		labels := kvFlag{}
		fs.Var(labels, "label", "workspace label selector k=v (repeatable)")
		parse(fs, rest)
		if err := arity(fs, 0, 0, "fleet quarantine --action A (--all | selectors)"); err != nil {
			return err
		}
		afterMillis, err := parseRFC3339Millis(*createdAfter)
		if err != nil {
			return err
		}
		beforeMillis, err := parseRFC3339Millis(*createdBefore)
		if err != nil {
			return err
		}
		request := proto.FleetQuarantineReq{
			Action: *action, IdempotencyKey: *idem, TimeoutMillis: (*deadline).Milliseconds(),
			Selector: proto.WorkspaceSelector{
				All: *all, Tenant: *tenant, Principal: *principal, Run: *run, Node: *nodeID,
				Model: *model, Backend: *backend, Labels: labels,
				CreatedAfter: afterMillis, CreatedBefore: beforeMillis,
			},
		}
		if *deadline < time.Millisecond {
			return errors.New("--deadline must be at least 1ms")
		}
		cl := commonFlags.client()
		defer cl.Close()
		operation, err := cl.QuarantineFleet(ctx, request)
		if err != nil {
			return err
		}
		if *wait && operation.State != proto.FleetStateCompleted {
			operationID := operation.ID
			operation, err = cl.WaitFleetOperation(ctx, operation.ID)
			if err != nil {
				return fmt.Errorf("fleet operation %s is still durable and may continue: %w", operationID, err)
			}
		}
		printFleetOperation(operation, commonFlags.json)
	case "get":
		parse(fs, rest)
		if err := arity(fs, 1, 1, "fleet get OPERATION"); err != nil {
			return err
		}
		cl := commonFlags.client()
		defer cl.Close()
		operation, err := cl.GetFleetOperation(ctx, fs.Arg(0))
		if err != nil {
			return err
		}
		printFleetOperation(operation, commonFlags.json)
	case "ls":
		parse(fs, rest)
		if err := arity(fs, 0, 0, "fleet ls"); err != nil {
			return err
		}
		cl := commonFlags.client()
		defer cl.Close()
		operations, err := cl.ListFleetOperations(ctx)
		if err != nil {
			return err
		}
		if commonFlags.json {
			printJSON(operations)
			return nil
		}
		tw := tabWriter()
		fmt.Fprintln(tw, "ID\tACTION\tSTATE\tTARGETS\tREQUESTED_BY\tCREATED")
		for _, operation := range operations {
			fmt.Fprintf(tw, "%s\t%s\t%s\t%d\t%s\t%s\n", operation.ID, operation.Action,
				operation.State, len(operation.Results), operation.RequestedBy,
				time.UnixMilli(operation.CreatedAt).Format(time.RFC3339))
		}
		tw.Flush()
	default:
		return fmt.Errorf("unknown fleet subcommand %q", sub)
	}
	return nil
}

func printFleetOperation(operation *proto.FleetOperation, jsonOutput bool) {
	if jsonOutput {
		printJSON(operation)
		return
	}
	fmt.Printf("%s\t%s\t%s\n", operation.ID, operation.Action, operation.State)
	tw := tabWriter()
	fmt.Fprintln(tw, "WORKSPACE\tNODE\tSTATE\tACK\tSNAPSHOT\tERROR")
	for _, result := range operation.Results {
		fmt.Fprintf(tw, "%s\t%s\t%s\t%t\t%s\t%s\n", result.Workspace, result.Node,
			result.State, result.Acknowledged, short(result.Snapshot), result.Error)
	}
	tw.Flush()
}

func short(s string) string {
	if len(s) > 24 {
		return s[:24] + "…"
	}
	return s
}

// ---------------------------------------------------------------------------
// exec / sh / attach
// ---------------------------------------------------------------------------

func cmdExec(ctx context.Context, args []string, pty bool) error {
	fs := flag.NewFlagSet("exec", flag.ExitOnError)
	var c common
	c.flags(fs)
	cwd := fs.String("cwd", "", "working directory inside the workspace")
	env := kvFlag{}
	fs.Var(env, "env", "extra env K=V (repeatable)")
	timeout := fs.Duration("timeout", 0, "server-side session timeout; the node kills the process after this (0 = none)")
	stdin := fs.Bool("stdin", false, "forward stdin (exec)")
	killOnInterrupt := fs.Bool("kill-on-interrupt", false, "Ctrl-C sends SIGINT to the remote process instead of detaching")
	parse(fs, args)
	if err := arity(fs, 1, -1, "exec WS -- cmd args… | sh WS [cmd]"); err != nil {
		return err
	}
	if *timeout < 0 {
		return errors.New("--timeout must not be negative")
	}
	wsID := fs.Arg(0)
	program := fs.Args()[1:]
	if len(program) > 0 && program[0] == "--" {
		program = program[1:]
	}
	if len(program) == 0 {
		if !pty {
			return errors.New("exec: no command")
		}
		program = []string{"sh", "-l"}
	}
	cl := c.client()
	defer cl.Close()
	kind := proto.SessionExec
	var rows, cols uint16
	if pty {
		kind = proto.SessionPTY
		if w, h, err := term.GetSize(int(os.Stdout.Fd())); err == nil {
			cols, rows = uint16(w), uint16(h)
		}
	}
	s, err := cl.Exec(ctx, proto.SOpenReq{WS: wsID, Kind: kind, Program: program, Cwd: *cwd, Env: env, Rows: rows, Cols: cols, Stdin: *stdin || pty, TimeoutSec: int64(timeout.Seconds())})
	if err != nil {
		return err
	}
	fmt.Fprintf(os.Stderr, "session %s\n", s.ID)
	return drive(ctx, s, driveOptions{WS: wsID, Session: s.ID, Raw: pty, ForwardStdin: *stdin || pty, KillOnInterrupt: *killOnInterrupt})
}

func cmdAttach(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("attach", flag.ExitOnError)
	var c common
	c.flags(fs)
	from := fs.Uint64("from", 0, "replay from seq")
	killOnInterrupt := fs.Bool("kill-on-interrupt", false, "Ctrl-C sends SIGINT to the remote process instead of detaching")
	parse(fs, args)
	if err := arity(fs, 2, 2, "attach WS SESSION [--from N]"); err != nil {
		return err
	}
	cl := c.client()
	defer cl.Close()
	s, err := cl.Attach(ctx, fs.Arg(0), fs.Arg(1), *from)
	if err != nil {
		return err
	}
	return drive(ctx, s, driveOptions{WS: fs.Arg(0), Session: fs.Arg(1), Raw: term.IsTerminal(int(os.Stdin.Fd())), ForwardStdin: true, KillOnInterrupt: *killOnInterrupt})
}

// liveSession is what drive needs from a client session. client.Session
// satisfies it; tests substitute a fake.
type liveSession interface {
	Chunks() <-chan client.Chunk
	Exit() *proto.ExitInfo
	Input(ctx context.Context, data []byte, eof bool) error
	Signal(ctx context.Context, sig string) error
	Close(ctx context.Context, kill bool) error
	Resize(ctx context.Context, rows, cols uint16) error
}

type driveOptions struct {
	WS, Session     string
	Raw             bool // put the local terminal in raw mode (pty sessions)
	ForwardStdin    bool
	KillOnInterrupt bool // SIGINT forwards to the remote process instead of detaching

	// Test seams; nil means the real terminal and process signals.
	stdin      io.Reader
	stdout     io.Writer
	stderr     io.Writer
	interrupts <-chan os.Signal
}

// drive pumps a session to the terminal, forwarding stdin and resizes.
//
// The session outlives this client on purpose: a Ctrl-C detaches and the
// process keeps running on the node, because an agent's long build should
// not die with the operator's terminal. --kill-on-interrupt opts into the
// conventional behaviour. In raw (pty) mode Ctrl-C is a byte the remote
// shell sees, so no signal arrives here.
func drive(ctx context.Context, s liveSession, o driveOptions) error {
	stdin, stdout, stderr := o.stdin, o.stdout, o.stderr
	if stdin == nil {
		stdin = os.Stdin
	}
	if stdout == nil {
		stdout = os.Stdout
	}
	if stderr == nil {
		stderr = os.Stderr
	}
	// The process-wide context is cancelled by the same SIGINT we handle
	// here; session calls must survive it or the detach itself fails.
	sctx := context.WithoutCancel(ctx)
	interrupts := o.interrupts
	if interrupts == nil {
		ch := make(chan os.Signal, 2)
		signal.Notify(ch, os.Interrupt)
		defer signal.Stop(ch)
		interrupts = ch
	}
	var restore func()
	if o.Raw && term.IsTerminal(int(os.Stdin.Fd())) {
		old, err := term.MakeRaw(int(os.Stdin.Fd()))
		if err == nil {
			restore = func() { _ = term.Restore(int(os.Stdin.Fd()), old) }
			defer restore()
		}
		go watchResize(sctx, s)
	}
	if o.ForwardStdin {
		go func() {
			buf := make([]byte, 4096)
			for {
				n, err := stdin.Read(buf)
				if n > 0 {
					if ierr := s.Input(sctx, append([]byte(nil), buf[:n]...), false); ierr != nil {
						return
					}
				}
				if err != nil {
					_ = s.Input(sctx, nil, true)
					return
				}
			}
		}()
	}
	chunks := s.Chunks()
	for chunks != nil {
		select {
		case ch, ok := <-chunks:
			if !ok {
				chunks = nil
				continue
			}
			writeChunk(ch, stdout, stderr)
		case <-interrupts:
			if o.KillOnInterrupt {
				// Forward once; a second Ctrl-C detaches so a process that
				// ignores SIGINT cannot trap the operator.
				if err := s.Signal(sctx, "INT"); err != nil {
					fmt.Fprintln(stderr, "remount: signal:", err)
				}
				o.KillOnInterrupt = false
				continue
			}
			// Output that already arrived belongs on the terminal before
			// the detach notice.
		drain:
			for {
				select {
				case ch, ok := <-chunks:
					if !ok {
						chunks = nil
						break drain
					}
					writeChunk(ch, stdout, stderr)
				default:
					break drain
				}
			}
			if chunks == nil {
				continue // it exited under us; report the exit, not a detach
			}
			if restore != nil {
				restore()
			}
			cctx, cancel := context.WithTimeout(sctx, 5*time.Second)
			err := s.Close(cctx, false)
			cancel()
			if err != nil {
				return fmt.Errorf("detach: %w (the session is still running; reattach with: remount attach %s %s)", err, o.WS, o.Session)
			}
			fmt.Fprintf(stderr, "\ndetached; reattach with: remount attach %s %s\n", o.WS, o.Session)
			return nil
		}
	}
	if restore != nil {
		restore()
	}
	exit := s.Exit()
	if exit == nil {
		return errors.New("session ended without an exit record")
	}
	if exit.Error != "" {
		fmt.Fprintln(stderr, "remount:", exit.Error)
	}
	if exit.Code != 0 {
		return exitError(exit.Code)
	}
	return nil
}

func writeChunk(ch client.Chunk, stdout, stderr io.Writer) {
	switch ch.Stream {
	case proto.StreamStdout:
		_, _ = stdout.Write(ch.Data)
	case proto.StreamStderr:
		_, _ = stderr.Write(ch.Data)
	case proto.StreamGap:
		var gap proto.Gap
		_ = proto.Unmarshal(ch.Data, &gap)
		fmt.Fprintf(stderr, "\n[remount: output seq %d-%d elided]\n", gap.From, gap.To)
	}
}

// ---------------------------------------------------------------------------
// fs
// ---------------------------------------------------------------------------

func cmdFS(ctx context.Context, args []string) error {
	if len(args) == 0 || isHelp(args[0]) {
		return errors.New("fs read|write|ls|stat|rm|mv|mkdir|search|edit WS PATH…")
	}
	sub, rest := args[0], args[1:]
	fs := flag.NewFlagSet("fs "+sub, flag.ExitOnError)
	var c common
	c.flags(fs)
	recursive := fs.Bool("r", false, "recursive (rm)")
	glob := fs.String("glob", "", "filename glob (search)")
	max := fs.Int("max", 0, "max results (search)")
	all := fs.Bool("all", false, "replace all occurrences (edit)")
	parse(fs, rest)
	a := fs.Args()
	need := func(min, max int, u string) error { return arity(fs, min, max, u) }
	mutation := func(m mutationResult) {
		if c.json {
			m.OK = true
			printJSON(m)
		}
	}
	// Arity is checked before dialing so a typo never costs a connection.
	switch sub {
	case "read", "cat":
		if err := need(2, 2, "fs read WS PATH"); err != nil {
			return err
		}
	case "write":
		if err := need(2, 2, "fs write WS PATH < data"); err != nil {
			return err
		}
	case "ls":
		if err := need(1, 2, "fs ls WS [PATH]"); err != nil {
			return err
		}
	case "stat":
		if err := need(2, 2, "fs stat WS PATH"); err != nil {
			return err
		}
	case "rm":
		if err := need(2, 2, "fs rm [-r] WS PATH"); err != nil {
			return err
		}
	case "mv":
		if err := need(3, 3, "fs mv WS FROM TO"); err != nil {
			return err
		}
	case "mkdir":
		if err := need(2, 2, "fs mkdir WS PATH"); err != nil {
			return err
		}
	case "search", "grep":
		if err := need(2, 3, "fs search WS PATTERN [PATH]"); err != nil {
			return err
		}
	case "edit":
		if err := need(4, 4, "fs edit [--all] WS PATH OLD NEW"); err != nil {
			return err
		}
	default:
		return fmt.Errorf("unknown fs subcommand %q", sub)
	}
	cl := c.client()
	defer cl.Close()
	switch sub {
	case "read", "cat":
		b, err := cl.ReadFile(ctx, a[0], a[1])
		if err != nil {
			return err
		}
		_, _ = os.Stdout.Write(b)
	case "write":
		data, err := io.ReadAll(os.Stdin)
		if err != nil {
			return err
		}
		if err := cl.WriteFile(ctx, a[0], a[1], data, 0); err != nil {
			return err
		}
		n := len(data)
		mutation(mutationResult{Operation: "fs.write", Workspace: a[0], Path: a[1], Count: &n})
	case "ls":
		p := "/"
		if len(a) > 1 {
			p = a[1]
		}
		ents, err := cl.ListDir(ctx, a[0], p)
		if err != nil {
			return err
		}
		if c.json {
			printJSON(ents)
			return nil
		}
		for _, e := range ents {
			kind := "-"
			if e.IsDir {
				kind = "d"
			} else if e.IsLink {
				kind = "l"
			}
			fmt.Printf("%s %04o %10d %s %s\n", kind, e.Mode, e.Size, time.UnixMilli(e.ModTime).Format("2006-01-02 15:04"), e.Name)
		}
	case "stat":
		e, err := cl.Stat(ctx, a[0], a[1])
		if err != nil {
			return err
		}
		printJSON(e)
	case "rm":
		if err := cl.Remove(ctx, a[0], a[1], *recursive); err != nil {
			return err
		}
		mutation(mutationResult{Operation: "fs.rm", Workspace: a[0], Path: a[1]})
	case "mv":
		if err := cl.Rename(ctx, a[0], a[1], a[2]); err != nil {
			return err
		}
		mutation(mutationResult{Operation: "fs.mv", Workspace: a[0], Path: a[1], To: a[2]})
	case "mkdir":
		if err := cl.Mkdir(ctx, a[0], a[1]); err != nil {
			return err
		}
		mutation(mutationResult{Operation: "fs.mkdir", Workspace: a[0], Path: a[1]})
	case "search", "grep":
		p := "/"
		if len(a) > 2 {
			p = a[2]
		}
		res, err := cl.Search(ctx, a[0], p, a[1], *glob, *max)
		if err != nil {
			return err
		}
		for _, m := range res.Matches {
			fmt.Printf("%s:%d:%s\n", m.Path, m.Line, m.Text)
		}
		if res.Truncated {
			fmt.Fprintln(os.Stderr, "(truncated)")
		}
	case "edit":
		n, err := cl.Edit(ctx, a[0], a[1], []proto.FSEdit{{Old: a[2], New: a[3], All: *all}})
		if err != nil {
			return err
		}
		if c.json {
			mutation(mutationResult{Operation: "fs.edit", Workspace: a[0], Path: a[1], Count: &n})
			return nil
		}
		fmt.Fprintf(os.Stderr, "%d replacement(s)\n", n)
	}
	return nil
}

// ---------------------------------------------------------------------------
// port forward
// ---------------------------------------------------------------------------

func cmdPort(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("port", flag.ExitOnError)
	var c common
	c.flags(fs)
	local := fs.String("local", "", "local listen address (default 127.0.0.1:PORT)")
	parse(fs, args)
	if err := arity(fs, 2, 2, "port WS PORT [--local ADDR]"); err != nil {
		return err
	}
	port, err := strconv.Atoi(fs.Arg(1))
	if err != nil || port < 1 || port > 65535 {
		return fmt.Errorf("PORT %q must be 1-65535", fs.Arg(1))
	}
	if *local == "" {
		*local = "127.0.0.1:" + fs.Arg(1)
	}
	cl := c.client()
	defer cl.Close()
	ln, err := net.Listen("tcp", *local)
	if err != nil {
		return err
	}
	defer ln.Close()
	fmt.Fprintf(os.Stderr, "forwarding %s -> %s:%d\n", ln.Addr(), fs.Arg(0), port)
	go func() { <-ctx.Done(); ln.Close() }()
	for {
		conn, err := ln.Accept()
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return err
		}
		go func() {
			defer conn.Close()
			s, err := cl.OpenPort(ctx, fs.Arg(0), port)
			if err != nil {
				fmt.Fprintln(os.Stderr, "port:", err)
				return
			}
			go func() {
				buf := make([]byte, 32<<10)
				for {
					n, err := conn.Read(buf)
					if n > 0 {
						if s.Input(ctx, append([]byte(nil), buf[:n]...), false) != nil {
							return
						}
					}
					if err != nil {
						_ = s.Input(ctx, nil, true)
						return
					}
				}
			}()
			for ch := range s.Chunks() {
				if ch.Stream == proto.StreamStdout {
					if _, err := conn.Write(ch.Data); err != nil {
						_ = s.Close(ctx, true)
						return
					}
				}
			}
		}()
	}
}

// ---------------------------------------------------------------------------
// nodes / events / timers
// ---------------------------------------------------------------------------

func cmdNodes(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("nodes", flag.ExitOnError)
	var c common
	c.flags(fs)
	parse(fs, args)
	if err := arity(fs, 0, 0, "nodes"); err != nil {
		return err
	}
	cl := c.client()
	defer cl.Close()
	nodes, err := cl.ListNodes(ctx)
	if err != nil {
		return err
	}
	if c.json {
		printJSON(nodes)
		return nil
	}
	tw := tabWriter()
	fmt.Fprintln(tw, "ID\tONLINE\tOS/ARCH\tCPU\tMEM_MiB\tBACKENDS\tPROTOCOL\tLABELS\tWORKSPACES")
	for _, n := range nodes {
		fmt.Fprintf(tw, "%s\t%v\t%s/%s\t%d\t%d\t%s\t%s\t%s\t%d\n", n.ID, n.Online, n.Info.OS, n.Info.Arch, n.Info.CPU, n.Info.MemMiB, strings.Join(n.Info.Backends, ","), strings.Join(n.Protocol, ","), kvFlag(n.Labels).String(), len(n.Workspaces))
	}
	tw.Flush()
	return nil
}

func cmdTimers(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("timers", flag.ExitOnError)
	var c common
	c.flags(fs)
	parse(fs, args)
	if err := arity(fs, 0, 0, "timers"); err != nil {
		return err
	}
	cl := c.client()
	defer cl.Close()
	timers, err := cl.ListTimers(ctx)
	if err != nil {
		return err
	}
	printJSON(timers)
	return nil
}

// ---------------------------------------------------------------------------
// helpers
// ---------------------------------------------------------------------------

func tabWriter() *tabwriter {
	return &tabwriter{w: bufio.NewWriter(os.Stdout)}
}

// tabwriter is a tiny column aligner (avoids text/tabwriter's import for a
// binary that already pulls plenty). Columns are padded to the widest cell.
type tabwriter struct {
	w    *bufio.Writer
	rows [][]string
}

func (t *tabwriter) Write(p []byte) (int, error) {
	for _, line := range strings.Split(strings.TrimRight(string(p), "\n"), "\n") {
		t.rows = append(t.rows, strings.Split(line, "\t"))
	}
	return len(p), nil
}

func (t *tabwriter) Flush() {
	var widths []int
	for _, r := range t.rows {
		for i, c := range r {
			if i >= len(widths) {
				widths = append(widths, 0)
			}
			if len(c) > widths[i] {
				widths[i] = len(c)
			}
		}
	}
	for _, r := range t.rows {
		for i, c := range r {
			if i == len(r)-1 {
				t.w.WriteString(c)
			} else {
				t.w.WriteString(c + strings.Repeat(" ", widths[i]-len(c)+2))
			}
		}
		t.w.WriteString("\n")
	}
	t.w.Flush()
}

func init() {
	// Quieter default logging for the CLI; server/node set their own.
	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: levelFromEnv()})))
	_ = http.DefaultClient
}

func levelFromEnv() slog.Level {
	switch strings.ToLower(os.Getenv("REMOUNT_LOG")) {
	case "debug":
		return slog.LevelDebug
	case "warn":
		return slog.LevelWarn
	case "error":
		return slog.LevelError
	}
	return slog.LevelInfo
}
