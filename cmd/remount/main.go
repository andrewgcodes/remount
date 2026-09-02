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
	var err error
	switch os.Args[1] {
	case "server":
		err = cmdServer(ctx, os.Args[2:])
	case "up":
		err = cmdUp(ctx, os.Args[2:])
	case "standalone":
		err = cmdStandalone(ctx, os.Args[2:])
	case "ws":
		err = cmdWS(ctx, os.Args[2:])
	case "exec":
		err = cmdExec(ctx, os.Args[2:], false)
	case "sh":
		err = cmdExec(ctx, os.Args[2:], true)
	case "attach":
		err = cmdAttach(ctx, os.Args[2:])
	case "fs":
		err = cmdFS(ctx, os.Args[2:])
	case "port":
		err = cmdPort(ctx, os.Args[2:])
	case "fleet":
		err = cmdFleet(ctx, os.Args[2:])
	case "status":
		err = cmdStatus(ctx, os.Args[2:])
	case "inspect":
		err = cmdInspect(ctx, os.Args[2:])
	case "doctor":
		err = cmdDoctor(ctx, os.Args[2:])
	case "metrics":
		err = cmdMetrics(ctx, os.Args[2:])
	case "nodes":
		err = cmdNodes(ctx, os.Args[2:])
	case "events":
		err = cmdEvents(ctx, os.Args[2:])
	case "timers":
		err = cmdTimers(ctx, os.Args[2:])
	case "version":
		fmt.Println("remount", version)
	case "help", "-h", "--help":
		usage()
	default:
		usage()
		os.Exit(2)
	}
	if err != nil {
		var ee exitError
		if errors.As(err, &ee) {
			os.Exit(int(ee))
		}
		fmt.Fprintln(os.Stderr, "remount:", err)
		os.Exit(1)
	}
}

type exitError int

func (e exitError) Error() string { return "exit " + strconv.Itoa(int(e)) }

func usage() {
	fmt.Fprint(os.Stderr, `remount — any agent, any machine, never holding the keys, never losing its place

  remount server      run the control plane + relay + artifact store
  remount up          enroll this machine as a node (outbound only)
  remount standalone  server + node in one process (try it on a laptop)

  remount ws create [--name N] [--backend B] [--label k=v] [--binding ID] [--env K=V] [--exclude GLOB]
  remount ws ls | get WS | destroy WS | move WS [--node ID] [--cpu N] | sleep WS (--after 1h | --on EVENT) | wake WS | snapshot WS
  remount exec WS -- cmd args...      run a command (stdout/stderr/exit streamed)
  remount sh WS [cmd]                 interactive shell (pty)
  remount attach WS SESSION [--from N]
  remount fs read|write|ls|stat|rm|mv|search|edit WS ...
  remount port WS PORT [--local 127.0.0.1:PORT]
  remount fleet quarantine --action freeze (--all | SELECTORS...) | ls | get OPERATION
  remount nodes | events [--follow] [--ws WS] | timers

Inspection, at three depths. All take --json.
  remount status [--watch 5s]         the fleet in one screen
  remount inspect WS                  one workspace, down to session log positions
  remount doctor                      every check for damage, loss or disagreement
  remount metrics [--node ID]         raw counters

Common flags (or env): --server REMOUNT_SERVER (default http://127.0.0.1:7443) --token REMOUNT_TOKEN
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
	return client.New(client.Options{Dialer: c.dialer(), Token: c.token, Principal: principal})
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
	mode := fs.String("mode", envOr("REMOUNT_SECURITY_MODE", server.ModeStandalone), "security mode: standalone, production-single-tenant, production-multi-tenant")
	parse(fs, args)
	if *token == "" && !*insecure {
		return errors.New("--token is required (or --insecure for local experiments)")
	}
	if err := os.MkdirAll(*data, 0o700); err != nil {
		return err
	}
	b, err := loadBindings(*bindings)
	if err != nil {
		return err
	}
	srv, err := server.New(server.Options{DataDir: *data, Token: *token, Bindings: b, LeaseSec: *lease, Logger: slog.Default(), Mode: *mode})
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
	image := fs.String("image", "ubuntu:24.04", "default docker image")
	var allow, allowPrivate listFlag
	fs.Var(&allow, "allow", "host pattern workspaces may reach without a credential (repeatable)")
	fs.Var(&allowPrivate, "allow-private", "host pattern allowed to resolve to a private address (repeatable)")
	parse(fs, args)
	n, err := buildNode(*data, c, labels, *backends, *image, allow, allowPrivate)
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

func buildNode(data string, c common, labels map[string]string, backends, image string, allow, allowPrivate []string) (*node.Node, error) {
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
	if err := os.MkdirAll(*data, 0o700); err != nil {
		return err
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
	n, err := buildNode(filepath.Join(*data, "node"), c, map[string]string{"standalone": "true"}, *backends, "ubuntu:24.04", allow, []string{"127.0.0.1", "localhost"})
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
	if len(args) == 0 {
		return errors.New("ws: create|ls|get|destroy|move|sleep|wake|snapshot")
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
		principal := fs.String("principal", "", "principal (default: caller)")
		run := fs.String("run", "", "run identifier used by fleet selectors")
		model := fs.String("model", "", "model identifier used by fleet selectors")
		labels, workspaceLabels, env := kvFlag{}, kvFlag{}, kvFlag{}
		fs.Var(labels, "label", "placement label k=v (repeatable)")
		fs.Var(workspaceLabels, "workspace-label", "workspace label k=v (repeatable)")
		fs.Var(env, "env", "env K=V; values may be ref:<binding> or ${REMOUNT_BROKER} (repeatable)")
		var bindings, exclude listFlag
		fs.Var(&bindings, "binding", "binding id (repeatable)")
		fs.Var(&exclude, "exclude", "snapshot exclude glob (repeatable)")
		wait := fs.Bool("wait", true, "wait until claimed")
		parse(fs, rest)
		cl := c.client()
		defer cl.Close()
		spec := proto.WorkspaceSpec{
			Name: *name, Run: *run, Model: *model, Labels: workspaceLabels, Image: *image, Principal: *principal,
			Requires:  proto.Requires{Backend: *backend, CPU: *cpu, MemMiB: *mem},
			Placement: proto.Placement{Allow: labels, Node: *nodeID},
			Bindings:  bindings, Env: env, Exclude: exclude,
		}
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
		if fs.NArg() < 1 {
			return errors.New("ws get WS")
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
		if fs.NArg() < 1 {
			return errors.New("ws destroy WS")
		}
		cl := c.client()
		defer cl.Close()
		return cl.DestroyWorkspace(ctx, fs.Arg(0))
	case "move":
		nodeID := fs.String("node", "", "pin to node id")
		cpu := fs.Int("cpu", 0, "required cpus")
		mem := fs.Int("mem", 0, "required memory MiB")
		backend := fs.String("backend", "", "required backend")
		labels := kvFlag{}
		fs.Var(labels, "label", "placement label k=v")
		parse(fs, rest)
		if fs.NArg() < 1 {
			return errors.New("ws move WS [--node ID] [--cpu N] [--label k=v]")
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
		fmt.Fprintf(os.Stderr, "moved: node=%s gen=%d restored_from=%s\n", ws.Node, ws.Generation, short(ws.LastSnapshot))
	case "sleep":
		after := fs.Duration("after", 0, "wake after duration")
		on := fs.String("on", "", "wake on event type")
		parse(fs, rest)
		if fs.NArg() < 1 || (*after == 0 && *on == "") {
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
		if fs.NArg() < 1 {
			return errors.New("ws wake WS")
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
		fmt.Fprintf(os.Stderr, "awake: node=%s gen=%d\n", ws.Node, ws.Generation)
	case "snapshot":
		parse(fs, rest)
		if fs.NArg() < 1 {
			return errors.New("ws snapshot WS")
		}
		cl := c.client()
		defer cl.Close()
		res, err := cl.Snapshot(ctx, fs.Arg(0), true)
		if err != nil {
			return err
		}
		fmt.Println(res.Artifact)
		fmt.Fprintf(os.Stderr, "%d bytes\n", res.Bytes)
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
	if len(args) == 0 {
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
		if fs.NArg() != 1 {
			return errors.New("fleet get OPERATION")
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
	timeout := fs.Duration("timeout", 0, "kill after duration")
	stdin := fs.Bool("stdin", false, "forward stdin (exec)")
	parse(fs, args)
	if fs.NArg() < 1 {
		return errors.New("exec WS -- cmd args… | sh WS [cmd]")
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
	return drive(ctx, s, pty, *stdin || pty)
}

func cmdAttach(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("attach", flag.ExitOnError)
	var c common
	c.flags(fs)
	from := fs.Uint64("from", 0, "replay from seq")
	parse(fs, args)
	if fs.NArg() < 2 {
		return errors.New("attach WS SESSION [--from N]")
	}
	cl := c.client()
	defer cl.Close()
	s, err := cl.Attach(ctx, fs.Arg(0), fs.Arg(1), *from)
	if err != nil {
		return err
	}
	return drive(ctx, s, term.IsTerminal(int(os.Stdin.Fd())), true)
}

// drive pumps a session to the terminal, forwarding stdin and resizes.
func drive(ctx context.Context, s *client.Session, raw, forwardStdin bool) error {
	var restore func()
	if raw && term.IsTerminal(int(os.Stdin.Fd())) {
		old, err := term.MakeRaw(int(os.Stdin.Fd()))
		if err == nil {
			restore = func() { _ = term.Restore(int(os.Stdin.Fd()), old) }
			defer restore()
		}
		go watchResize(ctx, s)
	}
	if forwardStdin {
		go func() {
			buf := make([]byte, 4096)
			for {
				n, err := os.Stdin.Read(buf)
				if n > 0 {
					if ierr := s.Input(ctx, append([]byte(nil), buf[:n]...), false); ierr != nil {
						return
					}
				}
				if err != nil {
					_ = s.Input(ctx, nil, true)
					return
				}
			}
		}()
	}
	exit := client.Copy(s, os.Stdout, os.Stderr)
	if restore != nil {
		restore()
	}
	if exit == nil {
		return errors.New("session ended without an exit record")
	}
	if exit.Error != "" {
		fmt.Fprintln(os.Stderr, "remount:", exit.Error)
	}
	if exit.Code != 0 {
		return exitError(exit.Code)
	}
	return nil
}

// ---------------------------------------------------------------------------
// fs
// ---------------------------------------------------------------------------

func cmdFS(ctx context.Context, args []string) error {
	if len(args) == 0 {
		return errors.New("fs read|write|ls|stat|rm|mv|search|edit WS PATH…")
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
	cl := c.client()
	defer cl.Close()
	a := fs.Args()
	need := func(n int, u string) error {
		if len(a) < n {
			return errors.New(u)
		}
		return nil
	}
	switch sub {
	case "read", "cat":
		if err := need(2, "fs read WS PATH"); err != nil {
			return err
		}
		b, err := cl.ReadFile(ctx, a[0], a[1])
		if err != nil {
			return err
		}
		_, _ = os.Stdout.Write(b)
	case "write":
		if err := need(2, "fs write WS PATH < data"); err != nil {
			return err
		}
		data, err := io.ReadAll(os.Stdin)
		if err != nil {
			return err
		}
		return cl.WriteFile(ctx, a[0], a[1], data, 0)
	case "ls":
		if err := need(1, "fs ls WS [PATH]"); err != nil {
			return err
		}
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
		if err := need(2, "fs stat WS PATH"); err != nil {
			return err
		}
		e, err := cl.Stat(ctx, a[0], a[1])
		if err != nil {
			return err
		}
		printJSON(e)
	case "rm":
		if err := need(2, "fs rm [-r] WS PATH"); err != nil {
			return err
		}
		return cl.Remove(ctx, a[0], a[1], *recursive)
	case "mv":
		if err := need(3, "fs mv WS FROM TO"); err != nil {
			return err
		}
		return cl.Rename(ctx, a[0], a[1], a[2])
	case "mkdir":
		if err := need(2, "fs mkdir WS PATH"); err != nil {
			return err
		}
		return cl.Mkdir(ctx, a[0], a[1])
	case "search", "grep":
		if err := need(2, "fs search WS PATTERN [PATH]"); err != nil {
			return err
		}
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
		if err := need(4, "fs edit [--all] WS PATH OLD NEW"); err != nil {
			return err
		}
		n, err := cl.Edit(ctx, a[0], a[1], []proto.FSEdit{{Old: a[2], New: a[3], All: *all}})
		if err != nil {
			return err
		}
		fmt.Fprintf(os.Stderr, "%d replacement(s)\n", n)
	default:
		return fmt.Errorf("unknown fs subcommand %q", sub)
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
	if fs.NArg() < 2 {
		return errors.New("port WS PORT [--local ADDR]")
	}
	port, err := strconv.Atoi(fs.Arg(1))
	if err != nil {
		return err
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
	fmt.Fprintln(tw, "ID\tONLINE\tOS/ARCH\tCPU\tMEM_MiB\tBACKENDS\tLABELS\tWORKSPACES")
	for _, n := range nodes {
		fmt.Fprintf(tw, "%s\t%v\t%s/%s\t%d\t%d\t%s\t%s\t%d\n", n.ID, n.Online, n.Info.OS, n.Info.Arch, n.Info.CPU, n.Info.MemMiB, strings.Join(n.Info.Backends, ","), kvFlag(n.Labels).String(), len(n.Workspaces))
	}
	tw.Flush()
	return nil
}

func cmdEvents(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("events", flag.ExitOnError)
	var c common
	c.flags(fs)
	follow := fs.Bool("follow", false, "stream new events")
	ws := fs.String("ws", "", "filter by workspace")
	from := fs.Uint64("from", 1, "first seq")
	parse(fs, args)
	cl := c.client()
	defer cl.Close()
	print := func(e proto.Event) {
		if c.json {
			var payload any
			_ = proto.Unmarshal(e.Payload, &payload)
			printJSON(map[string]any{"seq": e.Seq, "at": time.UnixMilli(e.At).Format(time.RFC3339Nano), "type": e.Type, "stream": e.Stream, "principal": e.Principal, "node": e.Node, "payload": payload})
			return
		}
		var payload any
		_ = proto.Unmarshal(e.Payload, &payload)
		pj, _ := json.Marshal(payload)
		fmt.Printf("%6d %s %-18s %-30s %-14s %s\n", e.Seq, time.UnixMilli(e.At).Format("15:04:05.000"), e.Type, e.Stream, e.Principal, string(pj))
	}
	if !*follow {
		evs, err := cl.ReadEvents(ctx, *from, *ws)
		if err != nil {
			return err
		}
		for _, e := range evs {
			print(e)
		}
		return nil
	}
	ch, err := cl.TailEvents(ctx, *from, *ws)
	if err != nil {
		return err
	}
	for e := range ch {
		print(e)
	}
	return nil
}

func cmdTimers(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("timers", flag.ExitOnError)
	var c common
	c.flags(fs)
	parse(fs, args)
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
