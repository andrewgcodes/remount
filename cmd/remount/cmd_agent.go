package main

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
	"time"

	"golang.org/x/term"

	"remount.dev/remount/internal/acp"
	"remount.dev/remount/internal/client"
	"remount.dev/remount/internal/launch"
	"remount.dev/remount/internal/localfs"
	"remount.dev/remount/internal/proto"
)

// ---------------------------------------------------------------------------
// agent: durable agents (ADR 0043)
// ---------------------------------------------------------------------------

const agentUsage = `agent create RECIPE [--dir PATH | --base NAME | --repo URL[@REF] | --ws WS] [--binding ID[:PRESET]]... [--sleep-after DUR] [--max-turns N] [--approve M] [--parent ID] [--at TIME] [--acp-cmd ARG]... [--detach] -- TASK…
    agent ls [--status S] [--ws WS] [--parent ID]
    agent get ID
    agent open ID [--ui]
    agent message ID [--steer] -- TEXT…   (stdin when no TEXT)
    agent cancel | sleep | wake | destroy ID
    agent fork ID [--name N] [-- TASK…]
    agent watch ID [--from N] [--no-follow] [--raw] [--thoughts] [--quiet]
    agent diff ID [--wake] [--stat]
    agent approvals ID
    agent approve APPROVAL [--option X | --deny | --content JSON]`

func cmdAgent(ctx context.Context, args []string) error {
	if len(args) == 0 || isHelp(args[0]) {
		return errors.New(agentUsage)
	}
	sub, rest := args[0], args[1:]
	switch sub {
	case "create":
		return cmdAgentCreate(ctx, rest)
	case "ls", "list":
		return cmdAgentLs(ctx, rest)
	case "get":
		return cmdAgentGet(ctx, rest)
	case "open":
		return cmdAgentOpen(ctx, rest)
	case "message", "msg", "send":
		return cmdAgentMessage(ctx, rest)
	case "cancel", "sleep", "wake", "destroy":
		return cmdAgentSimple(ctx, sub, rest)
	case "fork":
		return cmdAgentFork(ctx, rest)
	case "watch":
		return cmdAgentWatch(ctx, rest)
	case "diff":
		return cmdAgentDiff(ctx, rest)
	case "approvals":
		return cmdAgentApprovals(ctx, rest)
	case "approve":
		return cmdApprove(ctx, rest)
	}
	return fmt.Errorf("agent: unknown subcommand %q\n%s", sub, agentUsage)
}

// agentSeedFlags are the launch flags `agent create` shares with `run`.
type agentSeedFlags struct {
	dir, repo, base, ws, image, backend, name, model, security, sandbox, approve, mountPath, recipeFile, parent, at string
	includeGit                                                                                                      bool
	repoDepth                                                                                                       int
	bindings, exclude, acpCmd                                                                                       listFlag
	sleepAfter                                                                                                      time.Duration
	maxTurns                                                                                                        int
}

func (f *agentSeedFlags) register(fs *flag.FlagSet) {
	fs.StringVar(&f.dir, "dir", "", "seed the workspace from this local directory (gitignore-aware)")
	fs.BoolVar(&f.includeGit, "include-git", true, "with --dir, include the .git directory")
	fs.StringVar(&f.repo, "repo", "", "clone this repository into a fresh workspace: URL[@REF]; the node clones through the broker")
	fs.IntVar(&f.repoDepth, "repo-depth", 0, "with --repo, shallow-clone depth (0 = full history)")
	fs.StringVar(&f.base, "base", "", "seed from a named base (remount base ls)")
	fs.StringVar(&f.ws, "ws", "", "run in an existing workspace (it outlives the agent)")
	fs.StringVar(&f.image, "image", "", "container image (default: the recipe's)")
	fs.StringVar(&f.backend, "backend", "", "require a node backend (docker, process)")
	fs.StringVar(&f.name, "name", "", "agent (and workspace) name")
	fs.StringVar(&f.model, "model", "", "model id passed to the harness")
	fs.StringVar(&f.security, "security", "", "local | isolated | multi_tenant (default local)")
	fs.StringVar(&f.sandbox, "sandbox", launch.SandboxWorkspaceWrite, "read-only | workspace-write | full")
	fs.StringVar(&f.approve, "approve", launch.ApproveNever, "never | auto | on-request: what happens when the harness asks permission")
	fs.StringVar(&f.mountPath, "mount-path", "", "absolute path the tree appears at inside the workspace (default /work; needs docker)")
	fs.StringVar(&f.recipeFile, "recipe-file", "", "load the recipe from this YAML file instead of a built-in")
	fs.DurationVar(&f.sleepAfter, "sleep-after", 0, "put the workspace to sleep this long after the agent starts waiting for input (0 = never)")
	fs.IntVar(&f.maxTurns, "max-turns", 0, "finish the agent after this many turns (0 = unlimited)")
	fs.StringVar(&f.parent, "parent", "", "create a child of this agent: it inherits bindings and policy caps and reports back when it finishes")
	fs.StringVar(&f.at, "at", "", "hold the first run until this time: RFC 3339, HH:MM (next occurrence, local time) or a duration from now (90m)")
	fs.Var(&f.bindings, "binding", "provider binding ID[:PRESET][?host=…] (repeatable)")
	fs.Var(&f.exclude, "exclude", "snapshot exclude glob (repeatable)")
	fs.Var(&f.acpCmd, "acp-cmd", "ACP server argv element (repeatable); overrides the recipe's acp.command")
}

// agentPlan is a validated agent launch: the recipe, the launch options and
// the derived plan, ready to become an AgentCreateReq once the seed (a
// packed --dir) is uploaded.
type agentPlan struct {
	recipe     *launch.Recipe
	recipeYAML string
	opts       launch.Options
	plan       *launch.Plan
	policy     proto.AgentPolicy
	dir        string
	includeGit bool
	parent     string
}

// loadRecipe resolves RECIPE to a built-in or the --recipe-file.
func loadRecipe(name, file string) (*launch.Recipe, string, error) {
	if file == "" {
		r, err := launch.Load(name)
		return r, "", err
	}
	src, err := os.ReadFile(file)
	if err != nil {
		return nil, "", err
	}
	r, err := launch.Parse(src)
	if err != nil {
		return nil, "", fmt.Errorf("%s: %w", file, err)
	}
	if r.Name != name {
		return nil, "", fmt.Errorf("%s declares recipe %q; the command names %q", file, r.Name, name)
	}
	return r, string(src), nil
}

// planAgent validates the seed flags against the recipe and the task the
// way `run` does, so both commands refuse the same mistakes before anything
// is uploaded.
func planAgent(ctx context.Context, c *common, f *agentSeedFlags, recipe *launch.Recipe, recipeYAML, task string) (*agentPlan, error) {
	if recipe.Mode() != launch.ModeACP && len(f.acpCmd) == 0 {
		return nil, fmt.Errorf("recipe %s has no acp command; pass --acp-cmd or use remount run", recipe.Name)
	}
	if recipe.CommandFromArgs && len(f.acpCmd) == 0 {
		return nil, fmt.Errorf("recipe %s needs --acp-cmd", recipe.Name)
	}
	if strings.TrimSpace(task) == "" {
		return nil, fmt.Errorf("recipe %s needs a task after --", recipe.Name)
	}
	if f.sleepAfter < 0 {
		return nil, errors.New("--sleep-after must not be negative")
	}
	if f.maxTurns < 0 {
		return nil, errors.New("--max-turns must not be negative")
	}
	switch f.approve {
	case "", proto.ApproveNever, proto.ApproveAuto, proto.ApproveOnRequest:
	default:
		return nil, fmt.Errorf("--approve must be %s, %s or %s", proto.ApproveNever, proto.ApproveAuto, proto.ApproveOnRequest)
	}
	o := launch.Options{
		Recipe: recipe, Task: task, WS: f.ws, Base: f.base, Name: f.name, Image: f.image, Backend: f.backend,
		Security: f.security, Sandbox: f.sandbox, Model: f.model, Exclude: f.exclude, MountPath: f.mountPath,
		// launch.Options validates the harness's own approve modes; the
		// Agent policy carries the real value.
		Approve: launch.ApproveNever,
	}
	if recipe.CommandFromArgs {
		o.Args = f.acpCmd
	}
	for _, spec := range f.bindings {
		b, err := launch.ParseBinding(spec)
		if err != nil {
			return nil, err
		}
		o.Bindings = append(o.Bindings, b)
	}
	if f.dir != "" && f.ws != "" {
		return nil, errors.New("--dir and --ws are mutually exclusive")
	}
	if f.dir != "" && f.base != "" {
		return nil, errors.New("--dir and --base are mutually exclusive")
	}
	if f.repo != "" {
		if f.dir != "" {
			return nil, errors.New("--dir and --repo are mutually exclusive")
		}
		r, err := proto.ParseRepoFlag(f.repo, f.repoDepth)
		if err != nil {
			return nil, err
		}
		o.Repo = r
	} else if f.repoDepth != 0 {
		return nil, errors.New("--repo-depth needs --repo")
	}
	var err error
	if o.Bindings, err = defaultBindings(recipe, o.Bindings, c.localBindings(ctx)); err != nil {
		return nil, err
	}
	// Validate wants a seed it can name; a --dir is uploaded afterwards.
	probe := o
	if f.dir != "" {
		probe.RestoreFrom = "pending"
	}
	plan, err := probe.Validate()
	if err != nil {
		return nil, err
	}
	policy := proto.AgentPolicy{SleepAfterSec: int64(f.sleepAfter / time.Second), Approve: f.approve, MaxTurns: f.maxTurns}
	if policy.Approve == "" {
		policy.Approve = proto.ApproveNever
	}
	if f.at != "" {
		at, err := parseStartAt(f.at, time.Now())
		if err != nil {
			return nil, err
		}
		policy.StartAt = at.UnixMilli()
	}
	return &agentPlan{recipe: recipe, recipeYAML: recipeYAML, opts: o, plan: plan, policy: policy, dir: f.dir, includeGit: f.includeGit, parent: f.parent}, nil
}

// parseStartAt reads --at: an RFC 3339 instant, a wall-clock HH:MM (the next
// occurrence in local time, so 09:00 typed at 17:00 means tomorrow), or a
// duration from now. Whatever the form, the result is in the future.
func parseStartAt(s string, now time.Time) (time.Time, error) {
	if t, err := time.Parse(time.RFC3339, s); err == nil {
		if !t.After(now) {
			return time.Time{}, fmt.Errorf("--at %s is in the past", s)
		}
		return t, nil
	}
	if d, err := time.ParseDuration(s); err == nil {
		if d <= 0 {
			return time.Time{}, fmt.Errorf("--at %s must be a positive duration", s)
		}
		return now.Add(d), nil
	}
	if clock, err := time.ParseInLocation("15:04", s, now.Location()); err == nil {
		t := time.Date(now.Year(), now.Month(), now.Day(), clock.Hour(), clock.Minute(), 0, 0, now.Location())
		if !t.After(now) {
			t = t.AddDate(0, 0, 1)
		}
		return t, nil
	}
	return time.Time{}, fmt.Errorf("--at %q is not RFC 3339, HH:MM or a duration", s)
}

// create uploads the seed if any and creates the agent.
func (p *agentPlan) create(ctx context.Context, cl *client.Client, acpCmd []string, idem string) (*proto.Agent, error) {
	req := proto.AgentCreateReq{
		Name: p.opts.Name,
		Spec: proto.AgentSpec{
			Recipe: p.recipe.Name, RecipeYAML: p.recipeYAML, Task: p.opts.Task,
			Providers: p.plan.Data.Providers, Primary: p.plan.Data.Primary, Model: p.opts.Model,
			Sandbox: p.opts.Sandbox, ACPCommand: acpCmd, Auth: p.plan.Auth, Mode: proto.AgentModeACP,
		},
		Policy: p.policy,
		Parent: p.parent,
	}
	for _, binding := range p.opts.Bindings {
		req.Spec.BindingSpecs = append(req.Spec.BindingSpecs, binding.String())
	}
	if p.opts.WS != "" {
		req.WS = p.opts.WS
	} else {
		spec := p.plan.Spec
		if p.dir != "" {
			id, _, err := uploadDir(ctx, cl, p.dir, localfs.PackOptions{ExcludeGit: !p.includeGit, Excludes: p.opts.Exclude})
			if err != nil {
				return nil, err
			}
			spec.RestoreFrom = id
		}
		req.Workspace = &spec
	}
	var options []client.OperationOption
	if idem != "" {
		options = append(options, client.WithIdempotencyKey(idem))
	}
	return cl.CreateAgent(ctx, req, options...)
}

func cmdAgentCreate(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("agent create", flag.ExitOnError)
	var c common
	c.flags(fs)
	var f agentSeedFlags
	f.register(fs)
	detach := fs.Bool("detach", false, "print the agent id and return instead of watching")
	idem := fs.String("idempotency-key", "", "make a retried create return the first agent")
	parse(fs, args)
	if fs.NArg() == 0 || isHelp(fs.Arg(0)) {
		return fmt.Errorf("%s\nrecipes: %s", agentUsage, strings.Join(launch.Builtin(), ", "))
	}
	rest := fs.Args()[1:]
	if len(rest) > 0 && rest[0] == "--" {
		rest = rest[1:]
	}
	recipe, yaml, err := loadRecipe(fs.Arg(0), f.recipeFile)
	if err != nil {
		return err
	}
	p, err := planAgent(ctx, &c, &f, recipe, yaml, strings.Join(rest, " "))
	if err != nil {
		return err
	}
	if _, err := c.ensureLocalServer(ctx); err != nil {
		return err
	}
	cl := c.client()
	defer cl.Close()
	a, err := p.create(ctx, cl, f.acpCmd, *idem)
	if err != nil {
		return err
	}
	if p.plan.Auth == launch.AuthWorkspaceResident {
		fmt.Fprintf(os.Stderr, "note: %s will use its own login kept inside workspace %s (no provider binding given)\n", recipe.Name, a.WS)
	}
	if *detach || c.json {
		if c.json {
			printJSON(a)
		} else {
			fmt.Println(a.ID)
			fmt.Fprintf(os.Stderr, "watch: remount agent watch %s\n", a.ID)
		}
		return nil
	}
	return watchAgent(ctx, cl, a.ID, 0, watchOptions{follow: true, interactive: term.IsTerminal(int(os.Stdin.Fd())), showStderr: true})
}

func cmdAgentLs(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("agent ls", flag.ExitOnError)
	var c common
	c.flags(fs)
	status := fs.String("status", "", "only agents in this status")
	ws := fs.String("ws", "", "only the agent of this workspace")
	parent := fs.String("parent", "", "only children of this agent")
	parse(fs, args)
	if err := arity(fs, 0, 0, "agent ls [--status S] [--ws WS] [--parent ID]"); err != nil {
		return err
	}
	cl := c.client()
	defer cl.Close()
	list, err := cl.ListAgents(ctx, proto.AgentListReq{Status: *status, WS: *ws, Parent: *parent})
	if err != nil {
		return err
	}
	if c.json {
		printJSON(list)
		return nil
	}
	tw := tabWriter()
	fmt.Fprintln(tw, "ID\tNAME\tRECIPE\tSTATUS\tWS\tTURNS\tPENDING\tAGE")
	for _, a := range list {
		fmt.Fprintf(tw, "%s\t%s\t%s\t%s\t%s\t%d\t%d\t%s\n", a.ID, short(a.Name), a.Spec.Recipe, a.Status, a.WS, a.Turns, a.PendingApprovals,
			time.Since(time.UnixMilli(a.CreatedAt)).Truncate(time.Second))
	}
	tw.Flush()
	return nil
}

func cmdAgentGet(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("agent get", flag.ExitOnError)
	var c common
	c.flags(fs)
	parse(fs, args)
	if err := arity(fs, 1, 1, "agent get ID"); err != nil {
		return err
	}
	cl := c.client()
	defer cl.Close()
	a, err := cl.GetAgent(ctx, fs.Arg(0))
	if err != nil {
		return err
	}
	if c.json {
		printJSON(a)
		return nil
	}
	printAgent(os.Stdout, a, c.server)
	return nil
}

func printAgent(w io.Writer, a *proto.Agent, server string) {
	fmt.Fprintf(w, "%s  %s\n", a.ID, a.Status)
	if a.StatusReason != "" {
		fmt.Fprintf(w, "  reason:     %s\n", a.StatusReason)
	}
	if a.Name != "" {
		fmt.Fprintf(w, "  name:       %s\n", a.Name)
	}
	fmt.Fprintf(w, "  recipe:     %s (%s)\n", a.Spec.Recipe, a.Mode)
	fmt.Fprintf(w, "  workspace:  %s", a.WS)
	if a.OwnsWS {
		fmt.Fprint(w, " (owned)")
	}
	fmt.Fprintln(w)
	fmt.Fprintf(w, "  turns:      %d", a.Turns)
	if a.Policy.MaxTurns > 0 {
		fmt.Fprintf(w, " of %d", a.Policy.MaxTurns)
	}
	fmt.Fprintln(w)
	if len(a.Inbox) > 0 {
		fmt.Fprintf(w, "  inbox:      %d queued\n", len(a.Inbox))
	}
	if a.PendingApprovals > 0 {
		fmt.Fprintf(w, "  approvals:  %d pending (remount agent approvals %s)\n", a.PendingApprovals, a.ID)
	}
	if a.ACPSessionID != "" {
		fmt.Fprintf(w, "  acp session: %s\n", a.ACPSessionID)
	}
	if a.ForkedFrom != "" {
		fmt.Fprintf(w, "  forked from: %s\n", a.ForkedFrom)
	}
	if a.Parent != "" {
		fmt.Fprintf(w, "  parent:     %s\n", a.Parent)
	}
	fmt.Fprintf(w, "  transcript: %d records (%d retained from %d)\n", a.TranscriptNext, a.TranscriptNext-a.TranscriptFirst, a.TranscriptFirst)
	fmt.Fprintf(w, "  url:        %s\n", agentURL(a, server))
}

// agentURL is the stable link of an agent: the one the control plane minted
// when it knows its public URL, else one under the server this CLI talks to.
func agentURL(a *proto.Agent, server string) string {
	if a.URL != "" {
		return a.URL
	}
	return strings.TrimSuffix(server, "/") + "/a/" + a.ID
}

// cmdAgentOpen prints the agent's stable URL. With --ui it starts the
// recipe's web UI inside the workspace and prints the authenticated preview
// URL that reaches it.
func cmdAgentOpen(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("agent open", flag.ExitOnError)
	var c common
	c.flags(fs)
	ui := fs.Bool("ui", false, "start the recipe's web UI in the workspace and print its preview URL")
	parse(fs, args)
	if err := arity(fs, 1, 1, "agent open ID [--ui]"); err != nil {
		return err
	}
	cl := c.client()
	defer cl.Close()
	a, err := cl.GetAgent(ctx, fs.Arg(0))
	if err != nil {
		return err
	}
	out := map[string]string{"id": a.ID, "url": agentURL(a, c.server)}
	if *ui {
		port, err := startAgentUI(ctx, cl, a)
		if err != nil {
			return err
		}
		out["ui"] = fmt.Sprintf("%s/v1/agents/%s/ports/%d/", strings.TrimSuffix(c.server, "/"), a.ID, port)
	}
	if c.json {
		printJSON(out)
		return nil
	}
	fmt.Println(out["url"])
	if *ui {
		fmt.Println(out["ui"])
	}
	return nil
}

// startAgentUI writes the recipe's UI launcher into the workspace and starts
// it as a detached exec session, waking the workspace if needed. It returns
// the UI's port inside the workspace.
func startAgentUI(ctx context.Context, cl *client.Client, a *proto.Agent) (int, error) {
	var (
		recipe *launch.Recipe
		err    error
	)
	if a.Spec.RecipeYAML != "" {
		recipe, err = launch.Parse([]byte(a.Spec.RecipeYAML))
	} else {
		recipe, err = launch.Load(a.Spec.Recipe)
	}
	if err != nil {
		return 0, err
	}
	if recipe.UI == nil {
		return 0, fmt.Errorf("recipe %s has no ui; nothing to open", recipe.Name)
	}
	if _, err := cl.AgentMaterialized(ctx, a.ID, true, proto.AgentWokenByRequest); err != nil {
		return 0, err
	}
	ws, err := cl.GetWorkspace(ctx, a.WS)
	if err != nil {
		return 0, err
	}
	data := launch.Data{
		Recipe: recipe.Name, Workspace: a.WS, Sandbox: a.Spec.Sandbox, Approve: a.Policy.Approve, Model: a.Spec.Model,
		Providers: a.Spec.Providers, Primary: a.Spec.Primary, Broker: "${REMOUNT_BROKER}", Port: recipe.UI.Port,
	}
	script, err := recipe.UILauncher(data)
	if err != nil {
		return 0, err
	}
	bindings, err := launch.BindingsForWorkspace(a.Spec.BindingSpecs, ws.Spec.Labels, ws.Spec.Bindings)
	if err != nil {
		return 0, err
	}
	env := map[string]string{}
	for _, b := range bindings {
		for k, v := range b.SessionEnv() {
			env[k] = v
		}
	}
	lp := recipe.UILauncherPath()
	if err := cl.WriteFile(ctx, a.WS, lp, []byte(script), 0o755); err != nil {
		return 0, err
	}
	s, err := cl.Exec(ctx, proto.SOpenReq{WS: a.WS, Kind: proto.SessionExec, Program: []string{"/bin/sh", lp}, Env: env})
	if err != nil {
		return 0, err
	}
	// Detach: the UI keeps running in the workspace; closing kills nothing.
	if err := s.Close(ctx, false); err != nil {
		return 0, err
	}
	return recipe.UI.Port, nil
}

func cmdAgentMessage(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("agent message", flag.ExitOnError)
	var c common
	c.flags(fs)
	steer := fs.Bool("steer", false, "interrupt the current turn with this message instead of queueing it")
	idem := fs.String("idempotency-key", "", "make a retried send deliver once")
	parse(fs, args)
	if fs.NArg() < 1 {
		return errors.New("missing argument: agent message ID [--steer] -- TEXT…")
	}
	rest := fs.Args()[1:]
	if len(rest) > 0 && rest[0] == "--" {
		rest = rest[1:]
	}
	text := strings.Join(rest, " ")
	if text == "" {
		b, err := io.ReadAll(io.LimitReader(os.Stdin, proto.MaxAgentMessageSize+1))
		if err != nil {
			return err
		}
		text = strings.TrimRight(string(b), "\n")
	}
	if strings.TrimSpace(text) == "" {
		return errors.New("agent message: empty message")
	}
	cl := c.client()
	defer cl.Close()
	req := proto.AgentMessageReq{ID: fs.Arg(0), Text: text}
	if *steer {
		req.Kind = proto.AgentMessageSteer
	}
	var options []client.OperationOption
	if *idem != "" {
		options = append(options, client.WithIdempotencyKey(*idem))
	}
	res, err := cl.MessageAgent(ctx, req, options...)
	if err != nil {
		return err
	}
	if c.json {
		printJSON(res)
		return nil
	}
	fmt.Printf("%s %s queued=%d\n", res.Agent.ID, res.Agent.Status, len(res.Agent.Inbox))
	if res.Degraded {
		fmt.Fprintln(os.Stderr, "note: the harness cannot take input mid-turn; the message was queued as a follow-up")
	}
	if res.Woken {
		fmt.Fprintln(os.Stderr, "note: the workspace was asleep and is waking")
	}
	return nil
}

// cmdAgentSimple runs the one-argument lifecycle verbs.
func cmdAgentSimple(ctx context.Context, verb string, args []string) error {
	fs := flag.NewFlagSet("agent "+verb, flag.ExitOnError)
	var c common
	c.flags(fs)
	idem := fs.String("idempotency-key", "", "make a retry return the first result")
	parse(fs, args)
	if err := arity(fs, 1, 1, "agent "+verb+" ID"); err != nil {
		return err
	}
	var options []client.OperationOption
	if *idem != "" {
		options = append(options, client.WithIdempotencyKey(*idem))
	}
	cl := c.client()
	defer cl.Close()
	id := fs.Arg(0)
	var (
		a   *proto.Agent
		err error
	)
	switch verb {
	case "cancel":
		a, err = cl.CancelAgent(ctx, id, options...)
	case "sleep":
		a, err = cl.SleepAgent(ctx, id, options...)
	case "wake":
		a, err = cl.WakeAgent(ctx, id, proto.AgentWokenByRequest, options...)
	case "destroy":
		if err = cl.DestroyAgent(ctx, id, options...); err == nil {
			if c.json {
				printJSON(map[string]string{"id": id, "status": proto.AgentDestroyed})
			} else {
				fmt.Println(id, proto.AgentDestroyed)
			}
			return nil
		}
	}
	if err != nil {
		return err
	}
	if c.json {
		printJSON(a)
		return nil
	}
	fmt.Println(a.ID, a.Status)
	return nil
}

func cmdAgentFork(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("agent fork", flag.ExitOnError)
	var c common
	c.flags(fs)
	name := fs.String("name", "", "name of the new agent")
	idem := fs.String("idempotency-key", "", "make a retried fork return the first child")
	parse(fs, args)
	if fs.NArg() < 1 {
		return errors.New("missing argument: agent fork ID [--name N] [-- TASK…]")
	}
	rest := fs.Args()[1:]
	if len(rest) > 0 && rest[0] == "--" {
		rest = rest[1:]
	}
	var options []client.OperationOption
	if *idem != "" {
		options = append(options, client.WithIdempotencyKey(*idem))
	}
	cl := c.client()
	defer cl.Close()
	a, err := cl.ForkAgent(ctx, proto.AgentForkReq{ID: fs.Arg(0), Name: *name, Task: strings.Join(rest, " ")}, options...)
	if err != nil {
		return err
	}
	if c.json {
		printJSON(a)
		return nil
	}
	fmt.Println(a.ID, a.WS)
	fmt.Fprintf(os.Stderr, "watch: remount agent watch %s\n", a.ID)
	return nil
}

func cmdAgentDiff(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("agent diff", flag.ExitOnError)
	var c common
	c.flags(fs)
	wake := fs.Bool("wake", false, "wake a sleeping agent's workspace to compute the diff")
	stat := fs.Bool("stat", false, "print git status instead of the patch")
	parse(fs, args)
	if err := arity(fs, 1, 1, "agent diff ID [--wake] [--stat]"); err != nil {
		return err
	}
	cl := c.client()
	defer cl.Close()
	d, err := cl.Diff(ctx, fs.Arg(0), *wake)
	if err != nil {
		return err
	}
	if c.json {
		printJSON(d)
		return nil
	}
	if *stat {
		fmt.Print(d.Status)
		return nil
	}
	fmt.Print(d.Diff)
	if d.Truncated {
		fmt.Fprintln(os.Stderr, "note: diff truncated; run git diff in the workspace for the rest")
	}
	return nil
}

func cmdAgentApprovals(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("agent approvals", flag.ExitOnError)
	var c common
	c.flags(fs)
	status := fs.String("status", proto.ApprovalPending, "pending | decided | expired | all")
	parse(fs, args)
	if err := arity(fs, 1, 1, "agent approvals ID [--status S]"); err != nil {
		return err
	}
	pass := []string{"--agent", fs.Arg(0), "--status", *status, "--server", c.server}
	if c.token != "" {
		pass = append(pass, "--token", c.token)
	}
	if c.json {
		pass = append(pass, "--json")
	}
	return cmdApprovals(ctx, pass)
}

// ---------------------------------------------------------------------------
// watch: the transcript as a conversation
// ---------------------------------------------------------------------------

type watchOptions struct {
	follow      bool
	interactive bool
	raw         bool
	thoughts    bool
	showStderr  bool

	// Test seams; nil means the process's own streams.
	stdin  io.Reader
	stdout io.Writer
	stderr io.Writer
}

func cmdAgentWatch(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("agent watch", flag.ExitOnError)
	var c common
	c.flags(fs)
	from := fs.Uint64("from", 0, "start at this transcript index (default: the beginning)")
	noFollow := fs.Bool("no-follow", false, "print what is recorded and return")
	raw := fs.Bool("raw", false, "print every transcript record as JSON")
	thoughts := fs.Bool("thoughts", false, "show the harness's thinking chunks")
	quiet := fs.Bool("quiet", false, "hide the harness's stderr")
	interactive := fs.Bool("interactive", true, "on a terminal, read follow-up messages and approval decisions from stdin")
	parse(fs, args)
	if err := arity(fs, 1, 1, "agent watch ID [--from N] [--no-follow] [--raw]"); err != nil {
		return err
	}
	cl := c.client()
	defer cl.Close()
	o := watchOptions{follow: !*noFollow, raw: *raw || c.json, thoughts: *thoughts, showStderr: !*quiet}
	o.interactive = *interactive && !*noFollow && term.IsTerminal(int(os.Stdin.Fd()))
	return watchAgent(ctx, cl, fs.Arg(0), *from, o)
}

// watchPollIdle bounds how long watch sleeps between empty transcript
// pages; it starts lower and backs off to this.
const watchPollIdle = 1500 * time.Millisecond

// watchAgent renders the durable transcript from index from and, with
// follow, keeps rendering until the agent reaches a terminal status. On a
// terminal it also takes follow-up messages and approval decisions from
// stdin. Ctrl-C detaches; it never cancels the agent.
func watchAgent(ctx context.Context, cl *client.Client, id string, from uint64, o watchOptions) error {
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
	a, err := cl.GetAgent(ctx, id)
	if err != nil {
		return err
	}
	r := newTranscriptRenderer(stdout, stderr, o)
	var lines chan string
	if o.interactive {
		lines = make(chan string, 1)
		go readLines(stdin, lines)
	}
	var (
		delay      = 200 * time.Millisecond
		lastStatus string
		shown      = map[string]bool{} // approvals already printed
		lastAgent  = time.Now()
	)
	for {
		page, err := cl.Transcript(ctx, id, from, proto.MaxTranscriptPage)
		if err != nil {
			if ctx.Err() != nil {
				break
			}
			return err
		}
		if page.Gap != nil {
			r.gap(page.Gap.From, page.Gap.To)
		}
		for i := range page.Records {
			r.record(&page.Records[i])
		}
		from = page.Next
		if page.Done {
			break
		}
		if !o.follow && len(page.Records) == 0 {
			r.flush()
			return nil
		}
		if len(page.Records) > 0 {
			delay = 200 * time.Millisecond
			if !o.follow {
				continue
			}
		}
		// Empty page: look at the agent (bounded rate) for status changes,
		// approvals and the exit condition.
		if len(page.Records) == 0 || time.Since(lastAgent) > time.Second {
			lastAgent = time.Now()
			if a, err = cl.GetAgent(ctx, id); err != nil {
				if ctx.Err() != nil {
					break
				}
				return err
			}
			if a.Status != lastStatus {
				lastStatus = a.Status
				r.status(a)
			}
			if a.PendingApprovals > 0 {
				list, err := cl.ListApprovals(ctx, proto.ApprovalListReq{Agent: id, Status: proto.ApprovalPending})
				if err == nil {
					for i := range list {
						if !shown[list[i].ID] {
							shown[list[i].ID] = true
							r.approval(&list[i], o.interactive)
						}
					}
				}
			}
			if agentTerminalStatus(a.Status) && from >= a.TranscriptNext {
				break
			}
		}
		if len(page.Records) > 0 {
			continue
		}
		select {
		case <-ctx.Done():
			r.flush()
			fmt.Fprintf(stderr, "\ndetached; resume with: remount agent watch %s --from %d\n", id, from)
			return nil
		case line, ok := <-lines:
			if !ok {
				lines = nil
				continue
			}
			if err := watchInput(ctx, cl, a, line, stderr); err != nil {
				fmt.Fprintf(stderr, "error: %v\n", err)
			}
			delay = 200 * time.Millisecond
		case <-time.After(delay):
			if delay < watchPollIdle {
				delay += delay / 2
			}
		}
	}
	r.flush()
	if ctx.Err() != nil {
		fmt.Fprintf(stderr, "\ndetached; resume with: remount agent watch %s --from %d\n", id, from)
		return nil
	}
	// The transcript can report done before the poll loop looked at the
	// agent; the terminal status is still worth a line.
	if a, err = cl.GetAgent(ctx, id); err != nil {
		return err
	}
	if a.Status != lastStatus {
		r.status(a)
	}
	if a.Status == proto.AgentFailed {
		fmt.Fprintf(stderr, "agent %s failed: %s\n", a.ID, a.StatusReason)
		return exitError(1)
	}
	return nil
}

func agentTerminalStatus(s string) bool {
	return s == proto.AgentFailed || s == proto.AgentFinished || s == proto.AgentDestroyed
}

// readLines feeds stdin lines to ch until EOF.
func readLines(r io.Reader, ch chan<- string) {
	defer close(ch)
	sc := bufio.NewScanner(r)
	sc.Buffer(make([]byte, 64*1024), proto.MaxAgentMessageSize)
	for sc.Scan() {
		ch <- sc.Text()
	}
}

// watchInput interprets one typed line. While approvals are pending a
// short answer decides the oldest one; anything else is a message.
func watchInput(ctx context.Context, cl *client.Client, a *proto.Agent, line string, stderr io.Writer) error {
	text := strings.TrimSpace(line)
	if text == "" {
		return nil
	}
	if a.PendingApprovals > 0 {
		list, err := cl.ListApprovals(ctx, proto.ApprovalListReq{Agent: a.ID, Status: proto.ApprovalPending})
		if err != nil {
			return err
		}
		if len(list) > 0 {
			ap := &list[0]
			req := proto.ApprovalDecideReq{ID: ap.ID}
			decided := true
			switch strings.ToLower(text) {
			case "n", "no", "deny", "d":
				req.Denied = true
			case "y", "yes", "allow", "a":
				req.Option = firstAllowOption(ap)
				if req.Option == "" && ap.Kind != proto.ApprovalEgress {
					return fmt.Errorf("approval %s offers options %s; type one", ap.ID, approvalOptions(ap.Options))
				}
			default:
				decided = false
				for _, opt := range ap.Options {
					if opt.ID == text {
						req.Option, decided = opt.ID, true
					}
				}
				if !decided && ap.Kind == proto.ApprovalElicitation && strings.HasPrefix(text, "{") {
					req.Content, decided = json.RawMessage(text), true
				}
			}
			if decided {
				res, err := cl.DecideApproval(ctx, req)
				if err != nil {
					return err
				}
				fmt.Fprintf(stderr, "approval %s %s %s\n", res.ID, res.Status, approvalDecisionText(res))
				return nil
			}
		}
	}
	res, err := cl.MessageAgent(ctx, proto.AgentMessageReq{ID: a.ID, Text: text})
	if err != nil {
		return err
	}
	if res.Degraded {
		fmt.Fprintln(stderr, "(queued: the harness cannot take input mid-turn)")
	}
	return nil
}

// firstAllowOption is the option a plain "yes" picks: the first allow-kind
// option, else the first option.
func firstAllowOption(ap *proto.Approval) string {
	for _, o := range ap.Options {
		if strings.HasPrefix(string(o.Kind), "allow") {
			return o.ID
		}
	}
	if len(ap.Options) > 0 {
		return ap.Options[0].ID
	}
	return ""
}

// transcriptRenderer turns transcript records into a readable conversation:
// prompts, the agent's text as it streams, tool calls with their outcome,
// plans, permission requests, the harness's stderr and exits.
type transcriptRenderer struct {
	out, err io.Writer
	o        watchOptions
	// midLine is true while agent text is streaming without a trailing
	// newline; anything else printed first ends the line.
	midLine bool
	tools   map[string]string // tool call id -> title
}

func newTranscriptRenderer(out, err io.Writer, o watchOptions) *transcriptRenderer {
	return &transcriptRenderer{out: out, err: err, o: o, tools: map[string]string{}}
}

func (r *transcriptRenderer) flush() {
	if r.midLine {
		fmt.Fprintln(r.out)
		r.midLine = false
	}
}

func (r *transcriptRenderer) note(format string, args ...any) {
	r.flush()
	fmt.Fprintf(r.err, format+"\n", args...)
}

func (r *transcriptRenderer) gap(from, to uint64) {
	r.note("[transcript records %d..%d were evicted]", from, to-1)
}

func (r *transcriptRenderer) status(a *proto.Agent) {
	if r.o.raw {
		return
	}
	switch a.Status {
	case proto.AgentWaitingInput:
		if r.o.interactive {
			r.note("[waiting for input — type a message]")
		} else {
			r.note("[waiting for input: remount agent message %s -- …]", a.ID)
		}
	case proto.AgentWaitingApproval, proto.AgentRunning:
	case proto.AgentFailed:
		r.note("[failed: %s]", a.StatusReason)
	case proto.AgentFinished:
		r.note("[finished: %s]", a.StatusReason)
	case proto.AgentScheduled:
		r.note("[scheduled: starts %s]", time.UnixMilli(a.Policy.StartAt).Local().Format(time.RFC3339))
	default:
		r.note("[%s]", a.Status)
	}
}

func (r *transcriptRenderer) approval(ap *proto.Approval, interactive bool) {
	if r.o.raw {
		return
	}
	r.flush()
	fmt.Fprintf(r.err, "? approval %s (%s): %s\n", ap.ID, ap.Kind, ap.Title)
	if len(ap.Options) > 0 {
		var opts []string
		for _, o := range ap.Options {
			opts = append(opts, fmt.Sprintf("%s (%s)", o.ID, o.Name))
		}
		fmt.Fprintf(r.err, "  options: %s\n", strings.Join(opts, ", "))
	}
	if interactive {
		fmt.Fprintln(r.err, "  answer: yes | no | OPTION")
	} else {
		fmt.Fprintf(r.err, "  decide: remount approve %s [--option X | --deny]\n", ap.ID)
	}
}

func (r *transcriptRenderer) record(rec *proto.TranscriptRecord) {
	if r.o.raw {
		r.flush()
		enc := json.NewEncoder(r.out)
		_ = enc.Encode(rawRecord(rec))
		return
	}
	switch rec.Stream {
	case proto.StreamStderr:
		if r.o.showStderr {
			r.flush()
			text := strings.TrimRight(string(rec.Data), "\n")
			if text != "" {
				fmt.Fprintf(r.err, "  | %s\n", strings.ReplaceAll(text, "\n", "\n  | "))
			}
		}
	case proto.StreamStdout:
		// In ACP mode stdout is the protocol channel and arrives as frames;
		// a PTY-mode transcript is terminal output.
		r.flush()
		_, _ = r.out.Write(rec.Data)
	case proto.StreamExit:
		var e proto.ExitInfo
		if err := proto.Unmarshal(rec.Data, &e); err == nil {
			r.note("[harness exited: %s]", exitInfoText(&e))
		}
	case proto.StreamGap:
		r.note("[output gap inside run %s]", rec.Run)
	case proto.StreamACPIn, proto.StreamACPOut:
		var f proto.ACPFrameRecord
		if err := proto.Unmarshal(rec.Data, &f); err != nil {
			return
		}
		if f.Replayed {
			return
		}
		r.frame(rec.Stream, &f)
	}
}

// rawRecord is a transcript record with ACP frames decoded for --raw/--json.
func rawRecord(rec *proto.TranscriptRecord) map[string]any {
	m := map[string]any{"index": rec.Index, "run": rec.Run, "seq": rec.Seq, "stream": rec.Stream, "at": rec.At}
	switch rec.Stream {
	case proto.StreamACPIn, proto.StreamACPOut:
		var f proto.ACPFrameRecord
		if err := proto.Unmarshal(rec.Data, &f); err == nil {
			m["frame"] = f.Frame
			if f.Redacted {
				m["redacted"] = true
			}
			if f.Truncated {
				m["truncated"] = true
			}
			if f.Replayed {
				m["replayed"] = true
			}
			return m
		}
	case proto.StreamExit:
		var e proto.ExitInfo
		if err := proto.Unmarshal(rec.Data, &e); err == nil {
			m["exit"] = e
			return m
		}
	case proto.StreamInfo, proto.StreamGap:
		var v any
		if err := proto.Unmarshal(rec.Data, &v); err == nil {
			m["data"] = v
			return m
		}
	}
	m["text"] = string(rec.Data)
	return m
}

// jsonrpcFrame is the envelope shape of an ACP line; only the fields the
// renderer looks at.
type jsonrpcFrame struct {
	Method string          `json:"method"`
	Params json.RawMessage `json:"params"`
	Result json.RawMessage `json:"result"`
	Error  *struct {
		Code    int    `json:"code"`
		Message string `json:"message"`
	} `json:"error"`
}

func (r *transcriptRenderer) frame(stream uint8, f *proto.ACPFrameRecord) {
	var env jsonrpcFrame
	if err := json.Unmarshal(f.Frame, &env); err != nil {
		if f.Truncated {
			r.note("[frame truncated]")
		}
		return
	}
	if stream == proto.StreamACPOut {
		switch env.Method {
		case acp.MethodSessionPrompt:
			var req acp.PromptRequest
			if json.Unmarshal(env.Params, &req) == nil {
				r.flush()
				fmt.Fprintf(r.out, "\n> %s\n\n", strings.ReplaceAll(blocksText(req.Prompt), "\n", "\n> "))
			}
		case acp.MethodSessionCancel:
			r.note("[cancel requested]")
		}
		return
	}
	switch env.Method {
	case acp.MethodSessionUpdate:
		var n acp.SessionNotification
		if json.Unmarshal(env.Params, &n) == nil {
			r.update(n.Update)
		}
	case acp.MethodSessionRequestPermission:
		var req acp.RequestPermissionRequest
		if json.Unmarshal(env.Params, &req) == nil {
			title := r.tools[string(req.ToolCall.ToolCallID)]
			if req.ToolCall.Title != nil && *req.ToolCall.Title != "" {
				title = *req.ToolCall.Title
			}
			r.note("? permission: %s", title)
		}
	case "":
		// A response. The prompt response carries the stop reason; anything
		// but end_turn is worth a line.
		if env.Error != nil {
			r.note("[harness error %d: %s]", env.Error.Code, env.Error.Message)
			return
		}
		var res acp.PromptResponse
		if json.Unmarshal(env.Result, &res) == nil && res.StopReason != "" {
			r.flush()
			if res.StopReason != acp.StopReasonEndTurn {
				r.note("[turn ended: %s]", res.StopReason)
			}
		}
	}
}

func (r *transcriptRenderer) update(u acp.SessionUpdate) {
	switch u.Kind {
	case acp.SessionUpdateKindAgentMessageChunk:
		c, err := u.AsAgentMessageChunk()
		if err != nil {
			return
		}
		text := blockText(c.Content)
		if text == "" {
			return
		}
		_, _ = io.WriteString(r.out, text)
		r.midLine = !strings.HasSuffix(text, "\n")
	case acp.SessionUpdateKindAgentThoughtChunk:
		if !r.o.thoughts {
			return
		}
		c, err := u.AsAgentThoughtChunk()
		if err != nil {
			return
		}
		if text := blockText(c.Content); text != "" {
			r.flush()
			fmt.Fprintf(r.err, "  ~ %s\n", strings.TrimRight(text, "\n"))
		}
	case acp.SessionUpdateKindToolCall:
		tc, err := u.AsToolCall()
		if err != nil {
			return
		}
		r.tools[string(tc.ToolCallID)] = tc.Title
		r.flush()
		kind := ""
		if tc.Kind != "" {
			kind = " (" + string(tc.Kind) + ")"
		}
		fmt.Fprintf(r.err, "  * %s%s\n", tc.Title, kind)
	case acp.SessionUpdateKindToolCallUpdate:
		tu, err := u.AsToolCallUpdate()
		if err != nil || tu.Status == nil {
			return
		}
		title := r.tools[string(tu.ToolCallID)]
		if tu.Title != nil && *tu.Title != "" {
			title = *tu.Title
			r.tools[string(tu.ToolCallID)] = title
		}
		switch *tu.Status {
		case acp.ToolCallStatusCompleted:
			r.flush()
			fmt.Fprintf(r.err, "  = %s: done\n", title)
		case acp.ToolCallStatusFailed:
			r.flush()
			fmt.Fprintf(r.err, "  = %s: failed\n", title)
		}
	case acp.SessionUpdateKindPlan:
		p, err := u.AsPlan()
		if err != nil {
			return
		}
		r.flush()
		fmt.Fprintln(r.err, "  plan:")
		for _, e := range p.Entries {
			mark := " "
			switch e.Status {
			case acp.PlanEntryStatusInProgress:
				mark = ">"
			case acp.PlanEntryStatusCompleted:
				mark = "x"
			}
			fmt.Fprintf(r.err, "    [%s] %s\n", mark, e.Content)
		}
	}
}

// blocksText joins the text of content blocks.
func blocksText(blocks []acp.ContentBlock) string {
	var b strings.Builder
	for _, blk := range blocks {
		b.WriteString(blockText(blk))
	}
	return b.String()
}

// blockText renders one content block: text verbatim, anything else as a
// placeholder naming its kind.
func blockText(blk acp.ContentBlock) string {
	switch blk.Kind {
	case acp.ContentBlockKindText:
		t, err := blk.AsText()
		if err != nil {
			return ""
		}
		return t.Text
	case acp.ContentBlockKindResourceLink:
		l, err := blk.AsResourceLink()
		if err != nil {
			return ""
		}
		return "[" + l.URI + "]"
	case "":
		return ""
	}
	return "[" + blk.Kind + "]"
}

func exitInfoText(e *proto.ExitInfo) string {
	if e.Error != "" {
		return e.Error
	}
	if e.Signal != "" {
		return "signal " + e.Signal
	}
	return "exit status " + strconv.Itoa(e.Code)
}
