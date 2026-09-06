package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	"golang.org/x/term"

	"remount.dev/remount/internal/client"
	"remount.dev/remount/internal/launch"
	"remount.dev/remount/internal/localfs"
	"remount.dev/remount/internal/proto"
)

// ---------------------------------------------------------------------------
// run: seed a workspace, install a harness, launch it against brokered keys
// ---------------------------------------------------------------------------

const runUsage = "run RECIPE [--auth subscription|api-key] [--dir PATH | --base NAME | --repo URL[@REF] | --ws WS] [--binding ID[:PRESET]]... [--security P] [--sandbox M] [--approve M] [--mount-path /abs] [--detach] [--pty] [--sleep-after DUR] [--max-turns N] [--parent ID] [--at TIME | --sleep-until HH:MM] -- TASK…\n    run RECIPE --queue FILE [--sleep-after DUR | --sleep-until HH:MM] | --queue-continue QUEUE"

func cmdRun(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("run", flag.ExitOnError)
	var c common
	c.flags(fs)
	dir := fs.String("dir", "", "seed the workspace from this local directory (gitignore-aware)")
	// Unlike ws create, .git is kept by default: the harness is expected to
	// commit its work.
	includeGit := fs.Bool("include-git", true, "with --dir, include the .git directory")
	repo := fs.String("repo", "", "clone this repository into a fresh workspace: URL[@REF]; the node clones through the broker")
	repoDepth := fs.Int("repo-depth", 0, "with --repo, shallow-clone depth (0 = full history)")
	base := fs.String("base", "", "seed from a named base (remount base ls)")
	wsID := fs.String("ws", "", "run in an existing workspace")
	image := fs.String("image", "", "container image (default: the recipe's)")
	backend := fs.String("backend", "", "require a node backend (docker, process)")
	name := fs.String("name", "", "workspace name")
	model := fs.String("model", "", "model id passed to the harness")
	auth := fs.String("auth", "", "subscription | api-key (required by recipes that support both explicitly)")
	security := fs.String("security", "", "local | isolated | multi_tenant (default local)")
	sandbox := fs.String("sandbox", launch.SandboxWorkspaceWrite, "read-only | workspace-write | full")
	approve := fs.String("approve", launch.ApproveNever, "never | on-request")
	detach := fs.Bool("detach", false, "print ids and return; attach later with remount attach")
	resume := fs.Bool("resume", false, "resume the harness's previous conversation in this workspace")
	recipeFile := fs.String("recipe-file", "", "load the recipe from this YAML file instead of a built-in")
	timeout := fs.Duration("timeout", 0, "server-side session timeout (0 = none)")
	killOnInterrupt := fs.Bool("kill-on-interrupt", false, "Ctrl-C kills the harness instead of detaching")
	pty := fs.Bool("pty", false, "run the harness's own terminal UI in a session instead of a durable ACP agent (recipes with acp)")
	maxTurns := fs.Int("max-turns", 0, "agent mode: finish after this many turns (0 = unlimited)")
	mountPath := fs.String("mount-path", "", "absolute path the tree appears at inside the workspace (default /work; needs docker)")
	queueFile := fs.String("queue", "", "run the tasks listed in this file one after another (ADR 0041); - reads stdin")
	queueContinue := fs.String("queue-continue", "", "continue an existing queue where it stopped")
	sleepAfter := fs.Duration("sleep-after", 0, "with --queue, sleep the workspace this long between tasks")
	sleepUntil := fs.String("sleep-until", "", "with --queue, sleep the workspace until this local HH:MM between tasks; for an agent, hold its first run until then")
	parent := fs.String("parent", "", "agent mode: create a child of this agent (inherits bindings and policy caps)")
	at := fs.String("at", "", "agent mode: hold the first run until this time (RFC 3339, HH:MM or a duration)")
	var bindings, exclude listFlag
	fs.Var(&bindings, "binding", "provider binding ID[:PRESET][?host=…] (repeatable)")
	fs.Var(&exclude, "exclude", "snapshot exclude glob (repeatable)")
	parse(fs, args)
	if fs.NArg() == 0 || isHelp(fs.Arg(0)) {
		return fmt.Errorf("%s\nrecipes: %s\npresets: remount binding preset ls", runUsage, strings.Join(launch.Builtin(), ", "))
	}
	if *timeout < 0 {
		return errors.New("--timeout must not be negative")
	}
	rest := fs.Args()[1:]
	if len(rest) > 0 && rest[0] == "--" {
		rest = rest[1:]
	}
	queued := *queueFile != "" || *queueContinue != ""
	if *queueFile != "" && *queueContinue != "" {
		return errors.New("--queue and --queue-continue are mutually exclusive")
	}
	if *sleepAfter != 0 && *sleepUntil != "" {
		return errors.New("--sleep-after and --sleep-until are mutually exclusive")
	}
	if *sleepAfter < 0 {
		return errors.New("--sleep-after must not be negative")
	}
	if *sleepUntil != "" {
		if _, err := launch.NextWallClock(time.Now(), *sleepUntil); err != nil {
			return err
		}
	}
	if *sleepUntil != "" && *at != "" {
		return errors.New("--sleep-until and --at are the same thing for an agent; pass one")
	}
	if queued && (*detach || *resume) {
		return errors.New("--queue runs in the foreground; --detach and --resume do not apply")
	}
	if queued && len(rest) > 0 {
		return errors.New("--queue takes its tasks from the file; nothing may follow --")
	}

	recipe, recipeYAML, err := loadRecipe(fs.Arg(0), *recipeFile)
	if err != nil {
		return err
	}
	agentMode := recipe.Mode() == launch.ModeACP && !queued && !*resume && !*pty
	if !agentMode {
		if !queued && *sleepUntil != "" {
			return errors.New("--sleep-until needs --queue or an agent (acp) recipe")
		}
		if *parent != "" || *at != "" {
			return errors.New("--parent and --at apply to agents: an acp recipe without --queue, --resume or --pty")
		}
	}

	// An ACP-capable recipe runs as a durable Agent: the conversation lives in
	// the control plane and survives this process, node loss and sleep.
	if agentMode {
		startAt := *at
		if *sleepUntil != "" {
			startAt = *sleepUntil
		}
		seed := agentSeedFlags{
			dir: *dir, repo: *repo, base: *base, ws: *wsID, image: *image, backend: *backend, name: *name, model: *model,
			security: *security, sandbox: *sandbox, approve: *approve, mountPath: *mountPath, recipeFile: *recipeFile,
			auth:       normalizeAuthFlag(*auth),
			includeGit: *includeGit, repoDepth: *repoDepth, bindings: bindings, exclude: exclude,
			sleepAfter: *sleepAfter, maxTurns: *maxTurns, parent: *parent, at: startAt,
		}
		if *timeout != 0 {
			return errors.New("--timeout applies to sessions; agents finish by --max-turns or remount agent cancel")
		}
		p, err := planAgent(ctx, &c, &seed, recipe, recipeYAML, strings.Join(rest, " "))
		if err != nil {
			return err
		}
		if _, err := c.ensureLocalServer(ctx); err != nil {
			return err
		}
		cl := c.client()
		defer cl.Close()
		a, err := p.create(ctx, cl, nil, "")
		if err != nil {
			return err
		}
		if p.plan.Auth == launch.AuthWorkspaceResident || p.plan.Auth == launch.AuthSubscription {
			fmt.Fprintf(os.Stderr, "note: %s will use its own login kept inside workspace %s (no provider binding given)\n", recipe.Name, a.WS)
		}
		if *detach {
			if c.json {
				printJSON(a)
				return nil
			}
			fmt.Printf("%s %s\n", a.ID, a.WS)
			fmt.Fprintf(os.Stderr, "watch: remount agent watch %s\nevents: remount events --ws %s --follow\n", a.ID, a.WS)
			return nil
		}
		fmt.Fprintf(os.Stderr, "agent %s in %s (Ctrl-C detaches; resume with: remount agent watch %s)\n", a.ID, a.WS, a.ID)
		return watchAgent(ctx, cl, a.ID, 0, watchOptions{follow: true, interactive: term.IsTerminal(int(os.Stdin.Fd())), showStderr: true})
	}
	if !queued && *sleepAfter != 0 {
		return errors.New("--sleep-after needs --queue or an acp recipe")
	}
	if *maxTurns != 0 {
		return errors.New("--max-turns applies to agents (recipes with acp)")
	}
	o := launch.Options{
		Recipe: recipe, WS: *wsID, Base: *base, Name: *name, Image: *image, Backend: *backend,
		Auth: normalizeAuthFlag(*auth), Security: *security, Sandbox: *sandbox, Approve: *approve, Model: *model, Exclude: exclude, Resume: *resume,
		Timeout: *timeout, Stderr: os.Stderr, MountPath: *mountPath,
	}
	if queued {
		if recipe.CommandFromArgs {
			return fmt.Errorf("recipe %s takes its command from the arguments and cannot run a queue", recipe.Name)
		}
	} else if recipe.CommandFromArgs {
		o.Args = rest
	} else {
		o.Task = strings.Join(rest, " ")
	}
	for _, spec := range bindings {
		b, err := launch.ParseBinding(spec)
		if err != nil {
			return err
		}
		o.Bindings = append(o.Bindings, b)
	}
	if *dir != "" && *wsID != "" {
		return errors.New("--dir and --ws are mutually exclusive")
	}
	if *dir != "" && *base != "" {
		return errors.New("--dir and --base are mutually exclusive")
	}
	if *repo != "" {
		if *dir != "" {
			return errors.New("--dir and --repo are mutually exclusive")
		}
		r, err := proto.ParseRepoFlag(*repo, *repoDepth)
		if err != nil {
			return err
		}
		o.Repo = r
	} else if *repoDepth != 0 {
		return errors.New("--repo-depth needs --repo")
	}
	stdinTTY := term.IsTerminal(int(os.Stdin.Fd()))
	usePTY := !*detach && stdinTTY && term.IsTerminal(int(os.Stdout.Fd()))
	o.PTY = usePTY
	// Piped input is forwarded; a terminal on a non-pty run is not, so the
	// harness sees EOF instead of a pipe nobody writes to.
	o.Stdin = !*detach && !stdinTTY
	if usePTY {
		if w, h, err := term.GetSize(int(os.Stdout.Fd())); err == nil {
			o.Cols, o.Rows = uint16(w), uint16(h)
		}
	}
	var tasks []string
	if *queueFile != "" {
		tasks, err = readQueueFile(*queueFile)
		if err != nil {
			return err
		}
	}
	if o.Auth == "" {
		if o.Bindings, err = defaultBindings(recipe, o.Bindings, c.localBindings(ctx)); err != nil {
			return err
		}
	}
	// Fail on flag errors before uploading anything.
	probe := o
	if queued {
		probe.Task = "probe"
		if len(tasks) > 0 {
			probe.Task = tasks[0]
		}
	}
	if _, err := probe.Validate(); err != nil {
		return err
	}
	if _, err := c.ensureLocalServer(ctx); err != nil {
		return err
	}
	cl := c.client()
	defer cl.Close()
	if *dir != "" {
		id, _, err := uploadDir(ctx, cl, *dir, localfs.PackOptions{ExcludeGit: !*includeGit, Excludes: exclude})
		if err != nil {
			return err
		}
		o.RestoreFrom = id
	}
	if queued {
		return runQueue(ctx, cl, c, o, tasks, *queueContinue, *sleepAfter, *sleepUntil)
	}
	res, err := launch.Start(ctx, cl, o)
	if err != nil {
		if res != nil && res.Workspace != nil {
			fmt.Fprintf(os.Stderr, "workspace %s kept for inspection: remount inspect %s\n", res.Workspace.ID, res.Workspace.ID)
		}
		return err
	}
	ws, s := res.Workspace, res.Session
	if res.Auth == launch.AuthWorkspaceResident || res.Auth == launch.AuthSubscription {
		fmt.Fprintf(os.Stderr, "note: %s will use its own login kept inside workspace %s (no provider binding given)\n", recipe.Name, ws.ID)
	}
	if *detach {
		if c.json {
			printJSON(map[string]any{"ws": ws.ID, "session": s.ID, "recipe": recipe.Name, "auth": res.Auth, "task_hash": res.Run.TaskHash})
			return nil
		}
		fmt.Printf("%s %s\n", ws.ID, s.ID)
		fmt.Fprintf(os.Stderr, "attach: remount attach %s %s\nevents: remount events --ws %s --follow\n", ws.ID, s.ID, ws.ID)
		return nil
	}
	fmt.Fprintf(os.Stderr, "session %s (Ctrl-C detaches; reattach with: remount attach %s %s)\n", s.ID, ws.ID, s.ID)
	return drive(ctx, s, driveOptions{WS: ws.ID, Session: s.ID, Raw: usePTY, ForwardStdin: usePTY || o.Stdin, KillOnInterrupt: *killOnInterrupt})
}

// The binding command moved to cmd_binding.go, where `preset ls` sits beside
// the create, rotate and revoke operations that share its flag vocabulary.

// readQueueFile parses the task list at path, or stdin for "-".
func readQueueFile(path string) ([]string, error) {
	var r io.Reader = os.Stdin
	if path != "-" {
		f, err := os.Open(path)
		if err != nil {
			return nil, err
		}
		defer f.Close()
		r = f
	}
	tasks, err := launch.ParseQueueFile(r)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", path, err)
	}
	return tasks, nil
}

// runQueue drives `remount run --queue`: the tasks come from a file, the
// cursor lives on the control plane, and the exit status is the first
// failing task's.
func runQueue(ctx context.Context, cl *client.Client, c common, o launch.Options, tasks []string, cont string, sleepAfter time.Duration, sleepUntil string) error {
	o.PTY, o.Stdin = false, false
	qo := launch.QueueOptions{Tasks: tasks, Continue: cont, SleepAfter: sleepAfter, SleepUntil: sleepUntil, Run: o, Output: os.Stdout}
	res, err := launch.RunQueue(ctx, cl, qo)
	if res != nil && res.Queue != nil {
		if c.json {
			printJSON(res.Queue)
		} else if res.Workspace != nil {
			fmt.Fprintf(os.Stderr, "queue %s: %s (%d/%d) in %s\n", res.Queue.ID, res.Queue.Status, res.Queue.Cursor, len(res.Queue.Items), res.Workspace.ID)
		}
	}
	return err
}
