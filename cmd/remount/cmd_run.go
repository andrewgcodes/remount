package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"strings"

	"golang.org/x/term"

	"remount.dev/remount/internal/launch"
	"remount.dev/remount/internal/localfs"
)

// ---------------------------------------------------------------------------
// run: seed a workspace, install a harness, launch it against brokered keys
// ---------------------------------------------------------------------------

const runUsage = "run RECIPE [--dir PATH | --base NAME | --ws WS] [--binding ID[:PRESET]]... [--security P] [--sandbox M] [--approve M] [--detach] -- TASK…"

func cmdRun(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("run", flag.ExitOnError)
	var c common
	c.flags(fs)
	dir := fs.String("dir", "", "seed the workspace from this local directory (gitignore-aware)")
	// Unlike ws create, .git is kept by default: the harness is expected to
	// commit its work.
	includeGit := fs.Bool("include-git", true, "with --dir, include the .git directory")
	repo := fs.String("repo", "", "seed from a git URL (needs the git connector)")
	base := fs.String("base", "", "seed from a named base (remount base ls)")
	wsID := fs.String("ws", "", "run in an existing workspace")
	image := fs.String("image", "", "container image (default: the recipe's)")
	backend := fs.String("backend", "", "require a node backend (docker, process)")
	name := fs.String("name", "", "workspace name")
	model := fs.String("model", "", "model id passed to the harness")
	security := fs.String("security", "", "local | isolated | multi_tenant (default local)")
	sandbox := fs.String("sandbox", launch.SandboxWorkspaceWrite, "read-only | workspace-write | full")
	approve := fs.String("approve", launch.ApproveNever, "never | on-request")
	detach := fs.Bool("detach", false, "print ids and return; attach later with remount attach")
	resume := fs.Bool("resume", false, "resume the harness's previous conversation in this workspace")
	recipeFile := fs.String("recipe-file", "", "load the recipe from this YAML file instead of a built-in")
	timeout := fs.Duration("timeout", 0, "server-side session timeout (0 = none)")
	killOnInterrupt := fs.Bool("kill-on-interrupt", false, "Ctrl-C kills the harness instead of detaching")
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

	var recipe *launch.Recipe
	var err error
	if *recipeFile != "" {
		src, rerr := os.ReadFile(*recipeFile)
		if rerr != nil {
			return rerr
		}
		recipe, err = launch.Parse(src)
		if err != nil {
			return fmt.Errorf("%s: %w", *recipeFile, err)
		}
		if recipe.Name != fs.Arg(0) {
			return fmt.Errorf("%s declares recipe %q; the command names %q", *recipeFile, recipe.Name, fs.Arg(0))
		}
	} else {
		recipe, err = launch.Load(fs.Arg(0))
		if err != nil {
			return err
		}
	}

	o := launch.Options{
		Recipe: recipe, WS: *wsID, Base: *base, Repo: *repo, Name: *name, Image: *image, Backend: *backend,
		Security: *security, Sandbox: *sandbox, Approve: *approve, Model: *model, Exclude: exclude, Resume: *resume,
		Timeout: *timeout, Stderr: os.Stderr,
	}
	if recipe.CommandFromArgs {
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
	stdinTTY := term.IsTerminal(int(os.Stdin.Fd()))
	pty := !*detach && stdinTTY && term.IsTerminal(int(os.Stdout.Fd()))
	o.PTY = pty
	// Piped input is forwarded; a terminal on a non-pty run is not, so the
	// harness sees EOF instead of a pipe nobody writes to.
	o.Stdin = !*detach && !stdinTTY
	if pty {
		if w, h, err := term.GetSize(int(os.Stdout.Fd())); err == nil {
			o.Cols, o.Rows = uint16(w), uint16(h)
		}
	}
	// Fail on flag errors before uploading anything.
	if _, err := o.Validate(); err != nil {
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
	res, err := launch.Start(ctx, cl, o)
	if err != nil {
		if res != nil && res.Workspace != nil {
			fmt.Fprintf(os.Stderr, "workspace %s kept for inspection: remount inspect %s\n", res.Workspace.ID, res.Workspace.ID)
		}
		return err
	}
	ws, s := res.Workspace, res.Session
	if res.Auth == launch.AuthWorkspaceResident {
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
	return drive(ctx, s, driveOptions{WS: ws.ID, Session: s.ID, Raw: pty, ForwardStdin: pty || o.Stdin, KillOnInterrupt: *killOnInterrupt})
}

// ---------------------------------------------------------------------------
// binding preset ls
// ---------------------------------------------------------------------------

func cmdBinding(ctx context.Context, args []string) error {
	if len(args) < 2 || args[0] != "preset" || args[1] != "ls" {
		return errors.New("binding: preset ls")
	}
	fs := flag.NewFlagSet("binding preset ls", flag.ExitOnError)
	var c common
	c.flags(fs)
	parse(fs, args[2:])
	if err := arity(fs, 0, 0, "binding preset ls"); err != nil {
		return err
	}
	presets := launch.Presets()
	if c.json {
		printJSON(presets)
		return nil
	}
	tw := tabWriter()
	fmt.Fprintln(tw, "PRESET\tHOSTS\tKEY ENV\tBASE URL ENV\tHEADER\tNOTE")
	for _, p := range presets {
		hosts := strings.Join(p.Hosts, ",")
		if p.HostParam != "" {
			hosts += " (?host=" + p.HostParam + ")"
		}
		fmt.Fprintf(tw, "%s\t%s\t%s\t%s\t%s\t%s\n", p.Name, hosts, p.KeyEnv, p.BaseURLEnv, p.Header, p.Note)
	}
	tw.Flush()
	return nil
}
