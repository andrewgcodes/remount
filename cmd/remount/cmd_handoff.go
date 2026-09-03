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
)

// ---------------------------------------------------------------------------
// handoff: move this checkout and the harness's conversation into a workspace
// ---------------------------------------------------------------------------

const handoffUsage = "handoff [--recipe R] [--task \"continue\"] [--dir .] [--home ~] [--binding ID[:PRESET]]... [--security P] [--sandbox M] [--approve M] [--name N] [--backend B] [--image IMG] [--attach]"

func cmdHandoff(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("handoff", flag.ExitOnError)
	var c common
	c.flags(fs)
	recipeName := fs.String("recipe", "", "harness recipe; default: detected from the state under --home")
	task := fs.String("task", "", "prompt for the resumed harness (default: \""+launch.DefaultResumeTask+"\")")
	dir := fs.String("dir", ".", "local checkout to hand off")
	home := fs.String("home", "", "where the harness keeps its state locally (default: your home directory)")
	includeGit := fs.Bool("include-git", true, "include the .git directory")
	image := fs.String("image", "", "container image (default: the recipe's)")
	backend := fs.String("backend", "", "require a node backend (docker, process)")
	name := fs.String("name", "", "workspace name")
	model := fs.String("model", "", "model id passed to the harness")
	security := fs.String("security", "", "local | isolated | multi_tenant (default local)")
	sandbox := fs.String("sandbox", launch.SandboxWorkspaceWrite, "read-only | workspace-write | full")
	approve := fs.String("approve", launch.ApproveNever, "never | on-request")
	attach := fs.Bool("attach", false, "stay attached to the resumed harness instead of printing ids")
	timeout := fs.Duration("timeout", 0, "server-side session timeout (0 = none)")
	killOnInterrupt := fs.Bool("kill-on-interrupt", false, "with --attach, Ctrl-C kills the harness instead of detaching")
	var bindings, exclude listFlag
	fs.Var(&bindings, "binding", "provider binding ID[:PRESET][?host=…] (repeatable)")
	fs.Var(&exclude, "exclude", "upload exclude glob (repeatable)")
	parse(fs, args)
	if fs.NArg() > 0 && isHelp(fs.Arg(0)) {
		return errors.New(handoffUsage)
	}
	if err := arity(fs, 0, 0, handoffUsage); err != nil {
		return err
	}
	if *timeout < 0 {
		return errors.New("--timeout must not be negative")
	}
	o := launch.HandoffOptions{
		Dir: *dir, Home: *home, ExcludeGit: !*includeGit,
		Run: launch.Options{
			Task: *task, Name: *name, Image: *image, Backend: *backend, Security: *security, Sandbox: *sandbox,
			Approve: *approve, Model: *model, Exclude: exclude, Timeout: *timeout, Stderr: os.Stderr,
		},
	}
	if *recipeName != "" {
		r, err := launch.Load(*recipeName)
		if err != nil {
			return err
		}
		o.Recipe = r
	}
	for _, spec := range bindings {
		b, err := launch.ParseBinding(spec)
		if err != nil {
			return err
		}
		o.Run.Bindings = append(o.Run.Bindings, b)
	}
	stdinTTY := term.IsTerminal(int(os.Stdin.Fd()))
	pty := *attach && stdinTTY && term.IsTerminal(int(os.Stdout.Fd()))
	o.Run.PTY = pty
	o.Run.Stdin = *attach && !stdinTTY
	if pty {
		if w, h, err := term.GetSize(int(os.Stdout.Fd())); err == nil {
			o.Run.Cols, o.Run.Rows = uint16(w), uint16(h)
		}
	}
	cl := c.client()
	defer cl.Close()
	res, err := launch.Handoff(ctx, cl, o)
	if err != nil {
		if res != nil && res.Result != nil && res.Workspace != nil {
			fmt.Fprintf(os.Stderr, "workspace %s kept for inspection: remount inspect %s\n", res.Workspace.ID, res.Workspace.ID)
		}
		return err
	}
	ws, s := res.Workspace, res.Session
	if c.json {
		printJSON(map[string]any{
			"ws": ws.ID, "session": s.ID, "recipe": res.Recipe.Name, "artifact": res.Artifact,
			"state_carried": res.StateCarried, "state_missing": res.StateMissing, "mount_path": res.MountPath,
			"files": res.Manifest.Files, "bytes": res.Manifest.Bytes,
		})
		if !*attach {
			return nil
		}
	} else {
		where := "/work"
		if res.MountPath != "" {
			where = res.MountPath
		}
		fmt.Fprintf(os.Stderr, "handed off %s (%d files) with %s state %s to %s at %s\n",
			*dir, res.Manifest.Files, res.Recipe.Name, strings.Join(res.StateCarried, ","), ws.ID, where)
		fmt.Fprintf(os.Stderr, "attach: remount attach %s %s\nresume later: remount resume %s\nbring it back: remount pull %s\n", ws.ID, s.ID, ws.ID, ws.ID)
		if !*attach {
			fmt.Printf("%s %s\n", ws.ID, s.ID)
			return nil
		}
	}
	return drive(ctx, s, driveOptions{WS: ws.ID, Session: s.ID, Raw: pty, ForwardStdin: pty || o.Run.Stdin, KillOnInterrupt: *killOnInterrupt})
}

// ---------------------------------------------------------------------------
// resume: pick a handed-off or run workspace back up
// ---------------------------------------------------------------------------

const resumeUsage = "resume WS [--task \"continue\"] [--recipe R] [--detach] [--no-attach]"

func cmdResume(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("resume", flag.ExitOnError)
	var c common
	c.flags(fs)
	recipeName := fs.String("recipe", "", "harness recipe (default: the one recorded on the workspace)")
	task := fs.String("task", "", "prompt for the resumed harness (default: \""+launch.DefaultResumeTask+"\")")
	sandbox := fs.String("sandbox", launch.SandboxWorkspaceWrite, "read-only | workspace-write | full")
	approve := fs.String("approve", launch.ApproveNever, "never | on-request")
	detach := fs.Bool("detach", false, "print ids and return")
	noAttach := fs.Bool("no-attach", false, "start a new harness session even if one is running")
	timeout := fs.Duration("timeout", 0, "server-side session timeout (0 = none)")
	killOnInterrupt := fs.Bool("kill-on-interrupt", false, "Ctrl-C kills the harness instead of detaching")
	parse(fs, args)
	if err := arity(fs, 1, 1, resumeUsage); err != nil {
		return err
	}
	if *timeout < 0 {
		return errors.New("--timeout must not be negative")
	}
	o := launch.ResumeOptions{
		WS: fs.Arg(0), Task: *task, Attach: !*noAttach, Sandbox: *sandbox, Approve: *approve,
		Timeout: *timeout, Stderr: os.Stderr,
	}
	if *recipeName != "" {
		r, err := launch.Load(*recipeName)
		if err != nil {
			return err
		}
		o.Recipe = r
	}
	stdinTTY := term.IsTerminal(int(os.Stdin.Fd()))
	pty := !*detach && stdinTTY && term.IsTerminal(int(os.Stdout.Fd()))
	o.PTY = pty
	o.Stdin = !*detach && !stdinTTY
	if pty {
		if w, h, err := term.GetSize(int(os.Stdout.Fd())); err == nil {
			o.Cols, o.Rows = uint16(w), uint16(h)
		}
	}
	cl := c.client()
	defer cl.Close()
	res, err := launch.Resume(ctx, cl, o)
	if err != nil {
		return err
	}
	ws, s := res.Workspace, res.Session
	if *detach {
		if c.json {
			printJSON(map[string]any{"ws": ws.ID, "session": s.ID, "recipe": res.Recipe.Name, "attached": res.Attached})
			return nil
		}
		fmt.Printf("%s %s\n", ws.ID, s.ID)
		fmt.Fprintf(os.Stderr, "attach: remount attach %s %s\n", ws.ID, s.ID)
		return nil
	}
	if !res.Attached {
		fmt.Fprintf(os.Stderr, "session %s (Ctrl-C detaches; reattach with: remount attach %s %s)\n", s.ID, ws.ID, s.ID)
	}
	// A joined session was opened by someone else; its pty-ness is unknown,
	// so drive it the way attach does.
	raw := pty
	if res.Attached {
		raw = stdinTTY
	}
	return drive(ctx, s, driveOptions{WS: ws.ID, Session: s.ID, Raw: raw, ForwardStdin: raw || o.Stdin || res.Attached, KillOnInterrupt: *killOnInterrupt})
}
