package launch

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"remount.dev/remount/internal/client"
	"remount.dev/remount/internal/localfs"
	"remount.dev/remount/internal/proto"
)

// DefaultResumeTask is the prompt a resumed harness receives when the caller
// gives none.
const DefaultResumeTask = "Continue where you left off."

// HandoffOptions describe moving a local, in-progress harness conversation
// into a workspace (ADR 0041).
type HandoffOptions struct {
	// Dir is the local checkout; "" means the current directory.
	Dir string
	// Home is where the harness keeps its state locally; "" means the
	// user's home directory. Each recipe StateDir is read from here.
	Home string
	// Recipe is the harness; nil auto-detects from the state present in
	// Home among Candidates.
	Recipe *Recipe
	// Candidates are the recipes DetectRecipe considers; nil means every
	// built-in one.
	Candidates []*Recipe
	// ExcludeGit drops .git from the upload.
	ExcludeGit bool
	// Run carries everything else: bindings, security, sandbox, task,
	// timeout, PTY. Run.Task defaults to DefaultResumeTask; Run.WS, Dir,
	// RestoreFrom, Base and Recipe are set here and must be empty.
	Run Options
}

// HandoffResult is what Handoff did before starting the harness.
type HandoffResult struct {
	*Result
	Recipe *Recipe
	// Artifact is the uploaded checkout plus state.
	Artifact string
	// StateCarried lists the recipe state paths found locally and packed.
	StateCarried []string
	// StateMissing lists the ones absent locally; the harness starts fresh
	// if all are.
	StateMissing []string
	// MountPath is where the tree appears inside the workspace, or "" for
	// the default.
	MountPath string
	Manifest  localfs.Manifest
}

// DetectRecipe returns the single candidate whose state is present in home.
// Zero or several matches are errors that name the choices, because guessing
// would hand a conversation to the wrong harness.
func DetectRecipe(home string, candidates []*Recipe) (*Recipe, error) {
	if candidates == nil {
		for _, name := range Builtin() {
			r, err := Load(name)
			if err != nil {
				return nil, err
			}
			candidates = append(candidates, r)
		}
	}
	var found []*Recipe
	for _, r := range candidates {
		if len(r.ResumeCommand) == 0 {
			continue
		}
		for _, dir := range r.StateDirs {
			if _, err := os.Lstat(filepath.Join(home, filepath.FromSlash(dir))); err == nil {
				found = append(found, r)
				break
			}
		}
	}
	switch len(found) {
	case 1:
		return found[0], nil
	case 0:
		return nil, fmt.Errorf("no harness state found under %s; pass --recipe", home)
	}
	names := make([]string, 0, len(found))
	for _, r := range found {
		names = append(names, r.Name)
	}
	sort.Strings(names)
	return nil, fmt.Errorf("state for several harnesses found under %s (%s); pass --recipe", home, strings.Join(names, ", "))
}

// Handoff packs the checkout together with the harness's local state into
// one artifact, creates a workspace restored from it and starts the recipe's
// resume command there. A path-keyed recipe pins the workspace's mount path
// to the checkout's absolute path so the harness finds its state where it
// left it; that needs a node with a namespaced backend.
func Handoff(ctx context.Context, cl *client.Client, o HandoffOptions) (*HandoffResult, error) {
	if o.Run.WS != "" || o.Run.Dir != "" || o.Run.RestoreFrom != "" || o.Run.Base != "" || o.Run.Recipe != nil {
		return nil, errors.New("handoff derives the workspace from the checkout; --ws, --dir, --base and the recipe are set by Handoff")
	}
	dir := o.Dir
	if dir == "" {
		dir = "."
	}
	dir, err := filepath.Abs(dir)
	if err != nil {
		return nil, err
	}
	dir, err = filepath.EvalSymlinks(dir)
	if err != nil {
		return nil, err
	}
	if st, err := os.Stat(dir); err != nil {
		return nil, err
	} else if !st.IsDir() {
		return nil, fmt.Errorf("%s is not a directory", dir)
	}
	home := o.Home
	if home == "" {
		home, err = os.UserHomeDir()
		if err != nil {
			return nil, err
		}
	}
	r := o.Recipe
	if r == nil {
		r, err = DetectRecipe(home, o.Candidates)
		if err != nil {
			return nil, err
		}
	}
	if len(r.ResumeCommand) == 0 {
		return nil, fmt.Errorf("recipe %s has no resume_command; it cannot continue a conversation", r.Name)
	}
	if r.CommandFromArgs {
		return nil, fmt.Errorf("recipe %s takes its command from the arguments and has no conversation to hand off", r.Name)
	}
	stderr := o.Run.Stderr
	if stderr == nil {
		stderr = io.Discard
	}
	res := &HandoffResult{Recipe: r}

	var extra []localfs.ExtraTree
	for _, state := range r.StateDirs {
		local := filepath.Join(home, filepath.FromSlash(state))
		if st, err := os.Lstat(filepath.Join(dir, filepath.FromSlash(state))); err == nil && !st.IsDir() {
			// The checkout already carries this file (HOME is the workspace
			// root, so both names collapse); the checkout's copy wins.
			fmt.Fprintf(stderr, "note: %s exists in the checkout; the copy under %s is left out\n", state, home)
			continue
		}
		extra = append(extra, localfs.ExtraTree{Local: local, Archive: state})
	}
	var buf bytes.Buffer
	m, err := localfs.Pack(dir, localfs.PackOptions{ExcludeGit: o.ExcludeGit, Excludes: o.Run.Exclude, Extra: extra}, &buf)
	if err != nil {
		return nil, fmt.Errorf("pack %s: %w", dir, err)
	}
	res.Manifest = m
	missing := map[string]bool{}
	for _, name := range m.Missing {
		missing[name] = true
	}
	for _, e := range extra {
		if missing[e.Archive] {
			res.StateMissing = append(res.StateMissing, e.Archive)
		} else {
			res.StateCarried = append(res.StateCarried, e.Archive)
		}
	}
	for _, w := range m.Warnings {
		fmt.Fprintf(stderr, "warning: %s\n", w)
	}
	if len(res.StateCarried) == 0 {
		fmt.Fprintf(stderr, "warning: no %s state found under %s; the harness starts a new conversation\n", r.Name, home)
	}
	artifactID, size, err := cl.UploadArtifact(ctx, &buf)
	if err != nil {
		return nil, fmt.Errorf("upload: %w", err)
	}
	res.Artifact = artifactID
	fmt.Fprintf(stderr, "uploaded %d files (%d bytes) as %s\n", m.Files, size, artifactID)

	run := o.Run
	run.Recipe = r
	run.RestoreFrom = artifactID
	run.Resume = true
	if run.Task == "" {
		run.Task = DefaultResumeTask
	}
	if run.Labels == nil {
		run.Labels = map[string]string{}
	}
	run.Labels[LabelOrigin] = dir
	if r.PathKeyed {
		if run.MountPath != "" && run.MountPath != dir {
			return nil, fmt.Errorf("recipe %s keys its state on the working directory; the tree must stay at %s, not --mount-path %s", r.Name, dir, run.MountPath)
		}
		run.MountPath = dir
	}
	res.MountPath = run.MountPath
	if run.WaitClaimed <= 0 {
		run.WaitClaimed = 60 * time.Second
	}
	started, err := Start(ctx, cl, run)
	if started != nil {
		res.Result = started
	}
	return res, err
}

// ResumeOptions describe continuing a workspace's harness conversation.
type ResumeOptions struct {
	WS string
	// Task is the prompt for the resumed harness; "" means DefaultResumeTask.
	Task string
	// Recipe overrides the one recorded on the workspace.
	Recipe *Recipe
	// Attach, when true, reattaches to a live harness session instead of
	// starting a new one when one exists.
	Attach bool
	// Wait bounds how long a paused workspace may take to wake and be
	// claimed; zero means 60s.
	Wait time.Duration
	// The remaining fields are passed through to Start.
	PTY        bool
	Rows, Cols uint16
	Stdin      bool
	Timeout    time.Duration
	Stderr     io.Writer
	Security   string
	Sandbox    string
	Approve    string
}

// ResumeResult is either a reattached live session or a fresh resume run.
type ResumeResult struct {
	Workspace *proto.Workspace
	Session   *client.Session
	// Attached is true when an existing harness session was joined; Run is
	// nil in that case.
	Attached bool
	Run      *Result
	Recipe   *Recipe
}

// Resume attaches to the workspace's live harness session if there is one;
// otherwise it wakes the workspace when it is asleep and starts the recipe's
// resume command, rebuilding bindings and model from the labels Start wrote.
func Resume(ctx context.Context, cl *client.Client, o ResumeOptions) (*ResumeResult, error) {
	if o.WS == "" {
		return nil, errors.New("workspace id is required")
	}
	stderr := o.Stderr
	if stderr == nil {
		stderr = io.Discard
	}
	ws, err := cl.GetWorkspace(ctx, o.WS)
	if err != nil {
		return nil, err
	}
	switch ws.State {
	case proto.WSDestroyed, proto.WSDestroying:
		return nil, fmt.Errorf("workspace %s is %s", ws.ID, ws.State)
	case proto.WSFailed:
		return nil, fmt.Errorf("workspace %s is failed; inspect it before resuming", ws.ID)
	}
	r := o.Recipe
	if r == nil {
		name := ws.Spec.Labels[LabelRecipe]
		if name == "" {
			return nil, fmt.Errorf("workspace %s was not created by remount run or handoff; pass --recipe", ws.ID)
		}
		r, err = Load(name)
		if err != nil {
			return nil, fmt.Errorf("workspace %s: %w", ws.ID, err)
		}
	}
	if len(r.ResumeCommand) == 0 {
		return nil, fmt.Errorf("recipe %s has no resume_command", r.Name)
	}
	res := &ResumeResult{Workspace: ws, Recipe: r}

	if ws.State == proto.WSClaimed && o.Attach {
		sessions, err := cl.ListSessions(ctx, ws.ID)
		if err != nil {
			return nil, err
		}
		var live *proto.SessionStatus
		for i := range sessions {
			st := &sessions[i]
			if st.Info.Run != nil && !st.Exited && (live == nil || st.Info.OpenedAt > live.Info.OpenedAt) {
				live = st
			}
		}
		if live != nil {
			s, err := cl.Attach(ctx, ws.ID, live.Info.ID, 0)
			if err != nil {
				return nil, err
			}
			fmt.Fprintf(stderr, "attached to running %s session %s\n", live.Info.Run.Recipe, live.Info.ID)
			res.Session, res.Attached = s, true
			return res, nil
		}
	}

	wait := o.Wait
	if wait <= 0 {
		wait = 60 * time.Second
	}
	if ws.State != proto.WSClaimed {
		if ws.State == proto.WSPaused {
			if _, err := cl.WakeWorkspace(ctx, ws.ID); err != nil {
				return nil, err
			}
			fmt.Fprintf(stderr, "waking %s\n", ws.ID)
		}
		wctx, cancel := context.WithTimeout(ctx, wait)
		ws, err = cl.WaitClaimed(wctx, ws.ID)
		cancel()
		if err != nil {
			return nil, fmt.Errorf("%s not claimed: %w (is a matching node online?)", o.WS, err)
		}
		res.Workspace = ws
	}

	var bindings []Binding
	if specs := ws.Spec.Labels[LabelBindings]; specs != "" {
		for _, spec := range strings.Split(specs, ",") {
			b, err := ParseBinding(spec)
			if err != nil {
				return nil, fmt.Errorf("workspace %s label %s: %w", ws.ID, LabelBindings, err)
			}
			bindings = append(bindings, b)
		}
	}
	task := o.Task
	if task == "" {
		task = DefaultResumeTask
	}
	security := o.Security
	if security == "" {
		security = ws.Spec.Security.Profile
	}
	run, err := Start(ctx, cl, Options{
		Recipe: r, Task: task, WS: ws.ID, Bindings: bindings, Model: ws.Spec.Labels[LabelModel],
		Security: security, Sandbox: o.Sandbox, Approve: o.Approve, Resume: true,
		PTY: o.PTY, Rows: o.Rows, Cols: o.Cols, Stdin: o.Stdin, Timeout: o.Timeout, Stderr: stderr,
	})
	if run != nil {
		res.Run = run
		res.Session = run.Session
	}
	return res, err
}
