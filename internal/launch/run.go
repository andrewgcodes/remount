package launch

import (
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"path"
	"strings"
	"time"

	"remount.dev/remount/internal/client"
	"remount.dev/remount/internal/proto"
)

// Options describes one `remount run`.
type Options struct {
	Recipe *Recipe
	// Task is the prompt; Args is the argv after -- (custom recipe).
	Task string
	Args []string

	// Exactly one of WS (an existing workspace) or a seed for a new one.
	WS          string
	Dir         string // packed and uploaded by the caller into RestoreFrom
	RestoreFrom string
	Base        string
	Repo        proto.RepoSpec // cloned by the node before the workspace is ready (ADR 0054)

	Name             string
	Image            string
	Backend          string
	Bindings         []Binding
	Security         string
	Sandbox          string
	Approve          string
	Model            string
	Exclude          []string
	Resume           bool
	Conversation     string
	ConversationPath string
	// MountPath pins where the tree appears inside the workspace (ADR 0041).
	// A handoff sets it to the local checkout's path so path-keyed harness
	// state stays valid; only namespaced backends can claim such a workspace.
	MountPath string
	// Labels are merged into the workspace's labels at create.
	Labels map[string]string
	// BeforeOpen runs once the workspace is claimed and the harness is
	// installed, before the session opens. A queue driver records its
	// durable state here so the first task cannot run unrecorded.
	BeforeOpen func(ctx context.Context, ws *proto.Workspace) error

	// PTY opens the harness on a pseudo-terminal (attached mode).
	PTY        bool
	Rows, Cols uint16
	// Stdin keeps an exec session's stdin open because the caller will
	// forward bytes into it. Leave it false when nothing will be written:
	// harnesses such as OpenCode read a non-terminal stdin to EOF before
	// starting, so an open, silent pipe stalls the run forever.
	Stdin   bool
	Timeout time.Duration
	// Stderr receives progress and the install log; nil discards.
	Stderr io.Writer
	// WaitClaimed bounds how long a new workspace may take to be claimed.
	WaitClaimed time.Duration
}

// Result is a started launch.
type Result struct {
	Workspace *proto.Workspace
	Session   *client.Session
	// Created is true when Start made the workspace (so a failed launch may
	// destroy it) and false when it reused Options.WS.
	Created bool
	Auth    string
	Run     proto.RunInfo
}

// Plan is what Start will do, computed without talking to the server so the
// CLI can validate flags and tests can inspect the derived policy.
type Plan struct {
	Auth string
	Spec proto.WorkspaceSpec
	// SessionEnv is passed at open so a reused workspace also sees the
	// binding placeholders and base URLs.
	SessionEnv map[string]string
	Data       Data
	Program    []string
	Run        proto.RunInfo
}

// Validate checks Options and returns the Plan.
func (o *Options) Validate() (*Plan, error) {
	r := o.Recipe
	if r == nil {
		return nil, errors.New("recipe is required")
	}
	if o.Repo.URL != "" {
		repo, err := o.Repo.Normalize()
		if err != nil {
			return nil, err
		}
		o.Repo = repo
	}
	seeds := 0
	for _, set := range []bool{o.WS != "", o.RestoreFrom != "", o.Base != "", o.Repo.URL != ""} {
		if set {
			seeds++
		}
	}
	if seeds > 1 {
		return nil, errors.New("--ws, --dir/--restore-from, --base and --repo are mutually exclusive")
	}
	switch o.Sandbox {
	case "":
		o.Sandbox = SandboxWorkspaceWrite
	case SandboxReadOnly, SandboxWorkspaceWrite, SandboxFull:
	default:
		return nil, fmt.Errorf("--sandbox must be %s, %s or %s", SandboxReadOnly, SandboxWorkspaceWrite, SandboxFull)
	}
	switch o.Approve {
	case "":
		o.Approve = ApproveNever
	case ApproveNever, ApproveOnRequest:
	default:
		return nil, fmt.Errorf("--approve must be %s or %s", ApproveNever, ApproveOnRequest)
	}
	switch o.Security {
	case "":
		o.Security = proto.SecurityLocal
	case proto.SecurityLocal, proto.SecurityIsolated, proto.SecurityMultiTenant:
	default:
		return nil, fmt.Errorf("--security must be %s, %s or %s", proto.SecurityLocal, proto.SecurityIsolated, proto.SecurityMultiTenant)
	}
	if r.CommandFromArgs && len(o.Args) == 0 {
		return nil, fmt.Errorf("recipe %s runs the command given after --; none was given", r.Name)
	}
	if !r.CommandFromArgs && strings.TrimSpace(o.Task) == "" {
		return nil, fmt.Errorf("recipe %s needs a task after --", r.Name)
	}
	if o.Resume && len(r.ResumeCommand) == 0 {
		return nil, fmt.Errorf("recipe %s has no resume mode", r.Name)
	}
	if o.Conversation != "" {
		if !o.Resume || !conversationIDPattern.MatchString(o.Conversation) {
			return nil, errors.New("conversation must be a UUID for a resume launch")
		}
		if scopedHandoff(r) {
			if err := validateConversationPath(r.Name, o.Conversation, o.ConversationPath); err != nil {
				return nil, err
			}
		}
	} else if o.ConversationPath != "" {
		return nil, errors.New("conversation transcript requires a conversation UUID")
	}

	providers := make([]string, 0, len(o.Bindings))
	sessionEnv := map[string]string{}
	seenBinding := map[string]bool{}
	for _, b := range o.Bindings {
		if seenBinding[b.ID] {
			return nil, fmt.Errorf("binding %s given twice", b.ID)
		}
		seenBinding[b.ID] = true
		if !r.Accepts(b.Preset.Name) {
			return nil, fmt.Errorf("recipe %s does not consume %s bindings (accepts %s)", r.Name, b.Preset.Name, strings.Join(r.Providers, ", "))
		}
		for _, p := range providers {
			if p == b.Preset.Name {
				return nil, fmt.Errorf("two bindings use the %s preset; a harness reads one key per provider", p)
			}
		}
		providers = append(providers, b.Preset.Name)
		for k, v := range b.SessionEnv() {
			sessionEnv[k] = v
		}
	}
	auth, err := r.AuthMode(providers)
	if err != nil {
		return nil, err
	}
	if auth == AuthWorkspaceResident && o.Security != proto.SecurityLocal {
		return nil, fmt.Errorf("recipe %s would rely on a login kept inside the workspace, which --security %s forbids; bind a provider key instead", r.Name, o.Security)
	}

	d := Data{
		Task: o.Task, Args: o.Args, Recipe: r.Name, Workspace: o.WS,
		Sandbox: o.Sandbox, Approve: o.Approve, Model: o.Model, Conversation: o.Conversation,
		Providers: providers, Broker: "${REMOUNT_BROKER}",
	}
	if len(providers) > 0 {
		d.Primary = providers[0]
	}
	if _, err := r.Launcher(d, o.Resume); err != nil {
		return nil, err
	}

	plan := &Plan{Auth: auth, SessionEnv: sessionEnv, Data: d, Program: []string{"/bin/sh", r.LauncherPath()}}
	plan.Run = proto.RunInfo{Recipe: r.Name, Sandbox: o.Sandbox, Auth: auth}
	if !r.CommandFromArgs {
		plan.Run.TaskHash = TaskHash(o.Task)
	} else {
		plan.Run.TaskHash = TaskHash(strings.Join(o.Args, "\x00"))
	}
	if o.WS != "" {
		return plan, nil
	}

	if err := proto.ValidateMountPath(o.MountPath); err != nil {
		return nil, err
	}
	if o.MountPath != "" && o.MountPath != proto.DefaultMountPath && o.Backend == "process" {
		return nil, fmt.Errorf("--backend process cannot mount the tree at %s: it has no mount namespace (ADR 0041); use docker", o.MountPath)
	}
	spec := proto.WorkspaceSpec{
		Name: o.Name, Image: o.Image, RestoreFrom: o.RestoreFrom, Base: o.Base, Repo: o.Repo,
		Requires:  proto.Requires{Backend: o.Backend},
		Env:       map[string]string{},
		Exclude:   append([]string(nil), o.Exclude...),
		Labels:    map[string]string{},
		MountPath: o.MountPath,
	}
	for k, v := range o.Labels {
		spec.Labels[k] = v
	}
	// The recipe and its bindings are recorded so `remount resume` can
	// rebuild the launch without the original command line.
	spec.Labels[LabelRecipe] = r.Name
	if spec.Image == "" {
		spec.Image = r.Image
	}
	specs := make([]string, 0, len(o.Bindings))
	for _, b := range o.Bindings {
		spec.Bindings = append(spec.Bindings, b.ID)
		specs = append(specs, b.String())
	}
	if len(specs) > 0 {
		spec.Labels[LabelBindings] = strings.Join(specs, ",")
	}
	if o.Model != "" {
		spec.Labels[LabelModel] = o.Model
	}
	delete(spec.Labels, LabelConversation)
	delete(spec.Labels, LabelConversationPath)
	if o.Conversation != "" {
		spec.Labels[LabelConversation] = o.Conversation
		spec.Labels[LabelConversationPath] = o.ConversationPath
	}
	for k, v := range sessionEnv {
		spec.Env[k] = v
	}
	spec.Security = proto.SecuritySpec{Profile: o.Security, Network: egressPolicy(r, o.Bindings, o.Security, o.Sandbox)}
	normalized, err := proto.NormalizeSecurity(spec.Security)
	if err != nil {
		return nil, fmt.Errorf("security policy: %w", err)
	}
	spec.Security = normalized
	plan.Spec = spec
	return plan, nil
}

// egressPolicy derives the typed network policy for a launch.
//
// A typed policy replaces the node's allow-list decision entirely, so once
// any rule is present every destination the harness needs must be listed.
// --sandbox names what the harness may write, so read-only and
// workspace-write share one egress shape: the harness must still be able to
// install itself and reach its provider.
//
//	read-only, workspace-write
//	                 local: node allow-list (no typed policy)
//	                 isolated/multi_tenant: deny; provider hosts, plus GET/HEAD
//	                 on recipe hosts so the harness can install itself
//	full             local: allow everything
//	                 isolated/multi_tenant: deny; provider and recipe hosts,
//	                 every method, CONNECT included
func egressPolicy(r *Recipe, bindings []Binding, security, sandbox string) proto.NetworkPolicy {
	if security == proto.SecurityLocal {
		if sandbox == SandboxFull {
			return proto.NetworkPolicy{Default: proto.NetworkDefaultAllow}
		}
		return proto.NetworkPolicy{}
	}
	policy := proto.NetworkPolicy{Default: proto.NetworkDefaultDeny}
	for _, b := range bindings {
		policy.Rules = append(policy.Rules, proto.EgressRule{
			ID: "run-provider-" + b.ID, Protocol: proto.EgressProtocolHTTPS, Hosts: b.Hosts(),
		})
	}
	if len(r.Hosts) == 0 {
		return policy
	}
	hosts := append([]string(nil), r.Hosts...)
	if sandbox != SandboxFull {
		policy.Rules = append(policy.Rules, proto.EgressRule{
			ID: "run-recipe-fetch", Protocol: proto.EgressProtocolHTTPS, Hosts: hosts,
			Methods: []string{"GET", "HEAD"},
		})
		return policy
	}
	policy.Rules = append(policy.Rules,
		proto.EgressRule{ID: "run-recipe-https", Protocol: proto.EgressProtocolHTTPS, Hosts: hosts},
		proto.EgressRule{ID: "run-recipe-connect", Protocol: proto.EgressProtocolConnect, Hosts: hosts},
	)
	for _, b := range bindings {
		policy.Rules = append(policy.Rules, proto.EgressRule{
			ID: "run-provider-connect-" + b.ID, Protocol: proto.EgressProtocolConnect, Hosts: b.Hosts(),
		})
	}
	return policy
}

// Workspace labels written by Start and read back by Resume.
const (
	LabelRecipe           = "remount.recipe"
	LabelBindings         = "remount.bindings"
	LabelModel            = "remount.model"
	LabelConversation     = "remount.conversation"
	LabelConversationPath = "remount.conversation.path"
	// LabelOrigin is the local directory a handoff came from.
	LabelOrigin = "remount.origin"
)

// Start materializes (or reuses) the workspace, installs the harness, writes
// the launcher under .remount/launch and opens the session. The returned
// session is live; the caller drives or detaches from it.
func Start(ctx context.Context, cl *client.Client, o Options) (*Result, error) {
	plan, err := o.Validate()
	if err != nil {
		return nil, err
	}
	stderr := o.Stderr
	if stderr == nil {
		stderr = io.Discard
	}
	res := &Result{Auth: plan.Auth, Run: plan.Run}
	if o.WS == "" {
		ws, err := cl.CreateWorkspace(ctx, plan.Spec)
		if err != nil {
			return nil, err
		}
		res.Created = true
		res.Workspace = ws
		fmt.Fprintf(stderr, "workspace %s created\n", ws.ID)
		wait := o.WaitClaimed
		if wait <= 0 {
			wait = 60 * time.Second
		}
		wctx, cancel := context.WithTimeout(ctx, wait)
		ws, err = cl.WaitClaimed(wctx, ws.ID)
		cancel()
		if err != nil {
			hint := "is a matching node online?"
			if o.Security != proto.SecurityLocal {
				hint = "--security " + o.Security + " needs a node whose backend enforces egress; the process and docker backends only cooperate"
			} else if o.MountPath != "" {
				hint = "mounting the tree at " + o.MountPath + " needs a node with a namespaced backend such as docker; the process backend cannot"
			}
			return res, fmt.Errorf("%s created but not claimed: %w (%s)", res.workspaceID(plan), err, hint)
		}
		res.Workspace = ws
	} else {
		ws, err := cl.GetWorkspace(ctx, o.WS)
		if err != nil {
			return nil, err
		}
		security, err := proto.NormalizeSecurity(ws.Spec.Security)
		if err != nil {
			return nil, err
		}
		if proto.ProfileRank(security.Profile) < proto.ProfileRank(o.Security) {
			return nil, proto.Err(proto.CodeDenied, "workspace %s has security profile %s, below requested %s; create a workspace with the required profile", ws.ID, security.Profile, o.Security)
		}
		for _, b := range o.Bindings {
			if !contains(ws.Spec.Bindings, b.ID) {
				return nil, fmt.Errorf("workspace %s does not carry binding %s; bindings are fixed at ws create", ws.ID, b.ID)
			}
		}
		if plan.Auth == AuthWorkspaceResident && ws.Spec.Security.Profile != "" && ws.Spec.Security.Profile != proto.SecurityLocal {
			return nil, fmt.Errorf("workspace %s has security profile %s, which forbids a login kept inside the workspace", ws.ID, ws.Spec.Security.Profile)
		}
		res.Workspace = ws
	}
	wsID := res.Workspace.ID
	plan.Data.Workspace = wsID

	if o.Recipe.Install != "" {
		fmt.Fprintf(stderr, "installing %s\n", o.Recipe.Name)
		if err := runInstall(ctx, cl, wsID, plan.SessionEnv, o.Recipe.Install, stderr); err != nil {
			return res, err
		}
	}
	if o.BeforeOpen != nil {
		if err := o.BeforeOpen(ctx, res.Workspace); err != nil {
			return res, err
		}
	}
	if o.Conversation != "" && scopedHandoff(o.Recipe) {
		entry, err := cl.Stat(ctx, wsID, o.ConversationPath)
		if err != nil {
			return res, fmt.Errorf("selected conversation transcript unavailable: %w", err)
		}
		if entry.IsDir || entry.IsLink || !fs.FileMode(entry.Mode).IsRegular() || entry.Size <= 0 || entry.Size > handoffMaxFileBytes {
			return res, errors.New("selected conversation transcript is not a bounded regular file")
		}
		data, err := cl.ReadFile(ctx, wsID, o.ConversationPath)
		if err != nil {
			return res, fmt.Errorf("read selected conversation transcript: %w", err)
		}
		if _, ok := conversationMetadata(data, o.Recipe.Name, o.Conversation, path.IsAbs); !ok {
			return res, errors.New("selected conversation transcript is invalid or does not match its UUID")
		}
	}
	script, err := o.Recipe.Launcher(plan.Data, o.Resume)
	if err != nil {
		return res, err
	}
	if err := cl.WriteFile(ctx, wsID, o.Recipe.LauncherPath(), []byte(script), 0o700); err != nil {
		return res, fmt.Errorf("write launcher: %w", err)
	}
	kind := proto.SessionExec
	if o.PTY {
		kind = proto.SessionPTY
	}
	run := plan.Run
	s, err := cl.Exec(ctx, proto.SOpenReq{
		WS: wsID, Kind: kind, Program: plan.Program, Env: plan.SessionEnv,
		Rows: o.Rows, Cols: o.Cols, Stdin: o.Stdin, TimeoutSec: int64(o.Timeout.Seconds()), Run: &run,
	})
	if err != nil {
		return res, err
	}
	res.Session = s
	return res, nil
}

func (r *Result) workspaceID(plan *Plan) string {
	if r.Workspace != nil {
		return r.Workspace.ID
	}
	return plan.Spec.Name
}

// runInstall runs the recipe's install script in the workspace, streaming
// its output, and fails on a non-zero exit.
func runInstall(ctx context.Context, cl *client.Client, wsID string, env map[string]string, script string, out io.Writer) error {
	s, err := cl.Exec(ctx, proto.SOpenReq{WS: wsID, Kind: proto.SessionExec, Program: []string{"/bin/sh", "-c", ". ./.remount/env 2>/dev/null; " + script}, Env: env})
	if err != nil {
		return fmt.Errorf("install: %w", err)
	}
	for ch := range s.Chunks() {
		switch ch.Stream {
		case proto.StreamStdout, proto.StreamStderr:
			_, _ = out.Write(ch.Data)
		}
	}
	if err := s.Err(); err != nil {
		return fmt.Errorf("install: %w", err)
	}
	exit := s.Exit()
	if exit == nil {
		return errors.New("install: session ended without an exit record")
	}
	if exit.Code != 0 || exit.Signal != "" {
		return fmt.Errorf("install exited %d%s", exit.Code, signalSuffix(exit.Signal))
	}
	return nil
}

func signalSuffix(sig string) string {
	if sig == "" {
		return ""
	}
	return " (signal " + sig + ")"
}

func contains(list []string, s string) bool {
	for _, x := range list {
		if x == s {
			return true
		}
	}
	return false
}
