// Package launch turns a harness recipe plus a workspace into a running
// session. Recipes are data (YAML embedded in the binary or supplied with
// --recipe-file); this package renders them, installs the harness, writes a
// launcher that rediscovers the broker from .remount/env at every start, and
// opens the session. It never sees a secret: provider keys reach the harness
// as broker placeholders through the workspace's bindings.
package launch

import (
	"bytes"
	"crypto/sha256"
	"embed"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"path"
	"regexp"
	"slices"
	"sort"
	"strings"
	"text/template"

	"remount.dev/remount/internal/launch/yamlite"
	"remount.dev/remount/internal/proto"
)

//go:embed recipes/*.yaml
var embedded embed.FS

// Auth modes a recipe declares.
const (
	// AuthAPIKey: the harness reads a provider key and base URL from the
	// environment; Remount brokers the key and the workspace holds a
	// placeholder.
	AuthAPIKey = "api_key"
	// AuthWorkspaceResident: the harness runs its own login and keeps the
	// token in its state directory inside the workspace. Remount does not
	// see, proxy or rewrite it, so this mode is refused under the isolated
	// and multi-tenant security profiles.
	AuthWorkspaceResident = "workspace_resident"
	// AuthSubscription is the explicit launch mode for a provider-native
	// subscription login. Recipes still declare workspace_resident or either;
	// this value records what the operator selected for a run.
	AuthSubscription = "subscription"
	// AuthEither: api_key when a provider binding is present, otherwise
	// workspace_resident.
	AuthEither = "either"
)

// Sandbox levels map onto egress policy and a harness-visible hint.
const (
	SandboxReadOnly       = proto.AgentSandboxReadOnly
	SandboxWorkspaceWrite = proto.AgentSandboxWorkspaceWrite
	SandboxFull           = proto.AgentSandboxFull
)

// Approval policies for hosts outside the binding set.
const (
	ApproveNever     = "never"
	ApproveOnRequest = "on-request"
)

// LauncherDir is the workspace-relative directory launchers are written to.
// It lives under .remount, which the node owns and snapshots exclude, so a
// launcher never travels: every `run` regenerates it for the node it runs on.
const LauncherDir = ".remount/launch"

// Recipe is a harness described as data. Every string field except Name,
// Auth and the lists of names is a Go text/template rendered against Data.
type Recipe struct {
	Name        string `json:"name"`
	Description string `json:"description,omitempty"`
	// Image is the default workspace image (docker backend); empty means the
	// node's default.
	Image string `json:"image,omitempty"`
	// Auth is api_key (default), workspace_resident or either.
	Auth string `json:"auth,omitempty"`
	// Subscription describes the provider CLI's native subscription login.
	// Remount runs these commands inside the trusted local workspace and never
	// parses or persists their output.
	Subscription *SubscriptionSpec `json:"subscription,omitempty"`
	// Providers lists binding presets the harness can consume, in the order
	// the recipe prefers them.
	Providers []string `json:"providers,omitempty"`
	// Hosts are non-provider destinations the harness needs (its own
	// registry, update check, telemetry it cannot disable). They are allowed
	// only under --sandbox full.
	Hosts []string `json:"hosts,omitempty"`
	// Install is a POSIX sh script run once per workspace materialization
	// before the launcher; it must be idempotent.
	Install string `json:"install,omitempty"`
	// Env is exported by the launcher after .remount/env is sourced.
	Env map[string]string `json:"env,omitempty"`
	// Configure files are rewritten by the launcher at every start so a
	// broker URL inside them is always the current node's.
	Configure []ConfigFile `json:"configure,omitempty"`
	// Command is the argv the launcher execs.
	Command []string `json:"command,omitempty"`
	// CommandFromArgs makes the launcher exec the caller's argv verbatim
	// (the `custom` recipe) instead of Command.
	CommandFromArgs bool `json:"command_from_args,omitempty"`
	// ResumeCommand continues the harness's previous conversation in this
	// workspace; empty means the harness has no such mode.
	ResumeCommand []string `json:"resume_command,omitempty"`
	// StateDirs are where the harness keeps conversation state, relative to
	// the workspace root. Every backend sets HOME to the workspace root, so a
	// harness's ~/.dotdir lands here and snapshots carry it.
	StateDirs []string `json:"state_dirs,omitempty"`
	// PathKeyed harnesses key state on the working directory and need a
	// stable mount path across moves (ADR 0041).
	PathKeyed bool `json:"path_keyed,omitempty"`
	// SandboxFlags maps a --sandbox level to harness-native arguments
	// appended to Command.
	SandboxFlags map[string][]string `json:"sandbox_flags,omitempty"`
	// ApproveFlags maps an --approve policy to harness-native arguments.
	ApproveFlags map[string][]string `json:"approve_flags,omitempty"`
	ModelFlags   []string            `json:"model_flags,omitempty"`
	// ACP runs the harness as an Agent Client Protocol server over stdio
	// (ADR 0042). Recipes with it have a structured transcript; recipes
	// without run in PTY mode and prompts are typed via PromptTemplate.
	ACP *ACPSpec `json:"acp,omitempty"`
	// UI is the harness's own web interface, reachable only through the
	// preview proxy; Remount ships no end-user UI of its own (ADR 0046).
	UI *UISpec `json:"ui,omitempty"`
	// PromptTemplate renders a follow-up message into the bytes typed into a
	// PTY-mode harness; the template sees Data.Message. Empty means the
	// message followed by a newline.
	PromptTemplate string `json:"prompt_template,omitempty"`
	// SessionIDFrom locates the harness's own conversation id in its state
	// directory so a handoff can continue that conversation with
	// session/load on the node.
	SessionIDFrom *SessionIDFrom `json:"session_id_from,omitempty"`
}

// SubscriptionSpec describes provider-native subscription authentication.
type SubscriptionSpec struct {
	Login    []string `json:"login"`
	Status   []string `json:"status"`
	Logout   []string `json:"logout"`
	Verify   string   `json:"verify"`
	UnsetEnv []string `json:"unset_env,omitempty"`
	// Hosts are the provider endpoints the login and a subscription launch
	// reach directly. There is no broker in this mode, so a node serving
	// subscription launches must allow them without a credential; the
	// autostarted standalone does, an explicit node takes them via --allow.
	Hosts []string `json:"hosts,omitempty"`
	// BypassProxy drops HTTP(S)_PROXY from a subscription launch. Claude Code
	// runs on Bun, whose fetch hangs on chunked keep-alive responses through
	// an HTTPS CONNECT proxy (oven-sh/bun#30381), and a subscription launch
	// has to reach the provider directly rather than through the broker's
	// plain-HTTP /d/ path an API-key launch uses. Subscription auth is local
	// profile only, where the proxy is cooperative; the cost is that this
	// harness's provider traffic is absent from the broker's audit.
	BypassProxy bool `json:"bypass_proxy,omitempty"`
}

// ACPSpec is how a recipe starts its harness as an ACP agent.
type ACPSpec struct {
	// Command is the argv of the stdio ACP server; its cwd is the workspace
	// root (MountPath). Each element is a template.
	Command []string `json:"command"`
	// Env is extra environment for the ACP process, exported after the
	// recipe's Env. Never a credential: keys arrive as broker placeholders.
	Env map[string]string `json:"env,omitempty"`
	// SandboxModes maps Remount sandbox levels to ACP session mode ids. When
	// set, the node selects the mode before sending the first prompt.
	SandboxModes map[string]string `json:"sandbox_modes,omitempty"`
	// LoadSession is the recipe's claim that the agent advertises
	// session/load. The runner verifies it against Initialize and records a
	// mismatch rather than trusting either side blindly.
	LoadSession bool `json:"load_session,omitempty"`
	// Adapter names a bridge (an npm package, say) when the harness does not
	// speak ACP natively. Documentation only.
	Adapter string `json:"adapter,omitempty"`
}

// UISpec is a harness-native web UI the node can start next to the agent.
type UISpec struct {
	// Command is the argv; templates see Data.Port for the port to bind.
	Command []string `json:"command"`
	// Port is the port the UI listens on inside the workspace.
	Port int `json:"port"`
}

// SessionIDFrom describes where a harness records its conversation id.
type SessionIDFrom struct {
	// Glob is a workspace-relative pattern over the harness's state files;
	// the most recently modified match wins.
	Glob string `json:"glob"`
	// Key is a dotted path into the matched JSON file ("id",
	// "session.id"). Empty means the file's base name without extension is
	// the id.
	Key string `json:"key,omitempty"`
}

// Mode names how an Agent built from this recipe runs.
const (
	// ModeACP: structured transcript over the Agent Client Protocol.
	ModeACP = "acp"
	// ModePTY: the harness runs on a pseudo-terminal; the transcript is the
	// terminal log and messages are typed via PromptTemplate.
	ModePTY = "pty"
)

// Mode returns ModeACP when the recipe declares an ACP command, else
// ModePTY.
func (r *Recipe) Mode() string {
	if r.ACP != nil {
		return ModeACP
	}
	return ModePTY
}

// ConfigFile is one file the launcher writes before exec.
type ConfigFile struct {
	Path    string `json:"path"`
	Content string `json:"content"`
	Mode    int64  `json:"mode,omitempty"`
}

// Data is the template context for a single launch.
type Data struct {
	Task         string
	Args         []string
	Recipe       string
	Workspace    string
	Sandbox      string
	Approve      string
	Model        string
	Conversation string
	Auth         string
	// Providers are the presets bound for this run, in --binding order.
	Providers []string
	// Primary is Providers[0] or "".
	Primary string
	// Broker is the shell expression for the broker base URL. The launcher
	// sources .remount/env first, so it is always the hosting node's.
	Broker string
	// Message is the follow-up text a PromptTemplate renders.
	Message string
	// Port is the port a UI command binds.
	Port int
}

var recipeNamePattern = regexp.MustCompile(`^[a-z0-9][a-z0-9._-]{0,63}$`)

// Builtin returns the embedded recipe names, sorted.
func Builtin() []string {
	entries, err := fs.ReadDir(embedded, "recipes")
	if err != nil {
		return nil
	}
	var names []string
	for _, e := range entries {
		names = append(names, strings.TrimSuffix(e.Name(), ".yaml"))
	}
	sort.Strings(names)
	return names
}

// Load returns the embedded recipe called name.
func Load(name string) (*Recipe, error) {
	if !recipeNamePattern.MatchString(name) {
		return nil, fmt.Errorf("recipe %q: name must match %s", name, recipeNamePattern)
	}
	b, err := embedded.ReadFile(path.Join("recipes", name+".yaml"))
	if err != nil {
		return nil, fmt.Errorf("recipe %q is not built in (have %s)", name, strings.Join(Builtin(), ", "))
	}
	r, err := Parse(b)
	if err != nil {
		return nil, fmt.Errorf("built-in recipe %q: %w", name, err)
	}
	if r.Name != name {
		return nil, fmt.Errorf("built-in recipe file %s declares name %q", name, r.Name)
	}
	return r, nil
}

// Parse loads a recipe from YAML and validates it, including compiling every
// template so a typo fails at load rather than at exec.
func Parse(src []byte) (*Recipe, error) {
	var r Recipe
	if err := yamlite.Unmarshal(src, &r); err != nil {
		return nil, err
	}
	if err := r.Validate(); err != nil {
		return nil, err
	}
	return &r, nil
}

// Validate checks the recipe's shape and compiles its templates.
func (r *Recipe) Validate() error {
	if !recipeNamePattern.MatchString(r.Name) {
		return fmt.Errorf("recipe name %q must match %s", r.Name, recipeNamePattern)
	}
	switch r.Auth {
	case "":
		r.Auth = AuthAPIKey
	case AuthAPIKey, AuthWorkspaceResident, AuthEither:
	default:
		return fmt.Errorf("recipe %s: auth must be %s, %s or %s", r.Name, AuthAPIKey, AuthWorkspaceResident, AuthEither)
	}
	if r.Subscription != nil {
		if r.Auth == AuthAPIKey {
			return fmt.Errorf("recipe %s: subscription auth cannot be declared with api_key-only auth", r.Name)
		}
		if len(r.Subscription.Login) == 0 || len(r.Subscription.Status) == 0 || len(r.Subscription.Logout) == 0 {
			return fmt.Errorf("recipe %s: subscription login, status and logout commands are required", r.Name)
		}
		if strings.TrimSpace(r.Subscription.Verify) == "" {
			return fmt.Errorf("recipe %s: subscription verify script is required", r.Name)
		}
		seenEnv := map[string]bool{}
		for _, name := range r.Subscription.UnsetEnv {
			if !envNamePattern.MatchString(name) {
				return fmt.Errorf("recipe %s: subscription unset_env name %q is not a valid identifier", r.Name, name)
			}
			if seenEnv[name] {
				return fmt.Errorf("recipe %s: subscription unset_env name %q listed twice", r.Name, name)
			}
			seenEnv[name] = true
		}
	}
	if len(r.Command) == 0 && !r.CommandFromArgs {
		return fmt.Errorf("recipe %s: command is required", r.Name)
	}
	if r.Auth != AuthWorkspaceResident && len(r.Providers) == 0 {
		return fmt.Errorf("recipe %s: api_key auth needs at least one provider preset", r.Name)
	}
	seen := map[string]bool{}
	for _, p := range r.Providers {
		if _, ok := LookupPreset(p); !ok {
			return fmt.Errorf("recipe %s: unknown provider preset %q (have %s)", r.Name, p, strings.Join(PresetNames(), ", "))
		}
		if seen[p] {
			return fmt.Errorf("recipe %s: provider %q listed twice", r.Name, p)
		}
		seen[p] = true
	}
	for _, h := range r.Hosts {
		if h == "" || strings.ContainsAny(h, " /\t\n") {
			return fmt.Errorf("recipe %s: host %q is not a host pattern", r.Name, h)
		}
	}
	if r.Subscription != nil {
		for _, h := range r.Subscription.Hosts {
			if h == "" || strings.ContainsAny(h, " /\t\n") {
				return fmt.Errorf("recipe %s: subscription host %q is not a host pattern", r.Name, h)
			}
		}
	}
	for i, f := range r.Configure {
		if err := validateRelativePath(f.Path); err != nil {
			return fmt.Errorf("recipe %s: configure[%d]: %w", r.Name, i, err)
		}
		if f.Mode < 0 || f.Mode > 0o777 {
			return fmt.Errorf("recipe %s: configure[%d]: mode %o out of range", r.Name, i, f.Mode)
		}
	}
	for _, d := range r.StateDirs {
		if err := validateRelativePath(d); err != nil {
			return fmt.Errorf("recipe %s: state dir: %w", r.Name, err)
		}
	}
	for level := range r.SandboxFlags {
		switch level {
		case SandboxReadOnly, SandboxWorkspaceWrite, SandboxFull:
		default:
			return fmt.Errorf("recipe %s: sandbox_flags key %q is not a sandbox level", r.Name, level)
		}
	}
	for policy := range r.ApproveFlags {
		switch policy {
		case ApproveNever, ApproveOnRequest:
		default:
			return fmt.Errorf("recipe %s: approve_flags key %q is not an approval policy", r.Name, policy)
		}
	}
	if r.ACP != nil {
		if len(r.ACP.Command) == 0 {
			return fmt.Errorf("recipe %s: acp.command is required", r.Name)
		}
		for k := range r.ACP.Env {
			if !envNamePattern.MatchString(k) {
				return fmt.Errorf("recipe %s: acp.env name %q is not a valid identifier", r.Name, k)
			}
		}
		for level, mode := range r.ACP.SandboxModes {
			switch level {
			case SandboxReadOnly, SandboxWorkspaceWrite, SandboxFull:
			default:
				return fmt.Errorf("recipe %s: acp.sandbox_modes key %q is not a sandbox level", r.Name, level)
			}
			if strings.TrimSpace(mode) == "" {
				return fmt.Errorf("recipe %s: acp.sandbox_modes[%s] is empty", r.Name, level)
			}
		}
	}
	if r.UI != nil {
		if len(r.UI.Command) == 0 {
			return fmt.Errorf("recipe %s: ui.command is required", r.Name)
		}
		if r.UI.Port < 1 || r.UI.Port > 65535 {
			return fmt.Errorf("recipe %s: ui.port %d out of range", r.Name, r.UI.Port)
		}
	}
	if r.SessionIDFrom != nil {
		if err := validateRelativePath(r.SessionIDFrom.Glob); err != nil {
			return fmt.Errorf("recipe %s: session_id_from.glob: %w", r.Name, err)
		}
		if _, err := path.Match(r.SessionIDFrom.Glob, ""); err != nil {
			return fmt.Errorf("recipe %s: session_id_from.glob: %w", r.Name, err)
		}
	}
	// Compile every template once with a representative Data so a syntax
	// error or unknown field is a load-time error.
	probe := Data{Task: "t", Recipe: r.Name, Workspace: "ws_probe", Sandbox: SandboxWorkspaceWrite, Approve: ApproveNever, Model: "m", Broker: "$REMOUNT_BROKER", Message: "m", Port: 1, Auth: AuthAPIKey}
	if len(r.Providers) > 0 {
		probe.Providers = []string{r.Providers[0]}
		probe.Primary = r.Providers[0]
	}
	if _, err := r.render(probe); err != nil {
		return err
	}
	return nil
}

func validateRelativePath(p string) error {
	if p == "" {
		return errors.New("path is required")
	}
	if strings.HasPrefix(p, "/") {
		return fmt.Errorf("path %q must be relative to the workspace root (which is also $HOME)", p)
	}
	clean := path.Clean(p)
	if clean == "." || strings.HasPrefix(clean, "../") || clean == ".." {
		return fmt.Errorf("path %q escapes the workspace", p)
	}
	// The launcher directory is the one node-owned place a recipe may write:
	// files there are regenerated on every launch and never travel in a
	// snapshot, which is exactly right for a config that names the broker.
	if (clean == ".remount" || strings.HasPrefix(clean, ".remount/")) && !strings.HasPrefix(clean, LauncherDir+"/") {
		return fmt.Errorf("path %q is reserved for the node", p)
	}
	if strings.HasSuffix(clean, ".sh") && strings.HasPrefix(clean, LauncherDir+"/") {
		return fmt.Errorf("path %q would overwrite a launcher", p)
	}
	return nil
}

// rendered is a recipe with every template expanded for one launch.
type rendered struct {
	Install   string
	Env       map[string]string
	Configure []ConfigFile
	Command   []string
	Resume    []string
	ACP       []string
	ACPEnv    map[string]string
	UI        []string
	Prompt    string
}

func funcs(d Data) template.FuncMap {
	has := func(p string) bool {
		for _, q := range d.Providers {
			if q == p {
				return true
			}
		}
		return false
	}
	return template.FuncMap{
		"has": has,
		// key and baseURL return shell expressions for the preset's
		// variables; the values exist only at session start.
		"key": func(p string) (string, error) {
			ps, ok := LookupPreset(p)
			if !ok {
				return "", fmt.Errorf("unknown provider %q", p)
			}
			return "$" + ps.KeyEnv, nil
		},
		"baseURL": func(p string) (string, error) {
			ps, ok := LookupPreset(p)
			if !ok {
				return "", fmt.Errorf("unknown provider %q", p)
			}
			return "$" + ps.BaseURLEnv, nil
		},
		"json": func(s string) string {
			b, _ := json.Marshal(s)
			return string(b)
		},
		"shq":  ShellQuote,
		"join": strings.Join,
	}
}

func (r *Recipe) render(d Data) (*rendered, error) {
	if d.Conversation != "" && !conversationIDPattern.MatchString(d.Conversation) {
		return nil, errors.New("conversation must be a UUID")
	}
	if d.Broker == "" {
		d.Broker = "$REMOUNT_BROKER"
	}
	fm := funcs(d)
	one := func(what, src string) (string, error) {
		t, err := template.New(what).Funcs(fm).Option("missingkey=error").Parse(src)
		if err != nil {
			return "", fmt.Errorf("recipe %s: %s: %w", r.Name, what, err)
		}
		var b bytes.Buffer
		if err := t.Execute(&b, d); err != nil {
			return "", fmt.Errorf("recipe %s: %s: %w", r.Name, what, err)
		}
		return b.String(), nil
	}
	list := func(what string, in []string) ([]string, error) {
		out := make([]string, 0, len(in))
		for i, s := range in {
			v, err := one(fmt.Sprintf("%s[%d]", what, i), s)
			if err != nil {
				return nil, err
			}
			out = append(out, v)
		}
		return out, nil
	}
	out := &rendered{Env: map[string]string{}}
	var err error
	if out.Install, err = one("install", r.Install); err != nil {
		return nil, err
	}
	keys := make([]string, 0, len(r.Env))
	for k := range r.Env {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		if !envNamePattern.MatchString(k) {
			return nil, fmt.Errorf("recipe %s: env name %q is not a valid identifier", r.Name, k)
		}
		if out.Env[k], err = one("env."+k, r.Env[k]); err != nil {
			return nil, err
		}
	}
	for i, f := range r.Configure {
		c := f
		if c.Path, err = one(fmt.Sprintf("configure[%d].path", i), f.Path); err != nil {
			return nil, err
		}
		if err := validateRelativePath(c.Path); err != nil {
			return nil, fmt.Errorf("recipe %s: configure[%d]: %w", r.Name, i, err)
		}
		if c.Content, err = one(fmt.Sprintf("configure[%d].content", i), f.Content); err != nil {
			return nil, err
		}
		out.Configure = append(out.Configure, c)
	}
	if r.CommandFromArgs {
		out.Command = append([]string(nil), d.Args...)
	} else if out.Command, err = list("command", r.Command); err != nil {
		return nil, err
	}
	var launchFlags []string
	if flags, ok := r.SandboxFlags[d.Sandbox]; ok {
		extra, err := list("sandbox_flags."+d.Sandbox, flags)
		if err != nil {
			return nil, err
		}
		launchFlags = append(launchFlags, extra...)
	}
	if flags, ok := r.ApproveFlags[d.Approve]; ok {
		extra, err := list("approve_flags."+d.Approve, flags)
		if err != nil {
			return nil, err
		}
		launchFlags = append(launchFlags, extra...)
	}
	if d.Model != "" {
		extra, err := list("model_flags", r.ModelFlags)
		if err != nil {
			return nil, err
		}
		launchFlags = append(launchFlags, extra...)
	}
	out.Command = append(out.Command, launchFlags...)
	if out.Resume, err = list("resume_command", r.ResumeCommand); err != nil {
		return nil, err
	}
	if len(out.Resume) > 0 {
		flagIndex := len(out.Resume)
		if len(out.Resume) >= 3 && path.Base(out.Resume[0]) == "codex" && out.Resume[1] == "exec" && out.Resume[2] == "resume" {
			flagIndex = 2
		} else if r.Name == "codex" && len(out.Resume) >= 4 && slices.Equal(out.Resume[:4], []string{"sh", LauncherDir + "/codex-driver", "exec", "resume"}) {
			flagIndex = 3
		}
		out.Resume = slices.Insert(out.Resume, flagIndex, launchFlags...)
	}
	if r.ACP != nil {
		if out.ACP, err = list("acp.command", r.ACP.Command); err != nil {
			return nil, err
		}
		out.ACPEnv = map[string]string{}
		akeys := make([]string, 0, len(r.ACP.Env))
		for k := range r.ACP.Env {
			akeys = append(akeys, k)
		}
		sort.Strings(akeys)
		for _, k := range akeys {
			if out.ACPEnv[k], err = one("acp.env."+k, r.ACP.Env[k]); err != nil {
				return nil, err
			}
		}
	}
	if r.UI != nil {
		if out.UI, err = list("ui.command", r.UI.Command); err != nil {
			return nil, err
		}
	}
	tmpl := r.PromptTemplate
	if tmpl == "" {
		tmpl = "{{.Message}}\n"
	}
	if out.Prompt, err = one("prompt_template", tmpl); err != nil {
		return nil, err
	}
	return out, nil
}

// ACPArgv renders the argv of the recipe's ACP server, or an error when the
// recipe has none.
func (r *Recipe) ACPArgv(d Data) ([]string, error) {
	if r.ACP == nil {
		return nil, fmt.Errorf("recipe %s has no acp command; it runs in %s mode", r.Name, ModePTY)
	}
	out, err := r.render(d)
	if err != nil {
		return nil, err
	}
	return out.ACP, nil
}

// InstallScript renders the recipe's install script, "" when it has none.
func (r *Recipe) InstallScript(d Data) (string, error) {
	if r.Install == "" {
		return "", nil
	}
	out, err := r.render(d)
	if err != nil {
		return "", err
	}
	return out.Install, nil
}

// InstallMarkerPath is the file the node writes once a recipe's install
// script has run in a materialization; it holds the workspace generation so
// a move (new generation, possibly a different image) installs again.
func (r *Recipe) InstallMarkerPath() string {
	return LauncherDir + "/" + r.Name + ".installed"
}

// UIArgv renders the argv of the recipe's web UI bound to d.Port.
func (r *Recipe) UIArgv(d Data) ([]string, error) {
	if r.UI == nil {
		return nil, fmt.Errorf("recipe %s has no ui", r.Name)
	}
	if d.Port == 0 {
		d.Port = r.UI.Port
	}
	out, err := r.render(d)
	if err != nil {
		return nil, err
	}
	return out.UI, nil
}

// PromptBytes renders message into what a PTY-mode agent types at the
// harness.
func (r *Recipe) PromptBytes(message string) (string, error) {
	out, err := r.render(Data{Recipe: r.Name, Message: message})
	if err != nil {
		return "", err
	}
	return out.Prompt, nil
}

var envNamePattern = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)

// Argv renders the argv the launcher will exec for task.
func (r *Recipe) Argv(d Data) ([]string, error) {
	out, err := r.render(d)
	if err != nil {
		return nil, err
	}
	return out.Command, nil
}

// AuthMode decides how this launch authenticates given the operator's
// selection and the bound providers. Recipes with subscription metadata
// require an explicit selection so billing mode never depends on ambient
// bindings.
func (r *Recipe) AuthMode(requested string, providers []string) (string, error) {
	switch requested {
	case AuthAPIKey:
		if r.Auth == AuthWorkspaceResident {
			return "", fmt.Errorf("recipe %s does not support API-key authentication", r.Name)
		}
		if len(providers) == 0 {
			return "", fmt.Errorf("recipe %s with --auth api-key needs an explicit provider binding (--binding ID[:PRESET]); it accepts %s", r.Name, strings.Join(r.Providers, ", "))
		}
		return AuthAPIKey, nil
	case AuthSubscription:
		if r.Auth == AuthAPIKey || r.Subscription == nil {
			return "", fmt.Errorf("recipe %s does not support subscription authentication", r.Name)
		}
		if len(providers) != 0 {
			return "", fmt.Errorf("recipe %s with --auth %s cannot use provider bindings", r.Name, AuthSubscription)
		}
		return AuthSubscription, nil
	case AuthWorkspaceResident:
		if r.Auth == AuthAPIKey {
			return "", fmt.Errorf("recipe %s does not support workspace-resident authentication", r.Name)
		}
		if len(providers) != 0 {
			return "", fmt.Errorf("recipe %s workspace-resident authentication cannot use provider bindings", r.Name)
		}
		return AuthWorkspaceResident, nil
	case "":
	default:
		return "", fmt.Errorf("--auth must be subscription or api-key")
	}
	if r.Subscription != nil && r.Auth == AuthEither {
		return "", fmt.Errorf("recipe %s requires --auth subscription or --auth api-key", r.Name)
	}
	switch r.Auth {
	case AuthAPIKey:
		if len(providers) == 0 {
			return "", fmt.Errorf("recipe %s needs a provider binding (--binding ID[:PRESET]); it accepts %s", r.Name, strings.Join(r.Providers, ", "))
		}
		return AuthAPIKey, nil
	case AuthWorkspaceResident:
		return AuthWorkspaceResident, nil
	default:
		if len(providers) == 0 {
			return AuthWorkspaceResident, nil
		}
		return AuthAPIKey, nil
	}
}

// Accepts reports whether the recipe can consume a provider preset.
func (r *Recipe) Accepts(preset string) bool {
	for _, p := range r.Providers {
		if p == preset {
			return true
		}
	}
	return false
}

// LauncherPath is where the launcher for this recipe lives in a workspace.
func (r *Recipe) LauncherPath() string {
	return LauncherDir + "/" + r.Name + ".sh"
}

// Launcher renders the sh script the session runs. The script sources
// .remount/env, exports the recipe's env, rewrites its config files and
// execs the command, so nothing node-specific is captured at write time.
//
// Content is written through unquoted heredocs: `$REMOUNT_BROKER` and the
// preset variables expand at start, so a literal dollar sign in a config
// file must be written as `\$`.
func (r *Recipe) Launcher(d Data, resume bool) (string, error) {
	out, err := r.render(d)
	if err != nil {
		return "", err
	}
	argv := out.Command
	if len(argv) == 0 {
		return "", fmt.Errorf("recipe %s: nothing to run; pass the command after --", r.Name)
	}
	if resume {
		if len(out.Resume) == 0 {
			return "", fmt.Errorf("recipe %s has no resume_command", r.Name)
		}
		argv = out.Resume
	}
	return r.launcher(d, out, argv, nil)
}

// ACPLauncherPath is where the ACP launcher for this recipe lives.
func (r *Recipe) ACPLauncherPath() string {
	return LauncherDir + "/" + r.Name + ".acp.sh"
}

// ACPLauncher renders the sh script that execs the recipe's ACP server with
// the same preamble as Launcher, plus the ACP-specific env.
func (r *Recipe) ACPLauncher(d Data) (string, error) {
	if r.ACP == nil {
		return "", fmt.Errorf("recipe %s has no acp command", r.Name)
	}
	out, err := r.render(d)
	if err != nil {
		return "", err
	}
	return r.launcher(d, out, out.ACP, out.ACPEnv)
}

// UILauncherPath is where the UI launcher for this recipe lives.
func (r *Recipe) UILauncherPath() string {
	return LauncherDir + "/" + r.Name + ".ui.sh"
}

// UILauncher renders the sh script that execs the recipe's web UI with the
// same preamble as Launcher. Data.Port defaults to the recipe's UI port.
func (r *Recipe) UILauncher(d Data) (string, error) {
	if r.UI == nil {
		return "", fmt.Errorf("recipe %s has no ui", r.Name)
	}
	if d.Port == 0 {
		d.Port = r.UI.Port
	}
	out, err := r.render(d)
	if err != nil {
		return "", err
	}
	return r.launcher(d, out, out.UI, nil)
}

func (r *Recipe) launcher(d Data, out *rendered, argv []string, extraEnv map[string]string) (string, error) {
	var b strings.Builder
	b.WriteString("#!/bin/sh\n")
	b.WriteString("# Generated by `remount run`; regenerated on every launch. Do not edit.\n")
	b.WriteString("set -eu\n")
	b.WriteString("cd \"$(dirname \"$0\")/../..\"\n")
	b.WriteString("[ -f .remount/env ] && . ./.remount/env\n")
	fmt.Fprintf(&b, "export REMOUNT_RECIPE=%s REMOUNT_SANDBOX=%s\n", ShellQuote(d.Recipe), ShellQuote(d.Sandbox))
	if d.Auth == AuthSubscription {
		if r.Subscription == nil {
			return "", fmt.Errorf("recipe %s has no subscription auth configuration", r.Name)
		}
		for _, name := range r.Subscription.UnsetEnv {
			fmt.Fprintf(&b, "unset %s\n", name)
		}
		if r.Subscription.BypassProxy {
			b.WriteString("# Bun's fetch hangs through a CONNECT proxy (oven-sh/bun#30381); a\n# subscription launch reaches the provider directly.\n")
			b.WriteString("unset HTTPS_PROXY https_proxy HTTP_PROXY http_proxy\n")
		}
		fmt.Fprintf(&b, "if ! (\n%s\n) >/dev/null 2>&1; then\n", strings.TrimRight(r.Subscription.Verify, "\n"))
		fmt.Fprintf(&b, "  echo %s >&2\n", ShellQuote(fmt.Sprintf("%s subscription login is unavailable; run `remount auth login %s --ws %s`", r.Name, r.Name, d.Workspace)))
		b.WriteString("  exit 78\nfi\n")
	}
	// The key placeholder and base URL arrive in the session env (see
	// Binding.SessionEnv); presets only add aliases here.
	for _, p := range d.Providers {
		ps, ok := LookupPreset(p)
		if !ok {
			return "", fmt.Errorf("unknown provider %q", p)
		}
		extra := make([]string, 0, len(ps.ExtraEnv))
		for k := range ps.ExtraEnv {
			extra = append(extra, k)
		}
		sort.Strings(extra)
		for _, k := range extra {
			fmt.Fprintf(&b, "export %s=\"%s\"\n", k, escapeDQ(ps.ExtraEnv[k]))
		}
	}
	keys := make([]string, 0, len(out.Env))
	for k := range out.Env {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		// A value that rendered empty (no --model, provider not bound) is
		// left to the harness's own default rather than exported empty.
		if out.Env[k] == "" {
			continue
		}
		fmt.Fprintf(&b, "export %s=\"%s\"\n", k, escapeDQ(out.Env[k]))
	}
	ekeys := make([]string, 0, len(extraEnv))
	for k := range extraEnv {
		ekeys = append(ekeys, k)
	}
	sort.Strings(ekeys)
	for _, k := range ekeys {
		if extraEnv[k] == "" {
			continue
		}
		fmt.Fprintf(&b, "export %s=\"%s\"\n", k, escapeDQ(extraEnv[k]))
	}
	for i, f := range out.Configure {
		eof := fmt.Sprintf("REMOUNT_EOF_%d", i)
		if strings.Contains(f.Content, eof) {
			return "", fmt.Errorf("recipe %s: configure[%d] contains the heredoc delimiter", r.Name, i)
		}
		if dir := path.Dir(f.Path); dir != "." {
			fmt.Fprintf(&b, "mkdir -p %s\n", ShellQuote(dir))
		}
		fmt.Fprintf(&b, "cat > %s <<%s\n%s\n%s\n", ShellQuote(f.Path), eof, strings.TrimRight(f.Content, "\n"), eof)
		if f.Mode != 0 {
			fmt.Fprintf(&b, "chmod %o %s\n", f.Mode, ShellQuote(f.Path))
		}
	}
	b.WriteString("exec")
	for _, a := range argv {
		b.WriteString(" ")
		b.WriteString(ShellQuote(a))
	}
	b.WriteString("\n")
	return b.String(), nil
}

// escapeDQ prepares s for a double-quoted shell string while keeping `$`
// expansions, which is what recipe env values rely on.
func escapeDQ(s string) string {
	s = strings.ReplaceAll(s, `\`, `\\`)
	s = strings.ReplaceAll(s, `"`, `\"`)
	s = strings.ReplaceAll(s, "`", "\\`")
	return s
}

// ShellQuote single-quotes s for POSIX sh.
func ShellQuote(s string) string {
	if s == "" {
		return "''"
	}
	if !strings.ContainsAny(s, " \t\n'\"\\$`!*?[]{}()<>|&;#~") {
		return s
	}
	return "'" + strings.ReplaceAll(s, "'", `'\''`) + "'"
}

// TaskHash is what run.started records instead of the task text.
func TaskHash(task string) string {
	sum := sha256.Sum256([]byte(task))
	return hex.EncodeToString(sum[:8])
}

// Extract returns the harness's conversation id from the state files under
// fsys (rooted at the workspace), or "" when no state file matches yet. The
// newest match wins because a harness that keeps one file per conversation
// has the current one last.
func (s *SessionIDFrom) Extract(fsys fs.FS) (string, error) {
	matches, err := fs.Glob(fsys, s.Glob)
	if err != nil {
		return "", fmt.Errorf("session_id_from: %w", err)
	}
	var best string
	var bestMod int64
	for _, m := range matches {
		info, err := fs.Stat(fsys, m)
		if err != nil || info.IsDir() {
			continue
		}
		if mod := info.ModTime().UnixNano(); best == "" || mod > bestMod || (mod == bestMod && m > best) {
			best, bestMod = m, mod
		}
	}
	if best == "" {
		return "", nil
	}
	if s.Key == "" {
		base := path.Base(best)
		return strings.TrimSuffix(base, path.Ext(base)), nil
	}
	b, err := fs.ReadFile(fsys, best)
	if err != nil {
		return "", fmt.Errorf("session_id_from: %w", err)
	}
	if len(b) > maxSessionStateFile {
		return "", fmt.Errorf("session_id_from: %s is larger than %d bytes", best, maxSessionStateFile)
	}
	var doc any
	if err := json.Unmarshal(b, &doc); err != nil {
		return "", fmt.Errorf("session_id_from: %s: %w", best, err)
	}
	cur := doc
	for _, part := range strings.Split(s.Key, ".") {
		obj, ok := cur.(map[string]any)
		if !ok {
			return "", fmt.Errorf("session_id_from: %s has no %q", best, s.Key)
		}
		cur, ok = obj[part]
		if !ok {
			return "", fmt.Errorf("session_id_from: %s has no %q", best, s.Key)
		}
	}
	id, ok := cur.(string)
	if !ok || id == "" {
		return "", fmt.Errorf("session_id_from: %s: %q is not a string", best, s.Key)
	}
	return id, nil
}

// maxSessionStateFile bounds how much of a harness state file Extract reads.
const maxSessionStateFile = 4 << 20
