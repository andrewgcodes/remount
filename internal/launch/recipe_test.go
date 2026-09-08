package launch

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"remount.dev/remount/internal/providerauth"
)

func TestEveryBuiltinRecipeLoadsAndRenders(t *testing.T) {
	names := Builtin()
	want := []string{"aider", "claude", "cline", "codex", "custom", "gemini", "goose", "opencode", "openhands", "pi"}
	if strings.Join(names, ",") != strings.Join(want, ",") {
		t.Fatalf("builtin recipes = %v, want %v", names, want)
	}
	for _, name := range names {
		r, err := Load(name)
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		if r.Name != name {
			t.Fatalf("%s: name %q", name, r.Name)
		}
		d := Data{Task: "say hi", Args: []string{"echo", "hi"}, Recipe: name, Workspace: "ws_1", Sandbox: SandboxWorkspaceWrite, Approve: ApproveNever, Model: "m"}
		if len(r.Providers) > 0 {
			d.Providers = []string{r.Providers[0]}
			d.Primary = r.Providers[0]
		}
		for _, sb := range []string{SandboxReadOnly, SandboxWorkspaceWrite, SandboxFull} {
			d.Sandbox = sb
			script, err := r.Launcher(d, false)
			if err != nil {
				t.Fatalf("%s/%s: %v", name, sb, err)
			}
			if !strings.HasPrefix(script, "#!/bin/sh\n") || !strings.Contains(script, ". ./.remount/env") {
				t.Fatalf("%s: launcher must source .remount/env:\n%s", name, script)
			}
			if strings.Contains(script, "REMOUNT_BROKER=") {
				t.Fatalf("%s: launcher must not bake a broker address in:\n%s", name, script)
			}
			shellCheck(t, script)
		}
		// Every recipe with a resume command renders it too.
		if len(r.ResumeCommand) > 0 {
			if _, err := r.Launcher(d, true); err != nil {
				t.Fatalf("%s resume: %v", name, err)
			}
		} else if _, err := r.Launcher(d, true); err == nil {
			t.Fatalf("%s: resume without resume_command must fail", name)
		}
	}
}

// shellCheck parses the script with sh -n when a shell is available so a
// quoting mistake in a recipe is caught by the unit suite.
func shellCheck(t *testing.T, script string) {
	t.Helper()
	sh, err := exec.LookPath("sh")
	if err != nil {
		t.Skip("no sh")
	}
	dir := t.TempDir()
	p := filepath.Join(dir, "launch.sh")
	if err := os.WriteFile(p, []byte(script), 0o700); err != nil {
		t.Fatal(err)
	}
	out, err := exec.Command(sh, "-n", p).CombinedOutput()
	if err != nil {
		t.Fatalf("sh -n: %v\n%s\n%s", err, out, script)
	}
}

func TestLauncherWritesConfigWithRuntimeBrokerAndQuotesTask(t *testing.T) {
	r, err := Load("opencode")
	if err != nil {
		t.Fatal(err)
	}
	task := "write a file; then $(echo pwned) 'quoted' \"double\""
	d := Data{Task: task, Recipe: "opencode", Sandbox: SandboxReadOnly, Approve: ApproveNever, Model: "openai/gpt-4o-mini", Providers: []string{"openai"}, Primary: "openai"}
	script, err := r.Launcher(d, false)
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{
		`"baseURL": "$OPENAI_BASE_URL"`,
		`"apiKey": "$OPENAI_API_KEY"`,
		`"edit": "deny"`,
		`"\$schema"`,
		"mkdir -p .remount/launch\ncat > .remount/launch/opencode.json <<REMOUNT_EOF_0",
		`export OPENCODE_CONFIG="$PWD/.remount/launch/opencode.json"`,
		`export XDG_STATE_HOME="/tmp/remount-opencode-state-$REMOUNT_WORKSPACE"`,
		"exec opencode run 'write a file; then $(echo pwned) '\\''quoted'\\'' \"double\"'",
	} {
		if !strings.Contains(script, want) {
			t.Errorf("launcher lacks %q:\n%s", want, script)
		}
	}
	shellCheck(t, script)

	// Actually run the launcher in a scratch workspace with a fake opencode
	// to prove the heredoc expands the broker at run time and the task
	// arrives byte-for-byte.
	sh, err := exec.LookPath("sh")
	if err != nil {
		t.Skip("no sh")
	}
	root := t.TempDir()
	if err := os.MkdirAll(filepath.Join(root, ".remount", "launch"), 0o755); err != nil {
		t.Fatal(err)
	}
	env := "REMOUNT_BROKER=http://broker.test:1/c/cap\nREMOUNT_WORKSPACE=ws_x\n"
	if err := os.WriteFile(filepath.Join(root, ".remount", "env"), []byte(env), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, ".remount", "launch", "opencode.sh"), []byte(script), 0o700); err != nil {
		t.Fatal(err)
	}
	bin := t.TempDir()
	fake := "#!/bin/sh\nprintf '%s\\n' \"$#\" \"$1\" \"$2\" > \"$OUT\"\n"
	if err := os.WriteFile(filepath.Join(bin, "opencode"), []byte(fake), 0o755); err != nil {
		t.Fatal(err)
	}
	outPath := filepath.Join(t.TempDir(), "argv")
	cmd := exec.Command(sh, filepath.Join(root, ".remount", "launch", "opencode.sh"))
	cmd.Env = append(os.Environ(), "PATH="+bin+":"+os.Getenv("PATH"), "OUT="+outPath,
		"OPENAI_API_KEY=ref-placeholder", "OPENAI_BASE_URL=http://broker.test:1/c/cap/d/api.openai.com/v1")
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("launcher: %v\n%s", err, out)
	}
	argv, err := os.ReadFile(outPath)
	if err != nil {
		t.Fatal(err)
	}
	if string(argv) != "2\nrun\n"+task+"\n" {
		t.Fatalf("fake harness saw %q", argv)
	}
	cfg, err := os.ReadFile(filepath.Join(root, ".remount/launch/opencode.json"))
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{
		`"$schema": "https://opencode.ai/config.json"`,
		`"baseURL": "http://broker.test:1/c/cap/d/api.openai.com/v1"`,
		`"apiKey": "ref-placeholder"`,
		`"model": "openai/gpt-4o-mini"`,
	} {
		if !strings.Contains(string(cfg), want) {
			t.Errorf("config lacks %q:\n%s", want, cfg)
		}
	}
}

func TestOpenCodeRecipeAllowsDefaultServiceHost(t *testing.T) {
	r, err := Load("opencode")
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Contains(r.Hosts, "opencode.ai") {
		t.Fatalf("OpenCode hosts = %v; default model cannot reach opencode.ai", r.Hosts)
	}
}

func TestOpenCodeKeepsTransientStateOutsideWorkspace(t *testing.T) {
	r, err := Load("opencode")
	if err != nil {
		t.Fatal(err)
	}
	if got := r.Env["XDG_STATE_HOME"]; got != "/tmp/remount-opencode-state-$REMOUNT_WORKSPACE" {
		t.Fatalf("XDG_STATE_HOME = %q", got)
	}
	for _, dir := range r.StateDirs {
		if dir == ".local/state/opencode" {
			t.Fatal("transient OpenCode state must not be declared movable")
		}
	}
}

func TestNPMRecipesInstallIntoWorkspaceLocalPrefix(t *testing.T) {
	if os.PathSeparator == '\\' {
		t.Skip("recipes run under POSIX sh on workspace nodes")
	}
	sh, err := exec.LookPath("sh")
	if err != nil {
		t.Skip("no sh")
	}
	for _, name := range []string{"claude", "cline", "codex", "gemini", "opencode", "pi"} {
		t.Run(name, func(t *testing.T) {
			r, err := Load(name)
			if err != nil {
				t.Fatal(err)
			}
			if got := r.Env["PATH"]; got != "${NPM_CONFIG_PREFIX:-$HOME/.local}/bin:$PATH" {
				t.Fatalf("PATH = %q", got)
			}
			script, err := r.InstallScript(Data{})
			if err != nil {
				t.Fatal(err)
			}
			root := t.TempDir()
			bin := t.TempDir()
			got := filepath.Join(t.TempDir(), "npm-env")
			npm := "#!/bin/sh\nprintf '%s\\n%s\\n' \"$NPM_CONFIG_PREFIX\" \"$PATH\" > \"$OUT\"\n"
			if err := os.WriteFile(filepath.Join(bin, "npm"), []byte(npm), 0o755); err != nil {
				t.Fatal(err)
			}
			cmd := exec.Command(sh, "-c", script)
			cmd.Env = []string{
				"HOME=" + root,
				"OUT=" + got,
				"PATH=" + bin + ":/usr/bin:/bin",
			}
			if out, err := cmd.CombinedOutput(); err != nil {
				t.Fatalf("install: %v\n%s\n%s", err, out, script)
			}
			b, err := os.ReadFile(got)
			if err != nil {
				t.Fatal(err)
			}
			wantPrefix := filepath.Join(root, ".local")
			lines := strings.Split(strings.TrimSpace(string(b)), "\n")
			if len(lines) != 2 || lines[0] != wantPrefix || !strings.HasPrefix(lines[1], wantPrefix+"/bin:") {
				t.Fatalf("npm env = %q, want prefix %q first on PATH", string(b), wantPrefix)
			}
		})
	}
}

func TestParseRejectsBadRecipes(t *testing.T) {
	cases := map[string]string{
		"no command":       "name: x\nproviders: [openai]\n",
		"bad name":         "name: Bad Name\ncommand: [a]\nproviders: [openai]\n",
		"unknown provider": "name: x\ncommand: [a]\nproviders: [nope]\n",
		"no providers":     "name: x\ncommand: [a]\n",
		"bad auth":         "name: x\ncommand: [a]\nauth: magic\n",
		"template error":   "name: x\ncommand: [\"{{.Nope}}\"]\nproviders: [openai]\n",
		"reserved path":    "name: x\ncommand: [a]\nproviders: [openai]\nconfigure:\n  - path: .remount/env\n    content: x\n",
		"launcher path":    "name: x\ncommand: [a]\nproviders: [openai]\nconfigure:\n  - path: .remount/launch/x.sh\n    content: x\n",
		"escape path":      "name: x\ncommand: [a]\nproviders: [openai]\nconfigure:\n  - path: ../x\n    content: x\n",
		"abs path":         "name: x\ncommand: [a]\nproviders: [openai]\nconfigure:\n  - path: /etc/passwd\n    content: x\n",
		"bad sandbox key":  "name: x\ncommand: [a]\nproviders: [openai]\nsandbox_flags:\n  yolo: [a]\n",
		"unknown field":    "name: x\ncommand: [a]\nproviders: [openai]\nfoo: 1\n",
		"bad env name":     "name: x\ncommand: [a]\nproviders: [openai]\nenv:\n  \"A-B\": x\n",
	}
	for name, src := range cases {
		if _, err := Parse([]byte(src)); err == nil {
			t.Errorf("%s: expected error", name)
		}
	}
	if _, err := Parse([]byte("name: x\ncommand: [a]\nproviders: [openai]\nconfigure:\n  - path: .remount/launch/x.json\n    content: x\n")); err != nil {
		t.Errorf("launcher-dir config must be allowed: %v", err)
	}
}

func TestBuiltinResumePolicyFlags(t *testing.T) {
	for _, tc := range []struct {
		name    string
		sandbox string
		flags   []string
	}{
		{"claude", SandboxReadOnly, []string{"--permission-mode", "plan"}},
		{"claude", SandboxWorkspaceWrite, []string{"--permission-mode", "acceptEdits"}},
		{"claude", SandboxFull, []string{"--dangerously-skip-permissions"}},
		{"codex", SandboxReadOnly, []string{"--sandbox", "read-only"}},
		{"codex", SandboxWorkspaceWrite, []string{"--sandbox", "workspace-write"}},
		{"codex", SandboxFull, []string{"--sandbox", "danger-full-access"}},
	} {
		t.Run(tc.name+"/"+tc.sandbox, func(t *testing.T) {
			r, err := Load(tc.name)
			if err != nil {
				t.Fatal(err)
			}
			d := Data{Recipe: tc.name, Task: "continue 'quoted' $(printf unsafe)\non another line", Sandbox: tc.sandbox, Approve: ApproveNever}
			var command, resume []string
			switch tc.name {
			case "claude":
				command = append([]string{"claude", "-p", d.Task, "--output-format", "text"}, tc.flags...)
				resume = append([]string{"claude", "--continue", "-p", d.Task, "--output-format", "text"}, tc.flags...)
			case "codex":
				command = append([]string{"sh", ".remount/launch/codex-driver", "exec", "--skip-git-repo-check", d.Task}, tc.flags...)
				resume = append([]string{"sh", ".remount/launch/codex-driver", "exec"}, tc.flags...)
				resume = append(resume, "resume", "--last", "--skip-git-repo-check", d.Task)
			}
			assertRecipeLaunchCommands(t, r, d, command, resume)
		})
	}
}

func TestRecipeResumePolicyFlags(t *testing.T) {
	r, err := Parse([]byte(`
name: policy-test
auth: workspace_resident
command: ["harness", "run", "{{.Task}}"]
resume_command: ["harness", "resume", "{{.Task}}"]
sandbox_flags:
  workspace-write: ["--sandbox", "{{.Sandbox}}"]
approve_flags:
  never: ["--approve", "{{.Approve}}"]
  on-request: ["--approve", "{{.Approve}}"]
`))
	if err != nil {
		t.Fatal(err)
	}
	for _, approve := range []string{ApproveNever, ApproveOnRequest} {
		t.Run(approve, func(t *testing.T) {
			d := Data{Recipe: r.Name, Task: "keep going", Sandbox: SandboxWorkspaceWrite, Approve: approve}
			flags := []string{"--sandbox", d.Sandbox, "--approve", d.Approve}
			command := append([]string{"harness", "run", d.Task}, flags...)
			resume := append([]string{"harness", "resume", d.Task}, flags...)
			assertRecipeLaunchCommands(t, r, d, command, resume)
		})
	}
	r.ResumeCommand = nil
	if _, err := r.Launcher(Data{Sandbox: SandboxWorkspaceWrite, Approve: ApproveNever}, true); err == nil {
		t.Fatal("policy flags must not create a resume command")
	}
}

func TestCodexResumeApprovalFlags(t *testing.T) {
	r, err := Load("codex")
	if err != nil {
		t.Fatal(err)
	}
	r.ApproveFlags = map[string][]string{ApproveNever: {"-c", "approval_policy=\"{{.Approve}}\""}}
	d := Data{Recipe: r.Name, Task: "continue", Sandbox: SandboxReadOnly, Approve: ApproveNever}
	flags := []string{"--sandbox", "read-only", "-c", "approval_policy=\"never\""}
	command := append([]string{"sh", ".remount/launch/codex-driver", "exec", "--skip-git-repo-check", d.Task}, flags...)
	resume := append([]string{"sh", ".remount/launch/codex-driver", "exec"}, flags...)
	resume = append(resume, "resume", "--last", "--skip-git-repo-check", d.Task)
	assertRecipeLaunchCommands(t, r, d, command, resume)
}

func TestCodexBoundAPIKey(t *testing.T) {
	r, err := Load("codex")
	if err != nil {
		t.Fatal(err)
	}
	for _, provider := range append([]string{""}, r.Providers...) {
		t.Run("provider="+provider, func(t *testing.T) {
			d := Data{Recipe: r.Name, Task: "continue", Primary: provider}
			want := ""
			if provider != "" {
				d.Providers = []string{provider}
				preset, _ := LookupPreset(provider)
				want = "export CODEX_API_KEY=\"$" + preset.KeyEnv + "\"\n"
			}
			for _, resume := range []bool{false, true} {
				script, err := r.Launcher(d, resume)
				if err != nil {
					t.Fatal(err)
				}
				if want == "" {
					if strings.Contains(script, "export CODEX_API_KEY=") {
						t.Fatal("unbound launch must not override Codex auth")
					}
				} else if !strings.Contains(script, want) {
					t.Errorf("launcher lacks %q:\n%s", want, script)
				}
			}
		})
	}
}

func TestCodexACPBoundAPIKey(t *testing.T) {
	r, err := Load("codex")
	if err != nil {
		t.Fatal(err)
	}
	for _, primary := range []string{"", "openai"} {
		t.Run("primary="+primary, func(t *testing.T) {
			d := Data{Recipe: r.Name, Primary: primary}
			if primary != "" {
				d.Providers = []string{primary}
			}
			script, err := r.ACPLauncher(d)
			if err != nil {
				t.Fatal(err)
			}
			for _, want := range []string{
				`export CODEX_API_KEY="$OPENAI_API_KEY"`,
				`export CODEX_CONFIG="{\"openai_base_url\":\"$OPENAI_BASE_URL\"}"`,
				`export DEFAULT_AUTH_REQUEST="{\"methodId\":\"api-key\"}"`,
			} {
				if primary == "" {
					if strings.Contains(script, want) {
						t.Fatalf("unbound ACP launcher contains %q:\n%s", want, script)
					}
				} else if !strings.Contains(script, want) {
					t.Fatalf("bound ACP launcher lacks %q:\n%s", want, script)
				}
			}
		})
	}
}

func TestCodexACPSandboxMode(t *testing.T) {
	r, err := Load("codex")
	if err != nil {
		t.Fatal(err)
	}
	for sandbox, mode := range map[string]string{
		SandboxReadOnly:       "read-only",
		SandboxWorkspaceWrite: "agent",
		SandboxFull:           "agent-full-access",
	} {
		t.Run(sandbox, func(t *testing.T) {
			script, err := r.ACPLauncher(Data{Recipe: r.Name, Sandbox: sandbox})
			if err != nil {
				t.Fatal(err)
			}
			want := `export INITIAL_AGENT_MODE="` + mode + `"`
			if !strings.Contains(script, want) {
				t.Fatalf("ACP launcher lacks %q:\n%s", want, script)
			}
		})
	}
}

func TestClaudeACPSandboxMode(t *testing.T) {
	r, err := Load("claude")
	if err != nil {
		t.Fatal(err)
	}
	for sandbox, mode := range map[string]string{
		SandboxReadOnly:       "plan",
		SandboxWorkspaceWrite: "acceptEdits",
		SandboxFull:           "bypassPermissions",
	} {
		t.Run(sandbox, func(t *testing.T) {
			if got := r.ACP.SandboxModes[sandbox]; got != mode {
				t.Fatalf("ACP mode = %q, want %q", got, mode)
			}
		})
	}
}

func TestCodexModelFlags(t *testing.T) {
	r, err := Load("codex")
	if err != nil {
		t.Fatal(err)
	}
	for _, model := range []string{"", "gpt-4.1-mini"} {
		t.Run("model="+model, func(t *testing.T) {
			d := Data{Recipe: r.Name, Task: "continue", Model: model, Sandbox: SandboxWorkspaceWrite, Approve: ApproveNever}
			flags := []string{"--sandbox", "workspace-write"}
			if model != "" {
				flags = append(flags, "--model", model)
			}
			command := append([]string{"sh", ".remount/launch/codex-driver", "exec", "--skip-git-repo-check", d.Task}, flags...)
			resume := append([]string{"sh", ".remount/launch/codex-driver", "exec"}, flags...)
			resume = append(resume, "resume", "--last", "--skip-git-repo-check", d.Task)
			assertRecipeLaunchCommands(t, r, d, command, resume)
		})
	}
}

func TestCodexRuntimeBrokerRouting(t *testing.T) {
	if os.PathSeparator == '\\' {
		t.Skip("recipes run under POSIX sh on workspace nodes")
	}
	sh, err := exec.LookPath("sh")
	if err != nil {
		t.Skip("no sh")
	}
	r, err := Load("codex")
	if err != nil {
		t.Fatal(err)
	}
	for _, bound := range []bool{false, true} {
		for _, resume := range []bool{false, true} {
			t.Run(fmt.Sprintf("bound=%t/resume=%t", bound, resume), func(t *testing.T) {
				root := t.TempDir()
				bin := t.TempDir()
				for _, dir := range []string{".remount/launch", ".codex"} {
					if err := os.MkdirAll(filepath.Join(root, dir), 0o755); err != nil {
						t.Fatal(err)
					}
				}
				config := "model_provider = \"user-owned\"\n"
				configPath := filepath.Join(root, ".codex", "config.toml")
				if err := os.WriteFile(configPath, []byte(config), 0o600); err != nil {
					t.Fatal(err)
				}
				fake := "#!/bin/sh\nprintf '%s\\000' \"$CODEX_API_KEY\" \"$@\"\n"
				if err := os.WriteFile(filepath.Join(bin, "codex"), []byte(fake), 0o755); err != nil {
					t.Fatal(err)
				}
				d := Data{Recipe: r.Name, Task: "continue 'quoted' $(printf unsafe)\non another line", Sandbox: SandboxWorkspaceWrite, Approve: ApproveNever, Model: "gpt-4.1-mini"}
				if resume {
					d.Conversation = "11111111-1111-4111-8111-111111111111"
				}
				if bound {
					d.Primary = "openai"
					d.Providers = []string{"openai"}
				}
				script, err := r.Launcher(d, resume)
				if err != nil {
					t.Fatal(err)
				}
				launcher := filepath.Join(root, r.LauncherPath())
				if err := os.WriteFile(launcher, []byte(script), 0o700); err != nil {
					t.Fatal(err)
				}
				for _, broker := range []string{"http://broker-one.test:123/c/first", "http://broker-two.test:456/c/second"} {
					env := "export OPENAI_BASE_URL=" + broker + "/d/api.openai.com/v1\n"
					if err := os.WriteFile(filepath.Join(root, ".remount", "env"), []byte(env), 0o600); err != nil {
						t.Fatal(err)
					}
					cmd := exec.Command(sh, launcher)
					cmd.Env = []string{"HOME=" + root, "PATH=" + bin + ":/usr/bin:/bin", "OPENAI_API_KEY=ref:test-binding", "CODEX_API_KEY=existing-auth"}
					out, err := cmd.CombinedOutput()
					if err != nil {
						t.Fatalf("launcher: %v\n%s\n%s", err, out, script)
					}
					want := []string{"existing-auth"}
					if bound {
						want = []string{"ref:test-binding", "-c", "openai_base_url=\"" + broker + "/d/api.openai.com/v1\""}
					}
					want = append(want, "exec")
					flags := []string{"--sandbox", "workspace-write", "--model", d.Model}
					if resume {
						want = append(want, flags...)
						want = append(want, "resume", d.Conversation, "--skip-git-repo-check", d.Task)
					} else {
						want = append(want, "--skip-git-repo-check", d.Task)
						want = append(want, flags...)
					}
					got := strings.Split(strings.TrimSuffix(string(out), "\x00"), "\x00")
					if !slices.Equal(got, want) {
						t.Errorf("codex received %q, want %q", got, want)
					}
					driver, err := os.ReadFile(filepath.Join(root, ".remount", "launch", "codex-driver"))
					if err != nil {
						t.Fatal(err)
					}
					for _, contents := range []string{script, string(driver)} {
						if strings.Contains(contents, broker) {
							t.Fatal("launcher and wrapper must discover the broker at runtime")
						}
						if strings.Contains(contents, "ref:test-binding") || strings.Contains(contents, "existing-auth") {
							t.Fatal("launcher and wrapper must not contain credential literals")
						}
					}
				}
				gotConfig, err := os.ReadFile(configPath)
				if err != nil || string(gotConfig) != config {
					t.Fatalf("user config changed: %q, %v", gotConfig, err)
				}
			})
		}
	}
}

func TestBuiltinExplicitConversationArgv(t *testing.T) {
	const id = "11111111-1111-4111-8111-111111111111"
	for _, name := range []string{"claude", "codex"} {
		t.Run(name, func(t *testing.T) {
			r, err := Load(name)
			if err != nil {
				t.Fatal(err)
			}
			d := Data{Recipe: name, Task: "continue", Sandbox: SandboxWorkspaceWrite, Model: "test-model", Conversation: id}
			out, err := r.render(d)
			if err != nil {
				t.Fatal(err)
			}
			if slices.Contains(out.Resume, "--last") || slices.Contains(out.Resume, "--continue") {
				t.Errorf("explicit conversation fell back to latest: %q", out.Resume)
			}
			if !slices.Contains(out.Resume, id) && !slices.Contains(out.Resume, "--resume="+id) {
				t.Errorf("selected conversation absent from argv: %q", out.Resume)
			}
		})
	}
}

func TestRecipeModelFlags(t *testing.T) {
	src := `
name: model-test
auth: workspace_resident
command: ["harness", "run", "{{.Task}}"]
resume_command: ["harness", "resume", "{{.Task}}"]
sandbox_flags:
  read-only: ["--sandbox", "{{.Sandbox}}"]
approve_flags:
  never: ["--approve", "{{.Approve}}"]
model_flags: ["--model", "{{.Model}}"]
`
	r, err := Parse([]byte(src))
	if err != nil {
		t.Fatal(err)
	}
	for _, model := range []string{"", "a model 'with quotes'"} {
		d := Data{Recipe: r.Name, Task: "continue", Sandbox: SandboxReadOnly, Approve: ApproveNever, Model: model}
		flags := []string{"--sandbox", d.Sandbox, "--approve", d.Approve}
		if model != "" {
			flags = append(flags, "--model", model)
		}
		command := append([]string{"harness", "run", d.Task}, flags...)
		resume := append([]string{"harness", "resume", d.Task}, flags...)
		assertRecipeLaunchCommands(t, r, d, command, resume)
	}
	if _, err := Parse([]byte(strings.ReplaceAll(src, "{{.Model}}", "{{.NoSuchField}}"))); err == nil {
		t.Fatal("model_flags template error must fail at load time")
	}
}

func assertRecipeLaunchCommands(t *testing.T, r *Recipe, d Data, command, resume []string) {
	t.Helper()
	originalCommand := slices.Clone(r.Command)
	originalResume := slices.Clone(r.ResumeCommand)
	for i := 0; i < 2; i++ {
		out, err := r.render(d)
		if err != nil {
			t.Fatal(err)
		}
		if !slices.Equal(out.Command, command) {
			t.Errorf("command = %q, want %q", out.Command, command)
		}
		if !slices.Equal(out.Resume, resume) {
			t.Errorf("resume = %q, want %q", out.Resume, resume)
		}
		for _, isResume := range []bool{false, true} {
			script, err := r.Launcher(d, isResume)
			if err != nil {
				t.Fatal(err)
			}
			want := command
			if isResume {
				want = resume
			}
			quoted := make([]string, len(want))
			for j, arg := range want {
				quoted[j] = ShellQuote(arg)
			}
			if tail := "exec " + strings.Join(quoted, " ") + "\n"; !strings.HasSuffix(script, tail) {
				t.Errorf("resume=%t: launcher does not end with %q:\n%s", isResume, tail, script)
			}
		}
	}
	if !slices.Equal(r.Command, originalCommand) || !slices.Equal(r.ResumeCommand, originalResume) {
		t.Fatal("rendering must not mutate recipe commands")
	}
}

func TestAuthMode(t *testing.T) {
	custom, _ := Load("custom")
	if m, err := custom.AuthMode("", nil); err != nil || m != AuthWorkspaceResident {
		t.Fatalf("custom without bindings: %s %v", m, err)
	}
	if m, err := custom.AuthMode("", []string{"openai"}); err != nil || m != AuthAPIKey {
		t.Fatalf("custom with binding: %s %v", m, err)
	}
	aider, _ := Load("aider")
	if _, err := aider.AuthMode("", nil); err == nil {
		t.Fatal("api_key recipe without a binding must be refused")
	}
	claude, _ := Load("claude")
	if _, err := claude.AuthMode("", nil); err == nil || !strings.Contains(err.Error(), "requires --auth") {
		t.Fatalf("ambiguous Claude auth accepted: %v", err)
	}
	if mode, err := claude.AuthMode(AuthSubscription, nil); err != nil || mode != AuthSubscription {
		t.Fatalf("subscription mode = %q, %v", mode, err)
	}
	if _, err := claude.AuthMode(AuthSubscription, []string{"anthropic"}); err == nil || !strings.Contains(err.Error(), "cannot use provider bindings") {
		t.Fatalf("subscription binding accepted: %v", err)
	}
	if _, err := claude.AuthMode(AuthAPIKey, nil); err == nil || !strings.Contains(err.Error(), "explicit provider binding") {
		t.Fatalf("api-key mode without binding accepted: %v", err)
	}
	if mode, err := claude.AuthMode(AuthAPIKey, []string{"anthropic"}); err != nil || mode != AuthAPIKey {
		t.Fatalf("api-key mode = %q, %v", mode, err)
	}
}

func TestSubscriptionLaunchersStripAPIKeysAndVerifyStatus(t *testing.T) {
	for _, name := range []string{"claude", "codex"} {
		r, err := Load(name)
		if err != nil {
			t.Fatal(err)
		}
		if r.Subscription == nil || len(r.Subscription.Login) == 0 || len(r.Subscription.Status) == 0 || len(r.Subscription.Logout) == 0 {
			t.Fatalf("%s subscription metadata = %+v", name, r.Subscription)
		}
		approved, ok := providerauth.Lookup(name)
		if !ok || !slices.Equal(r.Subscription.Login, approved.Login) || !slices.Equal(r.Subscription.Status, approved.Status) || !slices.Equal(r.Subscription.Logout, approved.Logout) || !slices.Equal(r.Subscription.UnsetEnv, approved.UnsetEnv) {
			t.Fatalf("%s recipe and node auth contracts differ: recipe=%+v node=%+v", name, r.Subscription, approved)
		}
		script, err := r.Launcher(Data{Recipe: name, Workspace: "ws_auth", Auth: AuthSubscription}, false)
		if err != nil {
			t.Fatal(err)
		}
		for _, key := range r.Subscription.UnsetEnv {
			if !strings.Contains(script, "unset "+key+"\n") {
				t.Errorf("%s launcher did not unset %s:\n%s", name, key, script)
			}
			if strings.Contains(script, "export "+key+"=") {
				t.Errorf("%s launcher re-exported %s:\n%s", name, key, script)
			}
		}
		if !strings.Contains(script, ") >/dev/null 2>&1") || !strings.Contains(script, "subscription login is unavailable") {
			t.Errorf("%s launcher does not fail closed with sanitized status output:\n%s", name, script)
		}
	}
}

func TestParseBinding(t *testing.T) {
	b, err := ParseBinding("b_openai")
	if err != nil || b.Preset.Name != "openai" {
		t.Fatalf("%+v %v", b, err)
	}
	env := b.SessionEnv()
	if env["OPENAI_API_KEY"] != "ref:b_openai" || env["OPENAI_BASE_URL"] != "${REMOUNT_BROKER}/d/api.openai.com/v1" {
		t.Fatalf("env %v", env)
	}
	b, err = ParseBinding("b_work-key:anthropic")
	if err != nil || b.ID != "b_work-key" || b.Preset.Name != "anthropic" {
		t.Fatalf("%+v %v", b, err)
	}
	b, err = ParseBinding("b_az:azure-openai?host=myres")
	if err != nil || b.Host != "myres" || b.Hosts()[0] != "myres.openai.azure.com" || b.BaseURLPath() != "/d/myres.openai.azure.com" {
		t.Fatalf("%+v %v", b, err)
	}
	for _, bad := range []string{"openai", "b_az:azure-openai", "b_openai?host=x", "b_x:nope", "b_az:azure-openai?host=a.b", "b_openai:openai?foo=1"} {
		if _, err := ParseBinding(bad); err == nil {
			t.Errorf("%q must be rejected", bad)
		}
	}
	if len(PresetNames()) != 13 {
		t.Fatalf("presets: %v", PresetNames())
	}
}

func TestBindingsForWorkspaceUsesAgentSpecsAndLegacyLabels(t *testing.T) {
	specs := []string{"b_team:openai", "b_az:azure-openai?host=myres"}
	got, err := BindingsForWorkspace(specs, map[string]string{LabelBindings: "b_wrong:anthropic"}, []string{"b_team", "b_az"})
	if err != nil {
		t.Fatal(err)
	}
	if got[0].ID != "b_team" || got[0].Preset.Name != "openai" || got[1].Host != "myres" {
		t.Fatalf("agent bindings = %+v", got)
	}
	legacy, err := BindingsForWorkspace(nil, map[string]string{LabelBindings: "b_team:openai"}, []string{"b_team"})
	if err != nil || len(legacy) != 1 || legacy[0].ID != "b_team" {
		t.Fatalf("legacy bindings = %+v, %v", legacy, err)
	}
	if _, err := BindingsForWorkspace([]string{"b_other:openai"}, nil, []string{"b_team"}); err == nil {
		t.Fatal("an unattached agent binding must be refused")
	}
	for _, specs := range [][]string{
		{"b_team:openai", "b_team:openai"},
		{"b_team:openai", "b_other:openai"},
	} {
		if _, err := BindingsForWorkspace(specs, nil, []string{"b_team", "b_other"}); err == nil {
			t.Fatalf("ambiguous agent bindings %v were accepted", specs)
		}
	}
}

func TestShellQuote(t *testing.T) {
	for in, want := range map[string]string{
		"":         "''",
		"plain":    "plain",
		"a b":      "'a b'",
		"it's":     `'it'\''s'`,
		"$HOME":    "'$HOME'",
		"a;b":      "'a;b'",
		"x=1,y=2":  "x=1,y=2",
		"path/to":  "path/to",
		"--flag=v": "--flag=v",
	} {
		if got := ShellQuote(in); got != want {
			t.Errorf("ShellQuote(%q) = %s, want %s", in, got, want)
		}
	}
}

// The ACP commands below were each checked against the harness's own
// documentation (see the comment in every recipe). A change here is a change
// to what runs in every Agent built from the recipe, so it is asserted.
func TestBuiltinACPCommands(t *testing.T) {
	want := map[string][]string{
		"opencode":  {"opencode", "acp"},
		"goose":     {"goose", "acp", "--with-builtin", "developer"},
		"openhands": {"openhands", "acp"},
		"gemini":    {"gemini", "--acp"},
		"cline":     {"cline", "--acp"},
		"pi":        {"npx", "-y", "pi-acp"},
		"claude":    {"npx", "-y", "@agentclientprotocol/claude-agent-acp"},
		"codex":     {"npx", "-y", "@agentclientprotocol/codex-acp"},
	}
	for _, name := range Builtin() {
		r, err := Load(name)
		if err != nil {
			t.Fatal(err)
		}
		d := Data{Recipe: name, Workspace: "ws_1", Sandbox: SandboxWorkspaceWrite, Approve: ApproveNever}
		if len(r.Providers) > 0 {
			d.Providers = []string{r.Providers[0]}
			d.Primary = r.Providers[0]
		}
		argv, ok := want[name]
		if !ok {
			if r.Mode() != ModePTY {
				t.Fatalf("%s: expected pty mode", name)
			}
			if _, err := r.ACPArgv(d); err == nil {
				t.Fatalf("%s: ACPArgv without acp must fail", name)
			}
			if _, err := r.ACPLauncher(d); err == nil {
				t.Fatalf("%s: ACPLauncher without acp must fail", name)
			}
			continue
		}
		if r.Mode() != ModeACP {
			t.Fatalf("%s: expected acp mode", name)
		}
		got, err := r.ACPArgv(d)
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		if strings.Join(got, " ") != strings.Join(argv, " ") {
			t.Fatalf("%s: acp command %q, want %q", name, got, argv)
		}
		script, err := r.ACPLauncher(d)
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		if !strings.HasSuffix(strings.TrimSpace(script), "exec "+strings.Join(argv, " ")) {
			t.Fatalf("%s: launcher tail:\n%s", name, script)
		}
		shellCheck(t, script)
		if r.ACPLauncherPath() == r.LauncherPath() {
			t.Fatalf("%s: acp launcher must not overwrite the pty launcher", name)
		}
	}
	if got := len(want); got != 8 {
		t.Fatalf("table has %d rows", got)
	}
}

func TestRecipeACPUIAndPromptFields(t *testing.T) {
	src := `
name: t
auth: workspace_resident
command: ["h"]
acp:
  command: ["h", "acp", "--ws", "{{.Workspace}}"]
  env:
    H_FLAG: "{{.Sandbox}}"
  load_session: true
ui:
  command: ["h", "web", "--port", "{{.Port}}"]
  port: 4096
prompt_template: "/say {{.Message}}\r"
session_id_from:
  glob: ".h/sessions/*.json"
  key: meta.id
`
	r, err := Parse([]byte(src))
	if err != nil {
		t.Fatal(err)
	}
	d := Data{Recipe: "t", Workspace: "ws_9", Sandbox: SandboxReadOnly, Approve: ApproveNever}
	argv, err := r.ACPArgv(d)
	if err != nil || strings.Join(argv, " ") != "h acp --ws ws_9" {
		t.Fatalf("acp argv %q %v", argv, err)
	}
	script, err := r.ACPLauncher(d)
	if err != nil || !strings.Contains(script, `export H_FLAG="read-only"`) {
		t.Fatalf("acp env not exported:\n%s\n%v", script, err)
	}
	ui, err := r.UIArgv(d)
	if err != nil || strings.Join(ui, " ") != "h web --port 4096" {
		t.Fatalf("ui argv %q %v", ui, err)
	}
	ui, err = r.UIArgv(Data{Recipe: "t", Port: 5000})
	if err != nil || ui[3] != "5000" {
		t.Fatalf("ui argv with explicit port %q %v", ui, err)
	}
	p, err := r.PromptBytes("hello world")
	if err != nil || p != "/say hello world\r" {
		t.Fatalf("prompt %q %v", p, err)
	}
	plain, _ := Parse([]byte("name: p\nauth: workspace_resident\ncommand: [\"h\"]\n"))
	if p, _ := plain.PromptBytes("x"); p != "x\n" {
		t.Fatalf("default prompt template %q", p)
	}

	dir := t.TempDir()
	fsys := os.DirFS(dir)
	if id, err := r.SessionIDFrom.Extract(fsys); err != nil || id != "" {
		t.Fatalf("no state yet: %q %v", id, err)
	}
	if err := os.MkdirAll(filepath.Join(dir, ".h", "sessions"), 0o755); err != nil {
		t.Fatal(err)
	}
	write := func(name, body string, age time.Duration) {
		p := filepath.Join(dir, ".h", "sessions", name)
		if err := os.WriteFile(p, []byte(body), 0o644); err != nil {
			t.Fatal(err)
		}
		when := time.Now().Add(-age)
		if err := os.Chtimes(p, when, when); err != nil {
			t.Fatal(err)
		}
	}
	write("old.json", `{"meta":{"id":"old"}}`, time.Hour)
	write("new.json", `{"meta":{"id":"new"}}`, 0)
	write("ignored.txt", `{"meta":{"id":"txt"}}`, 0)
	if id, err := r.SessionIDFrom.Extract(fsys); err != nil || id != "new" {
		t.Fatalf("newest wins: %q %v", id, err)
	}
	write("bad.json", `{"meta":{}}`, -time.Hour)
	if _, err := r.SessionIDFrom.Extract(fsys); err == nil {
		t.Fatal("missing key must be an error, not an empty id")
	}
	byName := &SessionIDFrom{Glob: ".h/sessions/*.json"}
	if id, err := byName.Extract(fsys); err != nil || id != "bad" {
		t.Fatalf("basename id %q %v", id, err)
	}

	for _, bad := range []string{
		"name: t\nauth: workspace_resident\ncommand: [\"h\"]\nacp:\n  load_session: true\n",
		"name: t\nauth: workspace_resident\ncommand: [\"h\"]\nui:\n  command: [\"h\"]\n  port: 0\n",
		"name: t\nauth: workspace_resident\ncommand: [\"h\"]\nui:\n  port: 80\n",
		"name: t\nauth: workspace_resident\ncommand: [\"h\"]\nsession_id_from:\n  glob: \"/abs/*.json\"\n",
		"name: t\nauth: workspace_resident\ncommand: [\"h\"]\nsession_id_from:\n  glob: \"../*.json\"\n",
		"name: t\nauth: workspace_resident\ncommand: [\"h\"]\nsession_id_from:\n  glob: \"[.json\"\n",
		"name: t\nauth: workspace_resident\ncommand: [\"h\"]\nacp:\n  command: [\"h\"]\n  env:\n    \"BAD-NAME\": x\n",
		"name: t\nauth: workspace_resident\ncommand: [\"h\"]\nprompt_template: \"{{.Nope}}\"\n",
	} {
		if _, err := Parse([]byte(bad)); err == nil {
			t.Fatalf("expected validation error for:\n%s", bad)
		}
	}
}

// A subscription launch has no broker in its path, so the recipe has to say
// which provider hosts the node must allow; a silent omission surfaces live as
// egress.denied on every request the harness makes.
func TestSubscriptionRecipesDeclareTheProviderHostsTheyReachDirectly(t *testing.T) {
	want := map[string][]string{"claude": {"api.anthropic.com"}, "codex": {"chatgpt.com", "auth.openai.com"}}
	for name, hosts := range want {
		r, err := Load(name)
		if err != nil {
			t.Fatal(err)
		}
		if r.Subscription == nil || !slices.Equal(r.Subscription.Hosts, hosts) {
			t.Fatalf("%s subscription hosts = %+v, want %v", name, r.Subscription, hosts)
		}
	}
}

// Claude Code's runtime cannot speak to the Anthropic API through an HTTPS
// CONNECT proxy (oven-sh/bun#30381), and a subscription launch has no /d/
// path to fall back on, so its launcher must drop the proxy variables after
// the API-key ones. Codex is unaffected and keeps the cooperative proxy.
func TestClaudeSubscriptionLaunchBypassesTheConnectProxy(t *testing.T) {
	claude, err := Load("claude")
	if err != nil {
		t.Fatal(err)
	}
	script, err := claude.Launcher(Data{Recipe: "claude", Workspace: "ws_auth", Auth: AuthSubscription}, false)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(script, "unset HTTPS_PROXY https_proxy HTTP_PROXY http_proxy") {
		t.Fatalf("claude subscription launcher keeps the proxy:\n%s", script)
	}
	codex, err := Load("codex")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(codex.Subscription.Verify, "codex login status 2>&1") {
		t.Fatalf("codex verify must capture stderr, where codex prints its status line: %q", codex.Subscription.Verify)
	}
	script, err = codex.Launcher(Data{Recipe: "codex", Workspace: "ws_auth", Auth: AuthSubscription}, false)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(script, "unset HTTPS_PROXY") {
		t.Fatalf("codex subscription launcher drops the proxy without a reason to:\n%s", script)
	}
	apiKey, err := claude.Launcher(Data{Recipe: "claude", Workspace: "ws_key", Auth: AuthAPIKey, Providers: []string{"anthropic"}, Primary: "anthropic"}, false)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(apiKey, "unset HTTPS_PROXY") {
		t.Fatalf("api-key launcher must keep the broker proxy:\n%s", apiKey)
	}
}
