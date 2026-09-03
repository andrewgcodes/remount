package launch

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

func TestEveryBuiltinRecipeLoadsAndRenders(t *testing.T) {
	names := Builtin()
	want := []string{"aider", "claude", "cline", "codex", "custom", "gemini", "goose", "opencode", "openhands"}
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

func TestAuthMode(t *testing.T) {
	custom, _ := Load("custom")
	if m, err := custom.AuthMode(nil); err != nil || m != AuthWorkspaceResident {
		t.Fatalf("custom without bindings: %s %v", m, err)
	}
	if m, err := custom.AuthMode([]string{"openai"}); err != nil || m != AuthAPIKey {
		t.Fatalf("custom with binding: %s %v", m, err)
	}
	aider, _ := Load("aider")
	if _, err := aider.AuthMode(nil); err == nil {
		t.Fatal("api_key recipe without a binding must be refused")
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
