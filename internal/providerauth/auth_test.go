package providerauth

import (
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"runtime"
	"strings"
	"testing"

	"remount.dev/remount/internal/proto"
)

func TestProgramUsesApprovedProviderContract(t *testing.T) {
	for _, tc := range []struct {
		recipe string
		action string
		want   string
		unset  []string
	}{
		{"claude", proto.AuthActionLogin, "exec 'claude' 'auth' 'login' '--claudeai'", []string{"ANTHROPIC_API_KEY", "ANTHROPIC_AUTH_TOKEN", "ANTHROPIC_BASE_URL"}},
		{"claude", proto.AuthActionStatus, "exec 'claude' 'auth' 'status'", []string{"ANTHROPIC_API_KEY"}},
		{"codex", proto.AuthActionLogin, "exec 'codex' 'login' '--device-auth'", []string{"CODEX_API_KEY", "OPENAI_API_KEY", "AZURE_OPENAI_API_KEY"}},
		{"codex", proto.AuthActionLogout, "exec 'codex' 'logout'", []string{"OPENAI_API_KEY"}},
	} {
		program, err := Program(tc.recipe, tc.action)
		if err != nil {
			t.Fatal(err)
		}
		if len(program) != 3 || program[0] != "/bin/sh" || program[1] != "-c" {
			t.Fatalf("%s %s program = %#v", tc.recipe, tc.action, program)
		}
		for _, name := range tc.unset {
			if !strings.Contains(program[2], "unset "+name+"\n") {
				t.Errorf("%s %s does not unset %s", tc.recipe, tc.action, name)
			}
		}
		if !strings.Contains(program[2], tc.want) {
			t.Errorf("%s %s script = %q, want %q", tc.recipe, tc.action, program[2], tc.want)
		}
	}
}

func TestProgramFindsRecipeInstalledProviderCLI(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("unavailable: provider auth commands use the recipe's Unix shell contract")
	}
	home := t.TempDir()
	bin := filepath.Join(home, ".local", "bin")
	if err := os.MkdirAll(bin, 0o700); err != nil {
		t.Fatal(err)
	}
	claude := filepath.Join(bin, "claude")
	if err := os.WriteFile(claude, []byte("#!/bin/sh\nprintf '%s\\n' \"$0\" \"$@\" \"${ANTHROPIC_API_KEY-unset}\"\n"), 0o700); err != nil {
		t.Fatal(err)
	}
	program, err := Program("claude", proto.AuthActionStatus)
	if err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command(program[0], program[1:]...)
	cmd.Env = append(os.Environ(), "HOME="+home, "PATH=/usr/bin:/bin", "ANTHROPIC_API_KEY=ambient")
	out, err := cmd.Output()
	if err != nil {
		t.Fatal(err)
	}
	want := strings.Join([]string{claude, "auth", "status", "unset", ""}, "\n")
	if string(out) != want {
		t.Fatalf("provider command output = %q, want %q", out, want)
	}
}

func TestLookupReturnsIndependentCopy(t *testing.T) {
	first, ok := Lookup("claude")
	if !ok {
		t.Fatal("claude contract missing")
	}
	first.Login[0] = "changed"
	second, _ := Lookup("claude")
	if reflect.DeepEqual(first.Login, second.Login) || second.Login[0] != "claude" {
		t.Fatalf("lookup returned shared command storage: %#v", second.Login)
	}
}

func TestProgramRejectsUnsupportedRequests(t *testing.T) {
	for _, tc := range []struct {
		recipe string
		action string
	}{
		{"other", proto.AuthActionLogin},
		{"claude", "refresh"},
	} {
		if _, err := Program(tc.recipe, tc.action); err == nil {
			t.Fatalf("Program(%q, %q) succeeded", tc.recipe, tc.action)
		}
	}
}
