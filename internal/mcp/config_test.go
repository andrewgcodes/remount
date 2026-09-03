package mcp

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

func TestConfigSnippetsAreSecretFreeAndHostNative(t *testing.T) {
	for _, host := range []string{"claude", "cursor", "gemini", "opencode"} {
		snippet, err := ConfigSnippet(host, "/opt/remount")
		if err != nil {
			t.Fatalf("%s: %v", host, err)
		}
		var decoded any
		if err := json.Unmarshal([]byte(snippet), &decoded); err != nil {
			t.Fatalf("%s invalid JSON: %v\n%s", host, err, snippet)
		}
		if strings.Contains(snippet, "REMOUNT_TOKEN=") || strings.Contains(snippet, ".remount/env") {
			t.Fatalf("%s embeds secret source: %s", host, snippet)
		}
	}
	codex, err := ConfigSnippet("codex", "remount")
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{"[mcp_servers.remount]", `env_vars = ["REMOUNT_SERVER", "REMOUNT_TOKEN", "REMOUNT_PRINCIPAL"]`, "required = true"} {
		if !strings.Contains(codex, want) {
			t.Errorf("codex snippet missing %q: %s", want, codex)
		}
	}
	goose, err := ConfigSnippet("goose", "remount")
	if err != nil {
		t.Fatal(err)
	}
	if goose != "goose session --with-extension \"remount mcp serve\"\n" {
		t.Fatalf("goose: %q", goose)
	}
}

func TestRewriteEnvUsesReferencesAndBrokerRoutes(t *testing.T) {
	env, err := rewriteEnv([]string{
		"PATH=/bin", "OPENAI_API_KEY=real-key-must-disappear", "OPENAI_BASE_URL=https://api.openai.com/v1/", "IGNORED=value",
	}, "http://127.0.0.1:1234/c/cap", map[string]string{"OPENAI_API_KEY": "b_openai"})
	if err != nil {
		t.Fatal(err)
	}
	joined := strings.Join(env, "\n")
	if strings.Contains(joined, "real-key-must-disappear") {
		t.Fatalf("secret retained: %s", joined)
	}
	if !strings.Contains(joined, "OPENAI_API_KEY=ref:b_openai") {
		t.Fatalf("placeholder missing: %s", joined)
	}
	if !strings.Contains(joined, "OPENAI_BASE_URL=http://127.0.0.1:1234/c/cap/d/api.openai.com/v1") {
		t.Fatalf("broker route missing: %s", joined)
	}
}

func TestRewriteEnvRejectsAmbiguousBaseURLWithoutEchoingIt(t *testing.T) {
	for _, secretURL := range []string{
		"http://user:password@example.test/v1",
		"https://example.test/v1?token=must-not-leak",
		"https://example.test/v1#must-not-leak",
	} {
		_, err := rewriteEnv([]string{"MODEL_BASE_URL=" + secretURL}, "http://broker", nil)
		if err == nil {
			t.Fatalf("expected rejection for %q", secretURL)
		}
		if strings.Contains(err.Error(), secretURL) || strings.Contains(err.Error(), "password") || strings.Contains(err.Error(), "must-not-leak") {
			t.Fatalf("error leaked value: %v", err)
		}
	}
}

func TestWrapExecutesArgvWithoutShellInterpolation(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("shell-free argv helper uses /bin/sh only as the child under test")
	}
	dir := t.TempDir()
	target := filepath.Join(dir, "literal")
	err := Wrap(context.Background(), WrapOptions{Command: []string{"/bin/sh", "-c", `test "$1" = '$(touch SHOULD_NOT_EXIST)' && printf ok > "$2"`, "sh", "$(touch SHOULD_NOT_EXIST)", target},
		Broker: "http://127.0.0.1:1", Env: []string{"PATH=/bin:/usr/bin"}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(target); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat("SHOULD_NOT_EXIST"); !os.IsNotExist(err) {
		t.Fatalf("argument was interpolated")
	}
}

func TestWrapRejectsCredentialBearingBrokerURLWithoutEcho(t *testing.T) {
	const broker = "https://user:must-not-leak@broker.test/cap"
	err := Wrap(context.Background(), WrapOptions{Command: []string{"unused"}, Broker: broker, Env: []string{"PATH=/bin"}})
	if err == nil || strings.Contains(err.Error(), broker) || strings.Contains(err.Error(), "must-not-leak") {
		t.Fatalf("unsafe broker error: %v", err)
	}
}
