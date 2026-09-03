package main

import (
	"context"
	"flag"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"remount.dev/remount/internal/proto"
)

func TestEgressRuleFlagParsesInlineAndFileStrictly(t *testing.T) {
	var rules egressRuleFlag
	inline := `{"id":"api-read","protocol":"HTTPS","hosts":["API.EXAMPLE:443"],"methods":["get"],"path_prefixes":["/v1"]}`
	if err := rules.Set(inline); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "rule.json")
	if err := os.WriteFile(path, []byte(`{"id":"packages","connector":"package","protocol":"https","hosts":["registry.example"]}`), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := rules.Set("@" + path); err != nil {
		t.Fatal(err)
	}
	if len(rules) != 2 || rules[0].ID != "api-read" || rules[1].ID != "packages" {
		t.Fatalf("rules=%#v", rules)
	}
	security, err := proto.NormalizeSecurity(proto.SecuritySpec{
		Network: proto.NetworkPolicy{Rules: rules},
	})
	if err != nil {
		t.Fatal(err)
	}
	if security.Network.Default != proto.NetworkDefaultDeny ||
		!reflect.DeepEqual(security.Network.Rules[0].Methods, []string{"GET"}) ||
		security.Network.Rules[1].Connector != proto.EgressConnectorPackage ||
		security.Network.Rules[1].SharedState != proto.SharedStateImmutableRead ||
		!reflect.DeepEqual(security.Network.Rules[1].Methods, []string{"GET", "HEAD"}) {
		t.Fatalf("normalized=%#v", security)
	}
	if err := rules.Set(`{"id":"bad","protocol":"https","hosts":["example.com"],"typo":true}`); err == nil {
		t.Fatal("unknown rule field was silently accepted")
	}
	if len(rules) != 2 {
		t.Fatalf("failed parse mutated rules: %#v", rules)
	}
}

func TestParseAllowsFlagsAfterPositionals(t *testing.T) {
	fs := flag.NewFlagSet("test", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	value := fs.String("value", "", "")
	parse(fs, []string{"workspace", "--value", "set"})
	if *value != "set" || fs.NArg() != 1 || fs.Arg(0) != "workspace" {
		t.Fatalf("value=%q args=%v", *value, fs.Args())
	}
}

func TestAbsFlagPathResolvesRelativeAndNamesFlag(t *testing.T) {
	dir := t.TempDir()
	wd, _ := os.Getwd()
	if err := os.Chdir(dir); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chdir(wd) })
	got, err := absFlagPath("data", "./remount-data")
	if err != nil {
		t.Fatal(err)
	}
	want, _ := filepath.EvalSymlinks(dir)
	gotReal, _ := filepath.EvalSymlinks(filepath.Dir(got))
	if !filepath.IsAbs(got) || gotReal != want || filepath.Base(got) != "remount-data" {
		t.Fatalf("got %q want under %q", got, want)
	}
	if _, err := absFlagPath("data", "  "); err == nil || !strings.Contains(err.Error(), "--data") {
		t.Fatalf("empty path error must name the flag: %v", err)
	}
	if _, err := buildNode("relative/node", common{}, nil, "process", "", nil, nil, nodeResourceOptions{}); err == nil || !strings.Contains(err.Error(), "absolute") {
		t.Fatalf("buildNode accepted a relative root: %v", err)
	}
}

func TestArityRejectsExtraPositionalsAndSuggestsImage(t *testing.T) {
	ctx := context.Background()
	err := cmdWS(ctx, []string{"create", "python:3.12", "--wait=false"})
	if err == nil || !strings.Contains(err.Error(), "--image python:3.12") {
		t.Fatalf("image-like positional: %v", err)
	}
	err = cmdWS(ctx, []string{"create", "extra"})
	if err == nil || !strings.Contains(err.Error(), `unexpected argument "extra"`) {
		t.Fatalf("plain extra positional: %v", err)
	}
	err = cmdWS(ctx, []string{"get", "ws_1", "ws_2"})
	if err == nil || !strings.Contains(err.Error(), `unexpected argument "ws_2"`) || strings.Contains(err.Error(), "--image") {
		t.Fatalf("ws get arity: %v", err)
	}
	err = cmdWS(ctx, []string{"destroy"})
	if err == nil || !strings.Contains(err.Error(), "missing argument: ws destroy WS") {
		t.Fatalf("ws destroy arity: %v", err)
	}
	err = cmdFS(ctx, []string{"mv", "ws_1", "a"})
	if err == nil || !strings.Contains(err.Error(), "fs mv WS FROM TO") {
		t.Fatalf("fs mv arity: %v", err)
	}
	err = cmdFS(ctx, []string{"ls", "ws_1", "/", "extra"})
	if err == nil || !strings.Contains(err.Error(), `unexpected argument "extra"`) {
		t.Fatalf("fs ls arity: %v", err)
	}
	err = cmdAttach(ctx, []string{"ws_1"})
	if err == nil || !strings.Contains(err.Error(), "attach WS SESSION") {
		t.Fatalf("attach arity: %v", err)
	}
	err = cmdPort(ctx, []string{"ws_1", "70000"})
	if err == nil || !strings.Contains(err.Error(), "1-65535") {
		t.Fatalf("port range: %v", err)
	}
	err = cmdFleet(ctx, []string{"get"})
	if err == nil || !strings.Contains(err.Error(), "fleet get OPERATION") {
		t.Fatalf("fleet get arity: %v", err)
	}
	if !looksLikeImage("ghcr.io/org/img") || !looksLikeImage("node:22") || looksLikeImage("ws_abc") || looksLikeImage("plain") {
		t.Fatal("looksLikeImage heuristics")
	}
}

func TestGlobalFlagsAcceptedBeforeCommand(t *testing.T) {
	globals, rest := splitGlobalFlags([]string{"--json", "--server", "http://x:1", "--token=t", "ws", "get", "ws_1"})
	if !reflect.DeepEqual(globals, []string{"--json", "--server", "http://x:1", "--token=t"}) ||
		!reflect.DeepEqual(rest, []string{"ws", "get", "ws_1"}) {
		t.Fatalf("globals=%v rest=%v", globals, rest)
	}
	globals, rest = splitGlobalFlags([]string{"--follow", "events"})
	if len(globals) != 0 || len(rest) != 2 {
		t.Fatalf("non-global flag consumed: %v %v", globals, rest)
	}
	// Globals ahead of a nested subcommand still reach its FlagSet: arity
	// runs after parse, so the positional count proves --json was not
	// mistaken for a positional.
	err := run(context.Background(), []string{"--json", "ws", "get", "ws_1", "ws_2"})
	if err == nil || !strings.Contains(err.Error(), `unexpected argument "ws_2"`) {
		t.Fatalf("run with leading global: %v", err)
	}
	err = run(context.Background(), []string{"--json", "server"})
	if err == nil || !strings.Contains(err.Error(), "does not accept --json") {
		t.Fatalf("server should refuse client globals: %v", err)
	}
}

func TestWorkspaceCreateRejectsClientSelectedPrincipalBeforeDial(t *testing.T) {
	err := cmdWS(context.Background(), []string{"create", "--principal", "a_attacker", "--wait=false"})
	if err == nil || !strings.Contains(err.Error(), "caller identity is authoritative") {
		t.Fatalf("error=%v", err)
	}
}
