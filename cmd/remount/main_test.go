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

func TestWorkspaceCreateRejectsClientSelectedPrincipalBeforeDial(t *testing.T) {
	err := cmdWS(context.Background(), []string{"create", "--principal", "a_attacker", "--wait=false"})
	if err == nil || !strings.Contains(err.Error(), "caller identity is authoritative") {
		t.Fatalf("error=%v", err)
	}
}
