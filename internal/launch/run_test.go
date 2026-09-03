package launch

import (
	"strings"
	"testing"

	"remount.dev/remount/internal/proto"
)

func TestEgressPolicyBySandboxAndProfile(t *testing.T) {
	r := &Recipe{Name: "x", Hosts: []string{"registry.npmjs.org"}}
	b, err := ParseBinding("b_openai")
	if err != nil {
		t.Fatal(err)
	}
	ruleIDs := func(p proto.NetworkPolicy) string {
		var ids []string
		for _, r := range p.Rules {
			ids = append(ids, r.ID+"/"+r.Protocol+"/"+strings.Join(r.Methods, ","))
		}
		return strings.Join(ids, " ")
	}
	cases := []struct {
		security, sandbox string
		def               string
		rules             string
	}{
		// --sandbox is about writes; read-only must still install and reach its provider.
		{proto.SecurityLocal, SandboxReadOnly, "", ""},
		{proto.SecurityLocal, SandboxWorkspaceWrite, "", ""},
		{proto.SecurityLocal, SandboxFull, proto.NetworkDefaultAllow, ""},
		{proto.SecurityIsolated, SandboxReadOnly, proto.NetworkDefaultDeny, "run-provider-b_openai/https/ run-recipe-fetch/https/GET,HEAD"},
		{proto.SecurityIsolated, SandboxWorkspaceWrite, proto.NetworkDefaultDeny, "run-provider-b_openai/https/ run-recipe-fetch/https/GET,HEAD"},
		{proto.SecurityMultiTenant, SandboxFull, proto.NetworkDefaultDeny, "run-provider-b_openai/https/ run-recipe-https/https/ run-recipe-connect/connect/ run-provider-connect-b_openai/connect/"},
	}
	for _, tc := range cases {
		p := egressPolicy(r, []Binding{b}, tc.security, tc.sandbox)
		if p.Default != tc.def || ruleIDs(p) != tc.rules {
			t.Errorf("%s/%s: default=%q rules=%q; want %q %q", tc.security, tc.sandbox, p.Default, ruleIDs(p), tc.def, tc.rules)
		}
		if _, err := proto.NormalizeSecurity(proto.SecuritySpec{Profile: tc.security, Network: p}); err != nil {
			t.Errorf("%s/%s: policy does not normalize: %v", tc.security, tc.sandbox, err)
		}
	}
	// A recipe with no hosts under a strict profile admits only its providers.
	p := egressPolicy(&Recipe{Name: "custom"}, []Binding{b}, proto.SecurityIsolated, SandboxFull)
	if len(p.Rules) != 1 || p.Rules[0].ID != "run-provider-b_openai" {
		t.Fatalf("rules=%s", ruleIDs(p))
	}
}
