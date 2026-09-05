package launch

import (
	"strings"
	"testing"

	"remount.dev/remount/internal/proto"
)

func TestConversationOptionsValidate(t *testing.T) {
	r, err := Load("codex")
	if err != nil {
		t.Fatal(err)
	}
	const id = "11111111-1111-4111-8111-111111111111"
	transcript := ".codex/sessions/2026/09/04/rollout-2026-09-04T12-00-00-" + id + ".jsonl"
	for _, conversation := range []string{id, "../session", "latest", "--last", strings.Repeat("z", 36)} {
		o := Options{Recipe: r, Resume: true, Task: "continue", Conversation: conversation, ConversationPath: transcript}
		plan, err := o.Validate()
		if conversation == id {
			if err != nil {
				t.Fatal(err)
			}
			if plan.Spec.Labels["remount.conversation"] != id || plan.Spec.Labels["remount.conversation.path"] != transcript {
				t.Errorf("conversation labels not persisted: %v", plan.Spec.Labels)
			}
		} else if err == nil {
			t.Errorf("invalid conversation %q accepted", conversation)
		}
	}
	for _, badPath := range []string{"", "../" + transcript, ".codex/auth.json", strings.ReplaceAll(transcript, id, "22222222-2222-4222-8222-222222222222")} {
		o := Options{Recipe: r, Resume: true, Task: "continue", Conversation: id, ConversationPath: badPath}
		if _, err := o.Validate(); err == nil {
			t.Errorf("invalid conversation transcript %q accepted", badPath)
		}
	}
}

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
