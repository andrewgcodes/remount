package policy

import (
	"regexp"
	"testing"
)

// credentialShaped matches values that are recognisably reusable credentials.
// The list is short on purpose: every entry is a format that is worthless to
// guess at and unambiguous when seen, so a match is a finding rather than a
// prompt for judgement.
var credentialShaped = regexp.MustCompile(
	`sk-[A-Za-z0-9_-]{16,}` +
		`|AKIA[0-9A-Z]{16}` +
		`|xox[baprs]-[A-Za-z0-9-]{10,}` +
		`|ghp_[A-Za-z0-9]{20,}` +
		`|-----BEGIN [A-Z ]*PRIVATE KEY-----`,
)

// credentialName matches a field whose name says it holds a credential. The
// separator before a bare "key" keeps Kubernetes' own `key:` selector field
// out of the rule, which names a map entry rather than a secret.
var credentialName = regexp.MustCompile(`(?i)(secrets?|passwords?|passwd|credentials?|tokens?|api[_.\-]?keys?|access[_.\-]?keys?|private[_.\-]?keys?|[_.\-]keys?)$`)

// assignment splits `name: value` and `name = value`, including the flag form
// `--token=value` a container command line uses.
var assignment = regexp.MustCompile(`^\s*-?\s*-{0,2}([A-Za-z0-9_.\-]+)\s*[:=]\s*(.*?)\s*$`)

// isReference reports whether a value points at a credential rather than being
// one: a shell or Terraform interpolation, a Helm action, a variable reference,
// or nothing at all.
func isReference(value string) bool {
	value = trimQuotes(value)
	switch value {
	case "", "[]", "{}", "null", "~":
		return true
	}
	switch value[0] {
	case '$', '&', '*', '|', '>':
		return true
	}
	for _, prefix := range []string{"{{", "var.", "local.", "module.", "each.", "data.", "!"} {
		if len(value) >= len(prefix) && value[:len(prefix)] == prefix {
			return true
		}
	}
	return false
}

// inertLiteral matches values that are not capable of being a credential: a
// boolean, a number, or a duration. `automountServiceAccountToken: false` is a
// field named for a token whose value is a policy decision.
var inertLiteral = regexp.MustCompile(`^(true|false|yes|no|on|off|[0-9]+([.smh][0-9a-z]*)?)$`)

func trimQuotes(value string) string {
	for len(value) >= 2 && (value[0] == '"' || value[0] == '\'') && value[len(value)-1] == value[0] {
		value = value[1 : len(value)-1]
	}
	return value
}

// credentialRules reject provider metadata that carries a reusable credential.
//
// This is the second two-owners case Plan B phase B5 §13.7 names, and it is a
// subtler one than placement. A secret in a tag, a label, or a tfvars file has
// two owners: the broker, which is supposed to be the only thing that ever sees
// the real value and substitutes it at the network edge, and the provider,
// which now also holds it, in state, in plan output, and in every console and
// billing export that reads metadata. The second owner never expires it.
var credentialRules = []rule{
	matchRule(
		"credential-literal",
		"a value shaped like a reusable credential appears in configuration",
		credentialShaped,
	),
	{
		name: "credential-field-holds-a-literal",
		why:  "a field named for a credential is set to a literal instead of a reference",
		violates: func(_, line string) bool {
			match := assignment.FindStringSubmatch(line)
			if match == nil {
				return false
			}
			if !credentialName.MatchString(match[1]) {
				return false
			}
			value := trimQuotes(match[2])
			return !isReference(value) && !inertLiteral.MatchString(value)
		},
	},
}

// TestProviderMetadataCarriesNoReusableCredential is the rule.
func TestProviderMetadataCarriesNoReusableCredential(t *testing.T) {
	found := scan(collect(t, infrastructureScopes...), credentialRules)
	for _, f := range found {
		t.Errorf("a credential has two owners: %s", f)
	}
	if len(found) > 0 {
		t.Log("the workspace is trusted with nothing; secrets reach the network edge through the broker, and configuration carries names and references only")
	}
}

// TestTheCredentialScanCatchesItsCanary is the control.
func TestTheCredentialScanCatchesItsCanary(t *testing.T) {
	found := scan(collect(t, "integration/policy/testdata/canary"), credentialRules)
	hit := map[string]bool{}
	for _, f := range found {
		hit[f.rule] = true
	}
	for _, r := range credentialRules {
		if !hit[r.name] {
			t.Errorf("rule %q found nothing in the canary; it cannot be trusted to have found nothing in deploy/ (%s)", r.name, r.why)
		}
	}
}

// TestAReferenceIsNotACredential pins the distinction the field rule turns on,
// because getting it wrong in either direction is expensive: too strict and the
// reference form is unusable, too loose and a literal passes as a reference.
func TestAReferenceIsNotACredential(t *testing.T) {
	references := []string{
		"", "\"\"", "[]", "{}",
		"${REMOUNT_TOKEN:-reference-development-token}",
		"{{ .Values.control.tokenSecret.key }}",
		"var.admin_token_env",
	}
	for _, value := range references {
		if !isReference(value) {
			t.Errorf("%q should read as a reference", value)
		}
	}
	for _, value := range []string{"false", "true", "300", "30s"} {
		if !inertLiteral.MatchString(value) {
			t.Errorf("%q should read as an inert literal, not a credential", value)
		}
	}
	literals := []string{
		"sk-live-0123456789abcdef",
		"hunter2",
		"\"an-actual-value\"",
	}
	for _, value := range literals {
		if isReference(value) {
			t.Errorf("%q should read as a literal", value)
		}
	}
}
