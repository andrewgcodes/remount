package policy

import (
	"regexp"
	"strings"
	"testing"
)

// infrastructureScopes are the configuration surfaces that declare Remount
// infrastructure. Everything here is executed by a machine: OpenTofu, Helm,
// Docker Compose, or a container build.
var infrastructureScopes = []string{
	"deploy/tofu",
	"deploy/helm",
	"deploy/compose",
	"packaging/container/Dockerfile",
	"images/workspace/Dockerfile",
}

// ownershipRules reject a configuration in which infrastructure-as-code asserts
// a fact the Remount control plane owns.
//
// The control plane grants a placement as a lease bound to a workspace
// generation, revokes it on node loss, and re-claims from the last checkpoint.
// Terraform and Helm reconcile against a desired state they computed earlier.
// If both held the fact, the reconcile would eventually move a live workspace
// to satisfy a plan that was already stale, and the operator would see a
// workspace relocate for no reason either system could explain.
var ownershipRules = []rule{
	matchRule(
		"runtime-cli-invocation",
		"infrastructure code invoking a workspace, session or checkpoint operation",
		regexp.MustCompile(`remount\s+(ws|session|sessions|exec|snapshot|snapshots|checkpoint|attach|move|sleep|wake)\b`),
	),
	matchRule(
		"runtime-api-call",
		"infrastructure code calling the workspace or session HTTP surface",
		regexp.MustCompile(`/v1/(ws|workspaces|sessions|claims|grants)\b`),
	),
	matchRule(
		"terraform-provisioner",
		"a Terraform provisioner, which runs imperative work inside a plan",
		regexp.MustCompile(`provisioner\s+"(local|remote)-exec"|null_resource`),
	),
	matchRule(
		"workspace-assignment",
		"a non-empty placement or workspace identifier assigned in configuration",
		regexp.MustCompile(`(node_assignments|workspace_ids?|session_ids?|claim_ids?|generation)\s*[:=]\s*[^\s{}\[\]"']|`+
			`(node_assignments|workspace_ids?|session_ids?)\s*[:=]\s*[\[{]\s*[^\s\]}]`),
	),
	matchRule(
		"kubernetes-workspace-object",
		"a Kubernetes object that would make the cluster an owner of Remount workspaces",
		regexp.MustCompile(`kind:\s*(CustomResourceDefinition|Workspace|RemountWorkspace)\b|apiVersion:\s*remount\.dev/`),
	),
}

// TestInfrastructureDoesNotOwnRuntimeFacts is the rule.
func TestInfrastructureDoesNotOwnRuntimeFacts(t *testing.T) {
	found := scan(collect(t, infrastructureScopes...), ownershipRules)
	for _, f := range found {
		t.Errorf("two owners for one fact: %s", f)
	}
	if len(found) > 0 {
		t.Log("workspace placement, claims, moves, sessions and checkpoints belong to the Remount control plane; infrastructure code declares capacity and capability only")
	}
}

// TestTheOwnershipScanCatchesItsCanary is the control.
//
// Every rule above is planted in testdata/canary, and the scan must find every
// one of them. Without this, a rule whose regexp stopped matching would report
// the tree clean and look like a pass forever.
func TestTheOwnershipScanCatchesItsCanary(t *testing.T) {
	found := scan(collect(t, "integration/policy/testdata/canary"), ownershipRules)
	hit := map[string]bool{}
	for _, f := range found {
		hit[f.rule] = true
	}
	for _, r := range ownershipRules {
		if !hit[r.name] {
			t.Errorf("rule %q found nothing in the canary; it cannot be trusted to have found nothing in deploy/ (%s)", r.name, r.why)
		}
	}
}

// TestCommentsAreStrippedBeforeScanning pins the reason the scan is not a plain
// grep: the modules and templates explain the boundary using the same words the
// rules match, and a scanner that judged prose would force the documentation to
// stop naming the hazard it prevents.
func TestCommentsAreStrippedBeforeScanning(t *testing.T) {
	cases := []struct {
		path string
		body string
		want string
	}{
		{"a.tf", "# use remount ws move instead\nimage = \"x\"\n", "image = \"x\""},
		{"a.tf", "/* remount ws move */\nx = 1\n", "x = 1"},
		{"a.yaml", "  # remount ws move\n  image: x\n", "image: x"},
		{"a.tpl", "{{/* remount ws move */}}\nkind: DaemonSet\n", "kind: DaemonSet"},
	}
	for _, c := range cases {
		stripped := stripComments(c.path, c.body)
		if len(scan([]configFile{{path: c.path, body: stripped}}, ownershipRules)) != 0 {
			t.Errorf("%s: a comment was scanned as executed configuration: %q", c.path, stripped)
		}
		if !strings.Contains(stripped, c.want) {
			t.Errorf("%s: stripping removed live configuration: %q", c.path, stripped)
		}
		if strings.Count(stripped, "\n") != strings.Count(c.body, "\n") {
			t.Errorf("%s: stripping changed the line count, so reported line numbers would be wrong", c.path)
		}
	}
}
