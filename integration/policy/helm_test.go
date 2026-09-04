package policy

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

const (
	chartPath   = "deploy/helm/remount-node"
	goldenPath  = "deploy/helm/remount-node/golden/default.yaml"
	valuesPath  = "deploy/helm/remount-node/golden/values.yaml"
	releaseName = "remount-nodes"
	namespace   = "remount"
)

func TestHelmLint(t *testing.T) {
	hasTool(t, "helm")
	root := repoRoot(t)
	output, err := runTool(t, root, "helm", "lint", chartPath, "-f", valuesPath)
	if err != nil {
		t.Fatalf("helm lint: %v\n%s", err, output)
	}
	if !strings.Contains(output, "0 chart(s) failed") {
		t.Fatalf("helm lint did not report a clean chart:\n%s", output)
	}
}

// TestHelmGoldenTemplateIsCurrent makes chart drift visible in review.
//
// A chart is a program whose output nobody normally reads: an operator runs
// `helm upgrade` and sees a diff of values, not a diff of manifests. Committing
// the render means a change to a template shows up as a change to the thing
// that will actually be applied to a cluster.
func TestHelmGoldenTemplateIsCurrent(t *testing.T) {
	hasTool(t, "helm")
	root := repoRoot(t)
	rendered, err := runTool(t, root, "helm", "template", releaseName, chartPath, "--namespace", namespace, "-f", valuesPath)
	if err != nil {
		t.Fatalf("helm template: %v\n%s", err, rendered)
	}
	golden, err := os.ReadFile(filepath.Join(root, goldenPath))
	if err != nil {
		t.Fatal(err)
	}
	goldenText := strings.ReplaceAll(string(golden), "\r\n", "\n")
	if rendered == goldenText {
		return
	}
	t.Errorf("the chart no longer renders %s. Regenerate it and review the diff:\n"+
		"  helm template %s %s --namespace %s -f %s > %s",
		goldenPath, releaseName, chartPath, namespace, valuesPath, goldenPath)
	for _, line := range firstDifference(goldenText, rendered) {
		t.Log(line)
	}
}

func TestHelmGoldenComparisonNormalizesWindowsCheckouts(t *testing.T) {
	golden := strings.ReplaceAll("line one\r\nline two\r\n", "\r\n", "\n")
	if diff := firstDifference(golden, "line one\nline two\n"); diff != nil {
		t.Fatalf("line-ending-only difference = %v", diff)
	}
}

// firstDifference reports the first differing line with a little context, which
// is what a reviewer needs; the full diff is one command away and in the error.
func firstDifference(want, got string) []string {
	wantLines := strings.Split(want, "\n")
	gotLines := strings.Split(got, "\n")
	for index := 0; index < len(wantLines) || index < len(gotLines); index++ {
		w, g := "<end of file>", "<end of file>"
		if index < len(wantLines) {
			w = wantLines[index]
		}
		if index < len(gotLines) {
			g = gotLines[index]
		}
		if w != g {
			return []string{
				"first difference at line " + itoa(index+1),
				"golden:   " + w,
				"rendered: " + g,
			}
		}
	}
	return nil
}

// TestHelmRefusesATwoOwnerConfiguration is Plan B §13.7 for the chart.
//
// Every case is a value an operator could plausibly set. `helm template` must
// fail on each, because a chart that renders a bad manifest has already lost:
// the rejection has to happen before anything reaches a cluster.
func TestHelmRefusesATwoOwnerConfiguration(t *testing.T) {
	hasTool(t, "helm")
	root := repoRoot(t)
	cases := []struct {
		name    string
		set     []string
		expect  string
		because string
	}{
		{
			name:    "workspace-in-capability-labels",
			set:     []string{"capabilityLabels.workspace=ws_01HZQ"},
			expect:  "not what is running on it",
			because: "a label naming a workspace makes the cluster an owner of a fact the control plane changes without it",
		},
		{
			name:    "credential-in-capability-labels",
			set:     []string{"capabilityLabels.vendor=sk-live-abcdefghijklmnopqrstuvwxyz012345"},
			expect:  "reusable credential",
			because: "pod and node metadata is readable by anyone with get pods",
		},
		{
			name:    "credential-named-label",
			set:     []string{"capabilityLabels.openai_api_key=placeholder-value"},
			expect:  "must not name a credential",
			because: "a label whose name is a credential invites the value to follow",
		},
		{
			name:    "workspace-operation-in-args",
			set:     []string{"node.extraArgs[0]=ws move ws_01HZQ"},
			expect:  "runtime workspace operation",
			because: "the chart schedules nodes; workspace lifecycle is the control plane's",
		},
		{
			name:    "unpinned-image",
			set:     []string{"image.digest="},
			expect:  "must be pinned by digest",
			because: "a tag lets the reviewed image and the running image diverge",
		},
		{
			name:    "tag-shaped-digest",
			set:     []string{"image.digest=latest"},
			expect:  "must be sha256:",
			because: "a digest-shaped field holding a tag is worse than a tag, because it reads as pinned",
		},
		{
			name:    "control-plane-image",
			set:     []string{"image.repository=ghcr.io/andrewgcodes/remount"},
			expect:  "FROM scratch",
			because: "the release image has no userland, so a process-backend session dies with sh not found",
		},
		{
			name:    "missing-control-endpoint",
			set:     []string{"control.endpoint="},
			expect:  "control.endpoint is required",
			because: "a node with no control endpoint joins nothing and reports no error worth reading",
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			args := []string{"template", releaseName, chartPath, "--namespace", namespace, "-f", valuesPath}
			for _, set := range c.set {
				args = append(args, "--set", set)
			}
			output, err := runTool(t, root, "helm", args...)
			if err == nil {
				t.Fatalf("helm template accepted %v; it must be refused, because %s\n%s", c.set, c.because, output)
			}
			if !strings.Contains(flatten(output), c.expect) {
				t.Fatalf("%s was refused, but not for the stated reason %q:\n%s", c.name, c.expect, output)
			}
		})
	}
}

// TestTheChartOwnsNoWorkspaceLifecycle checks the shape of the rendered output
// rather than the templates: no custom resource, no controller workload, and no
// permission to create one.
func TestTheChartOwnsNoWorkspaceLifecycle(t *testing.T) {
	for _, file := range collect(t, goldenPath) {
		for _, forbidden := range []string{"CustomResourceDefinition", "kind: Role", "kind: ClusterRole", "kind: RoleBinding", "kind: ClusterRoleBinding"} {
			if strings.Contains(file.body, forbidden) {
				t.Errorf("the rendered chart contains %s; a Remount node needs no Kubernetes API access, and granting it is the first step towards the cluster owning workspaces", forbidden)
			}
		}
		if !strings.Contains(file.body, "automountServiceAccountToken: false") {
			t.Error("the rendered service account still mounts its API token")
		}
	}
}
