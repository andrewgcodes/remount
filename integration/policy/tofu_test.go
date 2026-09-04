package policy

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

// cleanEnv builds the environment the validators run in from an allow list
// rather than a scrub list.
//
// "validate without cloud credentials" is a claim about what was absent, and a
// scrub list only removes the names somebody remembered. An allow list makes a
// forgotten AWS_PROFILE or GOOGLE_APPLICATION_CREDENTIALS impossible rather
// than unlikely.
func cleanEnv(t *testing.T) []string {
	t.Helper()
	environment := []string{
		"PATH=" + os.Getenv("PATH"),
		"HOME=" + t.TempDir(),
		"TMPDIR=" + os.Getenv("TMPDIR"),
		// OpenTofu writes a checkpoint request home unless told not to. A
		// validation gate must not need the network at all.
		"CHECKPOINT_DISABLE=1",
		"TF_IN_AUTOMATION=1",
		"TF_INPUT=0",
	}
	for _, name := range []string{"AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY", "AWS_PROFILE", "AWS_REGION", "GOOGLE_APPLICATION_CREDENTIALS", "ARM_CLIENT_ID"} {
		if _, ok := os.LookupEnv(name); ok {
			t.Logf("%s is set in this shell and is deliberately not passed to the validator", name)
		}
	}
	return environment
}

// flatten makes a validator's message matchable.
//
// OpenTofu draws its errors in a box and hard-wraps them at the terminal width,
// so a sentence in the module is several lines with gutter characters in the
// output. Matching against the raw text would tie the assertion to a wrap
// column, and the test would start failing for a reason that has nothing to do
// with the rule it checks.
func flatten(output string) string {
	replacer := strings.NewReplacer("\u2502", " ", "\u2575", " ", "\u2577", " ", "\n", " ")
	return strings.Join(strings.Fields(replacer.Replace(output)), " ")
}

func runTool(t *testing.T, directory, name string, args ...string) (string, error) {
	t.Helper()
	command := exec.Command(name, args...)
	command.Dir = directory
	command.Env = cleanEnv(t)
	output, err := command.CombinedOutput()
	return string(output), err
}

// copyTree copies a directory, skipping provider working directories.
func copyTree(t *testing.T, from, to string) {
	t.Helper()
	err := filepath.WalkDir(from, func(path string, entry os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		relative, err := filepath.Rel(from, path)
		if err != nil {
			return err
		}
		target := filepath.Join(to, relative)
		if entry.IsDir() {
			if entry.Name() == ".terraform" {
				return filepath.SkipDir
			}
			return os.MkdirAll(target, 0o755)
		}
		body, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		return os.WriteFile(target, body, 0o644)
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestTofuFormatIsClean(t *testing.T) {
	hasTool(t, "tofu")
	root := repoRoot(t)
	output, err := runTool(t, root, "tofu", "fmt", "-check", "-recursive", "-no-color", "deploy/tofu")
	if err != nil {
		t.Fatalf("tofu fmt -check -recursive deploy/tofu: %v\n%s", err, output)
	}
}

// TestTheModulesDeclareNoProvider is why validation needs no account.
//
// A module with a required_providers block needs a plugin from a registry, and
// a plugin generally needs credentials to do anything. Keeping the seam at
// terraform_data, a builtin, is what lets this gate run from a clean checkout
// on a machine with no cloud identity at all.
func TestTheModulesDeclareNoProvider(t *testing.T) {
	for _, file := range collect(t, "deploy/tofu") {
		if strings.Contains(file.body, "required_providers") {
			t.Errorf("%s declares required_providers; validation would then need a registry and, in practice, an account", file.path)
		}
	}
}

func TestTofuValidatesWithoutCloudCredentials(t *testing.T) {
	hasTool(t, "tofu")
	work := t.TempDir()
	copyTree(t, filepath.Join(repoRoot(t), "deploy", "tofu"), work)
	example := filepath.Join(work, "examples", "reference")

	if output, err := runTool(t, example, "tofu", "init", "-backend=false", "-input=false", "-no-color"); err != nil {
		t.Fatalf("tofu init -backend=false: %v\n%s", err, output)
	}
	output, err := runTool(t, example, "tofu", "validate", "-no-color")
	if err != nil {
		t.Fatalf("tofu validate: %v\n%s", err, output)
	}
	if !strings.Contains(output, "The configuration is valid") {
		t.Fatalf("tofu validate did not report success: %s", output)
	}
}

// rejection is one configuration the modules must refuse, and the reason the
// operator should see when they are refused.
type rejection struct {
	fixture string
	expect  string
	because string
}

// TestTofuRejectsATwoOwnerConfiguration is Plan B §13.7 as executable tests.
//
// Each fixture is a root module that a reasonable operator might write, wired
// to the real modules in deploy/tofu. `tofu validate` must fail on every one.
// A validation rule nobody has watched reject something is not yet a rule, so
// these fixtures are permanent controls rather than one-off experiments.
func TestTofuRejectsATwoOwnerConfiguration(t *testing.T) {
	hasTool(t, "tofu")
	cases := []rejection{
		{
			fixture: "moves-a-live-workspace",
			expect:  "Terraform does not place, move, or pin live workspaces.",
			because: "placement is a lease the control plane grants and revokes; a plan reconciling against it would relocate a running workspace",
		},
		{
			fixture: "credential-in-node-labels",
			expect:  "looks like a reusable credential",
			because: "node labels are provider metadata, readable by anyone with describe rights and exported to billing",
		},
		{
			fixture: "credential-as-token-value",
			expect:  "admin_token_env is the NAME of an environment variable",
			because: "the module accepts the name of a variable, so a literal token cannot reach state or a plan file",
		},
		{
			fixture: "floating-node-image",
			expect:  "image must be pinned by digest",
			because: "a tag lets the reviewed image and the running image diverge silently",
		},
		{
			fixture: "two-control-replicas",
			expect:  "The control plane is a single writer; replicas must be 1.",
			because: "two control planes are two SQLite databases, each answering coherently about a different fleet",
		},
		{
			fixture: "workspace-in-node-labels",
			expect:  "Labels describe what a node can do, not what is running on it.",
			because: "a label naming a workspace is Terraform asserting a fact that changes without it",
		},
	}
	for _, c := range cases {
		t.Run(c.fixture, func(t *testing.T) {
			work := t.TempDir()
			copyTree(t, filepath.Join(repoRoot(t), "deploy", "tofu", "modules"), filepath.Join(work, "modules"))
			copyTree(t, filepath.Join(repoRoot(t), "integration", "policy", "testdata", "tofu", c.fixture), work)

			if output, err := runTool(t, work, "tofu", "init", "-backend=false", "-input=false", "-no-color"); err != nil {
				t.Fatalf("tofu init: %v\n%s", err, output)
			}
			output, err := runTool(t, work, "tofu", "validate", "-no-color")
			if err == nil {
				t.Fatalf("tofu validate accepted %s; it must be refused, because %s\n%s", c.fixture, c.because, output)
			}
			if !strings.Contains(flatten(output), c.expect) {
				t.Fatalf("%s was refused, but not for the stated reason %q:\n%s", c.fixture, c.expect, output)
			}
		})
	}
}
