package evidence

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// fakeEnv keeps every test keyless: no real credential is ever needed to prove
// the scan works, which is the same rule the leak lanes follow.
func fakeEnv(pairs map[string]string) Lookup {
	return func(name string) (string, bool) {
		v, ok := pairs[name]
		return v, ok
	}
}

func TestARecordNeverCarriesTheValueOfAVariableItNames(t *testing.T) {
	const value = "sk-not-a-real-key-planted-by-this-test"
	lookup := fakeEnv(map[string]string{"OPENAI_API_KEY": value})

	rec := validRecord()
	rec.RequiredEnv = []string{"OPENAI_API_KEY"}
	if findings := rec.ScanSecrets(lookup); len(findings) != 0 {
		t.Fatalf("naming a variable is allowed, got %v", findings)
	}

	rec.Reason = "upstream refused " + value
	rec.Status = StatusFailed
	findings := rec.ScanSecrets(lookup)
	if len(findings) == 0 {
		t.Fatal("a record carrying the value of a variable it names was accepted")
	}
	var named bool
	for _, f := range findings {
		if f.Rule == "env-value:OPENAI_API_KEY" {
			named = true
		}
		if strings.Contains(f.String(), value) {
			t.Fatalf("the finding reproduced the secret: %s", f)
		}
	}
	if !named {
		t.Fatalf("no finding named the variable, got %v", findings)
	}
}

func TestAVariableNamedOnlyInProseIsStillScanned(t *testing.T) {
	lookup := fakeEnv(map[string]string{"REMOUNT_E13_MODEL_KEY": "planted-value-1234"})
	rec := validRecord()
	rec.Status = StatusUnavailable
	rec.Reason = "REMOUNT_E13_MODEL_KEY is absent, but here it is anyway: planted-value-1234"
	if findings := rec.ScanSecrets(lookup); len(findings) == 0 {
		t.Fatal("a variable named in free text must still be scanned for its value")
	}
}

func TestShortValuesDoNotDrownTheScanInFalsePositives(t *testing.T) {
	lookup := fakeEnv(map[string]string{"HOME": "/", "REMOUNT_LOG": "1"})
	rec := validRecord()
	rec.Reason = "HOME=/ and REMOUNT_LOG=1 were set"
	rec.Status = StatusFailed
	if findings := rec.ScanSecrets(lookup); len(findings) != 0 {
		t.Fatalf("a one-character value is not a credential, got %v", findings)
	}
}

func TestSecretShapedStringsAreFound(t *testing.T) {
	cases := map[string]string{
		"openai-key":        Canary,
		"e2b-key":           "e2b_0123456789abcdef0123",
		"aws-access-key-id": "AKIAQQQQQQQQQQQQQQQQ",
		"github-token":      "ghp_0123456789abcdefghijklmnopqrstuvwx",
		"slack-token":       "xoxb-0123456789-abcdefgh",
		"private-key-block": "-----BEGIN OPENSSH PRIVATE KEY-----",
		"bearer-header":     "Authorization: Bearer abcdefghijklmnop",
		"assigned-secret":   `"api_key": "abcdefghijklmnopqrstuvwxyz"`,
	}
	for rule, text := range cases {
		findings := ScanText("t", text)
		if len(findings) == 0 {
			t.Fatalf("%s: nothing found in %q", rule, text)
		}
		var matched bool
		for _, f := range findings {
			if f.Rule == rule {
				matched = true
			}
			if strings.Contains(f.Hint, text) && len(text) > 4 {
				t.Fatalf("%s: the hint reproduced the match", rule)
			}
		}
		if !matched {
			t.Fatalf("%s: no finding carried that rule, got %v", rule, findings)
		}
	}
}

func TestOrdinaryEvidenceProseIsNotACredential(t *testing.T) {
	clean := `{"scenario":"B11","required_env":["E2B_API_KEY","OPENAI_API_KEY"],` +
		`"reason":"missing prerequisite: E2B_API_KEY not set","owner":"internal/sim.TestRunOpenCodeDockerIntegration"}`
	if findings := ScanText("evidence.json", clean); len(findings) != 0 {
		t.Fatalf("a scan that fires on ordinary evidence gets suppressed and then proves nothing: %v", findings)
	}
}

func TestScanTreeFindsAPlantedCanaryAndReportsUnreadableFiles(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "clean.txt"), []byte("nothing here\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	findings, err := ScanTree(dir, fakeEnv(nil), nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(findings) != 0 {
		t.Fatalf("clean tree reported %v", findings)
	}
	if err := os.WriteFile(filepath.Join(dir, "planted.txt"), []byte(Canary), 0o600); err != nil {
		t.Fatal(err)
	}
	findings, err = ScanTree(dir, fakeEnv(nil), nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(findings) != 1 || findings[0].Where != "planted.txt" {
		t.Fatalf("the planted canary was not found: %v", findings)
	}
}

func TestScanTreeRefusesToCallAnUnscannedTreeClean(t *testing.T) {
	dir := t.TempDir()
	if _, err := ScanTree(filepath.Join(dir, "absent"), fakeEnv(nil), nil); err == nil {
		t.Fatal("a tree that could not be walked was reported clean")
	}
}
