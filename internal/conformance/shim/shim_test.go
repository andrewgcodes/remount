package shim

import (
	"encoding/json"
	"io"
	"net/http"
	"testing"
)

func TestShimServesHealth(t *testing.T) {
	s := Start(DefectNone)
	defer s.Close()
	res, err := http.Get(s.URL() + "/healthz")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = res.Body.Close() }()
	body, err := io.ReadAll(res.Body)
	if err != nil {
		t.Fatal(err)
	}
	var health map[string]any
	if err := json.Unmarshal(body, &health); err != nil {
		t.Fatalf("/healthz is not JSON: %v", err)
	}
	if health["ok"] != true || health["serving"] != true {
		t.Fatalf("/healthz reported %v", health)
	}
}

func TestEveryDefectHasACategory(t *testing.T) {
	for _, d := range Defects {
		if CategoryOf(d) == "" {
			t.Errorf("defect %q names no category", d)
		}
	}
	if CategoryOf(DefectVersionMutates) != "negotiation" {
		t.Errorf("the version defects belong to negotiation, got %q", CategoryOf(DefectVersionMutates))
	}
}

func TestMountPathValidationMatchesSectionFiveTwo(t *testing.T) {
	for path, want := range map[string]bool{
		"/work":              true,
		"/home/agent/repo":   true,
		"/":                  false,
		"/etc":               false,
		"/etc/conformance":   false,
		"/proc/conformance":  false,
		"relative/path":      false,
		"/work/../../escape": false,
	} {
		if got := validMountPath(path); got != want {
			t.Errorf("validMountPath(%q) = %v, want %v", path, got, want)
		}
	}
}

func TestInterpretCoversTheProgramsTheSuiteRuns(t *testing.T) {
	if out, code, interactive := interpret([]string{"/bin/echo", "hello"}); string(out) != "hello\n" || code != 0 || interactive {
		t.Errorf("echo interpreted as %q %d %v", out, code, interactive)
	}
	if _, code, _ := interpret([]string{"/bin/sh", "-c", "exit 7"}); code != 7 {
		t.Errorf("`exit 7` interpreted as code %d", code)
	}
	if _, _, interactive := interpret([]string{"/bin/cat"}); !interactive {
		t.Error("cat must stay open for input")
	}
}

// The shim's canary is synthetic and must never look like a provider key,
// because it is checked into the repository and searched for in event logs.
func TestSecretCanaryIsSynthetic(t *testing.T) {
	for _, prefix := range []string{"sk-", "ghp_", "AKIA", "xoxb-"} {
		if len(Secret) >= len(prefix) && Secret[:len(prefix)] == prefix {
			t.Fatalf("the canary %q is shaped like a real credential", Secret)
		}
	}
	if Placeholder == Secret {
		t.Fatal("the placeholder and the secret are the same value")
	}
}
