package redact

import (
	"regexp"
	"strings"
	"testing"
)

// Every value below is synthetic and shaped like a credential. None is real,
// and none may ever be replaced with a real one: the point of the fixture is
// that the scan has something to find.
const (
	canaryKey    = "sk-remount-canary-do-not-use-0000"
	canaryGitHub = "ghp_remountcanary000000000000000000000000"
	canaryLit    = "capability-canary-0123456789"
)

func TestStringScrubsCredentialShapes(t *testing.T) {
	cases := []struct {
		name  string
		in    string
		leaks string
	}{
		{"api key", "upstream said " + canaryKey, canaryKey},
		{"github token", "clone failed for " + canaryGitHub, canaryGitHub},
		{"bearer header", "Authorization: Bearer " + canaryKey, canaryKey},
		{"key equals value", "OPENAI_API_KEY=" + canaryKey, canaryKey},
		{"json field", `{"token": "` + canaryKey + `"}`, canaryKey},
		{"broker capability url", "http://127.0.0.1:9/c/" + canaryLit + "/d/x", canaryLit},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			// The instrument first: the untouched input must contain the
			// value, or a clean result below would prove nothing.
			if !strings.Contains(tc.in, tc.leaks) {
				t.Fatal("the fixture does not contain the value it claims to")
			}
			out := String(tc.in)
			if strings.Contains(out, tc.leaks) {
				t.Fatalf("String kept the credential: %q", out)
			}
			if !strings.Contains(out, Mark) {
				t.Fatalf("String returned %q with no redaction mark", out)
			}
		})
	}
}

func TestStringLeavesOrdinaryTextAlone(t *testing.T) {
	const message = "remount broker: egress to api.example:443 is not permitted for this workspace"
	if got := String(message); got != message {
		t.Fatalf("String rewrote ordinary text: %q", got)
	}
	if got := String(""); got != "" {
		t.Fatalf("String(%q) = %q", "", got)
	}
}

func TestRedactorReplacesLiteralsAndShapes(t *testing.T) {
	r := NewRedactor([]string{canaryLit, "shrt", `quo"ted-canary-value`})
	frame := `{"cap":"` + canaryLit + `","esc":"quo\"ted-canary-value","key":"` + canaryKey + `","keep":"shrt"}`
	out, changed := r.Apply([]byte(frame))
	if !changed {
		t.Fatal("Apply reported no change for a frame carrying three canaries")
	}
	for _, leaked := range []string{canaryLit, `quo\"ted-canary-value`, canaryKey} {
		if strings.Contains(string(out), leaked) {
			t.Fatalf("Apply kept %q: %s", leaked, out)
		}
	}
	// A literal shorter than the minimum is not replaced: doing so would
	// destroy the text without protecting anything.
	if !strings.Contains(string(out), "shrt") {
		t.Fatalf("Apply replaced a three-character literal: %s", out)
	}
	// An untouched frame is returned as-is, and the input is never modified.
	clean := []byte(`{"a":1}`)
	got, changed := r.Apply(clean)
	if changed || string(got) != `{"a":1}` {
		t.Fatalf("Apply(%s) = %s, %v", clean, got, changed)
	}
}

func TestPatternsHandsOutACopy(t *testing.T) {
	first := Patterns()
	if len(first) == 0 {
		t.Fatal("Patterns returned nothing")
	}
	first[0] = regexp.MustCompile(`^never-matches-anything$`)
	if String("value="+canaryKey) == "value="+canaryKey {
		t.Fatal("mutating the returned slice changed the package's own expressions")
	}
}
