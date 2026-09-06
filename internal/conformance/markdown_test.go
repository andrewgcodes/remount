package conformance

import (
	"strings"
	"testing"
	"time"
)

func markdownReport() *Report {
	started := time.Date(2026, 9, 5, 18, 30, 0, 0, time.UTC)
	row := func(id string, tier Tier, status Status, reason string) Result {
		return Result{
			Requirement: Requirement{
				ID: id, Version: ManifestVersion, Since: ManifestVersion,
				Category: CategoryWorkspace, Tier: tier,
				Title: "the obligation of " + id, Spec: "PROTOCOL.md §5",
			},
			Status: status, Reason: reason, StartedAt: started, DurationMS: 12,
		}
	}
	return &Report{
		ManifestVersion: ManifestVersion, ProtocolVersion: ProtocolVersion,
		Target: "remount standalone", TargetKind: KindBuilt,
		Endpoint: "http://127.0.0.1:7443", Negotiated: []string{"v1", "session-cap"},
		Environment: Environment{OS: "darwin", Arch: "arm64", Backend: "process"},
		Profile:     ProfileMultiTenantIsolated, Candidate: "8fd7bdf",
		StartedAt: started, DurationMS: 1500,
		Cleanup: CleanupVerified,
		Results: []Result{
			row("CONF-PROF-BACKEND-ISOLATED", TierRequired, StatusFailed, "n1: process | isolation=none"),
			row("CONF-WS-001", TierRequired, StatusPassed, ""),
			row("CONF-WS-003", TierRequired, StatusUnavailable, "the target parked the workspace"),
			row("CONF-APP-003", TierCapabilityGated, StatusUnavailable, "no approve-mode rule"),
			row("CONF-EVT-009", TierExtension, StatusUnavailable, "no destination"),
		},
	}
}

func TestWriteMarkdownCarriesTheHeaderFieldsAReviewNeeds(t *testing.T) {
	var b strings.Builder
	if err := markdownReport().WriteMarkdown(&b); err != nil {
		t.Fatal(err)
	}
	out := b.String()
	for _, want := range []string{
		"# Remount conformance — NOT CONFORMANT",
		"| Candidate | `8fd7bdf` |",
		"| Endpoint | `http://127.0.0.1:7443` |",
		"| Runtime profile | `multi-tenant-isolated` |",
		"| Backend | `process` |",
		"| Host | `darwin/arm64` |",
		"| Date (UTC) | 2026-09-05T18:30:00Z |",
		"## Required",
		"## Capability-gated (optional)",
		"## Extension",
		"| Requirement | Status | Reason |",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("the markdown does not carry %q", want)
		}
	}
}

// The footer is the line a reviewer skims. It must say how many checks were
// unavailable in words, because "53 checks, no failures" read off a table
// whose rows were never observed is the exact misreading §6.2 exists to
// prevent.
func TestWriteMarkdownFooterStatesTheUnavailableCount(t *testing.T) {
	var b strings.Builder
	if err := markdownReport().WriteMarkdown(&b); err != nil {
		t.Fatal(err)
	}
	out := b.String()
	if !strings.Contains(out, "**3 of 5 checks were unavailable") {
		t.Errorf("the footer does not state the unavailable count:\n%s", lastLines(out, 3))
	}
	if !strings.Contains(out, "1 of them are required tier") {
		t.Errorf("the footer does not separate the required-tier unavailable rows:\n%s", lastLines(out, 3))
	}
}

// A reason carrying a pipe would otherwise split the row it is reported in,
// and a truncated table is a report a reader stops trusting.
func TestWriteMarkdownEscapesTableSeparators(t *testing.T) {
	var b strings.Builder
	if err := markdownReport().WriteMarkdown(&b); err != nil {
		t.Fatal(err)
	}
	for _, line := range strings.Split(b.String(), "\n") {
		if !strings.HasPrefix(line, "| `CONF-PROF-BACKEND-ISOLATED`") {
			continue
		}
		if strings.Count(strings.ReplaceAll(line, "\\|", ""), "|") != 4 {
			t.Fatalf("the row has the wrong number of unescaped separators: %s", line)
		}
		return
	}
	t.Fatal("the failing row is missing from the required table")
}

func TestWriteMarkdownSaysEveryCheckRanWhenNoneWereUnavailable(t *testing.T) {
	r := markdownReport()
	r.Results = []Result{r.Results[1]}
	var b strings.Builder
	if err := r.WriteMarkdown(&b); err != nil {
		t.Fatal(err)
	}
	out := b.String()
	if !strings.Contains(out, "**0 of 1 checks were unavailable") || !strings.Contains(out, "Every check ran.") {
		t.Errorf("a fully observed run does not say so:\n%s", lastLines(out, 3))
	}
	if !strings.Contains(out, "# Remount conformance — CONFORMANT") {
		t.Error("a fully observed passing run is not reported conformant")
	}
}

func lastLines(s string, n int) string {
	lines := strings.Split(strings.TrimRight(s, "\n"), "\n")
	if len(lines) > n {
		lines = lines[len(lines)-n:]
	}
	return strings.Join(lines, "\n")
}
