package conformance

import (
	"bytes"
	"encoding/json"
	"encoding/xml"
	"strings"
	"testing"
	"time"
)

func sampleReport() *Report {
	m := Standard()
	pick := func(id string) Requirement {
		r, _ := m.Find(id)
		return r
	}
	return &Report{
		ManifestVersion: ManifestVersion,
		ProtocolVersion: ProtocolVersion,
		Target:          "sample",
		TargetKind:      KindExternal,
		Endpoint:        "http://127.0.0.1:1",
		Negotiated:      []string{CapabilityV1},
		Environment:     Environment{OS: "linux", Arch: "amd64", Backend: "process"},
		StartedAt:       time.Unix(1767225600, 0).UTC(),
		DurationMS:      1234,
		Cleanup:         CleanupVerified,
		Results: []Result{
			{Requirement: pick("CONF-NEG-001"), Status: StatusPassed, DurationMS: 5},
			{Requirement: pick("CONF-SESS-004"), Status: StatusFailed, Reason: "a retried keystroke was typed twice", DurationMS: 9},
			{Requirement: pick("CONF-BIND-002"), Status: StatusUnavailable, Reason: "no binding was configured", DurationMS: 1},
		},
	}
}

// The evidence record's field names are fixed by Plan B §6. This package
// duplicates internal/evidence.Record rather than importing it (see the note
// on EvidenceRecord), so the schema is asserted here instead of by the
// compiler.
func TestEvidenceRecordMatchesThePlanBSchema(t *testing.T) {
	body, err := json.Marshal(sampleReport().Evidence(EvidenceOptions{
		Scenario: "B20", Candidate: "efdfcea", Layer: "artifact",
		Command: "cmd/conformance --build .", Required: true, Owner: "cmd/conformance",
	}))
	if err != nil {
		t.Fatal(err)
	}
	var got map[string]any
	if err := json.Unmarshal(body, &got); err != nil {
		t.Fatal(err)
	}
	for _, key := range []string{
		"scenario", "candidate", "layer", "status", "command",
		"started_at", "duration_ms", "environment", "cleanup", "required",
	} {
		if _, ok := got[key]; !ok {
			t.Errorf("the evidence record omits the §6 field %q", key)
		}
	}
	env, ok := got["environment"].(map[string]any)
	if !ok {
		t.Fatalf("environment is %T, not an object", got["environment"])
	}
	for _, key := range []string{"os", "arch"} {
		if _, ok := env[key]; !ok {
			t.Errorf("the environment omits the §6 field %q", key)
		}
	}
	// §6.2 forbids an unexplained non-pass.
	if got["status"] != "failed" {
		t.Errorf("a report with a failed required requirement produced status %v", got["status"])
	}
	if reason, _ := got["reason"].(string); reason == "" {
		t.Error("a failed record carries no reason")
	}
}

func TestEvidenceRecordCarriesNoSecretValue(t *testing.T) {
	r := sampleReport()
	body, err := json.Marshal(r.Evidence(EvidenceOptions{Scenario: "B20", Candidate: "efdfcea", Layer: "artifact"}))
	if err != nil {
		t.Fatal(err)
	}
	// The conformance binding's canary must never reach a committed record.
	if bytes.Contains(body, []byte(conformanceBinding.Secret)) {
		t.Fatal("the evidence record carries the binding fixture's secret")
	}
}

func TestConformantRequiresEveryRequiredRowToBeObserved(t *testing.T) {
	r := sampleReport()
	r.Results[1].Status = StatusPassed
	r.Results[1].Reason = ""
	if ok, _ := r.Conformant(); !ok {
		t.Fatal("a report whose required rows all passed was judged non-conformant")
	}
	// An unobserved required row is not a pass: §6.2 has no skipped-success.
	r.Results[1].Status = StatusUnavailable
	r.Results[1].Reason = "the runner could not open a session"
	ok, why := r.Conformant()
	if ok {
		t.Fatal("a required requirement that could not be observed was counted as satisfied")
	}
	if !strings.Contains(why, "could not be observed") {
		t.Fatalf("the verdict did not distinguish an unobserved row from a failed one: %s", why)
	}
}

func TestConformantReportsFailedCleanup(t *testing.T) {
	r := sampleReport()
	r.Results[1].Status = StatusPassed
	r.Cleanup, r.CleanupReason = CleanupFailed, "a workspace could not be destroyed"
	ok, why := r.Conformant()
	if ok {
		t.Fatal("a run that left state behind was judged conformant")
	}
	if !strings.Contains(why, "cleanup failed") {
		t.Fatalf("the verdict did not mention cleanup: %s", why)
	}
}

func TestFailedCategoriesNamesOnlyFailedOnes(t *testing.T) {
	got := sampleReport().FailedCategories()
	if len(got) != 1 || got[0] != CategorySession {
		t.Fatalf("FailedCategories returned %v", got)
	}
}

func TestJUnitRendersEveryOutcome(t *testing.T) {
	var buf bytes.Buffer
	if err := sampleReport().WriteJUnit(&buf); err != nil {
		t.Fatal(err)
	}
	var suites struct {
		Tests    int `xml:"tests,attr"`
		Failures int `xml:"failures,attr"`
		Skipped  int `xml:"skipped,attr"`
		Suites   []struct {
			Name  string `xml:"name,attr"`
			Cases []struct {
				Name    string `xml:"name,attr"`
				Failure *struct {
					Message string `xml:"message,attr"`
				} `xml:"failure"`
				Skipped *struct {
					Message string `xml:"message,attr"`
				} `xml:"skipped"`
			} `xml:"testcase"`
		} `xml:"testsuite"`
	}
	if err := xml.Unmarshal(buf.Bytes(), &suites); err != nil {
		t.Fatalf("the JUnit output is not well-formed XML: %v", err)
	}
	if suites.Tests != 3 || suites.Failures != 1 || suites.Skipped != 1 {
		t.Fatalf("JUnit reported tests=%d failures=%d skipped=%d", suites.Tests, suites.Failures, suites.Skipped)
	}
	if len(suites.Suites) != 3 {
		t.Fatalf("JUnit produced %d suites for three categories", len(suites.Suites))
	}
	for _, s := range suites.Suites {
		for _, c := range s.Cases {
			if c.Failure != nil && c.Failure.Message == "" {
				t.Errorf("%s: a failure carries no message", c.Name)
			}
			if c.Skipped != nil && c.Skipped.Message == "" {
				t.Errorf("%s: an unavailable row carries no reason", c.Name)
			}
		}
	}
}

func TestSummaryLeadsWithTheVerdict(t *testing.T) {
	var buf bytes.Buffer
	if err := sampleReport().WriteSummary(&buf); err != nil {
		t.Fatal(err)
	}
	first, _, _ := strings.Cut(buf.String(), "\n")
	if !strings.HasPrefix(first, "NOT CONFORMANT") {
		t.Fatalf("the summary's first line is %q", first)
	}
	if !strings.Contains(buf.String(), "CONF-SESS-004") {
		t.Error("the summary does not name the failed requirement")
	}
	if !strings.Contains(buf.String(), "no binding was configured") {
		t.Error("the summary does not name why an unavailable row could not run")
	}
}

func TestWriteManifestListsEveryRequirement(t *testing.T) {
	var buf bytes.Buffer
	if err := WriteManifest(&buf, Standard()); err != nil {
		t.Fatal(err)
	}
	out := buf.String()
	for _, r := range Standard().Requirements {
		if !strings.Contains(out, r.ID) {
			t.Errorf("the rendered manifest omits %s", r.ID)
		}
	}
	for _, c := range Categories {
		if !strings.Contains(out, string(c)) {
			t.Errorf("the rendered manifest omits category %s", c)
		}
	}
}

func TestReportJSONRoundTrips(t *testing.T) {
	var buf bytes.Buffer
	if err := sampleReport().WriteJSON(&buf); err != nil {
		t.Fatal(err)
	}
	var back Report
	if err := json.Unmarshal(buf.Bytes(), &back); err != nil {
		t.Fatal(err)
	}
	if len(back.Results) != 3 || back.ManifestVersion != ManifestVersion {
		t.Fatalf("the report did not round-trip: %+v", back)
	}
}
