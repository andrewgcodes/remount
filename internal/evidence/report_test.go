package evidence

import (
	"encoding/xml"
	"strings"
	"testing"
	"time"
)

func sampleResult() Result {
	pass := validRecord()
	pass.Scenario = "B0.lint"
	pass.Required = true

	unavailable := validRecord()
	unavailable.Scenario = "B11"
	unavailable.Layer = LayerLiveService
	unavailable.Status = StatusUnavailable
	unavailable.Required = false
	unavailable.RequiredEnv = []string{"E2B_API_KEY"}
	unavailable.Reason = "missing prerequisite: E2B_API_KEY not set"

	failed := validRecord()
	failed.Scenario = "B0.race"
	failed.Status = StatusFailed
	failed.Required = true
	failed.Reason = "make race: exit status 1"

	res := Result{
		Schema: Schema, Candidate: "bb0a6cfdeadbeef1234567",
		StartedAt:   time.Date(2026, 9, 3, 12, 0, 0, 0, time.UTC),
		Environment: Environment{OS: "darwin", Arch: "arm64"},
		Records:     []Record{pass, unavailable, failed},
		Credentials: []Credential{{Name: "E2B_API_KEY"}},
		Probes:      []Probe{{Name: "gvisor", Script: backendGatesScript, Reason: "runsc is not registered with Docker"}},
	}
	res.Summary = summarize(res.Records)
	res.Verdict = verdict(res)
	return res
}

func TestJUnitMarksUnavailableAsSkippedWithItsReason(t *testing.T) {
	data, err := RenderJUnit(sampleResult())
	if err != nil {
		t.Fatal(err)
	}
	var suites struct {
		Suites []struct {
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
	if err := xml.Unmarshal(data, &suites); err != nil {
		t.Fatal(err)
	}
	for _, c := range suites.Suites[0].Cases {
		switch c.Name {
		case "B0.lint":
			if c.Failure != nil || c.Skipped != nil {
				t.Fatal("a passing row was marked")
			}
		case "B11":
			if c.Skipped == nil || !strings.Contains(c.Skipped.Message, "E2B_API_KEY") {
				t.Fatalf("an unavailable row lost its missing prerequisite: %+v", c.Skipped)
			}
		case "B0.race":
			if c.Failure == nil {
				t.Fatal("a failed row was not a JUnit failure")
			}
		}
	}
}

func TestMarkdownLeadsWithTheVerdictAndListsEveryUnavailableClaim(t *testing.T) {
	text := RenderMarkdown(sampleResult())
	if !strings.HasPrefix(text, "# Plan B evidence — failed") {
		t.Fatalf("the verdict must lead, got %q", strings.SplitN(text, "\n", 2)[0])
	}
	for _, want := range []string{
		"## Unavailable evidence",
		"**B11** (live-service, optional): missing prerequisite: E2B_API_KEY not set",
		"## Failures",
		"runsc is not registered with Docker",
	} {
		if !strings.Contains(text, want) {
			t.Fatalf("the summary omits %q", want)
		}
	}
}

func TestAFailedRequiredRowFailsTheVerdict(t *testing.T) {
	res := sampleResult()
	if res.Verdict != VerdictFailed {
		t.Fatalf("verdict %s with a failed required row", res.Verdict)
	}
	res.Records = res.Records[:2]
	res.Summary = summarize(res.Records)
	if got := verdict(res); got != VerdictComplete {
		t.Fatalf("verdict %s: an optional lane that could not run is not a failure once the summary lists it", got)
	}

	open := res.Records[1]
	open.Scenario, open.Required = "B14", true
	res.Records = append(res.Records, open)
	res.Summary = summarize(res.Records)
	if got := verdict(res); got != VerdictIncomplete {
		t.Fatalf("verdict %s: a required row that never ran is not a completion gate", got)
	}
	if res.Verdict = VerdictIncomplete; res.Failed(false) {
		t.Fatal("an incomplete run is a status report, not a failure")
	}
	if !res.Failed(true) {
		t.Fatal("the completion gate must refuse an incomplete run")
	}
}

func TestADirtyCandidateIsMarkedInTheSummary(t *testing.T) {
	res := sampleResult()
	res.Dirty = true
	if !strings.Contains(RenderMarkdown(res), "uncommitted changes present") {
		t.Fatal("a report built on a dirty tree must say so")
	}
}

func TestListNamesTheOpenRowsAndTheirTickets(t *testing.T) {
	text := RenderList(View(fakeEnv(nil)))
	if !strings.Contains(text, "59 of 59 registered acceptance scenarios shown") {
		t.Fatalf("the list must lead with the counts:\n%s", strings.SplitN(text, "\n", 2)[0])
	}
	for _, want := range []string{"OPEN: Plan B phase B1", "E1", "B32", "missing env: REMOUNT_INTEGRATION_OPENAI_KEY"} {
		if !strings.Contains(text, want) {
			t.Fatalf("the list omits %q", want)
		}
	}
}

func TestListJSONCarriesTheRecordedOutcomeSeparatelyFromEvidence(t *testing.T) {
	views := View(fakeEnv(nil))
	data, err := RenderListJSON(views)
	if err != nil {
		t.Fatal(err)
	}
	text := string(data)
	if !strings.Contains(text, `"recorded": "passed"`) {
		t.Fatal("the recorded outcomes from the closure document are missing")
	}
	if strings.Contains(text, `"evidence": "passed"`) {
		t.Fatal("a recorded outcome was promoted into evidence for this candidate")
	}
}
