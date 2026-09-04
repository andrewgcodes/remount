package evidence

import (
	"encoding/json"
	"encoding/xml"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

// RenderJSON is the machine-readable evidence manifest.
func RenderJSON(res Result) ([]byte, error) {
	data, err := json.MarshalIndent(res, "", "  ")
	if err != nil {
		return nil, err
	}
	return append(data, '\n'), nil
}

type junitFailure struct {
	Message string `xml:"message,attr"`
	Text    string `xml:",chardata"`
}

type junitSkipped struct {
	Message string `xml:"message,attr"`
}

type junitCase struct {
	XMLName   xml.Name      `xml:"testcase"`
	Name      string        `xml:"name,attr"`
	Classname string        `xml:"classname,attr"`
	Time      float64       `xml:"time,attr"`
	Failure   *junitFailure `xml:"failure,omitempty"`
	Skipped   *junitSkipped `xml:"skipped,omitempty"`
}

type junitSuite struct {
	XMLName  xml.Name    `xml:"testsuite"`
	Name     string      `xml:"name,attr"`
	Tests    int         `xml:"tests,attr"`
	Failures int         `xml:"failures,attr"`
	Skipped  int         `xml:"skipped,attr"`
	Cases    []junitCase `xml:"testcase"`
}

type junitSuites struct {
	XMLName xml.Name     `xml:"testsuites"`
	Name    string       `xml:"name,attr"`
	Suites  []junitSuite `xml:"testsuite"`
}

// RenderJUnit maps the run onto JUnit. An unavailable row becomes `skipped`
// carrying its missing prerequisite, because JUnit has no honest third state;
// readers must not treat a skipped case as a pass.
func RenderJUnit(res Result) ([]byte, error) {
	suite := junitSuite{Name: "plan-b"}
	for _, rec := range res.Records {
		c := junitCase{Name: rec.Scenario, Classname: string(rec.Layer), Time: float64(rec.DurationMS) / 1000}
		switch rec.Status {
		case StatusFailed:
			c.Failure = &junitFailure{Message: rec.Reason, Text: rec.Command}
			suite.Failures++
		case StatusUnavailable:
			c.Skipped = &junitSkipped{Message: "unavailable: " + rec.Reason}
			suite.Skipped++
		}
		suite.Tests++
		suite.Cases = append(suite.Cases, c)
	}
	data, err := xml.MarshalIndent(junitSuites{Name: "plan-b", Suites: []junitSuite{suite}}, "", "  ")
	if err != nil {
		return nil, err
	}
	return append([]byte(xml.Header), append(data, '\n')...), nil
}

// RenderMarkdown renders the human summary. Unavailable rows get their own
// prominent section: a reader who stops at the verdict must still be told what
// the run did not prove.
func RenderMarkdown(res Result) string {
	var b strings.Builder
	fmt.Fprintf(&b, "# Plan B evidence — %s\n\n", res.Verdict)
	fmt.Fprintf(&b, "- Candidate: `%s`%s\n", res.Candidate, dirtyNote(res))
	fmt.Fprintf(&b, "- Host: %s/%s\n", res.Environment.OS, res.Environment.Arch)
	fmt.Fprintf(&b, "- Started: %s (%d ms)\n", res.StartedAt.Format("2006-01-02T15:04:05Z"), res.DurationMS)
	fmt.Fprintf(&b, "- Rows: %d passed, %d failed, %d unavailable\n", res.Summary.Passed, res.Summary.Failed, res.Summary.Unavailable)
	fmt.Fprintf(&b, "- Required rows: %d of %d passed, %d failed, %d unavailable\n",
		res.Summary.RequiredPassed, res.Summary.RequiredTotal, res.Summary.RequiredFailed, res.Summary.RequiredUnavailable)
	fmt.Fprintf(&b, "- Registry: %d scenarios, %d with no owning proof\n", res.Summary.ScenariosRegistered, res.Summary.ScenariosUnowned)
	fmt.Fprintf(&b, "- External resources created: %d; cleanup verified %d, failed %d\n\n",
		res.Summary.ExternalResources, res.Summary.CleanupVerified, res.Summary.CleanupFailed)

	fmt.Fprintf(&b, "%s\n\n", verdictSentence(res))

	b.WriteString("## Credential presence\n\nDetected by presence only; no value is read into this report.\n\n")
	b.WriteString("| Variable | Present |\n|---|---|\n")
	for _, c := range res.Credentials {
		fmt.Fprintf(&b, "| `%s` | %t |\n", c.Name, c.Present)
	}

	b.WriteString("\n## Host probes\n\n| Probe | Available | Reason | Script |\n|---|---|---|---|\n")
	for _, p := range res.Probes {
		fmt.Fprintf(&b, "| %s | %t | %s | `%s` |\n", p.Name, p.Available, orDash(p.Reason), p.Script)
	}

	b.WriteString("\n## Rows\n\n| Scenario | Layer | Required | Status | Owner |\n|---|---|---|---|---|\n")
	for _, rec := range res.Records {
		fmt.Fprintf(&b, "| %s | %s | %t | %s | %s |\n", rec.Scenario, rec.Layer, rec.Required, rec.Status, orDash(rec.Owner))
	}

	b.WriteString("\n## Unavailable evidence\n\nEvery row below did not run. None of them is a pass.\n\n")
	any := false
	for _, rec := range res.Records {
		if rec.Status != StatusUnavailable {
			continue
		}
		any = true
		fmt.Fprintf(&b, "- **%s** (%s, %s): %s\n", rec.Scenario, rec.Layer, requiredWord(rec.Required), rec.Reason)
	}
	if !any {
		b.WriteString("- none\n")
	}

	if res.Summary.Failed > 0 {
		b.WriteString("\n## Failures\n\n")
		for _, rec := range res.Records {
			if rec.Status == StatusFailed {
				fmt.Fprintf(&b, "- **%s**: %s\n", rec.Scenario, rec.Reason)
			}
		}
	}
	if len(res.Leaks) > 0 {
		b.WriteString("\n## Leaks\n\nThe scan found credential-shaped strings. Values are never reproduced here.\n\n")
		for _, f := range res.Leaks {
			fmt.Fprintf(&b, "- %s\n", f)
		}
	}
	return b.String()
}

func verdictSentence(res Result) string {
	switch res.Verdict {
	case VerdictFailed:
		return "**This run failed.** A required row failed, cleanup failed, or the leak scan found something."
	case VerdictComplete:
		return "**Every required row passed in this run.**"
	default:
		return fmt.Sprintf("**This run is not a completion gate.** %d required rows did not run; each is listed under Unavailable evidence with its missing prerequisite. A recorded outcome in another document is provenance, not a pass earned here.", res.Summary.RequiredUnavailable)
	}
}

func dirtyNote(res Result) string {
	if !res.Dirty {
		return ""
	}
	return " — **uncommitted changes present; this report does not describe that commit alone**"
}

func requiredWord(required bool) string {
	if required {
		return "required"
	}
	return "optional"
}

func orDash(s string) string {
	if s == "" {
		return "—"
	}
	return s
}

// writeOutputs writes the three summaries side by side. A caller that reads
// only one of them still gets the unavailable rows.
func writeOutputs(dir string, res Result) error {
	data, err := RenderJSON(res)
	if err != nil {
		return err
	}
	if err := os.WriteFile(filepath.Join(dir, "evidence.json"), data, 0o644); err != nil {
		return err
	}
	junit, err := RenderJUnit(res)
	if err != nil {
		return err
	}
	if err := os.WriteFile(filepath.Join(dir, "junit.xml"), junit, 0o644); err != nil {
		return err
	}
	return os.WriteFile(filepath.Join(dir, "summary.md"), []byte(RenderMarkdown(res)), 0o644)
}
