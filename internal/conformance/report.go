package conformance

import (
	"encoding/json"
	"encoding/xml"
	"fmt"
	"io"
	"sort"
	"strings"
	"time"
)

// EvidenceRecord is the JSON object Plan B §6 says every acceptance scenario
// emits.
//
// It duplicates internal/evidence.Record rather than importing it, and that
// is deliberate. internal/evidence is an implementation package of this
// repository; importing it would make the conformance suite unusable against
// any other implementation, which is precisely the property §12.3 exists to
// protect. The field names are fixed by §6, the struct is nine lines, and
// report_test.go compares this encoding's JSON keys against the documented
// schema so the two cannot drift apart silently.
type EvidenceRecord struct {
	Scenario    string      `json:"scenario"`
	Candidate   string      `json:"candidate"`
	Layer       string      `json:"layer"`
	Status      string      `json:"status"`
	Command     string      `json:"command"`
	StartedAt   time.Time   `json:"started_at"`
	DurationMS  int64       `json:"duration_ms"`
	Environment Environment `json:"environment"`
	Cleanup     string      `json:"cleanup"`
	Artifacts   []string    `json:"artifacts,omitempty"`
	Required    bool        `json:"required"`
	Reason      string      `json:"reason,omitempty"`
	Owner       string      `json:"owner,omitempty"`
}

// EvidenceOptions names the things only the caller knows.
type EvidenceOptions struct {
	Scenario  string
	Candidate string
	Layer     string
	Command   string
	Required  bool
	Owner     string
	Artifacts []string
}

// Evidence renders a report as one Plan B §6 record. A run with any required
// failure is `failed`; a run whose required tier was fully observed and held
// is `passed`; a run that could not start is `unavailable`.
func (r *Report) Evidence(opts EvidenceOptions) EvidenceRecord {
	rec := EvidenceRecord{
		Scenario:    opts.Scenario,
		Candidate:   opts.Candidate,
		Layer:       opts.Layer,
		Command:     opts.Command,
		StartedAt:   r.StartedAt,
		DurationMS:  r.DurationMS,
		Environment: r.Environment,
		Cleanup:     string(r.Cleanup),
		Artifacts:   opts.Artifacts,
		Required:    opts.Required,
		Owner:       opts.Owner,
	}
	switch ok, why := r.Conformant(); {
	case r.Aborted != "":
		rec.Status, rec.Reason = "unavailable", r.Aborted
	case ok:
		rec.Status = "passed"
	default:
		rec.Status, rec.Reason = "failed", why
	}
	return rec
}

// WriteEvidence writes the §6 record as indented JSON.
func (r *Report) WriteEvidence(w io.Writer, opts EvidenceOptions) error {
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	return enc.Encode(r.Evidence(opts))
}

// WriteJSON writes the full report, which carries every row rather than the
// one-line verdict the evidence record carries.
func (r *Report) WriteJSON(w io.Writer) error {
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	return enc.Encode(r)
}

// ---- JUnit ---------------------------------------------------------------

type junitSuites struct {
	XMLName  xml.Name     `xml:"testsuites"`
	Name     string       `xml:"name,attr"`
	Tests    int          `xml:"tests,attr"`
	Failures int          `xml:"failures,attr"`
	Skipped  int          `xml:"skipped,attr"`
	Time     string       `xml:"time,attr"`
	Suites   []junitSuite `xml:"testsuite"`
}

type junitSuite struct {
	Name     string      `xml:"name,attr"`
	Tests    int         `xml:"tests,attr"`
	Failures int         `xml:"failures,attr"`
	Skipped  int         `xml:"skipped,attr"`
	Time     string      `xml:"time,attr"`
	Props    []junitProp `xml:"properties>property,omitempty"`
	Cases    []junitCase `xml:"testcase"`
}

type junitProp struct {
	Name  string `xml:"name,attr"`
	Value string `xml:"value,attr"`
}

type junitCase struct {
	Name      string        `xml:"name,attr"`
	ClassName string        `xml:"classname,attr"`
	Time      string        `xml:"time,attr"`
	Failure   *junitMessage `xml:"failure,omitempty"`
	Skipped   *junitMessage `xml:"skipped,omitempty"`
}

type junitMessage struct {
	Message string `xml:"message,attr"`
	Type    string `xml:"type,attr,omitempty"`
	Text    string `xml:",chardata"`
}

// WriteJUnit renders the report as JUnit XML, one suite per semantic
// category. An unavailable row is `skipped` and carries its reason, which is
// the closest JUnit has to Plan B's third outcome; the evidence record
// remains the authority on whether that made the run non-conformant.
func (r *Report) WriteJUnit(w io.Writer) error {
	byCategory := map[Category][]Result{}
	for _, res := range r.Results {
		byCategory[res.Requirement.Category] = append(byCategory[res.Requirement.Category], res)
	}
	out := junitSuites{
		Name: fmt.Sprintf("remount-conformance %s (protocol v%s) against %s", r.ManifestVersion, r.ProtocolVersion, r.Target),
		Time: seconds(r.DurationMS),
	}
	for _, c := range Categories {
		results := byCategory[c]
		if len(results) == 0 {
			continue
		}
		var suiteMS int64
		for _, res := range results {
			suiteMS += res.DurationMS
		}
		suite := junitSuite{Name: string(c), Time: seconds(suiteMS), Props: []junitProp{
			{Name: "manifest.version", Value: r.ManifestVersion},
			{Name: "protocol.version", Value: r.ProtocolVersion},
			{Name: "target.kind", Value: string(r.TargetKind)},
			{Name: "negotiated.capabilities", Value: strings.Join(r.Negotiated, ",")},
		}}
		for _, res := range results {
			tc := junitCase{
				Name:      res.Requirement.ID + " " + res.Requirement.Title,
				ClassName: "conformance." + string(c),
				Time:      seconds(res.DurationMS),
			}
			switch res.Status {
			case StatusFailed:
				tc.Failure = &junitMessage{Message: truncate(res.Reason, 400), Type: string(res.Requirement.Tier), Text: res.Reason}
				suite.Failures++
			case StatusUnavailable:
				tc.Skipped = &junitMessage{Message: truncate(res.Reason, 400), Text: res.Reason}
				suite.Skipped++
			}
			suite.Tests++
			suite.Cases = append(suite.Cases, tc)
		}
		out.Tests += suite.Tests
		out.Failures += suite.Failures
		out.Skipped += suite.Skipped
		out.Suites = append(out.Suites, suite)
	}
	if _, err := io.WriteString(w, xml.Header); err != nil {
		return err
	}
	enc := xml.NewEncoder(w)
	enc.Indent("", "  ")
	if err := enc.Encode(out); err != nil {
		return err
	}
	_, err := io.WriteString(w, "\n")
	return err
}

// ---- human-readable summary ---------------------------------------------

// WriteSummary prints the verdict first, then what did not hold. Leading with
// the verdict is the rule the rest of this repository's diagnostics follow.
func (r *Report) WriteSummary(w io.Writer) error {
	ok, why := r.Conformant()
	verdict := "CONFORMANT"
	if !ok {
		verdict = "NOT CONFORMANT"
	}
	b := &strings.Builder{}
	fmt.Fprintf(b, "%s: %s\n", verdict, r.Target)
	fmt.Fprintf(b, "  manifest %s, protocol v%s, %s, %s\n", r.ManifestVersion, r.ProtocolVersion, r.TargetKind, r.Endpoint)
	if !ok {
		fmt.Fprintf(b, "  because %s\n", why)
	}
	counts := r.Counts()
	fmt.Fprintf(b, "  %d passed, %d failed, %d unavailable of %d requirements in %s\n",
		counts[StatusPassed], counts[StatusFailed], counts[StatusUnavailable], len(r.Results), time.Duration(r.DurationMS)*time.Millisecond)
	byTier := r.CountsByTier()
	for _, tier := range Tiers {
		c := byTier[tier]
		if len(c) == 0 {
			continue
		}
		fmt.Fprintf(b, "    %-16s %d passed, %d failed, %d unavailable\n", tier, c[StatusPassed], c[StatusFailed], c[StatusUnavailable])
	}
	fmt.Fprintf(b, "  negotiated: %s\n", strings.Join(r.Negotiated, ", "))
	fmt.Fprintf(b, "  cleanup: %s%s\n", r.Cleanup, suffix(r.CleanupReason))
	if r.Aborted != "" {
		fmt.Fprintf(b, "  aborted: %s\n", r.Aborted)
	}
	for _, res := range r.Results {
		if res.Status == StatusFailed {
			fmt.Fprintf(b, "  FAIL %s [%s/%s] %s\n       %s\n", res.Requirement.ID, res.Requirement.Category, res.Requirement.Tier, res.Requirement.Title, res.Reason)
		}
	}
	for _, res := range r.Results {
		if res.Status == StatusUnavailable {
			fmt.Fprintf(b, "  UNAVAILABLE %s [%s/%s]\n       %s\n", res.Requirement.ID, res.Requirement.Category, res.Requirement.Tier, res.Reason)
		}
	}
	_, err := io.WriteString(w, b.String())
	return err
}

// WriteManifest prints the manifest itself, so an implementer can read the
// contract without reading the runner.
func WriteManifest(w io.Writer, m Manifest) error {
	b := &strings.Builder{}
	fmt.Fprintf(b, "remount conformance manifest %s (protocol v%s), %d requirements\n\n", m.Version, m.ProtocolVersion, len(m.Requirements))
	counts := m.Counts()
	for _, t := range Tiers {
		fmt.Fprintf(b, "  %-16s %d\n", t, counts[t])
	}
	grouped := m.ByCategory()
	for _, c := range Categories {
		rows := grouped[c]
		fmt.Fprintf(b, "\n%s (%d)\n", c, len(rows))
		for _, r := range rows {
			fmt.Fprintf(b, "  %-14s v%-7s %-16s %s\n", r.ID, r.Version, r.Tier, r.Title)
			fmt.Fprintf(b, "  %-14s %s%s\n", "", r.Spec, suffix(r.Gates()))
		}
	}
	_, err := io.WriteString(w, b.String())
	return err
}

// SortedFailureIDs returns failed requirement ids in lexical order.
func (r *Report) SortedFailureIDs() []string {
	var out []string
	for _, res := range r.Failures() {
		out = append(out, res.Requirement.ID)
	}
	sort.Strings(out)
	return out
}

func seconds(ms int64) string { return fmt.Sprintf("%.3f", float64(ms)/1000) }

func suffix(s string) string {
	if s == "" {
		return ""
	}
	return " — " + s
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "…"
}
