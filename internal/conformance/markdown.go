package conformance

import (
	"fmt"
	"io"
	"strings"
	"time"
)

// WriteMarkdown renders the report as a document an engineering review can be
// handed without editing.
//
// The shape follows the same rule as WriteSummary: the verdict first, then the
// evidence, then what nobody could observe. One table per tier keeps the three
// declarations of §12.5 apart on the page, because "required and failing",
// "optional and absent" and "extension and unimplemented" are three different
// sentences and a single merged table invites a reader to average them.
//
// The footer states the unavailable count in words rather than leaving it to
// be counted off the tables. A review that skims one line and walks away with
// "53 checks, no failures" when eleven of them were never observed is exactly
// the outcome Plan B §6.2 exists to prevent.
func (r *Report) WriteMarkdown(w io.Writer) error {
	b := &strings.Builder{}
	ok, why := r.Conformant()
	verdict := "CONFORMANT"
	if !ok {
		verdict = "NOT CONFORMANT"
	}

	fmt.Fprintf(b, "# Remount conformance — %s\n\n", verdict)
	if !ok {
		fmt.Fprintf(b, "**Not conformant** because %s\n\n", escapeCell(why))
	}

	fmt.Fprintln(b, "| Field | Value |")
	fmt.Fprintln(b, "|---|---|")
	fmt.Fprintf(b, "| Candidate | %s |\n", codeOrDash(r.Candidate))
	fmt.Fprintf(b, "| Target | %s |\n", escapeCell(orDash(r.Target)))
	fmt.Fprintf(b, "| Endpoint | %s |\n", codeOrDash(r.Endpoint))
	fmt.Fprintf(b, "| Target kind | %s |\n", codeOrDash(string(r.TargetKind)))
	fmt.Fprintf(b, "| Runtime profile | %s |\n", codeOrDash(r.Profile))
	fmt.Fprintf(b, "| Backend | %s |\n", codeOrDash(r.Environment.Backend))
	fmt.Fprintf(b, "| Host | %s |\n", codeOrDash(hostText(r.Environment)))
	fmt.Fprintf(b, "| Manifest | %s (protocol v%s) |\n", r.ManifestVersion, r.ProtocolVersion)
	fmt.Fprintf(b, "| Date (UTC) | %s |\n", r.StartedAt.UTC().Format(time.RFC3339))
	fmt.Fprintf(b, "| Duration | %s |\n", (time.Duration(r.DurationMS) * time.Millisecond).String())
	fmt.Fprintf(b, "| Negotiated | %s |\n", codeList(r.Negotiated))
	fmt.Fprintf(b, "| Cleanup | %s |\n", escapeCell(string(r.Cleanup)+suffix(r.CleanupReason)))
	fmt.Fprintln(b)

	if r.Aborted != "" {
		fmt.Fprintf(b, "> The run was aborted: %s\n>\n> No requirement below was judged.\n\n", escapeCell(r.Aborted))
	}

	counts := r.Counts()
	fmt.Fprintf(b, "%d passed, %d failed, %d unavailable of %d checks.\n\n",
		counts[StatusPassed], counts[StatusFailed], counts[StatusUnavailable], len(r.Results))

	byTier := map[Tier][]Result{}
	for _, res := range r.Results {
		byTier[res.Requirement.Tier] = append(byTier[res.Requirement.Tier], res)
	}
	for _, tier := range Tiers {
		rows := byTier[tier]
		if len(rows) == 0 {
			continue
		}
		fmt.Fprintf(b, "## %s\n\n", tierHeading(tier))
		fmt.Fprintln(b, "| Requirement | Status | Reason |")
		fmt.Fprintln(b, "|---|---|---|")
		for _, res := range rows {
			fmt.Fprintf(b, "| `%s` %s | %s | %s |\n",
				res.Requirement.ID, escapeCell(res.Requirement.Title),
				statusMark(res.Status), escapeCell(orDash(res.Reason)))
		}
		tierCounts := map[Status]int{}
		for _, res := range rows {
			tierCounts[res.Status]++
		}
		fmt.Fprintf(b, "\n%d passed, %d failed, %d unavailable in this tier.\n\n",
			tierCounts[StatusPassed], tierCounts[StatusFailed], tierCounts[StatusUnavailable])
	}

	unavailable := counts[StatusUnavailable]
	fmt.Fprintf(b, "---\n\n**%d of %d checks were unavailable: they could not be observed on this target, and an unavailable check is not a passing check.**",
		unavailable, len(r.Results))
	switch required := requiredUnavailable(r); {
	case unavailable == 0:
		fmt.Fprintln(b, " Every check ran.")
	case required == 0:
		fmt.Fprintln(b, " None of them are required tier, so the required verdict above rests on evidence that was actually observed.")
	default:
		fmt.Fprintf(b, " %d of them are required tier, and each one makes the run non-conformant on its own.\n", required)
	}
	_, err := io.WriteString(w, b.String())
	return err
}

func requiredUnavailable(r *Report) int {
	n := 0
	for _, res := range r.Results {
		if res.Status == StatusUnavailable && res.Requirement.Tier == TierRequired {
			n++
		}
	}
	return n
}

func tierHeading(t Tier) string {
	switch t {
	case TierRequired:
		return "Required"
	case TierCapabilityGated:
		return "Capability-gated (optional)"
	case TierExtension:
		return "Extension"
	default:
		return string(t)
	}
}

func statusMark(s Status) string {
	switch s {
	case StatusPassed:
		return "pass"
	case StatusFailed:
		return "**FAIL**"
	case StatusUnavailable:
		return "**unavailable**"
	default:
		return string(s)
	}
}

func hostText(e Environment) string {
	if e.OS == "" && e.Arch == "" {
		return ""
	}
	host := e.OS + "/" + e.Arch
	if e.Provider != "" {
		host += " (" + e.Provider + ")"
	}
	return host
}

// escapeCell keeps a reason with a pipe or a newline in it from breaking the
// table it is reported in. A truncated table is a report a reader stops
// trusting.
func escapeCell(s string) string {
	s = strings.ReplaceAll(s, "\r\n", " ")
	s = strings.ReplaceAll(s, "\n", " ")
	s = strings.ReplaceAll(s, "|", "\\|")
	return strings.TrimSpace(s)
}

func codeOrDash(s string) string {
	if strings.TrimSpace(s) == "" {
		return "—"
	}
	return "`" + escapeCell(s) + "`"
}

func codeList(items []string) string {
	if len(items) == 0 {
		return "—"
	}
	out := make([]string, 0, len(items))
	for _, item := range items {
		out = append(out, "`"+escapeCell(item)+"`")
	}
	return strings.Join(out, ", ")
}

func orDash(s string) string {
	if strings.TrimSpace(s) == "" {
		return "—"
	}
	return s
}
