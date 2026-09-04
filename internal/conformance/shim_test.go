package conformance

import (
	"context"
	"encoding/json"
	"os"
	"sort"
	"strings"
	"testing"
	"time"

	"remount.dev/remount/internal/conformance/shim"
)

// seedFile is the checked-in table of deterministic failure seeds (§12.4).
type seedTable struct {
	ManifestVersion string `json:"manifest_version"`
	ProtocolVersion string `json:"protocol_version"`
	Seeds           []seed `json:"seeds"`
}

type seed struct {
	Defect      string   `json:"defect"`
	Category    string   `json:"category"`
	Violation   string   `json:"violation"`
	MustFail    []string `json:"must_fail"`
	MustNotFail []string `json:"must_not_fail"`
}

func loadSeeds(t *testing.T) seedTable {
	t.Helper()
	raw, err := os.ReadFile("testdata/failure-seeds.json")
	if err != nil {
		t.Fatalf("reading the failure seeds: %v", err)
	}
	var table seedTable
	if err := json.Unmarshal(raw, &table); err != nil {
		t.Fatalf("parsing the failure seeds: %v", err)
	}
	if table.ManifestVersion != ManifestVersion {
		t.Fatalf("the seeds describe manifest %s, this build publishes %s", table.ManifestVersion, ManifestVersion)
	}
	if table.ProtocolVersion != ProtocolVersion {
		t.Fatalf("the seeds describe protocol v%s, this manifest describes v%s", table.ProtocolVersion, ProtocolVersion)
	}
	return table
}

// shimTarget starts a shim and points a conformance target at it, telling the
// runner exactly what the shim provides. Nothing else is asserted about the
// shim from the outside: the suite reaches it through the same surfaces it
// would reach any implementation through.
func shimTarget(t *testing.T, defect shim.Defect) *Target {
	t.Helper()
	server := shim.Start(defect)
	t.Cleanup(server.Close)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	target, err := Attach(ctx, AttachOptions{
		Name:     "conformance shim (" + string(defect) + ")",
		Endpoint: server.URL(),
		Kind:     KindExternal,
		Backend:  "shim",
		Binding: BindingFixture{
			ID:           shim.BindingID,
			Placeholder:  shim.Placeholder,
			Secret:       shim.Secret,
			Destinations: []string{shim.BoundHost},
			UnboundHost:  shim.UnboundHost,
		},
	})
	if err != nil {
		t.Fatalf("attaching to the shim: %v", err)
	}
	t.Cleanup(func() { _ = target.Close() })
	return target
}

func runAgainst(t *testing.T, target *Target, only []string) *Report {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	report, err := Run(ctx, target, RunOptions{Only: only, PerCheck: 60 * time.Second})
	if err != nil {
		t.Fatalf("running the manifest: %v", err)
	}
	return report
}

func statusOf(r *Report, id string) (Status, string) {
	for _, res := range r.Results {
		if res.Requirement.ID == id {
			return res.Status, res.Reason
		}
	}
	return "", "no such requirement in the report"
}

// TestB21BaselineShimPassesTheRequiredManifest establishes the control for
// B21. If the undefective shim did not pass, a defective one failing would
// prove nothing about the defect.
func TestB21BaselineShimPassesTheRequiredManifest(t *testing.T) {
	report := runAgainst(t, shimTarget(t, shim.DefectNone), nil)
	if ok, why := report.Conformant(); !ok {
		t.Fatalf("the baseline shim is not conformant, so no defect result is interpretable: %s", why)
	}
	if len(report.FailedCategories()) != 0 {
		t.Fatalf("the baseline shim failed categories %v", report.FailedCategories())
	}
	if report.Cleanup != CleanupVerified {
		t.Errorf("cleanup after the baseline run was %q: %s", report.Cleanup, report.CleanupReason)
	}
	// Conformance by unavailability would be worthless as a control, so the
	// baseline must have actually observed every required row.
	byTier := report.CountsByTier()
	if got := byTier[TierRequired][StatusUnavailable]; got != 0 {
		t.Errorf("%d required requirements were unavailable against the baseline shim", got)
	}
	if got := byTier[TierRequired][StatusPassed]; got != Standard().Counts()[TierRequired] {
		t.Errorf("the baseline shim passed %d of %d required requirements", got, Standard().Counts()[TierRequired])
	}
}

// TestB21BrokenImplementationFailsEachSemanticCategory is the scenario that
// gives the suite its credibility. For every checked-in failure seed the
// suite must fail exactly the rows the seed names, in the seed's category,
// and must keep passing the rows it names as untouched.
func TestB21BrokenImplementationFailsEachSemanticCategory(t *testing.T) {
	table := loadSeeds(t)
	if len(table.Seeds) != len(shim.Defects) {
		t.Fatalf("the seed table names %d defects, the shim implements %d", len(table.Seeds), len(shim.Defects))
	}
	covered := map[string]bool{}
	for _, s := range table.Seeds {
		t.Run(s.Defect, func(t *testing.T) {
			var only []string
			only = append(only, s.MustFail...)
			only = append(only, s.MustNotFail...)
			report := runAgainst(t, shimTarget(t, shim.Defect(s.Defect)), only)

			for _, id := range s.MustFail {
				got, reason := statusOf(report, id)
				if got != StatusFailed {
					t.Errorf("%s: %s is %q, not %q; the suite did not catch %q\n  reason: %s",
						s.Defect, id, got, StatusFailed, s.Violation, reason)
					continue
				}
				req, _ := Standard().Find(id)
				if string(req.Category) != s.Category {
					t.Errorf("%s: %s failed in category %q, the seed places the defect in %q",
						s.Defect, id, req.Category, s.Category)
				}
			}
			for _, id := range s.MustNotFail {
				if got, reason := statusOf(report, id); got == StatusFailed {
					t.Errorf("%s: %s also failed, so the seed does not isolate one category: %s", s.Defect, id, reason)
				}
			}
			if cats := report.FailedCategories(); len(cats) != 1 || string(cats[0]) != s.Category {
				t.Errorf("%s: failures landed in %v, the seed names only %q", s.Defect, cats, s.Category)
			}
		})
		covered[s.Category] = true
	}
	for _, c := range Categories {
		if !covered[string(c)] {
			t.Errorf("no failure seed exercises category %q, so nothing proves the suite can fail it", c)
		}
	}
}

// TestB22UnknownExtensionsStayIgnorableAndUnknownRequiredCapabilitiesFail is
// the two halves of B22 in one place: an identifier nobody implements must be
// harmless, and the absence of an identifier the baseline requires must not
// be.
func TestB22UnknownExtensionsStayIgnorableAndUnknownRequiredCapabilitiesFail(t *testing.T) {
	baseline := runAgainst(t, shimTarget(t, shim.DefectNone), []string{"CONF-NEG-002", "CONF-NEG-003"})
	if got, why := statusOf(baseline, "CONF-NEG-002"); got != StatusPassed {
		t.Errorf("an unknown extension was not ignorable against the baseline shim: %s (%s)", got, why)
	}
	if got, why := statusOf(baseline, "CONF-NEG-003"); got != StatusPassed {
		t.Errorf("a hello without the v1 baseline was not refused by the baseline shim: %s (%s)", got, why)
	}

	broken := runAgainst(t, shimTarget(t, shim.DefectNegotiation), []string{"CONF-NEG-002", "CONF-NEG-003"})
	got, why := statusOf(broken, "CONF-NEG-002")
	if got != StatusFailed {
		t.Errorf("an implementation that echoes identifiers it does not implement was judged %q", got)
	} else if !strings.Contains(why, unknownExtension) {
		t.Errorf("the failure did not name the echoed identifier: %s", why)
	}
	if got, why := statusOf(broken, "CONF-NEG-003"); got != StatusFailed {
		t.Errorf("an implementation that accepts a hello with no v1 baseline was judged %q (%s)", got, why)
	}
}

// TestB22CapabilityGatedRowsAreUnavailableNotPassed is the reporting half of
// B22: a requirement whose gate is absent must never be counted as satisfied.
func TestB22CapabilityGatedRowsAreUnavailableNotPassed(t *testing.T) {
	server := shim.Start(shim.DefectNone)
	t.Cleanup(server.Close)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	// No Binding fixture, so PrereqBindings is unavailable.
	target, err := Attach(ctx, AttachOptions{Name: "ungated shim", Endpoint: server.URL(), Kind: KindExternal, Backend: "shim"})
	if err != nil {
		t.Fatalf("attaching: %v", err)
	}
	t.Cleanup(func() { _ = target.Close() })
	report := runAgainst(t, target, []string{"CONF-BIND-001", "CONF-BIND-002"})
	for _, id := range []string{"CONF-BIND-001", "CONF-BIND-002"} {
		got, why := statusOf(report, id)
		if got != StatusUnavailable {
			t.Errorf("%s on a target without the binding prerequisite is %q, not %q", id, got, StatusUnavailable)
		}
		if why == "" {
			t.Errorf("%s is unavailable without naming what was missing", id)
		}
	}
}

// TestB23ProtocolVersionMismatchFailsBeforeAnyLifecycleMutation proves both
// halves of B23: the connection is refused, and the refusal is checked as an
// ordering claim rather than merely as an error.
func TestB23ProtocolVersionMismatchFailsBeforeAnyLifecycleMutation(t *testing.T) {
	baseline := runAgainst(t, shimTarget(t, shim.DefectNone), []string{"CONF-NEG-004"})
	if got, why := statusOf(baseline, "CONF-NEG-004"); got != StatusPassed {
		t.Fatalf("the baseline shim did not fail closed on an unnegotiated version: %s (%s)", got, why)
	}

	accepting := runAgainst(t, shimTarget(t, shim.DefectVersion), []string{"CONF-NEG-004"})
	got, why := statusOf(accepting, "CONF-NEG-004")
	if got != StatusFailed {
		t.Errorf("an implementation that accepts any frame version was judged %q", got)
	} else if !strings.Contains(why, "was accepted") {
		t.Errorf("the failure did not name the accepted version: %s", why)
	}

	// The interesting one. This implementation *does* refuse the connection,
	// so a suite that only checked for an error would pass it. It must fail
	// on the ordering claim instead.
	mutating := runAgainst(t, shimTarget(t, shim.DefectVersionMutates), []string{"CONF-NEG-004"})
	got, why = statusOf(mutating, "CONF-NEG-004")
	if got != StatusFailed {
		t.Fatalf("an implementation that mutated state before refusing an unnegotiated version was judged %q", got)
	}
	if strings.Contains(why, "was accepted") {
		t.Fatalf("the failure was attributed to acceptance, but this implementation refused the connection: %s", why)
	}
	if !strings.Contains(why, "mutated state before failing") && !strings.Contains(why, "changed the workspace inventory") {
		t.Fatalf("the failure did not rest on the ordering claim: %s", why)
	}
}

// TestB23TargetDeclaringAnotherProtocolVersionIsNotJudged is the other side
// of a version mismatch: an implementation that says it speaks something else
// is not measured against this manifest at all, because a verdict about a
// contract it never claimed would be meaningless.
func TestB23TargetDeclaringAnotherProtocolVersionIsNotJudged(t *testing.T) {
	server := shim.Start(shim.DefectNone)
	t.Cleanup(server.Close)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	target, err := Attach(ctx, AttachOptions{
		Name: "future implementation", Endpoint: server.URL(),
		Kind: KindExternal, DeclaredProtocol: "2", Backend: "shim",
	})
	if err != nil {
		t.Fatalf("attaching: %v", err)
	}
	t.Cleanup(func() { _ = target.Close() })
	report := runAgainst(t, target, nil)
	if report.Aborted == "" {
		t.Fatal("a target declaring protocol v2 was judged against the v1 manifest")
	}
	if len(report.Results) != 0 {
		t.Fatalf("an aborted run produced %d results", len(report.Results))
	}
	if ok, _ := report.Conformant(); ok {
		t.Fatal("an aborted run reported the target as conformant")
	}
	rec := report.Evidence(EvidenceOptions{Scenario: "B23", Candidate: "0000000", Layer: "code"})
	if rec.Status != "unavailable" || rec.Reason == "" {
		t.Fatalf("an aborted run produced evidence %q with reason %q", rec.Status, rec.Reason)
	}
}

// TestSeedTableNamesEveryDefect keeps the checked-in seeds and the shim from
// drifting apart in either direction.
func TestSeedTableNamesEveryDefect(t *testing.T) {
	table := loadSeeds(t)
	named := map[string]bool{}
	for _, s := range table.Seeds {
		named[s.Defect] = true
		if shim.CategoryOf(shim.Defect(s.Defect)) != s.Category {
			t.Errorf("seed %q claims category %q, the shim places it in %q", s.Defect, s.Category, shim.CategoryOf(shim.Defect(s.Defect)))
		}
		if len(s.MustFail) == 0 {
			t.Errorf("seed %q names nothing that must fail", s.Defect)
		}
		if s.Violation == "" {
			t.Errorf("seed %q does not say what it violates", s.Defect)
		}
		for _, id := range append(append([]string(nil), s.MustFail...), s.MustNotFail...) {
			if _, ok := Standard().Find(id); !ok {
				t.Errorf("seed %q names %s, which the manifest does not declare", s.Defect, id)
			}
		}
	}
	var missing []string
	for _, d := range shim.Defects {
		if !named[string(d)] {
			missing = append(missing, string(d))
		}
	}
	sort.Strings(missing)
	if len(missing) > 0 {
		t.Errorf("the shim implements defects the seed table does not name: %v", missing)
	}
}
