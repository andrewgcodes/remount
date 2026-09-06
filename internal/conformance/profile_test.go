package conformance

import (
	"context"
	"strings"
	"testing"
	"time"
)

func TestProfileObligationsAreOrderedAndMonotonic(t *testing.T) {
	prev := ProfileObligations(ProfileDev)
	for _, p := range ProfileNames[1:] {
		next := ProfileObligations(p)
		if len(next) < len(prev) {
			t.Fatalf("profile %q demands %d obligations, weaker profile demanded %d", p, len(next), len(prev))
		}
		have := map[string]bool{}
		for _, c := range next {
			have[c] = true
		}
		for _, c := range prev {
			if !have[c] {
				t.Errorf("profile %q drops obligation %q that a weaker profile carries", p, c)
			}
		}
		prev = next
	}
	for _, p := range ProfileNames {
		if len(ProfileObligations(p)) == 0 {
			t.Errorf("profile %q demands nothing, so judging against it would mean nothing", p)
		}
		for _, c := range ProfileObligations(p) {
			if profileObligationText[c] == "" {
				t.Errorf("profile %q names check %q with no stated obligation", p, c)
			}
		}
	}
}

func TestProfileRequirementIDIsStableAndUppercase(t *testing.T) {
	cases := map[string]string{
		ProfCheckBackendEnforcedEgress: "CONF-PROF-BACKEND-ENFORCED-EGRESS",
		ProfCheckRuntimeHealthy:        "CONF-PROF-RUNTIME-HEALTHY",
		ProfCheckHostMicroVMCompat:     "CONF-PROF-HOST-MICROVM-COMPAT",
	}
	for check, want := range cases {
		if got := ProfileRequirementID(check); got != want {
			t.Errorf("ProfileRequirementID(%q) = %q, want %q", check, got, want)
		}
	}
}

// An obligation a node never answered is the case that matters most: it is
// exactly where a suite is tempted to drop the row and report a shorter,
// greener table.
func TestAnObligationNoNodeAnsweredIsUnavailableNotAbsent(t *testing.T) {
	rows := foldProfileReports(ProfileMicroVM, []NodeProfileReport{{
		Node: "n1", Profile: ProfileMicroVM, Status: FindingUnavailable, Online: true,
		Checks: []Finding{
			{Check: ProfCheckBackendRegistered, Status: FindingPass, Detail: "one backend"},
		},
	}})
	want := len(ProfileObligations(ProfileMicroVM))
	if len(rows) != want {
		t.Fatalf("the fold produced %d rows for %d obligations", len(rows), want)
	}
	byID := map[string]Result{}
	for _, r := range rows {
		byID[r.Requirement.ID] = r
	}
	silent := byID[ProfileRequirementID(ProfCheckHostMicroVMCompat)]
	if silent.Status != StatusUnavailable {
		t.Fatalf("an unanswered obligation is %q, want %q", silent.Status, StatusUnavailable)
	}
	if !strings.Contains(silent.Reason, "n1") {
		t.Errorf("the unavailable reason %q does not name the node that stayed silent", silent.Reason)
	}
	if byID[ProfileRequirementID(ProfCheckBackendRegistered)].Status != StatusPassed {
		t.Error("an obligation every node passed should pass")
	}
}

// One node failing an obligation fails it for the deployment. An operator
// asking whether a fleet is fit for untrusted work is not asking whether some
// machine in it is.
func TestOneFailingNodeFailsTheFleet(t *testing.T) {
	rows := foldProfileReports(ProfileTrustedSingleTenant, []NodeProfileReport{
		{Node: "good", Online: true, Checks: []Finding{
			{Check: ProfCheckBackendRegistered, Status: FindingPass},
			{Check: ProfCheckBackendIsolated, Status: FindingPass},
			{Check: ProfCheckBackendBrokeredSecrets, Status: FindingPass},
		}},
		{Node: "weak", Online: true, Checks: []Finding{
			{Check: ProfCheckBackendRegistered, Status: FindingPass},
			{Check: ProfCheckBackendIsolated, Status: FindingFail, Detail: "isolation=none", Subject: "process"},
			{Check: ProfCheckBackendBrokeredSecrets, Status: FindingPass},
		}},
	})
	for _, r := range rows {
		if r.Requirement.ID != ProfileRequirementID(ProfCheckBackendIsolated) {
			continue
		}
		if r.Status != StatusFailed {
			t.Fatalf("the isolation row is %q, want %q", r.Status, StatusFailed)
		}
		if !strings.Contains(r.Reason, "weak") || !strings.Contains(r.Reason, "isolation=none") {
			t.Fatalf("the failure reason %q names neither the node nor the observed value", r.Reason)
		}
		return
	}
	t.Fatal("the isolation obligation produced no row at all")
}

// A finding with no status is a check nobody recorded. §6 is explicit that it
// is not a pass, and this is the arm a decoder is most likely to get wrong.
func TestAnEmptyFindingStatusIsUnavailableNeverPass(t *testing.T) {
	rows := foldProfileReports(ProfileDev, []NodeProfileReport{{
		Node: "n1", Online: true,
		Checks: []Finding{{Check: ProfCheckBackendRegistered, Detail: "no verdict recorded"}},
	}})
	if len(rows) != 1 || rows[0].Status != StatusUnavailable {
		t.Fatalf("a statusless finding folded to %+v, want one unavailable row", rows)
	}
}

// §6 permits a target to report checks this transcription does not know
// about. Dropping them would hide evidence the operator was handed.
func TestAnUnknownReportedCheckKeepsItsOwnRow(t *testing.T) {
	rows := foldProfileReports(ProfileDev, []NodeProfileReport{{
		Node: "n1", Online: true,
		Checks: []Finding{
			{Check: ProfCheckBackendRegistered, Status: FindingPass},
			{Check: "profile.node.online", Status: FindingUnavailable, Detail: "the node is offline"},
		},
	}})
	var found bool
	for _, r := range rows {
		if r.Requirement.ID == ProfileRequirementID("profile.node.online") {
			found = true
			if r.Status != StatusUnavailable {
				t.Errorf("the extra row is %q, want %q", r.Status, StatusUnavailable)
			}
		}
	}
	if !found {
		t.Fatal("a check the target reported was dropped from the report")
	}
}

func TestUnknownProfileIsRefusedBeforeAnythingIsJudged(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_, err := Run(ctx, &Target{DeclaredProtocol: ProtocolVersion}, RunOptions{Profile: "production"})
	if err == nil {
		t.Fatal("Run accepted a runtime profile §5 does not define")
	}
	if !strings.Contains(err.Error(), "multi-tenant-isolated") {
		t.Errorf("the refusal %q does not name the profiles that exist", err)
	}
}

// A target that does not implement node.profile.get must not make the profile
// obligations disappear. This is the whole point of the tier: a run that
// asked for a profile verdict and got silence is unavailable, not conformant.
func TestATargetThatCannotAnswerLeavesEveryObligationUnavailable(t *testing.T) {
	target := shimTarget(t, "")
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	report, err := Run(ctx, target, RunOptions{
		Profile:  ProfileMicroVM,
		Only:     append(prefixedIDs(ProfileMicroVM), ProfileSchedulingID),
		PerCheck: 20 * time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	if report.Profile != ProfileMicroVM {
		t.Fatalf("the report names profile %q", report.Profile)
	}
	obligations := len(ProfileObligations(ProfileMicroVM))
	if len(report.Results) < obligations {
		t.Fatalf("the run produced %d rows for %d obligations plus scheduling", len(report.Results), obligations)
	}
	for _, res := range report.Results {
		if !strings.HasPrefix(res.Requirement.ID, "CONF-PROF-") {
			t.Fatalf("--only leaked a non-profile row %s", res.Requirement.ID)
		}
		if res.Status == StatusPassed {
			t.Errorf("%s passed against a target that answers no profile operation", res.Requirement.ID)
		}
		if res.Reason == "" {
			t.Errorf("%s is %s with no reason; §6.2 forbids an unexplained non-pass", res.Requirement.ID, res.Status)
		}
	}
	if ok, _ := report.Conformant(); ok {
		t.Fatal("a run whose every profile obligation was unobservable reported conformant")
	}
}

func prefixedIDs(profile string) []string {
	var out []string
	for _, check := range ProfileObligations(profile) {
		out = append(out, ProfileRequirementID(check))
	}
	return out
}
