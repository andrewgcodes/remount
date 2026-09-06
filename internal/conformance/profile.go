package conformance

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"time"
)

// Runtime-profile conformance, transcribed from spec/PROTOCOL.md §5
// (`requires.profile`, `pending_reason`) and §6 (`node.profile.get`) rather
// than imported from internal/profile.
//
// The transcription rule of this package applies here for the same reason it
// applies to wire.go: a suite that asked internal/profile what the profiles
// require would be asking the implementation under test to grade its own
// homework, and could not judge a foreign implementation at all. The names
// below are the stable check identifiers §6 says a report carries; a target
// that reports a check this file does not know about still gets a row, and a
// check this file expects but the target never reported is `unavailable`,
// never dropped and never a pass.

// The four runtime profiles of §5, weakest first.
const (
	ProfileDev                 = "dev"
	ProfileTrustedSingleTenant = "trusted-single-tenant"
	ProfileMultiTenantIsolated = "multi-tenant-isolated"
	ProfileMicroVM             = "microvm"
)

// ProfileNames lists the profiles a run may be judged against, weakest first.
var ProfileNames = []string{ProfileDev, ProfileTrustedSingleTenant, ProfileMultiTenantIsolated, ProfileMicroVM}

// ValidProfile reports whether name is one §5 defines.
func ValidProfile(name string) bool {
	for _, p := range ProfileNames {
		if p == name {
			return true
		}
	}
	return false
}

// The check identifiers a §6 profile report carries.
const (
	ProfCheckBackendRegistered      = "profile.backend.registered"
	ProfCheckBackendIsolated        = "profile.backend.isolated"
	ProfCheckBackendBrokeredSecrets = "profile.backend.brokered_secrets"
	ProfCheckBackendUntrustedAbsent = "profile.backend.untrusted_absent"
	ProfCheckBackendSiblingIsolated = "profile.backend.sibling_isolation"
	ProfCheckBackendEnforcedEgress  = "profile.backend.enforced_egress"
	ProfCheckBackendNetworkNS       = "profile.backend.network_namespace"
	ProfCheckBackendStrongIsolation = "profile.backend.container_or_microvm"
	ProfCheckBackendMicroVM         = "profile.backend.microvm"
	ProfCheckBackendMultiTenant     = "profile.backend.multi_tenant"
	ProfCheckBackendDeviceIsolation = "profile.backend.device_isolation"
	ProfCheckRuntimeHealthy         = "profile.runtime.healthy"
	ProfCheckHostMicroVMCompat      = "profile.host.microvm_compat"
)

// profileObligations is what each profile demands, in evaluation order.
var profileObligations = map[string][]string{
	ProfileDev: {ProfCheckBackendRegistered},
	ProfileTrustedSingleTenant: {
		ProfCheckBackendRegistered, ProfCheckBackendIsolated, ProfCheckBackendBrokeredSecrets,
	},
	ProfileMultiTenantIsolated: {
		ProfCheckBackendRegistered, ProfCheckBackendIsolated, ProfCheckBackendBrokeredSecrets,
		ProfCheckBackendUntrustedAbsent, ProfCheckBackendSiblingIsolated, ProfCheckBackendEnforcedEgress,
		ProfCheckBackendNetworkNS, ProfCheckBackendStrongIsolation, ProfCheckRuntimeHealthy,
	},
	ProfileMicroVM: {
		ProfCheckBackendRegistered, ProfCheckBackendIsolated, ProfCheckBackendBrokeredSecrets,
		ProfCheckBackendUntrustedAbsent, ProfCheckBackendSiblingIsolated, ProfCheckBackendEnforcedEgress,
		ProfCheckBackendNetworkNS, ProfCheckBackendStrongIsolation, ProfCheckBackendMicroVM,
		ProfCheckBackendMultiTenant, ProfCheckBackendDeviceIsolation, ProfCheckRuntimeHealthy,
		ProfCheckHostMicroVMCompat,
	},
}

// profileObligationText states each obligation in the imperative of the spec.
var profileObligationText = map[string]string{
	ProfCheckBackendRegistered:      "every node registers at least one workspace backend",
	ProfCheckBackendIsolated:        "every registered backend isolates the workspace from the host",
	ProfCheckBackendBrokeredSecrets: "every registered backend authenticates to the broker",
	ProfCheckBackendUntrustedAbsent: "no shared-kernel development backend is registered",
	ProfCheckBackendSiblingIsolated: "every registered backend isolates one workspace from another",
	ProfCheckBackendEnforcedEgress:  "every registered backend forces egress through the broker gateway",
	ProfCheckBackendNetworkNS:       "every registered backend gives the workspace its own network namespace",
	ProfCheckBackendStrongIsolation: "every registered backend is a container or a microVM",
	ProfCheckBackendMicroVM:         "every registered backend is a microVM",
	ProfCheckBackendMultiTenant:     "every registered backend is approved for multi-tenant execution",
	ProfCheckBackendDeviceIsolation: "every registered backend isolates host devices",
	ProfCheckRuntimeHealthy:         "every host runtime check the node reports passes",
	ProfCheckHostMicroVMCompat:      "the host reports microVM compatibility",
}

// ProfileObligations lists the check identifiers a profile demands. An
// unknown name has no obligations, which is why RunOptions.Profile is
// validated before a run starts rather than silently judged against nothing.
func ProfileObligations(profile string) []string {
	return append([]string(nil), profileObligations[profile]...)
}

// ---- the §6 wire bodies -------------------------------------------------

// Finding is one named check result inside a §6 report. Status is `pass`,
// `fail` or `unavailable`; an empty status is a check nobody recorded and is
// read as unavailable, never as a pass.
type Finding struct {
	Severity string `cbor:"severity"`
	Check    string `cbor:"check"`
	Subject  string `cbor:"subject,omitempty"`
	Detail   string `cbor:"detail"`
	Hint     string `cbor:"hint,omitempty"`
	Status   string `cbor:"status,omitempty"`
}

// The three verdicts a Finding may carry (§6).
const (
	FindingPass        = "pass"
	FindingFail        = "fail"
	FindingUnavailable = "unavailable"
)

// NodeProfileGetReq is `node.profile.get`.
type NodeProfileGetReq struct {
	Node    string `cbor:"node,omitempty"`
	Profile string `cbor:"profile,omitempty"`
}

// NodeProfileReport is one node's evaluation of one runtime profile.
type NodeProfileReport struct {
	Node        string    `cbor:"node"`
	Profile     string    `cbor:"profile"`
	Status      string    `cbor:"status"`
	Checks      []Finding `cbor:"checks,omitempty"`
	EvaluatedAt int64     `cbor:"evaluated_at"`
	Configured  string    `cbor:"configured,omitempty"`
	Online      bool      `cbor:"online,omitempty"`
}

// NodeProfileGetRes answers `node.profile.get`.
type NodeProfileGetRes struct {
	Profile string              `cbor:"profile,omitempty"`
	Nodes   []NodeProfileReport `cbor:"nodes,omitempty"`
}

// ---- the requirement rows a profile run adds ----------------------------

// ProfileRequirementID is the stable requirement id for one profile check.
// `profile.backend.enforced_egress` becomes `CONF-PROF-BACKEND-ENFORCED-EGRESS`.
func ProfileRequirementID(check string) string {
	name := strings.TrimPrefix(check, "profile.")
	name = strings.NewReplacer(".", "-", "_", "-").Replace(name)
	return "CONF-PROF-" + strings.ToUpper(name)
}

// ProfileSchedulingID is the requirement that scheduling honours the profile
// rather than merely reporting on it.
const ProfileSchedulingID = "CONF-PROF-SCHEDULING"

// profileSpec cites the normative sections for every profile row.
const profileSpec = "PROTOCOL.md §5 (requires.profile, pending_reason), §6 (node.profile.get)"

// profileRequirement builds the row for one obligation. The rows are required
// tier: a run that named a profile asked for a verdict about that profile,
// and a required row that could not be observed is not a pass.
func profileRequirement(profile, check string) Requirement {
	title := profileObligationText[check]
	if title == "" {
		title = "the target reports check " + check
	}
	return Requirement{
		ID: ProfileRequirementID(check), Version: ManifestVersion, Since: ManifestVersion,
		// §12.1 names nine semantic categories and the manifest must cover
		// every one of them, so a profile row joins the category its
		// obligation belongs to — placement is a §5 workspace concern —
		// rather than inventing a tenth that the standard manifest would
		// then fail to populate.
		Category: CategoryWorkspace, Tier: TierRequired,
		Title: fmt.Sprintf("under runtime profile %q, %s", profile, title),
		Spec:  profileSpec,
	}
}

// runProfile judges the target against one named runtime profile.
//
// Nothing here can vanish. If `node.profile.get` cannot be reached, every
// obligation of the profile is reported unavailable carrying the transport's
// own reason; if the fleet is empty, they are unavailable saying so. A check
// the target reported that this file does not know about is added as its own
// row rather than being discarded.
// profileEvidence is what the §6 answer establishes about the fleet, which is
// what makes the scheduling row readable. Without it a target that ignores
// `requires.profile` entirely would pass scheduling by claiming every
// workspace it is handed.
type profileEvidence struct {
	// Answered is false when the target could not answer node.profile.get at
	// all. Nothing about profiles is then observable on it.
	Answered bool
	// AnyFail is true when at least one obligation failed somewhere in the
	// fleet, so no node should be able to take profile-constrained work.
	AnyFail bool
	// Reason carries why the operation could not be answered.
	Reason string
}

func runProfile(ctx context.Context, s *Session, profile string, per time.Duration) ([]Result, profileEvidence) {
	started := time.Now().UTC()
	rows := ProfileObligations(profile)

	cctx, cancel := context.WithTimeout(ctx, per)
	defer cancel()
	var res NodeProfileGetRes
	err := s.Call(cctx, "node.profile.get", NodeProfileGetReq{Profile: profile}, &res)

	elapsed := time.Since(started).Milliseconds()
	unavailableAll := func(reason string) []Result {
		out := make([]Result, 0, len(rows)+1)
		for _, check := range rows {
			out = append(out, Result{
				Requirement: profileRequirement(profile, check),
				Status:      StatusUnavailable, Reason: reason,
				StartedAt: started, DurationMS: elapsed,
			})
		}
		return out
	}
	switch {
	case err != nil:
		reason := fmt.Sprintf("the target could not answer node.profile.get for profile %q: %v", profile, err)
		return unavailableAll(reason), profileEvidence{Reason: reason}
	case len(res.Nodes) == 0:
		reason := "the target reports no nodes, so no evidence for this profile exists; an unchecked fleet is not a passing fleet"
		return unavailableAll(reason), profileEvidence{Answered: true, AnyFail: true, Reason: reason}
	}
	out := foldProfileReports(profile, res.Nodes)
	evidence := profileEvidence{Answered: true}
	for i := range out {
		out[i].StartedAt, out[i].DurationMS = started, elapsed
		if out[i].Status != StatusPassed {
			evidence.AnyFail = true
		}
	}
	return out, evidence
}

// foldProfileReports turns one §6 answer into one row per obligation.
//
// The fold is fail-closed across the fleet: one node failing an obligation
// fails it for the deployment, because scheduling may put untrusted work on
// any node that satisfies the constraint and an operator asking "is this
// fleet fit" is not asking "is some machine in it fit". A check the target
// reported that this transcription does not know about keeps its own row, and
// an obligation no node answered is unavailable rather than absent.
func foldProfileReports(profile string, nodes []NodeProfileReport) []Result {
	rows := ProfileObligations(profile)
	type verdict struct {
		status  Status
		reasons []string
	}
	folded := map[string]*verdict{}
	order := append([]string(nil), rows...)
	seen := map[string]bool{}
	for _, check := range rows {
		folded[check] = &verdict{status: StatusPassed}
		seen[check] = true
	}
	for _, node := range nodes {
		reported := map[string]bool{}
		for _, f := range node.Checks {
			reported[f.Check] = true
			v := folded[f.Check]
			if v == nil {
				// A check the target reported that this transcription does
				// not know about. §6 permits it; dropping it would hide
				// evidence, so it gets its own row.
				v = &verdict{status: StatusPassed}
				folded[f.Check] = v
				if !seen[f.Check] {
					order = append(order, f.Check)
					seen[f.Check] = true
				}
			}
			status := f.Status
			if status == "" {
				status = FindingUnavailable
			}
			switch status {
			case FindingPass:
			case FindingFail:
				v.status = StatusFailed
				v.reasons = append(v.reasons, fmt.Sprintf("%s: %s", node.Node, findingText(f)))
			default:
				if v.status != StatusFailed {
					v.status = StatusUnavailable
				}
				v.reasons = append(v.reasons, fmt.Sprintf("%s: %s", node.Node, findingText(f)))
			}
		}
		for _, check := range rows {
			if reported[check] {
				continue
			}
			v := folded[check]
			if v.status != StatusFailed {
				v.status = StatusUnavailable
			}
			v.reasons = append(v.reasons, fmt.Sprintf("%s: reported no result for %s", node.Node, check))
		}
	}

	out := make([]Result, 0, len(order))
	for _, check := range order {
		v := folded[check]
		sort.Strings(v.reasons)
		out = append(out, Result{
			Requirement: profileRequirement(profile, check),
			Status:      v.status,
			Reason:      strings.Join(v.reasons, "; "),
		})
	}
	return out
}

func findingText(f Finding) string {
	text := f.Detail
	if text == "" {
		text = f.Check
	}
	if f.Subject != "" {
		text = f.Subject + " — " + text
	}
	if f.Hint != "" {
		text += " (hint: " + f.Hint + ")"
	}
	return text
}

// runProfileScheduling proves that the control plane's placement honours
// `requires.profile` rather than merely reporting on it: a workspace asking
// for the profile must actually be claimed. A workspace parked with
// `pending_reason: profile_unschedulable` is a failure of this deployment
// against the profile it was asked about, not an unobservable row — the
// observation succeeded and its answer was no.
func runProfileScheduling(ctx context.Context, s *Session, profile string, per time.Duration, evidence profileEvidence) Result {
	req := Requirement{
		ID: ProfileSchedulingID, Version: ManifestVersion, Since: ManifestVersion,
		Category: CategoryWorkspace, Tier: TierRequired,
		Title: fmt.Sprintf("a workspace requiring runtime profile %q is placed on a node that satisfies it", profile),
		Spec:  profileSpec,
	}
	res := Result{Requirement: req, StartedAt: time.Now().UTC()}
	defer func() { res.DurationMS = time.Since(res.StartedAt).Milliseconds() }()

	// A target that cannot say which nodes satisfy the profile cannot be
	// judged on whether it honoured it. Claiming the workspace would prove
	// only that the constraint was ignored, which is the reading this row
	// must not be allowed to record as a pass.
	if !evidence.Answered {
		res.Status = StatusUnavailable
		res.Reason = "the target answers no runtime-profile evaluation, so a placement decision is not evidence that requires.profile was honoured: " + evidence.Reason
		return res
	}

	cctx, cancel := context.WithTimeout(ctx, per)
	defer cancel()

	ws, err := s.CreateWorkspace(cctx, WorkspaceSpec{
		Name:     "conformance-profile",
		Requires: Requires{Profile: profile},
	})
	if err != nil {
		res.Status = StatusFailed
		res.Reason = fmt.Sprintf("ws.create with requires.profile=%q was refused: %v", profile, err)
		return res
	}
	deadline := time.Now().Add(45 * time.Second)
	var last Workspace
	for time.Now().Before(deadline) {
		var got Workspace
		if err := s.Call(cctx, "ws.get", WSGetReq{ID: ws.ID}, &got); err != nil {
			res.Status = StatusUnavailable
			res.Reason = "ws.get stopped answering while waiting for placement: " + err.Error()
			return res
		}
		last = got
		if got.State == WSClaimed && got.Node != "" {
			if evidence.AnyFail {
				// The fleet's own evidence says no node satisfies the
				// profile, and the control plane placed the workspace
				// anyway. That is the failure this row exists to catch:
				// requires.profile was accepted and not enforced.
				res.Status = StatusFailed
				res.Reason = fmt.Sprintf("the control plane placed the workspace on %s although node.profile.get reports that no node satisfies %q: requires.profile was accepted and not enforced", got.Node, profile)
				return res
			}
			res.Status = StatusPassed
			return res
		}
		if got.PendingReason != "" {
			res.Status = StatusFailed
			res.Reason = fmt.Sprintf("the workspace stayed pending with pending_reason %q: no node satisfies runtime profile %q", got.PendingReason, profile)
			return res
		}
		if got.State == WSFailed {
			res.Status = StatusFailed
			res.Reason = fmt.Sprintf("the workspace reached %q instead of claimed", got.State)
			return res
		}
		select {
		case <-cctx.Done():
			res.Status = StatusUnavailable
			res.Reason = "the run was cancelled while waiting for placement: " + cctx.Err().Error()
			return res
		case <-time.After(200 * time.Millisecond):
		}
	}
	res.Status = StatusFailed
	res.Reason = fmt.Sprintf("the workspace never became claimed (last state %q, pending_reason %q)", last.State, last.PendingReason)
	return res
}
