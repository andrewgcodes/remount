// Package profile defines Remount's named runtime profiles: the fleet-level
// posture a node claims, the checks that prove it, and the evaluation the
// control plane runs against evidence it did not have to trust.
//
// A runtime profile is not the same thing as a workspace security profile
// (proto.SecuritySpec.Profile, "local"/"isolated"/"multi_tenant"). A workspace
// profile is a per-workspace placement contract enforced by
// proto.ValidateBackendSecurity. A runtime profile is a statement about the
// whole node: which backends it may register at all, and which host
// prerequisites must keep holding while it serves. The two compose; neither
// replaces the other, and nothing here weakens proto.ValidateBackendSecurity
// (ADR 0089).
//
// The evidence rule is the point of the package. Every predicate below reads
// proto.BackendDescriptor values, which a node derives mechanically from the
// Caps method of a backend it managed to construct. Findings a node reports
// about its own host are accepted only to *downgrade* a result: a node saying
// "pass" can never make a descriptor that fails a predicate satisfy it.
package profile

import (
	"fmt"
	"sort"
	"strings"

	"remount.dev/remount/internal/proto"
)

// Profile is a named runtime posture.
type Profile string

// The four named profiles, weakest first.
const (
	// Dev makes no security claim. Process and Docker backends are allowed.
	Dev Profile = "dev"
	// TrustedSingleTenant runs code the operator trusts not to be hostile,
	// but still isolates it from the host and keeps secrets in the broker.
	TrustedSingleTenant Profile = "trusted-single-tenant"
	// MultiTenantIsolated runs mutually untrusted workloads: deny-first
	// enforced egress, sibling isolation, a private network namespace, and no
	// shared-kernel backend registered at all.
	MultiTenantIsolated Profile = "multi-tenant-isolated"
	// MicroVM is MultiTenantIsolated restricted to verified microVM backends
	// with a host compatibility check.
	MicroVM Profile = "microvm"
)

// Check names. They are stable identifiers; a caller matches on these, never
// on Detail text.
const (
	CheckBackendRegistered      = "profile.backend.registered"
	CheckBackendIsolated        = "profile.backend.isolated"
	CheckBackendBrokeredSecrets = "profile.backend.brokered_secrets"
	CheckBackendUntrustedAbsent = "profile.backend.untrusted_absent"
	CheckBackendSiblingIsolated = "profile.backend.sibling_isolation"
	CheckBackendEnforcedEgress  = "profile.backend.enforced_egress"
	CheckBackendNetworkNS       = "profile.backend.network_namespace"
	CheckBackendStrongIsolation = "profile.backend.container_or_microvm"
	CheckBackendMicroVM         = "profile.backend.microvm"
	CheckBackendMultiTenant     = "profile.backend.multi_tenant"
	CheckBackendDeviceIsolation = "profile.backend.device_isolation"
	CheckRuntimeHealthy         = "profile.runtime.healthy"
	CheckHostMicroVMCompat      = "profile.host.microvm_compat"
)

// untrustedBackends are the shared-kernel development backends. They are
// refused by name as well as by predicate: "docker" satisfies neither sibling
// isolation nor enforced egress today, but naming it keeps the refusal
// legible if a future Caps change made the predicates pass without the
// boundary actually existing.
var untrustedBackends = []string{"process", "docker"}

// All lists the profiles weakest first.
func All() []Profile {
	return []Profile{Dev, TrustedSingleTenant, MultiTenantIsolated, MicroVM}
}

// Parse maps a configured string to a Profile. An empty string is Dev, which
// is the only default that claims nothing.
func Parse(s string) (Profile, error) {
	switch p := Profile(strings.TrimSpace(s)); p {
	case "":
		return Dev, nil
	case Dev, TrustedSingleTenant, MultiTenantIsolated, MicroVM:
		return p, nil
	default:
		return "", fmt.Errorf("unknown runtime profile %q; known profiles are %s", s, strings.Join(Names(), ", "))
	}
}

// Names lists the profile names weakest first.
func Names() []string {
	out := make([]string, 0, len(All()))
	for _, p := range All() {
		out = append(out, string(p))
	}
	return out
}

// Rank orders profiles weakest first; an unknown profile ranks below every
// known one so it can never satisfy a requirement by accident.
func (p Profile) Rank() int {
	for i, known := range All() {
		if known == p {
			return i
		}
	}
	return -1
}

// MakesSecurityClaim reports whether the profile promises an enforcement
// boundary. Dev does not, which is why drift never makes a Dev node
// unschedulable.
func (p Profile) MakesSecurityClaim() bool { return p != Dev && p.Rank() > 0 }

// Requirement is one named prerequisite of a profile.
type Requirement struct {
	Name        string `json:"name"`
	Description string `json:"description"`
}

var requirementText = map[string]string{
	CheckBackendRegistered:      "at least one workspace backend is registered",
	CheckBackendIsolated:        "every registered backend isolates the workspace from the host (isolation is not none)",
	CheckBackendBrokeredSecrets: "every registered backend authenticates to the broker (broker_identity is not none)",
	CheckBackendUntrustedAbsent: "no shared-kernel development backend (process, docker) is registered",
	CheckBackendSiblingIsolated: "every registered backend isolates one workspace from another",
	CheckBackendEnforcedEgress:  "every registered backend forces egress through the broker gateway",
	CheckBackendNetworkNS:       "every registered backend gives the workspace its own network namespace",
	CheckBackendStrongIsolation: "every registered backend is a container or a microVM",
	CheckBackendMicroVM:         "every registered backend is a microVM",
	CheckBackendMultiTenant:     "every registered backend is approved for multi-tenant execution",
	CheckBackendDeviceIsolation: "every registered backend isolates host devices",
	CheckRuntimeHealthy:         "every host runtime check this node reports passes",
	CheckHostMicroVMCompat:      "the host reports Firecracker microVM compatibility",
}

// Requirements lists, in evaluation order, what a profile demands.
func Requirements(p Profile) []Requirement {
	var names []string
	switch p {
	case Dev:
		names = []string{CheckBackendRegistered}
	case TrustedSingleTenant:
		names = []string{CheckBackendRegistered, CheckBackendIsolated, CheckBackendBrokeredSecrets}
	case MultiTenantIsolated:
		names = []string{
			CheckBackendRegistered, CheckBackendIsolated, CheckBackendBrokeredSecrets,
			CheckBackendUntrustedAbsent, CheckBackendSiblingIsolated, CheckBackendEnforcedEgress,
			CheckBackendNetworkNS, CheckBackendStrongIsolation, CheckRuntimeHealthy,
		}
	case MicroVM:
		names = []string{
			CheckBackendRegistered, CheckBackendIsolated, CheckBackendBrokeredSecrets,
			CheckBackendUntrustedAbsent, CheckBackendSiblingIsolated, CheckBackendEnforcedEgress,
			CheckBackendNetworkNS, CheckBackendStrongIsolation, CheckBackendMicroVM,
			CheckBackendMultiTenant, CheckBackendDeviceIsolation, CheckRuntimeHealthy,
			CheckHostMicroVMCompat,
		}
	default:
		return nil
	}
	out := make([]Requirement, 0, len(names))
	for _, name := range names {
		out = append(out, Requirement{Name: name, Description: requirementText[name]})
	}
	return out
}

// Report is one evaluation of one profile against one node's evidence.
type Report struct {
	Profile string          `json:"profile"`
	Status  string          `json:"status"`
	Checks  []proto.Finding `json:"checks"`
}

// OK reports whether every check passed.
func (r Report) OK() bool { return r.Status == proto.CheckPass }

// Failed lists the names of every check that did not pass, in report order.
func (r Report) Failed() []string {
	var out []string
	for _, c := range r.Checks {
		if c.Status != proto.CheckPass {
			out = append(out, c.Check)
		}
	}
	return out
}

// Evaluate decides whether descriptors plus the node's own runtime findings
// satisfy p.
//
// descriptors is the authoritative evidence: it is derived from the Caps of
// backends the node actually constructed, and a backend whose constructor
// failed is never in it. runtimeChecks are the node's self-reported host
// checks; they can only add a failing or unavailable result. Nothing in
// runtimeChecks can turn a failing descriptor predicate into a pass.
//
// An unavailable check is never a pass, so a Report whose status is
// unavailable must be treated exactly as unschedulable for that profile.
func Evaluate(p Profile, descriptors []proto.BackendDescriptor, runtimeChecks []proto.Finding) Report {
	rep := Report{Profile: string(p), Status: proto.CheckPass}
	if p.Rank() < 0 {
		rep.Status = proto.CheckFail
		rep.Checks = append(rep.Checks, finding(proto.CheckFail, CheckBackendRegistered, "",
			fmt.Sprintf("unknown runtime profile %q", string(p)),
			"configure one of "+strings.Join(Names(), ", ")))
		return rep
	}
	for _, req := range Requirements(p) {
		rep.Checks = append(rep.Checks, evaluateCheck(req.Name, descriptors, runtimeChecks))
	}
	for _, c := range rep.Checks {
		switch c.Status {
		case proto.CheckFail:
			rep.Status = proto.CheckFail
		case proto.CheckUnavailable:
			if rep.Status != proto.CheckFail {
				rep.Status = proto.CheckUnavailable
			}
		}
	}
	return rep
}

// Satisfied lists every profile the evidence satisfies, weakest first. It is
// the set the control plane schedules against.
func Satisfied(descriptors []proto.BackendDescriptor, runtimeChecks []proto.Finding) []Profile {
	var out []Profile
	for _, p := range All() {
		if Evaluate(p, descriptors, runtimeChecks).OK() {
			out = append(out, p)
		}
	}
	return out
}

func evaluateCheck(name string, descriptors []proto.BackendDescriptor, runtimeChecks []proto.Finding) proto.Finding {
	switch name {
	case CheckBackendRegistered:
		if len(descriptors) == 0 {
			return finding(proto.CheckFail, name, "", "no workspace backend is registered on this node",
				"start the node with --backend naming at least one backend")
		}
		return finding(proto.CheckPass, name, "",
			fmt.Sprintf("%d backend(s) registered: %s", len(descriptors), backendNames(descriptors)), "")
	case CheckRuntimeHealthy:
		return runtimeHealth(runtimeChecks)
	case CheckHostMicroVMCompat:
		return namedRuntimeCheck(name, runtimeChecks,
			"no host compatibility result was reported for this node",
			"register a verified firecracker backend so the node can report host compatibility")
	}
	if len(descriptors) == 0 {
		return finding(proto.CheckFail, name, "", "no workspace backend is registered on this node",
			"start the node with --backend naming at least one backend")
	}
	var offenders []string
	for _, d := range descriptors {
		if !backendSatisfies(name, d) {
			offenders = append(offenders, d.Name)
		}
	}
	if len(offenders) > 0 {
		sort.Strings(offenders)
		return finding(proto.CheckFail, name, strings.Join(offenders, ","),
			requirementText[name]+"; observed "+describeOffenders(name, descriptors, offenders),
			backendHint(name))
	}
	return finding(proto.CheckPass, name, backendNames(descriptors), requirementText[name], "")
}

// backendSatisfies is the whole predicate surface, one backend at a time.
// Each arm names the proto.BackendSecurityCaps field it reads; those fields
// are filled by workspace.Registry.Descriptors from the backend's own Caps.
func backendSatisfies(name string, d proto.BackendDescriptor) bool {
	s := d.Security
	switch name {
	case CheckBackendIsolated:
		return proto.IsolationRank(s.Isolation) > proto.IsolationRank("none")
	case CheckBackendBrokeredSecrets:
		return brokered(s.BrokerIdentity)
	case CheckBackendUntrustedAbsent:
		for _, weak := range untrustedBackends {
			if d.Name == weak {
				return false
			}
		}
		return true
	case CheckBackendSiblingIsolated:
		return s.SiblingIsolation
	case CheckBackendEnforcedEgress:
		return s.EgressMode == "enforced_gateway"
	case CheckBackendNetworkNS:
		return s.NetworkNamespace
	case CheckBackendStrongIsolation:
		return proto.IsolationRank(s.Isolation) >= proto.IsolationRank("container")
	case CheckBackendMicroVM:
		return s.Isolation == "microvm"
	case CheckBackendMultiTenant:
		return s.MultiTenant
	case CheckBackendDeviceIsolation:
		return s.DeviceIsolation
	default:
		return false
	}
}

// brokered reports whether a backend authenticates to the broker at all. An
// empty value is a backend that said nothing, which is not evidence.
func brokered(identity string) bool { return identity != "" && identity != "none" }

// runtimeHealth folds the node's self-reported host checks. A node's "pass"
// is not evidence for anything except that this fold found nothing wrong; a
// fail or unavailable is taken at face value because it can only downgrade.
func runtimeHealth(runtimeChecks []proto.Finding) proto.Finding {
	var failed, unavailable []string
	for _, c := range runtimeChecks {
		switch c.Status {
		case proto.CheckPass:
		case proto.CheckFail:
			failed = append(failed, c.Check)
		default:
			// An unstated status is a check whose result nobody recorded.
			// Treat it exactly like a check that could not run.
			unavailable = append(unavailable, c.Check)
		}
	}
	switch {
	case len(failed) > 0:
		sort.Strings(failed)
		return finding(proto.CheckFail, CheckRuntimeHealthy, strings.Join(failed, ","),
			"host runtime checks are failing: "+strings.Join(failed, ", "),
			"run `remount doctor --profile` against this node and repair the named prerequisite")
	case len(unavailable) > 0:
		sort.Strings(unavailable)
		return finding(proto.CheckUnavailable, CheckRuntimeHealthy, strings.Join(unavailable, ","),
			"host runtime checks could not run: "+strings.Join(unavailable, ", "),
			"an unavailable check is not a passing check; the node stays unschedulable for this profile")
	default:
		return finding(proto.CheckPass, CheckRuntimeHealthy, "",
			fmt.Sprintf("%d host runtime check(s) reported and none failed", len(runtimeChecks)), "")
	}
}

// namedRuntimeCheck requires one specific host check to be present and
// passing. Absence is unavailable, never pass: the control plane has no
// second source for a host property.
func namedRuntimeCheck(name string, runtimeChecks []proto.Finding, missingDetail, missingHint string) proto.Finding {
	for _, c := range runtimeChecks {
		if c.Check != name {
			continue
		}
		status := c.Status
		if status == "" {
			status = proto.CheckUnavailable
		}
		detail := c.Detail
		if detail == "" {
			detail = requirementText[name]
		}
		return finding(status, name, c.Subject, detail, c.Hint)
	}
	return finding(proto.CheckUnavailable, name, "", missingDetail, missingHint)
}

func describeOffenders(name string, descriptors []proto.BackendDescriptor, offenders []string) string {
	var parts []string
	for _, d := range descriptors {
		for _, o := range offenders {
			if d.Name != o {
				continue
			}
			parts = append(parts, d.Name+" "+observed(name, d))
		}
	}
	return strings.Join(parts, "; ")
}

// observed renders the exact field value the predicate rejected, so an
// operator reads the evidence rather than the rule.
func observed(name string, d proto.BackendDescriptor) string {
	s := d.Security
	switch name {
	case CheckBackendIsolated, CheckBackendStrongIsolation, CheckBackendMicroVM:
		return fmt.Sprintf("isolation=%s", orNone(s.Isolation))
	case CheckBackendBrokeredSecrets:
		return fmt.Sprintf("broker_identity=%s", orNone(s.BrokerIdentity))
	case CheckBackendUntrustedAbsent:
		return "is a shared-kernel development backend"
	case CheckBackendSiblingIsolated:
		return fmt.Sprintf("sibling_isolation=%t", s.SiblingIsolation)
	case CheckBackendEnforcedEgress:
		return fmt.Sprintf("egress_mode=%s", orNone(s.EgressMode))
	case CheckBackendNetworkNS:
		return fmt.Sprintf("network_namespace=%t", s.NetworkNamespace)
	case CheckBackendMultiTenant:
		return fmt.Sprintf("multi_tenant=%t", s.MultiTenant)
	case CheckBackendDeviceIsolation:
		return fmt.Sprintf("device_isolation=%t", s.DeviceIsolation)
	default:
		return ""
	}
}

func backendHint(name string) string {
	if name == CheckBackendUntrustedAbsent {
		return "start the node without process/docker, or lower --profile"
	}
	return "register only backends whose verified capabilities satisfy this profile, or lower --profile"
}

func orNone(s string) string {
	if s == "" {
		return "none"
	}
	return s
}

func backendNames(descriptors []proto.BackendDescriptor) string {
	names := make([]string, 0, len(descriptors))
	for _, d := range descriptors {
		names = append(names, d.Name)
	}
	sort.Strings(names)
	return strings.Join(names, ",")
}

// finding builds one check result. Severity carries the same verdict for
// readers that only understand severities: an unavailable check is a warning,
// never an info.
func finding(status, check, subject, detail, hint string) proto.Finding {
	severity := "info"
	switch status {
	case proto.CheckFail:
		severity = "error"
	case proto.CheckUnavailable:
		severity = "warn"
	}
	return proto.Finding{Severity: severity, Status: status, Check: check, Subject: subject, Detail: detail, Hint: hint}
}
