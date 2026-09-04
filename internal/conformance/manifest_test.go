package conformance

import (
	"context"
	"strings"
	"testing"
)

func TestStandardManifestValidates(t *testing.T) {
	if err := Standard().Validate(); err != nil {
		t.Fatalf("the published manifest does not satisfy its own invariants: %v", err)
	}
}

func TestManifestCoversEverySemanticCategory(t *testing.T) {
	grouped := Standard().ByCategory()
	for _, c := range Categories {
		if len(grouped[c]) == 0 {
			t.Errorf("category %q has no requirements", c)
		}
	}
	if len(grouped) != len(Categories) {
		t.Errorf("the manifest declares %d categories, §12.1 names %d", len(grouped), len(Categories))
	}
}

func TestEveryRequirementHasACheckAndEveryCheckHasARequirement(t *testing.T) {
	m := Standard()
	declared := map[string]bool{}
	for _, r := range m.Requirements {
		declared[r.ID] = true
		if _, ok := checks[r.ID]; !ok {
			t.Errorf("%s is declared but nothing asserts it", r.ID)
		}
	}
	for _, id := range CheckIDs() {
		if !declared[id] {
			t.Errorf("check %s asserts a requirement the manifest does not declare", id)
		}
	}
}

func TestEveryRequirementCitesTheSpec(t *testing.T) {
	for _, r := range Standard().Requirements {
		if !strings.Contains(r.Spec, "PROTOCOL.md") {
			t.Errorf("%s cites %q, which does not name the normative document", r.ID, r.Spec)
		}
	}
}

// A required requirement that names a gate is a contradiction: it would be
// unobservable on a target lacking the gate, and §12.5 already has a tier for
// that. Validate must refuse it rather than let a report silently skip a
// required row.
func TestRequiredRequirementMayNotBeGated(t *testing.T) {
	m := Standard()
	m.Requirements = append(m.Requirements, Requirement{
		ID: "CONF-NEG-001", Version: v1, Since: v1,
		Category: CategoryNegotiation, Tier: TierRequired,
		Title: "a required row that names a prerequisite", Spec: "PROTOCOL.md §3",
		Prereqs: []Prerequisite{PrereqBindings},
	})
	if err := m.Validate(); err == nil {
		t.Fatal("Validate accepted a required requirement that names a prerequisite")
	}
}

func TestCapabilityGatedRequirementMustNameItsGate(t *testing.T) {
	m := Standard()
	m.Requirements = append(m.Requirements, Requirement{
		ID: "CONF-TEST-GATE", Version: v1, Since: v1,
		Category: CategoryBinding, Tier: TierCapabilityGated,
		Title: "gated on nothing at all", Spec: "PROTOCOL.md §9",
	})
	checks["CONF-TEST-GATE"] = func(context.Context, *Session) error { return nil }
	defer delete(checks, "CONF-TEST-GATE")
	if err := m.Validate(); err == nil {
		t.Fatal("Validate accepted a capability-gated requirement that names no capability or prerequisite")
	}
}

func TestUnknownCapabilityGateIsRefused(t *testing.T) {
	m := Standard()
	m.Requirements = append(m.Requirements, Requirement{
		ID: "CONF-TEST-CAP", Version: v1, Since: v1,
		Category: CategoryBinding, Tier: TierCapabilityGated,
		Title: "gated on a capability nobody defines", Spec: "PROTOCOL.md §9",
		Capability: "x-not-a-capability",
	})
	checks["CONF-TEST-CAP"] = func(context.Context, *Session) error { return nil }
	defer delete(checks, "CONF-TEST-CAP")
	if err := m.Validate(); err == nil {
		t.Fatal("Validate accepted a gate on an identifier §3.1 does not define")
	}
}

func TestManifestCountsAreStable(t *testing.T) {
	m := Standard()
	counts := m.Counts()
	total := 0
	for _, t := range Tiers {
		total += counts[t]
	}
	if total != len(m.Requirements) {
		t.Fatalf("tier counts sum to %d, manifest holds %d requirements", total, len(m.Requirements))
	}
	if counts[TierRequired] == 0 {
		t.Fatal("the manifest has no required tier, so conformance would mean nothing")
	}
}

func TestDuplicateRequirementIDIsRefused(t *testing.T) {
	m := Standard()
	m.Requirements = append(m.Requirements, m.Requirements[0])
	if err := m.Validate(); err == nil {
		t.Fatal("Validate accepted a duplicate requirement id")
	}
}
