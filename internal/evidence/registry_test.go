package evidence

import (
	"fmt"
	"strings"
	"testing"
)

func TestRegistryValidates(t *testing.T) {
	if err := ValidateRegistry(); err != nil {
		t.Fatal(err)
	}
}

func TestRegistryCoversEveryHandoffAndPlanBScenario(t *testing.T) {
	have := map[string]Scenario{}
	for _, s := range Scenarios() {
		have[s.ID] = s
	}
	for i := 1; i <= 25; i++ {
		id := fmt.Sprintf("E%d", i)
		if _, ok := have[id]; !ok {
			t.Errorf("%s from the 2026-09-03 handoff is missing", id)
		}
	}
	for i := 1; i <= 32; i++ {
		id := fmt.Sprintf("B%d", i)
		if _, ok := have[id]; !ok {
			t.Errorf("%s from the Plan B phase tables is missing", id)
		}
	}
	if len(have) != 57 {
		t.Fatalf("registry holds %d rows, want E1-E25 plus B1-B32", len(have))
	}
}

func TestAnUnownedScenarioIsOpenNotAbsent(t *testing.T) {
	for _, s := range Scenarios() {
		if s.Owned() == (s.Open != "") {
			t.Fatalf("%s: a row must be owned or explicitly open, never both and never neither", s.ID)
		}
		if !s.Owned() && !strings.Contains(s.Open, "§") && !strings.Contains(s.Open, "decision") {
			t.Fatalf("%s: an open row must say what will close it, got %q", s.ID, s.Open)
		}
	}
}

// wiredScenarios is the explicit list of rows the aggregate runner executes.
// Wiring a scenario means the runner will re-earn it on every candidate, so
// adding an id here is a deliberate claim that the named proof exists, runs
// without credentials, and actually asserts the row's stated property. This
// test exists so that claim cannot be made by accident.
var wiredScenarios = map[string]string{
	"B1":  "real pinned OpenCode against the deterministic local model through the broker",
	"B2":  "ACP transcript carries the tool call and terminal result",
	"B3":  "cut mid-stream, replay from seq 0 is byte-identical",
	"B4":  "sleep, restore, and the prior conversation survives",
	"B5":  "the approval commits before the approved tool result exists anywhere",
	"B6":  "leak scan over every durable surface, each with a positive control",
	"B8":  "internal/provision/e2b contract tests against fixtures captured from the real api.e2b.app",
	"B9":  "ambiguous-create recovery, including the unresolvable case that must stay unknown",
	"B10": "provider stall proves control authority is not held across a provider call",
}

func TestOnlyDeliberatelyWiredScenariosAreWired(t *testing.T) {
	for _, s := range Scenarios() {
		reason, expected := wiredScenarios[s.ID]
		switch {
		case s.Wired() && !expected:
			t.Fatalf("%s is wired but not listed in wiredScenarios; wiring must be a deliberate act", s.ID)
		case !s.Wired() && expected:
			t.Fatalf("%s is listed as wired (%s) but carries no Argv", s.ID, reason)
		}
	}
}

// TestAWiredScenarioIsOwnedAndKeyless pins what wiring is allowed to mean. A
// wired row runs on every candidate, so it may not depend on a credential:
// a required row that silently needs a key would report failed on a clean
// checkout instead of unavailable, which inverts the honesty rule.
func TestAWiredScenarioIsOwnedAndKeyless(t *testing.T) {
	for _, s := range Scenarios() {
		if !s.Wired() {
			continue
		}
		if !s.Owned() {
			t.Fatalf("%s is wired but has no owning proof", s.ID)
		}
		if s.Required && len(s.Env) != 0 {
			t.Fatalf("%s is a required wired row that names environment %v; a required row must run without credentials", s.ID, s.Env)
		}
		if s.Layer == LayerLiveService {
			t.Fatalf("%s is wired at the live-service layer; those rows are optional supplements, not part of the keyless run", s.ID)
		}
	}
}

func TestScenariosAreCopiesNotLiveRegistryRows(t *testing.T) {
	first := Scenarios()
	first[0].Title = "mutated"
	first[0].Env = append(first[0].Env, "MUTATED")
	if Scenarios()[0].Title == "mutated" {
		t.Fatal("Scenarios returned a live row")
	}
	for _, name := range Scenarios()[0].Env {
		if name == "MUTATED" {
			t.Fatal("Scenarios shared its env slice with the registry")
		}
	}
}

func TestGvisorAndFirecrackerRowsAreBoundToTheirHostProbe(t *testing.T) {
	for id, probe := range hostProbeFor {
		s, ok := ScenarioByID(id)
		if !ok {
			t.Fatalf("%s is probed but not registered", id)
		}
		if s.Layer != LayerHostCI {
			t.Fatalf("%s is bound to the %s probe but is not a host-ci row", id, probe)
		}
	}
}

func TestScenarioByIDIsCaseInsensitive(t *testing.T) {
	if _, ok := ScenarioByID("b28"); !ok {
		t.Fatal("B28 not found")
	}
	if _, ok := ScenarioByID("B99"); ok {
		t.Fatal("an unregistered id was resolved")
	}
}
