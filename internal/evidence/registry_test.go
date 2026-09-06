package evidence

import (
	"fmt"
	"slices"
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
	if len(have) != 58 {
		t.Fatalf("registry holds %d rows, want E1-E25 plus B1-B32 and B34", len(have))
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
	"E4":  "exact gVisor candidate denies every forbidden lane, revokes synchronously, and cleans up",
	"E5":  "two isolated tenants share a node with cross-tenant denials recorded as tenant-scoped events",
	"B1":  "real pinned OpenCode against the deterministic local model through the broker",
	"B2":  "ACP transcript carries the tool call and terminal result",
	"B3":  "cut mid-stream, replay from seq 0 is byte-identical",
	"B4":  "sleep, restore, and the prior conversation survives",
	"B5":  "the approval commits before the approved tool result exists anywhere",
	"B6":  "leak scan over every durable surface, each with a positive control",
	"B14": "cut mid-command: byte-identical reattach or an explicit gap",
	"B15": "node lost before commit: source retained and fenced",
	"B16": "node lost after commit: only the new generation is claimable",
	"B17": "control restart refuses stale grants, nodes and ready messages",
	"B18": "queue resumes without repeating a completed item",
	"B20": "the black-box manifest against a freshly built binary",
	"B21": "a deliberately broken shim is caught in each semantic category",
	"B22": "unknown extensions ignorable, unknown required capabilities fail",
	"B23": "a version mismatch fails before anything is mutated",
	"B24": "a relay capture carries no plaintext payload, with a positive control",
	"B25": "mutation, replay and cross-destination substitution are rejected",
	"B26": "reconnect rekeys without losing the cursor or idempotency",
	"B27": "required encryption refuses before any mutation",
	"B28": "the aggregate exact-candidate gVisor isolation and enforced-gateway host lane",
	"B29": "the aggregate exact-candidate Firecracker KVM lifecycle, isolation, failure and cleanup lane",
	"B30": "10,000 reconnecting cursors and the remaining bounded resource workloads settle within declared ceilings",
	"B31": "reproducible builds across a cold cache and a different TMPDIR",
	"B32": "clean installs of the binary, images, wheel, tarball and module, each with a source-tree control",
	"B34": "a real Chromium answers every computer operation on the docker backend",
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

func TestB32RunsOnlyItsOwningProofs(t *testing.T) {
	s, ok := ScenarioByID("B32")
	if !ok {
		t.Fatal("B32 not found")
	}
	want := []string{"go", "test", "-count=1", "-timeout=20m", "-run", "^TestB32", "./integration/installs/"}
	if !slices.Equal(s.Argv, want) {
		t.Fatalf("B32 command = %q, want %q", s.Argv, want)
	}
}

// TestAWiredScenarioIsOwnedAndKeyless pins what wiring is allowed to mean. A
// wired row runs on every candidate, so it may not depend on a credential.
// Non-secret host prerequisites are allowed because the runner reports their
// absence as unavailable before dispatching the owning command.
func TestAWiredScenarioIsOwnedAndKeyless(t *testing.T) {
	for _, s := range Scenarios() {
		if !s.Wired() {
			continue
		}
		if !s.Owned() {
			t.Fatalf("%s is wired but has no owning proof", s.ID)
		}
		if credentials := credentialEnv(s.Env); s.Required && len(credentials) != 0 {
			t.Fatalf("%s is a required wired row that names credential environment %v; a required row must run without credentials", s.ID, credentials)
		}
		if s.Layer == LayerLiveService {
			t.Fatalf("%s is wired at the live-service layer; those rows are optional supplements, not part of the keyless run", s.ID)
		}
	}
}

func TestCredentialEnvDistinguishesHostPrerequisites(t *testing.T) {
	got := credentialEnv([]string{
		"REMOUNT_GVISOR_INTEGRATION", "REMOUNT_GVISOR_ROOTFS", "REMOUNT_CHAOS_IMAGE",
		"E2B_API_KEY", "REMOUNT_ENROLL_TOKEN",
	})
	if !slices.Equal(got, []string{"E2B_API_KEY", "REMOUNT_ENROLL_TOKEN"}) {
		t.Fatalf("credential environment = %v", got)
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

func TestHostBoundRowsUseACompatibleEvidenceLayer(t *testing.T) {
	for id, probe := range hostProbeFor {
		s, ok := ScenarioByID(id)
		if !ok {
			t.Fatalf("%s is probed but not registered", id)
		}
		if probe == "docker" && s.Layer == LayerArtifact {
			continue
		}
		if s.Layer != LayerHostCI {
			t.Fatalf("%s is bound to the %s probe but has incompatible layer %s", id, probe, s.Layer)
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
