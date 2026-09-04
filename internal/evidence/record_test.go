package evidence

import (
	"strings"
	"testing"
	"time"
)

func validRecord() Record {
	return Record{
		Scenario: "B3", Candidate: "bb0a6cfdeadbeef1234567", Layer: LayerSim,
		Status: StatusPassed, Command: "go test ./internal/sim -run TestX",
		StartedAt: time.Date(2026, 9, 3, 12, 0, 0, 0, time.UTC), DurationMS: 1234,
		Environment: Environment{OS: "linux", Arch: "amd64", Backend: "docker", Provider: "fake-e2b"},
		Cleanup:     CleanupVerified, Artifacts: []string{"test-report.json"},
	}
}

func TestValidRecordPasses(t *testing.T) {
	if err := validRecord().Validate(); err != nil {
		t.Fatalf("the schema example must validate: %v", err)
	}
}

func TestProductionIsNotAnEvidenceLayer(t *testing.T) {
	rec := validRecord()
	rec.Layer = "production"
	err := rec.Validate()
	if err == nil {
		t.Fatal("a `production` record was accepted; production proof needs an operated deployment")
	}
	if !strings.Contains(err.Error(), "operated deployment") {
		t.Fatalf("the refusal must say why, got %v", err)
	}
	for _, layer := range Layers {
		if layer == "production" {
			t.Fatal("`production` must not be a registered layer")
		}
	}
}

func TestThereIsNoSkippedSuccessOutcome(t *testing.T) {
	rec := validRecord()
	rec.Status = "skipped-success"
	err := rec.Validate()
	if err == nil {
		t.Fatal("`skipped-success` was accepted")
	}
	if !strings.Contains(err.Error(), string(StatusUnavailable)) {
		t.Fatalf("the refusal must name the honest outcome, got %v", err)
	}
}

func TestANonPassMustNameItsReason(t *testing.T) {
	for _, status := range []Status{StatusFailed, StatusUnavailable} {
		rec := validRecord()
		rec.Status = status
		if err := rec.Validate(); err == nil {
			t.Fatalf("%s without a reason was accepted; an unexplained row is a hidden one", status)
		}
		rec.Reason = "docker daemon is unreachable"
		if err := rec.Validate(); err != nil {
			t.Fatalf("%s with a reason must validate: %v", status, err)
		}
	}
}

func TestCleanupFailureIsNeverAPass(t *testing.T) {
	rec := validRecord()
	rec.Cleanup = CleanupFailed
	if err := rec.Validate(); err == nil {
		t.Fatal("a scenario whose cleanup failed was allowed to pass")
	}
}

func TestRequiredEnvHoldsNamesNotValues(t *testing.T) {
	rec := validRecord()
	rec.RequiredEnv = []string{"E2B_API_KEY=e2b_0123456789abcdef0123"}
	if err := rec.Validate(); err == nil {
		t.Fatal("a value smuggled into required_env was accepted")
	}
	rec.RequiredEnv = []string{"E2B_API_KEY"}
	if err := rec.Validate(); err != nil {
		t.Fatalf("a bare variable name must validate: %v", err)
	}
}

func TestProviderObjectIDsAreEphemeralOnly(t *testing.T) {
	rec := validRecord()
	rec.ProviderIDs = []string{"isandbox-abc123"}
	if err := rec.Validate(); err == nil {
		t.Fatal("a provider object id was accepted on an evergreen record")
	}
	rec.Ephemeral = true
	if err := rec.Validate(); err != nil {
		t.Fatalf("an ephemeral record may name a destroyed object: %v", err)
	}
	if err := rec.ValidateEvergreen(); err == nil {
		t.Fatal("an ephemeral provider record was accepted as evergreen proof")
	}
}

func TestCandidateMustBeACommit(t *testing.T) {
	for _, candidate := range []string{"", "HEAD", "main", "zzzzzzz"} {
		rec := validRecord()
		rec.Candidate = candidate
		if err := rec.Validate(); err == nil {
			t.Fatalf("candidate %q was accepted", candidate)
		}
	}
}

func TestStartedAtMustBeUTC(t *testing.T) {
	rec := validRecord()
	rec.StartedAt = rec.StartedAt.In(time.FixedZone("PDT", -7*3600))
	if err := rec.Validate(); err == nil {
		t.Fatal("a non-UTC timestamp was accepted")
	}
}

func TestStringsCoversEveryStringField(t *testing.T) {
	rec := validRecord()
	rec.Reason = "reason-field"
	rec.Owner = "owner-field"
	rec.RequiredEnv = []string{"E2B_API_KEY"}
	rec.ProviderIDs = []string{"isandbox-1"}
	rec.Ephemeral = true
	joined := strings.Join(rec.Strings(), "\x00")
	for _, want := range []string{"B3", rec.Candidate, "sim", "passed", rec.Command, "linux", "amd64",
		"docker", "fake-e2b", "verified", "reason-field", "owner-field", "test-report.json",
		"E2B_API_KEY", "isandbox-1"} {
		if !strings.Contains(joined, want) {
			t.Fatalf("Strings() omits %q; a field the leak scan cannot see is a field that can carry a secret", want)
		}
	}
}
