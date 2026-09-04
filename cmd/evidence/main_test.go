package main

import (
	"encoding/json"
	"flag"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"remount.dev/remount/internal/evidence"
)

func TestParsePermutesFlagsAheadOfPositionals(t *testing.T) {
	fs := flag.NewFlagSet("validate", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	evergreen := fs.Bool("evergreen", false, "")
	if err := parse(fs, []string{"records.json", "--evergreen"}); err != nil {
		t.Fatal(err)
	}
	if !*evergreen {
		t.Fatal("a flag after a positional was dropped, exactly the bug the repo already paid for once")
	}
	if fs.NArg() != 1 || fs.Arg(0) != "records.json" {
		t.Fatalf("the positional was lost: %v", fs.Args())
	}
}

func TestSplitIgnoresEmptyEntries(t *testing.T) {
	if got := split(" B0.lint , ,B0.race "); len(got) != 2 || got[0] != "B0.lint" || got[1] != "B0.race" {
		t.Fatalf("got %v", got)
	}
	if got := split(""); got != nil {
		t.Fatalf("an empty selector must select everything, got %v", got)
	}
}

func record() evidence.Record {
	return evidence.Record{
		Scenario: "B3", Candidate: "bb0a6cfdeadbeef1234567", Layer: evidence.LayerSim,
		Status: evidence.StatusPassed, Command: "go test ./internal/sim",
		StartedAt:   time.Date(2026, 9, 3, 12, 0, 0, 0, time.UTC),
		Environment: evidence.Environment{OS: "linux", Arch: "amd64"},
		Cleanup:     evidence.CleanupNotRequired,
	}
}

func TestDecodeRecordsAcceptsTheDocumentAnArrayAndOneRecord(t *testing.T) {
	doc, err := json.Marshal(evidence.Result{Schema: evidence.Schema, Records: []evidence.Record{record(), record()}})
	if err != nil {
		t.Fatal(err)
	}
	array, err := json.Marshal([]evidence.Record{record()})
	if err != nil {
		t.Fatal(err)
	}
	one, err := json.Marshal(record())
	if err != nil {
		t.Fatal(err)
	}
	for name, pair := range map[string]struct {
		data []byte
		want int
	}{"document": {doc, 2}, "array": {array, 1}, "record": {one, 1}} {
		got, err := decodeRecords(pair.data)
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		if len(got) != pair.want {
			t.Fatalf("%s: got %d records, want %d", name, len(got), pair.want)
		}
	}
	if _, err := decodeRecords([]byte(`"nonsense"`)); err == nil {
		t.Fatal("a non-evidence document was accepted")
	}
}

func TestValidateRefusesAProductionLayerFile(t *testing.T) {
	rec := record()
	rec.Layer = "production"
	data, err := json.Marshal([]evidence.Record{rec})
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "records.json")
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatal(err)
	}
	err = validate([]string{path})
	if err == nil {
		t.Fatal("a production-layer record file was accepted")
	}
	if !strings.Contains(err.Error(), "operated deployment") {
		t.Fatalf("the refusal must say why, got %v", err)
	}
}

func TestValidateRefusesProviderIDsAsEvergreenProof(t *testing.T) {
	rec := record()
	rec.Ephemeral = true
	rec.ProviderIDs = []string{"isandbox-abc"}
	data, err := json.Marshal([]evidence.Record{rec})
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "records.json")
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := validate([]string{path}); err != nil {
		t.Fatalf("an ephemeral record is valid on its own: %v", err)
	}
	if err := validate([]string{path, "--evergreen"}); err == nil {
		t.Fatal("ephemeral provider evidence was accepted as evergreen proof")
	}
}

func TestListRendersWithoutReadingACredentialValue(t *testing.T) {
	t.Setenv("REMOUNT_INTEGRATION_OPENAI_KEY", "sk-planted-by-this-test-0000000000")
	var buf strings.Builder
	if err := list([]string{"--unowned", "--required"}, &buf); err != nil {
		t.Fatal(err)
	}
	out := buf.String()
	if strings.Contains(out, "sk-planted") {
		t.Fatal("the listing printed a credential value")
	}
	if !strings.Contains(out, "OPEN: Plan B phase") {
		t.Fatalf("the unowned required rows were not listed:\n%s", out)
	}
	for _, id := range []string{"E1", "E7"} {
		if strings.Contains(out, "\n"+id+" ") {
			t.Fatalf("%s is owned and not required-unowned; the filters do not compose", id)
		}
	}
}

func TestListJSONEnumeratesEveryScenario(t *testing.T) {
	var buf strings.Builder
	if err := list([]string{"--json"}, &buf); err != nil {
		t.Fatal(err)
	}
	var views []evidence.ScenarioView
	if err := json.Unmarshal([]byte(buf.String()), &views); err != nil {
		t.Fatal(err)
	}
	if len(views) != 57 {
		t.Fatalf("got %d rows, want E1-E25 plus B1-B32", len(views))
	}
	for _, v := range views {
		if v.Evidence == evidence.StatusPassed {
			t.Fatalf("%s claims evidence nothing in this run earned", v.ID)
		}
	}
}
