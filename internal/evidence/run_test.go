package evidence

import (
	"context"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

// newRepo builds a throwaway candidate so a runner test never depends on the
// state of the checkout it runs in.
func newRepo(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	env := append(os.Environ(),
		"GIT_AUTHOR_NAME=t", "GIT_AUTHOR_EMAIL=t@example.invalid",
		"GIT_COMMITTER_NAME=t", "GIT_COMMITTER_EMAIL=t@example.invalid")
	for _, args := range [][]string{
		{"init", "--quiet"},
		{"commit", "--quiet", "--allow-empty", "-m", "candidate"},
	} {
		cmd := exec.Command("git", args...)
		cmd.Dir = dir
		cmd.Env = env
		if out, err := cmd.CombinedOutput(); err != nil {
			t.Fatalf("git %v: %v\n%s", args, err, out)
		}
	}
	return dir
}

// noGates selects a gate id that does not exist, so every gate reports as
// unselected instead of shelling out during a unit test.
var noGates = []string{"B0.none"}

// noScenarios selects a scenario id that does not exist, so no wired scenario
// executes its own proof inside a temporary repository that does not contain
// the tree those proofs need.
var noScenarios = []string{"B.none"}

func TestRunRefusesADirtyTreeUnlessDeveloperMode(t *testing.T) {
	dir := newRepo(t)
	if err := os.WriteFile(filepath.Join(dir, "stray.txt"), []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}
	out := t.TempDir()
	_, err := Run(context.Background(), Options{Root: dir, Out: out, Gates: noGates, Scenarios: noScenarios, Lookup: fakeEnv(nil)})
	if err == nil {
		t.Fatal("a dirty tree produced a report that names a commit it did not test")
	}
	if !strings.Contains(err.Error(), "developer mode") {
		t.Fatalf("the refusal must name the escape hatch, got %v", err)
	}
	if _, err := Run(context.Background(), Options{Root: dir, Out: out, Dev: true, Gates: noGates, Scenarios: noScenarios, Lookup: fakeEnv(nil)}); err != nil {
		t.Fatalf("developer mode must accept a dirty tree: %v", err)
	}
}

func TestRunReportsUnwiredScenariosAsUnavailableNeverPassed(t *testing.T) {
	dir := newRepo(t)
	out := t.TempDir()
	res, err := Run(context.Background(), Options{Root: dir, Out: out, Gates: noGates, Scenarios: noScenarios, Lookup: fakeEnv(nil)})
	if err != nil {
		t.Fatal(err)
	}
	if res.Verdict != VerdictIncomplete {
		t.Fatalf("verdict %s: a run that proved almost nothing must not read as complete", res.Verdict)
	}
	if res.Summary.RequiredUnavailable == 0 {
		t.Fatal("no required row was reported unavailable")
	}
	seen := map[string]Record{}
	for _, rec := range res.Records {
		if err := rec.Validate(); err != nil {
			t.Fatalf("the runner emitted an invalid record: %v", err)
		}
		seen[rec.Scenario] = rec
	}
	for _, id := range []string{"E1", "E9", "B1", "B32"} {
		rec, ok := seen[id]
		if !ok {
			t.Fatalf("%s is missing from the report", id)
		}
		if rec.Status != StatusUnavailable {
			t.Fatalf("%s reported %s without running", id, rec.Status)
		}
		if rec.Reason == "" {
			t.Fatalf("%s is unavailable without naming a prerequisite", id)
		}
	}
	if got := seen["B0.leak-canary"]; got.Status != StatusPassed || got.Cleanup != CleanupVerified {
		t.Fatalf("the leak canary must pass and verify its own cleanup, got %s/%s", got.Status, got.Cleanup)
	}
	if res.Summary.CleanupVerified == 0 {
		t.Fatal("no cleanup was verified, so the cleanup column proves nothing")
	}
}

func TestAnUnselectedGateIsUnavailableNotPassed(t *testing.T) {
	dir := newRepo(t)
	res, err := Run(context.Background(), Options{Root: dir, Out: t.TempDir(), Gates: noGates, Scenarios: noScenarios, Lookup: fakeEnv(nil)})
	if err != nil {
		t.Fatal(err)
	}
	for _, rec := range res.Records {
		if rec.Scenario != "B0.lint" {
			continue
		}
		if rec.Status != StatusUnavailable || !strings.Contains(rec.Reason, "not selected") {
			t.Fatalf("an unselected gate reported %s (%s)", rec.Status, rec.Reason)
		}
		return
	}
	t.Fatal("B0.lint is missing from the report")
}

func TestRunWritesAllThreeSummariesAndListsWhatItCouldNotProve(t *testing.T) {
	dir := newRepo(t)
	out := t.TempDir()
	res, err := Run(context.Background(), Options{Root: dir, Out: out, Gates: noGates, Scenarios: noScenarios, Lookup: fakeEnv(nil)})
	if err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"evidence.json", "junit.xml", "summary.md"} {
		if _, err := os.Stat(filepath.Join(out, name)); err != nil {
			t.Fatalf("%s was not written: %v", name, err)
		}
	}
	summary, err := os.ReadFile(filepath.Join(out, "summary.md"))
	if err != nil {
		t.Fatal(err)
	}
	text := string(summary)
	for _, want := range []string{"## Unavailable evidence", "not a completion gate", "**E1**", "**B1**", "Credential presence"} {
		if !strings.Contains(text, want) {
			t.Fatalf("the summary hides %q", want)
		}
	}
	var reloaded Result
	data, err := os.ReadFile(filepath.Join(out, "evidence.json"))
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(data, &reloaded); err != nil {
		t.Fatal(err)
	}
	if reloaded.Schema != Schema || len(reloaded.Records) != len(res.Records) {
		t.Fatalf("evidence.json does not round-trip: schema %d, %d records", reloaded.Schema, len(reloaded.Records))
	}
}

func TestCredentialsAreDetectedByPresenceOnly(t *testing.T) {
	dir := newRepo(t)
	out := t.TempDir()
	const value = "e2b_planted0123456789abcdef"
	res, err := Run(context.Background(), Options{Root: dir, Out: out, Gates: noGates, Scenarios: noScenarios,
		Lookup: fakeEnv(map[string]string{"E2B_API_KEY": value})})
	if err != nil {
		t.Fatal(err)
	}
	var found bool
	for _, c := range res.Credentials {
		if c.Name == "E2B_API_KEY" {
			found = true
			if !c.Present {
				t.Fatal("a present credential was reported absent")
			}
		}
	}
	if !found {
		t.Fatal("E2B_API_KEY was never probed")
	}
	for _, name := range []string{"evidence.json", "junit.xml", "summary.md"} {
		data, err := os.ReadFile(filepath.Join(out, name))
		if err != nil {
			t.Fatal(err)
		}
		if strings.Contains(string(data), value) {
			t.Fatalf("%s contains the credential value", name)
		}
	}
	if res.Verdict == VerdictFailed {
		t.Fatalf("presence detection must not fail the run: %v", res.Leaks)
	}
}

func TestALeakInTheEvidenceFailsTheRun(t *testing.T) {
	dir := newRepo(t)
	// The value is a string the report demonstrably contains, so this proves
	// the scan really reads the rendered evidence rather than an empty buffer.
	lookup := fakeEnv(map[string]string{"E2B_API_KEY": string(LayerLiveService)})
	res, err := Run(context.Background(), Options{Root: dir, Out: t.TempDir(), Gates: noGates, Scenarios: noScenarios, Lookup: lookup})
	if err != nil {
		t.Fatal(err)
	}
	if res.Verdict != VerdictFailed {
		t.Fatalf("a leaked value did not fail the run: %s", res.Verdict)
	}
	if !res.Failed(false) {
		t.Fatal("a failed verdict must exit non-zero")
	}
	if len(res.Leaks) == 0 {
		t.Fatal("the run failed without naming a leak")
	}
	for _, rec := range res.Records {
		if rec.Scenario == "B0.leak-canary" && rec.Status != StatusFailed {
			t.Fatalf("the canary row reported %s while the evidence leaked", rec.Status)
		}
	}
}

func TestRunExecutesASelectedGate(t *testing.T) {
	out := t.TempDir()
	// This test is about gate execution, so it selects no scenarios. Running
	// against the real root without that would execute every wired scenario,
	// including the docker-backed OpenCode lanes, and make a one-gate test
	// depend on a daemon and a package registry.
	res, err := Run(context.Background(), Options{Root: ".." + string(filepath.Separator) + "..",
		Out: out, Dev: true, Gates: []string{"B0.mod-verify"}, Scenarios: noScenarios, Lookup: fakeEnv(nil)})
	if err != nil {
		t.Fatal(err)
	}
	for _, rec := range res.Records {
		if rec.Scenario != "B0.mod-verify" {
			continue
		}
		if rec.Status != StatusPassed {
			t.Fatalf("go mod verify reported %s: %s", rec.Status, rec.Reason)
		}
		if len(rec.Artifacts) != 1 {
			t.Fatalf("a gate must keep its log, got %v", rec.Artifacts)
		}
		if _, err := os.Stat(filepath.Join(out, rec.Artifacts[0])); err != nil {
			t.Fatalf("the recorded log is absent: %v", err)
		}
		return
	}
	t.Fatal("B0.mod-verify is missing from the report")
}

func TestAGateLogNeverPersistsACredential(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "gate.log")
	const value = "planted-secret-value-0000"
	if err := writeLog(path, "harness said "+value+"\n", fakeEnv(map[string]string{"OPENAI_API_KEY": value})); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(data), value) {
		t.Fatal("the gate log persisted the credential it was scanning for")
	}
	if !strings.Contains(string(data), "redacted") {
		t.Fatalf("the log must say it was redacted, got %q", data)
	}
}

func TestHostProbesComeFromTheCheckedInGateScripts(t *testing.T) {
	dir := t.TempDir()
	write := func(rel, body string) {
		t.Helper()
		if err := os.MkdirAll(filepath.Join(dir, filepath.Dir(rel)), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(dir, rel), []byte(body), 0o755); err != nil {
			t.Fatal(err)
		}
	}
	write(backendGatesScript, "#!/bin/sh\nprintf 'docker status=available server_version=29.4.1\\n'\nprintf 'gvisor status=unavailable reason=runsc is not registered with Docker\\n'\nexit 4\n")
	write(firecrackerGateScript, "#!/bin/sh\necho 'UNAVAILABLE: /dev/kvm is not read-write' >&2\nexit 77\n")

	probes := ProbeBackends(context.Background(), dir)
	if len(probes) != 2 || probes[0].Name != "docker" || !probes[0].Available {
		t.Fatalf("docker verdict not carried through: %+v", probes)
	}
	if probes[1].Available || !strings.Contains(probes[1].Reason, "runsc is not registered") {
		t.Fatalf("the gvisor reason was lost: %+v", probes[1])
	}
	fc := ProbeFirecracker(context.Background(), dir)
	if fc.Available || fc.Reason != "/dev/kvm is not read-write" {
		t.Fatalf("firecracker verdict not carried through: %+v", fc)
	}
}

func TestAnAbsentGateScriptIsUnavailableNotAvailable(t *testing.T) {
	dir := t.TempDir()
	for _, p := range ProbeBackends(context.Background(), dir) {
		if p.Available {
			t.Fatalf("%s reported available with no gate script", p.Name)
		}
	}
	if ProbeFirecracker(context.Background(), dir).Available {
		t.Fatal("firecracker reported available with no gate script")
	}
}

func TestHostCIRowsCarryTheirProbeReason(t *testing.T) {
	probes := map[string]Probe{"gvisor": {Name: "gvisor", Script: backendGatesScript, Reason: "runsc is not registered with Docker"}}
	s, _ := ScenarioByID("E4")
	reason := scenarioReason(s, probes, fakeEnv(nil), nil)
	if !strings.Contains(reason, "runsc is not registered") {
		t.Fatalf("E4 must inherit the host gate's own reason, got %q", reason)
	}
}

// TestAWiredScenarioActuallyExecutesItsProof guards the gap that existed when
// scenario wiring was first added: Scenario.Argv was set and Wired() reported
// true, but the runner never dispatched it, so every wired row stayed
// unavailable forever. The mechanism existing is not the same as the mechanism
// running, and only executing one proves the difference.
func TestAWiredScenarioActuallyExecutesItsProof(t *testing.T) {
	dir := newRepo(t)
	marker := filepath.Join(t.TempDir(), "scenario-ran")
	wired := Scenario{
		ID: "B8", Title: "wired probe", Layer: LayerCode, Required: true,
		Owner: "probe", Argv: []string{"sh", "-c", "printf ran > " + marker},
	}
	res, err := Run(context.Background(), Options{
		Root: dir, Out: t.TempDir(), Gates: noGates, Lookup: fakeEnv(nil),
		Scenarios: []string{wired.ID}, overrideScenario: &wired,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, statErr := os.Stat(marker); statErr != nil {
		t.Fatalf("the wired scenario's command never ran: %v", statErr)
	}
	var found bool
	for _, rec := range res.Records {
		if rec.Scenario != wired.ID {
			continue
		}
		found = true
		if rec.Status != StatusPassed {
			t.Fatalf("a wired scenario whose command succeeded reported %s: %s", rec.Status, rec.Reason)
		}
		if rec.Command == wired.Owner {
			t.Fatal("the record still names the owner rather than the command that ran")
		}
	}
	if !found {
		t.Fatalf("%s is missing from the report", wired.ID)
	}
}

// TestAnUnselectedWiredScenarioIsUnavailableNotPassed keeps the selector on the
// same honesty terms as the gate list: leaving a row out of an invocation is a
// reason, never a silent omission and never a pass.
func TestAnUnselectedWiredScenarioIsUnavailableNotPassed(t *testing.T) {
	dir := newRepo(t)
	wired := Scenario{
		ID: "B8", Title: "wired probe", Layer: LayerCode, Required: true,
		Owner: "probe", Argv: []string{"sh", "-c", "exit 0"},
	}
	res, err := Run(context.Background(), Options{
		Root: dir, Out: t.TempDir(), Gates: noGates, Lookup: fakeEnv(nil),
		Scenarios: []string{"B.none"}, overrideScenario: &wired,
	})
	if err != nil {
		t.Fatal(err)
	}
	for _, rec := range res.Records {
		if rec.Scenario != wired.ID {
			continue
		}
		if rec.Status != StatusUnavailable {
			t.Fatalf("an unselected wired row reported %s, want unavailable", rec.Status)
		}
		if !strings.Contains(rec.Reason, "not selected") {
			t.Fatalf("an unselected row must say so, got %q", rec.Reason)
		}
		return
	}
	t.Fatalf("%s is missing from the report", wired.ID)
}
