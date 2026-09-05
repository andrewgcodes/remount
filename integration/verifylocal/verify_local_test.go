// Package verifylocal proves scripts/verify-local.sh reports what it ran.
//
// The script is a portable local precursor to CI. The claim that matters is
// not "the script exited zero": it is that zero means every gate it names
// actually ran and passed, without claiming native macOS or Windows evidence.
// An analyzer that is not installed did not pass, a selected subset is not the
// complete local set, and a suite that skips the external
// integration/publicsdk module is not the `make test` gate CI runs
// (docs/engineering/review-handoff-2026-09-04.md, RMR-012).
//
// Every heavy tool the script invokes is replaced by a recording stub on a
// PATH that contains nothing else, so a run takes milliseconds, never touches
// the network, and cannot see a real staticcheck or govulncheck that happens
// to be installed on the host. The lock-discipline script is real; it is a
// POSIX shell script over the source tree and finishes in well under a second.
package verifylocal

import (
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

// hostTools are the programs the script and lint-locks.sh need from the host.
// They are linked into an otherwise empty PATH so the run is independent of
// whatever else the developer has installed, in particular the analyzers.
var hostTools = []string{"bash", "sh", "basename", "dirname", "date", "mktemp", "sed", "tail", "rm", "env", "find", "awk", "grep", "wc", "cat"}

func repoRoot(t *testing.T) string {
	t.Helper()
	if runtime.GOOS == "windows" {
		t.Skip("unavailable: verify-local.sh is a POSIX shell script; the Windows lane runs its gates natively")
	}
	out, err := exec.Command("git", "rev-parse", "--show-toplevel").Output()
	if err != nil {
		t.Skipf("unavailable: not a git checkout: %v", err)
	}
	return strings.TrimSpace(string(out))
}

// stubs is a fake PATH: recording `go`, `make` and `gofmt` plus whichever
// analyzers a test chooses to make available.
type stubs struct {
	dir string
	log string
}

func newStubs(t *testing.T, failing map[string]bool, analyzers ...string) stubs {
	t.Helper()
	dir := t.TempDir()
	log := filepath.Join(dir, "calls.log")
	for _, tool := range hostTools {
		path, err := exec.LookPath(tool)
		if err != nil {
			t.Skipf("unavailable: %s not on PATH: %v", tool, err)
		}
		if err := os.Symlink(path, filepath.Join(dir, tool)); err != nil {
			t.Fatal(err)
		}
	}
	for _, tool := range append([]string{"go", "make", "gofmt"}, analyzers...) {
		status := "0"
		if failing[tool] {
			status = "1"
		}
		// Record the working directory and arguments, because the publicsdk
		// gate is distinguished from the in-module suite only by where it runs.
		body := "#!/bin/sh\nprintf '%s|%s|%s\\n' \"$(basename \"$0\")\" \"$PWD\" \"$*\" >>\"" + log + "\"\nexit " + status + "\n"
		if err := os.WriteFile(filepath.Join(dir, tool), []byte(body), 0o755); err != nil {
			t.Fatal(err)
		}
	}
	return stubs{dir: dir, log: log}
}

// run executes the script with the stub PATH and returns its combined output,
// exit status and the recorded tool invocations.
func (s stubs) run(t *testing.T, root string, args ...string) (string, int, []string) {
	t.Helper()
	cmd := exec.Command(filepath.Join(root, "scripts", "verify-local.sh"), args...)
	cmd.Dir = root
	cmd.Env = []string{"PATH=" + s.dir, "HOME=" + t.TempDir(), "TMPDIR=" + t.TempDir(), "TERM=dumb"}
	out, err := cmd.CombinedOutput()
	code := 0
	if err != nil {
		exitErr, ok := err.(*exec.ExitError)
		if !ok {
			t.Fatalf("run verify-local.sh %v: %v\n%s", args, err, out)
		}
		code = exitErr.ExitCode()
	}
	var calls []string
	if raw, err := os.ReadFile(s.log); err == nil {
		calls = strings.Split(strings.TrimSpace(string(raw)), "\n")
	}
	return string(out), code, calls
}

func TestUnavailableAnalyzerIsNotAPass(t *testing.T) {
	root := repoRoot(t)
	s := newStubs(t, nil) // no staticcheck, no govulncheck
	out, code, _ := s.run(t, root, "static")
	if code == 0 {
		t.Errorf("exit 0 with staticcheck and govulncheck absent; the analyzers never ran\n%s", out)
	}
	if claimsFullPass(out) {
		t.Errorf("a run whose analyzers never executed was reported as a pass\n%s", out)
	}
	for _, tool := range []string{"staticcheck", "govulncheck"} {
		if !strings.Contains(out, tool) {
			t.Errorf("verdict does not name the unavailable analyzer %s\n%s", tool, out)
		}
	}
	if !strings.Contains(out, "incomplete") {
		t.Errorf("verdict does not say the verification is incomplete\n%s", out)
	}
}

func TestUnavailableIsDistinctFromFailed(t *testing.T) {
	root := repoRoot(t)
	// Control: the same gate set with every tool present and passing is a
	// pass, so the previous test is not passing because `static` is broken.
	ok := newStubs(t, nil, "staticcheck", "govulncheck")
	out, code, calls := ok.run(t, root, "static")
	if code != 0 {
		t.Fatalf("static gates with both analyzers installed exited %d\n%s", code, out)
	}
	for _, tool := range []string{"staticcheck", "govulncheck"} {
		if !containsCall(calls, tool+"|") {
			t.Errorf("%s was installed but never invoked\n%s", tool, strings.Join(calls, "\n"))
		}
	}

	// A failing analyzer is a failure, and it is reported as such rather than
	// as unavailable: the two verdicts route the developer to different work.
	bad := newStubs(t, map[string]bool{"staticcheck": true}, "staticcheck", "govulncheck")
	out, code, _ = bad.run(t, root, "static")
	if code != 1 {
		t.Errorf("a failing gate exited %d, want 1\n%s", code, out)
	}
	if !strings.Contains(out, "do not push") || strings.Contains(out, "incomplete") {
		t.Errorf("a failing gate was not reported as a failure\n%s", out)
	}
}

func TestVerdictNamesTheGateSet(t *testing.T) {
	root := repoRoot(t)
	s := newStubs(t, nil, "staticcheck", "govulncheck")

	out, code, _ := s.run(t, root, "gofmt", "vet")
	if code != 0 {
		t.Fatalf("selected gates exited %d\n%s", code, out)
	}
	if claimsFullPass(out) {
		t.Errorf("two selected gates were reported as all gates passed\n%s", out)
	}
	if !strings.Contains(out, "selected") || !strings.Contains(out, "gofmt") || !strings.Contains(out, "vet") {
		t.Errorf("verdict does not name the selected gates\n%s", out)
	}

	out, code, calls := s.run(t, root, "fast")
	if code != 0 {
		t.Fatalf("fast exited %d\n%s", code, out)
	}
	if claimsFullPass(out) {
		t.Errorf("the fast subset was reported as all gates passed\n%s", out)
	}
	if !strings.Contains(out, "fast") || !strings.Contains(out, "race") || !strings.Contains(out, "conformance") {
		t.Errorf("fast verdict does not name the set or what it left out\n%s", out)
	}
	if containsCall(calls, "go|"+root+"|test -race") || containsCall(calls, "make|"+root+"|conformance") {
		t.Errorf("fast ran the race lane or conformance\n%s", strings.Join(calls, "\n"))
	}

	out, code, calls = s.run(t, root)
	if code != 0 {
		t.Fatalf("full run exited %d\n%s", code, out)
	}
	if claimsFullPass(out) {
		t.Errorf("portable host checks were reported as complete CI verification\n%s", out)
	}
	if !strings.Contains(out, "portable local") ||
		!strings.Contains(out, "native macOS and Windows") ||
		!strings.Contains(out, "incomplete") {
		t.Errorf("broad local verdict does not name unavailable native CI lanes\n%s", out)
	}
	for _, want := range []string{"go|" + root + "|test -race", "make|" + root + "|conformance", "make|" + root + "|dist", "staticcheck|", "govulncheck|"} {
		if !containsCall(calls, want) {
			t.Errorf("full run did not invoke %q\n%s", want, strings.Join(calls, "\n"))
		}
	}
}

func TestSuiteGateRunsThePublicSDKModule(t *testing.T) {
	root := repoRoot(t)
	s := newStubs(t, nil, "staticcheck", "govulncheck")
	out, code, calls := s.run(t, root, "suite")
	if code != 0 {
		t.Fatalf("suite exited %d\n%s", code, out)
	}
	if !containsCall(calls, "go|"+root+"|test ") {
		t.Errorf("suite did not run the in-module tests\n%s", strings.Join(calls, "\n"))
	}
	sdk := filepath.Join(root, "integration", "publicsdk")
	if !containsCall(calls, "go|"+sdk+"|test ") {
		t.Errorf("suite claims parity with `make test` but never tested the external module %s\n%s", sdk, strings.Join(calls, "\n"))
	}
}

func TestInvalidArgumentsRunNothing(t *testing.T) {
	root := repoRoot(t)
	// A mistyped invocation must not degrade into a run that can say "safe
	// to push": `all typo` is not the full set, `fast vet` is not fast, and a
	// misspelled gate name is not the gate. Each is a usage error before any
	// gate executes.
	for _, args := range [][]string{{"all", "typo"}, {"fast", "vet"}, {"vet", "typo"}, {"typo"}} {
		s := newStubs(t, nil, "staticcheck", "govulncheck")
		out, code, calls := s.run(t, root, args...)
		if code != 64 {
			t.Errorf("%v exited %d, want 64 (usage)\n%s", args, code, out)
		}
		if claimsFullPass(out) {
			t.Errorf("%v was reported as a pass\n%s", args, out)
		}
		if len(calls) != 0 {
			t.Errorf("%v ran gates before rejecting the arguments:\n%s", args, strings.Join(calls, "\n"))
		}
	}
}

// claimsFullPass recognises the one verdict that licenses a push. Only a run
// of the complete gate set in which every gate executed and passed may say it.
func claimsFullPass(out string) bool {
	return strings.Contains(out, "safe to push") || strings.Contains(out, "all gates passed") || strings.Contains(out, "ci.yml gates passed")
}

func containsCall(calls []string, prefix string) bool {
	for _, c := range calls {
		if strings.HasPrefix(c, prefix) {
			return true
		}
	}
	return false
}
