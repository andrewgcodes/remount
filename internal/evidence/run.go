package evidence

import (
	"context"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"slices"
	"sort"
	"strings"
	"time"
)

// Schema is the version of the aggregate evidence document. Readers must refuse
// a document they do not understand rather than interpret it optimistically.
const Schema = 1

// Options configures one aggregate run.
type Options struct {
	Root string // repository root
	Out  string // directory for evidence.json, junit.xml and summary.md
	// Dev allows a dirty tree. The completion gate refuses one, because a
	// report that names a commit it did not test is worse than no report.
	Dev bool
	// Gates selects gate ids to run. Empty runs every gate; a gate left out is
	// unavailable with that reason, never omitted.
	Gates []string
	// Scenarios selects scenario ids to execute. Empty executes every wired
	// scenario; an unselected one is unavailable with that reason rather than
	// omitted, on the same terms as Gates.
	Scenarios []string
	// RequireComplete makes the run fail unless every required row passed. It
	// is the Plan B §19 completion gate; the default run is a status report.
	RequireComplete bool
	Lookup          Lookup
	// overrideScenario replaces the registry row with the same id for one run.
	// It exists so a test can execute a real command through the real dispatch
	// path without depending on a registry row that will change as tickets
	// land. Nothing outside this package can set it.
	overrideScenario *Scenario
	Stdout           io.Writer
	Now              func() time.Time
	// Timeout bounds one gate. Zero means no bound beyond the caller's context.
	Timeout time.Duration
}

// Credential records that a credential was looked for, and whether it was
// there. The value is never read into the report; presence is the whole
// observation this plan is allowed to make.
type Credential struct {
	Name    string `json:"name"`
	Present bool   `json:"present"`
}

// Summary counts the run so a reader sees the shape before the rows.
type Summary struct {
	Passed              int `json:"passed"`
	Failed              int `json:"failed"`
	Unavailable         int `json:"unavailable"`
	RequiredTotal       int `json:"required_total"`
	RequiredPassed      int `json:"required_passed"`
	RequiredFailed      int `json:"required_failed"`
	RequiredUnavailable int `json:"required_unavailable"`
	ScenariosRegistered int `json:"scenarios_registered"`
	ScenariosUnowned    int `json:"scenarios_unowned"`
	ExternalResources   int `json:"external_resources"`
	CleanupVerified     int `json:"cleanup_verified"`
	CleanupFailed       int `json:"cleanup_failed"`
}

// Verdict is the one-word answer. `complete` is reserved for a run in which
// every required row was earned in this run; a run that merely did not fail is
// `incomplete`.
type Verdict string

const (
	VerdictComplete   Verdict = "complete"
	VerdictIncomplete Verdict = "incomplete"
	VerdictFailed     Verdict = "failed"
)

// Result is the whole aggregate run.
type Result struct {
	Schema      int          `json:"schema"`
	Candidate   string       `json:"candidate"`
	Dirty       bool         `json:"dirty"`
	Developer   bool         `json:"developer_mode"`
	StartedAt   time.Time    `json:"started_at"`
	DurationMS  int64        `json:"duration_ms"`
	Environment Environment  `json:"environment"`
	Probes      []Probe      `json:"probes"`
	Credentials []Credential `json:"credentials"`
	Records     []Record     `json:"records"`
	Summary     Summary      `json:"summary"`
	Verdict     Verdict      `json:"verdict"`
	// Leaks is empty on every healthy run. A non-empty list is a failure even
	// when every gate passed.
	Leaks []Finding `json:"leaks,omitempty"`
}

// Run executes the aggregate completion gate and writes the evidence documents.
func Run(ctx context.Context, opts Options) (Result, error) {
	if opts.Lookup == nil {
		opts.Lookup = OSLookup
	}
	if opts.Now == nil {
		opts.Now = func() time.Time { return time.Now().UTC() }
	}
	if opts.Stdout == nil {
		opts.Stdout = io.Discard
	}
	if opts.Root == "" {
		opts.Root = "."
	}
	if err := ValidateRegistry(); err != nil {
		return Result{}, err
	}

	started := opts.Now()
	commit, dirty, err := candidate(ctx, opts.Root)
	if err != nil {
		return Result{}, err
	}
	if dirty && !opts.Dev {
		return Result{}, fmt.Errorf("evidence: the tree is dirty; run the completion gate on a committed candidate, or pass developer mode to accept an unrecorded one")
	}

	res := Result{
		Schema:      Schema,
		Candidate:   commit,
		Dirty:       dirty,
		Developer:   opts.Dev,
		StartedAt:   started,
		Environment: Environment{OS: runtime.GOOS, Arch: runtime.GOARCH},
	}
	res.Probes = append(ProbeBackends(ctx, opts.Root), ProbeFirecracker(ctx, opts.Root))
	res.Credentials = credentials(opts.Lookup)

	if err := os.MkdirAll(filepath.Join(opts.Out, "logs"), 0o755); err != nil {
		return Result{}, err
	}

	for _, gate := range Gates() {
		if gate.ID == "B0.leak-canary" {
			continue // it scans the other rows, so it runs last
		}
		res.Records = append(res.Records, runGate(ctx, opts, res, gate))
	}
	res.Records = append(res.Records, scenarioRecords(ctx, res, opts)...)

	canary, leaks := leakCanaryRecord(opts, res)
	res.Leaks = leaks
	res.Records = append(res.Records, canary)

	res.DurationMS = opts.Now().Sub(started).Milliseconds()
	res.Summary = summarize(res.Records)
	res.Verdict = verdict(res)

	if err := writeOutputs(opts.Out, res); err != nil {
		return res, err
	}
	fmt.Fprint(opts.Stdout, RenderMarkdown(res))
	return res, nil
}

// Failed reports whether the run must exit non-zero.
func (r Result) Failed(requireComplete bool) bool {
	if r.Verdict == VerdictFailed {
		return true
	}
	return requireComplete && r.Verdict != VerdictComplete
}

func candidate(ctx context.Context, root string) (string, bool, error) {
	commit, err := git(ctx, root, "rev-parse", "HEAD")
	if err != nil {
		return "", false, err
	}
	status, err := git(ctx, root, "status", "--porcelain")
	if err != nil {
		return "", false, err
	}
	return strings.TrimSpace(commit), strings.TrimSpace(status) != "", nil
}

func git(ctx context.Context, root string, args ...string) (string, error) {
	cmd := exec.CommandContext(ctx, "git", args...)
	cmd.Dir = root
	out, err := cmd.Output()
	if err != nil {
		return "", fmt.Errorf("evidence: git %s: %w", strings.Join(args, " "), err)
	}
	return string(out), nil
}

// credentials reports presence for every environment variable the registry
// names. It never reads a value into the result.
func credentials(lookup Lookup) []Credential {
	names := map[string]bool{}
	for _, s := range Scenarios() {
		for _, name := range credentialEnv(s.Env) {
			names[name] = true
		}
	}
	sorted := make([]string, 0, len(names))
	for name := range names {
		sorted = append(sorted, name)
	}
	sort.Strings(sorted)
	out := make([]Credential, 0, len(sorted))
	for _, name := range sorted {
		value, ok := lookup(name)
		out = append(out, Credential{Name: name, Present: ok && value != ""})
	}
	return out
}

func selected(ids []string, id string) bool {
	return len(ids) == 0 || slices.Contains(ids, id)
}

func runGate(ctx context.Context, opts Options, res Result, gate Gate) Record {
	rec := Record{
		Scenario: gate.ID, Candidate: res.Candidate, Layer: gate.Layer,
		Command: strings.Join(gate.Argv, " "), StartedAt: opts.Now(),
		Environment: res.Environment, Cleanup: CleanupNotRequired, Required: true,
	}
	if !selected(opts.Gates, gate.ID) {
		rec.Status = StatusUnavailable
		rec.Reason = "not selected by this invocation's gate list; the completion gate runs every gate"
		return rec
	}

	runCtx := ctx
	if opts.Timeout > 0 {
		var cancel context.CancelFunc
		runCtx, cancel = context.WithTimeout(ctx, opts.Timeout)
		defer cancel()
	}
	cmd := exec.CommandContext(runCtx, gate.Argv[0], gate.Argv[1:]...)
	cmd.Dir = opts.Root
	out, err := cmd.CombinedOutput()
	rec.DurationMS = opts.Now().Sub(rec.StartedAt).Milliseconds()

	log := filepath.Join("logs", gate.ID+".log")
	if writeErr := writeLog(filepath.Join(opts.Out, log), string(out), opts.Lookup); writeErr == nil {
		rec.Artifacts = []string{log}
	}
	if err != nil {
		rec.Status = StatusFailed
		rec.Reason = fmt.Sprintf("%s: %v (see %s)", strings.Join(gate.Argv, " "), err, log)
		return rec
	}
	rec.Status = StatusPassed
	return rec
}

// writeLog refuses to persist a credential. A gate log that would carry one is
// replaced by a count, because the report must not become the leak.
func writeLog(path, text string, lookup Lookup) error {
	if findings := scanLog(text, lookup); len(findings) > 0 {
		text = fmt.Sprintf("redacted: the leak scan found %d credential-shaped strings in this gate's output\n", len(findings))
		for _, f := range findings {
			text += f.String() + "\n"
		}
	}
	return os.WriteFile(path, []byte(text), 0o644)
}

func scanLog(text string, lookup Lookup) []Finding {
	return append(ScanEnvValues("gate log", text, registryEnvNames(), lookup), ScanText("gate log", text)...)
}

// registryEnvNames is the set of variables any lane may hold. A leaked value
// rarely arrives next to its own variable name, so the scan asks about every
// name the registry knows rather than only the ones the text mentions.
func registryEnvNames() []string {
	seen := map[string]bool{}
	var names []string
	for _, s := range Scenarios() {
		for _, name := range credentialEnv(s.Env) {
			if !seen[name] {
				seen[name] = true
				names = append(names, name)
			}
		}
	}
	sort.Strings(names)
	return names
}

// scenarioRecords turns every registry row into a record. An unwired row is
// unavailable and names why; a recorded outcome from a source document is
// reported as provenance and is never promoted into a pass.
func scenarioRecords(ctx context.Context, res Result, opts Options) []Record {
	probes := map[string]Probe{}
	for _, p := range res.Probes {
		probes[p.Name] = p
	}
	var out []Record
	for _, s := range Scenarios() {
		if opts.overrideScenario != nil && opts.overrideScenario.ID == s.ID {
			s = *opts.overrideScenario
		}
		rec := Record{
			Scenario: s.ID, Candidate: res.Candidate, Layer: s.Layer,
			StartedAt: opts.Now(), Environment: res.Environment,
			Cleanup: CleanupNotRequired, Required: s.Required,
			RequiredEnv: s.Env, Owner: s.Owner, Command: s.Owner,
		}
		if rec.Command == "" {
			rec.Command = "none: the scenario has no owning proof yet"
		}
		// A wired row is re-earned on this candidate. Anything else reports why
		// it could not run; a recorded outcome from a document is provenance,
		// never a pass.
		if s.Wired() && selected(opts.Scenarios, s.ID) && len(missingEnv(s.Env, opts.Lookup)) == 0 && scenarioHostAvailable(s, probes) {
			out = append(out, runScenario(ctx, opts, rec, s))
			continue
		}
		rec.Status = StatusUnavailable
		rec.Reason = scenarioReason(s, probes, opts.Lookup, opts.Scenarios)
		out = append(out, rec)
	}
	return out
}

// scenarioHostAvailable reports whether the exact-host gate that is allowed to
// speak for this row says its mechanism is present. A row bound to a probe is
// never executed on a host the probe calls unavailable, because passing it
// there would prove something other than what the row claims.
func scenarioHostAvailable(s Scenario, probes map[string]Probe) bool {
	name, bound := hostProbeFor[s.ID]
	if !bound {
		return true
	}
	p, found := probes[name]
	return found && p.Available
}

// runScenario executes a wired scenario's own proof. It mirrors runGate rather
// than sharing it because a scenario failure is attributed to the scenario, and
// its log is scanned on the same terms: the evidence must not become the leak.
func runScenario(ctx context.Context, opts Options, rec Record, s Scenario) Record {
	rec.Command = strings.Join(s.Argv, " ")
	runCtx := ctx
	if opts.Timeout > 0 {
		var cancel context.CancelFunc
		runCtx, cancel = context.WithTimeout(ctx, opts.Timeout)
		defer cancel()
	}
	cmd := exec.CommandContext(runCtx, s.Argv[0], s.Argv[1:]...)
	cmd.Dir = opts.Root
	output, err := cmd.CombinedOutput()
	rec.DurationMS = opts.Now().Sub(rec.StartedAt).Milliseconds()

	log := filepath.Join("logs", "scenario-"+s.ID+".log")
	if writeErr := writeLog(filepath.Join(opts.Out, log), string(output), opts.Lookup); writeErr == nil {
		rec.Artifacts = []string{log}
	}
	if err != nil {
		rec.Status = StatusFailed
		rec.Reason = fmt.Sprintf("%s: %v (see %s)", rec.Command, err, log)
		return rec
	}
	rec.Status = StatusPassed
	return rec
}

func scenarioReason(s Scenario, probes map[string]Probe, lookup Lookup, selection []string) string {
	var parts []string
	if s.Wired() && !selected(selection, s.ID) {
		return "not selected by this invocation's scenario list; the completion gate runs every wired scenario"
	}
	if missing := missingEnv(s.Env, lookup); len(missing) > 0 {
		parts = append(parts, "missing prerequisite: "+strings.Join(missing, ", ")+" not set")
	}
	if name, ok := hostProbeFor[s.ID]; ok {
		if p, found := probes[name]; found && !p.Available {
			parts = append(parts, fmt.Sprintf("%s host gate (%s): %s", name, p.Script, p.Reason))
		}
	}
	if s.Open != "" {
		parts = append(parts, s.Open)
	} else {
		provenance := fmt.Sprintf("owning proof %s is not wired into the aggregate runner", s.Owner)
		if s.Recorded != "" {
			provenance += fmt.Sprintf("; last recorded outcome %s per %s", s.Recorded, s.Source)
		}
		parts = append(parts, provenance)
	}
	return strings.Join(parts, "; ")
}

// hostProbeFor names the exact-host gate whose verdict decides a scenario. The
// mapping is explicit so a renamed title can never silently detach a row from
// the probe that is allowed to speak for it.
var hostProbeFor = map[string]string{
	"E4":  "gvisor",
	"E5":  "gvisor",
	"B28": "gvisor",
	"B29": "firecracker",
	"B31": "docker",
	"B32": "docker",
}

func missingEnv(names []string, lookup Lookup) []string {
	var missing []string
	for _, name := range names {
		if value, ok := lookup(name); !ok || value == "" {
			missing = append(missing, name)
		}
	}
	return missing
}

// leakCanaryRecord is the positive control the repository learned to demand: a
// scan that has not demonstrably found a planted string proves nothing. It
// plants the canary, requires the scan to find exactly it, verifies the planted
// file is gone, and only then scans the real evidence.
func leakCanaryRecord(opts Options, res Result) (Record, []Finding) {
	rec := Record{
		Scenario: "B0.leak-canary", Candidate: res.Candidate, Layer: LayerCode,
		Command: "internal/evidence.leakCanaryRecord", StartedAt: opts.Now(),
		Environment: res.Environment, Required: true, Cleanup: CleanupFailed,
	}
	fail := func(reason string) (Record, []Finding) {
		rec.Status = StatusFailed
		rec.Reason = reason
		rec.DurationMS = opts.Now().Sub(rec.StartedAt).Milliseconds()
		return rec, nil
	}

	dir, err := os.MkdirTemp("", "remount-canary-")
	if err != nil {
		return fail("could not create the canary directory: " + err.Error())
	}
	planted := filepath.Join(dir, "planted.txt")
	if err := os.WriteFile(planted, []byte(Canary+"\n"), 0o600); err != nil {
		os.RemoveAll(dir)
		return fail("could not plant the canary: " + err.Error())
	}
	found, err := ScanTree(dir, opts.Lookup, nil)
	if err != nil {
		os.RemoveAll(dir)
		return fail("the canary scan failed: " + err.Error())
	}
	if len(found) == 0 {
		os.RemoveAll(dir)
		return fail("the leak scan did not find the planted canary, so a clean result would prove nothing")
	}
	if err := os.RemoveAll(dir); err != nil {
		return fail("could not remove the canary directory: " + err.Error())
	}
	if _, err := os.Stat(dir); !os.IsNotExist(err) {
		return fail("the canary directory is still present after cleanup")
	}
	rec.Cleanup = CleanupVerified

	var leaks []Finding
	for _, other := range res.Records {
		leaks = append(leaks, other.ScanSecrets(opts.Lookup)...)
	}
	preview, err := RenderJSON(res)
	if err != nil {
		return fail("could not render the evidence for scanning: " + err.Error())
	}
	leaks = append(leaks, ScanEnvValues("evidence.json", string(preview), registryEnvNames(), opts.Lookup)...)
	leaks = append(leaks, ScanText("evidence.json", string(preview))...)
	summary := RenderMarkdown(res)
	leaks = append(leaks, ScanEnvValues("summary.md", summary, registryEnvNames(), opts.Lookup)...)
	leaks = append(leaks, ScanText("summary.md", summary)...)
	if len(leaks) > 0 {
		rec.Status = StatusFailed
		rec.Reason = fmt.Sprintf("the evidence carries %d credential-shaped strings: %s", len(leaks), leaks[0])
		rec.DurationMS = opts.Now().Sub(rec.StartedAt).Milliseconds()
		return rec, leaks
	}
	rec.Status = StatusPassed
	rec.DurationMS = opts.Now().Sub(rec.StartedAt).Milliseconds()
	return rec, nil
}

func summarize(records []Record) Summary {
	var s Summary
	for _, rec := range records {
		switch rec.Status {
		case StatusPassed:
			s.Passed++
		case StatusFailed:
			s.Failed++
		case StatusUnavailable:
			s.Unavailable++
		}
		if rec.Required {
			s.RequiredTotal++
			switch rec.Status {
			case StatusPassed:
				s.RequiredPassed++
			case StatusFailed:
				s.RequiredFailed++
			case StatusUnavailable:
				s.RequiredUnavailable++
			}
		}
		switch rec.Cleanup {
		case CleanupVerified:
			s.ExternalResources++
			s.CleanupVerified++
		case CleanupFailed:
			s.ExternalResources++
			s.CleanupFailed++
		}
	}
	for _, sc := range Scenarios() {
		s.ScenariosRegistered++
		if !sc.Owned() {
			s.ScenariosUnowned++
		}
	}
	return s
}

func verdict(res Result) Verdict {
	if res.Summary.RequiredFailed > 0 || res.Summary.CleanupFailed > 0 || len(res.Leaks) > 0 {
		return VerdictFailed
	}
	if res.Summary.RequiredUnavailable > 0 {
		return VerdictIncomplete
	}
	return VerdictComplete
}
