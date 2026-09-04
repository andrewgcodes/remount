// Package evidence carries the Plan B acceptance ledger: the record schema every
// scenario emits, the registry of scenarios that must eventually emit one, and
// the aggregate reporter behind `make plan-b`.
//
// The failure this package exists to prevent is an unearned pass. A lane that
// could not run is `unavailable` and names its missing prerequisite; it is
// never rendered green, and no absent proof is silently omitted from a report.
package evidence

import (
	"fmt"
	"strings"
	"time"
)

// Layer names the kind of proof a record carries.
// `production` is deliberately absent: an operated deployment cannot be proven
// from a repository, so it belongs in a separate ledger.
type Layer string

const (
	LayerCode        Layer = "code"         // unit, race, fuzz, static, generator or fixture proof
	LayerSim         Layer = "sim"          // multi-peer composition under internal/sim fault injection
	LayerHostCI      Layer = "host-ci"      // an exact kernel/runtime mechanism on an identified host
	LayerLiveService Layer = "live-service" // the exact candidate against a real external service
	LayerArtifact    Layer = "artifact"     // a binary, image, package, SBOM or install path built locally
)

// Layers lists every valid evidence layer, in the order of the plan's table.
var Layers = []Layer{LayerCode, LayerSim, LayerHostCI, LayerLiveService, LayerArtifact}

// Status is the outcome of one scenario. There is no `skipped-success`: a
// scenario that did not run because a prerequisite was absent is unavailable.
type Status string

const (
	StatusPassed      Status = "passed"      // the command ran and every postcondition, cleanup included, held
	StatusFailed      Status = "failed"      // the command ran and a required postcondition failed
	StatusUnavailable Status = "unavailable" // a credential, daemon, host capability or service was absent
)

// Statuses lists every valid outcome.
var Statuses = []Status{StatusPassed, StatusFailed, StatusUnavailable}

// Cleanup says what happened to the external state a scenario created.
type Cleanup string

const (
	CleanupVerified    Cleanup = "verified"     // every resource was destroyed and its absence confirmed
	CleanupNotRequired Cleanup = "not-required" // the scenario created no external resource
	CleanupFailed      Cleanup = "failed"       // a resource could not be destroyed or its absence not confirmed
)

// Cleanups lists every valid cleanup outcome.
var Cleanups = []Cleanup{CleanupVerified, CleanupNotRequired, CleanupFailed}

// Environment identifies the machine and substrate a record was produced on.
type Environment struct {
	OS       string `json:"os"`
	Arch     string `json:"arch"`
	Backend  string `json:"backend,omitempty"`
	Provider string `json:"provider,omitempty"`
}

// Record is the one JSON object a scenario emits. Field names are fixed by the
// Plan B evidence model; the additional fields are additive and exist so a row
// can never be unavailable without naming why.
type Record struct {
	Scenario    string      `json:"scenario"`
	Candidate   string      `json:"candidate"` // the exact git commit under test
	Layer       Layer       `json:"layer"`
	Status      Status      `json:"status"`
	Command     string      `json:"command"`
	StartedAt   time.Time   `json:"started_at"`
	DurationMS  int64       `json:"duration_ms"`
	Environment Environment `json:"environment"`
	Cleanup     Cleanup     `json:"cleanup"`
	Artifacts   []string    `json:"artifacts,omitempty"`
	// Required distinguishes a completion-gate row from a supplementary lane.
	Required bool `json:"required"`
	// Reason names the missing prerequisite or the failed postcondition. It is
	// mandatory for anything but a pass, because an unexplained unavailable row
	// is indistinguishable from a hidden one.
	Reason string `json:"reason,omitempty"`
	// RequiredEnv names environment variables. Values never appear here or
	// anywhere else in the record; ScanSecrets enforces that.
	RequiredEnv []string `json:"required_env,omitempty"`
	// Owner is the test, script or command that proves the scenario. Empty
	// means nothing in the tree proves it yet.
	Owner string `json:"owner,omitempty"`
	// ProviderIDs may name provider objects only to prove they were destroyed,
	// and only in an ephemeral run artifact.
	ProviderIDs []string `json:"provider_ids,omitempty"`
	// Ephemeral marks a record that must not be committed as evergreen proof.
	Ephemeral bool `json:"ephemeral,omitempty"`
}

// commitPattern is deliberately loose: abbreviated and full hexadecimal object
// names both identify a candidate exactly once the repository is known.
const minCommitLen, maxCommitLen = 7, 64

// Validate reports whether a record is structurally admissible. It rejects the
// two shapes that would let a report lie: an invented layer or status, and an
// unexplained non-pass.
func (r Record) Validate() error {
	if strings.TrimSpace(r.Scenario) == "" {
		return fmt.Errorf("evidence: record has no scenario id")
	}
	if err := validateCandidate(r.Candidate); err != nil {
		return fmt.Errorf("evidence: %s: %w", r.Scenario, err)
	}
	if !contains(Layers, r.Layer) {
		if r.Layer == "production" {
			return fmt.Errorf("evidence: %s: `production` is not an evidence layer this plan generates; production proof needs an operated deployment", r.Scenario)
		}
		return fmt.Errorf("evidence: %s: layer %q is not one of %v", r.Scenario, r.Layer, Layers)
	}
	if !contains(Statuses, r.Status) {
		if r.Status == "skipped-success" {
			return fmt.Errorf("evidence: %s: there is no `skipped-success` outcome; a check that could not run is %q", r.Scenario, StatusUnavailable)
		}
		return fmt.Errorf("evidence: %s: status %q is not one of %v", r.Scenario, r.Status, Statuses)
	}
	if strings.TrimSpace(r.Command) == "" {
		return fmt.Errorf("evidence: %s: record names no command", r.Scenario)
	}
	if r.StartedAt.IsZero() {
		return fmt.Errorf("evidence: %s: record has no start time", r.Scenario)
	}
	if _, offset := r.StartedAt.Zone(); offset != 0 {
		return fmt.Errorf("evidence: %s: start time is not UTC", r.Scenario)
	}
	if r.DurationMS < 0 {
		return fmt.Errorf("evidence: %s: negative duration", r.Scenario)
	}
	if r.Environment.OS == "" || r.Environment.Arch == "" {
		return fmt.Errorf("evidence: %s: environment names no os/arch", r.Scenario)
	}
	if !contains(Cleanups, r.Cleanup) {
		return fmt.Errorf("evidence: %s: cleanup %q is not one of %v", r.Scenario, r.Cleanup, Cleanups)
	}
	if r.Cleanup == CleanupFailed && r.Status == StatusPassed {
		return fmt.Errorf("evidence: %s: a scenario whose cleanup failed did not pass", r.Scenario)
	}
	if r.Status != StatusPassed && strings.TrimSpace(r.Reason) == "" {
		return fmt.Errorf("evidence: %s: a %s record must name its reason", r.Scenario, r.Status)
	}
	for _, name := range r.RequiredEnv {
		if !envNamePattern.MatchString(name) {
			return fmt.Errorf("evidence: %s: %q is not an environment variable name; records name variables, never values", r.Scenario, name)
		}
	}
	if len(r.ProviderIDs) > 0 && !r.Ephemeral {
		return fmt.Errorf("evidence: %s: provider object ids belong only in an ephemeral run artifact", r.Scenario)
	}
	return nil
}

// ValidateEvergreen additionally refuses anything that must not be committed as
// standing proof: a run-scoped record, and any provider object id.
func (r Record) ValidateEvergreen() error {
	if err := r.Validate(); err != nil {
		return err
	}
	if r.Ephemeral || len(r.ProviderIDs) > 0 {
		return fmt.Errorf("evidence: %s: ephemeral provider evidence is not evergreen proof", r.Scenario)
	}
	return nil
}

func validateCandidate(candidate string) error {
	if len(candidate) < minCommitLen || len(candidate) > maxCommitLen {
		return fmt.Errorf("candidate %q is not a git commit", candidate)
	}
	for _, c := range candidate {
		if (c < '0' || c > '9') && (c < 'a' || c > 'f') {
			return fmt.Errorf("candidate %q is not a git commit", candidate)
		}
	}
	return nil
}

func contains[T comparable](values []T, want T) bool {
	for _, v := range values {
		if v == want {
			return true
		}
	}
	return false
}

// Strings returns every string field of the record. Leak scanning must see the
// whole record, so adding a string field without adding it here is a defect.
func (r Record) Strings() []string {
	out := []string{r.Scenario, r.Candidate, string(r.Layer), string(r.Status), r.Command,
		r.Environment.OS, r.Environment.Arch, r.Environment.Backend, r.Environment.Provider,
		string(r.Cleanup), r.Reason, r.Owner}
	out = append(out, r.Artifacts...)
	out = append(out, r.RequiredEnv...)
	out = append(out, r.ProviderIDs...)
	return out
}
