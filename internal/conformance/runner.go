package conformance

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"time"
)

// Status is the outcome of one requirement. It is Plan B §6.2's three-way
// vocabulary and nothing else: there is no "skipped and therefore fine".
type Status string

const (
	StatusPassed      Status = "passed"
	StatusFailed      Status = "failed"
	StatusUnavailable Status = "unavailable"
)

// Result is one judged requirement.
type Result struct {
	Requirement Requirement `json:"requirement"`
	Status      Status      `json:"status"`
	// Reason is the failed postcondition or the missing prerequisite. It is
	// mandatory for anything but a pass.
	Reason     string    `json:"reason,omitempty"`
	StartedAt  time.Time `json:"started_at"`
	DurationMS int64     `json:"duration_ms"`
}

// Cleanup says what happened to the state a run created.
type Cleanup string

const (
	CleanupVerified    Cleanup = "verified"
	CleanupNotRequired Cleanup = "not-required"
	CleanupFailed      Cleanup = "failed"
)

// Report is one complete run of one manifest against one target.
type Report struct {
	ManifestVersion string      `json:"manifest_version"`
	ProtocolVersion string      `json:"protocol_version"`
	Target          string      `json:"target"`
	TargetKind      TargetKind  `json:"target_kind"`
	Endpoint        string      `json:"endpoint"`
	Negotiated      []string    `json:"negotiated_capabilities"`
	Environment     Environment `json:"environment"`
	// Profile is the runtime profile this run judged the target against, or
	// empty when the run made no profile claim.
	Profile string `json:"profile,omitempty"`
	// Candidate is the commit or build identifier the caller says was under
	// test. The runner never invents one: a report that names a candidate it
	// did not verify is worse than a report that names none.
	Candidate     string    `json:"candidate,omitempty"`
	StartedAt     time.Time `json:"started_at"`
	DurationMS    int64     `json:"duration_ms"`
	Results       []Result  `json:"results"`
	Cleanup       Cleanup   `json:"cleanup"`
	CleanupReason string    `json:"cleanup_reason,omitempty"`
	// Aborted is set when the run could not start at all, for instance
	// because the target declares a protocol version this manifest does not
	// describe.
	Aborted string `json:"aborted,omitempty"`
}

// RunOptions bounds one run.
type RunOptions struct {
	// Manifest defaults to Standard().
	Manifest *Manifest
	// Only restricts the run to these requirement ids, for debugging a single
	// row. Empty runs everything.
	Only []string
	// PerCheck bounds one requirement. Zero uses two minutes.
	PerCheck time.Duration
	// Profile names a runtime profile (§5) to judge the target against, one
	// of ProfileNames. Empty judges only the protocol manifest. A named
	// profile adds one required row per obligation the profile carries plus
	// the scheduling row, and makes every workspace the run creates ask for
	// that profile.
	Profile string
	// Candidate is the commit or build identifier under test, recorded on
	// the report and rendered in the markdown header.
	Candidate string
	// Log receives a line per requirement as it completes.
	Log func(Result)
}

// Run judges a target against a manifest.
func Run(ctx context.Context, t *Target, opts RunOptions) (*Report, error) {
	m := Standard()
	if opts.Manifest != nil {
		m = *opts.Manifest
	}
	if err := m.Validate(); err != nil {
		return nil, err
	}
	if opts.Profile != "" && !ValidProfile(opts.Profile) {
		return nil, fmt.Errorf("conformance: unknown runtime profile %q; §5 defines %s", opts.Profile, strings.Join(ProfileNames, ", "))
	}
	start := time.Now()
	report := &Report{
		ManifestVersion: m.Version,
		ProtocolVersion: m.ProtocolVersion,
		Target:          t.Name,
		TargetKind:      t.Kind,
		Endpoint:        t.Endpoint,
		Environment:     t.Environment,
		Profile:         opts.Profile,
		Candidate:       opts.Candidate,
		StartedAt:       start.UTC(),
		Cleanup:         CleanupNotRequired,
	}
	if report.Environment.Backend == "" {
		report.Environment.Backend = t.Backend
	}
	// A target that declares another frame version is not described by this
	// manifest. Judging it anyway would produce a verdict about a contract it
	// never claimed to implement.
	if t.DeclaredProtocol != m.ProtocolVersion {
		report.Aborted = fmt.Sprintf("target declares protocol version %q; manifest %s describes version %q", t.DeclaredProtocol, m.Version, m.ProtocolVersion)
		report.DurationMS = time.Since(start).Milliseconds()
		return report, nil
	}

	s, err := NewSession(ctx, t)
	if err != nil {
		report.Aborted = fmt.Sprintf("could not open a conformance session: %v", err)
		report.DurationMS = time.Since(start).Milliseconds()
		return report, nil
	}
	defer s.Close()
	s.Profile = opts.Profile
	report.Negotiated = s.Control.Negotiated

	only := map[string]bool{}
	for _, id := range opts.Only {
		only[id] = true
	}
	perCheck := opts.PerCheck
	if perCheck == 0 {
		perCheck = 2 * time.Minute
	}

	// The profile rows run first. They are the cheapest evidence in the run
	// and they explain everything after them: a deployment that cannot
	// satisfy the named profile will park the workspaces the protocol
	// requirements need, and a reader who has already seen why does not have
	// to infer it from forty unavailable rows.
	if opts.Profile != "" {
		rows, evidence := runProfile(ctx, s, opts.Profile, perCheck)
		for _, res := range rows {
			if len(only) > 0 && !only[res.Requirement.ID] {
				continue
			}
			report.Results = append(report.Results, res)
			if opts.Log != nil {
				opts.Log(res)
			}
		}
		if len(only) == 0 || only[ProfileSchedulingID] {
			res := runProfileScheduling(ctx, s, opts.Profile, perCheck, evidence)
			report.Results = append(report.Results, res)
			if opts.Log != nil {
				opts.Log(res)
			}
		}
	}

	for _, r := range m.Requirements {
		if len(only) > 0 && !only[r.ID] {
			continue
		}
		res := runOne(ctx, s, r, perCheck)
		report.Results = append(report.Results, res)
		if opts.Log != nil {
			opts.Log(res)
		}
	}

	cleanupCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 3*time.Minute)
	defer cancel()
	if err := s.Cleanup(cleanupCtx); err != nil {
		report.Cleanup, report.CleanupReason = CleanupFailed, err.Error()
	} else if len(report.Results) > 0 {
		report.Cleanup = CleanupVerified
	}
	report.DurationMS = time.Since(start).Milliseconds()
	return report, nil
}

func runOne(ctx context.Context, s *Session, r Requirement, per time.Duration) Result {
	res := Result{Requirement: r, StartedAt: time.Now().UTC()}
	defer func() { res.DurationMS = time.Since(res.StartedAt).Milliseconds() }()

	if err := gate(s, r); err != nil {
		res.Status, res.Reason = StatusUnavailable, reasonOf(err)
		return res
	}
	check, ok := checks[r.ID]
	if !ok {
		res.Status, res.Reason = StatusFailed, "no check is registered for this requirement"
		return res
	}
	cctx, cancel := context.WithTimeout(ctx, per)
	defer cancel()

	err := runGuarded(cctx, s, check)
	switch {
	case err == nil:
		res.Status = StatusPassed
	case IsUnavailable(err):
		res.Status, res.Reason = StatusUnavailable, reasonOf(err)
	default:
		res.Status, res.Reason = StatusFailed, err.Error()
	}
	return res
}

// runGuarded turns a panicking check into a failure. A suite whose own bug
// aborts the run cannot report on the implementation it was judging.
func runGuarded(ctx context.Context, s *Session, check CheckFunc) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("the check itself panicked: %v", r)
		}
	}()
	return check(ctx, s)
}

func reasonOf(err error) string {
	var u *Unavailable
	if ok := asUnavailable(err, &u); ok {
		return u.Reason
	}
	return err.Error()
}

func asUnavailable(err error, target **Unavailable) bool {
	if u, ok := err.(*Unavailable); ok {
		*target = u
		return true
	}
	return false
}

// Counts summarizes a report by status.
func (r *Report) Counts() map[Status]int {
	out := map[Status]int{}
	for _, res := range r.Results {
		out[res.Status]++
	}
	return out
}

// CountsByTier summarizes a report by tier and status.
func (r *Report) CountsByTier() map[Tier]map[Status]int {
	out := map[Tier]map[Status]int{}
	for _, res := range r.Results {
		if out[res.Requirement.Tier] == nil {
			out[res.Requirement.Tier] = map[Status]int{}
		}
		out[res.Requirement.Tier][res.Status]++
	}
	return out
}

// Conformant reports whether the target satisfied the required tier, and why
// not when it did not.
//
// A required requirement that could not be observed is not a pass. That is
// the whole reason Plan B §6.2 refuses a "skipped-success" outcome: an
// implementation nobody could check is not an implementation anybody should
// trust.
func (r *Report) Conformant() (bool, string) {
	if r.Aborted != "" {
		return false, "the run was aborted: " + r.Aborted
	}
	var failed, unobserved []string
	for _, res := range r.Results {
		if res.Requirement.Tier != TierRequired {
			continue
		}
		switch res.Status {
		case StatusFailed:
			failed = append(failed, res.Requirement.ID)
		case StatusUnavailable:
			unobserved = append(unobserved, res.Requirement.ID)
		}
	}
	sort.Strings(failed)
	sort.Strings(unobserved)
	var why []string
	if len(failed) > 0 {
		why = append(why, "required requirements failed: "+strings.Join(failed, ", "))
	}
	if len(unobserved) > 0 {
		why = append(why, "required requirements could not be observed: "+strings.Join(unobserved, ", "))
	}
	if r.Cleanup == CleanupFailed {
		why = append(why, "cleanup failed: "+r.CleanupReason)
	}
	if len(why) == 0 {
		return true, ""
	}
	return false, strings.Join(why, "; ")
}

// FailedCategories lists the semantic categories in which at least one
// requirement failed, in canonical order. It is what proves a deliberately
// broken implementation was caught in the category it was broken in.
func (r *Report) FailedCategories() []Category {
	failed := map[Category]bool{}
	for _, res := range r.Results {
		if res.Status == StatusFailed {
			failed[res.Requirement.Category] = true
		}
	}
	var out []Category
	for _, c := range Categories {
		if failed[c] {
			out = append(out, c)
		}
	}
	return out
}

// Failures returns the failed results, in manifest order.
func (r *Report) Failures() []Result {
	var out []Result
	for _, res := range r.Results {
		if res.Status == StatusFailed {
			out = append(out, res)
		}
	}
	return out
}
