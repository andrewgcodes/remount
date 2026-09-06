package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"

	"remount.dev/remount/internal/conformance"
)

// `remount conformance` is the operator-facing form of cmd/conformance: the
// same black-box runner, pointed by default at whatever REMOUNT_SERVER
// already names, with the runtime-profile verdict and the review document in
// the same invocation.
//
// It exists because the audience is different. cmd/conformance is for someone
// judging an implementation and wiring the record into a Plan B lane;
// `remount conformance --profile multi-tenant-isolated` is for the operator
// who has just provisioned a machine and wants one command that answers "is
// this fleet fit for untrusted work, and what exactly is missing".
//
// The exit code is the same three-valued contract `remount doctor --profile`
// uses, for the same reason: an operator scripting either of them must be
// able to tell "it failed" from "nobody could tell", and neither may ever be
// spelled 0.
//
//	0  every judged requirement passed
//	1  at least one requirement failed, or cleanup failed
//	2  nothing failed but something could not be observed
func cmdConformance(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("conformance", flag.ExitOnError)
	var c common
	c.flags(fs)
	url := fs.String("url", "", "endpoint to judge (default: --server / REMOUNT_SERVER)")
	launch := fs.String("launch", "", "instead of an endpoint, start this binary in standalone mode and judge it; `self` uses the running executable")
	runtimeProfile := fs.String("profile", "", "also judge the target against a runtime profile: "+strings.Join(conformance.ProfileNames, ", "))
	backend := fs.String("backend", "process", "workspace backend to ask the target for")
	only := fs.String("only", "", "comma-separated requirement ids to run")
	markdown := fs.String("markdown", "", "write the review document (one table per tier) here")
	reportOut := fs.String("report", "", "write the full JSON report here")
	junit := fs.String("junit", "", "write JUnit XML here")
	evidence := fs.String("evidence", "", "write the Plan B §6 evidence record here")
	out := fs.String("out", "", "write conformance.json and conformance.md into this directory")
	scenario := fs.String("scenario", "B33", "scenario id for the evidence record")
	candidate := fs.String("candidate", "", "the commit or build under test (default: git rev-parse HEAD)")
	perCheck := fs.Duration("per-check", 2*time.Minute, "bound on one requirement")
	connect := fs.Duration("connect-timeout", 30*time.Second, "how long the endpoint is given to answer health")
	parse(fs, args)
	if err := arity(fs, 0, 0, "remount conformance [--profile P] [--markdown FILE]"); err != nil {
		return err
	}
	if *runtimeProfile != "" && !conformance.ValidProfile(*runtimeProfile) {
		return fmt.Errorf("unknown runtime profile %q; known profiles are %s",
			*runtimeProfile, strings.Join(conformance.ProfileNames, ", "))
	}
	jsonPath, markdownPath := *reportOut, *markdown
	if *out != "" {
		if err := os.MkdirAll(*out, 0o755); err != nil {
			return err
		}
		if jsonPath == "" {
			jsonPath = filepath.Join(*out, "conformance.json")
		}
		if markdownPath == "" {
			markdownPath = filepath.Join(*out, "conformance.md")
		}
	}

	target, err := conformanceTarget(ctx, c, *url, *launch, *backend, *connect)
	if err != nil {
		return err
	}
	defer func() { _ = target.Close() }()

	m := conformance.Standard()
	if err := m.Validate(); err != nil {
		return err
	}
	report, err := conformance.Run(ctx, target, conformance.RunOptions{
		Manifest:  &m,
		Only:      splitList(*only),
		PerCheck:  *perCheck,
		Profile:   *runtimeProfile,
		Candidate: firstNonEmpty(*candidate, gitHead(ctx)),
		Log: func(res conformance.Result) {
			fmt.Fprintf(os.Stderr, "%-12s %-14s %s\n", res.Status, res.Requirement.ID,
				firstNonEmpty(res.Reason, res.Requirement.Title))
		},
	})
	if err != nil {
		return err
	}

	if c.json {
		if err := report.WriteJSON(os.Stdout); err != nil {
			return err
		}
	} else if err := report.WriteSummary(os.Stdout); err != nil {
		return err
	}
	writes := []struct {
		path  string
		write func(io.Writer) error
	}{
		{jsonPath, report.WriteJSON},
		{markdownPath, report.WriteMarkdown},
		{*junit, report.WriteJUnit},
	}
	var artifacts []string
	for _, w := range writes {
		if w.path == "" {
			continue
		}
		if err := writeConformanceFile(w.path, w.write); err != nil {
			return err
		}
		artifacts = append(artifacts, w.path)
	}
	if *evidence != "" {
		opts := conformance.EvidenceOptions{
			Scenario:  *scenario,
			Candidate: report.Candidate,
			Layer:     "artifact",
			Command:   "remount conformance " + strings.Join(args, " "),
			Required:  true,
			Owner:     "cmd/remount conformance",
			Artifacts: artifacts,
		}
		if err := writeConformanceFile(*evidence, func(w io.Writer) error {
			return report.WriteEvidence(w, opts)
		}); err != nil {
			return err
		}
	}

	// Leading with the verdict, and never spelling "could not tell" as 0.
	ok, why := report.Conformant()
	failed := len(report.Failures()) > 0 || report.Cleanup == conformance.CleanupFailed
	switch {
	case failed:
		fmt.Fprintf(os.Stderr, "NOT CONFORMANT: %s\n", why)
		return exitError(1)
	case !ok:
		fmt.Fprintf(os.Stderr, "INCOMPLETE: %s; an unavailable check is not a passing check\n", why)
		return exitError(2)
	default:
		return nil
	}
}

// conformanceTarget resolves the three ways to name something to judge. The
// launched form exists so `make conformance-report` can produce evidence
// against a freshly booted standalone without a second tool: the binary it
// starts is this one, so the report is about the build in hand.
func conformanceTarget(ctx context.Context, c common, url, launch, backend string, connect time.Duration) (*conformance.Target, error) {
	if launch != "" {
		bin := launch
		if launch == "self" {
			exe, err := os.Executable()
			if err != nil {
				return nil, fmt.Errorf("locate this executable to launch it: %w", err)
			}
			bin = exe
		}
		return conformance.Launch(ctx, conformance.LaunchOptions{
			BinaryPath: bin, Backend: backend, Stdout: os.Stderr,
		})
	}
	endpoint := url
	if endpoint == "" {
		endpoint = c.server
	}
	if endpoint == "" {
		return nil, fmt.Errorf("name a target: --url URL, --server URL, REMOUNT_SERVER or --launch self")
	}
	return conformance.Attach(ctx, conformance.AttachOptions{
		Endpoint: endpoint, Token: c.token, Backend: backend, Timeout: connect,
	})
}

func writeConformanceFile(path string, write func(io.Writer) error) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	if err := write(f); err != nil {
		_ = f.Close()
		return err
	}
	return f.Close()
}

func gitHead(ctx context.Context) string {
	out, err := exec.CommandContext(ctx, "git", "rev-parse", "HEAD").Output()
	if err != nil {
		return "unknown"
	}
	return strings.TrimSpace(string(out))
}
