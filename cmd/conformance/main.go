// Command conformance judges an implementation against the Remount
// conformance manifest.
//
// It reaches the implementation only through public surfaces — the frame
// protocol at /v1/link, the HTTP artifact and event routes, and the event log
// — so it can judge an implementation that shares no code with this
// repository. Plan B §12.2's three target modes are the three ways to name
// one:
//
//	conformance --build .                       build this module and judge the binary
//	conformance --binary ./remount              judge a binary this run starts
//	conformance --endpoint http://host:7443     judge something already running
//	conformance --endpoint URL --external       judge another implementation
//
// It writes JUnit XML and the Plan B §6 evidence record.
package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"os"
	"os/exec"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"remount.dev/remount/internal/conformance"
)

func main() {
	if err := run(os.Args[1:], os.Stdout, os.Stderr); err != nil {
		fmt.Fprintln(os.Stderr, "conformance:", err)
		os.Exit(1)
	}
}

type options struct {
	build     string
	binary    string
	endpoint  string
	token     string
	cli       string
	external  bool
	protocol  string
	backend   string
	only      string
	junit     string
	evidence  string
	reportOut string
	scenario  string
	candidate string
	layer     string
	manifest  bool
	perCheck  time.Duration
	connect   time.Duration
}

func run(args []string, stdout, stderr *os.File) error {
	var o options
	fs := flag.NewFlagSet("conformance", flag.ContinueOnError)
	fs.SetOutput(stderr)
	fs.StringVar(&o.build, "build", "", "Go module directory to build ./cmd/remount from, then judge")
	fs.StringVar(&o.binary, "binary", "", "an already-built binary to start in standalone mode and judge")
	fs.StringVar(&o.endpoint, "endpoint", "", "an already-running endpoint to judge")
	fs.StringVar(&o.token, "token", os.Getenv("REMOUNT_TOKEN"), "bearer credential for --endpoint")
	fs.StringVar(&o.cli, "cli", "", "a command-line client for --endpoint")
	fs.BoolVar(&o.external, "external", false, "with --endpoint: the target is another implementation, not Remount")
	fs.StringVar(&o.protocol, "protocol", "", "the frame version the target declares (default: the manifest's)")
	fs.StringVar(&o.backend, "backend", "process", "workspace backend to ask the target for")
	fs.StringVar(&o.only, "only", "", "comma-separated requirement ids to run")
	fs.StringVar(&o.junit, "junit", "", "write JUnit XML here")
	fs.StringVar(&o.evidence, "evidence", "", "write the Plan B §6 evidence record here")
	fs.StringVar(&o.reportOut, "report", "", "write the full JSON report here")
	fs.StringVar(&o.scenario, "scenario", "B20", "scenario id for the evidence record")
	fs.StringVar(&o.candidate, "candidate", "", "the git commit under test (default: git rev-parse HEAD)")
	fs.StringVar(&o.layer, "layer", "artifact", "evidence layer for the record")
	fs.BoolVar(&o.manifest, "manifest", false, "print the manifest and exit without judging anything")
	fs.DurationVar(&o.perCheck, "per-check", 2*time.Minute, "bound on one requirement")
	fs.DurationVar(&o.connect, "connect-timeout", 30*time.Second, "how long --endpoint is given to answer health")
	if err := fs.Parse(args); err != nil {
		return err
	}

	if o.manifest {
		return conformance.WriteManifest(stdout, conformance.Standard())
	}
	m := conformance.Standard()
	if err := m.Validate(); err != nil {
		return err
	}

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	target, err := open(ctx, o, stderr)
	if err != nil {
		return err
	}
	defer func() { _ = target.Close() }()

	report, err := conformance.Run(ctx, target, conformance.RunOptions{
		Manifest: &m,
		Only:     split(o.only),
		PerCheck: o.perCheck,
		Log: func(res conformance.Result) {
			fmt.Fprintf(stderr, "%-12s %-14s %s\n", res.Status, res.Requirement.ID, first(res.Reason, res.Requirement.Title))
		},
	})
	if err != nil {
		return err
	}

	if err := report.WriteSummary(stdout); err != nil {
		return err
	}
	if o.junit != "" {
		if err := writeFile(o.junit, report.WriteJUnit); err != nil {
			return err
		}
	}
	if o.reportOut != "" {
		if err := writeFile(o.reportOut, report.WriteJSON); err != nil {
			return err
		}
	}
	if o.evidence != "" {
		candidate := o.candidate
		if candidate == "" {
			candidate = gitHead(ctx, o.build)
		}
		opts := conformance.EvidenceOptions{
			Scenario:  o.scenario,
			Candidate: candidate,
			Layer:     o.layer,
			Command:   "cmd/conformance " + strings.Join(args, " "),
			Required:  true,
			Owner:     "cmd/conformance",
		}
		if o.junit != "" {
			opts.Artifacts = append(opts.Artifacts, o.junit)
		}
		if err := writeFile(o.evidence, func(w io.Writer) error { return report.WriteEvidence(w, opts) }); err != nil {
			return err
		}
	}
	if ok, why := report.Conformant(); !ok {
		return fmt.Errorf("%s is not conformant: %s", target.Name, why)
	}
	return nil
}

func open(ctx context.Context, o options, stderr *os.File) (*conformance.Target, error) {
	switch {
	case o.endpoint != "":
		kind := conformance.KindEndpoint
		if o.external {
			kind = conformance.KindExternal
		}
		return conformance.Attach(ctx, conformance.AttachOptions{
			Endpoint: o.endpoint, Token: o.token, CLI: o.cli, Kind: kind,
			DeclaredProtocol: o.protocol, Backend: o.backend, Timeout: o.connect,
		})
	case o.build != "" || o.binary != "":
		return conformance.Launch(ctx, conformance.LaunchOptions{
			ModuleDir: o.build, BinaryPath: o.binary, Backend: o.backend, Stdout: stderr,
		})
	default:
		return nil, fmt.Errorf("name a target: --build DIR, --binary PATH or --endpoint URL")
	}
}

func writeFile(path string, write func(io.Writer) error) error {
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

func gitHead(ctx context.Context, dir string) string {
	cmd := exec.CommandContext(ctx, "git", "rev-parse", "HEAD")
	if dir != "" {
		cmd.Dir = dir
	}
	out, err := cmd.Output()
	if err != nil {
		return "unknown"
	}
	return strings.TrimSpace(string(out))
}

func split(s string) []string {
	if strings.TrimSpace(s) == "" {
		return nil
	}
	var out []string
	for _, p := range strings.Split(s, ",") {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	return out
}

func first(a, b string) string {
	if a != "" {
		return a
	}
	return b
}
