// Command evidence enumerates the Plan B acceptance scenarios and runs the
// aggregate completion gate behind `make plan-b`.
//
// It exists so that "what does this repository actually prove?" is a command
// rather than a reading exercise, and so that a scenario nobody has built yet
// is a visible open row instead of a silence.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"remount.dev/remount/internal/evidence"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	args := os.Args[1:]
	if len(args) == 0 {
		usage()
		os.Exit(2)
	}
	var err error
	switch args[0] {
	case "list":
		err = list(args[1:], os.Stdout)
	case "run":
		err = run(ctx, args[1:])
	case "validate":
		err = validate(args[1:])
	case "-h", "--help", "help":
		usage()
		return
	default:
		fmt.Fprintf(os.Stderr, "evidence: unknown subcommand %q\n", args[0])
		usage()
		os.Exit(2)
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func usage() {
	fmt.Fprint(os.Stderr, `usage: evidence <command> [flags]

  list                enumerate every acceptance scenario, its owning proof,
                      required environment and latest recorded outcome
  run                 run the Plan B aggregate completion gate
  validate FILE       validate evidence records against the schema

`)
}

func list(args []string, out io.Writer) error {
	fs := flag.NewFlagSet("list", flag.ContinueOnError)
	asJSON := fs.Bool("json", false, "emit JSON")
	required := fs.Bool("required", false, "only rows the completion gate needs")
	unowned := fs.Bool("unowned", false, "only rows nothing in the tree proves yet")
	if err := parse(fs, args); err != nil {
		return err
	}
	if err := evidence.ValidateRegistry(); err != nil {
		return err
	}
	views := evidence.View(evidence.OSLookup)
	filtered := views[:0]
	for _, v := range views {
		if *required && !v.Required {
			continue
		}
		if *unowned && v.Owned() {
			continue
		}
		filtered = append(filtered, v)
	}
	if *asJSON {
		data, err := evidence.RenderListJSON(filtered)
		if err != nil {
			return err
		}
		_, err = out.Write(data)
		return err
	}
	_, err := fmt.Fprint(out, evidence.RenderList(filtered))
	return err
}

func run(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("run", flag.ContinueOnError)
	out := fs.String("out", "dist/plan-b", "directory for evidence.json, junit.xml and summary.md")
	root := fs.String("root", ".", "repository root")
	dev := fs.Bool("dev", false, "developer mode: accept a dirty tree")
	gates := fs.String("gates", "", "comma-separated gate ids to run; empty runs every gate")
	scenarios := fs.String("scenarios", "", "comma-separated scenario ids to execute; empty executes every wired scenario")
	requireComplete := fs.Bool("require-complete", false, "fail unless every required row passed in this run")
	timeout := fs.Duration("gate-timeout", 30*time.Minute, "bound on one gate")
	if err := parse(fs, args); err != nil {
		return err
	}
	res, err := evidence.Run(ctx, evidence.Options{
		Root:            *root,
		Out:             *out,
		Dev:             *dev,
		Gates:           split(*gates),
		Scenarios:       split(*scenarios),
		RequireComplete: *requireComplete,
		Stdout:          os.Stdout,
		Timeout:         *timeout,
	})
	if err != nil {
		return err
	}
	fmt.Fprintf(os.Stderr, "\nevidence written to %s (evidence.json, junit.xml, summary.md)\n", *out)
	if res.Failed(*requireComplete) {
		return fmt.Errorf("evidence: verdict %s: %d required rows failed, %d unavailable, %d leaks",
			res.Verdict, res.Summary.RequiredFailed, res.Summary.RequiredUnavailable, len(res.Leaks))
	}
	return nil
}

func validate(args []string) error {
	fs := flag.NewFlagSet("validate", flag.ContinueOnError)
	evergreen := fs.Bool("evergreen", false, "also refuse ephemeral records and provider object ids")
	if err := parse(fs, args); err != nil {
		return err
	}
	if fs.NArg() != 1 {
		return errors.New("evidence: validate takes exactly one file")
	}
	data, err := os.ReadFile(fs.Arg(0))
	if err != nil {
		return fmt.Errorf("evidence: %w", err)
	}
	records, err := decodeRecords(data)
	if err != nil {
		return fmt.Errorf("evidence: %s: %w", fs.Arg(0), err)
	}
	var problems []string
	for _, rec := range records {
		check := rec.Validate
		if *evergreen {
			check = rec.ValidateEvergreen
		}
		if err := check(); err != nil {
			problems = append(problems, err.Error())
		}
		for _, f := range rec.ScanSecrets(evidence.OSLookup) {
			problems = append(problems, "leak: "+f.String())
		}
	}
	if len(problems) > 0 {
		return errors.New(strings.Join(problems, "\n"))
	}
	fmt.Printf("%d records valid\n", len(records))
	return nil
}

// decodeRecords accepts either the aggregate document or a bare array of
// records, so a CI job can validate whichever half it kept.
func decodeRecords(data []byte) ([]evidence.Record, error) {
	var result evidence.Result
	if err := json.Unmarshal(data, &result); err == nil && result.Schema != 0 {
		return result.Records, nil
	}
	var records []evidence.Record
	if err := json.Unmarshal(data, &records); err == nil {
		return records, nil
	}
	var one evidence.Record
	if err := json.Unmarshal(data, &one); err != nil {
		return nil, fmt.Errorf("not an evidence document, record array or record: %w", err)
	}
	return []evidence.Record{one}, nil
}

func split(s string) []string {
	var out []string
	for _, part := range strings.Split(s, ",") {
		if part = strings.TrimSpace(part); part != "" {
			out = append(out, part)
		}
	}
	return out
}

// parse permutes flags ahead of positionals. Go's flag package stops at the
// first positional argument, which has already silently dropped a flag in this
// repository's CLI once.
func parse(fs *flag.FlagSet, args []string) error {
	var flags, positional []string
	for i := 0; i < len(args); i++ {
		a := args[i]
		if a == "--" {
			positional = append(positional, args[i:]...)
			break
		}
		if len(a) > 1 && a[0] == '-' {
			flags = append(flags, a)
			name := strings.TrimLeft(a, "-")
			if strings.Contains(name, "=") {
				continue
			}
			f := fs.Lookup(name)
			if f == nil {
				continue
			}
			if b, ok := f.Value.(interface{ IsBoolFlag() bool }); ok && b.IsBoolFlag() {
				continue
			}
			if i+1 < len(args) {
				i++
				flags = append(flags, args[i])
			}
			continue
		}
		positional = append(positional, a)
	}
	return fs.Parse(append(flags, positional...))
}
