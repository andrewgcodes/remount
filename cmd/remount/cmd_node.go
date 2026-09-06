package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"strings"
	"time"

	"remount.dev/remount/internal/ids"
)

// cmdNode is the operator's node surface: mint the one-time credential a
// machine needs to join a production control plane, and list what joined.
//
// It exists because production mode refuses a shared node token, so between
// "the control plane is up" and "a node is attached" there was no command at
// all: `remount up --token "$(cat op.token)"` presents an operator bearer,
// which the node authenticator correctly refuses as `unauthorized`.
func cmdNode(ctx context.Context, args []string) error {
	if len(args) == 0 || isHelp(args[0]) {
		return errors.New("node: enroll|ls")
	}
	sub, rest := args[0], args[1:]
	fs := flag.NewFlagSet("node "+sub, flag.ExitOnError)
	var commonFlags common
	commonFlags.flags(fs)
	switch sub {
	case "enroll":
		return runNodeEnroll(ctx, fs, rest, &commonFlags)
	case "ls":
		parse(fs, rest)
		if err := arity(fs, 0, 0, "node ls"); err != nil {
			return err
		}
		return listNodes(ctx, commonFlags)
	default:
		return fmt.Errorf("unknown node subcommand %q", sub)
	}
}

// nodeEnrollmentTTLMax matches the control plane's ceiling. Rejecting a longer
// window here means the operator reads the bound from the command they typed
// rather than from a round trip.
const nodeEnrollmentTTLMax = 10 * time.Minute

func runNodeEnroll(ctx context.Context, fs *flag.FlagSet, args []string, commonFlags *common) error {
	name := fs.String("name", "", "pool or machine name the enrollment is recorded under (required)")
	tenant := fs.String("tenant", "", "exact tenant (required for a global operator)")
	ttl := fs.Duration("ttl", nodeEnrollmentTTLMax, "credential lifetime (1s to 10m)")
	idem := fs.String("idem", "", "stable idempotency key")
	out := fs.String("out", "", "exclusive private path to write the credential to")
	toStdout := fs.Bool("stdout", false, "print the credential once on stdout instead of writing a file")
	labels := kvFlag{}
	fs.Var(labels, "labels", "trusted placement label k=v bound to the enrollment (repeatable)")
	fs.Var(labels, "label", "alias for --labels")
	parse(fs, args)
	if err := arity(fs, 0, 1, "node enroll --name NAME [--tenant T] [--labels k=v] [--ttl 10m] (--out FILE | --stdout)"); err != nil {
		return err
	}
	// `node enroll web-1` reads as naturally as `--name web-1`; accept both
	// rather than making the obvious form an arity error.
	if fs.NArg() == 1 {
		if *name != "" && *name != fs.Arg(0) {
			return fmt.Errorf("node enroll names %q positionally and %q with --name", fs.Arg(0), *name)
		}
		*name = fs.Arg(0)
	}
	if strings.TrimSpace(*name) == "" {
		return errors.New("node enroll needs --name NAME")
	}
	// Naming the ceiling here rather than round-tripping to a bad_request:
	// the bound belongs to the credential's design, not to one deployment.
	if *ttl > nodeEnrollmentTTLMax || *ttl < time.Second {
		return fmt.Errorf("--ttl %s is outside the 1s to %s enrollment window", *ttl, nodeEnrollmentTTLMax)
	}
	if (*out != "") == *toStdout {
		return errors.New("node enroll needs exactly one of --out FILE and --stdout")
	}
	if *idem == "" {
		*idem = ids.New("idem")
	}
	// The destination is claimed before the credential exists. Minting first
	// and discovering the collision afterwards would leave a live one-time
	// credential nobody holds, counting against the deployment's enrollment
	// capacity until it expires.
	var target string
	var file *os.File
	if *out != "" {
		resolved, err := absFlagPath("out", *out)
		if err != nil {
			return err
		}
		target = resolved
		if file, err = reserveSecretFile(target); err != nil {
			return err
		}
		defer func() {
			if file != nil {
				_ = file.Close()
				_ = os.Remove(target)
			}
		}()
	}
	cl := commonFlags.client()
	defer cl.Close()
	res, err := cl.EnrollNode(ctx, *tenant, *name, labels, *ttl, *idem)
	if err != nil {
		return err
	}
	expires := time.UnixMilli(res.ExpiresAt).UTC().Format(time.RFC3339)
	if target != "" {
		writeErr := commitSecretFile(file, res.EnrollmentToken)
		file = nil
		if writeErr != nil {
			_ = os.Remove(target)
			return writeErr
		}
		if commonFlags.json {
			printJSON(map[string]any{
				"name": res.Name, "tenant": res.Tenant, "path": target, "expires_at": expires,
			})
			return nil
		}
		fmt.Fprintf(os.Stderr, "one-time enrollment for %s in tenant %s written privately to %s, expires %s\n",
			res.Name, res.Tenant, target, expires)
		fmt.Fprintf(os.Stderr, "start the node with: remount up --server %s --enrollment-file %s\n", commonFlags.server, target)
		return nil
	}
	if commonFlags.json {
		printJSON(map[string]any{
			"name": res.Name, "tenant": res.Tenant, "enrollment_token": res.EnrollmentToken, "expires_at": expires,
		})
		return nil
	}
	fmt.Println(res.EnrollmentToken)
	fmt.Fprintf(os.Stderr, "one-time enrollment for %s in tenant %s, expires %s (shown once; it is consumed by the first hello)\n",
		res.Name, res.Tenant, expires)
	return nil
}

// reserveSecretFile claims an exclusive private path, the same contract
// `--bootstrap-token-file` uses. O_EXCL rather than a truncating write:
// silently overwriting a file the operator already handed to a machine would
// strand that machine with a credential nobody can name any more.
func reserveSecretFile(path string) (*os.File, error) {
	file, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return nil, fmt.Errorf("create %s: %w", path, err)
	}
	if err := secureSecretFile(path); err != nil {
		_ = file.Close()
		_ = os.Remove(path)
		return nil, fmt.Errorf("secure %s: %w", path, err)
	}
	return file, nil
}

// commitSecretFile writes and durably closes a reserved file. The caller owns
// removing the path when this fails.
func commitSecretFile(file *os.File, secret string) error {
	_, err := fmt.Fprintf(file, "%s\n", secret)
	if syncErr := file.Sync(); err == nil {
		err = syncErr
	}
	if closeErr := file.Close(); err == nil {
		err = closeErr
	}
	return err
}

// writeSecretFile is reserve plus commit for a caller that already holds the
// secret.
func writeSecretFile(path, secret string) error {
	file, err := reserveSecretFile(path)
	if err != nil {
		return err
	}
	if err := commitSecretFile(file, secret); err != nil {
		_ = os.Remove(path)
		return err
	}
	return nil
}

// readSecretFile is the node side of writeSecretFile. It refuses a file any
// other local user can read because that enrollment has already failed at
// being one-time.
func readSecretFile(path string) (string, error) {
	info, err := os.Stat(path)
	if err != nil {
		return "", err
	}
	if err := validateSecretFilePermissions(path, info); err != nil {
		return "", err
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		return "", err
	}
	secret := strings.TrimSpace(string(raw))
	if secret == "" {
		return "", fmt.Errorf("%s is empty", path)
	}
	return secret, nil
}

// nodeCredential resolves what `remount up` presents in its hello.
//
// A file beats the ambient bearer on purpose: REMOUNT_TOKEN is commonly
// exported in an operator's shell for the client CLI, and a node that silently
// enrolled with an operator bearer instead of the credential the operator just
// named on the command line would be a confusing failure at best. The
// provisioner variable is last because it is set by machinery, not a person.
// The returned source names the winner for the log line; it never carries the
// credential itself.
func nodeCredential(explicit, enrollmentFile string) (token, source string, err error) {
	if path := strings.TrimSpace(enrollmentFile); path != "" {
		resolved, err := absFlagPath("enrollment-file", path)
		if err != nil {
			return "", "", err
		}
		secret, err := readSecretFile(resolved)
		if err != nil {
			return "", "", err
		}
		return secret, "enrollment-file", nil
	}
	if strings.TrimSpace(explicit) != "" {
		return explicit, "token", nil
	}
	if secret := strings.TrimSpace(os.Getenv("REMOUNT_ENROLL_TOKEN")); secret != "" {
		return secret, "REMOUNT_ENROLL_TOKEN", nil
	}
	return "", "none", nil
}
