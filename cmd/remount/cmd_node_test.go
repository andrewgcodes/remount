package main

import (
	"context"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

// TestWriteSecretFileIsExclusiveAndPrivate pins the two properties an
// enrollment file has to have: nobody else on the host can read it, and a
// second write never silently replaces a credential already handed out.
func TestWriteSecretFileIsExclusiveAndPrivate(t *testing.T) {
	path := filepath.Join(t.TempDir(), "enroll.token")
	if err := writeSecretFile(path, "enroll_abc"); err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if perm := info.Mode().Perm(); runtime.GOOS != "windows" && perm != 0o600 {
		t.Fatalf("enrollment file mode = %#o, want 0600", perm)
	}
	if err := validateSecretFilePermissions(path, info); err != nil {
		t.Fatalf("enrollment file permissions: %v", err)
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if got := strings.TrimSpace(string(raw)); got != "enroll_abc" {
		t.Fatalf("enrollment file content = %q", got)
	}
	if err := writeSecretFile(path, "enroll_def"); err == nil {
		t.Fatal("writeSecretFile overwrote an existing credential file")
	}
	// The refused write must not have truncated the original.
	raw, err = os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if got := strings.TrimSpace(string(raw)); got != "enroll_abc" {
		t.Fatalf("existing credential was disturbed: %q", got)
	}
}

func TestReadSecretFileRefusesAReadableFile(t *testing.T) {
	dir := t.TempDir()
	if runtime.GOOS != "windows" {
		open := filepath.Join(dir, "open.token")
		if err := os.WriteFile(open, []byte("enroll_abc\n"), 0o644); err != nil {
			t.Fatal(err)
		}
		if _, err := readSecretFile(open); err == nil || !strings.Contains(err.Error(), "0644") {
			t.Fatalf("readSecretFile on a world-readable path = %v", err)
		}
	}
	empty := filepath.Join(dir, "empty.token")
	if err := os.WriteFile(empty, []byte("\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := readSecretFile(empty); err == nil {
		t.Fatal("readSecretFile accepted an empty credential file")
	}
}

// TestNodeCredentialPrecedence is the rule `remount up` follows: the file the
// operator named wins over an ambient REMOUNT_TOKEN, which in turn wins over
// the variable a provisioner sets.
func TestNodeCredentialPrecedence(t *testing.T) {
	path := filepath.Join(t.TempDir(), "enroll.token")
	if err := writeSecretFile(path, "enroll_from_file"); err != nil {
		t.Fatal(err)
	}
	t.Setenv("REMOUNT_ENROLL_TOKEN", "enroll_from_provisioner")

	token, source, err := nodeCredential("operator-bearer", path)
	if err != nil {
		t.Fatal(err)
	}
	if token != "enroll_from_file" || source != "enrollment-file" {
		t.Fatalf("file did not win: token=%q source=%q", token, source)
	}
	if token, source, err = nodeCredential("operator-bearer", ""); err != nil {
		t.Fatal(err)
	} else if token != "operator-bearer" || source != "token" {
		t.Fatalf("explicit token did not win: token=%q source=%q", token, source)
	}
	if token, source, err = nodeCredential("", ""); err != nil {
		t.Fatal(err)
	} else if token != "enroll_from_provisioner" || source != "REMOUNT_ENROLL_TOKEN" {
		t.Fatalf("provisioner variable was not used: token=%q source=%q", token, source)
	}
	t.Setenv("REMOUNT_ENROLL_TOKEN", "")
	if token, source, err = nodeCredential("", ""); err != nil {
		t.Fatal(err)
	} else if token != "" || source != "none" {
		t.Fatalf("empty credential = %q from %q", token, source)
	}
	if _, _, err = nodeCredential("", filepath.Join(t.TempDir(), "missing")); err == nil {
		t.Fatal("a named enrollment file that does not exist must be an error, not a fallback")
	}
}

// TestNodeEnrollFlagValidation checks every refusal that happens before a
// connection is attempted, so a mistyped command fails locally and instantly.
func TestNodeEnrollFlagValidation(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	cases := []struct {
		name string
		args []string
		want string
	}{
		{"no subcommand", []string{}, "enroll|ls"},
		{"unknown subcommand", []string{"frobnicate"}, "unknown node subcommand"},
		{"no name", []string{"enroll", "--tenant", "acme", "--stdout"}, "--name"},
		{"ttl too long", []string{"enroll", "--name", "n1", "--ttl", "1h", "--stdout"}, "enrollment window"},
		{"ttl too short", []string{"enroll", "--name", "n1", "--ttl", "0s", "--stdout"}, "enrollment window"},
		{"no destination", []string{"enroll", "--name", "n1"}, "exactly one"},
		{"both destinations", []string{"enroll", "--name", "n1", "--stdout", "--out", filepath.Join(dir, "a")}, "exactly one"},
		{"conflicting names", []string{"enroll", "other", "--name", "n1", "--stdout"}, "positionally"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := cmdNode(ctx, tc.args)
			if err == nil {
				t.Fatalf("cmdNode(%v) succeeded; it must fail before dialing", tc.args)
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("cmdNode(%v) error = %v, want it to mention %q", tc.args, err, tc.want)
			}
		})
	}
	// A destination that already exists is refused before any credential is
	// minted, so a collision never strands a live one-time credential nobody
	// holds. --server points at a closed port: reaching the control plane at
	// all would show up as a dial error rather than this one.
	taken := filepath.Join(dir, "taken.enroll")
	if err := writeSecretFile(taken, "enroll_existing"); err != nil {
		t.Fatal(err)
	}
	err := cmdNode(ctx, []string{"enroll", "--name", "n1", "--out", taken, "--server", "http://127.0.0.1:1"})
	if err == nil || !strings.Contains(err.Error(), "file exists") {
		t.Fatalf("enroll onto an existing path = %v, want a local refusal", err)
	}
	if raw, readErr := os.ReadFile(taken); readErr != nil || strings.TrimSpace(string(raw)) != "enroll_existing" {
		t.Fatalf("the existing credential was disturbed: %q %v", raw, readErr)
	}

	// A positional name is equivalent to --name, so this one gets past
	// validation and fails on the connection instead.
	if err := cmdNode(ctx, []string{"enroll", "n1", "--stdout", "--server", "http://127.0.0.1:1"}); err == nil {
		t.Fatal("a positional name should reach the control plane and fail there")
	} else if strings.Contains(err.Error(), "--name") {
		t.Fatalf("a positional name was rejected as missing: %v", err)
	}
}
