package main

import (
	"context"
	"os"
	"path/filepath"
	"runtime"
	"testing"
)

// The range flag is the one place an operator states how much history to
// disclose, so its semantics are pinned rather than inferred: FROM..TO
// includes both endpoints, and nothing about it defaults.
func TestParseAuditRangeIsInclusiveAndExplicit(t *testing.T) {
	for _, tc := range []struct {
		spec       string
		from, to   uint64
		wantErrLog string
	}{
		{spec: "1..1", from: 1, to: 1},
		{spec: "10..20", from: 10, to: 20},
		{spec: " 10 .. 20 ", from: 10, to: 20},
		{spec: "", wantErrLog: "an omitted range must not default to everything"},
		{spec: "10", wantErrLog: "a single number is not a range"},
		{spec: "0..20", wantErrLog: "sequence 0 does not exist"},
		{spec: "20..10", wantErrLog: "a reversed range must not be silently swapped"},
		{spec: "10...20", wantErrLog: "a three-dot form must not be guessed at"},
		{spec: "a..20", wantErrLog: "a non-numeric endpoint must not be treated as zero"},
		{spec: "10..b", wantErrLog: "a non-numeric endpoint must not be treated as zero"},
	} {
		from, to, err := parseAuditRange(tc.spec)
		if tc.wantErrLog != "" {
			if err == nil {
				t.Errorf("parseAuditRange(%q) succeeded: %s", tc.spec, tc.wantErrLog)
			}
			continue
		}
		if err != nil || from != tc.from || to != tc.to {
			t.Errorf("parseAuditRange(%q) = (%d, %d, %v)", tc.spec, from, to, err)
		}
	}
	// The inclusive contract stated as arithmetic: 10..20 is eleven sequences,
	// not ten.
	from, to, err := parseAuditRange("10..20")
	if err != nil || to-from+1 != 11 {
		t.Fatalf("inclusive width = %d", to-from+1)
	}
}

// An audit bundle that silently replaced an earlier one would destroy the
// evidence it exists to preserve.
func TestWriteAuditBundleIsAtomicAndNeverClobbers(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "bundle.jsonl")
	if err := writeAuditBundle(context.Background(), path, []byte("first\n")); err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if runtime.GOOS == "windows" {
		t.Log("unavailable: Go file modes do not expose Windows ACL confidentiality")
	} else if info.Mode().Perm() != 0o600 {
		t.Fatalf("bundle mode = %v", info.Mode().Perm())
	}
	if err := writeAuditBundle(context.Background(), path, []byte("second\n")); err == nil {
		t.Fatal("a second export overwrote an existing bundle")
	}
	body, err := os.ReadFile(path)
	if err != nil || string(body) != "first\n" {
		t.Fatalf("bundle = %q err=%v", body, err)
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 1 {
		t.Fatalf("the failed export left staging behind: %d entries", len(entries))
	}
}
