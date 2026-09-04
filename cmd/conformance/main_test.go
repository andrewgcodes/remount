package main

import (
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// capture runs the command with args, sending stdout to a temporary file so
// the assertion reads exactly what a caller would see.
func capture(t *testing.T, args ...string) (string, error) {
	t.Helper()
	out, err := os.CreateTemp(t.TempDir(), "stdout")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = out.Close() }()
	runErr := run(args, out, os.Stderr)
	body, err := os.ReadFile(out.Name())
	if err != nil {
		t.Fatal(err)
	}
	return string(body), runErr
}

func TestManifestFlagPrintsTheContractWithoutATarget(t *testing.T) {
	body, err := capture(t, "--manifest")
	if err != nil {
		t.Fatalf("--manifest needed a target: %v", err)
	}
	for _, want := range []string{
		"remount conformance manifest", "protocol v1",
		"CONF-NEG-001", "CONF-SESS-004", "CONF-EVT-010",
		"required", "capability-gated", "extension",
		"PROTOCOL.md",
	} {
		if !strings.Contains(body, want) {
			t.Errorf("the printed manifest does not mention %q", want)
		}
	}
}

func TestNamingNoTargetIsAnError(t *testing.T) {
	_, err := capture(t)
	if err == nil {
		t.Fatal("the command judged something without being told what")
	}
	if !strings.Contains(err.Error(), "name a target") {
		t.Fatalf("the error does not say what is missing: %v", err)
	}
}

func TestUnreachableEndpointIsReportedNotIgnored(t *testing.T) {
	// Port 1 on loopback is reserved and refuses connections, so this is a
	// deterministic "the target is not there" rather than a flake.
	_, err := capture(t, "--endpoint", "http://127.0.0.1:1", "--connect-timeout", "500ms")
	if err == nil {
		t.Fatal("an endpoint that is not listening produced no error")
	}
	if !strings.Contains(err.Error(), "healthy") && !strings.Contains(err.Error(), "connect") {
		t.Fatalf("the error does not say the target was unreachable: %v", err)
	}
}

func TestSplitAndFirst(t *testing.T) {
	if got := split(" CONF-NEG-001 , ,CONF-WS-002 "); len(got) != 2 || got[0] != "CONF-NEG-001" || got[1] != "CONF-WS-002" {
		t.Fatalf("split returned %v", got)
	}
	if split("") != nil {
		t.Fatal("split of an empty string is not empty")
	}
	if first("", "fallback") != "fallback" || first("value", "fallback") != "value" {
		t.Fatal("first did not prefer the non-empty value")
	}
}

func TestWriteFileCreatesAndReports(t *testing.T) {
	path := filepath.Join(t.TempDir(), "out.txt")
	if err := writeFile(path, func(w io.Writer) error {
		_, err := w.Write([]byte("conformance"))
		return err
	}); err != nil {
		t.Fatal(err)
	}
	body, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if string(body) != "conformance" {
		t.Fatalf("wrote %q", body)
	}
}
