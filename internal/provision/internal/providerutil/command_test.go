package providerutil

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestOSRunnerUsesStdinOverridesEnvironmentAndSanitizesFailure(t *testing.T) {
	dir := t.TempDir()
	helper := filepath.Join(dir, "helper")
	script := "#!/bin/sh\nread line\nprintf '%s|%s' \"$line\" \"$PROVISION_TEST_ENV\"\n"
	if err := os.WriteFile(helper, []byte(script), 0o700); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PROVISION_TEST_ENV", "old")
	out, err := (OSRunner{}).Run(context.Background(), Command{Executable: helper, Stdin: []byte("stdin-value\n"), Env: map[string]string{"PROVISION_TEST_ENV": "new"}})
	if err != nil {
		t.Fatal(err)
	}
	if string(out) != "stdin-value|new" {
		t.Fatalf("output=%q", out)
	}

	fail := filepath.Join(dir, "fail")
	const secret = "stderr-secret-canary"
	if err := os.WriteFile(fail, []byte("#!/bin/sh\necho '"+secret+"' >&2\nexit 1\n"), 0o700); err != nil {
		t.Fatal(err)
	}
	_, err = (OSRunner{}).Run(context.Background(), Command{Executable: fail})
	if err == nil || strings.Contains(err.Error(), secret) {
		t.Fatalf("failure leaked stderr: %v", err)
	}
}

func TestOSRunnerBoundsStdout(t *testing.T) {
	buffer := &boundedBuffer{remaining: 8}
	if n, err := buffer.Write([]byte("123456789")); err != nil || n != 9 {
		t.Fatalf("write n=%d err=%v", n, err)
	}
	if !buffer.overflow || string(buffer.Bytes()) != "12345678" {
		t.Fatalf("buffer=%q overflow=%t", buffer.Bytes(), buffer.overflow)
	}
	out, err := (OSRunner{}).Run(context.Background(), Command{Executable: "printf", Args: []string{"123456789"}, MaxOutput: 8})
	if err == nil || !strings.Contains(err.Error(), "exceeded") {
		t.Fatalf("expected command output bound, got output=%q err=%v", out, err)
	}
}
