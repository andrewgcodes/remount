package gvisor

import (
	"context"
	"os"
	"path/filepath"
	goruntime "runtime"
	"testing"
	"time"
)

func TestExecRuntimeDoesNotWaitForInheritedOutput(t *testing.T) {
	if goruntime.GOOS == "windows" {
		t.Skip("unavailable: the fake runsc binary is a POSIX shell script")
	}
	dir := t.TempDir()
	binary := filepath.Join(dir, "runsc")
	if err := os.WriteFile(binary, []byte("#!/bin/sh\nsleep 2 &\nexit 0\n"), 0o755); err != nil {
		t.Fatal(err)
	}
	runtime := execRuntime{binary: binary, root: dir}
	started := time.Now()
	if err := runtime.run(context.Background(), "create", "workspace"); err != nil {
		t.Fatal(err)
	}
	if elapsed := time.Since(started); elapsed >= time.Second {
		t.Fatalf("runsc command waited for inherited output descriptor: %v", elapsed)
	}
}
