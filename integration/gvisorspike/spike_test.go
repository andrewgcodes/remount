// Package gvisorspike tests script ordering, not Linux kernel enforcement.
package gvisorspike

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"
)

// Every privileged command is a shell function supplied through BASH_ENV.
// The real script stops at sandbox creation, before any denial probe. These
// contracts must never be described as an E4 host-conformance pass.
func TestPacketPolicyCommitsBeforeSandboxOrLinkActivation(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("unavailable: POSIX script contract requires bash")
	}
	bash, err := exec.LookPath("bash")
	if err != nil {
		t.Skip("unavailable: bash not installed")
	}
	script, err := filepath.Abs("../../scripts/gvisor-spike.sh")
	if err != nil {
		t.Fatal(err)
	}
	for _, mode := range []string{"guest", "fallback", "both-fail"} {
		t.Run(mode, func(t *testing.T) {
			dir := t.TempDir()
			bin := filepath.Join(dir, "bin")
			if err := os.Mkdir(bin, 0o700); err != nil {
				t.Fatal(err)
			}
			// No real ip/nft/runsc (or other privileged command) is reachable.
			for _, name := range []string{"mkdir", "cat"} {
				path, err := exec.LookPath(name)
				if err != nil {
					t.Skipf("unavailable: %s not installed", name)
				}
				if err := os.Symlink(path, filepath.Join(bin, name)); err != nil {
					t.Fatal(err)
				}
			}
			envFile := filepath.Join(dir, "functions.sh")
			const functions = `record() { printf '%s\n' "$*" >> "$CALLS"; }
id() { echo 0; }
mktemp() { echo "$FIXTURE"; }
ip() {
  record "ip $*"
  if [[ "$1 $2" == "netns exec" ]]; then
    shift 3
    case "$1" in
      nft) shift; nft "$@" ;;
      sh) return 0 ;;
      ip) shift; ip "$@" ;;
      *) return 98 ;;
    esac
  fi
}
nft() {
  record "nft $*"
  if [[ "$1" == "-f" ]]; then
    local policy
    policy=$(cat)
    record "$policy"
    if [[ "$policy" == *"hook egress"* && "$MODE" != guest ]]; then return 1; fi
    if [[ "$policy" == *"hook ingress"* && "$MODE" == both-fail ]]; then return 1; fi
  fi
}
runsc() {
  record "runsc $*"
  if [[ " $* " == *" create "* ]]; then return 66; fi
}
mountpoint() { return 1; }
setsid() { :; }
socat() { :; }
umount() { :; }
kill() { :; }
rm() { record "cleanup $*"; }
`
			if err := os.WriteFile(envFile, []byte(functions), 0o600); err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			cmd := exec.CommandContext(ctx, bash, script)
			calls := filepath.Join(dir, "calls")
			cmd.Env = []string{"PATH=" + bin, "BASH_ENV=" + envFile, "FIXTURE=" + filepath.Join(dir, "spike.abc123"),
				"CALLS=" + calls, "MODE=" + mode, "REMOUNT_GVISOR_ROOTFS=" + dir}
			out, err := cmd.CombinedOutput()
			exit, ok := err.(*exec.ExitError)
			if !ok {
				t.Fatalf("script did not stop at the injected boundary: %v\n%s", err, out)
			}
			data, err := os.ReadFile(calls)
			if err != nil {
				t.Fatal(err)
			}
			log := string(data)
			wantCode := 66
			if mode == "both-fail" {
				wantCode = 1
				if strings.Contains(log, " create ") {
					t.Fatalf("sandbox created without a packet policy:\n%s", log)
				}
			}
			if exit.ExitCode() != wantCode {
				t.Fatalf("exit = %d, want %d\n%s\n%s", exit.ExitCode(), wantCode, out, log)
			}
			// Address assignment is allowed, but activating the host peer
			// before the deliberately failed create is forbidden.
			for _, line := range strings.Split(log, "\n") {
				if strings.HasPrefix(line, "ip link set rmh-") && strings.HasSuffix(line, " up") {
					t.Fatalf("host link activated before successful sandbox creation: %s", line)
				}
			}
			hasFallback := strings.Contains(log, "hook ingress")
			if hasFallback != (mode != "guest") {
				t.Fatalf("unexpected fallback selection:\n%s", log)
			}
			for _, want := range []string{"policy drop", "ip daddr 169.254.251.1 tcp dport 17443 accept", "ip link delete rmh-", "ip netns delete rmspike-"} {
				if !strings.Contains(log, want) {
					t.Errorf("missing policy or cleanup %q:\n%s", want, log)
				}
			}
			if strings.Contains(string(out), "gVisor E4 spike passed") {
				t.Fatal("injected failure was reported as a host pass")
			}
		})
	}
}
