#!/usr/bin/env bash
# Run every gate .github/workflows/ci.yml runs, locally, before pushing.
#
# This exists because CI cannot always answer. The repository is private, so
# Actions minutes are metered, and a push that fails CI costs minutes and
# teaches nothing when the budget is spent. Finding the failure here is free.
#
# Mirrors the ci.yml jobs, in the order they are cheapest to fail:
#
#   test            gofmt, vet, lint-locks, the suite (both Go modules), the
#                   race lane
#   build           make dist, which cross-compiles every platform
#   static-analysis go mod verify/tidy, staticcheck, govulncheck, seeded fuzz
#   conformance     make conformance, bounded fuzzing
#
# It also vets for linux and windows, which ci.yml only covers implicitly
# through the windows-test job. That job has caught real defects a
# darwin-only vet does not see.
#
# Only ci.yml is mirrored. The SDK, web, isolation, KVM, vendor, MinIO, MCP
# and release workflows have their own triggers and prerequisites and are not
# part of this verdict.
#
# Usage:
#   scripts/verify-local.sh              everything
#   scripts/verify-local.sh fast         skip the race lane and conformance
#   scripts/verify-local.sh <name>...    run only the named gates
#
# Exit status:
#   0  every selected gate ran and passed
#   1  at least one gate failed
#   2  no gate failed, but a gate could not run (an analyzer is not
#      installed) so the verdict is incomplete; a check that cannot run is
#      unavailable, never passed
#   64 usage error
#
# so `if scripts/verify-local.sh` is a usable precondition. Every gate runs
# even when an earlier one fails: one full report beats a bisect through six
# pushes. Only the complete gate set, with every gate executed, is reported
# as safe to push; `fast` and named gates say which subset they covered.
set -uo pipefail

cd "$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)"

# The exact measurements ci.yml excludes on hosted runners, and why, are
# recorded in that workflow's "record hosted-runner measurement debt" step.
# A developer host is not a shared runner, so they are NOT skipped here: this
# script is the place those measurements are expected to hold.
SUITE_TIMEOUT="${SUITE_TIMEOUT:-900s}"
RACE_TIMEOUT="${RACE_TIMEOUT:-1800s}"

failed=0
unavailable=0
declare -a results=()
declare -a unavailable_names=()

bold=$(tput bold 2>/dev/null || true)
red=$(tput setaf 1 2>/dev/null || true)
green=$(tput setaf 2 2>/dev/null || true)
dim=$(tput setaf 8 2>/dev/null || true)
reset=$(tput sgr0 2>/dev/null || true)

gate() { # gate <name> <command...>
  local name="$1"; shift
  printf '%s==> %s%s\n' "$bold" "$name" "$reset"
  local start; start=$(date +%s)
  local log; log=$(mktemp)
  if "$@" >"$log" 2>&1; then
    local secs=$(( $(date +%s) - start ))
    printf '%s    ok%s %s(%ss)%s\n' "$green" "$reset" "$dim" "$secs" "$reset"
    results+=("ok   $name (${secs}s)")
  else
    local secs=$(( $(date +%s) - start ))
    printf '%s    FAILED%s %s(%ss)%s\n' "$red" "$reset" "$dim" "$secs" "$reset"
    # Show the tail; the interesting part of a Go failure is at the end.
    sed 's/^/      /' "$log" | tail -40
    results+=("FAIL $name (${secs}s)")
    failed=$((failed + 1))
  fi
  rm -f "$log"
}

# A gate whose tool is absent did not run. It is neither a pass nor a
# failure, and the aggregate verdict must say so rather than fold it into
# "all gates passed": CI installs these tools, so a local run that lacks them
# has not earned the checks CI will make.
unavailable_gate() { # unavailable_gate <name> <reason> <how to install>
  local name="$1" reason="$2" install="$3"
  printf '%s==> %s%s\n' "$bold" "$name" "$reset"
  printf '%s    UNAVAILABLE%s %s(%s)%s\n' "$red" "$reset" "$dim" "$reason" "$reset"
  printf '      %s\n' "$install"
  results+=("UNAV $name ($reason)")
  unavailable_names+=("$name")
  unavailable=$((unavailable + 1))
}

# --- individual gates, each mirroring one ci.yml step -----------------------

gate_gofmt() { gate "gofmt" bash -c 'test -z "$(gofmt -l .)" || { gofmt -l .; exit 1; }'; }
gate_vet()   { gate "vet (host)" go vet ./...; }

gate_crossvet() {
  for goos in linux windows darwin; do
    gate "vet ($goos)" env GOOS="$goos" go vet ./...
  done
}

gate_locks() { gate "lock discipline" ./scripts/lint-locks.sh .; }

# `make test` is the in-module suite plus `make public-api`, which compiles
# and tests the exported SDK from the separate integration/publicsdk module.
# Only an external module can prove the public packages avoid Go `internal`
# imports, so parity with the test gate needs both.
publicsdk_test() {
  ( cd integration/publicsdk && go test -count=1 -timeout "$SUITE_TIMEOUT" ./... )
}

gate_suite() {
  gate "suite" go test -p 1 -count=1 -timeout "$SUITE_TIMEOUT" ./...
  gate "suite (integration/publicsdk)" publicsdk_test
}

gate_race() {
  gate "race" go test -race -p 1 -count=1 -timeout "$RACE_TIMEOUT" ./...
}

gate_build() { gate "build (make dist)" make dist; }

gate_static() {
  gate "go mod verify" go mod verify
  gate "go mod tidy -diff" go mod tidy -diff
  if command -v staticcheck >/dev/null; then
    gate "staticcheck" staticcheck ./...
  else
    unavailable_gate "staticcheck" "not installed" "go install honnef.co/go/tools/cmd/staticcheck@v0.8.1"
  fi
  if command -v govulncheck >/dev/null; then
    gate "govulncheck" govulncheck ./...
  else
    unavailable_gate "govulncheck" "not installed" "go install golang.org/x/vuln/cmd/govulncheck@v1.7.0"
  fi
  gate "seeded fuzz corpus" go test ./... -run '^Fuzz'
}

gate_conformance() {
  gate "conformance" make conformance
  gate "bounded fuzzing" make fuzz FUZZTIME=10s
}

# --- selection --------------------------------------------------------------

run_all() {
  gate_gofmt; gate_vet; gate_crossvet; gate_locks
  gate_suite; gate_build; gate_static; gate_race; gate_conformance
}

run_fast() {
  gate_gofmt; gate_vet; gate_crossvet; gate_locks
  gate_suite; gate_build; gate_static
}

# Which set ran decides what the verdict may claim. Only `all` is the full
# ci.yml gate set; the others are named as the subset they are.
gate_set="${1:-all}"
case "$gate_set" in
  all)  run_all ;;
  fast) run_fast ;;
  *)
    gate_set="selected"
    for name in "$@"; do
      if ! declare -F "gate_$name" >/dev/null; then
        echo "unknown gate: $name" >&2
        echo "available: gofmt vet crossvet locks suite race build static conformance" >&2
        exit 64
      fi
    done
    for name in "$@"; do "gate_$name"; done
    ;;
esac

# --- report -----------------------------------------------------------------

echo
printf '%s--- summary ---%s\n' "$bold" "$reset"
for line in "${results[@]}"; do
  case "$line" in
    FAIL*|UNAV*) printf '%s%s%s\n' "$red" "$line" "$reset" ;;
    *)           printf '%s%s%s\n' "$green" "$line" "$reset" ;;
  esac
done

case "$gate_set" in
  all)      set_desc="the full ci.yml gate set" ;;
  fast)     set_desc="the fast gate set (race lane and conformance did not run)" ;;
  selected) set_desc="selected gates: $*" ;;
esac
ran=${#results[@]}

if [ "$failed" -gt 0 ]; then
  printf '\n%s%d of %d gate(s) failed in %s — do not push%s\n' "$red" "$failed" "$ran" "$set_desc" "$reset"
  exit 1
fi
if [ "$unavailable" -gt 0 ]; then
  printf '\n%s%d of %d gate(s) could not run in %s: %s — verification incomplete, not a pass%s\n' \
    "$red" "$unavailable" "$ran" "$set_desc" "${unavailable_names[*]}" "$reset"
  exit 2
fi
case "$gate_set" in
  all) printf '\n%sall %d ci.yml gates passed — safe to push%s\n' "$green" "$ran" "$reset" ;;
  *)   printf '\n%s%d gate(s) passed in %s — not the full ci.yml gate set; run without arguments before pushing%s\n' \
         "$green" "$ran" "$set_desc" "$reset" ;;
esac
exit 0
