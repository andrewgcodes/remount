#!/usr/bin/env bash
# Run every gate .github/workflows/ci.yml runs, locally, before pushing.
#
# This exists because CI cannot always answer. The repository is private, so
# Actions minutes are metered, and a push that fails CI costs minutes and
# teaches nothing when the budget is spent. Finding the failure here is free.
#
# Mirrors the ci.yml jobs, in the order they are cheapest to fail:
#
#   test            gofmt, vet, lint-locks, the suite, the race lane
#   build           make dist, which cross-compiles every platform
#   static-analysis go mod verify/tidy, staticcheck, govulncheck, seeded fuzz
#   conformance     make conformance, bounded fuzzing
#
# It also vets for linux and windows, which ci.yml only covers implicitly
# through the windows-test job. That job has caught real defects a
# darwin-only vet does not see.
#
# Usage:
#   scripts/verify-local.sh              everything
#   scripts/verify-local.sh fast         skip the race lane and conformance
#   scripts/verify-local.sh <name>...    run only the named gates
#
# Exit status is the number of failed gates, so `if scripts/verify-local.sh`
# is a usable precondition. Every gate runs even when an earlier one fails:
# one full report beats a bisect through six pushes.
set -uo pipefail

cd "$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)"

# The exact measurements ci.yml excludes on hosted runners, and why, are
# recorded in that workflow's "record hosted-runner measurement debt" step.
# A developer host is not a shared runner, so they are NOT skipped here: this
# script is the place those measurements are expected to hold.
SUITE_TIMEOUT="${SUITE_TIMEOUT:-900s}"
RACE_TIMEOUT="${RACE_TIMEOUT:-1800s}"

failed=0
declare -a results=()

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

# --- individual gates, each mirroring one ci.yml step -----------------------

gate_gofmt() { gate "gofmt" bash -c 'test -z "$(gofmt -l .)" || { gofmt -l .; exit 1; }'; }
gate_vet()   { gate "vet (host)" go vet ./...; }

gate_crossvet() {
  for goos in linux windows darwin; do
    gate "vet ($goos)" env GOOS="$goos" go vet ./...
  done
}

gate_locks() { gate "lock discipline" ./scripts/lint-locks.sh .; }

gate_suite() {
  gate "suite" go test -p 1 -count=1 -timeout "$SUITE_TIMEOUT" ./...
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
    printf '%s==> staticcheck%s\n%s    skipped: not installed%s\n' "$bold" "$reset" "$dim" "$reset"
    printf '      go install honnef.co/go/tools/cmd/staticcheck@v0.8.1\n'
    results+=("skip staticcheck (not installed)")
  fi
  if command -v govulncheck >/dev/null; then
    gate "govulncheck" govulncheck ./...
  else
    printf '%s==> govulncheck%s\n%s    skipped: not installed%s\n' "$bold" "$reset" "$dim" "$reset"
    printf '      go install golang.org/x/vuln/cmd/govulncheck@v1.7.0\n'
    results+=("skip govulncheck (not installed)")
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

case "${1:-all}" in
  all)  run_all ;;
  fast) run_fast ;;
  *)
    for name in "$@"; do
      if declare -F "gate_$name" >/dev/null; then
        "gate_$name"
      else
        echo "unknown gate: $name" >&2
        echo "available: gofmt vet crossvet locks suite race build static conformance" >&2
        exit 2
      fi
    done
    ;;
esac

# --- report -----------------------------------------------------------------

echo
printf '%s--- summary ---%s\n' "$bold" "$reset"
for line in "${results[@]}"; do
  case "$line" in
    FAIL*) printf '%s%s%s\n' "$red" "$line" "$reset" ;;
    skip*) printf '%s%s%s\n' "$dim" "$line" "$reset" ;;
    *)     printf '%s%s%s\n' "$green" "$line" "$reset" ;;
  esac
done

if [ "$failed" -eq 0 ]; then
  printf '\n%sall gates passed — safe to push%s\n' "$green" "$reset"
else
  printf '\n%s%d gate(s) failed — do not push%s\n' "$red" "$failed" "$reset"
fi
exit "$failed"
