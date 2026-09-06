#!/bin/sh
# Build the binary in hand, boot it as a standalone deployment, judge it
# against the protocol manifest plus a runtime profile, and leave the JSON
# report, the markdown review document and the Plan B §6 evidence record
# behind.
#
# This is deliberately not `make conformance`. That target means the
# hostile-input and compromised-workspace unit suites under the race detector,
# CI depends on that meaning, and repointing it would silently replace one
# proof with another.
#
# usage: scripts/conformance-report.sh [OUTDIR] [PROFILE]
#
# Exit codes are the subcommand's: 0 conformant, 1 something failed,
# 2 nothing failed but something could not be observed.
set -eu

out=${1:-dist}
profile=${2:-dev}

root=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
cd "$root"
mkdir -p "$out"

bin="$out/remount-conformance-candidate"
CGO_ENABLED=0 go build -o "$bin" ./cmd/remount

# --launch self starts the binary that was just built in standalone mode on a
# free loopback port, judges it, and kills it again. The candidate under test
# is therefore an artifact of this commit rather than a library linked into a
# test process.
"$bin" conformance \
  --launch self \
  --profile "$profile" \
  --report "$out/conformance.json" \
  --markdown "$out/conformance.md" \
  --evidence "$out/conformance-evidence.json" \
  --scenario B33
