#!/bin/sh
set -eu

# 10,000 is the machine-sized width recorded in the verification ledger; the
# hosted plan-b workflow sets a runner-sized width in its environment.
: "${REMOUNT_SCALE_CURSORS:=10000}"
export REMOUNT_SCALE_CURSORS

# Keep resource measurements representative; make race remains a separate gate.
go test -count=1 -run '^TestPlanBScale' -v -timeout 30m ./internal/sim
