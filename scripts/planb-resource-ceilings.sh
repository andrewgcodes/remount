#!/bin/sh
set -eu

: "${REMOUNT_SCALE_CURSORS:=10000}"
export REMOUNT_SCALE_CURSORS

# Keep resource measurements representative; make race remains a separate gate.
go test -count=1 -run '^TestPlanBScale' -v -timeout 30m ./internal/sim
