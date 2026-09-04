#!/bin/sh
set -eu

: "${REMOUNT_SCALE_CURSORS:=10000}"
export REMOUNT_SCALE_CURSORS

go test -race -count=1 -run '^TestPlanBScale' -v -timeout 30m ./internal/sim
