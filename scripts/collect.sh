#!/bin/sh
# collect.sh — dump everything about a Remount deployment as one JSON document.
#
# Written for a machine reader. An agent debugging a deployment can run this
# once and have the whole picture in its context, instead of issuing a dozen
# commands and stitching the output together.
#
#   scripts/collect.sh                      > snapshot.json
#   scripts/collect.sh --deep               > snapshot.json   # re-hashes artifacts
#   scripts/collect.sh --ws ws_abc          > one-workspace.json
#
# Environment: REMOUNT_SERVER, REMOUNT_TOKEN, and REMOUNT (path to the binary).
set -eu

REMOUNT="${REMOUNT:-remount}"
if ! command -v "$REMOUNT" >/dev/null 2>&1 && [ ! -x "$REMOUNT" ]; then
	echo "collect.sh: remount binary not found; set REMOUNT=./remount or put remount on PATH" >&2
	exit 2
fi
DEEP=""
ONLY_WS=""

while [ $# -gt 0 ]; do
	case "$1" in
	--deep) DEEP="--deep" ;;
	--ws) ONLY_WS="$2"; shift ;;
	-h | --help)
		sed -n '2,12p' "$0" | sed 's/^# \{0,1\}//'
		exit 0
		;;
	*)
		echo "unknown flag: $1" >&2
		exit 2
		;;
	esac
	shift
done

# Every remount subcommand supports --json, so this is a matter of labelling
# and concatenating rather than parsing.
emit() {
	# emit KEY COMMAND...  — runs the command, wraps failures as an error object
	key="$1"
	shift
	printf '  "%s": ' "$key"
	if out=$("$@" --json 2>/dev/null); then
		if [ -n "$out" ]; then
			printf '%s' "$out"
		else
			printf 'null'
		fi
	else
		printf '{"error": "command failed: %s"}' "$*"
	fi
}

printf '{\n'
printf '  "collected_at": "%s",\n' "$(date -u +%Y-%m-%dT%H:%M:%SZ)"
printf '  "server": "%s",\n' "${REMOUNT_SERVER:-http://127.0.0.1:7443}"

emit status "$REMOUNT" status
printf ',\n'
emit nodes "$REMOUNT" nodes
printf ',\n'
emit workspaces "$REMOUNT" ws ls
printf ',\n'
emit timers "$REMOUNT" timers
printf ',\n'
emit metrics "$REMOUNT" metrics
printf ',\n'

# doctor exits non-zero when it finds problems, which is correct for a health
# check and unhelpful here, so the status is captured rather than propagated.
printf '  "doctor": '
if out=$("$REMOUNT" doctor $DEEP --json 2>/dev/null); then
	printf '%s' "$out"
else
	rc=$?
	if [ -n "${out:-}" ]; then
		printf '%s' "$out"
	else
		printf '{"error": "doctor failed", "exit": %d}' "$rc"
	fi
fi
printf ',\n'

# Per-workspace detail. This is where the expensive information lives, so it
# is limited unless one workspace was named.
printf '  "inspect": {\n'
first=1
if [ -n "$ONLY_WS" ]; then
	ids="$ONLY_WS"
else
	ids=$("$REMOUNT" ws ls --json 2>/dev/null |
		sed -n 's/.*"id": *"\([^"]*\)".*/\1/p' | head -25)
fi
for id in $ids; do
	[ -z "$id" ] && continue
	[ $first -eq 0 ] && printf ',\n'
	first=0
	printf '    "%s": ' "$id"
	if out=$("$REMOUNT" inspect "$id" --json 2>/dev/null); then
		printf '%s' "$out"
	else
		printf '{"error": "inspect failed"}'
	fi
done
printf '\n  }\n'
printf '}\n'
