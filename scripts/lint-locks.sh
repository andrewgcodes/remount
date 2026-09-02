#!/bin/sh
# lint-locks.sh — find reads of mutex-guarded state after the mutex is released.
#
# The bug this catches, which cost real debugging time and only showed up
# intermittently under the race detector:
#
#     if ws.State != proto.WSPending {
#         c.mu.Unlock()
#         return nil, proto.Err(..., ws.State)   // read after unlock
#     }
#
# It survives review because the unlock and the read are adjacent and the read
# looks like part of the same critical section.
#
# The scan walks forward from each Unlock and stops at the first return or
# closing brace, because an unlock on an early-return branch does not unlock
# the path that follows the branch. Anything read before that stop is read
# without the lock.
set -eu

ROOT="${1:-.}"
STATUS=0

for f in $(find "$ROOT" -name '*.go' -not -name '*_test.go' -not -path '*/.git/*'); do
	awk -v file="$f" '
		function strip(l) { sub(/^[ \t]+/, "", l); return l }

		# Start scanning after any unlock. A deferred unlock holds the lock
		# for the rest of the function, so it is not one.
		/\.mu\.Unlock\(\)|\.mu\.RUnlock\(\)|[a-zA-Z]\.Unlock\(\)|[a-zA-Z]\.RUnlock\(\)/ {
			if ($0 ~ /(^|[^A-Za-z0-9_])defer([^A-Za-z0-9_])/) next
			scanning = 1
			at = NR
			next
		}

		# An explicit, reasoned suppression ends the scan.
		/lint:locks-ok/ { scanning = 0; next }

		scanning {
			line = strip($0)

			# A guarded field read. Short receivers are the ones the locks in
			# this codebase protect.
			if (line !~ /^\/\// && line !~ /:=/ &&
			    line ~ /(^|[^A-Za-z0-9_])(ws|nd|st)\.[A-Z][A-Za-z]*/) {
				printf "%s:%d: reads guarded state %d line(s) after Unlock: %s\n",
					file, NR, NR - at, line
				bad = 1
				scanning = 0
				next
			}

			# An unlock followed by a return was a branch-local unlock. The
			# code after the branch is still holding the lock.
			if (line ~ /(^|[^A-Za-z0-9_])return([^A-Za-z0-9_]|$)/) { scanning = 0; next }
			if (line ~ /^\}/) { scanning = 0; next }
			if (NR - at >= 3) { scanning = 0 }
		}

		END { exit bad ? 1 : 0 }
	' "$f" || STATUS=1
done

if [ "$STATUS" -ne 0 ]; then
	echo ""
	echo "Hoist the value into a local before unlocking:"
	echo "    state := ws.State"
	echo "    c.mu.Unlock()"
	echo "    return fmt.Errorf(\"... %s\", state)"
	exit 1
fi
echo "lock discipline: no reads of guarded state after unlock"
