#!/bin/sh
# smoke.sh — prove the reference stack actually composes, rather than merely
# starting. Every assertion is a property an operator would notice losing:
# both nodes joined the fleet, a workspace ran real work, and it moved between
# nodes without losing its filesystem.
#
# A stack that is "up" but cannot place a workspace is not a working
# deployment, so this exits non-zero rather than printing a warning.
set -eu

compose_file="${COMPOSE_FILE:-deploy/compose/compose.yaml}"
token="${REMOUNT_TOKEN:-reference-development-token}"
port="${REMOUNT_PORT:-7443}"
server="http://127.0.0.1:${port}"

fail() { printf 'smoke: %s\n' "$1" >&2; exit 1; }

run() {
  # The control container holds the binary, so the smoke uses it rather than
  # requiring a matching host build. Server and token go through the
  # environment, not trailing flags: `exec WS -- cmd` passes everything after
  # `--` to the workspace, so appended flags would silently become arguments
  # to the command being run instead of to remount.
  docker compose -f "$compose_file" exec -T \
    -e REMOUNT_SERVER=http://127.0.0.1:7443 \
    -e REMOUNT_TOKEN="$token" \
    control /usr/local/bin/remount "$@"
}

printf 'smoke: waiting for the control plane\n'
i=0
while [ "$i" -lt 60 ]; do
  if curl -sf "$server/healthz" >/dev/null 2>&1; then break; fi
  i=$((i + 1))
  sleep 1
done
[ "$i" -lt 60 ] || fail "control plane never became reachable at $server"

health=$(curl -sf "$server/healthz") || fail "healthz failed"
printf 'smoke: healthz %s\n' "$health"
case "$health" in
  *'"serving":true'*) ;;
  *) fail "control plane is reachable but not serving: $health" ;;
esac

printf 'smoke: waiting for both nodes to join\n'
i=0
online=0
while [ "$i" -lt 60 ]; do
  # `nodes` prints an ONLINE column whose value is true/false, so count rows
  # rather than matching the word: the header would otherwise count as a node.
  online=$(run nodes 2>/dev/null | awk 'NR>1 && $2=="true"' | wc -l | tr -d ' ')
  [ "$online" -ge 2 ] && break
  i=$((i + 1))
  sleep 1
done
[ "$online" -ge 2 ] || fail "only $online node(s) joined the fleet; the stack composes two"
printf 'smoke: %s nodes online\n' "$online"

printf 'smoke: creating a workspace\n'
run ws create --name smoke >/dev/null 2>&1 || fail "ws create failed"
ws=$(run ws ls 2>/dev/null | awk '$2=="smoke"{print $1; exit}')
[ -n "$ws" ] || fail "the created workspace is not in ws ls"
printf 'smoke: workspace %s\n' "$ws"

printf 'smoke: running a command in it\n'
run exec "$ws" -- sh -c 'printf composed > marker.txt' >/dev/null 2>&1 \
  || fail "could not write into the workspace"

out=$(run exec "$ws" -- cat marker.txt 2>/dev/null | tail -1)
case "$out" in
  *composed*) printf 'smoke: workspace filesystem reads back %s\n' "$out" ;;
  *) fail "workspace did not read back what was written, got '$out'" ;;
esac

before=$(run ws get "$ws" 2>/dev/null | awk '/node/{print $2; exit}')
printf 'smoke: moving the workspace off node %s\n' "${before:-unknown}"
run ws move "$ws" >/dev/null 2>&1 || fail "ws move failed"

i=0
while [ "$i" -lt 60 ]; do
  state=$(run ws ls 2>/dev/null | awk -v w="$ws" '$1==w{print $3}')
  [ "$state" = "claimed" ] && break
  i=$((i + 1))
  sleep 1
done
[ "$state" = "claimed" ] || fail "workspace did not return to claimed after the move (state=$state)"

after=$(run exec "$ws" -- cat marker.txt 2>/dev/null | tail -1)
case "$after" in
  *composed*) ;;
  *) fail "the filesystem did not survive the move, got '$after'" ;;
esac
printf 'smoke: filesystem survived the move\n'

run ws destroy "$ws" >/dev/null 2>&1 || fail "ws destroy failed"
printf 'smoke: OK — two nodes, real workspace, real move, filesystem intact\n'
