#!/bin/sh
# Start command for the Remount E2B node template.
#
# E2B snapshots a template after this command has started and resumes every
# sandbox from that snapshot, so nothing passed at sandbox creation reaches
# this process as an environment variable. The Remount control plane's E2B
# driver instead writes the bootstrap values to BOOT through the sandbox's
# envd file API right after creation. This script waits for that file,
# imports it, removes it, and execs the node.
BOOT="${REMOUNT_BOOTSTRAP_FILE:-/home/user/remount-bootstrap.env}"
BIN=/usr/local/bin/remount
while [ ! -s "$BOOT" ]; do sleep 1; done
set -a
. "$BOOT"
set +a
rm -f "$BOOT"
if [ -n "${REMOUNT_BINARY_URL:-}" ] && [ ! -x "$BIN" ]; then
  curl -fsSL "$REMOUNT_BINARY_URL" -o "$BIN" && chmod +x "$BIN"
fi
DATA="${REMOUNT_DATA_DIR:-/var/lib/remount}"
mkdir -p "$DATA"
export PATH="/usr/local/bin:$PATH"
export REMOUNT_GVISOR_ROOTFS="${REMOUNT_GVISOR_ROOTFS:-/opt/remount/rootfs}"
set -- up --server "$REMOUNT_SERVER" --backend "${REMOUNT_BACKEND:-process}" --data "$DATA" --label vendor=e2b
[ -n "${REMOUNT_NODE_ID:-}" ] && set -- "$@" --node-id "$REMOUNT_NODE_ID"
# E2B keeps no start-command log an operator can read later, so the node
# writes its own next to its state.
exec "$BIN" "$@" >>"$DATA/node.log" 2>&1
