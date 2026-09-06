#!/bin/sh
set -eu

: "${REMOUNT_GVISOR_ROOTFS:?REMOUNT_GVISOR_ROOTFS is required}"
: "${REMOUNT_CHAOS_IMAGE:?REMOUNT_CHAOS_IMAGE is required}"
if [ "${REMOUNT_GVISOR_INTEGRATION:-}" != "1" ]; then
  echo "REMOUNT_GVISOR_INTEGRATION=1 is required" >&2
  exit 2
fi
test -x "$REMOUNT_GVISOR_ROOTFS/rawprobe"
test -x "$REMOUNT_GVISOR_ROOTFS/udpprobe"

root=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
tmp=$(mktemp -d)
trap 'rm -rf "$tmp"' EXIT HUP INT TERM

cd "$root"
sh integration/chaos/backend-gates.sh --probe
case "${1:-all}" in
  e4|all)
    sudo -n env REMOUNT_GVISOR_ROOTFS="$REMOUNT_GVISOR_ROOTFS" bash scripts/gvisor-spike.sh
    go test -c -o "$tmp/gvisor.test" ./internal/workspace/gvisor
    sudo -n env REMOUNT_GVISOR_INTEGRATION=1 REMOUNT_GVISOR_ROOTFS="$REMOUNT_GVISOR_ROOTFS" \
      "$tmp/gvisor.test" -test.run '^TestE4.*Conformance$' -test.v -test.timeout=2m
    ;;
esac
case "${1:-all}" in
  e5|all)
    if [ ! -x "$tmp/gvisor.test" ]; then
      go test -c -o "$tmp/gvisor.test" ./internal/workspace/gvisor
    fi
    sudo -n env REMOUNT_GVISOR_INTEGRATION=1 REMOUNT_GVISOR_ROOTFS="$REMOUNT_GVISOR_ROOTFS" \
      "$tmp/gvisor.test" -test.run '^TestE5SiblingTenantsCannotReachEachOther$' -test.v -test.timeout=3m
    go test -c -o "$tmp/node.test" ./internal/node
    sudo -n env REMOUNT_GVISOR_INTEGRATION=1 REMOUNT_GVISOR_ROOTFS="$REMOUNT_GVISOR_ROOTFS" \
      "$tmp/node.test" -test.run '^TestE5TenantIsolationConformance$' -test.v -test.timeout=2m
    ;;
  e4|drift) ;;
  *)
    echo "usage: $0 [e4|e5|drift|all]" >&2
    exit 2
    ;;
esac
# E26: a real gVisor node claiming multi-tenant-isolated loses a host
# prerequisite out of band and the control plane stops scheduling on it. The
# test drives the whole loop through a control plane, so it lives in
# internal/sim rather than in the backend package.
case "${1:-all}" in
  drift|all)
    go test -c -o "$tmp/sim.test" ./internal/sim
    sudo -n env REMOUNT_GVISOR_INTEGRATION=1 REMOUNT_GVISOR_ROOTFS="$REMOUNT_GVISOR_ROOTFS" \
      "$tmp/sim.test" -test.run '^TestE26ProfileDriftMakesNodeUnschedulable$' -test.v -test.timeout=6m
    ;;
esac
