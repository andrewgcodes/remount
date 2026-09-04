#!/usr/bin/env bash
set -euo pipefail

if [[ "$(uname -s)" != Linux ]]; then
  echo "UNAVAILABLE: Firecracker conformance requires Linux"
  exit 77
fi
for name in \
  REMOUNT_FIRECRACKER_DATA_ROOT \
  REMOUNT_FIRECRACKER_BINARY \
  REMOUNT_FIRECRACKER_JAILER \
  REMOUNT_FIRECRACKER_CGROUP_PARENT \
  REMOUNT_FIRECRACKER_KERNEL \
  REMOUNT_FIRECRACKER_ROOTFS \
  REMOUNT_FIRECRACKER_GUEST_MANIFEST
do
  if [[ -z "${!name:-}" ]]; then
    echo "UNAVAILABLE: $name is required"
    exit 77
  fi
done
for path in \
  "$REMOUNT_FIRECRACKER_BINARY" \
  "$REMOUNT_FIRECRACKER_JAILER" \
  "$REMOUNT_FIRECRACKER_KERNEL" \
  "$REMOUNT_FIRECRACKER_ROOTFS" \
  "$REMOUNT_FIRECRACKER_GUEST_MANIFEST"
do
  if [[ ! -f "$path" ]]; then
    echo "UNAVAILABLE: required artifact does not exist: $path"
    exit 77
  fi
done
if [[ ! -d "$REMOUNT_FIRECRACKER_DATA_ROOT" ]]; then
  echo "UNAVAILABLE: required data root does not exist: $REMOUNT_FIRECRACKER_DATA_ROOT"
  exit 77
fi
if ! sudo -n true 2>/dev/null; then
	echo "UNAVAILABLE: passwordless sudo is required for jailer, cgroups, TAP and netns"
	exit 77
fi
if ! sudo -n test -r /dev/kvm || ! sudo -n test -w /dev/kvm; then
  echo "UNAVAILABLE: /dev/kvm is not readable and writable by the privileged jailer launcher"
  exit 77
fi

test_binary="$(mktemp "${TMPDIR:-/tmp}/remount-firecracker-test.XXXXXX")"
trap 'rm -f "$test_binary"' EXIT

printf 'host='
uname -srmo
"$REMOUNT_FIRECRACKER_BINARY" --version
"$REMOUNT_FIRECRACKER_JAILER" --version
sha256sum \
  "$REMOUNT_FIRECRACKER_BINARY" \
  "$REMOUNT_FIRECRACKER_JAILER" \
  "$REMOUNT_FIRECRACKER_KERNEL" \
  "$REMOUNT_FIRECRACKER_ROOTFS" \
  "$REMOUNT_FIRECRACKER_GUEST_MANIFEST"

go test -count=1 ./internal/workspace/firecracker
go test -c -o "$test_binary" ./internal/workspace/firecracker
sudo -n env \
  REMOUNT_FIRECRACKER_INTEGRATION=1 \
  REMOUNT_FIRECRACKER_DATA_ROOT="$REMOUNT_FIRECRACKER_DATA_ROOT" \
  REMOUNT_FIRECRACKER_BINARY="$REMOUNT_FIRECRACKER_BINARY" \
  REMOUNT_FIRECRACKER_JAILER="$REMOUNT_FIRECRACKER_JAILER" \
  REMOUNT_FIRECRACKER_CGROUP_PARENT="$REMOUNT_FIRECRACKER_CGROUP_PARENT" \
  REMOUNT_FIRECRACKER_KERNEL="$REMOUNT_FIRECRACKER_KERNEL" \
  REMOUNT_FIRECRACKER_ROOTFS="$REMOUNT_FIRECRACKER_ROOTFS" \
  REMOUNT_FIRECRACKER_GUEST_MANIFEST="$REMOUNT_FIRECRACKER_GUEST_MANIFEST" \
  "$test_binary" \
  -test.v -test.count=1 \
  -test.run '^(TestB29FirecrackerHostSmoke|TestB29FirecrackerCheckpointMoveRestore|TestDiskFullRestoreStagingFailsClosedAndCleansUp)$' \
  -test.timeout=5m

if ps -eo comm= | rg -q '^(firecracker|jailer)(-|$)'; then
  echo "FAIL: retained Firecracker or jailer process"
  exit 1
fi
if sudo -n find /run/remount/netns -mindepth 1 -maxdepth 1 -print -quit 2>/dev/null | rg -q .; then
  echo "FAIL: retained Remount network namespace"
  exit 1
fi
if ip -o link show | rg -q 'rmh|rmg|rmte'; then
  echo "FAIL: retained Remount network link"
  exit 1
fi
if sudo -n nft list tables | rg -q 'remount_'; then
  echo "FAIL: retained Remount nftables table"
  exit 1
fi
cgroup_parent="/sys/fs/cgroup/${REMOUNT_FIRECRACKER_CGROUP_PARENT#/sys/fs/cgroup/}"
if sudo -n find "$cgroup_parent" -mindepth 1 -maxdepth 1 -type d -name 'rm-*' -print -quit 2>/dev/null | rg -q .; then
  echo "FAIL: retained Remount Firecracker cgroup"
  exit 1
fi

echo "PASS: Firecracker B29 exact-host conformance, lifecycle failure proofs and cleanup"
