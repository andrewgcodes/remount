#!/bin/sh
set -eu

unavailable() {
  echo "UNAVAILABLE: $*" >&2
  exit 77
}

[ "$(uname -s)" = Linux ] || unavailable "Firecracker requires Linux"
[ -r /dev/kvm ] && [ -w /dev/kvm ] || unavailable "/dev/kvm is not read-write"
command -v firecracker >/dev/null 2>&1 || unavailable "firecracker is not installed"
command -v jailer >/dev/null 2>&1 || unavailable "jailer is not installed"
[ -n "${REMOUNT_FIRECRACKER_KERNEL:-}" ] || unavailable "REMOUNT_FIRECRACKER_KERNEL is unset"
[ -n "${REMOUNT_FIRECRACKER_ROOTFS:-}" ] || unavailable "REMOUNT_FIRECRACKER_ROOTFS is unset"
[ -n "${REMOUNT_FIRECRACKER_CGROUP:-}" ] || unavailable "REMOUNT_FIRECRACKER_CGROUP is unset"
[ -f "$REMOUNT_FIRECRACKER_KERNEL" ] || unavailable "kernel image is not a regular file"
[ -f "$REMOUNT_FIRECRACKER_ROOTFS" ] || unavailable "rootfs image is not a regular file"
[ -d "$REMOUNT_FIRECRACKER_CGROUP" ] || unavailable "cgroup does not exist"

firecracker --version
jailer --version
echo "AVAILABLE: static Firecracker host prerequisites passed; run the full KVM lifecycle lane"
