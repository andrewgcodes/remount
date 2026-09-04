#!/bin/sh
set -eu

unavailable() {
  echo "UNAVAILABLE: $*" >&2
  exit 77
}

[ "$(uname -s)" = Linux ] || unavailable "Firecracker requires Linux"
sudo -n test -r /dev/kvm && sudo -n test -w /dev/kvm || unavailable "/dev/kvm is not read-write by the privileged jailer launcher"
firecracker=${REMOUNT_FIRECRACKER_BINARY:-$(command -v firecracker || true)}
jailer=${REMOUNT_FIRECRACKER_JAILER:-$(command -v jailer || true)}
[ -n "$firecracker" ] && [ -x "$firecracker" ] || unavailable "an executable Firecracker binary is not configured"
[ -n "$jailer" ] && [ -x "$jailer" ] || unavailable "an executable jailer binary is not configured"
[ -n "${REMOUNT_FIRECRACKER_KERNEL:-}" ] || unavailable "REMOUNT_FIRECRACKER_KERNEL is unset"
[ -n "${REMOUNT_FIRECRACKER_ROOTFS:-}" ] || unavailable "REMOUNT_FIRECRACKER_ROOTFS is unset"
[ -n "${REMOUNT_FIRECRACKER_GUEST_MANIFEST:-}" ] || unavailable "REMOUNT_FIRECRACKER_GUEST_MANIFEST is unset"
[ -n "${REMOUNT_FIRECRACKER_DATA_ROOT:-}" ] || unavailable "REMOUNT_FIRECRACKER_DATA_ROOT is unset"
[ -n "${REMOUNT_FIRECRACKER_CGROUP_PARENT:-}" ] || unavailable "REMOUNT_FIRECRACKER_CGROUP_PARENT is unset"
[ -f "$REMOUNT_FIRECRACKER_KERNEL" ] || unavailable "kernel image is not a regular file"
[ -f "$REMOUNT_FIRECRACKER_ROOTFS" ] || unavailable "rootfs image is not a regular file"
[ -f "$REMOUNT_FIRECRACKER_GUEST_MANIFEST" ] || unavailable "guest manifest is not a regular file"
[ -d "$REMOUNT_FIRECRACKER_DATA_ROOT" ] || unavailable "data root does not exist"
cgroup=/sys/fs/cgroup/${REMOUNT_FIRECRACKER_CGROUP_PARENT#/sys/fs/cgroup/}
[ -d "$cgroup" ] || unavailable "cgroup parent does not exist"

"$firecracker" --version
"$jailer" --version
echo "AVAILABLE: static Firecracker host prerequisites passed; run the full KVM lifecycle lane"
