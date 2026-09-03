#!/bin/sh
set -eu

# A missing runtime is an unavailable benchmark lane, never a passing one.
# With --probe, REMOUNT_CHAOS_IMAGE must name an image already approved for
# this host; Docker may pull it if it is not present locally.
probe=false
if [ "${1:-}" = "--probe" ]; then
  probe=true
elif [ "$#" -ne 0 ]; then
  echo "usage: $0 [--probe]" >&2
  exit 2
fi

docker_version=""
docker_reason="docker CLI not installed"
if command -v docker >/dev/null 2>&1; then
  if docker_version=$(docker version --format '{{.Server.Version}}' 2>/dev/null) && [ -n "$docker_version" ]; then
    printf 'docker status=available server_version=%s\n' "$docker_version"
    docker_reason=""
  else
    docker_reason="Docker daemon unreachable"
  fi
fi
if [ -n "$docker_reason" ]; then
  printf 'docker status=unavailable reason=%s\n' "$docker_reason"
  printf 'gvisor status=unavailable reason=Docker daemon is required by this benchmark lane\n'
  exit 3
fi

runtimes=$(docker info --format '{{json .Runtimes}}' 2>/dev/null || true)
case "$runtimes" in
  *\"runsc\"*) ;;
  *)
    printf 'gvisor status=unavailable reason=runsc is not registered with Docker\n'
    exit 4
    ;;
esac

if [ "$probe" = false ]; then
  printf 'gvisor status=unavailable reason=runsc is registered but no workload probe was requested\n'
  exit 5
fi

image=${REMOUNT_CHAOS_IMAGE:-}
if [ -z "$image" ]; then
  printf 'gvisor status=unavailable reason=REMOUNT_CHAOS_IMAGE is required for the workload probe\n'
  exit 6
fi
if ! docker run --rm --runtime=runsc "$image" true >/dev/null 2>&1; then
  printf 'gvisor status=unavailable reason=runsc workload probe failed\n'
  exit 7
fi
printf 'gvisor status=available runtime=runsc probe_image=%s\n' "$image"
