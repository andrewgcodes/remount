#!/bin/sh
# Evidence gate B34: drive a real Chromium through Remount's computer
# operations on the docker backend.
#
#   scripts/browser-conformance.sh build   build the reference browser image
#   scripts/browser-conformance.sh run     run the lane (default)
#   scripts/browser-conformance.sh all     build, then run
#
# A missing prerequisite exits 77 and names what is absent. It never reports a
# pass: a browser lane that passed without a browser would be worse than no
# lane at all. The Go test applies the same rule to the prerequisites only it
# can see, including whether this host routes to container addresses and
# whether it can reach the allowed destination the lane browses to.
#
# The lane allows exactly one real host and refuses another, so it can tell a
# host-policy decision from a browser that never authenticated to the broker
# (ADR 0095). Override either with REMOUNT_BROWSER_ALLOWED_HOST and
# REMOUNT_BROWSER_DENIED_HOST when this host reaches neither default.
set -eu

image="${REMOUNT_BROWSER_IMAGE:-remount-browser:local}"
allowed="${REMOUNT_BROWSER_ALLOWED_HOST:-example.com}"
denied="${REMOUNT_BROWSER_DENIED_HOST:-example.org}"
root=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
cd "$root"

unavailable() {
  echo "UNAVAILABLE: $*" >&2
  exit 77
}

command -v docker >/dev/null 2>&1 || unavailable "docker is not installed"
docker info >/dev/null 2>&1 || unavailable "no docker daemon is reachable"

build() {
  docker build -t "$image" images/browser
  # The probe is the image's own contract: a DevTools endpoint that answers.
  docker run --rm --entrypoint sh "$image" -c \
    'command -v chromium >/dev/null && command -v socat >/dev/null && command -v remount-browser-health >/dev/null'
}

run() {
  docker image inspect "$image" >/dev/null 2>&1 ||
    unavailable "image $image is absent; run '$0 build'"
  [ "$allowed" != "$denied" ] ||
    unavailable "the allowed and denied hosts are both $allowed; the lane cannot tell one decision from the other"
  REMOUNT_BROWSER_IMAGE="$image" \
  REMOUNT_BROWSER_ALLOWED_HOST="$allowed" \
  REMOUNT_BROWSER_DENIED_HOST="$denied" \
    go test -count=1 -v -timeout=20m \
      -run '^TestB34BrowserComputerConformance$' ./integration/browser/
}

case "${1:-run}" in
  build) build ;;
  run) run ;;
  all) build; run ;;
  *)
    echo "usage: $0 [build|run|all]" >&2
    exit 2
    ;;
esac
