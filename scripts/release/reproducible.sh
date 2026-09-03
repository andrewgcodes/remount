#!/bin/sh
set -eu

root=$(CDPATH= cd -- "$(dirname "$0")/../.." && pwd)
version=${1:-v0.1.0-repro}
case "$version" in *[!A-Za-z0-9._+-]*|'') echo "unsafe version" >&2; exit 2 ;; esac
tmp_dir=$(mktemp -d "${TMPDIR:-/tmp}/remount-repro.XXXXXX")
trap 'rm -rf "$tmp_dir"' EXIT HUP INT TERM

for pass in one two; do
  mkdir "$tmp_dir/$pass"
  for platform in linux/amd64 linux/arm64 darwin/amd64 darwin/arm64 windows/amd64 windows/arm64; do
    os=${platform%/*}
    arch=${platform#*/}
    extension=""
    [ "$os" != windows ] || extension=.exe
    (
      cd "$root"
      CGO_ENABLED=0 GOOS="$os" GOARCH="$arch" go build -trimpath \
        -ldflags="-s -w -X main.version=$version" \
        -o "$tmp_dir/$pass/remount-$os-$arch$extension" ./cmd/remount
    )
  done
done

for first in "$tmp_dir/one"/*; do
  name=${first##*/}
  cmp "$first" "$tmp_dir/two/$name" || {
    echo "non-reproducible artifact: $name" >&2
    exit 1
  }
done

host_os=$(uname -s | tr '[:upper:]' '[:lower:]')
case "$host_os" in darwin|linux) ;; *) host_os="" ;; esac
case $(uname -m) in x86_64|amd64) host_arch=amd64 ;; arm64|aarch64) host_arch=arm64 ;; *) host_arch="" ;; esac
if [ -n "$host_os" ] && [ -n "$host_arch" ]; then
  reported=$("$tmp_dir/one/remount-$host_os-$host_arch" version)
  [ "$reported" = "remount $version" ] || {
    echo "version mismatch: $reported" >&2
    exit 1
  }
fi

echo "six platform artifacts were byte-identical across two clean build directories ($version)"
