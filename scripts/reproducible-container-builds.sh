#!/bin/sh
set -eu

root=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
image="golang@sha256:648f440f42a0958804efb24df176f806f9d353b41f1c0627f666428e40310f6b"
version=$(git -C "$root" describe --tags --always --dirty 2>/dev/null || echo dev)
work=$(mktemp -d)
trap 'rm -rf "$work"' EXIT HUP INT TERM

build() {
	name=$1
	tmpdir=$2
	mkdir -p "$work/$name"
	docker run --rm \
		-e "REMOUNT_VERSION=$version" \
		-e "TMPDIR=$tmpdir" \
		-v "$root:/src:ro" \
		-v "$work/$name:/out" \
		-w /src \
		"$image" sh -eu -c '
			mkdir -p "$TMPDIR"
			for platform in linux/amd64 linux/arm64 darwin/amd64 darwin/arm64 windows/amd64 windows/arm64; do
				os=${platform%/*}
				arch=${platform#*/}
				ext=
				test "$os" != windows || ext=.exe
				CGO_ENABLED=0 GOOS="$os" GOARCH="$arch" go build -buildvcs=false -mod=readonly -trimpath \
					-ldflags="-s -w -X main.version=$REMOUNT_VERSION" \
					-o "/out/remount-$os-$arch$ext" ./cmd/remount
			done
		'
	(
		cd "$work/$name"
		find . -maxdepth 1 -type f ! -name checksums.txt -print0 | sort -z | xargs -0 sha256sum >checksums.txt
	)
}

build first /tmp/remount-build-one
build second /work/remount-build-two
diff -u "$work/first/checksums.txt" "$work/second/checksums.txt"
cat "$work/first/checksums.txt"
echo "verified: two clean containers produced byte-identical static binaries"
