#!/bin/sh
set -eu

repository=${REMOUNT_GITHUB_REPOSITORY:-andrewgcodes/remount}
version=${REMOUNT_VERSION:-}
install_dir=${REMOUNT_INSTALL_DIR:-/usr/local/bin}
require_signature=${REMOUNT_REQUIRE_SIGNATURE:-0}

fail() {
  echo "remount installer: $*" >&2
  exit 1
}

command -v curl >/dev/null 2>&1 || fail "curl is required"
case "$require_signature" in 0|1) ;; *) fail "REMOUNT_REQUIRE_SIGNATURE must be 0 or 1" ;; esac

printf '%s\n' "$repository" | grep -Eq '^[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+$' ||
  fail "REMOUNT_GITHUB_REPOSITORY must be owner/name"

if [ -z "$version" ]; then
  release_url=$(curl --proto '=https' --tlsv1.2 --fail --silent --show-error --location \
    --output /dev/null --write-out '%{url_effective}' "https://github.com/$repository/releases/latest")
  version=${release_url##*/}
fi
printf '%s\n' "$version" | grep -Eq '^v[0-9]+\.[0-9]+\.[0-9]+([+-][0-9A-Za-z.-]+)?$' ||
  fail "REMOUNT_VERSION must be a semantic release tag such as v0.1.0"

case $(uname -s) in
  Linux) os=linux ;;
  Darwin) os=darwin ;;
  *) fail "supported operating systems are Linux and macOS" ;;
esac
case $(uname -m) in
  x86_64|amd64) arch=amd64 ;;
  arm64|aarch64) arch=arm64 ;;
  *) fail "supported architectures are amd64 and arm64" ;;
esac

asset="remount-$os-$arch"
base="https://github.com/$repository/releases/download/$version"
tmp_dir=$(mktemp -d "${TMPDIR:-/tmp}/remount-install.XXXXXX")
stage=""
cleanup() {
  [ -z "$stage" ] || rm -f "$stage"
  rm -rf "$tmp_dir"
}
trap cleanup EXIT HUP INT TERM

download() {
  curl --proto '=https' --tlsv1.2 --fail --silent --show-error --location \
    --retry 3 --retry-delay 1 --output "$2" "$base/$1"
}

download "$asset" "$tmp_dir/$asset"
download checksums.txt "$tmp_dir/checksums.txt"
expected=$(awk -v name="$asset" '$2 == name || $2 == "./" name { print $1 }' "$tmp_dir/checksums.txt" | tr 'A-F' 'a-f')
[ -n "$expected" ] || fail "$asset is absent from checksums.txt"
case "$expected" in *[!0-9a-fA-F]*|'') fail "release checksum is malformed" ;; esac
[ "${#expected}" -eq 64 ] || fail "release checksum is malformed"
if command -v sha256sum >/dev/null 2>&1; then
  actual=$(sha256sum "$tmp_dir/$asset" | awk '{print $1}')
else
  command -v shasum >/dev/null 2>&1 || fail "sha256sum or shasum is required"
  actual=$(shasum -a 256 "$tmp_dir/$asset" | awk '{print $1}')
fi
[ "$actual" = "$expected" ] || fail "checksum mismatch for $asset"

if command -v cosign >/dev/null 2>&1; then
  download checksums.txt.sigstore.json "$tmp_dir/checksums.txt.sigstore.json"
  cosign verify-blob \
    --bundle "$tmp_dir/checksums.txt.sigstore.json" \
    --certificate-identity "https://github.com/$repository/.github/workflows/release.yml@refs/tags/$version" \
    --certificate-oidc-issuer "https://token.actions.githubusercontent.com" \
    "$tmp_dir/checksums.txt" >/dev/null
elif [ "$require_signature" = 1 ]; then
  fail "cosign is required when REMOUNT_REQUIRE_SIGNATURE=1"
else
  echo "remount installer: cosign not found; checksum verified but signer identity was not (set REMOUNT_REQUIRE_SIGNATURE=1 to fail closed)" >&2
fi

chmod 0755 "$tmp_dir/$asset"
actual_version=$("$tmp_dir/$asset" version)
[ "$actual_version" = "remount $version" ] || fail "binary reports '$actual_version', expected 'remount $version'"

mkdir -p "$install_dir"
stage=$(mktemp "$install_dir/.remount-install.XXXXXX")
cp "$tmp_dir/$asset" "$stage"
chmod 0755 "$stage"
mv -f "$stage" "$install_dir/remount"
stage=""
echo "installed remount $version to $install_dir/remount"
