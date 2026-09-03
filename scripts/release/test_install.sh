#!/bin/sh
set -eu

root=$(CDPATH= cd -- "$(dirname "$0")/../.." && pwd)
test_dir=$(mktemp -d "${TMPDIR:-/tmp}/remount-install-test.XXXXXX")
trap 'rm -rf "$test_dir"' EXIT HUP INT TERM
mkdir -p "$test_dir/bin" "$test_dir/fixtures" "$test_dir/good" "$test_dir/bad"
mkdir -p "$test_dir/tmp space"

cat >"$test_dir/fixtures/remount-linux-amd64" <<'EOF'
#!/bin/sh
echo "remount v1.2.3"
EOF
chmod 0755 "$test_dir/fixtures/remount-linux-amd64"
if command -v sha256sum >/dev/null 2>&1; then
  digest=$(sha256sum "$test_dir/fixtures/remount-linux-amd64" | awk '{print $1}')
else
  digest=$(shasum -a 256 "$test_dir/fixtures/remount-linux-amd64" | awk '{print $1}')
fi
printf '%s  remount-linux-amd64\n' "$digest" >"$test_dir/fixtures/checksums.txt"
: >"$test_dir/fixtures/checksums.txt.sigstore.json"

cat >"$test_dir/bin/uname" <<'EOF'
#!/bin/sh
case "${1:-}" in
  -s) echo Linux ;;
  -m) echo x86_64 ;;
  *) exit 2 ;;
esac
EOF
cat >"$test_dir/bin/cosign" <<'EOF'
#!/bin/sh
test "$1" = verify-blob
EOF
cat >"$test_dir/bin/curl" <<'EOF'
#!/bin/sh
set -eu
output=
url=
while [ "$#" -gt 0 ]; do
  case "$1" in
    --output) shift; output=$1 ;;
    http*) url=$1 ;;
  esac
  shift
done
test -n "$output" && test -n "$url"
name=${url##*/}
if [ "${INSTALL_TEST_TAMPER:-0}" = 1 ] && [ "$name" = remount-linux-amd64 ]; then
  printf tampered >"$output"
else
  cp "$INSTALL_TEST_FIXTURES/$name" "$output"
fi
EOF
chmod 0755 "$test_dir/bin/uname" "$test_dir/bin/cosign" "$test_dir/bin/curl"

PATH="$test_dir/bin:$PATH" \
TMPDIR="$test_dir/tmp space" \
INSTALL_TEST_FIXTURES="$test_dir/fixtures" \
REMOUNT_GITHUB_REPOSITORY=owner/repo \
REMOUNT_VERSION=v1.2.3 \
REMOUNT_INSTALL_DIR="$test_dir/good" \
REMOUNT_REQUIRE_SIGNATURE=1 \
  "$root/install.sh" >/dev/null
test "$("$test_dir/good/remount" version)" = "remount v1.2.3"

if PATH="$test_dir/bin:$PATH" \
  INSTALL_TEST_FIXTURES="$test_dir/fixtures" \
  INSTALL_TEST_TAMPER=1 \
  REMOUNT_GITHUB_REPOSITORY=owner/repo \
  REMOUNT_VERSION=v1.2.3 \
  REMOUNT_INSTALL_DIR="$test_dir/bad" \
  REMOUNT_REQUIRE_SIGNATURE=1 \
    "$root/install.sh" >/dev/null 2>&1; then
  echo "tampered asset was accepted" >&2
  exit 1
fi
test ! -e "$test_dir/bad/remount"

if PATH="$test_dir/bin:$PATH" \
  REMOUNT_GITHUB_REPOSITORY='owner/repo/extra' \
  REMOUNT_VERSION=v1.2.3 \
  REMOUNT_INSTALL_DIR="$test_dir/bad" \
    "$root/install.sh" >/dev/null 2>&1; then
  echo "unsafe repository was accepted" >&2
  exit 1
fi

if PATH="$test_dir/bin:$PATH" \
  REMOUNT_GITHUB_REPOSITORY=owner/repo \
  REMOUNT_VERSION=latest \
  REMOUNT_INSTALL_DIR="$test_dir/bad" \
    "$root/install.sh" >/dev/null 2>&1; then
  echo "non-semantic version was accepted" >&2
  exit 1
fi

if PATH="$test_dir/bin:$PATH" \
  REMOUNT_GITHUB_REPOSITORY=owner/repo \
  REMOUNT_VERSION=v1.2.3 \
  REMOUNT_REQUIRE_SIGNATURE=maybe \
  REMOUNT_INSTALL_DIR="$test_dir/bad" \
    "$root/install.sh" >/dev/null 2>&1; then
  echo "invalid signature policy was accepted" >&2
  exit 1
fi

echo "installer accepts verified bytes and rejects a checksum mismatch"
