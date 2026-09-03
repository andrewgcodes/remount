#!/usr/bin/env bash
set -euo pipefail

fixture="$(mktemp -d "${TMPDIR:-/tmp}/remount-action-test.XXXXXX")"
trap 'rm -rf "$fixture"' EXIT

cat >"$fixture/remount" <<'EOF'
#!/usr/bin/env bash
printf '%s\n' "$@" >"$TEST_ARGS"
printf '%s\n' "${REMOUNT_SERVER:-}" >"$TEST_SERVER"
if [[ "${REMOUNT_TOKEN:-}" == "secret-token" ]]; then
  printf '%s\n' '{"id":"ag_test","ws":"ws_test"}'
else
  exit 9
fi
EOF
chmod +x "$fixture/remount"

export TEST_ARGS="$fixture/args"
export TEST_SERVER="$fixture/server"
export GITHUB_OUTPUT="$fixture/output"
export REMOUNT_SERVER="https://remount.example"
export REMOUNT_TOKEN="secret-token"
export REMOUNT_ACTION_BINDING="b_repo"
export REMOUNT_ACTION_TASK="fix the race"
export REMOUNT_ACTION_RECIPE="opencode"
export REMOUNT_ACTION_REPOSITORY="acme/widgets"
export REMOUNT_ACTION_REF="deadbeef"
export REMOUNT_ACTION_GITHUB_SERVER="https://github.com"
export REMOUNT_ACTION_BINARY="$fixture/remount"

"$(dirname "$0")/../run.sh"

grep -Fx -- "https://remount.example" "$TEST_SERVER"
grep -Fx -- "agent-id=ag_test" "$GITHUB_OUTPUT"
grep -Fx -- "workspace-id=ws_test" "$GITHUB_OUTPUT"
grep -Fx -- "github.com/acme/widgets@deadbeef" "$TEST_ARGS"
grep -Fx -- "b_repo" "$TEST_ARGS"
grep -Fx -- "fix the race" "$TEST_ARGS"
if grep -Fq -- "secret-token" "$TEST_ARGS"; then
  echo "token leaked into process arguments" >&2
  exit 1
fi

export REMOUNT_SERVER="http://remount.example"
if "$(dirname "$0")/../run.sh" >"$fixture/http.out" 2>"$fixture/http.err"; then
  echo "non-HTTPS server was accepted" >&2
  exit 1
fi
grep -Fq -- "refusing non-HTTPS" "$fixture/http.err"
