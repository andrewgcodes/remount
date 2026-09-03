#!/usr/bin/env bash
set -euo pipefail

required=(REMOUNT_SERVER REMOUNT_ACTION_BINDING REMOUNT_ACTION_TASK REMOUNT_ACTION_RECIPE REMOUNT_ACTION_REPOSITORY REMOUNT_ACTION_REF GITHUB_OUTPUT)
for name in "${required[@]}"; do
  if [[ -z "${!name:-}" ]]; then
    echo "remount action: $name is required" >&2
    exit 2
  fi
done

case "$REMOUNT_SERVER" in
  https://*) ;;
  http://*)
    if [[ "${REMOUNT_ACTION_ALLOW_HTTP:-false}" != "true" ]]; then
      echo "remount action: refusing non-HTTPS server (set allow-insecure-http only for local tests)" >&2
      exit 2
    fi
    ;;
  *)
    echo "remount action: server must use https://" >&2
    exit 2
    ;;
esac

if [[ "$REMOUNT_ACTION_REPOSITORY" != */* ]] || [[ "$REMOUNT_ACTION_REPOSITORY" == *$'\n'* ]]; then
  echo "remount action: repository must be owner/name" >&2
  exit 2
fi
if [[ "$REMOUNT_ACTION_REF" == *$'\n'* ]] || [[ "$REMOUNT_ACTION_BINDING" == *$'\n'* ]]; then
  echo "remount action: ref and binding must be single-line values" >&2
  exit 2
fi

github_server="${REMOUNT_ACTION_GITHUB_SERVER:-https://github.com}"
repo_host="${github_server#https://}"
repo_host="${repo_host#http://}"
repo_host="${repo_host%/}"
repo="${repo_host}/${REMOUNT_ACTION_REPOSITORY}@${REMOUNT_ACTION_REF}"

binary="${REMOUNT_ACTION_BINARY:-remount}"
args=(--json run "$REMOUNT_ACTION_RECIPE" --repo "$repo" --binding "$REMOUNT_ACTION_BINDING" --detach)
if [[ -n "${REMOUNT_ACTION_MODEL:-}" ]]; then
  args+=(--model "$REMOUNT_ACTION_MODEL")
fi
args+=(-- "$REMOUNT_ACTION_TASK")

result="$(mktemp "${RUNNER_TEMP:-${TMPDIR:-/tmp}}/remount-action.XXXXXX")"
trap 'rm -f "$result"' EXIT
"$binary" "${args[@]}" >"$result"

python3 -c '
import json, pathlib, sys
data = json.loads(pathlib.Path(sys.argv[1]).read_text())
agent, workspace = data.get("id"), data.get("ws")
if not isinstance(agent, str) or not agent.startswith("ag_"):
    raise SystemExit("remount action: response has no Agent id")
if not isinstance(workspace, str) or not workspace.startswith("ws_"):
    raise SystemExit("remount action: response has no workspace id")
with open(sys.argv[2], "a", encoding="utf-8") as output:
    output.write(f"agent-id={agent}\nworkspace-id={workspace}\n")
' "$result" "$GITHUB_OUTPUT"
