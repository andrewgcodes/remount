#!/usr/bin/env bash
set -euo pipefail
set +x

harness=${1:-}
provider=${2:-}
case "$harness:$provider" in
  codex:openai|claude:openrouter) ;;
  *) echo "usage: e13-headless.sh codex openai | claude openrouter" >&2; exit 2 ;;
esac

: "${REMOUNT_E13_BINARY:?set REMOUNT_E13_BINARY to the candidate remount binary}"
: "${REMOUNT_E13_MODEL_KEY:?set REMOUNT_E13_MODEL_KEY only for this process}"

case "$harness" in
  codex) command -v codex >/dev/null || { echo "E13 unavailable: codex executable is absent" >&2; exit 77; } ;;
  claude) command -v claude >/dev/null || { echo "E13 unavailable: claude executable is absent" >&2; exit 77; } ;;
esac

run_dir=$(mktemp -d)
control_pid=
cleanup() {
  if [[ -n "$control_pid" ]]; then
    kill "$control_pid" 2>/dev/null || true
    wait "$control_pid" 2>/dev/null || true
  fi
  rm -rf "$run_dir"
}
trap cleanup EXIT INT TERM

bindings="$run_dir/bindings.json"
cat >"$bindings" <<'JSON'
[
  {
    "id": "b_model",
    "secret": "$REMOUNT_E13_MODEL_KEY",
    "destinations": ["api.openai.com", "openrouter.ai"],
    "placeholder": "sk-proj-REMOUNT-PLACEHOLDER-NOT-A-REAL-KEY",
    "ttl_sec": 600
  }
]
JSON
chmod 600 "$bindings"

export REMOUNT_SERVER=http://127.0.0.1:17443
export REMOUNT_TOKEN=
export REMOUNT_PRINCIPAL=e13-outer-harness
export REMOUNT_E13_MODEL_KEY
"$REMOUNT_E13_BINARY" standalone --listen 127.0.0.1:17443 --data "$run_dir/data" --bindings "$bindings" --backend process >"$run_dir/control.log" 2>&1 &
control_pid=$!
for _ in $(seq 1 100); do
  if curl --fail --silent "$REMOUNT_SERVER/healthz" >/dev/null; then break; fi
  if ! kill -0 "$control_pid" 2>/dev/null; then echo "E13 control plane exited before readiness" >&2; exit 1; fi
  sleep 0.1
done
curl --fail --silent "$REMOUNT_SERVER/healthz" >/dev/null || { echo "E13 control plane unavailable" >&2; exit 1; }

prompt="Use only the remount MCP tools. In fewer than 20 tool calls: call agent_create with recipe '$harness', task 'Reply with the exact text INNER_COMPLETE', bindings ['b_model:$provider'], backend 'process', sandbox 'workspace-write', and idempotency_key 'e13-child-create'. Poll get until it is waiting_input, finished, or failed. Read transcript_tail and events_tail for its workspace. If and only if the child completed and the canonical events were readable, finish with E13_COMPLETE. Never request, read, or print credentials or .remount/env."
transcript="$run_dir/$harness.jsonl"

if [[ "$harness" == codex ]]; then
  export CODEX_HOME="$run_dir/codex-home"
  mkdir -p "$CODEX_HOME"
  "$REMOUNT_E13_BINARY" mcp config codex --command "$REMOUNT_E13_BINARY" >"$CODEX_HOME/config.toml"
  printf '\ndefault_tools_approval_mode = "auto"\n' >>"$CODEX_HOME/config.toml"
  CODEX_API_KEY="$REMOUNT_E13_MODEL_KEY" codex exec --ephemeral --sandbox read-only --json "$prompt" >"$transcript"
else
  "$REMOUNT_E13_BINARY" mcp config claude --command "$REMOUNT_E13_BINARY" >"$run_dir/mcp.json"
  ANTHROPIC_AUTH_TOKEN="$REMOUNT_E13_MODEL_KEY" ANTHROPIC_BASE_URL=https://openrouter.ai/api \
    claude --bare -p "$prompt" --output-format stream-json --strict-mcp-config --mcp-config "$run_dir/mcp.json" \
      --permission-mode dontAsk --allowedTools "mcp__remount__agent_create" "mcp__remount__get" "mcp__remount__transcript_tail" "mcp__remount__events_tail" >"$transcript"
fi

grep -q 'E13_COMPLETE' "$transcript" || { echo "E13 harness did not report completion" >&2; exit 1; }
if grep -Fq "$REMOUNT_E13_MODEL_KEY" "$transcript"; then
  echo "E13 secret canary appeared in harness transcript" >&2
  exit 1
fi
if grep -Eq 'REMOUNT_(TOKEN|BROKER|WORKSPACE)=' "$transcript"; then
  echo "E13 .remount/env-shaped content appeared in harness transcript" >&2
  exit 1
fi
echo "E13 $harness live lifecycle passed"
