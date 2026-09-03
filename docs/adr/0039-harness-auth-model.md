# ADR 0039: Recipes are data; brokered keys and workspace-resident logins are distinct

## Status

Accepted.

## Context

`remount run RECIPE -- TASK` has to launch eight different coding harnesses
(Claude Code, Codex CLI, OpenCode, OpenHands, Goose, Gemini CLI, Aider, Cline)
plus anything a user brings, against a dozen model providers, without the
workspace ever holding a provider key (ADR 0007) and without the broker's
address ever being baked into a file that travels in a snapshot.

Two questions had to be settled before the first recipe was written.

1. **Is a recipe Go or data?** A Go interface per harness would make every new
   harness a code change and a release, and would invite each recipe to grow
   its own install and config logic.
2. **What does Remount do about harness-native subscription logins?** Claude
   Max, ChatGPT/Codex sign-in, Gemini's Google sign-in and Copilot all run their
   own OAuth flow inside the harness and keep the resulting token in the
   harness's state directory. That token is not an API key, is not ours to
   proxy, and its terms of use restrict third-party handling.

## Decision

1. **Recipes are YAML, embedded in the binary, loaded by one generic
   implementation.** `internal/launch/recipes/*.yaml` declare `image`,
   `install`, `env`, `configure` (templated files), `command`,
   `resume_command`, `state_dirs`, `providers`, `hosts`, `auth`, and
   `sandbox_flags`. `--recipe-file` loads a user's own with the same
   validator. Adding a harness is a YAML pull request. The parser is a small
   dependency-free subset (`internal/launch/yamlite`): block mappings and
   sequences, flow sequences of scalars, quoted and block scalars. Anchors,
   aliases, tags and flow mappings are refused, so a recipe can never be
   ambiguous.

2. **Provider bindings are presets, equally generic.** `anthropic`, `openai`,
   `google`, `openrouter`, `bedrock`, `vertex`, `azure-openai`, `mistral`,
   `groq`, `together`, `fireworks`, `deepseek` and `xai` each name the key
   env var, the base-URL env var, the provider hosts and the URL path. A
   binding id `b_openai` selects its preset by suffix; `b_custom:openai` and
   `b_bedrock:bedrock?host=us-east-1` are explicit. No recipe hard-codes one
   lab; a recipe lists which presets it can drive.

3. **Remount brokers API keys and nothing else.** The workspace receives
   `<PROVIDER>_API_KEY=ref:<binding>` and
   `<PROVIDER>_BASE_URL=${REMOUNT_BROKER}/d/<host>/<path>`; the node's broker
   substitutes the real key at the network edge and records `cred.used`.
   Remount never proxies, rewrites or stores a subscription token.

4. **Workspace-resident auth is allowed, named, and confined to `local`.**
   A recipe declares `auth: api_key | workspace_resident | either`. When a
   launch has no provider binding for a recipe that can use its own login,
   the run is marked `auth: workspace_resident`, the node emits
   `auth.workspace_resident{recipe}`, and the docs say plainly that the
   token lives in the workspace and travels with snapshots. `--security
   isolated` and `multi_tenant` refuse such a launch, because a secret-blind
   profile cannot honestly contain a secret it does not see.

5. **The launcher discovers the broker; it never embeds it.** `remount run`
   writes `.remount/launch/<recipe>.sh`, which sources `.remount/env` at exec
   time and only then expands provider URLs. Recipe config files that must
   name the broker (OpenCode's `opencode.json`) are written under
   `.remount/launch/` and pointed at with the harness's own config-path
   variable, so they are regenerated on every launch, never overwrite a
   project file, and never travel. `.remount/` is otherwise refused as a
   configure path.

6. **Every launch is a session with `RunInfo`.** `s.open` carries
   `{recipe, task_hash, sandbox, auth}`; the node emits `run.started` once per
   created session (an idempotent replay emits nothing) and `run.finished`
   from the exit path, so the records exist even when the client detached.
   The task text and argv never enter the event log; only a short hash does.

7. **`--sandbox` is typed egress plus a harness flag.** `read-only` and
   `workspace-write` deny by default under non-local profiles and admit only
   the bound providers (HTTPS) and the recipe's declared hosts (`GET`/`HEAD`
   for installs); `full` adds `CONNECT` to those hosts. Under `local`,
   `workspace-write` keeps the node's own allow list and `full` opens it.

## Consequences

- A harness that honors `*_API_KEY` and `*_BASE_URL` needs no recipe; it
  runs under `custom` with any preset.
- A user who wants Claude Max inside an isolated workspace is told no, with
  the reason, rather than given a profile that silently leaks its meaning.
- The recipe validator and launcher renderer are the security boundary for
  user-supplied recipes: paths are confined to the workspace (or the
  launcher directory), env names are validated, argv is shell-quoted
  byte-for-byte, and the heredoc delimiter is checked against content.
- The E1 integration lane (`TestRunOpenCodeDockerIntegration`) runs the real
  OpenCode against the real OpenAI API through the broker when a daemon and
  `REMOUNT_INTEGRATION_OPENAI_KEY` are present; the sim covers the rest.
