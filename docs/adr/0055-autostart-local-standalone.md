# ADR 0055: `remount run` starts a local standalone when nothing answers

## Status

Accepted.

## Context

Gate 1 of the build plan says the tutorial is one command. Before this change
it was four: build, start `remount standalone` with a hand-written bindings
file and an allow list, export `REMOUNT_SERVER`, then `remount run` with
`--binding`. Each step existed for a reason (the key never enters a workspace,
the install registry is reached through the broker) but every one of them was
a chance to get the setup wrong, and the first-run experience of a tool whose
pitch is "never holding the keys" was a JSON file with `"$OPENAI_API_KEY"` in
it.

## Decision

`remount run`, `remount handoff` and `remount resume` probe `/healthz` at the
default server address before dialing. When nothing answers **and** the
invocation is using the default address (no `--server`, no `REMOUNT_SERVER`,
no token) **and** `REMOUNT_AUTOSTART` is not `0`, the CLI starts
`remount standalone` in the background and waits for it to become healthy.

The standalone is started with:

- `--data` under `$REMOUNT_DATA`, else `$XDG_DATA_HOME/remount`, else
  `~/.local/share/remount`. Stable across working directories, so a workspace
  created from one checkout can be resumed from another.
- `--bindings` pointing at a file the CLI writes with one entry per fixed-host
  provider preset whose key variable is set. Every `secret` is `"$VAR"`: the
  file on disk never holds a key, and the standalone resolves it from the
  environment it inherited from the `remount run` that started it. Presets
  with a per-deployment host (Azure) are skipped; they still need an explicit
  binding.
- `--allow` for every host a built-in recipe installs from, so the harness
  can fetch itself. Provider hosts are reached through bindings, not the allow
  list.
- `--backend process,docker`, with docker only when the binary is on `PATH`.
- Its own session (`Setsid`; a detached process group on Windows), stdout and
  stderr appended to `standalone.log`, its pid in `standalone.pid`.

When no `--binding` was given, `run` picks the first binding among those the
autostarted standalone has that the recipe's `providers` list accepts, and
says so on stderr. This applies whether the standalone was just started or is
the one a previous `run` started; the pid file decides. It does not apply to a
server the user started themselves at the same address: the CLI does not
know that server's bindings and does not guess.

Validation still happens before anything is started. A `run` that fails on
its flags never leaves a standalone behind.

## Consequences

- The tutorial's first section is one command, and its output shows where
  the standalone's state and log are and how to stop it.
- Nothing changes for a deployment: any explicit server, token or
  `REMOUNT_AUTOSTART=0` turns the behavior off. The CLI never starts a server
  at an address other than `127.0.0.1:7443`.
- A key rotated in the shell is not seen by a standalone that is already
  running. The stderr line names the pid so the user can stop it; the next
  run starts a fresh one. `remount status` shows how many bindings the
  server holds.
- The CLI test suite runs with the default address and no server, so it
  exercises the "validation fails before autostart" path on every run; the
  unit tests cover the enable conditions, the binding synthesis (asserting the
  key value is absent from the file), the recipe-driven default pick, and
  that a dead pid stops the CLI trusting the bindings file.
