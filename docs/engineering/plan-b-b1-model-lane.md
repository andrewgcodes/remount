# Plan B B1: the deterministic OpenCode and broker lane

`plan-b-repository-executable-2026-09-03.md` §9 requires that the default
OpenCode end-to-end lane be deterministic and keyless, and that the live OpenAI
run become a supplement whose absence is `unavailable` rather than a pass.

Before this change the only real-harness proof in the tree was
`internal/sim.TestRunOpenCodeDockerIntegration`, which skipped without
`REMOUNT_INTEGRATION_OPENAI_KEY`. A clean checkout could not prove that a real
harness runs in a Remount workspace at all.

This document records what the lane now is, what it proves, and what it does
not.

## What runs

| Path | Role |
|---|---|
| `internal/modelfake` | the bounded, deterministic OpenAI-compatible server: Responses, Chat Completions and a fixed model list |
| `internal/sim/planb_model_lane_test.go` | B1, B3, B6 — the exec lane, the reattach proof, and the shared leak scan |
| `internal/sim/planb_agent_lane_test.go` | B2, B4, B5 — the ACP lane: transcript, approval ordering, sleep and reload |
| `internal/sim/run_docker_test.go` | B7 — the same scenario and the same leak scan against real OpenAI, gated on a key |

Both deterministic tests run a real, pinned OpenCode inside a real docker
workspace. Nothing about the harness, the broker, the container, the snapshot
or the event log is simulated. Only the model is local.

```text
OpenCode (docker workspace)
  -> OPENAI_API_KEY=ref:b_openai
  -> OPENAI_BASE_URL=${REMOUNT_BROKER}/d/127.0.0.1:PORT/v1
  -> node broker            substitutes ref:b_openai -> the synthetic bearer
  -> internal/modelfake     rejects any request that does not carry it
```

The binding, the placeholder, the broker path and the leak-blocking rule are
the production ones. The only thing the lane controls is the destination host,
which is what a hermetic lane must control.

## Why the fake demands a credential

A fake upstream that accepts anything would pass whether or not the broker
substituted. `modelfake` requires a synthetic server-side bearer, refuses a
request without it, and records the `Authorization` header of every request it
saw. The tests assert both directions: every request carried
`Bearer sk-remount-modelfake-upstream-…`, and no request carried
`ref:b_openai`.

The synthetic bearer has the shape of a provider key and no authority
anywhere. No real credential is ever planted, including as a scan canary.

## Why the answers are deterministic

`modelfake` resolves a script step from the request alone: the index is the
number of tool results the conversation already carries. A retried, replayed or
reattached request therefore gets the same answer rather than advancing the
script. A conversation that runs past the end of the script is a 409 naming the
overrun, never an empty turn that reads as success.

A request naming a model other than the scripted one is answered from a fixed
auxiliary string. OpenCode titles a session with a separate small model, and
letting that call walk the script would silently consume a step.

A script step that calls a tool the request does not declare is also a 409. A
newer OpenCode that renamed a tool then fails here with the names in the
message, rather than receiving a turn it quietly drops.

## Bounds

`MaxBodyBytes` (8 MiB), `MaxRequests` (64) and `MaxConcurrent` (8) default to
values a correct run stays far below. A harness that loops ends the test with a
429 in a few seconds instead of hanging it until the suite timeout.

## Model metadata without models.dev

OpenCode refuses to run a model its catalog does not know, and its catalog
comes from `models.dev`. The lane writes
`$HOME/.config/opencode/opencode.json` — the workspace root is `$HOME` — with
the model entries and nothing else. OpenCode deep-merges that under the
recipe's `OPENCODE_CONFIG`, so the recipe still owns the broker base URL, the
`ref:` placeholder and the model selection, and the lane owns only discovery.

`TestPlanBOpenCodeDeterministicModelLane` asserts this rather than claiming
it: before the harness starts it points `models.dev`, `models.opencode.ai` and
`opencode.ai` at a dead local address in the container's `/etc/hosts`, so a
lane that still depended on external discovery fails. `registry.npmjs.org`
stays reachable, because the pinned install is a declared prerequisite and not
model discovery.

## Pinning the harness

`opencode-ai@1.18.27`, integrity
`sha512-5xrG2gQEwV2sLus30SZX9GyLbPX3z57BCxddedDM0wx1bgnwlHVLOS/FD2uve7fEZlmkr7KYFbvs65ySz1rwzA==`.

The recipe's install line stops at `command -v opencode`, and a global npm
install lives in the image rather than in the workspace, so a restored
workspace would otherwise reinstall whatever npm calls latest. The lane writes
a shim at `<workspace>/.planb/bin/opencode` and puts it first on `PATH`. Every
launch either finds the pinned version already installed or fetches the exact
tarball, checks that it still hashes to the recorded integrity, installs it,
and asserts the binary reports the pinned version.

Changing either constant is a visible dependency update and needs a run of both
deterministic tests.

## The leak scan and its positive control

`planBLeakScan` is one function, shared by the deterministic lane and the live
B7 lane, so both scan the same surfaces:

| Surface | How |
|---|---|
| workspace tree | the docker backend's host directory, after the snapshot pass re-owns it |
| OpenCode's own state | `.local/share/opencode` inside that tree, asserted non-empty first |
| snapshot | the uploaded artifact, expanded — scanning the gzip blob would find nothing and call it clean |
| events | every event payload on the workspace stream |
| diagnostics | `remount doctor` control and node output, with verification on |

The ACP transcript is scanned in the agent lane, where one exists: the same
search first finds the tool call, the shell result and the stop reason, and is
only then believed when it reports the upstream value absent.

Each scan proves itself before it is believed. A synthetic canary is planted in
a workspace file, in the following snapshot, and in a posted event, and a scan
that cannot find its own canary fails the lane. `AGENTS.md` already records a
leak scan that reported clean because its pipeline always exited zero; a scan
that cannot demonstrate it works is not evidence.

## The approval ordering claim (B5)

The agent runs with `Approve: on-request` and a project-level OpenCode config
that makes the shell tool ask. OpenCode issues `session/request_permission`,
the node parks it as a durable Approval, and a deterministic test client
decides it. No person waits at a prompt.

The scripted shell command assembles its marker rather than containing it
(`printf 'remount-planb-%s-ok\n' shell`), so a search for
`remount-planb-shell-ok` matches the tool *result* and never the request that
asked for it. At the moment the decision is made the test asserts the marker is
in neither the transcript nor any request the upstream saw, and it records how
many requests the upstream had seen. After the decision it asserts that
`approval.decided` committed at a later event sequence than
`approval.pending`, and that the first request carrying the marker arrived
after that recorded count.

## Timing

On an Apple silicon host with the image already pulled:

| Test | Wall clock |
|---|---|
| `TestPlanBOpenCodeDeterministicModelLane` | ~8 s |
| `TestPlanBOpenCodeAgentTranscriptApprovalAndResume` | ~15 s, including a real sleep, restore and reinstall |
| both, in one `go test` under `env -i` | ~23 s |
| `TestRunOpenCodeDockerIntegration` (live) | ~78 s, and only with a key |

The workspace root is `$HOME`, so both lanes point `npm_config_cache` outside
the tree. Left alone, npm parks about 264 MB of tarballs in the workspace and
every snapshot, restore and leak scan in the lane becomes a walk of the package
cache: it cost roughly a minute per test and hid a snapshot rate limit behind
its own slowness.

## What this does not prove

- It is not evidence about the OpenAI service. A local endpoint proves request
  shape and broker behavior only; B7 remains the only lane that touches the
  real provider, and it is optional.
- It is not evidence about isolation. The docker backend cooperates rather than
  enforces egress, so the profile is `local`.
- It is not offline. Docker must be running and `registry.npmjs.org` must be
  reachable for the pinned install. Either being absent makes the lane
  `unavailable`, never a pass.
- The pin is evidence for exactly one OpenCode version. A newer OpenCode may
  change the API it calls or the config it accepts.
