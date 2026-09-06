# 0098. Provider subscription auth is explicit and confidential

Status: accepted (2026-09-06)

## Context

Claude Code and Codex can authenticate either with a provider API key or with a
user's Claude.ai/ChatGPT subscription. Those modes have different billing,
credential residency and revocation boundaries. Inferring one from a missing
binding or an ambient environment variable can silently spend the wrong quota.

Provider-native login is interactive and may print authorization URLs, device
codes, callbacks and account status. An ordinary Remount session deliberately
supports replay, disk spill and durable session-log publication, so it is the
wrong channel for that output. Conversely, accepting an arbitrary client
program merely because it is marked sensitive would create a general-purpose
unlogged command channel.

The provider CLIs already own OAuth/device authorization, token formats,
refresh and logout. Reimplementing those protocols in Remount would duplicate
security-sensitive provider behavior and drift from the supported client.

## Decision

Claude and Codex launches require an explicit billing mode:

- `--auth subscription` uses provider-native login state;
- `--auth api-key` requires a compatible brokered provider binding.

Omission is refused, and neither mode falls back to the other. Subscription
launchers unset provider API-key, token and base-URL variables, discard raw
status output, and proceed only when Claude reports first-party OAuth or Codex
reports ChatGPT login. The selected mode is recorded in `run.auth` and the
`remount.auth` workspace label. Resume preserves it and refuses a conflicting
override. Handoff accepts API-key mode but refuses subscription mode because it
does not import another machine's login.

`remount auth login|status|logout RECIPE --ws WS` invokes the official
provider commands. The provider remains authoritative for authorization,
refresh, logout and provider-side revocation.

Provider auth operations use additive `s.open` metadata:
`sensitive: true` and `auth_operation: {recipe, action}`. The node admits only
the exact built-in Claude or Codex command generated from its own contract,
with no client-supplied environment or working directory. The session:

- streams only to the opening client and refuses `no_sub`;
- cannot also be a harness run;
- refuses later attach and archived restoration;
- uses bounded memory with no spill or durable session-log publication;
- omits raw argv from session metadata and durable events;
- emits only sanitized start/finish events with recipe, action and exit
  metadata;
- is retained in memory for at most five minutes after completion.

Subscription authentication is restricted to the `local` security profile.
Its credential remains in the provider's normal state under the workspace
home. Same-UID and root code in that VM can read it. Filesystem persistence can
carry it through reconnect, snapshot, sleep and wake; Remount does not claim
that this makes the credential secret-blind or safe on an untrusted node.

The node/session lifecycle remains authoritative under the workspace grant and
generation. The provider process exit is the Remount commit point for the auth
operation; the sanitized finish event is the observable result. Provider-side
account state is authoritative beyond that boundary.

## Consequences

- A user chooses subscription quota or API billing deliberately on every new
  Claude/Codex launch.
- Ambient provider variables cannot override a subscription launch.
- Login output is not available for replay after a disconnect; the user reruns
  the operation.
- Sensitive sessions cannot be used as arbitrary confidential shells.
- Subscription credentials persist with a trusted-local workspace and share
  its compromise boundary.
- Custom recipes cannot add new sensitive provider commands without a node
  code change and review.
- Tmux, if added later, may improve same-host process reattachment but does not
  become auth, lifecycle, replay or migration authority.
