# ADR 0086: Fly processes select enrollment secrets explicitly

## Status

Accepted.

## Context

ADR 0085 fences Machine launch with the version returned by Fly's app-secrets
API and maps a per-Machine secret name to `REMOUNT_ENROLL_TOKEN`. Live testing
showed that a process secret reference alone was not injected: Fly accepted the
configuration and started the Machine, but the guest process had no enrollment
token.

Fly's process configuration requires `ignore_app_secrets` for explicit secret
selection. Without it, the accepted `secrets` mapping did not produce the
requested environment variable in the tested Machine runtime. Falling back to
the app-wide `REMOUNT_ENROLL_TOKEN` name made enrollment succeed, but would
allow concurrent Machine launches to overwrite each other's one-time tokens.

## Decision

Each Fly Machine process sets `ignore_app_secrets` to true and explicitly maps
its unique staged app-secret name to `REMOUNT_ENROLL_TOKEN`.

The secret version fence, unique name, lifetime retention, and cleanup rules in
ADR 0085 remain unchanged.

## Consequences

- Concurrent Machine launches do not share or overwrite one app-wide
  enrollment-token name.
- A Machine receives only its selected enrollment secret rather than every app
  secret.
- Fly accepting a Machine configuration remains insufficient evidence.
  Authenticated Remount enrollment and an online exact-candidate node are the
  readiness proof.
