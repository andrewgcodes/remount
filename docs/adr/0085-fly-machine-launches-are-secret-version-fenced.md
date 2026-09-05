# ADR 0085: Fly Machine launches are secret-version fenced

## Status

Accepted.

## Context

ADR 0062 selected staged Fly app secrets so an enrollment token never appears
in Machine configuration, arguments, inventory, or logs. A staged secret update
and a Machine launch are separate provider operations. Waiting for the Machine
to enter `started` does not prove that an unfenced launch observed the secret
update.

Fly's app-secrets API returns a monotonically increasing version. Machine
creation accepts `min_secrets_version`, which delays launch until the provider
can supply secrets at least as recent as that version.

## Decision

The Fly driver stages enrollment tokens through the app-secrets API and requires
the returned non-zero version. It passes that version as
`min_secrets_version` on Machine creation. A missing version is an error; the
driver does not launch an unfenced Machine.

The Machine process still receives only a secret-name reference. The staged
value is removed after Fly reports the Machine started, including best-effort
cleanup after a failed create.

## Consequences

- Machine startup is fenced against Fly secret-propagation lag.
- The server no longer needs the `fly` CLI to provision Fly Machines.
- Provider `started` remains only provider lifecycle evidence. It is not
  authenticated Remount enrollment or node readiness.
- Pool usability remains authoritative only after the control plane consumes
  the one-time enrollment token and observes the expected online node identity.
