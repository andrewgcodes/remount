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

The Machine process still receives only a secret-name reference. Fly's
`started` state precedes guest process startup, so the driver retains the app
secret for the Machine lifetime and records its non-sensitive secret name in
Machine metadata. Destruction removes the secret before deleting the Machine.
A failed create removes the secret only when no Machine was created; an
ambiguous post-create failure retains it so reconciliation cannot strand a
Machine before enrollment.

## Consequences

- Machine startup is fenced against Fly secret-propagation lag.
- A Machine restart can still receive the token, but one-time enrollment makes
  it unusable after the first successful consumption.
- Normal destruction removes the provider-side secret before deleting the
  Machine. Out-of-band Machine deletion can leave a stale, already-consumed
  app secret that requires operator cleanup.
- The server no longer needs the `fly` CLI to provision Fly Machines.
- Provider `started` remains only provider lifecycle evidence. It is not
  authenticated Remount enrollment or node readiness.
- Pool usability remains authoritative only after the control plane consumes
  the one-time enrollment token and observes the expected online node identity.
