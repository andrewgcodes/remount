# ADR 0068: principal tokens and one-time node enrollment

Status: accepted

## Context

A shared bearer cannot identify the principal behind a request and is unsafe
to bake into provisioned-node images. Pools also need a credential that can
authorize one node registration without becoming a reusable node secret.

## Decision

Remount principal credentials are short-lived Ed25519-signed tokens containing
an immutable id, subject, tenant, roles, kind, issue time and expiry. Access
tokens default to one hour; refresh tokens are distinct signed credentials.
Revocation is checked on every authentication. Roles are `operator`, `agent`,
`node`, `viewer` and `service`; tenant comparison precedes every role decision,
with only an operator whose tenant is `*` able to cross tenants.

Node enrollment credentials are 256-bit random opaque bearers, live for at
most ten minutes and are consumed atomically. Persistence stores only their
SHA-256 digests. Consumption, the node-id/public-key binding and
`node.enrolled` are one SQLite/outbox transaction. A second distinct node or
key cannot consume the credential; the enrolled node reconnects by signing its
hello with the already-bound private key, without needing any reusable node
secret. Pools place the plaintext token only in the provider's protected
environment, never argv, provider labels, a machine record or an event.

The identity package implements the control plane's existing authentication
and authorization seams. OIDC device flow is an exchange adapter; it does not
create a second token or authorization model.

## Rejected alternatives

- A shared server/node token cannot express tenant, role, revocation or
  principal attribution.
- A reusable pool secret in an image survives machine destruction and gives a
  compromised image authority to add arbitrary nodes.
- Storing plaintext enrollment tokens makes a database read sufficient to
  join the fleet.
- Authorizing from client-provided principal fields lets a bearer choose its
  own identity.

## Invariant

Identity is derived only from a verified, live, unrevoked credential. A node
enrollment token can commit at most one registration, and only its digest is
durable. Tenant isolation is evaluated before role permissions.
