# ADR 0068: principal identity, session capabilities and node enrollment

Status: accepted

## Context

A shared bearer cannot identify the principal behind a request and is unsafe
to bake into provisioned-node images. Pools also need a credential that can
authorize one node registration without becoming a reusable node secret.

## Decision

Remount principal credentials are bounded Ed25519-signed tokens containing an
immutable id, subject, tenant, audience, roles, kind, principal revision, issue
time and expiry. Access tokens default to one hour and are limited to 24 hours;
refresh tokens default to 30 days and are limited to 90 days. Every parse
limits encoded and decoded size, rejects unknown internal fields and roles,
requires the exact issuer and audience, and applies at most the configured
clock skew. Hosted callers use atomic `RotateRefresh`; the older `Refresh`
method remains source-compatible but must not be exposed by a production token
endpoint because it does not rotate the bearer.

Individual token ids can be revoked until their natural expiry. Principal
revocation advances a durable `(tenant, subject)` revision in the same SQLite
transaction as `identity.principal_revoked`. Access, refresh and session tokens
carry the revision at issue time and fail closed when it differs. Roles are
`operator`, `agent`, `node`, `viewer` and `service`; tenant comparison precedes
every role decision, with only an operator whose tenant is `*` able to cross
tenants.

Each managed session receives a separate short-lived `session-cap`, not the
workspace capability. It binds tenant, principal, roles, workspace, generation,
revision and the canonical identity
`spiffe://<tenant>/ws/<id>/gen/<n>/principal/<principal>`. A broker verifier
checks that token on every request. Therefore a principal revision stops only
that principal's broker requests while generation fencing still stops every
capability from an old workspace incarnation. This uses a SPIFFE-form identity;
it is not a claim that Remount implements the SPIFFE Workload API or issues an
X.509/JWT SVID.

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
create a second authorization model. Discovery requires the configured issuer
exactly, remote bodies and collections are bounded, polling preserves RFC 8628
`authorization_pending`, `slow_down`, denial and expiry states, and provider
tokens never enter events. The built-in JWKS verifier supports RS256, OIDC's
mandatory-to-implement signing algorithm, selects an exact bounded `kid`, and
rejects algorithm confusion. Issuer, audience, authorized party, nonce, issue
time and expiry are checked again after signature verification. Exact IdP group
names map to the five Remount roles; no matching group is denial.

Existing self-issued tokens without the new audience claim are deliberately
invalid after this rollout. Operators must log in again; silently accepting a
missing audience would preserve a confused-deputy credential.

## Authority and durable boundaries

- Authority: the control-plane signing key plus the current durable principal
  revision and node binding.
- Resources: an authenticated principal, a session's broker authority, and one
  node-id/public-key registration.
- Irreversible actions: consuming a refresh/enrollment bearer, advancing a
  principal revision, and binding a node id to a key.
- Durable commit: the SQLite resource row and its canonical event commit in one
  `eventlog.Transact` transaction before success is returned.
- Fences: token id, audience, kind, principal revision, expiry, workspace
  generation, and the enrolled node's Ed25519 key.
- Observable postcondition: an old revision is refused everywhere, a refresh
  or enrollment has at most one winner, and unrelated principals and workspace
  state remain live.

Revocation rows are eligible at token expiry and enrollment rows at their
maximum ten-minute expiry. `Maintainer.CollectExpired` removes and emits events
for a bounded batch; enrollment admission also collects a bounded expired batch
inside its transaction and rejects above the configured live cap (10,000 by
default). Principal revision rows are one per known principal and are retained
as authorization history. Production wiring must schedule maintenance, expose
counts and cleanup age, and report unavailable cleanup as unavailable rather
than healthy.

## Required integration contract

The core is necessary but does not by itself complete E8. Shared components
must preserve these rules:

1. The server creates `identity.Manager` with deployment-specific issuer and
   audience and no shared-token fallback in production. The relay authenticates
   every hello from the credential, ignores caller-supplied principal fields,
   refuses node-role credentials on client hellos, and closes or
   reauthenticates a connection when its access token expires.
2. Every control operation calls the manager as `control.Authorizer`; tenant is
   checked before role. Revoking a principal also advances each affected
   workspace's existing `AuthzRevision` through the central state machine and
   emits the state event in the same transaction.
3. Node renew carries that revision. A node adopts only the authoritative newer
   revision, closes precisely the revoked principal's sessions, and revalidates
   revision after acquiring the workspace boundary. Other principals and the
   workspace continue.
4. Session open calls `IssueSessionCapability` only after authorization and
   generation revalidation. The node injects a per-session broker URL; it does
   not write this capability into snapshot state. The broker calls
   `VerifySessionCapabilityFor` for every request so its local tenant/workspace/
   generation must match before policy or secret substitution.
5. `remount login` performs discovery and RFC 8628 device flow, constructs the
   RS256 verifier from the discovered JWKS URI, maps trusted group/tenant
   claims, and exchanges the verified identity for Remount tokens. It stores
   the refresh bearer in an OS credential store or mode-0600 file and uses
   `RotateRefresh`. The CLI never prints refresh, provider access, ID, device or
   enrollment bearers in logs/events.
6. Principal create/list/revoke and token issue endpoints authorize operator
   role and tenant before mutation. Node enrollment remains a separate 256-bit,
   digest-only, one-time ten-minute authority.
7. A joined maintenance loop calls `CollectExpired` with a bounded limit and
   publishes retained/live counts. Cancellation is joined before shutdown;
   repeated cleanup failures degrade readiness rather than silently allowing
   retained state to grow.

The E8 simulator must revoke `agent:alice` mid-run and prove, within one renew
interval, that her next node call and broker request fail and her live sessions
close while the workspace, owner capability, and another principal's session
continue. `integration/identity` proves only the durable core across restart.

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

## Standards

- OpenID Connect Discovery 1.0: https://openid.net/specs/openid-connect-discovery-1_0.html
- OpenID Connect Core 1.0, ID Token validation: https://openid.net/specs/openid-connect-core-1_0.html#IDTokenValidation
- OAuth 2.0 Device Authorization Grant (RFC 8628): https://www.rfc-editor.org/rfc/rfc8628.html
- JSON Web Key (RFC 7517): https://www.rfc-editor.org/rfc/rfc7517.html
- SPIFFE ID specification: https://github.com/spiffe/spiffe/blob/main/standards/SPIFFE-ID.md
