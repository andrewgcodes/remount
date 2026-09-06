# ADR 0091: Dynamic binding lifecycle and session-scoped principals

## Status

Accepted.

## Context

Bindings were static. `--bindings` was read once at start-up into
`control.bindings`, and the only way to add, change or withdraw a credential
was to edit the file and restart the control plane. That has three
consequences an adopter cannot design around:

- **Revocation is a restart.** An operator who learns a key leaked has no
  operation that stops Remount substituting it. Even a restart leaves every
  node holding an unexpired lease — the default TTL is 600 seconds, and a node
  only re-leases when a lease is within 60 seconds of expiry, so the credential
  keeps flowing for up to ten minutes after the operator acted.
- **Rotation is a restart, and a lossy one.** The same window applies, and
  there is no revision, so nothing distinguishes a lease minted from the old
  credential from one minted from the new.
- **There is no session-scoped identity.** The pieces exist —
  `CreatePrincipal`, `IssueSessionCapability`, workspace generations,
  `RevokePrincipal` — but assembling them correctly is left to the adopter, and
  assembling them incorrectly is silent.

The gap brief asks for the opposite default: create a session-scoped
principal, bind it to these destinations, hand the workspace only placeholders,
and let Remount enforce egress, audit, revocation and budgets.

## Decision

### The binding store is durable, mutable and tenant-scoped

A `bindings` table (`tenant`, `id`, `revision`, `revoked_at`, CBOR row) is
created in `migrate()` and read by `loadBindings()` during `New()`, next to the
other durable collections. An in-memory
`map[bindingKey(tenant, id)]Binding` under `control.mu` still serves
`bindingLease()`, so the lease path does no I/O under the lock. Every write
commits through `c.transact` with its event, as every other control-plane
resource does.

`--bindings` entries are **seeded** into the store on the first start that does
not already have them, with `revision` 1 and whatever tenant the file declares
— none by default, which makes them global: visible to every tenant, as they
were before. A row that already exists
wins. Rotation and revocation are durable decisions and must not be undone by
restarting with the original file. The cost is that editing the file after the
first start no longer changes anything; the operation to use is
`binding.rotate`. That trade is deliberate: the alternative — the file wins —
means a restart resurrects a revoked credential, which is the failure this ADR
exists to remove.

`binding.create` always writes an exact tenant, so global bindings can only
come from the file. A tenant-scoped binding shadows a global one with the same
id, and `bindingLease()` resolves tenant-first. Cross-tenant reads, rotations
and revocations are refused, and a workspace cannot name another tenant's
binding.

`binding.list` does show a tenant the global bindings, because any of its
workspaces may name one, and `Source` travels with them. `Source` is a
reference to a secret (`env://TOKEN`, a manager path), not the secret; the
credential itself never leaves the control plane except as a lease to a node.

### Revision, and how a revocation reaches a running workspace

`BindingSpec.Revision` increments on every rotation and on revocation.
`proto.BindingLease` gains `Revision` (the binding revision the lease was
minted from) and `Generation` (the workspace generation it was issued for), so
a lease is self-describing rather than only time-limited.

The propagation bound comes from the renew loop, which already runs at no more
than one third of the lease interval. `WSRenewResult` gains
`BindingRevision`: a 64-bit FNV-1a fingerprint over the `(id, revision,
revoked_at)` of every binding the workspace declares, computed under the lock
`wsRenew` already holds. `BindingLeaseRes` carries the same fingerprint. A node
whose stored fingerprint differs from the one renew reports re-leases
immediately.

It is a fingerprint rather than a counter because a workspace names a *set* of
bindings, and a set has no single monotonic revision to report. The node never
interprets the value; it only compares it, so a hash is sufficient. Zero is
reserved for "this workspace declares no bindings", and a hash that lands on
zero is coerced to one.

`bindingLease()` refuses rather than silently returning a shorter set:
`ErrReason(CodeUnauthorized, ReasonRevoked)` for a revoked binding and
`ErrReason(CodeNotFound, ReasonBindingMissing)` for one that is gone. On a
refusal the node **drops** the leases it holds and clears them from the broker.
A transport failure is not a refusal — the node keeps its leases and they
expire normally — because treating an unreachable control plane as a
revocation would turn a network blip into an outage.

A refusal also records the fingerprint control reported, so the node asks once
and then stops. Without that it would re-lease on every renew, forever, against
a binding the operator has already withdrawn — the retry-against-itself shape
in `MISTAKES.md` #48.

Refusing the whole set when one binding is revoked is fail-closed and
deliberate. A partial lease set would mean the node cannot tell "this binding
was withdrawn" from "control decided not to send it this time".

**Broker TTL expiry is not provider-side key revocation.** `binding.revoke`
stops Remount substituting the credential, durably and within one renew. The
credential itself remains valid at the provider until it is rotated or deleted
there. Revocation also drops `secret`/`source` from the durable row, so a
revoked credential is not carried in later snapshots of control state.

What revocation does *not* do is block traffic. The workspace still holds an
inert placeholder, and once no lease claims that placeholder the broker no
longer recognises it as one, so the request goes upstream carrying a string
that is not a credential. Denying traffic whose placeholder has lost its
binding belongs with the substitution and denial work in the broker
(ADR 0092); the property proved here is that the credential stops flowing.

### Cookies are a distinct kind

`Kind` is one of `api_key`, `bearer`, `cookie`, `header`. `cookie` exists
because the brief separates browser login state from API keys, and because the
two have different lifetimes, different rotation stories, and different blast
radii. A cookie binding is accepted, stored and leased with the same host
scoping as any other kind. It must never be mixed into an API-key binding: one
destination scope covering both means revoking the key also logs the browser
out, and a cookie leaked to an API host is a session, not a rate-limited call.

### Methods, path prefixes and retention

`Methods` and `PathPrefixes` narrow a binding beyond its destination hosts and
travel on the lease. Enforcement lives in the broker's rule order, which is
ADR 0092's scope; this ADR carries the policy to the node so that enforcement
has something to read.

`Retention{NoLog, Note}` records a provider data-retention requirement as
metadata. Remount cannot enforce a provider's policy. Recording the assertion
is still worth doing: it is the difference between an audit that can answer
"was this binding declared zero-retention" and one that cannot.

### Session-scoped principals

`principal.session.create` creates an ephemeral principal and issues its
workspace- and generation-bound capability in one call. The subject is
generated when the caller does not name one. The workspace must be claimed,
because the capability is bound to a live generation. The authority boundary is
revalidated after the capability is signed, so a capability minted for a
generation that moved in the meantime is never handed out.

The bearer is returned exactly once. The idempotency record stores the
principal, not the token: a replay re-mints the capability rather than
returning a stored one, because a credential must never enter durable state.

The ephemeral principal is an ordinary principal. `principal.revoke` ends it,
with the existing atomic identity-revision and workspace-authority commit.

The principal is created before its idempotency record commits, so a durable
write that fails in between leaves a directory entry with a generated id and no
token. That entry cannot authenticate — the bearer is never issued on the
failing path — so it is a tidiness cost rather than an authority leak, and
`principal.create` has the same shape today without any record at all.

### Reasons at existing failure sites

`proto.Error.Reason` (base commit 0ffc725) sub-classifies a code without
changing it. No code changed and no message text changed, because
`Client.staleGrantAuthority` still matches on message text and
`Client.nodeCall` retries on specific codes. The reasons added:

| Site | Code (unchanged) | Reason |
|---|---|---|
| `control.check` tenant and role denials | `denied` | `permission_denied` |
| `approvals.approvalAuthorize` | `denied` | `permission_denied` |
| `VerifyGrant` TTL | `unauthorized` | `grant_expired` |
| `node.authorizeClaims` generation | `conflict` | `generation_mismatch` |
| `node.authorizeClaims` stale authz revision | `unauthorized` | `revoked` |
| `reauthenticateClient` | `denied` | `revoked` |
| `verifySessionCapabilityForNode` capability | `denied` | `revoked` |
| `verifySessionCapabilityForNode` generation | `denied` | `generation_mismatch` |
| budget capacity, tenant quota, approval quota, volume quota | `resource_exhausted` | `quota_exceeded` |
| `bindingLease` / `wsCreate` revoked, missing | `unauthorized`, `not_found` | `revoked`, `binding_missing` |

`ReasonApprovalRequired` is not added here. The control plane returns an
approval's *status* as a value; the site that refuses a request because an
approval is pending is the broker's denial path, which is ADR 0092's scope.

A revoked principal surfaces as `denied` + `revoked`, not `unauthorized`: the
credential is checked at the re-authentication boundary, and changing that code
would break every caller that matches it today.

## Consequences

- An operator can add, rotate and withdraw a credential without a restart, and
  the withdrawal reaches a running workspace within one renew interval rather
  than at lease TTL — a bound of roughly one third of the lease rather than up
  to ten minutes.
- Editing `--bindings` after the first start has no effect. This is a
  behaviour change for existing deployments and is documented in §9.1.
- The binding table stores the credential in the control plane's SQLite file,
  at the same trust level the `--bindings` file already had. A deployment that
  wants secrets held elsewhere uses `source` with a `SecretResolver`, which
  stores only a reference.
- Binding events stream under the binding id rather than a workspace, so they
  are read from the tenant-wide event stream, not `events.tail{ws}`.
- Revoked rows are retained so an audit can still resolve a binding id seen in
  an older event. They are excluded from `binding.list` and `Control.Bindings`
  unless asked for.

## What stays static

The node allow list (`--allow`), the security profile floor, and the typed
`NetworkPolicy` on a workspace spec remain start-up and spec-time
configuration. A binding is a credential, not a network policy; making the
policy dynamic is a separate decision with a different fencing story, because
a policy change has to be ordered against in-flight requests rather than
against lease renewals.
