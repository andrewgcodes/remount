# ADR 0051: Revocation is a pushed epoch, fail closed, bounded by the lease

## Status

Accepted.

## Context

A grant is a signed claim set verified offline by the node (ADR 0010,
spec §4). That is what lets a node authorize a request with no round trip to
the control plane, and it is also why revocation did not work: a grant lived
up to an hour (capped by `LeaseUntil`), and `Workspace.AuthzRevision`, the one
field that could invalidate it, advanced only with the generation on a move or
quarantine. Removing a principal from a workspace's ACL therefore changed
nothing on the node until the principal's cached grant expired, and its live
sessions ran on regardless. There was also no operation that changed an ACL
after `ws.create`, so in practice the ACL was write-once.

Three designs were considered.

- **Short grants.** Cut grant lifetime to seconds so expiry does the work.
  Every request then costs a control round trip and the node still cannot end
  a running session, so the model is "you can't start anything new" rather
  than "you're out".
- **Online verification.** Have the node ask control on every request. Removes
  the offline property that makes the node usable across a partition and makes
  control a hot path for every keystroke.
- **Pushed epochs.** Keep offline verification and give the node the one
  number it needs to reject stale grants, delivered on a channel it already
  uses at a bounded interval: lease renew.

## Decision

1. **`AuthzRevision` is a revocation epoch.** Control advances it on every
   ACL change (the new `ws.acl` operation, owner or admin only) and, as
   before, on every generation change. It refuses at once to mint a grant for
   a principal the new ACL excludes: the ordinary `check` denies, so a revoked
   principal's *next* grant request fails immediately, before any node has
   heard about the change.

2. **Renew carries the epoch both ways.** `WSRenewReq.authz[id]` is the
   revision the node enforces; `WSRenewResult.authz_revision` is the
   authoritative one. When the node adopts a higher revision every grant
   minted under the old one fails the existing `authz_revision` check in
   `node.authorizeClaims`; the node also drops them from its cache.

3. **Control names who was revoked; the node closes exactly them.** An ACL
   change records the principals it removed as `AuthzRevocation{rev,
   principal}` on the workspace. Renew returns those recorded since the node's
   revision in `revoked`, and the node ends every live session of those
   principals with `exit{reason: "revoked"}` and emits `authz.revoked`. Other
   principals' sessions are untouched; their clients simply fetch fresh grants
   when their cached ones are refused.

4. **Bounded history, reset behind the floor.** Control keeps the last 64
   revocations per workspace and records the newest pruned revision as
   `revocation_floor`. A node whose known revision is below the floor cannot
   be given a complete list, so it receives `authz_reset` and closes every
   session of the workspace. Still-authorized principals reopen. Fail closed
   is the rule: a node that cannot tell who was revoked revokes everyone.

5. **The open window is closed too.** Authorizing an open and the session
   existing are not atomic. A push that lands between them has already listed
   the workspace's sessions and missed the new one, so after `Open` the node
   re-reads the workspace revision and, if the grant's revision is stale,
   terminates the session with the same reason and answers `unauthorized`.

6. **The bound is one renew interval, stated in the spec.** A node renews at
   no more than a third of the lease, so with the default 30 s lease a revoked
   principal loses access to the node within 10 s. A node that misses renew
   for a whole lease is fenced anyway, so no revoked grant outlives the lease.

7. **It is a named capability.** `authz-push` (ADR 0040) is what makes the
   fields above meaningful; an old node ignoring them is exactly the failure
   this ADR removes, so `isolated` and `multi_tenant` require it.

## Consequences

- Revocation latency on the node is ≤ lease/3 (10 s by default, 667 ms in the
  simulation world's 2 s lease); grant minting for a revoked principal is
  refused immediately.
- `ws.acl` re-stating the current ACL is a legitimate operation: it forces
  every grant on the workspace to re-verify, which is how a deployment whose
  *external* `Authorizer` withdrew a principal makes that stick. The built-in
  ACL is the principal binding this ADR revokes by name; an external
  authorizer's denial takes effect for new grants at once and for existing
  sessions at the next epoch, not by name. A per-principal revocation
  operation that closes sessions across every workspace of a tenant is left
  for the identity work in Phase 2.
- Widening an ACL also bumps the revision. Clients on the workspace see one
  refused call each and refetch; that cost is accepted for the simplicity of
  one rule.
- `Workspace` grows two fields (`revocations`, `revocation_floor`) and
  `ExitInfo` one (`reason`). All are additive and safe for an old peer to
  ignore, because the security behaviour hangs on the capability, not the
  fields.
- Proof: `internal/sim/revocation_test.go` revokes mid-session and asserts the
  PTY exits with `reason: revoked` within the bound, the next call is refused
  with `denied`, and the other principal's session and grant carry on;
  `internal/control/revocation_test.go` covers the epoch, the retained list,
  the floor and the idempotent replay; `internal/node/node_test.go` covers the
  cache purge, the per-principal closure, the reset and the open window;
  `internal/session` covers the exit reason.
