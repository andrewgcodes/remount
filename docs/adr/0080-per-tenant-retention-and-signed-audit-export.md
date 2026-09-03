# ADR 0080: Per-tenant retention removes content in place; audit export is tenant-scoped and signed

**Status:** accepted

## Context

Tenant policy has carried `Retention{Events, SessionLogs, Artifacts, MeterEvents}`
and `Residency{AllowedRegions, RequiredLabels}` since ADR 0070, but only
`SessionLogs` was ever enforced. `Events` and `Artifacts` were validated,
persisted and then never read, which is worse than absent: an operator could
set a seven-day event retention, see it accepted, and keep the data forever.

The canonical event log is one sequence and its retention deletes only a
contiguous oldest prefix. That is deliberate. ADR 0049 and ADR 0076 made a
retention hole observable as an explicit gap rather than as a shorter answer,
and `internal/compliance` relies on exactly that property: it walks every
sequence in the requested range and reports a `GapError` the moment one is
missing. Deleting one tenant's rows out of the middle of the sequence would
turn a retained-but-incomplete range into a bundle that looked complete.

`internal/compliance` itself was already written and tested — deterministic,
tenant-filtered, Ed25519-signed, hash-committed — but nothing called it. There
was no command, no operation, and no key.

## Decision

### 1. Event retention is two mechanisms, not one

**The prefix is deleted at the oldest cutoff any tenant still requires.**
`Control.TenantRetentionPlan` resolves every active tenant's `Retention.Events`
against the server's own `EventRetention` and takes the minimum instant. A
tenant that keeps events for ninety days therefore holds rows a seven-day
tenant would rather have gone. This is the property that makes one tenant's
policy unable to destroy another tenant's evidence, and it is the only
mechanism that removes rows.

**Between a tenant's own cutoff and that floor, the tenant's event content is
removed in place.** `eventlog.Log.RedactTenant` replaces the payload of one
tenant's expired rows with a constant marker,
`{"remount_redacted": "tenant_retention"}`. Sequence, time, type, tenant,
workspace and principal survive. Nothing is deleted, reordered or renumbered,
so contiguity — and therefore the compliance exporter's ability to distinguish
"complete" from "gapped" — is untouched. This is what makes `Retention.Events`
actually delete on the tenant's own schedule rather than on the slowest
tenant's schedule.

The redaction marker travels into an exported bundle as ordinary event content,
so a bundle covering a redacted range is explicit about it. A verifier sees a
complete, contiguous, signed range in which some payloads say they were
redacted. It never sees a silently shorter history.

The limits are stated rather than hidden:

- the envelope outlives the payload, so a tenant with a shorter policy than the
  longest-lived tenant is reported by `doctor` as `tenant.retention_violation`;
- events belonging to no tenant are held to the same floor, so the plan
  records how much longer than the server default that is; and
- the `MaxEvents` row cap remains a capacity backstop that can delete an oldest
  prefix regardless of age. It is a last resort, not a policy.

### 2. Artifact retention may only accelerate collection, and never crosses a namespace

`Retention.Artifacts` is a maximum age. The reference-aware collector already
removes unreferenced objects after a shared grace window; a tenant policy
shorter than that window collects sooner, and a policy longer than it changes
nothing. `artifact.TenantRetentionCollector` takes a per-tenant cutoff map
resolved once per namespace before that namespace is swept, so one tenant's
schedule can never select another tenant's object.

Reachability outranks age unconditionally. An object named by a live workspace,
base, fleet operation or session-log reference is never a candidate, at any
age. A store that cannot separate tenants — the single-namespace compatibility
shim — deliberately does not implement the interface; the server then keeps the
shared cutoff and `doctor` reports `tenant.retention_unavailable`, because
sweeping a shared namespace on one tenant's schedule would be the exact bug
this decision exists to prevent.

### 3. Residency is enforced at placement and audited for drift

Placement already refuses a node outside the tenant's residency policy. That
refusal moves `remount_tenant_residency_denied_total` and nothing else, because
it runs per candidate node inside the placement loop under the control mutex,
where a durable write would be both a lock violation and an amplifier. The
paired durable record is `residency.denied`, emitted by
`Control.EnforceTenantResidency` on the maintenance tick for a workspace that
is actually *held* outside its policy — the drift case a scheduler check cannot
catch, because the policy changed under a running workspace.

`doctor` reports `tenant.residency_violation` for a verified mismatch and
`tenant.residency_unavailable` when tenant policy or node labels cannot be
read. Consistent with the rest of the diagnostic surface, a check that cannot
run is unavailable, never healthy.

### 4. Audit export is authorized on the subject, bounded, and signed by a server-held key

`audit.export` resolves the tenant from the authenticated subject before
reading any event. A caller bound to one tenant cannot name another, `"*"` is
never a valid export subject, and no sequence number in the request can widen
the result, because the exporter filters on the resolved tenant rather than on
anything in the body. Administrative authority is required: an agent
credential cannot export its own tenant's audit record.

The range is inclusive on both ends and bounded, the bundle is bounded, and a
range the log can no longer serve completely is refused with `evicted` and the
oldest sequence still available. A bundle is never truncated to fit.

The signing key is Ed25519, generated on first use, stored once in the control
database and never returned to a client. `audit.key` publishes only the public
half, which is what makes a bundle verifiable by an auditor who cannot reach
the control plane. Two exports of one range produce byte-identical event lines
and the same `payload_sha256`; only the manifest's `created_at` differs.

## Consequences

- A tenant's event payloads are destroyed on its own schedule; its event
  envelopes are not. Deployments that need the envelope gone too must shorten
  the longest tenant retention, and `doctor` names which tenant is holding it.
- The canonical log gains one in-place mutation. It is content-only, constant,
  convergent, tenant-scoped and never touches ordering, but the log is no
  longer strictly append-only and this document is where that is recorded.
- An operator can produce a signed, verifiable, tenant-scoped disclosure with
  one command, and can verify it later with nothing but the public key.

## Alternatives rejected

**Delete one tenant's rows from the middle of the sequence.** This is the
obvious reading of "per-tenant retention" and it breaks the guarantee every
export depends on. A range with a hole would either report a permanent gap that
no operator could ever clear, or — worse — be filtered out of the gap check and
present as complete.

**Apply the shortest tenant's retention globally.** One tenant's policy would
then delete another tenant's evidence, which is the failure the required proofs
exist to forbid.

**Retain per tenant by partitioning the log.** A per-tenant sequence would make
independent retention trivial, but it gives up the single total order that
`seq` currently provides, and every cursor, export, subscription and
gap-detection path is written against that order. That is a larger change than
this decision, and it is the direction to take if envelope-level per-tenant
deletion later becomes a requirement rather than a reported limit.
