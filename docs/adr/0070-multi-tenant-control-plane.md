# ADR 0070: Multi-tenant control plane, quotas, and usage metering

Status: Accepted

Date: 2026-09-03

## Context

Identity already authenticates a subject with a tenant claim, but authentication
alone does not make a control plane multi-tenant. Tenant lifecycle, resource
admission, placement policy, retention policy, usage accounting, and billing
delivery need one durable authority. Checking a quota in memory and creating a
resource in a later transaction permits concurrent overcommit. Filtering a list
after it is loaded leaks whether another tenant's resource exists. Advancing an
export cursor before a remote billing service accepts usage loses billable data.

The workspace remains trusted with no credential. Billing credentials and
customer routing remain in the control plane and are resolved only at the
outbound request boundary.

## Decision

### Tenant authority and isolation

`internal/tenant.Store` is the durable authority for tenant state and policy.
Each tenant has an immutable ID, state (`active`, `suspended`, or terminal
`deleted`), monotonic revision, quota policy, retention policy, residency
policy, and non-secret billing routing metadata. Policy updates and lifecycle
changes use expected-revision compare-and-swap and a tenant-scoped operation ID.
An exact replay returns the recorded result; reuse with different arguments is
`conflict`.

Every resource row in the control plane carries a non-empty tenant. A normal
subject may address only the exact tenant in its authenticated claim. The only
cross-tenant authority is an authenticated operator whose claim tenant is `*`.
Adapters must authorize that rule before lookup, including get, list, usage,
event, artifact, node, volume, and notification paths. The tenant package
rejects empty and wildcard tenant keys on all tenant-scoped reads, so accidental
unfiltered use cannot become a global query. An operator list is an explicit
control-plane operation rather than an empty-tenant query.

Suspension rejects new resource admission but does not stop cleanup, usage
recording, or export. Deletion is terminal and is rejected while quota
reservations remain. Deleted tombstones continue to reserve the tenant ID.

### Lifecycle boundary statement

For tenant creation or policy mutation:

- Authority: an authenticated operator (`tenant=*`) after expected-revision and
  operation-id validation.
- Resource: the tenant row and its policy.
- Irreversible action: making a new tenant/policy revision authoritative to
  schedulers and API callers.
- Durable commit point: the SQLite transaction that writes `tenants`, records
  `tenant_operations`, and stages the corresponding canonical event in
  `event_outbox`.
- Observable postcondition: `Get` returns the committed revision after restart,
  exact retry returns the same result, and the canonical log contains exactly
  one tenant lifecycle event. A post-commit outbox drain failure is success;
  startup drain delivers the already-durable event.

For quota admission:

- Authority: the resource creator/claimer after tenant authorization and a
  fresh read of the active tenant policy.
- Resource: one `(tenant, resource kind, resource ID)` reservation.
- Irreversible action: allowing the corresponding workspace, node, artifact
  bytes, or active session to become externally observable.
- Durable commit point: one SQLite transaction that reads current reservations,
  writes the reservation or durable denial result, and stages
  `quota.reserved` or `quota.exceeded`.
- Observable postcondition: the resource boundary proceeds only after a
  successful reservation; concurrent attempts never exceed the limit; a denied
  retry remains `resource_exhausted` without another event. Resource creation
  must then commit in the same transaction through the integration seam below,
  or compensate by releasing the reservation before reporting failure.

For meter recording:

- Authority: the control-plane component that owns the measured boundary.
- Resource: immutable `(tenant, meter event ID)` usage.
- Irreversible action: accepting usage that may later be invoiced.
- Durable commit point: one SQLite transaction that writes the meter row and
  stages the canonical `usage.*` event.
- Observable postcondition: aggregate usage and export both contain the event
  after restart; exact replay returns its original sequence; changed replay is
  `conflict`.

### Quotas and placement

The four hard concurrent dimensions are `MaxWorkspaces`, `MaxNodes`,
`MaxArtifactBytes`, and `MaxActiveSessions`; zero means unlimited. Callers
reserve a positive integer amount before making a resource visible and release
the exact reservation when the resource is durably removed or a session's
active-capacity handoff completes. Idempotency lookup occurs before quota
evaluation, so retry at capacity does not fail a previously successful call.
Arithmetic avoids overflow. A quota denial commits its operation result and
`quota.exceeded` event before returning `resource_exhausted`.
Lowering a quota below current usage does not destroy running resources; it
blocks further admission until releases bring usage back below the limit.

Residency is an allowed-region set plus required node labels. The scheduler
must apply `tenant.MatchResidency` before offering a claim; labels form an AND
predicate and the region set is an allow-list. Placement failure is unavailable
capacity, never permission to violate residency.

Retention values govern canonical events, session logs, artifacts, and meter
events. Zero selects the deployment default. Every GC invocation is bounded.
Active quota reservations and deleted tenant tombstones are never age-pruned.
Operation journals are retained for a configured interval and have both global
and per-tenant bounds. Reservations and exporter cursors also have per-tenant system safety
ceilings even when a customer quota is unlimited. Meter rows are bounded per tenant; when an exporter cursor is behind,
retention cannot delete its unexported rows, so admission fails visibly with
`resource_exhausted` rather than silently losing billing data.

### Canonical usage

The only meter kinds are:

| Kind | Recording boundary |
|---|---|
| `usage.workspace_seconds` | whole elapsed seconds since the prior durable lease renewal, recorded at each renewal |
| `usage.brokered_request` | one after the broker accepts an upstream request |
| `usage.bytes_in` | bytes actually accepted at an ingress boundary |
| `usage.bytes_out` | bytes actually delivered at an egress boundary |
| `usage.storage_bytes` | whole byte-time sample according to the deployment's documented sampling interval |

Values are positive integers. Producers supply a stable ID of at most 100
characters; the store assigns a global monotonic sequence. Workspace,
principal, and binding fields are attribution, not authority. `/v1/usage` and
the CLI aggregate only the authenticated tenant unless the explicit operator
path selects another tenant.

### Export and delivery semantics

`BillingExporter.Export(context.Context, []MeterEvent) error` receives a
bounded ordered page. `Store.ExportOnce` reads from a durable
`(tenant, exporter)` next-sequence cursor, sends the page, and only then
compare-and-swaps the cursor and emits `usage.exported`. A crash after remote
acceptance and before cursor commit repeats the page. Delivery is therefore
at least once, ordered per tenant, never silently at most once. Exporters must
deduplicate by event ID. `EnsureExporter` must be called when billing is
configured, before meter retention runs.

The generic adapters are newline-delimited JSON to an `io.Writer` and a versioned
JSON HTTPS envelope. Generic HTTP destinations require HTTPS, port 443, an
exact hostname allow-list, no userinfo/query/fragment, no literal IP, proxy
disabled, redirects disabled, TLS 1.2 or later, and connect-time DNS validation
that rejects private, loopback, link-local, shared, benchmark, documentation,
reserved, or otherwise non-global addresses. The response body is bounded and
never copied into an error.

The Stripe adapter uses the stable v1 `POST /v1/billing/meter_events` form API,
not the preview v2 stream. It sends `event_name`, `identifier`, `timestamp`,
`payload[stripe_customer_id]`, and integer `payload[value]`. The canonical meter
event ID is both Stripe's identifier and the `Idempotency-Key`. Stripe documents
the identifier as unique for a rolling period of at least 24 hours, timestamps
as no more than 35 days old or five minutes in the future, asynchronous meter
processing, whole-number values, a v1 live-mode rate limit, and exponential
backoff for `429`. Remount validates the timestamp window and relies on its
durable cursor/retry schedule; it does not pretend this is global exactly-once
delivery. See Stripe's official [create meter event API](https://docs.stripe.com/api/billing/meter-event/create)
and [usage recording guide](https://docs.stripe.com/billing/subscriptions/usage-based/recording-usage-api).

API keys and generic bearer credentials are callback results resolved for one
request. They are not fields in tenant policy, SQLite rows, canonical events,
JSON output, or error strings. Customer IDs and event names are non-secret
routing metadata.

### Integration contract

The root integration constructs the store with the existing shared SQLite DB
and canonical log:

```go
tenants, err := tenant.NewStore(sqlite.DB(), log, tenant.Options{...})
```

The concrete seams are:

- Operator administration: `Create`, `UpdatePolicy`, and `SetState`, each with
  `tenant.Mutation{OperationID, Actor, ExpectedRevision}`.
- Read policy immediately before enforcement with `Get`; map `tenant.Error`
  codes directly to protocol `bad_request`, `not_found`, `conflict`,
  `resource_exhausted`, and `unavailable` without matching error text.
- Before workspace create, node claim, artifact growth, or session activation,
  call `Admit(tenant.Admission{Tenant, OperationID, Actor, Resource,
  ResourceID, Amount}, commit)`, where `commit(*eventlog.Tx)` writes the resource
  row and stages its event. Call `Retire(..., commit)` at the corresponding
  durable removal or active-session capacity handoff. Those callbacks give the
  reservation, resource, and events one transaction. `Reserve` and `Release`
  are convenience wrappers only for callers whose resource mutation is already
  complete or is not a SQLite row; such callers must compensate synchronously
  on failure and may not report success first.
- Filter scheduler candidates through `MatchResidency(policy.Residency, labels)`.
- At each tabled measurement boundary call `RecordMeter` with a producer-stable
  ID. Aggregate `/v1/usage` with `UsageSummary`; concurrent capacity comes from
  `CurrentUsage`.
- Register each configured sink with `EnsureExporter` before GC and run bounded
  `ExportOnce` calls. Retry transient errors with capped exponential backoff and
  jitter. An old Stripe event is an observable operator error, never skipped.
- Apply the returned retention policy to eventlog, session, and artifact GC;
  call `PruneMeter` in bounded pages. Report a stalled cursor/capacity condition
  in status, inspect, doctor, metrics, and canonical events.

Every HTTP, protocol, SDK, and CLI request must carry or derive a tenant and an
idempotency key before these calls. No package fallback may infer tenant from a
resource discovered by an unscoped lookup.

## Consequences

Tenant policy, admission, metering, and export progress survive restart and
compose with the canonical event outbox. Quota races serialize at the durable
authority. Billing delivery can duplicate but cannot silently skip. Explicit
bounds turn retention or exporter failures into visible backpressure.

The store adds SQLite write traffic and a reservation integration step. A
deployment must monitor quota denials, meter capacity, export lag, and oldest
unexported event age. Stripe processing is asynchronous, so local cursor
advance means Stripe accepted the event, not that an invoice already reflects
it.
