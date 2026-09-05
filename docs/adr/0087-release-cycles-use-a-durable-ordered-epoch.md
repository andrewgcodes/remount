# ADR 0087: Release cycles use a durable ordered epoch

## Status

Accepted.

## Context

Release prepare, abort, and commit already use an opaque operation ID for
exact retries. After an abort is durably published, the node retains a
same-generation tombstone so a lost acknowledgement can be replayed.

The node previously treated any different operation ID as authority to replace
that tombstone. Operation-ID inequality does not establish order. A delayed
request from an older aborted cycle could therefore arrive after a later abort
publication, replace the journal record, stop current sessions, and fence the
restored workspace.

## Decision

Each workspace stores a monotonically increasing `release_epoch`. Control
increments and persists it in the same transaction that enters the release or
destroy lifecycle state, before sending `ws.release`. The request carries both
the ordered epoch and an opaque operation ID.

The node applies these rules:

- an exact retry must match generation, release epoch, operation ID, and all
  release parameters, whether the release is still in memory or has reached
  the durable journal;
- a same-generation request may replace an abort-published tombstone only when
  its nonzero release epoch is greater and its operation ID is distinct;
- a higher generation may replace a committed tombstone;
- one operation ID cannot identify multiple release epochs;
- a different operation ID alone is never successor authority.

Abort publication clears only the active operation ID in control state. The
release epoch remains durable across restoration, restart, re-adoption, move,
sleep, and destroy. Promotion reconciles the greatest epoch present in either
the live workspace report or its abort-published release record before
authorizing another lifecycle operation.

## Consequences

- Delayed same-generation release requests fail before destructive work.
- Exact request and acknowledgement retries remain idempotent.
- A control-authorized later cycle can safely replace an abort-published
  tombstone.
- The generated protocol schema and Python and TypeScript SDK types expose the
  epoch on both workspaces and release requests.
- The additive field decodes as zero for older records and peers. A legacy
  first cycle remains readable, but a legacy successor after abort publication
  fails closed because it carries no ordering proof. Recovery assigns an
  observed epoch-zero release record a durable floor of one so failover cannot
  reopen the first-cycle compatibility exception.
