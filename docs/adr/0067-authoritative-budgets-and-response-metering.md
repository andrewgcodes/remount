# ADR 0067: Broker budgets reserve centrally and settle observed usage

## Status

Accepted.

## Context

The typed egress rules have node-local request and byte limits, but those
limits are scoped to one materialization. They cannot enforce a tenant,
principal, workspace, or binding allowance across retries, moves, or multiple
nodes. Checking a counter and incrementing it in separate operations would
also allow concurrent requests to overcommit the same last unit.

Model APIs report usage in different response formats, and streaming moves the
terminal usage record through a sequence of framed events. Treating a partial
response, an unsupported provider, or an unavailable price as a zero would
turn missing evidence into a budget grant.

## Decision

`Budget` attaches a rolling `1h`, `1d`, or `30d` window to exactly one tenant,
workspace, principal, or binding. It independently sets `MaxRequests`,
`MaxTokens`, and `MaxEstimatedCostMicros`; the field name deliberately says
that cost is an estimate rather than provider billing truth.

The control-plane store is authoritative. Before credential substitution or
upstream release, the broker calls `budget.Store.Reserve` with one stable
request key, the node and all four subject dimensions, the provider/model, and
a conservative input-plus-maximum-output token bound. One transaction checks
and reserves one request and the token/cost maximum against every matching
budget. Admission is all-or-none. An exact replay returns the prior reservation
even at capacity; reuse with changed arguments is a conflict. Node-local
counters may reject quickly but never grant.

After forwarding the response, the broker calls `Settle` exactly once:

- a complete supported usage record replaces the token/cost maximum with the
  observed input and output tokens;
- an unsupported provider is `request_only`, charges the request, and emits
  `budget.unmetered{reason: provider_unknown}`;
- a supported response that ends early, exceeds parser bounds, or has malformed
  terminal usage is `incomplete` and conservatively keeps the full reservation;
  it is never converted to zero usage; and
- a missing catalogue entry still accounts tokens and emits
  `budget.unmetered{reason: price_unavailable}`; if an applicable estimated-cost
  limit exists, admission is denied because that limit cannot be evaluated,
  otherwise estimated cost remains explicitly unavailable.

A complete usage result larger than its reservation is fully accounted and
flagged as an overrun. The broker integration must derive a true upper bound
from the request or refuse a supported request governed by a hard token budget;
settlement cannot undo provider work already released upstream.

Reservations have a bounded TTL. A node-death notification or TTL sweep moves
an unresolved reservation to `expired` and charges its reserved maximum. This
is intentionally conservative: releasing an ambiguous reservation would let a
crash erase usage. `budget.expired` names the reservation. Durable
implementations commit reservation/settlement/expiry rows and their events in
the same control transaction per ADR 0050.

`internal/broker/meter` observes response bytes while forwarding them unchanged.
It recognizes OpenAI, Anthropic, Google, and OpenRouter non-streaming and
streaming usage formats under hard response-byte and event-count limits.
Unknown providers are never guessed from a similar JSON shape.

Estimated text-token cost comes from the checked-in
`internal/budget/pricing.json`. Every entry is marked approximate, dated, and
identified by exact provider/model. Integer micro-USD arithmetic rounds upward.
Tenant overrides replace individual model entries without changing the
checked-in defaults. Dynamic routing, cache discounts, long-context tiers,
images, tools, and provider price changes can make this differ from an invoice;
operators update or override the catalogue and monitor unpriced results.

The executable in-memory authority bounds budgets, reservations, identifiers,
catalogue bytes, prices, tenant override sets, reservation TTL, and retention.
It also caps the number of simultaneous budget attachments evaluated for one
request so adversarial configuration cannot create unbounded admission work.
Terminal reservations remain long enough to answer the 30-day window and are
collected only by an explicit sweep. Production uses the same `budget.Store`
contract in the single authoritative control transaction; the in-memory
manager is for standalone mode and tests, not multi-controller authority.

Observable outcomes are:

- `egress.denied{reason: budget_exceeded, budget_id, limit}` for refusal;
- `budget.reserved`, `budget.settled`, and `budget.expired` for authority
  transitions;
- `budget.unmetered{reason}` for unsupported usage or price; and
- `GET /v1/usage` counters including active, incomplete, and unmetered requests.

## Consequences

Request and token admission cannot overcommit under concurrency when the
broker supplies an honest upper bound. Cost enforcement is deterministic and
conservative for catalogued text tokens, but remains explicitly estimated.
Unsupported providers retain request-limit protection without inventing token
usage. Missing terminal evidence consumes the reservation instead of silently
refunding it.

The authoritative store must be reachable before a governed request is
released; unavailable authority is a denial, not permission to use a stale
node-local counter. The rolling ledger costs storage, so production retention
and aggregation need bounded collection while preserving the full 30-day
query and idempotency windows.
