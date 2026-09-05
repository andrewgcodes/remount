# ADR 0083: Every admitted upstream attempt is charged, on every egress surface

## Status

Accepted. Amends the reservation-key paragraph of ADR 0067 and the surface
list implied by ADR 0066.

## Context

ADR 0066 and ADR 0067 describe approve-mode authority and central hard-budget
admission as properties of "a governed request". The generic HTTP proxy and
`CONNECT` implemented them; the managed package and git connectors did not.
Both connector surfaces selected a policy, substituted the bound credential,
and executed against the upstream without asking the approval authority or
reserving a budget unit. An `approve` rule on a registry or repository host
was recorded as a rule and behaved as `allow`; a hard tenant request ceiling
was invisible to `pip install` and `git push`.

ADR 0067 also let a workspace-supplied `Idempotency-Key` header select the
reservation key. The intent was that a transport retry of one provider
mutation would not double-charge. The effect was that a workspace could pin a
stable key, send the same request repeatedly, and have each request reach the
upstream while the central ledger recorded one unit. The broker cannot verify
that an arbitrary upstream honours the header, so the key was a caller-chosen
discount on a hard ceiling.

## Decision

1. One governance stage, `Broker.govern`, runs on every egress surface —
   generic proxy, `CONNECT`, package connector, git connector — after policy
   selection and connector authorization, and before credential substitution
   or any upstream I/O. It performs, in order: approve-mode authority, the
   approved rule's request count, and central hard-budget admission. Any
   refusal is written before a real secret enters the request.

2. The reservation key is fresh for every admitted attempt. The broker never
   derives it from a workspace-controlled header. The authority's exact-replay
   rule (ADR 0067) remains the transport-retry contract for a single
   reserve RPC, which reuses its own key; it is not an execution
   deduplication contract for repeated requests.

3. A workspace `Idempotency-Key` header is still forwarded to the upstream
   unchanged. Whether the upstream deduplicates the effect is the upstream's
   contract with the caller, not a reason to skip a request unit.

4. The package connector's immutable digest cache is an explicitly bounded
   result-deduplication contract: the response is served only when the caller
   named the exact digest and the immutable artifact is already held. It saves
   registry bytes, not request units. A cache hit is admitted and settled
   like any other request.

5. Connector attempts settle `request_only` once the response has been
   relayed, and `incomplete` when the connector fails before a response is
   observed, matching the generic proxy's treatment of unmetered providers.

## Consequences

- An `approve` rule on a package or git host now produces a pending approval
  and a `403` with `X-Remount-Approval`, exactly as on the generic proxy. Git
  clients see this on `info/refs` and again on each distinct pack body; that
  is the fingerprint contract of ADR 0066, not a connector special case.
- A hard request budget denies `pip`, `npm`, and `git` traffic with `429`
  before any credential is substituted.
- Two identical calls with the same `Idempotency-Key` consume two units. A
  caller that wants one charge for one logical mutation sends it once; a
  transport-level retry inside the broker's single reserve RPC is unchanged.
- The regression proof lives in `internal/broker/governance_test.go`: every
  surface, pending/denied/unavailable authority, exhausted/unavailable budget,
  one settlement per admitted attempt, the digest cache under admission, and a
  real `budget.Manager` with `MaxRequests: 1` receiving three keyed attempts.
