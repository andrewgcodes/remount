# ADR 0071: Provider webhooks enter the event log; notifications leave through durable cursors

## Status

Accepted.

## Context

`POST /v1/events` already accepted Remount's JSON envelope with either an API
bearer or a shared HMAC. GitHub, Slack and Linear sign different byte strings
and send provider-native bodies. Treating those bodies as the Remount envelope
would encourage an unauthenticated translation proxy and would make repository
or label-triggered wakeups depend on another process.

Notifications have a different durability hazard. Sending first and then
crashing before recording progress duplicates a notification; recording
progress first and then crashing silently loses it. The latter outcome is not
acceptable for an egress approval, a fenced workspace or failed pool capacity.

## Decision

### Inbound provider adapters

The HTTP boundary reads at most 1 MiB of the raw body and verifies it before
JSON decoding:

- GitHub uses `X-Hub-Signature-256`, the `sha256=` prefix and HMAC-SHA256 of
  the unmodified body. The implementation carries GitHub's published test
  vector and compares decoded MAC bytes in constant time. See [GitHub's webhook
  validation documentation](https://docs.github.com/en/webhooks/using-webhooks/validating-webhook-deliveries).
- Slack uses `X-Slack-Signature: v0=...` over
  `v0:<X-Slack-Request-Timestamp>:<raw-body>`. Timestamps outside the configured
  window (five minutes by default) are rejected in either direction. See
  [Slack's request-signing documentation](https://api.slack.com/docs/verifying-requests-from-slack).
- Linear uses the hexadecimal `Linear-Signature` HMAC-SHA256 of the raw body.
  See [Linear's webhook documentation](https://linear.app/developers/webhooks).
- The generic adapter uses an explicitly configured bearer selected by
  `X-Remount-Provider: generic`. Comparison is over fixed-size SHA-256 digests
  in constant time. It is distinct from an operator/API bearer.

Provider selection is fail-closed: the presence of a provider header cannot
fall through to another authentication method after verification fails.
Provider secrets remain configuration-only and never enter errors, canonical
events or logs. A valid request maps to a stable event type such as
`webhook.github.issue_comment`, `webhook.slack.app_mention` or
`webhook.linear.issue.create`; its decoded JSON becomes the canonical CBOR
payload. The old Remount envelope and its shared-HMAC compatibility path remain
additive.

Durable workspace timers gain a bounded `Match map[string]string`. Every entry
must match the posted payload; dotted paths select nested fields, while `repo`
and `label` cover the common GitHub `repository.full_name` and issue-label
shapes. A malformed or absent payload does not match. The canonical event is
committed before timer evaluation, and only a timer in the event's tenant (and,
when supplied, stream) may wake.

### Outbound notifier

Notifications are configured subscriptions, never destinations learned from
event payloads. Supported filters are `egress.pending`, `run.finished`,
`ws.fenced`, and `pool.*`; supported adapters are `slack_webhook` and
`generic_webhook`. Slack receives a bounded incoming-webhook JSON message as
described by [Slack's incoming webhook documentation](https://api.slack.com/messaging/webhooks).
Generic receivers get versioned JSON containing canonical event metadata, not
raw payloads. The notifier does not reproduce event payloads because they are
unnecessary for routing and widen the disclosure boundary.

Each tenant worker reads the committed canonical log through
`eventlog.Exporter` in count- and byte-bounded batches with concurrency one.
Its tenant-scoped `CursorStore` is the durable fence. For every batch:

1. unmatched events are still consumed as part of the contiguous sequence;
2. each matching configured destination is attempted with bounded exponential
   backoff and a request deadline;
3. a successful HTTP response is accepted before cursor compare-and-swap; or
4. after retry exhaustion, a sanitized dead-letter row must commit and a
   `notifier.dead_lettered` signal must commit before the cursor may advance.

A crash after external acceptance and before the cursor commit can duplicate a
notification. No ordering can eliminate that ambiguity without cooperation
from the receiver, so generic notifications include sequence numbers suitable
for receiver-side deduplication. A failed delivery, dead-letter commit or
signal commit never advances the cursor and emits/returns
`notifier.unavailable`; it is never reported healthy.

Dead letters contain only tenant, subscription id, sequence range, event types,
attempt count, a fixed reason class and timestamp. They exclude URL, Slack path,
bearer, response body, transport error and raw event payload. The SQLite store
has per-tenant admission, list and bounded prune operations; a full store
rejects rather than silently evicting an unexamined failure.

Destination validation requires HTTPS port 443, forbids redirects, URL
userinfo, query and fragment, accepts Slack only at `hooks.slack.com/services/…`,
and requires an exact configured hostname allow-list for generic receivers.
DNS is re-resolved before every attempt, all answers must be public, and the
production transport dials only one of the addresses that passed that check.
Private, loopback, link-local, multicast, unspecified and documentation
networks are rejected, preventing a configured hostname from rebinding into a
node-local service.

## Lifecycle boundary

- **Authority:** the committed canonical event log and the tenant-scoped
  compare-and-swap cursor.
- **Resource:** the bounded inbound body, export batch, subscription set,
  retry budget and dead-letter rows.
- **Irreversible action:** an outbound HTTPS request accepted by a receiver.
- **Fence:** provider MAC/replay window on ingress; canonical sequence and
  cursor revision on egress.
- **Durable commit point:** canonical append before wake evaluation; receiver
  acceptance or durable dead-letter plus signal before cursor advancement.
- **Observable postcondition:** the matching workspace resumes, or each
  selected event is delivered at least once, remains pending at its cursor, or
  has a visible sanitized dead-letter/unavailable signal. There is no silent
  skip.

## Consequences

Provider adapters remain small translation boundaries, while all lifecycle
authority stays in control. Timer matches survive controller restart because
they are part of the existing durable timer record.

Delivery is intentionally at-least-once, not exactly-once. Receivers should
deduplicate by subscription and canonical sequence. One slow destination can
hold its tenant worker at the current bounded batch; separate cursors isolate
other tenant workers. No real Slack credential was provisioned for this phase,
so acceptance is proven against a local receiver and must be labelled
"untested against Slack" until a vendor lane is run.
