# The node audit stream stopped at a restart — 2026-09-04

A node's observations stopped reaching the control plane's canonical log after
the node process restarted. Nothing reported it. `remount doctor` said
`healthy`, workspaces claimed and ran, sessions produced output, and
credentials were brokered to a live provider — with no record of any of it.

This was found on a real deployment, not in a test. It is the class of defect a
test suite is least likely to catch, because every component was working.

## What was observed

On the Modal deployment, after a redeploy restarted the node:

```
control's log, after the restart          control's log, before it
  ws.created        origin=control          s.opened      origin=node
  ws.claiming       origin=control          s.exited      origin=node
  ws.claimed        origin=control          fs.write      origin=node
  session.log.committed  origin=control     ws.snapshot   origin=node
  (nothing with origin=node, ever again)
```

Every event still arriving was emitted by *control*. Every event the *node*
observed was gone. The workspace was created with `secret_mode: brokered`, an
OpenAI request through the broker returned 200 with a real completion, and
neither `cred.used` nor `egress.allowed` was recorded.

The same binary on a standalone (server and node in one process) recorded all
of them, which is what made this hard to see: the topology that hides the bug
is the one a developer runs on a laptop, and the topology that has it is every
production deployment.

## Why

A node keeps its event store in memory (`eventlog.NewMemory`). Its producer
sequence therefore began again at 1 on every restart, while control still held
the watermark from that node identity's previous life.

Control's `eventsPost` is correct to distrust that. For a sequence at or below
its watermark it looks up what it already stored under `<node>:<seq>`; if the
event differs, it answers:

```
conflict: node event sequence 1 is out of order or changed
```

which is the right answer to a node claiming a sequence that already names a
different event. The node's forwarder, however, treated every failure as
transient and retried the same batch — forever. The first post-restart event
could never be accepted, so nothing behind it was ever sent. One permanent
rejection at the head of the queue stopped the whole audit stream, silently.

## The fix

Three changes, in `internal/node`:

1. **The producer watermark is durable.** `producer-seq.cbor` records the
   highest sequence the node has assigned, written through a rename. A restart
   resumes above it, so the ordinary case no longer collides at all.
2. **A collision is recoverable.** If control still reports one — an older
   node with no watermark file, or a watermark lost to a crash — the node
   re-issues the batch above the collision instead of retrying it. Control
   records the jump as one `event.producer_gap`, which is the honest account of
   what happened.
3. **A permanent rejection cannot block the queue.** Any other outright
   rejection (`bad_request`, `denied`, `resource_exhausted`) is bisected until
   the event control objects to stands alone; that one is dropped and counted
   in `remount_node_events_dropped_total`, and everything behind it proceeds.

`remount_node_event_post_failures_total` and
`remount_node_events_resequenced_total` make the recovery visible rather than
silent.

## Verified against the deployment that showed it

Redeployed to Modal with a node data directory carrying no watermark file, so
the first restart hit the collision path deliberately:

```
remount_node_event_post_failures_total   1     one collision
remount_node_events_resequenced_total    1     one re-issue
seq=1687  origin=control  event.producer_gap   the jump, recorded
seq=1688  origin=node     s.opened
seq=1689  origin=node     cred.used        binding=b_openai  decision=substituted
seq=1690  origin=node     egress.allowed
seq=1692  origin=node     s.exited
```

Three regression tests, each of which fails without the change:
`TestProducerSequenceContinuesAcrossRestart`,
`TestEventLoopReissuesABatchAfterASequenceCollision`, and
`TestEventLoopDropsOnlyTheEventControlRejects` in `internal/node`, plus
`TestNodeEventsSurviveANodeRestart` in `internal/sim`, which drives a real
restart through the simulated fleet.

## What this independently confirms

`docs/engineering/deep-bug-hunt-2026-09-03.md` (PR #7) lists this as **RBH-002**,
"Node event producer sequence restarts and its outbox is lost on process
restart", found by reading the code. It was reached here from the opposite
direction — watching a live deployment go quiet. Two independent paths to the
same defect is a reason to treat the rest of that report's findings as live.

## What to be careful of nearby

Moving a sequence space forward invalidates every cursor into it. Both `Reset`
call sites above raise the store's floor, and a read below the floor answers
`CodeEvicted`; the forwarder's subscription had to be re-seated in the same
change, at startup and after a re-issue. See `MISTAKES.md` #50 — this was
gotten wrong twice before it was gotten right, and both times it presented as
the fix not working rather than as a new bug.
