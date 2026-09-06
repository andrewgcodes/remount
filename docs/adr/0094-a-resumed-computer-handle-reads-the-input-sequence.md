# ADR 0094: A resumed computer handle reads the node's input sequence

## Status

Accepted. Refines ADR 0088.

## Context

ADR 0088 deduplicates `computer.input` by a per-computer sequence and says:
"The SDK keeps the counter, so a handle that reconnects and replays lands on a
sequence the node has already applied and is dropped."

That sentence is true, and the model behind it is only coherent when a
reconstructed handle is used to replay the *same* action. A handle addressed by
id has no memory of what it sent. It starts at one, and the node has already
applied one — and two, and three. Every action such a handle issues, up to the
node's high-water mark, is silently discarded.

The CLI made the cost concrete the first time it was driven against a real
Chromium. Each `remount computer …` invocation is a separate process holding a
separate handle, so the sequence restarts at one every time:

```
remount computer click "$WS" "$CMP" 150 50    # iseq 1 -> applied
remount computer type  "$WS" "$CMP" 'text'    # iseq 1 -> dropped as a duplicate
remount computer key   "$WS" "$CMP" Enter     # iseq 1 -> dropped as a duplicate
```

The click landed and nothing else did. The page reported an empty field and no
error was returned anywhere: `computer.input` answers `applied:false`, which the
handle discarded. A silent no-op is the worst shape this could have taken.

The two situations the one counter was covering are not the same:

- **A retried request** — the transport lost a response and `nodeCall` sends the
  identical body again. The sequence is unchanged, and dedup is exactly right.
- **A new action from a new handle** — the caller's process restarted, or a
  different process is driving the same browser. Nothing here is a replay, and
  dropping it loses input.

## Decision

**A handle that did not create the computer learns the node's applied sequence
before its first action.**

- `ComputerGetRes` gains `last_iseq`, the highest input sequence the node has
  applied. It is additive and older peers omit it, which reads as zero.
- `Client.Computer(ws, id)` — and the Python and TypeScript equivalents —
  return an unsynced handle. Its first `Input` performs a `computer.get`,
  adopts `last_iseq`, and continues from there. A handle returned by
  `CreateComputer` is synced already and pays for nothing.
- `Get` seeds the sequence whenever it is called, so a caller that already
  polls state never pays twice.

Dedup itself is unchanged. Within one logical `Input` call the sequence is
computed once and every transport retry carries it, so a lost response still
cannot produce a second click. What changes is that a *fresh* handle no longer
pretends its first action was a replay of an action it has never seen.

The narrow case this gives up is a caller that deliberately rebuilds a handle
by id in order to re-send an action it is unsure landed. That caller now gets
the action applied. It was never a supported way to ask the question: the
handle it rebuilt cannot know which sequence the lost attempt used, so the
protection it appeared to have was accidental. A caller that needs at-most-once
across a process boundary should reuse the handle, whose counter survives every
reconnect the client makes internally.

## Consequences

- `remount computer` works. Each invocation costs one extra round trip on its
  first action, which a CLI can afford and a long-lived handle never pays.
- `computer get --json` now reports `last_iseq`, so an operator can see how far
  a browser's input has advanced without guessing.
- The sim test that asserted the old behaviour now asserts this one, and keeps
  a direct proof of node-level dedup: two handles synced to the same point send
  the same sequence, and only the first is applied.
